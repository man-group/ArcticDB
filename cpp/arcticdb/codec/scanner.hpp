#pragma once

#include <arcticdb/codec/default_codecs.hpp>
#include <arcticdb/codec/encoded_field.hpp>
#include <arcticdb/column_store/column_data.hpp>
#include <arcticdb/column_store/statistics.hpp>
#include <arcticdb/storage/memory_layout.hpp>
#include <arcticdb/codec/compression/encoders.hpp>
#include <arcticdb/codec/compression/encoder_data.hpp>
#include <arcticdb/codec/compression/encoding_scan_result.hpp>
#include <arcticdb/util/configs_map.hpp>

namespace arcticdb {

class EncodingsList {
    uint64_t data_;

    static constexpr uint8_t to_pos(EncodingType encoding) {
        return 1 << (static_cast<uint8_t>(encoding));
    }

public:
    constexpr EncodingsList() : data_(0) {}

    constexpr EncodingsList(std::initializer_list<EncodingType> encodings) : data_(0) {
        for (const auto& encoding : encodings) {
            set(encoding);
        }
    }

    constexpr void set(EncodingType encoding) {
        data_ |= to_pos(encoding);
    }

    constexpr void unset(EncodingType encoding) {
        data_ &= ~to_pos(encoding);
    }

    [[nodiscard]] constexpr bool is_set(EncodingType encoding) const {
        return data_ & to_pos(encoding);
    }
};

constexpr EncodingsList IntegerEncodings {
    EncodingType::CONSTANT,
    EncodingType::DELTA,
    EncodingType::FFOR,
    EncodingType::FREQUENCY
    //EncodingType::RLE
};

constexpr EncodingsList FloatEncodings {
    EncodingType::CONSTANT,
    EncodingType::ALP
};

constexpr EncodingsList StringEncodings {
    EncodingType::CONSTANT,
    EncodingType::DELTA,
    EncodingType::FREQUENCY
    //EncodingType::RLE
};

EncodingsList possible_encodings(DataType data_type) {
    if(is_sequence_type(data_type)) {
        return StringEncodings;
    } else if(is_integer_type(data_type) || is_time_type(data_type)) {
        return IntegerEncodings;
    } else if(is_floating_point_type(data_type)) {
        return FloatEncodings;
    } else {
       util::raise_rte("Cannot select encoding for type {}", data_type);
    }
}

EncodingsList viable_encodings(
        EncodingsList input,
        FieldStatsImpl field_stats,
        DataType data_type,
        size_t row_count) {
    EncodingsList output;
    for(auto i = 0U; i < static_cast<uint8_t>(EncodingType::COUNT); ++i) {
        const auto type = EncodingType(i);
        if(input.is_set(type) && is_viable(type, field_stats, data_type, row_count))
            output.set(type);
    }
    return output;
}
size_t calculate_score(size_t speed_factor, size_t estimated_size, size_t original_size, size_t weight) {
    weight = std::max(100UL, weight);
    size_t size_ratio = (estimated_size * 100) / original_size;
    size_t score = (weight * size_ratio + (100 - weight) * speed_factor) / 100;
    return score;
}

// The below function can be used to compute a weighting for the size vs speed in
// compression. Currently we use a default weighting of 17, which means that for
// an encoding with a speed factor of 10 (10 microseconds per 100k rows) and another
// encoding with a speed factor of 20, the slower encoding will be chosen when the
// estimated size of the faster encoding is > 1.5x that of the slower encoding.
size_t compute_size_weight(double ratio, size_t baseline_size_ratio = 100) {
    double denominator = baseline_size_ratio * (ratio - 1.0) + 10.0;
    if (denominator <= 0.0)
        return 100;

    double computed_weight = 1000.0 / denominator;
    size_t weight = static_cast<size_t>(std::round(computed_weight));

    return std::min(weight, 100UL);
}


constexpr size_t MAX_ENCODINGS = 3;

struct EncodingScanResultSet {
    std::array<EncodingScanResult, MAX_ENCODINGS> results_;
    size_t members_ = 0UL;

    void try_emplace(EncodingScanResult result) {
        if (members_ < MAX_ENCODINGS) {
            results_[members_++] = result;
        } else {
            size_t max_pos = 0;
            for (auto i = 1UL; i < members_; ++i) {
                if (results_[i].cost_ > results_[max_pos].cost_)
                    max_pos = i;
            }
            if (result.cost_ < results_[max_pos].cost_)
                results_[max_pos] = result;
        }
    }

    size_t size() const {
        return members_;
    }

    const EncodingScanResult& operator[](size_t pos) {
        return results_[pos];
    }

    void select(size_t offset) {
        util::check(offset < members_, "Out-of-bounds offset given to EncodingScanResultSet: {}", offset);
        if(offset != 0)
            std::swap(results_[0], results_[offset]);

        members_ = 1;
    }

    void finalize() {
        std::sort(std::begin(results_), std::begin(results_) + members_, [] (const auto& l, const auto & r) {
            return l.cost_ < r.cost_;
        });
    }
};

EncodingScanResultSet predicted_optimal_encodings(ColumnData column_data) {
    const auto data_type = column_data.type().data_type();
    const auto field_stats = column_data.field_stats();
    const auto row_count = column_data.row_count();
    const auto original_size = column_data.buffer().bytes();
    auto encodings_for_type = possible_encodings(data_type);
    auto filtered_encodings = viable_encodings(encodings_for_type, column_data.field_stats(), data_type, column_data.row_count());
    EncodingScanResultSet results;
    
    if(is_viable(EncodingType::CONSTANT, field_stats, data_type, row_count)) {
        auto size = deterministic_size(EncodingType::CONSTANT, field_stats, data_type, row_count);
        results.try_emplace(create_scan_result(EncodingType::CONSTANT, *size, original_size, true));
        return results;
    }

    for(auto i = 0U; i < static_cast<uint8_t>(EncodingType::COUNT); ++i) {
        const auto type = EncodingType(i);
        if(!filtered_encodings.is_set(type))
            continue;

        // Could potentially introduce a scaling factor for deterministic results vs estimated results, i.e. try
        // encodings with a guaranteed size unless the estimated size indicates it will be substantially smaller.
        if(auto exact_size = deterministic_size(type, column_data.field_stats(), data_type, column_data.row_count())) {
            results.try_emplace(create_scan_result(type, *exact_size, original_size, true));
        } else {
            auto maybe_size = estimated_size(
                type,
                column_data.field_stats(),
                data_type,
                column_data.buffer().data(),
                column_data.row_count());

            results.try_emplace(create_scan_result(type, maybe_size, original_size, false));
        }
    }

    return results;
}

} // namespace arcticdb