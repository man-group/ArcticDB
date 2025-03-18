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
        results.try_emplace(create_scan_result(EncodingType::CONSTANT, *size, Constant::speed_factor(), original_size, true));
        return results;
    }

    for(auto i = 0U; i < static_cast<uint8_t>(EncodingType::COUNT); ++i) {
        const auto type = EncodingType(i);
        if(!filtered_encodings.is_set(type))
            continue;

        // Could potentially introduce a scaling factor for deterministic results vs estimated results, i.e. try
        // encodings with a guaranteed size unless the estimated size indicates it will be substantially smaller.
        if(auto exact_size = deterministic_size(type, column_data.field_stats(), data_type, column_data.row_count())) {
            results.try_emplace(create_scan_result(type, *exact_size, speed_factor(type), original_size, true));
        } else {
            auto maybe_size = estimated_size(
                type,
                column_data.field_stats(),
                data_type,
                column_data,
                column_data.row_count());

            results.try_emplace(create_scan_result(type, maybe_size, speed_factor(type), original_size, false));
        }
    }

    return results;
}

// For the time being an acceptable result is just one that achieves some degree of compression, however this could be
// made more sophisticated if multiple results were to be evaluated together (see below)
bool is_acceptable_result(const EncodingScanResult& result) {
    return result.estimated_size_ < result.original_size_;
}

void resolve_adaptive_encodings_size(const SegmentInMemory& seg, std::vector<EncodingScanResultSet>& column_encodings, SizeResult& result) {
    util::check(column_encodings.size() == seg.num_columns(), "Expected equality of encoding scan results to columns: {} != {}", column_encodings.size(), seg.num_columns());
    bool found_encoding = false;
    for (std::size_t column_index = 0; column_index < seg.num_columns(); ++column_index) {
        auto& encoding = column_encodings[column_index];        // We could potentially keep an array of results that on a full scan were worse than the partial scan predicted, but
        // might still be better than subsequent trials, then compare them all together at the end
        for(auto i = 0UL; i < encoding.size(); ++i) {
            if(encoding[i].is_deterministic_) {
                encoding.select(i);
                result.max_compressed_bytes_ += encoding[i].estimated_size_;
                result.uncompressed_bytes_ += encoding[i].original_size_;
                found_encoding = true;
                break;
            }

            const auto& column = seg.column(column_index);
            auto scanned_result = max_compressed_size(encoding[i].type_, column.get_statistics(), column.type().data_type(), column.row_count(), column.data());
            if(is_acceptable_result(scanned_result)) {
                encoding.select(i);
                result.max_compressed_bytes_ += encoding[i].estimated_size_;
                result.uncompressed_bytes_ += encoding[i].original_size_;
                found_encoding = true;
                break;
            }
        }
        if(!found_encoding) {
            auto size = encoding[0].original_size_;
            encoding[0] = create_scan_result(EncodingType::CONSTANT, size, speed_factor(EncodingType::CONSTANT), size, true);
            encoding.select(0);
        }
    }
}

std::vector<EncodingScanResultSet> get_encodings(const SegmentInMemory& seg) {
    std::vector<EncodingScanResultSet> output;
    output.reserve(seg.num_columns());
    for (std::size_t column_index = 0; column_index < seg.num_columns(); ++column_index)
        output.emplace_back(predicted_optimal_encodings(seg.column(position_t(column_index)).data()));

    return output;
}

} // namespace arcticdb