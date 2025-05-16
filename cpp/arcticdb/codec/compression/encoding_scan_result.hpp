#pragma once

#include <cstddef>
#include <cstdint>

#include <arcticdb/codec/compression/encoder_data.hpp>
#include <arcticdb/util/configs_map.hpp>

namespace arcticdb {

struct EncodingScanResult {

    EncodingScanResult() = default;

    EncodingScanResult(
            size_t cost,
            size_t estimated_size,
            size_t original_size,
            EncodingType type,
            bool is_deterministic,
            EncoderData&& data) :
        cost_(cost),
        estimated_size_(estimated_size),
        original_size_(original_size),
        type_(type),
        is_deterministic_(is_deterministic),
        data_(std::move(data)) {
    }

    ARCTICDB_MOVE_COPY_DEFAULT(EncodingScanResult)

    size_t cost_ = std::numeric_limits<size_t>::max();
    size_t estimated_size_ = 0UL;
    size_t original_size_ = 0UL;
    EncodingType type_ = EncodingType::PLAIN;
    bool is_deterministic_ = false;
    EncoderData data_ = EncoderData{};
};

inline size_t calculate_score(size_t speed_factor, size_t estimated_size, size_t original_size, size_t weight) {
    weight = std::min<size_t>(100UL, weight);
    size_t size_ratio = (estimated_size * 100) / original_size;
    size_t score = (weight * size_ratio + (100 - weight) * speed_factor) / 100;
    return score;
}

// The below function can be used to compute a weighting for the size vs speed in
// compression. Currently, we use a default weighting of 17, which means that for
// an encoding with a speed factor of 10 (10 microseconds per 100k rows) and another
// encoding with a speed factor of 20, the slower encoding will be chosen when the
// estimated size of the faster encoding is > 1.5x that of the slower encoding.
[[maybe_unused]] inline size_t compute_size_weight(double ratio, size_t baseline_size_ratio = 100) {
    double denominator = static_cast<double>(baseline_size_ratio) * (ratio - 1.0) + 10.0;
    if (denominator <= 0.0)
        return 100;

    double computed_weight = 1000.0 / denominator;
    auto weight = static_cast<size_t>(std::round(computed_weight));

    return std::min<size_t>(weight, 100UL);
}

inline EncodingScanResult create_scan_result(
        EncodingType encoding_type,
        size_t estimated_size,
        size_t speed,
        size_t original_size,
        bool is_deterministic,
        EncoderData&& data) {
    static size_t weight = ConfigsMap::instance()->get_int("Scanner.SizeWeighting", 50);
    auto score = calculate_score(speed, estimated_size, original_size, weight);
    return {score, estimated_size, original_size, encoding_type, is_deterministic, std::move(data)};
}

constexpr size_t MAX_ENCODINGS = 3;

struct EncodingScanResultSet {
    std::array<EncodingScanResult, MAX_ENCODINGS> results_;
    size_t members_ = 0UL;

    void try_emplace(const EncodingScanResult& result) {
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

    void set_first(EncodingScanResult&& result) {
        results_[0] = std::move(result);
        members_ = 1;
        select(0);
    }

    void sort() {
        std::sort(std::begin(results_), std::begin(results_) + members_, [] (const auto& l, const auto& r) {
            return std::tie(l.cost_, l.estimated_size_) < std::tie(r.cost_, r.estimated_size_);
        });
    }

    [[nodiscard]] size_t size() const {
        return members_;
    }

    const EncodingScanResult& operator[](size_t pos) const {
        return results_[pos];
    }

    EncodingScanResult& operator[](size_t pos) {
        return results_[pos];
    }

    [[nodiscard]] EncodingScanResult& first() {
        util::check(members_ == 1, "Unexpected number of results: {}", members_);
        return results_[0];
    }

    void select(size_t offset) {
        util::check(offset < members_, "Out-of-bounds offset given to EncodingScanResultSet: {}", offset);
        if(offset != 0)
            std::swap(results_[0], results_[offset]);

        members_ = 1;
    }

    void select(size_t offset, EncodingScanResult&& result) {
        util::check(offset < members_, "Out-of-bounds offset given to EncodingScanResultSet: {}", offset);
        if(offset != 0)
            std::swap(results_[0], results_[offset]);

        results_[0] = std::move(result);
        members_ = 1;
    }

    void finalize() {
        std::sort(std::begin(results_), std::begin(results_) + members_, [] (const auto& l, const auto & r) {
            return std::tie(l.cost_, l.estimated_size_) < std::tie(r.cost_, r.estimated_size_);
        });
    }
};

struct SegmentScanResults {
    ankerl::unordered_dense::map<position_t, EncodingScanResultSet> values_;
    ankerl::unordered_dense::map<position_t, EncodingScanResultSet> shapes_;

    void add_value(position_t column_index, const EncodingScanResultSet& results) {
        values_.emplace(column_index, results);
    }

    EncodingScanResultSet& value(position_t column_index) {
        auto it = values_.find(column_index);
        util::check(it != values_.end(), "No scan results found for values at column {}", column_index);
        return it->second;
    }

    [[nodiscard]] size_t values_size() const {
        return values_.size();
    }

    void reserve_values(size_t size) {
        values_.reserve(size);
    }

    void add_shape(position_t column_index, const EncodingScanResultSet& results) {
        shapes_.emplace(column_index, results);
    }

    EncodingScanResultSet& shape(position_t column_index) {
        auto it = shapes_.find(column_index);
        util::check(it != shapes_.end(), "No scan results found for values at column {}", column_index);
        return it->second;
    }

    [[nodiscard]] bool empty() const {
        return values_.empty();
    }
};

} // namespace arcticdb
