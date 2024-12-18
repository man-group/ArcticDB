#pragma once

#include <arcticdb/storage/memory_layout.hpp>

#include <ankerl/unordered_dense.h>

#include <span>
#include <algorithm>

namespace arcticdb {

template <typename T>
void set_value(T value, uint64_t& target) {
    memcpy(&target, &value, sizeof(T));
}

template <typename T>
void get_value(uint64_t value, T& target) {
    memcpy(&target, &value, sizeof(T));
}

class FieldStatsImpl : public FieldStats {
    FieldStatsImpl() = default;

    ARCTICDB_MOVE_COPY_DEFAULT(FieldStatsImpl)

    template <typename T>
    void set_max(T value) {
        set_value<T>(value, max_);
    }

    template <typename T>
    void set_min(T value) {
        set_value<T>(value, min_);
    }

    template <typename T>
    T get_max() {
        T value;
        get_value<T>(max_, value);
        return value;
    }

    template <typename T>
    T get_min() {
        T value;
        get_value<T>(max_, value);
        return value;
    }

    template<typename T>
    FieldStatsImpl(
            T max,
            T min,
            uint32_t unique_count,
            UniqueCountType unique_count_precision) {
        set_max(max);
        set_min(min);
        unique_count_ = unique_count;
        unique_count_precision_ = unique_count_precision;
        set_ = true;
    }
};

template <typename T>
FieldStats generate_statistics(std::span<T> data) {
    auto [col_min, col_max] = std::minmax_element(std::begin(data), std::end(data));
    ankerl::unordered_dense::set<T> unique;
    for(auto val : data) {
        unique.try_emplace(val);
    }
    FieldStats
}

} // namespace arcticdb