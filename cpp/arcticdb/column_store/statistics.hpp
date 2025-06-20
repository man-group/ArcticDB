#pragma once

#include <arcticdb/storage/memory_layout.hpp>
#include <arcticdb/column_store/column_data.hpp>

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

enum class FieldStatsValue : uint8_t {
        MIN = 1,
        MAX = 1 << 1,
        UNIQUE = 1 << 2
    };

struct FieldStatsImpl : public FieldStats {
    FieldStatsImpl() = default;

    ARCTICDB_MOVE_COPY_DEFAULT(FieldStatsImpl)

    template <typename T>
    void set_max(T value) {
        set_value<T>(value, max_);
        set_ |= static_cast<uint8_t>(FieldStatsValue::MAX);
    }

    template <typename T>
    void set_min(T value) {
        set_value<T>(value, min_);
        set_ |= static_cast<uint8_t>(FieldStatsValue::MIN);
    }

    void set_unique(
            uint32_t unique_count,
            UniqueCountType unique_count_precision) {
        unique_count_ = unique_count;
        unique_count_precision_ = unique_count_precision;
        set_ |= static_cast<uint8_t>(FieldStatsValue::UNIQUE);
    }

    [[nodiscard]] bool has_max() const {
        return set_ & static_cast<uint8_t>(FieldStatsValue::MAX);
    }

    [[nodiscard]] bool has_min() const {
        return set_ & static_cast<uint8_t>(FieldStatsValue::MIN);
    }

    [[nodiscard]] bool has_unique() const {
        return set_ & static_cast<uint8_t>(FieldStatsValue::UNIQUE);
    }

    [[nodiscard]] bool unique_count_is_precise() const {
        return unique_count_precision_ == UniqueCountType::PRECISE;
    };

    template <typename T>
    T get_max() {
        T value;
        get_value<T>(max_, value);
        return value;
    }

    template <typename T>
    T get_min() {
        T value;
        get_value<T>(min_, value);
        return value;
    }

    size_t get_unique_count() const {
        return unique_count_;
    }

    FieldStatsImpl(FieldStats base) {
        min_ = base.min_;
        max_ = base.max_;
        unique_count_ = base.unique_count_;
        unique_count_precision_ = base.unique_count_precision_;
        set_ = base.set_;
    }

    template<typename T>
    FieldStatsImpl(
            T min,
            T max,
            uint32_t unique_count,
            UniqueCountType unique_count_precision) {
        set_min(min);
        set_max(max);
        set_unique(unique_count, unique_count_precision);
    }

    FieldStatsImpl(
        uint32_t unique_count,
        UniqueCountType unique_count_precision) {
      set_unique(unique_count, unique_count_precision);
    }


    template<typename T>
    void compose(const FieldStatsImpl& other) {
        if (other.has_min()) {
            if (!has_min()) {
                min_ = other.min_;
                set_ |= static_cast<uint8_t>(FieldStatsValue::MIN);
            } else {
                T this_min, other_min;
                get_value<T>(min_, this_min);
                get_value<T>(other.min_, other_min);
                T result_min = std::min(this_min, other_min);
                set_value<T>(result_min, min_);
            }
        }

        if (other.has_max()) {
            if (!has_max()) {
                max_ = other.max_;
                set_ |= static_cast<uint8_t>(FieldStatsValue::MAX);
            } else {
                T this_max, other_max;
                get_value<T>(max_, this_max);
                get_value<T>(other.max_, other_max);
                T result_max = std::max(this_max, other_max);
                set_value<T>(result_max, max_);
            }
        }

        if (other.has_unique()) {
            if (!has_unique()) {
                unique_count_ = other.unique_count_;
                unique_count_precision_ = other.unique_count_precision_;
                set_ |= static_cast<uint8_t>(FieldStatsValue::UNIQUE);
            } else {
                util::check(unique_count_precision_ == other.unique_count_precision_,
                            "Mismatching unique count precision, {} != {}",
                            uint8_t(unique_count_precision_), uint8_t(other.unique_count_precision_));

                unique_count_ += other.unique_count_;
            }
        }
    }
};

template <typename T>
FieldStatsImpl generate_numeric_statistics(std::span<const T> data) {
    if(data.empty())
        return FieldStatsImpl{};

    auto [col_min, col_max] = std::minmax_element(std::begin(data), std::end(data));
    ankerl::unordered_dense::set<T> unique;
    for(auto val : data) {
        unique.emplace(val);
    }
    FieldStatsImpl field_stats(*col_min, *col_max, unique.size(), UniqueCountType::PRECISE);
    return field_stats;
}

inline FieldStatsImpl generate_string_statistics(std::span<const uint64_t> data) {
    ankerl::unordered_dense::set<uint64_t> unique;
    for(auto val : data) {
        unique.emplace(val);
    }
    FieldStatsImpl field_stats(unique.size(), UniqueCountType::PRECISE);
    return field_stats;
}

template <typename TagType>
FieldStatsImpl generate_column_statistics(ColumnData column_data) {
    using RawType = typename TagType::DataTypeTag::raw_type;
    if(column_data.num_blocks() == 1) {
        auto block = column_data.next<TagType>();
        const RawType* ptr = block->data();
        const size_t count = block->row_count();
        if constexpr (is_numeric_type(TagType::DataTypeTag::data_type)) {
            return generate_numeric_statistics<RawType>(std::span{ptr, count});
        } else if constexpr (is_dynamic_string_type(TagType::DataTypeTag::data_type) && !is_arrow_output_only_type(TagType::DataTypeTag::data_type)) {
            return generate_string_statistics(std::span{ptr, count});
        } else {
            util::raise_rte("Cannot generate statistics for data type");
        }
    } else {
        FieldStatsImpl stats;
        while (auto block = column_data.next<TagType>()) {
            const RawType* ptr = block->data();
            const size_t count = block->row_count();
            if constexpr (is_numeric_type(TagType::DataTypeTag::data_type)) {
                auto local_stats = generate_numeric_statistics<RawType>(std::span{ptr, count});
                stats.compose<RawType>(local_stats);
            } else if constexpr (is_dynamic_string_type(TagType::DataTypeTag::data_type) && !is_arrow_output_only_type(TagType::DataTypeTag::data_type)) {
                auto local_stats = generate_string_statistics(std::span{ptr, count});
                stats.compose<RawType>(local_stats);
            } else {
                util::raise_rte("Cannot generate statistics for data type");
            }
        }
        return stats;
    }
}
} // namespace arcticdb