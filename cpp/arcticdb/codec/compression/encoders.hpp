#pragma once

#include <arcticdb/column_store/statistics.hpp>
#include <arcticdb/storage/memory_layout.hpp>
#include <arcticdb/codec/compression/estimators.hpp>

namespace arcticdb {

constexpr size_t NUM_SAMPLES = 10;

struct Ffor {
    template <typename DataTypeTag>
    static uint64_t get_range(FieldStatsImpl field_stats) {
        using RawType = DataTypeTag::raw_type;
        auto max = field_stats.get_max<RawType>();
        auto min = field_stats.get_min<RawType>();
        return max - min;
    }

    static bool is_viable(FieldStatsImpl field_stats, DataType data_type, size_t) {
        return TypeDescriptor{data_type, Dimension::Dim0}.visit_tag([field_stats] (auto tag) {
            using TagType = decltype(tag);
            auto range = get_range<typename TagType::DataTypeTag>(field_stats);
            return std::bit_width(range) < std::numeric_limits<typename TagType::DataTypeTag::raw_type>::digits;
        });
    }

    static size_t predicted_size(
        FieldStatsImpl field_stats,
        DataType data_type,
        const uint8_t* data,
        size_t row_count) {
         return TypeDescriptor{data_type, Dimension::Dim0}.visit_tag([field_stats, data, row_count] (auto tag) -> size_t {
             using TagType = typename decltype(tag)::DataTypeTag;
             if constexpr (is_integer_type(TagType::data_type)) {
                 using RawType = typename TagType::raw_type;

                 auto estimate = estimate_compression(
                     field_stats,
                     reinterpret_cast<const RawType *>(data),
                     row_count,
                     DeltaEstimator<RawType>{},
                     NUM_SAMPLES);

                 return DeltaEstimator<RawType>::overhead() + estimate.estimated_bits_needed;
             } else {
                 util::raise_rte("Unsupported type for delta encoding");
             }
         });
    }
};

struct Delta {
    static bool is_viable(FieldStatsImpl field_stats, DataType, size_t) {
        return field_stats.get_sorted() == SortedValue::ASCENDING;
    }

    static size_t predicted_size(
        FieldStatsImpl field_stats,
        DataType data_type,
        const uint8_t* data,
        size_t row_count) {
        return TypeDescriptor{data_type, Dimension::Dim0}.visit_tag([data, row_count, field_stats] (auto tag) -> size_t {
            using TagType = typename decltype(tag)::DataTypeTag;
            if constexpr (is_integer_type(TagType::data_type)) {
                using RawType = typename TagType::raw_type;

                auto estimate = estimate_compression(
                    field_stats,
                    reinterpret_cast<const RawType *>(data),
                    row_count,
                    DeltaEstimator<RawType>{},
                    NUM_SAMPLES);

                return DeltaEstimator<RawType>::overhead() + estimate.estimated_bits_needed;
            } else {
                util::raise_rte("Unsupported type for delta encoding");
            }
        });
    }
};

struct Frequency {
    static bool is_viable(FieldStatsImpl field_stats, DataType, size_t row_count) {
        return field_stats.get_unique_count() < (row_count / 10);
    }

    static size_t predicted_size(
                FieldStatsImpl field_stats,
        DataType data_type,
        const uint8_t* data,
        size_t row_count) {
        return TypeDescriptor{data_type, Dimension::Dim0}.visit_tag([field_stats, data, row_count] (auto tag) -> size_t {
            using TagType = typename decltype(tag)::DataTypeTag;
            if constexpr (is_integer_type(TagType::data_type)) {
                using RawType = typename TagType::raw_type;

                auto estimate = estimate_compression(
                    field_stats,
                    reinterpret_cast<const RawType *>(data),
                    row_count,
                    DeltaEstimator<RawType>{},
                    NUM_SAMPLES);

                return DeltaEstimator<RawType>::overhead() + estimate.estimated_bits_needed;
            } else {
                util::raise_rte("Unsupported type for delta encoding");
            }
        });
    }
};

struct Constant {
    static bool is_viable(FieldStatsImpl field_stats, DataType, size_t ) {
        return field_stats.get_unique_count() == 1;
    }

    static bool predicted_size(FieldStatsImpl, DataType data_type, size_t) {
        return get_type_size(data_type);
    }
};

struct Alp {
    static bool is_viable(FieldStatsImpl, DataType, size_t) {
        return true;
    }
};

struct BitPack {
    template <typename DataTypeTag>
    static uint64_t get_max(FieldStatsImpl field_stats) {
        using RawType = DataTypeTag::raw_type;
        return field_stats.get_max<RawType>();
    }

    static bool is_viable(FieldStatsImpl field_stats, DataType data_type, size_t) {
        return TypeDescriptor{data_type, Dimension::Dim0}.visit_tag([field_stats] (auto tag) {
            using TagType = decltype(tag);
            auto max = get_max<typename TagType::DataTypeTag>(field_stats);
            return std::bit_width(max) < std::numeric_limits<typename TagType::DataTypeTag::raw_type>::digits;
        });
    }

    static bool predicted_size(FieldStatsImpl field_stats, DataType data_type, size_t row_count) {
            return TypeDescriptor{data_type, Dimension::Dim0}.visit_tag([field_stats, row_count] (auto tag) {
            using TagType = decltype(tag);
            auto max = get_max<typename TagType::DataTypeTag>(field_stats);
            return std::bit_width(max) * row_count;
        });
    }
};

template<typename F>
auto dispatch_encoding(EncodingType type, F&& f) {
    switch (type) {
    case EncodingType::FFOR:
        return std::forward<F>(f)(Ffor{});
    case EncodingType::DELTA:
        return std::forward<F>(f)(Delta{});
    case EncodingType::FREQUENCY:
        return std::forward<F>(f)(Frequency{});
    case EncodingType::CONSTANT:
        return std::forward<F>(f)(Constant{});
    case EncodingType::ALP:
        return std::forward<F>(f)(Alp{});
    case EncodingType::BITPACK:
        return std::forward<F>(f)(BitPack{});
    default:
        throw std::runtime_error("Unknown encoding type");
    }
}

inline bool is_viable(EncodingType encoding_type, FieldStatsImpl field_stats, DataType data_type, size_t row_count) {
    return dispatch_encoding(encoding_type, [&field_stats, data_type, row_count]( auto&& encoder) { return encoder.is_viable(field_stats, data_type, row_count); });
}




} // namespace arcticdb
