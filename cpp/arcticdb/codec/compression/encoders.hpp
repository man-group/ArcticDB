#pragma once

#include <arcticdb/column_store/statistics.hpp>
#include <arcticdb/storage/memory_layout.hpp>
#include <arcticdb/codec/compression/estimators.hpp>
#include <arcticdb/codec/compression/delta.hpp>

#include <arcticdb/codec/compression/encoding_scan_result.hpp>
namespace arcticdb {

constexpr size_t NUM_SAMPLES = 10;

template <typename T>
concept Encoder = requires(
        FieldStatsImpl field_stats,
        DataType data_type,
        size_t sample_count,
        ColumnData data,
        size_t row_count) {
    { T::is_viable(field_stats, data_type, sample_count) } -> std::same_as<bool>;
    { T::deterministic_size(field_stats, data_type, row_count) } -> std::same_as<std::optional<size_t>>;
    { T::max_compressed_size(field_stats, data_type, row_count, data) } -> std::same_as<EncodingScanResult>;
    { T::estimated_size(field_stats, data_type, data, row_count) } -> std::same_as<size_t>;
    { T::speed_factor() } -> std::same_as<size_t>;
};

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

    static size_t speed_factor() {
        return 10;
    }

    static std::optional<size_t> deterministic_size(FieldStatsImpl field_stats, DataType data_type, size_t num_rows) {
        return TypeDescriptor{data_type, Dimension::Dim0}.visit_tag([field_stats, num_rows] (auto tag) {
            using TagType = decltype(tag);
            auto range = get_range<typename TagType::DataTypeTag>(field_stats);
            return std::bit_width(range) * num_rows;
        });
    }

    static EncodingScanResult max_compressed_size(FieldStatsImpl field_stats, DataType data_type, size_t num_rows, ColumnData) {
        return create_scan_result(
            EncodingType::FFOR,
            *deterministic_size(field_stats, data_type, num_rows),
            speed_factor(),
            num_rows * get_type_size(data_type),
            true);

    }

    static size_t estimated_size(
        FieldStatsImpl field_stats,
        DataType data_type,
        ColumnData data,
        size_t row_count) {
         return TypeDescriptor{data_type, Dimension::Dim0}.visit_tag([field_stats, data, row_count] (auto tag) -> size_t {
             using TagType = typename decltype(tag)::DataTypeTag;
             if constexpr (is_integer_type(TagType::data_type)) {
                 using RawType = typename TagType::raw_type;

                 auto estimate = estimate_compression<RawType>(
                     field_stats,
                     data,
                     row_count,
                     FForEstimator<RawType>{},
                     NUM_SAMPLES);

                 return DeltaEstimator<RawType>::overhead() + estimate.estimated_bits_needed;
             } else {
                 util::raise_rte("Unsupported type for delta encoding");
             }
         });
    }
};

static_assert(Encoder<Ffor>);

struct Delta {
    static bool is_viable(FieldStatsImpl field_stats, DataType, size_t) {
        return field_stats.get_sorted() == SortedValue::ASCENDING;
    }

    static std::optional<size_t> deterministic_size(FieldStatsImpl, DataType, size_t) {
        return std::nullopt;
    }

    static EncodingScanResult max_compressed_size(FieldStatsImpl, DataType data_type, size_t num_rows, ColumnData data) {
        return make_scalar_type(data_type).visit_tag([data, num_rows] (auto tag) {
            using T = decltype(tag)::DataTypeTag::raw_type;
            DeltaCompressor<T> compressor;
            const auto original_size = num_rows * sizeof(T);
            size_t bytes = compressor.scan(data, num_rows);
            auto result = create_scan_result(EncodingType::DELTA, bytes, speed_factor(), original_size, false);
            result.data_ = compressor.data();
            return result;
        });
    }

    static size_t speed_factor() {
        return 20;
    }

    static size_t estimated_size(
        FieldStatsImpl field_stats,
        DataType data_type,
        ColumnData data,
        size_t row_count) {
        return TypeDescriptor{data_type, Dimension::Dim0}.visit_tag([data, row_count, field_stats] (auto tag) -> size_t {
            using TagType = typename decltype(tag)::DataTypeTag;
            if constexpr (is_integer_type(TagType::data_type)) {
                using RawType = typename TagType::raw_type;

                auto estimate = estimate_compression<RawType>(
                    field_stats,
                    data,
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

static_assert(Encoder<Delta>);

struct Frequency {
    static bool is_viable(FieldStatsImpl field_stats, DataType, size_t row_count) {
        return field_stats.get_unique_count() < (row_count / 10);
    }

    static std::optional<size_t> deterministic_size(FieldStatsImpl, DataType, size_t) {
        return std::nullopt;
    }

    static EncodingScanResult max_compressed_size(FieldStatsImpl , DataType , size_t , ColumnData ) {
        return {};
    }

    static size_t speed_factor() {
        return 20;
    }
    static size_t estimated_size(
        FieldStatsImpl field_stats,
        DataType data_type,
        ColumnData data,
        size_t row_count) {
        return TypeDescriptor{data_type, Dimension::Dim0}.visit_tag([field_stats, data, row_count] (auto tag) -> size_t {
            using TagType = typename decltype(tag)::DataTypeTag;
            if constexpr (is_integer_type(TagType::data_type)) {
                using RawType = typename TagType::raw_type;

                auto estimate = estimate_compression<RawType>(
                    field_stats,
                    data,
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

static_assert(Encoder<Frequency>);

struct Constant {
    static bool is_viable(FieldStatsImpl field_stats, DataType, size_t ) {
        return field_stats.get_unique_count() == 1;
    }

    static size_t speed_factor() {
        return 20;
    }
    static size_t estimated_size(
        FieldStatsImpl,
        DataType,
        ColumnData,
        size_t) {
        util::raise_rte("Unexpected estimating request in constant encoding");
    }

    static EncodingScanResult max_compressed_size(FieldStatsImpl field_stats, DataType data_type, size_t num_rows, ColumnData) {
        return create_scan_result(
            EncodingType::CONSTANT,
            *deterministic_size(field_stats, data_type, num_rows),
            speed_factor(),
            num_rows * get_type_size(data_type),
            true);
    }

    static std::optional<size_t> deterministic_size(FieldStatsImpl, DataType data_type, size_t) {
        return get_type_size(data_type);
    }
};

static_assert(Encoder<Constant>);

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

    static size_t speed_factor() {
        return 20;
    }
    static std::optional<size_t> deterministic_size(FieldStatsImpl field_stats, DataType data_type, size_t row_count) {
            return TypeDescriptor{data_type, Dimension::Dim0}.visit_tag([field_stats, row_count] (auto tag) {
            using TagType = decltype(tag);
            auto max = get_max<typename TagType::DataTypeTag>(field_stats);
            return std::bit_width(max) * row_count;
        });
    }

    static EncodingScanResult max_compressed_size(FieldStatsImpl field_stats, DataType data_type, size_t num_rows, ColumnData) {
       return create_scan_result(
            EncodingType::BITPACK,
            *deterministic_size(field_stats, data_type, num_rows),
            speed_factor(),
            num_rows * get_type_size(data_type),
            true);
    }

    static size_t estimated_size(
        FieldStatsImpl,
        DataType,
        ColumnData,
        size_t) {
        util::raise_rte("Unexpected estimating request in bitpack encoding");
    }

};

static_assert(Encoder<Constant>);

struct Alp {
    static bool is_viable(FieldStatsImpl, DataType, size_t) {
        return true;
    }

    static size_t speed_factor() {
        return 20;
    }
    static size_t estimated_size(FieldStatsImpl, DataType, ColumnData, size_t) {
        return 12;
    }

    static EncodingScanResult max_compressed_size(FieldStatsImpl , DataType , size_t , ColumnData) {
        return {};
    }


    static std::optional<size_t> deterministic_size(FieldStatsImpl, DataType, size_t) {
        return std::nullopt;
    }
};

static_assert(Encoder<Alp>);

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
    return dispatch_encoding(encoding_type, [&field_stats, data_type, row_count]( auto&& encoder) {
        return encoder.is_viable(field_stats, data_type, row_count);
    });
}

inline std::optional<size_t> deterministic_size(
        EncodingType encoding_type,
        FieldStatsImpl field_stats,
        DataType data_type,
        size_t row_count) {
    return dispatch_encoding(encoding_type, [&field_stats, data_type, row_count](auto &&encoder) {
        return encoder.deterministic_size(field_stats, data_type, row_count);
    });
}

inline size_t estimated_size(
        EncodingType encoding_type,
        FieldStatsImpl field_stats,
        DataType data_type,
        ColumnData data,
        size_t row_count) {
    return dispatch_encoding(encoding_type, [&field_stats, data_type, data, row_count](auto &&encoder) {
        return encoder.estimated_size(field_stats, data_type, data, row_count);
    });
}

inline size_t speed_factor(EncodingType encoding_type) {
    return dispatch_encoding(encoding_type, [](auto &&encoder) {
        return encoder.speed_factor();
    });
}

inline EncodingScanResult max_compressed_size(
    EncodingType encoding_type,
    FieldStatsImpl field_stats,
    DataType data_type,
    size_t row_count,
    ColumnData data) {
    return dispatch_encoding(encoding_type, [&field_stats, data_type, row_count, data](auto &&encoder) {
        return encoder.max_compressed_size(field_stats, data_type, row_count, data);
    });
}

} // namespace arcticdb
