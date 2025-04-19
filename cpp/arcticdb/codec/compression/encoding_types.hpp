#pragma once

#include <arcticdb/column_store/statistics.hpp>
#include <arcticdb/storage/memory_layout.hpp>
#include <arcticdb/codec/compression/estimators.hpp>
#include <arcticdb/codec/compression/delta.hpp>
#include <arcticdb/codec/compression/bitpack.hpp>
#include <arcticdb/codec/compression/constant.hpp>
#include <arcticdb/codec/compression/plain.hpp>

#include <arcticdb/codec/compression/encoding_scan_result.hpp>

namespace arcticdb {

constexpr size_t NUM_SAMPLES = 10;

template <typename T>
concept Encoder = requires(
        FieldStatsImpl field_stats,
        DataType data_type,
        size_t sample_count,
        ColumnData data,
        size_t row_count,
        size_t original_size,
        EncoderData& encoder_data) {
    { T::is_viable(field_stats, data_type, sample_count) } -> std::same_as<bool>;
    { T::deterministic_size(field_stats, data_type, row_count) } -> std::same_as<std::optional<std::pair<size_t, EncoderData>>>;
    { T::max_compressed_size(field_stats, data_type, row_count, data, encoder_data) } -> std::same_as<EncodingScanResult>;
    { T::estimated_size(field_stats, data_type, data, row_count, original_size) } -> std::same_as<EncodingScanResult>;
    { T::speed_factor() } -> std::same_as<size_t>;
};

template <typename T>
EncodingScanResult create_plain_result(size_t num_rows) {
  return create_scan_result(EncodingType::PLAIN, num_rows * sizeof(T) + PlainCompressor<T>::overhead(), 1, num_rows * sizeof(T), true, std::monostate{});
}

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
        return 11;
    }

    static std::optional<std::pair<size_t, EncoderData>> deterministic_size(FieldStatsImpl field_stats, DataType data_type, size_t num_rows) {
        return make_scalar_type(data_type).visit_tag([field_stats, num_rows] (auto tag) -> std::optional<std::pair<size_t, EncoderData>> {
            using TagType = decltype(tag);
            constexpr auto data_type = TagType::DataTypeTag::data_type;
            if constexpr(is_integer_type(data_type) || is_sequence_type(data_type)) {
                using RawType = TagType::DataTypeTag::raw_type;
                auto ffor_data = FForCompressor<RawType>::data_fom_stats(field_stats);
                const auto size = FForCompressor<RawType>::compressed_size(ffor_data, num_rows);
                return std::make_pair(size, ffor_data);
            } else {
                util::raise_rte("Unexpected type in ffor deterministic size calculation");
            }
        });
    }

    static EncodingScanResult max_compressed_size(FieldStatsImpl,  DataType data_type, size_t num_rows, ColumnData, EncoderData& data) {
        return make_scalar_type(data_type).visit_tag([data_type, num_rows, &data] (auto tag) {
            util::check(std::holds_alternative<FFORCompressData>(data), "Expected ffor compress data");
            auto ffor_data = std::get<FFORCompressData>(data);
            using TagType = decltype(tag);
            using RawType = TagType::DataTypeTag::raw_type;
            return create_scan_result(
                EncodingType::FFOR,
                FForCompressor<RawType>::compressed_size(ffor_data, num_rows),
                speed_factor(),
                num_rows * get_type_size(data_type),
                true,
                std::move(data));
        });
    }

    static EncodingScanResult estimated_size(
            FieldStatsImpl /*field_stats*/,
            DataType /*data_type*/,
            ColumnData /*data*/,
            size_t /*row_count*/,
            size_t /*original_size*/) {
        util::raise_rte("Did not expect sampling in deterministic compression");
     /*    return make_scalar_type(data_type).visit_tag([field_stats, data, row_count, original_size] (auto tag) -> EncodingScanResult {
             using TagType = typename decltype(tag)::DataTypeTag;
             if constexpr (is_integer_type(TagType::data_type)) {
                 using RawType = typename TagType::raw_type;

                 const auto estimate = estimate_compression<RawType>(
                     field_stats,
                     data,
                     row_count,
                     FForEstimator<RawType>{},
                     NUM_SAMPLES);

                 const auto size = FForEstimator<RawType>::overhead() + estimate.estimated_bytes_;
                 return create_scan_result(EncodingType::DELTA, size, speed_factor(), original_size, false);
             } else {
                 util::raise_rte("Unsupported type for delta encoding");
             }
         });*/
    }
};

static_assert(Encoder<Ffor>);

struct Delta {
    static bool is_viable(FieldStatsImpl field_stats, DataType, size_t) {
        return field_stats.get_sorted() == SortedValue::ASCENDING;
    }

    static std::optional<std::pair<size_t, EncoderData>> deterministic_size(FieldStatsImpl, DataType, size_t) {
        return std::nullopt;
    }

    static EncodingScanResult max_compressed_size(FieldStatsImpl, DataType data_type, size_t num_rows, ColumnData data, EncoderData&) {
        return make_scalar_type(data_type).visit_tag([data, num_rows] (auto tag) -> EncodingScanResult {
            using T = decltype(tag)::DataTypeTag::raw_type;
            if constexpr(std::is_integral_v<T> && !std::is_same_v<T, bool>) {
                DeltaCompressor<T> compressor;
                const auto original_size = num_rows * sizeof(T);
                size_t bytes = compressor.scan(data, num_rows);
                auto delta_data = compressor.data();
                return create_scan_result(EncodingType::DELTA, bytes, speed_factor(), original_size, false, std::move(delta_data));
            } else {
                util::raise_rte("Request to scan non-integral type in delta encoder");
            }
        });
    }

    static size_t speed_factor() {
        return 20;
    }

    static EncodingScanResult estimated_size(
        FieldStatsImpl field_stats,
        DataType data_type,
        ColumnData data,
        size_t row_count,
        size_t original_size) {
        return TypeDescriptor{data_type, Dimension::Dim0}.visit_tag([data, row_count, field_stats, original_size] (auto tag) -> EncodingScanResult {
            using TagType = typename decltype(tag)::DataTypeTag;
            if constexpr (is_integer_type(TagType::data_type)) {
                using RawType = typename TagType::raw_type;

                auto estimate = estimate_compression<RawType>(
                    field_stats,
                    data,
                    row_count,
                    DeltaEstimator<RawType>{},
                    NUM_SAMPLES);

                const auto size = DeltaEstimator<RawType>::overhead() + estimate.estimated_bytes_;
                return create_scan_result(EncodingType::DELTA, size, speed_factor(), original_size, false, std::monostate{});
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

    static std::optional<std::pair<size_t, EncoderData>> deterministic_size(FieldStatsImpl, DataType, size_t) {
        return std::nullopt;
    }

    static EncodingScanResult max_compressed_size(FieldStatsImpl , DataType data_type, size_t row_count, ColumnData column_data, EncoderData&) {
        return make_scalar_type(data_type).visit_tag([&column_data, row_count] (auto tag) {
           using RawType = decltype(tag)::DataTypeTag::raw_type;
            FrequencyCompressor<RawType> compressor;
            compressor.scan(column_data);
            auto size = compressor.max_required_bytes(column_data, row_count);
            if(!size)
                return create_plain_result<RawType>(row_count);

            return create_scan_result(
                EncodingType::FREQUENCY,
                *size,
                speed_factor(),
                row_count * sizeof(RawType),
                false,
                compressor.data()
                );
        });

        return {};
    }

    static size_t speed_factor() {
        return 20;
    }

    static EncodingScanResult estimated_size(
        FieldStatsImpl field_stats,
        DataType data_type,
        ColumnData data,
        size_t row_count,
        size_t original_size) {
        return TypeDescriptor{data_type, Dimension::Dim0}.visit_tag([field_stats, data, row_count, original_size] (auto tag) -> EncodingScanResult {
            using TagType = typename decltype(tag)::DataTypeTag;
            if constexpr (is_integer_type(TagType::data_type)) {
                using RawType = typename TagType::raw_type;

                const auto estimate = estimate_compression<RawType>(
                    field_stats,
                    data,
                    row_count,
                    FrequencyEstimator<RawType>{},
                    NUM_SAMPLES);

                const auto size = FrequencyEstimator<RawType>::overhead() + estimate.estimated_bytes_;
                return create_scan_result(EncodingType::FREQUENCY, size, speed_factor(), original_size, false, std::monostate{});
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
        return 1;
    }

    static EncodingScanResult estimated_size(
        FieldStatsImpl,
        DataType,
        ColumnData,
        size_t,
        size_t) {
        util::raise_rte("Unexpected estimating request in constant encoding");
    }

    static EncodingScanResult max_compressed_size(FieldStatsImpl field_stats, DataType data_type, size_t num_rows, ColumnData, EncoderData&) {
        auto size_calculation = deterministic_size(field_stats, data_type, num_rows);
        return create_scan_result(
            EncodingType::CONSTANT,
            size_calculation->first,
            speed_factor(),
            num_rows * get_type_size(data_type),
            true,
            std::move(size_calculation->second));
    }

    static std::optional<std::pair<size_t, EncoderData>> deterministic_size(FieldStatsImpl, DataType data_type, size_t) {
        return TypeDescriptor{data_type, Dimension::Dim0}.visit_tag([] (auto tag) {
            using TagType = decltype(tag);
            return std::make_pair(ConstantCompressor<typename TagType::DataTypeTag::raw_type>::compressed_size(), std::monostate{});
        });
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
        return 9;
    }
    static std::optional<std::pair<size_t, EncoderData>> deterministic_size(FieldStatsImpl field_stats, DataType data_type, size_t row_count) {
        return make_scalar_type(data_type).visit_tag([field_stats, row_count] (auto tag) {
            using TagType = decltype(tag);
            using RawType = TagType::DataTypeTag::raw_type;
            auto max = get_max<typename TagType::DataTypeTag>(field_stats);
            auto bits_needed =  std::bit_width(max);
            auto data = BitPackData{.bits_needed_ = bits_needed};
            auto size = BitPackCompressor<RawType>::compressed_size(data, row_count);
            return std::make_pair(size, data);
        });
    }

    static EncodingScanResult max_compressed_size(FieldStatsImpl, DataType data_type, size_t num_rows, ColumnData, EncoderData& data) {
        return make_scalar_type(data_type).visit_tag([&data, num_rows, data_type] (auto tag) {
            using TagType = decltype(tag);
            using RawType = TagType::DataTypeTag::raw_type;
            auto bitpack_data = std::get<BitPackData>(data);
            return create_scan_result(
                EncodingType::BITPACK,
                BitPackCompressor<RawType>::compressed_size(bitpack_data, num_rows),
                speed_factor(),
                num_rows * get_type_size(data_type),
                true,
                std::move(data));
        });
    }

    static EncodingScanResult estimated_size(
        FieldStatsImpl,
        DataType,
        ColumnData,
        size_t,
        size_t) {
        util::raise_rte("Unexpected estimating request in bitpack encoding");
    }
};

static_assert(Encoder<Constant>);

struct Alp {
    static bool is_viable(FieldStatsImpl, DataType data_type, size_t) {
        return data_type == DataType::FLOAT64 || data_type == DataType::FLOAT32;
    }

    static size_t speed_factor() {
        return 20;
    }

    static EncodingScanResult estimated_size(
        FieldStatsImpl field_stats,
        DataType data_type,
        ColumnData data,
        size_t row_count,
        size_t original_size) {
        return TypeDescriptor{data_type, Dimension::Dim0}.visit_tag([field_stats, data, row_count, original_size] (auto tag) -> EncodingScanResult {
            using TagType = typename decltype(tag)::DataTypeTag;
            if constexpr (is_floating_point_type(TagType::data_type)) {
                using RawType = typename TagType::raw_type;

                alp::state<RawType> state;
                std::array<RawType, alp::config::VECTOR_SIZE> sample_buf;
                auto sample_values = 0UL;
                if(data.num_blocks() == 1) {
                    auto first_block = data.buffer().blocks()[0];
                    sample_values = first_block->bytes() / sizeof(RawType);
                    alp::encoder<RawType>::init(
                        reinterpret_cast<RawType*>(first_block->data()),
                        0,
                        sample_values,
                        sample_buf.data(),
                        state);

                    if(state.scheme == alp::Scheme::ALP_RD)
                        alp::rd_encoder<RawType>::init(
                            reinterpret_cast<RawType *>(first_block->data()),
                            0,
                            sample_values,
                            sample_buf.data(),
                            state);
                } else {
                    ContiguousRangeForwardAdaptor<RawType, alp::config::VECTOR_SIZE> adaptor{data};
                    const auto vectors_in_first_block = (adaptor.block().bytes() / sizeof(RawType)) / alp::config::VECTOR_SIZE;
                    sample_values = std::max(vectors_in_first_block * alp::config::VECTOR_SIZE, alp::config::VECTOR_SIZE);
                    alp::encoder<RawType>::init(
                        adaptor.current(),
                        0,
                        sample_values,
                        sample_buf.data(),
                        state);

                    if(state.scheme == alp::Scheme::ALP_RD)
                        alp::rd_encoder<RawType>::init(
                            adaptor.current(),
                            0,
                            sample_values,
                            sample_buf.data(),
                            state);
                }

                ALPEstimator<RawType> estimator{state};

                auto estimate = estimate_compression<RawType>(
                    field_stats,
                    data,
                    row_count,
                    std::move(estimator),
                    NUM_SAMPLES);

                auto size = estimator.overhead() + estimate.estimated_bytes_;
                auto alp_data = ALPCompressData<RawType>{
                    .state_=state,
                    .max_bit_width_ = estimate.max_bit_width_,
                    .max_exceptions_ = estimate.max_exceptions_};

                return create_scan_result(EncodingType::ALP, size, speed_factor(), original_size, false, std::move(alp_data));
            } else {
                util::raise_rte("Unsupported type for ALP encoding");
            }
        });
    }


    static EncodingScanResult max_compressed_size(FieldStatsImpl, DataType data_type, size_t num_rows, ColumnData, EncoderData& data) {
        // We return the original size here as it is hard to determine what the actual maximum size
        // for an ALP encoded column will be, but if it gets larger than the original size the compressor
        // will fall back to a plain encoding
        return make_scalar_type(data_type).visit_tag([&data, num_rows, data_type] (auto tag) {
            using TagType = decltype(tag);
            using RawType = TagType::DataTypeTag::raw_type;
            return create_scan_result(
                EncodingType::ALP,
                num_rows * sizeof(RawType),
                speed_factor(),
                num_rows * get_type_size(data_type),
                true,
                std::move(data));
        });
    }


    static std::optional<std::pair<size_t, EncoderData>> deterministic_size(FieldStatsImpl, DataType, size_t) {
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

inline std::optional<std::pair<size_t, EncoderData>> deterministic_size(
        EncodingType encoding_type,
        FieldStatsImpl field_stats,
        DataType data_type,
        size_t row_count) {
    return dispatch_encoding(encoding_type, [&field_stats, data_type, row_count](auto &&encoder) {
        return encoder.deterministic_size(field_stats, data_type, row_count);
    });
}

inline EncodingScanResult estimated_size(
        EncodingType encoding_type,
        FieldStatsImpl field_stats,
        DataType data_type,
        ColumnData data,
        size_t row_count,
        size_t original_size) {
    return dispatch_encoding(encoding_type, [&field_stats, data_type, data, row_count, original_size](auto &&encoder) {
        return encoder.estimated_size(field_stats, data_type, data, row_count, original_size);
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
    ColumnData column_data,
    EncoderData& encoder_data) {
    return dispatch_encoding(encoding_type, [&field_stats, data_type, row_count, column_data, &encoder_data](auto &&encoder) {
        return encoder.max_compressed_size(field_stats, data_type, row_count, column_data, encoder_data);
    });
}

} // namespace arcticdb
