#pragma once

#include <arcticdb/column_store/statistics.hpp>
#include <arcticdb/storage/memory_layout.hpp>
#include <arcticdb/codec/compression/estimators.hpp>
#include <arcticdb/codec/compression/delta.hpp>
#include <arcticdb/codec/compression/bitpack.hpp>
#include <arcticdb/codec/compression/constant.hpp>
#include <arcticdb/codec/compression/plain.hpp>
#include <arcticdb/codec/compression/alp.hpp>
#include <arcticdb/codec/compression/encoding_scan_result.hpp>

namespace arcticdb {

constexpr size_t NUM_SAMPLES = 10;

inline size_t plain_result_bytes(size_t bytes) {
    return bytes + sizeof(PlainCompressData);
}

inline size_t plain_result_size(size_t num_rows, DataType data_type) {
    return plain_result_bytes(num_rows * get_type_size(data_type));
}

template <typename T>
size_t plain_result_size(size_t num_rows) {
    return plain_result_bytes(num_rows * sizeof(T));
}


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
  return create_scan_result(EncodingType::PLAIN, plain_result_size<T>(num_rows), 1, num_rows * sizeof(T), true, std::monostate{});
}

inline EncodingScanResult create_plain_result(ColumnData data) {
    return data.type().visit_tag([&data] (auto tag) {
        using TypeTag = decltype(tag);
        return create_plain_result<typename TypeTag::DataTypeTag::raw_type>(data.row_count());
    });
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
        return 10;
    }

    static std::optional<std::pair<size_t, EncoderData>> deterministic_size(FieldStatsImpl field_stats, DataType data_type, size_t num_rows) {
        return make_scalar_type(data_type).visit_tag([field_stats, num_rows] (auto tag) -> std::optional<std::pair<size_t, EncoderData>> {
            using TagType = decltype(tag);
            constexpr auto data_type = TagType::DataTypeTag::data_type;
            if constexpr(is_integer_type(data_type) || is_sequence_type(data_type) || is_time_type(data_type)) {
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
            const auto size = FForCompressor<RawType>::compressed_size(ffor_data, num_rows);
            ARCTICDB_DEBUG(log::codec(), "FFor compressor max compressed size: {}", size);
            return create_scan_result(
                EncodingType::FFOR,
                size,
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
        util::raise_rte("Did not expect sampling in deterministic compression");
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
                ARCTICDB_DEBUG(log::codec(), "Delta compressor max_compressed_size: {}", bytes);
                return create_scan_result(EncodingType::DELTA, bytes, speed_factor(), original_size, false, std::move(delta_data));
            } else {
                util::raise_rte("Request to scan non-integral type in delta encoder");
            }
        });
    }

    static size_t speed_factor() {
        return 16;
    }

    static EncodingScanResult estimated_size(
        FieldStatsImpl field_stats,
        DataType data_type,
        ColumnData data,
        size_t row_count,
        size_t original_size) {
        return TypeDescriptor{data_type, Dimension::Dim0}.visit_tag([data, row_count, field_stats, original_size] (auto tag) -> EncodingScanResult {
            using TagType = typename decltype(tag)::DataTypeTag;
            if constexpr (is_integer_type(TagType::data_type) || is_time_type(TagType::data_type)) {
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
    static bool is_viable(FieldStatsImpl field_stats, DataType data_type, size_t row_count) {
        if (!is_integer_type(data_type) && !is_sequence_type(data_type) && !is_floating_point_type(data_type))
            return false;

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

            ARCTICDB_DEBUG(log::codec(), "Frequency encoder max_compressed_size: {}", *size);
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
            constexpr auto data_type = TagType::data_type;
            if constexpr (is_integer_type(data_type) || is_sequence_type(data_type) || is_floating_point_type(data_type)) {
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

struct Plain {
    static bool is_viable(FieldStatsImpl, DataType, size_t) {
        return true;
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
        util::raise_rte("Unexpected estimating request in plain encoding");
    }

    static EncodingScanResult max_compressed_size(
            FieldStatsImpl field_stats,
            DataType data_type,
            size_t num_rows,
            ColumnData,
            EncoderData&) {
        auto size_calc = deterministic_size(field_stats, data_type, num_rows);
        ARCTICDB_DEBUG(log::codec(), "Plain compressor max_compressed_size: {}", size_calc->first);
        return create_scan_result(
            EncodingType::PLAIN,
            size_calc->first,
            speed_factor(),
            num_rows * get_type_size(data_type),
            true,
            std::move(size_calc->second)
        );
    }

    static std::optional<std::pair<size_t, EncoderData>> deterministic_size(
            FieldStatsImpl,
            DataType data_type,
            size_t num_rows) {
        return std::make_pair(plain_result_size(num_rows, data_type), std::monostate{});
    }
};

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
        ARCTICDB_DEBUG(log::codec(), "Constant encoding max_compressed_size: {}", size_calculation->first);
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

    template <typename DataTypeTag>
    static int64_t get_min(FieldStatsImpl field_stats) {
        using RawType = DataTypeTag::raw_type;
        return field_stats.get_min<RawType>();
    }

    static bool is_viable(FieldStatsImpl field_stats, DataType data_type, size_t) {
        return TypeDescriptor{data_type, Dimension::Dim0}.visit_tag([field_stats] (auto tag) -> bool {
            using TagType = decltype(tag);
            using RawType = TagType::DataTypeTag::raw_type;
            if constexpr (std::is_integral_v<RawType>) {
                if constexpr (std::is_signed_v<RawType>) {
                    const auto min = get_min<typename TagType::DataTypeTag>(field_stats);
                    if (min < 0)
                        return false;
                }
                auto max = get_max<typename TagType::DataTypeTag>(field_stats);
                return std::bit_width(max) < std::numeric_limits<typename TagType::DataTypeTag::raw_type>::digits;
            } else {
                util::raise_rte("Unexpected type in bitpack deterministic size");
            }
        });
    }

    static size_t speed_factor() {
        return 9;
    }

    static std::optional<std::pair<size_t, EncoderData>> deterministic_size(FieldStatsImpl field_stats, DataType data_type, size_t row_count) {
        return make_scalar_type(data_type).visit_tag([field_stats, row_count] (auto tag) -> std::optional<std::pair<size_t, EncoderData>> {
            using TagType = decltype(tag);
            using RawType = TagType::DataTypeTag::raw_type;
            if constexpr (std::is_integral_v<RawType>) {
                const auto max = get_max<typename TagType::DataTypeTag>(field_stats);
                const auto bits_needed = std::bit_width(max);
                const auto data = BitPackData{.bits_needed_ = static_cast<size_t>(bits_needed)};
                const auto size = BitPackCompressor<RawType>::compressed_size(data, row_count);
                return std::make_pair(size, data);
            } else {
                util::raise_rte("Unexpected type in bitpack deterministic size");
            }
        });
    }

    static EncodingScanResult max_compressed_size(FieldStatsImpl, DataType data_type, size_t num_rows, ColumnData, EncoderData& data) {
        return make_scalar_type(data_type).visit_tag([&data, num_rows, data_type] (auto tag) {
            using TagType = decltype(tag);
            using RawType = TagType::DataTypeTag::raw_type;
            auto bitpack_data = std::get<BitPackData>(data);
            const auto bytes = BitPackCompressor<RawType>::compressed_size(bitpack_data, num_rows);
            ARCTICDB_DEBUG(log::codec(), "Bitpack encoding max_compressed_size: {}", bytes);
            return create_scan_result(
                EncodingType::BITPACK,
                bytes,
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
                    util::check(adaptor.valid(), "Unexpected invalid adaptor");
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

                const auto overhead = estimator.overhead();
                auto estimate = estimate_compression<RawType>(
                    field_stats,
                    data,
                    row_count,
                    std::move(estimator),
                    NUM_SAMPLES);

                auto size = overhead + estimate.estimated_bytes_;
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

    template<typename T>
    static size_t max_compressed_block_size(const ALPCompressData<T>& alp_data) {
        if (alp_data.state_.scheme == alp::Scheme::ALP_RD)
            return worst_case_required_rd_size<T>();
        else
            return worst_case_required_alp_size<T>();
    }

    static EncodingScanResult max_compressed_size(FieldStatsImpl, DataType data_type, size_t num_rows, ColumnData,  EncoderData& encoder_data) {
        return make_scalar_type(data_type).visit_tag([&encoder_data, num_rows, data_type](auto tag) -> EncodingScanResult {
            using TagType = decltype(tag);
            using RawType = typename TagType::DataTypeTag::raw_type;
            if constexpr(std::is_floating_point_v<RawType>) {
                const ALPCompressData<RawType> &alp_data = std::get<ALPCompressData<RawType>>(encoder_data);

                const double max_comp_ratio = ConfigsMap::instance()->get_double("Alp.MaxCompressRatio", 0.8);
                const size_t original_size = num_rows * sizeof(RawType);
                const auto compress_limit = static_cast<size_t>(original_size * max_comp_ratio);

                const size_t n_blocks = (num_rows + BLOCK_SIZE - 1) / BLOCK_SIZE;
                const size_t worst_block = max_compressed_block_size<RawType>(alp_data);
                size_t worst_case_total = 0;
                if (n_blocks <= 1) {
                    worst_case_total = sizeof(ALPHeader<RawType>) + worst_block;
                } else {
                    worst_case_total = sizeof(ALPHeader<RawType>) + compress_limit + (worst_block * 2);
                }

                worst_case_total = std::max<size_t>(worst_case_total, Plain::deterministic_size(FieldStatsImpl{}, data_type, num_rows)->first);

                ARCTICDB_DEBUG(log::codec(), "ALP max_compressed_size: {}", worst_case_total);
                return create_scan_result(
                    EncodingType::ALP,
                    worst_case_total,
                    speed_factor(),
                    original_size,
                    true,
                    std::move(encoder_data)
                );
            } else {
                util::raise_rte("Unexpected type in ALP max_compressed_size");
            }
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
    case EncodingType::PLAIN:
        return std::forward<F>(f)(Plain{});
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

namespace fmt {

template<>
struct formatter<arcticdb::EncodingType> {
    constexpr auto parse(format_parse_context& ctx) -> decltype(ctx.begin()) {
        return ctx.begin();
    }

    template<typename FormatContext>
    auto format(const arcticdb::EncodingType& encoding, FormatContext& ctx) -> decltype(ctx.out()) {
        using namespace arcticdb;
        const char* name = nullptr;
        switch (encoding) {
        case EncodingType::PLAIN: name = "PLAIN"; break;
        case EncodingType::FFOR: name = "FFOR"; break;
        case EncodingType::DELTA: name = "DELTA"; break;
        case EncodingType::FREQUENCY: name = "FREQUENCY"; break;
        case EncodingType::CONSTANT: name = "CONSTANT"; break;
        case EncodingType::RLE: name = "RLE"; break;
        case EncodingType::ALP: name = "ALP"; break;
        case EncodingType::BITPACK: name = "BITPACK"; break;
        case EncodingType::COUNT: name = "COUNT"; break;
        default: name = "UNKNOWN"; break;
        }
        return fmt::format_to(ctx.out(), "{}", name);
    }
};

} // namespace fmt