#pragma once

#include <cstdint>
#include <cstddef>
#include <limits>

#include <arcticdb/codec/compression/fastlanes_common.hpp>
#include <arcticdb/log/log.hpp>
#include <arcticdb/codec/compression/bitpack_fused.hpp>
#include <arcticdb/util/preprocess.hpp>
#include <arcticdb/util/magic_num.hpp>
#include <arcticdb/codec/compression/contiguous_range_adaptor.hpp>
#include <arcticdb/column_store/column_data.hpp>
#include <arcticdb/codec/compression/encoder_data.hpp>
#include <arcticdb/codec/compression/compressor.hpp>
#include <arcticdb/codec/compression/compression_utils.hpp>

namespace arcticdb {

template<typename T>
class FForCompressKernel {
    const T reference_;

public:
    explicit FForCompressKernel(T reference) : reference_(reference) {}

    MakeUnsignedType<T> operator()(const T* __restrict ptr, size_t offset, size_t /*lane*/) const {
        return ptr[offset] - reference_;
    }
};

template<typename T>
class FForUncompressKernel {
    const T reference_;

public:
    explicit FForUncompressKernel(T reference) : reference_(reference) {}

    void operator()(T* __restrict ptr, size_t offset, T value, size_t /*lane*/) const {
        ptr[offset] = value + reference_;
    }
};

template<typename T>
struct FForHeader {
    T reference;
    uint32_t bits_needed;
    uint32_t num_rows;
};

struct FForRemainderMetadata {
    uint32_t size;
    uint32_t bit_width;
    util::SmallMagicNum<'F','r'> magic_;
};

template<typename T>
static size_t calculate_remainder_bytes(size_t count, size_t bit_width) {
    if (count == 0)
        return 0;

    constexpr auto header_bytes = header_size_in_t<FForRemainderMetadata, T>() * sizeof(T);
    constexpr size_t reference_bytes = sizeof(T);
    const auto compressed_bytes = round_up_bits(count * bit_width);
    return round_up_bytes_in_t<T>(header_bytes + reference_bytes + compressed_bytes);
}

template<typename T>
size_t compress_ffor_remainder(const T* input, size_t count, T* output, size_t bit_width, T reference) {
    auto* metadata [[maybe_unused]] = new (output) FForRemainderMetadata{
        static_cast<uint32_t>(count),
        static_cast<uint32_t>(bit_width),
        {}
    };

    ARCTICDB_DEBUG(log::codec(), "FFOR compressing {} values of remainder", count);
    auto data_start = output + header_size_in_t<FForRemainderMetadata, T>();
    *data_start = reference;
    auto data_out = reinterpret_cast<MakeUnsignedType<T>*>(data_start);
    ++data_out;

    size_t bit_pos = 0;
    MakeUnsignedType<T> current_word = 0;

    for (size_t i = 0; i < count; ++i) {
        MakeUnsignedType<T> delta = input[i] - reference;
        scalar_pack(delta, bit_width, bit_pos, current_word, data_out);
    }

    //TODO is this needed?
    if (bit_pos > 0) {
        *data_out++ = current_word;
    }

    return calculate_remainder_bytes<T>(count, bit_width) / sizeof(T);
}

template<typename T>
size_t decompress_ffor_remainder(const T* input, T* output) {
    const auto* metadata = reinterpret_cast<const FForRemainderMetadata*>(input);
    metadata->magic_.check();
    const uint32_t count = metadata->size;
    const uint32_t bit_width = metadata->bit_width;

    const T* data_start = input + header_size_in_t<FForRemainderMetadata, T>();
    const T reference = *data_start;
    auto data_in = reinterpret_cast<const MakeUnsignedType<T>*>(data_start);
    ++data_in;

    MakeUnsignedType<T> current_word = *data_in;
    size_t bit_pos = 0;

    for (size_t i = 0; i < count; ++i) {
        MakeUnsignedType<T> delta = scalar_unpack(bit_width, bit_pos, current_word, data_in);
        output[i] = delta + reference;
    }

    return calculate_remainder_bytes<T>(count, bit_width) / sizeof(T);
}

template <typename T>
struct FForCompressor : public FFORCompressData {
    explicit FForCompressor(FFORCompressData data) :
        FFORCompressData(data) {
    }

    static FFORCompressData data_fom_stats(const FieldStatsImpl& stats) {
        auto min = stats.get_min<T>();
        auto max = stats.get_max<T>();
        auto max_delta = max - min;
        auto bits_needed = std::bit_width(static_cast<MakeUnsignedType<T>>(max_delta));
        return {min, static_cast<size_t>(bits_needed)};
    }

    static FFORCompressData reference_and_bitwidth(ColumnData data) {
        if(data.has_field_stats()) {
            return data_fom_stats(data.field_stats());
        }

        return data.type().visit_tag([&data] (auto tdt) -> FFORCompressData {
            using TDT = decltype(tdt);
            using RawType = TDT::DataTypeTag::raw_type;

            auto i = data.cbegin<TDT, IteratorType::REGULAR>();
            T reference = *i;
            for(; i != data.cend<TDT, IteratorType::REGULAR>(); ++i) {
                reference = std::min<T>(reference, *i);
            }
            data.reset();

            T max_delta = 0;
            auto j = data.cbegin<TDT, IteratorType::REGULAR>();
            for (; j != data.cend<TDT, IteratorType::REGULAR>(); ++j) {
                max_delta = std::max<RawType>(max_delta, *j - reference);
            }

            size_t bits_needed = std::bit_width(static_cast<MakeUnsignedType<T>>(max_delta));
            return {reference, bits_needed};
        });
    }

    static constexpr size_t get_num_full_blocks(size_t count) {
        return count / BLOCK_SIZE;
    }

    static constexpr size_t remainder_count(size_t count) {
        return count - (get_num_full_blocks(count) * BLOCK_SIZE);
    }

    static size_t compressed_size(const FFORCompressData& data, size_t num_rows) {
        constexpr auto header_size = header_size_in_t<FForHeader<T>, T>() * sizeof(T);
        const auto simd_bytes =   round_up_bits(data.bits_needed_ * get_num_full_blocks(num_rows) * BLOCK_SIZE);
        const auto remainder_bytes = calculate_remainder_bytes<T>(remainder_count(num_rows), data.bits_needed_);
        ARCTICDB_DEBUG(log::codec(), "FFor calculated size: {} + {} + {} = {}", header_size, simd_bytes, remainder_bytes, header_size + simd_bytes + remainder_bytes);
        return header_size + simd_bytes + remainder_bytes;
    }

    size_t compress(ColumnData data, T *__restrict out, size_t estimated_size) {
        auto count = data.row_count();
        if (count == 0)
            return 0;

        ARCTICDB_DEBUG(log::codec(), "FFOR bitpacking requires {} bits", bits_needed_);

        auto *header [[maybe_unused]] = new(out) FForHeader<T>{
            get_reference<T>(),
            static_cast<uint32_t>(bits_needed_),
            static_cast<uint32_t>(count)
        };

        const auto num_full_blocks = get_num_full_blocks(count);
        size_t output_pos = header_size_in_t<FForHeader<T>, T>();

        FForCompressKernel<T> kernel(get_reference<T>());
        if (data.num_blocks() == 1) {
            auto in = data.buffer().ptr_cast<T>(0, num_full_blocks * sizeof(T));
            for (size_t block = 0; block < num_full_blocks; ++block) {
                output_pos += dispatch_bitwidth_fused<T, BitPackFused>(
                    in + block * BLOCK_SIZE,
                    out + output_pos,
                    bits_needed_,
                    kernel
                );
            }
        } else {
            ContiguousRangeForwardAdaptor<T, BLOCK_SIZE> adaptor{data};
            for (size_t block = 0; block < num_full_blocks; ++block) {
                auto in = adaptor.next();
                output_pos += dispatch_bitwidth_fused<T, BitPackFused>(
                    in,
                    out + output_pos,
                    bits_needed_,
                    kernel
                );
            }
        }
        ARCTICDB_DEBUG(log::codec(), "Encoded {} full blocks to {} bytes", num_full_blocks, output_pos * sizeof(T));

        size_t remaining = count % BLOCK_SIZE;
        ARCTICDB_DEBUG(log::codec(), "FFOR compressing {} remainder values", remaining);
        if (remaining > 0) {
            DynamicRangeRandomAccessAdaptor<T> adaptor{data};
            auto remaining_in = adaptor.at(num_full_blocks * BLOCK_SIZE, remaining);
            T *remaining_out = out + output_pos;

            output_pos += compress_ffor_remainder(
                remaining_in,
                remaining,
                remaining_out,
                bits_needed_,
                get_reference<T>()
            );
        }
        ARCTICDB_DEBUG(log::codec(), "FFOR Compressed size including remainder: {} from {} bytes", output_pos * sizeof(T), count * sizeof(T));
        const auto compressed_bytes = output_pos * sizeof(T);
        util::check(compressed_bytes == estimated_size, "Size mismatch in expected exact ffor encoding: {} != {}", estimated_size, compressed_bytes);
        return compressed_bytes;
    }

    size_t compress_shapes(const T *data, size_t count, T *__restrict out) {
        if (count == 0)
            return 0;

        ARCTICDB_DEBUG(log::codec(), "FFOR bitpacking (contiguous) requires {} bits", bits_needed_);

        auto *header [[maybe_unused]] = new(out) FForHeader<T>{
            get_reference<T>(),
            static_cast<uint32_t>(bits_needed_),
            static_cast<uint32_t>(count)
        };

        T *out_ptr = out + header_size_in_t<FForHeader<T>, T>();
        const size_t num_full_blocks = count / BLOCK_SIZE;
        size_t output_pos = header_size_in_t<FForHeader<T>, T>();

        FForCompressKernel<T> kernel(get_reference<T>());
        for (size_t block = 0; block < num_full_blocks; ++block) {
            output_pos += dispatch_bitwidth_fused<T, BitPackFused>(
                data + block * BLOCK_SIZE,
                out_ptr + (output_pos - header_size_in_t<FForHeader<T>, T>()),
                bits_needed_,
                kernel
            );
        }

        size_t remaining = count % BLOCK_SIZE;
        if (remaining > 0) {
            const T *remaining_in = data + num_full_blocks * BLOCK_SIZE;
            T *remaining_out = out_ptr + (output_pos - header_size_in_t<FForHeader<T>, T>());
            output_pos += compress_ffor_remainder(
                remaining_in,
                remaining,
                remaining_out,
                bits_needed_,
                get_reference<T>()
            );
        }

        ARCTICDB_DEBUG(log::codec(), "Compressed contiguous data: {} from {} bytes", output_pos * sizeof(T), count * sizeof(T));
        return output_pos;
    }
};

template <typename T>
struct FForDecompressor {
    static DecompressResult decompress(const T *__restrict in, T *__restrict out) {
        static constexpr size_t BLOCK_SIZE = 1024;
        const auto *header = reinterpret_cast<const FForHeader<T> *>(in);
        const size_t count = header->num_rows;
        if (count == 0)
            return {.compressed_ = 0, .uncompressed_ = 0};

        const T reference = header->reference;
        const size_t bits_needed = header->bits_needed;

        ARCTICDB_DEBUG(log::codec(), "Decompressing FFOR data packed to {} bits with reference {}", bits_needed, reference);

        const T *in_ptr = in + header_size_in_t<FForHeader<T>, T>();
        size_t input_offset = 0;

        const size_t num_full_blocks = count / BLOCK_SIZE;
        FForUncompressKernel<T> kernel(reference);
        size_t remaining = count % BLOCK_SIZE;
        if (bits_needed == 0) {
            std::fill(out, out + num_full_blocks * BLOCK_SIZE + remaining, reference);
            const size_t compressed_size = FForCompressor<T>::compressed_size(
                FFORCompressData{T(0), bits_needed}, count
            );
            return {.compressed_ = compressed_size, .uncompressed_ = count * sizeof(T)};
        } else {
            for (size_t block = 0; block < num_full_blocks; ++block) {
                if (block + 1 < num_full_blocks) {
                    size_t next_block_start_bits = (block + 1) * BLOCK_SIZE * bits_needed;
                    size_t next_block_start_bytes = round_up_bits(next_block_start_bits);
                    size_t cache_lines_to_prefetch = (round_up_bits(BLOCK_SIZE * bits_needed) + 63) / 64;
                    for (size_t i = 0; i < cache_lines_to_prefetch; ++i) {
                        ARCTICDB_PREFETCH(in_ptr + next_block_start_bytes + i * 64);
                    }
                }

                input_offset += dispatch_bitwidth_fused<T, BitUnpackFused>(
                    in_ptr + input_offset,
                    out + block * BLOCK_SIZE,
                    bits_needed,
                    kernel
                );
            }
        }

        ARCTICDB_DEBUG(log::codec(), "Decompressed {} full blocks to offset {}", num_full_blocks, input_offset);
        if (remaining > 0) {
            input_offset += decompress_ffor_remainder(
                in_ptr + input_offset,
                out + num_full_blocks * BLOCK_SIZE
            );
        }
        ARCTICDB_DEBUG(log::codec(), "Decompressed position: {} ({}) + header {} ({})", input_offset, round_up_bytes_in_t<T>(input_offset), header_size_in_t<FForHeader<T>, T>(), header_size_in_t<FForHeader<T>, T>() * sizeof(T));
        const auto decompressed_bytes = (input_offset + header_size_in_t<FForHeader<T>, T>()) * sizeof(T);
        ARCTICDB_DEBUG(log::codec(), "Decompressed {} remaining values to offset {}", remaining, decompressed_bytes);
        return {.compressed_ = decompressed_bytes, .uncompressed_ = count * sizeof(T)};
    }
};

} // namespace arcticdb