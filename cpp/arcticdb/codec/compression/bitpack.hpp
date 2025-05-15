#pragma once

#include <cstdint>
#include <cstddef>

#include <arcticdb/log/log.hpp>
#include <arcticdb/codec/compression/fastlanes_common.hpp>
#include <arcticdb/codec/compression/bitpack_fused.hpp>
#include <arcticdb/codec/compression/compressor.hpp>
#include <arcticdb/util/magic_num.hpp>
#include <arcticdb/column_store/column_data.hpp>
#include <arcticdb/codec/compression/contiguous_range_adaptor.hpp>
#include <arcticdb/codec/compression/encoder_data.hpp>
#include <arcticdb/codec/compression/compression_utils.hpp>

namespace arcticdb {

template<typename T>
class BitPackCompressKernel {
public:
    T operator()(const T *__restrict ptr, size_t offset, size_t /*lane*/) const {
        return ptr[offset];
    }
};

template<typename T>
class BitUnpackCompressKernel {
public:
    void operator()(T *__restrict ptr, size_t offset, T value, size_t /*lane*/) const {
        ptr[offset] = value;
    }
};

template<typename T>
struct BitPackHeader {
    uint32_t bits_needed;
    uint32_t num_rows;
};

struct BitPackRemainderHeader {
    uint32_t size;
    util::SmallMagicNum<'B', 'r'> magic_;
};

template<typename T>
static size_t calculate_remainder_data_size_bitpack(size_t count, size_t bit_width) {
    static constexpr size_t t_bit = Helper<T>::num_bits;
    return sizeof(BitPackRemainderHeader) / sizeof(T) +
        1 + (count * bit_width + t_bit - 1) / t_bit;
}

template<typename T>
size_t compress_bitwidth_remainder(const T *input, size_t count, T *output, size_t bit_width) {
    auto *metadata [[maybe_unused]] = new(output) BitPackRemainderHeader{
        static_cast<uint32_t>(count),
        {}
    };

    T *data_out = output + header_size_in_t<BitPackRemainderHeader, T>();
    size_t bit_pos = 0;
    T current_word = 0;

    for (size_t i = 0; i < count; ++i) {
        T value = input[i];
        scalar_pack(value, bit_width, bit_pos, current_word, data_out);
    }

    if (bit_pos > 0) {
        *data_out++ = current_word;
    }

    return calculate_remainder_data_size_bitpack<T>(count, bit_width);
}

template<typename T>
size_t decompress_bitwidth_remainder(const T *input, T *output, uint32_t bit_width) {
    const auto *metadata = reinterpret_cast<const BitPackRemainderHeader *>(input);

    metadata->magic_.check();
    const uint32_t count = metadata->size;

    const T *data_in = input + header_size_in_t<BitPackRemainderHeader, T>();
    T current_word = *data_in;
    size_t bit_pos = 0;

    for (size_t i = 0; i < count; ++i) {
        T value = scalar_unpack(bit_width, bit_pos, current_word, data_in);
        output[i] = value;
    }

    return calculate_remainder_data_size_bitpack<T>(count, bit_width);
}


template<typename T>
struct BitPackCompressor : public BitPackData {
    explicit BitPackCompressor(BitPackData data) :
        BitPackData(data) {
    }
    
    static BitPackData compute_bitwidth(ColumnData data) {
        if (data.has_field_stats()) {
            auto stats = data.field_stats();
            auto max_val = stats.get_max<T>();
            return BitPackData{static_cast<size_t>(std::bit_width(static_cast<std::make_unsigned_t<T>>(max_val)))};
        }

        T max_val = *data.buffer().ptr_cast<T>(0, sizeof(T));
        for (auto block_num = 0UL; block_num < data.num_blocks(); ++block_num) {
            auto block = data.buffer().blocks()[block_num];
            size_t block_rows = block->bytes() / sizeof(T);
            auto data_in = reinterpret_cast<const T *>(block->data());
            for (size_t k = 0; k < block_rows; ++k) {
                max_val = std::max(max_val, *data_in);
                ++data_in;
            }
        }

        return BitPackData{static_cast<size_t>(std::bit_width(static_cast<std::make_unsigned_t<T>>(max_val)))};
    }

    static size_t compressed_size(BitPackData bitpack_data, size_t count) {
        if (count == 0)
            return 0;

        const auto bit_width = bitpack_data.bits_needed_;
        const size_t header_size = header_size_in_t<BitPackHeader<T>, T>();

        const size_t num_full_blocks = count / BLOCK_SIZE;
        const size_t remainder = count % BLOCK_SIZE;
        const size_t full_block_size_in_t = round_up_bits_in_t<T>(BLOCK_SIZE * bit_width);
        size_t size_in_t = header_size + (num_full_blocks * full_block_size_in_t);

        if (remainder > 0) 
            size_in_t += calculate_remainder_data_size_bitpack<T>(remainder, bit_width);

        return size_in_t * sizeof(T);
    }

    size_t compress(ColumnData data, T *__restrict out, size_t estimated_size) {
        const auto count = data.row_count();
        if (count == 0)
            return 0;

        ARCTICDB_DEBUG(log::codec(), "Bitpacking requires {} bits", bits_needed_);
        util::check(bits_needed_ < sizeof(T) * CHAR_BIT, "Bit width {} >= type bits, no compression possible", bits_needed_, sizeof(T) * CHAR_BIT);
        auto *header [[maybe_unused]] = new(out) BitPackHeader<T>{
            static_cast<uint32_t>(bits_needed_),
            static_cast<uint32_t>(count)
        };

        constexpr auto header_size = header_size_in_t<BitPackHeader<T>, T>();
        T *out_ptr = out + header_size;
        const size_t num_full_blocks = count / BLOCK_SIZE;
        size_t compressed_size = header_size_in_t<BitPackHeader<T>, T>();

        BitPackCompressKernel<T> kernel;
        if (data.num_blocks() == 1) {
            auto in = data.buffer().ptr_cast<T>(0, num_full_blocks * sizeof(T));
            for (size_t block = 0; block < num_full_blocks; ++block) {
                compressed_size += dispatch_bitwidth_fused<T, BitPackFused>(
                    in + block * BLOCK_SIZE,
                    out_ptr + compressed_size - header_size_in_t<BitPackHeader<T>, T>(),
                    bits_needed_,
                    kernel
                );
            }
        } else {
            ContiguousRangeForwardAdaptor<T, BLOCK_SIZE> adaptor{data};
            for (size_t block = 0; block < num_full_blocks; ++block) {
                auto in = adaptor.next();
                compressed_size += dispatch_bitwidth_fused<T, BitPackFused>(
                    in,
                    out_ptr + compressed_size - header_size_in_t<BitPackHeader<T>, T>(),
                    bits_needed_,
                    kernel
                );
            }
        }
        ARCTICDB_DEBUG(log::codec(), "Encoded {} full blocks to {} bytes", num_full_blocks, compressed_size * sizeof(T));

        size_t remaining = count % BLOCK_SIZE;
        ARCTICDB_DEBUG(log::codec(), "Bitpack compressing {} remainder values", remaining);
        if (remaining > 0) {
            DynamicRangeRandomAccessAdaptor<T> adaptor{data};
            auto remaining_in = adaptor.at(num_full_blocks * BLOCK_SIZE, remaining);
            T *remaining_out = out_ptr + compressed_size - header_size_in_t<BitPackHeader<T>, T>();

            compressed_size += compress_bitwidth_remainder(
                remaining_in,
                remaining,
                remaining_out,
                bits_needed_
            );
        }

        const auto compressed_bytes = compressed_size * sizeof(T);
        ARCTICDB_DEBUG(log::codec(), "Compressed size including remainder: {} from {} bytes", compressed_bytes, count * sizeof(T));
        util::check(compressed_bytes == estimated_size, "Size mismatch in bitpack: {} != {}", estimated_size, compressed_bytes);
        return compressed_bytes;
    }
};

template<typename T>
struct BitPackDecompressor {
    static DecompressResult decompress(const T *__restrict in, T *__restrict out) {
        const auto *header = reinterpret_cast<const BitPackHeader<T> *>(in);
        const size_t count = header->num_rows;
        if (count == 0)
            return {.compressed_=sizeof(BitPackHeader<T>), .uncompressed_ = 0};

        const size_t bits_needed = header->bits_needed;
        ARCTICDB_DEBUG(log::codec(), "Decompressing Identity data packed to {} bits", bits_needed);

        const T *in_ptr = in + header_size_in_t<BitPackHeader<T>, T>();
        size_t input_offset = 0;

        const size_t num_full_blocks = count / BLOCK_SIZE;
        BitUnpackCompressKernel<T> kernel;
        size_t remaining = count % BLOCK_SIZE;

        if (bits_needed == 0) {
            std::fill(out, out + num_full_blocks * BLOCK_SIZE + remaining, static_cast<T>(0));
            const size_t compressed_size = BitPackCompressor<T>::compressed_size(
                BitPackData{bits_needed}, count
            );
            return {.compressed_ = compressed_size, .uncompressed_ = count * sizeof(T)};
        } else {
            for (size_t block = 0; block < num_full_blocks; ++block) {
                input_offset += dispatch_bitwidth_fused<T, BitUnpackFused>(
                    in_ptr + input_offset,
                    out + block * BLOCK_SIZE,
                    bits_needed,
                    kernel
                );
            }

            ARCTICDB_DEBUG(log::codec(), "Decompressed {} full blocks to offset {}", num_full_blocks, input_offset);
            if (remaining > 0) {
                input_offset += decompress_bitwidth_remainder(
                    in_ptr + input_offset,
                    out + num_full_blocks * BLOCK_SIZE,
                    bits_needed
                );
            }
            ARCTICDB_DEBUG(log::codec(), "Decompressed {} remaining values, total count {}", remaining, count);
        }
        const auto decompressed_bytes = (input_offset + header_size_in_t<BitPackHeader<T>, T>()) * sizeof(T);
        return {.compressed_ = decompressed_bytes, .uncompressed_ = count * sizeof(T)};
    }
};

} // namespace arcticdb