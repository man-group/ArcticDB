#pragma once

#include <cstdint>
#include <cstddef>
#include <limits>

#include <arcticdb/codec/compression/fastlanes_common.hpp>
#include <arcticdb/log/log.hpp>
#include <arcticdb/codec/compression/bitpack_fused.hpp>
#include <arcticdb/util/preprocess.hpp>
#include "util/magic_num.hpp"

namespace arcticdb {

template<typename T>
class FForCompressKernel {
    const T reference_;

public:
    ALWAYS_INLINE
    explicit FForCompressKernel(T reference) : reference_(reference) {}

    HOT_FUNCTION
    ALWAYS_INLINE
    VECTOR_HINT
    T operator()(T value, size_t /*lane*/) const {
        return value - reference_;
    }
};

template<typename T>
class FForUncompressKernel {
    const T reference_;

public:
    ALWAYS_INLINE
    explicit FForUncompressKernel(T reference) : reference_(reference) {}

    HOT_FUNCTION
    ALWAYS_INLINE
    VECTOR_HINT
    T operator()(T value, size_t /*lane*/) const {  // Removed initial_values parameter
        return value + reference_;
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
static size_t calculate_remainder_data_size(size_t count, size_t bit_width) {
    static constexpr size_t t_bit = Helper<T>::num_bits;
    return sizeof(FForRemainderMetadata)/sizeof(T) +
        1 + (count * bit_width + t_bit - 1) / t_bit;
}

template<typename T>
size_t compress_remainder(const T* input, size_t count, T* output, size_t bit_width, T reference) {
    auto* metadata [[maybe_unused]] = new (output) FForRemainderMetadata{
        static_cast<uint32_t>(count),
        static_cast<uint32_t>(bit_width),
        {}
    };

    T* data_out = output + header_size_in_t<FForRemainderMetadata, T>();
    *data_out++ = reference;

    size_t bit_pos = 0;
    T current_word = 0;

    for (size_t i = 0; i < count; ++i) {
        T delta = input[i] - reference;
        scalar_pack(delta, bit_width, bit_pos, current_word, data_out);
    }

    if (bit_pos > 0) {
        *data_out++ = current_word;
    }

    return calculate_remainder_data_size<T>(count, bit_width);
}

template<typename T>
size_t decompress_remainder(const T* input, T* output) {
    const auto* metadata = reinterpret_cast<const FForRemainderMetadata*>(input);
    metadata->magic_.check();
    const uint32_t count = metadata->size;
    const uint32_t bit_width = metadata->bit_width;

    const T* data_in = input + header_size_in_t<FForRemainderMetadata, T>();
    const T reference = *data_in++;

    T current_word = *data_in;
    size_t bit_pos = 0;

    for (size_t i = 0; i < count; ++i) {
        T delta = scalar_unpack(bit_width, bit_pos, current_word, data_in);
        output[i] = delta + reference;
    }

    return calculate_remainder_data_size<T>(count, bit_width);
}

template<typename T>
size_t encode_ffor_with_header(const T* in, T* out, size_t count) {
    if (count == 0) return 0;

    T reference = in[0];
    log::codec().info("Encoding {} rows FFOR with reference {}", count, reference);
    for (size_t i = 1; i < count; ++i) {
        reference = std::min(reference, in[i]);
    }

    T max_delta = 0;
    for (size_t i = 0; i < count; ++i) {
        max_delta = std::max(max_delta, in[i] - reference);
    }
    size_t bits_needed = std::bit_width(static_cast<std::make_unsigned_t<T>>(max_delta));
    log::codec().info("Max delta {} requires {} bits", max_delta, bits_needed);

    auto* header [[maybe_unused]] = new (out) FForHeader<T>{
        reference,
        static_cast<uint32_t>(bits_needed),
        static_cast<uint32_t>(count)
    };

    T* out_ptr = out + header_size_in_t<FForHeader<T>, T>();

    const size_t num_full_blocks = count / 1024;
    size_t compressed_size = sizeof(FForHeader<T>) / sizeof(T);

    FForCompressKernel<T> kernel(reference);

    for (size_t block = 0; block < num_full_blocks; ++block) {
        compressed_size += dispatch_bitwidth<T, BitPackFused>(
            in + block * 1024,
            out_ptr + compressed_size - header_size_in_t<FForHeader<T>, T>(),
            bits_needed,
            kernel
        );
    }
    log::codec().info("Encoded {} full blocks to {} bytes", num_full_blocks, compressed_size * sizeof(T));

    size_t remaining = count % 1024;
    log::codec().info("Compressing {} remainder values", remaining);
    if (remaining > 0) {
        const T* remaining_in = in + num_full_blocks * 1024;
        T* remaining_out = out_ptr + compressed_size - header_size_in_t<FForHeader<T>, T>();

        compressed_size += compress_remainder(
            remaining_in,
            remaining,
            remaining_out,
            bits_needed,
            reference
        );
    }
    log::codec().info("Compressed size including remainder: {} from {} bytes",
      compressed_size * sizeof(T),
      count * sizeof(T));

    return compressed_size;
}

template<typename T>
size_t decode_ffor_with_header(const T* in, T* out) {
    static constexpr size_t BLOCK_SIZE = 1024;
    const auto* header = reinterpret_cast<const FForHeader<T>*>(in);
    const size_t count = header->num_rows;
    if (count == 0)
        return 0;

    const T reference = header->reference;
    const size_t bits_needed = header->bits_needed;

    log::codec().info("Decompressing FFOR data packed to {} bits with reference {}", bits_needed, reference);

    const T* in_ptr = in + header_size_in_t<FForHeader<T>, T>();
    size_t input_offset = 0;

    const size_t num_full_blocks = count / BLOCK_SIZE;
    FForUncompressKernel<T> kernel(reference);

    if(bits_needed == 0) {
        std::fill(out, out + num_full_blocks * BLOCK_SIZE, reference);
        input_offset += num_full_blocks * BLOCK_SIZE ;
    } else {
        for (size_t block = 0; block < num_full_blocks; ++block) {
            input_offset += dispatch_bitwidth<T, BitUnpackFused>(
                in_ptr + input_offset,
                out + block * 1024,
                bits_needed,
                kernel
            );
        }
    }

    log::codec().info("Decompressed {} full blocks to offset {}", num_full_blocks, input_offset);
    size_t remaining = count % 1024;
    if (remaining > 0) {
        input_offset += decompress_remainder(
            in_ptr + input_offset,
            out + num_full_blocks * 1024
        );
    }
    log::codec().info("Decompresed {} remaining values to offset {}", remaining, input_offset * sizeof(T) + sizeof(FForHeader<T>));
    return count;
}

} // namespace arcticdb