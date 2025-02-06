#pragma once

#include <cstdint>
#include <cstddef>
#include <limits>

#include <arcticdb/codec/compression/fastlanes_common.hpp>
#include <arcticdb/log/log.hpp>
#include <arcticdb/codec/compression/bitpack_fused.hpp>
#include <arcticdb/util/preprocess.hpp>

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


template<typename T>
size_t compress_remainder(const T* input, size_t count, T* output, size_t bit_width, T reference) {
    assert(count < 1024);

    // Store metadata
    output[0] = static_cast<T>((bit_width << 8) | count);

    T* compressed = output + 1;
    T accumulator = 0;
    size_t bits_used = 0;

    // Compress values
    for (size_t i = 0; i < count; i++) {
        T delta = input[i] - reference;
        accumulator |= (delta << bits_used);
        bits_used += bit_width;

        if (bits_used >= sizeof(T) * 8) {
            *compressed++ = accumulator;
            accumulator = delta >> (sizeof(T) * 8 - bits_used);
            bits_used -= sizeof(T) * 8;
        }
    }

    // Write final word if needed
    if (bits_used > 0) {
        *compressed++ = accumulator;
    }

    return compressed - output;
}

// Helper function for remainder decompression
template<typename T>
size_t decompress_remainder(const T* input, T* output, T reference) {
    const T metadata = input[0];
    const size_t bit_width = metadata >> 8;
    const size_t count = metadata & 0xFF;

    const T* compressed = input + 1;
    T current_word = *compressed++;
    size_t bits_available = sizeof(T) * 8;
    const T mask = (T(1) << bit_width) - 1;

    for (size_t i = 0; i < count; i++) {
        T delta;
        if (bits_available >= bit_width) {
            delta = current_word & mask;
            current_word >>= bit_width;
            bits_available -= bit_width;
        } else {
            delta = current_word;
            current_word = *compressed++;
            delta |= (current_word << bits_available) & mask;
            current_word >>= (bit_width - bits_available);
            bits_available = sizeof(T) * 8 - (bit_width - bits_available);
        }
        output[i] = delta + reference;
    }

    return count;
}

template<typename T>
size_t encode_ffor_with_header(const T* in, T* out, size_t count) {
    if (count == 0) return 0;

    // Find reference value
    T reference = in[0];
    for (size_t i = 1; i < count; ++i) {
        reference = std::min(reference, in[i]);
    }

    // Calculate max delta
    T max_delta = 0;
    for (size_t i = 0; i < count; ++i) {
        max_delta = std::max(max_delta, in[i] - reference);
    }

    size_t bits_needed = std::bit_width(static_cast<std::make_unsigned_t<T>>(max_delta));

    // Write header
    auto* header [[maybe_unused]] = new (out) FForHeader<T>{
        reference,
        static_cast<uint32_t>(bits_needed),
        static_cast<uint32_t>(count)
    };

    T* out_ptr = out + sizeof(FForHeader<T>)/sizeof(T);

    // Process full blocks of 1024
    const size_t num_full_blocks = count / 1024;
    size_t compressed_size = sizeof(FForHeader<T>)/sizeof(T);

    FForCompressKernel<T> kernel(reference);

    // Compress full blocks
    for (size_t block = 0; block < num_full_blocks; ++block) {
        compressed_size += dispatch_bitwidth<T, BitPackFused>(
            in + block * 1024,
            out_ptr + compressed_size - sizeof(FForHeader<T>)/sizeof(T),
            bits_needed,
            kernel
        );
    }

    // Handle remaining values
    size_t remaining = count % 1024;
    if (remaining > 0) {
        const T* remaining_in = in + num_full_blocks * 1024;
        T* remaining_out = out_ptr + compressed_size - sizeof(FForHeader<T>)/sizeof(T);

        compressed_size += compress_remainder(
            remaining_in,
            remaining,
            remaining_out,
            bits_needed,
            reference
        );
    }

    return compressed_size;
}

template<typename T>
size_t decode_ffor_with_header(const T* in, T* out) {
    const auto* header = reinterpret_cast<const FForHeader<T>*>(in);
    const size_t count = header->num_rows;
    if (count == 0) return 0;

    const T reference = header->reference;
    const size_t bits_needed = header->bits_needed;

    const T* in_ptr = in + sizeof(FForHeader<T>)/sizeof(T);
    size_t input_offset = 0;

    // Process full blocks
    const size_t num_full_blocks = count / 1024;
    FForUncompressKernel<T> kernel(reference);

    // Decompress full blocks
    for (size_t block = 0; block < num_full_blocks; ++block) {
        input_offset += dispatch_bitwidth<T, BitUnpackFused>(
            in_ptr + input_offset,
            out + block * 1024,
            bits_needed,
            kernel
        );
    }

    // Handle remaining values
    size_t remaining = count % 1024;
    if (remaining > 0) {
        decompress_remainder(
            in_ptr + input_offset,
            out + num_full_blocks * 1024,
            reference
        );
    }

    return count;
}

} // namespace arcticdb