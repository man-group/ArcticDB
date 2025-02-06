/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#include <cstdint>
#include <cstddef>
#include <limits>
#include <algorithm>
#include <bit>

#include <arcticdb/codec/compression/fastlanes_common.hpp>
#include <arcticdb/codec/compression/bitpack_fused.hpp>
#include <arcticdb/util/preprocess.hpp>
#include "util/magic_num.hpp"

namespace arcticdb {

struct DeltaSize {
    uint32_t num_rows_;
};

template<typename T>
struct DeltaHeader : public DeltaSize {
    using h = Helper<T>;

    uint32_t bit_width_;
    std::array<T, h::num_lanes> initial_values_;
};

template <typename T>
struct DeltaScanKernel {
    using h = Helper<T>;
    static constexpr size_t num_lanes = h::num_lanes;
    T prev_[num_lanes];
    T max_delta_[num_lanes];

    explicit DeltaScanKernel(const T* lane_initial_values) {
        std::copy(lane_initial_values, lane_initial_values + num_lanes, prev_);
        std::fill(max_delta_, max_delta_ + num_lanes, 0);
    }

    HOT_FUNCTION
    void operator()(T value, size_t lane) {
        T delta = value - prev_[lane];
        max_delta_[lane] = std::max(max_delta_[lane], delta);
        prev_[lane] = value;
    }

    size_t required_bits() const {
        T overall_max = *std::max_element(max_delta_, max_delta_ + num_lanes);
        return overall_max == 0 ? 1 : std::bit_width(static_cast<T>(overall_max + 1));
    }
};


template<typename T>
struct DeltaCompressKernel {
    using h = Helper<T>;
    T prev_[h::num_lanes];

    explicit DeltaCompressKernel(const T* lane_initial_values) {
        std::copy(lane_initial_values, lane_initial_values + h::num_lanes, prev_);
    }

    HOT_FUNCTION
    ALWAYS_INLINE
    VECTOR_HINT
    T operator()(T value, size_t lane) {
        T result = value - prev_[lane];
        //ARCTICDB_DEBUG(log::codec(), "Encoding value {} ({} - {})", value, prev_[lane], result);
        prev_[lane] = value;
        return result;
    }
};


template<typename T>
struct DeltaUncompressKernel {
    using h = Helper<T>;
    T prev_[h::num_lanes];

    explicit DeltaUncompressKernel(const T* lane_initial_values) {
        std::copy(lane_initial_values, lane_initial_values + h::num_lanes, prev_);
    }
    HOT_FUNCTION
    ALWAYS_INLINE
    VECTOR_HINT
    T operator()(T delta, size_t lane) {
        T result = delta + prev_[lane];
        prev_[lane] = result;
        return result;
    }
};

struct RemainderMetadata {
    uint32_t size;
    uint32_t bit_width;
    util::SmallMagicNum<'R', 'm'> magic_;
};

template<typename T>
size_t calc_remainder_size(size_t count, size_t bit_width) {
    static constexpr size_t t_bit = Helper<T>::num_bits;
    return sizeof(RemainderMetadata) / sizeof(T) + 1 + (count * bit_width + t_bit - 1) / t_bit;
}

template<typename T>
size_t compress_remainder(const T* input, size_t count, T* output, size_t bit_width) {
    auto* metadata [[maybe_unused]] = new (output) RemainderMetadata{
        static_cast<uint32_t>(count),
        static_cast<uint32_t>(bit_width),
        {}
    };
    ARCTICDB_DEBUG(log::codec(), "Compressing {} values of remainder", count);
    T* data_out = output + sizeof(RemainderMetadata)/sizeof(T);

    // Store first value separately
    *data_out++ = input[0];

    T prev = input[0];
    size_t bit_pos = 0;
    T current_word = 0;

    for (size_t i = 1; i < count; ++i) {
        T delta = input[i] - prev;
        ARCTICDB_DEBUG(log::codec(), "Value {}, delta = {}", input[i], delta);
        scalar_pack(delta, bit_width, bit_pos, current_word, data_out);
        prev = input[i];
    }

    // Always write the last word, even if bit_pos is 0
    *data_out = current_word;

    return calc_remainder_size<T>(count, bit_width);
}

template<typename T>
size_t decompress_remainder(const T* input, T* output) {
    const auto* metadata = reinterpret_cast<const RemainderMetadata*>(input);
    const uint32_t count = metadata->size;
    const uint32_t bit_width = metadata->bit_width;

    const T* data_in = input + sizeof(RemainderMetadata)/sizeof(T);

    // Read first value
    output[0] = *data_in++;

    T current_word = *data_in;
    size_t bit_pos = 0;

    // Make sure we read all values including the last one
    for (size_t i = 1; i < count; ++i) {
        T delta = scalar_unpack(bit_width, bit_pos, current_word, data_in);
        output[i] = output[i-1] + delta;
        ARCTICDB_DEBUG(log::codec(), "Unpacked value {}", output[i]);
    }

     return calc_remainder_size<T>(count, bit_width);
}

template<typename T>
class ColumnCompressor {
    using Header = DeltaHeader<T>;
    using h = Helper<T>;
    static constexpr size_t BLOCK_SIZE = 1024;

private:
    uint32_t simd_bit_width_ = 0;
    uint32_t remainder_bit_width_ = 0;
    uint32_t compressed_rows_ = 0;
    size_t full_blocks_ = 0;
    size_t remainder_ = 0;
    std::array<T, h::num_lanes> initial_values_ = {};

    static constexpr size_t t_bits = h::num_bits;
    
    static size_t calculate_block_size(size_t bit_width) {
        const size_t bits_needed = BLOCK_SIZE * bit_width;
        return (bits_needed + t_bits - 1) / (t_bits);
    }

    size_t remainder_offset() const {
        return full_blocks_ * BLOCK_SIZE;
    }
    
    void advance_input_past_initials(const T*& input) {
        input += h::num_lanes;
    }
    
    void copy_input_to_initial_values(const T*& input) {
        for (size_t lane = 0; lane < h::num_lanes; ++lane) {
            initial_values_[lane] = input[lane];
        }
        advance_input_past_initials(input);
    }

    size_t create_full_header(T* output) {
        auto* header [[maybe_unused]] = new (output) Header{
            {compressed_rows_},
            simd_bit_width_,
            {initial_values_}
        };

        return sizeof(Header) / sizeof(T);
    }

    size_t create_size_only_header(T* output) {
        auto* size_header [[maybe_unused]] = new (output) DeltaSize {
            compressed_rows_
        };
        return sizeof(DeltaSize) / sizeof(T);
    }

    size_t calculate_full_blocks(size_t rows) {
        if(rows < h::num_lanes)
            return 0;

        return (rows - h::num_lanes) / BLOCK_SIZE;
    }

public:
    size_t scan(const T* input, size_t rows) {
        full_blocks_ = calculate_full_blocks(rows);
        const auto initials_size = full_blocks_ > 0 ? h::num_lanes : 0;
        compressed_rows_ = rows - initials_size;
        remainder_ = compressed_rows_ % BLOCK_SIZE;
        ARCTICDB_INFO(log::codec(), "Compressing {} rows, {} total blocks with {} initial value and remainder of {}",
                          rows, full_blocks_, initials_size, remainder_);

        size_t total_size = 0UL;
        log::codec().info("Total size including header: {}", total_size);
        if (full_blocks_ > 0) {
            total_size += sizeof(Header) / sizeof(T);
            copy_input_to_initial_values(input);

            DeltaScanKernel<T> scan_kernel(initial_values_.data());
            for (size_t block = 0; block < full_blocks_; block++) {
                FastLanesScan<T>::go(
                    input + block * BLOCK_SIZE,
                    scan_kernel
                );
            }
            simd_bit_width_ = scan_kernel.required_bits();
            total_size += full_blocks_ * calculate_block_size(simd_bit_width_);
        } else {
            total_size += sizeof(DeltaSize) / sizeof(T);
        }

        log::codec().info("Total size including full blocks: {}", total_size);
        if (remainder_ > 0) {
            const T* remainder_ptr = input + remainder_offset();
            T prev = remainder_ptr[0];
            T max_delta = 0;

            for (size_t i = 1; i < remainder_; i++) {
                T delta = remainder_ptr[i] - prev;
                max_delta = std::max(max_delta, delta);
                prev = remainder_ptr[i];
            }
            remainder_bit_width_ = max_delta == 0 ? 1 : std::bit_width(max_delta);
            total_size += calc_remainder_size<T>(remainder_, remainder_bit_width_);
        }
        log::codec().info("Total size including remainder: {}", total_size);
        return total_size;
    }

    size_t compress(const T* input, T* output) {
        size_t output_offset = 0;

        if (full_blocks_ > 0) {
            output_offset += create_full_header(output);
            log::codec().info("Compressed to offset {}", output_offset);
            advance_input_past_initials(input);
            DeltaCompressKernel<T> compress_kernel(initial_values_.data());
            log::codec().info("Writing full blocks at offset {}", output_offset);
            for (size_t block = 0; block < full_blocks_; block++) {
                output_offset += dispatch_bitwidth<T, BitPackFused>(
                    input + block * BLOCK_SIZE,
                    output + output_offset,
                    simd_bit_width_,
                    compress_kernel
                );
            }
        } else {
            output_offset += create_size_only_header(output);
        }
        log::codec().info("Writing remainder at {}", output_offset);
        if (remainder_ > 0) {
            const auto offset = remainder_offset();
            ARCTICDB_DEBUG(log::codec(), "Remainder offset: {}, first value {}", offset, input[offset]);
            output_offset += compress_remainder(
                input + offset,
                remainder_,
                output + output_offset,
                remainder_bit_width_
            );
        }
        log::codec().info("Compressed to {} bytes", output_offset);
        return output_offset;
    }
};

template<typename T>
class ColumnDecompressor {
    using Header = DeltaSize;
    using h = Helper<T>;
    static constexpr size_t BLOCK_SIZE = 1024;
    static constexpr size_t t_bits = h::num_bits;

private:
    const Header *header_;
    size_t full_blocks_;
    size_t remainder_;

    static size_t calculate_block_size(size_t bit_width) {
        const size_t bits_needed = BLOCK_SIZE * bit_width;
        return (bits_needed + t_bits - 1) / t_bits;
    }

    size_t calculate_input_offset() const {
        return full_blocks_ > 0 ? sizeof(DeltaHeader<T>) / sizeof(T) : sizeof(Header) / sizeof(T);
    }

    size_t initial_values_size() const {
        return full_blocks_ > 0 ? h::num_lanes : 0;
    }

    size_t remainder_offset() const {
        return initial_values_size() + (full_blocks_ * BLOCK_SIZE);
    }

public:
    void init(const T *input) {
        header_ = reinterpret_cast<const Header *>(input);
        full_blocks_ = header_->num_rows_ / BLOCK_SIZE;
        remainder_ = header_->num_rows_ % BLOCK_SIZE;
    }

    // Get total number of rows
    size_t num_rows() const { return header_->num_rows_ + initial_values_size(); }

    size_t decompress(const T *input, T *output) {
        size_t input_offset = calculate_input_offset();
        if (full_blocks_ > 0) {
            auto full_header = reinterpret_cast<const DeltaHeader<T> *>(header_);
            std::copy(full_header->initial_values_.begin(),
                      full_header->initial_values_.end(),
                      output);

            DeltaUncompressKernel<T> decompress_kernel(full_header->initial_values_.data());

            for (size_t block = 0; block < full_blocks_; block++) {
                input_offset += dispatch_bitwidth<T, BitUnpackFused>(
                    input + input_offset,
                    output + h::num_lanes + block * BLOCK_SIZE,
                    full_header->bit_width_,
                    decompress_kernel
                );
            }
        }

        if (remainder_ > 0) {
            input_offset += decompress_remainder(
                input + input_offset,
                output + remainder_offset()
            );
        }
        return input_offset;
    }

    static size_t compressed_size(const T* input) {
        const auto* size_header = reinterpret_cast<const DeltaSize*>(input);
        const size_t full_blocks = size_header->num_rows_ / BLOCK_SIZE;
        size_t input_offset;

        if (full_blocks > 0) {
            const auto* full_header = reinterpret_cast<const DeltaHeader<T>*>(input);
            input_offset = sizeof(DeltaHeader<T>) / sizeof(T) + full_blocks * calculate_block_size(full_header->bit_width_);
        } else {
            input_offset = sizeof(DeltaSize)/sizeof(T);
        }

        const size_t remainder = size_header->num_rows_ % BLOCK_SIZE;
        if (remainder > 0) {
            const auto* remainder_metadata = reinterpret_cast<const RemainderMetadata*>(input + input_offset);
            remainder_metadata->magic_.check();

            input_offset += calc_remainder_size<T>(remainder, remainder_metadata->bit_width);
        }

        return input_offset;
    }

};

} // namespace arcticdb