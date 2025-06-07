/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <cstdint>
#include <cstddef>
#include <limits>
#include <algorithm>
#include <bit>

#include <arcticdb/codec/compression/fastlanes_common.hpp>
#include <arcticdb/codec/compression/encoder_data.hpp>
#include <arcticdb/codec/compression/bitpack_fused.hpp>
#include <arcticdb/codec/compression/compressor.hpp>
#include <arcticdb/util/preprocess.hpp>
#include <arcticdb/util/magic_num.hpp>
#include <arcticdb/codec/compression/transpose.hpp>
#include <arcticdb/codec/compression/contiguous_range_adaptor.hpp>

namespace arcticdb {

struct DeltaSize {
    uint32_t num_rows_;
};

template<typename T>
struct DeltaHeader : public DeltaSize {
    using h = Helper<T>;

    uint32_t bit_width_;
};


template <typename T>
struct DeltaCompressKernel {
    using h = Helper<MakeUnsignedType<T>>;
    T prev_[h::num_lanes];

    ARCTICDB_NO_MOVE_OR_COPY(DeltaCompressKernel)

    explicit DeltaCompressKernel(const T* in) {
        for(auto i = 0UL; i < h::num_lanes; ++i) {
            const auto idx = i * h::num_bits;
            ARCTICDB_TRACE(log::codec(), "Setting inital {} to {} ({})", i, in[idx], idx);
            prev_[i] = in[idx];
        }
    }

    ARCTICDB_ALWAYS_INLINE MakeUnsignedType<T> operator()(const T* ptr, size_t offset, size_t lane) {
        T value = ptr[offset];
        MakeUnsignedType<T> delta = value - prev_[lane];
        prev_[lane] = value;
        return delta;
    }
};
template<typename T>
struct DeltaUncompressKernel {
    using h = Helper<T>;
    T prev_[h::num_lanes];

    ARCTICDB_NO_MOVE_OR_COPY(DeltaUncompressKernel)

    explicit DeltaUncompressKernel(const T* in) {
        std::copy(in, in + h::num_lanes, prev_);
    }

    void operator()(T* __restrict ptr, size_t offset, MakeUnsignedType<T> value, size_t lane)  {
        const T result = value + prev_[lane];
        prev_[lane] = result;
        ptr[offset] = result;
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
size_t compress_delta_remainder(const T* input, size_t count, T* output, size_t bit_width) {
    auto* metadata [[maybe_unused]] = new (output) RemainderMetadata{
        static_cast<uint32_t>(count),
        static_cast<uint32_t>(bit_width),
        {}
    };
    ARCTICDB_DEBUG(log::codec(), "Delta compressing {} values of remainder", count);
    auto* data_start = output + sizeof(RemainderMetadata) / sizeof(T);
    *data_start = input[0];
    auto data_out = reinterpret_cast<MakeUnsignedType<T>*>(data_start);
    ++data_out;

    T prev = input[0];
    size_t bit_pos = 0;
    MakeUnsignedType<T> current_word = 0;

    for (size_t i = 1; i < count; ++i) {
        MakeUnsignedType<T> delta = input[i] - prev;
        //ARCTICDB_TRACE(log::codec(), "Value {}, delta = {}", input[i], delta);
        scalar_pack(delta, bit_width, bit_pos, current_word, data_out);
        prev = input[i];
    }
    *data_out = current_word;

    return calc_remainder_size<T>(count, bit_width);
}

template<typename T>
size_t decompress_delta_remainder(const T* input, T* output) {
    const auto* metadata = reinterpret_cast<const RemainderMetadata*>(input);
    const uint32_t count = metadata->size;
    const uint32_t bit_width = metadata->bit_width;

    const T* data_start = input + sizeof(RemainderMetadata)/sizeof(T);
    output[0] = *data_start;
    auto data_in = reinterpret_cast<const MakeUnsignedType<T>*>(data_start);
    ++data_in;
    MakeUnsignedType<T> current_word = *data_in;
    size_t bit_pos = 0;

    for (size_t i = 1; i < count; ++i) {
        MakeUnsignedType<T> delta = scalar_unpack(bit_width, bit_pos, current_word, data_in);
        output[i] = output[i-1] + delta;
    }

     return calc_remainder_size<T>(count, bit_width);
}

template <typename T>
constexpr size_t initial_values_size() {
    return sizeof(std::array<T, Helper<T>::num_lanes>) / sizeof(T);
}

template <typename T>
size_t calculate_block_size(size_t bit_width) {
    const size_t bits_needed = BLOCK_SIZE * bit_width;
    constexpr auto t_bits = Helper<T>::num_bits;
    const auto size_in_t = (bits_needed + t_bits - 1) / (t_bits);
    return size_in_t + initial_values_size<T>();
}

template <typename T>
constexpr size_t delta_header_size() {
    return (sizeof(DeltaHeader<T>) + sizeof(T) - 1) / sizeof(T);
}

template<typename T>//, std::enable_if_t<std::is_integral_v<DataType>, int> = 0>
class DeltaCompressor : public DeltaCompressData {
    using Header = DeltaHeader<T>;
    using h = Helper<T>;

private:
    std::array<T, h::num_lanes> initial_values_ = {};

    static constexpr size_t t_bits = h::num_bits;

    [[nodiscard]] size_t remainder_offset() const {
        return full_blocks_ * BLOCK_SIZE;
    }

    void copy_input_to_initial_values(const T* input) {
        for (size_t lane = 0; lane < h::num_lanes; ++lane) {
            initial_values_[lane] = input[lane * t_bits];
        }
    }

    size_t create_full_header(T* output) {
        auto* header [[maybe_unused]] = new (output) Header{
            {compressed_rows_},
            simd_bit_width_
        };

        return delta_header_size<T>();
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

        return rows / BLOCK_SIZE;
    }

public:
    explicit DeltaCompressor(DeltaCompressData data) :
        DeltaCompressData(data) {
    }

    DeltaCompressor() = default;

    size_t scan(const ColumnData input, size_t rows) {
        full_blocks_ = calculate_full_blocks(rows);
        compressed_rows_ = rows;
        remainder_ = compressed_rows_ % BLOCK_SIZE;
        ARCTICDB_DEBUG(log::codec(), "Delta scanning {} rows, {} total blocks with remainder of {}", rows, full_blocks_, remainder_);

        size_t total_size = 0UL;
        if (full_blocks_ > 0) {
            total_size += delta_header_size<T>();
            ARCTICDB_DEBUG(log::codec(), "Total size including header: {}", total_size);

            MakeUnsignedType<T> max_delta = std::numeric_limits<MakeUnsignedType<T>>::lowest();
            if(input.num_blocks() == 1) {
                auto ptr = reinterpret_cast<const T*>(input.buffer().data());
                auto current = *ptr;
                for (auto i = 1UL; i < full_blocks_ * BLOCK_SIZE; ++i) {
                    const MakeUnsignedType<T> delta = ptr[i] - current;
                    current = ptr[i];
                    max_delta = std::max(delta, max_delta);
                }
            } else {
                ContiguousRangeForwardAdaptor<T, BLOCK_SIZE> adaptor(input);
                auto current = *adaptor.current();
                for(auto i = 0UL; i < full_blocks_; ++i) {
                    auto ptr = adaptor.next();
                    for (auto j = 0UL; j < BLOCK_SIZE; ++j) {
                        const MakeUnsignedType<T> delta = ptr[j] - current;
                        current = ptr[j];
                        max_delta = std::max(delta, max_delta);
                    }
                }
            }
            simd_bit_width_ = std::bit_width(static_cast<MakeUnsignedType<T>>(max_delta));
            util::check(simd_bit_width_ > 0, "Got zero maximum bit_width, value is constant!");
            total_size += full_blocks_ * calculate_block_size<T>(simd_bit_width_);
        } else {
            total_size += sizeof(DeltaSize) / sizeof(T);
        }

        ARCTICDB_DEBUG(log::codec(), "Total size including full blocks: {}", total_size);
        if (remainder_ > 0) {
            DynamicRangeRandomAccessAdaptor<T> adaptor{input};
            const T* remainder_ptr = adaptor.at(remainder_offset(), remainder_);
            T prev = remainder_ptr[0];
            MakeUnsignedType<T> max_delta = 0;

            for (size_t i = 1; i < remainder_; i++) {
                MakeUnsignedType<T> delta = remainder_ptr[i] - prev;
                max_delta = std::max(max_delta, delta);
                prev = remainder_ptr[i];
            }
            remainder_bit_width_ = max_delta == 0 ? 1 : std::bit_width(static_cast<MakeUnsignedType<T>>(max_delta));
            total_size += calc_remainder_size<T>(remainder_, remainder_bit_width_);
        }
        ARCTICDB_DEBUG(log::codec(), "Total size including remainder: {}", total_size);
        return total_size * sizeof(T);
    }

    void block_compress(
            const T* input,
            size_t& output_offset,
            std::array<T, BLOCK_SIZE>& transposed,
            T* output) {
        DeltaCompressKernel<T> compress_kernel(input);
        copy_input_to_initial_values(input);
        memcpy(output + output_offset, initial_values_.data(), sizeof(initial_values_));
        transpose(input, transposed.data());
        output_offset += initial_values_size<T>();
        output_offset += dispatch_bitwidth_fused<T, BitPackFused>(
            transposed.data(),
            output + output_offset,
            simd_bit_width_,
            compress_kernel
        );
    }

    size_t compress(ColumnData data, T* output, size_t expected_bytes) {
        size_t output_offset = 0;
        util::check(simd_bit_width_ < h::num_bits, "Bit width is {}, no compression possible", h::num_bits);

        if (full_blocks_ > 0) {
            output_offset += create_full_header(output);
            std::array<T, BLOCK_SIZE> transposed;
            ARCTICDB_DEBUG(log::codec(), "Writing full blocks at offset {}", output_offset);
            if(data.num_blocks() == 1) {
                auto input = data.buffer().ptr_cast<T>(0, full_blocks_ * BLOCK_SIZE);
                for (size_t block = 0UL; block < full_blocks_; block++) {
                    const auto input_offset = block * BLOCK_SIZE;
                    block_compress(input + input_offset, output_offset, transposed, output);
                }
            } else {
                ContiguousRangeForwardAdaptor<T, BLOCK_SIZE> adaptor(data);
                for(auto i = 0UL; i < full_blocks_; ++i) {
                    auto input = adaptor.next();
                    block_compress(input, output_offset, transposed, output);
                }
            }
        } else {
            output_offset += create_size_only_header(output);
        }

        ARCTICDB_DEBUG(log::codec(), "Writing remainder at {}", output_offset);
        if (remainder_ > 0) {
            DynamicRangeRandomAccessAdaptor<T> adaptor{data};
            const T* remainder_ptr = adaptor.at(remainder_offset(), remainder_);
            ARCTICDB_DEBUG(log::codec(), "Remainder offset: {} ({}), first value {}", remainder_offset(), output_offset, *remainder_ptr);
            output_offset += compress_delta_remainder(
                remainder_ptr,
                remainder_,
                output + output_offset,
                remainder_bit_width_
            );
        }
        ARCTICDB_DEBUG(log::codec(), "Compressed to {} bytes", output_offset);
        const auto output_bytes = output_offset * sizeof(T);
        util::check(output_bytes <= expected_bytes, "Buffer overflow on compression: {} > {}", output_bytes, expected_bytes);
        return output_bytes;
    }

    EncoderData data() {
        return {static_cast<DeltaCompressData>(*this)};
    }
};

template<typename T>
class DeltaDecompressor {
    using Header = DeltaSize;
    using h = Helper<T>;

private:
    const Header *header_;
    size_t full_blocks_;
    size_t remainder_;

    [[nodiscard]] size_t calculate_input_offset() const {
        return full_blocks_ > 0 ? sizeof(DeltaHeader<T>) / sizeof(T) : sizeof(Header) / sizeof(T);
    }

    [[nodiscard]] size_t remainder_offset() const {
        return full_blocks_ * BLOCK_SIZE;
    }

public:
    void init(const T *input) {
        header_ = reinterpret_cast<const Header *>(input);
        full_blocks_ = header_->num_rows_ / BLOCK_SIZE;
        remainder_ = header_->num_rows_ % BLOCK_SIZE;
    }

    [[nodiscard]] size_t num_rows() const {
        return header_->num_rows_;
    }

    DecompressResult decompress(const T *input, T *output) {
        size_t input_offset = calculate_input_offset();
        if (full_blocks_ > 0) {
            auto full_header = reinterpret_cast<const DeltaHeader<T> *>(header_);

            std::array<T, BLOCK_SIZE> untransposed;
            for (size_t block = 0; block < full_blocks_; ++block) {
                DeltaUncompressKernel<T> decompress_kernel(input + input_offset);
                input_offset += h::num_lanes;

                input_offset += dispatch_bitwidth_fused<T, BitUnpackFused>(
                    input + input_offset,
                    untransposed.data(),
                    full_header->bit_width_,
                    decompress_kernel
                );
                untranspose(untransposed.data(), output +  block * BLOCK_SIZE);
            }
        }

        if (remainder_ > 0) {
            ARCTICDB_DEBUG(log::codec(), "Decompressing remainder at offset {}", input_offset);
            input_offset += decompress_delta_remainder(
                input + input_offset,
                output + remainder_offset()
            );
        }

        return {.compressed_ = input_offset * sizeof(T), .uncompressed_ = header_->num_rows_ * sizeof(T)};
    }

    static size_t compressed_size(const T* input) {
        const auto* size_header = reinterpret_cast<const DeltaSize*>(input);
        const size_t full_blocks = size_header->num_rows_ / BLOCK_SIZE;
        size_t input_offset;

        if (full_blocks > 0) {
            const auto* full_header = reinterpret_cast<const DeltaHeader<T>*>(input);
            input_offset = delta_header_size<T>() + full_blocks * calculate_block_size<T>(full_header->bit_width_);
        } else {
            input_offset = sizeof(DeltaSize) / sizeof(T);
        }

        const size_t remainder = size_header->num_rows_ % BLOCK_SIZE;
        if (remainder > 0) {
            const auto* remainder_metadata = reinterpret_cast<const RemainderMetadata*>(input + input_offset);
            remainder_metadata->magic_.check();

            input_offset += calc_remainder_size<T>(remainder, remainder_metadata->bit_width);
        }
        return input_offset * sizeof(T);
    }


    static size_t compressed_size(const DeltaCompressData& data, size_t num_rows) {
        const size_t full_blocks = num_rows / BLOCK_SIZE;
        size_t total_size;

        if (full_blocks > 0) {
            total_size = delta_header_size<T>() + full_blocks * calculate_block_size<T>(data.simd_bit_width_);
        } else {
            total_size = sizeof(DeltaSize) / sizeof(T);
        }

        const size_t remainder = num_rows % BLOCK_SIZE;
        if (remainder > 0) {
            total_size += calc_remainder_size<T>(remainder, data.remainder_bit_width_);
        }
        return total_size;
    }
};

} // namespace arcticdb