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

namespace arcticdb {

template<typename T>
class FForCompressKernel {
    const T reference_;

public:
    explicit FForCompressKernel(T reference) : reference_(reference) {}

    T operator()(const T* __restrict ptr, size_t offset, size_t /*lane*/) const {
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

    //TODO is this needed?
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

template <typename T>
std::pair<T, size_t> reference_and_bitwidth(ColumnData data) {
    if(data.has_field_stats()) {
        auto stats = data.field_stats();
        auto min = stats.get_min<T>();
        auto max = stats.get_max<T>();
        auto max_delta = max - min;
        auto bits_needed = std::bit_width(static_cast<std::make_unsigned_t<T>>(max_delta));
        return {min, bits_needed};
    }

    return data.type().visit_tag([&data] (auto tdt) -> std::pair<T, size_t> {
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

        size_t bits_needed = std::bit_width(static_cast<std::make_unsigned_t<T>>(max_delta));
        return {reference, bits_needed};
    });
}

template <typename T>
struct FForCompressor {
    static size_t compress(ColumnData data, T *__restrict out, size_t count) {
        if (count == 0)
            return 0;

        const auto [reference, bits_needed] = reference_and_bitwidth<T>(data);
        ARCTICDB_DEBUG(log::codec(), "FFOR bitpacking requires {} bits", bits_needed);

        auto *header [[maybe_unused]] = new(out) FForHeader<T>{
            reference,
            static_cast<uint32_t>(bits_needed),
            static_cast<uint32_t>(count)
        };

        T *out_ptr = out + header_size_in_t<FForHeader<T>, T>();

        const size_t num_full_blocks = count / 1024;
        size_t compressed_size = sizeof(FForHeader<T>) / sizeof(T);

        FForCompressKernel<T> kernel(reference);
        if (data.num_blocks() == 1) {
            auto in = data.buffer().ptr_cast<T>(0, num_full_blocks * sizeof(T));
            for (size_t block = 0; block < num_full_blocks; ++block) {
                compressed_size += dispatch_bitwidth_fused<T, BitPackFused>(
                    in + block * BLOCK_SIZE,
                    out_ptr + compressed_size - header_size_in_t<FForHeader<T>, T>(),
                    bits_needed,
                    kernel
                );
            }
        } else {
            ContiguousRangeForwardAdaptor<T, BLOCK_SIZE> adaptor{data};
            for (size_t block = 0; block < num_full_blocks; ++block) {
                auto in = adaptor.next();
                compressed_size += dispatch_bitwidth_fused<T, BitPackFused>(
                    in,
                    out_ptr + compressed_size - header_size_in_t<FForHeader<T>, T>(),
                    bits_needed,
                    kernel
                );
            }
        }
        ARCTICDB_DEBUG(log::codec(),
                       "Encoded {} full blocks to {} bytes",
                       num_full_blocks,
                       compressed_size * sizeof(T));

        size_t remaining = count % 1024;
        ARCTICDB_DEBUG(log::codec(), "Compressing {} remainder values", remaining);
        if (remaining > 0) {
            DynamicRangeRandomAccessAdaptor<T> adaptor{data};
            auto remaining_in = adaptor.at(num_full_blocks * BLOCK_SIZE, remaining);
            T *remaining_out = out_ptr + compressed_size - header_size_in_t<FForHeader<T>, T>();

            compressed_size += compress_remainder(
                remaining_in,
                remaining,
                remaining_out,
                bits_needed,
                reference
            );
        }
        ARCTICDB_DEBUG(log::codec(), "Compressed size including remainder: {} from {} bytes",
                       compressed_size * sizeof(T),
                       count * sizeof(T));

        return compressed_size;
    }
};

template <typename T>
struct FForDecompressor {
    static size_t decompress(const T *__restrict in, T *__restrict out) {
        static constexpr size_t BLOCK_SIZE = 1024;
        const auto *header = reinterpret_cast<const FForHeader<T> *>(in);
        const size_t count = header->num_rows;
        if (count == 0)
            return 0;

        const T reference = header->reference;
        const size_t bits_needed = header->bits_needed;

        ARCTICDB_DEBUG(log::codec(),
                       "Decompressing FFOR data packed to {} bits with reference {}",
                       bits_needed,
                       reference);

        const T *in_ptr = in + header_size_in_t<FForHeader<T>, T>();
        size_t input_offset = 0;

        const size_t num_full_blocks = count / BLOCK_SIZE;
        FForUncompressKernel<T> kernel(reference);

        if (bits_needed == 0) {
            std::fill(out, out + num_full_blocks * BLOCK_SIZE, reference);
            input_offset += num_full_blocks * BLOCK_SIZE;
        } else {
            for (size_t block = 0; block < num_full_blocks; ++block) {
                input_offset += dispatch_bitwidth_fused<T, BitUnpackFused>(
                    in_ptr + input_offset,
                    out + block * 1024,
                    bits_needed,
                    kernel
                );
            }
        }

        ARCTICDB_DEBUG(log::codec(), "Decompressed {} full blocks to offset {}", num_full_blocks, input_offset);
        size_t remaining = count % 1024;
        if (remaining > 0) {
            input_offset += decompress_remainder(
                in_ptr + input_offset,
                out + num_full_blocks * 1024
            );
        }
        ARCTICDB_DEBUG(log::codec(), "Decompressed {} remaining values to offset {}", remaining, input_offset * sizeof(T) + sizeof(FForHeader<T>));
        return count;
    }
};

} // namespace arcticdb