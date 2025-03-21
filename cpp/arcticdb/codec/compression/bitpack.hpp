#include <cstdint>
#include <cstddef>

#include <arcticdb/log/log.hpp>
#include <arcticdb/codec/compression/fastlanes_common.hpp>
#include <arcticdb/codec/compression/bitpack_fused.hpp>
#include <arcticdb/util/magic_num.hpp>
#include <arcticdb/column_store/column_data.hpp>
#include <arcticdb/codec/compression/contiguous_range_adaptor.hpp>

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
    uint32_t bit_width;
    util::SmallMagicNum<'B', 'r'> magic_;
};

template<typename T>
static size_t calculate_remainder_data_size_identity(size_t count, size_t bit_width) {
    static constexpr size_t t_bit = Helper<T>::num_bits;
    return sizeof(BitPackRemainderHeader) / sizeof(T) +
        1 + (count * bit_width + t_bit - 1) / t_bit;
}

template<typename T>
size_t compress_identity_remainder(const T *input, size_t count, T *output, size_t bit_width) {
    auto *metadata = new(output) BitPackRemainderHeader{
        static_cast<uint32_t>(count),
        static_cast<uint32_t>(bit_width),
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

    return calculate_remainder_data_size_identity<T>(count, bit_width);
}

template<typename T>
size_t decompress_identity_remainder(const T *input, T *output) {
    const auto *metadata = reinterpret_cast<const BitPackRemainderHeader *>(input);
    metadata->magic_.check();
    const uint32_t count = metadata->size;
    const uint32_t bit_width = metadata->bit_width;

    const T *data_in = input + header_size_in_t<BitPackRemainderHeader, T>();
    T current_word = *data_in;
    size_t bit_pos = 0;

    for (size_t i = 0; i < count; ++i) {
        T value = scalar_unpack(bit_width, bit_pos, current_word, data_in);
        output[i] = value;
    }

    return calculate_remainder_data_size_identity<T>(count, bit_width);
}

template<typename T>
size_t compute_bitwidth(ColumnData data) {
    if (data.has_field_stats()) {
        auto stats = data.field_stats();
        auto max_val = stats.get_max<T>();
        return std::bit_width(static_cast<std::make_unsigned_t<T>>(max_val));
    }

    auto it = data.cbegin<T, IteratorType::REGULAR>();
    T max_val = *it;
    for (; it != data.cend<T, IteratorType::REGULAR>(); ++it) {
        max_val = std::max(max_val, *it);
    }
    data.reset();
    return std::bit_width(static_cast<std::make_unsigned_t<T>>(max_val));
}

template<typename T>
struct BitPackCompressor {
    static size_t compress(ColumnData data, T *__restrict out, size_t count) {
        if (count == 0)
            return 0;

        const size_t bits_needed = compute_bitwidth<T>(data);
        ARCTICDB_DEBUG(log::codec(), "Bitpacking requires {} bits", bits_needed);

        auto *header = new(out) BitPackHeader<T>{
            static_cast<uint32_t>(bits_needed),
            static_cast<uint32_t>(count)
        };

        T *out_ptr = out + header_size_in_t<BitPackHeader<T>, T>();
        const size_t num_full_blocks = count / BLOCK_SIZE;
        size_t compressed_size = sizeof(BitPackHeader<T>) / sizeof(T);

        BitPackCompressKernel<T> kernel;
        if (data.num_blocks() == 1) {
            auto in = data.buffer().ptr_cast<T>(0, num_full_blocks * sizeof(T));
            for (size_t block = 0; block < num_full_blocks; ++block) {
                compressed_size += dispatch_bitwidth_fused<T, BitPackFused>(
                    in + block * BLOCK_SIZE,
                    out_ptr + compressed_size - header_size_in_t<BitPackHeader<T>, T>(),
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
                    out_ptr + compressed_size - header_size_in_t<BitPackHeader<T>, T>(),
                    bits_needed,
                    kernel
                );
            }
        }
        ARCTICDB_DEBUG(log::codec(), "Encoded {} full blocks to {} bytes", num_full_blocks, compressed_size * sizeof(T));

        size_t remaining = count % BLOCK_SIZE;
        ARCTICDB_DEBUG(log::codec(), "Compressing {} remainder values", remaining);
        if (remaining > 0) {
            DynamicRangeRandomAccessAdaptor<T> adaptor{data};
            auto remaining_in = adaptor.at(num_full_blocks * BLOCK_SIZE, remaining);
            T *remaining_out = out_ptr + compressed_size - header_size_in_t<BitPackHeader<T>, T>();

            compressed_size += compress_identity_remainder(
                remaining_in,
                remaining,
                remaining_out,
                bits_needed
            );
        }

        ARCTICDB_DEBUG(log::codec(), "Compressed size including remainder: {} from {} bytes", compressed_size * sizeof(T), count * sizeof(T));
        return compressed_size;
    }
};

template<typename T>
struct BitPackDecompressor {
    static size_t decompress(const T *__restrict in, T *__restrict out) {
        const auto *header = reinterpret_cast<const BitPackHeader<T> *>(in);
        const size_t count = header->num_rows;
        if (count == 0)
            return 0;

        const size_t bits_needed = header->bits_needed;
        ARCTICDB_DEBUG(log::codec(),
                       "Decompressing Identity data packed to {} bits",
                       bits_needed);

        const T *in_ptr = in + header_size_in_t<BitPackHeader<T>, T>();
        size_t input_offset = 0;

        const size_t num_full_blocks = count / BLOCK_SIZE;
        BitUnpackCompressKernel<T> kernel;

        if (bits_needed == 0) {
            std::fill(out, out + num_full_blocks * BLOCK_SIZE, static_cast<T>(0));
            input_offset += num_full_blocks * BLOCK_SIZE;
        } else {
            for (size_t block = 0; block < num_full_blocks; ++block) {
                input_offset += dispatch_bitwidth_fused<T, BitUnpackFused>(
                    in_ptr + input_offset,
                    out + block * BLOCK_SIZE,
                    bits_needed,
                    kernel
                );
            }
        }

        ARCTICDB_DEBUG(log::codec(), "Decompressed {} full blocks to offset {}", num_full_blocks, input_offset);
        size_t remaining = count % BLOCK_SIZE;
        if (remaining > 0) {
            input_offset += decompress_identity_remainder(
                in_ptr + input_offset,
                out + num_full_blocks * BLOCK_SIZE
            );
        }
        ARCTICDB_DEBUG(log::codec(), "Decompressed {} remaining values, total count {}", remaining, count);
        return count;
    }
};

} // namespace arcticdb