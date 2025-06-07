#pragma once

#include <arcticdb/column_store/column_data.hpp>
#include <arcticdb/codec/compression/compressor.hpp>

#include <cstring>

namespace arcticdb {

struct PlainCompressData {
    uint32_t num_rows;
};

template<typename T>
struct PlainCompressor {

    constexpr static size_t overhead() {
        return sizeof(PlainCompressData);
    }

    static size_t compress(ColumnData data, T *__restrict out, size_t output_size) {
        auto *out_ptr = reinterpret_cast<uint8_t *>(out);
        PlainCompressData header = {.num_rows=static_cast<uint32_t>(data.row_count())};
        std::memcpy(out_ptr, &header, sizeof(PlainCompressData));
        out_ptr += sizeof(PlainCompressData);
        for (auto i = 0UL; i < data.num_blocks(); ++i) {
            auto block = data.buffer().blocks()[i];
            std::memcpy(out_ptr, block->data(), block->bytes());
            out_ptr += block->bytes();
        }
        size_t data_bytes = static_cast<size_t>(out_ptr - reinterpret_cast<uint8_t *>(out)) - sizeof(PlainCompressData);
        util::check(data_bytes == data.buffer().bytes(), "Size mismatch in plain compression: {} != {}", data_bytes, data.buffer().bytes());
        auto size_written = data_bytes + sizeof(PlainCompressData);
        util::check(output_size == size_written, "Expected write size mismatch in plain encoder, {} != {}", output_size, size_written);
        ARCTICDB_DEBUG(log::codec(), "Plain compressor compressed to {} bytes", size_written);
        return size_written;
    }

    static size_t compress_shapes(const T *data, size_t num_elements, T *__restrict out, size_t output_size) {
        auto *out_ptr = reinterpret_cast<uint8_t *>(out);
        PlainCompressData header = {0};

        std::memcpy(out_ptr, &header, sizeof(PlainCompressData));
        out_ptr += sizeof(PlainCompressData);

        size_t data_bytes = num_elements * sizeof(T);
        std::memcpy(out_ptr, reinterpret_cast<const uint8_t *>(data), data_bytes);
        out_ptr += data_bytes;

        header.num_rows = num_elements;
        std::memcpy(out, &header, sizeof(PlainCompressData));

        size_t size_written = data_bytes + sizeof(PlainCompressData);
        util::check(output_size == size_written, "Expected write size mismatch in plain encoder, {} != {}", output_size, size_written);
        return size_written;
    }

};

template<typename T>
struct PlainDecompressor {
    static DecompressResult decompress(const uint8_t *__restrict in, T *__restrict out) {
        PlainCompressData header;
        std::memcpy(&header, in, sizeof(PlainCompressData));
        in += sizeof(PlainCompressData);
        std::memcpy(out, in, header.num_rows * sizeof(T));
        return {.compressed_=header.num_rows * sizeof(T) + sizeof(PlainCompressData), .uncompressed_=header.num_rows * sizeof(T)};
    }
};
} //namespace arcticdb