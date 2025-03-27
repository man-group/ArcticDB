#pragma once

#include <arcticdb/column_store/column_data.hpp>

#include <cstring>

namespace arcticdb {

template<typename T>
struct PlainCompressor {
    static size_t compress(ColumnData data, T *__restrict out, size_t) {
        auto* __restrict out_ptr = reinterpret_cast<uint8_t*>(out);
        for(auto i = 0UL; i < data.num_blocks(); ++i) {
            auto block = data.buffer().blocks()[i];
            std::memcpy(out, block->data(), block->bytes());
            out_ptr += block->bytes();
        }
        const auto copied_bytes = (reinterpret_cast<T*>(out_ptr) - out) * sizeof(T);
        util::check(copied_bytes == data.buffer().bytes(), "Size mismatch in plan compression: {} != {}", copied_bytes, data.buffer().bytes());
        return copied_bytes;
    }
};

template<typename T>
struct PlainDecompressor {
    static size_t decompress(const T *__restrict in, T *__restrict out, size_t expected_count) {
        std::memcpy(out, in, expected_count * sizeof(T));
        return expected_count * sizeof(T);
    }
};

} //namespace arcticdb