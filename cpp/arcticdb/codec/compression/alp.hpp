#pragma once

#include <cstddef>
#include <cstdint>

#include <arcticdb/column_store/column_data.hpp>

namespace arcticdb {

template<typename T>
struct ALPCompressor {
    static size_t compress(ColumnData data, T *__restrict out, size_t count) {
        auto copied_bytes = 0UL;

        return copied_bytes;
    }
};

template<typename T>
struct ALPDecompressor {
    static size_t decompress(const T *__restrict in, T *__restrict out, size_t expected_count) {
        return expected_count * sizeof(T);
    }
};

} // namespace arcticdb