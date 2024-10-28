#pragma once

#include <arcticdb/log/log.hpp>
#include <arcticdb/util/preconditions.hpp>
#include <arcticdb/codec/compression/compressor.hpp>
#include <arcticdb/column_store/column_data.hpp>
#include <arcticdb/codec/compression/fastlanes_common.hpp>

#include <cstdint>

namespace arcticdb {

template <typename T>
struct ARCTICDB_PACKED ConstantCompressData {
    uint64_t size_;
    T value_;
};

template<typename T>
struct ConstantCompressor {
    std::optional<size_t> max_required_bytes(const T *data_in, size_t num_rows) {
        if (num_rows == 0)
            return 0;

        const auto *pos = data_in;
        const auto *end = pos + num_rows;
        T first = *pos;
        ++pos;
        do {
            if (*pos != first)
                return std::nullopt;

            ++pos;
        } while (pos != end);

        return sizeof(ConstantCompressData<T>);
    }

    static size_t compressed_size() {
        return header_size_in_t<ConstantCompressData<T>, T>() * sizeof(T);
    }

    static size_t compress(ColumnData data, T* output, size_t expected_bytes) {
        const auto num_rows = data.row_count();
        if (num_rows == 0)
            return 0;

        auto *state = reinterpret_cast<ConstantCompressData<T> *>(output);
        state->size_ = num_rows;
        state->value_ = *data.buffer().ptr_cast<T>(0, sizeof(T));
        util::check(expected_bytes == sizeof(ConstantCompressData<T>), "Unexpected output size in constant compression: {} != {}", expected_bytes, sizeof(ConstantCompressData<T>));
        return header_size_in_t<ConstantCompressData<T>, T>() * sizeof(T);
    }
};

template <typename T>
struct ConstantDecompressor {
    static DecompressResult decompress(const uint8_t *data_in, T *data_out) {
        const auto *state = reinterpret_cast<const ConstantCompressData<T>*>(data_in);
        auto *target = data_out;
        auto *target_end = target + state->size_;
        std::fill(target, target_end, state->value_);
        auto compressed_size = header_size_in_t<ConstantCompressData<T>, T>() * sizeof(T);
        return {.compressed_ = compressed_size, .uncompressed_=state->size_ * sizeof(T)};
    }
};
}