#pragma once

#include <arcticdb/log/log.hpp>
#include <arcticdb/util/preconditions.hpp>
#include <arcticdb/codec/compression/compressor.hpp>

#include <cstdint>

namespace arcticdb {

template <typename T>
struct __attribute((packed)) ConstantCompressData {
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

    size_t compress(const T *data_in, size_t num_rows, uint8_t *data_out) {
        if (num_rows == 0)
            return 0;

        auto *state = reinterpret_cast<ConstantCompressData<T> *>(data_out);
        state->size_ = num_rows;
        state->value_ = *data_in;
        return sizeof(ConstantCompressData<T>);
    }
};

template <typename T>
struct ConstantDecompressor {
    static DecompressResult decompress(const uint8_t *data_in, T *data_out) {
        const auto *state = reinterpret_cast<const ConstantCompressData<T>*>(data_in);
        auto *target = data_out;
        auto *target_end = target + state->size_;
        std::fill(target, target_end, state->value_);
        return {.compressed_ = state->size_, .uncompressed_=state->size_ * sizeof(T)};
    }
};
}