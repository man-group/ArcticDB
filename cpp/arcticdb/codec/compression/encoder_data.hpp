#pragma once

#include <cstddef>
#include <cstdint>

#include <arcticdb/storage/memory_layout.hpp>

namespace arcticdb {
struct DeltaCompressData {
    uint8_t simd_bit_width_ = 0;
    uint8_t remainder_bit_width_ = 0;
    uint32_t compressed_rows_ = 0;
    size_t full_blocks_ = 0;
    size_t remainder_ = 0;
};

//TODO lift this machinery out of here and stats into a base class
struct FFORCompressData {
    uint64_t data_;

    template <typename T>
    void set_value(T value) {
        memcpy(&data_, &value, sizeof(T));
    }

    template <typename T>
    T get_value() const {
        T target;
        memcpy(&target, &data_, sizeof(T));
        return target;
    }

    template <typename T>
    void set_reference(T value) {
        set_value(value);
    }

    template <typename T>
    T get_reference() const {
        return get_value<T>();
    }
};
using EncoderData = std::variant<std::monostate, DeltaCompressData, FFORCompressData>;

}