#pragma once

#include <cstddef>
#include <cstdint>

#include <arcticdb/storage/memory_layout.hpp>
#include <arcticdb/util/value_container.hpp>
#include <arcticdb/util/bitset.hpp>
#include <arcticdb/codec/compression/alp/constants.hpp>
#include <arcticdb/codec/compression/alp/encoder.hpp>
#include "alp_header.hpp"
#include "fastlanes_common.hpp"

namespace arcticdb {
struct DeltaCompressData {
    uint8_t simd_bit_width_ = 0;
    uint8_t remainder_bit_width_ = 0;
    uint32_t compressed_rows_ = 0;
    size_t full_blocks_ = 0;
    size_t remainder_ = 0;
};

struct FFORCompressData {
    ValueContainer reference_;
    size_t bits_needed_;

    template <typename T>
    FFORCompressData(T reference, size_t bits_needed) :
    bits_needed_(bits_needed) {
        set_reference(reference);
    }

    template <typename T>
    void set_reference(T value) {
        reference_.set_value(value);
    }

    template <typename T>
    T get_reference() const {
        return reference_.get_value<T>();
    }
};

template <typename T>
struct ALPCompressData {
    alp::state<T> state_;
    size_t max_bit_width_ = 0UL;
    size_t max_exceptions_ = 0UL;
};

struct BitPackData {
    size_t bits_needed_;
};

struct FrequencyEncodingData {
    ValueContainer value_;
    int32_t count_ = 0;
    std::pair<ValueContainer, uint32_t> leader_;
    std::shared_ptr<util::BitSet> bitset_;
    std::optional<size_t> expected_bytes_;
};

using EncoderData = std::variant<std::monostate, DeltaCompressData, FFORCompressData, FrequencyEncodingData, BitPackData, ALPCompressData<double>, ALPCompressData<float>>;

}