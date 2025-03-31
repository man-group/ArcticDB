#pragma once

#include <cstdint>
#include <cstddef>

#include <arcticdb/codec/compression/alp/alp.hpp>

#include <arcticdb/util/magic_num.hpp>

namespace arcticdb {

template<typename T>
struct StorageType {
    using unsigned_type = alp::inner_t<T>::ut;
    using signed_type = alp::inner_t<T>::st;
};

template<typename T>
struct RealDoubleHeader {
    util::SmallMagicNum<'R', 'D'> magic_;
    uint16_t exception_count_ = 0U;
    uint8_t right_bit_width_ = 0U;
    uint8_t left_bit_width_ = 0U;
    uint32_t dict_size_ = 0U;

    static constexpr size_t HeaderSize =
        sizeof(exception_count_) + sizeof(right_bit_width_) + sizeof(left_bit_width_) + sizeof(dict_size_)
            + sizeof(magic_);
    using RightType = uint64_t;

    uint8_t data_[1];
    RealDoubleHeader() = default;

    RealDoubleHeader(const alp::state <T> state) :
        exception_count_(state.exceptions_count),
        right_bit_width_(state.right_bit_width),
        left_bit_width_(state.left_bit_width) {
    }

    size_t left_size() const {
        return alp::config::VECTOR_SIZE * sizeof(uint16_t);
    }

    size_t right_size() const {
        return alp::config::VECTOR_SIZE * sizeof(RightType);
    }

    template<typename RawType>
    RawType *at(size_t bytes) {
        return reinterpret_cast<RawType *>(data_ + bytes);
    }

    template<typename RawType>
    const RawType *const_at(size_t bytes) const {
        return reinterpret_cast<const RawType *>(data_ + bytes);
    }

    uint16_t *left() {
        return at<uint16_t>(0UL);
    }

    RightType *right() {
        return at<RightType>(left_size());
    }

    uint16_t *exceptions() {
        return at<uint16_t>(left_size() + right_size());
    }

    size_t exceptions_bytes() const {
        return exception_count_ * sizeof(uint16_t);
    }

    size_t exceptions_count() const {
        return exception_count_;
    }

    uint16_t *exception_positions() {
        return at<uint16_t>(left_size() + right_size() + exceptions_bytes());
    }

    size_t exception_positions_size() const {
        return exception_count_ * sizeof(uint16_t);
    }

    uint16_t *dict() {
        return at<uint16_t>(left_size() + right_size() + exceptions_bytes() + exception_positions_size());
    }

    const uint16_t *dict() const {
        return const_at<uint16_t>(left_size() + right_size() + exceptions_bytes() + exception_positions_size());
    }

    size_t total_size() const {
        return HeaderSize + left_size() + right_size() + exceptions_bytes() + exception_positions_size() + dict_size();
    }

    size_t dict_size() const {
        return dict_size_ * sizeof(uint16_t);
    }

    void set_dict(uint16_t *dict_ptr, size_t dict_size) {
        dict_size_ = dict_size;
        memcpy(dict(), dict_ptr, dict_size_ * sizeof(uint16_t));
    }
};


template<typename T>
struct ALPDecimalHeader {
    util::SmallMagicNum<'R', 'D'> magic_;
    uint16_t exception_count_ = 0U;
    uint8_t bit_width_ = 0U;
    uint32_t dict_size_ = 0U;
    uint8_t exp_;
    uint8_t fac_;

    static constexpr size_t HeaderSize =
        sizeof(exception_count_) + sizeof(bit_width_) + sizeof(dict_size_) + sizeof(magic_);
    using EncodedType = StorageType<T>::signed_type;

    uint8_t data_[1];
    ALPDecimalHeader() = default;

    ALPDecimalHeader(const alp::state <T> state) :
        exception_count_(state.exceptions_count),
        bit_width_(state.bit_width),
        exp_(state.exp),
        fac_(state.fac) {
    }

    size_t data_size() const {
        return alp::config::VECTOR_SIZE * sizeof(EncodedType);
    }

    template<typename RawType>
    RawType *at(size_t bytes) {
        return reinterpret_cast<RawType *>(data_ + bytes);
    }

    template<typename RawType>
    const RawType *const_at(size_t bytes) const {
        return reinterpret_cast<const RawType *>(data_ + bytes);
    }

    EncodedType *data() {
        return at<EncodedType>(0UL);
    }

    T *exceptions() {
        return at<T>(data_size());
    }

    size_t exceptions_bytes() const {
        return exception_count_ * sizeof(T);
    }

    size_t exceptions_count() const {
        return exception_count_;
    }

    uint16_t *exception_positions() {
        return at<uint16_t>(data_size() + exceptions_bytes());
    }

    size_t exception_positions_size() const {
        return exception_count_ * sizeof(uint16_t);
    }

    size_t total_size() const {
        return HeaderSize + data_size() + exceptions_bytes() + exception_positions_size();
    }
};
} //namespace arcticdb