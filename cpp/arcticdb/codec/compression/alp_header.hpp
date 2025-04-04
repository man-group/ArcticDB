#pragma once

#include <cstdint>
#include <cstddef>

#include <arcticdb/codec/compression/alp/alp.hpp>
#include <arcticdb/codec/compression/alp/decoder.hpp>

#include <arcticdb/util/magic_num.hpp>

namespace arcticdb {

template<typename T, typename = std::enable_if_t<std::is_same_v<T, float> || std::is_same_v<T, double>>>
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
        sizeof(exception_count_)
        + sizeof(right_bit_width_)
        + sizeof(left_bit_width_)
        + sizeof(dict_size_)
        + sizeof(magic_);

    using RightType = StorageType<T>::unsigned_type;

    uint8_t data_[1] = {};

    RealDoubleHeader() = default;

    explicit RealDoubleHeader(const alp::state<T> state) :
        exception_count_(state.exceptions_count),
        right_bit_width_(state.right_bit_width),
        left_bit_width_(state.left_bit_width) {
    }

    [[nodiscard]] size_t left_size() const {
        return alp::config::VECTOR_SIZE * sizeof(uint16_t);
    }

    [[nodiscard]] size_t right_size() const {
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

    uint16_t* left() {
        return at<uint16_t>(0UL);
    }

    RightType* right() {
        return at<RightType>(left_size());
    }

    [[nodiscard]] const uint16_t* left() const {
        return const_at<uint16_t>(0UL);
    }

    const RightType* right() const {
        return const_at<RightType>(left_size());
    }

    uint16_t* exceptions() {
        return at<uint16_t>(left_size() + right_size());
    }

    [[nodiscard]] const uint16_t* exceptions() const {
        return const_at<uint16_t>(left_size() + right_size());
    }

    [[nodiscard]] size_t exceptions_bytes() const {
        return exception_count_ * sizeof(uint16_t);
    }

    [[nodiscard]] size_t exceptions_count() const {
        return exception_count_;
    }

    uint16_t* exception_positions() {
        return at<uint16_t>(left_size() + right_size() + exceptions_bytes());
    }

    [[nodiscard]] const uint16_t* exception_positions() const {
        return const_at<uint16_t>(left_size() + right_size() + exceptions_bytes());
    }
    [[nodiscard]] size_t exception_positions_size() const {
        return exception_count_ * sizeof(uint16_t);
    }

    uint16_t* dict() {
        return at<uint16_t>(left_size() + right_size() + exceptions_bytes() + exception_positions_size());
    }

    [[nodiscard]] const uint16_t* dict() const {
        return const_at<uint16_t>(left_size() + right_size() + exceptions_bytes() + exception_positions_size());
    }

    [[nodiscard]] size_t total_size() const {
        return HeaderSize + left_size() + right_size() + exceptions_bytes() + exception_positions_size() + dict_size();
    }

    [[nodiscard]] size_t dict_size() const {
        return dict_size_ * sizeof(uint16_t);
    }

    void set_dict(uint16_t* dict_ptr, size_t dict_size) {
        dict_size_ = dict_size;
        memcpy(dict(), dict_ptr, dict_size_ * sizeof(uint16_t));
    }
};

template<typename T>
struct ALPDecimalHeader {
    // It may be beneficial to order members according to their alignment.
    util::SmallMagicNum<'A', 'l'> magic_;
    uint16_t exception_count_ = 0U;
    uint8_t bit_width_ = 0U;
    uint8_t exp_ = 0;
    uint8_t fac_ = 0;

    static constexpr size_t HeaderSize =
        sizeof(magic_)
        + sizeof(exception_count_)
        + sizeof(bit_width_)
        + sizeof(exp_)
        + sizeof(fac_);

    using EncodedType = typename StorageType<T>::signed_type;

    uint8_t data_[1] = {};

    ALPDecimalHeader() = default;

    explicit ALPDecimalHeader(const alp::state<T>& state) :
        magic_{},
        exception_count_{state.exceptions_count},
        bit_width_{state.bit_width},
        exp_{state.exp},
        fac_{state.fac} {
    }

    [[nodiscard]] size_t data_size() const {
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

    const EncodedType* data() const {
        return const_at<EncodedType>(0UL);
    }

    T* exceptions() {
        return at<T>(data_size());
    }

    const T* exceptions() const {
        return const_at<T>(data_size());
    }

    [[nodiscard]] size_t exceptions_bytes() const {
        return exception_count_ * sizeof(T);
    }

    [[nodiscard]] size_t exceptions_count() const {
        return exception_count_;
    }

    uint16_t* exception_positions() {
        return at<uint16_t>(data_size() + exceptions_bytes());
    }

    [[nodiscard]] const uint16_t* exception_positions() const {
        return const_at<uint16_t>(data_size() + exceptions_bytes());
    }

    [[nodiscard]] size_t exception_positions_size() const {
        return exception_count_ * sizeof(uint16_t);
    }

    [[nodiscard]] size_t total_size() const {
        return HeaderSize + data_size() + exceptions_bytes() + exception_positions_size();
    }
};
} //namespace arcticdb