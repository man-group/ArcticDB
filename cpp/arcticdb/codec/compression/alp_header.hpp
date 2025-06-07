#pragma once

#include <cstdint>
#include <cstddef>

#include <arcticdb/codec/compression/alp/alp.hpp>
#include <arcticdb/codec/compression/alp/decoder.hpp>
#include <arcticdb/codec/compression/compression_utils.hpp>
#include <arcticdb/codec/compression/fastlanes_common.hpp>
#include <arcticdb/util/magic_num.hpp>

namespace arcticdb {

template<typename T, typename = std::enable_if_t<std::is_same_v<T, float> || std::is_same_v<T, double>>>
struct StorageType {
    using unsigned_type = alp::inner_t<T>::ut;
    using signed_type = alp::inner_t<T>::st;
};

struct RealDoubleBitwidths {
    uint8_t right_ = 0U;
    uint8_t left_ = 0U;
};

template<typename T>
struct RealDoubleColumnHeader {
    util::SmallMagicNum<'R', 'D'> magic_;
    RealDoubleBitwidths bit_widths_;
    uint32_t dict_size_ = 0U;

    static constexpr size_t HeaderSize =
        sizeof(magic_)
        + sizeof(bit_widths_)
        + sizeof(dict_size_);

    uint8_t data_[1] = {};

    RealDoubleColumnHeader() = default;

    explicit RealDoubleColumnHeader(const alp::state<T>& state)
        : magic_{},
          bit_widths_(state.right_bit_width, state.left_bit_width) {
        set_dict(state.left_parts_dict, state.actual_dictionary_size);
    }

    uint16_t* dict() {
        return reinterpret_cast<uint16_t*>(data_);
    }

    [[nodiscard]] const uint16_t* dict() const {
        return reinterpret_cast<const uint16_t*>(data_);
    }

    [[nodiscard]] size_t dict_size() const {
        return dict_size_ * sizeof(uint16_t);
    }

    [[nodiscard]] size_t total_size() const {
        return HeaderSize + dict_size();
    }

   [[nodiscard]] const RealDoubleBitwidths& bit_widths() const {
        return bit_widths_;
    }

    void set_dict(const uint16_t* dict_ptr, size_t dict_count) {
        dict_size_ = static_cast<uint32_t>(dict_count);
        memcpy(dict(), dict_ptr, dict_size_ * sizeof(uint16_t));
    }
};

template<typename T>
struct RealDoubleBlockHeader {
    uint16_t exception_count_ = 0U;
    util::SmallMagicNum<'R', 'b'> magic_;
    static constexpr size_t HeaderSize = sizeof(exception_count_) + sizeof(magic_);

    using RightType = StorageType<T>::unsigned_type;

    uint8_t data_[1] = {};

    template<typename RawType>
    RawType* at(size_t offset) {
        return reinterpret_cast<RawType*>(data_ + offset);
    }

    template<typename RawType>
    const RawType* const_at(size_t offset) const {
        return reinterpret_cast<const RawType*>(data_ + offset);
    }

    [[nodiscard]] size_t left_size(const RealDoubleBitwidths& bit_widths) const {
        return round_up_bits(alp::config::VECTOR_SIZE * bit_widths.left_);
    }

    [[nodiscard]] size_t right_size(const RealDoubleBitwidths& bit_widths) const {
        return round_up_bits(alp::config::VECTOR_SIZE * bit_widths.right_);
    }

    uint16_t* left() {
        return at<uint16_t>(0UL);
    }

    [[nodiscard]] const uint16_t* left() const {
        return const_at<uint16_t>(0UL);
    }

    RightType* right(const RealDoubleBitwidths& bit_widths) {
        return at<RightType>(left_size(bit_widths));
    }

    const RightType* right(const RealDoubleBitwidths& bit_widths) const {
        return const_at<RightType>(left_size(bit_widths));
    }

    uint16_t* exceptions(const RealDoubleBitwidths& bit_widths) {
        return at<uint16_t>(left_size(bit_widths) + right_size(bit_widths));
    }

    [[nodiscard]] const uint16_t* exceptions(const RealDoubleBitwidths& bit_widths) const {
        return const_at<uint16_t>(left_size(bit_widths) + right_size(bit_widths));
    }

    [[nodiscard]] size_t exceptions_bytes() const {
        return exception_count_ * sizeof(uint16_t);
    }

    [[nodiscard]] size_t exceptions_count() const {
        return exception_count_;
    }

    uint16_t* exception_positions(const RealDoubleBitwidths& bit_widths) {
        return at<uint16_t>(left_size(bit_widths) + right_size(bit_widths) + exceptions_bytes());
    }

    [[nodiscard]] const uint16_t* exception_positions(const RealDoubleBitwidths& bit_widths) const {
        return const_at<uint16_t>(left_size(bit_widths) + right_size(bit_widths) + exceptions_bytes());
    }

    [[nodiscard]] size_t exception_positions_size() const {
        return exception_count_ * sizeof(uint16_t);
    }

    [[nodiscard]] size_t total_size(const RealDoubleBitwidths& bit_widths) const {
        return HeaderSize + left_size(bit_widths) + right_size(bit_widths) + exceptions_bytes() + exception_positions_size();
    }
};

template<typename T>
struct ALPDecimalColumnHeader {
    util::SmallMagicNum<'A', 'l'> magic_;

    static constexpr size_t HeaderSize =
        sizeof(magic_);

    uint8_t data_[1] = {};

    ALPDecimalColumnHeader() = default;

    [[nodiscard]] size_t total_size() const {
        return HeaderSize;
    }
};

template<typename T>
struct ALPDecimalBlockHeader {
    using EncodedType = typename StorageType<T>::signed_type;

    util::SmallMagicNum<'D', 'b'> magic_;
    uint16_t exception_count_ = 0U;
    uint8_t bit_width_ = 0U;

    uint8_t exp_ = 0;
    uint8_t fac_ = 0;
    static constexpr size_t HeaderSize = sizeof(magic_) + sizeof(exception_count_) + sizeof(bit_width_) + sizeof(exp_) + sizeof(fac_);
    using h = Helper<std::make_unsigned_t<EncodedType>>;
    uint8_t data_[1] = {};

    template<typename RawType>
    RawType* at(size_t bytes) {
        return reinterpret_cast<RawType*>(data_ + bytes);
    }
    template<typename RawType>
    const RawType* const_at(size_t bytes) const {
        return reinterpret_cast<const RawType*>(data_ + bytes);
    }

    EncodedType* bases() {
        return at<EncodedType>(0UL);
    }

    const EncodedType* bases() const {
        return const_at<EncodedType>(0UL);
    }

    [[nodiscard]] constexpr size_t bases_size() const {
        return sizeof(EncodedType);
    }

    EncodedType* data() {
        return at<EncodedType>(bases_size());
    }
    const EncodedType* data() const {
        return const_at<EncodedType>(bases_size());
    }

    [[nodiscard]] size_t data_size() const {
        return alp::config::VECTOR_SIZE * sizeof(EncodedType);
    }

    T* exceptions() {
        return at<T>(bases_size() + data_size());
    }

    const T* exceptions() const {
        return const_at<T>(bases_size() + data_size());
    }

    [[nodiscard]] size_t exceptions_bytes() const {
        return exception_count_ * sizeof(T);
    }

    [[nodiscard]] size_t exceptions_count() const {
        return exception_count_;
    }

    uint16_t* exception_positions() {
        return at<uint16_t>(bases_size() + data_size() + exceptions_bytes());
    }

    [[nodiscard]] const uint16_t* exception_positions() const {
        return const_at<uint16_t>(bases_size() + data_size() + exceptions_bytes());
    }

    [[nodiscard]] size_t exception_positions_size() const {
        return exception_count_ * sizeof(uint16_t);
    }

    [[nodiscard]] size_t total_size() const {
        return HeaderSize + bases_size() + data_size() + exceptions_bytes() + exception_positions_size();
    }
};
} //namespace arcticdb