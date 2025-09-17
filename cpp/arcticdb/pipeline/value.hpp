/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/entity/types.hpp>

namespace arcticdb {

using namespace arcticdb::entity;

struct Value {
    Value() = default;

    template<typename T>
    requires(!std::convertible_to<T, std::string_view>)
    explicit Value(T t) : Value(t, DataType::UNKNOWN) {}

    template<typename T>
    requires(!std::convertible_to<T, std::string_view>)
    explicit Value(T t, const DataType data_type) : data_type_(data_type) {
        *reinterpret_cast<T*>(&data_) = t;
    }

    explicit Value(const std::string_view string_data, const DataType data_type) : data_type_(data_type) {
        assign_string(string_data.data(), string_data.size());
    }

    Value(const Value& other) : data_type_(other.data_type_) { assign(other); }

    Value(Value&& other) noexcept : data_type_(other.data_type_), len_(other.len_) {
        data_ = other.data_;
        other.data_type_ = DataType::UNKNOWN;
    }

    Value& operator=(const Value& other) {
        free_data();
        data_type_ = other.data_type_;
        assign(other);
        return *this;
    }

    Value& operator=(Value&& other) noexcept {
        free_data();
        data_type_ = other.data_type();
        data_ = other.data_;

        other.data_type_ = DataType::UNKNOWN;
        return *this;
    }

    ~Value() noexcept { free_data(); }

    [[nodiscard]] bool operator==(const Value& other) const {
        if (data_type_ != other.data_type_) {
            return false;
        }
        if (!has_sequence_type()) {
            return details::visit_type(data_type_, [&, this]<typename TypeTag>(TypeTag) {
                using raw_type = typename TypeTag::raw_type;
                return get<raw_type>() == other.get<raw_type>();
            });
        }
        return len_ == other.len_ && strcmp(*str_data(), *other.str_data()) == 0;
    }

    template<typename T>
    [[nodiscard]] T get() const {
        return *reinterpret_cast<const T*>(data_.data());
    }

    template<typename T>
    void set(T t) {
        debug::check<ErrorCode::E_ASSERTION_FAILURE>(
                details::visit_type(
                        data_type_,
                        []<typename TypeTag>(TypeTag) {
                            return std::is_same_v<std::decay_t<T>, typename TypeTag::raw_type>;
                        }
                ),
                "Value type of type {} cannot represent {}",
                data_type_,
                t
        );
        *reinterpret_cast<T*>(data_.data()) = t;
    }

    [[nodiscard]] char** str_data() { return reinterpret_cast<char**>(data_.data()); }

    [[nodiscard]] const char* const* str_data() const { return reinterpret_cast<const char* const*>(data_.data()); }

    [[nodiscard]] size_t len() const { return len_; }

    template<typename RawType>
    [[nodiscard]] std::string to_string() const {
        if (has_sequence_type()) {
            return "\"" + std::string(*str_data(), len()) + "\"";
        } else {
            return fmt::format("{}", get<RawType>());
        }
    }

    [[nodiscard]] bool has_sequence_type() const { return is_sequence_type(data_type_); }

    [[nodiscard]] TypeDescriptor descriptor() const { return make_scalar_type(data_type_); }

    [[nodiscard]] DataType data_type() const { return data_type_; }

  private:
    void assign_string(const char* c, const size_t len) {
        const auto data = new char[len + 1];
        memcpy(data, c, len);
        data[len] = '\0';
        *str_data() = data;
        len_ = len;
    }

    void assign(const Value& other) {
        if (is_sequence_type(other.data_type_)) {
            assign_string(*other.str_data(), other.len_);
        } else {
            data_ = other.data_;
        }
    }

    void free_data() {
        if (has_sequence_type()) {
            delete[] *str_data();
        }
    }

    DataType data_type_ = DataType::UNKNOWN;
    std::array<uint8_t, 8> data_{};
    size_t len_ = 0;
};

template<typename T>
Value construct_value(T val) {
    util::raise_rte("Unrecognized value {}", val);
    return Value{};
}

#define VALUE_CONSTRUCT(__T__, __DT__)                                                                                 \
    template<>                                                                                                         \
    inline Value construct_value(__T__ val) {                                                                          \
        return Value{val, DataType::__DT__};                                                                           \
    }

VALUE_CONSTRUCT(bool, BOOL8)
VALUE_CONSTRUCT(uint8_t, UINT8)
VALUE_CONSTRUCT(uint16_t, UINT16)
VALUE_CONSTRUCT(uint32_t, UINT32)
VALUE_CONSTRUCT(uint64_t, UINT64)
VALUE_CONSTRUCT(int8_t, INT8)
VALUE_CONSTRUCT(int16_t, INT16)
VALUE_CONSTRUCT(int32_t, INT32)
VALUE_CONSTRUCT(int64_t, INT64)
VALUE_CONSTRUCT(float, FLOAT32)
VALUE_CONSTRUCT(double, FLOAT64)

inline Value construct_string_value(const std::string& str) { return Value{str, DataType::UTF_DYNAMIC64}; }

inline std::optional<std::string> ascii_to_padded_utf32(std::string_view str, size_t width) {
    if (str.size() * arcticdb::entity::UNICODE_WIDTH > width)
        return std::nullopt;
    std::string rv(width, '\0');
    auto input = str.data();
    auto output = rv.data();
    for ([[maybe_unused]] const auto& c : str) {
        *output = *input++;
        output += arcticdb::entity::UNICODE_WIDTH;
    }
    return rv;
}

} // namespace arcticdb
