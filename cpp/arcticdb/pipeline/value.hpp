/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/entity/types.hpp>

namespace arcticdb {

using namespace arcticdb::entity;

struct ValueIterator {
    ValueIterator(char* data) :
        data_(data) {
    }

    bool finished() const {
        return false;
    }

    uint8_t* value() const {
        return reinterpret_cast<uint8_t *>(data_);
    }

    void next() {}

    char* data_;
};

struct Value {
    Value() = default;
    DataType data_type_;
    char data_[8] = {};
    size_t len_ = 0;

    template <typename T>
    Value(T t, DataType data_type=DataType::UNKNOWN):
    data_type_(data_type) {
        *reinterpret_cast<T*>(&data_) = t;
    }

    void assign(const Value& other) {
        if(is_sequence_type(other.data_type_))
            assign_string(*other.str_data(), other.len_);
        else
            memcpy(data_, other.data_, 8);
    }

    Value(const Value& other) :
        data_type_(other.data_type_) {
        assign(other);
    }

    Value& operator=(const Value& other) {
        data_type_ = other.data_type_;
        assign(other);
        return *this;
    }

    Value& operator==(const Value& other) {
        data_type_ = other.data_type_;
        assign(other);
        return *this;
    }

    ValueIterator get_iterator() {
        return {data_};
    }

    template <typename T>
    T get() const {
        return *reinterpret_cast<const T*>(&data_);
    }

    template <typename T>
    void set(T t) {
        *reinterpret_cast<T*>(data_) = t;
    }

    TypeDescriptor type() const {
        return make_scalar_type(data_type_);
    }

    char** str_data() {
        return reinterpret_cast<char**>(data_);
    }

    const char* const* str_data() const {
        return reinterpret_cast<const char* const *>(data_);
    }

    size_t len() const {
        return len_;
    }

    void assign_string(const char* c, size_t len) {
        auto data = new char[len + 1];
        memcpy(data, c, len);
        data[len] = 0;
        *str_data() = data;
        len_ = len;
    }

    void assign_string(const std::string& str) {
        len_ = str.size();
        auto data = new char[len_ + 1];
        memset(data, 0, len_ + 1);
        memcpy(data, str.data(), str.size()) ;

        *str_data() = data;
    }

    template<typename RawType>
    std::string to_string() const {
        if (has_sequence_type()) {
            return "\"" + std::string(*str_data(), len()) + "\"";
        }
        else {
            return fmt::format("{}", get<RawType>());
        }
    }

    ~Value() {
        if(has_sequence_type())
            delete[] *str_data();
    }

    bool has_sequence_type() const {
        return is_sequence_type(data_type_);
    }

    auto descriptor() {
        return TypeDescriptor {data_type_, Dimension::Dim0};
    }
};

template<typename T>
Value construct_value(T val) {
    util::raise_rte("Unrecognized value {}", val);
    return Value{};
}

#define VALUE_CONSTRUCT(__T__, __DT__)  \
   template<> \
    inline Value construct_value(__T__ val) { \
        Value value; \
        value.data_type_ = DataType::__DT__; \
        *(reinterpret_cast<__T__ *>(&(value.data_))) = val; \
        return value; \
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

inline Value construct_string_value(const std::string& str) {
    Value value;
    value.data_type_ = DataType::UTF_DYNAMIC64;
    value.assign_string(str);
    return value;
}

inline std::optional<std::string> ascii_to_padded_utf32(std::string_view str, size_t width) {
    if (str.size() * arcticdb::entity::UNICODE_WIDTH > width)
        return std::nullopt;
    std::string rv(width, '\0');
    auto input = str.data();
    auto output = rv.data();
    for ([[maybe_unused]] const auto& c: str) {
        *output = *input++;
        output += arcticdb::entity::UNICODE_WIDTH;
    }
    return rv;
}

} //namespace arcticdb
