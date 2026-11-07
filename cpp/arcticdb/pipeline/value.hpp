/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/entity/types.hpp>
#include <arcticdb/util/format_date.hpp>

#include <cmath>
#include <limits>

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

    Value(const Value& other) : data_type_(other.data_type_), python_float_(other.python_float_) { assign(other); }

    Value(Value&& other) noexcept : data_type_(other.data_type_), python_float_(other.python_float_), len_(other.len_) {
        data_ = other.data_;
        other.data_type_ = DataType::UNKNOWN;
    }

    Value& operator=(const Value& other) {
        free_data();
        data_type_ = other.data_type_;
        python_float_ = other.python_float_;
        assign(other);
        return *this;
    }

    Value& operator=(Value&& other) noexcept {
        free_data();
        data_type_ = other.data_type();
        python_float_ = other.python_float_;
        data_ = other.data_;

        other.data_type_ = DataType::UNKNOWN;
        return *this;
    }

    ~Value() noexcept { free_data(); }

    [[nodiscard]] bool operator==(const Value& other) const {
        if (data_type_ != other.data_type_ || python_float_ != other.python_float_) {
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
        ARCTICDB_DEBUG_CHECK(
                ErrorCode::E_ASSERTION_FAILURE,
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
        } else if (is_time_type(data_type_)) {
            // The offset of a tz-aware input is discarded before the Value is built, so this is the UTC instant.
            return util::format_timestamp(get<timestamp>());
        } else {
            return fmt::format("{}", get<RawType>());
        }
    }

    [[nodiscard]] bool has_sequence_type() const { return is_sequence_type(data_type_); }

    [[nodiscard]] TypeDescriptor descriptor() const { return make_scalar_type(data_type_); }

    [[nodiscard]] DataType data_type() const { return data_type_; }

    // A Python float literal is weakly typed, as in NumPy's NEP 50: it is stored as FLOAT64 but takes the precision
    // of a FLOAT32 operand, so that the result matches the equivalent pandas/NumPy expression.
    [[nodiscard]] bool is_python_float() const { return python_float_; }

    [[nodiscard]] Value resolved_against(DataType other) const {
        if (python_float_ && other == DataType::FLOAT32) {
            return Value{narrow_to_float(get<double>()), DataType::FLOAT32};
        }
        return *this;
    }

    [[nodiscard]] bool is_nan() const {
        return details::visit_type(data_type_, [this]<typename TagType>(TagType) -> bool {
            using RawType = typename TagType::raw_type;
            if constexpr (std::is_floating_point_v<RawType>) {
                return std::isnan(get<RawType>());
            }
            return false;
        });
    }

  private:
    friend Value construct_python_float_value(double val);

    // static_cast to float is undefined for finite doubles outside float's range; round to nearest as IEEE does
    static float narrow_to_float(double val) {
        constexpr double overflow_threshold = static_cast<double>(std::numeric_limits<float>::max()) + 0x1p103;
        if (std::abs(val) > std::numeric_limits<float>::max()) {
            const float magnitude = std::abs(val) >= overflow_threshold ? std::numeric_limits<float>::infinity()
                                                                        : std::numeric_limits<float>::max();
            return val < 0 ? -magnitude : magnitude;
        }
        return static_cast<float>(val);
    }

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
    bool python_float_ = false;
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

inline Value construct_python_float_value(double val) {
    Value value{val, DataType::FLOAT64};
    value.python_float_ = true;
    return value;
}

inline Value construct_timestamp_value(const timestamp ts) { return Value{ts, DataType::NANOSECONDS_UTC64}; }

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
