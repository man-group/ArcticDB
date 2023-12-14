// Copyright 2014 MongoDB Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once
#if defined(__clang__)
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wfloat-equal"
#elif defined(__GNUC__)
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wfloat-equal"
#endif

#include <chrono>
#include <cstring>

#include <bsoncxx/array/view.hpp>
#include <bsoncxx/decimal128.hpp>
#include <bsoncxx/document/view.hpp>
#include <bsoncxx/oid.hpp>
#include <bsoncxx/stdx/string_view.hpp>

#include <bsoncxx/config/prelude.hpp>

namespace bsoncxx {
BSONCXX_INLINE_NAMESPACE_BEGIN

///
/// An enumeration of each BSON type.
/// These x-macros will expand to be of the form:
///    k_double = 0x01,
///    k_string = 0x02,
///    k_document = 0x03,
///    k_array = 0x04 ...
///
enum class type : std::uint8_t {
#define BSONCXX_ENUM(name, val) k_##name = val,
#include <bsoncxx/enums/type.hpp>
#undef BSONCXX_ENUM
    k_utf8 = 0x02,
};

///
/// An enumeration of each BSON binary sub type.
/// These x-macros will expand to be of the form:
///   k_binary = 0x00,
///   k_function = 0x01,
///   k_binary_deprecated = 0x02,
///   k_uuid_deprecated = 0x03,
///   k_uuid = 0x04,
///   k_md5 = 0x05,
///   k_encrypted = 0x06,
///   k_column = 0x07,
///   k_user = 0x80
///
enum class binary_sub_type : std::uint8_t {
#define BSONCXX_ENUM(name, val) k_##name = val,
#include <bsoncxx/enums/binary_sub_type.hpp>
#undef BSONCXX_ENUM
};

///
/// Returns a stringification of the given type.
///
/// @param rhs
///   The type to stringify.
///
/// @return a std::string representation of the type.
///
BSONCXX_API std::string BSONCXX_CALL to_string(type rhs);

///
/// Returns a stringification of the given binary sub type.
///
/// @param rhs
///   The type to stringify.
///
/// @return a std::string representation of the type.
///
BSONCXX_API std::string BSONCXX_CALL to_string(binary_sub_type rhs);

namespace types {

///
/// A BSON double value.
///
struct BSONCXX_API b_double {
    static constexpr auto type_id = type::k_double;

    double value;

    ///
    /// Conversion operator unwrapping a double
    ///
    BSONCXX_INLINE operator double() const {
        return value;
    }
};

///
/// free function comparator for b_double
///
/// @relatesalso b_double
///
BSONCXX_INLINE bool operator==(const b_double& lhs, const b_double& rhs) {
    return lhs.value == rhs.value;
}

///
/// A BSON UTF-8 encoded string value.
///
struct BSONCXX_API b_string {
    static constexpr auto type_id = type::k_string;

    ///
    /// Constructor for b_string.
    ///
    /// @param t
    ///   The value to wrap.
    ///
    template <typename T,
              typename std::enable_if<!std::is_same<b_string, typename std::decay<T>::type>::value,
                                      int>::type = 0>
    BSONCXX_INLINE explicit b_string(T&& t) : value(std::forward<T>(t)) {}

    stdx::string_view value;

    ///
    /// Conversion operator unwrapping a string_view
    ///
    BSONCXX_INLINE operator stdx::string_view() const {
        return value;
    }
};

///
/// free function comparator for b_string
///
/// @relatesalso b_string
///
BSONCXX_INLINE bool operator==(const b_string& lhs, const b_string& rhs) {
    return lhs.value == rhs.value;
}

///
/// This class has been renamed to b_string
///
/// @deprecated use b_string instead.
///
BSONCXX_DEPRECATED typedef b_string b_utf8;

///
/// A BSON document value.
///
struct BSONCXX_API b_document {
    static constexpr auto type_id = type::k_document;

    document::view value;

    ///
    /// Conversion operator unwrapping a document::view
    ///
    BSONCXX_INLINE operator document::view() const {
        return value;
    }

    ///
    /// Returns an unwrapped document::view
    ///
    BSONCXX_INLINE document::view view() {
        return value;
    }
};

///
/// free function comparator for b_document
///
/// @relatesalso b_document
///
BSONCXX_INLINE bool operator==(const b_document& lhs, const b_document& rhs) {
    return lhs.value == rhs.value;
}

///
/// A BSON array value.
///
struct BSONCXX_API b_array {
    static constexpr auto type_id = type::k_array;

    array::view value;

    ///
    /// Conversion operator unwrapping an array::view
    ///
    BSONCXX_INLINE operator array::view() const {
        return value;
    }
};

///
/// free function comparator for b_array
///
/// @relatesalso b_array
///
BSONCXX_INLINE bool operator==(const b_array& lhs, const b_array& rhs) {
    return lhs.value == rhs.value;
}

///
/// A BSON binary data value.
///
struct BSONCXX_API b_binary {
    static constexpr auto type_id = type::k_binary;

    binary_sub_type sub_type;
    uint32_t size;
    const uint8_t* bytes;
};

///
/// free function comparator for b_binary
///
/// @relatesalso b_binary
///
BSONCXX_INLINE bool operator==(const b_binary& lhs, const b_binary& rhs) {
    return lhs.sub_type == rhs.sub_type && lhs.size == rhs.size &&
           (!lhs.size || (std::memcmp(lhs.bytes, rhs.bytes, lhs.size) == 0));
}

///
/// A BSON undefined value.
///
/// @deprecated
///   This BSON type is deprecated and use by clients is discouraged.
///
struct BSONCXX_API b_undefined {
    static constexpr auto type_id = type::k_undefined;
};

///
/// free function comparator for b_undefined
///
/// @relatesalso b_undefined
///
BSONCXX_INLINE bool operator==(const b_undefined&, const b_undefined&) {
    return true;
}

///
/// A BSON ObjectId value.
///
struct BSONCXX_API b_oid {
    static constexpr auto type_id = type::k_oid;

    oid value;
};

///
/// free function comparator for b_oid
///
/// @relatesalso b_oid
///
BSONCXX_INLINE bool operator==(const b_oid& lhs, const b_oid& rhs) {
    return lhs.value == rhs.value;
}

///
/// A BSON boolean value.
///
struct BSONCXX_API b_bool {
    static constexpr auto type_id = type::k_bool;

    bool value;

    ///
    /// Conversion operator unwrapping a bool
    ///
    BSONCXX_INLINE operator bool() const {
        return value;
    }
};

///
/// free function comparator for b_bool
///
/// @relatesalso b_bool
///
BSONCXX_INLINE bool operator==(const b_bool& lhs, const b_bool& rhs) {
    return lhs.value == rhs.value;
}

///
/// A BSON date value.
///
struct BSONCXX_API b_date {
    static constexpr auto type_id = type::k_date;

    ///
    /// Constructor for b_date
    ///
    /// @param value
    ///   Milliseconds since the system_clock epoch.
    ///
    BSONCXX_INLINE
    explicit b_date(std::chrono::milliseconds value) : value(value) {}

    ///
    /// Constructor for b_date
    ///
    /// @param tp
    ///   A system_clock time_point.
    ///
    BSONCXX_INLINE
    explicit b_date(const std::chrono::system_clock::time_point& tp)
        : value(std::chrono::duration_cast<std::chrono::milliseconds>(tp.time_since_epoch())) {}

    std::chrono::milliseconds value;

    ///
    /// Conversion operator unwrapping a int64_t
    ///
    BSONCXX_INLINE operator int64_t() const {
        return value.count();
    }

    ///
    /// Manually convert this b_date to an int64_t
    ///
    BSONCXX_INLINE int64_t to_int64() const {
        return value.count();
    }

    ///
    /// Conversion operator unwrapping a time_point
    ///
    BSONCXX_INLINE operator std::chrono::system_clock::time_point() const {
        return std::chrono::system_clock::time_point(
            std::chrono::duration_cast<std::chrono::system_clock::duration>(value));
    }
};

///
/// free function comparator for b_date
///
/// @relatesalso b_date
///
BSONCXX_INLINE bool operator==(const b_date& lhs, const b_date& rhs) {
    return lhs.value == rhs.value;
}

///
/// A BSON null value.
///
struct BSONCXX_API b_null {
    static constexpr auto type_id = type::k_null;
};

///
/// free function comparator for b_null
///
/// @relatesalso b_null
///
BSONCXX_INLINE bool operator==(const b_null&, const b_null&) {
    return true;
}

///
/// A BSON regex value.
///
struct BSONCXX_API b_regex {
    static constexpr auto type_id = type::k_regex;

    ///
    /// Constructor for b_regex
    ///
    /// @param regex
    ///   The regex pattern
    ///
    /// @param options
    ///   The regex options
    ///
    template <typename T,
              typename U = stdx::string_view,
              typename std::enable_if<!std::is_same<b_regex, typename std::decay<T>::type>::value,
                                      int>::type = 0>
    BSONCXX_INLINE explicit b_regex(T&& regex, U&& options = U{})
        : regex(std::forward<T>(regex)), options(std::forward<U>(options)) {}

    stdx::string_view regex;
    stdx::string_view options;
};

///
/// free function comparator for b_regex
///
/// @relatesalso b_regex
///
BSONCXX_INLINE bool operator==(const b_regex& lhs, const b_regex& rhs) {
    return lhs.regex == rhs.regex && lhs.options == rhs.options;
}

///
/// A BSON DBPointer value.
///
/// @deprecated
///   A BSON DBPointer (aka DBRef) is still supported but deprecated.
///
struct BSONCXX_API b_dbpointer {
    static constexpr auto type_id = type::k_dbpointer;

    stdx::string_view collection;
    oid value;
};

///
/// free function comparator for b_dbpointer
///
/// @relatesalso b_dbpointer
///
BSONCXX_INLINE bool operator==(const b_dbpointer& lhs, const b_dbpointer& rhs) {
    return lhs.collection == rhs.collection && lhs.value == rhs.value;
}

///
/// A BSON JavaScript code value.
///
struct BSONCXX_API b_code {
    static constexpr auto type_id = type::k_code;

    ///
    /// Constructor for b_code.
    ///
    /// @param t
    ///   The js code
    ///
    template <typename T,
              typename std::enable_if<!std::is_same<b_code, typename std::decay<T>::type>::value,
                                      int>::type = 0>
    BSONCXX_INLINE explicit b_code(T&& t) : code(std::forward<T>(t)) {}

    stdx::string_view code;

    ///
    /// Conversion operator unwrapping a string_view
    ///
    BSONCXX_INLINE operator stdx::string_view() const {
        return code;
    }
};

///
/// free function comparator for b_code
///
/// @relatesalso b_code
///
BSONCXX_INLINE bool operator==(const b_code& lhs, const b_code& rhs) {
    return lhs.code == rhs.code;
}

///
/// A BSON Symbol value.
///
/// @deprecated
///   This BSON type is deprecated and use by clients is discouraged.
///
struct BSONCXX_API b_symbol {
    static constexpr auto type_id = type::k_symbol;

    ///
    /// Constructor for b_symbol.
    ///
    /// @param t
    ///   The symbol.
    ///
    template <typename T,
              typename std::enable_if<!std::is_same<b_symbol, typename std::decay<T>::type>::value,
                                      int>::type = 0>
    BSONCXX_INLINE explicit b_symbol(T&& t) : symbol(std::forward<T>(t)) {}

    stdx::string_view symbol;

    ///
    /// Conversion operator unwrapping a string_view
    ///
    BSONCXX_INLINE operator stdx::string_view() const {
        return symbol;
    }
};

///
/// free function comparator for b_symbol
///
/// @relatesalso b_symbol
///
BSONCXX_INLINE bool operator==(const b_symbol& lhs, const b_symbol& rhs) {
    return lhs.symbol == rhs.symbol;
}

///
/// A BSON JavaScript code with scope value.
///
struct BSONCXX_API b_codewscope {
    static constexpr auto type_id = type::k_codewscope;

    ///
    /// Constructor for b_codewscope.
    ///
    /// @param code
    ///   The js code
    ///
    /// @param scope
    ///   A bson document view holding the scope environment.
    ///
    template <
        typename T,
        typename U,
        typename std::enable_if<!std::is_same<b_codewscope, typename std::decay<T>::type>::value,
                                int>::type = 0>
    BSONCXX_INLINE explicit b_codewscope(T&& code, U&& scope)
        : code(std::forward<T>(code)), scope(std::forward<U>(scope)) {}

    stdx::string_view code;
    document::view scope;
};

///
/// free function comparator for b_codewscope
///
/// @relatesalso b_codewscope
///
BSONCXX_INLINE bool operator==(const b_codewscope& lhs, const b_codewscope& rhs) {
    return lhs.code == rhs.code && lhs.scope == rhs.scope;
}

///
/// A BSON signed 32-bit integer value.
///
struct BSONCXX_API b_int32 {
    static constexpr auto type_id = type::k_int32;

    int32_t value;

    ///
    /// Conversion operator unwrapping a int32_t
    ///
    BSONCXX_INLINE operator int32_t() const {
        return value;
    }
};

///
/// free function comparator for b_int32
///
/// @relatesalso b_int32
///
BSONCXX_INLINE bool operator==(const b_int32& lhs, const b_int32& rhs) {
    return lhs.value == rhs.value;
}

///
/// A BSON replication timestamp value.
///
/// @warning
///   This BSON type is used internally by the MongoDB server - use by clients
///   is discouraged.
///
struct BSONCXX_API b_timestamp {
    static constexpr auto type_id = type::k_timestamp;

    uint32_t increment;
    uint32_t timestamp;
};

///
/// free function comparator for b_timestamp
///
/// @relatesalso b_timestamp
///
BSONCXX_INLINE bool operator==(const b_timestamp& lhs, const b_timestamp& rhs) {
    return lhs.increment == rhs.increment && lhs.timestamp == rhs.timestamp;
}

///
/// A BSON 64-bit signed integer value.
///
struct BSONCXX_API b_int64 {
    static constexpr auto type_id = type::k_int64;

    int64_t value;

    ///
    /// Conversion operator unwrapping a int64_t
    ///
    BSONCXX_INLINE operator int64_t() const {
        return value;
    }
};

///
/// free function comparator for b_int64
///
/// @relatesalso b_int64
///
BSONCXX_INLINE bool operator==(const b_int64& lhs, const b_int64& rhs) {
    return lhs.value == rhs.value;
}

///
/// A BSON Decimal128 value.
///
struct BSONCXX_API b_decimal128 {
    static constexpr auto type_id = type::k_decimal128;

    decimal128 value;

    ///
    /// Constructor for b_decimal128.
    ///
    /// @param t
    ///   The value to wrap.
    ///
    template <
        typename T,
        typename std::enable_if<!std::is_same<b_decimal128, typename std::decay<T>::type>::value,
                                int>::type = 0>
    BSONCXX_INLINE explicit b_decimal128(T&& t) : value(std::forward<T>(t)) {}
};

///
/// free function comparator for b_decimal128
///
/// @relatesalso b_decimal128
///
BSONCXX_INLINE bool operator==(const b_decimal128& lhs, const b_decimal128& rhs) {
    return lhs.value == rhs.value;
}

///
/// A BSON min-key value.
///
struct BSONCXX_API b_minkey {
    static constexpr auto type_id = type::k_minkey;
};

///
/// free function comparator for b_minkey
///
/// @relatesalso b_minkey
///
BSONCXX_INLINE bool operator==(const b_minkey&, const b_minkey&) {
    return true;
}

///
/// A BSON max-key value.
///
struct BSONCXX_API b_maxkey {
    static constexpr auto type_id = type::k_maxkey;
};

///
/// free function comparator for b_maxkey
///
/// @relatesalso b_maxkey
///
BSONCXX_INLINE bool operator==(const b_maxkey&, const b_maxkey&) {
    return true;
}

#define BSONCXX_ENUM(name, val)                                                \
    BSONCXX_INLINE bool operator!=(const b_##name& lhs, const b_##name& rhs) { \
        return !(lhs == rhs);                                                  \
    }
#include <bsoncxx/enums/type.hpp>
#undef BSONCXX_ENUM

}  // namespace types
BSONCXX_INLINE_NAMESPACE_END
}  // namespace bsoncxx

#include <bsoncxx/config/postlude.hpp>

#if defined(__clang__)
#pragma clang diagnostic pop
#elif defined(__GNUC__)
#pragma GCC diagnostic pop
#endif
