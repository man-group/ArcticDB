/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <fmt/format.h>

#include <arcticdb/util/constructors.hpp>

#include <cstdint>
#include <stdexcept>
#include <vector>
#include <unordered_map>

namespace arcticdb {

namespace {
using BaseType = std::uint32_t;
constexpr BaseType error_category_scale = 1000u;
}

enum class ErrorCategory : BaseType {
    INTERNAL = 1,
    NORMALIZATION = 2,
    MISSING_DATA = 3,
    SCHEMA = 4,
    STORAGE = 5,
    SORTING = 6,
    USER_INPUT = 7,
    COMPATIBILITY = 8,
    // NEW CATEGORIES MUST ALSO BE ADDED TO python_module.cpp:register_error_code_ecosystem
};

// FUTURE(GCC9): use magic_enum
inline std::unordered_map<ErrorCategory, const char*> get_error_category_names() {
    return {
        {ErrorCategory::INTERNAL, "INTERNAL"},
        {ErrorCategory::NORMALIZATION, "NORMALIZATION"},
        {ErrorCategory::MISSING_DATA, "MISSING_DATA"},
        {ErrorCategory::SCHEMA, "SCHEMA"},
        {ErrorCategory::STORAGE, "STORAGE"},
        {ErrorCategory::SORTING, "SORTING"},
        {ErrorCategory::USER_INPUT, "USER_INPUT"},
        {ErrorCategory::COMPATIBILITY, "COMPATIBILITY"},
    };
}

// A macro that will be expanded in different ways by redefining ERROR_CODE():
// FUTURE(GCC9): use magic_enum
#define ARCTIC_ERROR_CODES \
    ERROR_CODE(1000, E_INVALID_RANGE) \
    ERROR_CODE(1001, E_INVALID_ARGUMENT) \
    ERROR_CODE(1002, E_ASSERTION_FAILURE) \
    ERROR_CODE(1003, E_RUNTIME_ERROR) \
    ERROR_CODE(1004, E_STORED_CONFIG_ERROR) \
    ERROR_CODE(2000, E_INCOMPATIBLE_OBJECTS) \
    ERROR_CODE(2001, E_UNIMPLEMENTED_INPUT_TYPE) \
    ERROR_CODE(2002, E_UPDATE_NOT_SUPPORTED) \
    ERROR_CODE(2003, E_INCOMPATIBLE_INDEX)  \
    ERROR_CODE(2004, E_WRONG_SHAPE) \
    ERROR_CODE(3000, E_NO_SUCH_VERSION)  \
    ERROR_CODE(3010, E_UNREADABLE_SYMBOL_LIST)  \
    ERROR_CODE(4000, E_DESCRIPTOR_MISMATCH)  \
    ERROR_CODE(4001, E_COLUMN_DOESNT_EXIST)  \
    ERROR_CODE(4002, E_UNSUPPORTED_COLUMN_TYPE)  \
    ERROR_CODE(4003, E_UNSUPPORTED_INDEX_TYPE)   \
    ERROR_CODE(4004, E_OPERATION_NOT_SUPPORTED_WITH_PICKLED_DATA)  \
    ERROR_CODE(5000, E_KEY_NOT_FOUND) \
    ERROR_CODE(5001, E_DUPLICATE_KEY) \
    ERROR_CODE(5002, E_SYMBOL_NOT_FOUND) \
    ERROR_CODE(6000, E_UNSORTED_DATA) \
    ERROR_CODE(7000, E_INVALID_USER_ARGUMENT) \
    ERROR_CODE(7001, E_INVALID_DECIMAL_STRING)   \
    ERROR_CODE(7002, E_INVALID_CHAR_IN_SYMBOL) \
    ERROR_CODE(8000, E_UNRECOGNISED_COLUMN_STATS_VERSION) \

enum class ErrorCode : BaseType {
#define ERROR_CODE(code, Name, ...) Name = code,
    ARCTIC_ERROR_CODES
#undef ERROR_CODE
};

struct ErrorCodeData {
    std::string_view name_;
    std::string_view as_string_;
};

template<ErrorCode code>
inline constexpr ErrorCodeData error_code_data{};

#define ERROR_CODE(code, Name, ...) template<> inline constexpr ErrorCodeData error_code_data<ErrorCode::Name> \
    { #Name, "E" #code };
ARCTIC_ERROR_CODES
#undef ERROR_CODE

inline std::vector<ErrorCode> get_error_codes() {
    static std::vector<ErrorCode> error_codes{
#define ERROR_CODE(code, Name) ErrorCode::Name,
        ARCTIC_ERROR_CODES
#undef ERROR_CODE
    };
    return error_codes;
}

ErrorCodeData get_error_code_data(ErrorCode code);

constexpr ErrorCategory get_error_category(ErrorCode code) {
    return static_cast<ErrorCategory>(static_cast<BaseType>(code) / error_category_scale);
}

struct ArcticException : public std::runtime_error {
    explicit ArcticException(const std::string& msg_with_error_code):
            std::runtime_error(msg_with_error_code) {
    }
};

template<ErrorCategory error_category>
struct ArcticCategorizedException : public ArcticException {
    using ArcticException::ArcticException;
};

template<ErrorCode specific_code>
struct ArcticSpecificException : public ArcticCategorizedException<get_error_category(specific_code)> {
    static constexpr ErrorCategory category = get_error_category(specific_code);

    explicit ArcticSpecificException(const std::string& msg_with_error_code) :
            ArcticCategorizedException<category>(msg_with_error_code) {
        static_assert(get_error_category(specific_code) == category);
    }
};

using InternalException = ArcticCategorizedException<ErrorCategory::INTERNAL>;
using SchemaException = ArcticCategorizedException<ErrorCategory::SCHEMA>;
using NormalizationException = ArcticCategorizedException<ErrorCategory::NORMALIZATION>;
using NoSuchVersionException = ArcticSpecificException<ErrorCode::E_NO_SUCH_VERSION>;
using StorageException = ArcticCategorizedException<ErrorCategory::STORAGE>;
using MissingDataException = ArcticCategorizedException<ErrorCategory::MISSING_DATA>;
using SortingException = ArcticCategorizedException<ErrorCategory::SORTING>;
using UnsortedDataException = ArcticSpecificException<ErrorCode::E_UNSORTED_DATA>;
using UserInputException = ArcticCategorizedException<ErrorCategory::USER_INPUT>;
using CompatibilityException = ArcticCategorizedException<ErrorCategory::COMPATIBILITY>;

template<ErrorCode error_code>
[[noreturn]] void throw_error(const std::string& msg) {
    throw ArcticCategorizedException<get_error_category(error_code)>(msg);
}

template<>
[[noreturn]] inline void throw_error<ErrorCode::E_NO_SUCH_VERSION>(const std::string& msg) {
    throw ArcticSpecificException<ErrorCode::E_NO_SUCH_VERSION>(msg);
}

template<>
[[noreturn]] inline void throw_error<ErrorCode::E_UNSORTED_DATA>(const std::string& msg) {
    throw ArcticSpecificException<ErrorCode::E_UNSORTED_DATA>(msg);
}

}

namespace fmt {
template<>
struct formatter<arcticdb::ErrorCode> {
    template<typename ParseContext>
    constexpr auto parse(ParseContext &ctx) { return ctx.begin(); }

    template<typename FormatContext>
    auto format(arcticdb::ErrorCode code, FormatContext &ctx) const {
        std::string_view str = arcticdb::get_error_code_data(code).as_string_;
        std::copy(str.begin(), str.end(), ctx.out());
        return ctx.out();
    }
};
}
