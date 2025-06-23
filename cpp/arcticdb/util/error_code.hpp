/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <fmt/format.h>
#include <cstdint>
#include <stdexcept>
#include <vector>
#include <unordered_map>
#include <folly/Function.h>
#include <variant>

namespace arcticdb {

namespace detail {
using BaseType = std::uint32_t;
constexpr BaseType error_category_scale = 1000u;
}

enum class ErrorCategory : detail::BaseType {
    INTERNAL = 1,
    NORMALIZATION = 2,
    MISSING_DATA = 3,
    SCHEMA = 4,
    STORAGE = 5,
    SORTING = 6,
    USER_INPUT = 7,
    COMPATIBILITY = 8,
    /// Exceptions resulting in failure to encode or decode data while reading/writing from/to disc
    CODEC = 9
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
        {ErrorCategory::CODEC, "CODEC"},
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
    ERROR_CODE(2000, E_INCOMPATIBLE_OBJECTS)\
    ERROR_CODE(2001, E_UNIMPLEMENTED_INPUT_TYPE) \
    ERROR_CODE(2002, E_UPDATE_NOT_SUPPORTED) \
    ERROR_CODE(2003, E_INCOMPATIBLE_INDEX)  \
    ERROR_CODE(2004, E_WRONG_SHAPE) \
    ERROR_CODE(2005, E_COLUMN_SECONDARY_TYPE_MISMATCH) \
    ERROR_CODE(2006, E_UNIMPLEMENTED_COLUMN_SECONDARY_TYPE) \
    ERROR_CODE(3000, E_NO_SUCH_VERSION)  \
    ERROR_CODE(3001, E_NO_SYMBOL_DATA)  \
    ERROR_CODE(3010, E_UNREADABLE_SYMBOL_LIST)  \
    ERROR_CODE(4000, E_DESCRIPTOR_MISMATCH)  \
    ERROR_CODE(4001, E_COLUMN_DOESNT_EXIST)  \
    ERROR_CODE(4002, E_UNSUPPORTED_COLUMN_TYPE)  \
    ERROR_CODE(4003, E_UNSUPPORTED_INDEX_TYPE)   \
    ERROR_CODE(4004, E_OPERATION_NOT_SUPPORTED_WITH_PICKLED_DATA) \
    ERROR_CODE(5000, E_KEY_NOT_FOUND) \
    ERROR_CODE(5001, E_DUPLICATE_KEY) \
    ERROR_CODE(5002, E_SYMBOL_NOT_FOUND) \
    ERROR_CODE(5003, E_PERMISSION)    \
    ERROR_CODE(5004, E_RESOURCE_NOT_FOUND) \
    ERROR_CODE(5005, E_UNSUPPORTED_ATOMIC_OPERATION) \
    ERROR_CODE(5010, E_LMDB_MAP_FULL) \
    ERROR_CODE(5011, E_UNEXPECTED_LMDB_ERROR) \
    ERROR_CODE(5020, E_UNEXPECTED_S3_ERROR) \
    ERROR_CODE(5021, E_S3_RETRYABLE) \
    ERROR_CODE(5022, E_ATOMIC_OPERATION_FAILED) \
    ERROR_CODE(5023, E_NOT_IMPLEMENTED_BY_STORAGE) \
    ERROR_CODE(5024, E_BAD_REQUEST) \
    ERROR_CODE(5030, E_UNEXPECTED_AZURE_ERROR) \
    ERROR_CODE(5050, E_MONGO_BULK_OP_NO_REPLY) \
    ERROR_CODE(5051, E_UNEXPECTED_MONGO_ERROR) \
    ERROR_CODE(5090, E_NON_INCREASING_INDEX_VERSION) \
    ERROR_CODE(6000, E_UNSORTED_DATA) \
    ERROR_CODE(7000, E_INVALID_USER_ARGUMENT) \
    ERROR_CODE(7001, E_INVALID_DECIMAL_STRING)   \
    ERROR_CODE(7002, E_INVALID_CHAR_IN_NAME) \
    ERROR_CODE(7003, E_NAME_TOO_LONG) \
    ERROR_CODE(7004, E_NO_STAGED_SEGMENTS)\
    ERROR_CODE(7005, E_COLUMN_NOT_FOUND) \
    ERROR_CODE(7006, E_SORT_ON_SPARSE) \
    ERROR_CODE(7007, E_EMPTY_NAME) \
    ERROR_CODE(8000, E_UNRECOGNISED_COLUMN_STATS_VERSION)   \
    ERROR_CODE(9000, E_DECODE_ERROR) \
    ERROR_CODE(9001, E_UNKNOWN_CODEC) \
    ERROR_CODE(9002, E_ZSDT_ENCODING) \
    ERROR_CODE(9003, E_LZ4_ENCODING)  \
    ERROR_CODE(9004, E_INPUT_TOO_LARGE) \
    ERROR_CODE(9005, E_ENCODING_VERSION_MISMATCH)

enum class ErrorCode : detail::BaseType {
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
    return static_cast<ErrorCategory>(static_cast<detail::BaseType>(code) / detail::error_category_scale);
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

// Utility to treat exceptions as data
struct Error {

    explicit Error(folly::Function<void(std::string)> raiser, std::string msg);
    void throw_error();

    folly::Function<void(std::string)> raiser_;
    std::string msg_;
};

using CheckOutcome = std::variant<Error, std::monostate>;

using InternalException = ArcticCategorizedException<ErrorCategory::INTERNAL>;
using SchemaException = ArcticCategorizedException<ErrorCategory::SCHEMA>;
using NormalizationException = ArcticCategorizedException<ErrorCategory::NORMALIZATION>;
using NoSuchVersionException = ArcticSpecificException<ErrorCode::E_NO_SUCH_VERSION>;
using StorageException = ArcticCategorizedException<ErrorCategory::STORAGE>;
using MissingDataException = ArcticCategorizedException<ErrorCategory::MISSING_DATA>;
using PermissionException = ArcticSpecificException<ErrorCode::E_PERMISSION>;
using LMDBMapFullException = ArcticSpecificException<ErrorCode::E_LMDB_MAP_FULL>;
using UnexpectedLMDBErrorException = ArcticSpecificException<ErrorCode::E_UNEXPECTED_LMDB_ERROR>;
using UnexpectedS3ErrorException = ArcticSpecificException<ErrorCode::E_UNEXPECTED_S3_ERROR>;
using S3RetryableException = ArcticSpecificException<ErrorCode::E_S3_RETRYABLE>;
using UnexpectedAzureException = ArcticSpecificException<ErrorCode::E_UNEXPECTED_AZURE_ERROR>;
using MongoOperationNoReplyException = ArcticSpecificException<ErrorCode::E_MONGO_BULK_OP_NO_REPLY>;
using UnexpectedMongoException = ArcticSpecificException<ErrorCode::E_UNEXPECTED_MONGO_ERROR>;
using NonIncreasingIndexVersionException = ArcticSpecificException<ErrorCode::E_NON_INCREASING_INDEX_VERSION>;
using SortingException = ArcticCategorizedException<ErrorCategory::SORTING>;
using UnsortedDataException = ArcticSpecificException<ErrorCode::E_UNSORTED_DATA>;
using UserInputException = ArcticCategorizedException<ErrorCategory::USER_INPUT>;
using CompatibilityException = ArcticCategorizedException<ErrorCategory::COMPATIBILITY>;
using CodecException = ArcticCategorizedException<ErrorCategory::CODEC>;
using AtomicOperationFailedException = ArcticSpecificException<ErrorCode::E_ATOMIC_OPERATION_FAILED>;
using UnsupportedAtomicOperationException = ArcticSpecificException<ErrorCode::E_UNSUPPORTED_ATOMIC_OPERATION>;
using NotImplementedException = ArcticSpecificException<ErrorCode::E_NOT_IMPLEMENTED_BY_STORAGE>;

template<ErrorCode error_code>
[[noreturn]] void throw_error(const std::string& msg) {
    throw ArcticCategorizedException<get_error_category(error_code)>(msg);
}

template<>
[[noreturn]] inline void throw_error<ErrorCode::E_PERMISSION>(const std::string& msg) {
    throw ArcticSpecificException<ErrorCode::E_PERMISSION>(msg);
}

template<>
[[noreturn]] inline void throw_error<ErrorCode::E_UNEXPECTED_LMDB_ERROR>(const std::string& msg) {
    throw ArcticSpecificException<ErrorCode::E_UNEXPECTED_LMDB_ERROR>(msg);
}

template<>
[[noreturn]] inline void throw_error<ErrorCode::E_LMDB_MAP_FULL>(const std::string& msg) {
    throw ArcticSpecificException<ErrorCode::E_LMDB_MAP_FULL>(msg);
}

template<>
[[noreturn]] inline void throw_error<ErrorCode::E_UNEXPECTED_S3_ERROR>(const std::string& msg) {
    throw ArcticSpecificException<ErrorCode::E_UNEXPECTED_S3_ERROR>(msg);
}

template<>
[[noreturn]] inline void throw_error<ErrorCode::E_S3_RETRYABLE>(const std::string& msg) {
    throw ArcticSpecificException<ErrorCode::E_S3_RETRYABLE>(msg);
}

template<>
[[noreturn]] inline void throw_error<ErrorCode::E_UNEXPECTED_AZURE_ERROR>(const std::string& msg) {
    throw ArcticSpecificException<ErrorCode::E_UNEXPECTED_AZURE_ERROR>(msg);
}

template<>
[[noreturn]] inline void throw_error<ErrorCode::E_MONGO_BULK_OP_NO_REPLY>(const std::string& msg) {
    throw ArcticSpecificException<ErrorCode::E_MONGO_BULK_OP_NO_REPLY>(msg);
}

template<>
[[noreturn]] inline void throw_error<ErrorCode::E_UNEXPECTED_MONGO_ERROR>(const std::string& msg) {
    throw ArcticSpecificException<ErrorCode::E_UNEXPECTED_MONGO_ERROR>(msg);
}

template<>
[[noreturn]] inline void throw_error<ErrorCode::E_NON_INCREASING_INDEX_VERSION>(const std::string& msg) {
    throw ArcticSpecificException<ErrorCode::E_NON_INCREASING_INDEX_VERSION>(msg);
}

template<>
[[noreturn]] inline void throw_error<ErrorCode::E_NO_SUCH_VERSION>(const std::string& msg) {
    throw ArcticSpecificException<ErrorCode::E_NO_SUCH_VERSION>(msg);
}

template<>
[[noreturn]] inline void throw_error<ErrorCode::E_UNSORTED_DATA>(const std::string& msg) {
    throw ArcticSpecificException<ErrorCode::E_UNSORTED_DATA>(msg);
}

template<>
[[noreturn]] inline void throw_error<ErrorCode::E_ATOMIC_OPERATION_FAILED>(const std::string& msg) {
    throw ArcticSpecificException<ErrorCode::E_ATOMIC_OPERATION_FAILED>(msg);
}

template<>
[[noreturn]] inline void throw_error<ErrorCode::E_UNSUPPORTED_ATOMIC_OPERATION>(const std::string& msg) {
    throw ArcticSpecificException<ErrorCode::E_UNSUPPORTED_ATOMIC_OPERATION>(msg);
}

template<>
[[noreturn]] inline void throw_error<ErrorCode::E_NOT_IMPLEMENTED_BY_STORAGE>(const std::string& msg) {
    throw ArcticSpecificException<ErrorCode::E_NOT_IMPLEMENTED_BY_STORAGE>(msg);
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
