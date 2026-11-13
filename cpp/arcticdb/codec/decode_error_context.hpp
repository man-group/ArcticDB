/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/entity/variant_key.hpp>
#include <arcticdb/util/error_code.hpp>

#include <fmt/format.h>
#include <folly/ExceptionWrapper.h>

#include <cstddef>
#include <string>
#include <string_view>
#include <utility>

namespace arcticdb {

[[noreturn]] void rethrow_with_context(const DataIntegrityException& e, std::string_view context);
[[noreturn]] void rethrow_with_context(const CodecException& e, std::string_view context);

/// Runs `decode()`. If it fails with a DataIntegrityException or CodecException, rethrows the same type with
/// ", in <describe()>" appended, so each layer of the decode adds where it was. `describe` only runs on failure.
template<typename Decode, typename Describe>
decltype(auto) with_decode_context(Decode&& decode, const Describe& describe) {
    try {
        return std::forward<Decode>(decode)();
    } catch (const DataIntegrityException& e) {
        rethrow_with_context(e, describe());
    } catch (const CodecException& e) {
        rethrow_with_context(e, describe());
    }
}

/// with_decode_context for the decode of one column of a segment.
template<typename Decode>
decltype(auto) with_column_decode_context(std::string_view column_name, std::size_t field_index, Decode&& decode) {
    return with_decode_context(std::forward<Decode>(decode), [column_name, field_index] {
        return fmt::format("column '{}' (field {})", column_name, field_index);
    });
}

/// with_decode_context for the decode of a segment's string pool.
template<typename Decode>
decltype(auto) with_string_pool_decode_context(Decode&& decode) {
    return with_decode_context(std::forward<Decode>(decode), [] { return std::string{"string pool"}; });
}

/// "key <key> (symbol '<id>', version <n>, storage object name '<name>')", and what to do next. A hash mismatch always
/// means the bytes changed; a codec error means so only if this version can read the object's format.
std::string describe_key_for_decode_error(const entity::VariantKey& key, bool is_hash_mismatch);

/// with_decode_context for the decode of one storage object.
template<typename Decode>
decltype(auto) with_key_decode_context(const entity::VariantKey& key, Decode&& decode) {
    try {
        return std::forward<Decode>(decode)();
    } catch (const DataIntegrityException& e) {
        rethrow_with_context(e, describe_key_for_decode_error(key, /*is_hash_mismatch=*/true));
    } catch (const CodecException& e) {
        rethrow_with_context(e, describe_key_for_decode_error(key, /*is_hash_mismatch=*/false));
    }
}

/// Whether `error` is a decode error (DataIntegrityException or CodecException).
bool is_decode_error(const folly::exception_wrapper& error);

/// Rethrows `error` if it is a decode error, so that it keeps its type and the key context it already carries.
inline void rethrow_if_decode_error(const folly::exception_wrapper& error) {
    if (is_decode_error(error))
        error.throw_exception();
}

/// For handlers that report a failed read as missing data: a DataIntegrityException means the data is damaged, not
/// missing, so it is rethrown unchanged. (Codec errors keep their old conversion, which existing callers rely on.)
inline void rethrow_if_data_integrity_error(const folly::exception_wrapper& error) {
    if (error.is_compatible_with<DataIntegrityException>())
        error.throw_exception();
}

} // namespace arcticdb
