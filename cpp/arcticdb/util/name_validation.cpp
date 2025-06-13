/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/util/name_validation.hpp>

#include <arcticdb/entity/key.hpp>
#include <arcticdb/entity/types.hpp>
#include <arcticdb/util/configs_map.hpp>
#include <arcticdb/stream/index.hpp>
#include <arcticdb/storage/store.hpp>

namespace arcticdb {

// '*', '<' and '>' are problematic for S3
const auto UNSUPPORTED_S3_CHARS = std::set<char>{'*', '<', '>'};

[[nodiscard]] CheckOutcome verify_name(
        const std::string& name_type_for_error,
        const StringId& name,
        bool check_symbol_out_of_range = true,
        const std::set<char>& unsupported_chars = UNSUPPORTED_S3_CHARS,
        std::optional<char> unsupported_prefix = std::nullopt,
        std::optional<char> unsupported_suffix = std::nullopt) {
    if (name.empty()) {
        return Error{
            throw_error<ErrorCode::E_DESCRIPTOR_MISMATCH>,
            fmt::format("The {} cannot be an empty string.", name_type_for_error)
        };
    }

    if (name.size() > MAX_SYMBOL_LENGTH) {
        return Error{
            throw_error<ErrorCode::E_NAME_TOO_LONG>,
            fmt::format("The {} length exceeds the max supported length. {} length: {}, Max Supported Length: {}",
                        name_type_for_error,
                        name_type_for_error,
                        name.size(),
                        MAX_SYMBOL_LENGTH)
        };
    }

    for (unsigned char c: name) {
        if (check_symbol_out_of_range && (c < 32 || c > 126)) {
            return Error{throw_error<ErrorCode::E_INVALID_CHAR_IN_NAME>,
                         fmt::format(
                             "The {} can contain only valid ASCII chars in the range 32-126 inclusive. {}: {} BadChar: {}",
                             name_type_for_error,
                             name_type_for_error,
                             name,
                             c)
            };
        }

        if (unsupported_chars.find(c) != unsupported_chars.end()) {
            return Error{throw_error<ErrorCode::E_INVALID_CHAR_IN_NAME>, fmt::format(
                "The {} contains unsupported chars. {}: {} BadChar: {}",
                name_type_for_error,
                name_type_for_error,
                name,
                c)
            };
        }
    }

    if (unsupported_prefix.has_value() && name[0] == *unsupported_prefix) {
        return Error{throw_error<ErrorCode::E_INVALID_CHAR_IN_NAME>, fmt::format(
            "The {} starts with an unsupported prefix. {}: {} Unsupported prefix: {} ",
            name_type_for_error,
            name_type_for_error,
            name,
            *unsupported_prefix)
        };
    }

    if (unsupported_suffix.has_value() && name[name.size() - 1] == *unsupported_suffix) {
        return Error{throw_error<ErrorCode::E_INVALID_CHAR_IN_NAME>, fmt::format(
            "The {} ends with an unsupported suffix. {}: {} Unsupported suffix: {} ",
            name_type_for_error,
            name_type_for_error,
            name,
            *unsupported_suffix)
        };
    }

    return std::monostate{};
}

CheckOutcome verify_symbol_key(const StreamId& symbol_key) {
    if (ConfigsMap::instance()->get_int("VersionStore.NoStrictSymbolCheck")) {
        ARCTICDB_DEBUG(log::version(),
                       "Key with stream id {} will not be strictly checked because VersionStore.NoStrictSymbolCheck variable is set to 1.",
                       symbol_key);
        return std::monostate{};
    }

    return util::variant_match(
            symbol_key,
            [](const NumericId &num_symbol_key) -> CheckOutcome {
                (void) num_symbol_key; // Suppresses -Wunused-parameter
                ARCTICDB_DEBUG(log::version(), "Nothing to verify in stream id {} as it contains a NumericId.",
                               num_symbol_key);
                return std::monostate{};
            },
            [](const StringId &str_symbol_key) -> CheckOutcome {
                return verify_name("symbol key", str_symbol_key);
            }
    );
}

// Library names starting with "/" fail if storage is LMDB and library parts starting with "/" will fail in Mongo
constexpr auto UNSUPPORTED_LMDB_MONGO_PREFIX = '/';

void verify_library_path(const StringId& library_path, char delim) {
    CheckOutcome  res = verify_name("library name", library_path, false, {}, {}, delim);
    if (std::holds_alternative<Error>(res)) {
        std::get<Error>(res).throw_error();
    }
}

void verify_library_path_part(const std::string& library_part, char delim) {
    if (library_part.empty()) {
        user_input::raise<ErrorCode::E_INVALID_CHAR_IN_NAME>(
                "Library name has an empty part. Parts are separated by delimiter: '{}'. This is currently not supported.",
                delim
        );
    }
    if (library_part[0] == UNSUPPORTED_LMDB_MONGO_PREFIX) {
        user_input::raise<ErrorCode::E_INVALID_CHAR_IN_NAME>(
                "Library name part starts with an invalid character. This is currently not supported. Library Name Part: '{}', Bad prefix: {}",
                library_part,
                UNSUPPORTED_LMDB_MONGO_PREFIX
        );
    }
}

void verify_library_path_on_write(const Store* store, const StringId& library_path) {
    CheckOutcome res = verify_name("library name", library_path, true, UNSUPPORTED_S3_CHARS);
    if (std::holds_alternative<Error>(res)) {
        std::get<Error>(res).throw_error();
    }
    user_input::check<ErrorCode::E_INVALID_CHAR_IN_NAME>(
            store->is_path_valid(library_path),
            "The library name contains unsupported chars. Library Name: {}",
            library_path
    );
}

}