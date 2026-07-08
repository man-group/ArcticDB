/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/util/name_validation.hpp>

#include <arcticdb/entity/types.hpp>
#include <arcticdb/util/configs_map.hpp>
#include <arcticdb/storage/storage.hpp>
#include <arcticdb/storage/store.hpp>

namespace arcticdb {

[[nodiscard]] CheckOutcome verify_name(
        const std::string& name_type_for_error, const StringId& name, bool check_symbol_out_of_range,
        const std::set<char>& unsupported_chars, std::optional<char> unsupported_suffix = std::nullopt
) {
    if (name.empty()) {
        return Error{
                throw_error<ErrorCode::E_EMPTY_NAME>,
                fmt::format("The {} cannot be an empty string.", name_type_for_error)
        };
    }

    if (name.size() > MAX_SYMBOL_LENGTH) {
        return Error{
                throw_error<ErrorCode::E_NAME_TOO_LONG>,
                fmt::format(
                        "{} {} length exceeds the max supported length. {} length: {}, Max Supported Length: {}",
                        name_type_for_error,
                        name,
                        name_type_for_error,
                        name.size(),
                        MAX_SYMBOL_LENGTH
                )
        };
    }

    for (unsigned char c : name) {
        if (check_symbol_out_of_range && (c < 32 || c > 126)) {
            return Error{
                    throw_error<ErrorCode::E_INVALID_CHAR_IN_NAME>,
                    fmt::format(
                            "The {} can contain only valid ASCII chars in the range 32-126 inclusive. {}: {} BadChar: "
                            "{}",
                            name_type_for_error,
                            name_type_for_error,
                            name,
                            c
                    )
            };
        }

        if (unsupported_chars.find(c) != unsupported_chars.end()) {
            return Error{
                    throw_error<ErrorCode::E_INVALID_CHAR_IN_NAME>,
                    fmt::format(
                            "The {} contains unsupported chars. {}: {} BadChar: {}",
                            name_type_for_error,
                            name_type_for_error,
                            name,
                            c
                    )
            };
        }
    }

    if (unsupported_suffix.has_value() && name[name.size() - 1] == *unsupported_suffix) {
        return Error{
                throw_error<ErrorCode::E_INVALID_CHAR_IN_NAME>,
                fmt::format(
                        "The {} ends with an unsupported suffix. {}: {} Unsupported suffix: {} ",
                        name_type_for_error,
                        name_type_for_error,
                        name,
                        *unsupported_suffix
                )
        };
    }

    return std::monostate{};
}

[[nodiscard]] CheckOutcome verify_name_for_backend(
        const std::string& name_type_for_error, const VariantId& id, const std::shared_ptr<Store>& store
) {
    if (ConfigsMap::instance()->get_int("VersionStore.NoStrictSymbolCheck")) {
        ARCTICDB_DEBUG(
                log::version(),
                "Key with stream id {} will not be strictly checked because VersionStore.NoStrictSymbolCheck variable "
                "is set to 1.",
                id
        );
        return std::monostate{};
    }

    return util::variant_match(
            id,
            [&](const StringId& name) -> CheckOutcome {
                return verify_name(name_type_for_error, name, true, store->unsupported_symbol_chars());
            },
            [](const auto&) -> CheckOutcome { return std::monostate{}; }
    );
}

CheckOutcome verify_symbol_key(const StreamId& symbol_key, const std::shared_ptr<Store>& store) {
    return verify_name_for_backend("symbol key", symbol_key, store);
}

CheckOutcome verify_snapshot_id(const SnapshotId& snapshot_id, const std::shared_ptr<Store>& store) {
    return verify_name_for_backend("snapshot name", snapshot_id, store);
}

// Library names starting with "/" fail if storage is LMDB and library parts starting with "/" will fail in Mongo
constexpr auto UNSUPPORTED_LMDB_MONGO_PREFIX = '/';

void verify_library_path(const StringId& library_path, char delim) {
    CheckOutcome res = verify_name("library name", library_path, false, {}, delim);
    if (std::holds_alternative<Error>(res)) {
        std::get<Error>(res).throw_error();
    }
}

void verify_library_path_part(const std::string& library_part, char delim) {
    if (library_part.empty()) {
        user_input::raise<ErrorCode::E_INVALID_CHAR_IN_NAME>(
                "Library name has an empty part. Parts are separated by delimiter: '{}'. This is currently not "
                "supported.",
                delim
        );
    }
    if (library_part[0] == UNSUPPORTED_LMDB_MONGO_PREFIX) {
        user_input::raise<ErrorCode::E_INVALID_CHAR_IN_NAME>(
                "Library name part starts with an invalid character. This is currently not supported. Library Name "
                "Part: '{}', Bad prefix: {}",
                library_part,
                UNSUPPORTED_LMDB_MONGO_PREFIX
        );
    }
}

void verify_library_path_on_write(const Store* store, const StringId& library_path) {
    CheckOutcome res = verify_name("library name", library_path, true, store->unsupported_library_chars());
    if (std::holds_alternative<Error>(res)) {
        std::get<Error>(res).throw_error();
    }
    if (auto unsupported = store->verify_library_suffix(library_path)) {
        user_input::raise<ErrorCode::E_INVALID_CHAR_IN_NAME>(
                "The library name's suffix contains character unsupported by storage backend {}. Library Name: {} "
                "BadChar: {}",
                store->name(),
                library_path,
                *unsupported
        );
    }
}

} // namespace arcticdb