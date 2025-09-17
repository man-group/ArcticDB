/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <string_view>
#include <optional>
#include <arcticdb/storage/library_path.hpp>
#include <fmt/format.h>
#include <arcticdb/entity/variant_key.hpp>
#include <arcticdb/entity/key.hpp>
#include <arcticdb/entity/serialized_key.hpp>

namespace arcticdb::storage::object_store_utils {

struct FlatBucketizer {
    static std::string bucketize(const std::string& root_folder, const VariantKey&) { return root_folder; }

    static size_t bucketize_length(KeyType) { return 0; }
};

inline std::string get_root_folder(const LibraryPath& library_path) { return library_path.to_delim_path('/'); }

inline auto object_name_from_key(const VariantKey& key) { return to_tokenized_key(key); }

inline auto object_path(std::string_view folder, const VariantKey& key) {
    return fmt::format("{}/{}", folder, object_name_from_key(key));
}

inline auto key_type_folder(const std::string& root_folder, KeyType key_type) {
    return fmt::format("{}/{}", root_folder, key_type_long_name(key_type));
}

} // namespace arcticdb::storage::object_store_utils
