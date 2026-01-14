/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/entity/types.hpp>
#include <arcticdb/storage/store.hpp>

namespace arcticdb {

inline auto stream_id_prefix_matcher(const std::string& prefix) {
    return [&prefix](const StreamId& id) {
        return prefix.empty() || (std::holds_alternative<std::string>(id) &&
                                  std::get<std::string>(id).compare(0u, prefix.size(), prefix) == 0);
    };
}

std::vector<VariantKey> filter_keys_on_existence(
        const std::vector<VariantKey>& keys, const std::shared_ptr<Store>& store, bool pred
);
void filter_keys_on_existence(std::vector<AtomKey>& keys, const std::shared_ptr<Store>& store, bool pred);

AtomKey copy_index_key_recursively(
        const std::shared_ptr<Store>& source_store, const std::shared_ptr<Store>& target_store,
        const AtomKey& index_key, std::optional<VersionId> new_version_id
);

} // namespace arcticdb