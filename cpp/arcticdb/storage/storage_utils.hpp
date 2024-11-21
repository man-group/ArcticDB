/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/entity/types.hpp>
#include <arcticdb/storage/store.hpp>

#include <ranges>

namespace rng = std::ranges;

namespace arcticdb {

inline auto stream_id_prefix_matcher(const std::string &prefix) {
    return [&prefix](const StreamId& id) {
        return prefix.empty() || (std::holds_alternative<std::string>(id) &&
        std::get<std::string>(id).compare(0u, prefix.size(), prefix) == 0); };
}

inline std::vector<VariantKey> filter_keys_on_existence(
        const std::vector<VariantKey>& keys,
        const std::shared_ptr<Store>& store,
        bool pred
        ){
    auto key_existence = folly::collect(store->batch_key_exists(keys)).get();
    std::vector<VariantKey> res;
    for (size_t i = 0; i != keys.size(); i++) {
        if (key_existence[i] == pred) {
            res.push_back(keys[i]);
        }
    }
    return res;
}

inline void filter_keys_on_existence(std::vector<AtomKey>& keys, const std::shared_ptr<Store>& store, bool pred) {
    std::vector<VariantKey> var_vector;
    var_vector.reserve(keys.size());
    rng::copy(keys, std::back_inserter(var_vector));

    auto key_existence = store->batch_key_exists(var_vector);

    auto keys_itr = keys.begin();
    for (size_t i = 0; i != var_vector.size(); i++) {
        bool resolved = key_existence[i].wait().value();
        if (resolved == pred) {
            *keys_itr = std::move(std::get<AtomKey>(var_vector[i]));
            ++keys_itr;
        }
    }
    keys.erase(keys_itr, keys.end());
}

AtomKey copy_index_key_recursively(
        const std::shared_ptr<Store>& source_store,
        const std::shared_ptr<Store>& target_store,
        const AtomKey& index_key,
        std::optional<VersionId> new_version_id);

}  //namespace arcticdb