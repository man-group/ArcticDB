/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#pragma once
#include <vector>
#include <unordered_map>
#include <optional>
#include <algorithm>
#include <arcticdb/entity/atom_key.hpp>

namespace arcticdb {

using namespace arcticdb::entity;

class DeDupMap {
public:
    DeDupMap() = default;
    ARCTICDB_NO_COPY(DeDupMap);

    [[nodiscard]] std::optional<AtomKey> get_key_if_present(const AtomKey &key) const {
        const auto de_dup_candidates = de_dup_map_.find(key.content_hash());
        if (de_dup_candidates == de_dup_map_.end()) {
            return std::nullopt;
        }
        // Just content hash matching isn't enough, start and end index also need to be matched
        // which uniquely identifies the position of the segment
        const auto key_iterator = std::ranges::find_if(de_dup_candidates->second,
                                                 [&](const auto &k) {
                                                     return k.start_index() == key.start_index() &&
                                                            k.end_index() == key.end_index();
                                                 });
        return key_iterator == de_dup_candidates->second.end() ? std::nullopt : std::optional{*key_iterator};
    }

    void insert_key(const AtomKey &key) {
        if (const auto it = de_dup_map_.find(key.content_hash()); it != de_dup_map_.end()) {
            it->second.push_back(key);
        } else {
            de_dup_map_.emplace(key.content_hash(), std::vector{key});
        }
    }

private:
    std::unordered_map<ContentHash, std::vector<AtomKey>> de_dup_map_;
};

}