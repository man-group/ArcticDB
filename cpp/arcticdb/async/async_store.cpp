/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/async/async_store.hpp>
#include <arcticdb/storage/key_segment_pair.hpp>
#include <arcticdb/version/de_dup_map.hpp>

namespace arcticdb::async {
DeDupLookupResult lookup_match_in_dedup_map(
    const std::shared_ptr<DeDupMap> &de_dup_map,
    storage::KeySegmentPair& key_seg) {
    std::optional<AtomKey> de_dup_key;
    if (!de_dup_map || !(de_dup_key = de_dup_map->get_key_if_present(key_seg.atom_key()))) {
        ARCTICDB_DEBUG(log::version(),
                       "No existing key with same contents: writing new object {}",
                       key_seg.atom_key());
        return key_seg;
    } else {
        ARCTICDB_DEBUG(log::version(),
                       "Found existing key with same contents: using existing object {}",
                       *de_dup_key);
        return *de_dup_key;
    }
}
}
