/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/async/async_store.hpp>
#include <arcticdb/storage/key_segment_pair.hpp>
#include <arcticdb/toolbox/query_stats.hpp>
#include <arcticdb/version/de_dup_map.hpp>

namespace arcticdb::async {
DeDupLookupResult lookup_match_in_dedup_map(
        const std::shared_ptr<DeDupMap>& de_dup_map, storage::KeySegmentPair& key_seg
) {
    if (!de_dup_map) {
        ARCTICDB_DEBUG(log::version(), "No existing key with same contents: writing new object {}", key_seg.atom_key());
        return key_seg;
    }
    if (auto de_dup_key = de_dup_map->get_key_if_present(key_seg.atom_key())) {
        ARCTICDB_DEBUG(log::version(), "Found existing key with same contents: using existing object {}", *de_dup_key);
        if (query_stats::is_active()) {
            const uint64_t saved =
                    key_seg.segment_ptr() ? static_cast<uint64_t>(key_seg.segment_ptr()->calculate_size()) : 0;
            query_stats::record_dedup_lookup(/*hit=*/true, saved);
        }
        return *de_dup_key;
    }
    ARCTICDB_DEBUG(log::version(), "No existing key with same contents: writing new object {}", key_seg.atom_key());
    if (query_stats::is_active()) {
        query_stats::record_dedup_lookup(/*hit=*/false, 0);
    }
    return key_seg;
}
} // namespace arcticdb::async
