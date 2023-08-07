/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#pragma once

namespace arcticdb {

std::deque<AtomKey> backwards_compat_delete_all_versions(
    const std::shared_ptr<Store>& store,
    std::shared_ptr<VersionMap>& version_map,
    const StreamId& stream_id
    ) {
    std::deque<AtomKey> output;
    auto entry = version_map->check_reload(store, stream_id, LoadParameter{LoadType::LOAD_ALL}, __FUNCTION__);
    auto indexes = entry->get_indexes(false);
    output.assign(std::begin(indexes), std::end(indexes));

    if (auto ref_key = get_symbol_ref_key(store, stream_id); ref_key) {
        store->remove_key(ref_key.value()).wait();
    }
    version_map->remove_entry_version_keys(store, entry, stream_id);
    entry->clear();
    return output;
}

std::vector<AtomKey> backwards_compat_write_and_prune_previous(std::shared_ptr<Store>& store, std::shared_ptr<VersionMap>& version_map, const AtomKey &key) {
    log::version().debug("Version map pruning previous versions for stream {}", key.id());

    std::vector<AtomKey> output;
    auto entry = version_map->check_reload(store, key.id(), LoadParameter{LoadType::LOAD_ALL},  __FUNCTION__);

    auto old_entry = *entry;
    entry->clear();
    version_map->do_write(store, key, entry);
    version_map->remove_entry_version_keys(store, old_entry, key.id());
    output = old_entry.get_indexes(false);

    if(version_map->log_changes())
        log_write(store, key.id(), key.version_id());

    return output;
}
}