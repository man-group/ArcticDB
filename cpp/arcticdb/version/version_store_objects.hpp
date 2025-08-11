/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/entity/atom_key.hpp>
#include <arcticdb/version/version_map_entry.hpp>

namespace arcticdb::version_store {

/**
 * Parameter object for VersionedEngine::delete_tree to control which checks to perform before physically deleting data.
 *
 * Currently, there's no code path that doesn't require checking snapshots, so no field for it.
 */
struct PreDeleteChecks {
    /**
     * Toggles all checks based on the MasterSnapshotMap.
     */
    bool snapshots = true;

    /**
     * Check if the key being deleted is still visible. Eager deletion after tombstoning can disable this.
     */
    bool version_visible = true;

    /**
     * Check the neighbouring versions for data blocks shared with the key(s) being deleted.
     * (At the time of writing, data can only be shared between neighbouring versions. Future dedup may break this?)
     */
    bool prev_version = true;
    bool next_version = true;

    /**
     * Explicitly specify some key(s) to check.
     * For example, certain callers to delete_tree() already performed some of the checks above, so can disable the
     * corresponding flags and put the results here.
     */
    std::unordered_set<IndexTypeKey> could_share_data {};

    LoadType calc_load_type() const {
        if (prev_version) return LoadType::ALL;
        if (version_visible | next_version) return LoadType::DOWNTO;
        return LoadType::NOT_LOADED;
    }
};

static const PreDeleteChecks default_pre_delete_checks;

/**
 * Output from tombstone_versions() with additional fields on top of PreDeleteChecks. Safe to be sliced to the latter.
 */
struct TombstoneVersionResult : PreDeleteChecks {
    explicit TombstoneVersionResult(bool entry_already_scanned, StreamId symbol = "") :
        PreDeleteChecks{true, entry_already_scanned, entry_already_scanned, entry_already_scanned, {}},
        symbol(symbol) {}

    /**
     * The index key that just got tombstoned, if any.
     */
    std::vector<IndexTypeKey> keys_to_delete;
    /**
     * If the tombstoning of this version caused the entire symbol to disappear.
     */
    bool no_undeleted_left = false;
    /**
     * The most recent version written to the version list
     */
     VersionId latest_version_ = 0;

    /**
     * The symbol that was tombstoned
     */
    StreamId symbol;
};

/**
 * Output from [batch_]get_latest_undeleted_version_and_next_version_id. Contains info needed for writing operations
 * that depend on a previous live version existing:
 *  - update (with upsert false)
 *  - append (with upsert false)
 *  - batch_append (doesn't currently support upsert)
 *  - write_metadata
 *  - batch_write_metadata
 */
struct UpdateInfo {
    /**
     * The index key of the latest live version, or std::nullopt if no versions are live
     */
    std::optional<AtomKey> previous_index_key_;
    /**
     * The smallest version ID not in use (live or deleted). Zero iff no versions exist for the specified symbol
     */
    VersionId next_version_id_;
};

} // namespace
