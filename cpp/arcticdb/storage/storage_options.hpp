/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#pragma once

namespace arcticdb::storage {

/**
 * Applies to:
 * - LmdbStorage::do_remove
 * - InMemoryStore::remove_key_sync
 * - RemoveTask & RemoveBatchTask (passes value to worker threads)
 */
struct RemoveOpts {
    bool ignores_missing_key_ = false;
};

/**
 * ReadCompressedTask passes this to workers.
 */
struct ReadKeyOpts {
    /**
     * Applies to:
     * - generate_segments_from_keys (e.g. used by StreamReader)
     */
    bool ignores_missing_key_ = false;

    /**
     * Applies to:
     * - s3_storage-inl.cpp:do_read_impl()
     */
    bool dont_warn_about_missing_key = false;
};

/**
 * Applies to:
 * - LmdbStorage::do_update
 * - MongoStorage::do_update
 * - MemoryStorage::do_update
 */
struct UpdateOpts {
    bool upsert_ = false;
};

} // namespace arcticdb::storage
