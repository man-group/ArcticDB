/*
* Copyright 2023 Man Group Operations Ltd.
* NO WARRANTY, EXPRESSED OR IMPLIED
*/

#pragma once

namespace arcticdb::storage::op_ctx {

/**
 * Instead of plumbing optional parameters all the way from high-level logic via Library via Storages via Storage to
 * implementations, these thread-local wrappers are used to pass them statically.
 *
 * Each instance will override the value in the thread-local and restore the original when destroyed.
 */
template<typename Opts>
class OpContext {
    static thread_local Opts active;
    Opts saved;
public:
    OpContext(): saved(active) {
    }

    OpContext(Opts overrides): saved(active) {
        active = overrides;
    }

    ~OpContext() {
        active = saved;
    }

    static Opts& get() { return active; }
    Opts* operator->() { return &active; }
};

// Remember to specialise OpContext::active for each of below in the cpp

/**
 * Applies to:
 * - LmdbStorage::do_remove
 * - InMemoryStore::remove_key_sync
 * - RemoveTask & RemoveBatchTask (passes value to worker threads)
 */
struct RemoveOpts {
    bool ignores_missing_key = false;
};

/**
 * ReadCompressedTask passes this to workers.
 */
struct ReadKeyOpts {
    /**
     * Applies to:
     * - generate_segments_from_keys (e.g. used by StreamReader)
     */
    bool ignores_missing_key = false;

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

}
