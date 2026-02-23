/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

/*
 * version_map.hpp contains VersionMap which at it's core is a map of {Stream_id: VersionMapEntry} (see
 * version_map_entry.hpp) (see VersionMapImpl for details)
 *
 */
#pragma once

#include <shared_mutex>
#include <unordered_set>
#include <map>
#include <deque>
#include <gtest/gtest_prod.h>

#include <arcticdb/entity/types.hpp>
#include <arcticdb/entity/atom_key.hpp>
#include <arcticdb/stream/index.hpp>
#include <arcticdb/stream/stream_sink.hpp>
#include <arcticdb/stream/stream_writer.hpp>
#include <arcticdb/stream/stream_source.hpp>
#include <arcticdb/stream/stream_reader.hpp>
#include <arcticdb/python/python_utils.hpp>
#include <arcticdb/util/configs_map.hpp>
#include <arcticdb/storage/store.hpp>
#include <arcticdb/storage/storage.hpp>
#include <arcticdb/util/constants.hpp>
#include <arcticdb/util/key_utils.hpp>
#include <arcticdb/version/version_map_entry.hpp>
#include <arcticdb/async/batch_read_args.hpp>
#include <arcticdb/version/version_log.hpp>
#include <arcticdb/version/version_utils.hpp>
#include <arcticdb/util/lock_table.hpp>

namespace arcticdb {

enum class CacheStatus {
    VALID,                 // Cache covers the load strategy
    EXPIRED_TIME,          // Time-based expiry
    NEEDS_OLDER,           // Cache has data but needs older versions (extend)
    NEEDS_NEWER,           // Cache has data but needs newer versions (prepend)
    NEEDS_RELOAD,          // Cache requires full reload
    NOT_FOUND              // No cache entry at all
};

template<class Clock = util::SysClock>
class VersionMapImpl {
    /*
     * VersionMap at it's core is an in-memory map of {StreamId: VersionMapEntry}.
     *
     * It contains utility functions for working with retrieving and storing in-memory version state from
     * the structure in storage, which are used by PythonVersionStore to abstract all the versioning related operations.
     *
     * The core functionality here is functions like:
     *
     * get_latest_version, get_all_versions, get_specific version, write_version and delete_version.
     *
     * This allows us to move all the implementation details of how versions live in memory and storage outside
     * PythonVersionStore which is just an orchestrator.
     *
     * On Disk Structure for symbol 'sym' written like:
     * lib.write('sym', 1)  -> v0
     * lib.write('sym', 2)  -> v1
     * lib.write('sym', 3)  -> v2
     *
     * (Notation: KeyType: [ key | segment])
     * Version_refs : ['sym'| [v2|i2]]
     *                      |
     *                      |
     * Version Keys: ['sym',v2| [i2|v1]] -- ['sym', v1| [i1|i0]] -- ['sym', v0 |[i0]]
     *
     * When a version is tombstoned, e.g. lib.write('a', 1), lib.write('a', 2) with pruning will tombstone the first
     * version which creates a new version key on storage in the same order of the timeline of operations and that key
     * will have a tombstone key type in its segment instead of an index key, and a version key pointing to the next
     * version key.
     *
     * Note that there is no TOMBSTONE key type in the storage, it's just an indicator inside a version key that
     * tells us that this version has been deleted or pruned with tombstones enabled.
     *
     * . On storage this looks like:
     *
     * Version_refs : ['sym'| [v1|i1]]
     *     |
     *     |
     * Version Keys: ['sym',v1| [i1|v0]] (ts2) -- ['sym',v0| [v0|t0]] (ts1) --  ['sym',v2| [i0]] (ts0)
     *
     * We also have methods to compact this linked list on storage if it becomes too large on disk, which would lead
     * us to do multiple reads from storage if not compacted.
     *
     * FALLBACK TO ITERATION
     * We also have an alternative method to fetch all version keys which is to fall back to iterating the storage
     * to basically just fetch all relevant key types, which is useful in case we have consistency issues in
     * the ref keys.
     *
     * This class also has utils to check and fix the ref key structure using the iteration method from storage.
     *
     * Note that VERSION_JOURNAL is a key type which is only there for backwards compatibility reasons and is never
     * used in for new libraries.
     *
     * CACHING in VERSION MAP
     * when someone requests the latest version, we do have a grace period of DEFAULT_RELOAD_INTERVAL where we will
     * just use the data in the in memory map if it exists rather than reading the ref key from the storage.
     *
     */

    /**
     * If we one day replace the String in StreamId with something cheap to copy again, we can easily drop the & here.
     *
     * Methods already declared with const& were not touched during this change.
     */
    using MapType = std::map<StreamId, std::shared_ptr<VersionMapEntry>>;

    static constexpr uint64_t DEFAULT_CLOCK_UNSYNC_TOLERANCE = ONE_MILLISECOND * 200;
    static constexpr uint64_t DEFAULT_RELOAD_INTERVAL = ONE_SECOND * 2;
    MapType map_;
    bool validate_ = false;
    bool log_changes_ = false;
    std::optional<timestamp> reload_interval_;
    mutable std::mutex map_mutex_;
    std::shared_ptr<LockTable> lock_table_ = std::make_shared<LockTable>();

  public:
    VersionMapImpl() = default;

    ARCTICDB_NO_MOVE_OR_COPY(VersionMapImpl)

    void set_validate(bool value) { validate_ = value; }

    void set_log_changes(bool value) { log_changes_ = value; }

    bool log_changes() const { return log_changes_; }

    void set_reload_interval(timestamp interval) { reload_interval_ = std::make_optional<timestamp>(interval); }

    bool validate() const { return validate_; }

    void follow_version_chain(
            const std::shared_ptr<Store>& store, const VersionMapEntry& ref_entry,
            const std::shared_ptr<VersionMapEntry>& entry, const LoadStrategy& load_strategy
    ) const {
        auto next_key = ref_entry.head_;
        entry->head_ = ref_entry.head_;

        std::optional<VersionId> latest_version;
        LoadProgress load_progress;
        util::check(ref_entry.keys_.size() >= 2, "Invalid empty ref entry");
        std::optional<AtomKey> cached_penultimate_index;
        if (ref_entry.keys_.size() == 3) {
            util::check(
                    is_index_or_tombstone(ref_entry.keys_[1]),
                    "Expected index key in as second item in 3-item ref key, got {}",
                    ref_entry.keys_[1]
            );
            cached_penultimate_index = ref_entry.keys_[1];
        }

        if (key_exists_in_ref_entry(load_strategy, ref_entry, cached_penultimate_index)) {
            load_progress = ref_entry.load_progress_;
            entry->keys_.push_back(ref_entry.keys_[0]);
            if (cached_penultimate_index)
                entry->keys_.push_back(*cached_penultimate_index);
            // We are setting the next_version_key to null as we don't have information
            // about the next key in a chain. It will be loaded through the last key
            // either in keys or through head.
            entry->next_version_key_.reset();
        } else {
            do {
                ARCTICDB_DEBUG(log::version(), "Loading version key {}", next_key.value());
                auto [key, seg] = store->read_sync(next_key.value());
                next_key = read_segment_with_keys(seg, entry, load_progress);
                set_latest_version(entry, latest_version);
            } while (next_key && continue_when_loading_version(load_strategy, load_progress, latest_version) &&
                     continue_when_loading_from_time(load_strategy, load_progress) &&
                     continue_when_loading_latest(load_strategy, entry) &&
                     continue_when_loading_undeleted(load_strategy, entry, load_progress));
            entry->next_version_key_ = std::move(next_key);
        }
        entry->load_progress_ = std::move(load_progress);
    }

    void load_via_ref_key(
            std::shared_ptr<Store> store, const StreamId& stream_id, const LoadStrategy& load_strategy,
            const std::shared_ptr<VersionMapEntry>& entry
    ) {
        load_strategy.validate();
        static const auto max_trial_config = ConfigsMap::instance()->get_int("VersionMap.MaxReadRefTrials", 2);
        auto max_trials = max_trial_config;
        while (true) {
            try {
                VersionMapEntry ref_entry;
                read_symbol_ref(store, stream_id, ref_entry);
                if (ref_entry.empty())
                    return;

                follow_version_chain(store, ref_entry, entry, load_strategy);
                break;
            } catch (const std::exception& err) {
                if (--max_trials <= 0) {
                    throw;
                }
                // We retry to read via ref key because it could have been modified by someone else (e.g. compaction)
                log::version().warn(
                        "Loading versions from storage via ref key failed with error: {} for stream {}. Retrying",
                        err.what(),
                        stream_id
                );
                entry->head_.reset();
                entry->keys_.clear();
                continue;
            }
        }
        if (validate_)
            entry->validate();
    }

    void flush() {
        std::lock_guard lock(map_mutex_);
        map_.clear();
    }

    void load_via_iteration(
            std::shared_ptr<Store> store, const StreamId& stream_id, std::shared_ptr<VersionMapEntry>& entry,
            bool use_index_keys_for_iteration = false
    ) const {
        ARCTICDB_DEBUG(log::version(), "Attempting to iterate version keys");
        auto match_stream_id = [&stream_id](const AtomKey& k) { return k.id() == stream_id; };
        entry = build_version_map_entry_with_predicate_iteration(
                store,
                match_stream_id,
                stream_id,
                use_index_keys_for_iteration ? std::vector<KeyType>{KeyType::TABLE_INDEX, KeyType::MULTI_KEY}
                                             : std::vector<KeyType>{KeyType::VERSION},
                !use_index_keys_for_iteration
        );

        if (validate_)
            entry->validate();
    }

    // prevent_non_increasing_version_id should be set false only:
    // - testing purposes i.e. setting up a library with a broken version chain
    // - in a background job in cases where we want to explicitly do this e.g. to replicate a divergent version chain
    void write_version(
            std::shared_ptr<Store> store, const AtomKey& key, const std::optional<AtomKey>& previous_key,
            const bool prevent_non_increasing_version_id = true
    ) {
        LoadStrategy load_param{LoadType::LATEST, LoadObjective::INCLUDE_DELETED};
        auto entry = check_reload(store, key.id(), load_param, __FUNCTION__);

        do_write(store, key, entry, prevent_non_increasing_version_id);
        write_symbol_ref(store, key, previous_key, entry->head_.value());
        if (validate_)
            entry->validate();
        if (log_changes_)
            log_write(store, key.id(), key.version_id());
    }

    /**
     * Tombstone all non-deleted versions of the given stream and do the related housekeeping.
     * @param first_key_to_tombstone The first key in the version chain that should be tombstoned. When empty
     * then the first index key onwards is tombstoned, so the whole chain is tombstoned.
     */
    std::pair<VersionId, std::vector<AtomKey>> tombstone_from_key_or_all(
            std::shared_ptr<Store> store, const StreamId& stream_id,
            std::optional<AtomKey> first_key_to_tombstone = std::nullopt,
            std::optional<std::shared_ptr<VersionMapEntry>> cached_entry = std::nullopt
    ) {
        std::shared_ptr<VersionMapEntry> entry;
        if (cached_entry) {
            entry = cached_entry.value();
        } else {
            entry = check_reload(
                    store, stream_id, LoadStrategy{LoadType::ALL, LoadObjective::UNDELETED_ONLY}, __FUNCTION__
            );
        }
        auto output = tombstone_from_key_or_all_internal(store, stream_id, first_key_to_tombstone, entry);

        if (validate_)
            entry->validate();

        if (entry->head_)
            write_symbol_ref(store, *entry->keys_.cbegin(), std::nullopt, entry->head_.value());

        return output;
    }

    std::string dump_entry(const std::shared_ptr<Store>& store, const StreamId& stream_id) {
        const auto entry = check_reload(
                store, stream_id, LoadStrategy{LoadType::ALL, LoadObjective::INCLUDE_DELETED}, __FUNCTION__
        );
        return entry->dump();
    }

    std::vector<AtomKey> write_and_prune_previous(
            std::shared_ptr<Store> store, const AtomKey& key, const std::optional<AtomKey>& previous_key
    ) {
        ARCTICDB_DEBUG(log::version(), "Version map pruning previous versions for stream {}", key.id());
        auto entry =
                check_reload(store, key.id(), LoadStrategy{LoadType::ALL, LoadObjective::UNDELETED_ONLY}, __FUNCTION__);
        auto [_, result] = tombstone_from_key_or_all_internal(store, key.id(), previous_key, entry, false);

        std::vector<AtomKey> keys_to_write;
        std::optional<AtomKey> tombstone_all_key;
        keys_to_write.push_back(key);
        if (!result.empty()) {
            auto first_key_to_tombstone = previous_key ? previous_key : entry->get_first_index(false).first;
            tombstone_all_key = get_tombstone_all_key(first_key_to_tombstone.value(), store->current_timestamp());
            entry->try_set_tombstone_all(tombstone_all_key.value());
            keys_to_write.push_back(tombstone_all_key.value());
        }

        auto previous_index = do_write(store, key.version_id(), key.id(), std::span{keys_to_write}, entry);
        write_symbol_ref(store, *entry->keys_.cbegin(), previous_index, entry->head_.value());

        maybe_invalidate_cached_undeleted(*entry);
        if (log_changes_) {
            if (tombstone_all_key)
                log_tombstone_all(store, tombstone_all_key.value().id(), tombstone_all_key.value().version_id());
            log_write(store, key.id(), key.version_id());
        }

        return result;
    }

    std::pair<VersionId, std::deque<AtomKey>> delete_all_versions(
            std::shared_ptr<Store> store, const StreamId& stream_id
    ) {
        ARCTICDB_DEBUG(log::version(), "Version map deleting all versions for stream {}", stream_id);
        std::deque<AtomKey> output;
        auto [version_id, index_keys] = tombstone_from_key_or_all(store, stream_id);
        output.assign(std::begin(index_keys), std::end(index_keys));
        return {version_id, std::move(output)};
    }

    bool requires_compaction(const std::shared_ptr<VersionMapEntry>& entry) const {
        int64_t num_blocks = std::count_if(entry->keys_.cbegin(), entry->keys_.cend(), [](const AtomKey& key) {
            return key.type() == KeyType::VERSION;
        });

        static const auto max_blocks = ConfigsMap::instance()->get_int("VersionMap.MaxVersionBlocks", 5);
        if (num_blocks < max_blocks) {
            ARCTICDB_DEBUG(
                    log::version(),
                    "Not compacting as number of blocks {} is less than the permitted {}",
                    num_blocks,
                    max_blocks
            );
            return false;
        } else {
            return true;
        }
    }

    void compact_and_remove_deleted_indexes(std::shared_ptr<Store> store, const StreamId& stream_id) {
        // This method has no API, and is not tested in the rapidcheck tests, but could easily be enabled there.
        // It compacts the version map but skips any keys which have been deleted (to free up space).
        ARCTICDB_DEBUG(log::version(), "Version map compacting versions for stream {}", stream_id);
        auto entry = check_reload(
                store, stream_id, LoadStrategy{LoadType::ALL, LoadObjective::INCLUDE_DELETED}, __FUNCTION__
        );
        if (!requires_compaction(entry))
            return;

        auto latest_version = std::find_if(std::begin(entry->keys_), std::end(entry->keys_), [](const auto& key) {
            return is_index_key_type(key.type());
        });
        const auto new_version_id = latest_version->version_id();

        auto new_entry = std::make_shared<VersionMapEntry>();
        new_entry->keys_.push_front(*latest_version);

        if (const auto first_is_tombstone = entry->get_tombstone(new_version_id); first_is_tombstone)
            new_entry->keys_.emplace_front(std::move(*first_is_tombstone));

        std::advance(latest_version, 1);

        for (const auto& key : std::ranges::subrange{latest_version, entry->keys_.end()}) {
            if (is_index_key_type(key.type())) {
                const auto tombstone = entry->get_tombstone(key.version_id());
                if (tombstone) {
                    if (!store->key_exists(key).get())
                        ARCTICDB_DEBUG(log::version(), "Removing deleted key {}", key);
                    else {
                        if (tombstone->type() == KeyType::TOMBSTONE_ALL)
                            new_entry->try_set_tombstone_all(*tombstone);
                        else
                            new_entry->tombstones_.insert(std::make_pair(key.version_id(), *tombstone));

                        new_entry->keys_.push_back(key);
                    }
                } else {
                    new_entry->keys_.push_back(key);
                }
            }
        }
        new_entry->head_ = write_entry_to_storage(store, stream_id, new_version_id, new_entry);
        remove_entry_version_keys(store, entry, stream_id);
        if (validate_)
            new_entry->validate();

        std::swap(*entry, *new_entry);
    }

    VariantKey journal_key(
            std::shared_ptr<Store> store, const VersionId& version_id, const StreamId& stream_id,
            std::span<const AtomKey> keys, std::optional<AtomKey> prev_journal_key
    ) {
        ARCTICDB_SAMPLE(WriteJournalEntry, 0)
        ARCTICDB_DEBUG(log::version(), "Version map writing version for keys {}", keys);

        VariantKey journal_key;
        IndexAggregator<RowCountIndex> journal_agg(
                stream_id,
                [&store, &journal_key, &version_id, &stream_id](auto&& segment) {
                    const PartialKey pk{
                            KeyType::VERSION,
                            version_id,
                            stream_id,
                            IndexValue(NumericIndex{0}),
                            IndexValue(NumericIndex{0})
                    };

                    journal_key = store->write_sync(pk, std::forward<decltype(segment)>(segment));
                }
        );

        for (const auto& key : keys) {
            journal_agg.add_key(key);
        }
        if (prev_journal_key)
            journal_agg.add_key(*prev_journal_key);

        journal_agg.finalize();
        return journal_key;
    }

    AtomKey update_version_key(
            std::shared_ptr<Store> store, const VariantKey& version_key, const std::vector<AtomKey>& index_keys,
            const StreamId& stream_id
    ) const {
        folly::Future<VariantKey> journal_key_fut = folly::Future<VariantKey>::makeEmpty();

        IndexAggregator<RowCountIndex> version_agg(stream_id, [&journal_key_fut, &store, &version_key](auto&& segment) {
            journal_key_fut = store->update(version_key, std::forward<decltype(segment)>(segment)).wait();
        });

        for (auto& key : index_keys) {
            version_agg.add_key(key);
        }

        version_agg.commit();
        return to_atom(std::move(journal_key_fut).get());
    }

    /** To be run as a stand-alone job only because it calls flush(). */
    void compact_if_necessary_stand_alone(const std::shared_ptr<Store>& store, size_t batch_size) {
        auto map = get_num_version_entries(store, batch_size);
        size_t max_blocks = ConfigsMap::instance()->get_int("VersionMap.MaxVersionBlocks", 5);
        const auto total_symbols ARCTICDB_UNUSED = map.size();
        size_t num_sym_compacted = 0;
        for (const auto& [symbol, size] : map) {
            if (size < max_blocks)
                continue;

            try {
                compact(store, symbol);
                ++num_sym_compacted;
                flush();
            } catch (const std::exception& e) {
                log::version().warn("Error: {} in compacting {}", e.what(), symbol);
            }
            if (num_sym_compacted % 50 == 0) {
                ARCTICDB_RUNTIME_DEBUG(log::version(), "Compacted {} symbols", num_sym_compacted);
            }
        }
        ARCTICDB_RUNTIME_DEBUG(
                log::version(), "Compacted {} out of {} total symbols", num_sym_compacted, total_symbols
        );
    }

    void compact(std::shared_ptr<Store> store, const StreamId& stream_id) {
        ARCTICDB_DEBUG(log::version(), "Version map compacting versions for stream {}", stream_id);
        auto entry = check_reload(
                store, stream_id, LoadStrategy{LoadType::ALL, LoadObjective::INCLUDE_DELETED}, __FUNCTION__
        );
        if (entry->empty()) {
            log::version().warn("Entry is empty in compact");
            return;
        }

        if (entry->keys_.size() < 3)
            return;

        if (!requires_compaction(entry))
            return;

        auto new_entry = compact_entry(store, stream_id, entry);

        if (validate_)
            new_entry->validate();

        std::swap(*entry, *new_entry);
    }

    void overwrite_symbol_tree(
            std::shared_ptr<Store> store, const StreamId& stream_id, const std::vector<AtomKey>& index_keys
    ) {
        auto entry = std::make_shared<VersionMapEntry>();
        try {
            entry = check_reload(
                    store, stream_id, LoadStrategy{LoadType::ALL, LoadObjective::INCLUDE_DELETED}, __FUNCTION__
            );
        } catch (const storage::KeyNotFoundException& e) {
            log::version().debug(
                    "Failed to load version entry for symbol {} in overwrite_symbol_tree, creating new entry, "
                    "exception: {}",
                    stream_id,
                    e.what()
            );
        }
        auto old_entry = *entry;
        if (!index_keys.empty()) {
            entry->keys_.assign(std::begin(index_keys), std::end(index_keys));
            auto new_version_id = index_keys[0].version_id();
            entry->head_ = write_entry_to_storage(store, stream_id, new_version_id, entry);
            if (validate_)
                entry->validate();
        }
        try {
            remove_entry_version_keys(store, old_entry, stream_id);
        } catch (const storage::KeyNotFoundException& e) {
            log::version().debug(
                    "Failed to remove version keys for symbol {} in overwrite_symbol_tree, exception: {}",
                    stream_id,
                    e.what()
            );
        }
    }

    /**
     * @param skip_compat Do not check the legacy "journal" entries
     * @param iterate_on_failure Use `iterate_type` (slow!) if the linked-list-based load logic throws
     */
    std::shared_ptr<VersionMapEntry> check_reload(
            std::shared_ptr<Store> store, const StreamId& stream_id, const LoadStrategy& load_strategy,
            const char* function ARCTICDB_UNUSED
    ) {
        ARCTICDB_DEBUG(log::version(), "Check reload in function {} for id {}", function, stream_id);

        auto status = check_cache_status(stream_id, load_strategy);

        switch (status) {
        case CacheStatus::VALID:
            return get_entry(stream_id);
        case CacheStatus::NEEDS_NEWER: {
            auto entry = get_entry(stream_id);
            try {
                prepend_newer_versions(store, stream_id, entry);
                const auto clock_unsync_tolerance = ConfigsMap::instance()->get_int(
                        "VersionMap.UnsyncTolerance", DEFAULT_CLOCK_UNSYNC_TOLERANCE
                );
                entry->last_reload_time_ = Clock::nanos_since_epoch() - clock_unsync_tolerance;
            } catch (const std::exception&) {
                return storage_reload(store, stream_id, load_strategy);
            }
            return entry;
        }
        case CacheStatus::NEEDS_OLDER: {
            auto entry = get_entry(stream_id);
            try {
                extend_older_versions(store, entry, load_strategy);
            } catch (const std::exception&) {
                return storage_reload(store, stream_id, load_strategy);
            }
            return entry;
        }
        case CacheStatus::NEEDS_RELOAD:
        case CacheStatus::EXPIRED_TIME:
        case CacheStatus::NOT_FOUND:
        default:
            return storage_reload(store, stream_id, load_strategy);
        }
    }

    /**
     * Returns the second undeleted index (after the write).
     */
    std::optional<AtomKey> do_write(
            std::shared_ptr<Store> store, const AtomKey& key, const std::shared_ptr<VersionMapEntry>& entry,
            const bool prevent_non_increasing_version_id = true
    ) {
        return do_write(
                store, key.version_id(), key.id(), std::span{&key, 1}, entry, prevent_non_increasing_version_id
        );
    }

    std::optional<AtomKey> do_write(
            std::shared_ptr<Store> store, const VersionId& version_id, const StreamId& stream_id,
            const std::span<const AtomKey>& keys, const std::shared_ptr<VersionMapEntry>& entry,
            const bool prevent_non_increasing_version_id = true
    ) {
        if (validate_)
            entry->validate();

        auto atom_journal_key = to_atom(journal_key(store, version_id, stream_id, keys, entry->head_));

        bool has_index_key = false;
        auto original_head = entry->head_;
        if (original_head.has_value()) {
            entry->unshift_key(*original_head);
        }

        for (auto it = keys.rbegin(); it != keys.rend(); ++it) {
            const auto& key = *it;
            if (key.type() == KeyType::TABLE_INDEX) {
                util::check(
                        !has_index_key,
                        "There should be at most one index key in the list of keys when trying to write an entry to "
                        "the store, keys: {}",
                        fmt::format("{}", keys)
                );
                has_index_key = true;
                bool is_version_increasing =
                        !original_head.has_value() || key.version_id() > original_head->version_id();

                if (!is_version_increasing) {
                    if (prevent_non_increasing_version_id) {
                        storage::raise<ErrorCode::E_NON_INCREASING_INDEX_VERSION>(
                                "Trying to write TABLE_INDEX key with a non-increasing version. New version: {}, Last "
                                "version: {}, symbol: {}. This is most likely due to parallel writes to the same "
                                "symbol, which is not supported.",
                                key.version_id(),
                                original_head ? original_head->version_id() : VariantId{""},
                                key.id()
                        );
                    } else {
                        // This should happen only in tests and background jobs
                        log::version().warn(
                                "Force writing TABLE_INDEX key with a non-increasing version (Reading with as_of "
                                "version numbers and timestamps may no longer work as expected). New version: {}, Last "
                                "version: {}, symbol: {}",
                                key.version_id(),
                                original_head ? original_head->version_id() : VariantId{""},
                                key.id()
                        );
                    }
                }
            }

            write_to_entry(entry, key, atom_journal_key);
        }

        auto previous_index = entry->get_second_undeleted_index();
        return previous_index;
    }

    AtomKey write_tombstones(
            std::shared_ptr<Store> store, const std::vector<AtomKey>& keys, const StreamId& stream_id,
            const std::shared_ptr<VersionMapEntry>& entry, const std::optional<timestamp>& creation_ts = std::nullopt
    ) {
        static const bool should_log_individual_tombstones =
                ConfigsMap::instance()->get_int("VersionMap.LogIndividualTombstones", 1);
        auto tombstone_keys = write_tombstones_internal(store, keys, stream_id, entry, creation_ts);
        write_symbol_ref(store, tombstone_keys.front(), std::nullopt, entry->head_.value());
        if (log_changes_) {
            if (should_log_individual_tombstones) {
                for (const auto& key : tombstone_keys) {
                    log_tombstone(store, stream_id, key.version_id());
                }
            } else {
                log_tombstone(store, stream_id, tombstone_keys.front().version_id());
            }
        }

        return tombstone_keys.front();
    }

    std::vector<AtomKey> write_tombstones_internal(
            std::shared_ptr<Store> store, const std::vector<AtomKey>& keys, const StreamId& stream_id,
            const std::shared_ptr<VersionMapEntry>& entry, const std::optional<timestamp>& creation_ts = std::nullopt
    ) {
        user_input::check<ErrorCode::E_INVALID_USER_ARGUMENT>(keys.size() > 0, "No version ids to write tombstone for");
        if (validate_)
            entry->validate();

        const auto ts = creation_ts.value_or(store->current_timestamp());
        std::vector<AtomKey> tombstones;
        std::transform(keys.begin(), keys.end(), std::back_inserter(tombstones), [&](const AtomKey& k) {
            return index_to_tombstone(k.version_id(), stream_id, ts);
        });

        // sort the tombstone in descending order
        std::sort(tombstones.begin(), tombstones.end(), [](const AtomKey& a, const AtomKey& b) {
            return a.version_id() > b.version_id();
        });

        // It doesn't matter which version id we use here
        // as long as it is one of the version ids in the keys
        // tombstone keys use already existing version ids instead of creating new ones
        // It IS important that we log with the same version id as the tombstone key
        // for backwards compatibility with older replication logic
        auto tombstone_version_id = tombstones.front().version_id();
        do_write(store, tombstone_version_id, stream_id, std::span{tombstones}, entry);
        for (const auto& key : tombstones) {
            entry->tombstones_.try_emplace(key.version_id(), key);
        }
        maybe_invalidate_cached_undeleted(*entry);

        return tombstones;
    }

    void remove_entry_version_keys(
            const std::shared_ptr<Store>& store, const std::shared_ptr<VersionMapEntry>& entry,
            const StreamId& stream_id
    ) const {
        return remove_entry_version_keys(store, *entry, stream_id);
    }

    void remove_entry_version_keys(
            const std::shared_ptr<Store>& store, const VersionMapEntry& entry, const StreamId& stream_id
    ) const {
        if (entry.head_) {
            util::check(
                    entry.head_->id() == stream_id,
                    "Id mismatch for entry {} vs symbol {}",
                    entry.head_->id(),
                    stream_id
            );
            store->remove_key_sync(*entry.head_);
        }
        std::vector<folly::Future<Store::RemoveKeyResultType>> key_futs;
        for (const auto& key : entry.keys_) {
            util::check(key.id() == stream_id, "Id mismatch for entry {} vs symbol {}", key.id(), stream_id);
            if (key.type() == KeyType::VERSION)
                key_futs.emplace_back(store->remove_key(key));
        }
        folly::collect(key_futs).get();
    }

    /**
     * Public for testability only.
     *
     * @param stream_id symbol to check
     * @param load_param the load type
     * @return whether we have a cached entry suitable for the load strategy, so do not need to go to storage
     */
    bool has_cached_entry(const StreamId& stream_id, const LoadStrategy& requested_load_strategy) const {
        return check_cache_status(stream_id, requested_load_strategy) == CacheStatus::VALID;
    }

  private:
    CacheStatus check_cache_status(const StreamId& stream_id, const LoadStrategy& requested_load_strategy) const {
        LoadType requested_load_type = requested_load_strategy.load_type_;
        util::check(requested_load_type < LoadType::UNKNOWN, "Unexpected load type requested {}", requested_load_type);

        requested_load_strategy.validate();
        MapType::const_iterator entry_it;
        if (!find_entry(entry_it, stream_id)) {
            return CacheStatus::NOT_FOUND;
        }

        const timestamp reload_interval = reload_interval_.value_or(
                ConfigsMap::instance()->get_int("VersionMap.ReloadInterval", DEFAULT_RELOAD_INTERVAL)
        );

        const auto& entry = entry_it->second;
        if (const timestamp cache_timing = now() - entry->last_reload_time_; cache_timing > reload_interval) {
            ARCTICDB_DEBUG(
                    log::version(),
                    "Latest read time {} too long ago for last acceptable cached timing {} (cache period {}) for "
                    "symbol {}",
                    entry->last_reload_time_,
                    cache_timing,
                    reload_interval,
                    stream_id
            );

            return CacheStatus::EXPIRED_TIME;
        }

        return covers_load_strategy(*entry, requested_load_strategy);
    }

    /**
     * Check whether the entry's loaded data covers the requested load strategy.
     * Returns VALID if fully covered, NEEDS_NEWER if data beyond our latest is needed,
     * or NEEDS_OLDER if we need to load deeper into the chain.
     * Does NOT check time-based expiry or entry existence - those are handled by check_cache_status.
     */
    CacheStatus covers_load_strategy(const VersionMapEntry& entry, const LoadStrategy& requested_load_strategy) const {
        LoadType requested_load_type = requested_load_strategy.load_type_;

        const bool has_loaded_earliest_version_all = entry.load_progress_.is_earliest_version_loaded;
        const bool has_loaded_earliest_undeleted =
                entry.tombstone_all_.has_value() &&
                entry.load_progress_.oldest_loaded_index_version_ <= entry.tombstone_all_->version_id();
        const bool has_loaded_earliest =
                has_loaded_earliest_version_all ||
                (!requested_load_strategy.should_include_deleted() && has_loaded_earliest_undeleted);

        if (requested_load_type == LoadType::DOWNTO) {
            return loaded_as_far_as_version_id(entry, requested_load_strategy.load_until_version_.value());
        }

        if (requested_load_type == LoadType::FROM_TIME) {
            return loaded_as_far_as_timestamp(
                    entry,
                    requested_load_strategy.load_from_time_.value(),
                    requested_load_strategy.should_include_deleted(),
                    has_loaded_earliest
            );
        }

        if (has_loaded_earliest) {
            return CacheStatus::VALID;
        }

        switch (requested_load_type) {
        case LoadType::NOT_LOADED:
            return CacheStatus::VALID;
        case LoadType::LATEST: {
            auto opt_latest = entry.get_first_index(requested_load_strategy.should_include_deleted()).first;
            return opt_latest.has_value() ? CacheStatus::VALID : CacheStatus::NEEDS_RELOAD;
        }
        case LoadType::ALL:
        case LoadType::UNKNOWN:
        default:
            return CacheStatus::NEEDS_OLDER;
        }
    }

    std::shared_ptr<VersionMapEntry> compact_entry(
            std::shared_ptr<Store> store, const StreamId& stream_id, const std::shared_ptr<VersionMapEntry>& entry
    ) {
        // For compacting an entry, we compact from the second version key in the chain
        // This makes it concurrent safe (when use_tombstones is enabled)
        // The first version key is in head and the second version key is first in entry.keys_
        if (validate_)
            entry->validate();
        util::check(entry->head_.value().type() == KeyType::VERSION, "Type of head must be version");
        auto new_entry = std::make_shared<VersionMapEntry>(*entry);

        auto parent = std::find_if(std::begin(new_entry->keys_), std::end(new_entry->keys_), [](const auto& k) {
            return k.type() == KeyType ::VERSION;
        });

        // Copy version keys to be removed
        std::vector<VariantKey> version_keys_compacted;
        std::copy_if(
                parent + 1,
                std::end(new_entry->keys_),
                std::back_inserter(version_keys_compacted),
                [](const auto& k) { return k.type() == KeyType::VERSION; }
        );

        // Copy index keys to be compacted
        std::vector<AtomKey> index_keys_compacted;
        std::copy_if(
                parent + 1,
                std::end(new_entry->keys_),
                std::back_inserter(index_keys_compacted),
                [](const auto& k) { return is_index_or_tombstone(k); }
        );

        update_version_key(store, *parent, index_keys_compacted, stream_id);
        store->remove_keys(version_keys_compacted).get();

        new_entry->keys_.erase(
                std::remove_if(
                        parent + 1,
                        std::end(new_entry->keys_),
                        [](const auto& k) { return k.type() == KeyType::VERSION; }
                ),
                std::end(new_entry->keys_)
        );
        new_entry->next_version_key_.reset();

        if (validate_)
            new_entry->validate();
        return new_entry;
    }

    void write_to_entry(const std::shared_ptr<VersionMapEntry>& entry, const AtomKey& key, const AtomKey& journal_key)
            const {
        entry->unshift_key(key);
        entry->head_ = journal_key;

        if (validate_)
            entry->validate();
    }

    bool find_entry(MapType::const_iterator& entry, const StreamId& stream_id) const {
        std::lock_guard lock(map_mutex_);
        entry = map_.find(stream_id);
        if (entry == map_.cend()) {
            ARCTICDB_DEBUG(log::version(), "Did not find cached entry for stream id {}", stream_id);
            return false;
        }
        return true;
    }

    /**
     * Whether entry contains as much of the version map as specified by the requested version id.
     * Returns VALID if covered, NEEDS_NEWER if the version is beyond our latest known,
     * or NEEDS_OLDER if we haven't loaded deep enough in the chain.
     */
    CacheStatus loaded_as_far_as_version_id(const VersionMapEntry& entry, SignedVersionId requested_version_id) const {
        auto opt_latest = entry.get_first_index(true).first;
        if (requested_version_id >= 0) {
            auto requested = static_cast<VersionId>(requested_version_id);
            if (opt_latest.has_value() && requested > opt_latest->version_id()) {
                return CacheStatus::NEEDS_NEWER;
            }
            if (opt_latest.has_value() &&
                entry.load_progress_.oldest_loaded_index_version_ <= requested) {
                ARCTICDB_DEBUG(
                        log::version(),
                        "Loaded as far as required value {}, have {} to {}",
                        requested_version_id,
                        entry.load_progress_.oldest_loaded_index_version_,
                        opt_latest->version_id()
                );
                return CacheStatus::VALID;
            }
        } else {
            if (opt_latest.has_value()) {
                auto opt_version_id = get_version_id_negative_index(opt_latest->version_id(), requested_version_id);
                if (opt_version_id.has_value() &&
                    entry.load_progress_.oldest_loaded_index_version_ <= *opt_version_id) {
                    ARCTICDB_DEBUG(
                            log::version(),
                            "Loaded as far as required value {}, have {} and there are {} total versions",
                            requested_version_id,
                            entry.load_progress_.oldest_loaded_index_version_,
                            opt_latest->version_id()
                    );
                    return CacheStatus::VALID;
                }
            }
        }
        return CacheStatus::NEEDS_OLDER;
    }

    /**
     * Whether entry contains as much of the version map as specified by the requested timestamp.
     * Returns VALID if covered, NEEDS_NEWER if the timestamp is beyond our latest loaded,
     * or NEEDS_OLDER if we haven't loaded deep enough in the chain.
     */
    CacheStatus loaded_as_far_as_timestamp(
            const VersionMapEntry& entry, timestamp requested_timestamp, bool include_deleted_versions,
            bool has_loaded_earliest
    ) const {
        if (requested_timestamp < 0) {
            return entry.load_progress_.is_earliest_version_loaded
                    ? CacheStatus::VALID
                    : CacheStatus::NEEDS_RELOAD;
        }

        timestamp latest_loaded_timestamp = entry.last_reload_time_;
        if (requested_timestamp > latest_loaded_timestamp) {
            return CacheStatus::NEEDS_RELOAD;
        }

        timestamp earliest_loaded_timestamp = include_deleted_versions
                                                      ? entry.load_progress_.earliest_loaded_timestamp_
                                                      : entry.load_progress_.earliest_loaded_undeleted_timestamp_;
        if (earliest_loaded_timestamp <= requested_timestamp || has_loaded_earliest) {
            ARCTICDB_DEBUG(
                    log::version(),
                    "Loaded as far as required timestamp {}, have latest loaded timestamp {}",
                    requested_timestamp,
                    latest_loaded_timestamp
            );
            return CacheStatus::VALID;
        }
        return CacheStatus::NEEDS_RELOAD;
    }

    std::shared_ptr<VersionMapEntry>& get_entry(const StreamId& stream_id) {
        std::lock_guard lock(map_mutex_);
        if (auto result = map_.find(stream_id); result != std::end(map_))
            return result->second;

        return map_.try_emplace(stream_id, std::make_shared<VersionMapEntry>()).first->second;
    }

    AtomKey write_entry_to_storage(
            std::shared_ptr<Store> store, const StreamId& stream_id, VersionId version_id,
            const std::shared_ptr<VersionMapEntry>& entry
    ) {
        AtomKey journal_key;
        entry->validate_types();

        IndexAggregator<RowCountIndex> version_agg(
                stream_id,
                [&store, &journal_key, &version_id, &stream_id](auto&& segment) {
                    const PartialKey pk{
                            KeyType::VERSION,
                            version_id,
                            stream_id,
                            IndexValue(NumericIndex{0}),
                            IndexValue(NumericIndex{0})
                    };

                    journal_key = to_atom(store->write_sync(pk, std::forward<decltype(segment)>(segment)));
                }
        );

        for (const auto& key : entry->keys_) {
            version_agg.add_key(key);
        }

        version_agg.commit();
        auto previous_index = entry->get_second_undeleted_index();
        write_symbol_ref(store, *entry->keys_.cbegin(), previous_index, journal_key);
        return journal_key;
    }

    /**
     * Prepend newer versions to the cached entry.
     * Reads the VERSION_REF, then follows the chain from the new head until we find the cached head
     * (the "bridging point"). If found, prepends the new keys and updates head_ and load_progress_.
     * Throws on failure (e.g. compaction broke the chain), caller falls back to full reload.
     */
    void prepend_newer_versions(
            const std::shared_ptr<Store>& store, const StreamId& stream_id,
            const std::shared_ptr<VersionMapEntry>& entry
    ) {
        VersionMapEntry ref_entry;
        read_symbol_ref(store, stream_id, ref_entry);

        if (ref_entry.empty()) {
            util::raise_rte("Symbol {} was deleted from storage during prepend", stream_id);
        }

        VersionMapEntry newer_keys;
        std::optional<AtomKey> next_key = ref_entry.head_;

        while (next_key) {
            if (*next_key == *entry->head_) {
                // Found the bridging point - old head exists in the new chain
                newer_keys.keys_.push_back(*entry->head_);
                entry->keys_.insert(
                        entry->keys_.begin(),
                        std::make_move_iterator(newer_keys.keys_.begin()),
                        std::make_move_iterator(newer_keys.keys_.end())
                );
                entry->head_ = ref_entry.head_;

                for (auto& [vid, tk] : newer_keys.tombstones_) {
                    entry->tombstones_.try_emplace(vid, std::move(tk));
                }
                if (newer_keys.tombstone_all_) {
                    entry->try_set_tombstone_all(std::move(*newer_keys.tombstone_all_));
                }

                ARCTICDB_DEBUG(log::version(), "Prepended newer versions for symbol {}", stream_id);
                return;
            }

            auto [key, seg] = store->read_sync(next_key.value());
            next_key = read_segment_with_keys(seg, newer_keys, newer_keys.load_progress_);
        }

        util::raise_rte(
                "Failed to find bridging point while prepending newer versions for {}", stream_id
        );
    }

    /**
     * Continue loading older versions from the tail of the cached chain.
     * Uses next_version_key_ if set (normal path), otherwise discovers the continuation
     * point from the last VERSION key in keys_ or head_ (fast-path / do_write cases).
     */
    void extend_older_versions(
            const std::shared_ptr<Store>& store, const std::shared_ptr<VersionMapEntry>& entry,
            const LoadStrategy& load_strategy
    ) {
        if (entry->load_progress_.is_earliest_version_loaded) {
            return;
        }
        if (!entry->next_version_key_) {
            // No continuation pointer. This is only safe to mark as "fully loaded" if we
            // actually loaded version data from storage (oldest_loaded_index_version_ was set).
            // If the entry was built purely from writes, oldest_loaded_index_version_ is still
            // at max and we should NOT claim everything is loaded — let the caller fall back
            // to storage_reload.
            if (entry->load_progress_.oldest_loaded_index_version_ != std::numeric_limits<VersionId>::max()) {
                entry->load_progress_.is_earliest_version_loaded = true;
            }
            return;
        }

        std::optional<AtomKey> next_key = entry->next_version_key_;

        // If content already loaded (fast-path ref key bypass), INDEX keys from
        // already-read segments are in keys_. Read into temp to skip past them.
        // The fast path may have loaded content from multiple segments (head + penultimate),
        // so we keep skipping as long as the next segment's INDEX version_id is already present.
        // if (entry->next_version_key_content_loaded_) {
        //     while (next_key) {
        //         VersionMapEntry temp;
        //         LoadProgress temp_progress;
        //         auto [key, seg] = store->read_sync(next_key.value());
        //         next_key = read_segment_with_keys(seg, temp, temp_progress);
        //         // Check if the next segment's content is also already in the entry
        //         // by checking if its first INDEX key version_id is already loaded
        //         if (!next_key) {
        //             entry->load_progress_.is_earliest_version_loaded = true;
        //             entry->next_version_key_.reset();
        //             entry->next_version_key_content_loaded_ = false;
        //             return;
        //         }
        //         // If the next key's version_id has an INDEX already in entry->keys_, skip it too
        //         bool already_loaded = false;
        //         for (const auto& k : entry->keys_) {
        //             if (is_index_key_type(k.type()) && k.version_id() == next_key->version_id()) {
        //                 already_loaded = true;
        //                 break;
        //             }
        //         }
        //         if (!already_loaded) {
        //             break;
        //         }
        //     }
        // }

        LoadProgress load_progress = entry->load_progress_;
        std::optional<VersionId> latest_version;
        if (auto opt_latest = entry->get_first_index(true).first; opt_latest) {
            latest_version = opt_latest->version_id();
        }

        do {
            auto [key, seg] = store->read_sync(next_key.value());
            next_key = read_segment_with_keys(seg, *entry, load_progress);
        } while (next_key && continue_when_loading_version(load_strategy, load_progress, latest_version) &&
                 continue_when_loading_from_time(load_strategy, load_progress) &&
                 continue_when_loading_latest(load_strategy, entry) &&
                 continue_when_loading_undeleted(load_strategy, entry, load_progress));

        entry->load_progress_ = std::move(load_progress);
        entry->next_version_key_ = std::move(next_key);
    }


    std::shared_ptr<VersionMapEntry> storage_reload(
            std::shared_ptr<Store> store, const StreamId& stream_id, const LoadStrategy& load_strategy
    ) {
        /*
         * Goes to the storage for a given symbol, and recreates the VersionMapEntry from preferably the ref key
         * structure, and if that fails it then goes and builds that from iterating all keys from storage which can
         * be much slower, though always consistent.
         */

        auto entry = get_entry(stream_id);
        entry->clear();
        const auto clock_unsync_tolerance =
                ConfigsMap::instance()->get_int("VersionMap.UnsyncTolerance", DEFAULT_CLOCK_UNSYNC_TOLERANCE);
        entry->last_reload_time_ = Clock::nanos_since_epoch() - clock_unsync_tolerance;

        auto temp = std::make_shared<VersionMapEntry>(*entry);
        load_via_ref_key(store, stream_id, load_strategy, temp);
        std::swap(*entry, *temp);

        util::check(entry->keys_.empty() || entry->head_, "Non-empty VersionMapEntry should set head");
        if (validate_)
            entry->validate();

        return entry;
    }

    timestamp now() const { return Clock::nanos_since_epoch(); }

    std::shared_ptr<VersionMapEntry> rewrite_entry(
            std::shared_ptr<Store> store, const StreamId& stream_id, const std::shared_ptr<VersionMapEntry>& entry
    ) {
        auto new_entry = std::make_shared<VersionMapEntry>();
        std::copy_if(
                std::begin(entry->keys_),
                std::end(entry->keys_),
                std::back_inserter(new_entry->keys_),
                [](const auto& key) { return is_index_or_tombstone(key); }
        );
        const auto first_index = new_entry->get_first_index(true).first;
        util::check(static_cast<bool>(first_index), "No index exists in rewrite entry");
        auto version_id = first_index->version_id();
        new_entry->head_ = write_entry_to_storage(store, stream_id, version_id, new_entry);
        remove_entry_version_keys(store, entry, stream_id);

        if (validate_)
            new_entry->validate();

        return new_entry;
    }

  public:
    bool check_ref_key(std::shared_ptr<Store> store, const StreamId& stream_id) {
        auto entry_iteration = std::make_shared<VersionMapEntry>();
        load_via_iteration(store, stream_id, entry_iteration);
        auto maybe_latest_pair = get_latest_key_pair(entry_iteration);
        if (!maybe_latest_pair) {
            log::version().warn("LATEST version not found for {}", stream_id);
            return false;
        }

        VersionMapEntry ref_entry;
        read_symbol_ref(store, stream_id, ref_entry);

        if (ref_entry.empty() || ref_entry.keys_.size() < 2) {
            log::version().warn("Reference key error for stream id {}", stream_id);
            return false;
        }

        util::check(static_cast<bool>(ref_entry.head_), "Expected head to be set");
        if (maybe_latest_pair->first != ref_entry.keys_[0] || maybe_latest_pair->second != *ref_entry.head_) {
            log::version().warn(
                    "Ref entry is incorrect for stream {}, either {} != {} or {} != {}",
                    stream_id,
                    maybe_latest_pair->first,
                    ref_entry.head_.value(),
                    maybe_latest_pair->second,
                    ref_entry.keys_[0]
            );
            return false;
        }

        try {
            auto entry_ref = std::make_shared<VersionMapEntry>();
            load_via_ref_key(store, stream_id, LoadStrategy{LoadType::ALL, LoadObjective::INCLUDE_DELETED}, entry_ref);
            entry_ref->validate();
        } catch (const std::exception& err) {
            log::version().warn(
                    "Loading versions from storage via ref key failed with error: {} for stream {}",
                    err.what(),
                    stream_id
            );
            return false;
        }
        return true;
    }

    bool indexes_sorted(const std::shared_ptr<Store>& store, const StreamId& stream_id) {
        auto entry_ref = std::make_shared<VersionMapEntry>();
        load_via_ref_key(store, stream_id, LoadStrategy{LoadType::ALL, LoadObjective::INCLUDE_DELETED}, entry_ref);
        auto indexes = entry_ref->get_indexes(true);
        return std::is_sorted(std::cbegin(indexes), std::cend(indexes), [](const auto& l, const auto& r) {
            return l > r;
        });
    }

    void scan_and_rewrite(const std::shared_ptr<Store>& store, const StreamId& stream_id) {
        log::version().warn("Version map scanning and rewriting  versions for stream {}", stream_id);
        auto entry = get_entry(stream_id);
        entry->clear();
        load_via_iteration(store, stream_id, entry, false);
        remove_duplicate_index_keys(entry);
        rewrite_entry(store, stream_id, entry);
    }

    void remove_and_rewrite_version_keys(std::shared_ptr<Store> store, const StreamId& stream_id) {
        log::version().warn("Rewriting all index keys for {}", stream_id);
        auto entry = get_entry(stream_id);
        auto old_entry = entry;
        entry->clear();
        load_via_iteration(store, stream_id, entry, true);
        remove_duplicate_index_keys(entry);
        rewrite_entry(store, stream_id, entry);
        remove_entry_version_keys(store, old_entry, stream_id);
    }

    void fix_ref_key(std::shared_ptr<Store> store, const StreamId& stream_id) {
        if (check_ref_key(store, stream_id)) {
            log::version().warn("Key {} is fine, not fixing", stream_id);
            return;
        }

        log::version().warn("Fixing key {}", stream_id);
        scan_and_rewrite(store, stream_id);
    }

    std::vector<AtomKey> find_deleted_version_keys_for_entry(
            std::shared_ptr<Store> store, const StreamId& stream_id, const std::shared_ptr<VersionMapEntry>& entry
    ) {
        std::vector<AtomKey> missing_versions;

        iterate_keys_of_type_for_stream(
                store,
                KeyType::TABLE_INDEX,
                stream_id,
                [&entry, &missing_versions](const auto& vk) {
                    const auto& key = to_atom(vk);
                    auto it =
                            std::find_if(std::begin(entry->keys_), std::end(entry->keys_), [&](const auto& entry_key) {
                                return entry_key.type() == KeyType::VERSION &&
                                       std::tie(key.id(), key.version_id()) ==
                                               std::tie(entry_key.id(), entry_key.version_id());
                            });
                    if (it == std::end(entry->keys_)) {
                        util::check(
                                static_cast<bool>(entry->head_) || entry->empty(),
                                "Expected head to be set after load via iteration"
                        );
                        if (!entry->head_ ||
                            std::tie(key.id(), key.version_id()) !=
                                    std::tie(entry->head_.value().id(), entry->head_.value().version_id()))
                            missing_versions.push_back(key);
                    }
                }
        );
        return missing_versions;
    }

    std::vector<AtomKey> find_deleted_version_keys(std::shared_ptr<Store> store, const StreamId& stream_id) {
        auto entry = std::make_shared<VersionMapEntry>();
        load_via_iteration(store, stream_id, entry);
        return find_deleted_version_keys_for_entry(store, stream_id, entry);
    }

    void recover_deleted(std::shared_ptr<Store> store, const StreamId& stream_id) {
        auto& entry = get_entry(stream_id);
        entry->clear();
        load_via_iteration(store, stream_id, entry);

        auto missing_versions = find_deleted_version_keys_for_entry(store, stream_id, entry);

        entry->keys_.insert(std::begin(entry->keys_), std::begin(missing_versions), std::end(missing_versions));
        entry->sort();
        rewrite_entry(store, stream_id, entry);
    }

    std::shared_ptr<Lock> get_lock_object(const StreamId& stream_id) const {
        return lock_table_->get_lock_object(stream_id);
    }

  private:
    FRIEND_TEST(VersionMap, CacheInvalidationWithTombstoneAllAfterLoad);
    std::pair<VersionId, std::vector<AtomKey>> tombstone_from_key_or_all_internal(
            std::shared_ptr<Store> store, const StreamId& stream_id,
            std::optional<AtomKey> first_key_to_tombstone = std::nullopt,
            std::shared_ptr<VersionMapEntry> entry = nullptr, bool should_write_to_storage = true
    ) {
        if (!entry) {
            entry = check_reload(
                    store, stream_id, LoadStrategy{LoadType::ALL, LoadObjective::UNDELETED_ONLY}, __FUNCTION__
            );
        }

        if (!first_key_to_tombstone)
            first_key_to_tombstone = entry->get_first_index(false).first;

        std::vector<AtomKey> output;
        for (const auto& key : entry->keys_) {
            if (is_index_key_type(key.type()) && !entry->is_tombstoned(key) &&
                key.version_id() <= first_key_to_tombstone->version_id()) {
                output.emplace_back(key);
            }
        }

        const auto& latest_version = entry->get_first_index(true).first;
        const VersionId version_id = latest_version ? latest_version->version_id() : 0;

        if (!output.empty() && should_write_to_storage) {
            auto tombstone_key = write_tombstone_all_key_internal(store, first_key_to_tombstone.value(), entry);
            if (log_changes_) {
                log_tombstone_all(store, stream_id, tombstone_key.version_id());
            }
        }

        return {version_id, std::move(output)};
    }

    // Invalidates the cached undeleted entry if it got tombstoned either by a tombstone or by a tombstone_all
    void maybe_invalidate_cached_undeleted(VersionMapEntry& entry) {
        if (entry.is_tombstoned(entry.load_progress_.oldest_loaded_undeleted_index_version_)) {
            entry.load_progress_.oldest_loaded_undeleted_index_version_ = std::numeric_limits<VersionId>::max();
            entry.load_progress_.earliest_loaded_undeleted_timestamp_ = std::numeric_limits<timestamp>::max();
        }
    }

    AtomKey write_tombstone_all_key_internal(
            const std::shared_ptr<Store>& store, const AtomKey& previous_key,
            const std::shared_ptr<VersionMapEntry>& entry
    ) {
        auto tombstone_key = get_tombstone_all_key(previous_key, store->current_timestamp());
        entry->try_set_tombstone_all(tombstone_key);
        do_write(store, tombstone_key, entry);
        maybe_invalidate_cached_undeleted(*entry);
        return tombstone_key;
    }
};

using VersionMap = VersionMapImpl<>;

} // namespace arcticdb
