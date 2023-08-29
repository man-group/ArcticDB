/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

/*
 * version_map.hpp contains VersionMap which at it's core is a map of {Stream_id: VersionMapEntry} (see version_map_entry.hpp)
 * (see VersionMapImpl for details)
 *
 */
#pragma once

#include <arcticdb/entity/types.hpp>
#include <arcticdb/entity/atom_key.hpp>
#include <arcticdb/stream/index.hpp>
#include <arcticdb/stream/stream_sink.hpp>
#include <arcticdb/stream/stream_writer.hpp>
#include <arcticdb/stream/stream_source.hpp>
#include <arcticdb/stream/stream_reader.hpp>
#include <shared_mutex>
#include <arcticdb/python/python_utils.hpp>
#include <arcticdb/util/configs_map.hpp>
#include <arcticdb/storage/store.hpp>
#include <arcticdb/util/constants.hpp>
#include <arcticdb/util/key_utils.hpp>
#include <arcticdb/version/version_map_entry.hpp>
#include <arcticdb/async/batch_read_args.hpp>
#include <arcticdb/version/version_log.hpp>
#include <arcticdb/version/version_utils.hpp>
#include <arcticdb/util/lock_table.hpp>

#include <unordered_set>
#include <map>
#include <deque>

namespace arcticdb {

template<class Clock=util::SysClock>
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
     * When a version is tombstoned, e.g. lib.write('a', 1), lib.write('a', 2) with pruning will tombstone the first version
     * which creates a new version key on storage in the same order of the timeline of operations and that key
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
    using StreamIdArg = const StreamId&;
    using MapType =  std::map<StreamId, std::shared_ptr<VersionMapEntry>>;

    static constexpr uint64_t DEFAULT_CLOCK_UNSYNC_TOLERANCE = ONE_SECOND * 2;
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

    void set_validate(bool value) {
        validate_ = value;
    }

    void set_log_changes(bool value) {
        log_changes_ = value;
    }

    bool log_changes() const {
        return log_changes_;
    }

    void set_reload_interval(timestamp interval) {
        reload_interval_ = std::make_optional<timestamp>(interval);
    }

    bool validate() const {
        return validate_;
    }

    void follow_version_chain(
        const std::shared_ptr<Store>& store,
        const VersionMapEntry& ref_entry,
        const std::shared_ptr<VersionMapEntry>& entry,
        const LoadParameter& load_params) const {
        auto next_key = ref_entry.head_;
        entry->head_ = ref_entry.head_;

        std::optional<VersionId> latest_version;
        LoadProgress load_progress;
        util::check(ref_entry.keys_.size() >= 2, "Invalid number of keys in ref entry: {}", ref_entry.keys_.size());
        if (key_exists_in_ref_entry(load_params, ref_entry, std::nullopt, load_progress)) {
            entry->keys_.push_back(ref_entry.keys_[0]);
        } else {
            do {
                auto [key, seg] = store->read_sync(next_key.value());
                next_key = read_segment_with_keys(seg, entry, load_progress);
                set_latest_version(entry, latest_version);
            } while (next_key
            && !loaded_until_version_id(load_params, load_progress, latest_version)
            && !loaded_until_timestamp(load_params, load_progress)
            && load_latest_ongoing(load_params, entry)
            && looking_for_undeleted(load_params, entry, load_progress));
        }
        set_loaded_until(load_progress, entry);
    }

    void load_via_ref_key(
        std::shared_ptr<Store> store,
        const StreamId& stream_id,
        const LoadParameter& load_params,
        const std::shared_ptr<VersionMapEntry>& entry) {
        load_params.validate();
        static const auto max_trial_config = ConfigsMap::instance()->get_int("VersionMap.MaxReadRefTrials", 2);
        auto max_trials = max_trial_config;
        while (true) {
            try {
                VersionMapEntry ref_entry;
                read_symbol_ref(store, stream_id, ref_entry);
                if (ref_entry.empty())
                    return;

                follow_version_chain(store, ref_entry, entry, load_params);
                break;
            } catch (const std::exception &err) {
                if (--max_trials <= 0) {
                    throw;
                }
                // We retry to read via ref key because it could have been modified by someone else (e.g. compaction)
                log::version().warn(
                        "Loading versions from storage via ref key failed with error: {} for stream {}. Retrying",
                        err.what(), stream_id);
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
        std::shared_ptr<Store> store,
        StreamIdArg stream_id,
        std::shared_ptr<VersionMapEntry>& entry,
        bool use_index_keys_for_iteration=false) const {
        ARCTICDB_DEBUG(log::version(), "Attempting to iterate version keys");
        auto match_stream_id = [&stream_id](const AtomKey &k) { return k.id() == stream_id; };
        entry = build_version_map_entry_with_predicate_iteration(store, match_stream_id, stream_id,
                    use_index_keys_for_iteration ? std::vector<KeyType>{KeyType::TABLE_INDEX, KeyType::MULTI_KEY}:
                                                   std::vector<KeyType>{KeyType::VERSION},
                    !use_index_keys_for_iteration);

        if (validate_)
            entry->validate();
    }

    void write_version(std::shared_ptr<Store> store, const AtomKey &key) {
        LoadParameter load_param{LoadType::LOAD_LATEST};
        load_param.skip_compat_ = false;
        auto entry = check_reload(store, key.id(), load_param,  __FUNCTION__);

        do_write(store, key, entry);
        if (validate_)
            entry->validate();
        if(log_changes_)
            log_write(store, key.id(), key.version_id());
    }

    AtomKey write_tombstone_all_key(
            const std::shared_ptr<Store>& store,
            const AtomKey& previous_key,
            const std::shared_ptr<VersionMapEntry>& entry) {
        auto tombstone_key = get_tombstone_all_key(previous_key, store->current_timestamp());
        entry->try_set_tombstone_all(tombstone_key);
        do_write(store, tombstone_key, entry);
        return tombstone_key;
    }

    /**
     * Tombstone all non-deleted versions of the given stream and do the related housekeeping.
     * @param first_key_to_tombstone The first key in the version chain that should be tombstoned. When empty
     * then the first index key onwards is tombstoned, so the whole chain is tombstoned.
     */
    std::vector<AtomKey> tombstone_from_key_or_all(
            std::shared_ptr<Store> store,
            const StreamId& stream_id,
            std::optional<AtomKey> first_key_to_tombstone = std::nullopt
            ) {
        return tombstone_from_key_or_all_internal(store, stream_id, first_key_to_tombstone);
    }

    std::string dump_entry(const std::shared_ptr<Store> store, const StreamId& stream_id) {
        const auto entry = check_reload(store, stream_id, LoadParameter{LoadType::LOAD_ALL}, __FUNCTION__);
        return entry->dump();
    }

    std::vector<AtomKey> write_and_prune_previous(
        std::shared_ptr<Store> store,
        const AtomKey &key, const
        std::optional<AtomKey>& previous_key) {
        ARCTICDB_DEBUG(log::version(), "Version map pruning previous versions for stream {}", key.id());
        auto entry = check_reload(
                store,
                key.id(),
                LoadParameter{LoadType::LOAD_UNDELETED},
                __FUNCTION__);
        auto result = tombstone_from_key_or_all_internal(store, key.id(), previous_key, entry);

        do_write(store, key, entry);

        if (log_changes_)
            log_write(store, key.id(), key.version_id());

        return result;
    }

    std::deque<AtomKey> delete_all_versions(std::shared_ptr<Store> store, StreamIdArg stream_id) {
        ARCTICDB_DEBUG(log::version(), "Version map deleting all versions for stream {}", stream_id);
        std::deque<AtomKey> output;
        auto index_keys = tombstone_from_key_or_all(store, stream_id);
        output.assign(std::begin(index_keys), std::end(index_keys));
        return output;
    }

    bool requires_compaction(const std::shared_ptr<VersionMapEntry>& entry) const {
        int64_t num_blocks = std::count_if(entry->keys_.cbegin(), entry->keys_.cend(),
                                            [](const AtomKey &key) { return key.type() == KeyType::VERSION; });

        static const auto max_blocks = ConfigsMap::instance()->get_int("VersionMap.MaxVersionBlocks", 5);
        if (num_blocks < max_blocks) {
            ARCTICDB_DEBUG(log::version(), "Not compacting as number of blocks {} is less than the permitted {}", num_blocks,
                                 max_blocks);
            return false;
        } else {
            return true;
        }
    }

    void compact_and_remove_deleted_indexes(std::shared_ptr<Store> store, StreamIdArg stream_id) {
        ARCTICDB_DEBUG(log::version(), "Version map compacting versions for stream {}", stream_id);
        auto entry = check_reload(store, stream_id, LoadParameter{LoadType::LOAD_ALL}, __FUNCTION__);
        if (!requires_compaction(entry))
            return;

        auto latest_version = std::find_if(std::begin(entry->keys_), std::end(entry->keys_),
                                           [](const auto &key) { return is_index_key_type(key.type()); });
        const auto new_version_id = latest_version->version_id();

        auto new_entry = std::make_shared<VersionMapEntry>();
        new_entry->keys_.push_front(*latest_version);

        if (const auto first_is_tombstone = entry->get_tombstone_if_any(new_version_id); first_is_tombstone)
            new_entry->keys_.emplace_front(std::move(*first_is_tombstone));

        std::advance(latest_version, 1);

        for (const auto &key : folly::Range{latest_version, entry->keys_.end()}) {
            if (is_index_key_type(key.type())) {
                const auto tombstone = entry->is_tombstoned(key);
                if (tombstone) {
                    if (!store->key_exists(key).get())
                        ARCTICDB_DEBUG(log::version(), "Removing deleted key {}", key);
                    else {
                        if(tombstone.value().type() == KeyType::TOMBSTONE_ALL)
                            new_entry->try_set_tombstone_all(tombstone.value());
                        else
                            new_entry->tombstones_.insert(std::make_pair(key.version_id(), tombstone.value()));

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

        std::swap(entry, new_entry);
    }

    AtomKey update_version_key(
        std::shared_ptr<Store> store,
        const VariantKey& version_key,
        const std::vector<AtomKey>& index_keys,
        StreamIdArg stream_id) const {
        folly::Future<VariantKey> journal_key_fut = folly::Future<VariantKey>::makeEmpty();

        IndexAggregator<RowCountIndex> version_agg(stream_id, [&journal_key_fut, &store, &version_key](auto &&segment) {
            journal_key_fut = store->update(version_key, std::forward<decltype(segment)>(segment)).wait();
        });

        for (auto &key : index_keys) {
            version_agg.add_key(key);
        }

        version_agg.commit();
        return to_atom(std::move(journal_key_fut).get());
    }

    std::shared_ptr<VersionMapEntry> compact_entry(
        std::shared_ptr<Store> store,
        StreamIdArg stream_id,
        const std::shared_ptr<VersionMapEntry>& entry) {
        // For compacting an entry, we compact from the second version key in the chain
        // This makes it concurrent safe (when use_tombstones is enabled)
        // The first version key is in head and the second version key is first in entry.keys_
        entry->validate();
        util::check(entry->head_.value().type() == KeyType::VERSION, "Type of head must be version");
        auto new_entry = std::make_shared<VersionMapEntry>(*entry);

        auto parent = std::find_if(std::begin(new_entry->keys_), std::end(new_entry->keys_),
                [](const auto& k){return k.type() == KeyType ::VERSION;});

        // Copy version keys to be removed
        std::vector<VariantKey> version_keys_compacted;
        std::copy_if(parent + 1, std::end(new_entry->keys_), std::back_inserter(version_keys_compacted),
                [](const auto& k){return k.type() == KeyType::VERSION;});

        // Copy index keys to be compacted
        std::vector<AtomKey> index_keys_compacted;
        std::copy_if(parent + 1, std::end(new_entry->keys_), std::back_inserter(index_keys_compacted),
                     [](const auto& k){return is_index_or_tombstone(k);});

        update_version_key(store, *parent, index_keys_compacted, stream_id);
        store->remove_keys(version_keys_compacted).get();

        new_entry->keys_.erase(std::remove_if(parent + 1,
                std::end(new_entry->keys_),
                [](const auto& k){return k.type() == KeyType::VERSION;}),
            std::end(new_entry->keys_));

        new_entry->validate();
        return new_entry;
    }

    /** To be run as a stand-alone job only because it calls flush(). */
    void compact_if_necessary_stand_alone(const std::shared_ptr<Store>& store, size_t batch_size) {
        auto map = get_num_version_entries(store, batch_size);
        size_t max_blocks = ConfigsMap::instance()->get_int("VersionMap.MaxVersionBlocks", 5);
        const auto total_symbols = map.size();
        size_t num_sym_compacted = 0;
        for(const auto& [symbol, size] : map) {
            if(size < max_blocks)
                continue;

            try {
                compact(store, symbol);
                ++num_sym_compacted;
                flush();
            } catch (const std::exception& e) {
                log::version().warn("Error: {} in compacting {}", e.what(), symbol);
            }
            if (num_sym_compacted % 50 == 0) {
                log::version().info("Compacted {} symbols", num_sym_compacted);
            }
        }
        log::version().info("Compacted {} out of {} total symbols", num_sym_compacted, total_symbols);
    }

    void compact(std::shared_ptr<Store> store, StreamIdArg stream_id) {
        ARCTICDB_DEBUG(log::version(), "Version map compacting versions for stream {}", stream_id);
        auto entry = check_reload(store, stream_id, LoadParameter{LoadType::LOAD_ALL}, __FUNCTION__);
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

        std::swap(entry, new_entry);
    }

    void overwrite_symbol_tree(
            std::shared_ptr<Store> store, StreamIdArg stream_id, const std::vector<AtomKey>& index_keys) {
        auto entry = check_reload(store, stream_id, LoadParameter{LoadType::LOAD_ALL}, __FUNCTION__);
        auto old_entry = *entry;
        if (!index_keys.empty()) {
            entry->keys_.assign(std::begin(index_keys), std::end(index_keys));
            auto new_version_id = index_keys[0].version_id();
            entry->head_ = write_entry_to_storage(store, stream_id, new_version_id, entry);
            if (validate_)
                entry->validate();
        }
        remove_entry_version_keys(store, old_entry, stream_id);
    }

    /**
     * @param skip_compat Do not check the legacy "journal" entries
     * @param iterate_on_failure Use `iterate_type` (slow!) if the linked-list-based load logic throws
     */
    std::shared_ptr<VersionMapEntry> check_reload(
        std::shared_ptr<Store> store,
        StreamIdArg stream_id,
        const LoadParameter& load_param,
        const char* function ARCTICDB_UNUSED) {
        ARCTICDB_DEBUG(log::version(), "Check reload in function {}", function);

        if (has_cached_entry(stream_id, load_param))
            return get_entry(stream_id);

        if (!load_param.skip_compat_ && !has_stored_entry(store, stream_id))
            do_backwards_compat_check(store, stream_id);

        return storage_reload(store, stream_id, load_param, load_param.iterate_on_failure_);
    }

    void do_write(
        std::shared_ptr<Store> store,
        const AtomKey &key,
        const std::shared_ptr<VersionMapEntry> &entry) {
        if (validate_)
            entry->validate();

        auto journal_key = to_atom(std::move(journal_single_key(store, key, entry->head_)));
        write_to_entry(entry, key, journal_key);
        write_symbol_ref(store, key, std::nullopt, journal_key);
    }

    AtomKey write_tombstone(
        std::shared_ptr<Store> store,
        const std::variant<AtomKey, VersionId>& key,
        StreamIdArg stream_id,
        const std::shared_ptr<VersionMapEntry>& entry,
        const std::optional<timestamp>& creation_ts=std::nullopt) {
        if (validate_)
            entry->validate();

        auto tombstone = util::variant_match(key, [&stream_id, store, &creation_ts](const auto &k){
            return index_to_tombstone(k, stream_id, creation_ts.value_or(store->current_timestamp()));
        });
        do_write(store, tombstone,  entry);
        if(log_changes_)
            log_tombstone(store, tombstone.id(), tombstone.version_id());

        return tombstone;
    }

    void remove_entry_version_keys(
        const std::shared_ptr<Store>& store,
        const std::shared_ptr<VersionMapEntry>& entry,
        const StreamId &stream_id) const {
       return remove_entry_version_keys(store, *entry, stream_id);
    }

    void remove_entry_version_keys(
        const std::shared_ptr<Store>& store,
        const VersionMapEntry& entry,
        const StreamId &stream_id) const {
        if (entry.head_) {
            util::check(entry.head_.value().id() == stream_id, "Id mismatch for entry {} vs symbol {}",
                        entry.head_.value().id(), stream_id);
            store->remove_key_sync(entry.head_.value());
        }
        std::vector<folly::Future<Store::RemoveKeyResultType>> key_futs;
        for (const auto &key : entry.keys_) {
            util::check(key.id() == stream_id, "Id mismatch for entry {} vs symbol {}", key.id(), stream_id);
            if (key.type() == KeyType::VERSION)
                key_futs.emplace_back(store->remove_key(key));
        }
        folly::collect(key_futs).get();
    }

private:
    void write_to_entry(
        const std::shared_ptr<VersionMapEntry>& entry,
        const AtomKey& key,
        const AtomKey& journal_key) const {
        if (entry->head_)
            entry->unshift_key(entry->head_.value());

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

    bool has_cached_entry(const StreamId &stream_id, const LoadParameter& load_param) const {
        load_param.validate();
        MapType::const_iterator entry;
        if(!find_entry(entry, stream_id))
            return false;

        const timestamp reload_interval = reload_interval_.value_or(ConfigsMap::instance()->get_int("VersionMap.ReloadInterval", DEFAULT_RELOAD_INTERVAL));

        if (const timestamp cache_timing = now() - entry->second->last_reload_time_; cache_timing > reload_interval) {
            ARCTICDB_DEBUG(log::version(),
                    "Latest read time {} too long ago for last acceptable cached timing {} (cache period {})",
                    entry->second->last_reload_time_, cache_timing, reload_interval);

            return false;
        }

        // TODO: Fix #587
        if (entry->second->load_type_ < load_param.load_type_) {
            ARCTICDB_DEBUG(log::version(), "Required load type {} exceeds existing load type {}, will reload", load_param.load_type_, entry->second->load_type_);
            return false;
        }

        if(entry->second->load_type_ == LoadType::LOAD_DOWNTO) {
            if (load_param.load_type_ == LoadType::LOAD_UNDELETED) {
                return false;
            }
            if (load_param.load_type_ == LoadType::LOAD_DOWNTO ) {
                if (is_positive_version_query(load_param)) {
                    if (entry->second->loaded_until_ > static_cast<VersionId>(load_param.load_until_.value())) {
                        ARCTICDB_DEBUG(log::version(), "Not loaded as far as required value {}, only have {}",
                                       load_param.load_until_.value(), entry->second->loaded_until_);
                        return false;
                    }
                } else {
                    auto opt_latest = entry->second->get_first_index(true);
                    if (opt_latest.has_value()) {
                        auto opt_version_id = get_version_id_negative_index(opt_latest->version_id(), *load_param.load_until_);
                        if (opt_version_id.has_value() && entry->second->loaded_until_ > *opt_version_id) {
                            ARCTICDB_DEBUG(log::version(), "Not loaded as far as required value {}, only have {} and there are {} total versions",
                                           load_param.load_until_.value(), entry->second->loaded_until_, opt_latest->version_id());
                            return false;
                        }
                    }
                }

            }
        }

        if(load_param.load_type_ == LoadType::LOAD_UNDELETED && !entry->second->tombstone_all_ &&
           entry->second->load_type_ != LoadType::LOAD_UNDELETED)
            return false;

        ARCTICDB_DEBUG(log::version(), "{} Using cached entry for symbol {}", uintptr_t(this), stream_id);
        return true;
    }

    std::shared_ptr<VersionMapEntry>& get_entry(const StreamId& stream_id) {
        std::lock_guard lock(map_mutex_);
        if(auto result = map_.find(stream_id); result != std::end(map_))
            return result->second;

        return map_.try_emplace(stream_id, std::make_shared<VersionMapEntry>()).first->second;
    }

    AtomKey write_entry_to_storage(
            std::shared_ptr<Store> store,
            const StreamId &stream_id,
            VersionId version_id,
            const std::shared_ptr<VersionMapEntry> &entry) {
        AtomKey journal_key;
        entry->validate_types();

        IndexAggregator<RowCountIndex> version_agg(stream_id, [&store, &journal_key, &version_id, &stream_id](auto &&segment) {
            stream::StreamSink::PartialKey pk{
                    KeyType::VERSION,
                    version_id,
                    stream_id,
                    IndexValue(0),
                    IndexValue(0)};

            journal_key = to_atom(store->write_sync(pk, std::forward<decltype(segment)>(segment)));
        });

        for (const auto &key : entry->keys_) {
            version_agg.add_key(key);
        }

        version_agg.commit();
        write_symbol_ref(store, *entry->keys_.cbegin(), std::nullopt, journal_key);
        return journal_key;
    }

    bool has_stored_entry(std::shared_ptr<StreamSource> store, const StreamId &stream_id) const {
        return static_cast<bool>(get_symbol_ref_key(store, stream_id));
    }

    std::shared_ptr<VersionMapEntry> storage_reload(
        std::shared_ptr<Store> store,
        StreamIdArg stream_id,
        const LoadParameter& load_param,
        bool iterate_on_failure) {
        /*
         * Goes to the storage for a given symbol, and recreates the VersionMapEntry from preferably the ref key
         * structure, and if that fails it then goes and builds that from iterating all keys from storage which can
         * be much slower, though always consistent.
         */

        auto entry = get_entry(stream_id);
        entry->clear();
        const auto clock_unsync_tolerance = ConfigsMap::instance()->get_int("VersionMap.UnsyncTolerance",
                                                                            DEFAULT_CLOCK_UNSYNC_TOLERANCE);
        entry->last_reload_time_ = Clock::nanos_since_epoch() - clock_unsync_tolerance;
        entry->load_type_ = LoadType::NOT_LOADED; // FUTURE: to make more thread-safe with #368

        try {
            auto temp = std::make_shared<VersionMapEntry>(*entry);
            load_via_ref_key(store, stream_id, load_param, temp);
            std::swap(*entry, *temp);
            entry->load_type_ = load_param.load_type_;
        }
        catch (const std::runtime_error &err) {
            if (iterate_on_failure) {
                log::version().info(
                        "Loading versions from storage via ref key failed with error: {}, will load via iteration",
                        err.what());
            } else {
                throw;
            }
        }
        if (iterate_on_failure && entry->empty()) {
            (void) load_via_iteration(store, stream_id, entry);
            entry->load_type_ = LoadType::LOAD_ALL;
        }

        util::check(entry->keys_.empty() || entry->head_, "Non-empty VersionMapEntry should set head");
        if (validate_)
            entry->validate();

        return entry;
    }

    VariantKey journal_single_key(
        std::shared_ptr<StreamSink> store,
        const AtomKey &key,
        std::optional<AtomKey> prev_journal_key) {
        ARCTICDB_SAMPLE(WriteJournalEntry, 0)
        ARCTICDB_DEBUG(log::version(), "Version map writing version for key {}", key);

        VariantKey journal_key;
        IndexAggregator<RowCountIndex> journal_agg(key.id(), [&store, &journal_key, &key](auto &&segment) {
            stream::StreamSink::PartialKey pk{
                KeyType::VERSION,
                key.version_id(),
                key.id(),
                IndexValue(0),
                IndexValue(0)
            };

            journal_key = store->write_sync(pk, std::forward<decltype(segment)>(segment));
        });
        journal_agg.add_key(key);
        if (prev_journal_key)
            journal_agg.add_key(prev_journal_key.value());

        journal_agg.commit();
        return journal_key;
    }

    timestamp now() const {
        return Clock::nanos_since_epoch();
    }

    std::shared_ptr<VersionMapEntry> rewrite_entry(
        std::shared_ptr<Store> store,
        StreamIdArg stream_id,
        const std::shared_ptr<VersionMapEntry>& entry) {
        auto new_entry = std::make_shared<VersionMapEntry>();
        std::copy_if(std::begin(entry->keys_), std::end(entry->keys_), std::back_inserter(new_entry->keys_),
                     [](const auto &key) {
                         return is_index_or_tombstone(key);
                     });
        auto version_id = new_entry->get_first_index(true).value().version_id();
        new_entry->head_ = write_entry_to_storage(store, stream_id, version_id, new_entry);
        remove_entry_version_keys(store, entry, stream_id);

        if(validate_)
            new_entry->validate();

        return new_entry;
    }

public:
    bool check_ref_key(std::shared_ptr<Store> store, StreamIdArg stream_id) {
        auto entry_iteration = std::make_shared<VersionMapEntry>();
        load_via_iteration(store, stream_id, entry_iteration);
        auto maybe_latest_pair = get_latest_key_pair(entry_iteration);
        if (!maybe_latest_pair) {
            log::version().warn("Latest version not found for {}", stream_id);
            return false;
        }

        VersionMapEntry ref_entry;
        read_symbol_ref(store, stream_id, ref_entry);

        if (ref_entry.empty() || ref_entry.keys_.size() < 2) {
            log::version().warn("Reference key error for stream id {}", stream_id);
            return false;
        }

        util::check(static_cast<bool>(ref_entry.head_), "Expected head to be set");
        if(maybe_latest_pair.value().first != ref_entry.keys_[0] || maybe_latest_pair.value().second != ref_entry.head_.value()) {
            log::version().warn("Ref entry is incorrect for stream {}, either {} != {} or {} != {}",
                    stream_id,
                    maybe_latest_pair.value().first,
                    ref_entry.head_.value(),
                    maybe_latest_pair.value().second,
                    ref_entry.keys_[0]);
            return false;
        }

        try {
            auto entry_ref = std::make_shared<VersionMapEntry>();
            load_via_ref_key(store, stream_id, LoadParameter{LoadType::LOAD_ALL}, entry_ref);
            entry_ref->validate();
        } catch (const std::exception& err) {
            log::version().warn(
                    "Loading versions from storage via ref key failed with error: {} for stream {}",
                    err.what(), stream_id);
            return false;
        }
        return true;
    }

    void scan_and_rewrite(std::shared_ptr<Store> store, StreamIdArg stream_id) {
        log::version().warn("Version map scanning and rewriting  versions for stream {}", stream_id);
        auto entry = get_entry(stream_id);
        entry->clear();
        load_via_iteration(store, stream_id, entry, false);
        fix_stream_ids_of_index_keys(store, stream_id, entry);
        remove_duplicate_index_keys(entry);
        (void)rewrite_entry(store, stream_id, entry);
    }

    void remove_and_rewrite_version_keys(std::shared_ptr<Store> store, StreamIdArg stream_id) {
        log::version().warn("Rewriting all index keys for {}", stream_id);
        auto entry = get_entry(stream_id);
        auto old_entry = entry;
        entry->clear();
        load_via_iteration(store, stream_id, entry, true);
        fix_stream_ids_of_index_keys(store, stream_id, entry);
        remove_duplicate_index_keys(entry);
        (void)rewrite_entry(store, stream_id, entry);
        remove_entry_version_keys(store, old_entry, stream_id);
    }

    void fix_ref_key(std::shared_ptr<Store> store, StreamIdArg stream_id) {
        if(check_ref_key(store, stream_id)) {
            log::version().warn("Key {} is fine, not fixing", stream_id);
            return;
        }

        log::version().warn("Fixing key {}", stream_id);
        scan_and_rewrite(store, stream_id);
    }

    std::vector<AtomKey> find_deleted_version_keys_for_entry(
        std::shared_ptr<Store> store,
        StreamIdArg stream_id,
        const std::shared_ptr<VersionMapEntry>& entry) {
        std::vector<AtomKey> missing_versions;

        iterate_keys_of_type_for_stream(store, KeyType::TABLE_INDEX, stream_id, [&entry, &missing_versions] (const auto& vk) {
            const auto& key = to_atom(vk);
            auto it = std::find_if(std::begin(entry->keys_), std::end(entry->keys_), [&] (const auto& entry_key) {
                return entry_key.type() == KeyType::VERSION
                    && std::tie(key.id(), key.version_id()) == std::tie(entry_key.id(), entry_key.version_id());
            });
            if(it == std::end(entry->keys_)) {
                util::check(static_cast<bool>(entry->head_) || entry->empty(), "Expected head to be set after load via iteration");
                if(!entry->head_ || std::tie(key.id(), key.version_id()) != std::tie(entry->head_.value().id(), entry->head_.value().version_id()))
                    missing_versions.push_back(key);
            }
        });
        return missing_versions;
    }

    std::vector<AtomKey> find_deleted_version_keys(std::shared_ptr<Store> store, StreamIdArg stream_id) {
        auto entry = std::make_shared<VersionMapEntry>();
        load_via_iteration(store, stream_id, entry);
        return find_deleted_version_keys_for_entry(store, stream_id, entry);
    }

    void recover_deleted(std::shared_ptr<Store> store, StreamIdArg stream_id) {
        auto &entry = get_entry(stream_id);
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
    // Backwards compat stuff
    AtomKey rewrite_old_journal_keys(std::shared_ptr<Store> store, const StreamId &stream_id, const std::shared_ptr<VersionMapEntry>& entry) {
        util::check(!entry->keys_.empty(), "Can't rewrite empty version journal entry");
        auto version_id = entry->keys_[0].version_id();
        entry->head_ = std::make_optional(write_entry_to_storage(store, stream_id, version_id, entry));
        return entry->head_.value();
    }

    std::shared_ptr<VersionMapEntry> load_from_old_journal_keys(std::shared_ptr<StreamSource> store, const StreamId &stream_id) {
        ARCTICDB_DEBUG(log::version(), "Attempting to iterate old journal keys");
        auto match_stream_id = [&stream_id](const AtomKey &k) { return k.id() == stream_id; };
        auto old_entry = build_version_map_entry_with_predicate_iteration(store, match_stream_id, stream_id,
                                                                          {KeyType::VERSION_JOURNAL});
        auto index_keys = old_entry->get_indexes(false);
        std::sort(index_keys.begin(), index_keys.end(), std::greater<>());
        auto entry = std::make_shared<VersionMapEntry>();
        entry->keys_.assign(index_keys.begin(), index_keys.end());
        return entry;
    }

    std::shared_ptr<VersionMapEntry> do_backwards_compat_check(std::shared_ptr<Store> store, StreamIdArg stream_id) {
        ARCTICDB_TRACE(log::version(), "Didn't find a ref entry, scanning for old-style journal keys");
        auto entry = get_entry(stream_id);
        if (auto old_entry = load_from_old_journal_keys(store, stream_id); !old_entry->keys_.empty()) {
            entry->keys_ = std::move(old_entry->keys_);
            entry->head_ = rewrite_old_journal_keys(store, stream_id, entry);
            delete_keys_of_type_for_stream_sync(store, stream_id, KeyType::VERSION_JOURNAL);
        }
        return entry;
    }

    std::vector<AtomKey> tombstone_from_key_or_all_internal(std::shared_ptr<Store> store, const StreamId& stream_id,
                                                            std::optional<AtomKey> first_key_to_tombstone = std::nullopt,
                                                            std::shared_ptr<VersionMapEntry> entry = nullptr) {
        if (!entry) {
            entry = check_reload(
                    store,
                    stream_id,
                    LoadParameter{LoadType::LOAD_UNDELETED},
                    __FUNCTION__);
        }
        if (!first_key_to_tombstone)
            first_key_to_tombstone = entry->get_first_index(false);

        std::vector<AtomKey> output;
        for (const auto& key : entry->keys_) {
            if (is_index_key_type(key.type()) && !entry->is_tombstoned(key)
                && key.version_id() <= first_key_to_tombstone->version_id()) {
                output.emplace_back(key);
            }
        }

        if (!output.empty()) {
            auto tombstone_key = write_tombstone_all_key(store, first_key_to_tombstone.value(), entry);
            if(log_changes_) {
                log_tombstone_all(store, stream_id, tombstone_key.version_id());
            }
        }
        return output;
    }
};

using VersionMap = VersionMapImpl<>;

} //namespace arcticdb
