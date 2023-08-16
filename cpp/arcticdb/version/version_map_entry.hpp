/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

/*
 * version_map_entry contains the class VersionMapEntry which encapsulates the data contained for each stream_id
 * in the version_map. (See VersionMapEntry)
 */
#pragma once

#include <arcticdb/entity/atom_key.hpp>

#include <deque>
#include <vector>

namespace arcticdb {
using namespace arcticdb::entity;
using namespace arcticdb::stream;

enum class LoadType :
        uint32_t {
    NOT_LOADED = 0,
    LOAD_LATEST = 1,
    LOAD_LATEST_UNDELETED = 2,
    LOAD_DOWNTO = 3,
    LOAD_UNDELETED = 4,
    LOAD_FROM_TIME = 5,
    LOAD_ALL = 6
};

inline constexpr bool is_latest_load_type(LoadType load_type) {
    return load_type == LoadType::LOAD_LATEST || load_type == LoadType::LOAD_LATEST_UNDELETED;
}

inline constexpr bool is_partial_load_type(LoadType load_type) {
    return load_type == LoadType::LOAD_DOWNTO || load_type == LoadType::LOAD_FROM_TIME;
}

struct LoadParameter {
    explicit LoadParameter(LoadType load_type) :
        load_type_(load_type) {
    }

    LoadParameter(LoadType load_type, int64_t load_from_time_or_until) :
        load_type_(load_type) {
        switch(load_type_) {
            case LoadType::LOAD_FROM_TIME:
                load_from_time_ = load_from_time_or_until;
                break;
            case LoadType::LOAD_DOWNTO:
                load_until_ = load_from_time_or_until;
                break;
            default:
                internal::raise<ErrorCode::E_ASSERTION_FAILURE>("LoadParameter constructor with load_from_time_or_until parameter {} provided invalid load_type {}",
                                                                load_from_time_or_until, static_cast<uint32_t>(load_type));
        }
    }

    LoadType load_type_ = LoadType::NOT_LOADED;
    std::optional<SignedVersionId> load_until_ = std::nullopt;
    std::optional<timestamp> load_from_time_ = std::nullopt;
    bool use_previous_ = false;
    bool skip_compat_ = true;
    bool iterate_on_failure_ = false;

    void validate() const {
        util::check((load_type_ == LoadType::LOAD_DOWNTO) == load_until_.has_value(),
                    "Invalid load parameter: load_type {} with load_util {}", int(load_type_), load_until_.value_or(VersionId{}));
        util::check((load_type_ == LoadType::LOAD_FROM_TIME) == load_from_time_.has_value(),
            "Invalid load parameter: load_type {} with load_from_time_ {}", int(load_type_), load_from_time_.value_or(timestamp{}));
    }
};

template<typename T>
bool deque_is_unique(std::deque<T> vec) {
    sort(vec.begin(), vec.end());
    auto it = std::unique(vec.begin(), vec.end());
    return it == vec.end();
}

inline bool is_tombstone_key_type(const AtomKey& key) {
    return key.type() == KeyType::TOMBSTONE || key.type() == KeyType::TOMBSTONE_ALL;
}

inline bool is_index_or_tombstone(const AtomKey &key) {
    return is_index_key_type(key.type()) || is_tombstone_key_type(key);
}

inline void check_is_index_or_tombstone(const AtomKey &key) {
    util::check(is_index_or_tombstone(key), "Expected index or tombstone key type but got {}", key);
}

inline AtomKey index_to_tombstone(const AtomKey &index_key, StreamId stream_id, timestamp creation_ts) {
    return atom_key_builder()
        .version_id(index_key.version_id())
        .creation_ts(creation_ts)
        .content_hash(index_key.content_hash())
        .start_index(index_key.start_index())
        .end_index(index_key.end_index())
        .build(stream_id, KeyType::TOMBSTONE);
}

inline AtomKey index_to_tombstone(VersionId version_id, StreamId stream_id, timestamp creation_ts) {
    return atom_key_builder()
        .version_id(version_id)
        .creation_ts(creation_ts)
        .content_hash(0)
        .start_index(0)
        .end_index(0)
        .build(stream_id, KeyType::TOMBSTONE);
}

inline AtomKey get_tombstone_all_key(const AtomKey &latest, timestamp creation_ts) {
    return atom_key_builder()
        .version_id(latest.version_id())
        .creation_ts(creation_ts)
        .content_hash(latest.content_hash())
        .start_index(latest.start_index())
        .end_index(latest.end_index())
        .build(latest.id(), KeyType::TOMBSTONE_ALL);
}

struct VersionMapEntry {
    /*
      VersionMapEntry is all the data we have in-memory about each stream_id in the version map which in its essence
      is a map of StreamId: VersionMapEntry. It's created from the linked-list-like structure that we have in the
      storage, where the head_ points to the latest version and keys_ are basically all the index/version keys
      loaded in memory in a deque - based on the load_type.

      load_type signifies the current state of the in memory structure vs the state on disk, where LOAD_LATEST will
      just load the latest version, and LOAD_ALL loads everything in memory by going through the linked list on disk.

      It also contains a map of version_ids and the tombstone key corresponding to it iff it has been pruned or
      explicitly deleted.
     */
    VersionMapEntry() = default;

    ARCTICDB_MOVE_COPY_DEFAULT(VersionMapEntry)

    void sort() {
        util::check(!head_, "Expect sort to be called on newly read entry");

        if (keys_.empty())
            return;

        std::sort(std::begin(keys_), std::end(keys_), [](const AtomKey &l, const AtomKey &r) {
            return l.creation_ts() > r.creation_ts();
        });
    }

    void clear() {
        head_.reset();
        load_type_ = LoadType::NOT_LOADED;
        last_reload_time_ = 0;
        tombstones_.clear();
        tombstone_all_.reset();
        keys_.clear();
    }

    bool empty() const {
        return !head_;
    }

    friend void swap(VersionMapEntry &left, VersionMapEntry &right) noexcept {
        using std::swap;
        left.validate();
        right.validate();

        swap(left.keys_, right.keys_);
        swap(left.tombstones_, right.tombstones_);
        swap(left.last_reload_time_, right.last_reload_time_);
        swap(left.tombstone_all_, right.tombstone_all_);
        swap(left.head_, right.head_);
        swap(left.load_type_, right.load_type_);
    }

    // Below four functions used to return optional<AtomKey> of the tombstone, but copying keys is expensive and only
    // one function was actually interested in the key, so they now return bool. See get_tombstone_if_any().
    bool has_individual_tombstone(VersionId version_id) const {
        return tombstones_.count(version_id) != 0;
    }

    bool is_tombstoned_via_tombstone_all(VersionId version_id) const {
        return tombstone_all_ && tombstone_all_.value().version_id() >= version_id;
    }

    bool is_tombstoned(const AtomKey &key) const {
        return is_tombstoned_via_tombstone_all(key.version_id()) || has_individual_tombstone(key.version_id());
    }

    bool is_tombstoned(VersionId version_id) const {
        return is_tombstoned_via_tombstone_all(version_id) || has_individual_tombstone(version_id);
    }

    std::optional<AtomKey> get_tombstone_if_any(VersionId version_id) {
        if (tombstone_all_ && tombstone_all_.value().version_id() >= version_id) {
            return tombstone_all_;
        }
        auto it = tombstones_.find(version_id);
        return it != tombstones_.end() ? std::make_optional<AtomKey>(it->second) : std::nullopt;
    }

    std::string dump() const {
        std::ostringstream strm;
        strm << std::endl << "Last reload time: " << last_reload_time_ << std::endl;

        if(head_)
            strm << "Head: " << fmt::format("{}", head_.value()) << std::endl;

        if(tombstone_all_)
            strm << "Tombstone all: " << fmt::format("{}", tombstone_all_.value()) << std::endl;

        strm << "Keys: " << std::endl << std::endl;
        for(const auto& key: keys_)
            strm << fmt::format("    {}", key) << std::endl;

        strm << "Tombstones: " << std::endl << std::endl;
        for(const auto& tombstone: tombstones_)
            strm << fmt::format("    {} - {}", tombstone.first, tombstone.second) << std::endl;

        return strm.str();
    }

    void unshift_key(const AtomKey& key) {
        keys_.push_front(key);
    }

    std::vector<IndexTypeKey> get_indexes(bool include_deleted) const {
        std::vector<AtomKey> output;
        for (const auto &key: keys_) {
            if (is_index_key_type(key.type()) && (include_deleted || !is_tombstoned(key)))
                output.emplace_back(key);
        }
        return output;
    }

    std::vector<IndexTypeKey> get_tombstoned_indexes() const {
        std::vector<AtomKey> output;
        for (const auto &key: keys_) {
            if (is_index_key_type(key.type()) && is_tombstoned(key))
                output.emplace_back(key);
        }
        return output;
    }

    std::optional<AtomKey> get_first_index(bool include_deleted) const {
        std::optional<AtomKey> output;
        for (const auto &key: keys_) {
            if (is_index_key_type(key.type()) && (include_deleted || !is_tombstoned(key))) {
                output = key;
                break;
            }
        }
        return output;
    }

    void check_ordering() const {
        if(empty())
            return;

        auto first_index = std::find_if(std::begin(keys_), std::end(keys_),
                                        [](const auto &key) { return is_index_key_type(key.type()); });
        if(keys_.size() == 2 && is_tombstone_key_type(keys_[0]))
            return;
        util::check(first_index != std::end(keys_), "Didn't find any index keys");
        auto version_id = first_index->version_id();
        std::optional<timestamp> version_timestamp;
        for (const auto& key : keys_) {
            if(key.type() == KeyType::VERSION) {
                if(!version_timestamp)
                    version_timestamp = key.creation_ts();
                else {
                    util::check(key.creation_ts() <= version_timestamp.value(), "out of order timestamp: {} > {}", key.creation_ts(), version_timestamp.value());
                }
            }
            if (is_index_key_type(key.type())) {
                util::check(key.version_id() <= version_id, "Out of order version ids");
                version_id = key.version_id();
            }
        }
    }

    void check_head() const {
        if (head_) {
            auto it = std::find_if(keys_.begin(), keys_.end(), [&](auto k) {
                return head_.value() == k;
            });
            util::check(it == keys_.end(), "If keys are present head should be set");
        } else {
            util::check(keys_.empty(), "Head should be set when there are keys");
        }
    }

    void check_stream_id() const {
        if (empty())
            return;

        std::unordered_map<StreamId, std::vector<VersionId>> id_to_version_id;
        if (head_)
            id_to_version_id[head_.value().id()].push_back(head_.value().version_id());
        for (const auto& k: keys_)
            id_to_version_id[k.id()].push_back(k.version_id());
        util::check_rte(id_to_version_id.size() == 1, "Multiple symbols in keys: {}", fmt::format("{}", id_to_version_id));
    }

    void try_set_tombstone_all(const AtomKey& key) {
        util::check(key.type() == KeyType::TOMBSTONE_ALL, "Can't set tombstone all key with key {}", key);
        if(!tombstone_all_ || tombstone_all_.value().version_id() < key.version_id())
            tombstone_all_ = key;
    }

    void validate() const {
        if (!head_ && keys_.empty())
            return;
        check_is_index_or_tombstone(keys_[0]);
        check_ordering();
        util::check(deque_is_unique(keys_), "Keys deque is not unique");
        check_head();
        check_stream_id();
    }

    void validate_types() const {
        util::check(std::all_of(keys_.begin(), keys_.end(),
                                [](const AtomKey &key) {
                                    return is_index_key_type(key.type()) || key.type() == KeyType::VERSION || is_tombstone_key_type(key);
                                }),
                    "Unexpected key types in write entry");
    }

    std::optional<AtomKey> head_;
    LoadType load_type_ = LoadType::NOT_LOADED;
    timestamp last_reload_time_ = 0;
    VersionId loaded_until_ = 0;
    std::deque<AtomKey> keys_;
    std::unordered_map<VersionId, AtomKey> tombstones_;
    std::optional<AtomKey> tombstone_all_;
};

inline bool is_live_index_type_key(const AtomKeyImpl& key, const std::shared_ptr<VersionMapEntry>& entry) {
    return is_index_key_type(key.type()) && !entry->is_tombstoned(key);
}

inline std::optional<VersionId> get_prev_version_in_entry(const std::shared_ptr<VersionMapEntry>& entry, VersionId version_id) {
    //sorted in decreasing order
    //entry->keys_ is not sorted in version_id anymore (due to tombstones), we only need to fetch live index keys
    //which will be sorted on version_id
    auto index_keys = entry->get_indexes(false);

    if (auto iterator_lt = std::upper_bound(std::begin(index_keys), std::end(index_keys), version_id,
                                            [&](VersionId v_id, const AtomKey &key) {
                                                return key.version_id() < v_id;
                                            });iterator_lt != index_keys.end()) {
        return {iterator_lt->version_id()};
    }
    return std::nullopt;
}

inline std::optional<VersionId> get_next_version_in_entry(const std::shared_ptr<VersionMapEntry>& entry, VersionId version_id) {
    //sorted in decreasing order
    //entry->keys_ is not sorted in version_id any more (due to tombstones), we only need to fetch live index keys
    //which will be sorted on version_id
    auto index_keys = entry->get_indexes(false);

    if (auto iterator_gt = std::lower_bound(std::begin(index_keys), std::end(index_keys), version_id,
                                            [&](const AtomKey &key, VersionId v_id) {
                                                return key.version_id() > v_id;
                                            }); iterator_gt != index_keys.begin()) {
        iterator_gt--;
        return {iterator_gt->version_id()};
    }
    return std::nullopt;
}

inline std::optional<AtomKey> find_index_key_for_version_id(
    VersionId version_id,
    const std::shared_ptr<VersionMapEntry>& entry,
    bool included_deleted=true) {
    auto key = std::find_if(std::begin(entry->keys_), std::end(entry->keys_), [version_id] (const auto& key) {
        return is_index_key_type(key.type()) && key.version_id() == version_id;
    });
    if(key == std::end(entry->keys_))
        return std::nullopt;

    return included_deleted || !entry->is_tombstoned(*key) ? std::make_optional(*key) : std::nullopt;
}

inline std::optional<std::pair<AtomKey, AtomKey>> get_latest_key_pair(const std::shared_ptr<VersionMapEntry>& entry) {
    if (entry->head_ && !entry->keys_.empty()) {
        auto journal_key = entry->head_.value();
        auto index_key = entry->get_first_index(false);
        util::check(static_cast<bool>(index_key), "Did not find undeleted version");
        return std::make_pair(std::move(index_key.value()), std::move(journal_key));
    }
    return std::nullopt;
}

inline void remove_duplicate_index_keys(const std::shared_ptr<VersionMapEntry>& entry) {
    entry->keys_.erase(std::unique(entry->keys_.begin(), entry->keys_.end()), entry->keys_.end());
}
}

namespace fmt {
    template<>
    struct formatter<arcticdb::LoadType> {
        template<typename ParseContext>
        constexpr auto parse(ParseContext &ctx) { return ctx.begin(); }

    template<typename FormatContext>
    auto format(const arcticdb::LoadType &l, FormatContext &ctx) const {
        switch(l) {
        case arcticdb::LoadType::NOT_LOADED:
            return format_to(ctx.out(), "NOT_LOADED");
        case arcticdb::LoadType::LOAD_LATEST:
            return format_to(ctx.out(), "LOAD_LATEST");
        case arcticdb::LoadType::LOAD_LATEST_UNDELETED:
            return format_to(ctx.out(), "LOAD_LATEST_UNDELETED");
        case arcticdb::LoadType::LOAD_DOWNTO:
            return format_to(ctx.out(), "LOAD_DOWNTO");
        case arcticdb::LoadType::LOAD_UNDELETED:
            return format_to(ctx.out(), "LOAD_UNDELETED");
        case arcticdb::LoadType::LOAD_ALL:
            return format_to(ctx.out(), "LOAD_ALL");
        default:
            arcticdb::util::raise_rte("Unrecognized load type {}", int(l));
        }
    }
};
}
