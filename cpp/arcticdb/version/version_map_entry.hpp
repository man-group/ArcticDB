/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
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

enum class LoadType : uint32_t { NOT_LOADED = 0, LATEST, DOWNTO, FROM_TIME, ALL, UNKNOWN };

inline constexpr bool is_partial_load_type(LoadType load_type) {
    return load_type == LoadType::DOWNTO || load_type == LoadType::FROM_TIME;
}

// Used to specify whether we want to load all or only undeleted versions
enum class LoadObjective : uint32_t { INCLUDE_DELETED, UNDELETED_ONLY };

enum class VersionStatus { LIVE, TOMBSTONED, NEVER_EXISTED };

struct VersionDetails {
    std::optional<AtomKey> key_;
    VersionStatus version_status_;
};

// The LoadStrategy describes how to load versions from the version chain. It consists of:
// load_type: Describes up to which point in the chain we need to go.
// load_objective: Whether to include tombstoned versions
struct LoadStrategy {
    explicit LoadStrategy(LoadType load_type, LoadObjective load_objective) :
        load_type_(load_type),
        load_objective_(load_objective) {}

    LoadStrategy(LoadType load_type, LoadObjective load_objective, int64_t load_from_time_or_until) :
        load_type_(load_type),
        load_objective_(load_objective) {
        switch (load_type_) {
        case LoadType::FROM_TIME:
            load_from_time_ = load_from_time_or_until;
            break;
        case LoadType::DOWNTO:
            load_until_version_ = load_from_time_or_until;
            break;
        default:
            internal::raise<ErrorCode::E_ASSERTION_FAILURE>(
                    "LoadStrategy constructor with load_from_time_or_until parameter {} provided invalid load_type {}",
                    load_from_time_or_until,
                    static_cast<uint32_t>(load_type)
            );
        }
    }

    LoadType load_type_ = LoadType::NOT_LOADED;
    LoadObjective load_objective_ = LoadObjective::INCLUDE_DELETED;
    std::optional<SignedVersionId> load_until_version_ = std::nullopt;
    std::optional<timestamp> load_from_time_ = std::nullopt;

    bool should_include_deleted() const {
        switch (load_objective_) {
        case LoadObjective::INCLUDE_DELETED:
            return true;
        case LoadObjective::UNDELETED_ONLY:
            return false;
        default:
            util::raise_rte("Invalid load_objective: {}", load_objective_);
        }
    }

    void validate() const {
        internal::check<ErrorCode::E_ASSERTION_FAILURE>(
                (load_type_ == LoadType::DOWNTO) == load_until_version_.has_value(),
                "Invalid load parameter: load_type {} with load_util {}",
                int(load_type_),
                load_until_version_.value_or(VersionId{})
        );
        internal::check<ErrorCode::E_ASSERTION_FAILURE>(
                (load_type_ == LoadType::FROM_TIME) == load_from_time_.has_value(),
                "Invalid load parameter: load_type {} with load_from_time_ {}",
                int(load_type_),
                load_from_time_.value_or(timestamp{})
        );
    }
};

inline bool is_undeleted_strategy_subset(const LoadStrategy& left, const LoadStrategy& right) {
    switch (left.load_type_) {
    case LoadType::NOT_LOADED:
        return true;
    case LoadType::LATEST:
        // LATEST is not a subset of DOWNTO because DOWNTO may not reach the latest undeleted version.
        return right.load_type_ != LoadType::NOT_LOADED && right.load_type_ != LoadType::DOWNTO;
    case LoadType::DOWNTO:
        if (right.load_type_ == LoadType::ALL) {
            return true;
        }
        if (right.load_type_ == LoadType::DOWNTO &&
            ((left.load_until_version_.value() >= 0) == (right.load_until_version_.value() >= 0))) {
            // Left is subset of right only when the [load_until]s have same sign and left's version is >= right's
            // version
            return left.load_until_version_.value() >= right.load_until_version_.value();
        }
        break;
    case LoadType::FROM_TIME:
        if (right.load_type_ == LoadType::ALL) {
            return true;
        }
        if (right.load_type_ == LoadType::FROM_TIME) {
            return left.load_from_time_.value() >= right.load_from_time_.value();
        }
        break;
    case LoadType::ALL:
        return right.load_type_ == LoadType::ALL;
    default:
        util::raise_rte("Invalid load type: {}", left.load_type_);
    }
    return false;
}

// Returns a strategy which is guaranteed to load all versions requested by left and right.
// Works only on strategies with include_deleted=false.
inline LoadStrategy union_of_undeleted_strategies(const LoadStrategy& left, const LoadStrategy& right) {
    internal::check<ErrorCode::E_ASSERTION_FAILURE>(
            !left.should_include_deleted(),
            "Trying to produce a union of undeleted strategies but left strategy includes deleted."
    );
    internal::check<ErrorCode::E_ASSERTION_FAILURE>(
            !right.should_include_deleted(),
            "Trying to produce a union of undeleted strategies but right strategy includes deleted."
    );
    if (is_undeleted_strategy_subset(left, right)) {
        return right;
    }
    if (is_undeleted_strategy_subset(right, left)) {
        return left;
    }
    // If none is subset of the other, then we should load all versions. We can't be less conservative because we can't
    // know where to load to with strategies which have a different load type. E.g. for FROM_TIME and DOWNTO
    // we can't know where to read to unless we know the version chain.
    // A possible workaround for this is to restructure loading the version chain to get a set of LoadStrategies and
    // stop searching only when all of them are satisfied.
    return LoadStrategy{LoadType::ALL, LoadObjective::UNDELETED_ONLY};
}

template<typename T>
bool deque_is_unique(std::deque<T> vec) {
    sort(vec.begin(), vec.end());
    auto it = std::unique(vec.begin(), vec.end());
    return it == vec.end();
}

inline bool is_tombstone_key_type(const AtomKey& key) {
    return key.type() == KeyType::TOMBSTONE || key.type() == KeyType::TOMBSTONE_ALL;
}

inline bool is_index_or_tombstone(const AtomKey& key) {
    return is_index_key_type(key.type()) || is_tombstone_key_type(key);
}

inline void check_is_index_or_tombstone(const AtomKey& key) {
    util::check(is_index_or_tombstone(key), "Expected index or tombstone key type but got {}", key);
}

inline AtomKey index_to_tombstone(const AtomKey& index_key, const StreamId& stream_id, timestamp creation_ts) {
    return atom_key_builder()
            .version_id(index_key.version_id())
            .creation_ts(creation_ts)
            .content_hash(index_key.content_hash())
            .start_index(index_key.start_index())
            .end_index(index_key.end_index())
            .build(stream_id, KeyType::TOMBSTONE);
}

inline AtomKey index_to_tombstone(VersionId version_id, const StreamId& stream_id, timestamp creation_ts) {
    return atom_key_builder()
            .version_id(version_id)
            .creation_ts(creation_ts)
            .content_hash(0)
            .start_index(NumericIndex{0}) // TODO why not the one from the index key?
            .end_index(NumericIndex{0})
            .build(stream_id, KeyType::TOMBSTONE);
}

inline AtomKey get_tombstone_all_key(const AtomKey& latest, timestamp creation_ts) {
    return atom_key_builder()
            .version_id(latest.version_id())
            .creation_ts(creation_ts)
            .content_hash(latest.content_hash())
            .start_index(latest.start_index())
            .end_index(latest.end_index())
            .build(latest.id(), KeyType::TOMBSTONE_ALL);
}

struct LoadProgress {
    VersionId oldest_loaded_index_version_ = std::numeric_limits<VersionId>::max();
    VersionId oldest_loaded_undeleted_index_version_ = std::numeric_limits<VersionId>::max();
    timestamp earliest_loaded_timestamp_ = std::numeric_limits<timestamp>::max();
    timestamp earliest_loaded_undeleted_timestamp_ = std::numeric_limits<timestamp>::max();
    bool is_earliest_version_loaded{false};
};

struct VersionMapEntry {
    /*
      VersionMapEntry is all the data we have in-memory about each stream_id in the version map which in its essence
      is a map of StreamId: VersionMapEntry. It's created from the linked-list-like structure that we have in the
      storage, where the head_ points to the latest version and keys_ are basically all the index/version keys
      loaded in memory in a deque - based on the load_strategy.

      load_strategy signifies the current state of the in memory structure vs the state on disk, where LATEST will
      just load the latest version, and ALL loads everything in memory by going through the linked list on disk.

      It also contains a map of version_ids and the tombstone key corresponding to it iff it has been pruned or
      explicitly deleted.
     */
    VersionMapEntry() = default;

    ARCTICDB_MOVE_COPY_DEFAULT(VersionMapEntry)

    void sort() {
        util::check(!head_, "Expect sort to be called on newly read entry");

        if (keys_.empty())
            return;

        // Sorting by creation_ts is safe from clock skew because we don't support parallel writes to the same symbol.
        std::sort(std::begin(keys_), std::end(keys_), [](const AtomKey& l, const AtomKey& r) {
            return l.creation_ts() > r.creation_ts();
        });
    }

    void clear() {
        head_.reset();
        last_reload_time_ = 0;
        tombstones_.clear();
        tombstone_all_.reset();
        keys_.clear();
        load_progress_ = LoadProgress{};
    }

    bool empty() const { return !head_; }

    friend void swap(VersionMapEntry& left, VersionMapEntry& right) noexcept {
        using std::swap;
        left.validate();
        right.validate();

        swap(left.keys_, right.keys_);
        swap(left.tombstones_, right.tombstones_);
        swap(left.last_reload_time_, right.last_reload_time_);
        swap(left.tombstone_all_, right.tombstone_all_);
        swap(left.head_, right.head_);
        swap(left.load_progress_, right.load_progress_);
    }

    // Below four functions used to return optional<AtomKey> of the tombstone, but copying keys is expensive and only
    // one function was actually interested in the key, so they now return bool. See get_tombstone().
    bool has_individual_tombstone(VersionId version_id) const { return tombstones_.contains(version_id); }

    bool is_tombstoned_via_tombstone_all(VersionId version_id) const {
        return tombstone_all_ && tombstone_all_->version_id() >= version_id;
    }

    bool is_tombstoned(const AtomKey& key) const {
        return is_tombstoned_via_tombstone_all(key.version_id()) || has_individual_tombstone(key.version_id());
    }

    bool is_tombstoned(VersionId version_id) const {
        return is_tombstoned_via_tombstone_all(version_id) || has_individual_tombstone(version_id);
    }

    std::optional<AtomKey> get_tombstone(VersionId version_id) {
        if (tombstone_all_ && tombstone_all_->version_id() >= version_id) {
            return tombstone_all_;
        }
        auto it = tombstones_.find(version_id);
        return it != tombstones_.end() ? std::make_optional<AtomKey>(it->second) : std::nullopt;
    }

    std::string dump() const {
        std::ostringstream strm;
        strm << fmt::format("\nLast reload time: {}\n", last_reload_time_);

        if (head_)
            strm << fmt::format("Head: {}\n", *head_);

        if (tombstone_all_)
            strm << fmt::format("Tombstone all: {}\n", *tombstone_all_);

        strm << "Keys: \n\n";
        for (const auto& key : keys_)
            strm << fmt::format("    {}\n", key);

        strm << "Tombstones: \n\n";
        for (const auto& tombstone : tombstones_)
            strm << fmt::format("    {} - {}\n", tombstone.first, tombstone.second);

        return strm.str();
    }

    void unshift_key(const AtomKey& key) { keys_.push_front(key); }

    std::vector<IndexTypeKey> get_indexes(bool include_deleted) const {
        std::vector<AtomKey> output;
        for (const auto& key : keys_) {
            if (is_index_key_type(key.type()) && (include_deleted || !is_tombstoned(key)))
                output.emplace_back(key);
        }
        return output;
    }

    std::vector<IndexTypeKey> get_tombstoned_indexes() const {
        std::vector<AtomKey> output;
        for (const auto& key : keys_) {
            if (is_index_key_type(key.type()) && is_tombstoned(key))
                output.emplace_back(key);
        }
        return output;
    }

    std::pair<std::optional<AtomKey>, bool> get_first_index(bool include_deleted) const {
        for (const auto& key : keys_) {
            if (is_index_key_type(key.type())) {
                const auto tombstoned = is_tombstoned(key);
                if (!tombstoned || include_deleted)
                    return {key, tombstoned};
            }
        }
        return {std::nullopt, false};
    }

    std::optional<AtomKey> get_second_undeleted_index() const {
        std::optional<AtomKey> output;
        bool found_first = false;
        for (const auto& key : keys_) {
            if (is_index_key_type(key.type()) && !is_tombstoned(key)) {
                if (!found_first) {
                    found_first = true;
                } else {
                    output = key;
                    break;
                }
            }
        }
        return output;
    }

    void check_ordering() const {
        if (empty())
            return;

        auto first_index = std::find_if(std::begin(keys_), std::end(keys_), [](const auto& key) {
            return is_index_key_type(key.type());
        });
        if (keys_.size() == 2 && is_tombstone_key_type(keys_[0]))
            return;
        util::check(first_index != std::end(keys_), "Didn't find any index keys");
        auto version_id = first_index->version_id();
        std::optional<timestamp> version_timestamp;
        for (const auto& key : keys_) {
            if (key.type() == KeyType::VERSION) {
                if (!version_timestamp)
                    version_timestamp = key.creation_ts();
                else {
                    util::check(
                            key.creation_ts() <= *version_timestamp,
                            "out of order timestamp: {} > {}",
                            key.creation_ts(),
                            *version_timestamp
                    );
                }
            }
            if (is_index_key_type(key.type())) {
                util::check(
                        key.version_id() <= version_id,
                        "Out of order version ids: {} > {}",
                        key.version_id(),
                        version_id
                );
                version_id = key.version_id();
            }
        }
    }

    void check_head() const {
        if (head_) {
            auto it = std::find_if(keys_.begin(), keys_.end(), [&](auto k) { return head_.value() == k; });
            util::check(
                    it == keys_.end(),
                    "Head should not be in the keys list as this is causing a duplication, head {}, keys {}",
                    fmt::format("{}", head_.value()),
                    fmt::format("{}", keys_)
            );
        } else {
            util::check(keys_.empty(), "Head should be set when there are keys, keys: {}", fmt::format("{}", keys_));
        }
    }

    void check_stream_id() const {
        if (empty())
            return;

        std::unordered_map<StreamId, std::vector<VersionId>> id_to_version_id;
        if (head_)
            id_to_version_id[head_->id()].push_back(head_->version_id());
        for (const auto& k : keys_)
            id_to_version_id[k.id()].push_back(k.version_id());
        util::check_rte(
                id_to_version_id.size() == 1, "Multiple symbols in keys: {}", fmt::format("{}", id_to_version_id)
        );
    }

    void try_set_tombstone_all(const AtomKey& key) {
        util::check(key.type() == KeyType::TOMBSTONE_ALL, "Can't set tombstone all key with key {}", key);
        if (!tombstone_all_ || tombstone_all_->version_id() < key.version_id())
            tombstone_all_ = key;
    }

    void validate() const {
        if (!head_ && keys_.empty())
            return;
        check_is_index_or_tombstone(keys_[0]);
        check_ordering();
        util::check(deque_is_unique(keys_), "Keys deque is not unique, keys: {}", fmt::format("{}", keys_));
        check_head();
        check_stream_id();
    }

    void validate_types() const {
        util::check(
                std::all_of(
                        keys_.begin(),
                        keys_.end(),
                        [](const AtomKey& key) {
                            return is_index_key_type(key.type()) || key.type() == KeyType::VERSION ||
                                   is_tombstone_key_type(key);
                        }
                ),
                "Unexpected key types in write entry"
        );
    }

    std::optional<AtomKey> head_;
    timestamp last_reload_time_ = 0;
    LoadProgress load_progress_;
    std::deque<AtomKey> keys_;
    std::unordered_map<VersionId, AtomKey> tombstones_;
    std::optional<AtomKey> tombstone_all_;
};

inline bool is_live_index_type_key(const AtomKeyImpl& key, const std::shared_ptr<VersionMapEntry>& entry) {
    return is_index_key_type(key.type()) && !entry->is_tombstoned(key);
}

inline std::optional<VersionId> get_prev_version_in_entry(
        const std::shared_ptr<VersionMapEntry>& entry, VersionId version_id
) {
    // sorted in decreasing order
    // entry->keys_ is not sorted in version_id anymore (due to tombstones), we only need to fetch live index keys
    // which will be sorted on version_id
    auto index_keys = entry->get_indexes(false);

    if (auto iterator_lt = std::upper_bound(
                std::begin(index_keys),
                std::end(index_keys),
                version_id,
                [&](VersionId v_id, const AtomKey& key) { return key.version_id() < v_id; }
        );
        iterator_lt != index_keys.end()) {
        return {iterator_lt->version_id()};
    }
    return std::nullopt;
}

inline std::optional<VersionId> get_next_version_in_entry(
        const std::shared_ptr<VersionMapEntry>& entry, VersionId version_id
) {
    // sorted in decreasing order
    // entry->keys_ is not sorted in version_id any more (due to tombstones), we only need to fetch live index keys
    // which will be sorted on version_id
    auto index_keys = entry->get_indexes(false);

    if (auto iterator_gt = std::lower_bound(
                std::begin(index_keys),
                std::end(index_keys),
                version_id,
                [&](const AtomKey& key, VersionId v_id) { return key.version_id() > v_id; }
        );
        iterator_gt != index_keys.begin()) {
        iterator_gt--;
        return {iterator_gt->version_id()};
    }
    return std::nullopt;
}

inline VersionDetails find_index_key_for_version_id_and_tombstone_status(
        VersionId version_id, const std::shared_ptr<VersionMapEntry>& entry
) {
    auto key = std::find_if(std::begin(entry->keys_), std::end(entry->keys_), [version_id](const auto& key) {
        return is_index_key_type(key.type()) && key.version_id() == version_id;
    });
    if (key == std::end(entry->keys_))
        return VersionDetails{std::nullopt, VersionStatus::NEVER_EXISTED};
    return VersionDetails{*key, entry->is_tombstoned(*key) ? VersionStatus::TOMBSTONED : VersionStatus::LIVE};
}

inline std::optional<AtomKey> find_index_key_for_version_id(
        VersionId version_id, const std::shared_ptr<VersionMapEntry>& entry, bool included_deleted = true
) {
    auto version_details = find_index_key_for_version_id_and_tombstone_status(version_id, entry);
    if ((version_details.version_status_ == VersionStatus::TOMBSTONED && included_deleted) ||
        version_details.version_status_ == VersionStatus::LIVE)
        return version_details.key_;
    else
        return std::nullopt;
}

inline std::optional<std::pair<AtomKey, AtomKey>> get_latest_key_pair(const std::shared_ptr<VersionMapEntry>& entry) {
    if (entry->head_ && !entry->keys_.empty()) {
        auto journal_key = entry->head_.value();
        auto index_key = entry->get_first_index(false).first;
        util::check(static_cast<bool>(index_key), "Did not find undeleted version");
        return std::make_pair(std::move(*index_key), std::move(journal_key));
    }
    return std::nullopt;
}

inline void remove_duplicate_index_keys(const std::shared_ptr<VersionMapEntry>& entry) {
    auto& keys = entry->keys_;
    keys.erase(std::unique(keys.begin(), keys.end()), keys.end());
}
} // namespace arcticdb

namespace fmt {
template<>
struct formatter<arcticdb::LoadType> {
    template<typename ParseContext>
    constexpr auto parse(ParseContext& ctx) {
        return ctx.begin();
    }

    template<typename FormatContext>
    auto format(arcticdb::LoadType l, FormatContext& ctx) const {
        switch (l) {
        case arcticdb::LoadType::NOT_LOADED:
            return fmt::format_to(ctx.out(), "NOT_LOADED");
        case arcticdb::LoadType::LATEST:
            return fmt::format_to(ctx.out(), "LATEST");
        case arcticdb::LoadType::DOWNTO:
            return fmt::format_to(ctx.out(), "DOWNTO");
        case arcticdb::LoadType::ALL:
            return fmt::format_to(ctx.out(), "ALL");
        default:
            arcticdb::util::raise_rte("Unrecognized load type {}", int(l));
        }
    }
};

template<>
struct formatter<arcticdb::LoadObjective> {
    template<typename ParseContext>
    constexpr auto parse(ParseContext& ctx) {
        return ctx.begin();
    }

    template<typename FormatContext>
    auto format(arcticdb::LoadObjective l, FormatContext& ctx) const {
        switch (l) {
        case arcticdb::LoadObjective::INCLUDE_DELETED:
            return fmt::format_to(ctx.out(), "ANY");
        case arcticdb::LoadObjective::UNDELETED_ONLY:
            return fmt::format_to(ctx.out(), "UNDELETED_ONLY");
        default:
            arcticdb::util::raise_rte("Unrecognized to load {}", int(l));
        }
    }
};
} // namespace fmt
