/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/entity/key.hpp>
#include <arcticdb/entity/types.hpp>
#include <arcticdb/storage/store.hpp>
#include <arcticdb/stream/stream_sink.hpp>
#include <arcticdb/stream/index_aggregator.hpp>
#include <arcticdb/version/version_map_entry.hpp>
#include <arcticdb/entity/frame_and_descriptor.hpp>
#include <utility>
#include <memory>
#include <optional>

namespace arcticdb {

VariantKey write_multi_index_entry(
        std::shared_ptr<StreamSink> store, std::vector<AtomKey>& keys, const StreamId& stream_id,
        const py::object& metastruct, const py::object& user_meta, VersionId version_id
);

inline std::optional<AtomKey> read_segment_with_keys(
        const SegmentInMemory& seg, VersionMapEntry& entry, LoadProgress& load_progress
) {
    ssize_t row = 0;
    std::optional<AtomKey> next;
    VersionId oldest_loaded_index = std::numeric_limits<VersionId>::max();
    VersionId oldest_loaded_undeleted_index = std::numeric_limits<VersionId>::max();
    timestamp earliest_loaded_timestamp = std::numeric_limits<timestamp>::max();
    timestamp earliest_loaded_undeleted_timestamp = std::numeric_limits<timestamp>::max();
    timestamp latest_loaded_timestamp = std::numeric_limits<timestamp>::min();

    for (; row < ssize_t(seg.row_count()); ++row) {
        auto key = read_key_row(seg, row);
        ARCTICDB_TRACE(log::version(), "Reading key {}", key);

        if (is_index_key_type(key.type())) {
            entry.keys_.push_back(key);
            oldest_loaded_index = std::min(oldest_loaded_index, key.version_id());
            earliest_loaded_timestamp = std::min(earliest_loaded_timestamp, key.creation_ts());
            latest_loaded_timestamp = std::max(latest_loaded_timestamp, key.creation_ts());

            if (!entry.is_tombstoned(key)) {
                oldest_loaded_undeleted_index = std::min(oldest_loaded_undeleted_index, key.version_id());
                earliest_loaded_undeleted_timestamp = std::min(earliest_loaded_undeleted_timestamp, key.creation_ts());
            }

        } else if (key.type() == KeyType::TOMBSTONE) {
            entry.tombstones_.try_emplace(key.version_id(), key);
            entry.keys_.push_back(key);
        } else if (key.type() == KeyType::TOMBSTONE_ALL) {
            entry.try_set_tombstone_all(key);
            entry.keys_.push_back(key);
        } else if (key.type() == KeyType::VERSION) {
            entry.keys_.push_back(key);
            next = key;
            ++row;
            break;
        } else {
            util::raise_rte("Unexpected type in journal segment");
        }
    }
    util::check(row == ssize_t(seg.row_count()), "Unexpected ordering in journal segment");
    load_progress.oldest_loaded_index_version_ =
            std::min(load_progress.oldest_loaded_index_version_, oldest_loaded_index);
    load_progress.oldest_loaded_undeleted_index_version_ =
            std::min(load_progress.oldest_loaded_undeleted_index_version_, oldest_loaded_undeleted_index);
    load_progress.earliest_loaded_timestamp_ =
            std::min(load_progress.earliest_loaded_timestamp_, earliest_loaded_timestamp);
    load_progress.earliest_loaded_undeleted_timestamp_ =
            std::min(load_progress.earliest_loaded_undeleted_timestamp_, earliest_loaded_undeleted_timestamp);
    load_progress.is_earliest_version_loaded = !next.has_value();
    return next;
}

inline std::optional<AtomKey> read_segment_with_keys(
        const SegmentInMemory& seg, const std::shared_ptr<VersionMapEntry>& entry, LoadProgress& load_progress
) {
    return read_segment_with_keys(seg, *entry, load_progress);
}

template<class Predicate>
std::shared_ptr<VersionMapEntry> build_version_map_entry_with_predicate_iteration(
        const std::shared_ptr<StreamSource>& store, Predicate&& predicate, const StreamId& stream_id,
        const std::vector<KeyType>& key_types, bool perform_read_segment_with_keys = true
) {

    auto prefix = std::holds_alternative<StringId>(stream_id) ? std::get<StringId>(stream_id) : std::string();
    auto output = std::make_shared<VersionMapEntry>();
    std::vector<AtomKey> read_keys;
    for (auto key_type : key_types) {
        store->iterate_type(
                key_type,
                [&predicate, &read_keys, &store, &output, &perform_read_segment_with_keys](VariantKey&& vk) {
                    const auto& key = to_atom(std::move(vk));
                    if (!predicate(key))
                        return;

                    read_keys.push_back(key);
                    ARCTICDB_DEBUG(log::storage(), "Version map iterating key {}", key);
                    if (perform_read_segment_with_keys) {
                        auto [kv, seg] = store->read_sync(key);
                        LoadProgress load_progress;
                        (void)read_segment_with_keys(seg, output, load_progress);
                    }
                },
                prefix
        );
    }
    if (!perform_read_segment_with_keys) {
        output->keys_.insert(
                output->keys_.end(), std::move_iterator(read_keys.begin()), std::move_iterator(read_keys.end())
        );
        output->sort();
        // output->head_ isnt populated in this case
        return output;
    } else {
        if (output->keys_.empty())
            return output;
        util::check(!read_keys.empty(), "Expected there to be some read keys");
        auto latest_key =
                std::max_element(std::begin(read_keys), std::end(read_keys), [](const auto& left, const auto& right) {
                    return left.creation_ts() < right.creation_ts();
                });
        output->sort();
        output->head_ = *latest_key;
    }

    return output;
}

inline void check_is_version(const AtomKey& key) {
    util::check(key.type() == KeyType::VERSION, "Expected version key type but got {}", key);
}

inline void read_symbol_ref(
        const std::shared_ptr<StreamSource>& store, const StreamId& stream_id, VersionMapEntry& entry
) {
    std::pair<entity::VariantKey, SegmentInMemory> key_seg_pair;
    // Trying to read a missing ref key is expected e.g. when writing a previously missing symbol.
    // If the ref key is missing we keep the entry empty and should not raise warnings.
    storage::ReadKeyOpts read_opts;
    read_opts.dont_warn_about_missing_key = true;

    try {
        key_seg_pair = store->read_sync(RefKey{stream_id, KeyType::VERSION_REF}, read_opts);
    } catch (const storage::KeyNotFoundException&) {
        try {
            key_seg_pair = store->read_sync(RefKey{stream_id, KeyType::VERSION, true}, read_opts);
        } catch (const storage::KeyNotFoundException&) {
            return;
        }
    }

    LoadProgress load_progress;
    entry.head_ = read_segment_with_keys(key_seg_pair.second, entry, load_progress);
    entry.load_progress_ = load_progress;
}

inline void write_symbol_ref(
        std::shared_ptr<StreamSink> store, const AtomKey& latest_index, const std::optional<AtomKey>& previous_key,
        const AtomKey& journal_key
) {
    check_is_index_or_tombstone(latest_index);
    check_is_version(journal_key);
    if (previous_key)
        check_is_index_or_tombstone(*previous_key);

    ARCTICDB_DEBUG(
            log::version(),
            "Version map writing symbol ref for latest index: {} journal key {}",
            latest_index,
            journal_key
    );

    IndexAggregator<RowCountIndex> ref_agg(latest_index.id(), [&store, &latest_index](auto&& s) {
        auto segment = std::forward<decltype(s)>(s);
        store->write_sync(KeyType::VERSION_REF, latest_index.id(), std::move(segment));
    });
    ref_agg.add_key(latest_index);
    if (previous_key && is_index_key_type(latest_index.type()))
        ref_agg.add_key(*previous_key);

    ref_agg.add_key(journal_key);
    ref_agg.finalize();
    ARCTICDB_DEBUG(log::version(), "Done writing symbol ref for key: {}", journal_key);
}

// Given the latest version, and a negative index into the version map, returns the desired version ID or std::nullopt
// if it would be negative
inline std::optional<VersionId> get_version_id_negative_index(VersionId latest, SignedVersionId index) {
    internal::check<ErrorCode::E_ASSERTION_FAILURE>(
            index < 0, "get_version_id_negative_index expects a negative index, received {}", index
    );
    // +1 so that as_of=-1 returns the latest entry
    auto candidate_version_id = static_cast<SignedVersionId>(latest) + index + 1;
    return candidate_version_id >= 0 ? std::make_optional<VersionId>(static_cast<VersionId>(candidate_version_id))
                                     : std::nullopt;
}

std::unordered_map<StreamId, size_t> get_num_version_entries(const std::shared_ptr<Store>& store, size_t batch_size);

inline bool is_positive_version_query(const LoadStrategy& load_strategy) {
    return load_strategy.load_until_version_.value() >= 0;
}

inline bool continue_when_loading_version(
        const LoadStrategy& load_strategy, const LoadProgress& load_progress,
        const std::optional<VersionId>& latest_version
) {
    if (!load_strategy.load_until_version_)
        // Should continue when not loading down to a version
        return true;

    if (is_positive_version_query(load_strategy)) {
        if (load_progress.oldest_loaded_index_version_ > static_cast<VersionId>(*load_strategy.load_until_version_)) {
            // Should continue when version was not reached
            return true;
        }
    } else {
        if (latest_version.has_value()) {
            if (auto opt_version_id =
                        get_version_id_negative_index(*latest_version, *load_strategy.load_until_version_);
                opt_version_id && load_progress.oldest_loaded_index_version_ > *opt_version_id) {
                // Should continue when version was not reached
                return true;
            }
        } else {
            // Should continue if not yet reached any index key
            return true;
        }
    }
    ARCTICDB_DEBUG(
            log::version(),
            "Exiting load downto because loaded to version {} for request {} with {} total versions",
            load_progress.oldest_loaded_index_version_,
            *load_strategy.load_until_version_,
            latest_version.value()
    );
    return false;
}

inline void set_latest_version(
        const std::shared_ptr<VersionMapEntry>& entry, std::optional<VersionId>& latest_version
) {
    if (!latest_version) {
        auto latest = entry->get_first_index(true).first;
        if (latest)
            latest_version = latest->version_id();
    }
}

static constexpr timestamp nanos_to_seconds(timestamp nanos) { return nanos / timestamp(10000000000); }

inline bool continue_when_loading_from_time(const LoadStrategy& load_strategy, const LoadProgress& load_progress) {
    if (!load_strategy.load_from_time_)
        return true;

    auto loaded_deleted_or_undeleted_timestamp = load_strategy.should_include_deleted()
                                                         ? load_progress.earliest_loaded_timestamp_
                                                         : load_progress.earliest_loaded_undeleted_timestamp_;

    if (loaded_deleted_or_undeleted_timestamp > *load_strategy.load_from_time_)
        return true;

    ARCTICDB_DEBUG(
            log::version(),
            "Exiting load from timestamp because request {} <= {}",
            loaded_deleted_or_undeleted_timestamp,
            *load_strategy.load_from_time_
    );
    return false;
}

inline bool continue_when_loading_latest(
        const LoadStrategy& load_strategy, const std::shared_ptr<VersionMapEntry>& entry
) {
    if (!(load_strategy.load_type_ == LoadType::LATEST &&
          entry->get_first_index(load_strategy.should_include_deleted()).first))
        return true;

    ARCTICDB_DEBUG(
            log::version(),
            "Exiting because we found the latest version with include_deleted: {}",
            load_strategy.should_include_deleted()
    );
    return false;
}

inline bool continue_when_loading_undeleted(
        const LoadStrategy& load_strategy, const std::shared_ptr<VersionMapEntry>& entry,
        const LoadProgress& load_progress
) {
    if (load_strategy.should_include_deleted()) {
        return true;
    }

    if (entry->tombstone_all_) {
        // We need the check below because it is possible to have a tombstone_all which doesn't cover all version keys
        // after it. For example when we use prune_previous_versions (without write) we write a tombstone_all key which
        // applies to keys before the previous one. So it's possible the version chain can look like: v0 <- v1 <- v2 <-
        // tombstone_all(version=1) In this case we need to terminate at v1.
        const bool is_deleted_by_tombstone_all =
                entry->tombstone_all_->version_id() >= load_progress.oldest_loaded_index_version_;
        if (is_deleted_by_tombstone_all) {
            ARCTICDB_DEBUG(
                    log::version(),
                    "Exiting because tombstone all key deletes all versions beyond: {} and the oldest loaded index has "
                    "version: {}",
                    entry->tombstone_all_->version_id(),
                    load_progress.oldest_loaded_index_version_
            );
            return false;
        }
    }
    return true;
}

inline bool penultimate_key_contains_required_version_id(const AtomKey& key, const LoadStrategy& load_strategy) {
    if (is_positive_version_query(load_strategy)) {
        return key.version_id() <= static_cast<VersionId>(load_strategy.load_until_version_.value());
    } else {
        return *load_strategy.load_until_version_ == -1;
    }
}

inline bool key_exists_in_ref_entry(
        const LoadStrategy& load_strategy, const VersionMapEntry& ref_entry,
        std::optional<AtomKey>& cached_penultimate_key
) {
    // The 3 item ref key bypass can be used only when we are loading undeleted versions
    // because otherwise it might skip versions that are deleted but part of snapshots
    if (load_strategy.load_objective_ != LoadObjective::UNDELETED_ONLY)
        return false;

    if (load_strategy.load_type_ == LoadType::LATEST && is_index_key_type(ref_entry.keys_[0].type()))
        return true;

    if (cached_penultimate_key && is_partial_load_type(load_strategy.load_type_)) {
        load_strategy.validate();
        if (load_strategy.load_type_ == LoadType::DOWNTO &&
            penultimate_key_contains_required_version_id(*cached_penultimate_key, load_strategy)) {
            return true;
        }

        if (load_strategy.load_type_ == LoadType::FROM_TIME &&
            cached_penultimate_key->creation_ts() <= load_strategy.load_from_time_.value()) {
            return true;
        }
    }

    return false;
}

inline SortedValue deduce_sorted(SortedValue existing_frame, SortedValue input_frame) {
    using namespace arcticdb;
    constexpr auto UNKNOWN = SortedValue::UNKNOWN;
    constexpr auto ASCENDING = SortedValue::ASCENDING;
    constexpr auto DESCENDING = SortedValue::DESCENDING;
    constexpr auto UNSORTED = SortedValue::UNSORTED;

    SortedValue final_state;
    switch (existing_frame) {
    case UNKNOWN:
        if (input_frame != UNSORTED) {
            final_state = UNKNOWN;
        } else {
            final_state = UNSORTED;
        }
        break;
    case ASCENDING:
        if (input_frame == UNKNOWN) {
            final_state = UNKNOWN;
        } else if (input_frame != ASCENDING) {
            final_state = UNSORTED;
        } else {
            final_state = ASCENDING;
        }
        break;
    case DESCENDING:
        if (input_frame == UNKNOWN) {
            final_state = UNKNOWN;
        } else if (input_frame != DESCENDING) {
            final_state = UNSORTED;
        } else {
            final_state = DESCENDING;
        }
        break;
    default:
        final_state = UNSORTED;
        break;
    }
    return final_state;
}

FrameAndDescriptor frame_and_descriptor_from_segment(SegmentInMemory&& seg);

} // namespace arcticdb