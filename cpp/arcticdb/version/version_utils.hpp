/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/entity/key.hpp>
#include <arcticdb/entity/types.hpp>
#include <arcticdb/storage/store.hpp>
#include <arcticdb/stream/stream_sink.hpp>
#include <arcticdb/stream/index_aggregator.hpp>
#include <arcticdb/version/version_map_entry.hpp>
#include <arcticdb/python/python_utils.hpp>
#include <arcticdb/entity/frame_and_descriptor.hpp>

#include <utility>
#include <memory>
#include <optional>

namespace arcticdb {

inline entity::VariantKey write_multi_index_entry(
    std::shared_ptr<StreamSink> store,
    std::vector<AtomKey> &keys,
    const StreamId &stream_id,
    const py::object &metastruct,
    const py::object &user_meta,
    VersionId version_id
) {
    ARCTICDB_SAMPLE(WriteJournalEntry, 0)
    ARCTICDB_DEBUG(log::version(), "Version map writing multi key");
    folly::Future<VariantKey> multi_key_fut = folly::Future<VariantKey>::makeEmpty();

    IndexAggregator<RowCountIndex> multi_index_agg(stream_id, [&multi_key_fut, &store, version_id, stream_id](auto &&segment) {
        multi_key_fut = store->write(KeyType::MULTI_KEY,
                                     version_id,  // version_id
                                     stream_id,
                                     NumericIndex{0},  // start_index
                                     NumericIndex{0},  // end_index
                                     std::forward<decltype(segment)>(segment)).wait();
    });

    for (auto &key : keys) {
        multi_index_agg.add_key(to_atom(key));
    }
    TimeseriesDescriptor timeseries_descriptor;

    if (!metastruct.is_none()) {
        arcticdb::proto::descriptors::UserDefinedMetadata multi_key_proto;
        python_util::pb_from_python(metastruct, multi_key_proto);
        timeseries_descriptor.set_multi_key_metadata(std::move(multi_key_proto));
    }
    if (!user_meta.is_none()) {
        arcticdb::proto::descriptors::UserDefinedMetadata user_meta_proto;
        python_util::pb_from_python(user_meta, user_meta_proto);
        timeseries_descriptor.set_user_metadata(std::move(user_meta_proto));
    }
    multi_index_agg.set_timeseries_descriptor(timeseries_descriptor);
    multi_index_agg.commit();
    return multi_key_fut.wait().value();
}

inline std::optional<AtomKey> read_segment_with_keys(
    const SegmentInMemory &seg,
    VersionMapEntry &entry,
    LoadProgress& load_progress) {
    ssize_t row = 0;
    std::optional<AtomKey> next;
    VersionId oldest_loaded_index = std::numeric_limits<VersionId>::max();
    VersionId oldest_loaded_undeleted_index = std::numeric_limits<VersionId>::max();
    timestamp earliest_loaded_timestamp = std::numeric_limits<timestamp>::max();
    timestamp earliest_loaded_undeleted_timestamp = std::numeric_limits<timestamp>::max();

    for (; row < ssize_t(seg.row_count()); ++row) {
        auto key = read_key_row(seg, row);
        ARCTICDB_TRACE(log::version(), "Reading key {}", key);

        if (is_index_key_type(key.type())) {
            entry.keys_.push_back(key);
            oldest_loaded_index = std::min(oldest_loaded_index, key.version_id());
            earliest_loaded_timestamp = std::min(earliest_loaded_timestamp, key.creation_ts());

            if(!entry.is_tombstoned(key)) {
                oldest_loaded_undeleted_index = std::min(oldest_loaded_undeleted_index, key.version_id());
                earliest_loaded_undeleted_timestamp = std::min(earliest_loaded_timestamp, key.creation_ts());
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
    load_progress.oldest_loaded_index_version_ = std::min(load_progress.oldest_loaded_index_version_, oldest_loaded_index);
    load_progress.oldest_loaded_undeleted_index_version_ = std::min(load_progress.oldest_loaded_undeleted_index_version_, oldest_loaded_undeleted_index);
    load_progress.earliest_loaded_timestamp_ = std::min(load_progress.earliest_loaded_timestamp_, earliest_loaded_timestamp);
    load_progress.earliest_loaded_undeleted_timestamp_ = std::min(load_progress.earliest_loaded_undeleted_timestamp_, earliest_loaded_undeleted_timestamp);
    return next;
}

inline std::optional<AtomKey> read_segment_with_keys(
    const SegmentInMemory &seg,
    const std::shared_ptr<VersionMapEntry> &entry,
    LoadProgress& load_progress) {
    return read_segment_with_keys(seg, *entry, load_progress);
}

template<class Predicate>
std::shared_ptr<VersionMapEntry> build_version_map_entry_with_predicate_iteration(
    const std::shared_ptr<StreamSource> &store,
    Predicate &&predicate,
    const StreamId &stream_id,
    const std::vector<KeyType>& key_types,
    bool perform_read_segment_with_keys = true) {

    auto prefix = std::holds_alternative<StringId>(stream_id) ? std::get<StringId>(stream_id) : std::string();
    auto output = std::make_shared<VersionMapEntry>();
    std::vector<AtomKey> read_keys;
    for (auto key_type : key_types) {
        store->iterate_type(key_type,
                            [&predicate, &read_keys, &store, &output, &perform_read_segment_with_keys](VariantKey &&vk) {
                                const auto &key = to_atom(std::move(vk));
                                if (!predicate(key))
                                    return;

                                read_keys.push_back(key);
                                ARCTICDB_DEBUG(log::storage(), "Version map iterating key {}", key);
                                if (perform_read_segment_with_keys) {
                                    auto [kv, seg] = store->read_sync(to_atom(key));
                                    LoadProgress load_progress;
                                    (void)read_segment_with_keys(seg, output, load_progress);
                                }
                            },
                            prefix);
    }
    if (!perform_read_segment_with_keys) {
        output->keys_.insert(output->keys_.end(),
                             std::move_iterator(read_keys.begin()),
                             std::move_iterator(read_keys.end()));
        output->sort();
        // output->head_ isnt populated in this case
        return output;
    } else {
        if (output->keys_.empty())
            return output;
        util::check(!read_keys.empty(), "Expected there to be some read keys");
        auto latest_key = std::max_element(std::begin(read_keys), std::end(read_keys),
                                           [](const auto &left, const auto &right) {
                                               return left.creation_ts() < right.creation_ts();
                                           });
        output->sort();
        output->head_ = *latest_key;
    }

    return output;
}

inline void check_is_version(const AtomKey &key) {
    util::check(key.type() == KeyType::VERSION, "Expected version key type but got {}", key);
}

inline void read_symbol_ref(const std::shared_ptr<StreamSource>& store, const StreamId &stream_id, VersionMapEntry &entry) {
    std::pair<entity::VariantKey, SegmentInMemory> key_seg_pair;
    // Trying to read a missing ref key is expected e.g. when writing a previously missing symbol.
    // If the ref key is missing we keep the entry empty and should not raise warnings.
    auto read_opts = storage::ReadKeyOpts{};
    read_opts.dont_warn_about_missing_key=true;
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
    entry.loaded_with_progress_ = load_progress;
}

inline void write_symbol_ref(std::shared_ptr<StreamSink> store,
                             const AtomKey &latest_index,
                             const std::optional<AtomKey>& previous_key,
                             const AtomKey &journal_key) {
    check_is_index_or_tombstone(latest_index);
    check_is_version(journal_key);
    if(previous_key)
        check_is_index_or_tombstone(*previous_key);

    ARCTICDB_DEBUG(log::version(), "Version map writing symbol ref for latest index: {} journal key {}", latest_index,
                   journal_key);

    IndexAggregator<RowCountIndex> ref_agg(latest_index.id(), [&store, &latest_index](auto &&s) {
        auto segment = std::forward<decltype(s)>(s);
        store->write_sync(KeyType::VERSION_REF, latest_index.id(), std::move(segment));
    });
    ref_agg.add_key(latest_index);
    if(previous_key && is_index_key_type(latest_index.type()))
        ref_agg.add_key(*previous_key);

    ref_agg.add_key(journal_key);
    ref_agg.commit();
    ARCTICDB_DEBUG(log::version(), "Done writing symbol ref for key: {}", journal_key);
}

// Given the latest version, and a negative index into the version map, returns the desired version ID or std::nullopt if it would be negative
inline std::optional<VersionId> get_version_id_negative_index(VersionId latest, SignedVersionId index) {
    internal::check<ErrorCode::E_ASSERTION_FAILURE>(index < 0, "get_version_id_negative_index expects a negative index, received {}", index);
    // +1 so that as_of=-1 returns the latest entry
    auto candidate_version_id = static_cast<SignedVersionId>(latest) + index + 1;
    return candidate_version_id >= 0 ? std::make_optional<VersionId>(static_cast<VersionId>(candidate_version_id)) : std::nullopt;
}

std::unordered_map<StreamId, size_t> get_num_version_entries(const std::shared_ptr<Store> &store, size_t batch_size);

inline bool is_positive_version_query(const LoadStrategy& load_strategy) {
    return load_strategy.load_until_version_.value() >= 0;
}

inline bool loaded_until_version_id(const LoadStrategy &load_strategy, const LoadProgress& load_progress, const std::optional<VersionId>& latest_version) {
    if (!load_strategy.load_until_version_)
        return false;

    if (is_positive_version_query(load_strategy)) {
        if (load_progress.oldest_loaded_index_version_ > static_cast<VersionId>(*load_strategy.load_until_version_)) {
            return false;
        }
    } else {
        if (latest_version.has_value()) {
            if (auto opt_version_id = get_version_id_negative_index(*latest_version, *load_strategy.load_until_version_);
                opt_version_id && load_progress.oldest_loaded_index_version_ > *opt_version_id) {
                    return false;
            }
        } else {
            return false;
        }
    }
    ARCTICDB_DEBUG(log::version(),
                   "Exiting load downto because loaded to version {} for request {} with {} total versions",
                   load_progress.oldest_loaded_index_version_,
                   *load_strategy.load_until_version_,
                   latest_version.value()
                  );
    return true;
}

inline void set_latest_version(const std::shared_ptr<VersionMapEntry>& entry, std::optional<VersionId>& latest_version) {
    if (!latest_version) {
        auto latest = entry->get_first_index(true).first;
        if(latest)
            latest_version = latest->version_id();
    }
}

static constexpr timestamp nanos_to_seconds(timestamp nanos) {
    return nanos / timestamp(10000000000);
}

inline bool loaded_until_timestamp(const LoadStrategy &load_strategy, const LoadProgress& load_progress) {
    if (!load_strategy.load_from_time_)
        return false;

    auto loaded_deleted_or_undeleted_timestamp = load_strategy.should_include_deleted() ? load_progress.earliest_loaded_timestamp_ : load_progress.earliest_loaded_undeleted_timestamp_;

    if (loaded_deleted_or_undeleted_timestamp > *load_strategy.load_from_time_)
        return false;

    ARCTICDB_DEBUG(log::version(),
                   "Exiting load from timestamp because request {} <= {}",
                   loaded_deleted_or_undeleted_timestamp,
                   *load_strategy.load_from_time_);
    return true;
}

inline bool load_latest_ongoing(const LoadStrategy &load_strategy, const std::shared_ptr<VersionMapEntry> &entry) {
    if (!(load_strategy.load_type_ == LoadType::LOAD_LATEST && entry->get_first_index(load_strategy.should_include_deleted()).first))
        return true;

    ARCTICDB_DEBUG(log::version(), "Exiting because we found the latest version with include_deleted: {}", load_strategy.should_include_deleted());
    return false;
}

inline bool looking_for_undeleted(const LoadStrategy& load_strategy, const std::shared_ptr<VersionMapEntry>& entry, const LoadProgress& load_progress) {
    if(load_strategy.should_include_deleted()) {
        return true;
    }

    if(entry->tombstone_all_) {
        const bool is_deleted_by_tombstone_all = entry->tombstone_all_->version_id() >= load_progress.oldest_loaded_index_version_;
        if(is_deleted_by_tombstone_all) {
            ARCTICDB_DEBUG(
                log::version(),
                "Exiting because tombstone all key deletes all versions beyond: {} and the oldest loaded index has version: {}",
                entry->tombstone_all_->version_id(),
                load_progress.oldest_loaded_index_version_);
            return false;
        } else {
            return true;
        }
    } else {
        return true;
    }
}

inline bool penultimate_key_contains_required_version_id(const AtomKey& key, const LoadStrategy& load_strategy) {
    if(is_positive_version_query(load_strategy)) {
        return key.version_id() <= static_cast<VersionId>(load_strategy.load_until_version_.value());
    } else {
        return *load_strategy.load_until_version_ == -1;
    }
}

inline bool key_exists_in_ref_entry(const LoadStrategy& load_strategy, const VersionMapEntry& ref_entry, std::optional<AtomKey>& cached_penultimate_key) {
    if (load_strategy.load_type_ == LoadType::LOAD_LATEST && is_index_key_type(ref_entry.keys_[0].type()))
        return true;

    if(cached_penultimate_key && is_partial_load_type(load_strategy.load_type_)) {
        load_strategy.validate();
        if(load_strategy.load_type_ == LoadType::LOAD_DOWNTO && penultimate_key_contains_required_version_id(*cached_penultimate_key, load_strategy)) {
            return true;
        }

        if(load_strategy.load_type_ == LoadType::LOAD_FROM_TIME && cached_penultimate_key->creation_ts() <= load_strategy.load_from_time_.value()) {
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
    default:final_state = UNSORTED;
        break;
    }
    return final_state;
}

FrameAndDescriptor frame_and_descriptor_from_segment(SegmentInMemory&& seg);

} // namespace arcticdb