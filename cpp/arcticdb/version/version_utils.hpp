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

#include <utility>
#include <memory>
#include <optional>

namespace arcticdb {
using namespace arcticdb::storage;
using namespace arcticdb::entity;
using namespace arcticdb::stream;

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

    IndexAggregator<RowCountIndex> multi_index_agg(stream_id, [&](auto &&segment) {
        multi_key_fut = store->write(KeyType::MULTI_KEY,
                                     version_id,  // version_id
                                     stream_id,
                                     0,  // start_index
                                     0,  // end_index
                                     std::forward<SegmentInMemory>(segment)).wait();
    });

    for (auto &key : keys) {
        multi_index_agg.add_key(to_atom(key));
    }
    google::protobuf::Any any = {};
    TimeseriesDescriptor metadata;

    if (!metastruct.is_none()) {
        arcticdb::proto::descriptors::UserDefinedMetadata multi_key_proto;
        python_util::pb_from_python(metastruct, multi_key_proto);
        metadata.mutable_proto().mutable_multi_key_meta()->CopyFrom(multi_key_proto);
    }
    if (!user_meta.is_none()) {
        arcticdb::proto::descriptors::UserDefinedMetadata user_meta_proto;
        python_util::pb_from_python(user_meta, user_meta_proto);
        metadata.mutable_proto().mutable_user_meta()->CopyFrom(user_meta_proto);
    }
    any.PackFrom(metadata.proto());
    multi_index_agg.set_metadata(std::move(any));

    multi_index_agg.commit();
    return multi_key_fut.wait().value();
}

inline std::pair<std::optional<AtomKey>, VersionId> read_segment_with_keys(
    const SegmentInMemory &seg,
    VersionMapEntry &entry) {
    ssize_t row = 0;
    std::optional<AtomKey> next;
    VersionId oldest_loaded_index = std::numeric_limits<VersionId>::max();
    for (; row < ssize_t(seg.row_count()); ++row) {
        auto key = read_key_row(seg, row);
        ARCTICDB_TRACE(log::version(), "Reading key {}", key);
        if (is_index_key_type(key.type())) {
            entry.keys_.push_back(key);
            oldest_loaded_index = std::min(oldest_loaded_index, key.version_id());
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
    return std::make_pair(next, oldest_loaded_index);
}

inline std::pair<std::optional<AtomKey>, VersionId> read_segment_with_keys(
    const SegmentInMemory &seg,
    const std::shared_ptr<VersionMapEntry> &entry) {
    return read_segment_with_keys(seg, *entry);
}

template<class Predicate>
std::shared_ptr<VersionMapEntry> build_version_map_entry_with_predicate_iteration(
    const std::shared_ptr<StreamSource> &store,
    Predicate &&predicate,
    const StreamId &stream_id,
    std::vector<KeyType> key_types,
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
                                    read_segment_with_keys(seg, output);
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

inline std::optional<RefKey> get_symbol_ref_key(
    const std::shared_ptr<StreamSource> &store,
    const StreamId &stream_id) {
    auto ref_key = RefKey{stream_id, KeyType::VERSION_REF};
    if (store->key_exists_sync(ref_key))
        return std::make_optional(std::move(ref_key));

    // Old style ref_key
    ARCTICDB_DEBUG(log::version(), "Ref key {} not found, trying old style ref key", ref_key);
    ref_key = RefKey{stream_id, KeyType::VERSION, true};
    if (!store->key_exists_sync(ref_key))
        return std::nullopt;

    ARCTICDB_DEBUG(log::version(), "Found old-style ref key", ref_key);
    return std::make_optional(std::move(ref_key));
}

inline void read_symbol_ref(std::shared_ptr<StreamSource> store, const StreamId &stream_id, VersionMapEntry &entry) {
    auto maybe_ref_key = get_symbol_ref_key(store, stream_id);
    if (!maybe_ref_key)
        return;

    auto [key, seg] = store->read_sync(maybe_ref_key.value());
    VersionId version_id{};
    std::tie(entry.head_, version_id) = read_segment_with_keys(seg, entry);
}

inline void write_symbol_ref(std::shared_ptr<StreamSink> store,
                             const AtomKey &latest_index,
                             const AtomKey &journal_key) {
    check_is_index_or_tombstone(latest_index);
    check_is_version(journal_key);
    ARCTICDB_DEBUG(log::version(), "Version map writing symbol ref for latest index: {} journal key {}", latest_index,
                   journal_key);

    IndexAggregator<RowCountIndex> ref_agg(latest_index.id(), [&store, &latest_index](auto &&s) {
        auto segment = std::forward<SegmentInMemory>(s);
        store->write_sync(KeyType::VERSION_REF, latest_index.id(), std::move(segment));
    });
    ref_agg.add_key(latest_index);
    ref_agg.add_key(journal_key);
    ref_agg.commit();
    ARCTICDB_DEBUG(log::version(), "Done writing symbol ref for key: {}", journal_key);
}

std::unordered_map<StreamId, size_t> get_num_version_entries(const std::shared_ptr<Store> &store, size_t batch_size);

inline bool need_to_load_further(const LoadParameter &load_params, VersionId loaded_until) {
    if (!load_params.load_until_ || loaded_until > load_params.load_until_.value())
        return true;

    ARCTICDB_DEBUG(log::version(),
                   "Exiting load downto because request {} <= {}",
                   load_params.load_until_.value(),
                   loaded_until);
    return false;
}

inline bool load_latest_ongoing(const LoadParameter &load_params, const std::shared_ptr<VersionMapEntry> &entry) {
    if (!(load_params.load_type_ == LoadType::LOAD_LATEST_UNDELETED && entry->get_first_index(false)) &&
        !(load_params.load_type_ == LoadType::LOAD_LATEST && entry->get_first_index(true)))
        return true;

    ARCTICDB_DEBUG(log::version(), "Exiting because we found a non-deleted index in load latest");
    return false;
}

inline bool looking_for_undeleted(const LoadParameter& load_params, const std::shared_ptr<VersionMapEntry>& entry, const VersionId& oldest_loaded_index) {
    if(load_params.load_type_ != LoadType::LOAD_UNDELETED) {
        return true;
    }

    if(entry->tombstone_all_) {
        const bool is_deleted_by_tombstone_all = entry->tombstone_all_->version_id() >= oldest_loaded_index;
        if(is_deleted_by_tombstone_all) {
            ARCTICDB_DEBUG(
                log::version(),
                "Exiting because tombstone all key deletes all versions beyond: {} and the oldest loaded index has version: {}",
                entry->tombstone_all_->version_id(),
                oldest_loaded_index);
            return false;
        } else {
            return true;
        }
    } else {
        return true;
    }
}

void fix_stream_ids_of_index_keys(
    const std::shared_ptr<Store> &store,
    const StreamId &stream_id,
    const std::shared_ptr<VersionMapEntry> &entry);

inline SortedValue deduce_sorted(SortedValue existing_frame, SortedValue input_frame) {
    using namespace arcticdb;
    constexpr auto UNKNOWN = SortedValue::UNKNOWN;
    constexpr auto ASCENDING = SortedValue::ASCENDING;
    constexpr auto DESCENDING = SortedValue::DESCENDING;
    constexpr auto UNSORTED = SortedValue::UNSORTED;

    auto final_state = UNSORTED;
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

} // namespace arcticdb