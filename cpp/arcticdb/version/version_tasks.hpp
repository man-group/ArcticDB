/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/async/base_task.hpp>
#include <arcticdb/pipeline/read_frame.hpp>
#include <arcticdb/pipeline/index_utils.hpp>
#include <arcticdb/pipeline/query.hpp>
#include <arcticdb/version/version_store_objects.hpp>
#include <arcticdb/version/version_map.hpp>
#include <arcticdb/util/key_utils.hpp>

namespace arcticdb {

using VersionIdentifier = std::variant<VersionedItem, StreamId, std::shared_ptr<pipelines::PreloadedIndexQuery>>;

struct UpdateMetadataTask : async::BaseTask {
    const std::shared_ptr<Store> store_;
    const version_store::UpdateInfo update_info_;
    const AtomKey index_key_;
    arcticdb::proto::descriptors::UserDefinedMetadata user_meta_;
    VersionId version_id_ = 0;

    UpdateMetadataTask(
            std::shared_ptr<Store> store, version_store::UpdateInfo update_info,
            arcticdb::proto::descriptors::UserDefinedMetadata&& user_meta
    ) :
        store_(std::move(store)),
        update_info_(std::move(update_info)),
        user_meta_(std::move(user_meta)) {}

    AtomKey operator()() const {
        ARCTICDB_RUNTIME_DEBUG(log::version(), "Command: update metadata");
        util::check(
                update_info_.previous_index_key_.has_value(),
                "Cannot update metadata as there is no previous index key to update"
        );
        auto index_key = *(update_info_.previous_index_key_);
        auto segment = store_->read_sync(index_key).second;

        segment.mutable_index_descriptor().mutable_proto().mutable_user_meta()->CopyFrom(user_meta_);
        return to_atom(store_->write_sync(
                index_key.type(),
                update_info_.next_version_id_,
                index_key.id(),
                index_key.start_index(),
                index_key.end_index(),
                std::move(segment)
        ));
    }
};

struct MaybeDeletedAtomKey {
    AtomKey key;
    bool deleted;
};

struct AsyncRestoreVersionTask : async::BaseTask {
    const std::shared_ptr<Store> store_;
    std::shared_ptr<VersionMap> version_map_;
    const StreamId stream_id_;
    const AtomKey index_key_;
    std::optional<MaybeDeletedAtomKey> maybe_prev_;

    AsyncRestoreVersionTask(
            std::shared_ptr<Store> store, std::shared_ptr<VersionMap> version_map, StreamId stream_id,
            entity::AtomKey index_key, std::optional<MaybeDeletedAtomKey> maybe_prev
    ) :
        store_(std::move(store)),
        version_map_(std::move(version_map)),
        stream_id_(std::move(stream_id)),
        index_key_(std::move(index_key)),
        maybe_prev_(std::move(maybe_prev)) {}

    folly::Future<std::pair<VersionedItem, TimeseriesDescriptor>> operator()() {
        using namespace arcticdb::pipelines;
        auto [index_segment_reader, slice_and_keys] = index::read_index_to_vector(store_, index_key_);

        if (maybe_prev_ && !maybe_prev_->deleted && maybe_prev_->key.version_id() == index_key_.version_id()) {
            folly::Promise<std::pair<VersionedItem, TimeseriesDescriptor>> promise;
            auto future = promise.getFuture();
            promise.setTry(folly::Try(std::make_pair(VersionedItem{index_key_}, index_segment_reader.tsd())));
            return future;
        }

        auto tsd = std::make_shared<TimeseriesDescriptor>(index_segment_reader.tsd().clone());
        auto sk = std::make_shared<std::vector<SliceAndKey>>(std::move(slice_and_keys));
        auto version_id = get_next_version_from_key(maybe_prev_->key);
        std::vector<folly::Future<VariantKey>> fut_keys;
        for (const auto& slice_and_key : *sk)
            fut_keys.emplace_back(store_->copy(slice_and_key.key().type(), stream_id_, version_id, slice_and_key.key())
            );

        return folly::collect(fut_keys)
                .via(&async::io_executor())
                .thenValue([sk](auto keys) {
                    std::vector<SliceAndKey> res;
                    res.reserve(keys.size());
                    for (std::size_t i = 0; i < res.capacity(); ++i) {
                        res.emplace_back(SliceAndKey{(*sk)[i].slice_, std::move(to_atom(keys[i]))});
                    }
                    return res;
                })
                .thenValue([store = store_, version_map = version_map_, tsd = tsd, stream_id = stream_id_, version_id](
                                   auto&& new_slice_and_keys
                           ) {
                    auto index = index_type_from_descriptor(tsd->as_stream_descriptor());
                    return index::index_and_version(index, store, *tsd, new_slice_and_keys, stream_id, version_id);
                })
                .thenValue([store = store_, version_map = version_map_, tsd = tsd](auto versioned_item) {
                    version_map->write_version(store, versioned_item.key_, std::nullopt);
                    return std::make_pair(versioned_item, *tsd);
                });
    }
};

struct CheckReloadTask : async::BaseTask {
    const std::shared_ptr<Store> store_;
    const std::shared_ptr<VersionMap> version_map_;
    const StreamId stream_id_;
    const LoadStrategy load_strategy_;

    CheckReloadTask(
            std::shared_ptr<Store> store, std::shared_ptr<VersionMap> version_map, StreamId stream_id,
            LoadStrategy load_strategy
    ) :
        store_(std::move(store)),
        version_map_(std::move(version_map)),
        stream_id_(std::move(stream_id)),
        load_strategy_(load_strategy) {}

    std::shared_ptr<VersionMapEntry> operator()() const {
        return version_map_->check_reload(store_, stream_id_, load_strategy_, __FUNCTION__);
    }
};

struct SetupPipelineContextTask : async::BaseTask {
    const std::shared_ptr<Store> store_;
    const VersionIdentifier version_info_;
    const std::shared_ptr<ReadQuery> read_query_;
    const ReadOptions read_options_;

    SetupPipelineContextTask(
            std::shared_ptr<Store> store, VersionIdentifier version_info, std::shared_ptr<ReadQuery> read_query,
            ReadOptions read_options
    ) :
        store_(std::move(store)),
        version_info_(std::move(version_info)),
        read_query_(std::move(read_query)),
        read_options_(std::move(read_options)) {}

    std::shared_ptr<pipelines::PipelineContext> operator()() const;
};

struct WriteVersionTask : async::BaseTask {
    const std::shared_ptr<Store> store_;
    const std::shared_ptr<VersionMap> version_map_;
    const AtomKey key_;
    const std::optional<AtomKey> previous_key_;

    WriteVersionTask(
            std::shared_ptr<Store> store, std::shared_ptr<VersionMap> version_map, AtomKey key,
            const std::optional<AtomKey>& previous_key
    ) :
        store_(std::move(store)),
        version_map_(std::move(version_map)),
        key_(std::move(key)),
        previous_key_(previous_key) {}

    folly::Unit operator()() {
        ScopedLock lock(version_map_->get_lock_object(key_.id()));
        version_map_->write_version(store_, key_, previous_key_);
        return folly::Unit{};
    }
};

struct WriteTombstonesTask : async::BaseTask {
    const std::shared_ptr<Store> store_;
    const std::shared_ptr<VersionMap> version_map_;
    const std::vector<AtomKey> keys_;
    const StreamId stream_id_;
    const std::shared_ptr<VersionMapEntry> entry_;

    WriteTombstonesTask(
            std::shared_ptr<Store> store, std::shared_ptr<VersionMap> version_map, std::vector<AtomKey> keys,
            StreamId stream_id, std::shared_ptr<VersionMapEntry> entry
    ) :
        store_(std::move(store)),
        version_map_(std::move(version_map)),
        keys_(std::move(keys)),
        stream_id_(std::move(stream_id)),
        entry_(std::move(entry)) {}

    folly::Future<AtomKey> operator()() {
        ScopedLock lock(version_map_->get_lock_object(stream_id_));
        return version_map_->write_tombstones(store_, keys_, stream_id_, entry_);
    }
};

struct WriteAndPrunePreviousTask : async::BaseTask {
    const std::shared_ptr<Store> store_;
    const std::shared_ptr<VersionMap> version_map_;
    const AtomKey key_;
    const std::optional<AtomKey> maybe_prev_;

    WriteAndPrunePreviousTask(
            std::shared_ptr<Store> store, std::shared_ptr<VersionMap> version_map, AtomKey key,
            std::optional<AtomKey> maybe_prev
    ) :
        store_(std::move(store)),
        version_map_(std::move(version_map)),
        key_(std::move(key)),
        maybe_prev_(std::move(maybe_prev)) {}

    folly::Future<std::vector<AtomKey>> operator()() {
        ScopedLock lock(version_map_->get_lock_object(key_.id()));
        return version_map_->write_and_prune_previous(store_, key_, maybe_prev_);
    }
};

struct TombstoneAllTask : async::BaseTask {
    const std::shared_ptr<Store> store_;
    const std::shared_ptr<VersionMap> version_map_;
    const StreamId stream_id_;
    const std::optional<AtomKey> maybe_prev_;
    const std::optional<std::shared_ptr<VersionMapEntry>> entry_;

    TombstoneAllTask(
            std::shared_ptr<Store> store, std::shared_ptr<VersionMap> version_map, StreamId stream_id,
            std::optional<AtomKey> maybe_prev, std::optional<std::shared_ptr<VersionMapEntry>> entry
    ) :
        store_(std::move(store)),
        version_map_(std::move(version_map)),
        stream_id_(std::move(stream_id)),
        maybe_prev_(std::move(maybe_prev)),
        entry_(std::move(entry)) {}

    folly::Future<std::pair<VersionId, std::vector<AtomKey>>> operator()() {
        ScopedLock lock(version_map_->get_lock_object(stream_id_));
        return version_map_->tombstone_from_key_or_all(store_, stream_id_, maybe_prev_, entry_);
    }
};

} // namespace arcticdb
