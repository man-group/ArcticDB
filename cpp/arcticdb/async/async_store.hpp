/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/async/task_scheduler.hpp>
#include <arcticdb/util/clock.hpp>
#include <arcticdb/storage/store.hpp>
#include <arcticdb/entity/performance_tracing.hpp>
#include <arcticdb/storage/library.hpp>
#include <folly/futures/Future.h>
#include <arcticdb/async/tasks.hpp>
#include <arcticdb/stream/stream_utils.hpp>
#include <arcticdb/processing/clause.hpp>
#include <arcticdb/storage/key_segment_pair.hpp>

namespace arcticdb::async {

std::pair<VariantKey, std::optional<Segment>> lookup_match_in_dedup_map(
    const std::shared_ptr<DeDupMap> &de_dup_map,
    storage::KeySegmentPair&& key_seg);

template <typename Callable>
auto read_and_continue(const VariantKey& key, std::shared_ptr<storage::Library> library, const storage::ReadKeyOpts& opts, Callable&& c) {
    return async::submit_io_task(ReadCompressedTask{key, library, opts, std::forward<decltype(c)>(c)})
        .via(&async::cpu_executor())
        .thenValue([](auto &&result) mutable {
            auto&& [key_seg, continuation] = std::forward<decltype(result)>(result);
            return continuation(std::move(key_seg));
        }
    );
}

/*
 * AsyncStore is a wrapper around a Store that provides async methods for writing and reading data.
 * It is used by the VersionStore to write data to the Store asynchronously.
 * It also can be used to write data synchronously (using the `*_sync` methods) and
 * to write batch of data (using the `batch_*` methods).
 * It can return both uncompressed `SegmentInMemory` or compressed `Segment` depending on the method.
 */
template<class ClockType = util::SysClock>
class AsyncStore : public Store {
public:
    AsyncStore(
        std::shared_ptr<storage::Library> library,
        const arcticdb::proto::encoding::VariantCodec &codec,
        EncodingVersion encoding_version
    ) :
        library_(std::move(library)),
        codec_(std::make_shared<arcticdb::proto::encoding::VariantCodec>(codec)),
        encoding_version_(encoding_version) {
    }

    folly::Future<entity::VariantKey> write(
        stream::KeyType key_type,
        VersionId version_id,
        const StreamId &stream_id,
        IndexValue start_index,
        IndexValue end_index,
        SegmentInMemory &&segment) override {

        util::check(segment.descriptor().id() == stream_id,
                    "Descriptor id mismatch in atom key {} != {}",
                    stream_id,
                    segment.descriptor().id());

        return async::submit_cpu_task(EncodeAtomTask{
            key_type, version_id, stream_id, start_index, end_index, current_timestamp(),
            std::move(segment), codec_, encoding_version_
        })
            .via(&async::io_executor())
            .thenValue(WriteSegmentTask{library_});
    }

folly::Future<entity::VariantKey> write(
    stream::KeyType key_type,
    VersionId version_id,
    const StreamId &stream_id,
    timestamp creation_ts,
    IndexValue start_index,
    IndexValue end_index,
    SegmentInMemory &&segment) override {

    util::check(segment.descriptor().id() == stream_id,
                "Descriptor id mismatch in atom key {} != {}",
                stream_id,
                segment.descriptor().id());

    return async::submit_cpu_task(EncodeAtomTask{
        key_type, version_id, stream_id, start_index, end_index, creation_ts,
        std::move(segment), codec_, encoding_version_
    })
        .via(&async::io_executor())
        .thenValue(WriteSegmentTask{library_});
}

folly::Future<VariantKey> write(PartialKey pk, SegmentInMemory &&segment) override {
    return write(pk.key_type, pk.version_id, pk.stream_id, pk.start_index, pk.end_index, std::move(segment));
}

folly::Future<entity::VariantKey> write(
    KeyType key_type,
    const StreamId &stream_id,
    SegmentInMemory &&segment) override {
    util::check(is_ref_key_class(key_type), "Expected ref key type got  {}", key_type);
    return async::submit_cpu_task(EncodeRefTask{
        key_type, stream_id, std::move(segment), codec_, encoding_version_
    })
        .via(&async::io_executor())
        .thenValue(WriteSegmentTask{library_});
}

entity::VariantKey write_sync(
    stream::KeyType key_type,
    VersionId version_id,
    const StreamId &stream_id,
    IndexValue start_index,
    IndexValue end_index,
    SegmentInMemory &&segment) override {

    util::check(segment.descriptor().id() == stream_id,
                "Descriptor id mismatch in atom key {} != {}",
                stream_id,
                segment.descriptor().id());

    auto encoded = EncodeAtomTask{
        key_type, version_id, stream_id, start_index, end_index, current_timestamp(),
        std::move(segment), codec_, encoding_version_
    }();
    return WriteSegmentTask{library_}(std::move(encoded));
}

entity::VariantKey write_sync(PartialKey pk, SegmentInMemory &&segment) override {
    return write_sync(pk.key_type, pk.version_id, pk.stream_id, pk.start_index, pk.end_index, std::move(segment));
}

entity::VariantKey write_sync(
    KeyType key_type,
    const StreamId &stream_id,
    SegmentInMemory &&segment) override {
    util::check(is_ref_key_class(key_type), "Expected ref key type got  {}", key_type);
    auto encoded = EncodeRefTask{key_type, stream_id, std::move(segment), codec_, encoding_version_}();
    return WriteSegmentTask{library_}(std::move(encoded));
}

folly::Future<folly::Unit> write_compressed(storage::KeySegmentPair &&ks) override {
    return async::submit_io_task(WriteCompressedTask{std::move(ks), library_});
}

void write_compressed_sync(storage::KeySegmentPair &&ks) override {
    library_->write(Composite<storage::KeySegmentPair>(std::move(ks)));
}

folly::Future<entity::VariantKey> update(const entity::VariantKey &key,
                                         SegmentInMemory &&segment,
                                         storage::UpdateOpts opts) override {
    auto stream_id = variant_key_id(key);
    util::check(segment.descriptor().id() == stream_id,
                "Descriptor id mismatch in variant key {} != {}",
                stream_id,
                segment.descriptor().id());

    return async::submit_cpu_task(EncodeSegmentTask{
        key, std::move(segment), codec_, encoding_version_
    })
        .via(&async::io_executor())
        .thenValue(UpdateSegmentTask{library_, opts});
}

folly::Future<VariantKey> copy(
        KeyType key_type,
        const StreamId &stream_id,
        VersionId version_id,
        const VariantKey &source_key) override {
    return async::submit_io_task(CopyCompressedTask<ClockType>{source_key, key_type, stream_id, version_id, library_});
}

VariantKey copy_sync(
        KeyType key_type,
        const StreamId &stream_id,
        VersionId version_id,
        const VariantKey &source_key) override {
    return CopyCompressedTask<ClockType>{source_key, key_type, stream_id, version_id, library_}();
}

timestamp current_timestamp() override {
    return ClockType::nanos_since_epoch();
}

void iterate_type(
        KeyType type,
        const entity::IterateTypeVisitor& func,
        const std::string &prefix) override {
    library_->iterate_type(type, func, prefix);
}

folly::Future<std::pair<entity::VariantKey, SegmentInMemory>> read(
        const entity::VariantKey &key,
        storage::ReadKeyOpts opts) override {
    return read_and_continue(key, library_, opts, DecodeSegmentTask{});
}

std::pair<entity::VariantKey, SegmentInMemory> read_sync(const entity::VariantKey &key,
                                                         storage::ReadKeyOpts opts) override {
    return DecodeSegmentTask{}(read_dispatch(key, library_, opts));
}

folly::Future<storage::KeySegmentPair> read_compressed(
        const entity::VariantKey &key,
        storage::ReadKeyOpts opts) override {
    return read_and_continue(key, library_, opts, PassThroughTask{});
}

storage::KeySegmentPair read_compressed_sync(
        const entity::VariantKey& key,
        storage::ReadKeyOpts opts
        ) override {
        return read_dispatch( key, library_, opts );
}

folly::Future<std::pair<std::optional<VariantKey>, std::optional<google::protobuf::Any>>> read_metadata(const entity::VariantKey &key, storage::ReadKeyOpts opts) override {
    return read_and_continue(key, library_, opts, DecodeMetadataTask{});
}

folly::Future<std::tuple<VariantKey, std::optional<google::protobuf::Any>, StreamDescriptor>> read_metadata_and_descriptor(
        const entity::VariantKey &key,
        storage::ReadKeyOpts opts) override {
    return read_and_continue(key, library_, opts, DecodeMetadataAndDescriptorTask{});
}

folly::Future<std::pair<VariantKey, TimeseriesDescriptor>> read_timeseries_descriptor(
        const entity::VariantKey &key,
        storage::ReadKeyOpts opts = storage::ReadKeyOpts{}) override {
    return read_and_continue(key, library_, opts, DecodeTimeseriesDescriptorTask{});
}

folly::Future<bool> key_exists(const entity::VariantKey &key) override {
    return async::submit_io_task(KeyExistsTask{&key, library_});
}

bool key_exists_sync(const entity::VariantKey &key) override {
    return KeyExistsTask{&key, library_}();
}

bool supports_prefix_matching() const override {
    return library_->supports_prefix_matching();
}

bool fast_delete() override {
    return library_->fast_delete();
}

void move_storage(KeyType key_type, timestamp horizon, size_t storage_index) override {
    library_->move_storage(key_type, horizon, storage_index);
}

folly::Future<folly::Unit> batch_write_compressed(std::vector<storage::KeySegmentPair> kvs) override {
    return async::submit_io_task(WriteCompressedBatchTask(std::move(kvs), library_));
}

folly::Future<RemoveKeyResultType> remove_key(const entity::VariantKey &key, storage::RemoveOpts opts) override {
    return async::submit_io_task(RemoveTask{key, library_, opts});
}

RemoveKeyResultType remove_key_sync(const entity::VariantKey &key, storage::RemoveOpts opts) override {
    return RemoveTask{key, library_, opts}();
}

folly::Future<std::vector<RemoveKeyResultType>> remove_keys(const std::vector<entity::VariantKey> &keys,
                                                            storage::RemoveOpts opts) override {
    return keys.empty() ?
           std::vector<RemoveKeyResultType>() :
           async::submit_io_task(RemoveBatchTask{keys, library_, opts});
}

folly::Future<std::vector<RemoveKeyResultType>> remove_keys(std::vector<entity::VariantKey> &&keys,
                                                            storage::RemoveOpts opts) override {
    return keys.empty() ?
           std::vector<RemoveKeyResultType>() :
           async::submit_io_task(RemoveBatchTask{std::move(keys), library_, opts});
}

folly::Future<std::vector<VariantKey>> batch_read_compressed(
        std::vector<std::pair<entity::VariantKey, ReadContinuation>> &&keys_and_continuations,
        const BatchReadArgs &args) override {
    util::check(!keys_and_continuations.empty(), "Unexpected empty keys/continuation vector in batch_read_compressed");
    return folly::collect(folly::window(std::move(keys_and_continuations), [this] (auto&& key_and_continuation) {
        auto [key, continuation] = std::forward<decltype(key_and_continuation)>(key_and_continuation);
        return read_and_continue(key, library_, storage::ReadKeyOpts{}, std::move(continuation));
    }, args.batch_size_)).via(&async::io_executor());
}

std::vector<folly::Future<pipelines::SegmentAndSlice>> batch_read_uncompressed(
        std::vector<pipelines::RangesAndKey>&& ranges_and_keys,
        std::shared_ptr<std::unordered_set<std::string>> columns_to_decode) override {
    return folly::window(
        std::move(ranges_and_keys),
        [this, columns_to_decode](auto&& ranges_and_key) {
            const auto key = ranges_and_key.key_;
            return read_and_continue(key, library_, storage::ReadKeyOpts{}, DecodeSliceTask{std::move(ranges_and_key), columns_to_decode});
        }, async::TaskScheduler::instance()->io_thread_count() * 2);
}

std::vector<folly::Future<bool>> batch_key_exists(
        const std::vector<entity::VariantKey> &keys) override {
    std::vector<folly::Future<bool>> res;
    res.reserve(keys.size());
    for (auto itr = keys.cbegin(); itr < keys.cend(); ++itr) {
        res.push_back(async::submit_io_task(KeyExistsTask(itr, library_)));
    }
    return res;
}


    folly::Future<SliceAndKey> async_write(
            folly::Future<std::tuple<PartialKey, SegmentInMemory, pipelines::FrameSlice>> &&input_fut,
            const std::shared_ptr<DeDupMap> &de_dup_map) override {
        using KeyOptSegment = std::pair<VariantKey, std::optional<Segment>>;
        return std::move(input_fut).thenValue([this] (auto&& input) {
            auto [key, seg, slice] = std::forward<decltype(input)>(input);
            auto key_seg = EncodeAtomTask{
                std::move(key),
                ClockType::nanos_since_epoch(),
                std::move(seg),
                codec_,
                encoding_version_}();
            return std::pair<storage::KeySegmentPair, FrameSlice>(std::move(key_seg), std::move(slice));
        })
        .thenValue([de_dup_map](auto &&ks) -> std::pair<KeyOptSegment, pipelines::FrameSlice> {
            auto [key_seg, slice] = std::forward<decltype(ks)>(ks);
            return std::make_pair(lookup_match_in_dedup_map(de_dup_map, std::move(key_seg)), std::move(slice));
        })
        .via(&async::io_executor())
        .thenValue([lib=library_](auto &&item) {
            auto [key_opt_segment, slice] = std::forward<decltype(item)>(item);
            if (key_opt_segment.second)
                lib->write(Composite<storage::KeySegmentPair>({VariantKey{key_opt_segment.first},
                                                               std::move(*key_opt_segment.second)}));

            return SliceAndKey{slice, to_atom(key_opt_segment.first)};
        });
    }

    void set_failure_sim(const arcticdb::proto::storage::VersionStoreConfig::StorageFailureSimulator &cfg) override {
        library_->set_failure_sim(cfg);
    }

private:
    std::shared_ptr<storage::Library> library_;
    std::shared_ptr<arcticdb::proto::encoding::VariantCodec> codec_;
    const EncodingVersion encoding_version_;
};

} // namespace arcticdb::async
