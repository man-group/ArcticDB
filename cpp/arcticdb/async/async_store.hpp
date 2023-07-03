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

namespace arcticdb::async {

std::pair<VariantKey, std::optional<Segment>> lookup_match_in_dedup_map(
    const std::shared_ptr<DeDupMap> &de_dup_map,
    storage::KeySegmentPair&& key_seg);

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
        library_->write(Composite<storage::KeySegmentPair>{std::move(ks)});
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

    folly::Future<VariantKey> copy(KeyType key_type,
                                   const StreamId &stream_id,
                                   VersionId version_id,
                                   const VariantKey &source_key) override {
        return async::submit_io_task(CopyCompressedTask<ClockType>{source_key, key_type, stream_id, version_id,
                                                                   library_});
    }

    VariantKey copy_sync(KeyType key_type,
                         const StreamId &stream_id,
                         VersionId version_id,
                         const VariantKey &source_key) override {
        return CopyCompressedTask<ClockType>{source_key, key_type, stream_id, version_id, library_}();
    }

    timestamp current_timestamp() override {
        return ClockType::nanos_since_epoch();
    }

    void iterate_type(KeyType type, entity::IterateTypeVisitor func,
                      const std::string &prefix) override {
        library_->iterate_type(type, func, prefix);
    }

    folly::Future<std::pair<entity::VariantKey, SegmentInMemory>> read(const entity::VariantKey &key,
                                                                       storage::ReadKeyOpts opts) override {
        return async::submit_io_task(ReadCompressedTask{key, library_, opts})
            .via(&async::cpu_executor())
            .thenValue(DecodeSegmentTask{});
    }

    std::pair<entity::VariantKey, SegmentInMemory> read_sync(const entity::VariantKey &key,
                                                             storage::ReadKeyOpts opts) override {
        auto read_task = ReadCompressedTask{key, library_, opts};
        return DecodeSegmentTask{}(read_task());
    }

    folly::Future<storage::KeySegmentPair> read_compressed(const entity::VariantKey &key,
                                                           storage::ReadKeyOpts opts) override {
        return async::submit_io_task(ReadCompressedTask{key, library_, opts});
    }

    folly::Future<std::pair<std::optional<VariantKey>, std::optional<google::protobuf::Any>>>
    read_metadata(const entity::VariantKey &key, storage::ReadKeyOpts opts) override {
        return async::submit_io_task(ReadCompressedTask{key, library_, opts})
            .via(&async::cpu_executor())
            .thenValue(DecodeMetadataTask{});
    }

    folly::Future<std::tuple<VariantKey, std::optional<google::protobuf::Any>, StreamDescriptor>>
    read_metadata_and_descriptor(
        const entity::VariantKey &key,
        storage::ReadKeyOpts opts) override {
        return async::submit_io_task(ReadCompressedTask{key, library_, opts})
            .via(&async::cpu_executor())
            .thenValue(DecodeMetadataAndDescriptorTask{});
    }

    folly::Future<std::pair<VariantKey, TimeseriesDescriptor>>
    read_timeseries_descriptor(
        const entity::VariantKey &key) override {
        return async::submit_io_task(ReadCompressedTask{key, library_, storage::ReadKeyOpts{}})
            .via(&async::cpu_executor())
            .thenValue(DecodeTimeseriesDescriptorTask{});
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

    void copy_to_results(
        std::vector<folly::Future<storage::KeySegmentPair>> &batch,
        std::vector<storage::KeySegmentPair> &res
    ) const {
        auto vec = folly::collect(batch).get();
        res.insert(end(res), std::make_move_iterator(std::begin(vec)), std::make_move_iterator(std::end(vec)));
    }

    void copy_to_results_with_failure(
        std::vector<folly::Future<storage::KeySegmentPair>> &batch,
        std::vector<storage::KeySegmentPair> &res,
        const std::vector<VariantKey> &keys) const {
        auto collected_kvs = folly::collectAll(batch).get();
        for (size_t idx = 0; idx != batch.size(); idx++) {
            if (collected_kvs[idx].hasValue()) {
                res.emplace_back(std::move(collected_kvs[idx].value()));
            } else {
                log::storage().warn("Found an unreadable key {}", keys[idx]);
            }
        }
    }

    std::vector<storage::KeySegmentPair> batch_read_compressed(
        std::vector<entity::VariantKey> &&ks,
        const BatchReadArgs &args,
        bool may_fail) override {
        auto keys = std::move(ks);
        std::vector<folly::Future<storage::KeySegmentPair>> batch{};
        batch.reserve(args.batch_size_);
        std::vector<storage::KeySegmentPair> res;
        res.reserve(keys.size());
        for (auto key : keys) {
            batch.push_back(async::submit_io_task(ReadCompressedTask(std::move(key),
                                                                     library_,
                                                                     storage::ReadKeyOpts{})));
            if (batch.size() == args.batch_size_) {
                if (may_fail)
                    copy_to_results_with_failure(batch, res, keys);
                else
                    copy_to_results(batch, res);

                batch.clear();
            }
        }
        if (!batch.empty()) {
            if (may_fail)
                copy_to_results_with_failure(batch, res, keys);
            else
                copy_to_results(batch, res);
        }

        return res;
    }

    folly::Future<std::vector<VariantKey>> batch_read_compressed(
        std::vector<entity::VariantKey> &&ks,
        std::vector<ReadContinuation> &&continuations,
        const BatchReadArgs &args) override {
        auto keys = std::move(ks);
        util::check(!keys.empty(), "Unexpected empty keys in batch_read_compressed");

        auto key_seg_futs = folly::window(keys, [*this](auto &&key) {
            return async::submit_io_task(ReadCompressedTask(std::forward<decltype(key)>(key), library_, storage::ReadKeyOpts{}));
        }, args.batch_size_);

        util::check(key_seg_futs.size() == keys.size(),
                    "Size mismatch in batch_read_compressed: {} != {}",
                    key_seg_futs.size(),
                    keys.size());
        std::vector<folly::Future<VariantKey>> result;
        result.reserve(key_seg_futs.size());
        for (auto &&key_seg_fut : folly::enumerate(key_seg_futs)) {
            result.emplace_back(std::move(*key_seg_fut).thenValue([continuation =
            std::move(continuations[key_seg_fut.index])](auto &&key_seg) mutable {
                return continuation(std::forward<decltype(key_seg)>(key_seg));
            }));
        }

        return folly::collect(result).via(&async::io_executor());
    }

    std::vector<Composite<ProcessingUnit>> batch_read_uncompressed(
        std::vector<Composite<pipelines::SliceAndKey>> &&sks,
        const std::vector<std::shared_ptr<Clause>>& clauses,
        const std::shared_ptr<std::unordered_set<std::string>>& filter_columns,
        const BatchReadArgs & args) override {
        auto slice_and_keys = std::move(sks);
        std::vector<Composite<ProcessingUnit>> res;
        res.reserve(slice_and_keys.size());
        std::vector<folly::Future<Composite<ProcessingUnit>>> batch;
        size_t current_size = 0;
        for (auto &&s : slice_and_keys) {
            auto sk = std::move(s);
            if (args.scheduler_ == BatchReadArgs::CPU) {
                batch.push_back(
                    async::submit_io_task(ReadCompressedSlicesTask(std::move(sk), library_))
                        .via(&async::cpu_executor())
                        .thenValue(DecodeSlicesTask{filter_columns})
                        .thenValue(MemSegmentProcessingTask{shared_from_this(), clauses}));
            }
            // IO option will execute all work in the same Folly thread potentially limiting context switches.
            else {
                batch.push_back(
                    async::submit_io_task(ReadCompressedSlicesTask(std::move(sk), library_))
                        .thenValue(DecodeSlicesTask{filter_columns})
                        .thenValue(MemSegmentProcessingTask{shared_from_this(), clauses}));
            }

            if (++current_size == args.batch_size_) {
                auto segments = folly::collect(batch).get();
                res.insert(std::end(res),
                           std::make_move_iterator(std::begin(segments)),
                           std::make_move_iterator(std::end(segments)));
                current_size = 0;
                batch.clear();
            }
        }

        if (!batch.empty()) {
            auto segments = folly::collect(batch).get();
            res.insert(std::end(res),
                       std::make_move_iterator(std::begin(segments)),
                       std::make_move_iterator(std::end(segments)));
        }

        slice_and_keys.clear();
        return res;
    }

    std::vector<folly::Future<bool>> batch_key_exists(const std::vector<entity::VariantKey> &keys)
    override {
        std::vector<folly::Future<bool>> res;
        res.reserve(keys.size());
        for (auto itr = keys.cbegin(); itr < keys.cend(); ++itr) {
            res.push_back(async::submit_io_task(KeyExistsTask(itr, library_)));
        }
        return res;
    }

    bool batch_all_keys_exist_sync(const std::unordered_set<entity::VariantKey> &keys)
    override {
        return std::all_of(keys.begin(), keys.end(), [that = this](const entity::VariantKey &key) {
            return that->key_exists_sync(key);
        });
    }

    folly::Future<std::vector<VariantKey>> batch_write(
        std::vector<std::pair<PartialKey, SegmentInMemory>> &&key_seg_pairs,
        const std::shared_ptr<DeDupMap> &de_dup_map,
        const BatchWriteArgs &args) override {
        auto key_segments = std::move(key_seg_pairs);
        std::vector<folly::Future<VariantKey>> futs;
        futs.reserve(key_seg_pairs.size());
        std::size_t write_count = args.lib_write_count == 0 ? 16ULL : args.lib_write_count;

        auto encode_futs = folly::window(key_segments, [*this](auto &&ks) {
            auto [key, seg] = std::forward<decltype(ks)>(ks);
            return async::submit_cpu_task(
                EncodeAtomTask(std::move(key),
                               ClockType::nanos_since_epoch(),
                               std::move(seg),
                               codec_,
                               encoding_version_));
        }, write_count);

        for (folly::Future<storage::KeySegmentPair>& encode_fut : encode_futs) {
            futs.emplace_back(
                std::move(encode_fut).thenValue([de_dup_map](auto &&ks) -> std::pair<VariantKey,
                                                                                     std::optional<Segment>> {
                        auto key_seg = std::forward<decltype(ks)>(ks);
                        return lookup_match_in_dedup_map(de_dup_map, std::move(key_seg));
                    })
                    .via(&async::io_executor()).thenValue([lib = library_](auto &&item) {
                        auto key_opt_segment = std::forward<decltype(item)>(item);
                        if (key_opt_segment.second)
                            lib->write(Composite<storage::KeySegmentPair>({VariantKey{key_opt_segment.first},
                                                                           std::move(*key_opt_segment.second)}));

                        return key_opt_segment.first;
                    }));
        }

        return folly::collect(futs).via(&async::io_executor());
    }

    void set_failure_sim(const arcticdb::proto::storage::VersionStoreConfig::StorageFailureSimulator &cfg)
    override {
        library_->set_failure_sim(cfg);
    }

private:
    std::shared_ptr<storage::Library> library_;
    std::shared_ptr<arcticdb::proto::encoding::VariantCodec> codec_;
    const EncodingVersion encoding_version_;
};

} // namespace arcticdb::async
