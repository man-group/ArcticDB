/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/async/task_scheduler.hpp>
#include <arcticdb/util/clock.hpp>
#include <arcticdb/storage/store.hpp>
#include <arcticdb/storage/library.hpp>
#include <folly/futures/Future.h>
#include <arcticdb/async/tasks.hpp>
#include <arcticdb/processing/clause.hpp>
#include <arcticdb/storage/key_segment_pair.hpp>

namespace arcticdb::toolbox::apy {
class LibraryTool;
}

namespace arcticdb::async {

using ExistingObject = entity::VariantKey;
using NewObject = storage::KeySegmentPair;
using DeDupLookupResult = std::variant<ExistingObject, NewObject>;

DeDupLookupResult lookup_match_in_dedup_map(
        const std::shared_ptr<DeDupMap>& de_dup_map, storage::KeySegmentPair& key_seg
);

template<typename Callable>
auto read_and_continue(
        const VariantKey& key, std::shared_ptr<storage::Library> library, const storage::ReadKeyOpts& opts, Callable&& c
) {
    return async::submit_io_task(ReadCompressedTask{key, library, opts, std::forward<decltype(c)>(c)})
            .thenValueInline([](auto&& result) mutable {
                auto&& [key_seg_fut, continuation] = std::forward<decltype(result)>(result);
                return std::move(key_seg_fut)
                        .thenValueInline([continuation = std::move(continuation)](storage::KeySegmentPair&& key_seg
                                         ) mutable { return continuation(std::move(key_seg)); });
            });
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
            std::shared_ptr<storage::Library> library, const proto::encoding::VariantCodec& codec,
            EncodingVersion encoding_version
    ) :
        library_(std::move(library)),
        codec_(std::make_shared<proto::encoding::VariantCodec>(codec)),
        encoding_version_(encoding_version) {}

    folly::Future<entity::VariantKey> write(
            stream::KeyType key_type, VersionId version_id, const StreamId& stream_id, IndexValue start_index,
            IndexValue end_index, SegmentInMemory&& segment
    ) override {

        util::check(
                segment.descriptor().id() == stream_id,
                "Descriptor id mismatch in atom key {} != {}",
                stream_id,
                segment.descriptor().id()
        );

        return async::submit_cpu_task(EncodeAtomTask{
                                              key_type,
                                              version_id,
                                              stream_id,
                                              start_index,
                                              end_index,
                                              current_timestamp(),
                                              std::move(segment),
                                              codec_,
                                              encoding_version_
                                      })
                .via(&async::io_executor())
                .thenValue(WriteSegmentTask{library_});
    }

    folly::Future<entity::VariantKey> write(
            stream::KeyType key_type, VersionId version_id, const StreamId& stream_id, timestamp creation_ts,
            IndexValue start_index, IndexValue end_index, SegmentInMemory&& segment
    ) override {

        util::check(
                segment.descriptor().id() == stream_id,
                "Descriptor id mismatch in atom key {} != {}",
                stream_id,
                segment.descriptor().id()
        );

        return async::submit_cpu_task(EncodeAtomTask{
                                              key_type,
                                              version_id,
                                              stream_id,
                                              start_index,
                                              end_index,
                                              creation_ts,
                                              std::move(segment),
                                              codec_,
                                              encoding_version_
                                      })
                .via(&async::io_executor())
                .thenValue(WriteSegmentTask{library_});
    }

    folly::Future<VariantKey> write(PartialKey pk, SegmentInMemory&& segment) override {
        return write(pk.key_type, pk.version_id, pk.stream_id, pk.start_index, pk.end_index, std::move(segment));
    }

    folly::Future<entity::VariantKey> write(KeyType key_type, const StreamId& stream_id, SegmentInMemory&& segment)
            override {
        util::check(is_ref_key_class(key_type), "Expected ref key type got  {}", key_type);
        return async::submit_cpu_task(EncodeRefTask{key_type, stream_id, std::move(segment), codec_, encoding_version_})
                .via(&async::io_executor())
                .thenValue(WriteSegmentTask{library_});
    }

    folly::Future<VariantKey> write_maybe_blocking(
            PartialKey pk, SegmentInMemory&& segment, std::shared_ptr<folly::NativeSemaphore> semaphore
    ) override {
        log::version().debug("Waiting for semaphore for write_maybe_blocking {}", pk);
        semaphore->wait();
        log::version().debug("Starting write_maybe_blocking {}", pk);
        return write(pk.key_type, pk.version_id, pk.stream_id, pk.start_index, pk.end_index, std::move(segment))
                .thenTryInline([semaphore](folly::Try<VariantKey> keyTry) {
                    semaphore->post();
                    keyTry.throwUnlessValue();
                    return keyTry.value();
                });
    }

    entity::VariantKey write_sync(
            stream::KeyType key_type, VersionId version_id, const StreamId& stream_id, IndexValue start_index,
            IndexValue end_index, SegmentInMemory&& segment
    ) override {

        util::check(
                segment.descriptor().id() == stream_id,
                "Descriptor id mismatch in atom key {} != {}",
                stream_id,
                segment.descriptor().id()
        );

        auto encoded = EncodeAtomTask{
                key_type,
                version_id,
                stream_id,
                start_index,
                end_index,
                current_timestamp(),
                std::move(segment),
                codec_,
                encoding_version_
        }();
        return WriteSegmentTask{library_}(std::move(encoded));
    }

    entity::VariantKey write_sync(PartialKey pk, SegmentInMemory&& segment) override {
        return write_sync(pk.key_type, pk.version_id, pk.stream_id, pk.start_index, pk.end_index, std::move(segment));
    }

    entity::VariantKey write_sync(KeyType key_type, const StreamId& stream_id, SegmentInMemory&& segment) override {
        util::check(is_ref_key_class(key_type), "Expected ref key type got  {}", key_type);
        auto encoded = EncodeRefTask{key_type, stream_id, std::move(segment), codec_, encoding_version_}();
        return WriteSegmentTask{library_}(std::move(encoded));
    }

    entity::VariantKey write_if_none_sync(KeyType key_type, const StreamId& stream_id, SegmentInMemory&& segment)
            override {
        util::check(is_ref_key_class(key_type), "Expected ref key type got  {}", key_type);
        auto encoded = EncodeRefTask{key_type, stream_id, std::move(segment), codec_, encoding_version_}();
        return WriteIfNoneTask{library_}(std::move(encoded));
    }

    bool is_path_valid(const std::string_view path) const override { return library_->is_path_valid(path); }

    folly::Future<folly::Unit> write_compressed(storage::KeySegmentPair ks) override {
        return async::submit_io_task(WriteCompressedTask{std::move(ks), library_});
    }

    void write_compressed_sync(storage::KeySegmentPair ks) override { library_->write(ks); }

    folly::Future<entity::VariantKey> update(
            const entity::VariantKey& key, SegmentInMemory&& segment, storage::UpdateOpts opts
    ) override {
        auto stream_id = variant_key_id(key);
        util::check(
                segment.descriptor().id() == stream_id,
                "Descriptor id mismatch in variant key {} != {}",
                stream_id,
                segment.descriptor().id()
        );

        return async::submit_cpu_task(EncodeSegmentTask{key, std::move(segment), codec_, encoding_version_})
                .via(&async::io_executor())
                .thenValue(UpdateSegmentTask{library_, opts});
    }

    folly::Future<VariantKey> copy(
            KeyType key_type, const StreamId& stream_id, VersionId version_id, const VariantKey& source_key
    ) override {
        return async::submit_io_task(
                CopyCompressedTask<ClockType>{source_key, key_type, stream_id, version_id, library_}
        );
    }

    VariantKey copy_sync(
            KeyType key_type, const StreamId& stream_id, VersionId version_id, const VariantKey& source_key
    ) override {
        return CopyCompressedTask<ClockType>{source_key, key_type, stream_id, version_id, library_}();
    }

    timestamp current_timestamp() override { return ClockType::nanos_since_epoch(); }

    void iterate_type(KeyType type, const entity::IterateTypeVisitor& func, const std::string& prefix) override {
        library_->iterate_type(type, func, prefix);
    }

    folly::Future<folly::Unit> visit_object_sizes(
            KeyType type, const std::optional<StreamId>& stream_id_opt, storage::ObjectSizesVisitor visitor
    ) override {
        std::string prefix;
        if (stream_id_opt) {
            const auto& stream_id = *stream_id_opt;
            prefix = std::holds_alternative<StringId>(stream_id) ? std::get<StringId>(stream_id) : std::string();
        }

        if (library_->supports_object_size_calculation()) {
            // The library has native support for some kind of clever size calculation, so let it take over
            return async::submit_io_task(VisitObjectSizesTask{type, prefix, std::move(visitor), library_});
        }

        // No native support for a clever size calculation, so just read keys and sum their sizes
        KeySizeCalculators key_size_calculators;
        iterate_type(
                type,
                [&key_size_calculators, &stream_id_opt, &visitor](VariantKey&& k) {
                    key_size_calculators.emplace_back(std::move(k), [visitor, stream_id_opt](auto&& key_seg) {
                        if (!stream_id_opt || variant_key_id(key_seg.variant_key()) == *stream_id_opt) {
                            auto compressed_size = key_seg.segment().size();
                            visitor(key_seg.variant_key(), compressed_size);
                        }
                        return std::forward<decltype(key_seg)>(key_seg).variant_key();
                    });
                },
                prefix
        );

        read_ignoring_key_not_found(std::move(key_size_calculators));
        return folly::makeFuture();
    }

    folly::Future<std::shared_ptr<storage::ObjectSizes>> get_object_sizes(
            KeyType type, const std::optional<StreamId>& stream_id_opt
    ) override {
        auto counter = std::make_shared<std::atomic_uint64_t>(0);
        auto bytes = std::make_shared<std::atomic_uint64_t>(0);
        storage::ObjectSizesVisitor visitor = [counter, bytes](const VariantKey&, storage::CompressedSize size) {
            counter->fetch_add(1, std::memory_order_relaxed);
            bytes->fetch_add(size, std::memory_order_relaxed);
        };

        return visit_object_sizes(type, stream_id_opt, std::move(visitor))
                .thenValueInline([counter, bytes, type](folly::Unit&&) {
                    return std::make_shared<storage::ObjectSizes>(type, *counter, *bytes);
                });
    }

    bool do_iterate_type_until_match(
            KeyType key_type, const IterateTypePredicate& visitor, const std::string& prefix = ""
    ) override {
        return library_->do_iterate_type_until_match(key_type, visitor, prefix);
    }

    bool scan_for_matching_key(KeyType key_type, const IterateTypePredicate& predicate) override {
        return library_->scan_for_matching_key(key_type, predicate);
    }

    folly::Future<std::pair<entity::VariantKey, SegmentInMemory>> read(
            const entity::VariantKey& key, storage::ReadKeyOpts opts
    ) override {
        return read_and_continue(key, library_, opts, DecodeSegmentTask{});
    }

    std::pair<entity::VariantKey, SegmentInMemory> read_sync(const entity::VariantKey& key, storage::ReadKeyOpts opts)
            override {
        return DecodeSegmentTask{}(read_sync_dispatch(key, library_, opts));
    }

    folly::Future<storage::KeySegmentPair> read_compressed(const entity::VariantKey& key, storage::ReadKeyOpts opts)
            override {
        return read_and_continue(key, library_, opts, PassThroughTask{});
    }

    storage::KeySegmentPair read_compressed_sync(const entity::VariantKey& key, storage::ReadKeyOpts opts) override {
        return read_sync_dispatch(key, library_, opts);
    }

    folly::Future<std::pair<std::optional<VariantKey>, std::optional<google::protobuf::Any>>> read_metadata(
            const entity::VariantKey& key, storage::ReadKeyOpts opts
    ) override {
        return read_and_continue(key, library_, opts, DecodeMetadataTask{});
    }

    folly::Future<std::tuple<VariantKey, std::optional<google::protobuf::Any>, StreamDescriptor>>
    read_metadata_and_descriptor(const entity::VariantKey& key, storage::ReadKeyOpts opts) override {
        return read_and_continue(key, library_, opts, DecodeMetadataAndDescriptorTask{});
    }

    folly::Future<std::pair<VariantKey, TimeseriesDescriptor>> read_timeseries_descriptor(
            const entity::VariantKey& key, storage::ReadKeyOpts opts
    ) override {
        return read_and_continue(key, library_, opts, DecodeTimeseriesDescriptorTask{});
    }

    folly::Future<bool> key_exists(entity::VariantKey&& key) {
        return async::submit_io_task(KeyExistsTask{std::move(key), library_});
    }

    folly::Future<bool> key_exists(const entity::VariantKey& key) override {
        return async::submit_io_task(KeyExistsTask{key, library_});
    }

    bool key_exists_sync(const entity::VariantKey& key) override { return KeyExistsTask{key, library_}(); }

    bool key_exists_sync(entity::VariantKey&& key) { return KeyExistsTask{std::move(key), library_}(); }

    bool supports_prefix_matching() const override { return library_->supports_prefix_matching(); }

    bool supports_atomic_writes() const override { return library_->supports_atomic_writes(); }

    std::string key_path(const VariantKey& key) const { return library_->key_path(key); }

    bool fast_delete() override { return library_->fast_delete(); }

    void move_storage(KeyType key_type, timestamp horizon, size_t storage_index) override {
        library_->move_storage(key_type, horizon, storage_index);
    }

    folly::Future<folly::Unit> batch_write_compressed(std::vector<storage::KeySegmentPair> kvs) override {
        return async::submit_io_task(WriteCompressedBatchTask(std::move(kvs), library_));
    }

    folly::Future<RemoveKeyResultType> remove_key(const entity::VariantKey& key, storage::RemoveOpts opts) override {
        return async::submit_io_task(RemoveTask{key, library_, opts});
    }

    RemoveKeyResultType remove_key_sync(const entity::VariantKey& key, storage::RemoveOpts opts) override {
        return RemoveTask{key, library_, opts}();
    }

    folly::Future<std::vector<RemoveKeyResultType>> remove_keys(
            const std::vector<entity::VariantKey>& keys, storage::RemoveOpts opts
    ) override {
        return keys.empty() ? std::vector<RemoveKeyResultType>()
                            : async::submit_io_task(RemoveBatchTask{keys, library_, opts});
    }

    folly::Future<std::vector<RemoveKeyResultType>> remove_keys(
            std::vector<entity::VariantKey>&& keys, storage::RemoveOpts opts
    ) override {
        return keys.empty() ? std::vector<RemoveKeyResultType>()
                            : async::submit_io_task(RemoveBatchTask{std::move(keys), library_, opts});
    }

    std::vector<RemoveKeyResultType> remove_keys_sync(
            const std::vector<entity::VariantKey>& keys, storage::RemoveOpts opts
    ) override {
        return keys.empty() ? std::vector<RemoveKeyResultType>() : RemoveBatchTask{keys, library_, opts}();
    }

    std::vector<RemoveKeyResultType> remove_keys_sync(std::vector<entity::VariantKey>&& keys, storage::RemoveOpts opts)
            override {
        return keys.empty() ? std::vector<RemoveKeyResultType>() : RemoveBatchTask{std::move(keys), library_, opts}();
    }

    std::vector<folly::Future<VariantKey>> batch_read_compressed(
            std::vector<std::pair<entity::VariantKey, ReadContinuation>>&& keys_and_continuations,
            const BatchReadArgs& args
    ) override {
        util::check(
                !keys_and_continuations.empty(), "Unexpected empty keys/continuation vector in batch_read_compressed"
        );
        return folly::window(
                std::move(keys_and_continuations),
                [this](auto&& key_and_continuation) {
                    auto [key, continuation] = std::forward<decltype(key_and_continuation)>(key_and_continuation);
                    return read_and_continue(key, library_, storage::ReadKeyOpts{}, std::move(continuation));
                },
                args.batch_size_
        );
    }

    std::vector<folly::Future<pipelines::SegmentAndSlice>> batch_read_uncompressed(
            std::vector<pipelines::RangesAndKey>&& ranges_and_keys,
            std::shared_ptr<std::unordered_set<std::string>> columns_to_decode
    ) override {
        ARCTICDB_RUNTIME_DEBUG(log::version(), "Reading {} keys", ranges_and_keys.size());
        std::vector<folly::Future<pipelines::SegmentAndSlice>> output;
        for (auto&& ranges_and_key : ranges_and_keys) {
            const auto key = ranges_and_key.key_;
            output.emplace_back(read_and_continue(
                    key, library_, storage::ReadKeyOpts{}, DecodeSliceTask{std::move(ranges_and_key), columns_to_decode}
            ));
        }
        return output;
    }

    std::vector<folly::Future<bool>> batch_key_exists(const std::vector<entity::VariantKey>& keys) override {
        std::vector<folly::Future<bool>> res;
        res.reserve(keys.size());
        for (const auto& key : keys) {
            res.push_back(async::submit_io_task(KeyExistsTask(key, library_)));
        }
        return res;
    }

    folly::Future<SliceAndKey> async_write(
            folly::Future<std::tuple<PartialKey, SegmentInMemory, pipelines::FrameSlice>>&& input_fut,
            const std::shared_ptr<DeDupMap>& de_dup_map
    ) override {
        return std::move(input_fut)
                .thenValue([this](auto&& input) {
                    auto [key, seg, slice] = std::forward<decltype(input)>(input);
                    auto key_seg = EncodeAtomTask{
                            std::move(key), ClockType::nanos_since_epoch(), std::move(seg), codec_, encoding_version_
                    }();
                    return std::pair<storage::KeySegmentPair, FrameSlice>(std::move(key_seg), std::move(slice));
                })
                .thenValue([de_dup_map](auto&& ks) -> std::pair<DeDupLookupResult, pipelines::FrameSlice> {
                    auto& [key_seg, slice] = ks;
                    return std::pair{lookup_match_in_dedup_map(de_dup_map, key_seg), std::move(slice)};
                })
                .via(&async::io_executor())
                .thenValue([lib = library_](auto&& item) {
                    auto& [dedup_lookup, slice] = item;
                    return util::variant_match(
                            dedup_lookup,
                            [&](NewObject& obj) {
                                lib->write(obj);
                                return SliceAndKey{slice, obj.atom_key()};
                            },
                            [&](ExistingObject& obj) { return SliceAndKey{slice, to_atom(std::move(obj))}; }
                    );
                });
    }

    void set_failure_sim(const arcticdb::proto::storage::VersionStoreConfig::StorageFailureSimulator& cfg) override {
        library_->set_failure_sim(cfg);
    }

    std::string name() const override { return library_->name(); }

  private:
    friend class arcticdb::toolbox::apy::LibraryTool;
    std::shared_ptr<storage::Library> library_;
    std::shared_ptr<arcticdb::proto::encoding::VariantCodec> codec_;
    const EncodingVersion encoding_version_;
};

} // namespace arcticdb::async
