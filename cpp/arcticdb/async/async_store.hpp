/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/async/task_scheduler.hpp>
#include <arcticdb/util/clock.hpp>
#include <arcticdb/util/configs_map.hpp>
#include <arcticdb/storage/store.hpp>
#include <arcticdb/storage/library.hpp>
#include <folly/futures/Future.h>
#include <folly/executors/CPUThreadPoolExecutor.h>
#include <folly/executors/thread_factory/NamedThreadFactory.h>
#include <arcticdb/async/tasks.hpp>
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

/*
 * Read-path thread-pool routing.
 *
 * By default a read is one IO task carrying both the storage fetch and the decode continuation, the latter running
 * inline on the IO thread. folly's IOThreadPoolExecutor binds a task to one EventBase thread's private queue at
 * enqueue time and has no work stealing, so a long decode blocks every task queued behind it on that thread even
 * while other IO threads are idle. The CPU pool has a shared queue and is work-conserving.
 *
 * Two routings are available, off by default:
 *
 *   ReadPath.DecodeOnCpuPool               hop the decode continuation to the CPU pool instead of running it inline.
 *   ReadPath.CpuPoolForNonBlockingStorage  submit the whole read task to the CPU pool when the storage's fetch does
 *                                          not block (Storage::fetch_blocks) -- on such a backend an "IO" task is
 *                                          pure CPU work and does not need an IO thread at all.
 *
 * Both are read once per process: they choose a scheduling shape, not a per-read decision.
 */
inline bool decode_on_cpu_pool() {
    static const bool enabled = ConfigsMap::instance()->get_int("ReadPath.DecodeOnCpuPool", 0) != 0;
    return enabled;
}

inline bool cpu_pool_for_non_blocking_storage() {
    static const bool enabled = ConfigsMap::instance()->get_int("ReadPath.CpuPoolForNonBlockingStorage", 0) != 0;
    return enabled;
}

/*
 * ReadPath.ContinueInReadTask: run the continuation (the decode) inside the read task itself whenever the fetch has
 * already completed, which it always has on a synchronous storage (LMDB, in-memory: Storages::async_read wraps the
 * result in makeFuture). Today's shape returns the fetched future from the IO task and attaches the continuation
 * afterwards with thenValueInline, so the decode runs on whichever thread gets there second: the IO thread that
 * completed the task (the usual case), or -- when the IO thread finished before the submitter attached its callback
 * -- the submitting thread, which on the clause path is a CPU-pool thread inside fill_read_window. This switch removes
 * that race: the decode always runs on the IO thread that did the read.
 */
inline bool continue_in_read_task() {
    static const bool enabled = ConfigsMap::instance()->get_int("ReadPath.ContinueInReadTask", 0) != 0;
    return enabled;
}

template<typename Callable>
struct ReadAndContinueTask : BaseTask {
    ReadCompressedTask<Callable> read_;

    explicit ReadAndContinueTask(ReadCompressedTask<Callable>&& read) : read_(std::move(read)) {}

    ARCTICDB_MOVE_ONLY_DEFAULT(ReadAndContinueTask)

    auto operator()() {
        auto key_seg_continuation = read_();
        auto& key_seg_fut = key_seg_continuation.key_seg_;
        auto& continuation = key_seg_continuation.continuation_;
        if (key_seg_fut.isReady()) {
            return folly::makeFutureWith([&]() mutable { return continuation(std::move(key_seg_fut).value()); });
        }
        return std::move(key_seg_fut)
                .thenValueInline([continuation = std::move(continuation)](storage::KeySegmentPair&& key_seg) mutable {
                    return continuation(std::move(key_seg));
                });
    }
};

/*
 * ReadPath.SharedQueueIOPool (measurement only): run each read task -- fetch and decode together, as today -- on a
 * separate pool of io_thread_count() threads with ONE shared queue (folly::CPUThreadPoolExecutor) instead of the IO
 * pool. folly's IOThreadPoolExecutor binds each task to a thread round-robin at enqueue time and never steals, so a
 * read's N segments are dealt 8-or-9 per thread up front and the read waits for the unluckiest thread. A shared queue
 * is work-conserving: a thread that finishes early takes the next segment.
 */
inline bool shared_queue_io_pool() {
    static const bool enabled = ConfigsMap::instance()->get_int("ReadPath.SharedQueueIOPool", 1) != 0;
    return enabled;
}

inline folly::Executor& shared_queue_io_executor() {
    static auto* exec = new folly::CPUThreadPoolExecutor(
            async::TaskScheduler::instance()->io_thread_count(),
            std::make_shared<folly::NamedThreadFactory>("IOShared")
    );
    return *exec;
}

template<typename Callable>
auto read_and_continue(
        const VariantKey& key, std::shared_ptr<storage::Library> library, const storage::ReadKeyOpts& opts, Callable&& c
) {
    auto task = ReadCompressedTask{key, library, opts, std::forward<decltype(c)>(c)};
    if (shared_queue_io_pool()) {
        return folly::via(
                folly::getKeepAliveToken(shared_queue_io_executor()),
                ReadAndContinueTask<typename decltype(task)::ContinuationType>{std::move(task)}
        );
    }
    if (continue_in_read_task() && !decode_on_cpu_pool() && !cpu_pool_for_non_blocking_storage()) {
        return async::submit_io_task(ReadAndContinueTask<typename decltype(task)::ContinuationType>{std::move(task)});
    }
    const bool on_cpu_pool = cpu_pool_for_non_blocking_storage() && !library->fetch_blocks();
    auto fetched = on_cpu_pool ? async::submit_cpu_task(std::move(task)) : async::submit_io_task(std::move(task));

    // Measurement only: ReadPath.DecodeOnCpuPoolEvery=N hops every Nth decode to the CPU pool, to emulate on an
    // uncontended machine the share of decodes that the submit/attach race puts on the CPU pool on a small runner.
    static const int64_t every = ConfigsMap::instance()->get_int("ReadPath.DecodeOnCpuPoolEvery", 0);
    static std::atomic<uint64_t> counter{0};
    const bool hop_this = decode_on_cpu_pool() || (every > 0 && counter.fetch_add(1) % every == 0);
    return std::move(fetched).thenValueInline([hop = hop_this](auto&& result) mutable {
        auto&& [key_seg_fut, continuation] = std::forward<decltype(result)>(result);
        if (hop) {
            return std::move(key_seg_fut)
                    .via(&async::cpu_executor())
                    .thenValue([continuation = std::move(continuation)](storage::KeySegmentPair&& key_seg) mutable {
                        return continuation(std::move(key_seg));
                    });
        }
        return std::move(key_seg_fut)
                .thenValueInline([continuation = std::move(continuation)](storage::KeySegmentPair&& key_seg) mutable {
                    return continuation(std::move(key_seg));
                });
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

    folly::Future<VariantKey> write(stream::PartialKey pk, SegmentInMemory&& segment) override {
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
            stream::PartialKey pk, SegmentInMemory&& segment, std::shared_ptr<folly::NativeSemaphore> semaphore
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

    entity::VariantKey write_sync(stream::PartialKey pk, SegmentInMemory&& segment) override {
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

    const std::set<char>& unsupported_symbol_chars() const override { return library_->unsupported_symbol_chars(); }

    const std::set<char>& unsupported_library_chars() const override { return library_->unsupported_library_chars(); }

    std::optional<char> verify_library_suffix(std::string_view path) const override {
        return library_->verify_library_suffix(path);
    }

    folly::Future<folly::Unit> write_compressed(storage::KeySegmentPair ks) override {
        return async::submit_io_task(WriteCompressedTask{std::move(ks), library_});
    }

    void write_compressed_sync(storage::KeySegmentPair ks) override { library_->write(ks); }

    void update_compressed_sync(storage::KeySegmentPair ks, storage::UpdateOpts opts) override {
        library_->update(ks, opts);
    }

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

    entity::VariantKey update_sync(const entity::VariantKey& key, SegmentInMemory&& segment, storage::UpdateOpts opts)
            override {
        auto stream_id = variant_key_id(key);
        util::check(
                segment.descriptor().id() == stream_id,
                "Descriptor id mismatch in variant key {} != {}",
                stream_id,
                segment.descriptor().id()
        );

        auto encoded = EncodeSegmentTask{key, std::move(segment), codec_, encoding_version_}();
        return UpdateSegmentTask{library_, opts}(std::move(encoded));
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

        const auto start = std::chrono::steady_clock::now();
        return visit_object_sizes(type, stream_id_opt, std::move(visitor))
                .thenValueInline([counter, bytes, type, start](folly::Unit&&) {
                    const std::chrono::nanoseconds elapsed = std::chrono::steady_clock::now() - start;
                    return std::make_shared<storage::ObjectSizes>(type, *counter, *bytes, elapsed.count());
                });
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

    std::pair<VariantKey, TimeseriesDescriptor> read_timeseries_descriptor_sync(
            const entity::VariantKey& key, storage::ReadKeyOpts opts
    ) override {
        auto key_seg = read_sync_dispatch(key, library_, opts);
        return DecodeTimeseriesDescriptorTask{}(std::move(key_seg));
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

    folly::Future<folly::Unit> remove_key(const entity::VariantKey& key, storage::RemoveOpts opts) override {
        return async::submit_io_task(RemoveTask{key, library_, opts});
    }

    void remove_key_sync(const entity::VariantKey& key, storage::RemoveOpts opts) override {
        RemoveTask{key, library_, opts}();
    }

    folly::Future<folly::Unit> remove_keys(const std::vector<entity::VariantKey>& keys, storage::RemoveOpts opts)
            override {
        return remove_keys(std::vector<entity::VariantKey>{keys}, opts);
    }

    folly::Future<folly::Unit> remove_keys(std::vector<entity::VariantKey>&& keys, storage::RemoveOpts opts) override {
        if (keys.empty()) {
            return folly::Unit{};
        }
        const auto batch_size = library_->max_delete_batch_size();
        if (!batch_size.has_value() || keys.size() <= *batch_size) {
            return async::submit_io_task(RemoveBatchTask{std::move(keys), library_, opts});
        }
        auto chunks = chunk_keys(std::move(keys), *batch_size);
        auto futs = folly::window(
                std::move(chunks),
                [this, opts](std::vector<entity::VariantKey>&& chunk) {
                    return async::submit_io_task(RemoveBatchTask{std::move(chunk), library_, opts});
                },
                async::TaskScheduler::instance()->io_thread_count()
        );
        return folly::collect(std::move(futs)).via(&async::io_executor()).thenValue([](auto&&) {
            return folly::Unit{};
        });
    }

    void remove_keys_sync(const std::vector<entity::VariantKey>& keys, storage::RemoveOpts opts) override {
        remove_keys_sync(std::vector<entity::VariantKey>{keys}, opts);
    }

    void remove_keys_sync(std::vector<entity::VariantKey>&& keys, storage::RemoveOpts opts) override {
        if (keys.empty()) {
            return;
        }
        const auto batch_size = library_->max_delete_batch_size();
        if (!batch_size.has_value() || keys.size() <= *batch_size) {
            RemoveBatchTask{std::move(keys), library_, opts}();
            return;
        }
        for (auto& chunk : chunk_keys(std::move(keys), *batch_size)) {
            RemoveBatchTask{std::move(chunk), library_, opts}();
        }
    }

    std::optional<size_t> max_delete_batch_size() const override { return library_->max_delete_batch_size(); }

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

    std::function<folly::Future<pipelines::SegmentAndSlice>(pipelines::RangesAndKey&&)> make_uncompressed_reader(
            std::shared_ptr<std::unordered_set<std::string>> columns_to_decode
    ) override {
        return [this, columns_to_decode](pipelines::RangesAndKey&& ranges_and_key) {
            const auto key = ranges_and_key.key_;
            return read_and_continue(
                    key, library_, storage::ReadKeyOpts{}, DecodeSliceTask{std::move(ranges_and_key), columns_to_decode}
            );
        };
    }

    std::vector<folly::Future<bool>> batch_key_exists(const std::vector<entity::VariantKey>& keys) override {
        std::vector<folly::Future<bool>> res;
        res.reserve(keys.size());
        for (const auto& key : keys) {
            res.push_back(async::submit_io_task(KeyExistsTask(key, library_)));
        }
        return res;
    }

    SliceAndKey write_if_new(std::pair<DeDupLookupResult, FrameSlice>&& item) {
        auto [dedup_lookup, slice] = std::move(item);
        return util::variant_match(
                std::move(dedup_lookup),
                [&, this](NewObject&& obj) {
                    library_->write(obj);
                    return SliceAndKey{std::move(slice), std::move(obj).atom_key()};
                },
                [&](ExistingObject&& obj) { return SliceAndKey{std::move(slice), to_atom(std::move(obj))}; }
        );
    }

    folly::Future<SliceAndKey> async_write(
            folly::Future<std::tuple<stream::PartialKey, SegmentInMemory, pipelines::FrameSlice>>&& input_fut,
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
                .thenValue([this](auto&& item) { return write_if_new(std::move(item)); });
    }

    folly::Future<SliceAndKey> compress_and_schedule_async_write(
            std::tuple<stream::PartialKey, SegmentInMemory, pipelines::FrameSlice>&& input,
            const std::shared_ptr<DeDupMap>& de_dup_map
    ) override {
        auto [partial_key, seg, input_slice] = std::move(input);
        storage::KeySegmentPair key_seg = EncodeAtomTask{
                std::move(partial_key), ClockType::nanos_since_epoch(), std::move(seg), codec_, encoding_version_
        }();
        DeDupLookupResult dedup_lookup = lookup_match_in_dedup_map(de_dup_map, key_seg);
        return folly::via(
                &io_executor(),
                [dedup_lookup = std::move(dedup_lookup), slice = std::move(input_slice), this]() mutable {
                    return write_if_new(std::pair{std::move(dedup_lookup), std::move(slice)});
                }
        );
    }

    void set_failure_sim(const arcticdb::proto::storage::VersionStoreConfig::StorageFailureSimulator& cfg) override {
        library_->set_failure_sim(cfg);
    }

    std::string name() const override { return library_->name(); }

    storage::OpenMode open_mode() const override { return library_->open_mode(); }

  private:
    friend class arcticdb::toolbox::apy::LibraryTool;

    static std::vector<std::vector<entity::VariantKey>> chunk_keys(
            std::vector<entity::VariantKey>&& keys, size_t batch_size
    ) {
        std::vector<std::vector<entity::VariantKey>> chunks;
        chunks.reserve((keys.size() + batch_size - 1) / batch_size);
        for (size_t i = 0; i < keys.size(); i += batch_size) {
            const size_t end = std::min(i + batch_size, keys.size());
            chunks.emplace_back(std::make_move_iterator(keys.begin() + i), std::make_move_iterator(keys.begin() + end));
        }
        return chunks;
    }

    std::shared_ptr<storage::Library> library_;
    std::shared_ptr<arcticdb::proto::encoding::VariantCodec> codec_;
    const EncodingVersion encoding_version_;
};

} // namespace arcticdb::async
