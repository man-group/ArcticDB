/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/entity/atom_key.hpp>
#include <arcticdb/storage/library.hpp>
#include <arcticdb/storage/storage_options.hpp>
#include <arcticdb/storage/store.hpp>
#include <arcticdb/entity/types.hpp>
#include <arcticdb/util/hash.hpp>
#include <arcticdb/stream/stream_utils.hpp>
#include <arcticdb/stream/protobuf_mappings.hpp>
#include <arcticdb/stream/stream_source.hpp>
#include <arcticdb/column_store/memory_segment.hpp>
#include <arcticdb/entity/variant_key.hpp>
#include <arcticdb/stream/stream_sink.hpp>
#include <arcticdb/async/base_task.hpp>
#include <arcticdb/async/bit_rate_stats.hpp>
#include <arcticdb/pipeline/frame_slice.hpp>
#include <arcticdb/processing/processing_unit.hpp>
#include <arcticdb/util/constructors.hpp>
#include <arcticdb/codec/codec.hpp>
#include <arcticdb/util/test/random_throw.hpp>

#include <type_traits>
#include <ranges>

namespace arcticdb::async {

using KeyType = entity::KeyType;
using AtomKey = entity::AtomKey;
using IndexValue = entity::IndexValue;

struct EncodeAtomTask : BaseTask {
    using PartialKey = stream::StreamSink::PartialKey;
    PartialKey partial_key_;
    timestamp creation_ts_;
    SegmentInMemory segment_;
    std::shared_ptr<arcticdb::proto::encoding::VariantCodec> codec_meta_;
    EncodingVersion encoding_version_;

    EncodeAtomTask(
        PartialKey &&pk,
        timestamp creation_ts,
        SegmentInMemory &&segment,
        std::shared_ptr<arcticdb::proto::encoding::VariantCodec> codec_meta,
        EncodingVersion encoding_version) :
            partial_key_(std::move(pk)),
            creation_ts_(creation_ts),
            segment_(std::move(segment)),
            codec_meta_(std::move(codec_meta)),
            encoding_version_(encoding_version) {
    }

    EncodeAtomTask(
        std::pair<PartialKey, SegmentInMemory>&& pk_seg,
        timestamp creation_ts,
        std::shared_ptr<arcticdb::proto::encoding::VariantCodec> codec_meta,
        EncodingVersion encoding_version) :
            partial_key_(std::move(pk_seg.first)),
            creation_ts_(creation_ts),
            segment_(std::move(pk_seg.second)),
            codec_meta_(std::move(codec_meta)),
            encoding_version_(encoding_version) {
    }

    EncodeAtomTask(
        KeyType key_type,
        GenerationId gen_id,
        StreamId stream_id,
        IndexValue start_index,
        IndexValue end_index,
        timestamp creation_ts,
        SegmentInMemory &&segment,
        const std::shared_ptr<arcticdb::proto::encoding::VariantCodec> &codec_meta,
        EncodingVersion encoding_version) :
            EncodeAtomTask(
                PartialKey{key_type, gen_id, std::move(stream_id), std::move(start_index), std::move(end_index)},
                creation_ts,
                std::move(segment),
                codec_meta,
                encoding_version) {
    }

    ARCTICDB_MOVE_ONLY_DEFAULT(EncodeAtomTask)

    storage::KeySegmentPair encode() {
        ARCTICDB_DEBUG(log::codec(), "Encoding object with partial key {}", partial_key_);
        ARCTICDB_DEBUG_THROW(5)
        auto enc_seg = ::arcticdb::encode_dispatch(std::move(segment_), *codec_meta_, encoding_version_);
        auto content_hash = get_segment_hash(enc_seg);

        AtomKey k = partial_key_.build_key(creation_ts_, content_hash);
        return {std::move(k), std::move(enc_seg)};
    }

    storage::KeySegmentPair operator()() {
        ARCTICDB_SAMPLE(EncodeAtomTask, 0)
        return encode();
    }
};

struct EncodeSegmentTask : BaseTask {
    VariantKey key_;
    SegmentInMemory segment_;
    std::shared_ptr<arcticdb::proto::encoding::VariantCodec> codec_meta_;
    EncodingVersion encoding_version_;

    EncodeSegmentTask(entity::VariantKey key,
                      SegmentInMemory &&segment,
                      std::shared_ptr<arcticdb::proto::encoding::VariantCodec> codec_meta,
                      EncodingVersion encoding_version)
            : key_(std::move(key)),
              segment_(std::move(segment)),
              codec_meta_(std::move(codec_meta)),
              encoding_version_(encoding_version){}


    ARCTICDB_MOVE_ONLY_DEFAULT(EncodeSegmentTask)

    storage::KeySegmentPair encode() {
        auto enc_seg = ::arcticdb::encode_dispatch(std::move(segment_), *codec_meta_, encoding_version_);
        return {std::move(key_), std::move(enc_seg)};
    }

    storage::KeySegmentPair operator()() {
        ARCTICDB_SAMPLE(EncodeSegmentTask, 0)
        ARCTICDB_DEBUG_THROW(5)
        return encode();
    }
};

struct EncodeRefTask : BaseTask {
    KeyType key_type_;
    StreamId id_;
    SegmentInMemory segment_;
    std::shared_ptr<arcticdb::proto::encoding::VariantCodec> codec_meta_;
    EncodingVersion encoding_version_;

    EncodeRefTask(
        KeyType key_type,
        StreamId stream_id,
        SegmentInMemory &&segment,
        std::shared_ptr<arcticdb::proto::encoding::VariantCodec> codec_meta,
        EncodingVersion encoding_version
    )
        : key_type_(key_type),
          id_(std::move(stream_id)),
          segment_(std::move(segment)),
          codec_meta_(std::move(codec_meta)),
          encoding_version_(encoding_version){
    }

    ARCTICDB_MOVE_ONLY_DEFAULT(EncodeRefTask)

    [[nodiscard]] storage::KeySegmentPair encode() {
        auto enc_seg = ::arcticdb::encode_dispatch(std::move(segment_), *codec_meta_, encoding_version_);
        auto k = RefKey{id_, key_type_};
        return {std::move(k), std::move(enc_seg)};
    }

    storage::KeySegmentPair operator()() {
        ARCTICDB_SAMPLE(EncodeAtomTask, 0)
        return encode();
    }
};

struct WriteSegmentTask : BaseTask {
    std::shared_ptr<storage::Library> lib_;

    explicit WriteSegmentTask(std::shared_ptr<storage::Library> lib) :
        lib_(std::move(lib)) {
    }

    ARCTICDB_MOVE_ONLY_DEFAULT(WriteSegmentTask)

    VariantKey operator()(storage::KeySegmentPair &&key_seg) const {
        ARCTICDB_SAMPLE(WriteSegmentTask, 0)
        auto k = key_seg.variant_key();
        lib_->write(std::move(key_seg));
        return k;
    }
};

struct WriteIfNoneTask : BaseTask {
    std::shared_ptr<storage::Library> lib_;

    explicit WriteIfNoneTask(std::shared_ptr<storage::Library> lib) :
    lib_(std::move(lib)) {
    }

    ARCTICDB_MOVE_ONLY_DEFAULT(WriteIfNoneTask)

    VariantKey operator()(storage::KeySegmentPair &&key_seg) const {
        ARCTICDB_SAMPLE(WriteSegmentTask, 0)
        auto k = key_seg.variant_key();
        lib_->write_if_none(std::move(key_seg));
        return k;
    }
};

struct UpdateSegmentTask : BaseTask {
    std::shared_ptr<storage::Library> lib_;
    storage::UpdateOpts opts_;

    explicit UpdateSegmentTask(std::shared_ptr<storage::Library> lib, storage::UpdateOpts opts) :
        lib_(std::move(lib)),
        opts_(opts) {
    }

    ARCTICDB_MOVE_ONLY_DEFAULT(UpdateSegmentTask)

    VariantKey operator()(storage::KeySegmentPair &&key_seg) const {
        ARCTICDB_SAMPLE(UpdateSegmentTask, 0)
        auto k = key_seg.variant_key();
        lib_->update(std::move(key_seg), opts_);
        return k;
    }
};

template <typename Callable>
struct KeySegmentContinuation {
    folly::Future<storage::KeySegmentPair> key_seg_;
    Callable continuation_;
};

inline folly::Future<storage::KeySegmentPair> read_dispatch(entity::VariantKey&& variant_key, const std::shared_ptr<storage::Library>& lib, const storage::ReadKeyOpts& opts) {
    return util::variant_match(variant_key, [&lib, &opts](auto&& key) {
        return lib->read(key, opts);
    });
}

inline storage::KeySegmentPair read_sync_dispatch(const entity::VariantKey& variant_key, const std::shared_ptr<storage::Library>& lib, storage::ReadKeyOpts opts) {
    return util::variant_match(variant_key, [&lib, opts](const auto &key) {
        return lib->read_sync(key, opts);
    });
}

template <typename Callable>
struct ReadCompressedTask : BaseTask {
    entity::VariantKey key_;
    std::shared_ptr<storage::Library> lib_;
    storage::ReadKeyOpts opts_;
    Callable continuation_;

    using ContinuationType = Callable;

    ReadCompressedTask(entity::VariantKey key, std::shared_ptr<storage::Library> lib, storage::ReadKeyOpts opts, Callable&& continuation)
        : key_(std::move(key)),
        lib_(std::move(lib)),
        opts_(opts),
        continuation_(std::move(continuation)){
        ARCTICDB_DEBUG(log::storage(), "Creating read compressed task for key {}: {}",
                             variant_key_type(key_),
                             variant_key_view(key_));
    }

    ARCTICDB_MOVE_ONLY_DEFAULT(ReadCompressedTask)

    KeySegmentContinuation<ContinuationType> operator()() {
        ARCTICDB_SAMPLE(ReadCompressed, 0)
        return KeySegmentContinuation<decltype(continuation_)>{read_dispatch(std::move(key_), lib_, opts_), std::move(continuation_)};
    }
};

struct PassThroughTask : BaseTask {
    PassThroughTask() = default;

   storage::KeySegmentPair operator()(storage::KeySegmentPair &&ks) const {
        return ks;
    }
};

template <typename ClockType>
struct CopyCompressedTask : BaseTask {
    entity::VariantKey source_key_;
    KeyType key_type_;
    StreamId stream_id_;
    VersionId version_id_;
    std::shared_ptr<storage::Library> lib_;

    CopyCompressedTask(entity::VariantKey source_key,
                       KeyType key_type,
                       StreamId stream_id,
                       VersionId version_id,
                       std::shared_ptr<storage::Library> lib) :
        source_key_(std::move(source_key)),
        key_type_(key_type),
        stream_id_(std::move(stream_id)),
        version_id_(version_id),
        lib_(std::move(lib)) {
        ARCTICDB_DEBUG(log::storage(), "Creating copy compressed task for key {} -> {} {} {}",
                             variant_key_view(source_key_),
                             key_type_, stream_id_, version_id_);
    }

    ARCTICDB_MOVE_ONLY_DEFAULT(CopyCompressedTask)

    VariantKey copy() {
        return std::visit([this](const auto &source_key) {
            auto key_seg = lib_->read_sync(source_key);
            auto target_key_seg = stream::make_target_key<ClockType>(key_type_, stream_id_, version_id_, source_key, std::move(key_seg.segment()));
            auto return_key = target_key_seg.variant_key();
            lib_->write(std::move(target_key_seg));
            return return_key;
        }, source_key_);
    }

    VariantKey operator()() {
        ARCTICDB_SAMPLE(CopyCompressed, 0)
        return copy();
    }
};

// Used in arcticdb-enterprise, do not remove without checking whether it is still used there
struct CopyCompressedInterStoreTask : async::BaseTask {

    using AllOk = std::monostate;
    using FailedTargets = std::unordered_set<std::string>;
    using ProcessingResult = std::variant<AllOk, FailedTargets>;

    CopyCompressedInterStoreTask(entity::VariantKey key_to_read,
                                 std::optional<entity::AtomKey> key_to_write,
                                 bool check_key_exists_on_targets,
                                 bool retry_on_failure,
                                 std::shared_ptr<Store> source_store,
                                 std::vector<std::shared_ptr<Store>> target_stores,
                                 std::shared_ptr<BitRateStats> bit_rate_stats=nullptr)
        : key_to_read_(std::move(key_to_read)),
          key_to_write_(std::move(key_to_write)),
          check_key_exists_on_targets_(check_key_exists_on_targets),
          retry_on_failure_(retry_on_failure),
          source_store_(std::move(source_store)),
          target_stores_(std::move(target_stores)),
          bit_rate_stats_(std::move(bit_rate_stats)){
        ARCTICDB_DEBUG(log::storage(), "Creating copy compressed inter-store task from key {}: {} -> {}: {}",
                       variant_key_type(key_to_read_),
                       variant_key_view(key_to_read_),
                       key_to_write_.has_value() ? variant_key_type(key_to_write_.value()) : variant_key_type(key_to_read_),
                       key_to_write_.has_value() ? variant_key_view(key_to_write_.value()) : variant_key_view(key_to_read_));
    }

    ARCTICDB_MOVE_ONLY_DEFAULT(CopyCompressedInterStoreTask)

    ProcessingResult operator()() {
        auto res = copy();

        if (!res.empty() && retry_on_failure_) {
            res = copy();
        }

        if (!res.empty()) {
            return res;
        }

        return AllOk{};
    }

private:
    entity::VariantKey key_to_read_;
    std::optional<entity::AtomKey> key_to_write_;
    bool check_key_exists_on_targets_;
    bool retry_on_failure_;
    std::shared_ptr<Store> source_store_;
    std::vector<std::shared_ptr<Store>> target_stores_;
    std::shared_ptr<BitRateStats> bit_rate_stats_;

    // Returns an empty set if the copy succeeds, otherwise the set contains the names of the target stores that failed
    std::unordered_set<std::string> copy() {
        ARCTICDB_SAMPLE(copy, 0)
        std::size_t bytes{0};
        interval timer;
        timer.start();
        if (check_key_exists_on_targets_) {
            target_stores_.erase(std::remove_if(target_stores_.begin(), target_stores_.end(),
                                                [that=this](const std::shared_ptr<Store>& target_store) {
                                                    return target_store->key_exists_sync(that->key_to_read_);
                                                }), target_stores_.end());
        }
        std::unordered_set<std::string> failed_targets;
        if (!target_stores_.empty()) {
            storage::KeySegmentPair key_segment_pair;
            try {
                key_segment_pair = source_store_->read_compressed_sync(key_to_read_);
            } catch (const storage::KeyNotFoundException& e) {
                log::storage().debug("Key {} not found on the source: {}", variant_key_view(key_to_read_), e.what());
                return failed_targets;
            }
            bytes = key_segment_pair.segment().size();
            if (key_to_write_.has_value()) {
                key_segment_pair.set_key(*key_to_write_);
            }

            for (auto & target_store : target_stores_) {
                try {
                    target_store->write_compressed_sync(key_segment_pair);
                } catch (const storage::DuplicateKeyException& e) {
                    log::storage().debug("Key {} already exists on the target: {}", variant_key_view(key_to_read_), e.what());
                } catch (const std::exception& e) {
                    auto name = target_store->name();
                    log::storage().error("Failed to write key {} to store {}: {}", variant_key_view(key_to_read_), name, e.what());
                    failed_targets.insert(name);
                }
            }
        }
        timer.end();
        auto time_ms = timer.get_results_total() * 1000;
        if (bit_rate_stats_) {
            bit_rate_stats_->add_stat(bytes, time_ms);
        }

        return failed_targets;
    }
};

struct DecodeSegmentTask : BaseTask {
    ARCTICDB_MOVE_ONLY_DEFAULT(DecodeSegmentTask)

    DecodeSegmentTask() = default;

    std::pair<VariantKey, SegmentInMemory> operator()(storage::KeySegmentPair &&ks) const {
        ARCTICDB_SAMPLE(DecodeAtomTask, 0)

        auto key_seg = std::move(ks);
        ARCTICDB_DEBUG(log::storage(), "ReadAndDecodeAtomTask decoding segment with key {}",
                             variant_key_view(key_seg.variant_key()));

        return {key_seg.variant_key(), decode_segment(std::move(key_seg.segment()))};
    }
};

struct DecodeSliceTask : BaseTask {
    ARCTICDB_MOVE_ONLY_DEFAULT(DecodeSliceTask)

    pipelines::RangesAndKey ranges_and_key_;
    std::shared_ptr<std::unordered_set<std::string>> columns_to_decode_;

    explicit DecodeSliceTask(
            pipelines::RangesAndKey&& ranges_and_key,
            std::shared_ptr<std::unordered_set<std::string>> columns_to_decode):
            ranges_and_key_(std::move(ranges_and_key)),
            columns_to_decode_(std::move(columns_to_decode)) {
    }

    pipelines::SegmentAndSlice operator()(storage::KeySegmentPair&& key_segment_pair) {
        ARCTICDB_SAMPLE(DecodeSliceTask, 0)
        ARCTICDB_DEBUG(log::memory(), "Decode into slice {}", key_segment_pair.variant_key());
        return decode_into_slice(std::move(key_segment_pair));
    }

private:
    pipelines::SegmentAndSlice decode_into_slice(storage::KeySegmentPair&& key_segment_pair);
};

struct SegmentFunctionTask : BaseTask {
    stream::StreamSource::ReadContinuation func_;

    explicit SegmentFunctionTask(
        stream::StreamSource::ReadContinuation func) :
        func_(std::move(func)) {
    }

    ARCTICDB_MOVE_ONLY_DEFAULT(SegmentFunctionTask)

     entity::VariantKey operator()(storage::KeySegmentPair &&key_seg) {
        ARCTICDB_SAMPLE(SegmentFunctionTask, 0)
        return func_(std::move(key_seg));
    }
};

struct MemSegmentProcessingTask : BaseTask {
    std::vector<std::shared_ptr<Clause>> clauses_;
    std::vector<EntityId> entity_ids_;
    timestamp creation_time_;

    explicit MemSegmentProcessingTask(
           std::vector<std::shared_ptr<Clause>> clauses,
           std::vector<EntityId>&& entity_ids) :
        clauses_(std::move(clauses)),
        entity_ids_(std::move(entity_ids)),
        creation_time_(util::SysClock::coarse_nanos_since_epoch()){
    }

    ARCTICDB_MOVE_ONLY_DEFAULT(MemSegmentProcessingTask)

    std::vector<EntityId> operator()() {
        ARCTICDB_DEBUG_THROW(5)
        const auto nanos_start = util::SysClock::coarse_nanos_since_epoch();
        const auto time_in_queue = double(nanos_start - creation_time_) / BILLION;
        ARCTICDB_RUNTIME_DEBUG(log::inmem(), "Segment processing task running after {}s queue time", time_in_queue);
        for (auto it = clauses_.cbegin(); it != clauses_.cend(); ++it) {
            entity_ids_ = (*it)->process(std::move(entity_ids_));

            auto next_it = std::next(it);
            if(next_it != clauses_.cend() && (*it)->clause_info().output_structure_ != (*next_it)->clause_info().input_structure_)
                break;
        }
        const auto nanos_end = util::SysClock::coarse_nanos_since_epoch();
        const auto time_taken = double(nanos_end - nanos_start) / BILLION;
        ARCTICDB_RUNTIME_DEBUG(log::inmem(), "Segment processing task completed after {}s run time", time_taken);
        return std::move(entity_ids_);
    }

};

struct DecodeMetadataTask : BaseTask {
    ARCTICDB_MOVE_ONLY_DEFAULT(DecodeMetadataTask)

    DecodeMetadataTask() = default;

    std::pair<std::optional<VariantKey>, std::optional<google::protobuf::Any>> operator()(storage::KeySegmentPair &&ks) const {
        ARCTICDB_SAMPLE(ReadMetadataTask, 0)
        auto key_seg = std::move(ks);
        ARCTICDB_DEBUG(log::storage(), "ReadAndDecodeMetadataTask decoding segment with key {}", variant_key_view(key_seg.variant_key()));

        auto meta = decode_metadata_from_segment(key_seg.segment());
        std::pair<VariantKey, std::optional<google::protobuf::Any>> output;
        output.first = key_seg.variant_key();
        output.second = std::move(meta);

        return output;
    }
};

struct DecodeTimeseriesDescriptorTask : BaseTask {
    ARCTICDB_MOVE_ONLY_DEFAULT(DecodeTimeseriesDescriptorTask)

    DecodeTimeseriesDescriptorTask() = default;

    std::pair<VariantKey, TimeseriesDescriptor> operator()(storage::KeySegmentPair &&ks) const {
        ARCTICDB_SAMPLE(DecodeTimeseriesDescriptorTask, 0)
        auto key_seg = std::move(ks);
        ARCTICDB_DEBUG(log::storage(), "DecodeTimeseriesDescriptorTask decoding segment with key {}", variant_key_view(key_seg.variant_key()));

        auto maybe_desc = decode_timeseries_descriptor(key_seg.segment());

        util::check(static_cast<bool>(maybe_desc), "Failed to decode timeseries descriptor");
        return std::make_pair(
            std::move(key_seg.variant_key()),
            std::move(*maybe_desc));

    }
};

struct DecodeMetadataAndDescriptorTask : BaseTask {
    ARCTICDB_MOVE_ONLY_DEFAULT(DecodeMetadataAndDescriptorTask)

    DecodeMetadataAndDescriptorTask() = default;

    std::tuple<VariantKey, std::optional<google::protobuf::Any>, StreamDescriptor> operator()(storage::KeySegmentPair &&ks) const {
        ARCTICDB_SAMPLE(ReadMetadataAndDescriptorTask, 0)
        ARCTICDB_DEBUG_THROW(5)
        auto key_seg = std::move(ks);
        ARCTICDB_DEBUG(log::storage(), "DecodeMetadataAndDescriptorTask decoding segment with key {}", variant_key_view(key_seg.variant_key()));

        auto [any, descriptor] = decode_metadata_and_descriptor_fields(key_seg.segment());
        return std::make_tuple(
            std::move(key_seg.variant_key()),
            std::move(any),
            std::move(descriptor)
            );
    }
};
struct KeyExistsTask : BaseTask {
    const VariantKey key_;
    std::shared_ptr<storage::Library> lib_;

    KeyExistsTask(auto &&key, std::shared_ptr<storage::Library> lib): key_(std::forward<decltype(key)>(key)), lib_(std::move(lib)) {
        ARCTICDB_DEBUG(log::storage(), "Creating key exists task for key {}",key_);
    }

    bool operator()() {
        ARCTICDB_SAMPLE(KeyExistsTask, 0)
        return lib_->key_exists(key_);
    }
};

struct WriteCompressedTask : BaseTask {
    storage::KeySegmentPair kv_;
    std::shared_ptr<storage::Library> lib_;

    WriteCompressedTask(storage::KeySegmentPair&& key_seg, std::shared_ptr<storage::Library> lib) :
            kv_(std::move(key_seg)),
            lib_(std::move(lib)) {
        ARCTICDB_DEBUG(log::storage(), "Creating write compressed task");
    }

    ARCTICDB_MOVE_ONLY_DEFAULT(WriteCompressedTask)

    folly::Future<folly::Unit> write() {
        lib_->write(std::move(kv_));
        return folly::makeFuture();
    }

    folly::Future<folly::Unit> operator()() {
        ARCTICDB_SAMPLE(WriteCompressed, 0)
        return write();
    }
};

struct WriteCompressedBatchTask : BaseTask {
    std::vector<storage::KeySegmentPair> kvs_;
    std::shared_ptr<storage::Library> lib_;

    WriteCompressedBatchTask(std::vector<storage::KeySegmentPair> &&kvs, std::shared_ptr<storage::Library> lib) : kvs_(
        std::move(kvs)), lib_(std::move(lib)) {
        util::check(!kvs_.empty(), "WriteCompressedBatch task created with no data");

        ARCTICDB_DEBUG(log::storage(), "Creating read and decode task for {} keys", kvs_.size());
    }

    ARCTICDB_MOVE_ONLY_DEFAULT(WriteCompressedBatchTask)

    folly::Future<folly::Unit> write() {
        for(auto&& kv : kvs_)
            lib_->write(std::move(kv));

        return folly::makeFuture();
    }

    folly::Future<folly::Unit> operator()() {
        ARCTICDB_SAMPLE(WriteCompressedBatch, 0)
        return write();
    }
};

struct RemoveTask : BaseTask {
    VariantKey key_;
    std::shared_ptr<storage::Library> lib_;
    storage::RemoveOpts opts_;

    RemoveTask(const VariantKey &key_, std::shared_ptr<storage::Library> lib_, storage::RemoveOpts opts) :
            key_(key_),
            lib_(std::move(lib_)),
            opts_(opts){
        ARCTICDB_DEBUG(log::storage(), "Creating remove task for key {}: {}", variant_key_type(key_), variant_key_view(key_));
    }

    ARCTICDB_MOVE_ONLY_DEFAULT(RemoveTask)

    stream::StreamSink::RemoveKeyResultType operator()() {
        lib_->remove(std::move(key_), opts_);
        return {};
    }
};

struct RemoveBatchTask : BaseTask {
    std::vector<VariantKey> keys_;
    std::shared_ptr<storage::Library> lib_;
    storage::RemoveOpts opts_;

    RemoveBatchTask(
        std::vector<VariantKey> key_,
        std::shared_ptr<storage::Library> lib_,
        storage::RemoveOpts opts) :
        keys_(std::move(key_)),
        lib_(std::move(lib_)),
        opts_(opts){
        ARCTICDB_DEBUG(log::storage(), "Creating remove task for {} keys", keys_.size());
    }

    ARCTICDB_MOVE_ONLY_DEFAULT(RemoveBatchTask)


    std::vector<stream::StreamSink::RemoveKeyResultType> operator()() {
        lib_->remove(std::span(keys_), opts_);
        return {};
    }
};

}