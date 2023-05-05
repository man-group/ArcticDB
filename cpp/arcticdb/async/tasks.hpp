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
#include <arcticdb/entity/types.hpp>
#include <arcticdb/util/hash.hpp>
#include <arcticdb/stream/stream_utils.hpp>
#include <arcticdb/stream/protobuf_mappings.hpp>
#include <arcticdb/column_store/memory_segment.hpp>
#include <arcticdb/entity/variant_key.hpp>
#include <arcticdb/stream/stream_sink.hpp>
#include <arcticdb/async/base_task.hpp>
#include <arcticdb/pipeline/frame_slice.hpp>
#include <arcticdb/processing/processing_segment.hpp>
#include <arcticdb/util/constructors.hpp>

#include <type_traits>

namespace arcticdb::async {

using KeyType = entity::KeyType;
using AtomKey = entity::AtomKey;
using ContentHash = entity::ContentHash;
using IndexValue = entity::IndexValue;

struct EncodeAtomTask : BaseTask {
    using PartialKey = stream::StreamSink::PartialKey;
    PartialKey partial_key_;
    timestamp creation_ts_;
    SegmentInMemory segment_;
    std::shared_ptr<storage::Library> lib_;
    std::shared_ptr<arcticdb::proto::encoding::VariantCodec> codec_meta_;
    size_t id_;

    EncodeAtomTask(PartialKey &&pk,
                   timestamp creation_ts,
                   SegmentInMemory &&segment,
                   std::shared_ptr<storage::Library> lib,
                   std::shared_ptr<arcticdb::proto::encoding::VariantCodec> codec_meta,
                   size_t id)
        : partial_key_(std::move(pk)),
          creation_ts_(creation_ts),
          segment_(std::move(segment)),
          lib_(std::move(lib)),
          codec_meta_(std::move(codec_meta)),
          id_(id){}

    EncodeAtomTask(
        KeyType key_type,
        GenerationId gen_id,
        StreamId stream_id,
        IndexValue start_index,
        IndexValue end_index,
        timestamp creation_ts,
        SegmentInMemory &&segment,
        const std::shared_ptr<storage::Library> &lib,
        const std::shared_ptr<arcticdb::proto::encoding::VariantCodec> &codec_meta,
        size_t id
    )
        : EncodeAtomTask(
                PartialKey{key_type, gen_id, std::move(stream_id), std::move(start_index), std::move(end_index)},
                creation_ts,
                std::move(segment),
                lib,
                codec_meta,
                id) {
    }

    ARCTICDB_MOVE_ONLY_DEFAULT(EncodeAtomTask)

    storage::KeySegmentPair encode() {
        auto enc_seg = ::arcticdb::encode(std::move(segment_), *codec_meta_);
        auto content_hash = hash_segment_header(enc_seg.header());

        AtomKey k = partial_key_.build_key(creation_ts_, content_hash);
        return {std::move(k), std::move(enc_seg), id_};
    }

    storage::KeySegmentPair operator()() {
        ARCTICDB_SAMPLE(EncodeAtomTask, 0)
        return encode();
    }
};

struct EncodeSegmentTask : BaseTask {
    VariantKey key_;
    SegmentInMemory segment_;
    std::shared_ptr<storage::Library> lib_;
    std::shared_ptr<arcticdb::proto::encoding::VariantCodec> codec_meta_;
    size_t id_;

    EncodeSegmentTask(entity::VariantKey key,
                      SegmentInMemory &&segment,
                      std::shared_ptr<storage::Library> lib,
                      std::shared_ptr<arcticdb::proto::encoding::VariantCodec> codec_meta,
                      size_t id)
            : key_(std::move(key)),
              segment_(std::move(segment)),
              lib_(std::move(lib)),
              codec_meta_(std::move(codec_meta)),
              id_(id){}


    ARCTICDB_MOVE_ONLY_DEFAULT(EncodeSegmentTask)

    storage::KeySegmentPair encode() {
        auto enc_seg = ::arcticdb::encode(std::move(segment_), *codec_meta_);
        return {std::move(key_), std::move(enc_seg), size_t(id_)};
    }

    storage::KeySegmentPair operator()() {
        ARCTICDB_SAMPLE(EncodeSegmentTask, 0)
        return encode();
    }
};

struct EncodeRefTask : BaseTask {
    KeyType key_type_;
    StreamId id_;
    SegmentInMemory segment_;
    std::shared_ptr<arcticdb::proto::encoding::VariantCodec> codec_meta_;

    EncodeRefTask(
        KeyType key_type,
        StreamId stream_id,
        SegmentInMemory &&segment,
        std::shared_ptr<arcticdb::proto::encoding::VariantCodec> codec_meta
    )
        : key_type_(key_type),
          id_(std::move(stream_id)),
          segment_(std::move(segment)),
          codec_meta_(std::move(codec_meta)) {
    }

    ARCTICDB_MOVE_ONLY_DEFAULT(EncodeRefTask)

    [[nodiscard]] storage::KeySegmentPair encode() {
        auto enc_seg = ::arcticdb::encode(std::move(segment_), *codec_meta_);
        auto k = RefKey{id_, key_type_};
        return {std::move(k), std::move(enc_seg), size_t(0)};
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
        lib_->write(Composite<storage::KeySegmentPair>(std::move(key_seg)));
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
        lib_->update(Composite<storage::KeySegmentPair>(std::move(key_seg)), opts_);
        return k;
    }
};

struct ReadCompressedTask : BaseTask {
    entity::VariantKey key_;
    std::shared_ptr<storage::Library> lib_;
    storage::ReadKeyOpts opts_;

    ReadCompressedTask(entity::VariantKey key, std::shared_ptr<storage::Library> lib, storage::ReadKeyOpts opts)
        : key_(std::move(key)),
        lib_(std::move(lib)),
        opts_(opts) {
        ARCTICDB_DEBUG(log::storage(), "Creating read compressed task for key {}: {}",
                             variant_key_type(key_),
                             variant_key_view(key_));
    }

    ARCTICDB_MOVE_ONLY_DEFAULT(ReadCompressedTask)

    storage::KeySegmentPair read() {
        return std::visit([that=this](const auto &key) { return that->lib_->read(key, that->opts_); }, key_);
    }

    storage::KeySegmentPair operator()() {
        ARCTICDB_SAMPLE(ReadCompressed, 0)
        return read();
    }
};

struct ReadCompressedSlicesTask : BaseTask {
    Composite<pipelines::SliceAndKey> slice_and_keys_;
    std::shared_ptr<storage::Library> lib_;

    ReadCompressedSlicesTask(Composite<pipelines::SliceAndKey>&& sk, std::shared_ptr<storage::Library> lib)
            : slice_and_keys_(std::move(sk)),
            lib_(std::move(lib)) {
        ARCTICDB_DEBUG(log::storage(), "Creating read compressed slices task for slice and key {}",
                             slice_and_keys_);
    }

    ARCTICDB_MOVE_ONLY_DEFAULT(ReadCompressedSlicesTask)

     Composite<std::pair<Segment, pipelines::SliceAndKey>> read() {
        return slice_and_keys_.transform([that=this](const auto &sk){
            ARCTICDB_DEBUG(log::version(), "Reading key {}", sk.key());
            return std::make_pair(that->lib_->read(sk.key()).release_segment(), sk);
        });
     }

    Composite<std::pair<Segment, pipelines::SliceAndKey>> operator()() {
        ARCTICDB_SAMPLE(ReadCompressed, 0)
        return read();
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
                       const StreamId& stream_id,
                       VersionId version_id,
                       std::shared_ptr<storage::Library> lib) :
        source_key_(std::move(source_key)),
        key_type_(key_type),
        stream_id_(stream_id),
        version_id_(version_id),
        lib_(std::move(lib)) {
        ARCTICDB_DEBUG(log::storage(), "Creating copy compressed task for key {} -> {} {} {}",
                             variant_key_view(source_key_),
                             key_type_, stream_id_, version_id_);
    }

    ARCTICDB_MOVE_ONLY_DEFAULT(CopyCompressedTask)

    VariantKey copy() {
        return std::visit([that = this](const auto &source_key) {
            auto key_seg = that->lib_->read(source_key);
            auto target_key_seg = stream::make_target_key<ClockType>(that->key_type_, that->stream_id_, that->version_id_, source_key, std::move(key_seg.segment()));
            auto return_key = target_key_seg.variant_key();
            that->lib_->write(Composite<storage::KeySegmentPair>{std::move(target_key_seg) });
            return return_key;
        }, source_key_);
    }

    VariantKey operator()() {
        ARCTICDB_SAMPLE(CopyCompressed, 0)
        return copy();
    }
};

struct DecodeSegmentTask : BaseTask {
    ARCTICDB_MOVE_ONLY_DEFAULT(DecodeSegmentTask)

    DecodeSegmentTask() = default;

    std::pair<VariantKey, SegmentInMemory> operator()(storage::KeySegmentPair &&ks) const {
        ARCTICDB_SAMPLE(DecodeAtomTask, 0)

        auto key_seg = std::move(ks);
        ARCTICDB_DEBUG(log::storage(), "ReadAndDecodeAtomTask decoding segment of size {} with key {}",
                             key_seg.segment().total_segment_size(),
                             variant_key_view(key_seg.variant_key()));

        return {key_seg.variant_key(), decode(std::move(key_seg.segment()))};
    }
};

struct DecodeSlicesTask : BaseTask {
    ARCTICDB_MOVE_ONLY_DEFAULT(DecodeSlicesTask)

    StreamDescriptor desc_;
    std::shared_ptr<std::unordered_set<std::string>> filter_columns_;

    DecodeSlicesTask(
            const StreamDescriptor& desc,
            const std::shared_ptr<std::unordered_set<std::string>>& filter_columns)  :
                desc_(desc),
                filter_columns_(filter_columns) {
            }

    Composite<pipelines::SliceAndKey> operator()(Composite<std::pair<Segment, pipelines::SliceAndKey>> && skp) const {
        ARCTICDB_SAMPLE(DecodeAtomTask, 0)
        auto sk_pairs = std::move(skp);
        return sk_pairs.transform([that=this] (auto&& seg_slice_pair){
            ARCTICDB_DEBUG(log::version(), "Decoding slice {}", seg_slice_pair.second.key());
            return that->decode_into_slice(std::forward<std::pair<Segment, pipelines::SliceAndKey>>(seg_slice_pair));
        });
    }

private:
    pipelines::SliceAndKey decode_into_slice(std::pair<Segment, pipelines::SliceAndKey>&& sk_pair) const;
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

// This class is used to restart the pipeline following a repartition
struct MemSegmentPassthroughProcessingTask : BaseTask {
    std::shared_ptr<Store> store_;
    std::shared_ptr<std::vector<Clause>> clauses_;
    std::optional<Composite<ProcessingSegment>> starting_segments_;

    explicit MemSegmentPassthroughProcessingTask(
            const std::shared_ptr<Store> store,
            std::shared_ptr<std::vector<Clause>> clauses,
            Composite<ProcessingSegment>&& starting_segments) :
            store_(store),
            clauses_(std::move(clauses)),
            starting_segments_(std::move(starting_segments)){
    }

    [[nodiscard]]
    Composite<ProcessingSegment> process(Composite<ProcessingSegment>&& proc){
        auto procs = std::move(proc);
        for(const auto& clause : *clauses_) {
            if(clause.requires_repartition())
                break;

            procs = clause.process(store_, std::move(procs));
        }
        return procs;
    }

    ARCTICDB_MOVE_ONLY_DEFAULT(MemSegmentPassthroughProcessingTask)

    Composite<ProcessingSegment> operator()() {
        return process(std::move(starting_segments_.value()));
    }
};

struct MemSegmentProcessingTask : BaseTask {
    std::shared_ptr<Store> store_;
    std::shared_ptr<std::vector<Clause>> clauses_;

    explicit MemSegmentProcessingTask(
           const std::shared_ptr<Store> store,
           std::shared_ptr<std::vector<Clause>> clauses) :
        store_(store),
        clauses_(std::move(clauses)) {
    }

    ProcessingSegment slice_to_segment(Composite<pipelines::SliceAndKey>&& is) {
        auto inputs = std::move(is);
        return ProcessingSegment(inputs.as_range());
    }

    [[nodiscard]]
    Composite<ProcessingSegment> process(Composite<ProcessingSegment>&& proc){
        auto procs = std::move(proc);
        for(const auto& clause : *clauses_) {
            procs = clause.process(store_, std::move(procs));

            if(clause.requires_repartition())
                break;
        }
        return procs;
    }

    ARCTICDB_MOVE_ONLY_DEFAULT(MemSegmentProcessingTask)

    Composite<ProcessingSegment> operator()(Composite<pipelines::SliceAndKey>&& sk) {
        return process(Composite<ProcessingSegment>(slice_to_segment(std::move(sk))));
    }

};

struct MemSegmentFunctionTask : BaseTask {
    stream::StreamSource::DecodeContinuation func_;

    explicit MemSegmentFunctionTask(
            stream::StreamSource::DecodeContinuation&& func) :
            func_(std::move(func)) {
    }

    ARCTICDB_MOVE_ONLY_DEFAULT(MemSegmentFunctionTask)

    folly::Unit operator()(std::pair<VariantKey, SegmentInMemory> &&seg_pair) {
        func_(std::move(seg_pair.second));
        return folly::Unit{};
    }
};

struct DecodeMetadataTask : BaseTask {
    ARCTICDB_MOVE_ONLY_DEFAULT(DecodeMetadataTask)

    DecodeMetadataTask() = default;

    std::pair<std::optional<VariantKey>, std::optional<google::protobuf::Any>> operator()(storage::KeySegmentPair &&ks) const {
        ARCTICDB_SAMPLE(ReadMetadataTask, 0)
        auto key_seg = std::move(ks);
        ARCTICDB_DEBUG(log::storage(), "ReadAndDecodeMetadataTask decoding segment of size {} with key {}",
                             key_seg.segment().total_segment_size(), variant_key_view(key_seg.variant_key()));

        auto meta = decode_metadata(key_seg.segment());
        std::pair<std::optional<VariantKey>, std::optional<google::protobuf::Any>> output;
        output.first = std::make_optional(key_seg.variant_key());
        output.second = meta ? std::make_optional(std::move(meta.value())) : std::nullopt;
        return output;
    }
};

struct DecodeMetadataAndDescriptorTask : BaseTask {
    ARCTICDB_MOVE_ONLY_DEFAULT(DecodeMetadataAndDescriptorTask)

    DecodeMetadataAndDescriptorTask() = default;

    std::tuple<VariantKey, std::optional<google::protobuf::Any>, StreamDescriptor::Proto> operator()(storage::KeySegmentPair &&ks) const {
        ARCTICDB_SAMPLE(ReadMetadataAndDescriptorTask, 0)
        auto key_seg = std::move(ks);
        ARCTICDB_DEBUG(log::storage(), "DecodeMetadataAndDescriptorTask decoding segment of size {} with key {}",
                      key_seg.segment().total_segment_size(), variant_key_view(key_seg.variant_key()));

        auto meta = decode_metadata(key_seg.segment());
        return std::make_tuple<VariantKey, std::optional<google::protobuf::Any>, StreamDescriptor::Proto>(
            std::move(key_seg.variant_key()),
            meta ? std::make_optional(std::move(meta.value())) : std::nullopt,
            std::move(*key_seg.segment().header().mutable_stream_descriptor())
            );
    }
};

template<typename ConstVarKeyGetter,
        typename=std::enable_if_t<!std::is_reference_v<ConstVarKeyGetter> // Make obvious if iterator& is captured
                        && std::is_same_v<decltype(*std::declval<ConstVarKeyGetter>()), const entity::VariantKey&>>>
struct KeyExistsTask : BaseTask {
    ConstVarKeyGetter key_;
    std::shared_ptr<storage::Library> lib_;

    /**
     * @param key Could be a vector iterator or a pointer to a const VariantKey.
     */
    KeyExistsTask(ConstVarKeyGetter key, std::shared_ptr<storage::Library> lib): key_(key), lib_(std::move(lib)) {
        ARCTICDB_DEBUG(log::storage(), "Creating key exists task for key {}", variant_key_view(*key_));
    }

    bool operator()() {
        ARCTICDB_SAMPLE(KeyExistsTask, 0)
        return lib_->key_exists(*key_);
    }
};

struct WriteCompressedTask : BaseTask {
    storage::KeySegmentPair kv_;
    std::shared_ptr<storage::Library> lib_;

    WriteCompressedTask(storage::KeySegmentPair &&kv, std::shared_ptr<storage::Library> lib) :
    kv_(std::move(kv)), lib_(std::move(lib)) {
        ARCTICDB_DEBUG(log::storage(), "Creating write compressed task");
    }

    ARCTICDB_MOVE_ONLY_DEFAULT(WriteCompressedTask)

    folly::Future<folly::Unit> write() {
        lib_->write(Composite<storage::KeySegmentPair>(std::move(kv_)));
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
        lib_->write(Composite<storage::KeySegmentPair>(std::move(kvs_)));
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
        lib_->remove(Composite<VariantKey>(std::move(key_)), opts_);
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
        lib_->remove(Composite<VariantKey>(std::move(keys_)), opts_);
        return {};
    }
};

}