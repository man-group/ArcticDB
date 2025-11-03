/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/column_store/memory_segment.hpp>
#include <arcticdb/entity/index_range.hpp>
#include <arcticdb/entity/variant_key.hpp>
#include <arcticdb/storage/storage.hpp>
#include <arcticdb/storage/storage_options.hpp>
#include <arcticdb/version/de_dup_map.hpp>
#include <arcticdb/pipeline/frame_slice.hpp>

#include <folly/synchronization/NativeSemaphore.h>
#include <folly/futures/Future.h>
// FIXME: winnt.h is included by folly/futures/Future.h at some point and adds unwanted macros
#ifdef DELETE
#undef DELETE
#endif

namespace arcticdb::stream {

using KeyType = entity::KeyType;
using IndexValue = entity::IndexValue;

struct PartialKey {
    KeyType key_type;
    VersionId version_id;
    StreamId stream_id;
    IndexValue start_index;
    IndexValue end_index;

    [[nodiscard]] AtomKey build_key(timestamp creation_ts, ContentHash content_hash) const {
        return entity::atom_key_builder()
                .gen_id(version_id)
                .start_index(start_index)
                .end_index(end_index)
                .content_hash(content_hash)
                .creation_ts(creation_ts)
                .build(stream_id, key_type);
    }
};

struct StreamSink {
    /**
     The remove_key{,s,sync} methods used to return the key to indicate success/not. However, most implementations
     moved() the key internally to avoid expensive string copying, so no key can actually be returned.
     In the future, may return a bool.
    */
    using RemoveKeyResultType = folly::Unit;

    virtual ~StreamSink() = default;

    [[nodiscard]] virtual folly::Future<entity::VariantKey> write(
            KeyType key_type, VersionId version_id, const StreamId& stream_id, IndexValue start_index,
            IndexValue end_index, SegmentInMemory&& segment
    ) = 0;

    [[nodiscard]] virtual folly::Future<entity::VariantKey> write(
            stream::KeyType key_type, VersionId version_id, const StreamId& stream_id, timestamp creation_ts,
            IndexValue start_index, IndexValue end_index, SegmentInMemory&& segment
    ) = 0;

    [[nodiscard]] virtual folly::Future<entity::VariantKey> write(
            KeyType key_type, const StreamId& stream_id, SegmentInMemory&& segment
    ) = 0;

    virtual entity::VariantKey write_sync(
            stream::KeyType key_type, VersionId version_id, const StreamId& stream_id, IndexValue start_index,
            IndexValue end_index, SegmentInMemory&& segment
    ) = 0;

    [[nodiscard]] virtual folly::Future<entity::VariantKey> update(
            const VariantKey& key, SegmentInMemory&& segment, storage::UpdateOpts = storage::UpdateOpts{}
    ) = 0;

    [[nodiscard]] virtual folly::Future<entity::VariantKey> write(PartialKey pk, SegmentInMemory&& segment) = 0;

    // shared_ptr for semaphore as executing futures need guarantees it is in a valid state, so need to participate
    // in ownership
    [[nodiscard]] virtual folly::Future<entity::VariantKey> write_maybe_blocking(
            PartialKey pk, SegmentInMemory&& segment, std::shared_ptr<folly::NativeSemaphore> semaphore
    ) = 0;

    virtual entity::VariantKey write_sync(PartialKey pk, SegmentInMemory&& segment) = 0;

    virtual entity::VariantKey write_sync(KeyType key_type, const StreamId& stream_id, SegmentInMemory&& segment) = 0;

    virtual bool supports_atomic_writes() const = 0;

    virtual entity::VariantKey write_if_none_sync(
            KeyType key_type, const StreamId& stream_id, SegmentInMemory&& segment
    ) = 0;

    [[nodiscard]] virtual folly::Future<folly::Unit> write_compressed(storage::KeySegmentPair ks) = 0;

    virtual void write_compressed_sync(storage::KeySegmentPair ks) = 0;

    virtual void update_compressed_sync(storage::KeySegmentPair ks, storage::UpdateOpts opts) = 0;

    [[nodiscard]] virtual folly::Future<pipelines::SliceAndKey> async_write(
            folly::Future<std::tuple<PartialKey, SegmentInMemory, pipelines::FrameSlice>>&& input_fut,
            const std::shared_ptr<DeDupMap>& de_dup_map
    ) = 0;

    [[nodiscard]] virtual folly::Future<pipelines::SliceAndKey> async_write(
            std::tuple<stream::PartialKey, SegmentInMemory, pipelines::FrameSlice>&& input,
            const std::shared_ptr<DeDupMap>& de_dup_map
    ) = 0;

    virtual bool is_path_valid(const std::string_view path) const = 0;

    [[nodiscard]] virtual folly::Future<folly::Unit> batch_write_compressed(std::vector<storage::KeySegmentPair> kvs
    ) = 0;

    [[nodiscard]] virtual folly::Future<RemoveKeyResultType> remove_key(
            const entity::VariantKey& key, storage::RemoveOpts opts = storage::RemoveOpts{}
    ) = 0;

    virtual RemoveKeyResultType remove_key_sync(
            const entity::VariantKey& key, storage::RemoveOpts opts = storage::RemoveOpts{}
    ) = 0;

    [[nodiscard]] virtual folly::Future<std::vector<RemoveKeyResultType>> remove_keys(
            const std::vector<entity::VariantKey>& keys, storage::RemoveOpts opts = storage::RemoveOpts{}
    ) = 0;

    [[nodiscard]] virtual folly::Future<std::vector<RemoveKeyResultType>> remove_keys(
            std::vector<entity::VariantKey>&& keys, storage::RemoveOpts opts = storage::RemoveOpts{}
    ) = 0;

    virtual std::vector<RemoveKeyResultType> remove_keys_sync(
            const std::vector<entity::VariantKey>& keys, storage::RemoveOpts opts = storage::RemoveOpts{}
    ) = 0;

    virtual std::vector<RemoveKeyResultType> remove_keys_sync(
            std::vector<entity::VariantKey>&& keys, storage::RemoveOpts opts = storage::RemoveOpts{}
    ) = 0;

    virtual timestamp current_timestamp() = 0;
};

} // namespace arcticdb::stream

namespace fmt {
using namespace arcticdb::stream;

template<>
struct formatter<PartialKey> {
    template<typename ParseContext>
    constexpr auto parse(ParseContext& ctx) {
        return ctx.begin();
    }

    template<typename FormatContext>
    auto format(const PartialKey& pk, FormatContext& ctx) const {
        return fmt::format_to(
                ctx.out(), "'{}:{}:{}:{}:{}", pk.key_type, pk.stream_id, pk.version_id, pk.start_index, pk.end_index
        );
    }
};
} // namespace fmt
