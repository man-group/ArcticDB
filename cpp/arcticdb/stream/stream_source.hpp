/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/column_store/memory_segment.hpp>
#include <arcticdb/codec/segment.hpp>
#include <arcticdb/storage/storage.hpp>
#include <arcticdb/storage/storage_options.hpp>
#include <arcticdb/entity/atom_key.hpp>
#include <arcticdb/util/configs_map.hpp>
#include <arcticdb/async/batch_read_args.hpp>
#include <arcticdb/processing/clause.hpp>

#include <folly/futures/Future.h>
#include <folly/Function.h>

namespace arcticdb::stream {

using ReadKeyOutput = std::pair<entity::VariantKey, SegmentInMemory>;

struct StreamSource {

    virtual ~StreamSource() = default;

    virtual folly::Future<ReadKeyOutput> read(
        const entity::VariantKey &key,
        storage::ReadKeyOpts opts = storage::ReadKeyOpts{}) = 0;

    virtual ReadKeyOutput read_sync(
        const entity::VariantKey &key,
        storage::ReadKeyOpts opts = storage::ReadKeyOpts{}) = 0;

    virtual folly::Future<storage::KeySegmentPair> read_compressed(
        const entity::VariantKey &key,
        storage::ReadKeyOpts opts = storage::ReadKeyOpts{}) = 0;

    virtual void iterate_type(
        KeyType type,
        entity::IterateTypeVisitor func,
        const std::string &prefix = std::string{}) = 0;

    [[nodiscard]] virtual folly::Future<bool> key_exists(const entity::VariantKey &key) = 0;
    virtual bool key_exists_sync(const entity::VariantKey &key) = 0;
    virtual bool supports_prefix_matching() const = 0;
    virtual bool fast_delete() = 0;

    virtual std::vector<storage::KeySegmentPair> batch_read_compressed(
        std::vector<entity::VariantKey> &&keys, const BatchReadArgs &args, bool may_fail) = 0;

    using ReadContinuation = folly::Function<entity::VariantKey(storage::KeySegmentPair &&)>;

    virtual folly::Future<std::vector<entity::VariantKey>> batch_read_compressed(
        std::vector<entity::VariantKey>&& keys,
        std::vector<ReadContinuation>&& continuations,
        const BatchReadArgs &args) = 0;

    /**
     * See storage_utils for the wrapper filter_keys_on_existence(vector).
     */
    [[nodiscard]] virtual std::vector<folly::Future<bool>> batch_key_exists(
            const std::vector<entity::VariantKey> &keys) = 0;

    [[nodiscard]] virtual bool batch_all_keys_exist_sync(
            const std::unordered_set<entity::VariantKey> &keys) = 0;

    using DecodeContinuation = folly::Function<folly::Unit(SegmentInMemory &&)>;

    virtual std::vector<Composite<ProcessingUnit>> batch_read_uncompressed(
        std::vector<Composite<pipelines::SliceAndKey>> &&keys,
        const std::vector<std::shared_ptr<Clause>>& clauses,
        const std::shared_ptr<std::unordered_set<std::string>>& filter_columns,
        const BatchReadArgs &args) = 0;

    virtual folly::Future<std::pair<std::optional<VariantKey>, std::optional<google::protobuf::Any>>> read_metadata(
        const entity::VariantKey &key,
        storage::ReadKeyOpts opts = storage::ReadKeyOpts{}) = 0;

    virtual folly::Future<std::tuple<VariantKey, std::optional<google::protobuf::Any>, StreamDescriptor>> read_metadata_and_descriptor(
        const entity::VariantKey& key,
        storage::ReadKeyOpts opts = storage::ReadKeyOpts{}
        ) = 0;

    virtual folly::Future<std::pair<VariantKey, TimeseriesDescriptor>>
        read_timeseries_descriptor(const entity::VariantKey& key) = 0;


};

} // namespace arcticdb::stream