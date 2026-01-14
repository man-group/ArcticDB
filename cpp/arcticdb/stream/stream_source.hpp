/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/storage/storage.hpp>
#include <arcticdb/storage/storage_options.hpp>
#include <arcticdb/storage/storage_exceptions.hpp>
#include <arcticdb/async/batch_read_args.hpp>
#include <arcticdb/processing/clause.hpp>

#include <folly/futures/Future.h>
#include <folly/Function.h>

namespace arcticdb::stream {

struct StreamSource {

    virtual ~StreamSource() = default;

    virtual folly::Future<std::pair<entity::VariantKey, SegmentInMemory>> read(
            const entity::VariantKey& key, storage::ReadKeyOpts opts = storage::ReadKeyOpts{}
    ) = 0;

    virtual std::pair<entity::VariantKey, SegmentInMemory> read_sync(
            const entity::VariantKey& key, storage::ReadKeyOpts opts = storage::ReadKeyOpts{}
    ) = 0;

    virtual folly::Future<storage::KeySegmentPair> read_compressed(
            const entity::VariantKey& key, storage::ReadKeyOpts opts = storage::ReadKeyOpts{}
    ) = 0;

    virtual storage::KeySegmentPair read_compressed_sync(
            const entity::VariantKey& key, storage::ReadKeyOpts opts = storage::ReadKeyOpts{}
    ) = 0;

    virtual void iterate_type(
            KeyType type, const entity::IterateTypeVisitor& func, const std::string& prefix = std::string{}
    ) = 0;

    [[nodiscard]] virtual folly::Future<std::shared_ptr<storage::ObjectSizes>> get_object_sizes(
            KeyType type, const std::optional<StreamId>& stream_id
    ) = 0;

    [[nodiscard]] virtual folly::Future<folly::Unit> visit_object_sizes(
            KeyType type, const std::optional<StreamId>& stream_id_opt, storage::ObjectSizesVisitor visitor
    ) = 0;

    virtual bool scan_for_matching_key(KeyType key_type, const IterateTypePredicate& predicate) = 0;

    [[nodiscard]] virtual folly::Future<bool> key_exists(const entity::VariantKey& key) = 0;
    [[nodiscard]] virtual bool key_exists_sync(const entity::VariantKey& key) = 0;
    [[nodiscard]] virtual bool supports_prefix_matching() const = 0;
    [[nodiscard]] virtual bool fast_delete() = 0;

    using ReadContinuation = folly::Function<VariantKey(storage::KeySegmentPair&&)>;
    using KeySizeCalculators = std::vector<std::pair<VariantKey, ReadContinuation>>;

    virtual std::vector<folly::Future<VariantKey>> batch_read_compressed(
            std::vector<std::pair<entity::VariantKey, ReadContinuation>>&& ks, const BatchReadArgs& args
    ) = 0;

    [[nodiscard]] virtual std::vector<folly::Future<bool>> batch_key_exists(const std::vector<entity::VariantKey>& keys
    ) = 0;

    virtual std::vector<folly::Future<pipelines::SegmentAndSlice>> batch_read_uncompressed(
            std::vector<pipelines::RangesAndKey>&& ranges_and_keys,
            std::shared_ptr<std::unordered_set<std::string>> columns_to_decode
    ) = 0;

    virtual folly::Future<std::pair<std::optional<VariantKey>, std::optional<google::protobuf::Any>>> read_metadata(
            const entity::VariantKey& key, storage::ReadKeyOpts opts = storage::ReadKeyOpts{}
    ) = 0;

    virtual folly::Future<std::tuple<VariantKey, std::optional<google::protobuf::Any>, StreamDescriptor>>
    read_metadata_and_descriptor(const entity::VariantKey& key, storage::ReadKeyOpts opts = storage::ReadKeyOpts{}) = 0;

    virtual folly::Future<std::pair<VariantKey, TimeseriesDescriptor>> read_timeseries_descriptor(
            const entity::VariantKey& key, storage::ReadKeyOpts opts = storage::ReadKeyOpts{}
    ) = 0;

    virtual void read_ignoring_key_not_found(KeySizeCalculators&& calculators) {
        if (calculators.empty()) {
            return;
        }

        std::vector<folly::Future<folly::Unit>> res;
        for (auto&& fut : batch_read_compressed(std::move(calculators), BatchReadArgs{})) {
            // Ignore some exceptions, someone might be deleting while we scan
            res.push_back(std::move(fut).thenValue([](auto&&) { return folly::Unit{}; }
            ).thenError(folly::tag_t<storage::KeyNotFoundException>{}, [](auto&&) { return folly::Unit{}; }));
        }

        folly::collect(res).get();
    }
};

} // namespace arcticdb::stream