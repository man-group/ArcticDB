/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the
 * file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source
 * License, use of this software will be governed by the Apache License, version 2.0.
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

struct StreamSource {

  virtual ~StreamSource() = default;

  virtual folly::Future<std::pair<entity::VariantKey, SegmentInMemory>>
  read(const entity::VariantKey& key,
       storage::ReadKeyOpts opts = storage::ReadKeyOpts{}) = 0;

  virtual std::pair<entity::VariantKey, SegmentInMemory>
  read_sync(const entity::VariantKey& key,
            storage::ReadKeyOpts opts = storage::ReadKeyOpts{}) = 0;

  virtual folly::Future<storage::KeySegmentPair>
  read_compressed(const entity::VariantKey& key,
                  storage::ReadKeyOpts opts = storage::ReadKeyOpts{}) = 0;

  virtual storage::KeySegmentPair read_compressed_sync(const entity::VariantKey& key,
                                                       storage::ReadKeyOpts opts) = 0;

  virtual void iterate_type(KeyType type, const entity::IterateTypeVisitor& func,
                            const std::string& prefix = std::string{}) = 0;

  [[nodiscard]] virtual folly::Future<bool>
  key_exists(const entity::VariantKey& key) = 0;
  virtual bool key_exists_sync(const entity::VariantKey& key) = 0;
  virtual bool supports_prefix_matching() const = 0;
  virtual bool fast_delete() = 0;

  using ReadContinuation =
      folly::Function<entity::VariantKey(storage::KeySegmentPair&&)>;

  virtual folly::Future<std::vector<VariantKey>> batch_read_compressed(
      std::vector<std::pair<entity::VariantKey, ReadContinuation>>&& ks,
      const BatchReadArgs& args) = 0;

  [[nodiscard]] virtual std::vector<folly::Future<bool>>
  batch_key_exists(const std::vector<entity::VariantKey>& keys) = 0;

  using DecodeContinuation = folly::Function<folly::Unit(SegmentInMemory&&)>;

  virtual std::vector<folly::Future<pipelines::SegmentAndSlice>>
  batch_read_uncompressed(
      std::vector<pipelines::RangesAndKey>&& ranges_and_keys,
      std::shared_ptr<std::unordered_set<std::string>> columns_to_decode) = 0;

  virtual folly::Future<
      std::pair<std::optional<VariantKey>, std::optional<google::protobuf::Any>>>
  read_metadata(const entity::VariantKey& key,
                storage::ReadKeyOpts opts = storage::ReadKeyOpts{}) = 0;

  virtual folly::Future<
      std::tuple<VariantKey, std::optional<google::protobuf::Any>, StreamDescriptor>>
  read_metadata_and_descriptor(const entity::VariantKey& key,
                               storage::ReadKeyOpts opts = storage::ReadKeyOpts{}) = 0;

  virtual folly::Future<std::pair<VariantKey, TimeseriesDescriptor>>
  read_timeseries_descriptor(const entity::VariantKey& key,
                             storage::ReadKeyOpts opts = storage::ReadKeyOpts{}) = 0;

  virtual folly::Future<std::pair<VariantKey, TimeseriesDescriptor>>
  read_timeseries_descriptor_for_incompletes(
      const entity::VariantKey& key,
      storage::ReadKeyOpts opts = storage::ReadKeyOpts{}) = 0;
};

} // namespace arcticdb::stream