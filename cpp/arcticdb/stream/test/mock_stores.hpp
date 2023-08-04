/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/stream/aggregator.hpp>
#include <arcticdb/stream/stream_utils.hpp>
#include <arcticdb/stream/stream_reader.hpp>
#include <arcticdb/stream/stream_writer.hpp>
#include <arcticdb/storage/store.hpp>

#include <arcticdb/entity/atom_key.hpp>
#include <folly/gen/Base.h>
#include <folly/futures/Future.h>
#include <arcticdb/util/variant.hpp>
#include <arcticdb/pipeline/slicing.hpp>
#include <arcticdb/pipeline/input_tensor_frame.hpp>
#include <arcticdb/storage/test/in_memory_store.hpp>

#include <mutex>

namespace arcticdb {

template<KeyType key_type>
class TypeFilteredStore :
    public InMemoryStore {
    folly::Future<folly::Unit> iterate_type(
        KeyType kt, folly::Function<void(entity::VariantKey &&)> func,
        const std::string &/*prefix*/) override {
        if (kt == key_type)
            for (auto &item : seg_by_atom_key_) {
                fmt::print("{}", item.first);
                entity::AtomKey key(item.first);
                func(std::move(key));
            }
    }

    folly::Future<VariantKey> write(
        KeyType kt, VersionId version_id,
        StreamId stream_id, IndexValue start_index, IndexValue end_index,
        SegmentInMemory &&segment
    ) override {
        if (key_type == kt)
            InMemoryStore::write(kt, version_id, stream_id, start_index, end_index, std::move(segment));

        return atom_key_builder().gen_id(version_id).content_hash(content_hash_).creation_ts(creation_ts_)
                                 .start_index(start_index).end_index(end_index).build(stream_id, key_type);
    }

};

class NullStore :
    public Store {

  public:
    NullStore() = default;

    folly::Future<std::pair<AtomKey, arcticdb::SegmentInMemory>> read(const AtomKey &key) override {
        return std::make_pair(key, arcticdb::SegmentInMemory{});
    }

    std::atomic<timestamp> creation_ts{0};
    HashedValue content_hash = 0x42;

    folly::Future<VariantKey>
    write(
        KeyType key_type, VersionId gen_id,
        StreamId tsid, IndexValue start_index, IndexValue end_index,
        SegmentInMemory &&
    ) override {
        auto key = atom_key_builder().gen_id(gen_id).content_hash(content_hash).creation_ts(creation_ts)
                                     .start_index(start_index).end_index(end_index).build(tsid, key_type);
        return folly::makeFuture(key);
    }

    folly::Future<RemoveKeyResultType> remove_key(const VariantKey &key) override {
        return folly::makeFuture({});
    }

    void read_atoms(
        const std::vector<entity::AtomKey> &,
        folly::Function<void(std::vector<std::pair<entity::AtomKey, SegmentInMemory >> &&)> &&
    ) override {
        throw std::runtime_error("Not implemented");
    }

    void read_refs(
            const std::vector<entity::RefKey> &,
            folly::Function<void(std::vector<std::pair<entity::RefKey, SegmentInMemory >> &&)> &&
    ) override {
        throw std::runtime_error("Not implemented");
    }

    folly::Future<folly::Unit> iterate_type(KeyType, folly::Function<void(entity::VariantKey &&)>, const std::string &) override {
        throw std::runtime_error("Not implemented");
    }

    folly::Future<entity::RefKey>
    write(stream::KeyType /*key_type*/, StreamId /*stream_id*/, SegmentInMemory &&/*segment*/) override {
        util::raise_rte("Not implemented");
    }

    folly::Future<std::pair<entity::RefKey, SegmentInMemory>> read(const entity::RefKey &/*key*/) override {
        util::raise_rte("Not implemented");
    }

    folly::Future<std::vector<RemoveKeyResultType>> remove_keys(const std::vector<entity::VariantKey>&) override {
        util::raise_rte("Not implemented");
    }

    folly::Future<std::vector<RemoveKeyResultType>> remove_keys(std::vector<entity::VariantKey>&&) override {
        util::raise_rte("Not implemented");
    }

    folly::Future<std::vector<AtomKey>> batch_write(
        std::vector<std::pair<PartialKey, SegmentInMemory>> &&,
        const std::unordered_map<ContentHash, AtomKey>&,
        const BatchWriteArgs &
    ) override {
        throw std::runtime_error("Not implemented for tests");
    }

    std::vector<folly::Future<storage::KeySegmentPair>>
    batch_read_compressed(std::vector<entity::VariantKey> &&, const BatchReadArgs &) override {
        throw std::runtime_error("Not implemented for tests");
    }

    virtual std::vector<folly::Future<bool>> batch_key_exists(std::vector<entity::VariantKey> &) {
        throw std::runtime_error("Not implemented for tests");
    }

    folly::Future<std::vector<VariantKey>>
    batch_read_compressed(
        std::vector<entity::VariantKey>&&,
        std::vector<ReadContinuation>&&,
        const BatchReadArgs &
    ) override {
        throw std::runtime_error("Not implemented for tests");
    }

    folly::Future<std::pair<VariantKey, std::optional<google::protobuf::Any>>>

    read_metadata(const entity::VariantKey &) override {
        util::raise_rte("Not implemented for tests");
    }

    void set_failure_sim(const arcticdb::proto::storage::VersionStoreConfig::StorageFailureSimulator &) override {}

};

} //namespace arcticdb