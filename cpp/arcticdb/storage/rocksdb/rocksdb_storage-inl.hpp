/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#include <folly/gen/Base.h>
#ifndef ARCTICDB_ROCKSDB_STORAGE_H_
#error "This should only be included by rocksdb_storage.hpp"
#endif

#include <arcticdb/util/preconditions.hpp>
#include <arcticdb/entity/performance_tracing.hpp>
#include <arcticdb/storage/rocksdb/rocksdb_storage.hpp>
#include <arcticdb/storage/storage_options.hpp>

namespace arcticdb::storage::rocksdb {

    namespace fg = folly::gen;

    inline void RocksDBStorage::do_write(Composite<KeySegmentPair>&& kvs) {
        ARCTICDB_SAMPLE(RocksDBStorageWrite, 0)

        auto fmt_db = [](auto &&k) { return variant_key_type(k.variant_key()); };

        (fg::from(kvs.as_range()) | fg::move | fg::groupBy(fmt_db)).foreach([&](auto &&group) {

            for (auto &kv : group.values()) {
                util::variant_match(kv.variant_key(),
                                    [&](const RefKey &key) {
                                        (void)key;
                                    },
                                    [&](const AtomKey &key) {
                                        (void)key;
                                    }
                );
            }
        });
    }

    inline void RocksDBStorage::do_update(Composite<KeySegmentPair>&& kvs, UpdateOpts /*opts*/) {
        ARCTICDB_SAMPLE(RocksDBStorageUpdate, 0)

        ARCTICDB_RUNTIME_DEBUG(log::storage(), "RocksDB asked to update keys...");

        auto fmt_db = [](auto &&k) { return variant_key_type(k.variant_key()); };

        (fg::from(kvs.as_range()) | fg::move | fg::groupBy(fmt_db)).foreach([&](auto &&group) {
            for (auto &kv : group.values()) {
                (void)kv;
                // If opts.upsert_ is True, then will write the data even if the data was not already there
                // Essentially erase, and insert new key/value
            }
        });
    }

    inline void RocksDBStorage::do_read(Composite<VariantKey>&& ks, const ReadVisitor& visitor, ReadKeyOpts) {
        (void) visitor;
        ARCTICDB_SAMPLE(RocksDBStorageRead, 0)
        auto fmt_db = [](auto &&k) { return variant_key_type(k); };

        (fg::from(ks.as_range()) | fg::move | fg::groupBy(fmt_db)).foreach([&](auto &&group) {
            for (auto &k : group.values()) {
                (void)k;
                // Go find key and create a segment with Segment::from_bytes or something, then forward to visitor
                // --> Do we want to do this on the fly? Or should we cache the segments in some way...
                //if(it != key_vec.end()) {
                //    ARCTICDB_DEBUG(log::storage(), "Read key {}: {}", variant_key_type(k), variant_key_view(k));
                //    auto seg = it->second;
                //    visitor(k, std::move(seg));
                //} else {
                //    throw KeyNotFoundException(std::move(ks));
                //}
            }
        });
    }

    inline bool RocksDBStorage::do_key_exists(const VariantKey& /*key*/) {
        ARCTICDB_SAMPLE(RocksDBStorageKeyExists, 0)
        // return true/false
        return true;
    }

    inline void RocksDBStorage::do_remove(Composite<VariantKey>&& ks, RemoveOpts /*opts*/)
    {
        ARCTICDB_SAMPLE(RocksDBStorageRemove, 0)
        auto fmt_db = [](auto &&k) { return variant_key_type(k); };

        (fg::from(ks.as_range()) | fg::move | fg::groupBy(fmt_db)).foreach([&](auto &&group) {
            for (auto &k: group.values()) {
                (void)k;
                //if(it != key_vec.end()) {
                //    ARCTICDB_DEBUG(log::storage(), "Read key {}: {}, with {} bytes of data", variant_key_type(k), variant_key_view(k));
                //    key_vec.erase(it);
                //} else if (!opts.ignores_missing_key_) {
                //    util::raise_rte("Failed to find segment for key {}",variant_key_view(k));
                //}
            }
        });
    }

    bool RocksDBStorage::do_fast_delete() {
        return false;
    }

    inline void RocksDBStorage::do_iterate_type(KeyType /*key_type*/, const IterateTypeVisitor& /*visitor*/, const std::string& /*prefix*/) {
        ARCTICDB_SAMPLE(RocksDBStorageItType, 0)
        // Need to send them the key. Note that we must implement the prefix check for the test_symbol_list.py to work
        //visitor(std::move(key));
    }
}
