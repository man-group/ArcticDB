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
#include <arcticdb/storage/storage_utils.hpp>
#include <arcticdb/entity/serialized_key.hpp>
#include <arcticdb/codec/segment.hpp>

#include <rocksdb/slice.h>
#include <rocksdb/options.h>

namespace arcticdb::storage::rocksdb {

    namespace fg = folly::gen;

    inline void RocksDBStorage::do_write(Composite<KeySegmentPair>&& kvs) {
        ARCTICDB_SAMPLE(RocksDBStorageWrite, 0)
        do_write_internal(std::move(kvs));
    }

    inline void RocksDBStorage::do_update(Composite<KeySegmentPair>&& kvs, UpdateOpts opts) {
        ARCTICDB_SAMPLE(RocksDBStorageUpdate, 0)

        auto keys = kvs.transform([](const auto& kv){return kv.variant_key();});
        // Deleting keys (no error is thrown if the keys already exist)
        RemoveOpts remove_opts;
        remove_opts.ignores_missing_key_ = opts.upsert_;
        auto failed_deletes = do_remove_internal(std::move(keys), remove_opts);
        if (!failed_deletes.empty()) {
            throw KeyNotFoundException(Composite<VariantKey>(std::move(failed_deletes)));
        }
        do_write_internal(std::move(kvs));
    }

    inline void RocksDBStorage::do_read(Composite<VariantKey>&& ks, const ReadVisitor& visitor, ReadKeyOpts) {
        ARCTICDB_SAMPLE(RocksDBStorageRead, 0)
        auto grouper = [](auto &&k) { return variant_key_type(k); };

        (fg::from(ks.as_range()) | fg::move | fg::groupBy(grouper)).foreach([&](auto &&group) {
            auto key_type_name = fmt::format("{}", group.key());
            auto handle = handles_by_key_type_.at(key_type_name);
            for (const auto &k : group.values()) {
                std::string k_str = to_serialized_key(k);
                std::string value;
                auto s = db_->Get(::rocksdb::ReadOptions(), handle, ::rocksdb::Slice(k_str), &value);
                util::check(s.ok(), DEFAULT_ROCKSDB_NOT_OK_ERROR + s.ToString());
                visitor(k, Segment::from_bytes(reinterpret_cast<uint8_t*>(value.data()), value.size()));
            }
        });
    }

    inline bool RocksDBStorage::do_key_exists(const VariantKey& key) {
        ARCTICDB_SAMPLE(RocksDBStorageKeyExists, 0)
        std::string value; // unused
        auto key_type_name = fmt::format("{}", variant_key_type(key));
        auto k_str = to_serialized_key(key);
        auto s = db_->Get(::rocksdb::ReadOptions(), handles_by_key_type_.at(key_type_name), ::rocksdb::Slice(k_str), &value);
        return !s.IsNotFound();
    }

    inline void RocksDBStorage::do_remove(Composite<VariantKey>&& ks, RemoveOpts opts)
    {
        ARCTICDB_SAMPLE(RocksDBStorageRemove, 0)

        auto failed_deletes = do_remove_internal(std::move(ks), opts);
        if (!failed_deletes.empty()) {
            throw KeyNotFoundException(Composite<VariantKey>(std::move(failed_deletes)));
        }
    }

    bool RocksDBStorage::do_fast_delete() {
        foreach_key_type([&](KeyType key_type) {
            if (key_type == KeyType::TOMBSTONE) {
                // TOMBSTONE and LOCK both format to code 'x' - do not try to drop both
                return;
            }
            auto key_type_name = fmt::format("{}", key_type);
            auto handle = handles_by_key_type_.at(key_type_name);
            ARCTICDB_DEBUG(log::storage(), "dropping {}", key_type_name);
            db_->DropColumnFamily(handle);
        });
        return true;
    }

    inline void RocksDBStorage::do_iterate_type(KeyType key_type, const IterateTypeVisitor& visitor, const std::string& prefix) {
        ARCTICDB_SAMPLE(RocksDBStorageItType, 0)
        auto prefix_matcher = stream_id_prefix_matcher(prefix);

        auto key_type_name = fmt::format("{}", key_type);
        auto handle = handles_by_key_type_.at(key_type_name);
        ::rocksdb::Iterator* it = db_->NewIterator(::rocksdb::ReadOptions(), handle);
        for (it->SeekToFirst(); it->Valid(); it->Next()) {
            auto key_slice = it->key();
            auto k = variant_key_from_bytes(reinterpret_cast<const uint8_t *>(key_slice.data()), key_slice.size(), key_type);

            ARCTICDB_DEBUG(log::storage(), "Iterating key {}: {}", variant_key_type(k), variant_key_view(k));
            if (prefix_matcher(variant_key_id(k))) {
                visitor(std::move(k));
            }
        }
        auto s = it->status();
        util::check(s.ok(), DEFAULT_ROCKSDB_NOT_OK_ERROR + s.ToString());
        delete it;
    }

    inline std::vector<VariantKey> RocksDBStorage::do_remove_internal(Composite<VariantKey>&& ks, RemoveOpts opts)
    {
        auto grouper = [](auto &&k) { return variant_key_type(k); };
        std::vector<VariantKey> failed_deletes;

        (fg::from(ks.as_range()) | fg::move | fg::groupBy(grouper)).foreach([&](auto &&group) {
            auto key_type_name = fmt::format("{}", group.key());
            // If no key of this type has been written before, this can fail
            auto handle = handles_by_key_type_.at(key_type_name);
            auto options = ::rocksdb::WriteOptions(); // Should this be const class attr? Used in write_internal too.
            options.sync = true;
            for (const auto &k : group.values()) {
                if (do_key_exists(k)) {
                    auto k_str = to_serialized_key(k);
                    auto s = db_->Delete(options, handle, ::rocksdb::Slice(k_str));
                    util::check(s.ok(), DEFAULT_ROCKSDB_NOT_OK_ERROR + s.ToString());
                    ARCTICDB_DEBUG(log::storage(), "Deleted segment for key {}", variant_key_view(k));
                } else if (!opts.ignores_missing_key_) {
                    log::storage().warn("Failed to delete segment for key {}", variant_key_view(k));
                    failed_deletes.push_back(k);
                }
            }
        });
        return failed_deletes;
    }

    inline void RocksDBStorage::do_write_internal(Composite<KeySegmentPair>&& kvs)
    {
        auto grouper = [](auto &&kv) { return kv.key_type(); };
        (fg::from(kvs.as_range()) | fg::move | fg::groupBy(grouper)).foreach([&](auto &&group) {
            auto key_type_name = fmt::format("{}", group.key());
            auto handle = handles_by_key_type_.at(key_type_name);
            for (auto &kv : group.values()) {
                auto options = ::rocksdb::WriteOptions();
                options.sync = true;
                auto k_str = to_serialized_key(kv.variant_key());

                auto& seg = kv.segment();
                auto hdr_sz = seg.segment_header_bytes_size();
                auto total_sz = seg.total_segment_size(hdr_sz);
                std::string seg_data;
                seg_data.resize(total_sz);
                seg.write_to(reinterpret_cast<std::uint8_t *>(seg_data.data()), hdr_sz);
                auto override = std::holds_alternative<RefKey>(kv.variant_key());
                if (!override && do_key_exists(kv.variant_key())) {
                    throw DuplicateKeyException(kv.variant_key());
                }
                auto s = db_->Put(options, handle, ::rocksdb::Slice(k_str), ::rocksdb::Slice(seg_data));
                util::check(s.ok(), DEFAULT_ROCKSDB_NOT_OK_ERROR + s.ToString());
            }
        });
    }
}
