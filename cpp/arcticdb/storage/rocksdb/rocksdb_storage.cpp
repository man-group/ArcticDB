/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/storage/rocksdb/rocksdb_storage.hpp>

#include <filesystem>

#include <arcticdb/util/preconditions.hpp>
#include <arcticdb/entity/performance_tracing.hpp>
#include <arcticdb/storage/storage_options.hpp>
#include <arcticdb/storage/storage_utils.hpp>
#include <arcticdb/entity/serialized_key.hpp>
#include <arcticdb/codec/segment.hpp>

#include <rocksdb/db.h>
#include <rocksdb/options.h>
#include <rocksdb/utilities/options_util.h>
#include <rocksdb/slice.h>

namespace arcticdb::storage::rocksdb {

namespace fs = std::filesystem;
namespace fg = folly::gen;

RocksDBStorage::RocksDBStorage(const LibraryPath &library_path, OpenMode mode, const Config& conf) :
    Storage(library_path, mode) {

    fs::path root_path = conf.path().c_str();
    auto lib_path_str = library_path.to_delim_path(fs::path::preferred_separator);

    auto lib_dir = root_path / lib_path_str;
    auto db_name = lib_dir.generic_string();

    std::set<std::string> key_names;
    arcticdb::entity::foreach_key_type([&](KeyType&& key_type) {
        std::string handle_name = fmt::format("{}", key_type);
        key_names.insert(handle_name);
    });

    std::vector<::rocksdb::ColumnFamilyDescriptor> column_families;
    ::rocksdb::DBOptions db_options;

    ::rocksdb::ConfigOptions cfg_opts;
    auto s = ::rocksdb::LoadLatestOptions(cfg_opts, db_name, &db_options, &column_families);
    if (s.ok()) {
        std::set<std::string> existing_key_names{};
        for (const auto& desc : column_families) {
            if (desc.name != ::rocksdb::kDefaultColumnFamilyName) {
                existing_key_names.insert(desc.name);
            }
        }
        util::check(existing_key_names == key_names, "Existing database has incorrect key columns.");
    } else if (s.IsNotFound()) {
        util::check_arg(mode > OpenMode::READ, "Missing dir {} for lib={}. mode={}",
                            db_name, lib_path_str, mode);
        // Default column family required, error if not provided.
        column_families.emplace_back(::rocksdb::kDefaultColumnFamilyName, ::rocksdb::ColumnFamilyOptions());
        for (const auto& key_name: key_names) {
            util::check(key_name != ::rocksdb::kDefaultColumnFamilyName,
                "Key name clash with mandatory default column family name: \"" +
                ::rocksdb::kDefaultColumnFamilyName + "\"");
            column_families.emplace_back(key_name, ::rocksdb::ColumnFamilyOptions());
        }
        fs::create_directories(lib_dir);
        db_options.create_if_missing = true;
        db_options.create_missing_column_families = true;
    } else {
        raise<ErrorCode::E_UNEXPECTED_ROCKSDB_ERROR>(DEFAULT_ROCKSDB_NOT_OK_ERROR + s.ToString());
    }

    std::vector<::rocksdb::ColumnFamilyHandle*> handles;
    // Note: the "default" handle will be returned as well. It is necessary to delete this, but not
    // rocksdb's internal handle to the default column family as returned by DefaultColumnFamily().
    s = ::rocksdb::DB::Open(db_options, db_name, column_families, &handles, &db_);
    storage::check<ErrorCode::E_UNEXPECTED_ROCKSDB_ERROR>(s.ok(), DEFAULT_ROCKSDB_NOT_OK_ERROR + s.ToString());
    util::check(handles.size() == column_families.size(), "Open returned wrong number of handles.");
    for (std::size_t i = 0; i < handles.size(); i++) {
        handles_by_key_type_.emplace(column_families[i].name, handles[i]);
    }
    handles.clear();
}

RocksDBStorage::~RocksDBStorage() {
    for (const auto& [key_type_name, handle]: handles_by_key_type_) {
        auto s = db_->DestroyColumnFamilyHandle(handle);
        storage::check<ErrorCode::E_UNEXPECTED_ROCKSDB_ERROR>(s.ok(), DEFAULT_ROCKSDB_NOT_OK_ERROR + s.ToString());
    }
    handles_by_key_type_.clear();
    auto s = db_->Close();
    storage::check<ErrorCode::E_UNEXPECTED_ROCKSDB_ERROR>(s.ok(), DEFAULT_ROCKSDB_NOT_OK_ERROR + s.ToString());
    delete db_;
}

void RocksDBStorage::do_write(Composite<KeySegmentPair>&& kvs) {
    ARCTICDB_SAMPLE(RocksDBStorageWrite, 0)
    do_write_internal(std::move(kvs));
}

void RocksDBStorage::do_update(Composite<KeySegmentPair>&& kvs, UpdateOpts opts) {
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

void RocksDBStorage::do_read(Composite<VariantKey>&& ks, const ReadVisitor& visitor, ReadKeyOpts) {
    ARCTICDB_SAMPLE(RocksDBStorageRead, 0)
    auto grouper = [](auto &&k) { return variant_key_type(k); };
    std::vector<VariantKey> failed_reads;

    (fg::from(ks.as_range()) | fg::move | fg::groupBy(grouper)).foreach([&](auto &&group) {
        auto key_type_name = fmt::format("{}", group.key());
        auto handle = handles_by_key_type_.at(key_type_name);
        for (const auto &k : group.values()) {
            std::string k_str = to_serialized_key(k);
            std::string value;
            auto s = db_->Get(::rocksdb::ReadOptions(), handle, ::rocksdb::Slice(k_str), &value);
            storage::check<ErrorCode::E_UNEXPECTED_ROCKSDB_ERROR>(s.IsNotFound() || s.ok(), DEFAULT_ROCKSDB_NOT_OK_ERROR + s.ToString());
            if(s.IsNotFound()) {
                log::storage().debug("Failed to read segment for key {}", variant_key_view(k));
                failed_reads.push_back(k);
            }
            else {
                visitor(k, Segment::from_bytes(reinterpret_cast<uint8_t*>(value.data()), value.size()));
            }
        }
    });
    if(!failed_reads.empty())
        throw KeyNotFoundException(Composite<VariantKey>(std::move(failed_reads)));
}

bool RocksDBStorage::do_key_exists(const VariantKey& key) {
    ARCTICDB_SAMPLE(RocksDBStorageKeyExists, 0)
    std::string value; // unused
    auto key_type_name = fmt::format("{}", variant_key_type(key));
    auto k_str = to_serialized_key(key);
    auto handle = handles_by_key_type_.at(key_type_name);
    if (!db_->KeyMayExist(::rocksdb::ReadOptions(), handle, ::rocksdb::Slice(k_str), &value)) {
        return false;
    }
    auto s = db_->Get(::rocksdb::ReadOptions(), handle, ::rocksdb::Slice(k_str), &value);
    storage::check<ErrorCode::E_UNEXPECTED_ROCKSDB_ERROR>(s.IsNotFound() || s.ok(), DEFAULT_ROCKSDB_NOT_OK_ERROR + s.ToString());
    return !s.IsNotFound();
}

void RocksDBStorage::do_remove(Composite<VariantKey>&& ks, RemoveOpts opts) {
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

void RocksDBStorage::do_iterate_type(KeyType key_type, const IterateTypeVisitor& visitor, const std::string& prefix) {
    ARCTICDB_SAMPLE(RocksDBStorageItType, 0)
    auto prefix_matcher = stream_id_prefix_matcher(prefix);

    auto key_type_name = fmt::format("{}", key_type);
    auto handle = handles_by_key_type_.at(key_type_name);
    auto it = std::unique_ptr<::rocksdb::Iterator>(db_->NewIterator(::rocksdb::ReadOptions(), handle));
    for (it->SeekToFirst(); it->Valid(); it->Next()) {
        auto key_slice = it->key();
        auto k = variant_key_from_bytes(reinterpret_cast<const uint8_t *>(key_slice.data()), key_slice.size(), key_type);

        ARCTICDB_DEBUG(log::storage(), "Iterating key {}: {}", variant_key_type(k), variant_key_view(k));
        if (prefix_matcher(variant_key_id(k))) {
            visitor(std::move(k));
        }
    }
    auto s = it->status();
    storage::check<ErrorCode::E_UNEXPECTED_ROCKSDB_ERROR>(s.IsNotFound() || s.ok(), DEFAULT_ROCKSDB_NOT_OK_ERROR + s.ToString());
}

std::vector<VariantKey> RocksDBStorage::do_remove_internal(Composite<VariantKey>&& ks, RemoveOpts opts) {
    auto grouper = [](auto &&k) { return variant_key_type(k); };
    std::vector<VariantKey> failed_deletes;

    (fg::from(ks.as_range()) | fg::move | fg::groupBy(grouper)).foreach([&](auto &&group) {
        auto key_type_name = fmt::format("{}", group.key());
        // If no key of this type has been written before, this can fail
        auto handle = handles_by_key_type_.at(key_type_name);
        for (const auto &k : group.values()) {
            if (do_key_exists(k)) {
                auto k_str = to_serialized_key(k);
                auto s = db_->Delete(::rocksdb::WriteOptions(), handle, ::rocksdb::Slice(k_str));
                storage::check<ErrorCode::E_UNEXPECTED_ROCKSDB_ERROR>(s.ok(), DEFAULT_ROCKSDB_NOT_OK_ERROR + s.ToString());
                ARCTICDB_DEBUG(log::storage(), "Deleted segment for key {}", variant_key_view(k));
            } else if (!opts.ignores_missing_key_) {
                log::storage().warn("Failed to delete segment for key {}", variant_key_view(k));
                failed_deletes.push_back(k);
            }
        }
    });
    return failed_deletes;
}

void RocksDBStorage::do_write_internal(Composite<KeySegmentPair>&& kvs) {
    auto grouper = [](auto &&kv) { return kv.key_type(); };
    (fg::from(kvs.as_range()) | fg::move | fg::groupBy(grouper)).foreach([&](auto &&group) {
        auto key_type_name = fmt::format("{}", group.key());
        auto handle = handles_by_key_type_.at(key_type_name);
        for (auto &kv : group.values()) {
            auto k_str = to_serialized_key(kv.variant_key());

            auto& seg = kv.segment();
            auto hdr_sz = seg.segment_header_bytes_size();
            auto total_sz = seg.total_segment_size(hdr_sz);
            std::string seg_data;
            seg_data.resize(total_sz);
            seg.write_to(reinterpret_cast<std::uint8_t *>(seg_data.data()), hdr_sz);
            auto allow_override = std::holds_alternative<RefKey>(kv.variant_key());
            if (!allow_override && do_key_exists(kv.variant_key())) {
                throw DuplicateKeyException(kv.variant_key());
            }
            auto s = db_->Put(::rocksdb::WriteOptions(), handle, ::rocksdb::Slice(k_str), ::rocksdb::Slice(seg_data));
            storage::check<ErrorCode::E_UNEXPECTED_ROCKSDB_ERROR>(s.ok(), DEFAULT_ROCKSDB_NOT_OK_ERROR + s.ToString());
        }
    });
}
} //namespace arcticdb::storage::rocksdb
