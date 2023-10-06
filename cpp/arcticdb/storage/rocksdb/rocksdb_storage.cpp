/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */
#define ARCTICDB_ROCKSDB_STORAGE_H_
#include <arcticdb/storage/rocksdb/rocksdb_storage-inl.hpp>
#include <arcticdb/storage/rocksdb/rocksdb_storage.hpp>
#include <rocksdb/db.h>
#include <rocksdb/options.h>
#include <rocksdb/utilities/options_util.h>
#include <filesystem>

namespace fs = std::filesystem;

namespace arcticdb::storage::rocksdb {

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

        ::rocksdb::ConfigOptions cfg_opts; // not used
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
                            "Key name clash with mandatory default column name");
                column_families.emplace_back(key_name, ::rocksdb::ColumnFamilyOptions());
            }
            fs::create_directories(lib_dir);
            db_options.create_if_missing = true;
            db_options.create_missing_column_families = true;
        } else {
            util::raise_rte(DEFAULT_ROCKSDB_NOT_OK_ERROR + s.ToString());
        }

        std::vector<::rocksdb::ColumnFamilyHandle*> handles;
        s = ::rocksdb::DB::Open(db_options, db_name, column_families, &handles, &db_);
        util::check(s.ok(), DEFAULT_ROCKSDB_NOT_OK_ERROR + s.ToString());
        util::check(handles.size() == column_families.size(), "Open returned wrong number of handles.");
        for (std::size_t i = 0; i < handles.size(); i++) {
            // There is an extra handle which we save here under "default". Confusingly, we must also hang on to this,
            // and then delete it with DestroyColumnFamilyHandle(...) in the destructor otherwise the references to the
            // column family descriptor for the default column will be one-too-many when we delete db_. There is a separate
            // handle to the default column family which we can access with DefaultColumnFamily(). We do not need
            // to delete this, despite the fact that it is another handle to the "default" column family.
            handles_by_key_type_.emplace(column_families[i].name, handles[i]);
        }
        handles.clear();
    }

    RocksDBStorage::~RocksDBStorage() {
        for (const auto& [key_type_name, handle]: handles_by_key_type_) {
            auto s = db_->DestroyColumnFamilyHandle(handle);
            util::check(s.ok(), DEFAULT_ROCKSDB_NOT_OK_ERROR + s.ToString());
        }
        handles_by_key_type_.clear();
        auto s = db_->Close();
        util::check(s.ok(), DEFAULT_ROCKSDB_NOT_OK_ERROR + s.ToString());
        delete db_;
    }
} // arcticdb::storage::memory
