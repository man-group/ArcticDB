/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/storage/lmdb/lmdb_storage.hpp>
#include <arcticdb/storage/lmdb/lmdb_real_client.hpp>
#include <arcticdb/storage/lmdb/lmdb_mock_client.hpp>

#include <filesystem>

#include <arcticdb/log/log.hpp>
#include <arcticdb/entity/atom_key.hpp>
#include <arcticdb/storage/constants.hpp>
#include <arcticdb/storage/library_path.hpp>
#include <arcticdb/storage/open_mode.hpp>
#include <arcticdb/util/format_bytes.hpp>
#include <arcticdb/util/preconditions.hpp>
#include <arcticdb/util/pb_util.hpp>
#include <arcticdb/entity/serialized_key.hpp>
#include <arcticdb/storage/storage.hpp>
#include <arcticdb/storage/storage_options.hpp>
#include <arcticdb/storage/storage_utils.hpp>

#include <google/protobuf/io/zero_copy_stream_impl_lite.h>
#include <folly/gen/Base.h>

namespace arcticdb::storage::lmdb {

namespace fg = folly::gen;

void raise_lmdb_exception(const ::lmdb::error& e) {
    auto error_code = e.code();

    if (error_code == MDB_NOTFOUND) {
        throw KeyNotFoundException(fmt::format("Key Not Found Error: LMDBError#{}: {}", error_code, e.what()));
    }

    if (error_code == MDB_KEYEXIST) {
        throw DuplicateKeyException(fmt::format("Duplicate Key Error: LMDBError#{}: {}", error_code, e.what()));
    }

    if (error_code == MDB_MAP_FULL) {
        throw LMDBMapFullException(fmt::format("Map Full Error: LMDBError#{}: {}", error_code, e.what()));
    }

    raise<ErrorCode::E_UNEXPECTED_LMDB_ERROR>(fmt::format("Unexpected LMDB Error: LMDBError#{}: {}", error_code, e.what()));
}

void LmdbStorage::do_write_internal(Composite<KeySegmentPair>&& kvs, ::lmdb::txn& txn) {
    auto fmt_db = [](auto &&kv) { return kv.key_type(); };

    (fg::from(kvs.as_range()) | fg::move | fg::groupBy(fmt_db)).foreach([&](auto &&group) {
        auto db_name = fmt::format("{}", group.key());

        ARCTICDB_SUBSAMPLE(LmdbStorageOpenDb, 0)
        ::lmdb::dbi& dbi = dbi_by_key_type_.at(db_name);

        ARCTICDB_SUBSAMPLE(LmdbStorageWriteValues, 0)
        for (auto &kv : group.values()) {
            ARCTICDB_DEBUG(log::storage(), "Lmdb storage writing segment with key {}", kv.key_view());
            auto k = to_serialized_key(kv.variant_key());
            auto &seg = kv.segment();
            int64_t overwrite_flag = std::holds_alternative<RefKey>(kv.variant_key()) ? 0 : MDB_NOOVERWRITE;
            try {
                lmdb_client_->write(db_name, k, std::move(seg), txn, dbi, overwrite_flag);
            } catch (const ::lmdb::key_exist_error&) {
                throw DuplicateKeyException(kv.variant_key());
            } catch (const ::lmdb::error& ex) {
                raise_lmdb_exception(ex);
            }
        }
    });
}

void LmdbStorage::do_write(Composite<KeySegmentPair>&& kvs) {
    ARCTICDB_SAMPLE(LmdbStorageWrite, 0)
    std::lock_guard<std::mutex> lock{*write_mutex_};
    auto txn = ::lmdb::txn::begin(env()); // scoped abort on exception, so no partial writes
    ARCTICDB_SUBSAMPLE(LmdbStorageInTransaction, 0)
    do_write_internal(std::move(kvs), txn);
    ARCTICDB_SUBSAMPLE(LmdbStorageCommit, 0)
    txn.commit();
}

void LmdbStorage::do_update(Composite<KeySegmentPair>&& kvs, UpdateOpts opts) {
    ARCTICDB_SAMPLE(LmdbStorageUpdate, 0)
    std::lock_guard<std::mutex> lock{*write_mutex_};
    auto txn = ::lmdb::txn::begin(env());
    ARCTICDB_SUBSAMPLE(LmdbStorageInTransaction, 0)
    auto keys = kvs.transform([](const auto& kv){return kv.variant_key();});
    // Deleting keys (no error is thrown if the keys already exist)
    RemoveOpts remove_opts;
    remove_opts.ignores_missing_key_ = opts.upsert_;
    auto failed_deletes = do_remove_internal(std::move(keys), txn, remove_opts);
    if(!failed_deletes.empty()) {
        ARCTICDB_SUBSAMPLE(LmdbStorageCommit, 0)
        txn.commit();
        std::string err_message = fmt::format("do_update called with upsert=false on non-existent key(s): {}", failed_deletes);
        throw KeyNotFoundException(Composite<VariantKey>(std::move(failed_deletes)), err_message);
    }
    do_write_internal(std::move(kvs), txn);
    ARCTICDB_SUBSAMPLE(LmdbStorageCommit, 0)
    txn.commit();
}

void LmdbStorage::do_read(Composite<VariantKey>&& ks, const ReadVisitor& visitor, storage::ReadKeyOpts) {
    ARCTICDB_SAMPLE(LmdbStorageRead, 0)
    auto txn = std::make_shared<::lmdb::txn>(::lmdb::txn::begin(env(), nullptr, MDB_RDONLY));

    auto fmt_db = [](auto &&k) { return variant_key_type(k); };
    ARCTICDB_SUBSAMPLE(LmdbStorageInTransaction, 0)
    std::vector<VariantKey> failed_reads;

    (fg::from(ks.as_range()) | fg::move | fg::groupBy(fmt_db)).foreach([&](auto &&group) {
        auto db_name = fmt::format("{}", group.key());
        ARCTICDB_SUBSAMPLE(LmdbStorageOpenDb, 0)
        ::lmdb::dbi& dbi = dbi_by_key_type_.at(db_name);
        for (auto &k : group.values()) {
            auto stored_key = to_serialized_key(k);
            try {
                auto segment = lmdb_client_->read(db_name, stored_key, *txn, dbi);

                if (segment.has_value()) {
                    ARCTICDB_SUBSAMPLE(LmdbStorageVisitSegment, 0)
                    std::any keepalive;
                    segment.value().set_keepalive(std::any(std::move(txn)));
                    visitor(k, std::move(segment.value()));
                    ARCTICDB_DEBUG(log::storage(), "Read key {}: {}, with {} bytes of data", variant_key_type(k),
                                   variant_key_view(k), segment.value().total_segment_size());
                } else {
                    ARCTICDB_DEBUG(log::storage(), "Failed to find segment for key {}", variant_key_view(k));
                    failed_reads.push_back(k);
                }
            } catch (const ::lmdb::not_found_error&) {
                ARCTICDB_DEBUG(log::storage(), "Failed to find segment for key {}", variant_key_view(k));
                failed_reads.push_back(k);
            } catch (const ::lmdb::error& ex) {
                raise_lmdb_exception(ex);
            }
        }
    });
    if(!failed_reads.empty())
        throw KeyNotFoundException(Composite<VariantKey>(std::move(failed_reads)));
}

bool LmdbStorage::do_key_exists(const VariantKey&key) {
    ARCTICDB_SAMPLE(LmdbStorageKeyExists, 0)
    auto txn = ::lmdb::txn::begin(env(), nullptr, MDB_RDONLY);
    ARCTICDB_SUBSAMPLE(LmdbStorageInTransaction, 0)

    auto db_name = fmt::format("{}", variant_key_type(key));
    ARCTICDB_SUBSAMPLE(LmdbStorageOpenDb, 0)

    try {
        ::lmdb::dbi& dbi = dbi_by_key_type_.at(db_name);
        auto stored_key = to_serialized_key(key);
        return lmdb_client_->exists(db_name, stored_key, txn, dbi);
    } catch ([[maybe_unused]] const ::lmdb::not_found_error &ex) {
        ARCTICDB_DEBUG(log::storage(), "Caught lmdb not found error: {}", ex.what());
    } catch (const ::lmdb::error& ex) {
        raise_lmdb_exception(ex);
    }
    return false;
}

std::vector<VariantKey> LmdbStorage::do_remove_internal(Composite<VariantKey>&& ks, ::lmdb::txn& txn, RemoveOpts opts)
{
    auto fmt_db = [](auto &&k) { return variant_key_type(k); };
    std::vector<VariantKey> failed_deletes;

    (fg::from(ks.as_range()) | fg::move | fg::groupBy(fmt_db)).foreach([&](auto &&group) {
        auto db_name = fmt::format("{}", group.key());
        ARCTICDB_SUBSAMPLE(LmdbStorageOpenDb, 0)
        try {
            // If no key of this type has been written before, this can fail
            ::lmdb::dbi& dbi = dbi_by_key_type_.at(db_name);
            for (auto &k : group.values()) {
                auto stored_key = to_serialized_key(k);

                if (lmdb_client_->remove(db_name, stored_key, txn, dbi)) {
                    ARCTICDB_DEBUG(log::storage(), "Deleted segment for key {}", variant_key_view(k));
                } else {
                    if (!opts.ignores_missing_key_) {
                        log::storage().warn("Failed to delete segment for key {}", variant_key_view(k));
                        failed_deletes.push_back(k);
                    }
                }
            }
        } catch (const ::lmdb::not_found_error&) {
            if (!opts.ignores_missing_key_) {
                for (auto &k : group.values()) {
                    log::storage().warn("Failed to delete segment for key {}", variant_key_view(k) );
                                        failed_deletes.push_back(k);
                }
            }
        } catch (const ::lmdb::error& ex) {
            raise_lmdb_exception(ex);
        }
    });
    return failed_deletes;
}

void LmdbStorage::do_remove(Composite<VariantKey>&& ks, RemoveOpts opts)
{
    ARCTICDB_SAMPLE(LmdbStorageRemove, 0)
    std::lock_guard<std::mutex> lock{*write_mutex_};
    auto txn = ::lmdb::txn::begin(env());
    ARCTICDB_SUBSAMPLE(LmdbStorageInTransaction, 0)
    auto failed_deletes = do_remove_internal(std::move(ks), txn, opts);
    ARCTICDB_SUBSAMPLE(LmdbStorageCommit, 0)
    txn.commit();

    if(!failed_deletes.empty())
        throw KeyNotFoundException(Composite<VariantKey>(std::move(failed_deletes)));
}

bool LmdbStorage::do_fast_delete() {
    std::lock_guard<std::mutex> lock{*write_mutex_};
    // bool is probably not the best return type here but it does help prevent the insane boilerplate for
    // an additional function that checks whether this is supported (like the prefix matching)
    auto dtxn = ::lmdb::txn::begin(env());
    foreach_key_type([&] (KeyType key_type) {
        if (key_type == KeyType::TOMBSTONE) {
            // TOMBSTONE and LOCK both format to code 'x' - do not try to drop both
            return;
        }
        auto db_name = fmt::format("{}", key_type);
        ARCTICDB_SUBSAMPLE(LmdbStorageOpenDb, 0)
        ARCTICDB_DEBUG(log::storage(), "dropping {}", db_name);
        ::lmdb::dbi& dbi = dbi_by_key_type_.at(db_name);
        try {
            ::lmdb::dbi_drop(dtxn, dbi);
        } catch (const ::lmdb::error& ex) {
            raise_lmdb_exception(ex);
        }
    });

    dtxn.commit();
    return true;
}

void LmdbStorage::do_iterate_type(KeyType key_type, const IterateTypeVisitor& visitor, const std::string &prefix) {
    ARCTICDB_SAMPLE(LmdbStorageItType, 0);
    auto txn = ::lmdb::txn::begin(env(), nullptr, MDB_RDONLY); // scoped abort on
    std::string type_db = fmt::format("{}", key_type);
    ::lmdb::dbi& dbi = dbi_by_key_type_.at(type_db);

    try {
        auto keys = lmdb_client_->list(type_db, prefix, txn, dbi, key_type);
        for (auto &k: keys) {
            ARCTICDB_SUBSAMPLE(LmdbStorageVisitKey, 0)
            visitor(std::move(k));
        }
    } catch (const ::lmdb::error& ex) {
        raise_lmdb_exception(ex);
    }
}

void remove_db_files(const fs::path& lib_path) {
    std::vector<std::string> files = {"lock.mdb", "data.mdb"};

    for (const auto& file : files) {
        fs::path file_path = lib_path / file;
        try {
            if (fs::exists(file_path)) {
                fs::remove(file_path);
            }
        } catch (const std::filesystem::filesystem_error& e) {
            raise<ErrorCode::E_UNEXPECTED_LMDB_ERROR>(
                    fmt::format("Unexpected LMDB Error: Failed to remove LMDB file at path: {} error: {}",
                                file_path.string(), e.what()));
        }
    }

    if (fs::exists(lib_path)) {
        if (!fs::is_empty(lib_path)) {
            log::storage().warn(fmt::format("Skipping deletion of directory holding LMDB library during "
                                            "library deletion as it contains files unrelated to LMDB"));
        } else {
            try {
                fs::remove_all(lib_path);
            } catch (const fs::filesystem_error& e) {
                raise<ErrorCode::E_UNEXPECTED_LMDB_ERROR>(
                        fmt::format("Unexpected LMDB Error: Failed to remove directory: {} error: {}",
                                    lib_path.string(), e.what()));
            }
        }
    }
}

void LmdbStorage::cleanup() {
    env_.reset();
    remove_db_files(lib_dir_);
}


namespace {
template<class T>
T or_else(T val, T or_else_val, T def = T()) {
    return val == def ? or_else_val : val;
}
} // anonymous

LmdbStorage::LmdbStorage(const LibraryPath &library_path, OpenMode mode, const Config &conf) :
        Storage(library_path, mode) {
    if (conf.use_mock_storage_for_testing()) {
        lmdb_client_ = std::make_unique<MockLmdbClient>();
    }
    else {
        lmdb_client_ = std::make_unique<RealLmdbClient>();
    }
    write_mutex_ = std::make_unique<std::mutex>();
    env_ = std::make_unique<::lmdb::env>(::lmdb::env::create(conf.flags()));
    dbi_by_key_type_ = std::unordered_map<std::string, ::lmdb::dbi>{};

    fs::path root_path = conf.path().c_str();
    auto lib_path_str = library_path.to_delim_path(fs::path::preferred_separator);

    lib_dir_ = root_path / lib_path_str;

    warn_if_lmdb_already_open();

    if (!fs::exists(lib_dir_)) {
        util::check_arg(mode > OpenMode::READ, "Missing dir {} for lib={}. mode={}",
                        lib_dir_.generic_string(), lib_path_str, mode);

        fs::create_directories(lib_dir_);
    }

    if (fs::exists(lib_dir_ / "data.mdb")) {
        if (conf.recreate_if_exists() && mode >= OpenMode::WRITE) {
            fs::remove(lib_dir_ / "data.mdb");
            fs::remove(lib_dir_ / "lock.mdb");
        }
    }

    bool is_read_only = ((conf.flags() & MDB_RDONLY) != 0);
    util::check_arg(is_read_only || mode != OpenMode::READ,
                    "Flags {} and operating mode {} are conflicting",
                    conf.flags(), mode
    );
    // Windows needs a sensible size as it allocates disk for the whole file even before any writes. Linux just gets an arbitrarily large size
    // that it probably won't ever reach.
#ifdef _WIN32
    // At least enough for 300 cols and 1M rows
    constexpr uint64_t default_map_size = 2ULL * (1ULL << 30); /* 2 GiB */
#else
    constexpr uint64_t default_map_size = 400ULL * (1ULL << 30); /* 400 GiB */
#endif
    auto mapsize = or_else(static_cast<uint64_t>(conf.map_size()), default_map_size);
    env().set_mapsize(mapsize);
    env().set_max_dbs(or_else(static_cast<unsigned int>(conf.max_dbs()), 1024U));
    env().set_max_readers(or_else(conf.max_readers(), 1024U));
    env().open(lib_dir_.generic_string().c_str(), MDB_NOTLS);

    auto txn = ::lmdb::txn::begin(env());

    try {

        arcticdb::entity::foreach_key_type([&txn, this](KeyType &&key_type) {
            std::string db_name = fmt::format("{}", key_type);
            ::lmdb::dbi dbi = ::lmdb::dbi::open(txn, db_name.data(), MDB_CREATE);
            dbi_by_key_type_.insert(std::make_pair(std::move(db_name), std::move(dbi)));
        });
    } catch (const ::lmdb::error& ex) {
        raise_lmdb_exception(ex);
    }

    txn.commit();

    ARCTICDB_DEBUG(log::storage(), "Opened lmdb storage at {} with map size {}", lib_dir_.string(), format_bytes(mapsize));
}

void LmdbStorage::warn_if_lmdb_already_open() {
    uint64_t& count_for_pid = ++times_path_opened[lib_dir_.string()];
    // Only warn for the "base" config library to avoid spamming users with more warnings if they decide to ignore it and continue
    if (count_for_pid != 1 && lib_dir_.string().find(CONFIG_LIBRARY_NAME) != std::string::npos) {
        std::filesystem::path user_facing_path = lib_dir_;
        // Strip magic name from warning as it will confuse users
        user_facing_path.remove_filename();
        log::storage().warn(fmt::format(
                "LMDB path at {} has already been opened in this process which is not supported by LMDB. "
                "You should only open a single Arctic instance over a given LMDB path. "
                "To continue safely, you should delete this Arctic instance and any others over the LMDB path in this "
                "process and then try again. Current process ID=[{}]",
                user_facing_path.string(), getpid()));
    }
}

LmdbStorage::LmdbStorage(LmdbStorage&& other)  noexcept
    : Storage(std::move(static_cast<Storage&>(other))),
    write_mutex_(std::move(other.write_mutex_)),
    env_(std::move(other.env_)),
    dbi_by_key_type_(std::move(other.dbi_by_key_type_)),
    lib_dir_(std::move(other.lib_dir_)) {
    other.lib_dir_ = "";
    lmdb_client_ = std::move(other.lmdb_client_);
}

LmdbStorage::~LmdbStorage() {
    if (!lib_dir_.empty()) {
        --times_path_opened[lib_dir_.string()];
    }
}

void LmdbStorage::reset_warning_counter() {
    times_path_opened = std::unordered_map<std::string, uint64_t>{};
}

} // namespace arcticdb::storage::lmdb
