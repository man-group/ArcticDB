/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/storage/lmdb/lmdb_storage.hpp>

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
            MDB_val mdb_key;
            mdb_key.mv_data = k.data();
            mdb_key.mv_size = k.size();

            std::size_t hdr_sz = seg.segment_header_bytes_size();

            MDB_val mdb_val;
            mdb_val.mv_size = seg.total_segment_size(hdr_sz);
            int64_t overwrite_flag = std::holds_alternative<RefKey>(kv.variant_key()) ? 0 : MDB_NOOVERWRITE;
            ARCTICDB_SUBSAMPLE(LmdbPut, 0)
            int res = ::mdb_put(txn.handle(), dbi.handle(), &mdb_key, &mdb_val, MDB_RESERVE | overwrite_flag);
            if (res == MDB_KEYEXIST) {
                throw DuplicateKeyException(kv.variant_key());
            } else if (res == MDB_MAP_FULL) {
                throw ::lmdb::map_full_error("mdb_put", res);
            } else if (res != 0) {
                throw std::runtime_error(fmt::format("Invalid lmdb error code {} while putting key {}",
                                                     res, kv.key_view()));
            }
            ARCTICDB_SUBSAMPLE(LmdbMemCpy, 0)
            // mdb_val now points to a reserved memory area we must write to
            seg.write_to(reinterpret_cast<std::uint8_t *>(mdb_val.mv_data), hdr_sz);
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

void LmdbStorage::do_update(Composite<KeySegmentPair>&& kvs, StorageUpdateOptions opts) {
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
            MDB_val mdb_key{stored_key.size(), stored_key.data()};
            MDB_val mdb_val;
            ARCTICDB_SUBSAMPLE(LmdbStorageGet, 0)

            if (::lmdb::dbi_get(*txn, dbi.handle(), &mdb_key, &mdb_val)) {
                ARCTICDB_SUBSAMPLE(LmdbStorageVisitSegment, 0)
                auto segment = Segment::from_bytes(reinterpret_cast<std::uint8_t *>(mdb_val.mv_data),
                                                   mdb_val.mv_size);
                std::any keepalive;
                segment.set_keepalive(std::any(std::move(txn)));
                visitor(k, std::move(segment));

                ARCTICDB_DEBUG(log::storage(), "Read key {}: {}, with {} bytes of data", variant_key_type(k), variant_key_view(k), mdb_val.mv_size);
            } else {
                ARCTICDB_DEBUG(log::storage(), "Failed to find segment for key {}",variant_key_view(k));
                failed_reads.push_back(k);
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
        if (unsigned int tmp; ::mdb_dbi_flags(txn, dbi, &tmp) == EINVAL) {
            return false;
        }
        auto stored_key = to_serialized_key(key);
        MDB_val mdb_key{stored_key.size(), stored_key.data()};
        MDB_val mdb_val;
        return ::lmdb::dbi_get(txn, dbi.handle(), &mdb_key, &mdb_val);
    } catch ([[maybe_unused]] const ::lmdb::not_found_error &ex) {
        ARCTICDB_DEBUG(log::storage(), "Caught lmdb not found error: {}", ex.what());
        return false;
    }
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
                MDB_val mdb_key{stored_key.size(), stored_key.data()};
                ARCTICDB_SUBSAMPLE(LmdbStorageDel, 0)

                if (::lmdb::dbi_del(txn, dbi.handle(), &mdb_key)) {
                    ARCTICDB_DEBUG(log::storage(), "Deleted segment for key {}", variant_key_view(k));
                } else {
                    if (!opts.ignores_missing_key_) {
                        log::storage().warn("Failed to delete segment for key {}", variant_key_view(k));
                        failed_deletes.push_back(k);
                    }
                }
            }
        } catch (const std::exception&) {
            if (!opts.ignores_missing_key_) {
                for (auto &k : group.values()) {
                    log::storage().warn("Failed to delete segment for key {}", variant_key_view(k) );
                                        failed_deletes.push_back(k);
                }
            }
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
        ::lmdb::dbi_drop(dtxn, dbi);
    });

    dtxn.commit();
    return true;
}

void LmdbStorage::do_iterate_type(KeyType key_type, const IterateTypeVisitor& visitor, const std::string &prefix) {
    ARCTICDB_SAMPLE(LmdbStorageItType, 0);
    auto txn = ::lmdb::txn::begin(env(), nullptr, MDB_RDONLY); // scoped abort on
    std::string type_db = fmt::format("{}", key_type);
    ::lmdb::dbi& dbi = dbi_by_key_type_.at(type_db);

    ARCTICDB_SUBSAMPLE(LmdbStorageOpenCursor, 0)
    auto db_cursor = ::lmdb::cursor::open(txn, dbi);

    MDB_val mdb_db_key;
    ARCTICDB_SUBSAMPLE(LmdbStorageCursorFirst, 0)
    if (!db_cursor.get(&mdb_db_key, nullptr, MDB_cursor_op::MDB_FIRST)) {
        return;
    }
    auto prefix_matcher = stream_id_prefix_matcher(prefix);
    do {
        auto k = variant_key_from_bytes(
            static_cast<uint8_t *>(mdb_db_key.mv_data),
            mdb_db_key.mv_size,
            key_type);

        ARCTICDB_DEBUG(log::storage(), "Iterating key {}: {}", variant_key_type(k), variant_key_view(k));
        if (prefix_matcher(variant_key_id(k))) {
            ARCTICDB_SUBSAMPLE(LmdbStorageVisitKey, 0)
            visitor(std::move(k));
        }
        ARCTICDB_SUBSAMPLE(LmdbStorageCursorNext, 0)
    } while (db_cursor.get(&mdb_db_key, nullptr, MDB_cursor_op::MDB_NEXT));
}


namespace {
template<class T>
T or_else(T val, T or_else_val, T def = T()) {
    return val == def ? or_else_val : val;
}
} // anonymous

LmdbStorage::LmdbStorage(const LibraryPath &library_path, OpenMode mode, const Config &conf) :
        Storage(library_path, mode) {

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

    arcticdb::entity::foreach_key_type([&txn, this](KeyType&& key_type) {
        std::string db_name = fmt::format("{}", key_type);
        ::lmdb::dbi dbi = ::lmdb::dbi::open(txn, db_name.data(), MDB_CREATE);
        dbi_by_key_type_.insert(std::make_pair(std::move(db_name), std::move(dbi)));
    });

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
