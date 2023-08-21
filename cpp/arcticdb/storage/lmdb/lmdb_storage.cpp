/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#define ARCTICDB_LMDB_STORAGE_H_ 1
#include <arcticdb/storage/lmdb/lmdb_storage-inl.hpp>
#include <arcticdb/log/log.hpp>
#include <arcticdb/entity/atom_key.hpp>
#include <arcticdb/storage/library_path.hpp>
#include <arcticdb/storage/open_mode.hpp>
#include <arcticdb/util/format_bytes.hpp>

#include <filesystem>

namespace arcticdb::storage::lmdb {

namespace {
template<class T>
T or_else(T val, T or_else_val, T def = T()) {
    return val == def ? or_else_val : val;
}
} // anonymous

LmdbStorage::LmdbStorage(const LibraryPath &library_path, OpenMode mode, const Config &conf) :
        Storage(library_path, mode),
        write_mutex_(new std::mutex{}),
        env_(std::make_unique<::lmdb::env>(::lmdb::env::create(conf.flags()))),
        dbi_by_key_type_() {
    fs::path root_path = conf.path().c_str();
    auto lib_path_str = library_path.to_delim_path(fs::path::preferred_separator);

    auto lib_dir = root_path / lib_path_str;
    if (!fs::exists(lib_dir)) {
        util::check_arg(mode > OpenMode::READ, "Missing dir {} for lib={}. mode={}",
                        lib_dir.generic_string(), lib_path_str, mode);

        fs::create_directories(lib_dir);
    }

    if (fs::exists(lib_dir / "data.mdb")) {
        if (conf.recreate_if_exists() && mode >= OpenMode::WRITE) {
            fs::remove(lib_dir / "data.mdb");
            fs::remove(lib_dir / "lock.mdb");
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
    constexpr uint64_t default_map_size = 1ULL << 27; /* 128 MiB */
#else
    constexpr uint64_t default_map_size = 100ULL * (4ULL << 30); /* 400 GiB */
#endif
    auto mapsize = or_else(static_cast<uint64_t>(conf.map_size()), default_map_size);
    env().set_mapsize(mapsize);
    env().set_max_dbs(or_else(static_cast<unsigned int>(conf.max_dbs()), 1024U));
    env().set_max_readers(or_else(conf.max_readers(), 1024U));
    env().open(lib_dir.generic_string().c_str(), MDB_NOTLS);

    auto txn = ::lmdb::txn::begin(env());

    arcticdb::entity::foreach_key_type([&txn, this](KeyType&& key_type) {
        std::string db_name = fmt::format("{}", key_type);
        ::lmdb::dbi dbi = ::lmdb::dbi::open(txn, db_name.data(), MDB_CREATE);
        dbi_by_key_type_.insert({std::move(db_name), std::move(dbi)});
    });

    txn.commit();

    ARCTICDB_DEBUG(log::storage(), "Opened lmdb storage at {} with map size {}", lib_dir.generic_string(), format_bytes(mapsize));
}

} // namespace arcticdb::storage::lmdb
