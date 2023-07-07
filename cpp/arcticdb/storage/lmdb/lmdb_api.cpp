#include <arcticdb/storage/lmdb/lmdb_api.hpp>
#include <arcticdb/util/format_bytes.hpp>

namespace arcticdb::storage::lmdb {

namespace {
    template<class T>
    T or_else(T val, T or_else_val, T def = T()) {
        return val == def ? or_else_val : val;
    }
} // anonymous

void LmdbStorageApiInstance::init(const LibraryPath &library_path, OpenMode mode, const LmdbStorage::Config& conf) {
    LmdbStorageApiInstance::instance_ = std::make_shared<LmdbStorageApiInstance>();
    LmdbStorageApiInstance::instance_->lmdb_env_
      = std::make_shared<::lmdb::env>(::lmdb::env::create());

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
    LmdbStorageApiInstance::instance_->lmdb_env_->set_mapsize(mapsize);
    unsigned int count = or_else(static_cast<unsigned int>(conf.max_dbs()), 1024U);
    LmdbStorageApiInstance::instance_->lmdb_env_->set_max_dbs(count);
    LmdbStorageApiInstance::instance_->lmdb_env_->set_max_readers(or_else(conf.max_readers(), 1024U));
    LmdbStorageApiInstance::instance_->lmdb_env_->open(lib_dir.generic_string().c_str(), MDB_NOTLS);

    ARCTICDB_DEBUG(log::storage(), "Opened lmdb storage at {} with map size {}", lib_dir.generic_string(), format_bytes(mapsize));
}

std::shared_ptr<::lmdb::env> LmdbStorageApiInstance::global_lmdb_env() {
    return lmdb_env_;
}

std::shared_ptr<LmdbStorageApiInstance> LmdbStorageApiInstance::instance(const LibraryPath &library_path, OpenMode mode, const LmdbStorage::Config& conf) {
    std::call_once(LmdbStorageApiInstance::init_flag_, &LmdbStorageApiInstance::init, library_path, mode, conf);
    return instance_;
}

void LmdbStorageApiInstance::destroy_instance() {
    LmdbStorageApiInstance::instance_.reset();
}

std::shared_ptr<LmdbStorageApiInstance> LmdbStorageApiInstance::instance_;
std::once_flag LmdbStorageApiInstance::init_flag_;

}
