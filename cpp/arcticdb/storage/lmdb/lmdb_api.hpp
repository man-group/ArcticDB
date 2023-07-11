#pragma once

#include <map>
#include <memory>
#include <mutex>

#include <arcticdb/storage/lmdb/lmdb_storage.hpp>

#ifdef ARCTICDB_USING_CONDA
#include <lmdb++.h>
#else
#include <third_party/lmdbxx/lmdb++.h>
#endif

namespace arcticdb::storage::lmdb {

class LmdbEnvironments {
public:
    static std::shared_ptr<::lmdb::env> lmdb_env(const LibraryPath& library_path, OpenMode mode, const LmdbStorage::Config& conf);

    static std::unordered_map<std::string, std::shared_ptr<::lmdb::env>> envs_by_path_;
    static std::mutex mutex_;

    static void tear_down_lmdb_environments();
};
}
