#pragma once

#include <memory>
#include <mutex>

#include <arcticdb/storage/lmdb/lmdb_storage.hpp>

#ifdef ARCTICDB_USING_CONDA
#include <lmdb++.h>
#else
#include <third_party/lmdbxx/lmdb++.h>
#endif

namespace arcticdb::storage::lmdb {

class LmdbStorageApiInstance {
public:
    std::shared_ptr<::lmdb::env> global_lmdb_env();

    static std::shared_ptr<LmdbStorageApiInstance> instance_;
    static std::once_flag init_flag_;

    static void init(const LibraryPath &library_path, OpenMode mode, const LmdbStorage::Config& conf);
    static std::shared_ptr<LmdbStorageApiInstance> instance(const LibraryPath &library_path, OpenMode mode, const LmdbStorage::Config& conf);
    static void destroy_instance();
private:
    std::shared_ptr<::lmdb::env> lmdb_env_;
};
}
