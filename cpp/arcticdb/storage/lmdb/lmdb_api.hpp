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

class LmdbStorageApiInstance {
public:
    static std::shared_ptr<::lmdb::env> global_lmdb_env(const LibraryPath& library_path, OpenMode mode, const LmdbStorage::Config& conf);

    // TODO keying this off strings is weird
    static std::unordered_map<
      std::string, std::shared_ptr<::lmdb::env>> instances_;
    static std::mutex mutex_;

    static void destroy_instances();
};
}
