/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/storage/lmdb/lmdb_api.hpp>
#include <arcticdb/storage/lmdb/lmdb_storage.hpp>
#include <arcticdb/log/log.hpp>
#include <arcticdb/storage/library_path.hpp>
#include <arcticdb/storage/open_mode.hpp>

#include <filesystem>

namespace arcticdb::storage::lmdb {


LmdbStorage::LmdbStorage(const LibraryPath &library_path, OpenMode mode, const Config &conf) :
    Parent(library_path, mode),
    write_mutex_(new std::mutex{}),
    env_(LmdbStorageApiInstance::instance(library_path, mode, conf)->global_lmdb_env()) {
}

} // namespace arcticdb::storage::lmdb
