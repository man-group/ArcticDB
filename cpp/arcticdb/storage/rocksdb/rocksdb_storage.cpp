/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */
#define ARCTICDB_ROCKSDB_STORAGE_H_
#include <arcticdb/storage/rocksdb/rocksdb_storage-inl.hpp>
#include <arcticdb/storage/rocksdb/rocksdb_storage.hpp>

namespace arcticdb::storage::rocksdb {
    RocksDBStorage::RocksDBStorage(const LibraryPath &library_path, OpenMode mode, const Config&) :
        Storage(library_path, mode) {
    }
} // arcticdb::storage::memory
