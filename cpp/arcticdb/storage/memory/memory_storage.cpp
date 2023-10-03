/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */
#define ARCTICDB_MEMORY_STORAGE_H_
#include <arcticdb/storage/memory/memory_storage-inl.hpp>
#include <arcticdb/storage/memory/memory_storage.hpp>

namespace arcticdb::storage::memory {
    MemoryStorage::MemoryStorage(const LibraryPath &library_path, OpenMode mode, const Config&) :
        Storage(library_path, mode) {
        arcticdb::entity::foreach_key_type([this](KeyType&& key_type) {
            data_[key_type];
        });
    }
} // arcticdb::storage::memory
