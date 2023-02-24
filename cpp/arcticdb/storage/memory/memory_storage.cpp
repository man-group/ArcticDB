/*
* Copyright 2023 Man Group Operations Ltd.
* NO WARRANTY, EXPRESSED OR IMPLIED
*/

#include <arcticdb/storage/memory/memory_storage.hpp>

namespace arcticdb::storage::memory {
    MemoryStorage::MemoryStorage(const LibraryPath &library_path, OpenMode mode, const Config&) :
        Parent(library_path, mode),
        mutex_(std::make_unique<MutexType>()) {
    }
} // arcticdb::storage::memory
