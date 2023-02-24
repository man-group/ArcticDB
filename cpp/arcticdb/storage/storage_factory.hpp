/*
* Copyright 2023 Man Group Operations Ltd.
* NO WARRANTY, EXPRESSED OR IMPLIED
*/

#pragma once

#include <arcticdb/storage/common.hpp>
#include <arcticdb/storage/library_path.hpp>
#include <arcticdb/storage/open_mode.hpp>

#include <optional>
#include <vector>

namespace arcticdb::storage {

template<class Impl>
class StorageFactory {
  public:

    StorageFactory() = default;

    auto create_storage(const LibraryPath &lib, OpenMode mode) {
        return derived().do_create_storage(lib, mode);
    }

  private:
    Impl &derived() {
        return *static_cast<Impl *>(this);
    }
};

} // namespace arcticdb::storage
