/*
* Copyright 2023 Man Group Operations Ltd.
* NO WARRANTY, EXPRESSED OR IMPLIED
*/

#pragma once

#include <arcticdb/storage/common.hpp>
#include <arcticdb/storage/mongo/mongo_storage.hpp>
#include <arcticdb/storage/lmdb/lmdb_storage.hpp>
#include <arcticdb/storage/s3/s3_storage.hpp>
#include <arcticdb/storage/s3/nfs_backed_storage.hpp>
#include <arcticdb/storage/memory/memory_storage.hpp>
#include <arcticdb/storage/library_path.hpp>
#include <arcticdb/storage/open_mode.hpp>
#include <arcticdb/storage/storage_factory.hpp>
#include <arcticdb/storage/variant_storage.hpp>

#include <variant>
#include <optional>
#include <vector>
#include <memory>

namespace arcticdb::storage {

using VariantStorageTypes = std::variant<lmdb::LmdbStorage, mongo::MongoStorage, s3::S3Storage, memory::MemoryStorage, nfs_backed::NfsBackedStorage>;
using VariantStorage = variant::VariantStorage<VariantStorageTypes>;

class VariantStorageFactory final : public StorageFactory<VariantStorageFactory> {
    using Parent = StorageFactory<VariantStorageFactory>;
    friend Parent;
    using StorageType = VariantStorage;

public:
    template<class T, std::enable_if_t<!std::is_same_v<std::decay_t<T>, VariantStorageFactory>, int> = 0>
    explicit VariantStorageFactory(T &&v) :
        factory_variant_(std::move(v)) {}

  protected:
    auto do_create_storage(const LibraryPath &lib, OpenMode mode) {
        return std::visit([&](auto &&impl) {
            auto s = impl.create_storage(lib, mode);
            return std::make_unique<VariantStorage>(std::move(s));
        }, factory_variant_);
    }
  private:
    std::variant<lmdb::LmdbStorageFactory, mongo::MongoStorageFactory, s3::S3StorageFactory, memory::MemoryStorageFactory, nfs_backed::NfsBackedStorageFactory> factory_variant_;
};

std::shared_ptr<VariantStorageFactory> create_storage_factory(
    const arcticdb::proto::storage::VariantStorage &storage);

std::unique_ptr<VariantStorage> create_storage(
    const LibraryPath& library_path,
    OpenMode mode,
    const arcticdb::proto::storage::VariantStorage &storage_config);

} // namespace arcticdb::storage
