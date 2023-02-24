/*
* Copyright 2023 Man Group Operations Ltd.
* NO WARRANTY, EXPRESSED OR IMPLIED
*/

#include <arcticdb/storage/storage_factory.hpp>
#include <arcticdb/storage/config_resolvers.hpp>
#include <arcticdb/storage/lmdb/lmdb_storage.hpp>
#include <arcticdb/storage/memory/memory_storage.hpp>
#include <arcticdb/storage/mongo/mongo_storage.hpp>
#include <arcticdb/storage/s3/s3_storage.hpp>
#include <arcticdb/storage/variant_storage_factory.hpp>
#include <arcticdb/util/pb_util.hpp>

#include <folly/Range.h>
#include <memory>

namespace arcticdb::storage {

std::shared_ptr<VariantStorageFactory> create_storage_factory(
    const arcticdb::proto::storage::VariantStorage &storage) {
    std::shared_ptr<VariantStorageFactory> res;
    auto type_name = util::get_arcticdb_pb_type_name(storage.config());

    if (type_name == s3::S3Storage::Config::descriptor()->full_name()) {
        s3::S3Storage::Config s3_config;
        storage.config().UnpackTo(&s3_config);
        res = std::make_shared<VariantStorageFactory>(
            s3::S3StorageFactory(s3_config)
        );
    } else if (type_name == lmdb::LmdbStorage::Config::descriptor()->full_name()) {
        lmdb::LmdbStorage::Config lmbd_config;
        storage.config().UnpackTo(&lmbd_config);
        res = std::make_shared<VariantStorageFactory>(
            lmdb::LmdbStorageFactory(lmbd_config)
        );
    } else if (type_name == mongo::MongoStorage::Config::descriptor()->full_name()) {
        mongo::MongoStorage::Config mongo_config;
        storage.config().UnpackTo(&mongo_config);
        res = std::make_shared<VariantStorageFactory>(
            mongo::MongoStorageFactory(mongo_config)
        );
    } else if (type_name == memory::MemoryStorage::Config::descriptor()->full_name()) {
        memory::MemoryStorage::Config memory_config;
    storage.config().UnpackTo(&memory_config);
    res = std::make_shared<VariantStorageFactory>(
            memory::MemoryStorageFactory(memory_config)
        );
    } else if (type_name == nfs_backed::NfsBackedStorage::Config::descriptor()->full_name()) {
        nfs_backed::NfsBackedStorage::Config nfs_backed_config;
        storage.config().UnpackTo(&nfs_backed_config);
        res = std::make_shared<VariantStorageFactory>(
            nfs_backed::NfsBackedStorageFactory(nfs_backed_config)
        );
    } else
        throw std::runtime_error(fmt::format("Unknown config type {}", type_name));

    return res;
}

std::unique_ptr<VariantStorage> create_storage(
    const LibraryPath &library_path,
    OpenMode mode,
    const arcticdb::proto::storage::VariantStorage &storage_config) {
    return create_storage_factory(storage_config)->create_storage(library_path, mode);
}

} // namespace arcticdb::storage