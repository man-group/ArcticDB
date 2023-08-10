/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/storage/storage_factory.hpp>
#include <arcticdb/storage/config_resolvers.hpp>
#include <arcticdb/storage/lmdb/lmdb_storage.hpp>
#include <arcticdb/storage/memory/memory_storage.hpp>
#include <arcticdb/storage/mongo/mongo_storage.hpp>
#include <arcticdb/storage/s3/s3_storage.hpp>
#ifndef ARCTICDB_USING_CONDA //Awaiting Azure sdk support in conda https://github.com/man-group/ArcticDB/issues/519
#include <arcticdb/storage/azure/azure_storage.hpp>
#endif
#include <arcticdb/storage/variant_storage_factory.hpp>
#include <arcticdb/util/pb_util.hpp>

#include <folly/Range.h>
#include <memory>

namespace arcticdb::storage {

std::unique_ptr<VariantStorage> create_storage(
    const LibraryPath &library_path,
    OpenMode mode,
    const arcticdb::proto::storage::StorageConfig &storage_config) {

    std::unique_ptr<VariantStorage> storage;
    auto type_name = util::get_arcticdb_pb_type_name(storage_config.config());

    if (type_name == s3::S3Storage::Config::descriptor()->full_name()) {
        s3::S3Storage::Config s3_config;
        storage_config.config().UnpackTo(&s3_config);
        storage = std::make_unique<VariantStorage>(
                s3::S3Storage(library_path, mode, s3_config)
        );
    } else if (type_name == lmdb::LmdbStorage::Config::descriptor()->full_name()) {
        lmdb::LmdbStorage::Config lmbd_config;
        storage_config.config().UnpackTo(&lmbd_config);
        storage = std::make_unique<VariantStorage>(
                lmdb::LmdbStorage(library_path, mode, lmbd_config)
        );
    } else if (type_name == mongo::MongoStorage::Config::descriptor()->full_name()) {
        mongo::MongoStorage::Config mongo_config;
        storage_config.config().UnpackTo(&mongo_config);
        storage = std::make_unique<VariantStorage>(
                mongo::MongoStorage(library_path, mode, mongo_config)
        );
    } else if (type_name == memory::MemoryStorage::Config::descriptor()->full_name()) {
        memory::MemoryStorage::Config memory_config;
        storage_config.config().UnpackTo(&memory_config);
        storage = std::make_unique<VariantStorage>(
                memory::MemoryStorage(library_path, mode, memory_config)
        );
    } else if (type_name == nfs_backed::NfsBackedStorage::Config::descriptor()->full_name()) {
        nfs_backed::NfsBackedStorage::Config nfs_backed_config;
        storage_config.config().UnpackTo(&nfs_backed_config);
        storage = std::make_unique<VariantStorage>(
                nfs_backed::NfsBackedStorage(library_path, mode, nfs_backed_config)
        );
#ifndef ARCTICDB_USING_CONDA //Awaiting Azure sdk support in conda https://github.com/man-group/ArcticDB/issues/519
        } else if (type_name == azure::AzureStorage::Config::descriptor()->full_name()) {
        azure::AzureStorage::Config azure_config;
        storage_config.config().UnpackTo(&azure_config);
        storage = std::make_unique<VariantStorage>(
            azure::AzureStorage(library_path, mode, azure_config)
        );
#endif
    } else
        throw std::runtime_error(fmt::format("Unknown config type {}", type_name));

    return storage;
}

} // namespace arcticdb::storage
