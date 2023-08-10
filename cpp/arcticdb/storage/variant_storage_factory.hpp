/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/storage/common.hpp>
#include <arcticdb/storage/mongo/mongo_storage.hpp>
#include <arcticdb/storage/lmdb/lmdb_storage.hpp>
#include <arcticdb/storage/s3/s3_storage.hpp>
#include <arcticdb/storage/s3/nfs_backed_storage.hpp>
#ifndef ARCTICDB_USING_CONDA //Awaiting Azure sdk support in conda https://github.com/man-group/ArcticDB/issues/519
#include <arcticdb/storage/azure/azure_storage.hpp>
#endif
#include <arcticdb/storage/memory/memory_storage.hpp>
#include <arcticdb/storage/library_path.hpp>
#include <arcticdb/storage/open_mode.hpp>
#include <arcticdb/storage/storage_factory.hpp>
#include <arcticdb/storage/variant_storage.hpp>

#include <variant>
#include <optional>
#include <vector>
#include <memory>

namespace arcticdb{
#ifdef ARCTICDB_USING_CONDA
static const bool AZURE_SUPPORT = false;
#else
static const bool AZURE_SUPPORT = true;
#endif

namespace storage {

#ifdef ARCTICDB_USING_CONDA //Awaiting Azure sdk support in conda https://github.com/man-group/ArcticDB/issues/519
using VariantStorageTypes = std::variant<lmdb::LmdbStorage, mongo::MongoStorage, s3::S3Storage, memory::MemoryStorage, nfs_backed::NfsBackedStorage>;
#else
using VariantStorageTypes = std::variant<lmdb::LmdbStorage, mongo::MongoStorage, s3::S3Storage, memory::MemoryStorage, nfs_backed::NfsBackedStorage, azure::AzureStorage>;
#endif
using VariantStorage = variant::VariantStorage<VariantStorageTypes>;

std::unique_ptr<VariantStorage> create_storage(
    const LibraryPath& library_path,
    OpenMode mode,
    const arcticdb::proto::storage::StorageConfig &storage_config);

} // namespace storage
} // namespace arcticdb
