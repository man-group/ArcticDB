/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/storage/mongo/mongo_storage.hpp>
#define ARCTICDB_MONGO_STORAGE_H_
#include <arcticdb/storage/mongo/mongo_storage-inl.hpp>
#include <arcticdb/storage/mongo/mongo_instance.hpp>
#include <arcticdb/entity/index_range.hpp>
#include <arcticdb/storage/mongo/mongo_client.hpp>

#include <google/protobuf/io/zero_copy_stream_impl_lite.h>
#include <arcticdb/util/configs_map.hpp>
#include <fmt/format.h>

namespace arcticdb::storage::mongo {

using Config = arcticdb::proto::mongo_storage::Config;

MongoStorage::MongoStorage(
    const LibraryPath &lib,
    OpenMode mode,
    const Config &config) :
    Storage(lib, mode),
    instance_(MongoInstance::instance()),
    client_(std::make_unique<MongoClient>(
        config,
        ConfigsMap::instance()->get_int("MongoClient.MinPoolSize", 100),
        ConfigsMap::instance()->get_int("MongoClient.MaxPoolSize", 1000),
        ConfigsMap::instance()->get_int("MongoClient.SelectionTimeoutMs", 120000))
        ) {
    instance_.reset(); //Just want to ensure singleton here, not hang onto it
    auto key_rg = lib.as_range();
    auto it = key_rg.begin();
    db_ = fmt::format("arcticc_{}", *it++);
    std::ostringstream strm;
    for (; it != key_rg.end(); ++it) {
        strm << *it->get() << "__";
    }
    prefix_ = strm.str();
}

}
