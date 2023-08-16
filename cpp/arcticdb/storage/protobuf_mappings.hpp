/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/entity/protobufs.hpp>
#include <arcticdb/entity/protobufs.hpp>
#include <arcticdb/entity/protobufs.hpp>
#include <arcticdb/storage/library_path.hpp>
#include <arcticdb/storage/storage_factory.hpp>
#include <arcticdb/storage/config_resolvers.hpp>
#include <arcticdb/storage/memory/memory_storage.hpp>
#include <arcticdb/storage/lmdb/lmdb_storage.hpp>
#include <arcticdb/storage/mongo/mongo_storage.hpp>
#include <arcticdb/util/pb_util.hpp>

#include <vector>
#include <arcticdb/storage/common.hpp>

namespace arcticdb::storage {

inline arcticdb::proto::storage::LibraryPath encode_library_path(const storage::LibraryPath& library_path) {
    arcticdb::proto::storage::LibraryPath output;
    for (auto &part : library_path.as_range()) {
        output.add_parts(std::string(part));
    }
    return output;
}

inline arcticdb::storage::LibraryPath decode_library_path(const arcticdb::proto::storage::LibraryPath& protobuf_path) {
    std::vector<std::string> parts;
    for (auto i = 0; i < protobuf_path.parts_size(); ++i)
        parts.push_back(protobuf_path.parts(i));

    return storage::LibraryPath(parts);
}

inline arcticdb::proto::storage::LibraryDescriptor encode_library_descriptor(storage::LibraryDescriptor library_descriptor) {
    arcticdb::proto::storage::LibraryDescriptor output;
    output.set_name(library_descriptor.name_);
    output.set_description(library_descriptor.description_);
    for(auto& id : library_descriptor.storage_ids_)
        output.add_storage_ids(id.value);

    return output;
}

inline storage::LibraryDescriptor decode_library_descriptor(const arcticdb::proto::storage::LibraryDescriptor & protobuf_descriptor) {
    storage::LibraryDescriptor output;
    output.name_ = protobuf_descriptor.name();
    output.description_ = protobuf_descriptor.description();
    for(int i = 0; i < protobuf_descriptor.storage_ids_size(); ++i)
        output.storage_ids_.push_back(StorageName(protobuf_descriptor.storage_ids(i)));

    switch (protobuf_descriptor.store_type_case()){
        case arcticdb::proto::storage::LibraryDescriptor::StoreTypeCase::kVersion:
            output.config_ = protobuf_descriptor.version(); // copy
            break;
            case arcticdb::proto::storage::LibraryDescriptor::StoreTypeCase::STORE_TYPE_NOT_SET:
            // nothing to do, the variant is a monostate (empty struct) by default
            break;
        default: util::raise_rte("Unsupported store config type {}", protobuf_descriptor.store_type_case());
    }

    return output;
}

using MemConfig  = storage::details::InMemoryConfigResolver::MemoryConfig;

inline std::vector<std::pair<std::string, MemConfig>> convert_environment_config(arcticdb::proto::storage::EnvironmentConfigsMap envs) {
    std::vector<std::pair<std::string, MemConfig>> env_by_id;
    for (auto&[env_key, env_config] : envs.env_by_id()) {
        MemConfig current;
        for (auto &[storage_key, storage_value] : env_config.storage_by_id())
            current.storages_.insert(std::make_pair(storage::StorageName(storage_key), storage_value));

        for (auto &[library_key, library_value] : env_config.lib_by_path())
            current.libraries_.insert(std::make_pair(LibraryPath::from_delim_path(library_key), library_value));

        env_by_id.push_back(std::make_pair(env_key, current));
    }
    return env_by_id;
}

inline proto::mongo_storage::Config create_mongo_config(InstanceUri uri, uint32_t flags = 0) {
    proto::mongo_storage::Config output;
    output.set_uri(uri.value);
    output.set_flags(flags);
    return output;
}

} //namespace arcticdb::storage