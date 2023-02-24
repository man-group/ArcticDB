/*
* Copyright 2023 Man Group Operations Ltd.
* NO WARRANTY, EXPRESSED OR IMPLIED
*/

#pragma once

#include <storage.pb.h>
#include <encoding.pb.h>
#include <descriptors.pb.h>
#include <s3_storage.pb.h>
#include <lmdb_storage.pb.h>
#include <mongo_storage.pb.h>
#include <in_memory_storage.pb.h>
#include <nfs_backed_storage.pb.h>
#include <config.pb.h>
#include <logger.pb.h>
#include <utils.pb.h>

namespace arcticdb::proto {
    namespace encoding = arcticc::pb2::encoding_pb2;
    namespace storage = arcticc::pb2::storage_pb2;
    namespace descriptors = arcticc::pb2::descriptors_pb2;
    namespace s3_storage = arcticc::pb2::s3_storage_pb2;
    namespace lmdb_storage = arcticc::pb2::lmdb_storage_pb2;
    namespace mongo_storage = arcticc::pb2::mongo_storage_pb2;
    namespace memory_storage = arcticc::pb2::in_memory_storage_pb2;
    namespace config = arcticc::pb2::config_pb2;
    namespace nfs_backed_storage = arcticc::pb2::nfs_backed_storage_pb2;
    namespace logger = arcticc::pb2::logger_pb2;
    namespace utils = arcticc::pb2::utils_pb2;

} //namespace arcticdb
