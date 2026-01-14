/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <storage.pb.h>
#include <encoding.pb.h>
#include <s3_storage.pb.h>
#include <gcp_storage.pb.h>
#include <lmdb_storage.pb.h>
#include <mongo_storage.pb.h>
#include <in_memory_storage.pb.h>
#include <nfs_backed_storage.pb.h>
#include <azure_storage.pb.h>
#include <mapped_file_storage.pb.h>
#include <config.pb.h>
#include <utils.pb.h>

namespace arcticdb::proto {

namespace encoding = arcticc::pb2::encoding_pb2;
namespace storage = arcticc::pb2::storage_pb2;
namespace s3_storage = arcticc::pb2::s3_storage_pb2;
namespace gcp_storage = arcticc::pb2::gcp_storage_pb2;
namespace lmdb_storage = arcticc::pb2::lmdb_storage_pb2;
namespace mapped_file_storage = arcticc::pb2::mapped_file_storage_pb2;
namespace mongo_storage = arcticc::pb2::mongo_storage_pb2;
namespace memory_storage = arcticc::pb2::in_memory_storage_pb2;
namespace azure_storage = arcticc::pb2::azure_storage_pb2;
namespace config = arcticc::pb2::config_pb2;
namespace nfs_backed_storage = arcticc::pb2::nfs_backed_storage_pb2;
namespace utils = arcticc::pb2::utils_pb2;

} // namespace arcticdb::proto
