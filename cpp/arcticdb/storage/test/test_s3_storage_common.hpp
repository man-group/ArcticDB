/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/storage/s3/s3_storage.hpp>
#include <arcticdb/storage/s3/nfs_backed_storage.hpp>
#include <arcticdb/entity/protobufs.hpp>
#include <arcticdb/storage/s3/s3_api.hpp>
#include <aws/core/auth/AWSCredentialsProvider.h>
#include <aws/core/Aws.h>
#include <arcticdb/storage/test/fake_s3_client.hpp>
#include <arcticdb/entity/ref_key.hpp>
#include <folly/Range.h>
#include <arcticdb/util/buffer.hpp>

namespace ac = arcticdb;
namespace as = arcticdb::storage;
namespace as3 = arcticdb::storage::s3;
namespace ae = arcticdb::entity;
namespace anfs = arcticdb::storage::nfs_backed;

// This uses the fake s3 client, uncomment if you want to test against
// the real one. N.B. You'll need real credentials
//#define USE_FAKE_CLIENT 1

static const char* S3CredentialName = "PSFBIAZFKAAAPHGD";
static const char* S3CredentialsKey = "01330000a+950d2890F14714bfd183KPCLMPEJEBLGBFGN";
static const char* S3TestBucketName = "alpha-data-dev-arcticnative-ahl-research-1";
static const char* S3TestEndpoint = "105.fb.gdc.storage.res.m";

static const char* NFSCredentialName = "BFAXHZ08R66DT41SZJQS";
static const char* NFSCredentialsKey = "ko3Uj8nlqEHq9h+vp1QZayXy/kvaOkX38a+3n206";
static const char* NFSTestBucketName = "arctic-test";
static const char* NFSTestEndpoint = "data.vast.gdc.storage.res.m";

#ifdef USE_FAKE_CLIENT

#define GET_S3 fake_s3_storage(cfg)
#else
#define GET_S3 real_s3_storage(lib_path, cfg)


#endif

inline auto test_s3_path_and_config() {
    auto environment_name = as::EnvironmentName{"res"};
    auto storage_name = as::StorageName{"lmdb_01"};
    arcticdb::storage::LibraryPath lib_path{"test", "my_s3"};

    arcticdb::proto::s3_storage::Config cfg;
    cfg.set_bucket_name(S3TestBucketName);
    cfg.set_credential_name(S3CredentialName);
    cfg.set_credential_key(S3CredentialsKey);
    cfg.set_endpoint(S3TestEndpoint);
    return std::make_pair(cfg, lib_path);
}

inline auto test_vast_path_and_config() {
    auto environment_name = as::EnvironmentName{"res"};
    auto storage_name = as::StorageName{"vast_01"};
    arcticdb::storage::LibraryPath lib_path{"test", "my_vast"};

    arcticdb::proto::nfs_backed_storage::Config cfg;
    cfg.set_bucket_name(NFSTestBucketName);
    cfg.set_credential_name(NFSCredentialName);
    cfg.set_credential_key(NFSCredentialsKey);
    cfg.set_endpoint(NFSTestEndpoint);
    return std::make_pair(cfg, lib_path);
}

inline auto real_s3_storage(const arcticdb::storage::LibraryPath& lib_path, arcticdb::proto::s3_storage::Config& cfg) {
    as3::S3Storage storage(lib_path, as::OpenMode::WRITE, cfg);
    auto client = as::S3TestClientAccessor<as3::S3Storage>::get_client(storage);
    std::string bucket_name{as::S3TestClientAccessor<as3::S3Storage>::get_bucket_name(storage)};
    std::string root_folder{as::S3TestClientAccessor<as3::S3Storage>::get_root_folder(storage)};
    return std::make_tuple(root_folder, bucket_name, client);
}

inline auto real_vast_storage(const arcticdb::storage::LibraryPath& lib_path, const arcticdb::proto::nfs_backed_storage::Config& cfg) {

    anfs::NfsBackedStorage storage(lib_path, as::OpenMode::WRITE, cfg);
    auto client = as::S3TestClientAccessor<anfs::NfsBackedStorage>::get_client(storage);
    std::string bucket_name{as::S3TestClientAccessor<anfs::NfsBackedStorage>::get_bucket_name(storage)};
    std::string root_folder{as::S3TestClientAccessor<anfs::NfsBackedStorage>::get_root_folder(storage)};
    return std::make_tuple(root_folder, bucket_name, client);
}

inline auto fake_s3_storage(arcticdb::storage::LibraryPath lib_path) {
    auto client = arcticdb::storage::fake::S3Client();
    std::string bucket_name{S3TestBucketName};
    std::string root_folder = arcticdb::storage::s3::get_root_folder(lib_path);
    return std::make_tuple(root_folder, bucket_name, client);
}

template <typename ClientType>
void write_test_ref_key(ae::RefKey k, std::string root_folder, std::string bucket_name, ClientType& client) {
    as::KeySegmentPair kv(k);
    kv.segment().header().set_start_ts(1234);
    kv.segment().set_buffer(std::make_shared<ac::Buffer>());

    std::vector<as::KeySegmentPair> ka{std::move(kv)};
    arcticdb::storage::s3::detail::do_write_impl(ac::Composite<as::KeySegmentPair>(std::move(ka)), root_folder, bucket_name, client, arcticdb::storage::s3::detail::FlatBucketizer{});
}

inline auto thread_id() {
    return std::hash<std::thread::id>()(std::this_thread::get_id());
}


