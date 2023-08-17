/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#include <gtest/gtest.h>

#define ARCTICDB_NFS_BACKED_STORAGE_H_ "TODO: We should extract the stuff we want to to a separate header"
#include <arcticdb/storage/s3/nfs_backed_storage-inl.hpp>
#include <arcticdb/entity/atom_key.hpp>

TEST(KeyTransformation, Roundtrip) {
    using namespace arcticdb;
    using namespace arcticdb::storage;

    auto k = entity::atom_key_builder().gen_id(3).start_index(0).end_index(1).creation_ts(999)
        .content_hash(12345).build("hello", entity::KeyType::TABLE_DATA);

    nfs_backed::NfsBucketizer b;
    std::string root_folder{"example/test"};
    auto str = b.bucketize(root_folder, k);
    ASSERT_EQ(str, "example/test/360/345");

    RefKey r{"thing", KeyType::APPEND_REF};
    str = b.bucketize(root_folder, r);
    ASSERT_EQ(str, "example/test/917");
}