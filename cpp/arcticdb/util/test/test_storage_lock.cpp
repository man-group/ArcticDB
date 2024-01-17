/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#include <gtest/gtest.h>
#include <arcticdb/util/storage_lock.hpp>

using namespace arcticdb;

TEST(StorageLock, Minimal) {
    arcticdb::stream_descriptor(
            "version",
            arcticdb::stream::RowCountIndex(),
            {arcticdb::scalar_field(arcticdb::DataType::UINT64, "version")}
    );
}
