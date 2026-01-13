/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <gtest/gtest.h>
#include <arcticdb/entity/stream_descriptor.hpp>
#include <arcticdb/stream/index.hpp>

namespace arcticdb {

TEST(StreamDescriptor, InitializerList) {
    auto desc ARCTICDB_UNUSED = stream::TimeseriesIndex::default_index().create_stream_descriptor(
            999,
            {scalar_field(DataType::UINT64, "val1"),
             scalar_field(DataType::UINT64, "val2verylongname"),
             scalar_field(DataType::UINT64, "val3")}
    );
}
} // namespace arcticdb