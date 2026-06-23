/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <gtest/gtest.h>

#include <arcticdb/storage/storage_utils.hpp>
#include <arcticdb/storage/test/in_memory_store.hpp>
#include <arcticdb/pipeline/frame_slice.hpp>
#include <arcticdb/pipeline/index_writer.hpp>
#include <arcticdb/stream/index.hpp>

namespace arcticdb {
using namespace arcticdb::pipelines;

// Copying a timeseries index key must preserve its timestamp range rather than rewriting it with a row-count range.
TEST(StorageUtils, CopyIndexKeyPreservesTimeseriesRange) {
    const StreamId stream_id{"sym"};
    const timestamp start = 1'600'000'000'000'000'000;
    const timestamp end = start + 86'400'000'000'000;

    StreamDescriptor desc{stream_id, IndexDescriptorImpl{IndexDescriptorImpl::Type::TIMESTAMP, 1}};
    desc.add_field(scalar_field(DataType::NANOSECONDS_UTC64, "time"));
    desc.add_field(scalar_field(DataType::UINT8, "col"));
    TimeseriesDescriptor tsd;
    tsd.set_stream_descriptor(desc);

    auto source = std::make_shared<InMemoryStore>();
    auto target = std::make_shared<InMemoryStore>();

    index::IndexWriter<stream::TimeseriesIndex> writer(source, IndexPartialKey{stream_id, 0}, std::move(tsd));
    writer.add(
            atom_key_builder().start_index(start).end_index(end).build(stream_id, KeyType::TABLE_DATA),
            FrameSlice{ColRange{1, 2}, RowRange{0, 1}}
    );
    const auto source_key = std::move(writer.commit()).get();

    const auto target_key = copy_index_key_recursively(source, target, source_key, std::nullopt);

    EXPECT_EQ(target_key.start_index(), IndexValue{NumericIndex{start}});
    EXPECT_EQ(target_key.end_index(), IndexValue{NumericIndex{end}});
}

} // namespace arcticdb
