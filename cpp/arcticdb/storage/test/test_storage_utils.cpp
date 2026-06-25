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
#include <arcticdb/stream/test/stream_test_common.hpp>
#include <arcticdb/pipeline/frame_slice.hpp>
#include <arcticdb/pipeline/index_writer.hpp>
#include <arcticdb/stream/index.hpp>

namespace arcticdb {
using namespace arcticdb::pipelines;

namespace {

template<typename IndexType>
AtomKey write_source_index(const std::shared_ptr<Store>& store, const StreamId& stream_id) {
    const std::array fields{scalar_field(DataType::UINT8, "col")};
    TimeseriesDescriptor tsd;
    tsd.set_stream_descriptor(get_test_descriptor<IndexType>(stream_id, fields));

    index::IndexWriter<IndexType> writer(store, IndexPartialKey{stream_id, 0}, std::move(tsd));
    for (size_t i = 0; i < 3; ++i) {
        const timestamp start = 100 + static_cast<timestamp>(i) * 10;
        writer.add(
                atom_key_builder().start_index(start).end_index(start + 10).build(stream_id, KeyType::TABLE_DATA),
                FrameSlice{ColRange{1, 2}, RowRange{i, i + 1}}
        );
    }
    return std::move(writer.commit()).get();
}

} // namespace

template<typename IndexType>
class CopyIndexKeyTest : public ::testing::Test {};

using IndexTypes = ::testing::Types<stream::TimeseriesIndex, stream::RowCountIndex>;
TYPED_TEST_SUITE(CopyIndexKeyTest, IndexTypes);

TYPED_TEST(CopyIndexKeyTest, PreservesIndexRange) {
    using IndexType = TypeParam;
    const StreamId stream_id{"sym"};
    auto source = std::make_shared<InMemoryStore>();
    auto target = std::make_shared<InMemoryStore>();

    const auto source_key = write_source_index<IndexType>(source, stream_id);
    const auto target_key = copy_index_key_recursively(source, target, source_key, std::nullopt);

    EXPECT_EQ(target_key.start_index(), source_key.start_index());
    EXPECT_EQ(target_key.end_index(), source_key.end_index());

    if constexpr (std::is_same_v<IndexType, stream::TimeseriesIndex>) {
        EXPECT_EQ(target_key.start_index(), IndexValue{NumericIndex{100}});
        EXPECT_EQ(target_key.end_index(), IndexValue{NumericIndex{130}});
    } else {
        EXPECT_EQ(target_key.start_index(), IndexValue{NumericIndex{0}});
        EXPECT_EQ(target_key.end_index(), IndexValue{NumericIndex{2}});
    }
}

} // namespace arcticdb
