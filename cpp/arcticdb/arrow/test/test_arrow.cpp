/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#include <gtest/gtest.h>

#include <arcticdb/util/test/generators.hpp>
#include <arcticdb/arrow/arrow_utils.hpp>
#include <arcticdb/arrow/arrow_data.hpp>

TEST(Arrow, ConvertColumn) {
      using namespace arcticdb;
    using TDT = TypeDescriptorTag<DataTypeTag<DataType::UINT16>, DimensionTag<Dimension ::Dim0>>;
    Column column(static_cast<TypeDescriptor>(TDT{}), 0, AllocationType::DETACHABLE, Sparsity::NOT_PERMITTED);
    for(auto i= 0; i < 10; ++i) {
        column.set_scalar<uint16_t>(i, i);
    }

    auto data = arrow_data_from_column(column, "column_1");
    ASSERT_EQ(data[0].data_->n_buffers, 2);
    ASSERT_EQ(data[0].data_->offset, 0);
    ASSERT_EQ(data[0].data_->n_children, 0);
}

TEST(Arrow, ConvertSegment) {
    using namespace arcticdb;
    auto desc = stream_descriptor(StreamId{"id"}, stream::RowCountIndex{}, {
        scalar_field(DataType::UINT8, "uint8"),
        scalar_field(DataType::UINT32, "uint32")});

    SegmentInMemory segment(desc, 20, AllocationType::DETACHABLE, Sparsity::NOT_PERMITTED, OutputFormat::ARROW, DataTypeMode::INTERNAL);

    auto col1_ptr = segment.column(0).buffer().data();
    auto col2_ptr = reinterpret_cast<uint32_t*>(segment.column(1).buffer().data());
    for(auto j = 0; j < 20; ++j ) {
        *col1_ptr++ = j;
        *col2_ptr++ = j * 2;
    }

    auto vec = segment_to_arrow_data(segment);
    ASSERT_EQ(vec.size(), 2);
}