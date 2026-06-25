/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <gtest/gtest.h>

#include <arcticdb/entity/types.hpp>
#include <arcticdb/pipeline/input_frame.hpp>
#include <arcticdb/pipeline/slicing.hpp>

using namespace arcticdb;
using namespace arcticdb::pipelines;

InputFrame create_frame(
        const StreamId& symbol, size_t offset, size_t num_rows, size_t num_columns, const IndexDescriptorImpl& index
) {
    InputFrame frame;
    frame.offset = offset;
    frame.num_rows = num_rows;
    frame.desc().set_id(symbol);
    frame.desc().set_index(index);
    if (index.type() == IndexDescriptor::Type::TIMESTAMP) {
        frame.desc().add_scalar_field(DataType::NANOSECONDS_UTC64, "ts");
    }
    for (size_t idx = 0; idx < num_columns; ++idx) {
        frame.desc().add_scalar_field(DataType::INT64, fmt::format("col_{}", idx));
    }
    return frame;
}

TEST(SpecificSlicer, SingleSliceRowTangeIndexed) {
    StreamId symbol{"my symbol"};
    const auto frame = create_frame(symbol, 0, 10, 10, IndexDescriptorImpl{IndexDescriptor::Type::ROWCOUNT, 0});
    SpecificSlicer slicer{{{1, 9}}, {{2, 5}}};
    auto slices = slicer(frame);
    ASSERT_EQ(slices.size(), 1);
    const auto& slice = slices.front();
    ASSERT_EQ(slice.desc()->id(), symbol);
    ASSERT_EQ(slice.rows(), RowRange(1, 9));
    ASSERT_EQ(slice.columns(), ColRange(2, 5));
    for (size_t idx = 0; idx < 3; ++idx) {
        const auto& field = slice.desc()->field(idx);
        ASSERT_EQ(field.name(), fmt::format("col_{}", idx + 2));
        ASSERT_EQ(field.type(), make_scalar_type(DataType::INT64));
    }
}

TEST(SpecificSlicer, SingleSliceTimeseries) {
    StreamId symbol{"my symbol"};
    const auto frame = create_frame(symbol, 0, 10, 10, IndexDescriptorImpl{IndexDescriptor::Type::TIMESTAMP, 1});
    SpecificSlicer slicer{{{1, 9}}, {{2, 5}}};
    auto slices = slicer(frame);
    ASSERT_EQ(slices.size(), 1);
    const auto& slice = slices.front();
    ASSERT_EQ(slice.desc()->id(), symbol);
    ASSERT_EQ(slice.rows(), RowRange(1, 9));
    ASSERT_EQ(slice.columns(), ColRange(2, 5));
    const auto& index_field = slice.desc()->field(0);
    ASSERT_EQ(index_field.name(), "ts");
    ASSERT_EQ(index_field.type(), make_scalar_type(DataType::NANOSECONDS_UTC64));
    for (size_t idx = 1; idx < 4; ++idx) {
        const auto& field = slice.desc()->field(idx);
        ASSERT_EQ(field.name(), fmt::format("col_{}", idx));
        ASSERT_EQ(field.type(), make_scalar_type(DataType::INT64));
    }
}

TEST(SpecificSlicer, SingleSliceTimeseriesFirstColSlice) {
    StreamId symbol{"my symbol"};
    const auto frame = create_frame(symbol, 0, 10, 10, IndexDescriptorImpl{IndexDescriptor::Type::TIMESTAMP, 1});
    SpecificSlicer slicer{{{1, 9}}, {{1, 5}}};
    auto slices = slicer(frame);
    ASSERT_EQ(slices.size(), 1);
    const auto& slice = slices.front();
    ASSERT_EQ(slice.desc()->id(), symbol);
    ASSERT_EQ(slice.rows(), RowRange(1, 9));
    ASSERT_EQ(slice.columns(), ColRange(1, 5));
    const auto& index_field = slice.desc()->field(0);
    ASSERT_EQ(index_field.name(), "ts");
    ASSERT_EQ(index_field.type(), make_scalar_type(DataType::NANOSECONDS_UTC64));
    for (size_t idx = 1; idx < 5; ++idx) {
        const auto& field = slice.desc()->field(idx);
        ASSERT_EQ(field.name(), fmt::format("col_{}", idx - 1));
        ASSERT_EQ(field.type(), make_scalar_type(DataType::INT64));
    }
}

TEST(SpecificSlicer, InvalidRowRangeStart) {
    StreamId symbol{"my symbol"};
    const auto frame = create_frame(symbol, 5, 10, 10, IndexDescriptorImpl{IndexDescriptor::Type::ROWCOUNT, 0});
    SpecificSlicer slicer{{{1, 12}}, {{2, 5}}};
    ASSERT_THROW(slicer(frame), InternalException);
}

TEST(SpecificSlicer, InvalidRowRangeEnd) {
    StreamId symbol{"my symbol"};
    const auto frame = create_frame(symbol, 0, 10, 10, IndexDescriptorImpl{IndexDescriptor::Type::ROWCOUNT, 0});
    SpecificSlicer slicer{{{1, 12}}, {{2, 5}}};
    ASSERT_THROW(slicer(frame), InternalException);
}

TEST(SpecificSlicer, MultipleSlicesRowTangeIndexed) {
    StreamId symbol{"my symbol"};
    const auto frame =
            create_frame(symbol, 100'000, 180'000, 100, IndexDescriptorImpl{IndexDescriptor::Type::ROWCOUNT, 0});
    SpecificSlicer slicer{{{100'000, 190'000}, {190'000, 280'000}}, {{0, 50}, {50, 100}}};
    auto slices = slicer(frame);
    ASSERT_EQ(slices.size(), 4);
    for (size_t idx = 0; idx < 4; ++idx) {
        const auto& slice = slices.at(idx);
        const bool left_col_slice = idx < 2;
        const bool top_row_slice = (idx % 2) == 0;
        ASSERT_EQ(slice.desc()->id(), symbol);
        ASSERT_EQ(slice.rows(), top_row_slice ? RowRange(100'000, 190'000) : RowRange(190'000, 280'000));
        ASSERT_EQ(slice.columns(), left_col_slice ? ColRange(0, 50) : ColRange(50, 100));
        for (size_t field_idx = 0; field_idx < 50; ++field_idx) {
            const auto& field = slice.desc()->field(field_idx);
            ASSERT_EQ(field.name(), fmt::format("col_{}", field_idx + (left_col_slice ? 0 : 50)));
            ASSERT_EQ(field.type(), make_scalar_type(DataType::INT64));
        }
    }
}

TEST(SpecificSlicer, MultipleSlicesTimeseries) {
    StreamId symbol{"my symbol"};
    const auto frame =
            create_frame(symbol, 100'000, 180'000, 100, IndexDescriptorImpl{IndexDescriptor::Type::TIMESTAMP, 1});
    SpecificSlicer slicer{{{100'000, 190'000}, {190'000, 280'000}}, {{1, 51}, {51, 101}}};
    auto slices = slicer(frame);
    ASSERT_EQ(slices.size(), 4);
    for (size_t idx = 0; idx < 4; ++idx) {
        const auto& slice = slices.at(idx);
        const bool left_col_slice = idx < 2;
        const bool top_row_slice = (idx % 2) == 0;
        ASSERT_EQ(slice.desc()->id(), symbol);
        ASSERT_EQ(slice.rows(), top_row_slice ? RowRange(100'000, 190'000) : RowRange(190'000, 280'000));
        ASSERT_EQ(slice.columns(), left_col_slice ? ColRange(1, 51) : ColRange(51, 101));
        const auto& index_field = slice.desc()->field(0);
        ASSERT_EQ(index_field.name(), "ts");
        ASSERT_EQ(index_field.type(), make_scalar_type(DataType::NANOSECONDS_UTC64));
        for (size_t field_idx = 0; field_idx < 50; ++field_idx) {
            const auto& field = slice.desc()->field(field_idx + 1);
            ASSERT_EQ(field.name(), fmt::format("col_{}", field_idx + (left_col_slice ? 0 : 50)));
            ASSERT_EQ(field.type(), make_scalar_type(DataType::INT64));
        }
    }
}
