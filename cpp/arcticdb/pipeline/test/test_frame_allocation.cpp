#include <gtest/gtest.h>

#include <arcticdb/pipeline/frame_utils.hpp>
#include <arcticdb/pipeline/read_frame.hpp>
#include <arcticdb/pipeline/pipeline_context.hpp>

TEST(OutputFrame, AllocateChunked) {
    using namespace arcticdb;
    using namespace arcticdb::pipelines;

    auto context = std::make_shared<PipelineContext>();
    FrameSlice slice1{{1, 45}, {0, 400}};
    context->slice_and_keys_.emplace_back(SliceAndKey{slice1, AtomKey{}});
    FrameSlice slice2{{46, 70}, {0, 400}};
    context->slice_and_keys_.emplace_back(SliceAndKey{slice2, AtomKey{}});
    FrameSlice slice3{{1, 45}, {400, 500}};
    context->slice_and_keys_.emplace_back(SliceAndKey{slice3, AtomKey{}});
    FrameSlice slice4{{46, 70}, {400, 500}};
    context->slice_and_keys_.emplace_back(SliceAndKey{slice4, AtomKey{}});
    FrameSlice slice5{{1, 45}, {500, 720}};
    context->slice_and_keys_.emplace_back(SliceAndKey{slice5, AtomKey{}});
    FrameSlice slice6{{46, 70}, {500, 720}};
    context->slice_and_keys_.emplace_back(SliceAndKey{slice6, AtomKey{}});

    for (auto i = 0; i < 6; ++i) {
        if (!(i & 1))
            context->fetch_index_.set_bit(i);
    }

    auto index = stream::TimeseriesIndex::default_index();
    auto desc = index.create_stream_descriptor(
            NumericId{123},
            {
                    scalar_field(DataType::ASCII_DYNAMIC64, "col_1"),
                    scalar_field(DataType::ASCII_DYNAMIC64, "col_2"),
                    scalar_field(DataType::ASCII_DYNAMIC64, "col_3"),
                    scalar_field(DataType::ASCII_DYNAMIC64, "col_4"),
                    scalar_field(DataType::ASCII_DYNAMIC64, "col_5"),
                    scalar_field(DataType::ASCII_DYNAMIC64, "col_6"),
            }
    );

    context->set_on_disk_descriptor(std::move(desc));
    auto read_options = ReadOptions{};
    read_options.set_output_config(ArrowOutputConfig{.default_string_format_ = ArrowOutputStringFormat::LARGE_STRING});
    auto frame = allocate_frame(context, read_options);
    ASSERT_EQ(frame.row_count(), 720);
}

TEST(OutputFrame, AllocateContiguousNumpy) {
    using namespace arcticdb;
    using namespace arcticdb::pipelines;

    auto context = std::make_shared<PipelineContext>();
    FrameSlice slice1{{1, 45}, {0, 10}};
    context->slice_and_keys_.emplace_back(SliceAndKey{slice1, AtomKey{}});
    FrameSlice slice2{{1, 45}, {10, 30}};
    context->slice_and_keys_.emplace_back(SliceAndKey{slice2, AtomKey{}});
    FrameSlice slice3{{1, 45}, {30, 60}};
    context->slice_and_keys_.emplace_back(SliceAndKey{slice3, AtomKey{}});
    context->fetch_index_.set_bit(0);
    context->fetch_index_.set_bit(1);
    context->fetch_index_.set_bit(2);

    auto index = stream::TimeseriesIndex::default_index();
    auto desc = index.create_stream_descriptor(
            NumericId{123},
            {
                    scalar_field(DataType::ASCII_DYNAMIC64, "col_1"),
                    scalar_field(DataType::FLOAT64, "col_2"),
            }
    );

    context->set_on_disk_descriptor(desc);
    auto read_options = ReadOptions{};
    read_options.set_output_config(PandasOutputConfig{});
    auto frame = allocate_frame(context, read_options);
    ASSERT_EQ(frame.row_count(), 60);
    for (auto& column : frame.columns()) {
        ASSERT_EQ(column->num_blocks(), 1);
    }
}

TEST(OutputFrame, AllocateMixedNumpyArrow) {
    using namespace arcticdb;
    using namespace arcticdb::pipelines;

    auto context = std::make_shared<PipelineContext>();
    FrameSlice slice1{{1, 45}, {0, 10}};
    context->slice_and_keys_.emplace_back(SliceAndKey{slice1, AtomKey{}});
    FrameSlice slice2{{1, 45}, {10, 30}};
    context->slice_and_keys_.emplace_back(SliceAndKey{slice2, AtomKey{}});
    FrameSlice slice3{{1, 45}, {30, 60}};
    context->slice_and_keys_.emplace_back(SliceAndKey{slice3, AtomKey{}});
    context->fetch_index_.set_bit(0);
    context->fetch_index_.set_bit(1);
    context->fetch_index_.set_bit(2);

    auto index = stream::TimeseriesIndex::default_index();
    auto desc = index.create_stream_descriptor(
            NumericId{123},
            {
                    scalar_field(DataType::ASCII_DYNAMIC64, "col_1"),
                    scalar_field(DataType::FLOAT64, "col_2"),
            }
    );

    context->set_on_disk_descriptor(desc);
    auto read_options = ReadOptions{};
    read_options.set_output_config(PandasOutputConfig{PandasStringFormat::ARROW_LARGE_STRING});
    auto frame = allocate_frame(context, read_options);
    ASSERT_EQ(frame.row_count(), 60);
    ASSERT_EQ(frame.column(0).num_blocks(), 1);
    ASSERT_EQ(frame.column(1).num_blocks(), 3);
    ASSERT_EQ(frame.column(2).num_blocks(), 1);
}

TEST(OutputFrame, OffsetAndRowCountWithDifferentlyColumnSlicedRowSlices) {
    using namespace arcticdb;
    using namespace arcticdb::pipelines;

    // Column-projected read after columns_per_segment changed from 1 to 4 between writes: slice_and_keys_ is ordered by
    // column slice, so the later rows (in column slice [1, 5)) come before the earlier rows (in column slice [2, 3))
    auto context = std::make_shared<PipelineContext>();
    context->slice_and_keys_.emplace_back(SliceAndKey{FrameSlice{{1, 5}, {20, 30}}, AtomKey{}});
    context->slice_and_keys_.emplace_back(SliceAndKey{FrameSlice{{1, 5}, {30, 35}}, AtomKey{}});
    context->slice_and_keys_.emplace_back(SliceAndKey{FrameSlice{{2, 3}, {0, 10}}, AtomKey{}});
    context->slice_and_keys_.emplace_back(SliceAndKey{FrameSlice{{2, 3}, {10, 20}}, AtomKey{}});
    for (auto i = 0; i < 4; ++i) {
        context->fetch_index_.set_bit(i);
    }

    const auto [offset, row_count] = offset_and_row_count(context);
    ASSERT_EQ(offset, 0);
    ASSERT_EQ(row_count, 35);
    ASSERT_EQ(output_block_row_counts(context), (std::vector<size_t>{10, 10, 10, 5}));
    ASSERT_EQ(context->first_row(), 0);
    ASSERT_EQ(context->last_row(), 35);
    ASSERT_EQ(context->calc_rows(), 35);
}
