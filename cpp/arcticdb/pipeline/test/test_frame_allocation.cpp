#include <gtest/gtest.h>

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

    context->set_descriptor(desc);
    auto read_options = ReadOptions{};
    read_options.set_output_format(OutputFormat::ARROW);
    auto frame = allocate_frame(context, read_options);
    ASSERT_EQ(frame.row_count(), 720);
}