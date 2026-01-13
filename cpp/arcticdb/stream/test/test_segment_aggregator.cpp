/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <gtest/gtest.h>
#include <arcticdb/stream/segment_aggregator.hpp>
#include <arcticdb/util/test/generators.hpp>
#include <arcticdb/pipeline/frame_slice.hpp>
#include <arcticdb/util/test/generators.hpp>

TEST(SegmentAggregator, Basic) {
    using namespace arcticdb;

    std::string symbol{"aggregator"};

    std::vector<SegmentInMemory> segments;

    size_t count = 0;
    for (size_t i = 0; i < 10; ++i) {
        auto wrapper = SinkWrapper(
                symbol, {scalar_field(DataType::UINT64, "numbers"), scalar_field(DataType::ASCII_DYNAMIC64, "strings")}
        );

        for (timestamp j = 0; j < 20; ++j) {
            wrapper.aggregator_.start_row(timestamp(count++))([&](auto&& rb) {
                rb.set_scalar(1, j);
                rb.set_string(2, fmt::format("{}", i + j));
            });
        }

        wrapper.aggregator_.commit();
        segments.emplace_back(std::move(wrapper.segment()));
    }

    SegmentSinkWrapper seg_wrapper(
            symbol,
            TimeseriesIndex::default_index(),
            fields_from_range(std::vector<FieldRef>{
                    scalar_field(DataType::UINT64, "numbers"), scalar_field(DataType::ASCII_DYNAMIC64, "strings")
            })
    );

    for (auto& segment : segments) {
        pipelines::FrameSlice slice(segment);
        seg_wrapper.aggregator_.add_segment(std::move(segment), slice, false);
    }

    seg_wrapper.aggregator_.commit();
    auto seg = seg_wrapper.segment();

    count = 0;
    for (size_t i = 0; i < 10; ++i) {
        for (size_t j = 0; j < 20; ++j) {
            ASSERT_EQ(seg.scalar_at<uint64_t>(count, 1), j);
            auto str = seg.string_at(count, 2).value();
            ASSERT_EQ(str, fmt::format("{}", i + j));
            ++count;
        }
    }
}
