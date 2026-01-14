/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/stream/row_builder.hpp>
#include <arcticdb/stream/aggregator.hpp>

#include <gtest/gtest.h>

#include <memory>

using namespace arcticdb;
namespace as = arcticdb::stream;

struct SegmentsSink {
    SegmentsSink() = default;
    std::vector<SegmentInMemory> segments;
};

TEST(Aggregator, BasicAndSegmenting) {
    const auto index = as::TimeseriesIndex::default_index();
    as::FixedSchema schema{
            index.create_stream_descriptor(
                    NumericId{123},
                    {
                            scalar_field(DataType::UINT8, "uint8"),
                    }
            ),
            index
    };

    SegmentsSink sink;

    as::FixedTimestampAggregator agg(
            std::move(schema),
            [&](SegmentInMemory&& mem) { sink.segments.push_back(std::move(mem)); },
            as::RowCountSegmentPolicy{8}
    );

    ASSERT_EQ(0, agg.row_count());

    for (timestamp i = 0; i < 7; i++) {
        agg.start_row(timestamp{i})([&](auto& rb) { rb.set_scalar(1, uint8_t(i)); });
    }

    ASSERT_EQ(7, agg.row_count());
    ASSERT_EQ(0, sink.segments.size());

    agg.start_row(timestamp{8})([](auto& rb) { rb.set_scalar(1, uint8_t{42}); });

    ASSERT_EQ(0, agg.row_count());
    ASSERT_EQ(1, sink.segments.size());
    ASSERT_EQ(8, sink.segments[0].row_count());

    agg.start_row(timestamp{8})([](auto& rb) { rb.set_scalar(1, uint8_t{42}); });

    ASSERT_EQ(1, agg.row_count());
    ASSERT_EQ(1, sink.segments.size());

    agg.commit();

    ASSERT_EQ(2, sink.segments.size());
    ASSERT_EQ(1, sink.segments[1].row_count());
}
