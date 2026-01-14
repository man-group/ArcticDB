/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <gtest/gtest.h>

#include <arcticdb/stream/row_builder.hpp>
#include <arcticdb/stream/aggregator.hpp>

namespace as = arcticdb::stream;

struct SegmentHolder {
    arcticdb::SegmentInMemory segment;
};

TEST(RowBuilder, Basic) {
    using namespace arcticdb;
    const auto index = as::TimeseriesIndex::default_index();
    as::FixedSchema schema{
            index.create_stream_descriptor(
                    NumericId{123},
                    {
                            arcticdb::scalar_field(DataType::UINT8, "bbb"),
                            arcticdb::scalar_field(DataType::INT8, "AAA"),
                    }
            ),
            index
    };

    SegmentHolder holder;

    as::FixedTimestampAggregator agg(std::move(schema), [&](SegmentInMemory&& mem) {
        holder.segment = std::move(mem);
    });

    ASSERT_EQ(agg.row_count(), 0);

    auto& rb = agg.row_builder();

    ASSERT_TRUE(rb.find_field("AAA"));
    ASSERT_FALSE(rb.find_field("BBB"));

    // nominal case using low level api
    rb.start_row(timestamp{1});
    rb.set_scalar(1, uint8_t{3});
    rb.set_scalar(2, int8_t{-2});
    rb.end_row();

    // now using the transactional api with auto commit
    // out of order fields are ok
    agg.start_row(arcticdb::timestamp{2})([](auto& rb) {
        rb.set_scalar(2, int8_t{-66});
        rb.set_scalar(1, uint8_t{42});
    });

    ASSERT_EQ(2, agg.row_count());

    // TODO uncomment this once rollback on segment is implemented
    //    ASSERT_THROW(agg.start_row(timestamp{3})([](auto & rb){
    //       rb.set_scalar(1, 666.);
    //    }), std::invalid_argument);
    //    ASSERT_EQ(2, agg.row_count());

    // monotonic index
    ASSERT_THROW(rb.start_row(timestamp{1}), ArcticCategorizedException<ErrorCategory::INTERNAL>);

    // all fields required by schema
    rb.start_row(timestamp{3});
    rb.set_scalar(1, uint8_t{3});
    // ASSERT_THROW(rb.end_row(), std::invalid_argument);

    agg.commit();

    ASSERT_EQ(agg.row_count(), 0);

    // index survives commit_row
    ASSERT_THROW(rb.start_row(timestamp{1}), ArcticCategorizedException<ErrorCategory::INTERNAL>);

    // can still build using a row builder

    rb.start_row(timestamp{3});
    rb.set_scalar(1, uint8_t{3});
    rb.set_scalar(2, int8_t{-2});
    rb.end_row();
}
