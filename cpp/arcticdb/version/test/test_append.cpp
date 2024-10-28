/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <gtest/gtest.h>

#include <arcticdb/util/test/generators.hpp>

TEST(Append, OutOfOrder) {
    using namespace arcticdb;
    auto engine = get_test_engine();

    SegmentsSink sink;
    auto commit_func = [&](SegmentInMemory&& mem) { sink.segments_.push_back(std::move(mem)); };

    auto agg = get_test_aggregator(std::move(commit_func), "test", {scalar_field(DataType::UINT64, "uint64")});

    for (size_t i = 0; i < 10; ++i) {
        agg.start_row(timestamp(i))([&](auto& rb) { rb.set_scalar(1, i * 3); });
    }
    agg.commit();
    auto& seg = sink.segments_[0];

    SegmentToInputFrameAdapter frame(std::move(seg));
    engine.write_versioned_dataframe_internal("symbol", std::move(frame.input_frame_), false, false, false);
}