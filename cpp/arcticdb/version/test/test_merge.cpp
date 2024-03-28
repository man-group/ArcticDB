/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#include <gtest/gtest.h>

#include <arcticdb/stream/index.hpp>
#include <arcticdb/stream/schema.hpp>
#include <arcticdb/stream/aggregator.hpp>
#include <arcticdb/util/test/generators.hpp>
#include <arcticdb/stream/test/stream_test_common.hpp>
#include <arcticdb/stream/test/test_store_common.hpp>


struct MergeReadsTestStore : arcticdb::TestStore {
protected:
    std::string get_name() override {
        return "test.merge_streams";
    }
};

TEST_F(MergeReadsTestStore, SimpleStaticSchema) {
    using namespace arcticdb;
    using namespace arcticdb::stream;
    using MergeAggregator =  Aggregator<TimeseriesIndex,
                                          FixedSchema,
                                          stream::RowCountSegmentPolicy,
                                          stream::SparseColumnPolicy>;


    using MergeSinkWrapper = SinkWrapperImpl<MergeAggregator>;
    const auto NumTests = 10;

    const std::string stream_id1("test_merge_1");
    const std::string stream_id2("test_merge_2");

    MergeSinkWrapper wrapper1(stream_id1, {
        scalar_field(DataType::UINT64, "thing1"),
        scalar_field(DataType::UINT64, "thing2")}
        );

    auto& aggregator1 = wrapper1.aggregator_;

    MergeSinkWrapper wrapper2(stream_id2, {
        scalar_field(DataType::UINT64, "thing1"),
        scalar_field(DataType::UINT64, "thing2")}
    );

    auto& aggregator2 = wrapper2.aggregator_;

    for(timestamp i = 0; i < NumTests; i += 2) {
        aggregator1.start_row(i)([&] (auto&& rb) {
            rb.set_scalar(1, i);
            rb.set_scalar(2, i + 1);
        });

        aggregator2.start_row(i + 1)([&] (auto&& rb) {
            rb.set_scalar(1, i + 1);
            rb.set_scalar(2, i + 2);
        });
    }

    aggregator1.commit();
    aggregator2.commit();

    test_store_->write_individual_segment(stream_id1, std::move(wrapper1.segment()), false);
    test_store_->write_individual_segment(stream_id2, std::move(wrapper2.segment()), false);

    const std::string target_id("some_merged_stuff");
    pipelines::ReadQuery read_query;
    auto read_result = test_store_->read_dataframe_merged(target_id, {stream_id1, stream_id2}, pipelines::VersionQuery{}, read_query, ReadOptions{});
    const auto& frame = read_result.frame_data.frame();

    for(timestamp i = 0; i < NumTests; i += 2) {
        check_value(frame.scalar_at<uint64_t>(i, 2).value(), uint64_t(i));
        check_value(frame.scalar_at<uint64_t>(i, 3).value(), uint64_t(i + 1));
        check_value(frame.scalar_at<uint64_t>(i + 1, 2).value(), uint64_t(i + 1));
        check_value(frame.scalar_at<uint64_t>(i + 1, 3).value(), uint64_t(i + 2));
    }
}

TEST_F(MergeReadsTestStore, SparseTarget) {
    using namespace arcticdb;
    using namespace arcticdb::stream;
    using MergeAggregator =  Aggregator<TimeseriesIndex,
                                        FixedSchema,
                                        stream::RowCountSegmentPolicy,
                                        stream::SparseColumnPolicy>;


    using MergeSinkWrapper = SinkWrapperImpl<MergeAggregator>;
    const auto NumTests = 10;

    const std::string stream_id1("test_merge_1");
    const std::string stream_id2("test_merge_2");

    MergeSinkWrapper wrapper1(stream_id1, {
        scalar_field(DataType::UINT64, "thing1"),
        scalar_field(DataType::UINT64, "thing2")}
    );

    auto& aggregator1 = wrapper1.aggregator_;

    MergeSinkWrapper wrapper2(stream_id2, {
        scalar_field(DataType::UINT64, "thing1"),
        scalar_field(DataType::UINT64, "thing3")}
    );

    auto& aggregator2 = wrapper2.aggregator_;

    for(timestamp i = 0; i < NumTests; i += 2) {
        aggregator1.start_row(i)([&] (auto&& rb) {
            rb.set_scalar(1, i);
            rb.set_scalar(2, i + 1);
        });

        aggregator2.start_row(i + 1)([&] (auto&& rb) {
            rb.set_scalar(1, i + 1);
            rb.set_scalar(2, i + 2);
        });
    }

    aggregator1.commit();
    aggregator2.commit();

    test_store_->write_individual_segment(stream_id1, std::move(wrapper1.segment()), false);
    test_store_->write_individual_segment(stream_id2, std::move(wrapper2.segment()), false);

    const std::string target_id("some_merged_stuff");
    ReadOptions read_options;
    read_options.set_allow_sparse(true);
    pipelines::ReadQuery read_query;
    auto read_result = test_store_->read_dataframe_merged(target_id, {stream_id1, stream_id2}, pipelines::VersionQuery{}, read_query, read_options);
    const auto& frame = read_result.frame_data.frame();

    for(timestamp i = 0; i < NumTests; i += 2) {
        check_value(frame.scalar_at<uint64_t>(i, 2).value(), uint64_t(i));
        check_value(frame.scalar_at<uint64_t>(i, 3).value(), uint64_t(i + 1));
        check_value(frame.scalar_at<uint64_t>(i, 4).value(), uint64_t(0));
        check_value(frame.scalar_at<uint64_t>(i + 1, 2).value(), uint64_t(i + 1));
        check_value(frame.scalar_at<uint64_t>(i + 1, 3).value(), uint64_t(0));
        check_value(frame.scalar_at<uint64_t>(i + 1, 4).value(), uint64_t(i + 2));
    }
}

TEST_F(MergeReadsTestStore, SparseSource) {
    using namespace arcticdb;
    using namespace arcticdb::stream;
    using DynamicAggregator =  Aggregator<TimeseriesIndex, DynamicSchema, stream::NeverSegmentPolicy, stream::SparseColumnPolicy>;
    using DynamicSinkWrapper = SinkWrapperImpl<DynamicAggregator>;
    const auto NumTests = 12;

    const std::string stream_id1("test_merge_1");
    const std::string stream_id2("test_merge_2");

    DynamicSinkWrapper wrapper1(stream_id1, {});
    auto& aggregator1 = wrapper1.aggregator_;
    DynamicSinkWrapper wrapper2(stream_id2, {});
    auto& aggregator2 = wrapper2.aggregator_;

    for(timestamp i = 0; i < NumTests; i += 4) {
        aggregator1.start_row(i)([&] (auto&& rb) {
            rb.set_scalar_by_name("first", int64_t(i), DataType::INT64);
            rb.set_scalar_by_name("second", int64_t(i + 1), DataType::INT64);
        });

        aggregator2.start_row(i + 1)([&] (auto&& rb) {
            rb.set_scalar_by_name("first", int64_t(i + 1), DataType::INT64);
            rb.set_scalar_by_name("third", int64_t(i + 2), DataType::INT64);
        });

        aggregator1.start_row(i + 2)([&] (auto&& rb) {
            rb.set_scalar_by_name("first", int64_t(i + 2), DataType::INT64);
            rb.set_scalar_by_name("third", int64_t(i + 3), DataType::INT64);
        });

        aggregator2.start_row(i + 3)([&] (auto&& rb) {
            rb.set_scalar_by_name("first", int64_t(i + 3), DataType::INT64);
            rb.set_scalar_by_name("fourth", int64_t(i + 4), DataType::INT64);
        });
    }

    aggregator1.commit();
    aggregator2.commit();

    test_store_->write_individual_segment(stream_id1, std::move(wrapper1.segment()), false);
    test_store_->write_individual_segment(stream_id2, std::move(wrapper2.segment()), false);

    const std::string target_id("some_merged_stuff");
    ReadOptions read_options;
    read_options.set_allow_sparse(true);
    pipelines::ReadQuery read_query;
    auto read_result = test_store_->read_dataframe_merged(target_id, {stream_id1, stream_id2}, pipelines::VersionQuery{}, read_query, read_options);
    const auto& frame = read_result.frame_data.frame();

    for(timestamp i = 0; i < NumTests; i += 4) {
        check_value(frame.scalar_at<uint64_t>(i, 2).value(), uint64_t(i));
        check_value(frame.scalar_at<uint64_t>(i, 3).value(), uint64_t(i + 1));
        check_value(frame.scalar_at<uint64_t>(i, 4).value(), uint64_t(0));
        check_value(frame.scalar_at<uint64_t>(i, 5).value(), uint64_t(0));

        check_value(frame.scalar_at<uint64_t>(i + 1, 2).value(), uint64_t(i + 1));
        check_value(frame.scalar_at<uint64_t>(i + 1, 3).value(), uint64_t(0));
        check_value(frame.scalar_at<uint64_t>(i + 1, 4).value(), uint64_t(i + 2));
        check_value(frame.scalar_at<uint64_t>(i + 1, 5).value(), uint64_t(0));

        check_value(frame.scalar_at<uint64_t>(i + 2, 2).value(), uint64_t(i + 2));
        check_value(frame.scalar_at<uint64_t>(i + 2, 3).value(), uint64_t(0));
        check_value(frame.scalar_at<uint64_t>(i + 2, 4).value(), uint64_t(i + 3));
        check_value(frame.scalar_at<uint64_t>(i + 2, 5).value(), uint64_t(0));

        check_value(frame.scalar_at<uint64_t>(i + 3, 2).value(), uint64_t(i + 3));
        check_value(frame.scalar_at<uint64_t>(i + 3, 3).value(), uint64_t(0));
        check_value(frame.scalar_at<uint64_t>(i + 3, 4).value(), uint64_t(0));
        check_value(frame.scalar_at<uint64_t>(i + 3, 5).value(), uint64_t(i + 4));
    }
}