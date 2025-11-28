/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <gtest/gtest.h>

#include <folly/futures/Future.h>
#include <arcticdb/util/preconditions.hpp>
#include <unordered_map>
#include <string>
#include <arcticdb/async/task_scheduler.hpp>
#include <arcticdb/util/test/generators.hpp>
#include <variant>
#include <arcticdb/stream/test/stream_test_common.hpp>

namespace arcticdb {

using PipelineValue = SegmentInMemory;
using PipelineFunction = folly::Function<PipelineValue(PipelineValue)>;

struct PipelineStage {
    PipelineFunction func_;

    explicit PipelineStage(PipelineFunction&& func) : func_(std::move(func)) {}

    ARCTICDB_MOVE_ONLY_DEFAULT(PipelineStage)
};

class Pipeline {
  private:
    std::shared_ptr<folly::CPUThreadPoolExecutor> executor_;
    std::vector<PipelineStage> stages_;
    folly::PromiseContract<PipelineValue> chain_;

  public:
    explicit Pipeline(const std::shared_ptr<folly::CPUThreadPoolExecutor>& executor) :
        executor_(executor),
        chain_(folly::makePromiseContract<PipelineValue>(executor_.get())) {}

    void add(PipelineStage&& stage) { stages_.emplace_back(std::move(stage)); }

    ARCTICDB_NO_MOVE_OR_COPY(Pipeline)

    auto finalize() {
        for (auto& stage : stages_)
            chain_.future = std::move(chain_.future).thenValue(std::move(stage.func_));

        stages_.clear();
    }

    auto run(PipelineValue&& val) {
        chain_.promise.setValue(std::move(val));
        return std::move(chain_.future);
    }
};

struct TestFilter {
    using FilterFunction = folly::Function<bool(const SegmentInMemory::Row&)>;
    FilterFunction filter_func_;

    explicit TestFilter(FilterFunction&& func) : filter_func_(std::move(func)) {}

    SegmentInMemory operator()(SegmentInMemory input) {
        SegmentInMemory output{input.descriptor()};
        for (const auto& row : input) {
            if (filter_func_(row))
                output.push_back(row);
        }
        return output;
    }
};

template<class TDT>
struct TestProjection {
    using RawType = typename TDT::DataTypeTag::raw_type;
    using ProjectionFunction = folly::Function<RawType(const SegmentInMemory::Row&)>;
    std::string field_name_;
    ProjectionFunction projection_func_;

    TestProjection(std::string field_name, ProjectionFunction&& func) :
        field_name_(std::move(field_name)),
        projection_func_(std::move(func)) {}

    SegmentInMemory operator()(SegmentInMemory segment) {
        auto desc = segment.descriptor();
        auto fd = scalar_field(TDT::DataTypeTag::data_type, field_name_);
        desc.add_field(fd);
        auto col_index = segment.add_column(fd, 0, AllocationType::DYNAMIC);
        auto& column = segment.column(col_index);
        for (auto&& row : folly::enumerate(segment)) {
            column.set_scalar(row.index, projection_func_(*row));
        }
        return segment;
    }
};

template<class TDT>
struct TestAggregation {
    using RawType = typename TDT::DataTypeTag::raw_type;
    using AggregationFunction = folly::Function<std::optional<RawType>(const SegmentInMemoryImpl::Location&)>;
    std::string field_name_;
    AggregationFunction aggregation_func_;

    TestAggregation(std::string field_name, AggregationFunction&& func) :
        field_name_(std::move(field_name)),
        aggregation_func_(std::move(func)) {}

    SegmentInMemory operator()(SegmentInMemory input) {
        const auto& input_desc = input.descriptor();
        auto index_field_count = input_desc.index().field_count();
        StreamDescriptor desc{input_desc.id(), input_desc.index()};
        for (auto i = 0u; i < index_field_count; ++i) {
            const auto& field = input_desc.fields(i);
            desc.add_field(FieldRef{field.type(), field.name()});
        }

        const auto& agg_field_pos = input.descriptor().find_field(field_name_);
        if (!agg_field_pos)
            util::raise_rte("Field {} not found in aggregation", field_name_);

        const auto& agg_field = input.descriptor().field(agg_field_pos.value());
        if (std::find_if(desc.fields().begin(), desc.fields().end(), [&agg_field](const auto& field) {
                return agg_field == field;
            }) == desc.fields().end())
            desc.add_field(FieldRef{agg_field.type(), agg_field.name()});

        SegmentInMemory output{StreamDescriptor{std::move(desc)}};
        for (const auto& row : input) {
            if (auto maybe_val = aggregation_func_(row[agg_field_pos.value()])) {
                auto col_num = size_t(0);
                for (auto i = 0u; i < index_field_count; ++i)
                    output.set_value(col_num++, row[i]);

                output.set_scalar(col_num, maybe_val.value());
                output.end_row();
            }
        }
        return output;
    }
};

} // namespace arcticdb

TEST(Pipeline, Basic) {
    using namespace arcticdb;
    auto ex = std::make_shared<folly::CPUThreadPoolExecutor>(5);

    SegmentsSink sink;
    auto commit_func = [&](SegmentInMemory&& mem) { sink.segments_.push_back(std::move(mem)); };

    auto agg = get_test_aggregator(std::move(commit_func), "test", {scalar_field(DataType::UINT64, "uint64")});

    const size_t NumTests = 100;

    for (timestamp i = 0; i < timestamp(NumTests); ++i) {
        agg.start_row(i)([&](auto& rb) { rb.set_scalar(1, i * 3); });
    }

    agg.commit();
    auto& seg = sink.segments_[0];

    ASSERT_EQ(seg.row_count(), 100);

    Pipeline pipeline(ex);
    TestFilter even_filter{[](const SegmentInMemory::Row& row) {
        return row[0].visit([](auto val) {
            if constexpr (std::is_same_v<bool, decltype(val)>)
                return val == false;
            else if constexpr (std::is_integral_v<decltype(val)>)
                return val % 2 == 0;
            else
                return false;
        });
    }};

    using TypeDescriptor = TypeDescriptorTag<DataTypeTag<DataType::INT32>, DimensionTag<Dimension::Dim0>>;
    TestProjection<TypeDescriptor> double_it{"doubled", [](const SegmentInMemory::Row& row) {
                                                 return row[0].visit([](auto val) {
                                                     return static_cast<int32_t>(val * 2);
                                                 });
                                             }};

    uint32_t count = 0;
    uint32_t sum = 0;
    TestAggregation<TypeDescriptor> sum_of_10{
            "doubled",
            [&](const SegmentInMemoryImpl::Location& loc) -> std::optional<int32_t> {
                sum += loc.value<int32_t>();
                if (++count % 10 == 0) {
                    auto ret = sum;
                    sum = 0;
                    return ret;
                } else {
                    return std::nullopt;
                }
            }
    };

    pipeline.add(PipelineStage{std::move(even_filter)});
    pipeline.add(PipelineStage(std::move(double_it)));
    pipeline.add(PipelineStage{std::move(sum_of_10)});
    pipeline.finalize();
    auto fut = pipeline.run(std::move(seg));
    auto output = std::move(fut).get();
    ASSERT_EQ(output.row_count(), 5);
}
