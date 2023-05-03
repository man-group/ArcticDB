/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/entity/key.hpp>
#include <arcticdb/column_store/column.hpp>
#include <arcticdb/pipeline/value.hpp>
#include <arcticdb/processing/expression_context.hpp>
#include <arcticdb/processing/expression_node.hpp>
#include <arcticdb/processing/processing_segment.hpp>
#include <arcticdb/entity/types.hpp>
#include <arcticdb/processing/aggregation.hpp>
#include <arcticdb/processing/grouper.hpp>
#include <arcticdb/stream/aggregator.hpp>
#include <arcticdb/util/movable_priority_queue.hpp>
#include <arcticdb/stream/merge.hpp>
#include <arcticdb/pipeline/index_utils.hpp>
#include <arcticdb/util/composite.hpp>

#include <folly/Poly.h>
#include <folly/futures/Future.h>

#include <vector>
#include <unordered_map>
#include <string>
#include <variant>
#include <memory>
#include <atomic>

namespace arcticdb {

using NormMetaDescriptor = std::shared_ptr<arcticdb::proto::descriptors::NormalizationMetadata>;


struct IClause {
    template<class Base>
    struct Interface : Base {
        [[nodiscard]] Composite<ProcessingSegment>
        process(std::shared_ptr<Store> store, Composite<ProcessingSegment> &&segs) const {
            return std::move(folly::poly_call<0>(*this, store, std::move(segs)));
        }

        [[nodiscard]] std::optional<std::vector<Composite<ProcessingSegment>>> repartition(
                [[maybe_unused]] std::vector<Composite<ProcessingSegment>> &&comps) const {
            return folly::poly_call<1>(*this, std::move(comps));
        }

        [[nodiscard]] bool requires_repartition() const { return folly::poly_call<2>(*this); }

        [[nodiscard]] bool can_combine_with_column_selection() const { return folly::poly_call<3>(*this); }

        [[nodiscard]] std::optional<std::unordered_set<std::string>> input_columns() const { return folly::poly_call<4>(*this); }

        // Returns the name of the index after this clause if it has been modified, std::nullopt otherwise
        [[nodiscard]] std::optional<std::string> new_index() const { return folly::poly_call<5>(*this); }
    };

    template<class T>
    using Members = folly::PolyMembers<
            &T::process,
            &T::repartition,
            &T::requires_repartition,
            &T::can_combine_with_column_selection,
            &T::input_columns,
            &T::new_index>;
};

using Clause = folly::Poly<IClause>;

std::vector<Composite<ProcessingSegment>> single_partition(std::vector<Composite<ProcessingSegment>> &&segs);

struct ColumnStatsGenerationClause {
    std::unordered_set<std::string> input_columns_;
    std::shared_ptr<std::vector<ColumnStatsAggregator>> column_stats_aggregators_;

    explicit ColumnStatsGenerationClause(std::unordered_set<std::string>&& input_columns,
                                         std::shared_ptr<std::vector<ColumnStatsAggregator>> column_stats_aggregators) :
            input_columns_(std::move(input_columns)),
            column_stats_aggregators_(std::move(column_stats_aggregators)) {
    }

    ARCTICDB_MOVE_COPY_DEFAULT(ColumnStatsGenerationClause)

    [[nodiscard]] Composite<ProcessingSegment>
    process(std::shared_ptr<Store> store, Composite<ProcessingSegment> &&p) const;

    [[nodiscard]] std::optional<std::unordered_set<std::string>> input_columns() const {
        return input_columns_;
    }

    [[nodiscard]] bool requires_repartition() const { return false; }

    [[nodiscard]] std::optional<std::vector<Composite<ProcessingSegment>>>
    repartition([[maybe_unused]] std::vector<Composite<ProcessingSegment>> &&) const { return std::nullopt; }
    [[nodiscard]] bool can_combine_with_column_selection() const { return false; }
    [[nodiscard]] std::optional<std::string> new_index() const { return std::nullopt; }
};

struct FilterClause {
    std::unordered_set<std::string> input_columns_;
    std::shared_ptr<ExpressionContext> expression_context_;
    PipelineOptimisation optimisation_;

    explicit FilterClause(std::unordered_set<std::string> input_columns,
                          ExpressionContext expression_context,
                          std::optional<PipelineOptimisation> optimisation) :
        input_columns_(std::move(input_columns)),
        expression_context_(std::make_shared<ExpressionContext>(std::move(expression_context))),
        optimisation_(optimisation.value_or(PipelineOptimisation::SPEED)) {}

    FilterClause() = delete;

    ARCTICDB_MOVE_COPY_DEFAULT(FilterClause)

    [[nodiscard]] Composite<ProcessingSegment>
    process(std::shared_ptr<Store> store, Composite<ProcessingSegment> &&p) const;

    [[nodiscard]] std::optional<std::unordered_set<std::string>> input_columns() const {
        return input_columns_;
    }

    [[nodiscard]] bool requires_repartition() const { return false; }

    [[nodiscard]] std::optional<std::vector<Composite<ProcessingSegment>>>
    repartition([[maybe_unused]] std::vector<Composite<ProcessingSegment>> &&) const { return std::nullopt; }
    [[nodiscard]] bool can_combine_with_column_selection() const { return true; }
    [[nodiscard]] std::optional<std::string> new_index() const { return std::nullopt; }
    [[nodiscard]] std::string to_string() const;
    void set_pipeline_optimisation(PipelineOptimisation pipeline_optimisation) {
        optimisation_ = pipeline_optimisation;
    }
};

/**
 * Hacky short-cut to reduce memory usage for head(). Very similar to how FilterClause above works.
 * Limitations:
 * - Must be the only Clause (otherwise may return wrong results)
 * - If the n is so big that multiple RowRanges/ProcessingSegments are passed in then much more state tracking is
 * required than what is reasonable for a stop-gap, so this optimisation just disables itself.
 */
struct RowNumberLimitClause {
    size_t n;

    explicit RowNumberLimitClause(size_t n) : n(n) {}

    ARCTICDB_MOVE_COPY_DEFAULT(RowNumberLimitClause)

    [[nodiscard]] Composite<ProcessingSegment>
    process(std::shared_ptr<Store> store, Composite<ProcessingSegment> &&p) const {
        auto procs = std::move(p);
        procs.broadcast([&store, this](ProcessingSegment &proc) {
            auto row_range = proc.data()[0].slice_.row_range;
            if (row_range.start() == 0ULL && row_range.end() > n) {
                util::BitSet bv(static_cast<util::BitSet::size_type>(row_range.diff()));
                bv.set_range(0, bv_size(n));
                proc.apply_filter(bv, store, PipelineOptimisation::MEMORY);
            } else if (!warning_shown) {
                warning_shown = true;
                log::version().info("RowNumberLimitFilter bypassed because rows.start() == {}", row_range.start());
            }
        });
        return procs;
    }

    [[nodiscard]] std::optional<std::unordered_set<std::string>> input_columns() const {
        return std::nullopt;
    }

    mutable bool warning_shown = false; // folly::Poly can't deal with atomic_bool
    [[nodiscard]] bool requires_repartition() const { return false; }

    [[nodiscard]] std::optional<std::vector<Composite<ProcessingSegment>>>
    repartition([[maybe_unused]] std::vector<Composite<ProcessingSegment>> &&) const { return std::nullopt; }
    [[nodiscard]] bool can_combine_with_column_selection() const { return true; }
    [[nodiscard]] std::optional<std::string> new_index() const { return std::nullopt; }
};

template<typename GrouperType, typename BucketizerType>
struct PartitionClause {
    std::string grouping_column_;

    explicit PartitionClause(const std::string& grouping_column) :
            grouping_column_(grouping_column) {
    }
    PartitionClause() = delete;

    ARCTICDB_MOVE_COPY_DEFAULT(PartitionClause)

    [[nodiscard]] Composite<ProcessingSegment>
    process(std::shared_ptr<Store> store, Composite<ProcessingSegment> &&p) const {
        Composite<ProcessingSegment> output;

        auto procs = std::move(p);
        procs.broadcast([&output, &store, &grouping_column = grouping_column_](auto &proc) {
            // TODO (AN-469): I would push all of the logic between here and calling partition_processing_segment down
            // into partition_processing_segment, just passing the column name to group on, processing segment, and
            // store in
            auto partitioning_column = proc.get(ColumnName(grouping_column), store);
            if (std::holds_alternative<ColumnWithStrings>(partitioning_column)) {
                ColumnWithStrings col = std::get<ColumnWithStrings>(partitioning_column);

                col.column_->type().visit_tag([&output, &proc, &col, &store](auto type_desc_tag) {
                    using TypeDescriptorTag = decltype(type_desc_tag);

                    using ResolvedGrouperType = typename GrouperType::template Grouper<TypeDescriptorTag>;

                    auto grouper = std::make_shared<ResolvedGrouperType>(ResolvedGrouperType());
                    // TODO (AN-469): We should put some thought into how to pick an appropriate value for num_buckets
                    auto num_cores = std::thread::hardware_concurrency() == 0 ? 16 : std::thread::hardware_concurrency();
                    auto num_buckets = ConfigsMap::instance()->get_int("Partition.NumBuckets", num_cores);
                    auto bucketizer = std::make_shared<BucketizerType>(num_buckets);

                    output.push_back(
                            partition_processing_segment<ResolvedGrouperType, BucketizerType>(proc, col, store, grouper, bucketizer));
                });
            } else {
                util::raise_rte("Expected single column from expression");
            }
        });

        return output;
    }

    [[nodiscard]] std::optional<std::unordered_set<std::string>> input_columns() const {
        return std::unordered_set<std::string>({grouping_column_});
    }

    [[nodiscard]] bool requires_repartition() const { return true; }

    [[nodiscard]] std::optional<std::vector<Composite<ProcessingSegment>>>
    repartition([[maybe_unused]] std::vector<Composite<ProcessingSegment>> &&c) const {
        auto comps = std::move(c);
        std::unordered_map<size_t, Composite<ProcessingSegment>> partition_map;

        for (auto &comp : comps) {
            comp.broadcast([&partition_map](auto &proc) {
                auto bucket_id = proc.get_bucket();

                if (partition_map.find(bucket_id) == partition_map.end()) {
                    partition_map[bucket_id] = Composite<ProcessingSegment>(std::move(proc));
                } else {
                    partition_map[bucket_id].push_back(std::move(proc));
                }
            });
        }

        std::vector<Composite<ProcessingSegment>> ret;
        for (auto &[key, value] : partition_map) {
            ret.push_back(std::move(value));
        }

        return ret;
    }
    [[nodiscard]] bool can_combine_with_column_selection() const { return true; }
    [[nodiscard]] std::optional<std::string> new_index() const { return std::nullopt; }
    [[nodiscard]] std::string to_string() const {
        return fmt::format("GROUPBY Column[\"{}\"]", grouping_column_);
    }
};

struct AggregationClause {
    std::string grouping_column_;
    std::unordered_set<std::string> input_columns_;
    std::unordered_map<std::string, std::string> aggregation_map_;
    std::vector<GroupingAggregator> aggregators_;

    AggregationClause() = delete;

    ARCTICDB_MOVE_COPY_DEFAULT(AggregationClause)

    AggregationClause(const std::string& grouping_column, const std::unordered_map<std::string, std::string>& aggregations):
            grouping_column_(grouping_column),
            aggregation_map_(aggregations) {
        for (const auto& [column_name, aggregation_operator]: aggregations) {
            auto [_, inserted] = input_columns_.insert(column_name);
            user_input::check<ErrorCode::E_INVALID_USER_ARGUMENT>(inserted,
                                                                  "Cannot perform two aggregations over the same column: {}",
                                                                  column_name);
            auto typed_column_name = ColumnName(column_name);
            if (aggregation_operator == "sum") {
                aggregators_.emplace_back(SumAggregator(typed_column_name, typed_column_name));
            } else if (aggregation_operator == "mean") {
                aggregators_.emplace_back(MeanAggregator(typed_column_name, typed_column_name));
            } else if (aggregation_operator == "max") {
                aggregators_.emplace_back(MaxOrMinAggregator(typed_column_name, typed_column_name, Extremum::MAX));
            } else if (aggregation_operator == "min") {
                aggregators_.emplace_back(MaxOrMinAggregator(typed_column_name, typed_column_name, Extremum::MIN));
            } else {
                user_input::raise<ErrorCode::E_INVALID_USER_ARGUMENT>("Unknown aggregation operator provided: {}", aggregation_operator);
            }
        }
    }

    [[nodiscard]] Composite<ProcessingSegment>
    process(std::shared_ptr<Store> store, Composite<ProcessingSegment> &&p) const;

    [[nodiscard]] std::optional<std::unordered_set<std::string>> input_columns() const {
        return input_columns_;
    }

    [[nodiscard]] bool requires_repartition() const { return false; }

    [[nodiscard]] std::optional<std::vector<Composite<ProcessingSegment>>>
    repartition([[maybe_unused]] std::vector<Composite<ProcessingSegment>> &&comps) const { return std::nullopt; }
    [[nodiscard]] bool can_combine_with_column_selection() const { return false; }
    [[nodiscard]] std::optional<std::string> new_index() const { return grouping_column_; }
    [[nodiscard]] std::string to_string() const;
};

struct PassthroughClause {
    PassthroughClause() = default;
    ARCTICDB_MOVE_COPY_DEFAULT(PassthroughClause)

    [[nodiscard]] Composite<ProcessingSegment>
    process([[maybe_unused]] const std::shared_ptr<Store> &store, Composite<ProcessingSegment> &&p) const {
        auto procs = std::move(p);
        return procs;
    }

    [[nodiscard]] std::optional<std::unordered_set<std::string>> input_columns() const {
        return std::nullopt;
    }

    [[nodiscard]] std::optional<std::vector<Composite<ProcessingSegment>>>
    repartition([[maybe_unused]] std::vector<Composite<ProcessingSegment>> &&comps) const { return std::nullopt; }

    [[nodiscard]] bool requires_repartition() const { return false; }
    [[nodiscard]] bool can_combine_with_column_selection() const { return true; }
    [[nodiscard]] std::optional<std::string> new_index() const { return std::nullopt; }
};

struct SliceAndKeyWrapper {
    pipelines::SliceAndKey seg_;
    std::shared_ptr<Store> store_;
    SegmentInMemory::iterator it_;
    const StreamId id_;

    explicit SliceAndKeyWrapper(pipelines::SliceAndKey &&seg, std::shared_ptr<Store> store) :
            seg_(std::move(seg)),
            store_(std::move(store)),
            it_(seg_.segment(store_).begin()),
            id_(seg_.segment(store_).descriptor().id()) {
    }

    bool advance() {
        return ++it_ != seg_.segment(store_).end();
    }

    SegmentInMemory::Row &row() {
        return *it_;
    }

    const StreamId &id() const {
        return id_;
    }
};

struct MergeClause {
    stream::Index index_;
    stream::VariantColumnPolicy density_policy_;
    StreamId stream_id_;
    bool add_symbol_column_ = false;
    StreamId target_id_;
    StreamDescriptor stream_descriptor_;

    MergeClause(
            stream::Index index,
            const stream::VariantColumnPolicy &density_policy,
            const StreamId& stream_id,
            const StreamDescriptor& stream_descriptor) :
            index_(std::move(index)),
            density_policy_(density_policy),
            stream_id_(stream_id),
            stream_descriptor_(stream_descriptor) {
    }

    [[nodiscard]] Composite<ProcessingSegment>
    process(std::shared_ptr<Store> store, Composite<ProcessingSegment> &&p) const;

    [[nodiscard]] std::optional<std::unordered_set<std::string>> input_columns() const {
        return std::nullopt;
    }

    [[nodiscard]] std::optional<std::vector<Composite<ProcessingSegment>>>

    repartition([[maybe_unused]] std::vector<Composite<ProcessingSegment>> &&comps) const {
        return single_partition(std::move(comps));
    }

    [[nodiscard]] bool requires_repartition() const { return true; }
    [[nodiscard]] bool can_combine_with_column_selection() const { return true; }
    [[nodiscard]] std::optional<std::string> new_index() const { return std::nullopt; }
};

struct SplitClause {
    const size_t rows_;

    explicit SplitClause(size_t rows) :
            rows_(rows) {
    }

    [[nodiscard]] Composite<ProcessingSegment>
    process(std::shared_ptr<Store> store, Composite<ProcessingSegment> &&procs) const {
        using namespace arcticdb::pipelines;

        auto proc_composite = std::move(procs);
        Composite<ProcessingSegment> ret;
        proc_composite.broadcast([&store, rows = rows_, &ret](auto &&p) {
            auto proc = std::forward<decltype(p)>(p);
            auto slice_and_keys = proc.data();
            for (auto &slice_and_key: slice_and_keys) {
                auto split_segs = slice_and_key.segment(store).split(rows);
                const ColRange col_range{slice_and_key.slice().col_range};
                size_t start_row = slice_and_key.slice().row_range.start();
                size_t end_row = 0;
                for (auto &item : split_segs) {
                    end_row = start_row + item.row_count();
                    const RowRange row_range{start_row, end_row};
                    ret.push_back(ProcessingSegment{std::move(item), FrameSlice{col_range, row_range}, proc.dynamic_schema_});
                    start_row = end_row;
                }
            }
        });
        return ret;
    }

    [[nodiscard]] std::optional<std::unordered_set<std::string>> input_columns() const {
        return std::nullopt;
    }

    [[nodiscard]] std::optional<std::vector<Composite<ProcessingSegment>>>
    repartition([[maybe_unused]] std::vector<Composite<ProcessingSegment>> &&comps) const { return std::nullopt; }

    [[nodiscard]] bool requires_repartition() const { return false; }
    [[nodiscard]] bool can_combine_with_column_selection() const { return true; }
    [[nodiscard]] std::optional<std::string> new_index() const { return std::nullopt; }
};

struct SortClause {
    const std::string column_;

    explicit SortClause(std::string column) :
            column_(std::move(column)) {
    }

    [[nodiscard]] Composite<ProcessingSegment>
    process(std::shared_ptr<Store> store, Composite<ProcessingSegment> &&p) const {
        auto procs = std::move(p);
        procs.broadcast([&store, &column = column_](auto &proc) {
            auto slice_and_keys = proc.data();
            for (auto &slice_and_key: slice_and_keys) {
                slice_and_key.segment(store).sort(column);
            }
        });
        return procs;
    }

    [[nodiscard]] std::optional<std::unordered_set<std::string>> input_columns() const {
        return std::nullopt;
    }

    [[nodiscard]] std::optional<std::vector<Composite<ProcessingSegment>>>
    repartition([[maybe_unused]] std::vector<Composite<ProcessingSegment>> &&comps) const { return std::nullopt; }

    [[nodiscard]] bool requires_repartition() const { return false; }
    [[nodiscard]] bool can_combine_with_column_selection() const { return true; }
    [[nodiscard]] std::optional<std::string> new_index() const { return std::nullopt; }
};

struct ProjectClause {
    std::unordered_set<std::string> input_columns_;
    std::string output_column_;
    std::shared_ptr<ExpressionContext> expression_context_;

    explicit ProjectClause(std::unordered_set<std::string> input_columns,
                           std::string output_column,
                           ExpressionContext expression_context) :
            input_columns_(input_columns),
            output_column_(output_column),
            expression_context_(std::make_shared<ExpressionContext>(std::move(expression_context))) {
    }

    ProjectClause() = delete;

    ARCTICDB_MOVE_COPY_DEFAULT(ProjectClause)

    [[nodiscard]] Composite<ProcessingSegment>
    process(std::shared_ptr<Store> store, Composite<ProcessingSegment> &&p) const;

    [[nodiscard]] std::optional<std::unordered_set<std::string>> input_columns() const {
        return input_columns_;
    }

    [[nodiscard]] std::optional<std::vector<Composite<ProcessingSegment>>>
    repartition([[maybe_unused]] std::vector<Composite<ProcessingSegment>> &&comps) const { return std::nullopt; }

    [[nodiscard]] bool requires_repartition() const { return false; }
    [[nodiscard]] bool can_combine_with_column_selection() const { return true; }
    [[nodiscard]] std::optional<std::string> new_index() const { return std::nullopt; }
    [[nodiscard]] std::string to_string() const;
};

struct RemoveColumnPartitioningClause {
    stream::Index index_;
    const bool dedup_rows_;
    const arcticdb::proto::descriptors::IndexDescriptor::Type descriptor_type_;

    RemoveColumnPartitioningClause(
            const stream::Index& index,
            const arcticdb::proto::descriptors::IndexDescriptor::Type descriptor_type = IndexDescriptor::ROWCOUNT,
            bool dedup_rows = false) :
            index_(index),
            dedup_rows_(dedup_rows),
            descriptor_type_(descriptor_type) {
    }

    ARCTICDB_MOVE_COPY_DEFAULT(RemoveColumnPartitioningClause)

    [[nodiscard]] Composite<ProcessingSegment>
    process(std::shared_ptr<Store> store, Composite<ProcessingSegment> &&p) const;

    [[nodiscard]] std::optional<std::unordered_set<std::string>> input_columns() const {
        return std::nullopt;
    }

    mutable bool warning_shown = false; // folly::Poly can't deal with atomic_bool
    [[nodiscard]] bool requires_repartition() const { return false; }

    [[nodiscard]] std::optional<std::vector<Composite<ProcessingSegment>>>
    repartition([[maybe_unused]] std::vector<Composite<ProcessingSegment>> &&) const { return std::nullopt; }
    [[nodiscard]] bool can_combine_with_column_selection() const { return false; }
    [[nodiscard]] std::optional<std::string> new_index() const { return std::nullopt; }
};

}//namespace arcticdb
