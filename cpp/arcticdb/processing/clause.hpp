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
#include <arcticdb/processing/execution_context.hpp>
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

        [[nodiscard]] std::shared_ptr<ExecutionContext> execution_context() const { return folly::poly_call<3>(*this); }

        [[nodiscard]] bool can_combine_with_column_selection() const { return folly::poly_call<4>(*this); }
    };

    template<class T>
    using Members = folly::PolyMembers<&T::process, &T::repartition, &T::requires_repartition, &T::execution_context, &T::can_combine_with_column_selection>;
};

using Clause = folly::Poly<IClause>;

std::vector<Composite<ProcessingSegment>> single_partition(std::vector<Composite<ProcessingSegment>> &&segs);

struct ColumnStatsGenerationClause {
    std::shared_ptr<ExecutionContext> execution_context_;
    std::shared_ptr<std::vector<ColumnStatsAggregator>> column_stats_aggregators_;

    explicit ColumnStatsGenerationClause(std::shared_ptr<ExecutionContext> execution_context,
                                         std::shared_ptr<std::vector<ColumnStatsAggregator>> column_stats_aggregators) :
            execution_context_(std::move(execution_context)),
            column_stats_aggregators_(std::move(column_stats_aggregators)) {
    }

    [[nodiscard]] Composite<ProcessingSegment>
    process(std::shared_ptr<Store> store, Composite<ProcessingSegment> &&p) const;

    [[nodiscard]] std::shared_ptr<ExecutionContext> execution_context() const { return execution_context_; }

    [[nodiscard]] bool requires_repartition() const { return false; }

    [[nodiscard]] std::optional<std::vector<Composite<ProcessingSegment>>>
    repartition([[maybe_unused]] std::vector<Composite<ProcessingSegment>> &&) const { return std::nullopt; }
    [[nodiscard]] bool can_combine_with_column_selection() const { return false; }
};

struct FilterClause {
    std::shared_ptr<ExecutionContext> execution_context_;

    explicit FilterClause(std::shared_ptr<ExecutionContext> execution_context) :
        execution_context_(std::move(execution_context)) {
    }

    [[nodiscard]] Composite<ProcessingSegment>
    process(std::shared_ptr<Store> store, Composite<ProcessingSegment> &&p) const;

    [[nodiscard]] std::shared_ptr<ExecutionContext> execution_context() const { return execution_context_; }

    [[nodiscard]] bool requires_repartition() const { return false; }

    [[nodiscard]] std::optional<std::vector<Composite<ProcessingSegment>>>
    repartition([[maybe_unused]] std::vector<Composite<ProcessingSegment>> &&) const { return std::nullopt; }
    [[nodiscard]] bool can_combine_with_column_selection() const { return true; }
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

    [[nodiscard]] Composite<ProcessingSegment>
    process(std::shared_ptr<Store> store, Composite<ProcessingSegment> &&p) const {
        auto procs = std::move(p);
        procs.broadcast([&store, this](ProcessingSegment &proc) {
            auto row_range = proc.data()[0].slice_.row_range;
            if (row_range.start() == 0ULL && row_range.end() > n) {
                util::BitSet bv(static_cast<util::BitSet::size_type>(row_range.diff()));
                bv.set_range(0, bv_size(n));
                proc.apply_filter(bv, store);
            } else if (!warning_shown) {
                warning_shown = true;
                log::version().info("RowNumberLimitFilter bypassed because rows.start() == {}", row_range.start());
            }
        });
        return procs;
    }

    [[nodiscard]] std::shared_ptr<ExecutionContext> execution_context() const { return nullptr; }

    mutable bool warning_shown = false; // folly::Poly can't deal with atomic_bool
    [[nodiscard]] bool requires_repartition() const { return false; }

    [[nodiscard]] std::optional<std::vector<Composite<ProcessingSegment>>>
    repartition([[maybe_unused]] std::vector<Composite<ProcessingSegment>> &&) const { return std::nullopt; }
    [[nodiscard]] bool can_combine_with_column_selection() const { return true; }
};

template<typename GrouperType, typename BucketizerType>
struct PartitionClause {
    std::shared_ptr<ExecutionContext> execution_context_;

    explicit PartitionClause(std::shared_ptr<ExecutionContext> execution_context) :
            execution_context_(std::move(execution_context)) {
    }

    [[nodiscard]] Composite<ProcessingSegment>
    process(std::shared_ptr<Store> store, Composite<ProcessingSegment> &&p) const {
        Composite<ProcessingSegment> output;

        auto procs = std::move(p);
        procs.broadcast([&output, &store, &execution_context = execution_context_](auto &proc) {
            proc.set_execution_context(execution_context);
            // TODO (AN-469): I would push all of the logic between here and calling partition_processing_segment down
            // into partition_processing_segment, just passing the column name to group on, processing segment, and
            // store in
            auto partitioning_column = proc.get(ColumnName(execution_context->root_node_name_.value), store);
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

    [[nodiscard]] std::shared_ptr<ExecutionContext> execution_context() const { return execution_context_; }

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
};

inline StreamDescriptor empty_descriptor() {
    return StreamDescriptor{StreamId{"merged"}, IndexDescriptor{0, IndexDescriptor::ROWCOUNT}, {}};
}

struct AggregationClause {
    std::shared_ptr<ExecutionContext> execution_context_;
    std::vector<AggregationFactory> aggregation_operators_;
    std::shared_ptr<std::once_flag> reset_descriptor_ = std::make_shared<std::once_flag>();
    std::shared_ptr<std::once_flag> set_name_index_ = std::make_shared<std::once_flag>();
    AggregationClause() = delete;

    ARCTICDB_MOVE_COPY_DEFAULT(AggregationClause)

    AggregationClause(std::shared_ptr<ExecutionContext> execution_context,
                               std::vector<AggregationFactory> &&aggregation_operators) :
            execution_context_(std::move(execution_context)),
            aggregation_operators_(std::forward<std::vector<AggregationFactory>>(aggregation_operators)) {
    }

    [[nodiscard]] Composite<ProcessingSegment>
    process(std::shared_ptr<Store> store, Composite<ProcessingSegment> &&p) const;

    [[nodiscard]] std::shared_ptr<ExecutionContext> execution_context() const { return execution_context_; }

    [[nodiscard]] bool requires_repartition() const { return false; }

    [[nodiscard]] std::optional<std::vector<Composite<ProcessingSegment>>>
    repartition([[maybe_unused]] std::vector<Composite<ProcessingSegment>> &&comps) const { return std::nullopt; }
    [[nodiscard]] bool can_combine_with_column_selection() const { return false; }
};

struct PassthroughClause {
    [[nodiscard]] Composite<ProcessingSegment>
    process([[maybe_unused]] const std::shared_ptr<Store> &store, Composite<ProcessingSegment> &&p) const {
        auto procs = std::move(p);
        return procs;
    }

    [[nodiscard]] std::shared_ptr<ExecutionContext> execution_context() const { return nullptr; }

    [[nodiscard]] std::optional<std::vector<Composite<ProcessingSegment>>>
    repartition([[maybe_unused]] std::vector<Composite<ProcessingSegment>> &&comps) const { return std::nullopt; }

    [[nodiscard]] bool requires_repartition() const { return false; }
    [[nodiscard]] bool can_combine_with_column_selection() const { return true; }

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
    const std::shared_ptr<ExecutionContext> execution_context_;

    MergeClause(
            stream::Index index,
            const stream::VariantColumnPolicy &density_policy,
            const StreamId& stream_id,
            std::shared_ptr<ExecutionContext> execution_context ) :
            index_(std::move(index)),
            density_policy_(density_policy),
            stream_id_(stream_id),
            execution_context_(std::move(execution_context)) {
    }

    [[nodiscard]] Composite<ProcessingSegment>
    process(std::shared_ptr<Store> store, Composite<ProcessingSegment> &&p) const;

    [[nodiscard]] std::shared_ptr<ExecutionContext> execution_context() const { return nullptr; }

    [[nodiscard]] std::optional<std::vector<Composite<ProcessingSegment>>>

    repartition([[maybe_unused]] std::vector<Composite<ProcessingSegment>> &&comps) const {
        return single_partition(std::move(comps));
    }

    [[nodiscard]] bool requires_repartition() const { return true; }
    [[nodiscard]] bool can_combine_with_column_selection() const { return true; }
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
                    ret.push_back(ProcessingSegment{std::move(item), FrameSlice{col_range, row_range}});
                    start_row = end_row;
                }
            }
        });
        return ret;
    }

    [[nodiscard]] std::shared_ptr<ExecutionContext> execution_context() const { return nullptr; }

    [[nodiscard]] std::optional<std::vector<Composite<ProcessingSegment>>>
    repartition([[maybe_unused]] std::vector<Composite<ProcessingSegment>> &&comps) const { return std::nullopt; }

    [[nodiscard]] bool requires_repartition() const { return false; }
    [[nodiscard]] bool can_combine_with_column_selection() const { return true; }
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

    [[nodiscard]] std::shared_ptr<ExecutionContext> execution_context() const { return nullptr; }

    [[nodiscard]] std::optional<std::vector<Composite<ProcessingSegment>>>
    repartition([[maybe_unused]] std::vector<Composite<ProcessingSegment>> &&comps) const { return std::nullopt; }

    [[nodiscard]] bool requires_repartition() const { return false; }
    [[nodiscard]] bool can_combine_with_column_selection() const { return true; }
};

struct ProjectClause {
    //TODO can't use root node name all the time, do something like this
    //const std::shared_ptr<std::map<std::string, std::string>> projections_;
    std::string column_name_;
    const std::shared_ptr<ExecutionContext> execution_context_;
    std::shared_ptr<std::once_flag> add_column_ = std::make_shared<std::once_flag>();

    //TODO support multiple projections
    //explicit ProjectClause(std::shared_ptr<std::map<std::string, std::string>> projections) :
    explicit ProjectClause(const std::string& column_name, std::shared_ptr<ExecutionContext> execution_context) :
        column_name_(column_name),
        execution_context_(std::move(execution_context)) {
    }

    [[nodiscard]] Composite<ProcessingSegment>
    process(std::shared_ptr<Store> store, Composite<ProcessingSegment> &&p) const;

    [[nodiscard]] std::shared_ptr<ExecutionContext> execution_context() const { return execution_context_; }

    [[nodiscard]] std::optional<std::vector<Composite<ProcessingSegment>>>
    repartition([[maybe_unused]] std::vector<Composite<ProcessingSegment>> &&comps) const { return std::nullopt; }

    [[nodiscard]] bool requires_repartition() const { return false; }
    [[nodiscard]] bool can_combine_with_column_selection() const { return true; }
};

class ClauseBuilder {
private:
    std::vector<Clause> clauses_;
    std::shared_ptr<ExecutionContext> execution_context_;
    std::vector<AggregationFactory> operators_;

public:
    void add_ProjectClause(const std::string& column_name, const std::shared_ptr<ExecutionContext>& ec) {
        clauses_.emplace_back(ProjectClause(column_name, ec));
    }

    void add_FilterClause(const std::shared_ptr<ExecutionContext>& ec) {
        clauses_.emplace_back(FilterClause(ec));
    }

    void prepare_AggregationClause(const std::shared_ptr<ExecutionContext>& ec) {
        execution_context_ = ec;
        if (!operators_.empty()) {
            util::raise_rte("Prior aggregation clause not finalised before adding new aggregation clause!");
        }

        operators_ = std::vector<AggregationFactory>{};
    }

    void add_SumAggregationOperator(ColumnName &&input, ColumnName &&output) {
        operators_.emplace_back(Sum{std::move(input), std::move(output)});
    }

    void add_MeanAggregationOperator(ColumnName &&input, ColumnName &&output) {
        operators_.emplace_back(Mean{std::move(input), std::move(output)});
    }

    void add_MaxAggregationOperator(ColumnName &&input, ColumnName &&output) {
        operators_.emplace_back(MaxOrMin{std::move(input), std::move(output), Extremum::max});
    }

    void add_MinAggregationOperator(ColumnName &&input, ColumnName &&output) {
        operators_.emplace_back(MaxOrMin{std::move(input), std::move(output), Extremum::min});
    }

    void finalize_AggregationClause() {
        // TODO: Complete hack. Two clauses shouldn't share an EC.
        clauses_.emplace_back(
                 PartitionClause<arcticdb::grouping::HashingGroupers, arcticdb::grouping::ModuloBucketizer>{execution_context_});
        clauses_.emplace_back(AggregationClause{execution_context_, std::move(operators_)});
        execution_context_.reset();
    }

    [[nodiscard]] std::vector<Clause> get_clauses() const {
        return clauses_;
    }
};

struct RemoveColumnPartitioningClause {
    std::shared_ptr<ExecutionContext> execution_context_;

    RemoveColumnPartitioningClause(
            std::shared_ptr<ExecutionContext> execution_context ) :
            execution_context_(std::move(execution_context)) {
    }
    [[nodiscard]] Composite<ProcessingSegment>
    process(std::shared_ptr<Store> store, Composite<ProcessingSegment> &&p) const;

    [[nodiscard]] std::shared_ptr<ExecutionContext> execution_context() const { return nullptr; }

    mutable bool warning_shown = false; // folly::Poly can't deal with atomic_bool
    [[nodiscard]] bool requires_repartition() const { return false; }

    [[nodiscard]] std::optional<std::vector<Composite<ProcessingSegment>>>
    repartition([[maybe_unused]] std::vector<Composite<ProcessingSegment>> &&) const { return std::nullopt; }
    [[nodiscard]] bool can_combine_with_column_selection() const { return false; }
};

}//namespace arcticdb
