/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/pipeline/frame_slice.hpp>
#include <arcticdb/processing/expression_context.hpp>
#include <arcticdb/processing/expression_node.hpp>
#include <arcticdb/entity/types.hpp>
#include <arcticdb/processing/clause_utils.hpp>
#include <arcticdb/processing/aggregation_interface.hpp>
#include <arcticdb/processing/processing_unit.hpp>
#include <arcticdb/processing/sorted_aggregation.hpp>
#include <arcticdb/stream/aggregator.hpp>
#include <folly/Poly.h>
#include <arcticdb/pipeline/pipeline_common.hpp>
#include <arcticdb/version/merge_options.hpp>
#include <arcticdb/util/string_utils.hpp>
#include <arcticdb/pipeline/input_frame.hpp>
#include <arcticdb/util/flatten_utils.hpp>
#include <arcticdb/python/gil_lock.hpp>
#include <arcticdb/python/python_to_tensor_frame.hpp>
#include <arcticdb/pipeline/frame_utils.hpp>

#include <optional>
#include <vector>
#include <string>
#include <variant>
#include <memory>

namespace arcticdb {

using ResampleOrigin = std::variant<std::string, timestamp>;

using RangesAndKey = pipelines::RangesAndKey;
using SliceAndKey = pipelines::SliceAndKey;

namespace stream {
struct PartialKey;
} // namespace stream

namespace pipelines {
struct InputFrame;
}

class DeDupMap;

struct IClause {
    template<class Base>
    struct Interface : Base {
        // Reorders ranges_and_keys into the order they should be queued up to be read from storage.
        // Returns a vector where each element is a vector of indexes into ranges_and_keys representing the segments
        // needed for one ProcessingUnit.
        [[nodiscard]] std::vector<std::vector<size_t>> structure_for_processing(
                std::vector<RangesAndKey>& ranges_and_keys
        ) {
            return folly::poly_call<0>(*this, ranges_and_keys);
        }

        [[nodiscard]] std::vector<std::vector<EntityId>> structure_for_processing(
                std::vector<std::vector<EntityId>>&& entity_ids_vec
        ) {
            return folly::poly_call<1>(*this, std::move(entity_ids_vec));
        }

        [[nodiscard]] std::vector<EntityId> process(std::vector<EntityId>&& entity_ids) const {
            return folly::poly_call<2>(*this, std::move(entity_ids));
        }

        [[nodiscard]] const ClauseInfo& clause_info() const { return folly::poly_call<3>(*this); };

        void set_processing_config(const ProcessingConfig& processing_config) {
            folly::poly_call<4>(*this, processing_config);
        }

        void set_component_manager(std::shared_ptr<ComponentManager> component_manager) {
            folly::poly_call<5>(*this, component_manager);
        }

        OutputSchema modify_schema(OutputSchema&& output_schema) const {
            return folly::poly_call<6>(*this, std::move(output_schema));
        }

        OutputSchema join_schemas(std::vector<OutputSchema>&& input_schemas) const {
            return folly::poly_call<7>(*this, std::move(input_schemas));
        }
    };

    template<class T>
    using Members = folly::PolyMembers<
            folly::sig<std::vector<std::vector<size_t>>(std::vector<RangesAndKey>&)>(&T::structure_for_processing),
            folly::sig<std::vector<std::vector<EntityId>>(std::vector<std::vector<EntityId>>&&)>(
                    &T::structure_for_processing
            ),
            &T::process, &T::clause_info, &T::set_processing_config, &T::set_component_manager, &T::modify_schema,
            &T::join_schemas>;
};

using Clause = folly::Poly<IClause>;

template<typename ClauseType>
bool is(const std::shared_ptr<Clause>& clause) {
    return folly::poly_type(*clause) == typeid(ClauseType);
}

void check_column_presence(
        OutputSchema& output_schema, const std::unordered_set<std::string>& required_columns,
        std::string_view clause_name
);

OutputSchema modify_schema(OutputSchema&& schema, const std::vector<std::shared_ptr<Clause>>& clauses);

struct PassthroughClause {
    ClauseInfo clause_info_;

    PassthroughClause() = default;
    ARCTICDB_MOVE_COPY_DEFAULT(PassthroughClause)

    [[nodiscard]] std::vector<std::vector<size_t>> structure_for_processing(std::vector<RangesAndKey>& ranges_and_keys
    ) {
        return structure_by_row_slice(ranges_and_keys); // TODO: No structuring?
    }

    [[nodiscard]] std::vector<std::vector<EntityId>> structure_for_processing(
            std::vector<std::vector<EntityId>>&& entity_ids_vec
    ) {
        return entity_ids_vec; // TODO: structure by row slice?
    }

    [[nodiscard]] std::vector<EntityId> process(std::vector<EntityId>&& entity_ids) const;

    [[nodiscard]] const ClauseInfo& clause_info() const { return clause_info_; }

    void set_processing_config(ARCTICDB_UNUSED const ProcessingConfig&) {}

    void set_component_manager(ARCTICDB_UNUSED std::shared_ptr<ComponentManager>) {}

    OutputSchema modify_schema(OutputSchema&& output_schema) const { return output_schema; }

    OutputSchema join_schemas(std::vector<OutputSchema>&&) const {
        util::raise_rte("PassThroughClause::join_schemas should never be called");
    }
};

struct FilterClause {
    ClauseInfo clause_info_;
    std::shared_ptr<ComponentManager> component_manager_;
    std::shared_ptr<ExpressionContext> expression_context_;
    PipelineOptimisation optimisation_;

    explicit FilterClause(
            std::unordered_set<std::string> input_columns, ExpressionContext expression_context,
            std::optional<PipelineOptimisation> optimisation
    ) :
        expression_context_(std::make_shared<ExpressionContext>(std::move(expression_context))),
        optimisation_(optimisation.value_or(PipelineOptimisation::SPEED)) {
        user_input::check<ErrorCode::E_INVALID_USER_ARGUMENT>(
                expression_context_->root_ && expression_context_->root_->is_operation(),
                "FilterClause AST would produce a column, not a bitset"
        );
        clause_info_.input_columns_ = std::move(input_columns);
    }

    FilterClause() = delete;

    ARCTICDB_MOVE_COPY_DEFAULT(FilterClause)

    [[nodiscard]] std::vector<std::vector<size_t>> structure_for_processing(std::vector<RangesAndKey>& ranges_and_keys
    ) {
        return structure_by_row_slice(ranges_and_keys);
    }

    [[nodiscard]] std::vector<std::vector<EntityId>> structure_for_processing(
            std::vector<std::vector<EntityId>>&& entity_ids_vec
    ) {
        return structure_by_row_slice(*component_manager_, std::move(entity_ids_vec));
    }

    [[nodiscard]] std::vector<EntityId> process(std::vector<EntityId>&& entity_ids) const;

    [[nodiscard]] const ClauseInfo& clause_info() const { return clause_info_; }

    void set_processing_config(const ProcessingConfig& processing_config) {
        expression_context_->dynamic_schema_ = processing_config.dynamic_schema_;
    }

    void set_component_manager(std::shared_ptr<ComponentManager> component_manager) {
        component_manager_ = component_manager;
    }

    OutputSchema modify_schema(OutputSchema&& output_schema) const;

    OutputSchema join_schemas(std::vector<OutputSchema>&&) const {
        util::raise_rte("FilterClause::join_schemas should never be called");
    }

    [[nodiscard]] std::string to_string() const;

    void set_pipeline_optimisation(PipelineOptimisation pipeline_optimisation) {
        optimisation_ = pipeline_optimisation;
    }
};

struct ProjectClause {
    ClauseInfo clause_info_;
    std::shared_ptr<ComponentManager> component_manager_;
    std::string output_column_;
    std::shared_ptr<ExpressionContext> expression_context_;

    explicit ProjectClause(
            std::unordered_set<std::string> input_columns, std::string output_column,
            ExpressionContext expression_context
    ) :
        output_column_(std::move(output_column)),
        expression_context_(std::make_shared<ExpressionContext>(std::move(expression_context))) {
        user_input::check<ErrorCode::E_INVALID_USER_ARGUMENT>(
                expression_context_->root_ &&
                        (expression_context_->root_->is_operation() || expression_context_->root_->is_value()),
                "ProjectClause AST would not produce a column"
        );
        clause_info_.input_columns_ = std::move(input_columns);
    }

    ProjectClause() = delete;

    ARCTICDB_MOVE_COPY_DEFAULT(ProjectClause)

    [[nodiscard]] std::vector<std::vector<size_t>> structure_for_processing(std::vector<RangesAndKey>& ranges_and_keys
    ) {
        return structure_by_row_slice(ranges_and_keys);
    }

    [[nodiscard]] std::vector<std::vector<EntityId>> structure_for_processing(
            std::vector<std::vector<EntityId>>&& entity_ids_vec
    ) {
        return structure_by_row_slice(*component_manager_, std::move(entity_ids_vec));
    }

    [[nodiscard]] std::vector<EntityId> process(std::vector<EntityId>&& entity_ids) const;

    [[nodiscard]] const ClauseInfo& clause_info() const { return clause_info_; }

    void set_processing_config(const ProcessingConfig& processing_config) {
        expression_context_->dynamic_schema_ = processing_config.dynamic_schema_;
    }

    void set_component_manager(std::shared_ptr<ComponentManager> component_manager) {
        component_manager_ = component_manager;
    }

    OutputSchema modify_schema(OutputSchema&& output_schema) const;

    OutputSchema join_schemas(std::vector<OutputSchema>&&) const {
        util::raise_rte("ProjectClause::join_schemas should never be called");
    }

    [[nodiscard]] std::string to_string() const;

  private:
    void add_column(ProcessingUnit& proc, const ColumnWithStrings& col) const;
};

template<typename GrouperType, typename BucketizerType>
struct PartitionClause {
    ClauseInfo clause_info_;
    std::shared_ptr<ComponentManager> component_manager_;
    ProcessingConfig processing_config_;
    std::string grouping_column_;

    explicit PartitionClause(const std::string& grouping_column) : grouping_column_(grouping_column) {
        clause_info_.input_columns_ = {grouping_column_};
    }
    PartitionClause() = delete;

    ARCTICDB_MOVE_COPY_DEFAULT(PartitionClause)

    [[nodiscard]] std::vector<std::vector<size_t>> structure_for_processing(std::vector<RangesAndKey>& ranges_and_keys
    ) {
        return structure_by_row_slice(ranges_and_keys);
    }

    [[nodiscard]] std::vector<std::vector<EntityId>> structure_for_processing(
            std::vector<std::vector<EntityId>>&& entity_ids_vec
    ) {
        return structure_by_row_slice(*component_manager_, std::move(entity_ids_vec));
    }

    [[nodiscard]] std::vector<EntityId> process(std::vector<EntityId>&& entity_ids) const {
        if (entity_ids.empty()) {
            return {};
        }
        auto proc =
                gather_entities<std::shared_ptr<SegmentInMemory>, std::shared_ptr<RowRange>, std::shared_ptr<ColRange>>(
                        *component_manager_, std::move(entity_ids)
                );
        std::vector<ProcessingUnit> partitioned_procs = partition_processing_segment<GrouperType, BucketizerType>(
                proc, ColumnName(grouping_column_), processing_config_.dynamic_schema_
        );
        std::vector<EntityId> output;
        for (auto&& partitioned_proc : partitioned_procs) {
            std::vector<EntityId> proc_entity_ids = push_entities(*component_manager_, std::move(partitioned_proc));
            output.insert(output.end(), proc_entity_ids.begin(), proc_entity_ids.end());
        }
        return output;
    }

    [[nodiscard]] const ClauseInfo& clause_info() const { return clause_info_; }

    void set_processing_config(const ProcessingConfig& processing_config) { processing_config_ = processing_config; }

    void set_component_manager(std::shared_ptr<ComponentManager> component_manager) {
        component_manager_ = component_manager;
    }

    OutputSchema modify_schema(OutputSchema&& output_schema) const {
        check_column_presence(output_schema, clause_info_.input_columns_, "GroupBy");
        return output_schema;
    }

    OutputSchema join_schemas(std::vector<OutputSchema>&&) const {
        util::raise_rte("GroupByClause::join_schemas should never be called");
    }

    [[nodiscard]] std::string to_string() const { return fmt::format("GROUPBY Column[\"{}\"]", grouping_column_); }
};

struct NamedAggregator {
    NamedAggregator(
            const std::string& aggregation_operator, const std::string& input_column_name,
            const std::string& output_column_name
    ) :
        aggregation_operator_(aggregation_operator),
        input_column_name_(input_column_name),
        output_column_name_(output_column_name) {}

    std::string aggregation_operator_;
    std::string input_column_name_;
    std::string output_column_name_;
};

struct AggregationClause {
    ClauseInfo clause_info_;
    std::shared_ptr<ComponentManager> component_manager_;
    ProcessingConfig processing_config_;
    std::string grouping_column_;
    std::vector<GroupingAggregator> aggregators_;
    std::string str_;

    AggregationClause() = delete;

    ARCTICDB_MOVE_COPY_DEFAULT(AggregationClause)

    AggregationClause(const std::string& grouping_column, const std::vector<NamedAggregator>& aggregations);

    [[noreturn]] std::vector<std::vector<size_t>> structure_for_processing(std::vector<RangesAndKey>&) {
        internal::raise<ErrorCode::E_ASSERTION_FAILURE>("AggregationClause should never be first in the pipeline");
    }

    [[nodiscard]] std::vector<std::vector<EntityId>> structure_for_processing(
            std::vector<std::vector<EntityId>>&& entity_ids_vec
    );

    [[nodiscard]] std::vector<EntityId> process(std::vector<EntityId>&& entity_ids) const;

    [[nodiscard]] const ClauseInfo& clause_info() const { return clause_info_; }

    void set_processing_config(const ProcessingConfig& processing_config) { processing_config_ = processing_config; }

    void set_component_manager(std::shared_ptr<ComponentManager> component_manager) {
        component_manager_ = component_manager;
    }

    OutputSchema modify_schema(OutputSchema&& output_schema) const;

    OutputSchema join_schemas(std::vector<OutputSchema>&&) const {
        util::raise_rte("AggregationClause::join_schemas should never be called");
    }

    [[nodiscard]] std::string to_string() const;
};

template<ResampleBoundary closed_boundary>
struct ResampleClause {
    using BucketGeneratorT = std::function<std::vector<
            timestamp>(timestamp, timestamp, std::string_view, ResampleBoundary, timestamp, const ResampleOrigin&)>;
    ClauseInfo clause_info_;
    std::shared_ptr<ComponentManager> component_manager_;
    ProcessingConfig processing_config_;
    std::string rule_;
    ResampleBoundary label_boundary_;
    // This will contain the data range specified by the user (if any) intersected with the range of timestamps for the
    // symbol
    std::optional<TimestampRange> date_range_;
    // Inject this as a callback in the ctor to avoid language-specific dependencies this low down in the codebase
    BucketGeneratorT generate_bucket_boundaries_;
    std::vector<timestamp> bucket_boundaries_;
    std::vector<SortedAggregatorInterface> aggregators_;
    std::string str_;
    timestamp offset_;
    ResampleOrigin origin_;

    ResampleClause() = delete;

    ARCTICDB_MOVE_COPY_DEFAULT(ResampleClause)

    ResampleClause(
            std::string rule, ResampleBoundary label_boundary, BucketGeneratorT&& generate_bucket_boundaries,
            timestamp offset, ResampleOrigin origin
    );

    [[nodiscard]] std::vector<std::vector<size_t>> structure_for_processing(std::vector<RangesAndKey>& ranges_and_keys);

    [[nodiscard]] std::vector<std::vector<EntityId>> structure_for_processing(
            std::vector<std::vector<EntityId>>&& entity_ids_vec
    );

    [[nodiscard]] std::vector<EntityId> process(std::vector<EntityId>&& entity_ids) const;

    [[nodiscard]] const ClauseInfo& clause_info() const;

    void set_processing_config(const ProcessingConfig& processing_config);

    void set_component_manager(std::shared_ptr<ComponentManager> component_manager);

    OutputSchema modify_schema(OutputSchema&& output_schema) const;

    OutputSchema join_schemas(std::vector<OutputSchema>&&) const {
        util::raise_rte("ResampleClause::join_schemas should never be called");
    }

    [[nodiscard]] std::string to_string() const;

    [[nodiscard]] std::string rule() const;

    void set_aggregations(const std::vector<NamedAggregator>& named_aggregators);

    void set_date_range(timestamp date_range_start, timestamp date_range_end);

    void check_origin_supported_with_date_range() const;

    std::vector<timestamp> generate_bucket_boundaries(
            timestamp first_ts, timestamp last_ts, bool responsible_for_first_overlapping_bucket
    ) const;
};

template<typename T>
struct is_resample : std::false_type {};

template<ResampleBoundary closed_boundary>
struct is_resample<ResampleClause<closed_boundary>> : std::true_type {};

struct RemoveColumnPartitioningClause {
    ClauseInfo clause_info_;
    std::shared_ptr<ComponentManager> component_manager_;
    mutable bool warning_shown = false; // folly::Poly can't deal with atomic_bool
    size_t incompletes_after_;

    RemoveColumnPartitioningClause(size_t incompletes_after = 0) : incompletes_after_(incompletes_after) {
        clause_info_.can_combine_with_column_selection_ = false;
    }
    ARCTICDB_MOVE_COPY_DEFAULT(RemoveColumnPartitioningClause)

    [[nodiscard]] std::vector<std::vector<size_t>> structure_for_processing(std::vector<RangesAndKey>& ranges_and_keys
    ) {
        ranges_and_keys.erase(ranges_and_keys.begin(), ranges_and_keys.begin() + incompletes_after_);
        return structure_by_row_slice(ranges_and_keys);
    }

    [[nodiscard]] std::vector<std::vector<EntityId>> structure_for_processing(
            std::vector<std::vector<EntityId>>&& entity_ids_vec
    ) {
        return structure_by_row_slice(*component_manager_, std::move(entity_ids_vec));
    }

    [[nodiscard]] std::vector<EntityId> process(std::vector<EntityId>&& entity_ids) const;

    [[nodiscard]] const ClauseInfo& clause_info() const { return clause_info_; }

    void set_processing_config(ARCTICDB_UNUSED const ProcessingConfig& processing_config) {}

    void set_component_manager(std::shared_ptr<ComponentManager> component_manager) {
        component_manager_ = component_manager;
    }

    OutputSchema modify_schema(OutputSchema&& output_schema) const { return output_schema; }

    OutputSchema join_schemas(std::vector<OutputSchema>&&) const {
        util::raise_rte("RemoveColumnPartitioningClause::join_schemas should never be called");
    }
};

struct SplitClause {
    ClauseInfo clause_info_;
    std::shared_ptr<ComponentManager> component_manager_;
    const size_t rows_;

    explicit SplitClause(size_t rows) : rows_(rows) {}

    [[nodiscard]] std::vector<std::vector<size_t>> structure_for_processing(std::vector<RangesAndKey>& ranges_and_keys
    ) {
        return structure_by_row_slice(ranges_and_keys);
    }

    [[nodiscard]] std::vector<std::vector<EntityId>> structure_for_processing(
            std::vector<std::vector<EntityId>>&& entity_ids_vec
    ) {
        return structure_by_row_slice(*component_manager_, std::move(entity_ids_vec));
    }

    [[nodiscard]] std::vector<EntityId> process(std::vector<EntityId>&& entity_ids) const;

    [[nodiscard]] const ClauseInfo& clause_info() const { return clause_info_; }

    void set_processing_config(ARCTICDB_UNUSED const ProcessingConfig& processing_config) {}

    void set_component_manager(std::shared_ptr<ComponentManager> component_manager) {
        component_manager_ = component_manager;
    }

    OutputSchema modify_schema(OutputSchema&& output_schema) const { return output_schema; }

    OutputSchema join_schemas(std::vector<OutputSchema>&&) const {
        util::raise_rte("SplitClause::join_schemas should never be called");
    }
};

struct SortClause {
    ClauseInfo clause_info_;
    std::shared_ptr<ComponentManager> component_manager_;
    const std::string column_;
    size_t incompletes_after_;

    explicit SortClause(std::string column, size_t incompletes_after) :
        column_(std::move(column)),
        incompletes_after_(incompletes_after) {}

    [[nodiscard]] std::vector<std::vector<size_t>> structure_for_processing(std::vector<RangesAndKey>& ranges_and_keys
    ) {
        ranges_and_keys.erase(ranges_and_keys.begin(), ranges_and_keys.begin() + incompletes_after_);
        return structure_by_row_slice(ranges_and_keys);
    }

    [[nodiscard]] std::vector<std::vector<EntityId>> structure_for_processing(
            std::vector<std::vector<EntityId>>&& entity_ids_vec
    ) {
        return structure_by_row_slice(*component_manager_, std::move(entity_ids_vec));
    }

    [[nodiscard]] std::vector<EntityId> process(std::vector<EntityId>&& entity_ids) const;

    [[nodiscard]] const ClauseInfo& clause_info() const { return clause_info_; }

    void set_processing_config(ARCTICDB_UNUSED const ProcessingConfig& processing_config) {}

    void set_component_manager(std::shared_ptr<ComponentManager> component_manager) {
        component_manager_ = component_manager;
    }

    OutputSchema modify_schema(OutputSchema&& output_schema) const { return output_schema; }

    OutputSchema join_schemas(std::vector<OutputSchema>&&) const {
        util::raise_rte("SortClause::join_schemas should never be called");
    }
};

struct MergeClause {
    ClauseInfo clause_info_;
    std::shared_ptr<ComponentManager> component_manager_;
    stream::Index index_;
    stream::VariantColumnPolicy density_policy_;
    StreamId stream_id_;
    StreamId target_id_;
    StreamDescriptor stream_descriptor_;
    bool add_symbol_column_ = false;
    bool dynamic_schema_;

    MergeClause(
            stream::Index index, const stream::VariantColumnPolicy& density_policy, const StreamId& stream_id,
            const StreamDescriptor& stream_descriptor, bool dynamic_schema
    );

    [[noreturn]] std::vector<std::vector<size_t>> structure_for_processing(std::vector<RangesAndKey>&) {
        internal::raise<ErrorCode::E_ASSERTION_FAILURE>("MergeClause should never be first in the pipeline");
    }

    [[nodiscard]] std::vector<std::vector<EntityId>> structure_for_processing(
            std::vector<std::vector<EntityId>>&& entity_ids_vec
    );

    [[nodiscard]] std::vector<EntityId> process(std::vector<EntityId>&& entity_ids) const;

    [[nodiscard]] const ClauseInfo& clause_info() const;

    void set_processing_config(const ProcessingConfig& processing_config);

    void set_component_manager(std::shared_ptr<ComponentManager> component_manager);

    OutputSchema modify_schema(OutputSchema&& output_schema) const;

    OutputSchema join_schemas(std::vector<OutputSchema>&&) const {
        util::raise_rte("MergeClause::join_schemas should never be called");
    }
};

struct ColumnStatsGenerationClause {
    ClauseInfo clause_info_;
    std::shared_ptr<ComponentManager> component_manager_;
    ProcessingConfig processing_config_;
    std::shared_ptr<std::vector<ColumnStatsAggregator>> column_stats_aggregators_;

    explicit ColumnStatsGenerationClause(
            std::unordered_set<std::string>&& input_columns,
            std::shared_ptr<std::vector<ColumnStatsAggregator>> column_stats_aggregators
    ) :
        column_stats_aggregators_(std::move(column_stats_aggregators)) {
        clause_info_.input_columns_ = std::move(input_columns);
        clause_info_.can_combine_with_column_selection_ = false;
    }

    ARCTICDB_MOVE_COPY_DEFAULT(ColumnStatsGenerationClause)

    [[nodiscard]] std::vector<std::vector<size_t>> structure_for_processing(std::vector<RangesAndKey>& ranges_and_keys
    ) {
        return structure_by_row_slice(ranges_and_keys);
    }

    [[nodiscard]] std::vector<std::vector<EntityId>> structure_for_processing(
            std::vector<std::vector<EntityId>>&& entity_ids_vec
    ) {
        return structure_by_row_slice(*component_manager_, std::move(entity_ids_vec));
    }

    [[nodiscard]] std::vector<EntityId> process(std::vector<EntityId>&& entity_ids) const;

    [[nodiscard]] const ClauseInfo& clause_info() const { return clause_info_; }

    void set_processing_config(const ProcessingConfig& processing_config) { processing_config_ = processing_config; }

    void set_component_manager(std::shared_ptr<ComponentManager> component_manager) {
        component_manager_ = component_manager;
    }

    OutputSchema modify_schema(ARCTICDB_UNUSED OutputSchema&& output_schema) const {
        // This clause is not used at the moment. Returning empty output schema so that unit tests can succeed.
        return OutputSchema{};
    }

    OutputSchema join_schemas(std::vector<OutputSchema>&&) const {
        util::raise_rte("ColumnStatsGenerationClause::join_schemas should never be called");
    }
};

// Used by head and tail to discard rows not requested by the user
struct RowRangeClause {
    enum class RowRangeType : uint8_t { HEAD, TAIL, RANGE };

    ClauseInfo clause_info_;
    std::shared_ptr<ComponentManager> component_manager_;
    RowRangeType row_range_type_;
    // As passed into head or tail
    int64_t n_{0};

    // User provided values, which are used to calculate start and end.
    // Both can be provided with negative values to wrap indices.
    int64_t user_provided_start_{0};
    int64_t user_provided_end_{0};

    // Row range to keep. Zero-indexed, inclusive of start, exclusive of end.
    // If the RowRangeType is `HEAD` or `TAIL`, this is calculated from `n` and
    // the total rows as passed in by `set_processing_config`.
    // If the RowRangeType is `RANGE`, then start and end are set using the
    // user-provided values as passed in by `set_processing_config`.
    uint64_t start_{0};
    uint64_t end_{0};

    explicit RowRangeClause(RowRangeType row_range_type, int64_t n) : row_range_type_(row_range_type), n_(n) {
        clause_info_.input_structure_ = ProcessingStructure::ALL;
    }

    explicit RowRangeClause(std::optional<int64_t> start, std::optional<int64_t> end) {
        // start and end both absent is a no-op, the Python layer just skips the clause in this case
        util::check(start || end, "Expect at least one of start and end to be present");
        if (start && end) {
            row_range_type_ = RowRangeType::RANGE;
            user_provided_start_ = *start;
            user_provided_end_ = *end;
        } else if (start) {
            // start=0 and end absent is a no-op, the Python layer just skips the clause in this case
            util::check(start != 0, "Did not expect end=nullopt and start==0");
            row_range_type_ = RowRangeType::TAIL;
            n_ = -1 * start.value();
        } else if (end) {
            row_range_type_ = RowRangeType::HEAD;
            n_ = end.value();
        }
        clause_info_.input_structure_ = ProcessingStructure::ALL;
    }

    RowRangeClause() = delete;

    ARCTICDB_MOVE_COPY_DEFAULT(RowRangeClause)

    [[nodiscard]] std::vector<std::vector<size_t>> structure_for_processing(std::vector<RangesAndKey>& ranges_and_keys);

    [[nodiscard]] std::vector<std::vector<EntityId>> structure_for_processing(
            std::vector<std::vector<EntityId>>&& entity_ids_vec
    );

    [[nodiscard]] std::vector<EntityId> process(std::vector<EntityId>&& entity_ids) const;

    [[nodiscard]] const ClauseInfo& clause_info() const { return clause_info_; }

    void set_processing_config(const ProcessingConfig& processing_config);

    void set_component_manager(std::shared_ptr<ComponentManager> component_manager) {
        component_manager_ = component_manager;
    }

    OutputSchema modify_schema(OutputSchema&& output_schema) const { return output_schema; }

    OutputSchema join_schemas(std::vector<OutputSchema>&&) const {
        util::raise_rte("RowRangeClause::join_schemas should never be called");
    }

    [[nodiscard]] std::string to_string() const;

    void calculate_start_and_end(size_t total_rows);
};

struct DateRangeClause {

    ClauseInfo clause_info_;
    std::shared_ptr<ComponentManager> component_manager_;
    ProcessingConfig processing_config_;
    // Time range to keep, inclusive of start and end
    timestamp start_;
    timestamp end_;

    explicit DateRangeClause(timestamp start, timestamp end) : start_(start), end_(end) {}

    DateRangeClause() = delete;

    ARCTICDB_MOVE_COPY_DEFAULT(DateRangeClause)

    [[nodiscard]] std::vector<std::vector<size_t>> structure_for_processing(std::vector<RangesAndKey>& ranges_and_keys);

    [[nodiscard]] std::vector<std::vector<EntityId>> structure_for_processing(
            std::vector<std::vector<EntityId>>&& entity_ids_vec
    ) {
        return structure_by_row_slice(*component_manager_, std::move(entity_ids_vec));
    }

    [[nodiscard]] std::vector<EntityId> process(std::vector<EntityId>&& entity_ids) const;

    [[nodiscard]] const ClauseInfo& clause_info() const { return clause_info_; }

    void set_processing_config(const ProcessingConfig& processing_config);

    void set_component_manager(std::shared_ptr<ComponentManager> component_manager) {
        component_manager_ = component_manager;
    }

    OutputSchema modify_schema(OutputSchema&& output_schema) const;

    OutputSchema join_schemas(std::vector<OutputSchema>&&) const {
        util::raise_rte("DateRangeClause::join_schemas should never be called");
    }

    [[nodiscard]] timestamp start() const { return start_; }

    [[nodiscard]] timestamp end() const { return end_; }

    [[nodiscard]] std::string to_string() const;
};

struct ConcatClause {
    ClauseInfo clause_info_;
    std::shared_ptr<ComponentManager> component_manager_;
    JoinType join_type_;

    explicit ConcatClause(JoinType join_type);

    ARCTICDB_MOVE_COPY_DEFAULT(ConcatClause)

    [[nodiscard]] std::vector<std::vector<size_t>> structure_for_processing(std::vector<RangesAndKey>&) {
        internal::raise<ErrorCode::E_ASSERTION_FAILURE>("ConcatClause should never be first in the pipeline");
    }

    [[nodiscard]] std::vector<std::vector<EntityId>> structure_for_processing(
            std::vector<std::vector<EntityId>>&& entity_ids_vec
    );

    [[nodiscard]] std::vector<EntityId> process(std::vector<EntityId>&& entity_ids) const;

    [[nodiscard]] const ClauseInfo& clause_info() const { return clause_info_; }

    void set_processing_config(const ProcessingConfig&) {}

    void set_component_manager(std::shared_ptr<ComponentManager> component_manager) {
        component_manager_ = component_manager;
    }

    OutputSchema modify_schema(OutputSchema&& output_schema) const { return output_schema; }

    OutputSchema join_schemas(std::vector<OutputSchema>&& input_schemas) const;

    [[nodiscard]] std::string to_string() const;
};

struct WriteClause {
    ClauseInfo clause_info_;
    std::shared_ptr<ComponentManager> component_manager_;
    IndexPartialKey index_partial_key_;
    std::shared_ptr<DeDupMap> dedup_map_;
    std::shared_ptr<Store> store_;

    WriteClause(
            const IndexPartialKey& index_partial_key, std::shared_ptr<DeDupMap> dedup_map, std::shared_ptr<Store> store,
            ProcessingStructure input_processing_structure
    );
    ARCTICDB_MOVE_COPY_DEFAULT(WriteClause)

    [[nodiscard]] std::vector<std::vector<size_t>> structure_for_processing(std::vector<RangesAndKey>&);

    [[nodiscard]] std::vector<std::vector<EntityId>> structure_for_processing(
            std::vector<std::vector<EntityId>>&& entity_ids_vec
    );

    [[nodiscard]] std::vector<EntityId> process(std::vector<EntityId>&& entity_ids) const;

    [[nodiscard]] const ClauseInfo& clause_info() const;

    void set_processing_config(const ProcessingConfig&);

    void set_component_manager(std::shared_ptr<ComponentManager> component_manager);

    OutputSchema modify_schema(OutputSchema&& output_schema) const;

    OutputSchema join_schemas(std::vector<OutputSchema>&&) const;

    [[nodiscard]] std::string to_string() const;

  private:
    stream::PartialKey create_partial_key(const SegmentInMemory& segment, const RowRange& row_range) const;
};

/// Type used as the key when target column values of type TDT are indexed for matching.
template<util::type_descriptor_tag TDT>
using MatchKeyType = std::conditional_t<
        is_sequence_type(TDT::data_type()), std::optional<std::string_view>, typename TDT::DataTypeTag::raw_type>;

template<typename TDT, typename T>
concept sequence_type_raw_value =
        util::type_descriptor_tag<TDT> && is_sequence_type(TDT::data_type()) &&
        util::any_of<std::remove_cvref_t<T>, PyObject*, typename TDT::RawType, std::optional<std::string_view>>;

template<typename TDT, typename T>
concept raw_value_for_type_descriptor =
        util::type_descriptor_tag<TDT> &&
        (std::same_as<T, typename TDT::DataTypeTag::raw_type> || sequence_type_raw_value<TDT, T>);

/// Whether value, one of the representations merge-update uses for a column of type TDT, denotes a missing entry.
/// TDT is always the type of the column itself. value can be the raw type stored in the target column, or, only for
/// sequence types, the PyObject* read from the source tensor or the decoded std::optional<std::string_view> match
/// key.
template<util::type_descriptor_tag TDT, typename V>
requires raw_value_for_type_descriptor<TDT, V>
constexpr bool is_na(V value) {
    if constexpr (is_floating_point_type(TDT::data_type())) {
        return std::isnan(value);
    } else if constexpr (is_time_type(TDT::data_type())) {
        return value == NaT;
    } else if constexpr (is_sequence_type(TDT::data_type())) {
        if constexpr (std::same_as<std::remove_const_t<V>, PyObject*>) {
            return is_py_none(value) || is_py_nan(value);
        } else if constexpr (std::same_as<V, std::optional<std::string_view>>) {
            return !value.has_value();
        } else {
            return !is_a_string(value);
        }
    } else {
        return false;
    }
}

template<util::type_descriptor_tag TDT, typename SourceElementType>
auto get_source_value(std::span<SourceElementType> data, size_t row) {
    return data[row];
}

template<util::type_descriptor_tag TDT>
requires(is_sequence_type(TDT::data_type()))
TDT::RawType get_source_value(
        const std::pair<std::span<const typename TDT::RawType>, const StringPool*>& data, size_t row
) {
    return data.first[row];
}

template<util::type_descriptor_tag TDT>
requires(is_sequence_type(TDT::data_type()))
std::pair<std::optional<std::string_view>, std::optional<convert::PyStringWrapper>> get_source_string(
        std::span<PyObject* const> data, size_t row, std::string_view column_name, size_t source_row_offset,
        std::optional<ScopedGILLock>* gil
) {
    PyObject* const value = data[row];
    if (is_na<TDT>(value)) {
        return {std::nullopt, std::nullopt};
    }
    return util::variant_match(
            create_py_object_wrapper_or_error<TDT::data_type()>(data[row], *gil),
            [&](convert::StringEncodingError&& err
            ) -> std::pair<std::optional<std::string_view>, std::optional<convert::PyStringWrapper>> {
                err.row_index_in_slice_ = row;
                err.raise(column_name, source_row_offset);
            },
            [&](convert::PyStringWrapper&& wrapper
            ) -> std::pair<std::optional<std::string_view>, std::optional<convert::PyStringWrapper>> {
                return std::pair{
                        std::make_optional(std::string_view{wrapper.buffer_, wrapper.length_}),
                        std::make_optional(std::move(wrapper))
                };
            }
    );
}

template<util::type_descriptor_tag TDT>
requires(is_sequence_type(TDT::data_type()))
std::pair<std::optional<std::string_view>, std::optional<convert::PyStringWrapper>>
get_source_string(const std::pair<std::span<const typename TDT::RawType>, const StringPool*>& data, size_t row, std::string_view, size_t, std::optional<ScopedGILLock>*) {
    auto offset = data.first[row];
    if (is_na<TDT>(offset)) {
        return {std::nullopt, std::nullopt};
    }
    return {data.second->get_const_view(offset), std::nullopt};
}

template<util::type_descriptor_tag TDT>
struct NaAwareComparator {
    bool match_na;
    bool operator()(MatchKeyType<TDT> left, MatchKeyType<TDT> right) const {
        const bool left_na = is_na<TDT>(left);
        const bool right_na = is_na<TDT>(right);
        if (left_na || right_na) {
            return match_na && left_na && right_na;
        }
        return left == right;
    }
};

/// This clause will perform update values or insert values based on strategy_ in a segment. The source of new values is
/// the source_ member. Source and target must have the same index type. There are two actions
/// UPDATE: For a particular row in the segment if there's a row in source_ for which all values in the columns listed
/// in on and the index (only in case if timeseries) match update will be performed.
/// INSERT: Each row in source_ not matched by the target will be inserted
struct MergeUpdateClause {
    ClauseInfo clause_info_;
    std::shared_ptr<ComponentManager> component_manager_;
    /// Defines the order in which the columns will be iterated. Currently, it's the same as the order in which the user
    /// passed it via the API. In future query optimization can reoder it. Does not contain duplicates.
    std::vector<std::string> on_;
    /// Used to check if a column is in on_ without performing a linear search.
    ankerl::unordered_dense::set<std::string, util::TransparentStringHash, std::equal_to<>> on_set_;
    MergeStrategy strategy_;
    std::shared_ptr<InputFrame> source_;
    bool fake_index_name_ = false;
    MergeUpdateClause(
            std::vector<std::string>&& on, MergeStrategy strategy, std::shared_ptr<InputFrame> source,
            size_t rows_per_segment
    );
    ARCTICDB_MOVE_COPY_DEFAULT(MergeUpdateClause)

    /// Row range indexes require full table scan
    /// In case of timestamp index this will filter out only the ranges and keys whose index span contains at least one
    /// value from the source index. This does not mean that there's a match only that a match is possible. A crucial
    /// assumption is that the source is ordered. This means that after ranges_and_keys are ordered by row slice we can
    /// perform only forward iteration over the source index to find matches (except the edge of one segment starting
    /// with the same value as the previous ends, see below)
    [[nodiscard]] std::vector<std::vector<size_t>> structure_for_processing(std::vector<RangesAndKey>&);

    [[nodiscard]] std::vector<std::vector<EntityId>> structure_for_processing(
            std::vector<std::vector<EntityId>>&& entity_ids_vec
    );

    [[nodiscard]] std::vector<EntityId> process(std::vector<EntityId>&& entity_ids) const;

    [[nodiscard]] const ClauseInfo& clause_info() const;

    void set_processing_config(const ProcessingConfig&);

    void set_component_manager(std::shared_ptr<ComponentManager> component_manager);

    OutputSchema modify_schema(OutputSchema&& output_schema) const;

    OutputSchema join_schemas(std::vector<OutputSchema>&&) const;

    [[nodiscard]] std::string to_string() const;

    /// The Arrow source converted to a segment. Empty when the source is not Arrow.
    [[nodiscard]] const SegmentInMemory& source_as_segment() const;

    class MatchRecord {
      public:
        MatchRecord(std::span<ProcessingUnit> row_slices, size_t num_source_rows);
        void add_match(size_t source_row, size_t target_row_slice, size_t target_row);
        void add_match(size_t source_row, size_t target_row_slice, std::span<size_t> target_rows);
        template<util::type_descriptor_tag TargetTDT, util::type_descriptor_tag SourceTDT, typename Source>
        void filter_matching_rows(
                std::string_view column_name, size_t source_offset, const Source& variant_source, bool match_na
        ) {
            if (total_matched_target_rows_count_ == 0) {
                // This function can only remove matches in case of a mismatch. In case there are no matched
                // target rows, there's nothing left to remove.
                return;
            }
            util::variant_match(variant_source, [&](const auto& source) {
                // GIL will be acquired if there is a string that is not pure ASCII/UTF-8
                // In this case a PyObject will be allocated by convert::py_unicode_to_buffer
                // If such a string is encountered in a column, then the GIL will be held until that whole column has
                // been processed, on the assumption that if a column has one such string it will probably have many.
                std::optional<ScopedGILLock> gil;
                for (size_t row_slice_idx = 0; row_slice_idx < matched_target_rows_.size(); ++row_slice_idx) {
                    ProcessingUnit& row_slice = row_slices_[row_slice_idx];
                    const ColumnWithStrings target_column =
                            std::get<ColumnWithStrings>(row_slice.get(ColumnName{column_name}));
                    ColumnData col_data = target_column.column_->data();
                    auto target_column_accessor = random_accessor<TargetTDT>(&col_data);
                    std::vector<std::vector<size_t>>& matched_rows = matched_target_rows_[row_slice_idx];
                    for (size_t source_row_idx = 0; source_row_idx < matched_rows.size(); ++source_row_idx) {
                        const size_t discarded_matches =
                                std::erase_if(matched_rows[source_row_idx], [&](const size_t target_row) {
                                    const auto target_value = target_column_accessor[target_row];
                                    NaAwareComparator<SourceTDT> comparator{match_na};
                                    bool are_values_equal = false;
                                    if constexpr (is_sequence_type(SourceTDT::data_type())) {
                                        const auto [source_string, opt_owner] = get_source_string<SourceTDT>(
                                                source, source_row_idx, column_name, source_offset, &gil
                                        );
                                        const std::optional<std::string_view> target_string =
                                                target_column.string_at_offset(target_value);
                                        are_values_equal = comparator(source_string, target_string);
                                    } else {
                                        const auto source_value = get_source_value<SourceTDT>(source, source_row_idx);
                                        are_values_equal = comparator(target_value, source_value);
                                    }
                                    return !are_values_equal;
                                });
                        source_row_matched_count_[source_row_idx] -= discarded_matches;
                        total_matched_target_rows_count_ -= discarded_matches;
                        if (total_matched_target_rows_count_ == 0) {
                            return;
                        }
                    }
                }
            });
        }
        void clone_source_match(size_t source_row_src, size_t source_row_dst, size_t row_slice);
        void validate_rows_to_update(const MergeStrategy& strategy) const;
        [[nodiscard]] size_t total_unmatched_source_rows() const;
        [[nodiscard]] const std::vector<std::vector<size_t>>& matched_rows(size_t target_row_slice) const;
        [[nodiscard]] bool is_source_row_matched(size_t source_row) const;
        [[nodiscard]] bool has_matched_target_rows() const;
        [[nodiscard]] util::BitSet unmatched_source_rows() const;

      private:
        /// For each row slice, for each source row, store all target rows that match it
        std::vector<std::vector<std::vector<size_t>>> matched_target_rows_;
        std::span<ProcessingUnit> row_slices_;
        std::vector<size_t> source_row_matched_count_;
        size_t total_matched_target_rows_count_ = 0;
    };

  private:
    std::vector<ProcessingUnit> update_and_insert(
            const MatchRecord& match_record, const StreamDescriptor& target_descriptor,
            std::vector<ProcessingUnit>&& row_slices, std::pair<size_t, size_t> source_start_end
    ) const;

    std::vector<ProcessingUnit> update(
            const MatchRecord& match_record, std::vector<ProcessingUnit>&& row_slices,
            std::pair<size_t, size_t> source_start_end
    ) const;

    /// Filter segments which will be affected by the merge. The complexity is O(m * log(n)) where n is the number
    /// of rows in the source data and m is the number of row slices in the library
    std::vector<std::vector<size_t>> structure_for_processing_log(std::vector<RangesAndKey>& ranges_and_keys);

    MatchRecord match(std::span<ProcessingUnit> row_slices, std::pair<size_t, size_t> source_start_end) const;

    MatchRecord filter_on_additional_columns_match(
            const StreamDescriptor& source_descriptor, const StreamDescriptor& target_descriptor,
            std::span<ProcessingUnit> proc, std::optional<MatchRecord>&& match_record,
            std::pair<size_t, size_t> source_start_end
    ) const;

    MatchRecord initialize_rows_to_update_for_row_range_indexed_data(
            std::span<ProcessingUnit> row_slices, const StreamDescriptor& source_descriptor,
            std::pair<size_t, size_t> source_start_end
    ) const;

    size_t field_index_for_matching_on_column(std::string_view name, const StreamDescriptor& descriptor) const;

    /// For each processing group, identified by its row range, stores the first and last row in the source that
    /// overlaps with it. The interval is closed in the start and open in the end: [start, end). The key is the row
    /// range rather than the timestamp range because overlapping-window groups can share a timestamp range when
    /// boundary index values repeat.
    ankerl::unordered_dense::map<RowRange, std::pair<size_t, size_t>> source_start_end_for_row_range_;
    std::pair<size_t, size_t> get_source_start_end(std::span<const ProcessingUnit> row_slice) const;

    [[nodiscard]] bool must_structure_by_time_slice() const;

    /// Raises if a pandas source column is not contiguous in memory, which get_source_column requires.
    /// get_source_column returns string columns as PyObject* for pandas sources and as offsets into the string pool of
    /// the converted segment for Arrow sources.
    template<typename RawType>
    void check_source_tensor_is_contiguous(const NativeTensor& tensor, size_t field_index) const {
        user_input::check<ErrorCode::E_INVALID_USER_ARGUMENT>(
                util::is_cstyle_array<RawType>(tensor),
                "Fortran-style arrays are not supported by merge update yet. Column \"{}\" has data type {} of size {} "
                "bytes but the stride is {} bytes",
                source_->desc().field(field_index).name(),
                source_->desc().field(field_index).type(),
                sizeof(RawType),
                tensor.strides()[0]
        );
    }

    template<util::type_descriptor_tag TDT>
    requires(!is_sequence_type(TDT::data_type()))
    std::span<const typename TDT::RawType> get_source_column(
            size_t field_index, std::optional<std::pair<size_t, size_t>> required_range = std::nullopt
    ) const {
        using RawType = TDT::RawType;
        auto [first, last] = required_range.value_or(
                std::pair<size_t, size_t>{0, is_source_arrow() ? source_as_segment_.row_count() : source_->num_rows}
        );
        const auto element_count = last - first;
        if (is_source_arrow()) {
            // TODO: Assert contiguous
            return std::span{
                    reinterpret_cast<const RawType*>(source_as_segment_.column_data(field_index).buffer().data()) +
                            first,
                    element_count
            };
        } else {
            const NativeTensor& tensor = source_->get_tensor(field_index);
            check_source_tensor_is_contiguous<RawType>(tensor, field_index);
            return std::span{static_cast<const RawType*>(tensor.data()) + first, element_count};
        }
    }

    template<util::type_descriptor_tag TDT>
    requires(is_sequence_type(TDT::data_type()))
    std::variant<std::span<PyObject* const>, std::pair<std::span<const typename TDT::RawType>, StringPool*>>
    get_source_column(size_t field_index, std::optional<std::pair<size_t, size_t>> required_range = std::nullopt)
            const {
        using RawType = TDT::RawType;
        auto [first, last] = required_range.value_or(std::pair<size_t, size_t>{0, source_as_segment_.row_count()});
        const auto element_count = last - first;
        if (is_source_arrow()) {
            // TODO: Assert contiguous
            return std::pair{
                    std::span{
                            reinterpret_cast<const RawType*>(source_as_segment_.column_data(field_index).buffer().data()
                            ) + first,
                            element_count
                    },
                    source_as_segment().string_pool_ptr().get()
            };
        } else {
            const NativeTensor& tensor = source_->get_tensor(field_index);
            check_source_tensor_is_contiguous<PyObject*>(tensor, field_index);
            return std::span{static_cast<PyObject* const*>(tensor.data()) + first, element_count};
        }
    }

    bool is_source_arrow() const;

    const StreamDescriptor& get_source_descriptor() const;

    /// Used when the source is Arrow
    SegmentInMemory source_as_segment_;

    size_t rows_per_segment_;
};

struct CompactDataClause {
    /*
     * The algorithm chosen identifies collections of row slices that can be combined and/or split such that the
     * resulting segments on disk all have rows_per_segment rows to within a tolerance of 33%. An algorithm that only
     * combines segments was considered as it would be simpler to both reason about and implement. However, there will
     * always be pathological cases of updating single row dataframes between existing segments that could make some
     * row slices grow in an unbounded manner. A dynamic programming model was also considered, which would find an
     * optimal distribution of rows in each slice for a given input distribution. However, this was rejected on the
     * grounds that small changes to the input distribution could cause large changes to the optimal output
     * distribution, resulting in all of the data being resliced after a small append, for instance.
     */
    ClauseInfo clause_info_;
    std::shared_ptr<ComponentManager> component_manager_;
    uint64_t rows_per_segment_;
    std::shared_ptr<InputFrame> frame_;
    uint64_t min_rows_per_segment_;
    uint64_t max_rows_per_segment_;
    bool dynamic_schema_;

    CompactDataClause(uint64_t rows_per_segment, std::shared_ptr<InputFrame> frame = std::shared_ptr<InputFrame>());
    ARCTICDB_MOVE_COPY_DEFAULT(CompactDataClause)

    [[nodiscard]] std::vector<std::vector<size_t>> structure_for_processing(std::vector<RangesAndKey>& ranges_and_keys);

    [[nodiscard]] std::vector<std::vector<EntityId>> structure_for_processing(
            std::vector<std::vector<EntityId>>&& entity_ids_vec
    );

    [[nodiscard]] std::vector<EntityId> process(std::vector<EntityId>&& entity_ids) const;

    [[nodiscard]] const ClauseInfo& clause_info() const;

    void set_processing_config(const ProcessingConfig& processing_config);

    void set_component_manager(std::shared_ptr<ComponentManager> component_manager);

    OutputSchema modify_schema(OutputSchema&& output_schema) const;

    OutputSchema join_schemas(std::vector<OutputSchema>&&) const;

    [[nodiscard]] std::string to_string() const;

    [[nodiscard]] bool row_ranges_all_acceptable_lengths(const std::set<RowRange>& row_ranges) const;

    [[nodiscard]] std::set<RowRange> structure_row_ranges(const std::set<RowRange>& row_ranges) const;

  private:
    void add_segment_from_frame(
            const ProcessingUnit& proc, size_t col_range_start, std::vector<SegmentInMemory>& segments
    ) const;
};
} // namespace arcticdb
