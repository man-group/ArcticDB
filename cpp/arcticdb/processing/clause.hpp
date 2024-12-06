/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/entity/key.hpp>
#include <arcticdb/column_store/column.hpp>
#include <arcticdb/pipeline/frame_slice.hpp>
#include <arcticdb/pipeline/value.hpp>
#include <arcticdb/processing/expression_context.hpp>
#include <arcticdb/processing/expression_node.hpp>
#include <arcticdb/entity/types.hpp>
#include <arcticdb/processing/clause_utils.hpp>
#include <arcticdb/processing/unsorted_aggregation.hpp>
#include <arcticdb/processing/aggregation_interface.hpp>
#include <arcticdb/processing/processing_unit.hpp>
#include <arcticdb/processing/sorted_aggregation.hpp>
#include <arcticdb/processing/grouper.hpp>
#include <arcticdb/stream/aggregator.hpp>
#include <arcticdb/util/movable_priority_queue.hpp>
#include <arcticdb/pipeline/index_utils.hpp>

#include <folly/Poly.h>
#include <folly/futures/Future.h>

#include <vector>
#include <unordered_map>
#include <string>
#include <variant>
#include <memory>
#include <atomic>

namespace arcticdb {

using ResampleOrigin = std::variant<std::string, timestamp>;

using RangesAndKey = pipelines::RangesAndKey;
using SliceAndKey = pipelines::SliceAndKey;

struct IClause {
    template<class Base>
    struct Interface : Base {
        // Reorders ranges_and_keys into the order they should be queued up to be read from storage.
        // Returns a vector where each element is a vector of indexes into ranges_and_keys representing the segments needed
        // for one ProcessingUnit.
        [[nodiscard]] std::vector<std::vector<size_t>>
        structure_for_processing(std::vector<RangesAndKey>& ranges_and_keys) {
            return std::move(folly::poly_call<0>(*this, ranges_and_keys));
        }

        [[nodiscard]] std::vector<std::vector<EntityId>> structure_for_processing(
                std::vector<std::vector<EntityId>>&& entity_ids_vec) {
            return folly::poly_call<1>(*this, std::move(entity_ids_vec));
        }

        [[nodiscard]] std::vector<EntityId>
        process(std::vector<EntityId>&& entity_ids) const {
            return std::move(folly::poly_call<2>(*this, std::move(entity_ids)));
        }

        [[nodiscard]] const ClauseInfo& clause_info() const { return folly::poly_call<3>(*this); };

        void set_processing_config(const ProcessingConfig& processing_config) {
            folly::poly_call<4>(*this, processing_config);
        }

        void set_component_manager(std::shared_ptr<ComponentManager> component_manager) {
            folly::poly_call<5>(*this, component_manager);
        }
    };

    template<class T>
    using Members = folly::PolyMembers<
            folly::sig<std::vector<std::vector<size_t>>(std::vector<RangesAndKey>&)>(&T::structure_for_processing),
            folly::sig<std::vector<std::vector<EntityId>>(std::vector<std::vector<EntityId>>&&)>(&T::structure_for_processing),
            &T::process,
            &T::clause_info,
            &T::set_processing_config,
            &T::set_component_manager>;
};

using Clause = folly::Poly<IClause>;

struct PassthroughClause {
    ClauseInfo clause_info_;

    PassthroughClause() = default;
    ARCTICDB_MOVE_COPY_DEFAULT(PassthroughClause)

    [[nodiscard]] std::vector<std::vector<size_t>> structure_for_processing(
            std::vector<RangesAndKey>& ranges_and_keys) {
        return structure_by_row_slice(ranges_and_keys); // TODO: No structuring?
    }

    [[nodiscard]] std::vector<std::vector<EntityId>> structure_for_processing(std::vector<std::vector<EntityId>>&& entity_ids_vec) {
        return entity_ids_vec; // TODO: structure by row slice?
    }

    [[nodiscard]] std::vector<EntityId> process(std::vector<EntityId>&& entity_ids) const;

    [[nodiscard]] const ClauseInfo& clause_info() const {
        return clause_info_;
    }

    void set_processing_config(ARCTICDB_UNUSED const ProcessingConfig&) {}

    void set_component_manager(ARCTICDB_UNUSED std::shared_ptr<ComponentManager>) {}
};

struct FilterClause {
    ClauseInfo clause_info_;
    std::shared_ptr<ComponentManager> component_manager_;
    std::shared_ptr<ExpressionContext> expression_context_;
    PipelineOptimisation optimisation_;

    explicit FilterClause(std::unordered_set<std::string> input_columns,
                          ExpressionContext expression_context,
                          std::optional<PipelineOptimisation> optimisation) :
            expression_context_(std::make_shared<ExpressionContext>(std::move(expression_context))),
            optimisation_(optimisation.value_or(PipelineOptimisation::SPEED)) {
        clause_info_.input_columns_ = std::move(input_columns);
    }

    FilterClause() = delete;

    ARCTICDB_MOVE_COPY_DEFAULT(FilterClause)

    [[nodiscard]] std::vector<std::vector<size_t>> structure_for_processing(
            std::vector<RangesAndKey>& ranges_and_keys) {
        return structure_by_row_slice(ranges_and_keys);
    }

    [[nodiscard]] std::vector<std::vector<EntityId>> structure_for_processing(std::vector<std::vector<EntityId>>&& entity_ids_vec) {
        return structure_by_row_slice(*component_manager_, std::move(entity_ids_vec));
    }

    [[nodiscard]] std::vector<EntityId> process(std::vector<EntityId>&& entity_ids) const;

    [[nodiscard]] const ClauseInfo& clause_info() const {
        return clause_info_;
    }

    void set_processing_config(const ProcessingConfig& processing_config) {
        expression_context_->dynamic_schema_ = processing_config.dynamic_schema_;
    }

    void set_component_manager(std::shared_ptr<ComponentManager> component_manager) {
        component_manager_ = component_manager;
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

    explicit ProjectClause(std::unordered_set<std::string> input_columns,
                           std::string output_column,
                           ExpressionContext expression_context) :
            output_column_(output_column),
            expression_context_(std::make_shared<ExpressionContext>(std::move(expression_context))) {
        clause_info_.input_columns_ = std::move(input_columns);
        clause_info_.modifies_output_descriptor_ = true;
    }

    ProjectClause() = delete;

    ARCTICDB_MOVE_COPY_DEFAULT(ProjectClause)

    [[nodiscard]] std::vector<std::vector<size_t>> structure_for_processing(
            std::vector<RangesAndKey>& ranges_and_keys) {
        return structure_by_row_slice(ranges_and_keys);
    }

    [[nodiscard]] std::vector<std::vector<EntityId>> structure_for_processing(std::vector<std::vector<EntityId>>&& entity_ids_vec) {
        return structure_by_row_slice(*component_manager_, std::move(entity_ids_vec));
    }

    [[nodiscard]] std::vector<EntityId> process(std::vector<EntityId>&& entity_ids) const;

    [[nodiscard]] const ClauseInfo& clause_info() const {
        return clause_info_;
    }

    void set_processing_config(const ProcessingConfig& processing_config) {
        expression_context_->dynamic_schema_ = processing_config.dynamic_schema_;
    }

    void set_component_manager(std::shared_ptr<ComponentManager> component_manager) {
        component_manager_ = component_manager;
    }

    [[nodiscard]] std::string to_string() const;
};

template<typename GrouperType, typename BucketizerType>
struct PartitionClause {
    ClauseInfo clause_info_;
    std::shared_ptr<ComponentManager> component_manager_;
    ProcessingConfig processing_config_;
    std::string grouping_column_;

    explicit PartitionClause(const std::string& grouping_column) :
            processing_config_(),
            grouping_column_(grouping_column) {
        clause_info_.input_columns_ = {grouping_column_};
        clause_info_.modifies_output_descriptor_ = true;
    }
    PartitionClause() = delete;

    ARCTICDB_MOVE_COPY_DEFAULT(PartitionClause)

    [[nodiscard]] std::vector<std::vector<size_t>> structure_for_processing(
            std::vector<RangesAndKey>& ranges_and_keys) {
        return structure_by_row_slice(ranges_and_keys);
    }

    [[nodiscard]] std::vector<std::vector<EntityId>> structure_for_processing(std::vector<std::vector<EntityId>>&& entity_ids_vec) {
        return structure_by_row_slice(*component_manager_, std::move(entity_ids_vec));
    }

    [[nodiscard]] std::vector<EntityId> process(std::vector<EntityId>&& entity_ids) const {
        if (entity_ids.empty()) {
            return {};
        }
        auto proc = gather_entities<std::shared_ptr<SegmentInMemory>, std::shared_ptr<RowRange>, std::shared_ptr<ColRange>>(*component_manager_, std::move(entity_ids));
        std::vector<ProcessingUnit> partitioned_procs = partition_processing_segment<GrouperType, BucketizerType>(
                proc,
                ColumnName(grouping_column_),
                processing_config_.dynamic_schema_);
        std::vector<EntityId> output;
        for (auto &&partitioned_proc: partitioned_procs) {
            std::vector<EntityId> proc_entity_ids = push_entities(*component_manager_, std::move(partitioned_proc));
            output.insert(output.end(), proc_entity_ids.begin(), proc_entity_ids.end());
        }
        return output;
    }

    [[nodiscard]] const ClauseInfo& clause_info() const {
        return clause_info_;
    }

    void set_processing_config(const ProcessingConfig& processing_config) {
        processing_config_ = processing_config;
    }

    void set_component_manager(std::shared_ptr<ComponentManager> component_manager) {
        component_manager_ = component_manager;
    }

    [[nodiscard]] std::string to_string() const {
        return fmt::format("GROUPBY Column[\"{}\"]", grouping_column_);
    }
};

struct NamedAggregator {
    NamedAggregator(const std::string& aggregation_operator,
                    const std::string& input_column_name,
                    const std::string& output_column_name) :
            aggregation_operator_(aggregation_operator),
            input_column_name_(input_column_name),
            output_column_name_(output_column_name){

    }

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

    AggregationClause(const std::string& grouping_column,
                      const std::vector<NamedAggregator>& aggregations);

    [[noreturn]] std::vector<std::vector<size_t>> structure_for_processing(ARCTICDB_UNUSED std::vector<RangesAndKey>&) {
        internal::raise<ErrorCode::E_ASSERTION_FAILURE>("AggregationClause should never be first in the pipeline");
    }

    [[nodiscard]] std::vector<std::vector<EntityId>> structure_for_processing(std::vector<std::vector<EntityId>>&& entity_ids_vec);

    [[nodiscard]] std::vector<EntityId> process(std::vector<EntityId>&& entity_ids) const;

    [[nodiscard]] const ClauseInfo& clause_info() const {
        return clause_info_;
    }

    void set_processing_config(const ProcessingConfig& processing_config) {
        processing_config_ = processing_config;
    }

    void set_component_manager(std::shared_ptr<ComponentManager> component_manager) {
        component_manager_ = component_manager;
    }

    [[nodiscard]] std::string to_string() const;
};

template<ResampleBoundary closed_boundary>
struct ResampleClause {
    using BucketGeneratorT = std::function<std::vector<timestamp>(timestamp, timestamp, std::string_view, ResampleBoundary, timestamp, const ResampleOrigin&)>;
    ClauseInfo clause_info_;
    std::shared_ptr<ComponentManager> component_manager_;
    ProcessingConfig processing_config_;
    std::string rule_;
    ResampleBoundary label_boundary_;
    // This will contain the data range specified by the user (if any) intersected with the range of timestamps for the symbol
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

    ResampleClause(std::string rule,
        ResampleBoundary label_boundary,
        BucketGeneratorT&& generate_bucket_boundaries,
        timestamp offset,
        ResampleOrigin origin);

    [[nodiscard]] std::vector<std::vector<size_t>> structure_for_processing(
            std::vector<RangesAndKey>& ranges_and_keys);

    [[nodiscard]] std::vector<std::vector<EntityId>> structure_for_processing(std::vector<std::vector<EntityId>>&& entity_ids_vec);

    [[nodiscard]] std::vector<EntityId> process(std::vector<EntityId>&& entity_ids) const;

    [[nodiscard]] const ClauseInfo& clause_info() const;

    void set_processing_config(const ProcessingConfig& processing_config);

    void set_component_manager(std::shared_ptr<ComponentManager> component_manager);

    [[nodiscard]] std::string to_string() const;

    [[nodiscard]] std::string rule() const;

    void set_aggregations(const std::vector<NamedAggregator>& named_aggregators);

    void set_date_range(timestamp date_range_start, timestamp date_range_end);

    std::vector<timestamp> generate_bucket_boundaries(timestamp first_ts,
                                                      timestamp last_ts,
                                                      bool responsible_for_first_overlapping_bucket) const;

    std::shared_ptr<Column> generate_output_index_column(const std::vector<std::shared_ptr<Column>>& input_index_columns,
                                                         const std::vector<timestamp>& bucket_boundaries) const;
};

template<typename T>
struct is_resample: std::false_type{};

template<ResampleBoundary closed_boundary>
struct is_resample<ResampleClause<closed_boundary>>: std::true_type{};

struct RemoveColumnPartitioningClause {
    ClauseInfo clause_info_;
    std::shared_ptr<ComponentManager> component_manager_;
    mutable bool warning_shown = false; // folly::Poly can't deal with atomic_bool
    size_t incompletes_after_;

    RemoveColumnPartitioningClause(size_t incompletes_after=0):
            incompletes_after_(incompletes_after){
        clause_info_.can_combine_with_column_selection_ = false;
    }
    ARCTICDB_MOVE_COPY_DEFAULT(RemoveColumnPartitioningClause)

    [[nodiscard]] std::vector<std::vector<size_t>> structure_for_processing(
            std::vector<RangesAndKey>& ranges_and_keys) {
        ranges_and_keys.erase(ranges_and_keys.begin(), ranges_and_keys.begin() + incompletes_after_);
        return structure_by_row_slice(ranges_and_keys);
    }

    [[nodiscard]] std::vector<std::vector<EntityId>> structure_for_processing(std::vector<std::vector<EntityId>>&& entity_ids_vec) {
        return structure_by_row_slice(*component_manager_, std::move(entity_ids_vec));
    }

    [[nodiscard]] std::vector<EntityId> process(std::vector<EntityId>&& entity_ids) const;

    [[nodiscard]] const ClauseInfo& clause_info() const {
        return clause_info_;
    }

    void set_processing_config(ARCTICDB_UNUSED const ProcessingConfig& processing_config) {
    }

    void set_component_manager(std::shared_ptr<ComponentManager> component_manager) {
        component_manager_ = component_manager;
    }
};

struct SplitClause {
    ClauseInfo clause_info_;
    std::shared_ptr<ComponentManager> component_manager_;
    const size_t rows_;

    explicit SplitClause(size_t rows) :
            rows_(rows) {
    }

    [[nodiscard]] std::vector<std::vector<size_t>> structure_for_processing(
            std::vector<RangesAndKey>& ranges_and_keys) {
        return structure_by_row_slice(ranges_and_keys);
    }

    [[nodiscard]] std::vector<std::vector<EntityId>> structure_for_processing(std::vector<std::vector<EntityId>>&& entity_ids_vec) {
        return structure_by_row_slice(*component_manager_, std::move(entity_ids_vec));
    }

    [[nodiscard]] std::vector<EntityId> process(std::vector<EntityId>&& entity_ids) const;

    [[nodiscard]] const ClauseInfo& clause_info() const {
        return clause_info_;
    }

    void set_processing_config(ARCTICDB_UNUSED const ProcessingConfig& processing_config) {}

    void set_component_manager(std::shared_ptr<ComponentManager> component_manager) {
        component_manager_ = component_manager;
    }
};

struct SortClause {
    ClauseInfo clause_info_;
    std::shared_ptr<ComponentManager> component_manager_;
    const std::string column_;
    size_t incompletes_after_;

    explicit SortClause(std::string column, size_t incompletes_after) :
            column_(std::move(column)),
            incompletes_after_(incompletes_after){
    }

    [[nodiscard]] std::vector<std::vector<size_t>> structure_for_processing(
            std::vector<RangesAndKey>& ranges_and_keys) {
        ranges_and_keys.erase(ranges_and_keys.begin(), ranges_and_keys.begin() + incompletes_after_);
        return structure_by_row_slice(ranges_and_keys);
    }

    [[nodiscard]] std::vector<std::vector<EntityId>> structure_for_processing(std::vector<std::vector<EntityId>>&& entity_ids_vec) {
        return structure_by_row_slice(*component_manager_, std::move(entity_ids_vec));
    }

    [[nodiscard]] std::vector<EntityId> process(std::vector<EntityId>&& entity_ids) const;

    [[nodiscard]] const ClauseInfo& clause_info() const {
        return clause_info_;
    }

    void set_processing_config(ARCTICDB_UNUSED const ProcessingConfig& processing_config) {}

    void set_component_manager(std::shared_ptr<ComponentManager> component_manager) {
        component_manager_ = component_manager;
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
        stream::Index index,
        const stream::VariantColumnPolicy& density_policy,
        const StreamId& stream_id,
        const StreamDescriptor& stream_descriptor,
        bool dynamic_schema
    );

    [[noreturn]] std::vector<std::vector<size_t>> structure_for_processing(std::vector<RangesAndKey>&) {
        internal::raise<ErrorCode::E_ASSERTION_FAILURE>("MergeClause should never be first in the pipeline");
    }

    [[nodiscard]] std::vector<std::vector<EntityId>> structure_for_processing(std::vector<std::vector<EntityId>>&& entity_ids_vec);

    [[nodiscard]] std::vector<EntityId> process(std::vector<EntityId>&& entity_ids) const;

    [[nodiscard]] const ClauseInfo& clause_info() const;

    void set_processing_config(const ProcessingConfig& processing_config);

    void set_component_manager(std::shared_ptr<ComponentManager> component_manager);
};

struct ColumnStatsGenerationClause {
    ClauseInfo clause_info_;
    std::shared_ptr<ComponentManager> component_manager_;
    ProcessingConfig processing_config_;
    std::shared_ptr<std::vector<ColumnStatsAggregator>> column_stats_aggregators_;

    explicit ColumnStatsGenerationClause(
        std::unordered_set<std::string>&& input_columns,
        std::shared_ptr<std::vector<ColumnStatsAggregator>> column_stats_aggregators) :
            processing_config_(),
            column_stats_aggregators_(std::move(column_stats_aggregators)) {
        clause_info_.input_columns_ = std::move(input_columns);
        clause_info_.can_combine_with_column_selection_ = false;
        clause_info_.modifies_output_descriptor_ = true;
    }

    ARCTICDB_MOVE_COPY_DEFAULT(ColumnStatsGenerationClause)

    [[nodiscard]] std::vector<std::vector<size_t>> structure_for_processing(
            std::vector<RangesAndKey>& ranges_and_keys) {
        return structure_by_row_slice(ranges_and_keys);
    }

    [[nodiscard]] std::vector<std::vector<EntityId>> structure_for_processing(std::vector<std::vector<EntityId>>&& entity_ids_vec) {
        return structure_by_row_slice(*component_manager_, std::move(entity_ids_vec));
    }

    [[nodiscard]] std::vector<EntityId> process(std::vector<EntityId>&& entity_ids) const;

    [[nodiscard]] const ClauseInfo& clause_info() const {
        return clause_info_;
    }

    void set_processing_config(const ProcessingConfig& processing_config) {
        processing_config_ = processing_config;
    }

    void set_component_manager(std::shared_ptr<ComponentManager> component_manager) {
        component_manager_ = component_manager;
    }
};

// Used by head and tail to discard rows not requested by the user
struct RowRangeClause {
    enum class RowRangeType: uint8_t {
        HEAD,
        TAIL,
        RANGE
    };

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

    explicit RowRangeClause(RowRangeType row_range_type, int64_t n):
            row_range_type_(row_range_type),
            n_(n) {
        clause_info_.input_structure_ = ProcessingStructure::ALL;
    }

    explicit RowRangeClause(int64_t start, int64_t end):
            row_range_type_(RowRangeType::RANGE),
            user_provided_start_(start),
            user_provided_end_(end) {
        clause_info_.input_structure_ = ProcessingStructure::ALL;
    }

    RowRangeClause() = delete;

    ARCTICDB_MOVE_COPY_DEFAULT(RowRangeClause)

    [[nodiscard]] std::vector<std::vector<size_t>> structure_for_processing(std::vector<RangesAndKey>& ranges_and_keys);

    [[nodiscard]] std::vector<std::vector<EntityId>> structure_for_processing(std::vector<std::vector<EntityId>>&& entity_ids_vec);

    [[nodiscard]] std::vector<EntityId> process(std::vector<EntityId>&& entity_ids) const;

    [[nodiscard]] const ClauseInfo& clause_info() const {
        return clause_info_;
    }

    void set_processing_config(const ProcessingConfig& processing_config);

    void set_component_manager(std::shared_ptr<ComponentManager> component_manager) {
        component_manager_ = component_manager;
    }

    [[nodiscard]] std::string to_string() const;

    void calculate_start_and_end(size_t total_rows);
};

struct DateRangeClause {

    ClauseInfo clause_info_;
    std::shared_ptr<ComponentManager> component_manager_;
    // Time range to keep, inclusive of start and end
    timestamp start_;
    timestamp end_;

    explicit DateRangeClause(timestamp start, timestamp end):
            start_(start),
            end_(end) {
    }

    DateRangeClause() = delete;

    ARCTICDB_MOVE_COPY_DEFAULT(DateRangeClause)

    [[nodiscard]] std::vector<std::vector<size_t>> structure_for_processing(std::vector<RangesAndKey>& ranges_and_keys);

    [[nodiscard]] std::vector<std::vector<EntityId>> structure_for_processing(std::vector<std::vector<EntityId>>&& entity_ids_vec) {
        return structure_by_row_slice(*component_manager_, std::move(entity_ids_vec));
    }

    [[nodiscard]] std::vector<EntityId> process(std::vector<EntityId>&& entity_ids) const;

    [[nodiscard]] const ClauseInfo& clause_info() const {
        return clause_info_;
    }

    void set_processing_config(ARCTICDB_UNUSED const ProcessingConfig& processing_config) {
    }

    void set_component_manager(std::shared_ptr<ComponentManager> component_manager) {
        component_manager_ = component_manager;
    }

    [[nodiscard]] timestamp start() const {
        return start_;
    }

    [[nodiscard]] timestamp end() const {
        return end_;
    }

    [[nodiscard]] std::string to_string() const;
};

}//namespace arcticdb
