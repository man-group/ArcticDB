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
#include <arcticdb/processing/aggregation.hpp>
#include <arcticdb/processing/aggregation_interface.hpp>
#include <arcticdb/processing/processing_unit.hpp>
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

using RangesAndKey = pipelines::RangesAndKey;
using SliceAndKey = pipelines::SliceAndKey;

using NormMetaDescriptor = std::shared_ptr<arcticdb::proto::descriptors::NormalizationMetadata>;

// Contains constant data about the clause identifiable at construction time
struct ClauseInfo {
    // Whether processing segments need to be split into new processing segments after this clause's process method has finished
    bool requires_repartition_{false};
    // Whether it makes sense to combine this clause with specifying which columns to view in a call to read or similar
    bool can_combine_with_column_selection_{true};
    // The names of the columns that are needed for this clause to make sense
    // Could either be on disk, or columns created by earlier clauses in the processing pipeline
    std::optional<std::unordered_set<std::string>> input_columns_{std::nullopt};
    // The name of the index after this clause if it has been modified, std::nullopt otherwise
    std::optional<std::string> new_index_{std::nullopt};
    // Whether this clause modifies the output descriptor
    bool modifies_output_descriptor_{false};
};

// Changes how the clause behaves based on information only available after it is constructed
struct ProcessingConfig {
    bool dynamic_schema_{false};
    uint64_t total_rows_ = 0;
};

using EntityIds = std::vector<EntityId>;

struct IClause {
    template<class Base>
    struct Interface : Base {
        // Reorders ranges_and_keys into the order they should be queued up to be read from storage.
        // Returns a vector where each element is a vector of indexes into ranges_and_keys representing the segments needed
        // for one ProcessingUnit.
        // TODO #732: Factor out start_from as part of https://github.com/man-group/ArcticDB/issues/732
        [[nodiscard]] std::vector<std::vector<size_t>>
        structure_for_processing(std::vector<RangesAndKey>& ranges_and_keys,
                                 size_t start_from) const {
            return std::move(folly::poly_call<0>(*this, ranges_and_keys, start_from));
        }

        [[nodiscard]] Composite<EntityIds>
        process(Composite<EntityIds>&& entity_ids) const {
            return std::move(folly::poly_call<1>(*this, std::move(entity_ids)));
        }

        [[nodiscard]] std::optional<std::vector<Composite<EntityIds>>> repartition(
                std::vector<Composite<EntityIds>>&& entity_ids) const {
            return folly::poly_call<2>(*this, std::move(entity_ids));
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
            &T::structure_for_processing,
            &T::process,
            &T::repartition,
            &T::clause_info,
            &T::set_processing_config,
            &T::set_component_manager>;
};

using Clause = folly::Poly<IClause>;

std::vector<std::vector<size_t>> structure_by_row_slice(std::vector<RangesAndKey>& ranges_and_keys,
                                                        size_t start_from);
std::vector<std::vector<size_t>> structure_by_column_slice(std::vector<RangesAndKey>& ranges_and_keys);

Composite<ProcessingUnit> gather_entities(std::shared_ptr<ComponentManager> component_manager,
                                          Composite<EntityIds>&& entity_ids,
                                          bool include_atom_keys = false,
                                          bool include_bucket = false);

EntityIds push_entities(std::shared_ptr<ComponentManager> component_manager, ProcessingUnit&& proc);

struct PassthroughClause {
    ClauseInfo clause_info_;
    PassthroughClause() = default;
    ARCTICDB_MOVE_COPY_DEFAULT(PassthroughClause)

    [[nodiscard]] std::vector<std::vector<size_t>> structure_for_processing(
            std::vector<RangesAndKey>& ranges_and_keys,
            size_t start_from) const {
        return structure_by_row_slice(ranges_and_keys, start_from);
    }

    [[nodiscard]] Composite<EntityIds> process(
            Composite<EntityIds>&& entity_ids
            ) const;

    [[nodiscard]] std::optional<std::vector<Composite<EntityIds>>> repartition(
            ARCTICDB_UNUSED std::vector<Composite<EntityIds>>&&) const {
        return std::nullopt;
    }

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
            std::vector<RangesAndKey>& ranges_and_keys,
            size_t start_from) const {
        return structure_by_row_slice(ranges_and_keys, start_from);
    }

    [[nodiscard]] Composite<EntityIds> process(
            Composite<EntityIds>&& entity_ids
            ) const;

    [[nodiscard]] std::optional<std::vector<Composite<EntityIds>>> repartition(
            ARCTICDB_UNUSED std::vector<Composite<EntityIds>> &&) const {
        return std::nullopt;
    }

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
            std::vector<RangesAndKey>& ranges_and_keys,
            size_t start_from) const {
        return structure_by_row_slice(ranges_and_keys, start_from);
    }

    [[nodiscard]] Composite<EntityIds>
    process(Composite<EntityIds>&& entity_ids) const;

    [[nodiscard]] std::optional<std::vector<Composite<EntityIds>>> repartition(
            ARCTICDB_UNUSED std::vector<Composite<EntityIds>>&&) const {
        return std::nullopt;
    }

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
        clause_info_.requires_repartition_ = true;
        clause_info_.modifies_output_descriptor_ = true;
    }
    PartitionClause() = delete;

    ARCTICDB_MOVE_COPY_DEFAULT(PartitionClause)

    [[nodiscard]] std::vector<std::vector<size_t>> structure_for_processing(
            std::vector<RangesAndKey>& ranges_and_keys,
            size_t start_from) const {
        return structure_by_row_slice(ranges_and_keys, start_from);
    }

    [[nodiscard]] Composite<EntityIds> process(Composite<EntityIds>&& entity_ids) const {
        auto procs = gather_entities(component_manager_, std::move(entity_ids));
        Composite<EntityIds> output;
        procs.broadcast([&output, this](auto &proc) {
            Composite<ProcessingUnit> partitioned_proc = partition_processing_segment<GrouperType, BucketizerType>(proc,
                                                                                                                   ColumnName(grouping_column_),
                                                                                                                   processing_config_.dynamic_schema_);
            partitioned_proc.broadcast([&output, this](auto& p) {
                output.push_back(push_entities(component_manager_, std::move(p)));
            });
        });
        return output;
    }

    [[nodiscard]] std::optional<std::vector<Composite<EntityIds>>> repartition(
            std::vector<Composite<EntityIds>>&& entity_ids) const {
        std::vector<Composite<ProcessingUnit>> input_procs;
        input_procs.reserve(entity_ids.size());
        for (auto&& comp: entity_ids) {
            input_procs.emplace_back(gather_entities(component_manager_, std::move(comp), false, true));
        }
        std::unordered_map<size_t, Composite<ProcessingUnit>> partition_map;
        schema::check<ErrorCode::E_COLUMN_DOESNT_EXIST>(
                std::any_of(input_procs.begin(), input_procs.end(), [](const Composite<ProcessingUnit>& proc) {
                    return !proc.empty();
                }),
                "Grouping column {} does not exist or is empty", grouping_column_
        );

        for (auto &comp : input_procs) {
            comp.broadcast([&partition_map](auto &proc) {
                internal::check<ErrorCode::E_ASSERTION_FAILURE>(proc.bucket_.has_value(),
                                                                "PartitionClause::repartition failed, all processing units must have a bucket ID");

                if (partition_map.find(*proc.bucket_) == partition_map.end()) {
                    partition_map[*proc.bucket_] = Composite<ProcessingUnit>(std::move(proc));
                } else {
                    partition_map[*proc.bucket_].push_back(std::move(proc));
                }
            });
        }

        std::vector<Composite<EntityIds>> ret;
        ret.reserve(partition_map.size());
        for (auto&& [_, comp] : partition_map) {
            ret.emplace_back(comp.transform([this](auto&& proc) {
                return push_entities(component_manager_, std::move(proc));
            }));
        }

        return ret;
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

struct AggregationClause {
    ClauseInfo clause_info_;
    std::shared_ptr<ComponentManager> component_manager_;
    ProcessingConfig processing_config_;
    std::string grouping_column_;
    std::unordered_map<std::string, std::string> aggregation_map_;
    std::vector<GroupingAggregator> aggregators_;

    AggregationClause() = delete;

    ARCTICDB_MOVE_COPY_DEFAULT(AggregationClause)

    AggregationClause(const std::string& grouping_column,
                      const std::unordered_map<std::string,
                      std::string>& aggregations);

    [[noreturn]] std::vector<std::vector<size_t>> structure_for_processing(
            ARCTICDB_UNUSED const std::vector<RangesAndKey>&,
            ARCTICDB_UNUSED size_t) const {
        internal::raise<ErrorCode::E_ASSERTION_FAILURE>(
                "AggregationClause::structure_for_processing should never be called"
                );
    }

    [[nodiscard]] Composite<EntityIds> process(Composite<EntityIds>&& entity_ids) const;

    [[nodiscard]] std::optional<std::vector<Composite<EntityIds>>> repartition(
            ARCTICDB_UNUSED std::vector<Composite<EntityIds>>&&
            ) const {
        return std::nullopt;
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

    [[nodiscard]] std::string to_string() const;
};

struct RemoveColumnPartitioningClause {
    ClauseInfo clause_info_;
    std::shared_ptr<ComponentManager> component_manager_;
    mutable bool warning_shown = false; // folly::Poly can't deal with atomic_bool

    RemoveColumnPartitioningClause() {
        clause_info_.can_combine_with_column_selection_ = false;
    }
    ARCTICDB_MOVE_COPY_DEFAULT(RemoveColumnPartitioningClause)

    [[nodiscard]] std::vector<std::vector<size_t>> structure_for_processing(
            std::vector<RangesAndKey>& ranges_and_keys,
            size_t start_from) const {
        return structure_by_row_slice(ranges_and_keys, start_from);
    }

    [[nodiscard]] Composite<EntityIds> process(Composite<EntityIds>&& entity_ids) const;

    [[nodiscard]] std::optional<std::vector<Composite<EntityIds>>> repartition(
            ARCTICDB_UNUSED std::vector<Composite<EntityIds>>&&) const {
        return std::nullopt;
    }

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
            std::vector<RangesAndKey>& ranges_and_keys,
            size_t start_from) const {
        return structure_by_row_slice(ranges_and_keys, start_from);
    }

    [[nodiscard]] Composite<EntityIds> process(Composite<EntityIds>&& entity_ids) const;

    [[nodiscard]] std::optional<std::vector<Composite<EntityIds>>> repartition(
            ARCTICDB_UNUSED std::vector<Composite<EntityIds>>&&) const {
        return std::nullopt;
    }

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

    explicit SortClause(std::string column) :
            column_(std::move(column)) {
    }

    [[nodiscard]] std::vector<std::vector<size_t>> structure_for_processing(
            std::vector<RangesAndKey>& ranges_and_keys,
            size_t start_from) const {
        return structure_by_row_slice(ranges_and_keys, start_from);
    }

    [[nodiscard]] Composite<EntityIds> process(Composite<EntityIds>&& entity_ids) const;

    [[nodiscard]] std::optional<std::vector<Composite<EntityIds>>> repartition(
            ARCTICDB_UNUSED std::vector<Composite<EntityIds>>&&) const {
        return std::nullopt;
    }

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
        clause_info_.requires_repartition_ = true;
    }

    [[nodiscard]] std::vector<std::vector<size_t>> structure_for_processing(
            std::vector<RangesAndKey>& ranges_and_keys,
            size_t start_from) const {
        return structure_by_row_slice(ranges_and_keys, start_from);
    }

    [[nodiscard]] Composite<EntityIds> process(Composite<EntityIds>&& entity_ids) const;

    [[nodiscard]] std::optional<std::vector<Composite<EntityIds>>> repartition(
            std::vector<Composite<EntityIds>>&& entity_ids) const;

    [[nodiscard]] const ClauseInfo& clause_info() const {
        return clause_info_;
    }

    void set_processing_config(ARCTICDB_UNUSED const ProcessingConfig& processing_config) {
    }

    void set_component_manager(std::shared_ptr<ComponentManager> component_manager) {
        component_manager_ = component_manager;
    }
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
            std::vector<RangesAndKey>& ranges_and_keys,
            size_t start_from) const {
        return structure_by_row_slice(ranges_and_keys, start_from);
    }

    [[nodiscard]] Composite<EntityIds> process(Composite<EntityIds>&& entity_ids) const;

    [[nodiscard]] std::optional<std::vector<Composite<EntityIds>>> repartition(
            ARCTICDB_UNUSED std::vector<Composite<EntityIds>>&&) const {
        return std::nullopt;
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
    int64_t user_provided_start_;
    int64_t user_provided_end_;

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
    }

    explicit RowRangeClause(int64_t start, int64_t end):
            row_range_type_(RowRangeType::RANGE),
            user_provided_start_(start),
            user_provided_end_(end) {
    }

    RowRangeClause() = delete;

    ARCTICDB_MOVE_COPY_DEFAULT(RowRangeClause)

    [[nodiscard]] std::vector<std::vector<size_t>> structure_for_processing(
            std::vector<RangesAndKey>& ranges_and_keys,
            ARCTICDB_UNUSED size_t start_from) const;

    [[nodiscard]] Composite<EntityIds> process(Composite<EntityIds>&& entity_ids) const;

    [[nodiscard]] std::optional<std::vector<Composite<EntityIds>>> repartition(
            ARCTICDB_UNUSED std::vector<Composite<EntityIds>>&&) const {
        return std::nullopt;
    }

    [[nodiscard]] const ClauseInfo& clause_info() const {
        return clause_info_;
    }

    void set_processing_config(const ProcessingConfig& processing_config);

    void set_component_manager(std::shared_ptr<ComponentManager> component_manager) {
        component_manager_ = component_manager;
    }

    [[nodiscard]] std::string to_string() const;
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

    [[nodiscard]] std::vector<std::vector<size_t>> structure_for_processing(
            std::vector<RangesAndKey>& ranges_and_keys,
            size_t start_from) const;

    [[nodiscard]] Composite<EntityIds> process(Composite<EntityIds>&& entity_ids) const;

    [[nodiscard]] std::optional<std::vector<Composite<EntityIds>>> repartition(
            ARCTICDB_UNUSED std::vector<Composite<EntityIds>>&&) const {
        return std::nullopt;
    }

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
