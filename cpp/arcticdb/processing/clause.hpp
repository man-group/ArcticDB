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
#include <arcticdb/processing/processing_unit.hpp>
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
    uint64_t total_rows_;
};


struct IClause {
    template<class Base>
    struct Interface : Base {
        // (at least) the first clause in the pipeline will be parallelied over elements of the returned vector
        // TODO #732: Factor out start_from as part of https://github.com/man-group/ArcticDB/issues/732
        [[nodiscard]] std::vector<Composite<SliceAndKey>>
        structure_for_processing(std::vector<SliceAndKey>& slice_and_keys, size_t start_from) const {
            return std::move(folly::poly_call<0>(*this, slice_and_keys, start_from));
        }

        [[nodiscard]] Composite<ProcessingUnit>
        process(std::shared_ptr<Store> store, Composite<ProcessingUnit> &&segs) const {
            return std::move(folly::poly_call<1>(*this, store, std::move(segs)));
        }

        [[nodiscard]] std::optional<std::vector<Composite<ProcessingUnit>>> repartition(
                [[maybe_unused]] std::vector<Composite<ProcessingUnit>> &&comps) const {
            return folly::poly_call<2>(*this, std::move(comps));
        }

        [[nodiscard]] const ClauseInfo& clause_info() const { return folly::poly_call<3>(*this); };

        void set_processing_config(const ProcessingConfig& processing_config) {
            folly::poly_call<4>(*this, processing_config);
        }
    };

    template<class T>
    using Members = folly::PolyMembers<
            &T::structure_for_processing,
            &T::process,
            &T::repartition,
            &T::clause_info,
            &T::set_processing_config>;
};

using Clause = folly::Poly<IClause>;

std::vector<Composite<SliceAndKey>> structure_by_row_slice(std::vector<SliceAndKey>& slice_and_keys, size_t start_from);
std::vector<Composite<SliceAndKey>> structure_by_column_slice(std::vector<SliceAndKey>& slice_and_keys);

struct PassthroughClause {
    ClauseInfo clause_info_;
    PassthroughClause() = default;
    ARCTICDB_MOVE_COPY_DEFAULT(PassthroughClause)

    [[nodiscard]] std::vector<Composite<SliceAndKey>> structure_for_processing(
            std::vector<SliceAndKey>& slice_and_keys, size_t start_from) const {
        return structure_by_row_slice(slice_and_keys, start_from);
    }

    [[nodiscard]] Composite<ProcessingUnit> process(
            ARCTICDB_UNUSED const std::shared_ptr<Store> &store,
            Composite<ProcessingUnit> &&p
            ) const;

    [[nodiscard]] std::optional<std::vector<Composite<ProcessingUnit>>> repartition(
            ARCTICDB_UNUSED std::vector<Composite<ProcessingUnit>> &&comps) const {
        return std::nullopt;
    }

    [[nodiscard]] const ClauseInfo& clause_info() const {
        return clause_info_;
    }

    void set_processing_config(ARCTICDB_UNUSED const ProcessingConfig& processing_config) {}
};

struct FilterClause {
    ClauseInfo clause_info_;
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

    [[nodiscard]] std::vector<Composite<SliceAndKey>> structure_for_processing(
            std::vector<SliceAndKey>& slice_and_keys, size_t start_from) const {
        return structure_by_row_slice(slice_and_keys, start_from);
    }

    [[nodiscard]] Composite<ProcessingUnit> process(
            std::shared_ptr<Store> store,
            Composite<ProcessingUnit> &&p
            ) const;

    [[nodiscard]] std::optional<std::vector<Composite<ProcessingUnit>>> repartition(
            ARCTICDB_UNUSED std::vector<Composite<ProcessingUnit>> &&) const {
        return std::nullopt;
    }

    [[nodiscard]] const ClauseInfo& clause_info() const {
        return clause_info_;
    }

    void set_processing_config(const ProcessingConfig& processing_config) {
        expression_context_->dynamic_schema_ = processing_config.dynamic_schema_;
    }

    [[nodiscard]] std::string to_string() const;

    void set_pipeline_optimisation(PipelineOptimisation pipeline_optimisation) {
        optimisation_ = pipeline_optimisation;
    }
};

struct ProjectClause {
    ClauseInfo clause_info_;
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

    [[nodiscard]] std::vector<Composite<SliceAndKey>> structure_for_processing(
            std::vector<SliceAndKey>& slice_and_keys, size_t start_from) const {
        return structure_by_row_slice(slice_and_keys, start_from);
    }

    [[nodiscard]] Composite<ProcessingUnit>
    process(std::shared_ptr<Store> store, Composite<ProcessingUnit> &&p) const;

    [[nodiscard]] std::optional<std::vector<Composite<ProcessingUnit>>> repartition(
            ARCTICDB_UNUSED std::vector<Composite<ProcessingUnit>> &&comps) const {
        return std::nullopt;
    }

    [[nodiscard]] const ClauseInfo& clause_info() const {
        return clause_info_;
    }

    void set_processing_config(const ProcessingConfig& processing_config) {
        expression_context_->dynamic_schema_ = processing_config.dynamic_schema_;
    }

    [[nodiscard]] std::string to_string() const;
};

template<typename GrouperType, typename BucketizerType>
struct PartitionClause {
    ClauseInfo clause_info_;
    ProcessingConfig processing_config_;
    std::string grouping_column_;

    explicit PartitionClause(const std::string& grouping_column) :
            grouping_column_(grouping_column) {
        clause_info_.input_columns_ = {grouping_column_};
        clause_info_.requires_repartition_ = true;
        clause_info_.modifies_output_descriptor_ = true;
    }
    PartitionClause() = delete;

    ARCTICDB_MOVE_COPY_DEFAULT(PartitionClause)

    [[nodiscard]] std::vector<Composite<SliceAndKey>> structure_for_processing(
            std::vector<SliceAndKey>& slice_and_keys, size_t start_from) const {
        return structure_by_row_slice(slice_and_keys, start_from);
    }

    [[nodiscard]] Composite<ProcessingUnit> process(std::shared_ptr<Store> store,
                                                    Composite<ProcessingUnit> &&p) const {
        Composite<ProcessingUnit> output;
        auto procs = std::move(p);
        procs.broadcast([&output, &store, that=this](auto &proc) {
            output.push_back(partition_processing_segment<GrouperType, BucketizerType>(store,
                                                                                       proc,
                                                                                       ColumnName(that->grouping_column_),
                                                                                       that->processing_config_.dynamic_schema_));
        });
        return output;
    }

    [[nodiscard]] std::optional<std::vector<Composite<ProcessingUnit>>> repartition(
            ARCTICDB_UNUSED std::vector<Composite<ProcessingUnit>> &&c) const {
        auto comps = std::move(c);
        std::unordered_map<size_t, Composite<ProcessingUnit>> partition_map;
        schema::check<ErrorCode::E_COLUMN_DOESNT_EXIST>(
                std::any_of(comps.begin(), comps.end(), [](const Composite<ProcessingUnit>& proc) {
                    return !proc.empty();
                }),
                "Grouping column {} does not exist or is empty", grouping_column_
        );

        for (auto &comp : comps) {
            comp.broadcast([&partition_map](auto &proc) {
                auto bucket_id = proc.get_bucket();

                if (partition_map.find(bucket_id) == partition_map.end()) {
                    partition_map[bucket_id] = Composite<ProcessingUnit>(std::move(proc));
                } else {
                    partition_map[bucket_id].push_back(std::move(proc));
                }
            });
        }

        std::vector<Composite<ProcessingUnit>> ret;
        for (auto &[key, value] : partition_map) {
            ret.push_back(std::move(value));
        }

        return ret;
    }

    [[nodiscard]] const ClauseInfo& clause_info() const {
        return clause_info_;
    }

    void set_processing_config(const ProcessingConfig& processing_config) {
        processing_config_ = processing_config;
    }

    [[nodiscard]] std::string to_string() const {
        return fmt::format("GROUPBY Column[\"{}\"]", grouping_column_);
    }
};

inline StreamDescriptor empty_descriptor(arcticdb::proto::descriptors::IndexDescriptor::Type type = arcticdb::proto::descriptors::IndexDescriptor::ROWCOUNT, const StreamId &id = "merged") {
    const auto index = stream::variant_index_from_type(type);
    const auto field_count = util::variant_match(index, [] (const auto& idx) { return idx.field_count(); });
    return StreamDescriptor{StreamId{id}, IndexDescriptor{field_count, type}, std::make_shared<FieldCollection>()};
}

struct AggregationClause {
    ClauseInfo clause_info_;
    ProcessingConfig processing_config_;
    std::string grouping_column_;
    std::unordered_map<std::string, std::string> aggregation_map_;
    std::vector<GroupingAggregator> aggregators_;

    AggregationClause() = delete;

    ARCTICDB_MOVE_COPY_DEFAULT(AggregationClause)

    AggregationClause(const std::string& grouping_column,
                      const std::unordered_map<std::string,
                      std::string>& aggregations);

    [[nodiscard]] std::vector<Composite<SliceAndKey>> structure_for_processing(
            ARCTICDB_UNUSED const std::vector<SliceAndKey>& slice_and_keys, ARCTICDB_UNUSED size_t start_from) const {
        internal::raise<ErrorCode::E_ASSERTION_FAILURE>(
                "AggregationClause::structure_for_processing should never be called"
                );
    }

    [[nodiscard]] Composite<ProcessingUnit> process(std::shared_ptr<Store> store,
                                                    Composite<ProcessingUnit> &&p) const;

    [[nodiscard]] std::optional<std::vector<Composite<ProcessingUnit>>> repartition(
            ARCTICDB_UNUSED std::vector<Composite<ProcessingUnit>> &&comps
            ) const {
        return std::nullopt;
    }

    [[nodiscard]] const ClauseInfo& clause_info() const {
        return clause_info_;
    }

    void set_processing_config(const ProcessingConfig& processing_config) {
        processing_config_ = processing_config;
    }

    [[nodiscard]] std::string to_string() const;
};

struct RemoveColumnPartitioningClause {
    ClauseInfo clause_info_;
    mutable bool warning_shown = false; // folly::Poly can't deal with atomic_bool

    RemoveColumnPartitioningClause() {
        clause_info_.can_combine_with_column_selection_ = false;
    }
    ARCTICDB_MOVE_COPY_DEFAULT(RemoveColumnPartitioningClause)

    [[nodiscard]] std::vector<Composite<SliceAndKey>> structure_for_processing(
            std::vector<SliceAndKey>& slice_and_keys, size_t start_from) const {
        return structure_by_row_slice(slice_and_keys, start_from);
    }

    [[nodiscard]] Composite<ProcessingUnit> process(std::shared_ptr<Store> store,
                                                    Composite<ProcessingUnit> &&p) const;

    [[nodiscard]] std::optional<std::vector<Composite<ProcessingUnit>>> repartition(
            ARCTICDB_UNUSED std::vector<Composite<ProcessingUnit>> &&) const {
        return std::nullopt;
    }

    [[nodiscard]] const ClauseInfo& clause_info() const {
        return clause_info_;
    }

    void set_processing_config(ARCTICDB_UNUSED const ProcessingConfig& processing_config) {
    }
};

struct SplitClause {
    ClauseInfo clause_info_;
    const size_t rows_;

    explicit SplitClause(size_t rows) :
            rows_(rows) {
    }

    [[nodiscard]] std::vector<Composite<SliceAndKey>> structure_for_processing(
            std::vector<SliceAndKey>& slice_and_keys, size_t start_from) const {
        return structure_by_row_slice(slice_and_keys, start_from);
    }

    [[nodiscard]] Composite<ProcessingUnit> process(std::shared_ptr<Store> store,
                                                    Composite<ProcessingUnit> &&procs) const;

    [[nodiscard]] std::optional<std::vector<Composite<ProcessingUnit>>> repartition(
            ARCTICDB_UNUSED std::vector<Composite<ProcessingUnit>> &&comps) const {
        return std::nullopt;
    }

    [[nodiscard]] const ClauseInfo& clause_info() const {
        return clause_info_;
    }

    void set_processing_config(ARCTICDB_UNUSED const ProcessingConfig& processing_config) {}
};

struct SortClause {
    ClauseInfo clause_info_;
    const std::string column_;

    explicit SortClause(std::string column) :
            column_(std::move(column)) {
    }

    [[nodiscard]] std::vector<Composite<SliceAndKey>> structure_for_processing(
            std::vector<SliceAndKey>& slice_and_keys, size_t start_from) const {
        return structure_by_row_slice(slice_and_keys, start_from);
    }

    [[nodiscard]] Composite<ProcessingUnit> process(std::shared_ptr<Store> store,
                                                    Composite<ProcessingUnit> &&p) const;

    [[nodiscard]] std::optional<std::vector<Composite<ProcessingUnit>>> repartition(
            ARCTICDB_UNUSED std::vector<Composite<ProcessingUnit>> &&comps) const {
        return std::nullopt;
    }

    [[nodiscard]] const ClauseInfo& clause_info() const {
        return clause_info_;
    }

    void set_processing_config(ARCTICDB_UNUSED const ProcessingConfig& processing_config) {}
};

struct MergeClause {
    ClauseInfo clause_info_;
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

    [[nodiscard]] std::vector<Composite<SliceAndKey>> structure_for_processing(
            std::vector<SliceAndKey>& slice_and_keys, size_t start_from) const {
        return structure_by_row_slice(slice_and_keys, start_from);
    }

    [[nodiscard]] Composite<ProcessingUnit> process(std::shared_ptr<Store> store,
                                                    Composite<ProcessingUnit> &&p) const;

    [[nodiscard]] std::optional<std::vector<Composite<ProcessingUnit>>> repartition(
            std::vector<Composite<ProcessingUnit>> &&comps) const;

    [[nodiscard]] const ClauseInfo& clause_info() const {
        return clause_info_;
    }

    void set_processing_config(ARCTICDB_UNUSED const ProcessingConfig& processing_config) {
    }
};

struct ColumnStatsGenerationClause {
    ClauseInfo clause_info_;
    ProcessingConfig processing_config_;
    std::shared_ptr<std::vector<ColumnStatsAggregator>> column_stats_aggregators_;

    explicit ColumnStatsGenerationClause(std::unordered_set<std::string>&& input_columns,
                                         std::shared_ptr<std::vector<ColumnStatsAggregator>> column_stats_aggregators) :
            column_stats_aggregators_(std::move(column_stats_aggregators)) {
        clause_info_.input_columns_ = std::move(input_columns);
        clause_info_.can_combine_with_column_selection_ = false;
        clause_info_.modifies_output_descriptor_ = true;
    }

    ARCTICDB_MOVE_COPY_DEFAULT(ColumnStatsGenerationClause)

    [[nodiscard]] std::vector<Composite<SliceAndKey>> structure_for_processing(
            std::vector<SliceAndKey>& slice_and_keys, size_t start_from) const {
        return structure_by_row_slice(slice_and_keys, start_from);
    }

    [[nodiscard]] Composite<ProcessingUnit> process(std::shared_ptr<Store> store,
                                                    Composite<ProcessingUnit> &&p) const;

    [[nodiscard]] std::optional<std::vector<Composite<ProcessingUnit>>> repartition(
            ARCTICDB_UNUSED std::vector<Composite<ProcessingUnit>> &&) const {
        return std::nullopt;
    }

    [[nodiscard]] const ClauseInfo& clause_info() const {
        return clause_info_;
    }

    void set_processing_config(const ProcessingConfig& processing_config) {
        processing_config_ = processing_config;
    }
};

// Used by head and tail to discard rows not requested by the user
struct RowRangeClause {
    enum class RowRangeType: uint8_t {
        HEAD,
        TAIL
    };

    ClauseInfo clause_info_;
    RowRangeType row_range_type_;
    // As passed into head or tail
    int64_t n_;

    // Row range to keep. Zero-indexed, inclusive of start, exclusive of end
    // Calculated from n, whether the RowRangeType is head or tail, and the total rows as passed in by set_processing_config
    uint64_t start_{0};
    uint64_t end_{0};

    explicit RowRangeClause(RowRangeType row_range_type, int64_t n):
            row_range_type_(row_range_type),
            n_(n) {
    }

    RowRangeClause() = delete;

    ARCTICDB_MOVE_COPY_DEFAULT(RowRangeClause)

    [[nodiscard]] std::vector<Composite<SliceAndKey>> structure_for_processing(
            std::vector<SliceAndKey>& slice_and_keys, ARCTICDB_UNUSED size_t start_from) const;

    [[nodiscard]] Composite<ProcessingUnit> process(std::shared_ptr<Store> store,
                                                    Composite<ProcessingUnit> &&p) const;

    [[nodiscard]] std::optional<std::vector<Composite<ProcessingUnit>>> repartition(
            ARCTICDB_UNUSED std::vector<Composite<ProcessingUnit>> &&) const {
        return std::nullopt;
    }

    [[nodiscard]] const ClauseInfo& clause_info() const {
        return clause_info_;
    }

    void set_processing_config(const ProcessingConfig& processing_config);

    [[nodiscard]] std::string to_string() const;
};

struct DateRangeClause {

    ClauseInfo clause_info_;
    // Time range to keep, inclusive of start and end
    timestamp start_;
    timestamp end_;

    explicit DateRangeClause(timestamp start, timestamp end):
            start_(start),
            end_(end) {
    }

    DateRangeClause() = delete;

    ARCTICDB_MOVE_COPY_DEFAULT(DateRangeClause)

    [[nodiscard]] std::vector<Composite<SliceAndKey>> structure_for_processing(
            std::vector<SliceAndKey>& slice_and_keys, size_t start_from) const;

    [[nodiscard]] Composite<ProcessingUnit> process(std::shared_ptr<Store> store,
                                                    Composite<ProcessingUnit> &&p) const;

    [[nodiscard]] std::optional<std::vector<Composite<ProcessingUnit>>> repartition(
            ARCTICDB_UNUSED std::vector<Composite<ProcessingUnit>> &&) const {
        return std::nullopt;
    }

    [[nodiscard]] const ClauseInfo& clause_info() const {
        return clause_info_;
    }

    void set_processing_config(ARCTICDB_UNUSED const ProcessingConfig& processing_config) {
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
