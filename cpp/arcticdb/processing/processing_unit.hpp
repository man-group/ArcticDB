/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <fmt/core.h>
#include <arcticdb/async/task_scheduler.hpp>
#include <arcticdb/column_store/column_algorithms.hpp>
#include <arcticdb/column_store/memory_segment.hpp>
#include <arcticdb/processing/component_manager.hpp>
#include <arcticdb/processing/expression_context.hpp>
#include <arcticdb/processing/expression_node.hpp>
#include <arcticdb/pipeline/frame_slice.hpp>
#include <arcticdb/pipeline/filter_segment.hpp>
#include <unordered_map>
#include <vector>

namespace arcticdb {
enum class PipelineOptimisation : uint8_t { SPEED, MEMORY };

/*
 * A processing unit is designed to be used in conjunction with the clause processing framework.
 * At the start of each clause's process method ProcessingUnits are constructed from the provided entity IDs.
 * All clauses at time of writing need segments, row ranges, and column ranges. Some also require atom keys and
 * the partitioning bucket. In this case the previous clause must have populated these fields in the component
 * manager for the specified entity IDs, otherwise an assertion will fail.
 * At the end of the clause process method, any of these optional fields that are present will be pushed to the
 * component manager.
 * For the components stored in vectors, the vectors must be the same length, and the segment, row range, column
 * range, and atom key that share an index in their respective vectors are associated.
 *
 * In addition, the expression context is a constant, representing the AST for computing expressions in filter and
 * projection clauses.
 * computed_data_ holds a map from a string representation of a [sub-]expression of the AST to a computed value
 * of this expression. This way, if an expression appears twice in the AST, we will only compute it once.
 */
struct ProcessingUnit {
    std::optional<std::vector<std::shared_ptr<SegmentInMemory>>> segments_;
    std::optional<std::vector<std::shared_ptr<pipelines::RowRange>>> row_ranges_;
    std::optional<std::vector<std::shared_ptr<pipelines::ColRange>>> col_ranges_;
    std::optional<std::vector<std::shared_ptr<AtomKey>>> atom_keys_;
    std::optional<bucket_id> bucket_;
    std::optional<std::vector<uint64_t>> entity_fetch_count_;

    std::shared_ptr<ExpressionContext> expression_context_;
    std::unordered_map<std::string, VariantData> computed_data_;

    ProcessingUnit() = default;

    ProcessingUnit(
            SegmentInMemory&& seg, std::optional<pipelines::RowRange>&& row_range = std::nullopt,
            std::optional<pipelines::ColRange>&& col_range = std::nullopt
    ) {
        auto segment_in_memory = std::move(seg);
        auto rows = row_range.value_or(RowRange(0, segment_in_memory.row_count()));
        auto cols = col_range.value_or(ColRange(
                0,
                segment_in_memory.is_null() ? 0
                                            : segment_in_memory.descriptor().field_count() -
                                                      segment_in_memory.descriptor().index().field_count()
        ));
        segments_.emplace({std::make_shared<SegmentInMemory>(std::move(segment_in_memory))});
        row_ranges_.emplace({std::make_shared<pipelines::RowRange>(std::move(rows))});
        col_ranges_.emplace({std::make_shared<pipelines::ColRange>(std::move(cols))});
    }

    void set_segments(std::vector<std::shared_ptr<SegmentInMemory>>&& segments) {
        segments_.emplace(std::move(segments));
    }

    void set_row_ranges(std::vector<std::shared_ptr<pipelines::RowRange>>&& row_ranges) {
        row_ranges_.emplace(std::move(row_ranges));
    }

    void set_col_ranges(std::vector<std::shared_ptr<pipelines::ColRange>>&& col_ranges) {
        col_ranges_.emplace(std::move(col_ranges));
    }

    void set_atom_keys(std::vector<std::shared_ptr<AtomKey>>&& atom_keys) { atom_keys_.emplace(std::move(atom_keys)); }

    void set_bucket(bucket_id bucket) { bucket_.emplace(bucket); }

    void set_entity_fetch_count(std::vector<uint64_t>&& entity_fetch_count) {
        entity_fetch_count_.emplace(entity_fetch_count);
    }

    void apply_filter(util::BitSet&& bitset, PipelineOptimisation optimisation);

    void truncate(size_t start_row, size_t end_row);

    void set_expression_context(const std::shared_ptr<ExpressionContext>& expression_context) {
        expression_context_ = expression_context;
    }

    // The name argument to this function is either a column/value name, or uniquely identifies an ExpressionNode
    // object. If this function has been called before with the same ExpressionNode name, then we cache the result in
    // the computed_data_ map to avoid duplicating work.
    VariantData get(const VariantNode& name);
};

std::vector<ProcessingUnit> split_by_row_slice(ProcessingUnit&& proc);

inline std::vector<pipelines::SliceAndKey> collect_segments(ProcessingUnit&& proc) {
    std::vector<pipelines::SliceAndKey> output;
    internal::check<ErrorCode::E_ASSERTION_FAILURE>(
            proc.segments_.has_value() && proc.row_ranges_.has_value() && proc.col_ranges_.has_value(),
            "collect_segments requires all of segments, row_ranges, and col_ranges to be present"
    );
    for (auto&& [idx, segment] : folly::enumerate(*proc.segments_)) {
        pipelines::FrameSlice frame_slice(*proc.col_ranges_->at(idx), *proc.row_ranges_->at(idx));
        output.emplace_back(std::move(*segment), std::move(frame_slice));
    }

    return output;
}

template<typename Grouper, typename Bucketizer>
std::pair<std::vector<bucket_id>, std::vector<uint64_t>> get_buckets(
        const ColumnWithStrings& col, const Grouper& grouper, const Bucketizer& bucketizer
) {
    // Mapping from row to bucket
    // 255 reserved for Nones and NaNs in string/float columns
    // Faster to initialise to 255 and use a raw ptr for the output than to call emplace_back repeatedly
    std::vector<bucket_id> row_to_bucket(col.column_->last_row() + 1, std::numeric_limits<bucket_id>::max());
    auto out_ptr = row_to_bucket.data();
    // Tracks how many rows are in each bucket
    // Use to skip empty buckets, and presize columns in the output ProcessingUnit
    std::vector<uint64_t> bucket_counts(bucketizer.num_buckets(), 0);

    using TDT = typename Grouper::GrouperDescriptor;

    if (col.column_->is_sparse()) {
        arcticdb::for_each_enumerated<TDT>(
                *col.column_,
                [&] ARCTICDB_LAMBDA_INLINE_PRE(auto enumerating_it)
                        ARCTICDB_LAMBDA_INLINE_MID ARCTICDB_LAMBDA_INLINE_POST {
                            auto opt_group = grouper.group(enumerating_it.value(), col.string_pool_);
                            if (ARCTICDB_LIKELY(opt_group.has_value())) {
                                auto bucket = bucketizer.bucket(*opt_group);
                                row_to_bucket[enumerating_it.idx()] = bucket;
                                ++bucket_counts[bucket];
                            }
                        }
        );
    } else {
        arcticdb::for_each<TDT>(*col.column_, [&](auto val) {
            auto opt_group = grouper.group(val, col.string_pool_);
            if (ARCTICDB_LIKELY(opt_group.has_value())) {
                auto bucket = bucketizer.bucket(*opt_group);
                *out_ptr++ = bucket;
                ++bucket_counts[bucket];
            } else {
                ++out_ptr;
            }
        });
    }
    return {std::move(row_to_bucket), std::move(bucket_counts)};
}

template<typename GrouperType, typename BucketizerType>
std::vector<ProcessingUnit> partition_processing_segment(
        ProcessingUnit& input, const ColumnName& grouping_column_name, bool dynamic_schema
) {

    std::vector<ProcessingUnit> output;
    auto get_result = input.get(ColumnName(grouping_column_name));
    if (std::holds_alternative<ColumnWithStrings>(get_result)) {
        auto partitioning_column = std::get<ColumnWithStrings>(get_result);
        details::visit_scalar(
                partitioning_column.column_->type(),
                [&output, &input, &partitioning_column](auto type_desc_tag) {
                    using TypeDescriptorTag = decltype(type_desc_tag);
                    using DescriptorType = std::decay_t<TypeDescriptorTag>;
                    using TagType = typename DescriptorType::DataTypeTag;
                    using ResolvedGrouperType = typename GrouperType::template Grouper<TypeDescriptorTag>;

                    // Partitioning on an empty column should return an empty composite
                    if constexpr (!is_empty_type(TagType::data_type)) {
                        ResolvedGrouperType grouper;
                        auto num_buckets = ConfigsMap::instance()->get_int(
                                "Partition.NumBuckets", async::TaskScheduler::instance()->cpu_thread_count()
                        );
                        if (num_buckets > std::numeric_limits<bucket_id>::max()) {
                            log::version().warn(
                                    "GroupBy partitioning buckets capped at {} (received {})",
                                    std::numeric_limits<bucket_id>::max(),
                                    num_buckets
                            );
                            num_buckets = std::numeric_limits<bucket_id>::max();
                        }
                        std::vector<ProcessingUnit> procs{static_cast<bucket_id>(num_buckets)};
                        BucketizerType bucketizer(num_buckets);
                        auto [row_to_bucket, bucket_counts] = get_buckets(partitioning_column, grouper, bucketizer);
                        for (auto&& [input_idx, seg] : folly::enumerate(input.segments_.value())) {
                            auto new_segs = partition_segment(*seg, row_to_bucket, bucket_counts);
                            for (auto&& [output_idx, new_seg] : folly::enumerate(new_segs)) {
                                if (bucket_counts.at(output_idx) > 0) {
                                    auto& proc = procs.at(output_idx);
                                    if (!proc.segments_.has_value()) {
                                        proc.segments_ =
                                                std::make_optional<std::vector<std::shared_ptr<SegmentInMemory>>>();
                                        proc.row_ranges_ =
                                                std::make_optional<std::vector<std::shared_ptr<pipelines::RowRange>>>();
                                        proc.col_ranges_ =
                                                std::make_optional<std::vector<std::shared_ptr<pipelines::ColRange>>>();
                                    }
                                    proc.segments_->emplace_back(std::make_shared<SegmentInMemory>(std::move(new_seg)));
                                    proc.row_ranges_->emplace_back(input.row_ranges_->at(input_idx));
                                    proc.col_ranges_->emplace_back(input.col_ranges_->at(input_idx));
                                }
                            }
                        }
                        for (auto&& [idx, proc] : folly::enumerate(procs)) {
                            if (bucket_counts.at(idx) > 0) {
                                proc.bucket_ = idx;
                                output.emplace_back(std::move(proc));
                            }
                        }
                    }
                }
        );
    } else {
        internal::check<ErrorCode::E_ASSERTION_FAILURE>(
                dynamic_schema, "Grouping column missing from row-slice in static schema symbol"
        );
    }
    return output;
}

} // namespace arcticdb