/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <unordered_map>
#include <vector>

#include <fmt/core.h>

#include <arcticdb/async/task_scheduler.hpp>
#include <arcticdb/column_store/memory_segment.hpp>
#include <arcticdb/processing/expression_context.hpp>
#include <arcticdb/processing/expression_node.hpp>
#include <arcticdb/pipeline/frame_slice.hpp>
#include <arcticdb/pipeline/filter_segment.hpp>
#include <arcticdb/util/composite.hpp>
#include <arcticdb/util/string_utils.hpp>
#include <arcticdb/util/variant.hpp>

namespace arcticdb {
    enum class PipelineOptimisation : uint8_t {
        SPEED,
        MEMORY
    };

    /*
     * A processing unit is designed to be used in conjunction with the clause processing framework.
     * Each clause will execute in turn and will be passed the same mutable ProcessingUnit which it will have free
     * rein to modify prior to passing off to the next clause. In contract, clauses and the expression trees contained
     * within are immutable as they represent predefined behaviour.
     *
     * ProcessingUnit contains two main member attributes:
     *
     * ExpressionContext: This contains the expression tree for the currently running clause - each clause contains an
     * expression tree containing nodes that map to columns, constant values or other nodes in the tree. This allows
     * for the processing of expressions such as `(df['a'] + 4) < df['b']`. One instance of an expression context can be
     * shared across many ProcessingUnit's. See ExpressionContext class for more documentation.
     *
     * computed_data_: This contains a map from node name to previously computed result. Should an expression such as
     * df['col1'] + 1 appear multiple times in the tree this allows us to only perform the computation once and spill
     * the result to disk.
     */
    struct ProcessingUnit {
        // We want a collection of SliceAndKeys here so that we have all of the columns for a given row present in a single
        // ProcessingUnit, for 2 reasons:
        // 1: For binary operations in ExpressionNode involving 2 columns, we need both available in a single processing
        //    segment in order for "get" calls to make sense
        // 2: As "get" uses cached results where possible, it is not obvious which processing unit should hold the
        //    cached result for operations involving 2 columns (probably both). By moving these cached results up a level
        //    to contain all the information for a given row, this becomes unambiguous.
        std::vector<pipelines::SliceAndKey> data_;
        std::shared_ptr<ExpressionContext> expression_context_;
        std::unordered_map<std::string, VariantData> computed_data_;

        // Set by PartitioningClause
        std::optional<size_t> bucket_;

        ProcessingUnit() = default;

        ProcessingUnit(SegmentInMemory &&seg,
                       pipelines::FrameSlice&& slice,
                       std::optional<size_t> bucket = std::nullopt) :
                bucket_(bucket) {
            data_.emplace_back(pipelines::SliceAndKey{std::move(seg), std::move(slice)});
        }

        explicit ProcessingUnit(pipelines::SliceAndKey &&sk,
                                std::optional<size_t> bucket = std::nullopt) :
                bucket_(bucket) {
            data_.emplace_back(pipelines::SliceAndKey{std::move(sk)});
        }

        explicit ProcessingUnit(std::vector<pipelines::SliceAndKey> &&sk,
                                std::optional<size_t> bucket = std::nullopt) :
                bucket_(bucket) {
            data_ = std::move(sk);
        }

        explicit ProcessingUnit(SegmentInMemory &&seg,
                                std::optional<size_t> bucket = std::nullopt) :
                bucket_(bucket) {
            data_.emplace_back(pipelines::SliceAndKey{std::move(seg)});
        }

        std::vector<pipelines::SliceAndKey>&& release_data() {
            return std::move(data_);
        }

        std::vector<pipelines::SliceAndKey>& data() {
            return data_;
        }

        const std::vector<pipelines::SliceAndKey>& data() const {
            return data_;
        }

        ProcessingUnit &self() {
            return *this;
        }

        size_t get_bucket(){
            internal::check<ErrorCode::E_ASSERTION_FAILURE>(bucket_.has_value(),
                                                            "ProcessingUnit::get_bucket called, but bucket_ has no value");
            return *bucket_;
        }

        void set_bucket(size_t bucket) {
            bucket_ = bucket;
        }

        void apply_filter(const util::BitSet& bitset, const std::shared_ptr<Store>& store, PipelineOptimisation optimisation);

        void truncate(size_t start_row, size_t end_row, const std::shared_ptr<Store>& store);

        void set_expression_context(const std::shared_ptr<ExpressionContext>& expression_context) {
            expression_context_ = expression_context;
        }

        // The name argument to this function is either a column/value name, or uniquely identifies an ExpressionNode object.
        // If this function has been called before with the same ExpressionNode name, then we cache the result in the
        // computed_data_ map to avoid duplicating work.
        VariantData get(const VariantNode &name, const std::shared_ptr<Store> &store);
    };

    inline std::vector<pipelines::SliceAndKey> collect_segments(Composite<ProcessingUnit>&& p) {
        auto procs = std::move(p);
        std::vector<pipelines::SliceAndKey> output;

        procs.broadcast([&output] (auto&& p) {
            auto proc = std::forward<ProcessingUnit>(p);
            auto slice_and_keys = proc.release_data();
            std::move(std::begin(slice_and_keys), std::end(slice_and_keys), std::back_inserter(output));
        });

        return output;
    }

    template<typename Grouper, typename Bucketizer>
    std::pair<std::vector<std::optional<uint8_t>>, std::vector<uint64_t>> get_buckets(
            const ColumnWithStrings& col,
            Grouper& grouper,
            Bucketizer& bucketizer) {
        schema::check<ErrorCode::E_UNSUPPORTED_COLUMN_TYPE>(!col.column_->is_sparse(),
                                                            "GroupBy not supported with sparse columns");
        auto input_data = col.column_->data();
        // Mapping from row to bucket
        // std::nullopt only for Nones and NaNs in string/float columns
        std::vector<std::optional<uint8_t>> row_to_bucket;
        row_to_bucket.reserve(col.column_->row_count());
        // Tracks how many rows are in each bucket
        // Use to skip empty buckets, and presize columns in the output ProcessingUnit
        std::vector<uint64_t> bucket_counts(bucketizer.num_buckets(), 0);

        using TypeDescriptorTag = typename Grouper::GrouperDescriptor;
        using RawType = typename TypeDescriptorTag::DataTypeTag::raw_type;

        while (auto block = input_data.next<TypeDescriptorTag>()) {
            const auto row_count = block->row_count();
            auto ptr = reinterpret_cast<const RawType*>(block->data());
            for(auto i = 0u; i < row_count; ++i, ++ptr){
                if constexpr(std::is_same_v<typename Grouper::GrouperDescriptor, TypeDescriptorTag>) {
                    auto opt_group = grouper.group(*ptr, col.string_pool_);
                    if (opt_group.has_value()) {
                        auto bucket = bucketizer.bucket(*opt_group);
                        row_to_bucket.emplace_back(bucket);
                        ++bucket_counts[bucket];
                    } else {
                        row_to_bucket.emplace_back(std::nullopt);
                    }
                }
            }
        }
        return {std::move(row_to_bucket), std::move(bucket_counts)};
    }

    template<typename GrouperType, typename BucketizerType>
    Composite<ProcessingUnit> partition_processing_segment(
            const std::shared_ptr<Store>& store,
            ProcessingUnit& input,
            const ColumnName& grouping_column_name,
            bool dynamic_schema) {

        Composite<ProcessingUnit> output;
        auto get_result = input.get(ColumnName(grouping_column_name), store);
        if (std::holds_alternative<ColumnWithStrings>(get_result)) {
            auto partitioning_column = std::get<ColumnWithStrings>(get_result);
            partitioning_column.column_->type().visit_tag([&output, &input, &partitioning_column, &store](auto type_desc_tag) {
                using TypeDescriptorTag = decltype(type_desc_tag);
                using DescriptorType = std::decay_t<TypeDescriptorTag>;
                using TagType =  typename DescriptorType::DataTypeTag;
                using ResolvedGrouperType = typename GrouperType::template Grouper<TypeDescriptorTag>;

                // Partitioning on an empty column should return an empty composite
                if constexpr(!is_empty_type(TagType::data_type)) {
                    ResolvedGrouperType grouper;
                    auto num_buckets = ConfigsMap::instance()->get_int("Partition.NumBuckets",
                                                                       async::TaskScheduler::instance()->cpu_thread_count());
                    if (num_buckets > std::numeric_limits<uint8_t>::max()) {
                        log::version().warn("GroupBy partitioning buckets capped at {} (received {})",
                                            std::numeric_limits<uint8_t>::max(),
                                            num_buckets);
                        num_buckets = std::numeric_limits<uint8_t>::max();
                    }
                    std::vector<ProcessingUnit> procs{static_cast<size_t>(num_buckets)};
                    BucketizerType bucketizer(num_buckets);
                    auto [row_to_bucket, bucket_counts] = get_buckets(partitioning_column, grouper, bucketizer);
                    for (auto &seg_slice_and_key: input.data()) {
                        const SegmentInMemory &seg = seg_slice_and_key.segment(store);
                        auto new_segs = partition_segment(seg, row_to_bucket, bucket_counts);
                        for (auto &&new_seg: folly::enumerate(new_segs)) {
                            if (bucket_counts.at(new_seg.index) > 0) {
                                pipelines::FrameSlice new_slice{seg_slice_and_key.slice()};
                                new_slice.adjust_rows(new_seg->row_count());
                                procs.at(new_seg.index).data().emplace_back(
                                        pipelines::SliceAndKey{std::move(*new_seg), std::move(new_slice)});
                            }
                        }
                    }
                    for (auto &&proc: folly::enumerate(procs)) {
                        if (bucket_counts.at(proc.index) > 0) {
                            proc->set_bucket(proc.index);
                            output.push_back(std::move(*proc));
                        }
                    }
                }
            });
        } else {
            internal::check<ErrorCode::E_ASSERTION_FAILURE>(
                    dynamic_schema,
                    "Grouping column missing from row-slice in static schema symbol"
            );
        }
        return output;
    }

} //namespace arcticdb