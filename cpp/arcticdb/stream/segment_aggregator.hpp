/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/stream/aggregator.hpp>
#include <arcticdb/pipeline/frame_utils.hpp>
#include <arcticdb/util/format_date.hpp>
#include <arcticdb/util/memory_tracing.hpp>
#include <arcticdb/pipeline/filter_segment.hpp>
#include <arcticdb/stream/merge_utils.hpp>

namespace arcticdb::stream {

inline void convert_descriptor_types(StreamDescriptor & descriptor) {
    for(size_t i = 0; i < descriptor.field_count(); ++i) {
        if(is_integer_type(descriptor.field(i).type().data_type()))
            set_data_type(DataType::FLOAT64, descriptor.mutable_field(i).mutable_type());
    }
}

inline void convert_column_types(SegmentInMemory& segment) {
    for(const auto& column : segment.columns()) {
        if(is_integer_type(column->type().data_type())) {
            column->change_type(DataType::FLOAT64);
        }
    }

    convert_descriptor_types(segment.descriptor());
}

template<class Index, class Schema, class SegmentingPolicy = RowCountSegmentPolicy, class DensityPolicy = DenseColumnPolicy>
    class SegmentAggregator : public Aggregator<Index, Schema, SegmentingPolicy, DensityPolicy> {
public:
    using AggregatorType = Aggregator<Index, Schema, SegmentingPolicy, DensityPolicy>;
    using SliceCallBack = folly::Function<void(pipelines::FrameSlice&&)>;

    SegmentAggregator(
        SliceCallBack&& slice_callback,
        Schema &&schema,
        typename AggregatorType::Callback &&c,
        SegmentingPolicy &&segmenting_policy = SegmentingPolicy{}) :
        AggregatorType(std::move(schema), std::move(c), std::move(segmenting_policy)),
        slice_callback_(std::move(slice_callback)) {
    }

    void add_segment(SegmentInMemory&& seg, const pipelines::FrameSlice& slice, bool convert_int_to_float) {
        auto segment = std::move(seg);
        if constexpr (std::is_same_v<Schema, FixedSchema>) {
            if (stream_descriptor_.has_value()) {
                schema::check<ErrorCode::E_DESCRIPTOR_MISMATCH>(
                        segment.descriptor().fields() == stream_descriptor_->fields(),
                        "Stream descriptor mismatch when compacting segments with static schema");
            } else {
                stream_descriptor_ = segment.descriptor();
            }
        }
        segment.reset_timeseries_descriptor();
        AggregatorType::stats().update_many(segment.row_count(), segment.num_bytes());
        //TODO very specific use-case, you probably don't want this
        if(convert_int_to_float)
            convert_column_types(segment);

        ARCTICDB_DEBUG(log::version(), "Adding segment with descriptor {}", segment.descriptor());
        segments_.push_back(segment);
        slices_.push_back(slice);
        if (AggregatorType::segmenting_policy()(AggregatorType::stats())) {
            commit();
        }
        util::print_total_mem_usage(__FILE__, __LINE__, __FUNCTION__);
    }

    void finalize() override {
        commit();
    }

    virtual void commit() override {
        if(segments_.empty())
            return;

        util::check(segments_.size() == slices_.size(), "Segment and slice size mismatch, {} != {}", segments_.size(), slices_.size());
        if(segments_.size() == 1) {
            // One segment, and it could be huge, so don't duplicate it
            AggregatorType::segment() = segments_[0];
            if(!DensityPolicy::allow_sparse){ //static schema must have all columns as column slicing is removed
                auto descriptor = AggregatorType::default_descriptor();
                for(const auto& field : AggregatorType::segment().fields()) {//segment's index is not set up here
                    if(!descriptor.find_field(field.name())){//TODO: Bottleneck for wide segments
                        descriptor.add_field(field);//dynamic schema's default descriptor has no data column
                    }
                }
                AggregatorType::segment().change_schema(descriptor);
            }
        }
        else {
            AggregatorType::segment().init_column_map();
            merge_segments(segments_, AggregatorType::segment(), DensityPolicy::allow_sparse);
        }

        if (AggregatorType::segment().row_count() > 0) {
            auto slice = merge_slices(slices_, AggregatorType::segment().descriptor());
            AggregatorType::commit_impl(false);
            slice_callback_(std::move(slice));
        }
        segments_.clear();
        slices_.clear();
    }

protected:
    std::vector<SegmentInMemory> segments_;
    std::vector<pipelines::FrameSlice> slices_;
    SliceCallBack slice_callback_;

private:
    std::optional<StreamDescriptor> stream_descriptor_;

    virtual void merge_segments(
        std::vector<SegmentInMemory>& segments,
        SegmentInMemory& merged,
        bool is_sparse) {
        ARCTICDB_DEBUG(log::version(), "Appending {} segments", segments.size());
        timestamp min_idx = std::numeric_limits<timestamp>::max();
        timestamp max_idx = std::numeric_limits<timestamp>::min();
        for (auto &segment : segments) {
            ARCTICDB_DEBUG(log::version(), "Appending segment with {} rows", segment.row_count());
            for(const auto& field : segment.descriptor().fields()) {
                if(!merged.column_index(field.name())){//TODO: Bottleneck for wide segments
                    auto pos = merged.add_column(field, 0, false);
                    if (!is_sparse){
                        merged.column(pos).mark_absent_rows(merged.row_count());
                    }
                }
            }

            if (segment.row_count() && segment.descriptor().index().type() == IndexDescriptorImpl::Type::TIMESTAMP) {
                min_idx = std::min(min_idx, segment.begin()->begin()->value<timestamp>());
                max_idx = std::max(max_idx, (segment.end() - 1)->begin()->value<timestamp>());
            }

            merge_string_columns(segment, merged.string_pool_ptr(), false);
            merged.append(segment);
            merged.set_compacted(true);
            util::print_total_mem_usage(__FILE__, __LINE__, __FUNCTION__);
        }
    }
    virtual pipelines::FrameSlice merge_slices(
        std::vector<pipelines::FrameSlice>& slices,
        const StreamDescriptor& desc) {
        util::check(!slices.empty(), "Expected to merge non-empty slices_vector");

        pipelines::FrameSlice output{slices[0]};
        for(const auto& slice : slices) {
            output.row_range.first = std::min(output.row_range.first, slice.row_range.first);
            output.row_range.second = std::max(output.row_range.second, slice.row_range.second);
        }

        output.col_range.first = desc.index().field_count();
        output.col_range.second = desc.field_count();
        return output;
    }
};

} // namespace arcticdb