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
        SegmentingPolicy &&segmenting_policy = SegmentingPolicy{},
        std::optional<SegmentInMemory>&& last_committed_segment_ = std::nullopt) :
        AggregatorType(std::move(schema), std::move(c), std::move(segmenting_policy)),
        slice_callback_(std::move(slice_callback)),
        dedup_rows_(false),
        last_committed_segment_(std::move(last_committed_segment_)),
        previous_reduction_in_size_(0) {
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

    void set_dedup_rows(bool dedup_rows) {
        dedup_rows_ = dedup_rows;
    }

    void commit() override {
        if(segments_.empty())
            return;

        util::check(segments_.size() == slices_.size(), "Segment and slice size mismatch, {} != {}", segments_.size(), slices_.size());
        std::vector<std::size_t> segments_final_sizes;
        if(segments_.size() == 1 && !dedup_rows_) {
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
            merge_segments(std::move(segments_), AggregatorType::segment(), dedup_rows_, last_committed_segment_, segments_final_sizes, DensityPolicy::allow_sparse);
        }

        if (AggregatorType::segment().row_count() > 0) {
            auto [slice, new_reduction_in_size] = merge_slices(slices_, AggregatorType::segment().descriptor(), dedup_rows_, segments_final_sizes, previous_reduction_in_size_);
            previous_reduction_in_size_ = new_reduction_in_size;
            last_committed_segment_ = AggregatorType::segment().clone();
            AggregatorType::commit_impl(false);
            slice_callback_(std::move(slice));
        }
        segments_.clear();
        slices_.clear();
    }

private:
    std::vector<SegmentInMemory> segments_;
    std::vector<pipelines::FrameSlice> slices_;
    SliceCallBack slice_callback_;
    bool dedup_rows_;
    std::optional<SegmentInMemory> last_committed_segment_;
    std::optional<StreamDescriptor> stream_descriptor_;
    ssize_t previous_reduction_in_size_;
};

} // namespace arcticdb