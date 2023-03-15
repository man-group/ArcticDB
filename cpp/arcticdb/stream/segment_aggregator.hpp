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
#include <arcticdb/pipeline/filter_segment.hpp>

namespace arcticdb::stream {

inline void merge_string_column(
    ChunkedBuffer& src_buffer,
    const std::shared_ptr<StringPool>& src_pool,
    const std::shared_ptr<StringPool>& merged_pool,
    CursoredBuffer<ChunkedBuffer>& output,
    bool verify
    ) {
    using OffsetType = StringPool::offset_t;
    constexpr auto offset_size =  sizeof(OffsetType);
    auto num_strings = src_buffer.bytes() / offset_size;
    for(auto row = size_t(0); row < num_strings; ++row) {
        auto offset = get_offset_string_at(row, src_buffer);
        StringPool::offset_t new_value;
        if (offset != not_a_string() && offset != nan_placeholder()) {
            auto sv = get_string_from_pool(offset, *src_pool);
            new_value = merged_pool->get(sv).offset();
        } else {
            new_value = offset;
        }
        output.ensure<OffsetType>(1);
        util::check(new_value >= 0, "Unexpected negative number {} in merge string column", new_value);
        output.typed_cursor<OffsetType>() = new_value;
        output.commit();
    }
    if (verify) {
        const auto& out_buffer = output.buffer();
        auto num_out = out_buffer.bytes() /offset_size;
        util::check(num_strings == num_out, "Mismatch in input/output size {} != {}", num_strings, num_out);
        for(auto row = size_t(0); row < num_out; ++row) {
            auto offset = get_offset_string_at(row, out_buffer);
            if (offset != not_a_string() && offset != nan_placeholder()) {
                auto sv = get_string_from_pool(offset, *merged_pool);
                // Checking every position is accessible or not
                auto str = std::string(sv);
                log::version().debug("Accessed {} from string pool");
            }
        }
    }
}

inline void merge_string_columns(const SegmentInMemory& segment, const std::shared_ptr<StringPool>& merged_pool, bool verify) {
    for (size_t c = 0; c < segment.descriptor().field_count(); ++c) {
        auto &frame_field = segment.field(c);
        auto field_type = type_desc_from_proto(frame_field.type_desc());

        if (!is_sequence_type(field_type.data_type_))
            continue;

        auto &src = segment.column(static_cast<position_t>(c)).data().buffer();
        CursoredBuffer<ChunkedBuffer> cursor{src.bytes(), false};
        merge_string_column(src, segment.string_pool_ptr(), merged_pool, cursor, verify);
        std::swap(src, cursor.buffer());
    }
}

inline void merge_segments(
    std::vector<SegmentInMemory>& segments,
    SegmentInMemory& merged) {
    ARCTICDB_DEBUG(log::version(), "Appending {} segments", segments.size());
    timestamp min_idx = std::numeric_limits<timestamp>::max();
    timestamp max_idx = std::numeric_limits<timestamp>::min();
    for (auto &segment : segments) {
        std::vector<SegmentInMemory> history{{segment}};
        const auto& latest = *history.rbegin();
        ARCTICDB_DEBUG(log::version(), "Appending segment with {} rows", latest.row_count());
        for(const auto& field : latest.descriptor().fields()) {
            if(!merged.column_index(field.name()))
                merged.add_column(field, 0, false);
        }
        if (latest.row_count() && latest.descriptor().index().type() == IndexDescriptor::TIMESTAMP) {
            min_idx = std::min(min_idx, latest.begin()->begin()->value<timestamp>());
            max_idx = std::max(max_idx, (latest.end() - 1)->begin()->value<timestamp>());
        }
        merge_string_columns(latest, merged.string_pool_ptr(), false);
        merged.append(*history.rbegin());
        merged.set_compacted(true);
        util::print_total_mem_usage(__FILE__, __LINE__, __FUNCTION__);
    }
}

inline pipelines::FrameSlice merge_slices(
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

inline void convert_descriptor_types(StreamDescriptor & descriptor) {
    for(size_t i = 0; i < descriptor.field_count(); ++i) {
        if(is_integer_type(data_type_from_proto(descriptor.field(i).type_desc())))
            set_data_type(DataType::FLOAT64, *descriptor.field(i).mutable_type_desc());
    }
}

inline void convert_column_types(SegmentInMemory& segment) {
    for(const auto& column : segment.columns()) {
        if(is_integer_type(column->type().data_type())) {
            column->change_type(DataType::FLOAT64, std::shared_ptr<StringPool>{});
        }
    }

    convert_descriptor_types(segment.descriptor());
}

template<class Index, class Schema, class SegmentingPolicy = RowCountSegmentPolicy, class DensityPolicy = DenseColumnPolicy>
    class SegmentAggregator : public Aggregator<Index, Schema, SegmentingPolicy, DensityPolicy> {
public:
    using AggregatorType = Aggregator<Index, Schema, SegmentingPolicy, DensityPolicy>;
    using SliceCallBack = folly::Function<void(pipelines::FrameSlice)>;

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

    void commit() override {
        if(segments_.empty())
            return;

        util::check(segments_.size() == slices_.size(), "Segment and slice size mismatch, {} != {}", segments_.size(), slices_.size());
        if(segments_.size() == 1) {
            // One segment, and it could be huge, so don't duplicate it
            AggregatorType::segment() = segments_[0];
            if(!Schema::is_sparse())
                AggregatorType::segment().change_schema(AggregatorType::default_descriptor());
        }
        else {
            AggregatorType::segment().init_column_map();
            merge_segments(segments_, AggregatorType::segment());
        }
        auto slice = merge_slices(slices_, AggregatorType::segment().descriptor());
        if (AggregatorType::segment().row_count() > 0) {
            AggregatorType::commit_impl();
            slice_callback_(slice);
        }
        segments_.clear();
        slices_.clear();
    }

private:
    std::vector<SegmentInMemory> segments_;
    std::vector<pipelines::FrameSlice> slices_;
    SliceCallBack slice_callback_;
};

} // namespace arcticdb