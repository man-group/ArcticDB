/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <optional>
#include <arcticdb/pipeline/index_segment_reader.hpp>
#include <arcticdb/pipeline/pipeline_context.hpp>
#include <arcticdb/pipeline/read_frame.hpp>
#include <arcticdb/version/read_version_output.hpp>

namespace arcticdb {

std::pair<int, int> row_range_to_positive_bounds(
    const std::optional<std::pair<std::optional<int>, std::optional<int>>>& row_range,
    int row_count) {
    if (!row_range) {
        return {0, row_count};
    }

    int row_range_start = row_range->first.value_or(0);
    int row_range_end = row_range->second.value_or(row_count);

    if (row_range_start < 0) {
        row_range_start += row_count;
    }
    if (row_range_end < 0) {
        row_range_end += row_count;
    }

    return {row_range_start, row_range_end};
}

class ChunkIterator {
    pipelines::index::IndexSegmentReader index_segment_reader_;
    std::shared_ptr<pipelines::PipelineContext> pipeline_context_;
    AtomKey index_key_;
    std::shared_ptr<Store> store_;
    std::any handler_data_;
    DecodePathData shared_data_;
    ReadQuery read_query_;
    ReadOptions read_options_;
    size_t row_pos_ = 0;


    ChunkIterator(
        pipelines::index::IndexSegmentReader&& index_segment_reader,
        std::shared_ptr<pipelines::PipelineContext> pipeline_context,
        std::shared_ptr<Store> store,
        ReadQuery& read_query,
        const ReadOptions& read_options,
        std::any& handler_data,
        DecodePathData shared_data) :
        index_segment_reader_(std::move(index_segment_reader)),
        pipeline_context_(std::move(pipeline_context)),
        store_(store),
        handler_data_(handler_data),
        shared_data_(shared_data),
        read_query_(read_query),
        read_options_(read_options){
    }

    std::optional<ReadResult> next() {
        if(row_pos_ == pipeline_context_->slice_and_keys_.size())
            return std::nullopt;

        auto local_context = std::make_shared<PipelineContext>();
        auto previous_row_range = pipeline_context_->slice_and_keys_[row_pos_].slice_.row_range;
        auto current_row_range = previous_row_range;
        while (previous_row_range == current_row_range) {
            local_context->slice_and_keys_.emplace_back(pipeline_context_->slice_and_keys_[row_pos_]);
            local_context->fetch_index_.set_bit(row_pos_, pipeline_context_->fetch_index_[row_pos_]);
            ++row_pos_;
            current_row_range = pipeline_context_->slice_and_keys_[row_pos_].slice_.row_range;
        }

        auto frame = allocate_frame(local_context);
        auto total_rows = frame.row_count();
        auto output = fetch_data(frame, local_context, store_, read_options_.dynamic_schema_, shared_data_, handler_data_).thenValue(
            [this, local_context, frame](auto &&) mutable {
                reduce_and_fix_columns(local_context, frame, read_options_, handler_data_);
            }).thenValue(
            [this, frame, local_context, total_rows](auto &&) mutable {
                set_row_id_for_empty_columns_set(read_query_, *pipeline_context_, frame, total_rows);
                return ReadVersionOutput{VersionedItem{to_atom(index_key_)},
                                         FrameAndDescriptor{frame, std::move(index_segment_reader->mutable_tsd()), {}, shared_data.buffers()}};
            });
    }
};

} // namespace arcticdb