/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/pipeline/frame_utils.hpp>
#include <arcticdb/pipeline/frame_slice.hpp>
#include <arcticdb/entity/protobuf_mappings.hpp>

namespace arcticdb {

TimeseriesDescriptor make_timeseries_descriptor(
        // TODO: It would be more explicit to use uint64_t instead of size_t. Not doing now as it involves a lot of type
        // changes and needs to be done carefully.
        size_t total_rows, const StreamDescriptor& desc,
        arcticdb::proto::descriptors::NormalizationMetadata&& norm_meta,
        std::optional<arcticdb::proto::descriptors::UserDefinedMetadata>&& um, std::optional<AtomKey>&& prev_key,
        std::optional<AtomKey>&& next_key, bool bucketize_dynamic
) {
    auto frame_desc = std::make_shared<FrameDescriptorImpl>();
    frame_desc->total_rows_ = total_rows;
    frame_desc->column_groups_ = bucketize_dynamic;

    auto segment_desc = std::make_shared<SegmentDescriptorImpl>();
    segment_desc->index_ = desc.index();
    segment_desc->sorted_ = desc.sorted();

    auto proto = std::make_shared<TimeseriesDescriptor::Proto>();
    proto->mutable_normalization()->CopyFrom(norm_meta);
    auto user_meta = std::move(um);
    if (user_meta)
        *proto->mutable_user_meta() = std::move(*user_meta);

    if (prev_key)
        proto->mutable_next_key()->CopyFrom(key_to_proto(prev_key.value()));

    if (next_key)
        proto->mutable_next_key()->CopyFrom(key_to_proto(next_key.value()));

    // TODO maybe need ensure_norm_meta?
    return TimeseriesDescriptor{
            std::move(frame_desc), std::move(segment_desc), std::move(proto), desc.fields_ptr(), desc.id()
    };
}

TimeseriesDescriptor timeseries_descriptor_from_pipeline_context(
        const std::shared_ptr<pipelines::PipelineContext>& pipeline_context, std::optional<AtomKey>&& prev_key,
        bool bucketize_dynamic
) {
    return make_timeseries_descriptor(
            pipeline_context->total_rows_,
            pipeline_context->descriptor(),
            std::move(*pipeline_context->norm_meta_),
            pipeline_context->user_meta_ ? std::make_optional<arcticdb::proto::descriptors::UserDefinedMetadata>(
                                                   std::move(*pipeline_context->user_meta_)
                                           )
                                         : std::nullopt,
            std::move(prev_key),
            std::nullopt,
            bucketize_dynamic
    );
}

TimeseriesDescriptor index_descriptor_from_frame(
        const std::shared_ptr<pipelines::InputFrame>& frame, size_t existing_rows,
        std::optional<entity::AtomKey>&& prev_key
) {
    return make_timeseries_descriptor(
            frame->num_rows + existing_rows,
            frame->desc_for_tsd(),
            std::move(frame->norm_meta),
            std::move(frame->user_meta),
            std::move(prev_key),
            std::nullopt,
            frame->bucketize_dynamic
    );
}

size_t adjust_slice_ranges(std::span<pipelines::SliceAndKey> slice_and_keys) {
    if (slice_and_keys.empty())
        return 0;
    // Row and Col ranges input can be disjoint, "compress" them into the top left corner
    // e.g.
    //    1 3  6 9
    //    ---  ---
    // 3 |   ||   |
    // 5 |   ||   |
    //    1 3  4 7
    //    ---  ---
    // 7 |   ||   |
    // 8 |   ||   |
    //    ---  ---
    // becomes
    //    0 2  2 5
    //    ---  ---
    // 0 |   ||   |
    // 2 |   ||   |
    //    0 2  2 5
    //    ---  ---
    // 2 |   ||   |
    // 3 |   ||   |
    //    ---  ---
    bool increment_row_slice{false};
    size_t row_offset{0};
    size_t col_offset{0};
    for (auto it = slice_and_keys.begin(); it != slice_and_keys.end(); ++it) {
        auto& slice = it->slice();
        if (std::next(it) != slice_and_keys.end()) {
            auto& next_slice = std::next(it)->slice();
            // Hash groupings produce slices that all have row ranges starting at 0 and the same col ranges, hence the
            // second condition
            increment_row_slice = next_slice.row_range.first != slice.row_range.first ||
                                  next_slice.col_range.first <= slice.col_range.first;
        } else {
            increment_row_slice = true;
        }
        slice.row_range = pipelines::RowRange(row_offset, row_offset + slice.rows().diff());
        slice.col_range = pipelines::ColRange(col_offset, col_offset + slice.columns().diff());
        col_offset += slice.columns().diff();
        if (increment_row_slice) {
            row_offset += slice.rows().diff();
            col_offset = 0;
        }
    }
    return row_offset;
}

void adjust_slice_ranges(const std::shared_ptr<pipelines::PipelineContext>& pipeline_context) {
    using namespace arcticdb::pipelines;
    auto& slice_and_keys = pipeline_context->slice_and_keys_;
    pipeline_context->total_rows_ = adjust_slice_ranges(slice_and_keys);
}

size_t adjust_slice_rowcounts(std::vector<pipelines::SliceAndKey>& slice_and_keys) {
    using namespace arcticdb::pipelines;
    if (slice_and_keys.empty())
        return 0u;

    auto offset = 0;
    auto diff = slice_and_keys[0].slice_.row_range.diff();
    auto col_begin = slice_and_keys[0].slice_.col_range.first;

    for (auto it = slice_and_keys.begin(); it != slice_and_keys.end(); ++it) {
        if (it != slice_and_keys.begin() && it->slice_.col_range.first == col_begin) {
            offset += diff;
            diff = it->slice_.row_range.diff();
        }
        it->slice_.row_range = RowRange{offset, offset + diff};
    }
    return offset + diff;
}

size_t get_slice_rowcounts(std::vector<pipelines::SliceAndKey>& slice_and_keys) {
    if (slice_and_keys.empty()) {
        return 0;
    }
    auto current_col = slice_and_keys[0].slice_.col_range.first;
    size_t rowcount = 0u;
    for (auto& slice_and_key : slice_and_keys) {
        if (slice_and_key.slice_.col_range.first != current_col) {
            rowcount = 0u;
            current_col = slice_and_key.slice_.col_range.first;
        }
        size_t rows = slice_and_key.slice_.row_range.diff();
        rowcount += rows;
    }
    return rowcount;
}

std::pair<size_t, size_t> offset_and_row_count(const std::shared_ptr<pipelines::PipelineContext>& context) {
    // count rows
    std::size_t row_count = 0ULL;
    for (auto s = 0u; s < context->slice_and_keys_.size(); ++s) {
        if (context->fetch_index_[s]) {
            row_count += context->slice_and_keys_[s].slice_.row_range.diff();
            ARCTICDB_DEBUG(log::version(), "Adding {} rows", context->slice_and_keys_[s].slice_.row_range.diff());
        } else {
            ARCTICDB_DEBUG(
                    log::version(),
                    "Fetch index false for this slice, would have added {} rows",
                    context->slice_and_keys_[s].slice_.row_range.diff()
            );
        }
    }

    std::size_t offset = row_count ? context->slice_and_keys_[0].slice_.row_range.first : 0ULL;
    ARCTICDB_DEBUG(log::version(), "Got offset {} and row_count {}", offset, row_count);
    return std::make_pair(offset, row_count);
}

std::vector<size_t> output_block_row_counts(const std::shared_ptr<pipelines::PipelineContext>& context) {
    std::vector<size_t> output;
    output.reserve(context->slice_and_keys_.size());
    for (auto s = 0u; s < context->slice_and_keys_.size(); ++s) {
        if (context->fetch_index_[s])
            output.emplace_back(context->slice_and_keys_[s].slice_.row_range.diff());
    }
    return output;
}

bool index_is_not_timeseries_or_is_sorted_ascending(const pipelines::InputFrame& frame) {
    return !std::holds_alternative<stream::TimeseriesIndex>(frame.index) ||
           frame.desc().sorted() == SortedValue::ASCENDING;
}

} // namespace arcticdb
