/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/pipeline/frame_utils.hpp>
#include <arcticdb/stream/aggregator.hpp>
#include <arcticdb/pipeline/frame_slice.hpp>

namespace arcticdb {

arcticdb::proto::descriptors::TimeSeriesDescriptor get_timeseries_descriptor(
    const StreamDescriptor& descriptor,
    size_t total_rows,
    const std::optional<AtomKey>& next_key,
    arcticdb::proto::descriptors::NormalizationMetadata&& norm_meta) {

    arcticdb::proto::descriptors::TimeSeriesDescriptor tsd;
    tsd.set_total_rows(total_rows);
    if(next_key)
        tsd.mutable_next_key()->CopyFrom(encode_key(next_key.value()));

    tsd.mutable_normalization()->CopyFrom(norm_meta);

    tsd.mutable_stream_descriptor()->CopyFrom(descriptor.proto());
    return tsd;
}

google::protobuf::Any pack_timeseries_descriptor(
        StreamDescriptor&& descriptor,
        size_t total_rows,
        const std::optional<AtomKey>& next_key,
        arcticdb::proto::descriptors::NormalizationMetadata&& norm_meta) {

    auto tsd = get_timeseries_descriptor(descriptor, total_rows, next_key, std::move(norm_meta));
    google::protobuf::Any any;
    any.PackFrom(tsd);
    return any;
}

SegmentInMemory segment_from_frame(
        pipelines::InputTensorFrame&& frame,
        size_t existing_rows,
        std::optional<entity::AtomKey>&& prev_key,
        bool allow_sparse
) {
    using namespace arcticdb::stream;

    auto offset_in_frame = 0;
    auto slice_num_for_column = 0;
    const auto num_rows = frame.num_rows;
    auto index_tensor = std::move(frame.index_tensor);
    const bool has_index = frame.has_index();
    const auto index = std::move(frame.index);
    SegmentInMemory output;
    auto field_tensors = std::move(frame.field_tensors);

    std::visit([&](const auto& idx) {
        using IdxType = std::decay_t<decltype(idx)>;
        using SingleSegmentAggregator = Aggregator<IdxType, FixedSchema, NeverSegmentPolicy>;

        auto timeseries_desc = descriptor_from_frame(pipelines::InputTensorFrame{frame}, existing_rows, std::move(prev_key));
        auto norm_meta = timeseries_desc.normalization();
        StreamDescriptor descriptor(std::move(*timeseries_desc.mutable_stream_descriptor()));
        SingleSegmentAggregator agg{FixedSchema{descriptor, idx}, [&](auto&& segment) {
            auto any = pack_timeseries_descriptor(std::move(descriptor), existing_rows + num_rows, prev_key, std::move(norm_meta));
            segment.set_metadata(std::move(any));
            output = std::forward<SegmentInMemory>(segment);
        }};

        if (has_index) {
            util::check(static_cast<bool>(index_tensor), "Expected index tensor for index type {}", agg.descriptor().index());
            aggregator_set_data(type_desc_from_proto(agg.descriptor().field(0).type_desc()), index_tensor.value(), agg, 0, num_rows, offset_in_frame, slice_num_for_column,
                                num_rows, allow_sparse);
        }

        for(auto col = 0u; col < field_tensors.size(); ++col) {
            auto dest_col = col + agg.descriptor().index().field_count();
            auto &tensor = field_tensors[col];
            aggregator_set_data(type_desc_from_proto(agg.descriptor().field(dest_col).type_desc()), tensor, agg, dest_col, num_rows, offset_in_frame, slice_num_for_column,
                                num_rows, allow_sparse);
        }

        agg.end_block_write(num_rows);
        agg.commit();
    }, index);

    ARCTICDB_DEBUG(log::version(), "Constructed segment from frame of {} rows and {} columns at offset {}", output.row_count(), output.num_columns(), output.offset());
    return output;
}

arcticdb::proto::descriptors::TimeSeriesDescriptor make_descriptor(
        size_t rows,
        StreamDescriptor&& desc,
        arcticdb::proto::descriptors::NormalizationMetadata& norm_meta,
        std::optional<arcticdb::proto::descriptors::UserDefinedMetadata>&& um,
        std::optional<AtomKey>&& prev_key,
        bool bucketize_dynamic
    ) {

    arcticdb::proto::descriptors::TimeSeriesDescriptor metadata;
    metadata.set_total_rows(rows);
    *metadata.mutable_stream_descriptor() = desc.proto();
    metadata.mutable_normalization()->CopyFrom(norm_meta);
    auto user_meta = std::move(um);
    if(user_meta)
        *metadata.mutable_user_meta() = std::move(*user_meta);

    if(prev_key) {
        auto pb_key = encode_key(prev_key.value());
        *metadata.mutable_next_key() = std::move(pb_key);
    }
    if(bucketize_dynamic)
        metadata.mutable_column_groups()->set_enabled(true);

    //TODO maybe need ensure_norm_meta?
    return metadata;
}


arcticdb::proto::descriptors::TimeSeriesDescriptor make_descriptor(
    size_t rows,
    pipelines::index::IndexSegmentReader&& index_segment_reader,
    std::optional<AtomKey>&& prev_key,
    bool bucketize_dynamic
) {
    return make_descriptor(
        rows,
        StreamDescriptor{std::move(*index_segment_reader.mutable_tsd().mutable_stream_descriptor())},
        *index_segment_reader.mutable_tsd().mutable_normalization(),
        std::move(*index_segment_reader.mutable_tsd().mutable_user_meta()),
        std::move(prev_key),
        bucketize_dynamic);
}
arcticdb::proto::descriptors::TimeSeriesDescriptor make_descriptor(
    const std::shared_ptr<pipelines::PipelineContext>& pipeline_context,
    std::optional<AtomKey>&& prev_key,
    bool bucketize_dynamic) {
    return make_descriptor(
        pipeline_context->total_rows_,
        StreamDescriptor{std::move(pipeline_context->desc_->mutable_proto())},
        *pipeline_context->norm_meta_,
        pipeline_context->user_meta_ ? std::make_optional<arcticdb::proto::descriptors::UserDefinedMetadata>(std::move(*pipeline_context->user_meta_)) : std::nullopt,
        std::move(prev_key),
        bucketize_dynamic
        );
}
arcticdb::proto::descriptors::TimeSeriesDescriptor descriptor_from_frame(
        pipelines::InputTensorFrame&& frame,
        size_t existing_rows,
        std::optional<entity::AtomKey>&& prev_key
) {
    return make_descriptor(
        frame.num_rows + existing_rows,
        StreamDescriptor{std::move(frame.desc.mutable_proto())},
        frame.norm_meta,
        std::move(frame.user_meta),
        std::move(prev_key),
        frame.bucketize_dynamic);
}

void adjust_slice_rowcounts(const std::shared_ptr<pipelines::PipelineContext>& pipeline_context) {
    if(pipeline_context->slice_and_keys_.empty())
        return;

    pipeline_context->total_rows_ = adjust_slice_rowcounts(pipeline_context->slice_and_keys_);
}

size_t adjust_slice_rowcounts(std::vector<pipelines::SliceAndKey> & slice_and_keys) {
    using namespace arcticdb::pipelines;
    auto current_col = slice_and_keys[0].slice_.col_range.first;
    size_t offset = 0u;
    for (auto& slice_and_key : slice_and_keys) {
        if (slice_and_key.slice_.col_range.first != current_col) {
            offset = 0u;
            current_col = slice_and_key.slice_.col_range.first;
        }
        size_t rows = slice_and_key.slice_.row_range.diff();
        RowRange updated{offset, offset + rows};
        offset += rows;
        slice_and_key.slice_.row_range = updated;
    }
    return offset;
}

size_t get_slice_rowcounts(std::vector<pipelines::SliceAndKey> & slice_and_keys) {
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
    for(auto s = 0u; s < context->slice_and_keys_.size(); ++s) {
        if (context->fetch_index_[s]) {
            row_count += context->slice_and_keys_[s].slice_.row_range.diff();
            ARCTICDB_DEBUG(log::version(), "Adding {} rows", context->slice_and_keys_[s].slice_.row_range.diff());
        } else {
            ARCTICDB_DEBUG(log::version(), "Fetch index false for this slice, would have added {} rows", context->slice_and_keys_[s].slice_.row_range.diff());
        }
    }

    std::size_t offset = row_count ? context->slice_and_keys_[0].slice_.row_range.first : 0ULL;
    ARCTICDB_DEBUG(log::version(), "Got offset {} and row_count {}", offset, row_count);
    return std::make_pair(offset, row_count);
}

}
