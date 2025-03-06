/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#include <optional>
#include <arcticdb/pipeline/chunking.hpp>
#include <arcticdb/pipeline/read_frame.hpp>

namespace arcticdb {

ChunkIterator::ChunkIterator(
    pipelines::index::IndexSegmentReader&& index_segment_reader,
    std::shared_ptr<pipelines::PipelineContext> pipeline_context,
    AtomKey index_key,
    std::shared_ptr<Store> store,
    ReadQuery& read_query,
    const ReadOptions& read_options,
    std::any& handler_data,
    DecodePathData shared_data) :
    index_segment_reader_(std::move(index_segment_reader)),
    pipeline_context_(std::move(pipeline_context)),
    index_key_(index_key),
    store_(std::move(store)),
    handler_data_(handler_data),
    shared_data_(std::move(shared_data)),
    read_query_(std::make_shared<ReadQuery>(read_query)),
    read_options_(read_options),
    read_ahead_(ConfigsMap::instance()->get_int("Chunk.ReadaheadRows", 1)),
    row_ranges_to_read_(pipeline_context_->fetch_index_.count()) {
    do_read_ahead();
}

std::optional<ReadResult> ChunkIterator::next() {
    auto release_gil = std::make_unique<py::gil_scoped_release>();
    if (row_pos_ == row_ranges_to_read_)
        return std::nullopt;

    auto required_row_pos = row_pos_;
    util::check(required_row_pos < results_.size(),
                "Request for row pos that has not been scheduled: {} >= {}",
                required_row_pos,
                results_.size());
    ++row_pos_;
    do_read_ahead();
    auto read_version = std::move(results_[required_row_pos]).get();
    return create_python_read_result(read_version.versioned_item_, std::move(read_version.frame_and_descriptor_));
}

void ChunkIterator::do_read_ahead() {
    while (scheduled_row_pos_ < row_pos_ + read_ahead_ && scheduled_row_pos_ < row_ranges_to_read_)
        schedule_row_range();
}

void ChunkIterator::schedule_row_range() {
    auto local_context = std::make_shared<PipelineContext>();
    local_context->set_descriptor(pipeline_context_->descriptor());
    const auto previous_row_range = pipeline_context_->slice_and_keys_[slice_and_key_pos_].slice_.row_range;
    auto current_row_range = previous_row_range;
    const auto start_pos = slice_and_key_pos_;
    while (current_row_range == previous_row_range && slice_and_key_pos_ < pipeline_context_->slice_and_keys_.size()) {
        local_context->slice_and_keys_.emplace_back(pipeline_context_->slice_and_keys_[slice_and_key_pos_]);
        local_context->fetch_index_.set_bit(row_pos_, pipeline_context_->fetch_index_[slice_and_key_pos_]);
        ++slice_and_key_pos_;
        if (slice_and_key_pos_ == pipeline_context_->slice_and_keys_.size())
            break;

        current_row_range = pipeline_context_->slice_and_keys_[slice_and_key_pos_].slice_.row_range;
    }

    const auto row_range_size = slice_and_key_pos_ - start_pos;
    util::check(row_range_size == local_context->slice_and_keys_.size(),
                "Expected equality of range size and slice and keys {} != {},",
                row_range_size,
                local_context->slice_and_keys_.size());

    pipeline_context_->fetch_index_.copy_range(local_context->fetch_index_, start_pos, slice_and_key_pos_);
    local_context->fetch_index_.resize(row_range_size);
    local_context->norm_meta_ = pipeline_context_->norm_meta_;

    auto frame = allocate_frame(local_context);
    auto output = do_direct_read_or_process(store_,
                                            read_query_,
                                            read_options_,
                                            local_context,
                                            shared_data_,
                                            handler_data_).thenValue(
        [this, frame, local_context](auto&&) mutable {
            return ReadVersionOutput{
                VersionedItem{to_atom(index_key_)},
                FrameAndDescriptor{frame, TimeseriesDescriptor{index_segment_reader_.tsd()}, {}, {}}};
        });

    results_.emplace_back(std::move(output));
    ++scheduled_row_pos_;
}

} // namespace arcticdb