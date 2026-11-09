/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/pipeline/index_segment_reader.hpp>
#include <arcticdb/pipeline/pipeline_utils.hpp>
#include <arcticdb/pipeline/streaming_read_generator.hpp>
#include <arcticdb/version/version_core.hpp>

namespace arcticdb::version_store {
StreamingReadGenerator::StreamingReadGenerator(
        LocalVersionedEngine* engine, const StreamId& stream_id, const VersionQuery& version_query,
        const std::shared_ptr<ReadQuery>& read_query, const ReadOptions& read_options,
        std::optional<uint64_t> prefetch_rows, std::shared_ptr<std::any> handler_data
) :
    engine_(engine),
    stream_id_(stream_id),
    version_query_(version_query),
    read_query_(read_query),
    read_options_(read_options),
    prefetch_rows_(prefetch_rows),
    handler_data_(handler_data) {
    log::version().debug("StreamingReadGenerator initialised");
}

ReadResult StreamingReadGenerator::read_result() {
    py::gil_scoped_release release_gil;
    log::version().debug("StreamingReadGenerator::read_result() called");
    ensure_index_information();
    VersionedItem versioned_item{to_atom(index_information_->index_.first)};
    const auto& tsd = index_information_->index_.second.index_descriptor();
    return {versioned_item,
            ArrowOutputFrame{},
            read_options_.output_format_for_frame(),
            tsd.normalization(),
            tsd.user_metadata(),
            {},
            {},
            tsd.sorted()};
}

std::optional<ArrowOutputFrame> StreamingReadGenerator::next() {
    py::gil_scoped_release release_gil;
    ensure_index_information();
    if (idx_ >= row_ranges_.size()) {
        return std::nullopt;
    }
    const auto& row_range = row_ranges_[idx_++];
    // TODO: Handle user-provided row-range filters by truncating row_ranges_
    // Copy will be unavoidable as there will be many calls to read_frame_for_version in-flight at once when reading
    // ahead
    auto read_query_copy = std::make_shared<ReadQuery>(*read_query_);
    read_query_copy->row_range = SignedRowRange{row_range.first, row_range.second};
    // TODO: Async and read-ahead
    // TODO: read_frame_for_version is destructive of the index segment, find a more elegant solution than this clone
    auto index_seg = index_information_->index_.second.clone();
    auto read_version_output =
            read_frame_for_version(engine_->store(), index_information_, read_query_copy, read_options_, handler_data_)
                    .get();
    index_information_->index_.second = std::move(index_seg);
    // TODO: Cache this, or pass empty object to create_python_read_result
    VersionedItem versioned_item{to_atom(index_information_->index_.first)};
    auto read_result = create_python_read_result(
            versioned_item,
            read_options_.output_format_for_frame(),
            std::move(read_version_output.frame_and_descriptor_)
    );
    return std::get<ArrowOutputFrame>(std::move(read_result.frame_data));
}

void StreamingReadGenerator::ensure_index_information() {
    // TODO: Make this a std::call_once
    if (index_information_) {
        return;
    }
    auto opt_versioned_item = engine_->get_version_to_read(stream_id_, version_query_);
    // TODO: Check this is the same exception type as the regular read path
    missing_data::check<ErrorCode::E_NO_SUCH_VERSION>(
            opt_versioned_item.has_value(), "TODO: Copy message from regular read path"
    );
    // TODO: Does fetch_index_and_column_stats always return IndexInformation? If so, change API
    index_information_ = std::get<std::shared_ptr<IndexInformation>>(
            fetch_index_and_column_stats(engine_->store(), *opt_versioned_item, *read_query_).get()
    );
    // TODO: Get rid of this clone
    pipelines::index::IndexSegmentReader isr{index_information_->index_.second.clone()};
    for (const auto& slice_and_key : isr) {
        // TODO: Check this works with column slicing
        if (!row_ranges_.empty() && slice_and_key.slice().rows().first < row_ranges_.back().second) {
            break;
        }
        row_ranges_.emplace_back(slice_and_key.slice().rows());
    }
}
} // namespace arcticdb::version_store