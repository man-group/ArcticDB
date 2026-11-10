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

void StreamingReadGenerator::prefetch() {
    util::check(prefetch_rows_.has_value(), "StreamingReadGenerator: prefetch_rows_ should be populated");
    while (in_flight_rows_ < *prefetch_rows_ && !row_ranges_.empty()) {
        auto row_range = std::move(row_ranges_.front());
        in_flight_rows_ += row_range.diff();
        row_ranges_.pop_front();
        // TODO: Make all of this async
        // Copy will be unavoidable as there will be many calls to read_frame_for_version in-flight at once when reading
        // ahead
        auto read_query_copy = std::make_shared<ReadQuery>(*read_query_);
        read_query_copy->row_range = SignedRowRange{row_range.first, row_range.second};
        auto index_information_copy = std::make_shared<IndexInformation>(
                std::make_pair(index_information_->index_.first, index_information_->index_.second.clone()),
                std::nullopt
        );
        index_information_copy->column_stats_ = index_information_->column_stats_;
        auto read_version_output_fut = read_frame_for_version(
                engine_->store(), index_information_copy, read_query_copy, read_options_, handler_data_
        );
        frame_futs_.emplace_back(
                std::move(row_range),
                std::move(read_version_output_fut).thenValueInline([this](auto&& read_version_output) {
                    auto read_result = create_python_read_result(
                            {},
                            read_options_.output_format_for_frame(),
                            std::move(read_version_output.frame_and_descriptor_)
                    );
                    return std::get<ArrowOutputFrame>(std::move(read_result.frame_data));
                })
        );
    }
}

std::optional<ArrowOutputFrame> StreamingReadGenerator::next() {
    py::gil_scoped_release release_gil;
    ensure_index_information();
    if (frame_futs_.empty() && row_ranges_.empty()) {
        return std::nullopt;
    }
    if (!started_) {
        prefetch();
        started_ = true;
    }
    // TODO: Remove this hack
    if (in_flight_rows_ == 0) {
        auto prefetch_rows_before = *prefetch_rows_;
        prefetch_rows_ = 1;
        prefetch();
        prefetch_rows_ = prefetch_rows_before;
    }
    auto [row_range, frame_fut] = std::move(frame_futs_.front());
    frame_futs_.pop_front();
    in_flight_rows_ -= row_range.diff();
    auto res = std::move(frame_fut).get();
    prefetch();
    return res;
}

void StreamingReadGenerator::ensure_index_information() {
    if (index_information_) {
        return;
    }
    auto opt_versioned_item = engine_->get_version_to_read(stream_id_, version_query_);
    // E_NO_SUCH_VERSION probably more appropriate as we are not supporting incompletes, but this matches the regular
    // read path
    missing_data::check<ErrorCode::E_NO_SYMBOL_DATA>(
            opt_versioned_item.has_value(),
            "StreamingReadGenerator: no data for version {} of symbol {}",
            version_query_,
            stream_id_
    );
    index_information_ = fetch_index_and_column_stats(engine_->store(), *opt_versioned_item, *read_query_).get();
    pipelines::index::IndexSegmentReader isr{index_information_->index_.second.clone()};
    for (const auto& slice_and_key : isr) {
        if (!row_ranges_.empty() && slice_and_key.slice().rows().first < row_ranges_.back().second) {
            break;
        }
        row_ranges_.emplace_back(slice_and_key.slice().rows());
    }
    prefetch_rows_ = prefetch_rows_.value_or(1'000'000);
}
} // namespace arcticdb::version_store