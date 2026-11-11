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

void StreamingReadGenerator::fetch_next_slice() {
    auto row_slice = std::move(to_process_.front());
    in_flight_rows_ += row_slice.row_count();
    to_process_.pop_front();
    row_slice.opt_arrow_output_frame_fut_
            .emplace(folly::via(&async::cpu_executor(), [this, row_range = row_slice.row_range_] {
                         // read_frame_for_version is destructive of IndexInformation. In the case of the index segment,
                         // this is purely through extracting the TSD though, and it does not seem like a
                         // hard-requirement for column stats info either. In addition, column stats at this point are
                         // represented by a compressed Segment, implying this will be decoded again for each slice,
                         // which is obviously inefficient. Address all of these as part of 13346982120.
                         auto index_information_copy = std::make_shared<IndexInformation>(index_information_->clone());
                         // Copy will be unavoidable as there will be many calls to read_frame_for_version in-flight at
                         // once when reading ahead
                         auto read_query_copy = std::make_shared<ReadQuery>(read_query_->clone());
                         read_query_copy->row_range = SignedRowRange{row_range.first, row_range.second};
                         if (!read_query_copy->clauses_.empty()) {
                             RowRangeClause row_range_clause{row_range.first, row_range.second};
                             auto clauses = util::reserve_vector<std::shared_ptr<Clause>>(
                                     read_query_copy->clauses_.size() + 1
                             );
                             clauses.emplace_back(std::make_shared<Clause>(std::move(row_range_clause)));
                             clauses.insert(
                                     clauses.end(),
                                     std::make_move_iterator(read_query_copy->clauses_.begin()),
                                     std::make_move_iterator(read_query_copy->clauses_.end())
                             );
                             read_query_copy->clauses_ = std::move(clauses);
                             // TODO 13346982120: Augment plan_query to merge adjacent RowRange clauses when possible,
                             // and then call it again here
                         }
                         return read_frame_for_version(
                                 engine_->store(), index_information_copy, read_query_copy, read_options_, handler_data_
                         );
                     }).thenValueInline([this](auto&& read_version_output) {
                auto read_result = create_python_read_result(
                        {},
                        read_options_.output_format_for_frame(),
                        std::move(read_version_output.frame_and_descriptor_)
                );
                return std::get<ArrowOutputFrame>(std::move(read_result.frame_data));
            }));
    being_processed_.emplace_back(std::move(row_slice));
}

void StreamingReadGenerator::prefetch_slices() {
    while (in_flight_rows_ < *prefetch_rows_ && !to_process_.empty()) {
        fetch_next_slice();
    }
}

std::optional<ArrowOutputFrame> StreamingReadGenerator::next() {
    py::gil_scoped_release release_gil;
    ensure_index_information();
    if (being_processed_.empty() && to_process_.empty()) {
        return std::nullopt;
    }
    if (!started_) {
        prefetch_slices();
        started_ = true;
    }
    // Special case with no preloading - on each call to next the next row-slice is read
    if (*prefetch_rows_ == 0) {
        fetch_next_slice();
    }
    auto row_slice = std::move(being_processed_.front());
    being_processed_.pop_front();
    in_flight_rows_ -= row_slice.row_count();
    // Get the next slice before scheduling more work with prefetch_slices in case the newly scheduled work somehow
    // delays us being able to return
    util::check(
            row_slice.opt_arrow_output_frame_fut_.has_value(),
            "StreamingReadGenerator::next(): unexpected unscheduled future"
    );
    auto res = std::move(*row_slice.opt_arrow_output_frame_fut_).get();
    prefetch_slices();
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
        if (!to_process_.empty() && slice_and_key.slice().rows().first < to_process_.back().row_range_.second) {
            break;
        }
        to_process_.emplace_back(
                slice_and_key.slice().rows(), slice_and_key.key().start_time(), slice_and_key.key().end_time()
        );
    }
    read_query_->convert_to_positive_row_filter(isr.tsd().total_rows());
    // TODO: Use get_if
    if (std::holds_alternative<RowRange>(read_query_->row_filter)) {
        auto row_filter = std::get<RowRange>(read_query_->row_filter);
        // TODO: Make this more efficient
        for (auto row_slice = to_process_.begin(); row_slice != to_process_.end();) {
            if (row_slice->row_range_.second <= row_filter.first) {
                row_slice = to_process_.erase(row_slice);
            } else if (row_slice->row_range_.first >= row_filter.second) {
                row_slice = to_process_.erase(row_slice);
            } else if (row_slice->row_range_.first <= row_filter.first &&
                       row_filter.first < row_slice->row_range_.second) {
                row_slice->row_range_.first = row_filter.first;
                ++row_slice;
            } else if (row_slice->row_range_.first < row_filter.second &&
                       row_filter.second <= row_slice->row_range_.second) {
                row_slice->row_range_.second = row_filter.second;
                ++row_slice;
            } else {
                ++row_slice;
            }
        }
    }
    prefetch_rows_ = prefetch_rows_.value_or(1'000'000);
}
} // namespace arcticdb::version_store