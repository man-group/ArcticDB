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
#include <arcticdb/version/local_versioned_engine.hpp>
#include <arcticdb/version/version_core.hpp>

namespace arcticdb::version_store {
StreamingReadGenerator::StreamingReadGenerator(
        std::shared_ptr<Store> store, std::shared_ptr<VersionMap> version_map, const StreamId& stream_id,
        const VersionQuery& version_query, std::shared_ptr<ReadQuery> read_query, const ReadOptions& read_options,
        std::optional<uint64_t> prefetch_rows, std::shared_ptr<std::any> handler_data
) :
    store_(std::move(store)),
    version_map_(std::move(version_map)),
    stream_id_(stream_id),
    version_query_(version_query),
    read_query_(std::move(read_query)),
    read_options_(std::make_shared<ReadOptions>(read_options)),
    prefetch_rows_(prefetch_rows),
    handler_data_(std::move(handler_data)) {
    log::version().debug("StreamingReadGenerator initialised for version {} of {}", version_query_, stream_id_);
}

ReadResult StreamingReadGenerator::read_result() {
    py::gil_scoped_release release_gil;
    log::version().debug("StreamingReadGenerator::read_result() called for {}", stream_id_);
    ensure_index_information();
    VersionedItem versioned_item{to_atom(index_information_->index_.first)};
    const auto& tsd = index_information_->index_.second.index_descriptor();
    return {versioned_item,
            ArrowOutputFrame{},
            read_options_->output_format_for_frame(),
            tsd.normalization(),
            tsd.user_metadata(),
            {},
            {},
            tsd.sorted()};
}

void StreamingReadGenerator::fetch_next_slice() {
    auto row_slice = std::move(to_process_.front());
    in_flight_rows_ += row_slice.row_count();
    log::version().debug(
            "StreamingReadGenerator::fetch_next_slice() called for row-slice [{}, {}) of {}. Total in-flight rows now "
            "{}",
            row_slice.row_range_.first,
            row_slice.row_range_.second,
            stream_id_,
            in_flight_rows_
    );
    to_process_.pop_front();
    auto read_result_fut =
            folly::via(
                    &async::cpu_executor(),
                    [index_information = index_information_,
                     read_query = read_query_,
                     store = store_,
                     read_options = read_options_,
                     handler_data = handler_data_,
                     row_range = row_slice.row_range_] {
                        // read_frame_for_version is destructive of IndexInformation. In the case of the index segment,
                        // this is purely through extracting the TSD though, and it does not seem like a
                        // hard-requirement for column stats info either. In addition, column stats at this point are
                        // represented by a compressed Segment, implying this will be decoded again for each slice,
                        // which is obviously inefficient. Address all of these as part of 13346982120.
                        auto index_information_copy = std::make_shared<IndexInformation>(index_information->clone());
                        // Copy will be unavoidable as there will be many calls to read_frame_for_version in-flight at
                        // once when reading ahead
                        auto read_query_copy = std::make_shared<ReadQuery>(read_query->clone());
                        read_query_copy->row_range = RowRange{row_range.first, row_range.second};
                        return read_frame_for_version(
                                store, index_information_copy, read_query_copy, *read_options, handler_data
                        );
                    }
            ).thenValueInline([read_options = read_options_](auto&& read_version_output) {
                // TODO 13346982120: Only do the norm metadata conversion once, instead of once per slice
                // This also results in user metadata being passed from C++ to Python repeatedly
                // Retain the norm meta as well as the frame, as for Arrow output it is converted to match the frame
                return create_python_read_result(
                        {},
                        read_options->output_format_for_frame(),
                        std::move(read_version_output.frame_and_descriptor_)
                );
            });
    row_slice.opt_read_result_fut_.emplace(std::move(read_result_fut));
    being_processed_.emplace_back(std::move(row_slice));
}

void StreamingReadGenerator::prefetch_slices() {
    while (in_flight_rows_ < *prefetch_rows_ && !to_process_.empty()) {
        fetch_next_slice();
    }
}

std::optional<ReadResult> StreamingReadGenerator::next() {
    py::gil_scoped_release release_gil;
    log::version().debug("StreamingReadGenerator::next() called for {}", stream_id_);
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
            row_slice.opt_read_result_fut_.has_value(), "StreamingReadGenerator::next(): unexpected unscheduled future"
    );
    // TODO 13346982120: If there is an exception thrown during reading/processing, it will be re-raised on this call to
    // get(). If next() is then called again, we could crash as prefetch_slices() was not called after the exception
    // raise. We should clear to_process_, wait for all in-flight futures in being_processed_ (otherwise their memory
    // will leak), and then re-raise the exception.
    // Similarly, how should we handle a loop in the Python layer calling break? Or the generator object being deleted?
    auto res = std::move(*row_slice.opt_read_result_fut_).get();
    prefetch_slices();
    return res;
}

void StreamingReadGenerator::ensure_index_information() {
    // TODO 13346982120: Consider thread-safety of all methods in this class, but particularly this one
    if (index_information_) {
        return;
    }
    log::version().debug("StreamingReadGenerator: fetching index key for {}", stream_id_);
    auto opt_versioned_item = get_version_to_read(store_, version_map_, stream_id_, version_query_);
    validate_versioned_item(opt_versioned_item);
    // Do not assign directly to index_information_ in case any of the validation fails
    auto index_information = fetch_index_and_column_stats(store_, *opt_versioned_item, *read_query_).get();
    // TODO 13346982120: Does ISR have to consume the index segment?
    index::IndexSegmentReader isr{index_information->index_.second.clone()};
    validate_data_format_and_query(isr);
    for (const auto& slice_and_key : isr) {
        // We can extract all the row-slices from the first column slice
        if (!to_process_.empty() && slice_and_key.slice().rows().first < to_process_.back().row_range_.second) {
            break;
        }
        to_process_.emplace_back(
                slice_and_key.slice().rows(), slice_and_key.key().start_time(), slice_and_key.key().end_time()
        );
    }

    util::variant_match(
            read_query_->post_processing_filter(),
            [&](const RowRange& row_range) {
                for (auto row_slice = to_process_.begin(); row_slice != to_process_.end();) {
                    row_slice->row_range_.first = std::max(row_slice->row_range_.first, row_range.first);
                    row_slice->row_range_.second = std::min(row_slice->row_range_.second, row_range.second);
                    if (row_slice->row_range_.first >= row_slice->row_range_.second) {
                        row_slice = to_process_.erase(row_slice);
                    } else {
                        ++row_slice;
                    }
                }
            },
            [&](const IndexRange& index_range) {
                if (std::holds_alternative<timestamp>(index_range.start_) &&
                    std::holds_alternative<timestamp>(index_range.end_)) {
                    auto start_ts = std::get<timestamp>(index_range.start_);
                    auto end_ts = std::get<timestamp>(index_range.end_);
                    for (auto row_slice = to_process_.begin(); row_slice != to_process_.end();) {
                        if (row_slice->end_index_ <= start_ts || row_slice->start_index_ > end_ts) {
                            row_slice = to_process_.erase(row_slice);
                        } else {
                            ++row_slice;
                        }
                    }
                }
            },
            [](const std::monostate&) {}
    );
    // TODO 13346982120: Better heuristic when user doesn't specify, and a way to say "prefetch everything"
    prefetch_rows_ = prefetch_rows_.value_or(1'000'000);
    index_information_ = std::move(index_information);
    log::version().debug(
            "StreamingReadGenerator: index filtered down to {} row-slices for {}", to_process_.size(), stream_id_
    );
}

void StreamingReadGenerator::validate_versioned_item(const std::optional<VersionedItem>& opt_versioned_item) const {
    // E_NO_SUCH_VERSION probably more appropriate as we are not supporting incompletes, but this matches the regular
    // read path
    missing_data::check<ErrorCode::E_NO_SYMBOL_DATA>(
            opt_versioned_item.has_value(),
            "StreamingReadGenerator: no data for version {} of symbol {}",
            version_query_,
            stream_id_
    );
    schema::check<ErrorCode::E_OPERATION_NOT_SUPPORTED_WITH_RECURSIVE_NORMALIZED_DATA>(
            opt_versioned_item->key_.type() != KeyType::MULTI_KEY,
            "read_streaming not supported with recursively normalized data"
    );
}

void StreamingReadGenerator::validate_data_format_and_query(const pipelines::index::IndexSegmentReader& isr) {
    schema::check<ErrorCode::E_OPERATION_NOT_SUPPORTED_WITH_PICKLED_DATA>(
            !isr.is_pickled(), "read_streaming not supported with pickled data"
    );
    schema::check<ErrorCode::E_OPERATION_NOT_SUPPORTED_WITH_NUMPY_ARRAY>(
            isr.tsd().proto().normalization().input_type_case() !=
                    proto::descriptors::NormalizationMetadata::InputTypeCase::kNp,
            "read_streaming not supported with numpy arrays"
    );
    read_query_->convert_to_positive_row_filter(static_cast<int64_t>(isr.tsd().total_rows()));
    if (read_query_->row_filter.has_value()) {
        schema::check<ErrorCode::E_UNSUPPORTED_INDEX_TYPE>(
                isr.has_timestamp_index(), "Cannot apply date range filter to symbol with non-timestamp index"
        );
        const auto sorted_value = isr.sorted();
        sorting::check<ErrorCode::E_UNSORTED_DATA>(
                sorted_value == SortedValue::UNKNOWN || sorted_value == SortedValue::ASCENDING,
                "When filtering data using date_range, the symbol must be sorted in ascending order. ArcticDB believes "
                "it is not sorted in ascending order and cannot therefore filter the data using date_range."
        );
    }
}
} // namespace arcticdb::version_store