/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/version/version_core.hpp>
#include <arcticdb/column_store/column_algorithms.hpp>
#include <arcticdb/stream/segment_aggregator.hpp>
#include <arcticdb/pipeline/write_frame.hpp>
#include <arcticdb/pipeline/slicing.hpp>
#include <arcticdb/pipeline/frame_utils.hpp>
#include <pipeline/frame_slice.hpp>
#include <arcticdb/codec/codec.hpp>
#include <folly/futures/FutureSplitter.h>
#include <arcticdb/pipeline/write_options.hpp>
#include <arcticdb/stream/index.hpp>
#include <arcticdb/pipeline/query.hpp>
#include <arcticdb/pipeline/read_pipeline.hpp>
#include <arcticdb/async/task_scheduler.hpp>
#include <arcticdb/async/tasks.hpp>
#include <arcticdb/util/name_validation.hpp>
#include <arcticdb/stream/incompletes.hpp>
#include <arcticdb/pipeline/pipeline_context.hpp>
#include <arcticdb/pipeline/read_frame.hpp>
#include <arcticdb/pipeline/read_options.hpp>
#include <arcticdb/pipeline/column_mapping.hpp>
#include <arcticdb/stream/stream_sink.hpp>
#include <arcticdb/stream/schema.hpp>
#include <arcticdb/pipeline/index_writer.hpp>
#include <arcticdb/pipeline/index_utils.hpp>
#include <arcticdb/version/schema_checks.hpp>
#include <arcticdb/version/version_utils.hpp>
#include <arcticdb/entity/merge_descriptors.hpp>
#include <arcticdb/processing/component_manager.hpp>
#include <arcticdb/util/format_date.hpp>
#include <iterator>
#include <aws/core/utils/stream/ResponseStream.h>

namespace arcticdb::version_store {

[[nodiscard]] static ReadOptions defragmentation_read_options_generator(const WriteOptions& options) {
    ReadOptions read_options;
    read_options.set_dynamic_schema(options.dynamic_schema);
    return read_options;
}

namespace ranges = std::ranges;

static void modify_descriptor(
        const std::shared_ptr<pipelines::PipelineContext>& pipeline_context, const ReadOptions& read_options
) {

    if (opt_false(read_options.force_strings_to_object()) || opt_false(read_options.force_strings_to_fixed()))
        pipeline_context->orig_desc_ = pipeline_context->desc_;

    auto& desc = *pipeline_context->desc_;
    if (opt_false(read_options.force_strings_to_object())) {
        auto& fields = desc.fields();
        for (Field& field_desc : fields) {
            if (field_desc.type().data_type() == DataType::ASCII_FIXED64)
                set_data_type(DataType::ASCII_DYNAMIC64, field_desc.mutable_type());

            if (field_desc.type().data_type() == DataType::UTF_FIXED64)
                set_data_type(DataType::UTF_DYNAMIC64, field_desc.mutable_type());
        }
    } else if (opt_false(read_options.force_strings_to_fixed())) {
        auto& fields = desc.fields();
        for (Field& field_desc : fields) {
            if (field_desc.type().data_type() == DataType::ASCII_DYNAMIC64)
                set_data_type(DataType::ASCII_FIXED64, field_desc.mutable_type());

            if (field_desc.type().data_type() == DataType::UTF_DYNAMIC64)
                set_data_type(DataType::UTF_FIXED64, field_desc.mutable_type());
        }
    }
}

VersionedItem write_dataframe_impl(
        const std::shared_ptr<Store>& store, VersionId version_id, const std::shared_ptr<InputFrame>& frame,
        const WriteOptions& options, const std::shared_ptr<DeDupMap>& de_dup_map, bool sparsify_floats,
        bool validate_index
) {
    ARCTICDB_SUBSAMPLE_DEFAULT(WaitForWriteCompletion)
    ARCTICDB_DEBUG(
            log::version(),
            "write_dataframe_impl stream_id: {} , version_id: {}, {} rows",
            frame->desc().id(),
            version_id,
            frame->num_rows
    );
    auto atom_key_fut =
            async_write_dataframe_impl(store, version_id, frame, options, de_dup_map, sparsify_floats, validate_index);
    return {std::move(atom_key_fut).get()};
}

std::tuple<IndexPartialKey, SlicingPolicy> get_partial_key_and_slicing_policy(
        const WriteOptions& options, const InputFrame& frame, VersionId version_id, bool validate_index
) {
    if (version_id == 0) {
        auto check_outcome = verify_symbol_key(frame.desc().id());
        if (std::holds_alternative<Error>(check_outcome)) {
            std::get<Error>(check_outcome).throw_error();
        }
    }

    // Slice the frame according to the write options
    auto slicing_arg = get_slicing_policy(options, frame);
    auto partial_key = IndexPartialKey{frame.desc().id(), version_id};
    if (validate_index && !index_is_not_timeseries_or_is_sorted_ascending(frame)) {
        sorting::raise<ErrorCode::E_UNSORTED_DATA>(
                "When calling write with validate_index enabled, input data must be sorted"
        );
    }

    return std::make_tuple(partial_key, slicing_arg);
}

folly::Future<entity::AtomKey> async_write_dataframe_impl(
        const std::shared_ptr<Store>& store, VersionId version_id, const std::shared_ptr<InputFrame>& frame,
        const WriteOptions& options, const std::shared_ptr<DeDupMap>& de_dup_map, bool sparsify_floats,
        bool validate_index
) {
    ARCTICDB_SAMPLE(DoWrite, 0)
    frame->set_bucketize_dynamic(options.bucketize_dynamic);
    auto [partial_key, slicing_policy] =
            get_partial_key_and_slicing_policy(options, *frame, version_id, validate_index);
    return write_frame(std::move(partial_key), frame, slicing_policy, store, de_dup_map, sparsify_floats);
}

void sorted_data_check_append(const InputFrame& frame, index::IndexSegmentReader& index_segment_reader) {
    if (!index_is_not_timeseries_or_is_sorted_ascending(frame)) {
        sorting::raise<ErrorCode::E_UNSORTED_DATA>(
                "When calling append with validate_index enabled, input data must be sorted"
        );
    }
    sorting::check<ErrorCode::E_UNSORTED_DATA>(
            !std::holds_alternative<stream::TimeseriesIndex>(frame.index) ||
                    index_segment_reader.tsd().sorted() == SortedValue::ASCENDING,
            "When calling append with validate_index enabled, the existing data must be sorted"
    );
}

folly::Future<AtomKey> async_append_impl(
        const std::shared_ptr<Store>& store, const UpdateInfo& update_info, const std::shared_ptr<InputFrame>& frame,
        const WriteOptions& options, bool validate_index, bool empty_types
) {

    util::check(
            update_info.previous_index_key_.has_value(), "Cannot append as there is no previous index key to append to"
    );
    const StreamId stream_id = frame->desc().id();
    ARCTICDB_DEBUG(log::version(), "append stream_id: {} , version_id: {}", stream_id, update_info.next_version_id_);
    auto index_segment_reader = index::get_index_reader(*(update_info.previous_index_key_), store);
    bool bucketize_dynamic = index_segment_reader.bucketize_dynamic();
    auto row_offset = index_segment_reader.tsd().total_rows();
    util::check_rte(!index_segment_reader.is_pickled(), "Cannot append to pickled data");
    frame->set_offset(static_cast<ssize_t>(row_offset));
    fix_descriptor_mismatch_or_throw(APPEND, options.dynamic_schema, index_segment_reader, *frame, empty_types);
    if (validate_index) {
        sorted_data_check_append(*frame, index_segment_reader);
    }

    frame->set_bucketize_dynamic(bucketize_dynamic);
    auto slicing_arg = get_slicing_policy(options, *frame);
    return append_frame(
            IndexPartialKey{stream_id, update_info.next_version_id_},
            frame,
            slicing_arg,
            index_segment_reader,
            store,
            options.dynamic_schema,
            options.ignore_sort_order
    );
}

VersionedItem append_impl(
        const std::shared_ptr<Store>& store, const UpdateInfo& update_info, const std::shared_ptr<InputFrame>& frame,
        const WriteOptions& options, bool validate_index, bool empty_types
) {

    ARCTICDB_SUBSAMPLE_DEFAULT(WaitForWriteCompletion)
    auto version_key_fut = async_append_impl(store, update_info, frame, options, validate_index, empty_types);
    auto versioned_item = VersionedItem(std::move(version_key_fut).get());
    ARCTICDB_DEBUG(
            log::version(),
            "write_dataframe_impl stream_id: {} , version_id: {}",
            versioned_item.symbol(),
            update_info.next_version_id_
    );
    return versioned_item;
}

namespace {
bool is_before(const IndexRange& a, const IndexRange& b) { return a.start_ < b.start_; }

bool is_after(const IndexRange& a, const IndexRange& b) { return a.end_ > b.end_; }

void check_index_match(const Index& index, const IndexDescriptorImpl& desc) {
    if (std::holds_alternative<TimeseriesIndex>(index))
        util::check(
                desc.type() == IndexDescriptor::Type::TIMESTAMP || desc.type() == IndexDescriptor::Type::EMPTY,
                "Index mismatch, cannot update a non-timeseries-indexed frame with a timeseries"
        );
    else
        util::check(
                desc.type() == IndexDescriptorImpl::Type::ROWCOUNT,
                "Index mismatch, cannot update a timeseries with a non-timeseries-indexed frame"
        );
}

/// Represents all slices which are intersecting (but not overlapping) with range passed to update
/// First member is a vector of all segments intersecting with the first row-slice of the update range
/// Second member is a vector of all segments intersecting with the last row-slice of the update range
using IntersectingSegments = std::tuple<std::vector<SliceAndKey>, std::vector<SliceAndKey>>;

[[nodiscard]] folly::Future<IntersectingSegments> async_intersecting_segments(
        std::shared_ptr<std::vector<SliceAndKey>> affected_keys, const IndexRange& front_range,
        const IndexRange& back_range, VersionId version_id, const std::shared_ptr<Store>& store
) {
    if (!front_range.specified_ && !back_range.specified_) {
        return folly::makeFuture<IntersectingSegments>(IntersectingSegments{});
    }
    internal::check<ErrorCode::E_ASSERTION_FAILURE>(
            front_range.specified_ && back_range.specified_,
            "Both first and last index range of the update range must intersect with at least one of the slices in the "
            "dataframe"
    );
    std::vector<folly::Future<SliceAndKey>> maybe_intersect_before_fut;
    std::vector<folly::Future<SliceAndKey>> maybe_intersect_after_fut;

    for (const auto& affected_slice_and_key : *affected_keys) {
        const auto& affected_range = affected_slice_and_key.key().index_range();
        if (intersects(affected_range, front_range) && !overlaps(affected_range, front_range) &&
            is_before(affected_range, front_range)) {
            maybe_intersect_before_fut.emplace_back(async_rewrite_partial_segment(
                    affected_slice_and_key, front_range, version_id, AffectedSegmentPart::START, store
            ));
        }

        if (intersects(affected_range, back_range) && !overlaps(affected_range, back_range) &&
            is_after(affected_range, back_range)) {
            maybe_intersect_after_fut.emplace_back(async_rewrite_partial_segment(
                    affected_slice_and_key, back_range, version_id, AffectedSegmentPart::END, store
            ));
        }
    }
    return collectAll(
                   collectAll(maybe_intersect_before_fut)
                           .via(&async::io_executor())
                           .thenValueInline([store](auto&& maybe_intersect_before) {
                               return rollback_slices_on_quota_exceeded(
                                       std::move(maybe_intersect_before), std::static_pointer_cast<StreamSink>(store)
                               );
                           }),
                   collectAll(maybe_intersect_after_fut)
                           .via(&async::io_executor())
                           .thenValueInline([store](auto&& maybe_intersect_after) {
                               return rollback_slices_on_quota_exceeded(
                                       std::move(maybe_intersect_after), std::static_pointer_cast<StreamSink>(store)
                               );
                           })
    )
            .via(&async::cpu_executor())
            .thenValue([store](auto&& try_before_and_after) -> folly::Future<IntersectingSegments> {
                auto& [try_intersect_before, try_intersect_after] = try_before_and_after;

                return rollback_batches_on_quota_exceeded(
                               {std::move(try_intersect_before), std::move(try_intersect_after)}, store
                )
                        .via(&async::cpu_executor())
                        .thenValue([](auto&& batch) {
                            auto& intersect_before = batch[0];
                            auto& intersect_after = batch[1];
                            return IntersectingSegments{std::move(intersect_before), std::move(intersect_after)};
                        });
            })
            .via(&async::io_executor());
}
/// When segments are produced by the processing pipeline, some rows might be missing. Imagine a filter with the middle
/// rows filtered out. These slices cannot be put in an index key as the row slices in the index key must be contiguous.
/// This function adjusts the row slices so that there are no gaps.
/// @note The difference between this and adjust_slice_ranges is that this will not change the column slices. While
///     adjust_slice_ranges will change the column slices, making the first slice start from 0 even for timestamp-index
///     dataframes (which should have column range starting from 1 when being written to disk).
void compact_row_slices(std::span<SliceAndKey> slices) {
    debug::check<ErrorCode::E_ASSERTION_FAILURE>(
            ranges::adjacent_find(
                    slices,
                    [](const SliceAndKey& a, const SliceAndKey& b) {
                        return a > b || (a.slice().col_range != b.slice().col_range &&
                                         a.slice().col_range.start() != b.slice().col_range.end());
                    }
            ) == slices.end(),
            "SlicesAndKeys must be sorted by column slice and all column slices must be present"
    );

    size_t current_compacted_row = 0;
    size_t previous_uncompacted_end = slices.empty() ? 0 : slices.front().slice().row_range.end();
    size_t previous_col_slice_end = slices.empty() ? 0 : slices.front().slice().col_range.end();
    for (SliceAndKey& slice : slices) {
        // Aggregation clause performs aggregations in parallel for each group. Thus, it can produce several slices with
        // the exact row_range and column_range. The ordering doesn't matter, but this must be taken into account so
        // that the row slices are always increasing. The second condition in the "if" takes care of this scenario.
        if (slice.slice().row_range.start() < previous_uncompacted_end &&
            slice.slice().col_range.start() > previous_col_slice_end) {
            current_compacted_row = 0;
        }
        previous_uncompacted_end = slice.slice().row_range.end();
        const size_t rows_in_slice = slice.slice().row_range.diff();
        slice.slice().row_range.first = current_compacted_row;
        slice.slice().row_range.second = current_compacted_row + rows_in_slice;
        current_compacted_row += rows_in_slice;
        previous_col_slice_end = slice.slice().col_range.end();
    }
}
} // namespace

VersionedItem delete_range_impl(
        const std::shared_ptr<Store>& store, const StreamId& stream_id, const UpdateInfo& update_info,
        const UpdateQuery& query, const WriteOptions&&, bool dynamic_schema
) {

    util::check(update_info.previous_index_key_.has_value(), "Cannot delete from non-existent symbol {}", stream_id);
    util::check(std::holds_alternative<IndexRange>(query.row_filter), "Delete range requires index range argument");
    const auto& index_range = std::get<IndexRange>(query.row_filter);
    ARCTICDB_DEBUG(
            log::version(),
            "Delete range in versioned dataframe for stream_id: {} , version_id = {}",
            stream_id,
            update_info.next_version_id_
    );

    auto index_segment_reader = index::get_index_reader(update_info.previous_index_key_.value(), store);
    util::check_rte(!index_segment_reader.is_pickled(), "Cannot delete date range of pickled data");

    auto index = index_type_from_descriptor(index_segment_reader.tsd().as_stream_descriptor());
    util::check(
            std::holds_alternative<TimeseriesIndex>(index),
            "Delete in range will not work as expected with a non-timeseries index"
    );

    std::vector<FilterQuery<index::IndexSegmentReader>> queries = build_update_query_filters<index::IndexSegmentReader>(
            query.row_filter, index, index_range, dynamic_schema, index_segment_reader.bucketize_dynamic()
    );
    auto combined = combine_filter_functions(queries);
    auto affected_keys = filter_index(index_segment_reader, std::move(combined));
    std::vector<SliceAndKey> unaffected_keys;
    std::set_difference(
            std::begin(index_segment_reader),
            std::end(index_segment_reader),
            std::begin(affected_keys),
            std::end(affected_keys),
            std::back_inserter(unaffected_keys)
    );

    auto [intersect_before, intersect_after] = async_intersecting_segments(
                                                       std::make_shared<std::vector<SliceAndKey>>(affected_keys),
                                                       index_range,
                                                       index_range,
                                                       update_info.next_version_id_,
                                                       store
    )
                                                       .get();

    auto orig_filter_range = std::holds_alternative<std::monostate>(query.row_filter)
                                     ? get_query_index_range(index, index_range)
                                     : query.row_filter;

    size_t row_count = 0;
    const std::array<std::vector<SliceAndKey>, 5> groups{
            strictly_before(orig_filter_range, unaffected_keys),
            std::move(intersect_before),
            std::move(intersect_after),
            strictly_after(orig_filter_range, unaffected_keys)
    };
    auto flattened_slice_and_keys = flatten_and_fix_rows(groups, row_count);

    std::sort(std::begin(flattened_slice_and_keys), std::end(flattened_slice_and_keys));
    auto version_key_fut = util::variant_match(
            index,
            [&index_segment_reader, &flattened_slice_and_keys, &stream_id, &update_info, &store, &row_count](auto idx) {
                using IndexType = decltype(idx);
                auto tsd = std::make_shared<TimeseriesDescriptor>(index_segment_reader.tsd().clone());
                tsd->set_total_rows(row_count);
                return pipelines::index::write_index<IndexType>(
                        *tsd,
                        std::move(flattened_slice_and_keys),
                        IndexPartialKey{stream_id, update_info.next_version_id_},
                        store
                );
            }
    );
    auto versioned_item = VersionedItem(std::move(version_key_fut).get());
    ARCTICDB_DEBUG(log::version(), "updated stream_id: {} , version_id: {}", stream_id, update_info.next_version_id_);
    return versioned_item;
}

void check_update_data_is_sorted(const InputFrame& frame, const index::IndexSegmentReader& index_segment_reader) {
    bool is_time_series = std::holds_alternative<stream::TimeseriesIndex>(frame.index);
    sorting::check<ErrorCode::E_UNSORTED_DATA>(
            is_time_series, "When calling update, the input data must be a time series."
    );
    bool input_data_is_sorted =
            frame.desc().sorted() == SortedValue::ASCENDING || frame.desc().sorted() == SortedValue::UNKNOWN;
    // If changing this error message, the corresponding message in _normalization.py::restrict_data_to_date_range_only
    // should also be updated
    sorting::check<ErrorCode::E_UNSORTED_DATA>(
            input_data_is_sorted, "When calling update, the input data must be sorted."
    );
    bool existing_data_is_sorted = index_segment_reader.sorted() == SortedValue::ASCENDING ||
                                   index_segment_reader.sorted() == SortedValue::UNKNOWN;
    sorting::check<ErrorCode::E_UNSORTED_DATA>(
            existing_data_is_sorted, "When calling update, the existing data must be sorted."
    );
}

struct UpdateRanges {
    IndexRange front;
    IndexRange back;
    IndexRange original_index_range;
};

static UpdateRanges compute_update_ranges(
        const FilterRange& row_filter, const InputFrame& update_frame, std::span<SliceAndKey> update_slice_and_keys
) {
    return util::variant_match(
            row_filter,
            [&](std::monostate) -> UpdateRanges {
                util::check(
                        std::holds_alternative<TimeseriesIndex>(update_frame.index),
                        "Update with row count index is not permitted"
                );
                if (update_slice_and_keys.empty()) {
                    // If there are no new keys, then we can't intersect with the existing data.
                    return UpdateRanges{{}, {}, update_frame.index_range};
                }
                return UpdateRanges{
                        update_slice_and_keys.front().key().index_range(),
                        update_slice_and_keys.back().key().index_range(),
                        update_frame.index_range
                };
            },
            [&](const IndexRange& idx_range) { return UpdateRanges{idx_range, idx_range, idx_range}; },
            [](const RowRange&) -> UpdateRanges {
                util::raise_rte("Unexpected row_range in update query");
                return {};
            }
    );
}

static void check_can_update(
        const InputFrame& frame, const index::IndexSegmentReader& index_segment_reader, const UpdateInfo& update_info,
        bool dynamic_schema, bool empty_types
) {
    util::check(
            update_info.previous_index_key_.has_value(),
            "Cannot update as there is no previous index key to update into"
    );
    util::check_rte(!index_segment_reader.is_pickled(), "Cannot update pickled data");
    check_index_match(frame.index, index_segment_reader.tsd().index());
    const auto index_desc = index_segment_reader.tsd().index();
    util::check(index::is_timeseries_index(index_desc), "Update not supported for non-timeseries indexes");
    check_update_data_is_sorted(frame, index_segment_reader);
    (void)check_and_mark_slices(index_segment_reader, false, std::nullopt);
    fix_descriptor_mismatch_or_throw(UPDATE, dynamic_schema, index_segment_reader, frame, empty_types);
}

static std::shared_ptr<std::vector<SliceAndKey>> get_keys_affected_by_update(
        const index::IndexSegmentReader& index_segment_reader, const InputFrame& frame, const UpdateQuery& query,
        bool dynamic_schema
) {
    std::vector<FilterQuery<index::IndexSegmentReader>> queries = build_update_query_filters<index::IndexSegmentReader>(
            query.row_filter, frame.index, frame.index_range, dynamic_schema, index_segment_reader.bucketize_dynamic()
    );
    return std::make_shared<std::vector<SliceAndKey>>(
            filter_index(index_segment_reader, combine_filter_functions(queries))
    );
}

static std::vector<SliceAndKey> get_keys_not_affected_by_update(
        const index::IndexSegmentReader& index_segment_reader, std::span<const SliceAndKey> affected_keys
) {
    std::vector<SliceAndKey> unaffected_keys;
    std::set_difference(
            index_segment_reader.begin(),
            index_segment_reader.end(),
            affected_keys.begin(),
            affected_keys.end(),
            std::back_inserter(unaffected_keys)
    );
    return unaffected_keys;
}

static std::pair<std::vector<SliceAndKey>, size_t> get_slice_and_keys_for_update(
        const UpdateRanges& update_ranges, std::span<const SliceAndKey> unaffected_keys,
        std::span<const SliceAndKey> affected_keys, const IntersectingSegments& segments_intersecting_with_update_range,
        std::vector<SliceAndKey>&& new_slice_and_keys
) {
    const size_t new_keys_size = new_slice_and_keys.size();
    size_t row_count = 0;
    const std::array<std::vector<SliceAndKey>, 5> groups{
            strictly_before(update_ranges.original_index_range, unaffected_keys),
            std::move(std::get<0>(segments_intersecting_with_update_range)),
            std::move(new_slice_and_keys),
            std::move(std::get<1>(segments_intersecting_with_update_range)),
            strictly_after(update_ranges.original_index_range, unaffected_keys)
    };
    auto flattened_slice_and_keys = flatten_and_fix_rows(groups, row_count);
    util::check(
            unaffected_keys.size() + new_keys_size + (affected_keys.size() * 2) >= flattened_slice_and_keys.size(),
            "Output size mismatch: {} + {} + (2 * {}) < {}",
            unaffected_keys.size(),
            new_keys_size,
            affected_keys.size(),
            flattened_slice_and_keys.size()
    );
    std::sort(std::begin(flattened_slice_and_keys), std::end(flattened_slice_and_keys));
    return {flattened_slice_and_keys, row_count};
}

folly::Future<AtomKey> async_update_impl(
        const std::shared_ptr<Store>& store, const UpdateInfo& update_info, const UpdateQuery& query,
        const std::shared_ptr<InputFrame>& frame, WriteOptions&& options, bool dynamic_schema, bool empty_types
) {
    return index::async_get_index_reader(*(update_info.previous_index_key_), store)
            .thenValue([store, update_info, query, frame, options = std::move(options), dynamic_schema, empty_types](
                               index::IndexSegmentReader&& index_segment_reader
                       ) {
                check_can_update(*frame, index_segment_reader, update_info, dynamic_schema, empty_types);
                ARCTICDB_DEBUG(
                        log::version(),
                        "Update versioned dataframe for stream_id: {} , version_id = {}",
                        frame->desc().id(),
                        update_info.previous_index_key_->version_id()
                );
                frame->set_bucketize_dynamic(index_segment_reader.bucketize_dynamic());
                return slice_and_write(
                               frame,
                               get_slicing_policy(options, *frame),
                               IndexPartialKey{frame->desc().id(), update_info.next_version_id_},
                               store
                )
                        .via(&async::cpu_executor())
                        .thenValue([store,
                                    update_info,
                                    query,
                                    frame,
                                    dynamic_schema,
                                    index_segment_reader = std::move(index_segment_reader
                                    )](std::vector<SliceAndKey>&& new_slice_and_keys) mutable {
                            std::sort(std::begin(new_slice_and_keys), std::end(new_slice_and_keys));
                            auto affected_keys =
                                    get_keys_affected_by_update(index_segment_reader, *frame, query, dynamic_schema);
                            auto unaffected_keys =
                                    get_keys_not_affected_by_update(index_segment_reader, *affected_keys);
                            util::check(
                                    affected_keys->size() + unaffected_keys.size() == index_segment_reader.size(),
                                    "The sum of affected keys and unaffected keys must be "
                                    "equal to the total number of "
                                    "keys {} + {} != {}",
                                    affected_keys->size(),
                                    unaffected_keys.size(),
                                    index_segment_reader.size()
                            );
                            const UpdateRanges update_ranges =
                                    compute_update_ranges(query.row_filter, *frame, new_slice_and_keys);
                            auto new_slice_and_keys_ptr =
                                    std::make_shared<std::vector<SliceAndKey>>(std::move(new_slice_and_keys));
                            return async_intersecting_segments(
                                           affected_keys,
                                           update_ranges.front,
                                           update_ranges.back,
                                           update_info.next_version_id_,
                                           store
                            )
                                    .thenValue([new_slice_and_keys_ptr,
                                                update_ranges = update_ranges,
                                                unaffected_keys = std::move(unaffected_keys),
                                                affected_keys = std::move(affected_keys),
                                                index_segment_reader = std::move(index_segment_reader),
                                                frame,
                                                dynamic_schema,
                                                update_info,
                                                store](IntersectingSegments&& intersecting_segments) mutable {
                                        auto [flattened_slice_and_keys, row_count] = get_slice_and_keys_for_update(
                                                update_ranges,
                                                unaffected_keys,
                                                *affected_keys,
                                                std::move(intersecting_segments),
                                                std::move(*new_slice_and_keys_ptr)
                                        );
                                        auto tsd = index::get_merged_tsd(
                                                row_count, dynamic_schema, index_segment_reader.tsd(), frame
                                        );
                                        return index::write_index(
                                                index_type_from_descriptor(tsd.as_stream_descriptor()),
                                                std::move(tsd),
                                                std::move(flattened_slice_and_keys),
                                                IndexPartialKey{frame->desc().id(), update_info.next_version_id_},
                                                store
                                        );
                                    })
                                    .thenError([store,
                                                new_slice_and_keys_ptr](folly::exception_wrapper&& error) mutable {
                                        if (error.template is_compatible_with<QuotaExceededException>()) {
                                            return remove_slice_and_keys(std::move(*new_slice_and_keys_ptr), *store)
                                                    .thenTry([error](auto&&) {
                                                        return folly::makeSemiFuture<AtomKeyImpl>(error);
                                                    });
                                        }
                                        return folly::makeFuture<AtomKeyImpl>(error);
                                    });
                        });
            });
}

VersionedItem update_impl(
        const std::shared_ptr<Store>& store, const UpdateInfo& update_info, const UpdateQuery& query,
        const std::shared_ptr<InputFrame>& frame, WriteOptions&& options, bool dynamic_schema, bool empty_types
) {
    auto versioned_item = VersionedItem(
            async_update_impl(store, update_info, query, frame, std::move(options), dynamic_schema, empty_types).get()
    );
    ARCTICDB_DEBUG(
            log::version(), "updated stream_id: {} , version_id: {}", frame->desc().id(), update_info.next_version_id_
    );
    return versioned_item;
}

folly::Future<ReadVersionOutput> read_multi_key(
        const std::shared_ptr<Store>& store, const ReadOptions& read_options, const SegmentInMemory& index_key_seg,
        std::any& handler_data, AtomKey&& key
) {
    std::vector<AtomKey> keys;
    keys.reserve(index_key_seg.row_count());
    for (size_t idx = 0; idx < index_key_seg.row_count(); idx++) {
        keys.emplace_back(read_key_row(index_key_seg, static_cast<ssize_t>(idx)));
    }

    AtomKey dup{keys[0]};
    VersionedItem versioned_item{std::move(dup)};
    TimeseriesDescriptor multi_key_desc{index_key_seg.index_descriptor()};

    return read_frame_for_version(store, versioned_item, std::make_shared<ReadQuery>(), read_options, handler_data)
            .thenValue([multi_key_desc = std::move(multi_key_desc),
                        keys = std::move(keys),
                        key = std::move(key)](ReadVersionOutput&& read_version_output) mutable {
                multi_key_desc.mutable_proto().mutable_normalization()->CopyFrom(
                        read_version_output.frame_and_descriptor_.desc_.proto().normalization()
                );
                read_version_output.frame_and_descriptor_.desc_ = std::move(multi_key_desc);
                read_version_output.frame_and_descriptor_.keys_ = std::move(keys);
                read_version_output.versioned_item_ = VersionedItem(std::move(key));
                return std::move(read_version_output);
            });
}

void add_slice_to_component_manager(
        EntityId entity_id, pipelines::SegmentAndSlice& segment_and_slice,
        std::shared_ptr<ComponentManager> component_manager, EntityFetchCount fetch_count
) {
    ARCTICDB_DEBUG(log::memory(), "Adding entity id {}", entity_id);
    component_manager->add_entity(
            entity_id,
            std::make_shared<SegmentInMemory>(std::move(segment_and_slice.segment_in_memory_)),
            std::make_shared<RowRange>(std::move(segment_and_slice.ranges_and_key_.row_range_)),
            std::make_shared<ColRange>(std::move(segment_and_slice.ranges_and_key_.col_range_)),
            std::make_shared<AtomKey>(std::move(segment_and_slice.ranges_and_key_.key_)),
            fetch_count
    );
}

size_t num_scheduling_iterations(const std::vector<std::shared_ptr<Clause>>& clauses) {
    if (clauses.empty()) {
        return 0UL;
    }
    size_t res = 1UL;
    for (auto it = std::next(clauses.cbegin()); it != clauses.cend(); ++it) {
        auto prev_it = std::prev(it);
        if ((*prev_it)->clause_info().output_structure_ != (*it)->clause_info().input_structure_) {
            ++res;
        }
    }
    ARCTICDB_DEBUG(
            log::memory(), "Processing pipeline has {} scheduling stages after the initial read and process", res
    );
    return res;
}

void remove_processed_clauses(std::vector<std::shared_ptr<Clause>>& clauses) {
    // Erase all the clauses we have already scheduled to run
    ARCTICDB_SAMPLE_DEFAULT(RemoveProcessedClauses)
    if (!clauses.empty()) {
        auto it = std::next(clauses.cbegin());
        while (it != clauses.cend()) {
            auto prev_it = std::prev(it);
            if ((*prev_it)->clause_info().output_structure_ == (*it)->clause_info().input_structure_) {
                ++it;
            } else {
                break;
            }
        }
        clauses.erase(clauses.cbegin(), it);
    }
}

std::pair<std::vector<std::vector<EntityId>>, std::shared_ptr<ankerl::unordered_dense::map<EntityId, size_t>>>
get_entity_ids_and_position_map(
        std::shared_ptr<ComponentManager>& component_manager, size_t num_segments,
        std::vector<std::vector<size_t>>&& processing_unit_indexes
) {
    // Map from entity id to position in segment_and_slice_futures
    auto id_to_pos = std::make_shared<ankerl::unordered_dense::map<EntityId, size_t>>();
    id_to_pos->reserve(num_segments);

    // Map from position in segment_and_slice_future_splitters to entity ids
    std::vector<EntityId> pos_to_id;
    pos_to_id.reserve(num_segments);

    auto ids = component_manager->get_new_entity_ids(num_segments);
    for (auto&& [idx, id] : folly::enumerate(ids)) {
        pos_to_id.emplace_back(id);
        id_to_pos->emplace(id, idx);
    }

    std::vector<std::vector<EntityId>> entity_work_units;
    entity_work_units.reserve(processing_unit_indexes.size());
    for (const auto& indexes : processing_unit_indexes) {
        entity_work_units.emplace_back();
        entity_work_units.back().reserve(indexes.size());
        for (auto index : indexes) {
            entity_work_units.back().emplace_back(pos_to_id[index]);
        }
    }

    return std::make_pair(std::move(entity_work_units), std::move(id_to_pos));
}

std::shared_ptr<std::vector<folly::Future<std::vector<EntityId>>>> schedule_first_iteration(
        std::shared_ptr<ComponentManager> component_manager, size_t num_segments,
        std::vector<std::vector<EntityId>>&& entities_by_work_unit,
        std::shared_ptr<std::vector<EntityFetchCount>>&& segment_fetch_counts,
        std::vector<FutureOrSplitter>&& segment_and_slice_future_splitters,
        std::shared_ptr<ankerl::unordered_dense::map<EntityId, size_t>>&& id_to_pos,
        std::shared_ptr<std::vector<std::shared_ptr<Clause>>>& clauses
) {
    // Used to make sure each entity is only added into the component manager once
    auto slice_added_mtx = std::make_shared<std::vector<std::mutex>>(num_segments);
    auto slice_added = std::make_shared<std::vector<bool>>(num_segments, false);
    auto futures = std::make_shared<std::vector<folly::Future<std::vector<EntityId>>>>();

    for (auto& entity_ids : entities_by_work_unit) {
        std::vector<folly::Future<pipelines::SegmentAndSlice>> local_futs;
        local_futs.reserve(entity_ids.size());
        for (auto id : entity_ids) {
            const auto pos = id_to_pos->at(id);
            auto& future_or_splitter = segment_and_slice_future_splitters[pos];
            // Some of the entities for this unit of work may be shared with other units of work
            util::variant_match(
                    future_or_splitter,
                    [&local_futs](folly::Future<pipelines::SegmentAndSlice>& fut) {
                        local_futs.emplace_back(std::move(fut));
                    },
                    [&local_futs](folly::FutureSplitter<pipelines::SegmentAndSlice>& splitter) {
                        local_futs.emplace_back(splitter.getFuture());
                    }
            );
        }

        futures->emplace_back(folly::collect(local_futs)
                                      .via(&async::io_executor()
                                      ) // Stay on the same executor as the read so that we can inline if possible
                                      .thenValueInline([component_manager,
                                                        segment_fetch_counts,
                                                        id_to_pos,
                                                        slice_added_mtx,
                                                        slice_added,
                                                        clauses,
                                                        entity_ids = std::move(entity_ids
                                                        )](std::vector<pipelines::SegmentAndSlice>&& segment_and_slices
                                                       ) mutable {
                                          for (auto&& [idx, segment_and_slice] : folly::enumerate(segment_and_slices)) {
                                              auto entity_id = entity_ids[idx];
                                              auto pos = id_to_pos->at(entity_id);
                                              std::lock_guard lock{slice_added_mtx->at(pos)};
                                              if (!(*slice_added)[pos]) {
                                                  ARCTICDB_DEBUG(log::version(), "Adding entity {}", entity_id);
                                                  add_slice_to_component_manager(
                                                          entity_id,
                                                          segment_and_slice,
                                                          component_manager,
                                                          segment_fetch_counts->at(pos)
                                                  );
                                                  (*slice_added)[pos] = true;
                                              }
                                          }
                                          return async::MemSegmentProcessingTask(*clauses, std::move(entity_ids))();
                                      }));
    }
    return futures;
}

folly::Future<std::vector<EntityId>> schedule_remaining_iterations(
        std::vector<std::vector<EntityId>>&& entity_ids_vec,
        std::shared_ptr<std::vector<std::shared_ptr<Clause>>> clauses
) {
    auto scheduling_iterations = num_scheduling_iterations(*clauses);
    folly::Future<std::vector<std::vector<EntityId>>> entity_ids_vec_fut(std::move(entity_ids_vec));
    for (auto i = 0UL; i < scheduling_iterations; ++i) {
        entity_ids_vec_fut =
                std::move(entity_ids_vec_fut)
                        .thenValue([clauses,
                                    scheduling_iterations,
                                    i](std::vector<std::vector<EntityId>>&& entity_id_vectors) {
                            ARCTICDB_RUNTIME_DEBUG(
                                    log::memory(), "Scheduling iteration {} of {}", i, scheduling_iterations
                            );

                            util::check(
                                    !clauses->empty(),
                                    "Scheduling iteration {} has no clauses to process",
                                    scheduling_iterations
                            );
                            if (i > 0) {
                                remove_processed_clauses(*clauses);
                            }
                            auto next_units_of_work =
                                    clauses->front()->structure_for_processing(std::move(entity_id_vectors));

                            std::vector<folly::Future<std::vector<EntityId>>> work_futures;
                            for (auto& unit_of_work : next_units_of_work) {
                                ARCTICDB_RUNTIME_DEBUG(
                                        log::memory(), "Scheduling work for entity ids: {}", unit_of_work
                                );
                                work_futures.emplace_back(async::submit_cpu_task(
                                        async::MemSegmentProcessingTask{*clauses, std::move(unit_of_work)}
                                ));
                            }

                            return folly::collect(work_futures).via(&async::io_executor());
                        });
    }
    return std::move(entity_ids_vec_fut).thenValueInline(flatten_entities);
}

folly::Future<std::vector<EntityId>> schedule_clause_processing(
        std::shared_ptr<ComponentManager> component_manager,
        std::vector<folly::Future<pipelines::SegmentAndSlice>>&& segment_and_slice_futures,
        std::vector<std::vector<size_t>>&& processing_unit_indexes,
        std::shared_ptr<std::vector<std::shared_ptr<Clause>>> clauses
) {
    // All the shared pointers as arguments to this function and created within it are to ensure that resources are
    // correctly kept alive after this function returns its future
    const auto num_segments = segment_and_slice_futures.size();

    // Map from index in segment_and_slice_future_splitters to the number of calls to process in the first clause that
    // will require that segment
    auto segment_fetch_counts = generate_segment_fetch_counts(processing_unit_indexes, num_segments);

    auto segment_and_slice_future_splitters =
            split_futures(std::move(segment_and_slice_futures), *segment_fetch_counts);

    auto [entities_by_work_unit, entity_id_to_segment_pos] =
            get_entity_ids_and_position_map(component_manager, num_segments, std::move(processing_unit_indexes));

    // At this point we have a set of entity ids grouped by the work units produced by the original
    // structure_for_processing, and a map of those ids to the position in the vector of futures or future-splitters
    // (which is the same order as originally generated from the index via the pipeline_context and ranges_and_keys), so
    // we can add each entity id and its components to the component manager and schedule the first stage of work (i.e.
    // from the beginning until either the end of the pipeline or the next required structure_for_processing
    auto futures = schedule_first_iteration(
            component_manager,
            num_segments,
            std::move(entities_by_work_unit),
            std::move(segment_fetch_counts),
            std::move(segment_and_slice_future_splitters),
            std::move(entity_id_to_segment_pos),
            clauses
    );

    return folly::collect(*futures).via(&async::io_executor()).thenValueInline([clauses](auto&& entity_ids_vec) {
        remove_processed_clauses(*clauses);
        return schedule_remaining_iterations(std::move(entity_ids_vec), clauses);
    });
}

void set_output_descriptors(
        const ProcessingUnit& proc, const std::vector<std::shared_ptr<Clause>>& clauses,
        const std::shared_ptr<PipelineContext>& pipeline_context
) {
    std::optional<std::string> index_column;
    for (auto clause = clauses.rbegin(); clause != clauses.rend(); ++clause) {
        bool should_break = util::variant_match(
                (*clause)->clause_info().index_,
                [](const KeepCurrentIndex&) { return false; },
                [&](const KeepCurrentTopLevelIndex&) {
                    if (pipeline_context->norm_meta_->mutable_df()->mutable_common()->has_multi_index()) {
                        const auto& multi_index =
                                pipeline_context->norm_meta_->mutable_df()->mutable_common()->multi_index();
                        auto name = multi_index.name();
                        auto tz = multi_index.tz();
                        bool fake_name{false};
                        for (auto pos : multi_index.fake_field_pos()) {
                            if (pos == 0) {
                                fake_name = true;
                                break;
                            }
                        }
                        auto mutable_index =
                                pipeline_context->norm_meta_->mutable_df()->mutable_common()->mutable_index();
                        mutable_index->set_tz(tz);
                        mutable_index->set_is_physically_stored(true);
                        mutable_index->set_name(name);
                        mutable_index->set_fake_name(fake_name);
                    }
                    return true;
                },
                [&](const NewIndex& new_index) {
                    index_column = new_index;
                    auto mutable_index = pipeline_context->norm_meta_->mutable_df()->mutable_common()->mutable_index();
                    mutable_index->set_name(new_index);
                    mutable_index->clear_fake_name();
                    mutable_index->set_is_physically_stored(true);
                    return true;
                }
        );
        if (should_break) {
            break;
        }
    }
    std::optional<StreamDescriptor> new_stream_descriptor;
    if (proc.segments_.has_value() && !proc.segments_->empty()) {
        new_stream_descriptor = std::make_optional<StreamDescriptor>();
        new_stream_descriptor->set_index(proc.segments_->at(0)->descriptor().index());
        for (size_t idx = 0; idx < new_stream_descriptor->index().field_count(); idx++) {
            new_stream_descriptor->add_field(proc.segments_->at(0)->descriptor().field(idx));
        }
    }
    if (new_stream_descriptor.has_value() && proc.segments_.has_value()) {
        std::vector<std::shared_ptr<FieldCollection>> fields;
        for (const auto& segment : *proc.segments_) {
            fields.push_back(segment->descriptor().fields_ptr());
        }
        new_stream_descriptor = merge_descriptors(*new_stream_descriptor, fields, std::vector<std::string>{});
    }
    if (new_stream_descriptor.has_value()) {
        // Finding and erasing fields from the FieldCollection contained in StreamDescriptor is O(n) in number of fields
        // So maintain map from field names to types in the new_stream_descriptor to make these operations O(1)
        // Cannot use set of FieldRef as the name in the output might match the input, but with a different type after
        // processing
        std::unordered_map<std::string_view, TypeDescriptor> new_fields;
        for (const auto& field : new_stream_descriptor->fields()) {
            new_fields.emplace(field.name(), field.type());
        }
        // Columns might be in a different order to the original dataframe, so reorder here
        auto original_stream_descriptor = pipeline_context->descriptor();
        StreamDescriptor final_stream_descriptor{original_stream_descriptor.id()};
        final_stream_descriptor.set_index(new_stream_descriptor->index());
        // Erase field from new_fields as we add them to final_stream_descriptor, as all fields left in new_fields
        // after these operations were created by the processing pipeline, and so should be appended
        // Index columns should always appear first
        if (index_column.has_value()) {
            const auto nh = new_fields.extract(*index_column);
            internal::check<ErrorCode::E_ASSERTION_FAILURE>(
                    !nh.empty(), "New index column not found in processing pipeline"
            );
            final_stream_descriptor.add_field(FieldRef{nh.mapped(), nh.key()});
        }
        for (const auto& field : original_stream_descriptor.fields()) {
            if (const auto nh = new_fields.extract(field.name()); nh) {
                final_stream_descriptor.add_field(FieldRef{nh.mapped(), nh.key()});
            }
        }
        // Iterate through new_stream_descriptor->fields() rather than remaining new_fields to preserve ordering
        // e.g. if there were two projections then users will expect the column produced by the first one to appear
        // first in the output df
        for (const auto& field : new_stream_descriptor->fields()) {
            if (new_fields.contains(field.name())) {
                final_stream_descriptor.add_field(field);
            }
        }
        pipeline_context->set_descriptor(final_stream_descriptor);
    }
}

std::shared_ptr<std::unordered_set<std::string>> columns_to_decode(
        const std::shared_ptr<PipelineContext>& pipeline_context
) {
    std::shared_ptr<std::unordered_set<std::string>> res;
    ARCTICDB_DEBUG(
            log::version(),
            "Creating columns list with {} bits set",
            pipeline_context->overall_column_bitset_ ? pipeline_context->overall_column_bitset_->count() : -1
    );
    if (pipeline_context->overall_column_bitset_) {
        res = std::make_shared<std::unordered_set<std::string>>();
        auto en = pipeline_context->overall_column_bitset_->first();
        auto en_end = pipeline_context->overall_column_bitset_->end();
        while (en < en_end) {
            ARCTICDB_DEBUG(log::version(), "Adding field {}", pipeline_context->desc_->field(*en).name());
            res->insert(std::string(pipeline_context->desc_->field(*en++).name()));
        }
    }
    return res;
}

std::vector<RangesAndKey> generate_ranges_and_keys(PipelineContext& pipeline_context) {
    std::vector<RangesAndKey> res;
    res.reserve(pipeline_context.slice_and_keys_.size());
    bool is_incomplete{false};
    for (auto it = pipeline_context.begin(); it != pipeline_context.end(); it++) {
        if (it == pipeline_context.incompletes_begin()) {
            is_incomplete = true;
        }
        auto& sk = it->slice_and_key();
        // Take a copy here as things like defrag need the keys in pipeline_context->slice_and_keys_ that aren't being
        // modified at the end
        auto key = sk.key();
        res.emplace_back(sk.slice(), std::move(key), is_incomplete);
    }
    return res;
}

util::BitSet get_incompletes_bitset(const std::vector<RangesAndKey>& all_ranges) {
    util::BitSet output(all_ranges.size());
    util::BitSet::bulk_insert_iterator it(output);
    for (auto&& [index, range] : folly::enumerate(all_ranges)) {
        if (range.is_incomplete())
            it = index;
    }
    it.flush();
    return output;
}

std::vector<folly::Future<pipelines::SegmentAndSlice>> add_schema_check(
        const std::shared_ptr<PipelineContext>& pipeline_context,
        std::vector<folly::Future<pipelines::SegmentAndSlice>>&& segment_and_slice_futures,
        util::BitSet&& incomplete_bitset, const ProcessingConfig& processing_config
) {
    std::vector<folly::Future<pipelines::SegmentAndSlice>> res;
    res.reserve(segment_and_slice_futures.size());
    for (size_t i = 0; i < segment_and_slice_futures.size(); ++i) {
        auto&& fut = segment_and_slice_futures.at(i);
        const bool is_incomplete = incomplete_bitset[i];
        if (is_incomplete) {
            res.push_back(std::move(fut).thenValueInline([pipeline_desc = pipeline_context->descriptor(),
                                                          processing_config](SegmentAndSlice&& read_result) {
                if (!processing_config.dynamic_schema_) {
                    auto check =
                            check_schema_matches_incomplete(read_result.segment_in_memory_.descriptor(), pipeline_desc);
                    if (std::holds_alternative<Error>(check)) {
                        std::get<Error>(check).throw_error();
                    }
                }
                return std::move(read_result);
            }));
        } else {
            res.push_back(std::move(fut));
        }
    }
    return res;
}

std::vector<folly::Future<pipelines::SegmentAndSlice>> generate_segment_and_slice_futures(
        const std::shared_ptr<Store>& store, const std::shared_ptr<PipelineContext>& pipeline_context,
        const ProcessingConfig& processing_config, std::vector<RangesAndKey>&& all_ranges
) {
    auto incomplete_bitset = get_incompletes_bitset(all_ranges);
    auto segment_and_slice_futures =
            store->batch_read_uncompressed(std::move(all_ranges), columns_to_decode(pipeline_context));
    return add_schema_check(
            pipeline_context, std::move(segment_and_slice_futures), std::move(incomplete_bitset), processing_config
    );
}

static StreamDescriptor generate_initial_output_schema_descriptor(const PipelineContext& pipeline_context) {
    const StreamDescriptor& desc = pipeline_context.descriptor();
    // pipeline_context.overall_column_bitset_ can be different from std::nullopt only in case of static schema. We use
    // it to constrain the initial set of columns. If dynamic schema is used and only certain columns must be read we
    // use the whole descriptor and at the end of the read return only the ones that were selected. This is because
    // currently dynamic schema cannot handle column dependencies e.g.
    // columns on disk: col1, col2
    // q = QueryBuilder()
    // q = q["col1" < 1]
    // lib.read(columns=["col2", query_builder=q)
    // We want to read only col2 but the filtering requires col1, thus the OutputSchema must contain both col1 and col2
    // in order to satisfy the filter clause.
    if (pipeline_context.overall_column_bitset_) {
        FieldCollection fields_to_use;
        auto overall_fields_it = pipeline_context.overall_column_bitset_->first();
        const auto overall_fields_end = pipeline_context.overall_column_bitset_->end();
        while (overall_fields_it != overall_fields_end) {
            fields_to_use.add(desc.field(*overall_fields_it).ref());
            ++overall_fields_it;
        }
        return StreamDescriptor{desc.data_ptr(), std::make_shared<FieldCollection>(std::move(fields_to_use))};
    }
    return desc;
}

static OutputSchema create_initial_output_schema(PipelineContext& pipeline_context) {
    internal::check<ErrorCode::E_ASSERTION_FAILURE>(
            pipeline_context.norm_meta_, "Normalization metadata should not be missing during read_and_process"
    );
    return OutputSchema{generate_initial_output_schema_descriptor(pipeline_context), *pipeline_context.norm_meta_};
}

static OutputSchema generate_output_schema(PipelineContext& pipeline_context, const ReadQuery& read_query) {
    OutputSchema output_schema = create_initial_output_schema(pipeline_context);
    for (const auto& clause : read_query.clauses_) {
        output_schema = clause->modify_schema(std::move(output_schema));
    }
    if (read_query.columns) {
        std::unordered_set<std::string_view> selected_columns(read_query.columns->begin(), read_query.columns->end());
        FieldCollection fields_to_use;
        if (!pipeline_context.filter_columns_) {
            pipeline_context.filter_columns_ = std::make_shared<FieldCollection>();
        }
        for (const Field& field : output_schema.stream_descriptor().fields()) {
            if (selected_columns.contains(field.name())) {
                fields_to_use.add(field.ref());
                if (!pipeline_context.is_in_filter_columns_set(field.name())) {
                    pipeline_context.filter_columns_->add(field.ref());
                }
            }
        }
        pipeline_context.filter_columns_set_ = std::move(selected_columns);
        output_schema.set_stream_descriptor(StreamDescriptor{
                output_schema.stream_descriptor().data_ptr(),
                std::make_shared<FieldCollection>(std::move(fields_to_use))
        });
    }
    return output_schema;
}

static void generate_output_schema_and_save_to_pipeline(
        PipelineContext& pipeline_context, const ReadQuery& read_query
) {
    OutputSchema schema = generate_output_schema(pipeline_context, read_query);
    auto&& [descriptor, norm_meta, default_values] = schema.release();
    pipeline_context.set_descriptor(std::forward<StreamDescriptor>(descriptor));
    pipeline_context.norm_meta_ = std::make_shared<proto::descriptors::NormalizationMetadata>(
            std::forward<proto::descriptors::NormalizationMetadata>(norm_meta)
    );
    pipeline_context.default_values_ = std::forward<decltype(default_values)>(default_values);
}

folly::Future<std::vector<EntityId>> read_and_schedule_processing(
        const std::shared_ptr<Store>& store, const std::shared_ptr<PipelineContext>& pipeline_context,
        const std::shared_ptr<ReadQuery>& read_query, const ReadOptions& read_options,
        std::shared_ptr<ComponentManager> component_manager
) {
    const ProcessingConfig processing_config{
            opt_false(read_options.dynamic_schema()),
            pipeline_context->rows_,
            pipeline_context->descriptor().index().type()
    };
    for (auto& clause : read_query->clauses_) {
        clause->set_processing_config(processing_config);
        clause->set_component_manager(component_manager);
    }

    auto ranges_and_keys = generate_ranges_and_keys(*pipeline_context);

    // Each element of the vector corresponds to one processing unit containing the list of indexes in ranges_and_keys
    // required for that processing unit i.e. if the first processing unit needs ranges_and_keys[0] and
    // ranges_and_keys[1], and the second needs ranges_and_keys[2] and ranges_and_keys[3] then the structure will be
    // {{0, 1}, {2, 3}}
    std::vector<std::vector<size_t>> processing_unit_indexes;
    if (read_query->clauses_.empty()) {
        processing_unit_indexes = structure_by_row_slice(ranges_and_keys);
    } else {
        processing_unit_indexes = read_query->clauses_[0]->structure_for_processing(ranges_and_keys);
    }

    // Start reading as early as possible
    auto segment_and_slice_futures =
            generate_segment_and_slice_futures(store, pipeline_context, processing_config, std::move(ranges_and_keys));

    return schedule_clause_processing(
                   component_manager,
                   std::move(segment_and_slice_futures),
                   std::move(processing_unit_indexes),
                   std::make_shared<std::vector<std::shared_ptr<Clause>>>(read_query->clauses_)
    )
            .via(&async::cpu_executor());
}

/*
 * Processes the slices in the given pipeline_context.
 *
 * Slices are processed in an order defined by the first clause in the pipeline, with slices corresponding to the same
 * processing unit collected into a single ProcessingUnit. Slices contained within a single ProcessingUnit are processed
 * within a single thread.
 *
 * Where possible (generally, when there is no column slicing), clauses are processed in the same folly thread as the
 * decompression without context switching to try and optimise cache access.
 */
folly::Future<std::vector<SliceAndKey>> read_process_and_collect(
        const std::shared_ptr<Store>& store, const std::shared_ptr<PipelineContext>& pipeline_context,
        const std::shared_ptr<ReadQuery>& read_query, const ReadOptions& read_options
) {
    auto component_manager = std::make_shared<ComponentManager>();
    return read_and_schedule_processing(store, pipeline_context, read_query, read_options, component_manager)
            .thenValue([component_manager, pipeline_context, read_query](std::vector<EntityId>&& processed_entity_ids) {
                generate_output_schema_and_save_to_pipeline(*pipeline_context, *read_query);
                auto proc = gather_entities<
                        std::shared_ptr<SegmentInMemory>,
                        std::shared_ptr<RowRange>,
                        std::shared_ptr<ColRange>>(*component_manager, processed_entity_ids);
                return collect_segments(std::move(proc));
            });
}

void add_index_columns_to_query(const ReadQuery& read_query, const TimeseriesDescriptor& desc) {
    if (read_query.columns.has_value()) {
        auto index_columns = stream::get_index_columns_from_descriptor(desc);
        if (index_columns.empty())
            return;

        std::vector<std::string> index_columns_to_add;
        for (const auto& index_column : index_columns) {
            if (ranges::find(*read_query.columns, index_column) == std::end(*read_query.columns))
                index_columns_to_add.emplace_back(index_column);
        }
        read_query.columns->insert(
                std::begin(*read_query.columns), std::begin(index_columns_to_add), std::end(index_columns_to_add)
        );
    }
}

FrameAndDescriptor read_segment_impl(const std::shared_ptr<Store>& store, const VariantKey& key) {
    auto seg = store->read_compressed_sync(key).segment_ptr();
    return frame_and_descriptor_from_segment(decode_segment(*seg, AllocationType::DETACHABLE));
}

FrameAndDescriptor read_index_impl(const std::shared_ptr<Store>& store, const VersionedItem& version) {
    return read_segment_impl(store, version.key_);
}

std::optional<index::IndexSegmentReader> get_index_segment_reader(
        Store& store, const std::shared_ptr<PipelineContext>& pipeline_context, const VersionedItem& version_info
) {
    std::pair<VariantKey, SegmentInMemory> index_key_seg = [&]() {
        try {
            return store.read_sync(version_info.key_);
        } catch (const std::exception& ex) {
            ARCTICDB_DEBUG(log::version(), "Key not found from versioned item {}: {}", version_info.key_, ex.what());
            throw storage::NoDataFoundException(fmt::format(
                    "When trying to read version {} of symbol `{}`, failed to read key {}: {}",
                    version_info.version(),
                    version_info.symbol(),
                    version_info.key_,
                    ex.what()
            ));
        }
    }();

    if (variant_key_type(index_key_seg.first) == KeyType::MULTI_KEY) {
        pipeline_context->multi_key_ = std::move(index_key_seg.second);
        return std::nullopt;
    }
    return std::make_optional<index::IndexSegmentReader>(std::move(index_key_seg.second));
}

void check_can_read_index_only_if_required(
        const index::IndexSegmentReader& index_segment_reader, const ReadQuery& read_query
) {
    user_input::check<ErrorCode::E_INVALID_USER_ARGUMENT>(
            !(index_segment_reader.tsd().proto().normalization().has_custom() && read_query.columns &&
              read_query.columns->empty()),
            "Reading the index column is not supported when recursive or custom normalizers are used."
    );
    user_input::check<ErrorCode::E_INVALID_USER_ARGUMENT>(
            !(index_segment_reader.is_pickled() && read_query.columns && read_query.columns->empty()),
            "Reading index columns is not supported with pickled data."
    );
}

void check_multi_key_is_not_index_only(const PipelineContext& pipeline_context, const ReadQuery& read_query) {
    user_input::check<ErrorCode::E_INVALID_USER_ARGUMENT>(
            !read_query.columns || (!pipeline_context.only_index_columns_selected() && !read_query.columns->empty()),
            "Reading the index column is not supported when recursive or custom normalizers are used."
    );
}

void check_can_perform_processing(
        const std::shared_ptr<PipelineContext>& pipeline_context, const ReadQuery& read_query
) {
    // To remain backward compatibility, pending new major release to merge into below section
    // Ticket: 18038782559
    const bool is_pickled = pipeline_context->norm_meta_ && pipeline_context->is_pickled();
    util::check(
            !is_pickled ||
                    (!read_query.columns.has_value() && std::holds_alternative<std::monostate>(read_query.row_filter)),
            "Cannot perform processing such as row/column filtering, projection, aggregation, resampling, "
            "etc.. on pickled data"
    );
    if (pipeline_context->multi_key_) {
        check_multi_key_is_not_index_only(*pipeline_context, read_query);
    }

    // To keep
    if (pipeline_context->desc_) {
        util::check(
                pipeline_context->descriptor().index().type() == IndexDescriptor::Type::TIMESTAMP ||
                        !std::holds_alternative<IndexRange>(read_query.row_filter),
                "Cannot apply date range filter to symbol with non-timestamp index"
        );
        const auto sorted_value = pipeline_context->descriptor().sorted();
        sorting::check<ErrorCode::E_UNSORTED_DATA>(
                sorted_value == SortedValue::UNKNOWN || sorted_value == SortedValue::ASCENDING ||
                        !std::holds_alternative<IndexRange>(read_query.row_filter),
                "When filtering data using date_range, the symbol must be sorted in ascending order. ArcticDB believes "
                "it is not sorted in ascending order and cannot therefore filter the data using date_range."
        );
    }
    const bool is_query_empty =
            (!read_query.columns && !read_query.row_range &&
             std::holds_alternative<std::monostate>(read_query.row_filter) && read_query.clauses_.empty());
    const bool is_numpy_array = pipeline_context->norm_meta_ && pipeline_context->norm_meta_->has_np();
    if (!is_query_empty) {
        // Exception for filterig pickled data is skipped for now for backward compatibility
        if (pipeline_context->multi_key_) {
            schema::raise<ErrorCode::E_OPERATION_NOT_SUPPORTED_WITH_RECURSIVE_NORMALIZED_DATA>(
                    "Cannot perform processing such as row/column filtering, projection, aggregation, resampling, "
                    "etc.. on recursively normalized data"
            );
        } else if (is_numpy_array) {
            schema::raise<ErrorCode::E_OPERATION_NOT_SUPPORTED_WITH_NUMPY_ARRAY>(
                    "Cannot perform processing such as row/column filtering, projection, aggregation, resampling, "
                    "etc.. on recursively numpy array"
            );
        }
    }
}

static void read_indexed_keys_to_pipeline(
        const std::shared_ptr<PipelineContext>& pipeline_context, index::IndexSegmentReader&& index_segment_reader,
        ReadQuery& read_query, const ReadOptions& read_options
) {
    ARCTICDB_DEBUG(log::version(), "Read index segment with {} keys", index_segment_reader.size());
    check_can_read_index_only_if_required(index_segment_reader, read_query);
    add_index_columns_to_query(read_query, index_segment_reader.tsd());

    const auto& tsd = index_segment_reader.tsd();
    read_query.convert_to_positive_row_filter(static_cast<int64_t>(tsd.total_rows()));
    const bool bucketize_dynamic = index_segment_reader.bucketize_dynamic();
    pipeline_context->desc_ = tsd.as_stream_descriptor();

    const bool dynamic_schema = opt_false(read_options.dynamic_schema());
    auto queries = get_column_bitset_and_query_functions<index::IndexSegmentReader>(
            read_query, pipeline_context, dynamic_schema, bucketize_dynamic
    );

    pipeline_context->slice_and_keys_ = filter_index(index_segment_reader, combine_filter_functions(queries));
    pipeline_context->total_rows_ = pipeline_context->calc_rows();
    pipeline_context->rows_ = index_segment_reader.tsd().total_rows();
    pipeline_context->norm_meta_ = std::make_unique<arcticdb::proto::descriptors::NormalizationMetadata>(
            std::move(*index_segment_reader.mutable_tsd().mutable_proto().mutable_normalization())
    );
    pipeline_context->user_meta_ = std::make_unique<arcticdb::proto::descriptors::UserDefinedMetadata>(
            std::move(*index_segment_reader.mutable_tsd().mutable_proto().mutable_user_meta())
    );
    pipeline_context->bucketize_dynamic_ = bucketize_dynamic;
    check_can_perform_processing(pipeline_context, read_query);
    ARCTICDB_DEBUG(
            log::version(),
            "read_indexed_keys_to_pipeline: Symbol {} Found {} keys with {} total rows",
            pipeline_context->slice_and_keys_.size(),
            pipeline_context->total_rows_,
            pipeline_context->stream_id_
    );
}

// Returns true if there are staged segments
// When stage_results is present, only read keys represented by them.
static std::variant<bool, CompactionError> read_incompletes_to_pipeline(
        const std::shared_ptr<Store>& store, std::shared_ptr<PipelineContext>& pipeline_context,
        const std::optional<std::vector<StageResult>>& stage_results, const ReadQuery& read_query,
        const ReadOptions& read_options, const ReadIncompletesFlags& flags
) {

    std::vector<SliceAndKey> incomplete_segments;
    bool load_data{false};
    if (stage_results) {
        auto res = get_incomplete_segments_using_stage_results(
                store, pipeline_context, *stage_results, read_query, flags, load_data
        );
        if (std::holds_alternative<CompactionError>(res)) {
            return std::get<CompactionError>(res);
        } else {
            incomplete_segments = std::get<std::vector<SliceAndKey>>(res);
        }
    } else {
        incomplete_segments = get_incomplete(
                store,
                pipeline_context->stream_id_,
                read_query.row_filter,
                pipeline_context->last_row(),
                flags.via_iteration,
                false
        );
    }

    ARCTICDB_DEBUG(
            log::version(),
            "Symbol {}: Found {} incomplete segments",
            pipeline_context->stream_id_,
            incomplete_segments.size()
    );
    if (incomplete_segments.empty()) {
        return false;
    }

    // In order to have the right normalization metadata and descriptor we need to find the first non-empty segment.
    // Picking an empty segment when there are non-empty ones will impact the index type and column namings.
    // If all segments are empty we will proceed as if were appending/writing and empty dataframe.
    debug::check<ErrorCode::E_ASSERTION_FAILURE>(!incomplete_segments.empty(), "Incomplete segments must be non-empty");
    const auto first_non_empty_seg = ranges::find_if(incomplete_segments, [&](auto& slice) {
        auto res = slice.segment(store).row_count() > 0;
        ARCTICDB_DEBUG(log::version(), "Testing for non-empty seg {} res={}", slice.key(), res);
        return res;
    });
    const auto& seg = first_non_empty_seg != incomplete_segments.end() ? first_non_empty_seg->segment(store)
                                                                       : incomplete_segments.begin()->segment(store);
    ARCTICDB_DEBUG(
            log::version(),
            "Symbol {}: First segment has rows {} columns {} uncompressed bytes {} descriptor {}",
            pipeline_context->stream_id_,
            seg.row_count(),
            seg.columns().size(),
            seg.descriptor().uncompressed_bytes(),
            seg.index_descriptor()
    );
    // Mark the start point of the incompletes, so we know that there is no column slicing after this point
    pipeline_context->incompletes_after_ = pipeline_context->slice_and_keys_.size();

    if (!flags.has_active_version) {
        // If there are only incompletes we need to do the following (typically done when reading the index key):
        // - add the index columns to query
        // - in case of static schema: populate the descriptor and column_bitset
        add_index_columns_to_query(read_query, seg.index_descriptor());
        if (!flags.dynamic_schema) {
            pipeline_context->desc_ = seg.descriptor();
            get_column_bitset_in_context(read_query, pipeline_context);
        }
    }
    ranges::copy(incomplete_segments, std::back_inserter(pipeline_context->slice_and_keys_));

    if (!pipeline_context->norm_meta_) {
        pipeline_context->norm_meta_ = std::make_unique<arcticdb::proto::descriptors::NormalizationMetadata>();
        pipeline_context->norm_meta_->CopyFrom(seg.index_descriptor().proto().normalization());
        ensure_timeseries_norm_meta(*pipeline_context->norm_meta_, pipeline_context->stream_id_, flags.sparsify);
    }

    const StreamDescriptor& staged_desc = incomplete_segments[0].segment(store).descriptor();

    // We need to check that the index names match regardless of the dynamic schema setting
    // A more detailed check is done later in the do_compact function
    if (pipeline_context->desc_) {
        schema::check<ErrorCode::E_DESCRIPTOR_MISMATCH>(
                index_names_match(staged_desc, *pipeline_context->desc_),
                "The index names in the staged stream descriptor {} are not identical to that of the stream descriptor "
                "on storage {}",
                staged_desc,
                *pipeline_context->desc_
        );
    }

    if (flags.dynamic_schema) {
        ARCTICDB_DEBUG(log::version(), "read_incompletes_to_pipeline: Dynamic schema");
        pipeline_context->staged_descriptor_ = merge_descriptors(
                seg.descriptor(), incomplete_segments, read_query.columns, std::nullopt, flags.convert_int_to_float
        );
        if (pipeline_context->desc_) {
            const std::array staged_fields_ptr = {pipeline_context->staged_descriptor_->fields_ptr()};
            pipeline_context->desc_ =
                    merge_descriptors(*pipeline_context->desc_, staged_fields_ptr, read_query.columns);
        } else {
            pipeline_context->desc_ = pipeline_context->staged_descriptor_;
        }
    } else {
        ARCTICDB_DEBUG(log::version(), "read_incompletes_to_pipeline: Static schema");
        [[maybe_unused]] auto& first_incomplete_seg = incomplete_segments[0].segment(store);
        ARCTICDB_DEBUG(
                log::version(),
                "Symbol {}: First incomplete segment has rows {} columns {} uncompressed bytes {} descriptor {}",
                pipeline_context->stream_id_,
                first_incomplete_seg.row_count(),
                first_incomplete_seg.columns().size(),
                first_incomplete_seg.descriptor().uncompressed_bytes(),
                first_incomplete_seg.index_descriptor()
        );
        if (pipeline_context->desc_) {
            schema::check<ErrorCode::E_DESCRIPTOR_MISMATCH>(
                    columns_match(*pipeline_context->desc_, staged_desc, flags.convert_int_to_float),
                    "When static schema is used the staged stream descriptor {} must equal the stream descriptor on "
                    "storage {}",
                    staged_desc,
                    *pipeline_context->desc_
            );
        }
        pipeline_context->staged_descriptor_ = staged_desc;
        pipeline_context->desc_ = staged_desc;
    }

    modify_descriptor(pipeline_context, read_options);
    if (flags.convert_int_to_float) {
        stream::convert_descriptor_types(*pipeline_context->staged_descriptor_);
    }

    generate_filtered_field_descriptors(pipeline_context, read_query.columns);
    pipeline_context->total_rows_ = pipeline_context->calc_rows();
    return true;
}

static void check_incompletes_index_ranges_dont_overlap(
        const std::shared_ptr<PipelineContext>& pipeline_context,
        const std::optional<SortedValue> previous_sorted_value, const bool append_to_existing
) {
    /*
     Does nothing if the symbol is not timestamp-indexed
     Checks:
      - that the existing data is not known to be unsorted in the case of a parallel append
      - that the index ranges of incomplete segments do not overlap with one another
      - that the earliest timestamp in an incomplete segment is greater than the latest timestamp existing in the
        symbol in the case of a parallel append
     */
    if (pipeline_context->descriptor().index().type() == IndexDescriptorImpl::Type::TIMESTAMP) {
        std::optional<timestamp> last_existing_index_value;
        if (append_to_existing) {
            internal::check<ErrorCode::E_ASSERTION_FAILURE>(
                    previous_sorted_value.has_value(),
                    "When staged data is appended to existing data the descriptor should hold the \"sorted\" status of "
                    "the existing data"
            );
            sorting::check<ErrorCode::E_UNSORTED_DATA>(
                    *previous_sorted_value == SortedValue::ASCENDING || *previous_sorted_value == SortedValue::UNKNOWN,
                    "Cannot append staged segments to existing data as existing data is not sorted in ascending order"
            );
            auto last_indexed_slice_and_key = std::prev(pipeline_context->incompletes_begin())->slice_and_key();
            // -1 as end_time is stored as 1 greater than the last index value in the segment
            last_existing_index_value = last_indexed_slice_and_key.key().end_time() - 1;
        }

        // Use ordered set so that we only need to compare adjacent elements
        std::set<TimestampRange> unique_timestamp_ranges;
        for (auto it = pipeline_context->incompletes_begin(); it != pipeline_context->end(); ++it) {
            if (it->slice_and_key().slice().rows().diff() == 0) {
                continue;
            }

            const auto& key = it->slice_and_key().key();
            sorting::check<ErrorCode::E_UNSORTED_DATA>(
                    !last_existing_index_value.has_value() || key.start_time() >= *last_existing_index_value,
                    "Cannot append staged segments to existing data as incomplete segment contains index value < "
                    "existing data (in UTC): {} <= {}",
                    util::format_timestamp(key.start_time()),
                    // Should never reach "" but the standard mandates that all function arguments are evaluated
                    last_existing_index_value ? util::format_timestamp(*last_existing_index_value) : ""
            );
            auto [_, inserted] = unique_timestamp_ranges.emplace(key.start_time(), key.end_time());
            // This is correct because incomplete segments aren't column sliced
            sorting::check<ErrorCode::E_UNSORTED_DATA>(
                    // If the segment is entirely covering a single index value, then duplicates are fine
                    // -1 as end_time is stored as 1 greater than the last index value in the segment
                    inserted || key.end_time() - 1 == key.start_time(),
                    "Cannot finalize staged data as 2 or more incomplete segments cover identical index values (in "
                    "UTC): ({}, {})",
                    util::format_timestamp(key.start_time()),
                    util::format_timestamp(key.end_time())
            );
        }

        for (auto it = unique_timestamp_ranges.begin(); it != unique_timestamp_ranges.end(); it++) {
            auto next_it = std::next(it);
            if (next_it != unique_timestamp_ranges.end()) {
                sorting::check<ErrorCode::E_UNSORTED_DATA>(
                        // -1 as end_time is stored as 1 greater than the last index value in the segment
                        next_it->first >= it->second - 1,
                        "Cannot finalize staged data as incomplete segment index values overlap one another (in UTC): "
                        "({}, {}) intersects ({}, {})",
                        util::format_timestamp(it->first),
                        util::format_timestamp(it->second - 1),
                        util::format_timestamp(next_it->first),
                        util::format_timestamp(next_it->second - 1)
                );
            }
        }
    }
}

void init_sparse_dst_column_before_copy(
        Column& dst_column, size_t offset, size_t num_rows, size_t dst_rawtype_size, OutputFormat output_format,
        const std::optional<util::BitSet>& src_sparse_map, const std::optional<Value>& default_value
) {
    if (output_format != OutputFormat::ARROW || default_value.has_value()) {
        auto total_size = dst_rawtype_size * num_rows;
        auto dst_ptr = dst_column.bytes_at(offset, total_size);
        dst_column.type().visit_tag([&](auto dst_desc_tag) {
            util::initialize<decltype(dst_desc_tag)>(dst_ptr, total_size, default_value);
        });
    } else {
        if (src_sparse_map.has_value()) {
            create_dense_bitmap(offset, src_sparse_map.value(), dst_column, AllocationType::DETACHABLE);
        } else {
            create_dense_bitmap_all_zeros(offset, num_rows, dst_column, AllocationType::DETACHABLE);
        }
    }
}

void copy_frame_data_to_buffer(
        SegmentInMemory& destination, size_t target_index, SegmentInMemory& source, size_t source_index,
        const RowRange& row_range, DecodePathData shared_data, std::any& handler_data, const ReadOptions& read_options,
        const std::optional<Value>& default_value
) {
    const auto num_rows = row_range.diff();
    if (num_rows == 0) {
        return;
    }
    auto& src_column = source.column(static_cast<position_t>(source_index));
    auto& dst_column = destination.column(static_cast<position_t>(target_index));
    auto dst_rawtype_size = data_type_size(dst_column.type());
    auto offset = dst_rawtype_size * (row_range.first - destination.offset());
    const auto total_size = dst_rawtype_size * num_rows;
    dst_column.assert_size(offset + total_size);

    auto src_data = src_column.data();
    auto dst_ptr = dst_column.bytes_at(offset, total_size);

    auto type_promotion_error_msg = fmt::format(
            "Can't promote type {} to type {} in field {}",
            src_column.type(),
            dst_column.type(),
            destination.field(target_index).name()
    );
    if (auto handler = get_type_handler(read_options.output_format(), src_column.type(), dst_column.type()); handler) {
        const auto type_size = data_type_size(dst_column.type());
        const ColumnMapping mapping{
                src_column.type(),
                dst_column.type(),
                destination.field(target_index),
                type_size,
                num_rows,
                row_range.first,
                offset,
                total_size,
                target_index
        };
        handler->convert_type(
                src_column, dst_column, mapping, shared_data, handler_data, source.string_pool_ptr(), read_options
        );
    } else if (is_empty_type(src_column.type().data_type())) {
        init_sparse_dst_column_before_copy(
                dst_column,
                offset,
                num_rows,
                dst_rawtype_size,
                read_options.output_format(),
                std::nullopt,
                default_value
        );
        // Do not use src_column.is_sparse() here, as that misses columns that are dense, but have fewer than num_rows
        // values
    } else if (src_column.opt_sparse_map().has_value() &&
               is_valid_type_promotion_to_target(
                       src_column.type(), dst_column.type(), IntToFloatConversion::PERMISSIVE
               )) {
        details::visit_type(dst_column.type().data_type(), [&](auto dst_tag) {
            using dst_type_info = ScalarTypeInfo<decltype(dst_tag)>;
            typename dst_type_info::RawType* typed_dst_ptr =
                    reinterpret_cast<typename dst_type_info::RawType*>(dst_ptr);
            init_sparse_dst_column_before_copy(
                    dst_column,
                    offset,
                    num_rows,
                    dst_rawtype_size,
                    read_options.output_format(),
                    src_column.opt_sparse_map(),
                    default_value
            );
            details::visit_type(src_column.type().data_type(), [&](auto src_tag) {
                using src_type_info = ScalarTypeInfo<decltype(src_tag)>;
                arcticdb::for_each_enumerated<typename src_type_info::TDT>(
                        src_column,
                        [typed_dst_ptr] ARCTICDB_LAMBDA_INLINE(auto enumerating_it) {
                            typed_dst_ptr[enumerating_it.idx()] =
                                    static_cast<typename dst_type_info::RawType>(enumerating_it.value());
                        }
                );
            });
        });
    } else if (trivially_compatible_types(src_column.type(), dst_column.type())) {
        details::visit_type(src_column.type().data_type(), [&](auto src_desc_tag) {
            using SourceTDT = ScalarTagType<decltype(src_desc_tag)>;
            using SourceType = typename decltype(src_desc_tag)::DataTypeTag::raw_type;
            if (!src_column.is_sparse()) {
                while (auto block = src_data.next<SourceTDT>()) {
                    const auto row_count = block->row_count();
                    memcpy(dst_ptr, block->data(), row_count * sizeof(SourceType));
                    dst_ptr += row_count * sizeof(SourceType);
                }
            } else {
                init_sparse_dst_column_before_copy(
                        dst_column,
                        offset,
                        num_rows,
                        dst_rawtype_size,
                        read_options.output_format(),
                        src_column.opt_sparse_map(),
                        default_value
                );
                SourceType* typed_dst_ptr = reinterpret_cast<SourceType*>(dst_ptr);
                arcticdb::for_each_enumerated<SourceTDT>(src_column, [&] ARCTICDB_LAMBDA_INLINE(const auto& row) {
                    typed_dst_ptr[row.idx()] = row.value();
                });
            }
        });
    } else if (is_valid_type_promotion_to_target(
                       src_column.type(), dst_column.type(), IntToFloatConversion::PERMISSIVE
               ) ||
               (src_column.type().data_type() == DataType::UINT64 && dst_column.type().data_type() == DataType::INT64
               ) ||
               (src_column.type().data_type() == DataType::FLOAT64 && dst_column.type().data_type() == DataType::FLOAT32
               )) {
        // Arctic cannot contain both uint64 and int64 columns in the dataframe because there is no common type between
        // these types. This means that the second condition cannot happen during a regular read. The processing
        // pipeline, however, can produce a set of segments where some are int64 and other uint64. This can happen in
        // the sum aggregation (both for unsorted aggregations and resampling). Because we promote the sum type to the
        // largest type of the respective category. E.g., we have int8 and uint8. Dynamic schema will allow this, and
        // the global type descriptor will be int16. However, when segments are processed on their own int8 -> int64 and
        // uint8 -> int64. We have decided to allow this and assign a common type of int64 (done in the modify_schema
        // procedure). This is what pyarrow does as well. Because of the above, we allow here copying uint64 buffer in
        // an int64 buffer.
        //
        // Having float64 as a source and float32 as a destination should not appear during a regular read however it
        // can happen in the processing pipeline. E.g., performing a first/last/min/max aggregations in resampling or
        // groupby. There might be 4 segments float32, uint16, int8 and float32, if the first segment is in a separate
        // group/bucket and the second 3 segments are in the same group the processing pipeline will output two segments
        // one with float32 dtype and one with dtype:
        // common_type(common_type(uint16, int8), float32) = common_type(int32, float32) = float64
        details::visit_type(dst_column.type().data_type(), [&](auto dest_desc_tag) {
            using DestinationRawType = typename decltype(dest_desc_tag)::DataTypeTag::raw_type;
            auto typed_dst_ptr = reinterpret_cast<DestinationRawType*>(dst_ptr);
            details::visit_type(src_column.type().data_type(), [&](auto src_desc_tag) {
                using source_type_info = ScalarTypeInfo<decltype(src_desc_tag)>;
                if constexpr (std::is_arithmetic_v<typename source_type_info::RawType> &&
                              std::is_arithmetic_v<DestinationRawType>) {
                    if (src_column.is_sparse()) {
                        init_sparse_dst_column_before_copy(
                                dst_column,
                                offset,
                                num_rows,
                                dst_rawtype_size,
                                read_options.output_format(),
                                src_column.opt_sparse_map(),
                                default_value
                        );
                        arcticdb::for_each_enumerated<typename source_type_info::TDT>(
                                src_column,
                                [&] ARCTICDB_LAMBDA_INLINE(const auto& row) { typed_dst_ptr[row.idx()] = row.value(); }
                        );
                    } else {
                        arcticdb::for_each<typename source_type_info::TDT>(src_column, [&](const auto& value) {
                            *typed_dst_ptr = value;
                            ++typed_dst_ptr;
                        });
                    }

                } else {
                    util::raise_rte(type_promotion_error_msg.c_str());
                }
            });
        });
    } else {
        util::raise_rte(type_promotion_error_msg.c_str());
    }
}

struct CopyToBufferTask : async::BaseTask {
    SegmentInMemory source_segment_;
    SegmentInMemory target_segment_;
    FrameSlice frame_slice_;
    uint32_t required_fields_count_;
    DecodePathData shared_data_;
    std::any& handler_data_;
    const ReadOptions read_options_;
    std::shared_ptr<PipelineContext> pipeline_context_;

    CopyToBufferTask(
            SegmentInMemory&& source_segment, SegmentInMemory target_segment, FrameSlice frame_slice,
            uint32_t required_fields_count, DecodePathData shared_data, std::any& handler_data,
            const ReadOptions& read_options, std::shared_ptr<PipelineContext> pipeline_context
    ) :
        source_segment_(std::move(source_segment)),
        target_segment_(std::move(target_segment)),
        frame_slice_(std::move(frame_slice)),
        required_fields_count_(required_fields_count),
        shared_data_(std::move(shared_data)),
        handler_data_(handler_data),
        read_options_(read_options),
        pipeline_context_(std::move(pipeline_context)) {}

    folly::Unit operator()() {
        const size_t first_col = frame_slice_.columns().first;
        const bool first_col_slice = first_col == 0;
        const auto& fields = source_segment_.descriptor().fields();
        // Skip the "true" index fields (i.e. those stored in every column slice) if we are not in the first column
        // slice
        for (size_t idx = first_col_slice ? 0 : get_index_field_count(source_segment_); idx < fields.size(); ++idx) {
            // First condition required to avoid underflow when subtracting one unsigned value from another
            if (required_fields_count_ >= first_col && idx < required_fields_count_ - first_col) {
                // This is a required column in the output. The name in source_segment_ may not match that in
                // target_segment_ e.g. If 2 timeseries are joined that had differently named indexes
                copy_frame_data_to_buffer(
                        target_segment_,
                        idx + first_col,
                        source_segment_,
                        idx,
                        frame_slice_.row_range,
                        shared_data_,
                        handler_data_,
                        read_options_,
                        {}
                );
            } else {
                // All other columns use names to match the source with the destination
                const auto& field = fields.at(idx);
                const auto& field_name = field.name();
                auto frame_loc_opt = target_segment_.column_index(field_name);
                if (!frame_loc_opt) {
                    continue;
                }
                const std::optional<Value>& default_value = [&]() -> std::optional<Value> {
                    const auto it = pipeline_context_->default_values_.find(std::string{field_name});
                    if (it != pipeline_context_->default_values_.end()) {
                        return it->second;
                    }
                    return {};
                }();
                copy_frame_data_to_buffer(
                        target_segment_,
                        *frame_loc_opt,
                        source_segment_,
                        idx,
                        frame_slice_.row_range,
                        shared_data_,
                        handler_data_,
                        read_options_,
                        default_value
                );
            }
        }
        return folly::Unit{};
    }
};

folly::Future<folly::Unit> copy_segments_to_frame(
        const std::shared_ptr<Store>& store, const std::shared_ptr<PipelineContext>& pipeline_context,
        SegmentInMemory frame, std::any& handler_data, const ReadOptions& read_options
) {
    const auto required_fields_count =
            pipelines::index::required_fields_count(pipeline_context->descriptor(), *pipeline_context->norm_meta_);
    std::vector<folly::Future<folly::Unit>> copy_tasks;
    DecodePathData shared_data;
    for (auto context_row : folly::enumerate(*pipeline_context)) {
        auto& slice_and_key = context_row->slice_and_key();

        copy_tasks.emplace_back(async::submit_cpu_task(CopyToBufferTask{
                slice_and_key.release_segment(store),
                frame,
                context_row->slice_and_key().slice(),
                required_fields_count,
                shared_data,
                handler_data,
                read_options,
                pipeline_context
        }));
    }
    return folly::collect(copy_tasks).via(&async::cpu_executor()).unit();
}

folly::Future<SegmentInMemory> prepare_output_frame(
        std::vector<SliceAndKey>&& items, const std::shared_ptr<PipelineContext>& pipeline_context,
        const std::shared_ptr<Store>& store, const ReadOptions& read_options, std::any& handler_data
) {
    pipeline_context->clear_vectors();
    pipeline_context->slice_and_keys_ = std::move(items);
    adjust_slice_ranges(pipeline_context);
    mark_index_slices(pipeline_context);
    pipeline_context->ensure_vectors();

    for (auto row : *pipeline_context) {
        row.set_compacted(false);
        row.set_descriptor(row.slice_and_key().segment(store).descriptor_ptr());
        row.set_string_pool(row.slice_and_key().segment(store).string_pool_ptr());
    }

    auto frame = allocate_frame(pipeline_context, read_options);
    return copy_segments_to_frame(store, pipeline_context, frame, handler_data, read_options)
            .thenValue([frame](auto&&) { return frame; });
}

AtomKey index_key_to_column_stats_key(const IndexTypeKey& index_key) {
    // Note that we use the creation timestamp and content hash of the related index key
    // This gives a strong paper-trail if archaeology is required
    return atom_key_builder()
            .version_id(index_key.version_id())
            .creation_ts(index_key.creation_ts())
            .content_hash(index_key.content_hash())
            .build(index_key.id(), KeyType::COLUMN_STATS);
}

void create_column_stats_impl(
        const std::shared_ptr<Store>& store, const VersionedItem& versioned_item, ColumnStats& column_stats,
        const ReadOptions& read_options
) {
    using namespace arcticdb::pipelines;
    auto clause = column_stats.clause();
    if (!clause.has_value()) {
        log::version().warn("Cannot create empty column stats");
        return;
    }
    auto read_query = std::make_shared<ReadQuery>(
            std::vector<std::shared_ptr<Clause>>{std::make_shared<Clause>(std::move(*clause))}
    );

    auto column_stats_key = index_key_to_column_stats_key(versioned_item.key_);
    std::optional<SegmentInMemory> old_segment;
    try {
        old_segment = store->read(column_stats_key).get().second;
        ColumnStats old_column_stats{old_segment->fields()};
        // No need to redo work for any stats that already exist
        column_stats.drop(old_column_stats, false);
        // If all the column stats we are being asked to create already exist, there's no work to do
        if (column_stats.segment_column_names().empty()) {
            return;
        }
    } catch (...) {
        // Old segment doesn't exist
    }

    auto pipeline_context = std::make_shared<PipelineContext>();
    pipeline_context->stream_id_ = versioned_item.key_.id();
    auto maybe_isr = get_index_segment_reader(*store, pipeline_context, versioned_item);
    if (maybe_isr.has_value()) {
        read_indexed_keys_to_pipeline(pipeline_context, std::move(*maybe_isr), *read_query, read_options);
    }

    schema::check<ErrorCode::E_UNSUPPORTED_INDEX_TYPE>(
            !pipeline_context->multi_key_, "Column stats generation not supported with multi-indexed symbols"
    );
    schema::check<ErrorCode::E_OPERATION_NOT_SUPPORTED_WITH_PICKLED_DATA>(
            !pipeline_context->is_pickled(), "Cannot create column stats on pickled data"
    );

    auto segs = read_process_and_collect(store, pipeline_context, read_query, read_options).get();
    schema::check<ErrorCode::E_COLUMN_DOESNT_EXIST>(
            !segs.empty(), "Cannot create column stats for nonexistent columns"
    );

    // Convert SliceAndKey vector into SegmentInMemory vector
    std::vector<SegmentInMemory> segments_in_memory;
    for (auto& seg : segs) {
        segments_in_memory.emplace_back(seg.release_segment(store));
    }
    SegmentInMemory new_segment = merge_column_stats_segments(segments_in_memory);
    new_segment.descriptor().set_id(versioned_item.key_.id());

    storage::UpdateOpts update_opts;
    update_opts.upsert_ = true;
    if (!old_segment.has_value()) {
        // Old segment doesn't exist, just write new one
        store->update(column_stats_key, std::move(new_segment), update_opts).get();
        return;
    } else {
        // Check that the start and end index columns match
        internal::check<ErrorCode::E_ASSERTION_FAILURE>(
                new_segment.column(0) == old_segment->column(0) && new_segment.column(1) == old_segment->column(1),
                "Cannot create column stats, existing column stats row-groups do not match"
        );
        old_segment->concatenate(std::move(new_segment));
        store->update(column_stats_key, std::move(*old_segment), update_opts).get();
    }
}

void drop_column_stats_impl(
        const std::shared_ptr<Store>& store, const VersionedItem& versioned_item,
        const std::optional<ColumnStats>& column_stats_to_drop
) {
    storage::RemoveOpts remove_opts;
    remove_opts.ignores_missing_key_ = true;
    auto column_stats_key = index_key_to_column_stats_key(versioned_item.key_);
    if (!column_stats_to_drop.has_value()) {
        // Drop all column stats
        store->remove_key(column_stats_key, remove_opts).get();
    } else {
        SegmentInMemory segment_in_memory;
        try {
            segment_in_memory = store->read(column_stats_key).get().second;
        } catch (const std::exception& e) {
            log::version().warn("No column stats exist to drop: {}", e.what());
            return;
        }
        ColumnStats column_stats{segment_in_memory.fields()};
        column_stats.drop(*column_stats_to_drop);
        auto columns_to_keep = column_stats.segment_column_names();
        if (columns_to_keep.empty()) {
            // Drop all column stats
            store->remove_key(column_stats_key, remove_opts).get();
        } else {
            auto old_fields = segment_in_memory.fields().clone();
            for (const auto& field : old_fields) {
                auto column_name = field.name();
                if (!columns_to_keep.contains(std::string{column_name}) && column_name != start_index_column_name &&
                    column_name != end_index_column_name) {
                    segment_in_memory.drop_column(column_name);
                }
            }
            storage::UpdateOpts update_opts;
            update_opts.upsert_ = true;
            store->update(column_stats_key, std::move(segment_in_memory), update_opts).get();
        }
    }
}

FrameAndDescriptor read_column_stats_impl(const std::shared_ptr<Store>& store, const VersionedItem& versioned_item) {
    auto column_stats_key = index_key_to_column_stats_key(versioned_item.key_);
    // Remove try-catch once AsyncStore methods raise the new error codes themselves
    try {
        auto segment = store->read_compressed(column_stats_key).get().segment_ptr();
        auto segment_in_memory = decode_segment(*segment, AllocationType::DETACHABLE);
        TimeseriesDescriptor tsd;
        tsd.set_total_rows(segment_in_memory.row_count());
        tsd.set_stream_descriptor(segment_in_memory.descriptor());
        return {SegmentInMemory(std::move(segment_in_memory)), tsd, {}};
    } catch (const std::exception& e) {
        storage::raise<ErrorCode::E_KEY_NOT_FOUND>("Failed to read column stats key: {}", e.what());
    }
}

ColumnStats get_column_stats_info_impl(const std::shared_ptr<Store>& store, const VersionedItem& versioned_item) {
    auto column_stats_key = index_key_to_column_stats_key(versioned_item.key_);
    // Remove try-catch once AsyncStore methods raise the new error codes themselves
    try {
        auto stream_descriptor =
                std::get<StreamDescriptor>(store->read_metadata_and_descriptor(column_stats_key).get());
        return ColumnStats(stream_descriptor.fields());
    } catch (const std::exception& e) {
        storage::raise<ErrorCode::E_KEY_NOT_FOUND>("Failed to read column stats key: {}", e.what());
    }
}

folly::Future<SegmentInMemory> do_direct_read_or_process(
        const std::shared_ptr<Store>& store, const std::shared_ptr<ReadQuery>& read_query,
        const ReadOptions& read_options, const std::shared_ptr<PipelineContext>& pipeline_context,
        const DecodePathData& shared_data, std::any& handler_data
) {
    const bool direct_read = read_query->clauses_.empty();
    if (!direct_read) {
        ARCTICDB_SAMPLE(RunPipelineAndOutput, 0)
        util::check_rte(!pipeline_context->is_pickled(), "Cannot filter pickled data");
        return read_process_and_collect(store, pipeline_context, read_query, read_options)
                .thenValue([store, pipeline_context, read_options, &handler_data](std::vector<SliceAndKey>&& segs) {
                    return prepare_output_frame(std::move(segs), pipeline_context, store, read_options, handler_data);
                });
    } else {
        ARCTICDB_SAMPLE(MarkAndReadDirect, 0)
        util::check_rte(
                !(pipeline_context->is_pickled() && std::holds_alternative<RowRange>(read_query->row_filter)),
                "Cannot use head/tail/row_range with pickled data, use plain read instead"
        );
        mark_index_slices(pipeline_context);
        auto frame = allocate_frame(pipeline_context, read_options);
        util::print_total_mem_usage(__FILE__, __LINE__, __FUNCTION__);
        ARCTICDB_DEBUG(log::version(), "Fetching frame data");
        return fetch_data(
                std::move(frame), pipeline_context, store, *read_query, read_options, shared_data, handler_data
        );
    }
}

VersionedItem collate_and_write(
        const std::shared_ptr<Store>& store, const std::shared_ptr<PipelineContext>& pipeline_context,
        const std::vector<FrameSlice>& slices, std::vector<VariantKey> keys, size_t append_after,
        const std::optional<arcticdb::proto::descriptors::UserDefinedMetadata>& user_meta
) {
    util::check(keys.size() == slices.size(), "Mismatch between slices size and key size");
    TimeseriesDescriptor tsd;

    tsd.set_stream_descriptor(pipeline_context->descriptor());
    tsd.set_total_rows(pipeline_context->total_rows_);
    auto& tsd_proto = tsd.mutable_proto();
    tsd_proto.mutable_normalization()->CopyFrom(*pipeline_context->norm_meta_);
    if (user_meta)
        tsd_proto.mutable_user_meta()->CopyFrom(*user_meta);

    auto index = stream::index_type_from_descriptor(pipeline_context->descriptor());
    return util::variant_match(index, [&store, &pipeline_context, &slices, &keys, &append_after, &tsd](auto idx) {
        using IndexType = decltype(idx);
        index::IndexWriter<IndexType> writer(
                store, IndexPartialKey{pipeline_context->stream_id_, pipeline_context->version_id_}, std::move(tsd)
        );
        auto end = std::begin(pipeline_context->slice_and_keys_);
        std::advance(end, append_after);
        ARCTICDB_DEBUG(
                log::version(),
                "Adding {} existing keys and {} new keys: ",
                std::distance(std::begin(pipeline_context->slice_and_keys_), end),
                keys.size()
        );
        for (auto sk = std::begin(pipeline_context->slice_and_keys_); sk < end; ++sk)
            writer.add(sk->key(), sk->slice());

        for (const auto& key : folly::enumerate(keys)) {
            writer.add(to_atom(*key), slices[key.index]);
        }
        auto index_key_fut = writer.commit();
        return VersionedItem{std::move(index_key_fut).get()};
    });
}

void delete_incomplete_keys(PipelineContext& pipeline_context, Store& store) {
    std::vector<entity::VariantKey> keys_to_delete;
    keys_to_delete.reserve(pipeline_context.slice_and_keys_.size() - pipeline_context.incompletes_after());
    for (auto sk = pipeline_context.incompletes_begin(); sk != pipeline_context.end(); ++sk) {
        const auto& slice_and_key = sk->slice_and_key();
        if (ARCTICDB_LIKELY(slice_and_key.key().type() == KeyType::APPEND_DATA)) {
            keys_to_delete.emplace_back(slice_and_key.key());
        } else {
            log::storage().error(
                    "Delete incomplete keys procedure tries to delete a wrong key type {}. Key type must be {}.",
                    slice_and_key.key(),
                    KeyType::APPEND_DATA
            );
        }
    }

    ARCTICDB_DEBUG(
            log::version(),
            "delete_incomplete_keys Symbol {}: Deleting {} keys",
            pipeline_context.stream_id_,
            keys_to_delete.size()
    );
    store.remove_keys(keys_to_delete).get();
}

DeleteIncompleteKeysOnExit::DeleteIncompleteKeysOnExit(
        std::shared_ptr<PipelineContext> pipeline_context, std::shared_ptr<Store> store, bool via_iteration,
        std::optional<std::vector<StageResult>> stage_results
) :
    context_(std::move(pipeline_context)),
    store_(std::move(store)),
    via_iteration_(via_iteration),
    stage_results_(std::move(stage_results)) {}

DeleteIncompleteKeysOnExit::~DeleteIncompleteKeysOnExit() {
    if (released_)
        return;

    try {
        storage::RemoveOpts opts{.ignores_missing_key_ = true};
        if (context_->incompletes_after_) {
            delete_incomplete_keys(*context_, *store_);
        } else {
            // If an exception is thrown before read_incompletes_to_pipeline the keys won't be placed inside the
            // context thus they must be read manually.
            std::vector<VariantKey> keys_to_delete;
            if (stage_results_) {
                auto keys_to_delete_view =
                        *stage_results_ | std::views::transform(&StageResult::staged_segments) | std::views::join;
                keys_to_delete = std::vector<VariantKey>(keys_to_delete_view.begin(), keys_to_delete_view.end());
            } else {
                keys_to_delete = read_incomplete_keys_for_symbol(store_, context_->stream_id_, via_iteration_);
            }
            store_->remove_keys(keys_to_delete, opts).get();
        }
    } catch (const std::exception& e) {
        // Don't emit exceptions from destructor
        log::storage().error("Failed to delete staged segments: {}", e.what());
    }
}

std::optional<DeleteIncompleteKeysOnExit> get_delete_keys_on_failure(
        const std::shared_ptr<PipelineContext>& pipeline_context, const std::shared_ptr<Store>& store,
        const CompactIncompleteParameters& parameters
) {
    if (parameters.delete_staged_data_on_failure_) {
        return std::make_optional<DeleteIncompleteKeysOnExit>(
                pipeline_context, store, parameters.via_iteration_, parameters.stage_results
        );
    }
    return std::nullopt;
}

static void read_indexed_keys_for_compaction(
        const CompactIncompleteParameters& parameters, const UpdateInfo& update_info,
        const std::shared_ptr<Store>& store, const std::shared_ptr<PipelineContext>& pipeline_context,
        ReadQuery& read_query, const ReadOptions& read_options
) {
    const bool append_to_existing = parameters.append_ && update_info.previous_index_key_.has_value();
    if (append_to_existing) {
        auto maybe_isr = get_index_segment_reader(*store, pipeline_context, *update_info.previous_index_key_);
        if (maybe_isr.has_value()) {
            read_indexed_keys_to_pipeline(pipeline_context, std::move(*maybe_isr), read_query, read_options);
        }
    }
}

static void validate_slicing_policy_for_compaction(
        const CompactIncompleteParameters& parameters, const UpdateInfo& update_info,
        const std::shared_ptr<PipelineContext>& pipeline_context, const WriteOptions& write_options
) {
    const bool append_to_existing = parameters.append_ && update_info.previous_index_key_.has_value();
    if (append_to_existing) {
        if (!write_options.dynamic_schema && !pipeline_context->slice_and_keys_.empty()) {
            user_input::check<ErrorCode::E_INVALID_USER_ARGUMENT>(
                    pipeline_context->slice_and_keys_.front().slice().columns() ==
                            pipeline_context->slice_and_keys_.back().slice().columns(),
                    "Appending using sort_and_finalize_staged_data/compact_incompletes/finalize_staged_data is not"
                    " supported when existing data being appended to is column sliced."
            );
        }
    }
}

static SortedValue compute_sorted_status(const std::optional<SortedValue>& initial_sorted_status) {
    constexpr auto staged_segments_sorted_status = SortedValue::ASCENDING;
    if (initial_sorted_status.has_value()) {
        return deduce_sorted(*initial_sorted_status, staged_segments_sorted_status);
    }
    return staged_segments_sorted_status;
}

std::variant<VersionedItem, CompactionError> sort_merge_impl(
        const std::shared_ptr<Store>& store, const StreamId& stream_id,
        const std::optional<arcticdb::proto::descriptors::UserDefinedMetadata>& user_meta,
        const UpdateInfo& update_info, const CompactIncompleteParameters& compaction_parameters,
        const WriteOptions& write_options, std::shared_ptr<PipelineContext>& pipeline_context
) {
    auto read_query = ReadQuery{};

    read_indexed_keys_for_compaction(
            compaction_parameters, update_info, store, pipeline_context, read_query, ReadOptions{}
    );
    validate_slicing_policy_for_compaction(compaction_parameters, update_info, pipeline_context, write_options);
    const auto num_versioned_rows = pipeline_context->total_rows_;
    const bool append_to_existing = compaction_parameters.append_ && update_info.previous_index_key_.has_value();
    // Cache this before calling read_incompletes_to_pipeline as it changes the descripor
    const std::optional<SortedValue> initial_index_sorted_status =
            append_to_existing ? std::optional{pipeline_context->desc_->sorted()} : std::nullopt;
    const ReadIncompletesFlags read_incomplete_flags{
            .convert_int_to_float = compaction_parameters.convert_int_to_float_,
            .via_iteration = compaction_parameters.via_iteration_,
            .sparsify = compaction_parameters.sparsify_,
            .dynamic_schema = write_options.dynamic_schema,
            .has_active_version = update_info.previous_index_key_.has_value()
    };
    const auto read_incompletes_result = read_incompletes_to_pipeline(
            store,
            pipeline_context,
            compaction_parameters.stage_results,
            read_query,
            ReadOptions{},
            read_incomplete_flags
    );

    bool has_incomplete_segments;
    if (std::holds_alternative<CompactionError>(read_incompletes_result)) {
        return std::get<CompactionError>(read_incompletes_result);
    } else {
        has_incomplete_segments = std::get<bool>(read_incompletes_result);
    }

    user_input::check<ErrorCode::E_NO_STAGED_SEGMENTS>(
            has_incomplete_segments, "Finalizing staged data is not allowed with empty staging area"
    );

    std::vector<FrameSlice> slices;
    std::vector<folly::Future<VariantKey>> fut_vec;
    auto semaphore = std::make_shared<folly::NativeSemaphore>(n_segments_live_during_compaction());
    auto index = stream::index_type_from_descriptor(pipeline_context->descriptor());
    util::variant_match(
            index,
            [&](const stream::TimeseriesIndex& timeseries_index) {
                read_query.clauses_.emplace_back(std::make_shared<Clause>(
                        SortClause{timeseries_index.name(), pipeline_context->incompletes_after()}
                ));
                read_query.clauses_.emplace_back(std::make_shared<Clause>(RemoveColumnPartitioningClause{}));

                read_query.clauses_.emplace_back(std::make_shared<Clause>(MergeClause{
                        timeseries_index,
                        SparseColumnPolicy{},
                        stream_id,
                        pipeline_context->descriptor(),
                        write_options.dynamic_schema
                }));
                ReadOptions read_options;
                read_options.set_dynamic_schema(write_options.dynamic_schema);
                auto segments = read_process_and_collect(
                                        store,
                                        pipeline_context,
                                        std::make_shared<ReadQuery>(std::move(read_query)),
                                        read_options
                )
                                        .get();
                if (compaction_parameters.append_ && update_info.previous_index_key_ && !segments.empty()) {
                    const timestamp last_index_on_disc = update_info.previous_index_key_->end_time() - 1;
                    const timestamp incomplete_start =
                            std::get<timestamp>(TimeseriesIndex::start_value_for_segment(segments[0].segment(store)));
                    sorting::check<ErrorCode::E_UNSORTED_DATA>(
                            last_index_on_disc <= incomplete_start,
                            "Cannot append staged segments to existing data as incomplete segment contains index value "
                            "{} < existing data {}",
                            util::format_timestamp(incomplete_start),
                            util::format_timestamp(last_index_on_disc)
                    );
                }
                pipeline_context->total_rows_ = num_versioned_rows + get_slice_rowcounts(segments);

                auto index = index_type_from_descriptor(pipeline_context->descriptor());
                stream::SegmentAggregator<TimeseriesIndex, DynamicSchema, RowCountSegmentPolicy, SparseColumnPolicy>
                        aggregator{
                                [&slices](FrameSlice&& slice) { slices.emplace_back(std::move(slice)); },
                                DynamicSchema{*pipeline_context->staged_descriptor_, index},
                                [pipeline_context, &fut_vec, &store, &semaphore](SegmentInMemory&& segment) {
                                    const auto local_index_start = TimeseriesIndex::start_value_for_segment(segment);
                                    const auto local_index_end = TimeseriesIndex::end_value_for_segment(segment);
                                    const PartialKey pk{
                                            KeyType::TABLE_DATA,
                                            pipeline_context->version_id_,
                                            pipeline_context->stream_id_,
                                            local_index_start,
                                            local_index_end
                                    };
                                    fut_vec.emplace_back(store->write_maybe_blocking(pk, std::move(segment), semaphore)
                                    );
                                },
                                RowCountSegmentPolicy(write_options.segment_row_size)
                        };

                [[maybe_unused]] size_t count = 0;
                for (auto& sk : segments) {
                    SegmentInMemory segment = sk.release_segment(store);

                    ARCTICDB_DEBUG(
                            log::version(),
                            "sort_merge_impl Symbol {} Segment {}: Segment has rows {} columns {} uncompressed bytes "
                            "{}",
                            pipeline_context->stream_id_,
                            count++,
                            segment.row_count(),
                            segment.columns().size(),
                            segment.descriptor().uncompressed_bytes()
                    );
                    // Empty columns can appear only of one staged segment is empty and adds column which
                    // does not appear in any other segment. There can also be empty columns if all segments
                    // are empty in that case this loop won't be reached as segments.size() will be 0
                    if (write_options.dynamic_schema) {
                        segment.drop_empty_columns();
                    }

                    aggregator.add_segment(std::move(segment), sk.slice(), compaction_parameters.convert_int_to_float_);
                }
                aggregator.commit();
                pipeline_context->desc_->set_sorted(compute_sorted_status(initial_index_sorted_status));
            },
            [&](const auto&) {
                util::raise_rte(
                        "Sort merge only supports datetime indexed data. You data does not have a datetime index."
                );
            }
    );

    auto keys = folly::collect(fut_vec).get();
    auto vit =
            collate_and_write(store, pipeline_context, slices, keys, pipeline_context->incompletes_after(), user_meta);
    return vit;
}

std::variant<VersionedItem, CompactionError> compact_incomplete_impl(
        const std::shared_ptr<Store>& store, const StreamId& stream_id,
        const std::optional<arcticdb::proto::descriptors::UserDefinedMetadata>& user_meta,
        const UpdateInfo& update_info, const CompactIncompleteParameters& compaction_parameters,
        const WriteOptions& write_options, std::shared_ptr<PipelineContext>& pipeline_context
) {

    ReadQuery read_query;
    ReadOptions read_options;
    read_options.set_dynamic_schema(true);
    std::optional<SegmentInMemory> last_indexed;
    read_indexed_keys_for_compaction(
            compaction_parameters, update_info, store, pipeline_context, read_query, ReadOptions{}
    );
    validate_slicing_policy_for_compaction(compaction_parameters, update_info, pipeline_context, write_options);
    const bool append_to_existing = compaction_parameters.append_ && update_info.previous_index_key_.has_value();
    // Cache this before calling read_incompletes_to_pipeline as it changes the descriptor.
    const std::optional<SortedValue> initial_index_sorted_status =
            append_to_existing ? std::optional{pipeline_context->desc_->sorted()} : std::nullopt;
    const ReadIncompletesFlags read_incomplete_flags{
            .convert_int_to_float = compaction_parameters.convert_int_to_float_,
            .via_iteration = compaction_parameters.via_iteration_,
            .sparsify = compaction_parameters.sparsify_,
            .dynamic_schema = write_options.dynamic_schema,
            .has_active_version = update_info.previous_index_key_.has_value()
    };

    const auto read_incompletes_result = read_incompletes_to_pipeline(
            store,
            pipeline_context,
            compaction_parameters.stage_results,
            read_query,
            ReadOptions{},
            read_incomplete_flags
    );

    bool has_incomplete_segments;
    if (std::holds_alternative<CompactionError>(read_incompletes_result)) {
        return std::get<CompactionError>(read_incompletes_result);
    } else {
        has_incomplete_segments = std::get<bool>(read_incompletes_result);
    }

    user_input::check<ErrorCode::E_NO_STAGED_SEGMENTS>(
            has_incomplete_segments, "Finalizing staged data is not allowed with empty staging area"
    );
    if (compaction_parameters.validate_index_) {
        check_incompletes_index_ranges_dont_overlap(pipeline_context, initial_index_sorted_status, append_to_existing);
    }
    const auto& first_seg = pipeline_context->slice_and_keys_.begin()->segment(store);

    std::vector<FrameSlice> slices;
    bool dynamic_schema = write_options.dynamic_schema;
    const auto index = index_type_from_descriptor(first_seg.descriptor());
    auto policies = std::make_tuple(
            index,
            dynamic_schema ? VariantSchema{DynamicSchema::default_schema(index, stream_id)}
                           : VariantSchema{FixedSchema::default_schema(index, stream_id)},
            compaction_parameters.sparsify_ ? VariantColumnPolicy{SparseColumnPolicy{}}
                                            : VariantColumnPolicy{DenseColumnPolicy{}}
    );

    CompactionResult result =
            util::variant_match(std::move(policies), [&](auto&& idx, auto&& schema, auto&& column_policy) {
                using IndexType = std::remove_reference_t<decltype(idx)>;
                using SchemaType = std::remove_reference_t<decltype(schema)>;
                using ColumnPolicyType = std::remove_reference_t<decltype(column_policy)>;
                constexpr bool validate_index_sorted = IndexType::type() == IndexDescriptorImpl::Type::TIMESTAMP;
                const CompactionOptions compaction_options{
                        .convert_int_to_float = compaction_parameters.convert_int_to_float_,
                        .validate_index = validate_index_sorted,
                        .perform_schema_checks = true
                };
                CompactionResult compaction_result =
                        do_compact<IndexType, SchemaType, RowCountSegmentPolicy, ColumnPolicyType>(
                                pipeline_context->incompletes_begin(),
                                pipeline_context->end(),
                                pipeline_context,
                                slices,
                                store,
                                write_options.segment_row_size,
                                compaction_options
                        );
                if constexpr (std::is_same_v<IndexType, TimeseriesIndex>) {
                    pipeline_context->desc_->set_sorted(compute_sorted_status(initial_index_sorted_status));
                }
                return compaction_result;
            });

    return util::variant_match(
            std::move(result),
            [&slices, &pipeline_context, &store, &user_meta](CompactionWrittenKeys&& written_keys) -> VersionedItem {
                auto vit = collate_and_write(
                        store,
                        pipeline_context,
                        slices,
                        std::move(written_keys),
                        pipeline_context->incompletes_after(),
                        user_meta
                );
                return vit;
            },
            [](Error&& error) -> VersionedItem {
                error.throw_error();
                return VersionedItem{}; // unreachable
            }
    );
}

PredefragmentationInfo get_pre_defragmentation_info(
        const std::shared_ptr<Store>& store, const StreamId& stream_id, const UpdateInfo& update_info,
        const WriteOptions& options, size_t segment_size
) {
    util::check(update_info.previous_index_key_.has_value(), "No latest undeleted version found for data compaction");

    auto pipeline_context = std::make_shared<PipelineContext>();
    pipeline_context->stream_id_ = stream_id;
    pipeline_context->version_id_ = update_info.next_version_id_;

    auto read_query = std::make_shared<ReadQuery>();
    auto maybe_isr = get_index_segment_reader(*store, pipeline_context, *update_info.previous_index_key_);
    if (maybe_isr.has_value()) {
        read_indexed_keys_to_pipeline(
                pipeline_context, std::move(*maybe_isr), *read_query, defragmentation_read_options_generator(options)
        );
    }

    using CompactionStartInfo = std::pair<size_t, size_t>; // row, segment_append_after
    std::vector<CompactionStartInfo> first_col_segment_idx;
    const auto& slice_and_keys = pipeline_context->slice_and_keys_;
    first_col_segment_idx.reserve(slice_and_keys.size());
    std::optional<CompactionStartInfo> compaction_start_info;
    size_t segment_idx = 0, num_to_segments_after_compact = 0, new_segment_row_size = 0;
    for (const auto& slice_and_key : slice_and_keys) {
        auto& slice = slice_and_key.slice();

        if (slice.row_range.diff() < segment_size && !compaction_start_info)
            compaction_start_info = {slice.row_range.start(), segment_idx};

        if (slice.col_range.start() ==
            pipeline_context->descriptor().index().field_count()) { // where data column starts
            first_col_segment_idx.emplace_back(slice.row_range.start(), segment_idx);
            if (new_segment_row_size == 0)
                ++num_to_segments_after_compact;
            new_segment_row_size += slice.row_range.diff();
            if (new_segment_row_size >= segment_size)
                new_segment_row_size = 0;
        }
        ++segment_idx;
        if (compaction_start_info && slice.row_range.start() < compaction_start_info->first) {
            auto start_point = std::lower_bound(
                    first_col_segment_idx.begin(),
                    first_col_segment_idx.end(),
                    slice.row_range.start(),
                    [](auto lhs, auto rhs) { return lhs.first < rhs; }
            );
            if (start_point != first_col_segment_idx.end())
                compaction_start_info = *start_point;
            else {
                log::version().warn(
                        "Missing segment containing column 0 for row {}; Resetting compaction starting point to 0",
                        slice.row_range.start()
                );
                compaction_start_info = {0u, 0u};
            }
        }
    }
    return {pipeline_context,
            read_query,
            first_col_segment_idx.size() - num_to_segments_after_compact,
            compaction_start_info ? std::make_optional<size_t>(compaction_start_info->second) : std::nullopt};
}

bool is_symbol_fragmented_impl(size_t segments_need_compaction) {
    return static_cast<int64_t>(segments_need_compaction) >=
           ConfigsMap::instance()->get_int("SymbolDataCompact.SegmentCount", 100);
}

VersionedItem defragment_symbol_data_impl(
        const std::shared_ptr<Store>& store, const StreamId& stream_id, const UpdateInfo& update_info,
        const WriteOptions& options, size_t segment_size
) {
    auto pre_defragmentation_info = get_pre_defragmentation_info(store, stream_id, update_info, options, segment_size);
    util::check(
            is_symbol_fragmented_impl(pre_defragmentation_info.segments_need_compaction) &&
                    pre_defragmentation_info.append_after.has_value(),
            "Nothing to compact in defragment_symbol_data"
    );

    // in the new index segment, we will start appending after this value
    std::vector<FrameSlice> slices;
    const auto index = index_type_from_descriptor(pre_defragmentation_info.pipeline_context->descriptor());
    auto policies = std::make_tuple(
            index,
            options.dynamic_schema ? VariantSchema{DynamicSchema::default_schema(index, stream_id)}
                                   : VariantSchema{FixedSchema::default_schema(index, stream_id)}
    );

    CompactionResult result = util::variant_match(
            std::move(policies),
            [&slices, &store, &options, &pre_defragmentation_info, segment_size = segment_size](
                    auto&& idx, auto&& schema
            ) {
                pre_defragmentation_info.read_query->clauses_.emplace_back(std::make_shared<Clause>(
                        RemoveColumnPartitioningClause{pre_defragmentation_info.append_after.value()}
                ));
                auto segments = read_process_and_collect(
                                        store,
                                        pre_defragmentation_info.pipeline_context,
                                        pre_defragmentation_info.read_query,
                                        defragmentation_read_options_generator(options)
                )
                                        .get();
                using IndexType = std::remove_reference_t<decltype(idx)>;
                using SchemaType = std::remove_reference_t<decltype(schema)>;
                static constexpr CompactionOptions compaction_options = {
                        .convert_int_to_float = false, .validate_index = false, .perform_schema_checks = false
                };

                return do_compact<IndexType, SchemaType, RowCountSegmentPolicy, DenseColumnPolicy>(
                        segments.begin(),
                        segments.end(),
                        pre_defragmentation_info.pipeline_context,
                        slices,
                        store,
                        segment_size,
                        compaction_options
                );
            }
    );

    return util::variant_match(
            std::move(result),
            [&slices, &pre_defragmentation_info, &store](CompactionWrittenKeys&& written_keys) -> VersionedItem {
                return collate_and_write(
                        store,
                        pre_defragmentation_info.pipeline_context,
                        slices,
                        std::move(written_keys),
                        pre_defragmentation_info.append_after.value(),
                        std::nullopt
                );
            },
            [](Error&& error) -> VersionedItem {
                error.throw_error();
                return VersionedItem{}; // unreachable
            }
    );
}

void set_row_id_if_index_only(
        const PipelineContext& pipeline_context, SegmentInMemory& frame, const ReadQuery& read_query
) {
    if (read_query.columns && read_query.columns->empty() &&
        pipeline_context.descriptor().index().type() == IndexDescriptor::Type::ROWCOUNT) {
        frame.set_row_id(static_cast<ssize_t>(pipeline_context.rows_ - 1));
    }
}

std::shared_ptr<PipelineContext> setup_pipeline_context(
        const std::shared_ptr<Store>& store, const VersionIdentifier& version_info, ReadQuery& read_query,
        const ReadOptions& read_options
) {
    using namespace arcticdb::pipelines;
    auto pipeline_context = std::make_shared<PipelineContext>();

    const bool has_active_version = !std::holds_alternative<StreamId>(version_info);
    if (!has_active_version) {
        pipeline_context->stream_id_ = std::get<StreamId>(version_info);
    } else {
        if (std::holds_alternative<VersionedItem>(version_info)) {
            pipeline_context->stream_id_ = std::get<VersionedItem>(version_info).key_.id();
            auto maybe_isr =
                    get_index_segment_reader(*store, pipeline_context, std::get<VersionedItem>(version_info).key_);
            if (maybe_isr.has_value()) {
                read_indexed_keys_to_pipeline(pipeline_context, std::move(*maybe_isr), read_query, read_options);
            }
        } else { // std::holds_alternative<SchemaItem>(version_info)
            pipeline_context->stream_id_ = std::get<std::shared_ptr<SchemaItem>>(version_info)->key_.id();
            // The SchemaItem should be reusable if collect() is called multiple times on the same lazy dataframe, hence
            // the clone
            index::IndexSegmentReader isr(std::get<std::shared_ptr<SchemaItem>>(version_info)->index_seg_.clone());
            read_indexed_keys_to_pipeline(pipeline_context, std::move(isr), read_query, read_options);
        }
    }

    if (pipeline_context->multi_key_) {
        return pipeline_context;
    }

    if (read_options.get_incompletes()) {
        util::check(
                std::holds_alternative<IndexRange>(read_query.row_filter), "Streaming read requires date range filter"
        );
        const auto& query_range = std::get<IndexRange>(read_query.row_filter);
        const auto existing_range = pipeline_context->index_range();
        if (!existing_range.specified_ || query_range.end_ > existing_range.end_) {
            const ReadIncompletesFlags read_incompletes_flags{
                    .dynamic_schema = opt_false(read_options.dynamic_schema()), .has_active_version = has_active_version
            };
            read_incompletes_to_pipeline(
                    store, pipeline_context, std::nullopt, read_query, read_options, read_incompletes_flags
            );
        }
    }

    if (std::holds_alternative<StreamId>(version_info) && !pipeline_context->incompletes_after_) {
        missing_data::raise<ErrorCode::E_NO_SYMBOL_DATA>(
                "read_dataframe_impl: read returned no data for symbol {} (found no versions or append data)",
                pipeline_context->stream_id_
        );
    }

    modify_descriptor(pipeline_context, read_options);
    generate_filtered_field_descriptors(pipeline_context, read_query.columns);
    return pipeline_context;
}

VersionedItem generate_result_versioned_item(const VersionIdentifier& version_info) {
    VersionedItem versioned_item;
    if (std::holds_alternative<StreamId>(version_info)) {
        // This isn't ideal. It would be better if the version() and timestamp() methods on the C++ VersionedItem class
        // returned optionals, but this change would bubble up to the Python VersionedItem class defined in _store.py.
        // This class is very hard to change at this point, as users do things like pickling them to pass them around.
        // This at least gets the symbol attribute of VersionedItem correct. The creation timestamp will be zero, which
        // corresponds to 1970, and so with this obviously ridiculous version ID, it should be clear to users that these
        // values are meaningless before an indexed version exists.
        versioned_item = VersionedItem(AtomKeyBuilder()
                                               .version_id(std::numeric_limits<VersionId>::max())
                                               .build<KeyType::TABLE_INDEX>(std::get<StreamId>(version_info)));
    } else if (std::holds_alternative<VersionedItem>(version_info)) {
        versioned_item = std::get<VersionedItem>(version_info);
    } else { // std::holds_alternative<std::shared_ptr<SchemaItem>>(version_info)
        versioned_item = std::get<std::shared_ptr<SchemaItem>>(version_info)->key_;
    }
    return versioned_item;
}

folly::Future<ReadVersionOutput> read_frame_for_version(
        const std::shared_ptr<Store>& store, const VersionIdentifier& version_info,
        const std::shared_ptr<ReadQuery>& read_query, const ReadOptions& read_options, std::any& handler_data
) {
    return async::submit_io_task(SetupPipelineContextTask{store, version_info, read_query, read_options})
            .via(&async::cpu_executor())
            .thenValue([store, read_query, read_options, version_info, &handler_data](auto&& pipeline_context) {
                auto res_versioned_item = generate_result_versioned_item(version_info);
                if (pipeline_context->multi_key_) {
                    if (read_query) {
                        check_can_perform_processing(pipeline_context, *read_query);
                    }
                    return read_multi_key(
                            store,
                            read_options,
                            *pipeline_context->multi_key_,
                            handler_data,
                            std::move(res_versioned_item.key_)
                    );
                }
                ARCTICDB_DEBUG(log::version(), "Fetching data to frame");
                DecodePathData shared_data;
                return do_direct_read_or_process(
                               store, read_query, read_options, pipeline_context, shared_data, handler_data
                )
                        .thenValue([res_versioned_item = std::move(res_versioned_item),
                                    pipeline_context,
                                    read_options,
                                    &handler_data,
                                    read_query,
                                    shared_data](auto&& frame) mutable {
                            ARCTICDB_DEBUG(log::version(), "Reduce and fix columns");
                            return reduce_and_fix_columns(pipeline_context, frame, read_options, handler_data)
                                    .via(&async::cpu_executor())
                                    .thenValue([res_versioned_item,
                                                pipeline_context,
                                                frame,
                                                read_query,
                                                shared_data](auto&&) mutable {
                                        set_row_id_if_index_only(*pipeline_context, frame, *read_query);
                                        return ReadVersionOutput{
                                                std::move(res_versioned_item),
                                                {frame,
                                                 timeseries_descriptor_from_pipeline_context(
                                                         pipeline_context, {}, pipeline_context->bucketize_dynamic_
                                                 ),
                                                 {}}
                                        };
                                    });
                        });
            });
}

folly::Future<std::vector<SliceAndKey>> read_modify_write_data_keys(
        const std::shared_ptr<Store>& store, std::shared_ptr<ReadQuery> read_query, const ReadOptions& read_options,
        const WriteOptions& write_options, const IndexPartialKey& target_partial_index_key,
        const std::shared_ptr<PipelineContext>& pipeline_context
) {
    read_query->clauses_.push_back(std::make_shared<Clause>(
            WriteClause(write_options, target_partial_index_key, std::make_shared<DeDupMap>(), store)
    ));

    auto component_manager = std::make_shared<ComponentManager>();
    return read_and_schedule_processing(store, pipeline_context, read_query, read_options, component_manager)
            .thenValue([component_manager,
                        pipeline_context,
                        read_query = std::move(read_query)](std::vector<EntityId>&& processed_entity_ids) {
                generate_output_schema_and_save_to_pipeline(*pipeline_context, *read_query);
                std::vector<folly::Future<SliceAndKey>> write_segments_futures;
                ranges::transform(
                        std::get<0>(component_manager->get_entities<std::shared_ptr<folly::Future<SliceAndKey>>>(
                                processed_entity_ids
                        )),
                        std::back_inserter(write_segments_futures),
                        [](const std::shared_ptr<folly::Future<SliceAndKey>>& fut) {
                            return std::move(*fut).thenValueInline([](SliceAndKey&& slice_and_key) {
                                slice_and_key.unset_segment();
                                return slice_and_key;
                            });
                        }
                );
                return folly::collect(std::move(write_segments_futures));
            });
}

folly::Future<VersionedItem> read_modify_write_impl(
        const std::shared_ptr<Store>& store, const std::shared_ptr<ReadQuery>& read_query,
        const ReadOptions& read_options, const WriteOptions& write_options,
        const IndexPartialKey& target_partial_index_key, const std::shared_ptr<PipelineContext>& pipeline_context,
        std::optional<proto::descriptors::UserDefinedMetadata>&& user_meta_proto
) {
    return read_modify_write_data_keys(
                   store, read_query, read_options, write_options, target_partial_index_key, pipeline_context
    )
            .thenValue([&](std::vector<SliceAndKey>&& data_keys_and_slices) {
                debug::check<ErrorCode::E_ASSERTION_FAILURE>(
                        std::ranges::all_of(
                                data_keys_and_slices,
                                [](const SliceAndKey& slice_and_key) {
                                    return !slice_and_key.has_segment() || slice_and_key.segment().empty();
                                }
                        ),
                        "All segments should be empty after read_modify_write"
                );
                ranges::sort(data_keys_and_slices);
                compact_row_slices(data_keys_and_slices);
                const size_t row_count = data_keys_and_slices.empty()
                                                 ? 0
                                                 : data_keys_and_slices.back().slice().row_range.second -
                                                           data_keys_and_slices.front().slice().row_range.first;
                const TimeseriesDescriptor tsd = make_timeseries_descriptor(
                        row_count,
                        pipeline_context->descriptor(),
                        std::move(*pipeline_context->norm_meta_),
                        std::move(user_meta_proto),
                        std::nullopt,
                        std::nullopt,
                        write_options.bucketize_dynamic
                );
                return index::write_index(
                        index_type_from_descriptor(pipeline_context->descriptor()),
                        tsd,
                        std::move(data_keys_and_slices),
                        target_partial_index_key,
                        store
                );
            });
}

folly::Future<VersionedItem> merge_update_impl(
        const std::shared_ptr<Store>& store, const VersionIdentifier& version_info, const ReadOptions& read_options,
        const WriteOptions& write_options, const IndexPartialKey& target_partial_index_key,
        std::vector<std::string>&& on, const MergeStrategy& strategy, std::shared_ptr<InputFrame> source
) {
    auto read_query = std::make_shared<ReadQuery>();
    const StreamDescriptor& source_descriptor = source->desc();
    read_query->clauses_.push_back(std::make_shared<Clause>(MergeUpdateClause(std::move(on), strategy, source)));
    std::shared_ptr<PipelineContext> pipeline_context =
            setup_pipeline_context(store, version_info, *read_query, read_options);
    // TODO: Rely on modify_schema for this https://man312219.monday.com/boards/7852509418/pulses/10997979275
    schema::check<ErrorCode::E_DESCRIPTOR_MISMATCH>(
            columns_match(pipeline_context->descriptor(), source_descriptor),
            "Cannot perform merge update when the source and target schema are not the same.\nSource schema: "
            "{}\nTarget schema: {}",
            source_descriptor,
            pipeline_context->descriptor()
    );
    user_input::check<ErrorCode::E_INVALID_USER_ARGUMENT>(
            !write_options.dynamic_schema, "Cannot merge update with dynamic schema"
    );
    user_input::check<ErrorCode::E_INVALID_USER_ARGUMENT>(
            pipeline_context->descriptor().index().type() == IndexDescriptor::Type::TIMESTAMP &&
                    pipeline_context->descriptor().sorted() == SortedValue::ASCENDING,
            "Only timeseries ascending indexed target data is supported for merge update"
    );
    return read_modify_write_data_keys(
                   store, read_query, read_options, write_options, target_partial_index_key, pipeline_context
    )
            .thenValue([pipeline_context = std::move(pipeline_context),
                        store,
                        write_options,
                        source = std::move(source),
                        target_partial_index_key](std::vector<SliceAndKey>&& data_keys_and_slices) {
                // TODO: This needs to be changed to account for the INSERT option of merge update. Insert can create
                // new segments and shift row slices.
                ranges::sort(data_keys_and_slices);
                std::vector<SliceAndKey> merged_ranges_and_keys;
                auto new_slice = data_keys_and_slices.begin();
                for (SliceAndKey& slice : pipeline_context->slice_and_keys_) {
                    if (new_slice != data_keys_and_slices.end() && new_slice->slice_ == slice.slice_) {
                        merged_ranges_and_keys.push_back(std::move(*new_slice));
                        ++new_slice;
                    } else {
                        merged_ranges_and_keys.push_back(std::move(slice));
                    }
                }
                pipeline_context->slice_and_keys_.clear();
                const size_t row_count = merged_ranges_and_keys.empty()
                                                 ? 0
                                                 : merged_ranges_and_keys.back().slice().row_range.second -
                                                           merged_ranges_and_keys.front().slice().row_range.first;
                const TimeseriesDescriptor tsd = make_timeseries_descriptor(
                        row_count,
                        pipeline_context->descriptor(),
                        std::move(*pipeline_context->norm_meta_),
                        pipeline_context->user_meta_ ? std::make_optional(std::move(source->user_meta)) : std::nullopt,
                        std::nullopt,
                        std::nullopt,
                        write_options.bucketize_dynamic
                );
                return index::write_index(
                        index_type_from_descriptor(pipeline_context->descriptor()),
                        tsd,
                        std::move(merged_ranges_and_keys),
                        target_partial_index_key,
                        store
                );
            });
}

folly::Future<SymbolProcessingResult> read_and_process(
        const std::shared_ptr<Store>& store, const VersionIdentifier& version_info,
        const std::shared_ptr<ReadQuery>& read_query, const ReadOptions& read_options,
        std::shared_ptr<ComponentManager> component_manager
) {
    auto pipeline_context = setup_pipeline_context(store, version_info, *read_query, read_options);
    auto res_versioned_item = generate_result_versioned_item(version_info);

    user_input::check<ErrorCode::E_INVALID_USER_ARGUMENT>(
            !pipeline_context->multi_key_, "Multi-symbol joins not supported with recursively normalized data"
    );

    if (std::holds_alternative<StreamId>(version_info) && !pipeline_context->incompletes_after_) {
        return SymbolProcessingResult{std::move(res_versioned_item), {}, {}, {}};
    }

    schema::check<ErrorCode::E_DESCRIPTOR_MISMATCH>(
            !pipeline_context->is_pickled(), "Cannot perform multi-symbol join on pickled data"
    );

    OutputSchema output_schema = generate_output_schema(*pipeline_context, *read_query);
    ARCTICDB_DEBUG(log::version(), "Fetching data to frame");

    return read_and_schedule_processing(store, pipeline_context, read_query, read_options, std::move(component_manager))
            .thenValueInline([res_versioned_item = std::move(res_versioned_item),
                              pipeline_context,
                              output_schema = std::move(output_schema)](auto&& entity_ids) mutable {
                // Pipeline context user metadata is not populated in the case that only incomplete segments exist
                // for a symbol, no indexed versions
                return SymbolProcessingResult{
                        std::move(res_versioned_item),
                        pipeline_context->user_meta_ ? std::move(*pipeline_context->user_meta_)
                                                     : proto::descriptors::UserDefinedMetadata{},
                        std::move(output_schema),
                        std::move(entity_ids)
                };
            });
}
} // namespace arcticdb::version_store

namespace arcticdb {

void remove_written_keys(Store* const store, CompactionWrittenKeys&& written_keys) {
    log::version().debug("Error during compaction, removing {} keys written before failure", written_keys.size());
    store->remove_keys_sync(std::move(written_keys));
}

bool is_segment_unsorted(const SegmentInMemory& segment) {
    return segment.descriptor().sorted() == SortedValue::DESCENDING ||
           segment.descriptor().sorted() == SortedValue::UNSORTED;
}

CheckOutcome check_schema_matches_incomplete(
        const StreamDescriptor& stream_descriptor_incomplete, const StreamDescriptor& pipeline_desc,
        const bool convert_int_to_float
) {
    // We need to check that the index names match regardless of the dynamic schema setting
    if (!index_names_match(stream_descriptor_incomplete, pipeline_desc)) {
        return Error{
                throw_error<ErrorCode::E_DESCRIPTOR_MISMATCH>,
                fmt::format(
                        "{} All staged segments must have the same index names."
                        "{} is different than {}",
                        error_code_data<ErrorCode::E_DESCRIPTOR_MISMATCH>.name_,
                        stream_descriptor_incomplete,
                        pipeline_desc
                )
        };
    }
    if (!columns_match(pipeline_desc, stream_descriptor_incomplete, convert_int_to_float)) {
        return Error{
                throw_error<ErrorCode::E_DESCRIPTOR_MISMATCH>,
                fmt::format(
                        "{} When static schema is used all staged segments must have the same column and column types."
                        "{} is different than {}",
                        error_code_data<ErrorCode::E_DESCRIPTOR_MISMATCH>.name_,
                        stream_descriptor_incomplete,
                        pipeline_desc
                )
        };
    }
    return std::monostate{};
}

size_t n_segments_live_during_compaction() {
    int64_t default_count = 2 * async::TaskScheduler::instance()->io_thread_count();
    int64_t res = ConfigsMap::instance()->get_int("VersionStore.NumSegmentsLiveDuringCompaction", default_count);
    log::version().debug("Allowing up to {} segments to be live during compaction", res);
    static constexpr auto max_size = static_cast<int64_t>(folly::NativeSemaphore::value_max_v);
    util::check(
            res < max_size,
            "At most {} live segments during compaction supported but were {}, adjust "
            "VersionStore.NumSegmentsLiveDuringCompaction",
            max_size,
            res
    );
    util::check(res > 0, "VersionStore.NumSegmentsLiveDuringCompaction must be strictly positive but was {}", res);
    return static_cast<size_t>(res);
}

} // namespace arcticdb
