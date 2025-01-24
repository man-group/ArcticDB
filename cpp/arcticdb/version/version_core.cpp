/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#include <folly/futures/FutureSplitter.h>

#include <arcticdb/version/version_core.hpp>
#include <arcticdb/pipeline/write_options.hpp>
#include <arcticdb/stream/index.hpp>
#include <arcticdb/pipeline/query.hpp>
#include <arcticdb/pipeline/read_pipeline.hpp>
#include <arcticdb/async/task_scheduler.hpp>
#include <arcticdb/async/tasks.hpp>
#include <arcticdb/util/name_validation.hpp>
#include <arcticdb/stream/append_map.hpp>
#include <arcticdb/pipeline/pipeline_context.hpp>
#include <arcticdb/pipeline/read_frame.hpp>
#include <arcticdb/pipeline/read_options.hpp>
#include <arcticdb/stream/stream_sink.hpp>
#include <arcticdb/stream/schema.hpp>
#include <arcticdb/pipeline/index_writer.hpp>
#include <arcticdb/pipeline/index_utils.hpp>
#include <arcticdb/version/schema_checks.hpp>
#include <arcticdb/version/version_utils.hpp>
#include <arcticdb/entity/merge_descriptors.hpp>
#include <arcticdb/version/read_version_output.hpp>

namespace arcticdb::version_store {

void modify_descriptor(const std::shared_ptr<pipelines::PipelineContext>& pipeline_context, const ReadOptions& read_options) {

    if (opt_false(read_options.force_strings_to_object_) || opt_false(read_options.force_strings_to_fixed_))
        pipeline_context->orig_desc_ = pipeline_context->desc_;

    auto& desc = *pipeline_context->desc_;
    if (opt_false(read_options.force_strings_to_object_)) {
        auto& fields = desc.fields();
        for (Field& field_desc : fields) {
            if (field_desc.type().data_type() == DataType::ASCII_FIXED64)
                set_data_type(DataType::ASCII_DYNAMIC64, field_desc.mutable_type());

            if (field_desc.type().data_type() == DataType::UTF_FIXED64)
                set_data_type(DataType::UTF_DYNAMIC64, field_desc.mutable_type());
        }
    } else if (opt_false(read_options.force_strings_to_fixed_)) {
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
    const std::shared_ptr<Store>& store,
    VersionId version_id,
    const std::shared_ptr<pipelines::InputTensorFrame>& frame,
    const WriteOptions& options,
    const std::shared_ptr<DeDupMap>& de_dup_map,
    bool sparsify_floats,
    bool validate_index
    ) {
    ARCTICDB_SUBSAMPLE_DEFAULT(WaitForWriteCompletion)
    ARCTICDB_DEBUG(log::version(), "write_dataframe_impl stream_id: {} , version_id: {}, {} rows", frame->desc.id(), version_id, frame->num_rows);
    auto atom_key_fut = async_write_dataframe_impl(store, version_id, frame, options, de_dup_map, sparsify_floats, validate_index);
    return {std::move(atom_key_fut).get()};
}

folly::Future<entity::AtomKey> async_write_dataframe_impl(
    const std::shared_ptr<Store>& store,
    VersionId version_id,
    const std::shared_ptr<InputTensorFrame>& frame,
    const WriteOptions& options,
    const std::shared_ptr<DeDupMap> &de_dup_map,
    bool sparsify_floats,
    bool validate_index
    ) {
    ARCTICDB_SAMPLE(DoWrite, 0)
    if (version_id == 0)
        verify_symbol_key(frame->desc.id());
    // Slice the frame according to the write options
    frame->set_bucketize_dynamic(options.bucketize_dynamic);
    auto slicing_arg = get_slicing_policy(options, *frame);
    auto partial_key = IndexPartialKey{frame->desc.id(), version_id};
    if (validate_index && !index_is_not_timeseries_or_is_sorted_ascending(*frame)) {
        sorting::raise<ErrorCode::E_UNSORTED_DATA>("When calling write with validate_index enabled, input data must be sorted");
    }
    return write_frame(std::move(partial_key), frame, slicing_arg, store, de_dup_map, sparsify_floats);
}

namespace {
IndexDescriptorImpl check_index_match(const arcticdb::stream::Index& index, const IndexDescriptorImpl& desc) {
    if (std::holds_alternative<stream::TimeseriesIndex>(index))
        util::check(
            desc.type() == IndexDescriptor::Type::TIMESTAMP || desc.type() == IndexDescriptor::Type::EMPTY,
                    "Index mismatch, cannot update a non-timeseries-indexed frame with a timeseries");
    else
        util::check(desc.type() == IndexDescriptorImpl::Type::ROWCOUNT,
                    "Index mismatch, cannot update a timeseries with a non-timeseries-indexed frame");

    return desc;
}
}

void sorted_data_check_append(const InputTensorFrame& frame, index::IndexSegmentReader& index_segment_reader){
    if (!index_is_not_timeseries_or_is_sorted_ascending(frame)) {
        sorting::raise<ErrorCode::E_UNSORTED_DATA>("When calling append with validate_index enabled, input data must be sorted");
    }
    sorting::check<ErrorCode::E_UNSORTED_DATA>(
        !std::holds_alternative<stream::TimeseriesIndex>(frame.index) ||
        index_segment_reader.tsd().sorted() == SortedValue::ASCENDING,
        "When calling append with validate_index enabled, the existing data must be sorted");
}

folly::Future<AtomKey> async_append_impl(
    const std::shared_ptr<Store>& store,
    const UpdateInfo& update_info,
    const std::shared_ptr<InputTensorFrame>& frame,
    const WriteOptions& options,
    bool validate_index,
    bool empty_types) {

    util::check(update_info.previous_index_key_.has_value(), "Cannot append as there is no previous index key to append to");
    const StreamId stream_id = frame->desc.id();
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
    return append_frame(IndexPartialKey{stream_id, update_info.next_version_id_}, frame, slicing_arg, index_segment_reader, store, options.dynamic_schema, options.ignore_sort_order);
}

VersionedItem append_impl(
    const std::shared_ptr<Store>& store,
    const UpdateInfo& update_info,
    const std::shared_ptr<InputTensorFrame>& frame,
    const WriteOptions& options,
    bool validate_index,
    bool empty_types) {

    ARCTICDB_SUBSAMPLE_DEFAULT(WaitForWriteCompletion)
    auto version_key_fut = async_append_impl(store,
                                             update_info,
                                             frame,
                                             options,
                                             validate_index,
                                             empty_types);
    auto version_key = std::move(version_key_fut).get();
    auto versioned_item = VersionedItem(to_atom(std::move(version_key)));
    ARCTICDB_DEBUG(log::version(), "write_dataframe_impl stream_id: {} , version_id: {}", versioned_item.symbol(), update_info.next_version_id_);
    return versioned_item;
}

namespace {
bool is_before(const IndexRange& a, const IndexRange& b) {
    return a.start_ < b.start_;
}

bool is_after(const IndexRange& a, const IndexRange& b) {
    return a.end_ > b.end_;
}

std::vector<SliceAndKey> filter_existing_slices(std::vector<std::optional<SliceAndKey>>&& maybe_slices) {
    std::vector<SliceAndKey> result;
    for (auto& maybe_slice : maybe_slices) {
        if (maybe_slice.has_value()) {
            result.push_back(std::move(*maybe_slice));
        }
    }
    return result;
}

/// Represents all slices which are intersecting (but not overlapping) with range passed to update
/// First member is a vector of all segments intersecting with the first row-slice of the update range
/// Second member is a vector of all segments intersecting with the last row-slice of the update range
using IntersectingSegments = std::tuple<std::vector<SliceAndKey>, std::vector<SliceAndKey>>;

[[nodiscard]] folly::Future<IntersectingSegments> async_intersecting_segments(
    const std::vector<SliceAndKey>& affected_keys,
    const IndexRange& front_range,
    const IndexRange& back_range,
    VersionId version_id,
    const std::shared_ptr<Store>& store
) {
    if (!front_range.specified_ && !back_range.specified_) {
        return folly::makeFuture<IntersectingSegments>(IntersectingSegments{});
    }
    internal::check<ErrorCode::E_ASSERTION_FAILURE>(
        front_range.specified_ && back_range.specified_,
        "Both first and last index range of the update range must intersect with at least one of the slices in the dataframe");
    std::vector<folly::Future<std::optional<SliceAndKey>>> maybe_intersect_before_fut;
    std::vector<folly::Future<std::optional<SliceAndKey>>> maybe_intersect_after_fut;

    for (const auto& affected_slice_and_key : affected_keys) {
        const auto& affected_range = affected_slice_and_key.key().index_range();
        if (intersects(affected_range, front_range) && !overlaps(affected_range, front_range) &&
            is_before(affected_range, front_range)) {
            maybe_intersect_before_fut.emplace_back(async_rewrite_partial_segment(
                affected_slice_and_key,
                front_range,
                version_id,
                AffectedSegmentPart::START,
                store
            ));
        }

        if (intersects(affected_range, back_range) && !overlaps(affected_range, back_range) &&
            is_after(affected_range, back_range)) {
            maybe_intersect_after_fut.emplace_back(async_rewrite_partial_segment(
                affected_slice_and_key,
                back_range,
                version_id,
                AffectedSegmentPart::END,
                store
            ));
        }
    }
    return collect(
        collect(maybe_intersect_before_fut).via(&async::io_executor()).thenValueInline(filter_existing_slices),
        collect(maybe_intersect_after_fut).via(&async::io_executor()).thenValueInline(filter_existing_slices)
    ).via(&async::io_executor());
}

} // namespace

VersionedItem delete_range_impl(
    const std::shared_ptr<Store>& store,
    const StreamId& stream_id,
    const UpdateInfo& update_info,
    const UpdateQuery& query,
    const WriteOptions&& ,
    bool dynamic_schema) {

    util::check(update_info.previous_index_key_.has_value(), "Cannot delete from non-existent symbol {}", stream_id);
    util::check(std::holds_alternative<IndexRange>(query.row_filter), "Delete range requires index range argument");
    const auto& index_range = std::get<IndexRange>(query.row_filter);
    ARCTICDB_DEBUG(log::version(), "Delete range in versioned dataframe for stream_id: {} , version_id = {}", stream_id, update_info.next_version_id_);

    auto index_segment_reader = index::get_index_reader(update_info.previous_index_key_.value(), store);
    util::check_rte(!index_segment_reader.is_pickled(), "Cannot delete date range of pickled data");

    auto index = index_type_from_descriptor(index_segment_reader.tsd().as_stream_descriptor());
    util::check(std::holds_alternative<TimeseriesIndex>(index), "Delete in range will not work as expected with a non-timeseries index");

    std::vector<FilterQuery<index::IndexSegmentReader>> queries =
            build_update_query_filters<index::IndexSegmentReader>(query.row_filter, index, index_range, dynamic_schema, index_segment_reader.bucketize_dynamic());
    auto combined = combine_filter_functions(queries);
    auto affected_keys = filter_index(index_segment_reader, std::move(combined));
    std::vector<SliceAndKey> unaffected_keys;
    std::set_difference(std::begin(index_segment_reader),
                        std::end(index_segment_reader),
                        std::begin(affected_keys),
                        std::end(affected_keys),
                        std::back_inserter(unaffected_keys));

    auto [intersect_before, intersect_after] = async_intersecting_segments(affected_keys, index_range, index_range, update_info.next_version_id_, store).get();

    auto orig_filter_range = std::holds_alternative<std::monostate>(query.row_filter) ? get_query_index_range(index, index_range) : query.row_filter;

    size_t row_count = 0;
    const std::array<std::vector<SliceAndKey>, 5> groups{
        strictly_before(orig_filter_range, unaffected_keys),
        std::move(intersect_before),
        std::move(intersect_after),
        strictly_after(orig_filter_range, unaffected_keys)};
    auto flattened_slice_and_keys = flatten_and_fix_rows(groups, row_count);

    std::sort(std::begin(flattened_slice_and_keys), std::end(flattened_slice_and_keys));
    auto version_key_fut = util::variant_match(index, [&index_segment_reader, &flattened_slice_and_keys, &stream_id, &update_info, &store] (auto idx) {
        using IndexType = decltype(idx);
        return pipelines::index::write_index<IndexType>(index_segment_reader.tsd(), std::move(flattened_slice_and_keys), IndexPartialKey{stream_id, update_info.next_version_id_}, store);
    });
    auto version_key = std::move(version_key_fut).get();
    auto versioned_item = VersionedItem(to_atom(std::move(version_key)));
    ARCTICDB_DEBUG(log::version(), "updated stream_id: {} , version_id: {}", stream_id, update_info.next_version_id_);
    return versioned_item;
}

void check_update_data_is_sorted(const InputTensorFrame& frame, const index::IndexSegmentReader& index_segment_reader){
    bool is_time_series = std::holds_alternative<stream::TimeseriesIndex>(frame.index);
    sorting::check<ErrorCode::E_UNSORTED_DATA>(
        is_time_series,
        "When calling update, the input data must be a time series.");
    bool input_data_is_sorted = frame.desc.sorted() == SortedValue::ASCENDING ||
                                frame.desc.sorted() == SortedValue::UNKNOWN;
    // If changing this error message, the corresponding message in _normalization.py::restrict_data_to_date_range_only should also be updated
    sorting::check<ErrorCode::E_UNSORTED_DATA>(
        input_data_is_sorted,
        "When calling update, the input data must be sorted.");
    bool existing_data_is_sorted = index_segment_reader.sorted() == SortedValue::ASCENDING ||
                                    index_segment_reader.sorted() == SortedValue::UNKNOWN;
    sorting::check<ErrorCode::E_UNSORTED_DATA>(
         existing_data_is_sorted,
        "When calling update, the existing data must be sorted.");
}

struct UpdateRanges {
    IndexRange front;
    IndexRange back;
    IndexRange original_index_range;
};

static UpdateRanges compute_update_ranges(const FilterRange& row_filter, const InputTensorFrame& update_frame, std::span<SliceAndKey> update_slice_and_keys) {
    return util::variant_match(row_filter,
        [&](std::monostate) -> UpdateRanges {
            util::check(std::holds_alternative<TimeseriesIndex>(update_frame.index), "Update with row count index is not permitted");
            if (update_slice_and_keys.empty()) {
                // If there are no new keys, then we can't intersect with the existing data.
                return UpdateRanges{{}, {}, update_frame.index_range};
            }
            IndexRange back_range = update_slice_and_keys.back().key().index_range();
            back_range.adjust_open_closed_interval();
            return UpdateRanges{
                update_slice_and_keys.front().key().index_range(),
                std::move(back_range),
                update_frame.index_range};
        },
        [&](const IndexRange& idx_range) {
            return UpdateRanges{idx_range, idx_range, idx_range};
        },
        [](const RowRange&) -> UpdateRanges {
            util::raise_rte("Unexpected row_range in update query");
            return {};
        }
    );
}

static void check_can_update(
    const InputTensorFrame& frame,
    const index::IndexSegmentReader& index_segment_reader,
    const UpdateInfo& update_info,
    bool dynamic_schema,
    bool empty_types
) {
    util::check(update_info.previous_index_key_.has_value(), "Cannot update as there is no previous index key to update into");
    util::check_rte(!index_segment_reader.is_pickled(), "Cannot update pickled data");
    const auto index_desc = check_index_match(frame.index, index_segment_reader.tsd().index());
    util::check(index::is_timeseries_index(index_desc), "Update not supported for non-timeseries indexes");
    check_update_data_is_sorted(frame, index_segment_reader);
    (void)check_and_mark_slices(index_segment_reader, dynamic_schema, false, std::nullopt, index_segment_reader.bucketize_dynamic());
    fix_descriptor_mismatch_or_throw(UPDATE, dynamic_schema, index_segment_reader, frame, empty_types);
}

static std::shared_ptr<std::vector<SliceAndKey>> get_keys_affected_by_update(
        const index::IndexSegmentReader& index_segment_reader,
        const InputTensorFrame& frame,
        const UpdateQuery& query,
        bool dynamic_schema) {
    std::vector<FilterQuery<index::IndexSegmentReader>> queries = build_update_query_filters<index::IndexSegmentReader>(
    query.row_filter,
    frame.index,
    frame.index_range,
    dynamic_schema,
    index_segment_reader.bucketize_dynamic());
    return std::make_shared<std::vector<SliceAndKey>>(filter_index(index_segment_reader, combine_filter_functions(queries)));
}

static std::vector<SliceAndKey> get_keys_not_affected_by_update(
    const index::IndexSegmentReader& index_segment_reader,
    std::span<const SliceAndKey> affected_keys
) {
    std::vector<SliceAndKey> unaffected_keys;
    std::set_difference(index_segment_reader.begin(),
        index_segment_reader.end(),
        affected_keys.begin(),
        affected_keys.end(),
        std::back_inserter(unaffected_keys));
    return unaffected_keys;
}

static std::pair<std::vector<SliceAndKey>, size_t> get_slice_and_keys_for_update(
        const UpdateRanges& update_ranges,
        std::span<const SliceAndKey> unaffected_keys,
        std::span<const SliceAndKey> affected_keys,
        IntersectingSegments&& segments_intersecting_with_update_range,
        std::vector<SliceAndKey>&& new_slice_and_keys) {
    const size_t new_keys_size = new_slice_and_keys.size();
    size_t row_count = 0;
    const std::array<std::vector<SliceAndKey>, 5> groups{
        strictly_before(update_ranges.original_index_range, unaffected_keys),
        std::move(std::get<0>(segments_intersecting_with_update_range)),
        std::move(new_slice_and_keys),
        std::move(std::get<1>(segments_intersecting_with_update_range)),
        strictly_after(update_ranges.original_index_range, unaffected_keys)};
    auto flattened_slice_and_keys = flatten_and_fix_rows(groups, row_count);
    util::check(unaffected_keys.size() + new_keys_size + (affected_keys.size() * 2) >= flattened_slice_and_keys.size(),
            "Output size mismatch: {} + {} + (2 * {}) < {}",
            unaffected_keys.size(), new_keys_size, affected_keys.size(), flattened_slice_and_keys.size());
    std::sort(std::begin(flattened_slice_and_keys), std::end(flattened_slice_and_keys));
    return {flattened_slice_and_keys, row_count};
}

folly::Future<AtomKey> async_update_impl(
    const std::shared_ptr<Store>& store,
    const UpdateInfo& update_info,
    const UpdateQuery& query,
    const std::shared_ptr<InputTensorFrame>& frame,
    const WriteOptions& options,
    bool dynamic_schema,
    bool empty_types) {
    return index::async_get_index_reader(*(update_info.previous_index_key_), store).thenValue([
        store,
        update_info,
        query,
        frame,
        options=options,
        dynamic_schema,
        empty_types
        ](index::IndexSegmentReader&& index_segment_reader) {
        check_can_update(*frame, index_segment_reader, update_info, dynamic_schema, empty_types);
        ARCTICDB_DEBUG(log::version(), "Update versioned dataframe for stream_id: {} , version_id = {}", frame->desc.id(), update_info.previous_index_key_->version_id());
        frame->set_bucketize_dynamic(index_segment_reader.bucketize_dynamic());
        return slice_and_write(frame, get_slicing_policy(options, *frame), IndexPartialKey{frame->desc.id(), update_info.next_version_id_} , store
        ).via(&async::cpu_executor()).thenValue([
            store,
            update_info,
            query,
            frame,
            dynamic_schema,
            index_segment_reader=std::move(index_segment_reader)
        ](std::vector<SliceAndKey>&& new_slice_and_keys) mutable {
            std::sort(std::begin(new_slice_and_keys), std::end(new_slice_and_keys));
            auto affected_keys = get_keys_affected_by_update(index_segment_reader, *frame, query, dynamic_schema);
            auto unaffected_keys = get_keys_not_affected_by_update(index_segment_reader, *affected_keys);
            util::check(
                affected_keys->size() + unaffected_keys.size() == index_segment_reader.size(),
                "The sum of affected keys and unaffected keys must be equal to the total number of keys {} + {} != {}",
                affected_keys->size(), unaffected_keys.size(), index_segment_reader.size());
            const UpdateRanges update_ranges = compute_update_ranges(query.row_filter, *frame, new_slice_and_keys);

            return async_intersecting_segments(
                *affected_keys,
                update_ranges.front,
                update_ranges.back,
                update_info.next_version_id_,
                store).thenValue([new_slice_and_keys=std::move(new_slice_and_keys),
                    update_ranges=update_ranges,
                    unaffected_keys=std::move(unaffected_keys),
                    affected_keys=affected_keys,
                    index_segment_reader=std::move(index_segment_reader),
                    frame,
                    dynamic_schema,
                    update_info,
                    store](IntersectingSegments&& intersecting_segments) mutable {
                auto [flattened_slice_and_keys, row_count] = get_slice_and_keys_for_update(
                    update_ranges,
                    unaffected_keys,
                    *affected_keys,
                    std::move(intersecting_segments),
                    std::move(new_slice_and_keys));
                auto tsd = index::get_merged_tsd(row_count, dynamic_schema, index_segment_reader.tsd(), frame);
                return index::write_index(
                    index_type_from_descriptor(tsd.as_stream_descriptor()),
                    tsd,
                    std::move(flattened_slice_and_keys),
                    IndexPartialKey{frame->desc.id(), update_info.next_version_id_},
                    store
                );
            });
        });
    });
}

VersionedItem update_impl(
    const std::shared_ptr<Store>& store,
    const UpdateInfo& update_info,
    const UpdateQuery& query,
    const std::shared_ptr<InputTensorFrame>& frame,
    WriteOptions&& options,
    bool dynamic_schema,
    bool empty_types) {
    auto version_key = async_update_impl(store, update_info, query, frame, options, dynamic_schema, empty_types).get();
    auto versioned_item = VersionedItem(to_atom(std::move(version_key)));
    ARCTICDB_DEBUG(log::version(), "updated stream_id: {} , version_id: {}", frame->desc.id(), update_info.next_version_id_);
    return versioned_item;
}

folly::Future<ReadVersionOutput> read_multi_key(
    const std::shared_ptr<Store>& store,
    const SegmentInMemory& index_key_seg,
    std::shared_ptr<std::any>& handler_data) {
    std::vector<AtomKey> keys;
    for (size_t idx = 0; idx < index_key_seg.row_count(); idx++) {
        keys.emplace_back(stream::read_key_row(index_key_seg, static_cast<ssize_t>(idx)));
    }

    AtomKey dup{keys[0]};
    VersionedItem versioned_item{std::move(dup)};
    TimeseriesDescriptor multi_key_desc{index_key_seg.index_descriptor()};
    return read_frame_for_version(store, versioned_item, std::make_shared<ReadQuery>(), ReadOptions{}, handler_data)
    .thenValue([multi_key_desc=std::move(multi_key_desc), keys=std::move(keys)](ReadVersionOutput&& read_version_output) mutable {
        multi_key_desc.mutable_proto().mutable_normalization()->CopyFrom(read_version_output.frame_and_descriptor_.desc_.proto().normalization());
        read_version_output.frame_and_descriptor_.desc_ = std::move(multi_key_desc);
        read_version_output.frame_and_descriptor_.keys_ = std::move(keys);
        read_version_output.frame_and_descriptor_.buffers_ = std::make_shared<BufferHolder>();
        return std::move(read_version_output);
    });
}

void add_index_columns_to_query(const ReadQuery& read_query, const TimeseriesDescriptor& desc) {
    if (read_query.columns.has_value()) {
        auto index_columns = stream::get_index_columns_from_descriptor(desc);
        if(index_columns.empty())
            return;

        std::vector<std::string> index_columns_to_add;
        for(const auto& index_column : index_columns) {
            if(std::find(std::begin(*read_query.columns), std::end(*read_query.columns), index_column) == std::end(*read_query.columns))
                index_columns_to_add.emplace_back(index_column);
        }
        read_query.columns->insert(std::begin(*read_query.columns), std::begin(index_columns_to_add), std::end(index_columns_to_add));
    }
}

FrameAndDescriptor read_segment_impl(
    const std::shared_ptr<Store>& store,
    const VariantKey& key) {
    auto [_, seg] = store->read_sync(key);
    return frame_and_descriptor_from_segment(std::move(seg));
}

FrameAndDescriptor read_index_impl(
    const std::shared_ptr<Store>& store,
    const VersionedItem& version) {
    return read_segment_impl(store, version.key_);
}

std::optional<pipelines::index::IndexSegmentReader> get_index_segment_reader(
    const std::shared_ptr<Store>& store,
    const std::shared_ptr<PipelineContext>& pipeline_context,
    const VersionedItem& version_info) {
    std::pair<entity::VariantKey, SegmentInMemory> index_key_seg;
    try {
        index_key_seg = store->read_sync(version_info.key_);
    } catch (const std::exception& ex) {
        ARCTICDB_DEBUG(log::version(), "Key not found from versioned item {}: {}", version_info.key_, ex.what());
        throw storage::NoDataFoundException(fmt::format("When trying to read version {} of symbol `{}`, failed to read key {}: {}",
                                                        version_info.version(),
                                                        version_info.symbol(),
                                                        version_info.key_,
                                                        ex.what()));
    }

    if (variant_key_type(index_key_seg.first) == KeyType::MULTI_KEY) {
        pipeline_context->multi_key_ = index_key_seg.second;
        return std::nullopt;
    }
    return std::make_optional<pipelines::index::IndexSegmentReader>(std::move(index_key_seg.second));
}

void check_can_read_index_only_if_required(
        const index::IndexSegmentReader& index_segment_reader,
        const ReadQuery& read_query) {
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

void check_multi_key_is_not_index_only(
    const PipelineContext& pipeline_context,
    const ReadQuery& read_query) {
    user_input::check<ErrorCode::E_INVALID_USER_ARGUMENT>(
        !read_query.columns || (!pipeline_context.only_index_columns_selected() && !read_query.columns->empty()),
        "Reading the index column is not supported when recursive or custom normalizers are used."
    );
}

void read_indexed_keys_to_pipeline(
        const std::shared_ptr<Store>& store,
        const std::shared_ptr<PipelineContext>& pipeline_context,
        const VersionedItem& version_info,
        ReadQuery& read_query,
        const ReadOptions& read_options) {
    auto maybe_reader = get_index_segment_reader(store, pipeline_context, version_info);
    if(!maybe_reader)
        return;

    auto index_segment_reader = std::move(*maybe_reader);
    ARCTICDB_DEBUG(log::version(), "Read index segment with {} keys", index_segment_reader.size());
    check_can_read_index_only_if_required(index_segment_reader, read_query);
    check_column_and_date_range_filterable(index_segment_reader, read_query);
    add_index_columns_to_query(read_query, index_segment_reader.tsd());

    const auto& tsd = index_segment_reader.tsd();
    read_query.calculate_row_filter(static_cast<int64_t>(tsd.total_rows()));
    bool bucketize_dynamic = index_segment_reader.bucketize_dynamic();
    pipeline_context->desc_ = tsd.as_stream_descriptor();

    bool dynamic_schema = opt_false(read_options.dynamic_schema_);
    auto queries = get_column_bitset_and_query_functions<index::IndexSegmentReader>(
        read_query,
        pipeline_context,
        dynamic_schema,
        bucketize_dynamic);

    pipeline_context->slice_and_keys_ = filter_index(index_segment_reader, combine_filter_functions(queries));
    pipeline_context->total_rows_ = pipeline_context->calc_rows();
    pipeline_context->rows_ = index_segment_reader.tsd().total_rows();
    pipeline_context->norm_meta_ = std::make_unique<arcticdb::proto::descriptors::NormalizationMetadata>(std::move(*index_segment_reader.mutable_tsd().mutable_proto().mutable_normalization()));
    pipeline_context->user_meta_ = std::make_unique<arcticdb::proto::descriptors::UserDefinedMetadata>(std::move(*index_segment_reader.mutable_tsd().mutable_proto().mutable_user_meta()));
    pipeline_context->bucketize_dynamic_ = bucketize_dynamic;
}

// Returns true if there are staged segments
bool read_incompletes_to_pipeline(
    const std::shared_ptr<Store>& store,
    std::shared_ptr<PipelineContext>& pipeline_context,
    const ReadQuery& read_query,
    const ReadOptions& read_options,
    bool convert_int_to_float,
    bool via_iteration,
    bool sparsify,
    bool dynamic_schema) {

    auto incomplete_segments = get_incomplete(
        store,
        pipeline_context->stream_id_,
        read_query.row_filter,
        pipeline_context->last_row(),
        via_iteration,
        false);

    if(incomplete_segments.empty())
        return false;

    // In order to have the right normalization metadata and descriptor we need to find the first non-empty segment.
    // Picking an empty segment when there are non-empty ones will impact the index type and column namings.
    // If all segments are empty we will proceed as if were appending/writing and empty dataframe.
    debug::check<ErrorCode::E_ASSERTION_FAILURE>(!incomplete_segments.empty(), "Incomplete segments must be non-empty");
    const auto first_non_empty_seg = std::find_if(incomplete_segments.begin(), incomplete_segments.end(), [&](auto& slice){
        return slice.segment(store).row_count() > 0;
    });
    const auto& seg =
        first_non_empty_seg != incomplete_segments.end() ? first_non_empty_seg->segment(store) : incomplete_segments.begin()->segment(store);
    // Mark the start point of the incompletes, so we know that there is no column slicing after this point
    pipeline_context->incompletes_after_ = pipeline_context->slice_and_keys_.size();

    // If there are only incompletes we need to add the index here
    if(pipeline_context->slice_and_keys_.empty()) {
        add_index_columns_to_query(read_query, seg.index_descriptor());
    }
    pipeline_context->slice_and_keys_.insert(std::end(pipeline_context->slice_and_keys_), incomplete_segments.begin(), incomplete_segments.end());

    if (!pipeline_context->norm_meta_) {
        pipeline_context->norm_meta_ = std::make_unique<arcticdb::proto::descriptors::NormalizationMetadata>();
        pipeline_context->norm_meta_->CopyFrom(seg.index_descriptor().proto().normalization());
        ensure_timeseries_norm_meta(*pipeline_context->norm_meta_, pipeline_context->stream_id_, sparsify);
    }

    const StreamDescriptor &staged_desc = incomplete_segments[0].segment(store).descriptor();


    // We need to check that the index names match regardless of the dynamic schema setting
    // A more detailed check is done later in the do_compact function
    if (pipeline_context->desc_) {
        schema::check<ErrorCode::E_DESCRIPTOR_MISMATCH>(
            index_names_match(staged_desc, *pipeline_context->desc_),
            "The index names in the staged stream descriptor {} are not identical to that of the stream descriptor on storage {}",
            staged_desc,
            *pipeline_context->desc_
        );
    }

    if (dynamic_schema) {
        pipeline_context->staged_descriptor_ = merge_descriptors(seg.descriptor(), incomplete_segments, read_query.columns);
        if (pipeline_context->desc_) {
            const std::array fields_ptr = {pipeline_context->desc_->fields_ptr()};
            pipeline_context->desc_ = merge_descriptors(*pipeline_context->staged_descriptor_, fields_ptr, read_query.columns);
        } else {
            pipeline_context->desc_ = pipeline_context->staged_descriptor_;
        }
    } else {
        if (pipeline_context->desc_) {
            schema::check<ErrorCode::E_DESCRIPTOR_MISMATCH>(
                columns_match(staged_desc, *pipeline_context->desc_),
                "When static schema is used the staged stream descriptor {} must equal the stream descriptor on storage {}",
                staged_desc,
                *pipeline_context->desc_
            );
        }
        pipeline_context->staged_descriptor_ = staged_desc;
        pipeline_context->desc_ = staged_desc;
    }

    modify_descriptor(pipeline_context, read_options);
    if (convert_int_to_float) {
        stream::convert_descriptor_types(*pipeline_context->staged_descriptor_);
    }

    generate_filtered_field_descriptors(pipeline_context, read_query.columns);
    pipeline_context->total_rows_ = pipeline_context->calc_rows();
    return true;
}

void check_incompletes_index_ranges_dont_overlap(const std::shared_ptr<PipelineContext>& pipeline_context,
                                                 const std::optional<SortedValue>& previous_sorted_value) {
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
        // Beginning of incomplete segments == beginning of all segments implies all segments are incompletes, so we are
        // writing, not appending
        if (pipeline_context->incompletes_begin() != pipeline_context->begin()) {
            sorting::check<ErrorCode::E_UNSORTED_DATA>(
                    !previous_sorted_value.has_value() ||
                    *previous_sorted_value == SortedValue::ASCENDING ||
                    *previous_sorted_value == SortedValue::UNKNOWN,
                    "Cannot append staged segments to existing data as existing data is not sorted in ascending order");
            auto last_indexed_slice_and_key = std::prev(pipeline_context->incompletes_begin())->slice_and_key();
            // -1 as end_time is stored as 1 greater than the last index value in the segment
            last_existing_index_value = last_indexed_slice_and_key.key().end_time() - 1;
        }

        // Use ordered set so that we only need to compare adjacent elements
        std::set<TimestampRange> unique_timestamp_ranges;
        for (auto it = pipeline_context->incompletes_begin(); it!= pipeline_context->end(); it++) {
            if (it->slice_and_key().slice().rows().diff() == 0) {
                continue;
            }

            const auto& key = it->slice_and_key().key();
            sorting::check<ErrorCode::E_UNSORTED_DATA>(
                !last_existing_index_value.has_value() || key.start_time() >= *last_existing_index_value,
                "Cannot append staged segments to existing data as incomplete segment contains index value < existing data (in UTC): {} <= {}",
                date_and_time(key.start_time()),
                // Should never reach "" but the standard mandates that all function arguments are evaluated
                last_existing_index_value ? date_and_time(*last_existing_index_value) : ""
            );
            auto [_, inserted] = unique_timestamp_ranges.emplace(key.start_time(), key.end_time());
            // This is correct because incomplete segments aren't column sliced
            sorting::check<ErrorCode::E_UNSORTED_DATA>(
                    inserted,
                    "Cannot finalize staged data as 2 or more incomplete segments cover identical index values (in UTC): ({}, {})",
                    date_and_time(key.start_time()), date_and_time(key.end_time()));
        }

        for (auto it = unique_timestamp_ranges.begin(); it != unique_timestamp_ranges.end(); it++) {
            auto next_it = std::next(it);
            if (next_it != unique_timestamp_ranges.end()) {
                sorting::check<ErrorCode::E_UNSORTED_DATA>(
                        next_it->first >= it->second,
                        "Cannot finalize staged data as incomplete segment index values overlap one another (in UTC): ({}, {}) intersects ({}, {})",
                        date_and_time(it->first),
                        date_and_time(it->second - 1),
                        date_and_time(next_it->first),
                        date_and_time(next_it->second - 1));
            }
        }
    }
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
    const std::shared_ptr<Store>& store,
    const VersionedItem& versioned_item,
    ColumnStats& column_stats,
    const ReadOptions& read_options
) {
    using namespace arcticdb::pipelines;
    auto clause = column_stats.clause();
    if (!clause.has_value()) {
        log::version().warn("Cannot create empty column stats");
        return;
    }
    auto read_query = std::make_shared<ReadQuery>(std::vector<std::shared_ptr<Clause>>{std::make_shared<Clause>(std::move(*clause))});

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
    read_indexed_keys_to_pipeline(store, pipeline_context, versioned_item, *read_query, read_options);

    schema::check<ErrorCode::E_UNSUPPORTED_INDEX_TYPE>(
            !pipeline_context->multi_key_,
            "Column stats generation not supported with multi-indexed symbols"
            );
    schema::check<ErrorCode::E_OPERATION_NOT_SUPPORTED_WITH_PICKLED_DATA>(
            !pipeline_context->is_pickled(),
            "Cannot create column stats on pickled data"
            );

    auto segs = read_and_process(store, pipeline_context, read_query, read_options).get();
    schema::check<ErrorCode::E_COLUMN_DOESNT_EXIST>(!segs.empty(), "Cannot create column stats for nonexistent columns");

    // Convert SliceAndKey vector into SegmentInMemory vector
    std::vector<SegmentInMemory> segments_in_memory;
    for (auto& seg: segs)  {
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
                "Cannot create column stats, existing column stats row-groups do not match");
        old_segment->concatenate(std::move(new_segment));
        store->update(column_stats_key, std::move(*old_segment), update_opts).get();
    }
}

void drop_column_stats_impl(
    const std::shared_ptr<Store>& store,
    const VersionedItem& versioned_item,
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
            for (const auto& field: old_fields) {
                auto column_name = field.name();
                if (!columns_to_keep.contains(std::string{column_name}) && column_name != start_index_column_name && column_name != end_index_column_name) {
                    segment_in_memory.drop_column(column_name);
                }
            }
            storage::UpdateOpts update_opts;
            update_opts.upsert_ = true;
            store->update(column_stats_key, std::move(segment_in_memory), update_opts).get();
        }
    }
}

FrameAndDescriptor read_column_stats_impl(
    const std::shared_ptr<Store>& store,
    const VersionedItem& versioned_item) {
    auto column_stats_key = index_key_to_column_stats_key(versioned_item.key_);
    // Remove try-catch once AsyncStore methods raise the new error codes themselves
    try {
        auto segment_in_memory = store->read(column_stats_key).get().second;
        TimeseriesDescriptor tsd;
        tsd.set_total_rows(segment_in_memory.row_count());
        tsd.set_stream_descriptor(segment_in_memory.descriptor());
        return {SegmentInMemory(std::move(segment_in_memory)), tsd, {}, {}};
    } catch (const std::exception& e) {
        storage::raise<ErrorCode::E_KEY_NOT_FOUND>("Failed to read column stats key: {}", e.what());
    }
}

ColumnStats get_column_stats_info_impl(
    const std::shared_ptr<Store>& store,
    const VersionedItem& versioned_item) {
    auto column_stats_key = index_key_to_column_stats_key(versioned_item.key_);
    // Remove try-catch once AsyncStore methods raise the new error codes themselves
    try {
        auto stream_descriptor = std::get<StreamDescriptor>(store->read_metadata_and_descriptor(column_stats_key).get());
        return ColumnStats(stream_descriptor.fields());
    } catch (const std::exception& e) {
        storage::raise<ErrorCode::E_KEY_NOT_FOUND>("Failed to read column stats key: {}", e.what());
    }
}

VersionedItem collate_and_write(
    const std::shared_ptr<Store>& store,
    const std::shared_ptr<PipelineContext>& pipeline_context,
    const std::vector<FrameSlice>& slices,
    std::vector<VariantKey> keys,
    size_t append_after,
    const std::optional<arcticdb::proto::descriptors::UserDefinedMetadata>& user_meta
    ) {
    util::check(keys.size() == slices.size(), "Mismatch between slices size and key size");
    TimeseriesDescriptor tsd;

    tsd.set_stream_descriptor(pipeline_context->descriptor());
    tsd.set_total_rows(pipeline_context->total_rows_);
    auto& tsd_proto = tsd.mutable_proto();
    tsd_proto.mutable_normalization()->CopyFrom(*pipeline_context->norm_meta_);
    if(user_meta)
        tsd_proto.mutable_user_meta()->CopyFrom(*user_meta);

    auto index = stream::index_type_from_descriptor(pipeline_context->descriptor());
    return util::variant_match(index, [&store, &pipeline_context, &slices, &keys, &append_after, &tsd] (auto idx) {
        using IndexType = decltype(idx);
        index::IndexWriter<IndexType> writer(store, IndexPartialKey{pipeline_context->stream_id_, pipeline_context->version_id_}, std::move(tsd));
        auto end = std::begin(pipeline_context->slice_and_keys_);
        std::advance(end, append_after);
        ARCTICDB_DEBUG(log::version(), "Adding {} existing keys and {} new keys: ", std::distance(std::begin(pipeline_context->slice_and_keys_), end), keys.size());
        for(auto sk = std::begin(pipeline_context->slice_and_keys_); sk < end; ++sk)
            writer.add(sk->key(), sk->slice());

        for (auto key : folly::enumerate(keys)) {
            writer.add(to_atom(*key), slices[key.index]);
        }
        auto index_key =  writer.commit();
        return VersionedItem{to_atom(std::move(index_key).get())};
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
    store.remove_keys(keys_to_delete).get();
}

DeleteIncompleteKeysOnExit::DeleteIncompleteKeysOnExit(
        std::shared_ptr<PipelineContext> pipeline_context,
        std::shared_ptr<Store> store,
        bool via_iteration)
            : context_(std::move(pipeline_context)),
              store_(std::move(store)),
              via_iteration_(via_iteration) {
    }

DeleteIncompleteKeysOnExit::~DeleteIncompleteKeysOnExit() {
    if(released_)
        return;

    try {
        if (context_->incompletes_after_) {
            delete_incomplete_keys(*context_, *store_);
        } else {
            // If an exception is thrown before read_incompletes_to_pipeline the keys won't be placed inside the
            // context thus they must be read manually.
            auto entries = read_incomplete_keys_for_symbol(store_, context_->stream_id_, via_iteration_);
            store_->remove_keys(entries).get();
        }
    } catch (const std::exception& e) {
        // Don't emit exceptions from destructor
        log::storage().error("Failed to delete staged segments: {}", e.what());
    }
}

std::optional<DeleteIncompleteKeysOnExit> get_delete_keys_on_failure(
    const std::shared_ptr<PipelineContext>& pipeline_context,
    const std::shared_ptr<Store>& store,
    const CompactIncompleteOptions& options) {
    if(options.delete_staged_data_on_failure_)
        return std::make_optional<DeleteIncompleteKeysOnExit>(pipeline_context, store, options.via_iteration_);
    else
        return std::nullopt;
}

VersionedItem sort_merge_impl(
    const std::shared_ptr<Store>& store,
    const StreamId& stream_id,
    const std::optional<arcticdb::proto::descriptors::UserDefinedMetadata>& norm_meta,
    const UpdateInfo& update_info,
    const CompactIncompleteOptions& options,
    const WriteOptions& write_options,
    std::shared_ptr<PipelineContext>& pipeline_context) {
    auto read_query = std::make_shared<ReadQuery>();

    std::optional<SortedValue> previous_sorted_value;
    if(options.append_ && update_info.previous_index_key_.has_value()) {
        read_indexed_keys_to_pipeline(store, pipeline_context, *(update_info.previous_index_key_), *read_query, ReadOptions{});
        if (!write_options.dynamic_schema) {
            user_input::check<ErrorCode::E_INVALID_USER_ARGUMENT>(
                pipeline_context->slice_and_keys_.front().slice().columns() == pipeline_context->slice_and_keys_.back().slice().columns(),
                "Appending using sort and finalize is not supported when existing data being appended to is column sliced."
            );
        }
        previous_sorted_value.emplace(pipeline_context->desc_->sorted());
    }
    const auto num_versioned_rows = pipeline_context->total_rows_;

    const bool has_incomplete_segments = read_incompletes_to_pipeline(
        store,
        pipeline_context,
        *read_query,
        ReadOptions{},
        options.convert_int_to_float_,
        options.via_iteration_,
        options.sparsify_,
        write_options.dynamic_schema
    );
    user_input::check<ErrorCode::E_NO_STAGED_SEGMENTS>(
        has_incomplete_segments,
        "Finalizing staged data is not allowed with empty staging area"
    );

    std::vector<FrameSlice> slices;
    std::vector<folly::Future<VariantKey>> fut_vec;
    auto index = stream::index_type_from_descriptor(pipeline_context->descriptor());
    util::variant_match(index,
        [&](const stream::TimeseriesIndex &timeseries_index) {
            read_query->clauses_.emplace_back(std::make_shared<Clause>(SortClause{timeseries_index.name(), pipeline_context->incompletes_after()}));
            read_query->clauses_.emplace_back(std::make_shared<Clause>(RemoveColumnPartitioningClause{}));

            read_query->clauses_.emplace_back(std::make_shared<Clause>(MergeClause{
                timeseries_index,
                SparseColumnPolicy{},
                stream_id,
                pipeline_context->descriptor(),
                write_options.dynamic_schema
            }));
            ReadOptions read_options;
            read_options.dynamic_schema_ = write_options.dynamic_schema;
            auto segments = read_and_process(store, pipeline_context, read_query, read_options).get();
            if (options.append_ && update_info.previous_index_key_ && !segments.empty()) {
                const timestamp last_index_on_disc = update_info.previous_index_key_->end_time() - 1;
                const timestamp incomplete_start =
                    std::get<timestamp>(TimeseriesIndex::start_value_for_segment(segments[0].segment(store)));
                sorting::check<ErrorCode::E_UNSORTED_DATA>(
                    last_index_on_disc <= incomplete_start,
                    "Cannot append staged segments to existing data as incomplete segment contains index value {} < existing data {}",
                    date_and_time(incomplete_start),
                    date_and_time(last_index_on_disc)
                );
            }
            pipeline_context->total_rows_ = num_versioned_rows + get_slice_rowcounts(segments);

            auto index = index_type_from_descriptor(pipeline_context->descriptor());
            stream::SegmentAggregator<TimeseriesIndex, DynamicSchema, RowCountSegmentPolicy, SparseColumnPolicy>
            aggregator{
                [&slices](FrameSlice &&slice) {
                    slices.emplace_back(std::move(slice));
                },
                DynamicSchema{*pipeline_context->staged_descriptor_, index},
                [pipeline_context, &fut_vec, &store](SegmentInMemory &&segment) {
                    StreamDescriptor only_non_empty_cols;
                    const auto local_index_start = TimeseriesIndex::start_value_for_segment(segment);
                    const auto local_index_end = TimeseriesIndex::end_value_for_segment(segment);
                    stream::StreamSink::PartialKey
                    pk{KeyType::TABLE_DATA, pipeline_context->version_id_, pipeline_context->stream_id_, local_index_start, local_index_end};
                    fut_vec.emplace_back(store->write(pk, std::move(segment)));
                }};

            for(auto& sk : segments) {
                SegmentInMemory segment = sk.release_segment(store);
                // Empty columns can appear only of one staged segment is empty and adds column which
                // does not appear in any other segment. There can also be empty columns if all segments
                // are empty in that case this loop won't be reached as segments.size() will be 0
                if (write_options.dynamic_schema) {
                    segment.drop_empty_columns();
                }

                aggregator.add_segment(std::move(segment), sk.slice(), options.convert_int_to_float_);
            }
            aggregator.commit();
            pipeline_context->desc_->set_sorted(deduce_sorted(previous_sorted_value.value_or(SortedValue::ASCENDING), SortedValue::ASCENDING));
        },
        [&](const auto &) {
            util::raise_rte("Sort merge only supports datetime indexed data. You data does not have a datetime index.");
        }
        );

    auto keys = folly::collect(fut_vec).get();
    auto vit = collate_and_write(
        store,
        pipeline_context,
        slices,
        keys,
        pipeline_context->incompletes_after(),
        norm_meta);
    return vit;
}

VersionedItem compact_incomplete_impl(
    const std::shared_ptr<Store>& store,
    const StreamId& stream_id,
    const std::optional<arcticdb::proto::descriptors::UserDefinedMetadata>& user_meta,
    const UpdateInfo& update_info,
    const CompactIncompleteOptions& options,
    const WriteOptions& write_options,
    std::shared_ptr<PipelineContext>& pipeline_context) {

    ReadQuery read_query;
    ReadOptions read_options;
    read_options.set_dynamic_schema(true);
    std::optional<SegmentInMemory> last_indexed;
    std::optional<SortedValue> previous_sorted_value;

    if(options.append_ && update_info.previous_index_key_.has_value()) {
        read_indexed_keys_to_pipeline(store, pipeline_context, *(update_info.previous_index_key_), read_query, read_options);
        if (!write_options.dynamic_schema) {
            user_input::check<ErrorCode::E_INVALID_USER_ARGUMENT>(
                pipeline_context->slice_and_keys_.front().slice().columns() == pipeline_context->slice_and_keys_.back().slice().columns(),
                "Appending using sort and finalize is not supported when existing data being appended to is column sliced."
            );
        }
        previous_sorted_value.emplace(pipeline_context->desc_->sorted());
    }

    const bool has_incomplete_segments = read_incompletes_to_pipeline(
        store,
        pipeline_context,
        read_query,
        ReadOptions{},
        options.convert_int_to_float_,
        options.via_iteration_,
        options.sparsify_,
        write_options.dynamic_schema
    );
    user_input::check<ErrorCode::E_NO_STAGED_SEGMENTS>(
        has_incomplete_segments,
        "Finalizing staged data is not allowed with empty staging area"
    );
    if (options.validate_index_) {
        check_incompletes_index_ranges_dont_overlap(pipeline_context, previous_sorted_value);
    }
    const auto& first_seg = pipeline_context->slice_and_keys_.begin()->segment(store);

    std::vector<FrameSlice> slices;
    bool dynamic_schema = write_options.dynamic_schema;
    const auto index = index_type_from_descriptor(first_seg.descriptor());
    auto policies = std::make_tuple(
        index,
        dynamic_schema ? VariantSchema{DynamicSchema::default_schema(index, stream_id)} : VariantSchema{FixedSchema::default_schema(index, stream_id)},
        options.sparsify_ ? VariantColumnPolicy{SparseColumnPolicy{}} : VariantColumnPolicy{DenseColumnPolicy{}}
        );

    CompactionResult result = util::variant_match(std::move(policies), [
        &slices, pipeline_context=pipeline_context, &store, &options, &previous_sorted_value, &write_options] (auto &&idx, auto &&schema, auto &&column_policy) {
        using IndexType = std::remove_reference_t<decltype(idx)>;
        using SchemaType = std::remove_reference_t<decltype(schema)>;
        using ColumnPolicyType = std::remove_reference_t<decltype(column_policy)>;
        constexpr bool validate_index_sorted = IndexType::type() == IndexDescriptorImpl::Type::TIMESTAMP;

        CompactionResult result = do_compact<IndexType, SchemaType, RowCountSegmentPolicy, ColumnPolicyType>(
                pipeline_context->incompletes_begin(),
                pipeline_context->end(),
                pipeline_context,
                slices,
                store,
                options.convert_int_to_float_,
                write_options.segment_row_size,
                validate_index_sorted,
                check_schema_matches_incomplete);
        if constexpr(std::is_same_v<IndexType, TimeseriesIndex>) {
            pipeline_context->desc_->set_sorted(deduce_sorted(previous_sorted_value.value_or(SortedValue::ASCENDING), SortedValue::ASCENDING));
        }

        return result;
    });

    return util::variant_match(std::move(result),
                        [&slices, &pipeline_context, &store, &user_meta](CompactionWrittenKeys& written_keys) -> VersionedItem {
                            auto vit = collate_and_write(
                                store,
                                pipeline_context,
                                slices,
                                std::move(written_keys),
                                pipeline_context->incompletes_after(),
                                user_meta);
                            return vit;
                        },
                        [](Error& error) -> VersionedItem {
                            error.throw_error();
                            return VersionedItem{}; // unreachable
                        }
                    );
}

PredefragmentationInfo get_pre_defragmentation_info(
        const std::shared_ptr<Store>& store,
        const StreamId& stream_id,
        const UpdateInfo& update_info,
        const WriteOptions& options,
        size_t segment_size) {
    util::check(update_info.previous_index_key_.has_value(), "No latest undeleted version found for data compaction");

    auto pipeline_context = std::make_shared<PipelineContext>();
    pipeline_context->stream_id_ = stream_id;
    pipeline_context->version_id_ = update_info.next_version_id_;

    auto read_query = std::make_shared<ReadQuery>();
    read_indexed_keys_to_pipeline(store, pipeline_context, *(update_info.previous_index_key_), *read_query, defragmentation_read_options_generator(options));

    using CompactionStartInfo = std::pair<size_t, size_t>;//row, segment_append_after
    std::vector<CompactionStartInfo> first_col_segment_idx;
    const auto& slice_and_keys = pipeline_context->slice_and_keys_;
    first_col_segment_idx.reserve(slice_and_keys.size());
    std::optional<CompactionStartInfo> compaction_start_info;
    size_t segment_idx = 0, num_to_segments_after_compact = 0, new_segment_row_size = 0;
    for(const auto & slice_and_key : slice_and_keys) {
        auto &slice = slice_and_key.slice();

        if (slice.row_range.diff() < segment_size && !compaction_start_info)
            compaction_start_info = {slice.row_range.start(), segment_idx};
            
        if (slice.col_range.start() == pipeline_context->descriptor().index().field_count()){//where data column starts
            first_col_segment_idx.emplace_back(slice.row_range.start(), segment_idx);
            if (new_segment_row_size == 0)
                ++num_to_segments_after_compact;
            new_segment_row_size += slice.row_range.diff();
            if (new_segment_row_size >= segment_size)
                new_segment_row_size = 0;
        }
        ++segment_idx;
        if (compaction_start_info && slice.row_range.start() < compaction_start_info->first){
            auto start_point = std::lower_bound(first_col_segment_idx.begin(), first_col_segment_idx.end(), slice.row_range.start(), [](auto lhs, auto rhs){return lhs.first < rhs;});
            if (start_point != first_col_segment_idx.end())
                compaction_start_info = *start_point;
            else {
                log::version().warn("Missing segment containing column 0 for row {}; Resetting compaction starting point to 0", slice.row_range.start());
                compaction_start_info = {0u, 0u};
            }
        }
    }
    return {pipeline_context, read_query, first_col_segment_idx.size() - num_to_segments_after_compact, compaction_start_info ? std::make_optional<size_t>(compaction_start_info->second) : std::nullopt};
}

bool is_symbol_fragmented_impl(size_t segments_need_compaction){
    return static_cast<int64_t>(segments_need_compaction) >= ConfigsMap::instance()->get_int("SymbolDataCompact.SegmentCount", 100);
}

VersionedItem defragment_symbol_data_impl(
        const std::shared_ptr<Store>& store,
        const StreamId& stream_id,
        const UpdateInfo& update_info,
        const WriteOptions& options,
        size_t segment_size) {
    auto pre_defragmentation_info = get_pre_defragmentation_info(store, stream_id, update_info, options, segment_size);
    util::check(is_symbol_fragmented_impl(pre_defragmentation_info.segments_need_compaction) && pre_defragmentation_info.append_after.has_value(), "Nothing to compact in defragment_symbol_data");

    // in the new index segment, we will start appending after this value
    std::vector<FrameSlice> slices;
    const auto index = index_type_from_descriptor(pre_defragmentation_info.pipeline_context->descriptor());
    auto policies = std::make_tuple(
        index,
        options.dynamic_schema ? VariantSchema{DynamicSchema::default_schema(index, stream_id)} : VariantSchema{FixedSchema::default_schema(index, stream_id)}
        );

    CompactionResult result = util::variant_match(std::move(policies), [
        &slices, &store, &options, &pre_defragmentation_info, segment_size=segment_size] (auto &&idx, auto &&schema) {
        pre_defragmentation_info.read_query->clauses_.emplace_back(std::make_shared<Clause>(RemoveColumnPartitioningClause{pre_defragmentation_info.append_after.value()}));
        auto segments = read_and_process(store, pre_defragmentation_info.pipeline_context, pre_defragmentation_info.read_query, defragmentation_read_options_generator(options)).get();
        using IndexType = std::remove_reference_t<decltype(idx)>;
        using SchemaType = std::remove_reference_t<decltype(schema)>;

        StaticSchemaCompactionChecks checks = [](const StreamDescriptor&, const StreamDescriptor&) {
            // No defrag specific checks yet
            return std::monostate{};
        };

        return do_compact<IndexType, SchemaType, RowCountSegmentPolicy, DenseColumnPolicy>(
            segments.begin(),
            segments.end(),
            pre_defragmentation_info.pipeline_context,
            slices,
            store,
            false,
            segment_size,
            false,
            std::move(checks));
    });

    return util::variant_match(std::move(result),
                               [&slices, &pre_defragmentation_info, &store](CompactionWrittenKeys& written_keys) -> VersionedItem {
                                return collate_and_write(
                                        store,
                                        pre_defragmentation_info.pipeline_context,
                                        slices,
                                        std::move(written_keys),
                                        pre_defragmentation_info.append_after.value(),
                                        std::nullopt);
                               },
                               [](Error& error) -> VersionedItem {
                                   error.throw_error();
                                   return VersionedItem{}; // unreachable
                               }
    );
}

void set_row_id_if_index_only(
        const PipelineContext& pipeline_context,
        SegmentInMemory& frame,
        const ReadQuery& read_query) {
    if (read_query.columns &&
        read_query.columns->empty() &&
        pipeline_context.descriptor().index().type() == IndexDescriptor::Type::ROWCOUNT) {
        frame.set_row_id(static_cast<ssize_t>(pipeline_context.rows_ - 1));
    }
}

// This is the main user-facing read method that either returns all or
// part of a dataframe as-is, or transforms it via a processing pipeline
folly::Future<ReadVersionOutput> read_frame_for_version(
        const std::shared_ptr<Store>& store,
        const std::variant<VersionedItem, StreamId>& version_info,
        const std::shared_ptr<ReadQuery>& read_query,
        const ReadOptions& read_options,
        std::shared_ptr<std::any>& handler_data) {
    using namespace arcticdb::pipelines;
    auto pipeline_context = std::make_shared<PipelineContext>();
    VersionedItem res_versioned_item;

    if(std::holds_alternative<StreamId>(version_info)) {
        pipeline_context->stream_id_ = std::get<StreamId>(version_info);
        // This isn't ideal. It would be better if the version() and timestamp() methods on the C++ VersionedItem class
        // returned optionals, but this change would bubble up to the Python VersionedItem class defined in _store.py.
        // This class is very hard to change at this point, as users do things like pickling them to pass them around.
        // This at least gets the symbol attribute of VersionedItem correct. The creation timestamp will be zero, which
        // corresponds to 1970, and so with this obviously ridiculous version ID, it should be clear to users that these
        // values are meaningless before an indexed version exists.
        res_versioned_item = VersionedItem(AtomKeyBuilder()
                                           .version_id(std::numeric_limits<VersionId>::max())
                                           .build<KeyType::TABLE_INDEX>(std::get<StreamId>(version_info)));
    } else {
        pipeline_context->stream_id_ = std::get<VersionedItem>(version_info).key_.id();
        read_indexed_keys_to_pipeline(store, pipeline_context, std::get<VersionedItem>(version_info), *read_query, read_options);
        res_versioned_item = std::get<VersionedItem>(version_info);
    }

    if(pipeline_context->multi_key_) {
        check_multi_key_is_not_index_only(*pipeline_context, *read_query);
        return read_multi_key(store, *pipeline_context->multi_key_, handler_data);
    }

    if(opt_false(read_options.incompletes_)) {
        util::check(std::holds_alternative<IndexRange>(read_query->row_filter), "Streaming read requires date range filter");
        const auto& query_range = std::get<IndexRange>(read_query->row_filter);
        const auto existing_range = pipeline_context->index_range();
        if(!existing_range.specified_ || query_range.end_ > existing_range.end_)
            read_incompletes_to_pipeline(store, pipeline_context, *read_query, read_options, false, false, false,  opt_false(read_options.dynamic_schema_));
    }

    if(std::holds_alternative<StreamId>(version_info) && !pipeline_context->incompletes_after_) {
        missing_data::raise<ErrorCode::E_NO_SYMBOL_DATA>(
            "read_dataframe_impl: read returned no data for symbol {} (found no versions or append data)", pipeline_context->stream_id_);
    }

    modify_descriptor(pipeline_context, read_options);
    generate_filtered_field_descriptors(pipeline_context, read_query->columns);
    ARCTICDB_DEBUG(log::version(), "Fetching data to frame");

    DecodePathData shared_data;
    return do_direct_read_or_process(store, read_query, read_options, pipeline_context, shared_data, handler_data)
    .thenValue([res_versioned_item, pipeline_context, &read_options, &handler_data, read_query, shared_data](auto&& frame) mutable {
        ARCTICDB_DEBUG(log::version(), "Reduce and fix columns");
        return reduce_and_fix_columns(pipeline_context, frame, read_options, handler_data)
        .via(&async::cpu_executor())
        .thenValue([res_versioned_item, pipeline_context, frame, read_query, shared_data](auto&&) mutable {
            set_row_id_if_index_only(*pipeline_context, frame, *read_query);
            return ReadVersionOutput{std::move(res_versioned_item),
                                     {frame,
                                      timeseries_descriptor_from_pipeline_context(pipeline_context, {}, pipeline_context->bucketize_dynamic_),
                                      {},
                                      shared_data.buffers()}};
        });
    });
}
} //namespace arcticdb::version_store

namespace arcticdb {

Error::Error(folly::Function<void(std::string)> raiser, std::string msg)
    : raiser_(std::move(raiser)), msg_(std::move(msg)) {

}

void Error::throw_error() {
    raiser_(msg_);
}

void remove_written_keys(Store* const store, CompactionWrittenKeys&& written_keys) {
    log::version().debug("Error during compaction, removing {} keys written before failure", written_keys.size());
    store->remove_keys_sync(std::move(written_keys));
}

bool is_segment_unsorted(const SegmentInMemory& segment) {
    return segment.descriptor().sorted() == SortedValue::DESCENDING || segment.descriptor().sorted() == SortedValue::UNSORTED;
}

}
