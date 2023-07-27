/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/version/version_core.hpp>
#include <arcticdb/pipeline/write_options.hpp>
#include <arcticdb/stream/index.hpp>
#include <arcticdb/pipeline/query.hpp>
#include <arcticdb/pipeline/read_pipeline.hpp>
#include <arcticdb/async/task_scheduler.hpp>
#include <arcticdb/async/tasks.hpp>
#include <arcticdb/util/key_utils.hpp>
#include <arcticdb/util/optional_defaults.hpp>
#include <arcticdb/stream/append_map.hpp>
#include <arcticdb/pipeline/pipeline_context.hpp>
#include <arcticdb/pipeline/read_frame.hpp>
#include <arcticdb/pipeline/read_options.hpp>
#include <arcticdb/stream/stream_sink.hpp>
#include <arcticdb/stream/stream_writer.hpp>
#include <arcticdb/entity/type_utils.hpp>
#include <arcticdb/stream/schema.hpp>
#include <arcticdb/pipeline/index_writer.hpp>
#include <arcticdb/entity/metrics.hpp>
#include <arcticdb/pipeline/index_utils.hpp>
#include <arcticdb/util/composite.hpp>
#include <arcticdb/pipeline/column_mapping.hpp>
#include <arcticdb/version/schema_checks.hpp>
#include <arcticdb/version/version_utils.hpp>
#include <arcticdb/entity/merge_descriptors.hpp>

namespace arcticdb::version_store {

void modify_descriptor(const std::shared_ptr<pipelines::PipelineContext>& pipeline_context, const ReadOptions& read_options) {

    if (opt_false(read_options.force_strings_to_object_) || opt_false(read_options.force_strings_to_fixed_))
        pipeline_context->orig_desc_ = pipeline_context->desc_;

    auto& desc = *pipeline_context->desc_;
    if (opt_false(read_options.force_strings_to_object_)) {
        auto& fields = desc.fields();
        std::for_each(
            std::begin(fields),
            std::end(fields),
            [](auto& field_desc) {
                if (field_desc.type().data_type() == DataType::ASCII_FIXED64)
                    set_data_type(DataType::ASCII_DYNAMIC64, field_desc.mutable_type());

                if (field_desc.type().data_type() == DataType::UTF_FIXED64)
                    set_data_type(DataType::UTF_DYNAMIC64, field_desc.mutable_type());
            });
    }
    else if (opt_false(read_options.force_strings_to_fixed_)) {
        auto& fields = desc.fields();
        std::for_each(
            std::begin(fields),
            std::end(fields),
            [](auto& field_desc) {
                if (field_desc.type().data_type() == DataType::ASCII_DYNAMIC64)
                    set_data_type(DataType::ASCII_FIXED64, field_desc.mutable_type());

                if (field_desc.type().data_type() == DataType::UTF_DYNAMIC64)
                    set_data_type(DataType::UTF_FIXED64, field_desc.mutable_type());
            });
    }
}

VersionedItem write_dataframe_impl(
    const std::shared_ptr<Store>& store,
    VersionId version_id,
    pipelines::InputTensorFrame&& frame,
    const WriteOptions& options,
    const std::shared_ptr<DeDupMap>& de_dup_map,
    bool sparsify_floats,
    bool validate_index
    ) {
    ARCTICDB_SUBSAMPLE_DEFAULT(WaitForWriteCompletion)
    ARCTICDB_DEBUG(log::version(), "write_dataframe_impl stream_id: {} , version_id: {}, {} rows", frame.desc.id(), version_id, frame.num_rows);
    auto atom_key_fut = async_write_dataframe_impl(store, version_id, std::move(frame), options, de_dup_map, sparsify_floats, validate_index);
    return VersionedItem(std::move(atom_key_fut).get());
}

folly::Future<entity::AtomKey> async_write_dataframe_impl(
    const std::shared_ptr<Store>& store,
    VersionId version_id,
    InputTensorFrame&& frame,
    const WriteOptions& options,
    const std::shared_ptr<DeDupMap> &de_dup_map,
    bool sparsify_floats,
    bool validate_index
    ) {
    ARCTICDB_SAMPLE(DoWrite, 0)

    // Slice the frame according to the write options
    frame.set_bucketize_dynamic(options.bucketize_dynamic);
    auto slicing_arg = get_slicing_policy(options, frame);
    auto partial_key = IndexPartialKey{frame.desc.id(), version_id};
    sorting::check<ErrorCode::E_UNSORTED_DATA>(!validate_index || frame.desc.get_sorted() == SortedValue::ASCENDING || !std::holds_alternative<stream::TimeseriesIndex>(frame.index),
                "When calling write with validate_index enabled, input data must be sorted.");
    return write_frame(std::move(partial_key), std::move(frame), slicing_arg, store, de_dup_map, sparsify_floats);
}

namespace {
IndexDescriptor::Proto check_index_match(const arcticdb::stream::Index& index, const IndexDescriptor::Proto& desc) {
    if (std::holds_alternative<stream::TimeseriesIndex>(index))
        util::check(desc.kind() == IndexDescriptor::TIMESTAMP,
                    "Index mismatch, cannot update a non-timeseries-indexed frame with a timeseries");
    else
        util::check(desc.kind() == IndexDescriptor::ROWCOUNT,
                    "Index mismatch, cannot update a timeseries with a non-timeseries-indexed frame");

    return desc;
}
}

void sorted_data_check_append(InputTensorFrame& frame, index::IndexSegmentReader& index_segment_reader, bool validate_index){
    bool is_time_series = std::holds_alternative<stream::TimeseriesIndex>(frame.index);
    bool input_data_is_sorted = frame.desc.get_sorted() == SortedValue::ASCENDING;
    sorting::check<ErrorCode::E_UNSORTED_DATA>(
        input_data_is_sorted || !validate_index || !is_time_series,
        "validate_index set but input data index is not sorted");

    bool existing_data_is_sorted = index_segment_reader.mutable_tsd().mutable_proto().stream_descriptor().sorted() == arcticdb::proto::descriptors::SortedValue::ASCENDING;
    sorting::check<ErrorCode::E_UNSORTED_DATA>(
        existing_data_is_sorted || !validate_index || !is_time_series,
        "validate_index set but existing data index is not sorted");
}

folly::Future<AtomKey> async_append_impl(
    const std::shared_ptr<Store>& store,
    const UpdateInfo& update_info,
    InputTensorFrame&& frame,
    const WriteOptions& options,
    bool validate_index) {

    util::check(update_info.previous_index_key_.has_value(), "Cannot append as there is no previous index key to append to");
    const StreamId stream_id = frame.desc.id();
    ARCTICDB_DEBUG(log::version(), "append stream_id: {} , version_id: {}", stream_id, update_info.next_version_id_);
    auto index_segment_reader = index::get_index_reader(*(update_info.previous_index_key_), store);
    bool bucketize_dynamic = index_segment_reader.bucketize_dynamic();
    auto row_offset = index_segment_reader.tsd().proto().total_rows();
    util::check_rte(!index_segment_reader.is_pickled(), "Cannot append to pickled data");
    sorted_data_check_append(frame, index_segment_reader, validate_index);
    frame.set_offset(static_cast<ssize_t>(row_offset));
    fix_descriptor_mismatch_or_throw(APPEND, options.dynamic_schema, index_segment_reader, frame);

    frame.set_bucketize_dynamic(bucketize_dynamic);
    auto slicing_arg = get_slicing_policy(options, frame);
    return append_frame(IndexPartialKey{stream_id, update_info.next_version_id_}, std::move(frame), slicing_arg, index_segment_reader, store, options.dynamic_schema, options.ignore_sort_order);
}

VersionedItem append_impl(
    const std::shared_ptr<Store>& store,
    const UpdateInfo& update_info,
    InputTensorFrame&& frame,
    const WriteOptions& options,
    bool validate_index) {

    ARCTICDB_SUBSAMPLE_DEFAULT(WaitForWriteCompletion)
    auto version_key_fut = async_append_impl(store,
                                             update_info,
                                             std::move(frame),
                                             options,
                                             validate_index);
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

template <class KeyContainer>
    void ensure_keys_line_up(const KeyContainer& slice_and_keys) {
    std::optional<size_t> start;
    std::optional<size_t> end;
    SliceAndKey prev{};
    for(const auto& sk : slice_and_keys) {
        util::check(!start || sk.slice_.row_range.first == end.value(),
                    "Can't update as there is a sorting mismatch at key {} relative to previous key {} - expected index {} got {}",
                    sk, prev, end.value(), start.value());

        start = sk.slice_.row_range.first;
        end = sk.slice_.row_range.second;
        prev = sk;
    }
}

inline std::pair<std::vector<SliceAndKey>, std::vector<SliceAndKey>> intersecting_segments(
    const std::vector<SliceAndKey>& affected_keys,
    const IndexRange& front_range,
    const IndexRange& back_range,
    VersionId version_id,
    const std::shared_ptr<Store>& store) {
    std::vector<SliceAndKey> intersect_before;
    std::vector<SliceAndKey> intersect_after;

    for (const auto& affected_slice_and_key : affected_keys) {
        const auto& affected_range = affected_slice_and_key.key().index_range();
        if (intersects(affected_range, front_range) && !overlaps(affected_range, front_range)
        && is_before(affected_range, front_range)) {
            auto front_overlap_key = rewrite_partial_segment(affected_slice_and_key, front_range, version_id, false, store);
            if (front_overlap_key)
                intersect_before.push_back(front_overlap_key.value());
        }

        if (intersects(affected_range, back_range) && !overlaps(affected_range, back_range)
        && is_after(affected_range, back_range)) {
            auto back_overlap_key = rewrite_partial_segment(affected_slice_and_key, back_range, version_id, true, store);
            if (back_overlap_key)
                intersect_after.push_back(back_overlap_key.value());
        }
    }
    return std::make_pair(std::move(intersect_before), std::move(intersect_after));
}

} // namespace

VersionedItem delete_range_impl(
    const std::shared_ptr<Store>& store,
    const AtomKey& prev,
    const UpdateQuery& query,
    const WriteOptions&& ,
    bool dynamic_schema) {

    const StreamId& stream_id = prev.id();
    auto version_id = get_next_version_from_key(prev);
    util::check(std::holds_alternative<IndexRange>(query.row_filter), "Delete range requires index range argument");
    const auto& index_range = std::get<IndexRange>(query.row_filter);
    ARCTICDB_DEBUG(log::version(), "Delete range in versioned dataframe for stream_id: {} , version_id = {}", stream_id, version_id);

    auto index_segment_reader = index::get_index_reader(prev, store);
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

    auto [intersect_before, intersect_after] = intersecting_segments(affected_keys, index_range, index_range, version_id, store);

    auto orig_filter_range = std::holds_alternative<std::monostate>(query.row_filter) ? get_query_index_range(index, index_range) : query.row_filter;

    size_t row_count = 0;
    auto flattened_slice_and_keys = flatten_and_fix_rows(std::vector<std::vector<SliceAndKey>>{
        strictly_before(orig_filter_range, unaffected_keys),
        std::move(intersect_before),
        std::move(intersect_after),
        strictly_after(orig_filter_range, unaffected_keys)},
                                                         row_count
                                                         );

    std::sort(std::begin(flattened_slice_and_keys), std::end(flattened_slice_and_keys));
    bool bucketize_dynamic = index_segment_reader.bucketize_dynamic();
    auto time_series = timseries_descriptor_from_index_segment(row_count, std::move(index_segment_reader), std::nullopt, bucketize_dynamic);
    auto version_key_fut = util::variant_match(index, [&time_series, &flattened_slice_and_keys, &stream_id, &version_id, &store] (auto idx) {
        using IndexType = decltype(idx);
        return pipelines::index::write_index<IndexType>(std::move(time_series), std::move(flattened_slice_and_keys), IndexPartialKey{stream_id, version_id}, store);
    });
    auto version_key = std::move(version_key_fut).get();
    auto versioned_item = VersionedItem(to_atom(std::move(version_key)));
    ARCTICDB_DEBUG(log::version(), "updated stream_id: {} , version_id: {}", stream_id, version_id);
    return versioned_item;
}

void sorted_data_check_update(InputTensorFrame& frame, index::IndexSegmentReader& index_segment_reader){
    bool is_time_series = std::holds_alternative<stream::TimeseriesIndex>(frame.index);
    sorting::check<ErrorCode::E_UNSORTED_DATA>(
        is_time_series,
        "When calling update, the input data must be a time series.");
    bool input_data_is_sorted = frame.desc.get_sorted() == SortedValue::ASCENDING ||
                                frame.desc.get_sorted() == SortedValue::UNKNOWN;
    sorting::check<ErrorCode::E_UNSORTED_DATA>(
        input_data_is_sorted,
        "When calling update, the input data must be sorted.");
    bool existing_data_is_sorted = index_segment_reader.get_sorted() == SortedValue::ASCENDING ||
                                    index_segment_reader.get_sorted() == SortedValue::UNKNOWN;
    sorting::check<ErrorCode::E_UNSORTED_DATA>(
         existing_data_is_sorted,
        "When calling update, the existing data must be sorted.");
}

VersionedItem update_impl(
    const std::shared_ptr<Store>& store,
    const UpdateInfo& update_info,
    const UpdateQuery& query,
    InputTensorFrame&& frame,
    const WriteOptions&& options,
    bool dynamic_schema) {
    util::check(update_info.previous_index_key_.has_value(), "Cannot update as there is no previous index key to update into");
    const StreamId stream_id = frame.desc.id();
    ARCTICDB_DEBUG(log::version(), "Update versioned dataframe for stream_id: {} , version_id = {}", stream_id, update_info.previous_index_key_->version_id());
    auto index_segment_reader = index::get_index_reader(*(update_info.previous_index_key_), store);
    util::check_rte(!index_segment_reader.is_pickled(), "Cannot update pickled data");
    auto index_desc = check_index_match(frame.index, index_segment_reader.tsd().proto().stream_descriptor().index());
    util::check(index_desc.kind() == IndexDescriptor::TIMESTAMP, "Update not supported for non-timeseries indexes");
    sorted_data_check_update(frame, index_segment_reader);
    bool bucketize_dynamic = index_segment_reader.bucketize_dynamic();
    (void)check_and_mark_slices(index_segment_reader, dynamic_schema, false, std::nullopt, bucketize_dynamic);
    auto combined_sorting_info = deduce_sorted(frame.desc.get_sorted(), index_segment_reader.get_sorted());
    fix_descriptor_mismatch_or_throw(UPDATE, dynamic_schema, index_segment_reader, frame);

    std::vector<FilterQuery<index::IndexSegmentReader>> queries =
        build_update_query_filters<index::IndexSegmentReader>(query.row_filter, frame.index, frame.index_range, dynamic_schema, index_segment_reader.bucketize_dynamic());
    auto combined = combine_filter_functions(queries);
    auto affected_keys = filter_index(index_segment_reader, std::move(combined));
    std::vector<SliceAndKey> unaffected_keys;
    std::set_difference(std::begin(index_segment_reader),
                        std::end(index_segment_reader),
                        std::begin(affected_keys),
                        std::end(affected_keys),
                        std::back_inserter(unaffected_keys));

    frame.set_bucketize_dynamic(bucketize_dynamic);
    auto slicing_arg = get_slicing_policy(options, frame);

    auto new_slice_and_keys = slice_and_write(frame, slicing_arg, get_partial_key_gen(frame, IndexPartialKey{stream_id, update_info.next_version_id_}), store).wait().value();
    std::sort(std::begin(new_slice_and_keys), std::end(new_slice_and_keys));

    IndexRange orig_filter_range;
    auto[intersect_before, intersect_after] = util::variant_match(query.row_filter,
                        [&](std::monostate) {
                            util::check(std::holds_alternative<TimeseriesIndex>(frame.index), "Update with row count index is not permitted");
                            orig_filter_range = frame.index_range;
                            auto front_range = new_slice_and_keys.begin()->key().index_range();
                            auto back_range = new_slice_and_keys.rbegin()->key().index_range();
                            back_range.adjust_open_closed_interval();
                            return intersecting_segments(affected_keys, front_range, back_range, update_info.next_version_id_, store);
                        },
                        [&](const IndexRange& idx_range) {
                            orig_filter_range = idx_range;
                            return intersecting_segments(affected_keys, idx_range, idx_range, update_info.next_version_id_, store);
                        },
                        [](const RowRange&) {
                            util::raise_rte("Unexpected row_range in update query");
                            return std::pair(std::vector<SliceAndKey>{}, std::vector<SliceAndKey>{});  // unreachable
                        }
    );

    size_t row_count = 0;
    auto flattened_slice_and_keys = flatten_and_fix_rows(std::vector<std::vector<SliceAndKey>>{
        strictly_before(orig_filter_range, unaffected_keys),
        std::move(intersect_before),
        std::move(new_slice_and_keys),
        std::move(intersect_after),
        strictly_after(orig_filter_range, unaffected_keys)},
                                                         row_count
                                                         );

    std::sort(std::begin(flattened_slice_and_keys), std::end(flattened_slice_and_keys));
    auto existing_desc = index_segment_reader.tsd().as_stream_descriptor();
    auto desc = merge_descriptors(existing_desc, std::vector<std::shared_ptr<FieldCollection>>{ frame.desc.fields_ptr() }, {});
    desc.set_sorted(combined_sorting_info);
    auto time_series = make_timeseries_descriptor(row_count, std::move(desc), std::move(frame.norm_meta), std::move(frame.user_meta), std::nullopt, std::nullopt, bucketize_dynamic);
    auto index = index_type_from_descriptor(time_series.as_stream_descriptor());

    auto version_key_fut = util::variant_match(index, [&time_series, &flattened_slice_and_keys, &stream_id, &update_info, &store] (auto idx) {
        using IndexType = decltype(idx);
        return index::write_index<IndexType>(std::move(time_series), std::move(flattened_slice_and_keys), IndexPartialKey{stream_id, update_info.next_version_id_}, store);
    });
    auto version_key = std::move(version_key_fut).get();
    auto versioned_item = VersionedItem(to_atom(std::move(version_key)));
    ARCTICDB_DEBUG(log::version(), "updated stream_id: {} , version_id: {}", stream_id, update_info.next_version_id_);
    return versioned_item;
}

FrameAndDescriptor read_multi_key(
    const std::shared_ptr<Store>& store,
    const SegmentInMemory& index_key_seg) {
    const auto& multi_index_seg = index_key_seg;
    TimeseriesDescriptor tsd;
    multi_index_seg.metadata()->UnpackTo(&tsd.mutable_proto());
    std::vector<AtomKey> keys;
    for (size_t idx = 0; idx < index_key_seg.row_count(); idx++) {
        keys.push_back(stream::read_key_row(index_key_seg, static_cast<ssize_t>(idx)));
    }

    AtomKey dup{keys[0]};
    ReadQuery read_query;
    auto res = read_dataframe_impl(store, VersionedItem{std::move(dup)}, read_query, {});

    TimeseriesDescriptor multi_key_desc{tsd};
    multi_key_desc.mutable_proto().mutable_normalization()->CopyFrom(res.desc_.proto().normalization());
    return {res.frame_, multi_key_desc, keys, std::shared_ptr<BufferHolder>{}};
}

Composite<ProcessingSegment> process_remaining_clauses(
        const std::shared_ptr<Store>& store,
        std::vector<Composite<ProcessingSegment>>&& procs,
        std::vector<std::shared_ptr<Clause>> clauses ) { // pass by copy deliberately as we don't want to modify read_query
    while (!clauses.empty()) {
        if (clauses[0]->clause_info().requires_repartition_) {
            std::vector<Composite<ProcessingSegment>> repartitioned_procs = clauses[0]->repartition(std::move(procs)).value();
            // Erasing from front of vector not ideal, but they're just shared_ptr and there shouldn't be loads of clauses
            clauses.erase(clauses.begin());
            std::vector<folly::Future<Composite<ProcessingSegment>>> fut_procs;
            for (auto&& proc : repartitioned_procs) {
                fut_procs.emplace_back(
                        async::submit_cpu_task(
                                async::MemSegmentProcessingTask(store,
                                                                clauses,
                                                                std::move(proc))
                        )
                );
            }
            procs = folly::collect(fut_procs).get();
        } else {
            // Erasing from front of vector not ideal, but they're just shared_ptr and there shouldn't be loads of clauses
            clauses.erase(clauses.begin());
        }
    }
    return merge_composites(std::move(procs));
}

void set_output_descriptors(
        const Composite<ProcessingSegment>& merged_procs,
        const std::vector<std::shared_ptr<Clause>>& clauses,
        const std::shared_ptr<PipelineContext>& pipeline_context) {
    std::optional<std::string> index_column;
    for (auto clause = clauses.rbegin(); clause != clauses.rend(); ++clause) {
        if (auto new_index = (*clause)->clause_info().new_index_; new_index.has_value()) {
            index_column = new_index;
            pipeline_context->norm_meta_->mutable_df()->mutable_common()->mutable_index()->set_name(*new_index);
            pipeline_context->norm_meta_->mutable_df()->mutable_common()->mutable_index()->clear_fake_name();
            pipeline_context->norm_meta_->mutable_df()->mutable_common()->mutable_index()->set_is_not_range_index(
                    true);
            break;
        }
    }
    std::optional<StreamDescriptor> new_stream_descriptor;
    merged_procs.broadcast([&new_stream_descriptor](const auto& proc) {
        if (!new_stream_descriptor.has_value()) {
            if (proc.data_.size() > 0) {
                new_stream_descriptor = std::make_optional<StreamDescriptor>();
                new_stream_descriptor->set_index(proc.data_[0].segment_->descriptor().index());
                for (size_t idx = 0; idx < new_stream_descriptor->index().field_count(); idx++) {
                    new_stream_descriptor->add_field(proc.data_[0].segment_->descriptor().field(idx));
                }
            }
        }
        if (new_stream_descriptor.has_value()) {
            std::vector<std::shared_ptr<FieldCollection>> fields;
            for (const auto &slice_and_key: proc.data_) {
                fields.push_back(slice_and_key.segment_->descriptor().fields_ptr());
            }
            new_stream_descriptor = merge_descriptors(*new_stream_descriptor,
                                                      fields,
                                                      std::vector<std::string>{});
        }
    });
    if (new_stream_descriptor.has_value()) {
        // Columns might be in a different order to the original dataframe, so reorder here
        auto original_stream_descriptor = pipeline_context->descriptor();
        StreamDescriptor final_stream_descriptor{original_stream_descriptor.id()};
        final_stream_descriptor.set_index(new_stream_descriptor->index());
        // Erase field from new_stream_descriptor as we add them to final_stream_descriptor, as all fields left in new_stream_descriptor
        // after these operations were created by the processing pipeline, and so should be appended
        // Index columns should always appear first
        if (index_column.has_value()) {
            auto opt_idx = new_stream_descriptor->find_field(*index_column);
            internal::check<ErrorCode::E_ASSERTION_FAILURE>(opt_idx.has_value(), "New index column not found in processing pipeline");
            final_stream_descriptor.add_field(new_stream_descriptor->field(*opt_idx));
            new_stream_descriptor->erase_field(*opt_idx);
        }
        for (const auto& field: original_stream_descriptor.fields()) {
            if (auto position = new_stream_descriptor->find_field(field.name()); position.has_value()) {
                final_stream_descriptor.add_field(new_stream_descriptor->field(*position));

                new_stream_descriptor->erase_field(*position);
            }
        }
        for (const auto& field: new_stream_descriptor->fields()) {
            final_stream_descriptor.add_field(field);
        }
        pipeline_context->set_descriptor(final_stream_descriptor);
    }
}

/*
 * Processes the slices in the given pipeline_context.
 *
 * Slices are processed in order, with slices corresponding to the same row group collected into a single
 * Composite<SliceAndKey>. Slices contained within a single Composite<SliceAndKey> are processed within a single thread.
 *
 * The processing of a Composite<SliceAndKey> is scheduled via the Async Store. Within a single thread, the
 * segments will be retrieved from storage and decompressed before being passed to a MemSegmentProcessingTask which
 * will process all clauses up until a reducing clause.
 */
std::vector<SliceAndKey> read_and_process(
    const std::shared_ptr<Store>& store,
    const std::shared_ptr<PipelineContext>& pipeline_context,
    const ReadQuery& read_query,
    const ReadOptions& read_options,
    size_t start_from
    ) {
    std::sort(std::begin(pipeline_context->slice_and_keys_), std::end(pipeline_context->slice_and_keys_), [] (const SliceAndKey& left, const SliceAndKey& right) {
        return std::tie(left.slice().row_range.first, left.slice().col_range.first) < std::tie(right.slice().row_range.first, right.slice().col_range.first);
    });

    std::vector<Composite<SliceAndKey>> rows;
    auto sk_it = std::begin(pipeline_context->slice_and_keys_);
    std::advance(sk_it, start_from);
    while(sk_it != std::end(pipeline_context->slice_and_keys_)) {
        RowRange row_range{sk_it->slice().row_range};
        auto sk = Composite{std::move(*sk_it)};
        // Iterate through all SliceAndKeys that contain data for the same RowRange - i.e., iterate along column segments
        // for same row group
        while(++sk_it != std::end(pipeline_context->slice_and_keys_) && sk_it->slice().row_range == row_range) {
            sk.push_back(std::move(*sk_it));
        }

        util::check(!sk.empty(), "Should not push empty slice/key pairs to the pipeline");
        rows.emplace_back(std::move(sk));
    }
    std::shared_ptr<std::unordered_set<std::string>> filter_columns;
    if(pipeline_context->overall_column_bitset_) {
        filter_columns = std::make_shared<std::unordered_set<std::string>>();
        auto en = pipeline_context->overall_column_bitset_->first();
        auto en_end = pipeline_context->overall_column_bitset_->end();
        while (en < en_end) {
            filter_columns->insert(std::string(pipeline_context->desc_->field(*en++).name()));
        }
    }

    ProcessingConfig processing_config{opt_false(read_options.dynamic_schema_)};
    for (auto& clause: read_query.clauses_) {
        clause->set_processing_config(processing_config);
    }

    // At this stage, each Composite contains a single ProcessingSegment, which holds a row-slice
    // Different row-slices are held in different elements of the vector
    // All clauses that do not require repartitioning (e.g. filters and projections) will have already been applied
    // to these processing segments
    std::vector<Composite<ProcessingSegment>> procs = store->batch_read_uncompressed(
            std::move(rows),
            read_query.clauses_,
            filter_columns,
            BatchReadArgs{}
            );

    auto merged_procs = process_remaining_clauses(store, std::move(procs), read_query.clauses_);

    if (std::any_of(read_query.clauses_.begin(), read_query.clauses_.end(), [](const std::shared_ptr<Clause>& clause) {
        return clause->clause_info().modifies_output_descriptor_;
    })) {
        set_output_descriptors(merged_procs, read_query.clauses_, pipeline_context);
    }
    return collect_segments(std::move(merged_procs));
}

SegmentInMemory read_direct(const std::shared_ptr<Store>& store,
                            const std::shared_ptr<PipelineContext>& pipeline_context,
                            std::shared_ptr<BufferHolder> buffers,
                            const ReadOptions& read_options) {
    ARCTICDB_DEBUG(log::version(), "Allocating frame");
    ARCTICDB_SAMPLE_DEFAULT(ReadDirect)
    auto frame = allocate_frame(pipeline_context);
    util::print_total_mem_usage(__FILE__, __LINE__, __FUNCTION__);

    ARCTICDB_DEBUG(log::version(), "Fetching frame data");
    fetch_data(frame, pipeline_context, store, opt_false(read_options.dynamic_schema_), buffers).get();
    util::print_total_mem_usage(__FILE__, __LINE__, __FUNCTION__);
    return frame;
}

void add_index_columns_to_query(const ReadQuery& read_query, const TimeseriesDescriptor& desc) {
    if(!read_query.columns.empty()) {
        auto index_columns = stream::get_index_columns_from_descriptor(desc);
        if(index_columns.empty())
            return;

        std::vector<std::string> index_columns_to_add;
        for(const auto& index_column : index_columns) {
            if(std::find(std::begin(read_query.columns), std::end(read_query.columns), index_column) == std::end(read_query.columns))
                index_columns_to_add.push_back(index_column);
        }
        read_query.columns.insert(std::begin(read_query.columns), std::begin(index_columns_to_add), std::end(index_columns_to_add));
    }
}

FrameAndDescriptor read_index_impl(
    const std::shared_ptr<Store>& store,
    const VersionedItem& version) {
    auto fut_index = store->read(version.key_);
    auto [index_key, index_seg] = std::move(fut_index).get();
    TimeseriesDescriptor tsd;
    tsd.mutable_proto().set_total_rows(index_seg.row_count());
    tsd.mutable_proto().mutable_stream_descriptor()->CopyFrom(index_seg.descriptor().proto());
    return {SegmentInMemory(std::move(index_seg)), tsd, {}, {}};
}

namespace {

void ensure_norm_meta(arcticdb::proto::descriptors::NormalizationMetadata& norm_meta, const StreamId& stream_id, bool set_tz) {
    if(norm_meta.input_type_case() == arcticdb::proto::descriptors::NormalizationMetadata::INPUT_TYPE_NOT_SET)
        norm_meta.CopyFrom(make_timeseries_norm_meta(stream_id));

    if(set_tz && norm_meta.df().common().index().tz().empty())
        norm_meta.mutable_df()->mutable_common()->mutable_index()->set_tz("UTC");
}

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
        throw storage::NoDataFoundException(version_info.key_.id());
    }

    if (variant_key_type(index_key_seg.first) == KeyType::MULTI_KEY) {
        pipeline_context->multi_key_ = index_key_seg.second;
        return std::nullopt;
    }
    return std::make_optional<pipelines::index::IndexSegmentReader>(std::move(index_key_seg.second));
}

void read_indexed_keys_to_pipeline(
    const std::shared_ptr<Store>& store,
    const std::shared_ptr<PipelineContext>& pipeline_context,
    const VersionedItem& version_info,
    ReadQuery& read_query,
    const ReadOptions& read_options
    ) {
    auto maybe_reader = get_index_segment_reader(store, pipeline_context, version_info);
    if(!maybe_reader)
        return;

    auto index_segment_reader = std::move(maybe_reader.value());
    ARCTICDB_DEBUG(log::version(), "Read index segment with {} keys", index_segment_reader.size());
    check_column_and_date_range_filterable(index_segment_reader, read_query);

    add_index_columns_to_query(read_query, index_segment_reader.tsd());

    read_query.calculate_row_filter(static_cast<int64_t>(index_segment_reader.tsd().proto().total_rows()));
    bool bucketize_dynamic = index_segment_reader.bucketize_dynamic();
    pipeline_context->desc_ = index_segment_reader.tsd().as_stream_descriptor();

    bool dynamic_schema = opt_false(read_options.dynamic_schema_);
    auto queries = get_column_bitset_and_query_functions<index::IndexSegmentReader>(
        read_query,
        pipeline_context,
        dynamic_schema,
        bucketize_dynamic);

    pipeline_context->slice_and_keys_ = filter_index(index_segment_reader, combine_filter_functions(queries));
    pipeline_context->total_rows_ = pipeline_context->calc_rows();
    pipeline_context->norm_meta_ = std::make_unique<arcticdb::proto::descriptors::NormalizationMetadata>(std::move(*index_segment_reader.mutable_tsd().mutable_proto().mutable_normalization()));
    pipeline_context->user_meta_ = std::make_unique<arcticdb::proto::descriptors::UserDefinedMetadata>(std::move(*index_segment_reader.mutable_tsd().mutable_proto().mutable_user_meta()));
    pipeline_context->bucketize_dynamic_ = bucketize_dynamic;
}

void read_incompletes_to_pipeline(
    const std::shared_ptr<Store>& store,
    std::shared_ptr<PipelineContext>& pipeline_context,
    const ReadQuery& read_query,
    const ReadOptions& read_options,
    bool convert_int_to_float,
    bool via_iteration,
    bool sparsify) {

    auto incomplete_segments = get_incomplete(
        store,
        pipeline_context->stream_id_,
        read_query.row_filter,
        pipeline_context->last_row(),
        via_iteration,
        false);

    if(incomplete_segments.empty())
        return;

    // Mark the start point of the incompletes, so we know that there is no column slicing after this point
    pipeline_context->incompletes_after_ = pipeline_context->slice_and_keys_.size();

    // If there are only incompletes we need to add the index here
    if(pipeline_context->slice_and_keys_.empty()) {
        add_index_columns_to_query(read_query, incomplete_segments.begin()->segment(store).index_descriptor());
    }

    auto first_seg = incomplete_segments.begin()->segment(store);
    if (!pipeline_context->desc_)
        pipeline_context->desc_ = first_seg.descriptor();

    if (!pipeline_context->norm_meta_) {
        pipeline_context->norm_meta_ = std::make_unique<arcticdb::proto::descriptors::NormalizationMetadata>();
        auto segment_tsd = first_seg.index_descriptor();
        pipeline_context->norm_meta_->CopyFrom(segment_tsd.proto().normalization());
        ensure_norm_meta(*pipeline_context->norm_meta_, pipeline_context->stream_id_, sparsify);
    }

    pipeline_context->desc_ = merge_descriptors(pipeline_context->descriptor(), incomplete_segments, read_query.columns);
    modify_descriptor(pipeline_context, read_options);
    if (convert_int_to_float) {
        stream::convert_descriptor_types(*pipeline_context->desc_);
    }

    generate_filtered_field_descriptors(pipeline_context, read_query.columns);
    pipeline_context->slice_and_keys_.insert(std::end(pipeline_context->slice_and_keys_), std::begin(incomplete_segments),  std::end(incomplete_segments));
    pipeline_context->total_rows_ = pipeline_context->calc_rows();
}

void copy_frame_data_to_buffer(const SegmentInMemory& destination, size_t target_index, SegmentInMemory& source, size_t source_index, const RowRange& row_range) {
    auto num_rows = row_range.diff();
    if (num_rows == 0) {
        return;
    }
    auto& src_column = source.column(static_cast<position_t>(source_index));
    auto& dst_column = destination.column(static_cast<position_t>(target_index));
    auto& buffer = dst_column.data().buffer();
    auto dst_rawtype_size = sizeof_datatype(dst_column.type());
    auto offset = dst_rawtype_size * (row_range.first - destination.offset());
    auto total_size = dst_rawtype_size * num_rows;
    buffer.assert_size(offset + total_size);

    auto src_ptr = src_column.data().buffer().data();
    auto dst_ptr = buffer.data() + offset;

    auto type_promotion_error_msg = fmt::format("Can't promote type {} to type {} in field {}",
                                                src_column.type(), dst_column.type(), destination.field(target_index).name());

    if (trivially_compatible_types(src_column.type(), dst_column.type())) {
        memcpy(dst_ptr, src_ptr, total_size);
    } else if (has_valid_type_promotion(src_column.type(), dst_column.type())) {
        dst_column.type().visit_tag([&src_ptr, &dst_ptr, &src_column, &type_promotion_error_msg, num_rows] (auto dest_desc_tag) {
            using DestinationType =  typename decltype(dest_desc_tag)::DataTypeTag::raw_type;
            src_column.type().visit_tag([&src_ptr, &dst_ptr, &type_promotion_error_msg, num_rows] (auto src_desc_tag ) {
                using SourceType =  typename decltype(src_desc_tag)::DataTypeTag::raw_type;
                if constexpr(std::is_arithmetic_v<SourceType> && std::is_arithmetic_v<DestinationType>) {
                    auto typed_src_ptr = reinterpret_cast<SourceType *>(src_ptr);
                    auto typed_dst_ptr = reinterpret_cast<DestinationType *>(dst_ptr);
                    for (auto i = 0u; i < num_rows; ++i) {
                        *typed_dst_ptr++ = static_cast<DestinationType>(*typed_src_ptr++);
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

void copy_segments_to_frame(const std::shared_ptr<Store>& store, const std::shared_ptr<PipelineContext>& pipeline_context, const SegmentInMemory& frame) {
    for (auto context_row : folly::enumerate(*pipeline_context)) {
        auto& segment = context_row->slice_and_key().segment(store);
        const auto index_field_count = get_index_field_count(frame);
        for (auto idx = 0u; idx < index_field_count && context_row->fetch_index(); ++idx) {
            copy_frame_data_to_buffer(frame, idx, segment, idx, context_row->slice_and_key().slice_.row_range);
        }

        auto field_count = context_row->slice_and_key().slice_.col_range.diff() + index_field_count;
        for (auto field_col = index_field_count; field_col < field_count; ++field_col) {
            const auto& field_name = context_row->descriptor().fields(field_col).name();
            auto frame_loc_opt = frame.column_index(field_name);
            if (!frame_loc_opt)
                continue;

            copy_frame_data_to_buffer(frame, *frame_loc_opt, segment, field_col, context_row->slice_and_key().slice_.row_range);
        }
    }
}

SegmentInMemory prepare_output_frame(std::vector<SliceAndKey>&& items, const std::shared_ptr<PipelineContext>& pipeline_context, const std::shared_ptr<Store>& store, const ReadOptions& read_options) {
    pipeline_context->clear_vectors();
    pipeline_context->slice_and_keys_ = std::move(items);
	std::sort(std::begin(pipeline_context->slice_and_keys_), std::end(pipeline_context->slice_and_keys_), [] (const auto& left, const auto& right) {
		return std::tie(left.slice_.row_range, left.slice_.col_range) < std::tie(right.slice_.row_range, right.slice_.col_range);
	});
    adjust_slice_rowcounts(pipeline_context);
    const auto dynamic_schema = opt_false(read_options.dynamic_schema_);
    mark_index_slices(pipeline_context, dynamic_schema, pipeline_context->bucketize_dynamic_);
    pipeline_context->ensure_vectors();

    for(auto row : *pipeline_context) {
        row.set_compacted(false);
        row.set_descriptor(row.slice_and_key().segment(store).descriptor_ptr());
        row.set_string_pool(row.slice_and_key().segment(store).string_pool_ptr());
    }

    auto frame = allocate_frame(pipeline_context);
    copy_segments_to_frame(store, pipeline_context, frame);

    return frame;
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
    ReadQuery read_query({std::make_shared<Clause>(std::move(*clause))});

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
    read_indexed_keys_to_pipeline(store, pipeline_context, versioned_item, read_query, read_options);

    schema::check<ErrorCode::E_UNSUPPORTED_INDEX_TYPE>(
            !pipeline_context->multi_key_,
            "Column stats generation not supported with multi-indexed symbols"
            );
    schema::check<ErrorCode::E_OPERATION_NOT_SUPPORTED_WITH_PICKLED_DATA>(
            !pipeline_context->is_pickled(),
            "Cannot create column stats on pickled data"
            );

    auto segs = read_and_process(store, pipeline_context, read_query, read_options, 0u);
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
        tsd.mutable_proto().set_total_rows(segment_in_memory.row_count());
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

FrameAndDescriptor read_dataframe_impl(
    const std::shared_ptr<Store>& store,
    const std::variant<VersionedItem, StreamId>& version_info,
    ReadQuery& read_query,
    const ReadOptions& read_options
    ) {
    using namespace arcticdb::pipelines;
    auto pipeline_context = std::make_shared<PipelineContext>();

    if(std::holds_alternative<StreamId>(version_info)) {
        pipeline_context->stream_id_ = std::get<StreamId>(version_info);
    } else {
        pipeline_context->stream_id_ = std::get<VersionedItem>(version_info).key_.id();
        read_indexed_keys_to_pipeline(store, pipeline_context, std::get<VersionedItem>(version_info), read_query, read_options);
    }

    if(pipeline_context->multi_key_)
        return read_multi_key(store, *pipeline_context->multi_key_);

    if(opt_false(read_options.incompletes_)) {
        util::check(std::holds_alternative<IndexRange>(read_query.row_filter), "Streaming read requires date range filter");
        const auto& query_range = std::get<IndexRange>(read_query.row_filter);
        const auto existing_range = pipeline_context->index_range();
        if(!existing_range.specified_ || query_range.end_ > existing_range.end_)
            read_incompletes_to_pipeline(store, pipeline_context, read_query, read_options, false, false, false);
    }

    modify_descriptor(pipeline_context, read_options);
    generate_filtered_field_descriptors(pipeline_context, read_query.columns);
    ARCTICDB_DEBUG(log::version(), "Fetching data to frame");
    SegmentInMemory frame;
    auto buffers = std::make_shared<BufferHolder>();
    if(!read_query.clauses_.empty()) {
        ARCTICDB_SAMPLE(RunPipelineAndOutput, 0)
        util::check_rte(!pipeline_context->is_pickled(),"Cannot filter pickled data");
        auto segs = read_and_process(store, pipeline_context, read_query, read_options, 0u);

        frame = prepare_output_frame(std::move(segs), pipeline_context, store, read_options);
    } else {
        ARCTICDB_SAMPLE(MarkAndReadDirect, 0)
        util::check_rte(!(pipeline_context->is_pickled() && std::holds_alternative<RowRange>(read_query.row_filter)), "Cannot use head/tail/row_range with pickled data, use plain read instead");
        mark_index_slices(pipeline_context, opt_false(read_options.dynamic_schema_), pipeline_context->bucketize_dynamic_);
        frame = read_direct(store, pipeline_context, buffers, read_options);
    }

    ARCTICDB_DEBUG(log::version(), "Reduce and fix columns");
    reduce_and_fix_columns(pipeline_context, frame, read_options);
    return {frame, timeseries_descriptor_from_pipeline_context(pipeline_context, {}, pipeline_context->bucketize_dynamic_), {}, buffers};
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
    tsd.mutable_proto().set_total_rows(pipeline_context->total_rows_);
    tsd.mutable_proto().mutable_normalization()->CopyFrom(*pipeline_context->norm_meta_);
    if(user_meta)
        tsd.mutable_proto().mutable_user_meta()->CopyFrom(*user_meta);

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

VersionedItem sort_merge_impl(
    const std::shared_ptr<Store>& store,
    const StreamId& stream_id,
    const std::optional<arcticdb::proto::descriptors::UserDefinedMetadata>& norm_meta,
    const UpdateInfo& update_info,
    bool append,
    bool convert_int_to_float,
    bool via_iteration,
    bool sparsify
    ) {
    auto pipeline_context = std::make_shared<PipelineContext>();
    pipeline_context->stream_id_ = stream_id;
    pipeline_context->version_id_ = update_info.next_version_id_;
    ReadQuery read_query;

    if(append && update_info.previous_index_key_.has_value())
        read_indexed_keys_to_pipeline(store, pipeline_context, *(update_info.previous_index_key_), read_query, ReadOptions{});

    auto num_versioned_rows = pipeline_context->total_rows_;

    read_incompletes_to_pipeline(store, pipeline_context, read_query, ReadOptions{}, convert_int_to_float, via_iteration, sparsify);

    std::vector<entity::VariantKey> delete_keys;
    for(auto sk = pipeline_context->incompletes_begin(); sk != pipeline_context->end(); ++sk) {
        util::check(sk->slice_and_key().key().type() == KeyType::APPEND_DATA, "Deleting incorrect key type {}", sk->slice_and_key().key().type());
        delete_keys.emplace_back(sk->slice_and_key().key());
    }

    std::vector<FrameSlice> slices;
    std::vector<folly::Future<VariantKey>> fut_vec;
    std::optional<SegmentInMemory> last_indexed;
    auto index = stream::index_type_from_descriptor(pipeline_context->descriptor());
    util::variant_match(index,
        [&](const stream::TimeseriesIndex &timeseries_index) {
            read_query.clauses_.emplace_back(std::make_shared<Clause>(SortClause{timeseries_index.name()}));
            read_query.clauses_.emplace_back(std::make_shared<Clause>(RemoveColumnPartitioningClause{}));
            const auto split_size = ConfigsMap::instance()->get_int("Split.RowCount", 10000);
            read_query.clauses_.emplace_back(std::make_shared<Clause>(SplitClause{static_cast<size_t>(split_size)}));
            read_query.clauses_.emplace_back(std::make_shared<Clause>(MergeClause{timeseries_index, DenseColumnPolicy{}, stream_id, pipeline_context->descriptor()}));
            auto segments = read_and_process(store, pipeline_context, read_query, ReadOptions{}, pipeline_context->incompletes_after());
            pipeline_context->total_rows_ = num_versioned_rows + get_slice_rowcounts(segments);

            auto index = index_type_from_descriptor(pipeline_context->descriptor());
            stream::SegmentAggregator<TimeseriesIndex, DynamicSchema, RowCountSegmentPolicy, SparseColumnPolicy>
            aggregator{
                [&slices](FrameSlice &&slice) {
                    slices.emplace_back(std::move(slice));
                },
                DynamicSchema{pipeline_context->descriptor(), index},
                [pipeline_context=pipeline_context, &fut_vec, &store](SegmentInMemory &&segment) {
                    auto local_index_start = TimeseriesIndex::start_value_for_segment(segment);
                    auto local_index_end = TimeseriesIndex::end_value_for_segment(segment);
                    stream::StreamSink::PartialKey
                    pk{KeyType::TABLE_DATA, pipeline_context->version_id_, pipeline_context->stream_id_, local_index_start, local_index_end};
                    fut_vec.emplace_back(store->write(pk, std::move(segment)));
                }};

            for(auto sk = segments.begin(); sk != segments.end(); ++sk) {
                aggregator.add_segment(
                    std::move(sk->segment(store)),
                    sk->slice(),
                    convert_int_to_float);

                sk->unset_segment();
            }
            aggregator.commit();
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

    store->remove_keys(delete_keys).get();
    return vit;
}

VersionedItem compact_incomplete_impl(
    const std::shared_ptr<Store>& store,
    const StreamId& stream_id,
    const std::optional<arcticdb::proto::descriptors::UserDefinedMetadata>& user_meta,
    const UpdateInfo& update_info,
    bool append,
    bool convert_int_to_float,
    bool via_iteration,
    bool sparsify,
    const WriteOptions& write_options) {

    auto pipeline_context = std::make_shared<PipelineContext>();
    pipeline_context->stream_id_ = stream_id;
    pipeline_context->version_id_ = update_info.next_version_id_;
    ReadQuery read_query;
    ReadOptions read_options;
    read_options.set_dynamic_schema(true);

    std::optional<SegmentInMemory> last_indexed;
    if(append && update_info.previous_index_key_.has_value())
        read_indexed_keys_to_pipeline(store, pipeline_context, *(update_info.previous_index_key_), read_query, read_options);

    auto prev_size = pipeline_context->slice_and_keys_.size();
    read_incompletes_to_pipeline(store, pipeline_context, ReadQuery{}, ReadOptions{}, convert_int_to_float, via_iteration, sparsify);
    if (pipeline_context->slice_and_keys_.size() == prev_size) {
        util::raise_rte("No incomplete segments found for {}", stream_id);
    }
    const auto& first_seg = pipeline_context->slice_and_keys_.begin()->segment(store);

    std::vector<entity::VariantKey> delete_keys;
    for(auto sk = pipeline_context->incompletes_begin(); sk != pipeline_context->end(); ++sk) {
        util::check(sk->slice_and_key().key().type() == KeyType::APPEND_DATA, "Deleting incorrect key type {}", sk->slice_and_key().key().type());
        delete_keys.emplace_back(sk->slice_and_key().key());
    }

    std::vector<folly::Future<VariantKey>> fut_vec;
    std::vector<FrameSlice> slices;
    bool dynamic_schema = write_options.dynamic_schema;
    auto index = index_type_from_descriptor(first_seg.descriptor());
    auto policies = std::make_tuple(index, 
                                    dynamic_schema ? VariantSchema{DynamicSchema::default_schema(index)} : VariantSchema{FixedSchema::default_schema(index)}, 
                                    sparsify ? VariantColumnPolicy{SparseColumnPolicy{}} : VariantColumnPolicy{DenseColumnPolicy{}}
                                    );
    util::variant_match(std::move(policies), [
        &fut_vec, &slices, pipeline_context=pipeline_context, &store, convert_int_to_float] (auto &&idx, auto &&schema, auto &&column_policy) {
        using IndexType = std::remove_reference_t<decltype(idx)>;
        using SchemaType = std::remove_reference_t<decltype(schema)>;
        using ColumnPolicyType = std::remove_reference_t<decltype(column_policy)>;
        do_compact<IndexType, SchemaType, RowCountSegmentPolicy, ColumnPolicyType>(
                pipeline_context->incompletes_begin(),
                pipeline_context->end(),
                pipeline_context,
                fut_vec,
                slices,
                store,
                convert_int_to_float,
                std::nullopt);
    });

    auto keys = folly::collect(fut_vec).get();
    auto vit = collate_and_write(
        store,
        pipeline_context,
        slices,
        keys,
        pipeline_context->incompletes_after(),
        user_meta
        );


    store->remove_keys(delete_keys).get();
    return vit;
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

    ReadQuery read_query;
    read_indexed_keys_to_pipeline(store, pipeline_context, *(update_info.previous_index_key_), read_query, defragmentation_read_options_generator(options));

    using CompactionStartInfo = std::pair<size_t, size_t>;//row, segment_append_after
    std::vector<CompactionStartInfo> first_col_segment_idx;
    first_col_segment_idx.reserve(pipeline_context->slice_and_keys_.size());
    std::optional<CompactionStartInfo> compaction_start_info;
    size_t segment_idx = 0, num_to_segments_after_compact = 0, new_segment_row_size = 0;
    for(auto it = pipeline_context->slice_and_keys_.begin(); it != pipeline_context->slice_and_keys_.end(); it++) {
        auto &slice = it->slice();

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
            auto it = std::lower_bound(first_col_segment_idx.begin(), first_col_segment_idx.end(), slice.row_range.start(), [](auto lhs, auto rhs){return lhs.first < rhs;});
            if (it != first_col_segment_idx.end())
                compaction_start_info = *it;
            else{
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
    
    std::vector<entity::VariantKey> delete_keys;
    for(auto sk = std::next(pre_defragmentation_info.pipeline_context->begin(), pre_defragmentation_info.append_after.value()); sk != pre_defragmentation_info.pipeline_context->end(); ++sk) {
        delete_keys.emplace_back(sk->slice_and_key().key());
    }
    // in the new index segment, we will start appending after this value
    std::vector<folly::Future<VariantKey>> fut_vec;
    std::vector<FrameSlice> slices;
    auto index = index_type_from_descriptor(pre_defragmentation_info.pipeline_context->descriptor());
    auto policies = std::make_tuple(index,
                                    options.dynamic_schema ? VariantSchema{DynamicSchema::default_schema(index)} : VariantSchema{FixedSchema::default_schema(index)}
                                    );

    util::variant_match(std::move(policies), [
        &fut_vec, &slices, &store, &options, &pre_defragmentation_info, segment_size=segment_size] (auto &&idx, auto &&schema) {
        pre_defragmentation_info.read_query.clauses_.emplace_back(std::make_shared<Clause>(RemoveColumnPartitioningClause{}));
        auto segments = read_and_process(store, pre_defragmentation_info.pipeline_context, pre_defragmentation_info.read_query, defragmentation_read_options_generator(options), pre_defragmentation_info.append_after.value());
        using IndexType = std::remove_reference_t<decltype(idx)>;
        using SchemaType = std::remove_reference_t<decltype(schema)>;
        do_compact<IndexType, SchemaType, RowCountSegmentPolicy, DenseColumnPolicy>(
                    segments.begin(),
                    segments.end(),
                    pre_defragmentation_info.pipeline_context,
                    fut_vec,
                    slices,
                    store,
                    false,
                    segment_size);
    });

    auto keys = folly::collect(fut_vec).get();
    auto vit = collate_and_write(
            store,
            pre_defragmentation_info.pipeline_context,
            slices,
            keys,
            pre_defragmentation_info.append_after.value(),
            std::nullopt);
    
    store->remove_keys(delete_keys).get();
    return vit;
}

} //namespace arcticdb::version_store
