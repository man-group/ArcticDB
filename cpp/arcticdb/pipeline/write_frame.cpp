/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/column_store/string_pool.hpp>
#include <arcticdb/entity/native_tensor.hpp>
#include <arcticdb/python/python_utils.hpp>
#include <arcticdb/pipeline/input_tensor_frame.hpp>
#include <arcticdb/pipeline/index_writer.hpp>
#include <arcticdb/pipeline/frame_slice.hpp>
#include <arcticdb/pipeline/index_utils.hpp>
#include <arcticdb/pipeline/slicing.hpp>
#include <arcticdb/stream/protobuf_mappings.hpp>
#include <arcticdb/stream/stream_sink.hpp>
#include <arcticdb/entity/performance_tracing.hpp>
#include <arcticdb/stream/aggregator.hpp>
#include <arcticdb/entity/protobufs.hpp>
#include <arcticdb/util/offset_string.hpp>
#include <arcticdb/util/variant.hpp>
#include <arcticdb/python/python_types.hpp>
#include <arcticdb/pipeline/frame_utils.hpp>
#include <arcticdb/pipeline/write_frame.hpp>
#include <arcticdb/stream/append_map.hpp>
#include <arcticdb/version/version_utils.hpp>
#include <arcticdb/entity/merge_descriptors.hpp>
#include <arcticdb/async/task_scheduler.hpp>

#include <pybind11/pybind11.h>

namespace arcticdb::pipelines {

using namespace arcticdb::entity;
using namespace arcticdb::stream;

WriteToSegmentTask::WriteToSegmentTask(
    std::shared_ptr<InputTensorFrame> frame,
    FrameSlice slice,
    const SlicingPolicy& slicing,
    folly::Function<stream::StreamSink::PartialKey(const FrameSlice&)>&& partial_key_gen,
    size_t slice_num_for_column,
    Index index,
    bool sparsify_floats
    ) :
    frame_(std::move(frame)),
    slice_(std::move(slice)),
    slicing_(slicing),
    partial_key_gen_(std::move(partial_key_gen)),
    slice_num_for_column_(slice_num_for_column),
    index_(std::move(index)),
    sparsify_floats_(sparsify_floats) {
    slice_.check_magic();
}

std::tuple<stream::StreamSink::PartialKey, SegmentInMemory, FrameSlice> WriteToSegmentTask::operator() () {
    slice_.check_magic();
    magic_.check();
    return util::variant_match(index_, [this](auto& idx) {
        using IdxType = std::decay_t<decltype(idx)>;
        using SingleSegmentAggregator = Aggregator<IdxType, FixedSchema, NeverSegmentPolicy>;

        ARCTICDB_SUBSAMPLE_AGG(WriteSliceCopyToSegment)
        std::tuple<stream::StreamSink::PartialKey, SegmentInMemory, FrameSlice> output;

        auto key = partial_key_gen_(slice_);
        SingleSegmentAggregator agg{FixedSchema{*slice_.desc(), frame_->index}, [key=std::move(key), slice=slice_, &output](auto&& segment) {
            output = std::make_tuple(key, std::forward<SegmentInMemory>(segment), slice);
        }};

        auto regular_slice_size = util::variant_match(slicing_,
            [&](const NoSlicing&) {
                return slice_.row_range.second - slice_.row_range.first;
            },
            [&](const auto& slicer) {
                return slicer.row_per_slice();
            });

        // Offset is used for index value in row-count index
        auto offset_in_frame = slice_begin_pos(slice_, *frame_);
        agg.set_offset(offset_in_frame);

        auto rows_to_write = slice_.row_range.second - slice_.row_range.first;
        if (frame_->desc.index().field_count() > 0) {
            util::check(static_cast<bool>(frame_->index_tensor), "Got null index tensor in write_slices");
            auto opt_error = aggregator_set_data(
                frame_->desc.fields(0).type(),
                frame_->index_tensor.value(),
                agg, 0, rows_to_write, offset_in_frame, slice_num_for_column_, regular_slice_size, false);
            if (opt_error.has_value()) {
                opt_error->raise(frame_->desc.fields(0).name(), offset_in_frame);
            }
        }

        for (size_t col = 0, end = slice_.col_range.diff(); col < end; ++col) {
            auto abs_col = col + frame_->desc.index().field_count();
            auto& fd = slice_.non_index_field(col);
            auto& tensor = frame_->field_tensors[slice_.absolute_field_col(col)];
            auto opt_error = aggregator_set_data(
                fd.type(),
                tensor, agg, abs_col, rows_to_write, offset_in_frame, slice_num_for_column_,
                regular_slice_size, sparsify_floats_);
            if (opt_error.has_value()) {
                opt_error->raise(fd.name(), offset_in_frame);
            }
        }

        agg.end_block_write(rows_to_write);
        agg.commit();
        return output;
    });
}

std::vector<std::pair<FrameSlice, size_t>> get_slice_and_rowcount(const std::vector<FrameSlice>& slices) {
    std::vector<std::pair<FrameSlice, size_t>> slice_and_rowcount;
    slice_and_rowcount.reserve(slices.size());
    size_t slice_num_for_column = 0;
    std::optional<size_t> first_row;
    for(const auto& slice : slices) {
        if (!first_row)
            first_row = slice.row_range.first;

        if (slice.row_range.first == first_row.value())
            slice_num_for_column = 0;

        slice_and_rowcount.emplace_back(slice, slice_num_for_column);
        ++slice_num_for_column;
    }
    return slice_and_rowcount;
}

folly::Future<std::vector<SliceAndKey>> write_slices(
        const std::shared_ptr<InputTensorFrame> &frame,
        std::vector<FrameSlice>&& slices,
        const SlicingPolicy &slicing,
        IndexPartialKey&& key,
        const std::shared_ptr<stream::StreamSink>& sink,
        const std::shared_ptr<DeDupMap>& de_dup_map,
        bool sparsify_floats) {
    ARCTICDB_SAMPLE(WriteSlices, 0)

    auto slice_and_rowcount = get_slice_and_rowcount(slices);

    const auto write_window = ConfigsMap::instance()->get_int("VersionStore.BatchWriteWindow", 2 * async::TaskScheduler::instance()->io_thread_count());
    return folly::collect(folly::window(std::move(slice_and_rowcount), [de_dup_map, frame, slicing, key=std::move(key), sink, sparsify_floats](auto&& slice) {
            return async::submit_cpu_task(WriteToSegmentTask(
                frame,
                slice.first,
                slicing,
                get_partial_key_gen(frame, key),
                slice.second,
                frame->index,
                sparsify_floats))
            .then([sink, de_dup_map] (auto&& ks) {
                return sink->async_write(ks, de_dup_map);
            });
    }, write_window)).via(&async::io_executor());
}

folly::Future<entity::VariantKey> write_multi_index(
        const std::shared_ptr<InputTensorFrame>& frame,
        std::vector<SliceAndKey>&& slice_and_keys,
        const IndexPartialKey& partial_key,
        const std::shared_ptr<stream::StreamSink>& sink
) {
    auto timeseries_desc = index_descriptor_from_frame(frame, frame->offset);
    index::IndexWriter<stream::RowCountIndex> writer(sink, partial_key, std::move(timeseries_desc));
    for (auto &slice_and_key : slice_and_keys) {
        writer.add(slice_and_key.key(), slice_and_key.slice_);
    }
    return writer.commit();
}

folly::Future<std::vector<SliceAndKey>> slice_and_write(
        const std::shared_ptr<InputTensorFrame> &frame,
        const SlicingPolicy &slicing,
        IndexPartialKey&& key,
        const std::shared_ptr<stream::StreamSink>& sink,
        const std::shared_ptr<DeDupMap>& de_dup_map,
        bool sparsify_floats) {
    ARCTICDB_SUBSAMPLE_DEFAULT(SliceFrame)
    auto slices = slice(*frame, slicing);
    ARCTICDB_SUBSAMPLE_DEFAULT(SliceAndWrite)
    return write_slices(frame, std::move(slices), slicing, std::move(key), sink, de_dup_map, sparsify_floats);
}

folly::Future<entity::AtomKey>
write_frame(
        IndexPartialKey&& key,
        const std::shared_ptr<InputTensorFrame>& frame,
        const SlicingPolicy &slicing,
        const std::shared_ptr<Store>& store,
        const std::shared_ptr<DeDupMap>& de_dup_map,
        bool sparsify_floats) {
    ARCTICDB_SAMPLE_DEFAULT(WriteFrame)
    auto fut_slice_keys = slice_and_write(frame, slicing, IndexPartialKey{key}, store, de_dup_map, sparsify_floats);
    // Write the keys of the slices into an index segment
    ARCTICDB_SUBSAMPLE_DEFAULT(WriteIndex)
    return std::move(fut_slice_keys).thenValue([frame=frame, key = std::move(key), &store](auto&& slice_keys) mutable {
        return index::write_index(frame, std::forward<decltype(slice_keys)>(slice_keys), key, store);
    });
}

folly::Future<entity::AtomKey> append_frame(
        IndexPartialKey&& key,
        const std::shared_ptr<InputTensorFrame>& frame,
        const SlicingPolicy& slicing,
        index::IndexSegmentReader& index_segment_reader,
        const std::shared_ptr<Store>& store,
        bool dynamic_schema,
        bool ignore_sort_order)
{
    ARCTICDB_SAMPLE_DEFAULT(AppendFrame)
    util::variant_match(frame->index,
                        [&index_segment_reader, &frame, ignore_sort_order](const TimeseriesIndex &) {
                            util::check(frame->has_index(), "Cannot append timeseries without index");
                            util::check(static_cast<bool>(frame->index_tensor), "Got null index tensor in append_frame");
                            auto& frame_index = frame->index_tensor.value();
                            util::check(frame_index.data_type() == DataType::NANOSECONDS_UTC64,
                                        "Expected timestamp index in append, got type {}", frame_index.data_type());
                            if (index_segment_reader.tsd().proto().total_rows() != 0 && frame_index.size() != 0) {
                                auto first_index = NumericIndex{*frame_index.ptr_cast<timestamp>(0)};
                                auto prev = std::get<NumericIndex>(index_segment_reader.last()->key().end_index());
                                util::check(ignore_sort_order || prev - 1 <= first_index,
                                            "Can't append dataframe with start index {} to existing sequence ending at {}",
                                            util::format_timestamp(first_index), util::format_timestamp(prev));
                            }
                        },
                        [](const auto &) {
                            //Do whatever, but you can't range search it
                        }
    );

    auto existing_slices = unfiltered_index(index_segment_reader);
    auto keys_fut = slice_and_write(frame, slicing, IndexPartialKey{key}, store);
    return std::move(keys_fut)
    .thenValue([dynamic_schema, slices_to_write=std::move(existing_slices), frame=frame, index_segment_reader=std::move(index_segment_reader), key=std::move(key), store](auto&& slice_and_keys_to_append) mutable {
        slices_to_write.insert(std::end(slices_to_write), std::make_move_iterator(std::begin(slice_and_keys_to_append)), std::make_move_iterator(std::end(slice_and_keys_to_append)));
        std::sort(std::begin(slices_to_write), std::end(slices_to_write));
        if(dynamic_schema) {
            auto merged_descriptor =
                merge_descriptors(frame->desc, std::vector< std::shared_ptr<FieldCollection>>{ index_segment_reader.tsd().fields_ptr()}, {});
            merged_descriptor.set_sorted(deduce_sorted(index_segment_reader.get_sorted(), frame->desc.get_sorted()));
            auto tsd =
                make_timeseries_descriptor(frame->num_rows + frame->offset, std::move(merged_descriptor), std::move(frame->norm_meta), std::move(frame->user_meta), std::nullopt, std::nullopt, frame->bucketize_dynamic);
            return index::write_index(stream::index_type_from_descriptor(frame->desc), std::move(tsd), std::move(slices_to_write), key, store);
        } else {
            frame->desc.set_sorted(deduce_sorted(index_segment_reader.get_sorted(), frame->desc.get_sorted()));
            return index::write_index(frame, std::move(slices_to_write), key, store);
        }
    });
}

void update_string_columns(const SegmentInMemory& original, SegmentInMemory output) {
    util::check(original.descriptor() == output.descriptor(), "Update string column handling expects identical descriptors");
    for (size_t column = 0; column < static_cast<size_t>(original.descriptor().fields().size()); ++column) {
        auto &frame_field = original.field(column);
        auto field_type = frame_field.type().data_type();

        if (is_sequence_type(field_type)) {
            auto &target = output.column(static_cast<position_t>(column)).data().buffer();
            size_t end = output.row_count();
            for(auto row = 0u; row < end; ++row) {
                auto val = get_string_from_buffer(row, target, original.const_string_pool());
                util::variant_match(val,
                                    [&] (std::string_view sv) {
                                        auto off_str = output.string_pool().get(sv);
                                        set_offset_string_at(row, target, off_str.offset());
                                    },
                                    [&] (entity::position_t offset) {
                                        set_offset_string_at(row, target, offset);
                                    });

            }
        }
    }
}

std::optional<SliceAndKey> rewrite_partial_segment(
        const SliceAndKey& existing,
        IndexRange index_range,
         VersionId version_id,
         bool before,
         const std::shared_ptr<Store>& store) {
    const auto& key =  existing.key();
    const auto& existing_range =key.index_range();
    auto kv = store->read(key).get();
    auto &segment = kv.second;

    auto start = std::get<timestamp>(index_range.start_);
    auto end = std::get<timestamp>(index_range.end_);

    if(!before) {
        util::check(existing_range.start_ < index_range.start_, "Unexpected index range in after: {} !< {}", existing_range.start_, index_range.start_);
        auto bound = std::lower_bound(std::begin(segment), std::end(segment), start, [] ( auto& row, timestamp t) {return row.template index<TimeseriesIndex>() < t; });
        size_t num_rows = std::distance(std::begin(segment), bound);
        if(num_rows == 0)
            return std::nullopt;

        auto output = SegmentInMemory{segment.descriptor(), num_rows};
        std::copy(std::begin(segment), bound, std::back_inserter(output));
        update_string_columns(segment, output);
        FrameSlice new_slice{
            std::make_shared<StreamDescriptor>(output.descriptor()),
            existing.slice_.col_range,
            RowRange{0, num_rows},
            existing.slice_.hash_bucket(),
            existing.slice_.num_buckets()};
        auto fut_key = store->write(key.type(), version_id, key.id(), existing_range.start_, index_range.start_, std::move(output));
        return SliceAndKey{std::move(new_slice), std::get<AtomKey>(std::move(fut_key).get())};
    }
    else {
        util::check(existing_range.end_ > index_range.end_, "Unexpected non-intersection of update indices: {} !> {}", existing_range.end_ , index_range.end_);

        auto bound = std::upper_bound(std::begin(segment), std::end(segment), end, [] ( timestamp t, auto& row) {
            return t < row.template index<TimeseriesIndex>();
        });
        size_t num_rows = std::distance(bound, std::end(segment));
        if(num_rows == 0)
            return std::nullopt;

        auto output = SegmentInMemory{segment.descriptor(), num_rows};
        std::copy(bound, std::end(segment), std::back_inserter(output));
        update_string_columns(segment, output);
        FrameSlice new_slice{
            std::make_shared<StreamDescriptor>(output.descriptor()),
                    existing.slice_.col_range,
                    RowRange{0, num_rows},
                    existing.slice_.hash_bucket(),
                    existing.slice_.num_buckets()};
        auto fut_key = store->write(key.type(), version_id, key.id(), index_range.end_, existing_range.end_, std::move(output));
        return SliceAndKey{std::move(new_slice), std::get<AtomKey>(std::move(fut_key).get())};
    }
}

std::vector<SliceAndKey> flatten_and_fix_rows(const std::vector<std::vector<SliceAndKey>>& groups, size_t& global_count) {
    std::vector<SliceAndKey> output;
    global_count = 0;
    for(auto group : groups) {
        if(group.empty()) continue;
        auto group_start = group.begin()->slice_.row_range.first;
        auto group_end = std::accumulate(std::begin(group), std::end(group), 0ULL, [](size_t a, const SliceAndKey& sk) { return std::max(a, sk.slice_.row_range.second); });
        std::transform(std::begin(group), std::end(group), std::back_inserter(output), [&] (auto& sk) {
            auto range_start = global_count + (sk.slice_.row_range.first - group_start);
            auto new_range = RowRange{range_start, range_start + (sk.slice_.row_range.diff())};
            sk.slice_.row_range = new_range;
            return sk; });
        global_count += (group_end - group_start) ;
    }
    return output;
}

} //namespace arcticdb::pipelines
