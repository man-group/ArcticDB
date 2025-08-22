/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/pipeline/input_tensor_frame.hpp>
#include <arcticdb/pipeline/frame_slice.hpp>
#include <arcticdb/pipeline/index_utils.hpp>
#include <arcticdb/pipeline/slicing.hpp>
#include <arcticdb/stream/protobuf_mappings.hpp>
#include <arcticdb/stream/stream_sink.hpp>
#include <arcticdb/entity/performance_tracing.hpp>
#include <arcticdb/stream/aggregator.hpp>
#include <arcticdb/util/variant.hpp>
#include <arcticdb/pipeline/frame_utils.hpp>
#include <arcticdb/pipeline/write_frame.hpp>
#include <arcticdb/async/task_scheduler.hpp>
#include <arcticdb/util/format_date.hpp>

#include <vector>
#include <array>
#include <ranges>

namespace arcticdb::pipelines {

using namespace arcticdb::entity;
using namespace arcticdb::stream;
namespace ranges = std::ranges;

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
    if (frame_->seg.has_value()) {
        ARCTICDB_SUBSAMPLE_AGG(WriteSliceCopyToSegment)
        auto key = partial_key_gen_(slice_);
        const auto& frame = *frame_->seg;
        SegmentInMemory seg;
        // TODO: Use attach_descriptor rather than adding one field at a time
//        seg.attach_descriptor(slice_.desc());
        if (frame_->desc.index().field_count() > 0) {
            // TODO: Add index column to all segments
        }
        for (size_t col_idx = slice_.columns().first; col_idx < slice_.columns().second; ++col_idx) {
            const auto& source_column = frame.column(col_idx);
            // TODO: Handle multiple record batches
            // TODO: Handle bool columns
            // TODO: Handle strings
            // Inclusive
            const auto first_byte = slice_.rows().first * get_type_size(source_column.type().data_type());
            const auto bytes = (slice_.rows().second * get_type_size(source_column.type().data_type())) - first_byte;
            ChunkedBuffer chunked_buffer;
            chunked_buffer.add_external_block(source_column.data().buffer().bytes_at(first_byte, bytes), bytes, 0);
            seg.add_column(frame.field(col_idx),
                           std::make_shared<Column>(source_column.type(), Sparsity::NOT_PERMITTED, std::move(chunked_buffer)));
        }
        seg.descriptor().set_id(key.stream_id);
        seg.descriptor().set_index({IndexDescriptorImpl::Type::ROWCOUNT, 0});
        seg.set_row_data((slice_.rows().second - slice_.rows().first) - 1);
        return {std::move(key), std::move(seg), std::move(slice_)};
    } else {
        return util::variant_match(index_, [this](auto& idx) {
            using IdxType = std::decay_t<decltype(idx)>;
            using SingleSegmentAggregator = Aggregator<IdxType, FixedSchema, NeverSegmentPolicy>;

            ARCTICDB_SUBSAMPLE_AGG(WriteSliceCopyToSegment)
            std::tuple<stream::StreamSink::PartialKey, SegmentInMemory, FrameSlice> output;

            auto key = partial_key_gen_(slice_);
            SingleSegmentAggregator agg{
                    FixedSchema{*slice_.desc(), frame_->index},
                    [key = std::move(key), slice = slice_, &output](auto&& segment) {
                        output = std::make_tuple(key, std::forward<SegmentInMemory>(segment), slice);
                    },
                    NeverSegmentPolicy{},
                    *slice_.desc()
            };

            auto regular_slice_size = util::variant_match(
                    slicing_,
                    [&](const NoSlicing&) { return slice_.row_range.second - slice_.row_range.first; },
                    [&](const auto& slicer) { return slicer.row_per_slice(); }
            );

            // Offset is used for index value in row-count index
            auto offset_in_frame = slice_begin_pos(slice_, *frame_);
            agg.set_offset(offset_in_frame);

            auto rows_to_write = slice_.row_range.second - slice_.row_range.first;
            if (frame_->desc.index().field_count() > 0) {
                util::check(static_cast<bool>(frame_->index_tensor), "Got null index tensor in WriteToSegmentTask");
                auto opt_error = aggregator_set_data(
                        frame_->desc.fields(0).type(),
                        frame_->index_tensor.value(),
                        agg,
                        0,
                        rows_to_write,
                        offset_in_frame,
                        slice_num_for_column_,
                        regular_slice_size,
                        false
                );
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
                        tensor,
                        agg,
                        abs_col,
                        rows_to_write,
                        offset_in_frame,
                        slice_num_for_column_,
                        regular_slice_size,
                        sparsify_floats_
                );
                if (opt_error.has_value()) {
                    opt_error->raise(fd.name(), offset_in_frame);
                }
            }

            agg.end_block_write(rows_to_write);

            if (ConfigsMap().instance()->get_int("Statistics.GenerateOnWrite", 0) == 1)
                agg.segment().calculate_statistics();

            agg.finalize();
            return output;
        });
    }
}

std::vector<std::pair<FrameSlice, size_t>> get_slice_and_rowcount(const std::vector<FrameSlice>& slices) {
    std::vector<std::pair<FrameSlice, size_t>> slice_and_rowcount;
    slice_and_rowcount.reserve(slices.size());
    size_t slice_num_for_column = 0;
    std::optional<size_t> first_row;
    for(const auto& slice : slices) {
        if (!first_row)
            first_row = slice.row_range.first;

        if (slice.row_range.first == *first_row)
            slice_num_for_column = 0;

        slice_and_rowcount.emplace_back(slice, slice_num_for_column);
        ++slice_num_for_column;
    }
    return slice_and_rowcount;
}

int64_t write_window_size() {
    return ConfigsMap::instance()->get_int("VersionStore.BatchWriteWindow", int64_t(2 * async::TaskScheduler::instance()->io_thread_count()));
}

folly::Future<std::vector<SliceAndKey>> write_slices(
        const std::shared_ptr<InputTensorFrame> &frame,
        std::vector<FrameSlice>&& slices,
        const SlicingPolicy &slicing,
        TypedStreamVersion&& key,
        const std::shared_ptr<stream::StreamSink>& sink,
        const std::shared_ptr<DeDupMap>& de_dup_map,
        bool sparsify_floats) {
    ARCTICDB_SAMPLE(WriteSlices, 0)

    auto slice_and_rowcount = get_slice_and_rowcount(slices);

    int64_t write_window = write_window_size();
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

folly::Future<std::vector<SliceAndKey>> slice_and_write(
        const std::shared_ptr<InputTensorFrame> &frame,
        const SlicingPolicy &slicing,
        IndexPartialKey&& key,
        const std::shared_ptr<stream::StreamSink>& sink,
        const std::shared_ptr<DeDupMap>& de_dup_map,
        bool sparsify_floats) {
    ARCTICDB_SUBSAMPLE_DEFAULT(SliceFrame)
    auto slices = slice(*frame, slicing);
    if(slices.empty())
        return folly::makeFuture(std::vector<SliceAndKey>{});

    ARCTICDB_SUBSAMPLE_DEFAULT(SliceAndWrite)
    TypedStreamVersion tsv{std::move(key.id), key.version_id, KeyType::TABLE_DATA};
    return write_slices(frame, std::move(slices), slicing, std::move(tsv), sink, de_dup_map, sparsify_floats);
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
    return std::move(fut_slice_keys).thenValue([frame=frame, key=std::move(key), &store](auto&& slice_keys) mutable {
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
                            if (index_segment_reader.tsd().total_rows() != 0 && frame_index.size() != 0) {
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
        auto tsd = index::get_merged_tsd(frame->num_rows + frame->offset, dynamic_schema, index_segment_reader.tsd(), frame);
        return index::write_index(stream::index_type_from_descriptor(tsd.as_stream_descriptor()), tsd, std::move(slices_to_write), key, store);
    });
}

/// @brief Find the row range of affected rows during a partial rewrite (on update)
/// During partial rewrite the segment is either affected from the beginning to a
/// certain row or from a certain row to the end. Thus the row range will always be
/// either [0, row) or [row, end).
static RowRange partial_rewrite_row_range(
    const SegmentInMemory& segment,
    const IndexRange& range,
    AffectedSegmentPart affected_end
) {
    if (affected_end == AffectedSegmentPart::START) {
        const timestamp start = std::get<timestamp>(range.start_);
        auto bound = std::lower_bound(std::begin(segment), std::end(segment), start, [](const auto& row, timestamp t) {
            return row.template index<TimeseriesIndex>() < t;
        });
        return {0, bound->row_pos()};
    } else {
        const timestamp end = std::get<timestamp>(range.inclusive_end());
        auto bound = std::upper_bound(std::begin(segment), std::end(segment), end, [](timestamp t, const auto& row) {
            return t < row.template index<TimeseriesIndex>();
        });
        return {bound->row_pos(), segment.row_count()};
    }
}

folly::Future<std::optional<SliceAndKey>> async_rewrite_partial_segment(
        const SliceAndKey& existing,
        const IndexRange& index_range,
        VersionId version_id,
        AffectedSegmentPart affected_part,
        const std::shared_ptr<Store>& store) {
    return store->read(existing.key()).thenValueInline([
        existing,
        index_range,
        version_id,
        affected_part,
        store](std::pair<VariantKey, SegmentInMemory>&& key_segment) -> folly::Future<std::optional<SliceAndKey>> {
        const auto& key = existing.key();
        const SegmentInMemory& segment = key_segment.second;
        const RowRange affected_row_range = partial_rewrite_row_range(segment, index_range, affected_part);
        const auto num_rows = int64_t(affected_row_range.end() - affected_row_range.start());
        if (num_rows <= 0)
            return std::nullopt;

        SegmentInMemory output = segment.truncate(affected_row_range.start(), affected_row_range.end(), true);
        const IndexValue start_ts = TimeseriesIndex::start_value_for_segment(output);
        // +1 as in the key we store one nanosecond greater than the last index value in the segment
        const IndexValue end_ts = std::get<NumericIndex>(TimeseriesIndex::end_value_for_segment(output)) + 1;
        FrameSlice new_slice{
            std::make_shared<StreamDescriptor>(output.descriptor()),
            existing.slice_.col_range,
            RowRange{0, num_rows},
            existing.slice_.hash_bucket(),
            existing.slice_.num_buckets()};
       return store->write(
             key.type(),
             version_id,
             key.id(),
             start_ts,
             end_ts,
             std::move(output)
       ).thenValueInline([new_slice=std::move(new_slice)](VariantKey&& k) {
          return std::make_optional<SliceAndKey>(new_slice, std::get<AtomKey>(std::move(k)));
       });
    });
}

std::vector<SliceAndKey> flatten_and_fix_rows(const std::array<std::vector<SliceAndKey>, 5>& groups, size_t& global_count) {
    std::vector<SliceAndKey> output;
    output.reserve(groups.size());
    global_count = 0;
    for (const std::vector<SliceAndKey>& group : groups) {
        if (group.empty())
            continue;

        auto group_start = group.begin()->slice_.row_range.first;
        auto group_end = std::accumulate(std::begin(group), std::end(group), 0ULL, [](size_t a, const SliceAndKey& sk) {
            return std::max(a, sk.slice_.row_range.second);
        });

        ranges::transform(group, std::back_inserter(output), [&](SliceAndKey sk) {
            auto range_start = global_count + (sk.slice_.row_range.first - group_start);
            sk.slice_.row_range = RowRange{range_start, range_start + sk.slice_.row_range.diff()};
            return sk;
        });

        global_count += (group_end - group_start);
    }
    return output;
}

} //namespace arcticdb::pipelines
