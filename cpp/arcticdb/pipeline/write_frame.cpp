/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

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

folly::Future<std::vector<SliceAndKey>> write_slices(
        const InputTensorFrame &frame,
        std::vector<FrameSlice>&& slices,
        const SlicingPolicy &slicing,
        folly::Function<stream::StreamSink::PartialKey(const FrameSlice &)>&& partial_key_gen,
        const std::shared_ptr<stream::StreamSink>& sink,
        const std::shared_ptr<DeDupMap>& de_dup_map,
        bool sparsify_floats) {
    ARCTICDB_SAMPLE(WriteSlices, 0)
    std::vector<std::pair<stream::StreamSink::PartialKey, SegmentInMemory>> key_segs;
    key_segs.reserve(slices.size());

    std::vector<std::vector<folly::Future<VariantKey>>> key_groups;

    // construct batch
    util::variant_match(frame.index, [&](auto &idx) {
        using IdxType = std::decay_t<decltype(idx)>;
        using SingleSegmentAggregator = Aggregator<IdxType, FixedSchema, NeverSegmentPolicy>;

        size_t slice_num_for_column = 0;
        std::optional<size_t> first_row;
        for (const FrameSlice &slice : slices) {
            // Build in mem segment
            ARCTICDB_SUBSAMPLE_AGG(WriteSliceCopyToSegment)
            if(!first_row)
                first_row = slice.row_range.first;

            if(slice.row_range.first == first_row.value())
                slice_num_for_column = 0;

            SingleSegmentAggregator agg{FixedSchema{*slice.desc(), frame.index}, [&](auto &&segment) {
                auto key = partial_key_gen(slice);
                key_segs.emplace_back(partial_key_gen(slice), std::forward<SegmentInMemory>(segment));
            }};

            auto regular_slice_size = util::variant_match(slicing,

              [&](const NoSlicing &) {
                  return slice.row_range.second - slice.row_range.first;
              },
              [&](const auto &slicer) {
                return slicer.row_per_slice();
              });

            auto offset_in_frame = slice_begin_pos(slice, frame);
            // Offset is used for index value in row-count index
            agg.set_offset(offset_in_frame);
            auto rows_to_write = slice.row_range.second - slice.row_range.first;
            if (frame.desc.index().field_count() > 0) {
                util::check(static_cast<bool>(frame.index_tensor), "Got null index tensor in write_slices");
                aggregator_set_data(
                    frame.desc.fields(0).type(),
                    frame.index_tensor.value(),
                    agg, 0, rows_to_write, offset_in_frame, slice_num_for_column, regular_slice_size, false);
            }

            for (size_t col = 0, end = slice.col_range.diff(); col < end; ++col) {
                auto abs_col = col + frame.desc.index().field_count();
                auto &fd = slice.non_index_field(col);
                auto &tensor = frame.field_tensors[slice.absolute_field_col(col)];
                aggregator_set_data(
                    fd.type(),
                    tensor, agg, abs_col, rows_to_write, offset_in_frame, slice_num_for_column,
                    regular_slice_size, sparsify_floats);
            }

            ++slice_num_for_column;
            agg.end_block_write(rows_to_write);
            agg.commit();
        }
    });

    auto fut_writes = sink->batch_write(std::move(key_segs), de_dup_map);

    ARCTICDB_SUBSAMPLE_DEFAULT(WriteSlicesWait)
    return std::move(fut_writes).thenValue([slices = std::move(slices)](auto &&keys) mutable {
        std::vector<SliceAndKey> res;
        res.reserve(keys.size());
        for (std::size_t i = 0; i < res.capacity(); ++i) {
            res.emplace_back(SliceAndKey{slices[i], std::move(to_atom(keys[i]))});
        }
        return res;
    });
}

folly::Future<entity::VariantKey> write_multi_index(
        InputTensorFrame&& frame,
        std::vector<SliceAndKey>&& slice_and_keys,
        const IndexPartialKey& partial_key,
        const std::shared_ptr<stream::StreamSink>& sink
) {
    auto timeseries_desc = index_descriptor_from_frame(std::move(frame), frame.offset);
    index::IndexWriter<stream::RowCountIndex> writer(sink, partial_key, std::move(timeseries_desc));
    for (auto &slice_and_key : slice_and_keys) {
        writer.add(slice_and_key.key(), slice_and_key.slice_);
    }
    return writer.commit();
}

folly::Future<std::vector<SliceAndKey>> slice_and_write(
        InputTensorFrame &frame,
        const SlicingPolicy &slicing,
        folly::Function<stream::StreamSink::PartialKey(const FrameSlice &)>&& partial_key_gen,
        const std::shared_ptr<stream::StreamSink>& sink,
        const std::shared_ptr<DeDupMap>& de_dup_map,
        bool sparsify_floats) {
    ARCTICDB_SUBSAMPLE_DEFAULT(SliceFrame)
    auto slices = slice(frame, slicing);
    ARCTICDB_SUBSAMPLE_DEFAULT(SliceAndWrite)
    return write_slices(frame, std::move(slices), slicing, std::move(partial_key_gen), sink, de_dup_map, sparsify_floats);
}

folly::Future<entity::AtomKey>
write_frame(
        IndexPartialKey&& key,
        InputTensorFrame&& frame,
        const SlicingPolicy &slicing,
        const std::shared_ptr<Store>& store,
        const std::shared_ptr<DeDupMap>& de_dup_map,
        bool sparsify_floats) {
    ARCTICDB_SAMPLE_DEFAULT(WriteFrame)
    auto fut_slice_keys = slice_and_write(frame, slicing, get_partial_key_gen(frame, key), store, de_dup_map, sparsify_floats);
    // Write the keys of the slices into an index segment
    ARCTICDB_SUBSAMPLE_DEFAULT(WriteIndex)
    return std::move(fut_slice_keys).thenValue([frame = std::move(frame), key = std::move(key), &store](auto&& slice_keys) mutable {
        return index::write_index(std::move(frame), std::move(slice_keys), key, store);
    });
}

folly::Future<entity::AtomKey> append_frame(
        const IndexPartialKey& key,
        InputTensorFrame&& frame,
        const SlicingPolicy& slicing,
        index::IndexSegmentReader& index_segment_reader,
        const std::shared_ptr<Store>& store,
        bool dynamic_schema,
        bool ignore_sort_order)
{
    ARCTICDB_SAMPLE_DEFAULT(AppendFrame)
    util::variant_match(frame.index,
                        [&index_segment_reader, &frame, ignore_sort_order](const TimeseriesIndex &) {
                            util::check(frame.has_index(), "Cannot append timeseries without index");
                            util::check(static_cast<bool>(frame.index_tensor), "Got null index tensor in append_frame");
                            auto& frame_index = frame.index_tensor.value();
                            util::check(frame_index.size() > 0, "Cannot append empty frame");
                            util::check(frame_index.data_type() == DataType::NANOSECONDS_UTC64,
                                        "Expected timestamp index in append, got type {}", frame_index.data_type());
                            if (index_segment_reader.tsd().proto().total_rows() != 0) {
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
    auto keys_fut = slice_and_write(frame, slicing, get_partial_key_gen(frame, key), store);
    return std::move(keys_fut)
    .thenValue([dynamic_schema, slices_to_write = std::move(existing_slices), frame = std::move(frame), index_segment_reader = std::move(index_segment_reader), key = std::move(key), &store](auto&& slice_and_keys_to_append) mutable {
        slices_to_write.insert(std::end(slices_to_write), std::make_move_iterator(std::begin(slice_and_keys_to_append)), std::make_move_iterator(std::end(slice_and_keys_to_append)));
        std::sort(std::begin(slices_to_write), std::end(slices_to_write));
        if(dynamic_schema) {
            auto merged_descriptor =
                merge_descriptors(frame.desc, std::vector< std::shared_ptr<FieldCollection>>{ index_segment_reader.tsd().fields_ptr()}, {});
            merged_descriptor.set_sorted(deduce_sorted(index_segment_reader.get_sorted(), frame.desc.get_sorted()));
            auto tsd =
                make_timeseries_descriptor(frame.num_rows + frame.offset, std::move(merged_descriptor), std::move(frame.norm_meta), std::move(frame.user_meta), std::nullopt, std::nullopt, frame.bucketize_dynamic);
            return index::write_index(stream::index_type_from_descriptor(frame.desc), std::move(tsd), std::move(slices_to_write), key, store);
        } else {
            frame.desc.set_sorted(deduce_sorted(index_segment_reader.get_sorted(), frame.desc.get_sorted()));
            return index::write_index(std::move(frame), std::move(slices_to_write), key, store);
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
                                    [&] (StringPool::offset_t offset) {
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
