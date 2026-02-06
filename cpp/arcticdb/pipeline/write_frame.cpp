/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/pipeline/input_frame.hpp>
#include <arcticdb/pipeline/frame_slice.hpp>
#include <arcticdb/pipeline/index_utils.hpp>
#include <arcticdb/pipeline/slicing.hpp>
#include <arcticdb/stream/stream_sink.hpp>
#include <arcticdb/entity/performance_tracing.hpp>
#include <arcticdb/stream/aggregator.hpp>
#include <arcticdb/util/variant.hpp>
#include <arcticdb/pipeline/frame_utils.hpp>
#include <arcticdb/pipeline/write_frame.hpp>
#include <arcticdb/async/task_scheduler.hpp>
#include <arcticdb/async/tasks.hpp>
#include <arcticdb/util/format_date.hpp>

#include <vector>
#include <array>

namespace arcticdb::pipelines {

using namespace arcticdb::entity;
using namespace arcticdb::stream;
namespace ranges = std::ranges;

WriteToSegmentTask::WriteToSegmentTask(
        std::shared_ptr<InputFrame> frame, FrameSlice slice, const SlicingPolicy& slicing,
        folly::Function<PartialKey(const FrameSlice&)>&& partial_key_gen, size_t slice_num_for_column, Index index,
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

std::tuple<PartialKey, SegmentInMemory, FrameSlice> WriteToSegmentTask::operator()() {
    ARCTICDB_SUBSAMPLE_AGG(WriteSliceCopyToSegment)
    slice_.check_magic();
    magic_.check();
    auto key = partial_key_gen_(slice_);
    auto seg = frame_->has_segment() ? slice_segment() : slice_tensors();
    seg.descriptor().set_id(key.stream_id);
    return {std::move(key), std::move(seg), std::move(slice_)};
}

SegmentInMemory WriteToSegmentTask::slice_segment() const {
    const auto& frame = frame_->segment();
    const auto offset = frame_->offset;
    SegmentInMemory seg;
    if (frame.descriptor().index().field_count() > 0) {
        seg.descriptor().set_index({IndexDescriptorImpl::Type::TIMESTAMP, 1});
    } else {
        seg.descriptor().set_index({IndexDescriptorImpl::Type::ROWCOUNT, 0});
    }
    for (size_t col_idx = 0; col_idx < frame.descriptor().index().field_count(); ++col_idx) {
        seg.add_column(
                frame.field(col_idx).name(),
                std::make_shared<Column>(slice_column(frame, col_idx, offset, seg.string_pool()))
        );
    }
    for (size_t col_idx = slice_.columns().first; col_idx < slice_.columns().second; ++col_idx) {
        seg.add_column(
                frame.field(col_idx).name(),
                std::make_shared<Column>(slice_column(frame, col_idx, offset, seg.string_pool()))
        );
    }
    seg.set_row_data((slice_.rows().second - slice_.rows().first) - 1);
    return seg;
}

Column WriteToSegmentTask::slice_column(
        const SegmentInMemory& frame, size_t col_idx, size_t offset, StringPool& string_pool
) const {
    const auto& source_column = frame.column(col_idx);
    const auto type_size = get_type_size(source_column.type().data_type());
    const auto first_pos = (slice_.rows().first - offset) * type_size;
    const auto bytes = ((slice_.rows().second - offset) * type_size) - first_pos;

    if (is_bool_type(source_column.type().data_type())) {
        // Bool columns from arrow come as packed bitsets and are stored in `MemBlockType::EXTERNAL_PACKED` memory.
        // We need special handling to unpack them because our internal representation is unpacked.
        // We do the unpacking inside `WriteToSegmentTask` so the CPU intensive unpacking can happen in parallel.
        Column res(
                make_scalar_type(DataType::BOOL8),
                slice_.rows().diff(),
                AllocationType::PRESIZED,
                Sparsity::NOT_PERMITTED
        );
        res.set_row_data(slice_.rows().diff() - 1);
        auto dest_ptr = res.data().buffer().data();

        const auto& buffer = source_column.data().buffer();
        auto [_, offset_in_block, block_index] = buffer.block_and_offset(first_pos);
        auto pos_in_res = 0u;
        while (pos_in_res < bytes) {
            const auto block = buffer.blocks()[block_index++];
            util::check(
                    block->get_type() == MemBlockType::EXTERNAL_PACKED,
                    "Expected to see a packed external block but got: {}",
                    block->get_type()
            );
            const auto packed_block = static_cast<ExternalPackedMemBlock*>(block);
            auto num_bits = std::min(packed_block->logical_size() - offset_in_block, bytes - pos_in_res);
            auto offset_in_bits = packed_block->shift() + offset_in_block;
            packed_bits_to_buffer(packed_block->data(), num_bits, offset_in_bits, dest_ptr + pos_in_res);
            offset_in_block = 0;
            pos_in_res += num_bits;
        }
        return res;
    }
    // Note that this is O(log(n)) where n is the number of input record batches. We could amortize this across the
    // columns if it proves to be a bottleneck, as the block structure of all of the columns is the same up to
    // multiples of the type size
    const auto byte_blocks_at = source_column.data().buffer().byte_blocks_at(first_pos, bytes);
    if (is_sequence_type(source_column.type().data_type())) {
        const auto& block_offsets = source_column.data().buffer().block_offsets();
        auto first_block_offset = source_column.data().buffer().block_and_offset(first_pos).block_index_;
        auto block_offsets_it = block_offsets.cbegin() + first_block_offset;
        Column res(
                make_scalar_type(DataType::UTF_DYNAMIC64),
                slice_.rows().diff(),
                AllocationType::PRESIZED,
                Sparsity::NOT_PERMITTED
        );
        res.set_row_data(slice_.rows().diff() - 1);
        auto col_data = res.data();
        // String columns use int64_t as the offsets
        auto col_it = col_data.begin<ScalarTagType<DataTypeTag<DataType::INT64>>>();
        details::visit_type(source_column.type().data_type(), [&](auto tag) {
            using type_info = ScalarTypeInfo<decltype(tag)>;
            using Rawtype = type_info::RawType;
            // Already checked the data type above, this is just to reduce code generation
            if constexpr (is_sequence_type(type_info::data_type)) {
                for (const auto& offset_ptr_and_size : byte_blocks_at) {
                    // The extra string buffer is always one block by construction, so use a raw pointer into it to
                    // avoid block_and_offset calculations
                    char* string_data = reinterpret_cast<char*>(
                            source_column.get_extra_buffer(*block_offsets_it++, ExtraBufferType::STRING).data()
                    );
                    auto offset_ptr = reinterpret_cast<const Rawtype*>(offset_ptr_and_size.first);
                    auto num_elements = offset_ptr_and_size.second / sizeof(Rawtype);
                    for (size_t idx = 0; idx < num_elements; ++idx, ++offset_ptr) {
                        auto strings_buffer_offset = *offset_ptr;
                        // Note that in arrow_data_to_segment we store the last value from the offsets buffer as extra
                        // bytes. So when idx == size - 1, (ptr + 1) is still within the ExternalMemBlock.
                        auto string_length = *(offset_ptr + 1) - strings_buffer_offset;
                        std::string_view str(&string_data[strings_buffer_offset], string_length);
                        *col_it++ = string_pool.get(str).offset();
                    }
                }
            }
        });
        return res;
    } else { // Numeric types
        ChunkedBuffer chunked_buffer;
        if (byte_blocks_at.size() == 1) {
            // All required bytes lie within a single block, so we can avoid a copy by adding an external block
            chunked_buffer.add_external_block(byte_blocks_at.front().first, bytes);
        } else {
            // Required bytes span multiple blocks, so we need to memcpy them into a single block for encoding
            chunked_buffer = truncate(source_column.data().buffer(), first_pos, first_pos + bytes);
        }
        return {source_column.type(), Sparsity::NOT_PERMITTED, std::move(chunked_buffer)};
    }
}

SegmentInMemory WriteToSegmentTask::slice_tensors() const {
    return util::variant_match(index_, [this](auto& idx) {
        using IdxType = std::decay_t<decltype(idx)>;
        using SingleSegmentAggregator = Aggregator<IdxType, FixedSchema, NeverSegmentPolicy>;

        SegmentInMemory output;
        SingleSegmentAggregator agg{
                FixedSchema{*slice_.desc(), frame_->index},
                [&output](auto&& segment) { output = std::move(segment); },
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
        if (frame_->desc().index().field_count() > 0) {
            const auto& opt_index_tensor = frame_->opt_index_tensor();
            util::check(opt_index_tensor.has_value(), "Got null index tensor in WriteToSegmentTask");
            auto opt_error = aggregator_set_data(
                    frame_->desc().fields(0).type(),
                    *opt_index_tensor,
                    agg,
                    0,
                    rows_to_write,
                    offset_in_frame,
                    slice_num_for_column_,
                    regular_slice_size,
                    false
            );
            if (opt_error.has_value()) {
                opt_error->raise(frame_->desc().fields(0).name(), offset_in_frame);
            }
        }

        const auto& field_tensors = frame_->field_tensors();
        for (size_t col = 0, end = slice_.col_range.diff(); col < end; ++col) {
            auto abs_col = col + frame_->desc().index().field_count();
            auto& fd = slice_.non_index_field(col);
            auto& tensor = field_tensors[slice_.absolute_field_col(col)];
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

std::vector<std::pair<FrameSlice, size_t>> get_slice_and_rowcount(const std::vector<FrameSlice>& slices) {
    std::vector<std::pair<FrameSlice, size_t>> slice_and_rowcount;
    slice_and_rowcount.reserve(slices.size());
    size_t slice_num_for_column = 0;
    std::optional<size_t> first_row;
    for (const auto& slice : slices) {
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
    return ConfigsMap::instance()->get_int(
            "VersionStore.BatchWriteWindow", int64_t(2 * async::TaskScheduler::instance()->io_thread_count())
    );
}

folly::SemiFuture<std::vector<folly::Try<SliceAndKey>>> write_slices(
        const std::shared_ptr<InputFrame>& frame, std::vector<FrameSlice>&& slices, const SlicingPolicy& slicing,
        TypedStreamVersion&& key, const std::shared_ptr<stream::StreamSink>& sink,
        const std::shared_ptr<DeDupMap>& de_dup_map, bool sparsify_floats
) {
    ARCTICDB_SAMPLE(WriteSlices, 0)

    auto slice_and_rowcount = get_slice_and_rowcount(slices);

    int64_t write_window = write_window_size();
    auto window = folly::window(
            std::move(slice_and_rowcount),
            [de_dup_map, frame, slicing, key = std::move(key), sink, sparsify_floats](auto&& slice) {
                return async::submit_cpu_task(WriteToSegmentTask(
                                                      frame,
                                                      slice.first,
                                                      slicing,
                                                      get_partial_key_gen(frame, key),
                                                      slice.second,
                                                      frame->index,
                                                      sparsify_floats
                                              ))
                        .then([sink, de_dup_map](auto&& ks) {
                            return sink->async_write(std::forward<decltype(ks)>(ks), de_dup_map);
                        });
            },
            write_window
    );
    return folly::collectAll(std::move(window));
}

folly::Future<std::vector<SliceAndKey>> slice_and_write(
        const std::shared_ptr<InputFrame>& frame, const SlicingPolicy& slicing, IndexPartialKey&& key,
        const std::shared_ptr<stream::StreamSink>& sink, const std::shared_ptr<DeDupMap>& de_dup_map,
        bool sparsify_floats
) {
    ARCTICDB_SUBSAMPLE_DEFAULT(SliceFrame)
    auto slices = slice(*frame, slicing);
    if (slices.empty())
        return folly::makeFuture(std::vector<SliceAndKey>{});

    ARCTICDB_SUBSAMPLE_DEFAULT(SliceAndWrite)
    TypedStreamVersion tsv{std::move(key.id), key.version_id, KeyType::TABLE_DATA};
    return write_slices(frame, std::move(slices), slicing, std::move(tsv), sink, de_dup_map, sparsify_floats)
            .via(&async::cpu_executor())
            .thenValue([sink](std::vector<folly::Try<SliceAndKey>>&& ks) {
                return rollback_slices_on_quota_exceeded(std::move(ks), sink);
            })
            .via(&async::io_executor());
}

folly::Future<entity::AtomKey> write_frame(
        IndexPartialKey&& key, const std::shared_ptr<InputFrame>& frame, const SlicingPolicy& slicing,
        const std::shared_ptr<Store>& store, const std::shared_ptr<DeDupMap>& de_dup_map, bool sparsify_floats
) {
    ARCTICDB_SAMPLE_DEFAULT(WriteFrame)
    auto fut_slice_keys = slice_and_write(frame, slicing, IndexPartialKey{key}, store, de_dup_map, sparsify_floats);
    // Write the keys of the slices into an index segment
    ARCTICDB_SUBSAMPLE_DEFAULT(WriteIndex)
    return std::move(fut_slice_keys)
            .thenValue([frame = frame, key = std::move(key), &store](auto&& slice_keys) mutable {
                return index::write_index(frame, std::forward<decltype(slice_keys)>(slice_keys), key, store);
            });
}

folly::Future<entity::AtomKey> append_frame(
        IndexPartialKey&& key, const std::shared_ptr<InputFrame>& frame, const SlicingPolicy& slicing,
        index::IndexSegmentReader& index_segment_reader, const std::shared_ptr<Store>& store, bool dynamic_schema,
        bool ignore_sort_order
) {
    ARCTICDB_SAMPLE_DEFAULT(AppendFrame)
    util::variant_match(
            frame->index,
            [&index_segment_reader, &frame, ignore_sort_order](const TimeseriesIndex&) {
                util::check(frame->has_index(), "Cannot append timeseries without index");
                if (index_segment_reader.tsd().total_rows() != 0 && frame->num_rows != 0) {
                    auto first_index = NumericIndex{frame->index_value_at(0)};
                    auto prev = std::get<NumericIndex>(index_segment_reader.last()->key().end_index());
                    util::check(
                            ignore_sort_order || prev - 1 <= first_index,
                            "Can't append dataframe with start index {} to existing sequence ending at {}",
                            util::format_timestamp(first_index),
                            util::format_timestamp(prev)
                    );
                }
            },
            [](const auto&) {
                // Do whatever, but you can't range search it
            }
    );

    auto existing_slices = unfiltered_index(index_segment_reader);
    auto keys_fut = slice_and_write(frame, slicing, IndexPartialKey{key}, store);
    return std::move(keys_fut).thenValue([dynamic_schema,
                                          slices_to_write = std::move(existing_slices),
                                          frame = frame,
                                          index_segment_reader = std::move(index_segment_reader),
                                          key = std::move(key),
                                          store](auto&& slice_and_keys_to_append) mutable {
        slices_to_write.insert(
                std::end(slices_to_write),
                std::make_move_iterator(std::begin(slice_and_keys_to_append)),
                std::make_move_iterator(std::end(slice_and_keys_to_append))
        );
        std::sort(std::begin(slices_to_write), std::end(slices_to_write));
        auto tsd = index::get_merged_tsd(
                frame->num_rows + frame->offset, dynamic_schema, index_segment_reader.tsd(), frame
        );
        return index::write_index(
                stream::index_type_from_descriptor(tsd.as_stream_descriptor()),
                tsd,
                std::move(slices_to_write),
                key,
                store
        );
    });
}

/// @brief Find the row range of affected rows during a partial rewrite (on update)
/// During partial rewrite the segment is either affected from the beginning to a
/// certain row or from a certain row to the end. Thus the row range will always be
/// either [0, row) or [row, end).
static RowRange partial_rewrite_row_range(
        const SegmentInMemory& segment, const IndexRange& range, AffectedSegmentPart affected_end
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

folly::Future<SliceAndKey> async_rewrite_partial_segment(
        const SliceAndKey& existing, const IndexRange& index_range, VersionId version_id,
        AffectedSegmentPart affected_part, const std::shared_ptr<Store>& store
) {
    return store->read(existing.key())
            .thenValueInline(
                    [existing, index_range, version_id, affected_part, store](
                            std::pair<VariantKey, SegmentInMemory>&& key_segment
                    ) -> folly::Future<SliceAndKey> {
                        const auto& key = existing.key();
                        const SegmentInMemory& segment = key_segment.second;
                        const RowRange affected_row_range =
                                partial_rewrite_row_range(segment, index_range, affected_part);
                        const auto num_rows = int64_t(affected_row_range.end() - affected_row_range.start());
                        if (num_rows <= 0) {
                            return folly::Try<SliceAndKey>{}; // Empty future
                        }
                        SegmentInMemory output =
                                segment.truncate(affected_row_range.start(), affected_row_range.end(), true);
                        const IndexValue start_ts = TimeseriesIndex::start_value_for_segment(output);
                        // +1 as in the key we store one nanosecond greater than the last index value in the segment
                        const IndexValue end_ts =
                                std::get<NumericIndex>(TimeseriesIndex::end_value_for_segment(output)) + 1;
                        FrameSlice new_slice{
                                std::make_shared<StreamDescriptor>(output.descriptor()),
                                existing.slice_.col_range,
                                RowRange{0, num_rows},
                                existing.slice_.hash_bucket(),
                                existing.slice_.num_buckets()
                        };
                        return store->write(key.type(), version_id, key.id(), start_ts, end_ts, std::move(output))
                                .thenValueInline([new_slice = std::move(new_slice)](VariantKey&& k) {
                                    return SliceAndKey{new_slice, std::get<AtomKey>(std::move(k))};
                                });
                    }
            );
}

std::vector<SliceAndKey> flatten_and_fix_rows(
        const std::array<std::vector<SliceAndKey>, 5>& groups, size_t& global_count
) {
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

folly::Future<StreamSink::RemoveKeyResultType> remove_slice_and_keys(
        std::vector<SliceAndKey>&& slices, StreamSink& sink
) {
    std::vector<VariantKey> keys;
    std::transform(
            std::make_move_iterator(std::begin(slices)),
            std::make_move_iterator(std::end(slices)),
            std::back_inserter(keys),
            [](SliceAndKey&& key) { return std::move(key).key(); }
    );
    return sink.remove_keys(std::move(keys)).thenValue([](auto&&) { return folly::makeFuture(); });
}

folly::Future<StreamSink::RemoveKeyResultType> remove_slice_and_keys_batches(
        std::vector<std::vector<SliceAndKey>>&& slices_batches, StreamSink& sink
) {
    return folly::collect(folly::window(
                                  std::move(slices_batches),
                                  [&sink](std::vector<SliceAndKey>&& slice_keys) {
                                      return remove_slice_and_keys(std::move(slice_keys), sink);
                                  },
                                  write_window_size()
                          ))
            .via(&async::io_executor())
            .thenValue([](auto&&) { return folly::makeFuture(); });
};

folly::SemiFuture<std::vector<SliceAndKey>> rollback_slices_on_quota_exceeded(
        std::vector<folly::Try<SliceAndKey>>&& try_slices, const std::shared_ptr<StreamSink>& sink
) {
    auto remove_slice_and_keys_func = [sink](std::vector<SliceAndKey>&& slice_keys) {
        return remove_slice_and_keys(std::move(slice_keys), *sink);
    };

    return rollback_on_quota_exceeded<SliceAndKey>(std::move(try_slices), std::move(remove_slice_and_keys_func));
}

folly::SemiFuture<std::vector<std::vector<SliceAndKey>>> rollback_batches_on_quota_exceeded(
        std::vector<folly::Try<std::vector<SliceAndKey>>>&& try_slice_batches,
        const std::shared_ptr<stream::StreamSink>& sink
) {
    auto remove_keys_func = [sink](std::vector<std::vector<SliceAndKey>>&& slice_keys_per_batch) {
        return remove_slice_and_keys_batches(std::move(slice_keys_per_batch), *sink);
    };

    return rollback_on_quota_exceeded<std::vector<SliceAndKey>>(
            std::move(try_slice_batches), std::move(remove_keys_func)
    );
}
} // namespace arcticdb::pipelines
