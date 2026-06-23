/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include "codec/segment.hpp"
#include "column_store/memory_segment.hpp"
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

#include <ankerl/unordered_dense.h>

#include <vector>
#include <array>

namespace arcticdb::pipelines {

using namespace arcticdb::entity;
using namespace arcticdb::stream;
namespace ranges = std::ranges;

WriteToSegmentTask::WriteToSegmentTask(
        std::shared_ptr<InputFrame> frame, FrameSlice slice,
        const std::optional<TypedStreamVersion>& typed_stream_version, bool sparsify_floats
) :
    frame_(std::move(frame)),
    slice_(std::move(slice)),
    typed_stream_version_(typed_stream_version),
    sparsify_floats_(sparsify_floats) {
    slice_.check_magic();
}

std::tuple<PartialKey, SegmentInMemory, FrameSlice> WriteToSegmentTask::operator()() {
    ARCTICDB_SUBSAMPLE_AGG(WriteSliceCopyToSegment)
    slice_.check_magic();
    magic_.check();
    auto seg = slice();
    auto key = generate_partial_key(seg);
    seg.descriptor().set_id(key.stream_id);
    return {std::move(key), std::move(seg), std::move(slice_)};
}

PartialKey WriteToSegmentTask::generate_partial_key(const SegmentInMemory& seg) const {
    if (!typed_stream_version_.has_value()) {
        return {};
    }
    const auto start_index = frame_->has_index()
                                     ? *seg.column(0).scalar_at<timestamp>(0)
                                     : entity::safe_convert_to_numeric_index(slice_.row_range.first, "Rows");
    const auto end_index = frame_->has_index()
                                   ? end_index_generator(*seg.column(0).scalar_at<timestamp>(seg.row_count() - 1))
                                   : entity::safe_convert_to_numeric_index(slice_.row_range.second, "Rows");
    return {typed_stream_version_->type,
            typed_stream_version_->version_id,
            typed_stream_version_->id,
            start_index,
            end_index};
}

struct ArrowInputContiguousSlice {
    // Both start_pos and size refer to logical pos and size in the respective blocks
    // E.g if we have a 40 byte block consisting of 5 int64s and we care about the middle 3 values we would have
    // start_pos = 1 and size = 3
    size_t start_pos;
    size_t size;
    IMemBlock* block;
    std::optional<ExternalPackedMemBlock*> bitmap_block;
    std::optional<ExternalMemBlock*> offsets_block;
    std::optional<ExternalMemBlock*> strings_block;
};

std::vector<ArrowInputContiguousSlice> arrow_contiguous_slices_in_range(const Column& col, size_t from, size_t to) {
    auto slices = std::vector<ArrowInputContiguousSlice>();
    const auto& buffer = col.buffer();
    const auto type_size = get_type_size(col.type().data_type());
    const auto from_byte = from * type_size;
    const auto to_byte = to * type_size;

    // Note that this is O(log(n)) where n is the number of input record batches. We could amortize this across the
    // columns if it proves to be a bottleneck, as the block structure of all of the columns is the same up to
    // multiples of the type size
    auto [_, offset_in_block, block_index] = buffer.block_and_offset(from_byte);
    auto current_byte = from_byte;
    while (current_byte < to_byte) {
        const auto block_offset = buffer.block_offsets()[block_index];
        const auto block = buffer.blocks()[block_index++];
        auto num_bytes_in_arrow_slice = std::min(block->logical_size() - offset_in_block, to_byte - current_byte);
        auto slice = ArrowInputContiguousSlice{
                .start_pos = offset_in_block / type_size,
                .size = num_bytes_in_arrow_slice / type_size,
                .block = block,
                .bitmap_block = std::nullopt,
                .offsets_block = std::nullopt,
                .strings_block = std::nullopt
        };
        if (col.has_extra_buffer(block_offset, ExtraBufferType::BITMAP)) {
            const auto& bitmap_buffer = col.get_extra_buffer(block_offset, ExtraBufferType::BITMAP);
            util::check(
                    bitmap_buffer.num_blocks() == 1,
                    "Expected exactly one bitmap block but got {}",
                    bitmap_buffer.num_blocks()
            );
            const auto bitmap_block = bitmap_buffer.blocks()[0];
            util::check(
                    bitmap_block->get_type() == MemBlockType::EXTERNAL_PACKED,
                    "Expected to see a packed external block but got: {}",
                    bitmap_block->get_type()
            );
            slice.bitmap_block = static_cast<ExternalPackedMemBlock*>(bitmap_block);
        }
        if (col.has_extra_buffer(block_offset, ExtraBufferType::OFFSET)) {
            const auto& offsets_buffer = col.get_extra_buffer(block_offset, ExtraBufferType::OFFSET);
            util::check(
                    offsets_buffer.num_blocks() == 1,
                    "Expected exactly one offsets block but got {}",
                    offsets_buffer.num_blocks()
            );
            auto offsets_block = offsets_buffer.blocks()[0];
            util::check(
                    offsets_block->get_type() == MemBlockType::EXTERNAL_WITH_EXTRA_BYTES,
                    "Expected to see an external block but got: {}",
                    offsets_block->get_type()
            );
            slice.offsets_block = static_cast<ExternalMemBlock*>(offsets_block);
        }
        if (col.has_extra_buffer(block_offset, ExtraBufferType::STRING)) {
            const auto& strings_buffer = col.get_extra_buffer(block_offset, ExtraBufferType::STRING);
            util::check(
                    strings_buffer.num_blocks() == 1,
                    "Expected exactly one strings block but got {}",
                    strings_buffer.num_blocks()
            );
            auto strings_block = strings_buffer.blocks()[0];
            util::check(
                    strings_block->get_type() == MemBlockType::EXTERNAL_WITH_EXTRA_BYTES,
                    "Expected to see an external block but got: {}",
                    strings_block->get_type()
            );
            slice.strings_block = static_cast<ExternalMemBlock*>(strings_block);
        }
        slices.emplace_back(std::move(slice));
        current_byte += num_bytes_in_arrow_slice;
        offset_in_block = 0;
    }
    return slices;
}

util::BitSet construct_sparse_map(const std::vector<ArrowInputContiguousSlice>& slices, size_t num_rows) {
    // Construct sparse map by populating inverse bitset
    // This way construction for fully dense columns is fast
    util::BitSet dest_bitset;
    util::BitSet::bulk_insert_iterator inserter(dest_bitset);
    auto dest_pos = 0u;
    for (const auto& slice : slices) {
        if (slice.bitmap_block.has_value()) {
            for (auto i = 0u; i < slice.size; ++i) {
                auto pos_in_bitmap_block = slice.bitmap_block.value()->shift() + slice.start_pos + i;
                auto is_set = get_bit_at(slice.bitmap_block.value()->data(), pos_in_bitmap_block);
                if (!is_set) {
                    inserter = dest_pos + i;
                }
            }
        }
        dest_pos += slice.size;
    }
    inserter.flush();
    dest_bitset.invert();
    dest_bitset.resize(num_rows);
    return dest_bitset;
}

template<typename DestTypeInfo>
requires(!is_bool_type(DestTypeInfo::data_type) && !is_sequence_type(DestTypeInfo::data_type))
void merge_arrow_slices_into_column(
        const Column& source_column, const std::vector<ArrowInputContiguousSlice>& slices, Column& dest,
        [[maybe_unused]] StringPool& string_pool
) {
    using DestType = typename DestTypeInfo::RawType;
    const auto type_size = get_type_size(source_column.type().data_type());
    auto dest_dense_ptr = reinterpret_cast<DestType*>(dest.data().buffer().data());
    auto logical_pos = 0u;
    for (const auto& slice : slices) {
        if (slice.bitmap_block.has_value() && dest.is_sparse()) {
            // We need to iterate over sparse only when the slice is sparse and destination is sparse
            iterate_over_set_positions(
                    dest.sparse_map(),
                    logical_pos,
                    logical_pos + slice.size,
                    [&] ARCTICDB_LAMBDA_INLINE(size_t pos) {
                        auto pos_in_block = (pos - logical_pos + slice.start_pos) * type_size;
                        *dest_dense_ptr++ = *reinterpret_cast<DestType*>(slice.block->ptr(pos_in_block));
                    }
            );
        } else {
            // If slice is non sparse we can memcpy the entire slice
            memcpy(dest_dense_ptr, slice.block->ptr(slice.start_pos * type_size), slice.size * type_size);
            dest_dense_ptr += slice.size;
        }
        logical_pos += slice.size;
    }
}

template<typename DestTypeInfo>
requires(is_bool_type(DestTypeInfo::data_type))
void merge_arrow_slices_into_column(
        [[maybe_unused]] const Column& source_column, const std::vector<ArrowInputContiguousSlice>& slices,
        Column& dest, [[maybe_unused]] StringPool& string_pool
) {
    using DestType = typename DestTypeInfo::RawType;
    auto dest_dense_ptr = reinterpret_cast<DestType*>(dest.data().buffer().data());
    // Bool columns from arrow come as packed bitsets and are stored in `MemBlockType::EXTERNAL_PACKED` memory.
    // We need special handling to unpack them because our internal representation is unpacked.
    // We do the unpacking inside `WriteToSegmentTask` so the CPU intensive unpacking can happen in parallel.
    auto logical_pos = 0u;
    for (const auto& slice : slices) {
        util::check(
                slice.block->get_type() == MemBlockType::EXTERNAL_PACKED,
                "Expected to see a packed external block but got: {}",
                slice.block->get_type()
        );
        const auto packed_block = static_cast<ExternalPackedMemBlock*>(slice.block);
        if (slice.bitmap_block.has_value() && dest.is_sparse()) {
            // We need to iterate over sparse only when the slice is sparse and destination is sparse
            iterate_over_set_positions(
                    dest.sparse_map(),
                    logical_pos,
                    logical_pos + slice.size,
                    [&] ARCTICDB_LAMBDA_INLINE(size_t pos) {
                        auto bit_in_block = (pos - logical_pos) + slice.start_pos + packed_block->shift();
                        *dest_dense_ptr++ = get_bit_at(packed_block->data(), bit_in_block);
                    }
            );
        } else {
            // If slice is dense we can use the more efficient
            packed_bits_to_buffer(
                    packed_block->data(),
                    slice.size,
                    slice.start_pos + packed_block->shift(),
                    reinterpret_cast<uint8_t*>(dest_dense_ptr)
            );
            dest_dense_ptr += slice.size;
        }
        logical_pos += slice.size;
    }
}

template<typename DestTypeInfo>
requires(is_sequence_type(DestTypeInfo::data_type))
void merge_arrow_slices_into_column(
        const Column& source_column, const std::vector<ArrowInputContiguousSlice>& slices, Column& dest,
        StringPool& string_pool
) {
    using DestType = typename DestTypeInfo::RawType;
    auto dest_dense_ptr = reinterpret_cast<DestType*>(dest.data().buffer().data());
    details::visit_type(source_column.type().data_type(), [&](auto tag) {
        using source_type_info = ScalarTypeInfo<decltype(tag)>;
        using SourceType = typename source_type_info::RawType;
        // Already checked the data type above, this is just to reduce code generation
        if constexpr (is_sequence_type(source_type_info::data_type)) {
            auto logical_pos = 0u;
            for (const auto& slice : slices) {
                util::check(slice.strings_block.has_value(), "Expected to have strings block in strings arrow column");
                auto strings = reinterpret_cast<const char*>(slice.strings_block.value()->data());

                auto apply_to_every_set_position_in_slice = [&](auto&& f) {
                    if (slice.bitmap_block.has_value() && dest.is_sparse()) {
                        iterate_over_set_positions(
                                dest.sparse_map(), logical_pos, logical_pos + slice.size, std::forward<decltype(f)>(f)
                        );
                    } else {
                        for (auto pos = logical_pos; pos < logical_pos + slice.size; ++pos) {
                            f(pos);
                        }
                    }
                };

                if (slice.offsets_block.has_value()) {
                    // Dictionary-encoded strings
                    // keys can be int32 >= 0 (from pyarrow) or uint32 (from polars). reinterpret_cast<uint32_t*> works
                    // for both
                    auto keys = reinterpret_cast<const uint32_t*>(slice.block->data());
                    auto offsets = reinterpret_cast<const int64_t*>(slice.offsets_block.value()->data());
                    // Use the pool_offsets cache to not repeatedly call `string_pool.get` on duplicate strings
                    ankerl::unordered_dense::map<uint32_t, DestType> pool_offsets;
                    auto add_dense_string = [&] ARCTICDB_LAMBDA_INLINE(size_t pos) {
                        auto key = keys[(pos - logical_pos) + slice.start_pos];
                        if (auto it = pool_offsets.find(key); it != pool_offsets.end()) {
                            *dest_dense_ptr++ = it->second;
                        } else {
                            auto string_start = offsets[key];
                            auto string_length = offsets[key + 1] - string_start;
                            std::string_view str(&strings[string_start], string_length);
                            auto dest_offset = static_cast<DestType>(string_pool.get(str).offset());
                            pool_offsets.emplace(key, dest_offset);
                            *dest_dense_ptr++ = dest_offset;
                        }
                    };
                    apply_to_every_set_position_in_slice(std::move(add_dense_string));
                } else {
                    // Variable-length strings
                    util::check(
                            slice.block->physical_bytes() == slice.block->logical_size() + sizeof(SourceType),
                            "Expected offsets buffer to have one extra offset value but instead got physical: {}, "
                            "logical: {}",
                            slice.block->physical_bytes(),
                            slice.block->logical_size()
                    );
                    auto offsets = reinterpret_cast<const SourceType*>(slice.block->data());
                    auto add_dense_string = [&] ARCTICDB_LAMBDA_INLINE(size_t pos) {
                        auto pos_in_offsets = (pos - logical_pos) + slice.start_pos;
                        auto strings_buffer_offset = offsets[pos_in_offsets];
                        // offsets[pos_in_offsets + 1] is in bounds because the block carries one extra offset.
                        auto string_length = offsets[pos_in_offsets + 1] - strings_buffer_offset;
                        std::string_view str(&strings[strings_buffer_offset], string_length);
                        *dest_dense_ptr++ = string_pool.get(str).offset();
                    };
                    apply_to_every_set_position_in_slice(std::move(add_dense_string));
                }
                logical_pos += slice.size;
            }
        }
    });
}

Column WriteToSegmentTask::slice_column(const Column& source_column, size_t offset, StringPool& string_pool) const {
    const auto type_size = get_type_size(source_column.type().data_type());
    const auto begin_pos = (slice_.rows().first - offset);
    const auto end_pos = (slice_.rows().second - offset);

    auto contiguous_slices = arrow_contiguous_slices_in_range(source_column, begin_pos, end_pos);

    auto dest_bitset = construct_sparse_map(contiguous_slices, slice_.rows().diff());
    const auto num_set = dest_bitset.count();
    const auto num_nulls = slice_.rows().diff() - num_set;

    auto dest_column_type = source_column.type();
    if (is_bool_type(source_column.type().data_type())) {
        dest_column_type = make_scalar_type(DataType::BOOL8);
    } else if (is_sequence_type(source_column.type().data_type())) {
        dest_column_type = make_scalar_type(DataType::UTF_DYNAMIC64);
    } else if (num_nulls == 0 && contiguous_slices.size() == 1) {
        // Dense numeric column consisting of a single contiguous slice of memory is the only
        // case where we can zero copy construct the result column.
        const auto& slice = contiguous_slices.front();
        ChunkedBuffer chunked_buffer;
        chunked_buffer.add_external_block(slice.block->ptr(slice.start_pos * type_size), slice.size * type_size);
        Column dest{source_column.type(), Sparsity::NOT_PERMITTED, std::move(chunked_buffer)};
        dest.set_row_data(slice_.rows().diff() - 1);
        return dest;
    }
    Column dest(
            dest_column_type,
            num_set,
            AllocationType::PRESIZED,
            num_nulls == 0 ? Sparsity::NOT_PERMITTED : Sparsity::PERMITTED
    );
    if (num_nulls > 0) {
        dest.set_sparse_map(std::move(dest_bitset));
    }
    dest.set_row_data(slice_.rows().diff() - 1);

    details::visit_type(dest.type().data_type(), [&](auto tag) {
        using dest_type_info = ScalarTypeInfo<decltype(tag)>;
        merge_arrow_slices_into_column<dest_type_info>(source_column, contiguous_slices, dest, string_pool);
    });
    return dest;
}

SegmentInMemory WriteToSegmentTask::slice() const {
    SegmentInMemory seg;
    seg.descriptor().set_index(slice_.desc()->index());

    const auto offset_in_frame = slice_begin_pos(slice_, *frame_);
    seg.set_offset(offset_in_frame);
    const auto rows_to_write = slice_.row_range.second - slice_.row_range.first;
    const auto index_field_count = frame_->desc().index().field_count();

    auto add_tensor_column = [&](size_t abs_col, const NativeTensor& tensor, const Field& fd, bool sparsify) {
        seg.add_column(
                FieldRef{fd.type(), fd.name()},
                std::make_shared<Column>(fd.type(), 0, AllocationType::DYNAMIC, Sparsity::NOT_PERMITTED)
        );
        auto opt_error = segment_set_data(fd.type(), tensor, seg, abs_col, rows_to_write, offset_in_frame, sparsify);
        if (opt_error.has_value()) {
            opt_error->raise(fd.name(), offset_in_frame);
        }
    };

    auto add_arrow_column = [&](const Column& source_column, const Field& fd) {
        seg.add_column(
                fd.name(), std::make_shared<Column>(slice_column(source_column, frame_->offset, seg.string_pool()))
        );
    };

    // Index column
    if (frame_->has_index()) {
        const auto& fd = frame_->desc().fields(0);
        util::variant_match(
                frame_->field_data(0),
                [&](const NativeTensor& tensor) { add_tensor_column(0, tensor, fd, false); },
                [&](const Column& source_column) { add_arrow_column(source_column, fd); }
        );
    }

    // Non-index columns
    for (size_t col = 0, end = slice_.col_range.diff(); col < end; ++col) {
        const auto abs_col = col + index_field_count;
        const auto& fd = slice_.non_index_field(col);
        const auto col_idx = slice_.absolute_field_col(col) + index_field_count;
        util::variant_match(
                frame_->field_data(col_idx),
                [&](const NativeTensor& tensor) { add_tensor_column(abs_col, tensor, fd, sparsify_floats_); },
                [&](const Column& source_column) { add_arrow_column(source_column, fd); }
        );
    }

    seg.end_block_write(rows_to_write);

    if (ConfigsMap().instance()->get_int("Statistics.GenerateOnWrite", 0) == 1)
        seg.calculate_statistics();

    return seg;
}

int64_t write_window_size() {
    return ConfigsMap::instance()->get_int(
            "VersionStore.BatchWriteWindow", int64_t(2 * async::TaskScheduler::instance()->io_thread_count())
    );
}

folly::SemiFuture<std::vector<folly::Try<SliceAndKey>>> write_slices(
        const std::shared_ptr<InputFrame>& frame, std::vector<FrameSlice>&& slices, TypedStreamVersion&& key,
        const std::shared_ptr<stream::StreamSink>& sink, const std::shared_ptr<DeDupMap>& de_dup_map,
        bool sparsify_floats
) {
    ARCTICDB_SAMPLE(WriteSlices, 0)

    int64_t write_window = write_window_size();
    auto window = folly::window(
            std::move(slices),
            [de_dup_map, frame, key = std::move(key), sink, sparsify_floats](auto&& slice) {
                return async::submit_cpu_task(WriteToSegmentTask(frame, slice, key, sparsify_floats))
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
    return write_slices(frame, std::move(slices), std::move(tsv), sink, de_dup_map, sparsify_floats)
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

folly::Future<AtomKey> append_frame(
        IndexPartialKey&& key, const std::shared_ptr<InputFrame>& frame, const SlicingPolicy& slicing,
        index::IndexSegmentReader& index_segment_reader, const std::shared_ptr<Store>& store, bool dynamic_schema
) {
    ARCTICDB_SAMPLE_DEFAULT(AppendFrame)
    auto existing_slices = unfiltered_index(index_segment_reader);
    const auto tsd =
            index::get_merged_tsd(frame->num_rows + frame->offset, dynamic_schema, index_segment_reader.tsd(), frame);
    auto keys_fut = slice_and_write(frame, slicing, IndexPartialKey{key}, store);
    return std::move(keys_fut).thenValue([tsd = std::move(tsd),
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
        ranges::sort(slices_to_write);
        return index::write_index(
                index_type_from_descriptor(tsd.as_stream_descriptor()), tsd, std::move(slices_to_write), key, store
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

folly::Future<folly::Unit> remove_slice_and_keys(std::vector<SliceAndKey>&& slices, StreamSink& sink) {
    std::vector<VariantKey> keys;
    std::transform(
            std::make_move_iterator(std::begin(slices)),
            std::make_move_iterator(std::end(slices)),
            std::back_inserter(keys),
            [](SliceAndKey&& key) { return std::move(key).key(); }
    );
    return sink.remove_keys(std::move(keys)).thenValue([](auto&&) { return folly::makeFuture(); });
}

folly::Future<folly::Unit> remove_slice_and_keys_batches(
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
