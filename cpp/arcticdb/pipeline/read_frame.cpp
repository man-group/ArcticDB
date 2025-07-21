/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/util/variant.hpp>

#include <arcticdb/codec/encoding_sizes.hpp>
#include <arcticdb/codec/codec.hpp>
#include <arcticdb/column_store/string_pool.hpp>
#include <arcticdb/pipeline/read_frame.hpp>
#include <arcticdb/pipeline/pipeline_context.hpp>
#include <arcticdb/pipeline/frame_utils.hpp>
#include <arcticdb/pipeline/frame_slice_map.hpp>
#include <arcticdb/async/task_scheduler.hpp>
#include <arcticdb/util/encoding_conversion.hpp>
#include <arcticdb/util/type_handler.hpp>
#include <arcticdb/entity/type_utils.hpp>
#include <arcticdb/codec/slice_data_sink.hpp>
#include <arcticdb/storage/store.hpp>
#include <arcticdb/stream/index.hpp>
#include <arcticdb/pipeline/column_mapping.hpp>
#include <arcticdb/util/magic_num.hpp>
#include <arcticdb/codec/segment_identifier.hpp>
#include <arcticdb/pipeline/string_reducers.hpp>
#include <arcticdb/pipeline/read_query.hpp>

#include <ankerl/unordered_dense.h>


namespace arcticdb::pipelines {

/*
   _  _  _
  |i||_||_|    We mark the first slice of each row of the grid as requiring a fetch_index
  |i||_||_|    This helps us calculate the row_count during allocation. Note that all slices
  |i||_||_|    contain the relevant index, but we only want to count it once per grid row.

 */
void mark_index_slices(const std::shared_ptr<PipelineContext>& context) {
    context->fetch_index_ = check_and_mark_slices(
        context->slice_and_keys_,
        true,
        context->incompletes_after_).value();
}

StreamDescriptor get_filtered_descriptor(StreamDescriptor&& descriptor, OutputFormat output_format, const std::shared_ptr<FieldCollection>& filter_columns) {
    // We assume here that filter_columns_ will always contain the index.

    auto desc = std::move(descriptor);
    auto index = stream::index_type_from_descriptor(desc);
    return util::variant_match(index, [&desc, &filter_columns, output_format] (const auto& idx) {
        const std::shared_ptr<FieldCollection>& fields = filter_columns ? filter_columns : desc.fields_ptr();
        auto handlers = TypeHandlerRegistry::instance();

        for(auto& field : *fields) {
            if(auto handler = handlers->get_handler(output_format, field.type())) {
                auto output_type =  handler->output_type(field.type());
                if(output_type != field.type())
                    field.mutable_type() = output_type;
            }
        }

        return StreamDescriptor{index_descriptor_from_range(desc.id(), idx, *fields)};
    });
}

StreamDescriptor get_filtered_descriptor(const std::shared_ptr<PipelineContext>& context, OutputFormat output_format) {
    return get_filtered_descriptor(context->descriptor().clone(), output_format, context->filter_columns_);
}

void handle_modified_descriptor(const std::shared_ptr<PipelineContext>& context, SegmentInMemory& output) {
    if(context->orig_desc_) {
        for(const auto& field : context->orig_desc_.value().fields()) {
            auto col_index = output.column_index(field.name());
            if(!col_index)
                continue;

            auto& column = output.column(static_cast<position_t>(*col_index));
            if(field.type().data_type() != column.type().data_type())
                column.set_orig_type(field.type());
        }
    }
}

void finalize_segment_setup(SegmentInMemory& output, size_t offset, size_t row_count, const std::shared_ptr<PipelineContext>& context) {
    output.set_offset(static_cast<position_t>(offset));
    output.set_row_data(static_cast<ssize_t>(row_count - 1));
    output.init_column_map();
    handle_modified_descriptor(context, output);
}

SegmentInMemory allocate_chunked_frame(const std::shared_ptr<PipelineContext>& context, OutputFormat output_format) {
    ARCTICDB_SAMPLE_DEFAULT(AllocContiguousFrame)
    auto [offset, row_count] = offset_and_row_count(context);
    auto block_row_counts = output_block_row_counts(context);
    ARCTICDB_DEBUG(log::version(), "Allocated chunked frame with offset {} and row count {}", offset, row_count);
    SegmentInMemory output{get_filtered_descriptor(context, output_format), 0, AllocationType::DETACHABLE, Sparsity::NOT_PERMITTED, output_format, DataTypeMode::EXTERNAL};
    auto handlers = TypeHandlerRegistry::instance();

    for(auto& column : output.columns()) {
        auto handler = handlers->get_handler(output_format, column->type());
        const auto data_size = data_type_size(column->type(), output_format, DataTypeMode::EXTERNAL);
        for(auto block_row_count : block_row_counts) {
            const auto bytes = block_row_count * data_size;
            column->allocate_data(bytes);
            column->advance_data(bytes);
        }
    }

    finalize_segment_setup(output, offset, row_count, context);
    return output;
}

SegmentInMemory allocate_contiguous_frame(const std::shared_ptr<PipelineContext>& context, OutputFormat output_format) {
    ARCTICDB_SAMPLE_DEFAULT(AllocChunkedFrame)
    auto [offset, row_count] = offset_and_row_count(context);
    SegmentInMemory output{get_filtered_descriptor(context, output_format), row_count, AllocationType::DETACHABLE, Sparsity::NOT_PERMITTED, output_format, DataTypeMode::EXTERNAL};
    finalize_segment_setup(output, offset, row_count, context);
    return output;
}

SegmentInMemory allocate_frame(const std::shared_ptr<PipelineContext>& context, OutputFormat output_format) {
   if(output_format == OutputFormat::ARROW)
       return allocate_chunked_frame(context, output_format);
   else
       return allocate_contiguous_frame(context, output_format);
}

size_t get_index_field_count(const SegmentInMemory& frame) {
    return frame.descriptor().index().field_count();
}

const uint8_t* skip_heading_fields(const SegmentHeader & hdr, const uint8_t*& data) {
    const auto has_magic_numbers = hdr.encoding_version() == EncodingVersion::V2;
    const auto start [[maybe_unused]] = data;
    if(has_magic_numbers)
        util::check_magic<MetadataMagic>(data);

    if (hdr.has_metadata_field()) {
        auto metadata_size = encoding_sizes::ndarray_field_compressed_size(hdr.metadata_field().ndarray());
        ARCTICDB_DEBUG(log::version(), "Skipping {} bytes of metadata", metadata_size);
        data += metadata_size;
    }

    if(has_magic_numbers) {
        util::check_magic<SegmentDescriptorMagic>(data);
        data += sizeof(SegmentDescriptor);
        skip_identifier(data);
        util::check_magic<DescriptorFieldsMagic>(data);
    }

    if(hdr.has_descriptor_field()) {
        auto descriptor_field_size = encoding_sizes::ndarray_field_compressed_size(hdr.descriptor_field().ndarray());
        ARCTICDB_DEBUG(log::version(), "Skipping {} bytes of descriptor", descriptor_field_size);
        data += descriptor_field_size;
    }

    if(has_magic_numbers)
        util::check_magic<IndexMagic>(data);

    if(hdr.has_index_descriptor_field()) {
        auto index_fields_size = encoding_sizes::ndarray_field_compressed_size(hdr.index_descriptor_field().ndarray());
        ARCTICDB_DEBUG(log::version(), "Skipping {} bytes of index descriptor", index_fields_size);
            data += index_fields_size;
    }
    ARCTICDB_DEBUG(log::version(), "Skip header fields skipped {} bytes", data - start);
    return data;
}

const uint8_t* skip_to_string_pool(const SegmentHeader & hdr, const uint8_t* data) {
    const uint8_t* output = data;
    const auto& body_fields = hdr.body_fields();
    const auto magic_number_size = hdr.encoding_version() == EncodingVersion::V2 ? sizeof(ColumnMagic) : 0;
    for(auto i = 0U; i < body_fields.size(); ++i)
        output += encoding_sizes::field_compressed_size(hdr.body_fields().at(i)) + magic_number_size;

    return output;
}

void decode_string_pool(const SegmentHeader& hdr, const uint8_t*& data, const uint8_t *begin ARCTICDB_UNUSED, const uint8_t* end, PipelineContextRow &context) {
    if (hdr.has_string_pool_field()) {
        ARCTICDB_DEBUG(log::codec(), "Decoding string pool at position: {}", data - begin);
        util::check(data != end, "Reached end of input block with string pool fields to decode");
        context.allocate_string_pool();
        std::optional<util::BitMagic> bv;

        // Note that this will decode the entire string pool into a ChunkedBuffer with exactly 1 chunk
        if(EncodingVersion(hdr.encoding_version()) == EncodingVersion::V2)
            util::check_magic<StringPoolMagic>(data);

        util::check(hdr.string_pool_field().has_ndarray(), "Expected string pool field to be ndarray");
        data += decode_ndarray(string_pool_descriptor().type(),
                       hdr.string_pool_field().ndarray(),
                       data,
                       context.string_pool(),
                       bv,
                       hdr.encoding_version());

        ARCTICDB_TRACE(log::codec(), "Decoded string pool to position {}", data - begin);
    }
}

void decode_index_field(
        SegmentInMemory &frame,
        const EncodedFieldImpl& field,
        const uint8_t*& data,
        const uint8_t *begin ARCTICDB_UNUSED,
        const uint8_t* end ARCTICDB_UNUSED,
        PipelineContextRow &context,
        EncodingVersion encoding_version,
        OutputFormat output_format) {
    if (get_index_field_count(frame)) {
        if (!context.fetch_index()) {
            // not selected, skip decompression
            auto size = encoding_sizes::ndarray_field_compressed_size(field.ndarray());
            if (encoding_version == EncodingVersion::V2)
                size += sizeof(ColumnMagic);

            data += size;
        } else {
            auto &buffer = frame.column(0).data().buffer();
            auto &frame_field_descriptor = frame.field(0);
            auto sz = data_type_size(frame_field_descriptor.type(), output_format, DataTypeMode::EXTERNAL);
            const auto& slice_and_key = context.slice_and_key();
            auto offset = sz * (slice_and_key.slice_.row_range.first - frame.offset());
            auto tot_size = sz * slice_and_key.slice_.row_range.diff();

            SliceDataSink sink(buffer.bytes_at(offset, tot_size), tot_size);
            ARCTICDB_DEBUG(log::storage(), "Creating index slice with total size {} ({} - {})", tot_size, sz,
                           slice_and_key.slice_.row_range.diff());

            const auto fields_match = frame_field_descriptor.type() == context.descriptor().fields(0).type();
            util::check(fields_match, "Cannot coerce index type from {} to {}",
                        context.descriptor().fields(0).type(), frame_field_descriptor.type());

            std::optional<util::BitMagic> bv;
            data += decode_field(frame_field_descriptor.type(), field, data, sink, bv, encoding_version);
            util::check(!bv, "Unexpected sparse vector in index field");
            ARCTICDB_DEBUG(log::codec(), "Decoded index column {} to position {}", 0, data - begin);
        }
    }
}

void handle_truncation(
    Column& dest_column,
    const ColumnTruncation& truncate) {
    if(dest_column.num_blocks() == 1 && truncate.start_ && truncate.end_) {
        dest_column.truncate_single_block(*truncate.start_, *truncate.end_);
    } else {
        if(truncate.start_)
            dest_column.truncate_first_block(*truncate.start_);
        if(truncate.end_)
            dest_column.truncate_last_block(*truncate.end_);
    }
}

void handle_truncation(
        Column& dest_column,
        const ColumnMapping& mapping) {
    handle_truncation(dest_column, mapping.truncate_);
}

void create_dense_bitmap(size_t offset, const util::BitSet& sparse_map, Column& dest_column, AllocationType allocation_type) {
    auto& sparse_buffer = dest_column.create_extra_buffer(
        offset,
        ExtraBufferType::BITMAP,
        bitset_packed_size_bytes(sparse_map.size()),
        allocation_type);

    bitset_to_packed_bits(sparse_map, sparse_buffer.data());
}

void create_dense_bitmap_all_zeros(size_t offset, size_t num_bits, Column& dest_column, AllocationType allocation_type) {
    auto num_bytes = bitset_packed_size_bytes(num_bits);
    auto& sparse_buffer = dest_column.create_extra_buffer(
            offset,
            ExtraBufferType::BITMAP,
            num_bytes,
            allocation_type);
    std::memset(sparse_buffer.data(), 0, num_bytes);
}

void decode_or_expand(
    const uint8_t*& data,
    Column& dest_column,
    const EncodedFieldImpl& encoded_field_info,
    const DecodePathData& shared_data,
    std::any& handler_data,
	EncodingVersion encoding_version,
    const ColumnMapping& mapping,
    const std::shared_ptr<StringPool>& string_pool,
    OutputFormat output_format) {
    const auto source_type_desc = mapping.source_type_desc_;
    const auto dest_type_desc = mapping.dest_type_desc_;
    auto* dest = dest_column.bytes_at(mapping.offset_bytes_, mapping.dest_bytes_);
    if(auto handler = get_type_handler(output_format, source_type_desc, dest_type_desc); handler) {
        handler->handle_type(data, dest_column, encoded_field_info, mapping, shared_data, handler_data, encoding_version, string_pool);
    } else {
        ARCTICDB_TRACE(log::version(), "Decoding standard field to position {}", mapping.offset_bytes_);
        const auto dest_bytes = mapping.dest_bytes_;
        std::optional<util::BitMagic> bv;
        if (encoded_field_info.has_ndarray() && encoded_field_info.ndarray().sparse_map_bytes() > 0) {
            const auto &ndarray = encoded_field_info.ndarray();
            const auto bytes = encoding_sizes::data_uncompressed_size(ndarray);

            ChunkedBuffer sparse = ChunkedBuffer::presized(bytes);
            SliceDataSink sparse_sink{sparse.data(), bytes};
            data += decode_field(source_type_desc, encoded_field_info, data, sparse_sink, bv, encoding_version);
            source_type_desc.visit_tag([dest, dest_bytes, &bv, &sparse](const auto tdt) {
                using TagType = decltype(tdt);
                using RawType = typename TagType::DataTypeTag::raw_type;
                util::default_initialize<TagType>(dest, dest_bytes);
                util::expand_dense_buffer_using_bitmap<RawType>(bv.value(), sparse.data(), dest);
            });

            // TODO We must handle sparse columns in ArrowStringHandler and deduplicate logic between the two.
            // Consider registering a sparse handler on the TypeHandlerRegistry.
            if(output_format == OutputFormat::ARROW)
                create_dense_bitmap(mapping.offset_bytes_, *bv, dest_column, AllocationType::DETACHABLE);
        } else {
            SliceDataSink sink(dest, dest_bytes);
            const auto &ndarray = encoded_field_info.ndarray();
            if (const auto bytes = encoding_sizes::data_uncompressed_size(ndarray); bytes < dest_bytes) {
                ARCTICDB_TRACE(log::version(), "Default initializing as only have {} bytes of {}", bytes, dest_bytes);
                source_type_desc.visit_tag([dest, bytes, dest_bytes](const auto tdt) {
                    using TagType = decltype(tdt);
                    util::default_initialize<TagType>(dest + bytes, dest_bytes - bytes);
                });
            }
            data += decode_field(source_type_desc, encoded_field_info, data, sink, bv, encoding_version);
        }
    }
    handle_truncation(dest_column, mapping);
}

ColumnTruncation get_truncate_range_from_rows(
    const RowRange& slice_range,
    size_t row_filter_start,
    size_t row_filter_end) {
    util::check(row_filter_start < slice_range.end() && row_filter_end > slice_range.start(),
        "row range filter unexpectedly got a slice with no intersection with requested row range. "
        "Slice: {} - {}. Filter: {} - {}", slice_range.start(), slice_range.end(), row_filter_start, row_filter_end);

    std::optional<int64_t> truncate_start;
    std::optional<int64_t> truncate_end;
    if(row_filter_start > slice_range.start())
        truncate_start = row_filter_start;

    if(row_filter_end < slice_range.end())
        truncate_end = row_filter_end;

    return {truncate_start, truncate_end};
}

ColumnTruncation get_truncate_range_from_index(
    const Column& column,
    const RowRange& slice_range,
    const TimestampRange& timestamp_range) {
    auto start_row = column.search_sorted<timestamp>(timestamp_range.first, false, slice_range.start(), slice_range.end());
    auto end_row = column.search_sorted<timestamp>(timestamp_range.second, true, slice_range.start(), slice_range.end());
    util::check(start_row < slice_range.end() && end_row > slice_range.start(),
        "date range filter unexpectedly got a slice with no intersection with requested date range. "
        "Slice: {} - {}. Offsets with requested values: {} - {}", slice_range.start(), slice_range.end(), start_row, end_row);

    return get_truncate_range_from_rows(slice_range, start_row, end_row);
}

ColumnTruncation get_truncate_range(
        const SegmentInMemory& frame,
        const PipelineContextRow& context,
        const ReadOptions& read_options,
        const ReadQuery& read_query,
        EncodingVersion encoding_version,
        const EncodedFieldImpl& index_field,
        const uint8_t* index_field_offset) {
    ColumnTruncation truncate_rows;
    const auto& row_range = context.slice_and_key().slice().row_range;
    const auto& first_row_offset = frame.offset();
    auto adjusted_row_range = RowRange(row_range.first - first_row_offset, row_range.second - first_row_offset);
    if(read_options.output_format() == OutputFormat::ARROW) {
        util::variant_match(read_query.row_filter,
            [&truncate_rows, &adjusted_row_range, &frame, &context, &index_field, index_field_offset, encoding_version] (const IndexRange& index_filter) {
                // Time filter is inclusive of both end points
                const auto& time_filter = static_cast<const TimestampRange&>(index_filter);
                // We have historically had some bugs where the start and end index values in the atom key do not
                // exactly reflect the first and last timestamps in the index of the corresponding data keys, so use the
                // index column as a definitive source of truth
                auto [index_column, first_ts, last_ts] = [&]() {
                    std::shared_ptr<Column> _index_column;
                    timestamp _first_ts;
                    timestamp _last_ts;
                    if(context.fetch_index()) {
                        _index_column =  frame.column_ptr(0);
                        _first_ts = *_index_column->scalar_at<timestamp>(adjusted_row_range.first);
                        _last_ts = *_index_column->scalar_at<timestamp>(adjusted_row_range.second - 1);
                    } else {
                        const auto& index_type = frame.descriptor().fields(0UL).type();
                        _index_column = std::make_shared<Column>(index_type);
                        std::optional<util::BitMagic> bv;
                        (void)decode_field(index_type, index_field, index_field_offset, *_index_column, bv, encoding_version);
                        _index_column->set_row_data(_index_column->row_count() - 1);
                        _first_ts = *_index_column->scalar_at<timestamp>(0);
                        _last_ts = *_index_column->scalar_at<timestamp>(_index_column->row_count() - 1);
                    }
                    return std::make_tuple(_index_column, _first_ts, _last_ts);
                }();
                // The `get_truncate_range_from_index` is O(logn). This check serves to avoid the expensive O(logn)
                // check for blocks in the middle of the range
                // Note that this is slightly stricter than entity::contains, as if a time filter boundary exactly matches
                // the segment index boundary, we would keep the whole segment and no log-complexity search is required
                if ((time_filter.first > first_ts && time_filter.first <= last_ts) ||
                    (time_filter.second >= first_ts && time_filter.second < last_ts)) {
                    if(context.fetch_index()) {
                        truncate_rows = get_truncate_range_from_index(*index_column, adjusted_row_range, time_filter);
                    } else {
                        truncate_rows = get_truncate_range_from_index(*index_column, {0, index_column->row_count()}, time_filter);
                        if (truncate_rows.start_.has_value()) {
                            truncate_rows.start_ = *truncate_rows.start_ + adjusted_row_range.first;
                        }
                        if (truncate_rows.end_.has_value()) {
                            truncate_rows.end_ = *truncate_rows.end_ + adjusted_row_range.first;
                        }
                    }
                }
                // Because of an old bug where end_index values in the index key could be larger than the last_ts+1,
                // we need to handle the case where we need to drop the entire first block.
                if (time_filter.first > last_ts) {
                    truncate_rows.start_ = adjusted_row_range.second;
                }
            },
            [&truncate_rows, &adjusted_row_range, &first_row_offset] (const RowRange& row_filter) {
                // The row_filter is with respect to global offset. Column truncation works on column row indices.
                auto row_filter_start = row_filter.first - first_row_offset;
                auto row_filter_end = row_filter.second - first_row_offset;
                truncate_rows = get_truncate_range_from_rows(adjusted_row_range, row_filter_start, row_filter_end);
            },
            [] (const auto&) {
                // Do nothing
            });
    }
    return truncate_rows;
};

size_t get_field_range_compressed_size(
        size_t start_idx,
        size_t num_fields,
        const SegmentHeader& hdr,
        const EncodedFieldCollection& fields) {
    size_t total = 0ULL;
    const size_t magic_num_size = EncodingVersion(hdr.encoding_version()) == EncodingVersion::V2 ? sizeof(ColumnMagic) : 0u;
    ARCTICDB_DEBUG(log::version(), "Skipping between {} and {}", start_idx, start_idx + num_fields);
    for(auto i = start_idx; i < start_idx + num_fields; ++i) {
        const auto& field = fields.at(i);
        ARCTICDB_DEBUG(log::version(), "Adding {}", encoding_sizes::ndarray_field_compressed_size(field.ndarray()) + magic_num_size);
        total += encoding_sizes::ndarray_field_compressed_size(field.ndarray()) + magic_num_size;
    }
    ARCTICDB_DEBUG(log::version(), "Fields {} to {} contain {} bytes", start_idx, start_idx + num_fields, total);
    return total;
}

void advance_field_size(
    const EncodedFieldImpl& field,
    const uint8_t*& data,
    bool has_magic_numbers
    ) {
    const size_t magic_num_size = has_magic_numbers ? sizeof(ColumnMagic) : 0ULL;
    data += encoding_sizes::ndarray_field_compressed_size(field.ndarray()) + magic_num_size;
}

void advance_skipped_cols(
        const uint8_t*& data,
        const StaticColumnMappingIterator& it,
        const EncodedFieldCollection& fields,
        const SegmentHeader& hdr) {
    const auto next_col = it.prev_col_offset() + 1;
    auto skipped_cols = it.source_col() - next_col;
    if(skipped_cols) {
        const auto bytes_to_skip = get_field_range_compressed_size((next_col -  it.first_slice_col_offset()) + it.index_fieldcount(), skipped_cols, hdr, fields);
        data += bytes_to_skip;
    }
}

void advance_to_end(
    const uint8_t*& data,
    const StaticColumnMappingIterator& it,
    const EncodedFieldCollection& fields,
    const SegmentHeader& hdr) {
    const auto next_col = it.prev_col_offset() + 1;
    auto skipped_cols = it.last_slice_col_offset() - next_col;
    if(skipped_cols) {
        const auto bytes_to_skip = get_field_range_compressed_size((next_col -  it.first_slice_col_offset()) + it.index_fieldcount(), skipped_cols, hdr, fields);
        data += bytes_to_skip;
    }
}

template<typename IteratorType>
bool remaining_fields_empty(IteratorType it, const PipelineContextRow& context) {
    while(it.has_next()) {
        const StreamDescriptor& stream_desc = context.descriptor();
        const Field& field = stream_desc.fields(it.source_field_pos());
        if(!is_empty_type(field.type().data_type())) {
            return false;
        }
        it.advance();
    }
    return true;
}

void check_type_compatibility(
        const ColumnMapping& m,
        std::string_view field_name,
        size_t source_col,
        size_t dest_col) {

    const bool types_trivially_compatible = trivially_compatible_types(m.source_type_desc_, m.dest_type_desc_);
    const bool any_type_is_empty = is_empty_type(m.source_type_desc_.data_type()) || is_empty_type(m.dest_type_desc_.data_type());
    util::check(
        types_trivially_compatible || any_type_is_empty,
        "Column type conversion from {} to {} not implemented in column {}:{} -> {}:{}",
        m.source_type_desc_,
        m.dest_type_desc_,
        source_col,
        field_name,
        dest_col,
        m.frame_field_descriptor_.name()
    );
}

void check_data_left_for_subsequent_fields(
        const uint8_t* data,
        const uint8_t* end,
        const StaticColumnMappingIterator& it,
        const PipelineContextRow& context) {
    const bool have_more_compressed_data = data != end;
    util::check(have_more_compressed_data || remaining_fields_empty(it, context),
                "Reached end of input block with {} fields to decode", it.remaining_fields());
}


void decode_into_frame_static(
        SegmentInMemory &frame,
        PipelineContextRow &context,
        const storage::KeySegmentPair& key_seg,
        const DecodePathData& shared_data,
        std::any& handler_data,
        const ReadQuery& read_query,
        const ReadOptions& read_options) {
    ARCTICDB_SAMPLE_DEFAULT(DecodeIntoFrame)
    ARCTICDB_DEBUG(log::version(), "Statically decoding segment with key {}", key_seg.atom_key());
    const auto& seg = key_seg.segment();
    const uint8_t *data = seg.buffer().data();
    const uint8_t *begin = data;
    const uint8_t *end = begin + seg.buffer().bytes();
    auto &hdr = seg.header();
    auto index_fieldcount = get_index_field_count(frame);
    data = skip_heading_fields(hdr, data);
    context.set_descriptor(seg.descriptor());
    context.set_compacted(hdr.compacted());
    ARCTICDB_DEBUG(log::version(), "Num fields: {}", seg.descriptor().field_count());
    const auto encoding_version = hdr.encoding_version();
    const bool has_magic_nums = encoding_version == EncodingVersion::V2;
    const auto& fields = hdr.body_fields();

    // data == end in case we have empty data types (e.g. {EMPTYVAL, Dim0}, {EMPTYVAL, Dim1}) for which we store nothing
    // in storage as they can be reconstructed in the type handler on the read path.
    if (data != end || !fields.empty()) {
        auto string_pool_data = skip_to_string_pool(hdr, data);
        decode_string_pool(hdr, string_pool_data, begin, end, context);

        auto& index_field = fields.at(0u);
        const auto index_field_offset = data;
        decode_index_field(frame, index_field, data, begin, end, context, encoding_version, read_options.output_format());
        auto truncate_range = get_truncate_range(frame, context, read_options, read_query, encoding_version, index_field, index_field_offset);
        if(context.fetch_index() && get_index_field_count(frame)) {
            handle_truncation(frame.column(0), truncate_range);
        }

        StaticColumnMappingIterator it(context, index_fieldcount);
        if(it.invalid())
            return;

        while (it.has_next()) {
            advance_skipped_cols(data, it, fields, hdr);
            if(has_magic_nums)
                util::check_magic_in_place<ColumnMagic>(data);

            auto& encoded_field = fields.at(it.source_field_pos());
            util::check(it.source_field_pos() < size_t(fields.size()), "Field index out of range: {} !< {}", it.source_field_pos(), fields.size());
            auto field_name = context.descriptor().fields(it.source_field_pos()).name();
            auto& column = frame.column(static_cast<ssize_t>(it.dest_col()));
            ColumnMapping mapping{frame, it.dest_col(), it.source_field_pos(), context, read_options.output_format()};
            mapping.set_truncate(truncate_range);

            check_type_compatibility(mapping, field_name, it.source_col(), it.dest_col());
            check_data_left_for_subsequent_fields(data, end, it, context);

            decode_or_expand(
                data,
                column,
                encoded_field,
                shared_data,
                handler_data,
                encoding_version,
                mapping,
                context.string_pool_ptr(),
                read_options.output_format()
            );

            ARCTICDB_TRACE(log::codec(), "Decoded or expanded static column {} to position {}", field_name, data - begin);

            it.advance();
            if(it.at_end_of_selected()) {
                advance_to_end(data, it, fields, hdr);
                break;
            } else if (has_magic_nums) {
                util::check_magic_in_place<ColumnMagic>(data);
            }
        }
    }
    ARCTICDB_TRACE(log::codec(), "Frame decoded into static schema");
}

void check_mapping_type_compatibility(const ColumnMapping& m) {
    util::check(
        is_valid_type_promotion_to_target(m.source_type_desc_, m.dest_type_desc_),
        "Can't promote type {} to type {} in field {}",
        m.source_type_desc_,
        m.dest_type_desc_,
        m.frame_field_descriptor_.name()
    );
}

// If the source and destination types are different, then sizeof(destination type) >= sizeof(source type)
// We have decoded the column of source type directly onto the output buffer above
// We therefore need to iterate backwards through the source values, static casting them to the destination
// type to avoid overriding values we haven't cast yet.
template <typename SourceType, typename DestinationType>
void promote_integral_type(
    const ColumnMapping& m,
    const ReadOptions& read_options,
    Column& column) {
    const auto src_data_type_size = data_type_size(m.source_type_desc_, read_options.output_format(), DataTypeMode::INTERNAL);
    const auto dest_data_type_size = data_type_size(m.dest_type_desc_, read_options.output_format(), DataTypeMode::INTERNAL);

    const auto src_ptr_offset = src_data_type_size * (m.num_rows_ - 1);
    const auto dest_ptr_offset = dest_data_type_size * (m.num_rows_ - 1);

    auto src_ptr = reinterpret_cast<SourceType*>(column.bytes_at(m.offset_bytes_ + src_ptr_offset, 0UL)); // No bytes required as we are at the end
    auto dest_ptr = reinterpret_cast<DestinationType*>(column.bytes_at(m.offset_bytes_ + dest_ptr_offset, 0UL));
    for (auto i = 0u; i < m.num_rows_; ++i) {
        *dest_ptr-- = static_cast<DestinationType>(*src_ptr--);
    }
}

bool source_is_empty(const ColumnMapping& m) {
    return is_empty_type(m.source_type_desc_.data_type());
}

void handle_type_promotion(
    const ColumnMapping& m,
    const DecodePathData& shared_data,
    const ReadOptions& read_options,
    Column& column
    ) {
    if (!trivially_compatible_types(m.source_type_desc_, m.dest_type_desc_) && !source_is_empty(m)) {
        m.dest_type_desc_.visit_tag([&column, &m, shared_data, &read_options] (auto dest_desc_tag) {
            using DestinationType =  typename decltype(dest_desc_tag)::DataTypeTag::raw_type;
            m.source_type_desc_.visit_tag([&column, &m, &read_options] (auto src_desc_tag ) {
                using SourceType =  typename decltype(src_desc_tag)::DataTypeTag::raw_type;
                if constexpr(std::is_arithmetic_v<SourceType> && std::is_arithmetic_v<DestinationType>) {
                    promote_integral_type<SourceType, DestinationType>(m, read_options, column);
                } else {
                    util::raise_rte("Can't promote type {} to type {} in field {}", m.source_type_desc_, m.dest_type_desc_, m.frame_field_descriptor_.name());
                }
            });
        });
    }
}

void decode_into_frame_dynamic(
        SegmentInMemory& frame,
        PipelineContextRow& context,
        const storage::KeySegmentPair& key_seg,
        const DecodePathData& shared_data,
        std::any& handler_data,
        const ReadQuery& read_query,
        const ReadOptions& read_options) {
    ARCTICDB_SAMPLE_DEFAULT(DecodeIntoFrame)
    ARCTICDB_DEBUG(log::version(), "Dynamically decoding segment with key {}", key_seg.atom_key());
    const auto& seg = key_seg.segment();
    const uint8_t *data = seg.buffer().data();
    const uint8_t *begin = data;
    const uint8_t *end = begin + seg.buffer().bytes();
    auto &hdr = seg.header();
    auto index_fieldcount = get_index_field_count(frame);
    data = skip_heading_fields(hdr, data);
    context.set_descriptor(std::make_shared<StreamDescriptor>(seg.descriptor()));
    context.set_compacted(hdr.compacted());

    const auto encoding_version = hdr.encoding_version();
    const bool has_magic_numbers = encoding_version == EncodingVersion::V2;

    if (!hdr.body_fields().empty()) {
        auto string_pool_data = skip_to_string_pool(hdr, data);
        decode_string_pool(hdr, string_pool_data, begin, end, context);

        const auto& fields = hdr.body_fields();
        auto& index_field = fields.at(0u);
        auto index_field_offset = data;
        decode_index_field(frame, index_field, data, begin, end, context, encoding_version, read_options.output_format());
        auto truncate_range = get_truncate_range(frame, context, read_options, read_query, encoding_version, index_field, index_field_offset);
        if (get_index_field_count(frame)) {
            handle_truncation(frame.column(0), truncate_range);
        }

        auto field_count = context.slice_and_key().slice_.col_range.diff() + index_fieldcount;
        for (auto field_col = index_fieldcount; field_col < field_count; ++field_col) {
            auto field_name = context.descriptor().fields(field_col).name();
            auto& encoded_field = fields.at(field_col);
            auto column_output_destination = frame.column_index(field_name);
            if (!column_output_destination) {
                advance_field_size(encoded_field, data, has_magic_numbers);
                continue;
            }

            auto dst_col = *column_output_destination;
            auto& column = frame.column(static_cast<position_t>(dst_col));
            ColumnMapping mapping{frame, dst_col, field_col, context, read_options.output_format()};
            check_mapping_type_compatibility(mapping);
            mapping.set_truncate(truncate_range);
            util::check(data != end || source_is_empty(mapping), "Reached end of input block with {} fields to decode", field_count - field_col);

            decode_or_expand(
                data,
                column,
                encoded_field,
                shared_data,
                handler_data,
                encoding_version,
                mapping,
                context.string_pool_ptr(),
                read_options.output_format()
            );

            handle_type_promotion(mapping, shared_data, read_options, column);
            ARCTICDB_TRACE(log::codec(), "Decoded or expanded dynamic column {} to position {}", frame.field(dst_col).name(), data - begin);
        }
    } else {
        ARCTICDB_DEBUG(log::version(), "Empty segment");
    }
}

/*
 * For message data written with append_incomplete we might have a column missing in a given slice, this code block
 * takes a column and the final allocated buffer in the frame, and zeroes out the memory area corresponding
 * to the appropriate slice if the field is missing in that slice.
 */
class NullValueReducer {
    Column &column_;
    const int type_bytes_;
    const std::vector<size_t>& block_offsets_;
    std::shared_ptr<PipelineContext> context_;
    SegmentInMemory frame_;
    size_t pos_;
    DecodePathData shared_data_;
    std::any& handler_data_;
    const OutputFormat output_format_;

public:
    NullValueReducer(
        Column &column,
        std::shared_ptr<PipelineContext> &context,
        SegmentInMemory frame,
        DecodePathData shared_data,
        std::any& handler_data,
        OutputFormat output_format) :
            column_(column),
            type_bytes_(column_.type().get_type_bytes()),
            block_offsets_(column_.block_offsets()),
            context_(context),
            frame_(std::move(frame)),
            pos_(frame_.offset()),
            shared_data_(std::move(shared_data)),
            handler_data_(handler_data),
            output_format_(output_format){
    }

    [[nodiscard]] static size_t cursor(const PipelineContextRow &context_row) {
        return context_row.slice_and_key().slice_.row_range.first;
    }

    void backfill_all_zero_validity_bitmaps(size_t offset_bytes_start, size_t offset_bytes_end) {
        // Explanation: offset_bytes_start and offset_bytes_end should both be elements of block_offsets_ by
        // construction. We must add an all zeros validity bitmap for each row-slice read from storage where this
        // column was missing, in order to correctly populate the Arrow record-batches for the output
        auto it = std::ranges::lower_bound(block_offsets_, offset_bytes_start);
        auto end_it = std::ranges::lower_bound(block_offsets_, offset_bytes_end);
        util::check(it != block_offsets_.cend() && *it == offset_bytes_start &&
                    end_it != block_offsets_.cend() && *end_it == offset_bytes_end,
                    "NullValueReducer: Failed to find one of offset_bytes ({} or {}) in block_offsets_ {}",
                    offset_bytes_start, offset_bytes_end, block_offsets_);
        for (; it != end_it; ++it) {
            auto rows = (*std::next(it) - *it) / type_bytes_;
            create_dense_bitmap_all_zeros(*it, rows, column_, AllocationType::DETACHABLE);
        }
    }

    void reduce(PipelineContextRow &context_row){
        auto &slice_and_key = context_row.slice_and_key();
        auto sz_to_advance = slice_and_key.slice_.row_range.diff();
        auto current_pos = context_row.slice_and_key().slice_.row_range.first;
        if (current_pos != pos_) {
            const auto num_rows = current_pos - pos_;
            const auto start_row = pos_ - frame_.offset();
            if (const std::shared_ptr<TypeHandler>& handler = get_type_handler(output_format_, column_.type()); handler) {
                handler->default_initialize(column_.buffer(), start_row * handler->type_size(), num_rows * handler->type_size(), shared_data_, handler_data_);
            } else if (output_format_ != OutputFormat::ARROW) {
                // Arrow does not care what values are in the main buffer where the validity bitmap is zero
                column_.default_initialize_rows(start_row, num_rows, false);
            }
            if (output_format_ == OutputFormat::ARROW) {
                backfill_all_zero_validity_bitmaps(start_row * type_bytes_, block_offsets_.at(context_row.index()));
            }
            pos_ = current_pos + sz_to_advance;
        } else {
            pos_ += sz_to_advance;
        }
    }

    void finalize() {
        auto total_rows = frame_.row_count();
        auto end =  frame_.offset() + total_rows;
        if(pos_ != end) {
            util::check(pos_ < end, "Overflow in finalize {} > {}", pos_, end);
            const auto num_rows = end - pos_;
            const auto start_row = pos_ - frame_.offset();
            if (const std::shared_ptr<TypeHandler>& handler = get_type_handler(output_format_, column_.type()); handler) {
                handler->default_initialize(column_.buffer(), start_row * handler->type_size(), num_rows * handler->type_size(), shared_data_, handler_data_);
            } else if (output_format_ != OutputFormat::ARROW) {
                // Arrow does not care what values are in the main buffer where the validity bitmap is zero
                column_.default_initialize_rows(start_row, num_rows, false);
            }
            if (output_format_ == OutputFormat::ARROW) {
                backfill_all_zero_validity_bitmaps(start_row * type_bytes_, block_offsets_.back());
            }
        }
    }
};

struct ReduceColumnTask : async::BaseTask {
    SegmentInMemory frame_;
    size_t column_index_;
    std::shared_ptr<FrameSliceMap> slice_map_;
    std::shared_ptr<PipelineContext> context_;
    DecodePathData shared_data_;
    std::any& handler_data_;
    ReadOptions read_options_;

    ReduceColumnTask(
        SegmentInMemory frame,
        size_t c,
        std::shared_ptr<FrameSliceMap> slice_map,
        std::shared_ptr<PipelineContext>& context,
        DecodePathData shared_data,
        std::any& handler_data,
        const ReadOptions& read_options) :
        frame_(std::move(frame)),
        column_index_(c),
        slice_map_(std::move(slice_map)),
        context_(context),
        shared_data_(std::move(shared_data)),
        handler_data_(handler_data),
        read_options_(read_options) {
    }

    folly::Unit operator()() {
        const auto &frame_field = frame_.field(column_index_);
        const auto field_type = frame_field.type().data_type();
        auto &column = frame_.column(static_cast<position_t>(column_index_));
        const auto dynamic_schema = read_options_.dynamic_schema().value_or(false);

        const auto column_data = slice_map_->columns_.find(frame_field.name());
        if(dynamic_schema && column_data == slice_map_->columns_.end()) {
            if (const std::shared_ptr<TypeHandler>& handler = get_type_handler(read_options_.output_format(), column.type()); handler) {
                handler->default_initialize(column.buffer(), 0, frame_.row_count() * handler->type_size(), shared_data_, handler_data_);
            } else {
                if (is_fixed_string_type(field_type)) {
                    // Special case where we have a fixed-width string column that is all null (e.g. dynamic schema
                    // where this column was not present in any of the read row-slices)
                    // All other column types are allocated in the output frame as detachable by default since we know
                    // we will be handing them off to Python to free. This is not the case for fixed-width string
                    // columns, since the buffer still contains string pool offsets at this point, and so will usually
                    // be swapped out with the inflated strings buffer. However, if there are no strings to inflate, we
                    // still need to make this buffer detachable and full of nulls of the correct length
                    auto buffer_size = frame_.row_count() * (field_type == DataType::UTF_FIXED64 ? 4 : 1);
                    ChunkedBuffer new_buffer(buffer_size, AllocationType::DETACHABLE);
                    memset(new_buffer.data(), 0, buffer_size);
                    auto& prev_buffer = column.buffer();
                    swap(prev_buffer, new_buffer);
                } else {
                    column.default_initialize_rows(0, frame_.row_count(), false);
                }
            }
        } else if (column_data != slice_map_->columns_.end()) {
            if(dynamic_schema) {
                NullValueReducer null_reducer{column, context_, frame_, shared_data_, handler_data_, read_options_.output_format()};
                for (const auto &row : column_data->second) {
                    PipelineContextRow context_row{context_, row.second.context_index_};
                    null_reducer.reduce(context_row);
                }
                null_reducer.finalize();
            }
            if (is_sequence_type(field_type)) {
                if (is_fixed_string_type(field_type)) {
                    auto string_reducer = get_fixed_string_reducer(column, context_, frame_, frame_field, *slice_map_);
                    for (const auto &row : column_data->second) {
                        PipelineContextRow context_row{context_, row.second.context_index_};
                        if (context_row.slice_and_key().slice().row_range.diff() > 0)
                            string_reducer->reduce(context_row, row.second.column_index_);
                    }
                    string_reducer->finalize();
                }

                column.set_inflated(frame_.row_count());
            }
        } else if (!dynamic_schema && column_data == slice_map_->columns_.end() && is_sequence_type(column.type().data_type())) {
            internal::raise<ErrorCode::E_ASSERTION_FAILURE>("Column with index {} is not in static schema slice map.", column_index_);
        }
        return folly::Unit{};
    }
};

folly::Future<folly::Unit> reduce_and_fix_columns(
    std::shared_ptr<PipelineContext> &context,
    SegmentInMemory &frame,
    const ReadOptions& read_options,
    std::any& handler_data
) {
    ARCTICDB_SAMPLE_DEFAULT(ReduceAndFixStringCol)
    ARCTICDB_DEBUG(log::version(), "Reduce and fix columns");
    if(frame.empty())
        return folly::Unit{};

    auto slice_map = std::make_shared<FrameSliceMap>(context, read_options.dynamic_schema().value_or(false));

    // This logic mimics that in ReduceColumnTask operator() to identify whether the task will actually do any work
    // This is to avoid scheduling work that is a no-op
    std::vector<size_t> fields_to_reduce;
    for (size_t idx=0; idx<frame.descriptor().fields().size(); ++idx) {
        const auto& frame_field = frame.field(idx);
        if (read_options.dynamic_schema().value_or(false) ||
            (slice_map->columns_.contains(frame_field.name()) && is_sequence_type(frame_field.type().data_type()))) {
            fields_to_reduce.emplace_back(idx);
        }
    }

    DecodePathData shared_data;
    static const auto batch_size = ConfigsMap::instance()->get_int("ReduceColumns.BatchSize", 100);
    return folly::collect(
            folly::window(std::move(fields_to_reduce),
                          [context, frame, slice_map, shared_data, read_options, &handler_data] (size_t field) mutable {
                              return async::submit_cpu_task(ReduceColumnTask(frame, field, slice_map, context, shared_data, handler_data, read_options));
                          }, batch_size)).via(&async::io_executor()).unit();
}

folly::Future<SegmentInMemory> fetch_data(
    SegmentInMemory&& frame,
    const std::shared_ptr<PipelineContext> &context,
    const std::shared_ptr<stream::StreamSource>& ssource,
    const ReadQuery& read_query,
    const ReadOptions& read_options,
    DecodePathData shared_data,
    std::any& handler_data
    ) {
    ARCTICDB_SAMPLE_DEFAULT(FetchSlices)
    if (frame.empty())
        return frame;

    std::vector<std::pair<VariantKey, stream::StreamSource::ReadContinuation>> keys_and_continuations;
    keys_and_continuations.reserve(context->slice_and_keys_.size());
    context->ensure_vectors();
    {
        ARCTICDB_SUBSAMPLE_DEFAULT(QueueReadContinuations)
        const auto dynamic_schema = read_options.dynamic_schema().value_or(false);
        for ( auto& row : *context) {
            keys_and_continuations.emplace_back(row.slice_and_key().key(),
            [row=row, frame=frame, dynamic_schema=dynamic_schema, shared_data, &handler_data, read_query, read_options](auto &&ks) mutable {
                auto key_seg = std::forward<storage::KeySegmentPair>(ks);
                if(dynamic_schema) {
                    decode_into_frame_dynamic(frame, row, key_seg, shared_data, handler_data, read_query, read_options);
                } else {
                    decode_into_frame_static(frame, row, key_seg, shared_data, handler_data, read_query, read_options);
                }

                return key_seg.variant_key();
            });
        }
    }
    ARCTICDB_SUBSAMPLE_DEFAULT(DoBatchReadCompressed)
    return folly::collect(ssource->batch_read_compressed(std::move(keys_and_continuations), BatchReadArgs{}))
    .via(&async::io_executor())
    .thenValue([frame](auto&&){ return frame; });
}

} // namespace read
