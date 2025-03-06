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
#include <arcticdb/util/spinlock.hpp>
#include <arcticdb/pipeline/string_reducers.hpp>
#include <arcticdb/pipeline/read_query.hpp>
#include <arcticdb/entity/merge_descriptors.hpp>
#include <arcticdb/pipeline//error.hpp>
#include <arcticdb/version/schema_checks.hpp>
#include <arcticdb/async/tasks.hpp>
#include <ankerl/unordered_dense.h>
#include <folly/gen/Base.h>

namespace arcticdb::pipelines {

/*
   _  _  _
  |i||_||_|    We mark the first slice of each row of the grid as requiring a fetch_index
  |i||_||_|    This helps us calculate the row_count during allocation. Note that all slices
  |i||_||_|    contain the relevant index, but we only want to count it once per grid row.

 */
void mark_index_slices(
    const std::shared_ptr<PipelineContext>& context,
    bool dynamic_schema,
    bool column_groups) {
    context->fetch_index_ = check_and_mark_slices(
        context->slice_and_keys_,
        dynamic_schema,
        true,
        context->incompletes_after_,
        column_groups).value();
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

SegmentInMemory allocate_chunked_frame(const std::shared_ptr<PipelineContext>& context, OutputFormat output_format, AllocationType allocation_type) {
    ARCTICDB_SAMPLE_DEFAULT(AllocContiguousFrame)
    auto [offset, row_count] = offset_and_row_count(context);
    auto block_row_counts = output_block_row_counts(context);
    ARCTICDB_DEBUG(log::version(), "Allocated chunked frame with offset {} and row count {}", offset, row_count);
    SegmentInMemory output{get_filtered_descriptor(context, output_format), 0, allocation_type, Sparsity::NOT_PERMITTED, output_format, DataTypeMode::EXTERNAL};
    auto handlers = TypeHandlerRegistry::instance();

    for(auto& column : output.columns()) {
        auto handler = handlers->get_handler(output_format, column->type());
        const auto extra_rows = handler ? handler->extra_rows() : 0;
        const auto data_size = data_type_size(column->type(), output_format, DataTypeMode::EXTERNAL);
        for(auto block_row_count : block_row_counts) {
            const auto bytes = (block_row_count + extra_rows) * data_size;
            column->allocate_data(bytes);
            column->advance_data(bytes);
        }
    }

    finalize_segment_setup(output, offset, row_count, context);
    return output;
}

SegmentInMemory allocate_contiguous_frame(const std::shared_ptr<PipelineContext>& context, OutputFormat output_format, AllocationType allocation_type) {
    ARCTICDB_SAMPLE_DEFAULT(AllocChunkedFrame)
    auto [offset, row_count] = offset_and_row_count(context);
    SegmentInMemory output{get_filtered_descriptor(context, output_format),  row_count, allocation_type, Sparsity::NOT_PERMITTED, output_format, DataTypeMode::EXTERNAL};
    finalize_segment_setup(output, offset, row_count, context);
    return output;
}

SegmentInMemory allocate_frame(const std::shared_ptr<PipelineContext>& context, OutputFormat output_format, AllocationType allocation_type) {
   if(output_format == OutputFormat::PANDAS)
       return allocate_contiguous_frame(context, output_format, allocation_type);
   else
       return allocate_chunked_frame(context, output_format, allocation_type);
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

            SliceDataSink sink(buffer.data() + offset, tot_size);
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
    if(dest_column.num_blocks() == 1 && truncate.start_ && truncate.end_)
        dest_column.truncate_single_block(*truncate.start_, *truncate.end_);
    else if(truncate.start_)
        dest_column.truncate_first_block(*truncate.start_);
    else if(truncate.end_)
        dest_column.truncate_last_block(*truncate.end_);
}

void handle_truncation(
        Column& dest_column,
        const ColumnMapping& mapping) {
    handle_truncation(dest_column, mapping.truncate_);
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

            ChunkedBuffer sparse{bytes};
            SliceDataSink sparse_sink{sparse.data(), bytes};
            data += decode_field(source_type_desc, encoded_field_info, data, sparse_sink, bv, encoding_version);
            source_type_desc.visit_tag([dest, dest_bytes, &bv, &sparse](const auto tdt) {
                using TagType = decltype(tdt);
                using RawType = typename TagType::DataTypeTag::raw_type;
                util::default_initialize<TagType>(dest, dest_bytes);
                util::expand_dense_buffer_using_bitmap<RawType>(bv.value(), sparse.data(), dest);
            });
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

    if(mapping.requires_truncation())
        handle_truncation(dest_column, mapping);
}

template <typename IndexValueType>
ColumnTruncation get_truncate_range_from_index(
    const Column& column,
    const IndexValueType& start,
    const IndexValueType& end,
    std::optional<int64_t> start_offset = std::nullopt,
    std::optional<int64_t> end_offset = std::nullopt) {
    int64_t start_row = column.search_sorted<IndexValueType>(start, false, start_offset, end_offset);
    int64_t end_row = column.search_sorted<IndexValueType>(end, true, start_offset, end_offset);
    std::optional<int64_t> truncate_start;
    std::optional<int64_t> truncate_end;
    if((start_offset && start_row != *start_offset) || (!start_offset && start_row > 0))
        truncate_start = start_row;

    if((end_offset && end_row != *end_offset) || (!end_offset && end_row < column.row_count() - 1))
        truncate_end = end_row;

    return {truncate_start, truncate_end};
}

std::pair<std::optional<int64_t>, std::optional<int64_t>> get_truncate_range_from_rows(
    const RowRange& row_range,
    size_t start_offset,
    size_t end_offset) {
    std::optional<int64_t> truncate_start;
    std::optional<int64_t> truncate_end;
    if(contains(row_range, start_offset))
        truncate_start = start_offset;

    if(contains(row_range, end_offset))
        truncate_end = end_offset;

    return std::make_pair(truncate_start, truncate_end);
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
    if(read_options.output_format() == OutputFormat::ARROW) {
    util::variant_match(read_query.row_filter,
        [&truncate_rows, &frame, &context, &index_field, index_field_offset, encoding_version] (const IndexRange& index_range) {
            const auto& time_range = static_cast<const TimestampRange&>(index_range);
            const auto& slice_time_range =  context.slice_and_key().key().time_range();
            if(contains(slice_time_range, time_range.first) || contains(slice_time_range, time_range.second)) {
                if(context.fetch_index()) {
                    const auto& index_column = frame.column(0);
                    const auto& current_row_range = context.slice_and_key().slice().row_range;
                    truncate_rows = get_truncate_range_from_index(index_column, time_range.first, time_range.second, current_row_range.first, current_row_range.second);
            } else {
                const auto& frame_index_desc = frame.descriptor().fields(0UL);
                Column sink{frame_index_desc.type(), encoding_sizes::field_uncompressed_size(index_field), AllocationType::PRESIZED, Sparsity::PERMITTED};
                std::optional<util::BitMagic> bv;
                (void)decode_field(frame_index_desc.type(), index_field, index_field_offset, sink, bv, encoding_version);
                truncate_rows = get_truncate_range_from_index(sink, time_range.first, time_range.second);
            }
        }
            },
        [&context] (const RowRange& row_range) {
            const auto& slice_row_range = context.slice_and_key().slice().row_range;
            get_truncate_range_from_rows(row_range, slice_row_range.start(), slice_row_range.end());
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
        if(context.fetch_index() && truncate_range.requires_truncation())
            handle_truncation(frame.column(0), truncate_range);

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

    void reduce(PipelineContextRow &context_row){
        auto &slice_and_key = context_row.slice_and_key();
        auto sz_to_advance = slice_and_key.slice_.row_range.diff();
        auto current_pos = context_row.slice_and_key().slice_.row_range.first;
        if (current_pos != pos_) {
            const auto num_rows = current_pos - pos_;
            const auto start_row = pos_ - frame_.offset();
            if (const std::shared_ptr<TypeHandler>& handler = get_type_handler(output_format_, column_.type()); handler) {
                handler->default_initialize(column_.buffer(), start_row * handler->type_size(), num_rows * handler->type_size(), shared_data_, handler_data_);
            } else {
                column_.default_initialize_rows(start_row, num_rows, false);
            }
            pos_ = current_pos + sz_to_advance;
        }
        else
            pos_ += sz_to_advance;
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
            } else {
                column_.default_initialize_rows(start_row, num_rows, false);
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
                column.default_initialize_rows(0, frame_.row_count(), false);
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
        std::any& handler_data) {
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
    SegmentInMemory frame,
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
    return ssource->batch_read_compressed(std::move(keys_and_continuations), BatchReadArgs{})
    .thenValue([frame](auto&&){ return frame; });
}

void set_output_descriptors(
    const ProcessingUnit& proc,
    const std::vector<std::shared_ptr<Clause>>& clauses,
    const std::shared_ptr<PipelineContext>& pipeline_context) {
    std::optional<std::string> index_column;
    for (auto clause = clauses.rbegin(); clause != clauses.rend(); ++clause) {
        bool should_break = util::variant_match(
            (*clause)->clause_info().index_,
            [](const KeepCurrentIndex&) { return false; },
            [&](const KeepCurrentTopLevelIndex&) {
                if (pipeline_context->norm_meta_->mutable_df()->mutable_common()->has_multi_index()) {
                    const auto& multi_index = pipeline_context->norm_meta_->mutable_df()->mutable_common()->multi_index();
                    auto name = multi_index.name();
                    auto tz = multi_index.tz();
                    bool fake_name{false};
                    for (auto pos: multi_index.fake_field_pos()) {
                        if (pos == 0) {
                            fake_name = true;
                            break;
                        }
                    }
                    auto mutable_index = pipeline_context->norm_meta_->mutable_df()->mutable_common()->mutable_index();
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
            });
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
        for (const auto& segment: *proc.segments_) {
            fields.push_back(segment->descriptor().fields_ptr());
        }
        new_stream_descriptor = merge_descriptors(*new_stream_descriptor,
                                                  fields,
                                                  std::vector<std::string>{});
    }
    if (new_stream_descriptor.has_value()) {
        // Finding and erasing fields from the FieldCollection contained in StreamDescriptor is O(n) in number of fields
        // So maintain map from field names to types in the new_stream_descriptor to make these operations O(1)
        // Cannot use set of FieldRef as the name in the output might match the input, but with a different type after processing
        std::unordered_map<std::string_view, TypeDescriptor> new_fields;
        for (const auto& field: new_stream_descriptor->fields()) {
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
            internal::check<ErrorCode::E_ASSERTION_FAILURE>(!nh.empty(), "New index column not found in processing pipeline");
            final_stream_descriptor.add_field(FieldRef{nh.mapped(), nh.key()});
        }
        for (const auto& field: original_stream_descriptor.fields()) {
            if (const auto nh = new_fields.extract(field.name()); nh) {
                final_stream_descriptor.add_field(FieldRef{nh.mapped(), nh.key()});
            }
        }
        // Iterate through new_stream_descriptor->fields() rather than remaining new_fields to preserve ordering
        // e.g. if there were two projections then users will expect the column produced by the first one to appear
        // first in the output df
        for (const auto& field: new_stream_descriptor->fields()) {
            if (new_fields.contains(field.name())) {
                final_stream_descriptor.add_field(field);
            }
        }
        pipeline_context->set_descriptor(final_stream_descriptor);
    }
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
        // Take a copy here as things like defrag need the keys in pipeline_context->slice_and_keys_ that aren't being modified at the end
        auto key = sk.key();
        res.emplace_back(sk.slice(), std::move(key), is_incomplete);
    }
    return res;
}

util::BitSet get_incompletes_bitset(const std::vector<RangesAndKey>& all_ranges) {
    util::BitSet output(all_ranges.size());
    util::BitSet::bulk_insert_iterator it(output);
    for(auto&& [index, range] : folly::enumerate(all_ranges)) {
        if(range.is_incomplete())
            it = index;
    }
    it.flush();
    return output;
}

std::shared_ptr<std::unordered_set<std::string>> columns_to_decode(const std::shared_ptr<PipelineContext>& pipeline_context) {
    std::shared_ptr<std::unordered_set<std::string>> res;
    ARCTICDB_DEBUG(log::version(), "Creating columns list with {} bits set", pipeline_context->overall_column_bitset_ ? pipeline_context->overall_column_bitset_->count() : -1);
    if(pipeline_context->overall_column_bitset_) {
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

std::vector<folly::Future<pipelines::SegmentAndSlice>> add_schema_check(
    const std::shared_ptr<PipelineContext> &pipeline_context,
    std::vector<folly::Future<pipelines::SegmentAndSlice>>&& segment_and_slice_futures,
    util::BitSet&& incomplete_bitset,
    const ProcessingConfig &processing_config) {
    std::vector<folly::Future<pipelines::SegmentAndSlice>> res;
    res.reserve(segment_and_slice_futures.size());
    for (size_t i = 0; i < segment_and_slice_futures.size(); ++i) {
        auto&& fut = segment_and_slice_futures.at(i);
        const bool is_incomplete = incomplete_bitset[i];
        if (is_incomplete) {
            res.push_back(
                std::move(fut)
                    .thenValueInline([pipeline_desc=pipeline_context->descriptor(), processing_config](SegmentAndSlice &&read_result) {
                        if (!processing_config.dynamic_schema_) {
                            auto check = check_schema_matches_incomplete(read_result.segment_in_memory_.descriptor(), pipeline_desc);
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

CheckOutcome check_schema_matches_incomplete(const StreamDescriptor& stream_descriptor_incomplete, const StreamDescriptor& pipeline_desc) {
    // We need to check that the index names match regardless of the dynamic schema setting
    if(!index_names_match(stream_descriptor_incomplete, pipeline_desc)) {
        return Error{
            throw_error<ErrorCode::E_DESCRIPTOR_MISMATCH>,
            fmt::format("{} All staged segments must have the same index names."
                        "{} is different than {}",
                        error_code_data<ErrorCode::E_DESCRIPTOR_MISMATCH>.name_,
                        stream_descriptor_incomplete,
                        pipeline_desc)
        };
    }
    if (!columns_match(stream_descriptor_incomplete, pipeline_desc)) {
        return Error{
            throw_error<ErrorCode::E_DESCRIPTOR_MISMATCH>,
            fmt::format("{} When static schema is used all staged segments must have the same column and column types."
                        "{} is different than {}",
                        error_code_data<ErrorCode::E_DESCRIPTOR_MISMATCH>.name_,
                        stream_descriptor_incomplete,
                        pipeline_desc)
        };
    }
    return std::monostate{};
}

std::vector<folly::Future<pipelines::SegmentAndSlice>> generate_segment_and_slice_futures(
    const std::shared_ptr<Store> &store,
    const std::shared_ptr<PipelineContext> &pipeline_context,
    const ProcessingConfig &processing_config,
    std::vector<RangesAndKey>&& all_ranges) {
    auto incomplete_bitset = get_incompletes_bitset(all_ranges);
    auto segment_and_slice_futures = store->batch_read_uncompressed(std::move(all_ranges), columns_to_decode(pipeline_context));
    return add_schema_check(pipeline_context, std::move(segment_and_slice_futures), std::move(incomplete_bitset), processing_config);
}

std::pair<std::vector<std::vector<EntityId>>, std::shared_ptr<ankerl::unordered_dense::map<EntityId, size_t>>> get_entity_ids_and_position_map(
    std::shared_ptr<ComponentManager>& component_manager,
    size_t num_segments,
    std::vector<std::vector<size_t>>&& processing_unit_indexes) {
    // Map from entity id to position in segment_and_slice_futures
    auto id_to_pos = std::make_shared<ankerl::unordered_dense::map<EntityId, size_t>>();
    id_to_pos->reserve(num_segments);

    // Map from position in segment_and_slice_future_splitters to entity ids
    std::vector<EntityId> pos_to_id;
    pos_to_id.reserve(num_segments);

    auto ids = component_manager->get_new_entity_ids(num_segments);
    for (auto&& [idx, id]: folly::enumerate(ids)) {
        pos_to_id.emplace_back(id);
        id_to_pos->emplace(id, idx);
    }

    std::vector<std::vector<EntityId>> entity_work_units;
    entity_work_units.reserve(processing_unit_indexes.size());
    for (const auto& indexes: processing_unit_indexes) {
        entity_work_units.emplace_back();
        entity_work_units.back().reserve(indexes.size());
        for (auto index: indexes) {
            entity_work_units.back().emplace_back(pos_to_id[index]);
        }
    }

    return std::make_pair(std::move(entity_work_units), std::move(id_to_pos));
}

void add_slice_to_component_manager(
    EntityId entity_id,
    pipelines::SegmentAndSlice& segment_and_slice,
    std::shared_ptr<ComponentManager> component_manager,
    EntityFetchCount fetch_count) {
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

std::shared_ptr<std::vector<folly::Future<std::vector<EntityId>>>> schedule_first_iteration(
    std::shared_ptr<ComponentManager> component_manager,
    size_t num_segments,
    std::vector<std::vector<EntityId>>&& entities_by_work_unit,
    std::shared_ptr<std::vector<EntityFetchCount>>&& segment_fetch_counts,
    std::vector<FutureOrSplitter>&& segment_and_slice_future_splitters,
    std::shared_ptr<ankerl::unordered_dense::map<EntityId, size_t>>&& id_to_pos,
    std::shared_ptr<std::vector<std::shared_ptr<Clause>>>& clauses) {
    // Used to make sure each entity is only added into the component manager once
    auto slice_added_mtx = std::make_shared<std::vector<std::mutex>>(num_segments);
    auto slice_added = std::make_shared<std::vector<bool>>(num_segments, false);
    auto futures = std::make_shared<std::vector<folly::Future<std::vector<EntityId>>>>();

    for (auto&& entity_ids: entities_by_work_unit) {
        std::vector<folly::Future<pipelines::SegmentAndSlice>> local_futs;
        local_futs.reserve(entity_ids.size());
        for (auto id: entity_ids) {
            const auto pos = id_to_pos->at(id);
            auto& future_or_splitter = segment_and_slice_future_splitters[pos];
            // Some of the entities for this unit of work may be shared with other units of work
            util::variant_match(future_or_splitter,
                                [&local_futs] (folly::Future<pipelines::SegmentAndSlice>& fut) {
                                    local_futs.emplace_back(std::move(fut));
                                },
                                [&local_futs] (folly::FutureSplitter<pipelines::SegmentAndSlice>& splitter) {
                                    local_futs.emplace_back(splitter.getFuture());
                                });
        }

        futures->emplace_back(
            folly::collect(local_futs)
                .via(&async::io_executor()) // Stay on the same executor as the read so that we can inline if possible
                .thenValueInline([component_manager, segment_fetch_counts, id_to_pos, slice_added_mtx, slice_added, clauses,entity_ids = std::move(entity_ids)]
                                     (std::vector<pipelines::SegmentAndSlice>&& segment_and_slices) mutable {
                    for (auto&& [idx, segment_and_slice]: folly::enumerate(segment_and_slices)) {
                        auto entity_id = entity_ids[idx];
                        auto pos = id_to_pos->at(entity_id);
                        std::lock_guard lock{slice_added_mtx->at(pos)};
                        if (!(*slice_added)[pos]) {
                            ARCTICDB_DEBUG(log::version(), "Adding entity {}", entity_id);
                            add_slice_to_component_manager(entity_id, segment_and_slice, component_manager, segment_fetch_counts->at(pos));
                            (*slice_added)[pos] = true;
                        }
                    }
                    return async::MemSegmentProcessingTask(*clauses, std::move(entity_ids))();
                }));
    }
    return futures;
}


size_t num_scheduling_iterations(const std::vector<std::shared_ptr<Clause>>& clauses) {
    size_t res = 1UL;
    auto it = std::next(clauses.cbegin());
    while (it != clauses.cend()) {
        auto prev_it = std::prev(it);
        if ((*prev_it)->clause_info().output_structure_ != (*it)->clause_info().input_structure_) {
            ++res;
        }
        ++it;
    }
    ARCTICDB_DEBUG(log::memory(), "Processing pipeline has {} scheduling stages after the initial read and process", res);
    return res;
}

void remove_processed_clauses(std::vector<std::shared_ptr<Clause>>& clauses) {
    // Erase all the clauses we have already scheduled to run
    ARCTICDB_SAMPLE_DEFAULT(RemoveProcessedClauses)
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

folly::Future<std::vector<EntityId>> schedule_clause_processing(
    std::shared_ptr<ComponentManager> component_manager,
    std::vector<folly::Future<pipelines::SegmentAndSlice>>&& segment_and_slice_futures,
    std::vector<std::vector<size_t>>&& processing_unit_indexes,
    std::shared_ptr<std::vector<std::shared_ptr<Clause>>> clauses) {
    // All the shared pointers as arguments to this function and created within it are to ensure that resources are
    // correctly kept alive after this function returns its future
    const auto num_segments = segment_and_slice_futures.size();

    // Map from index in segment_and_slice_future_splitters to the number of calls to process in the first clause that
    // will require that segment
    auto segment_fetch_counts = generate_segment_fetch_counts(processing_unit_indexes, num_segments);

    auto segment_and_slice_future_splitters = split_futures(std::move(segment_and_slice_futures), *segment_fetch_counts);

    auto [entities_by_work_unit, entity_id_to_segment_pos] = get_entity_ids_and_position_map(component_manager, num_segments, std::move(processing_unit_indexes));

    // At this point we have a set of entity ids grouped by the work units produced by the original structure_for_processing,
    // and a map of those ids to the position in the vector of futures or future-splitters (which is the same order as
    // originally generated from the index via the pipeline_context and ranges_and_keys), so we can add each entity id and
    // its components to the component manager and schedule the first stage of work (i.e. from the beginning until either
    // the end of the pipeline or the next required structure_for_processing
    auto futures = schedule_first_iteration(
        component_manager,
        num_segments,
        std::move(entities_by_work_unit),
        std::move(segment_fetch_counts),
        std::move(segment_and_slice_future_splitters),
        std::move(entity_id_to_segment_pos),
        clauses);

    auto entity_ids_vec_fut = folly::collect(*futures).via(&async::io_executor());

    const auto scheduling_iterations = num_scheduling_iterations(*clauses);
    for (auto i = 1UL; i < scheduling_iterations; ++i) {
        entity_ids_vec_fut = std::move(entity_ids_vec_fut).thenValue([clauses, scheduling_iterations, i] (std::vector<std::vector<EntityId>>&& entity_id_vectors) {
            ARCTICDB_RUNTIME_DEBUG(log::memory(), "Scheduling iteration {} of {}", i, scheduling_iterations);

            util::check(!clauses->empty(), "Scheduling iteration {} has no clauses to process", scheduling_iterations);
            remove_processed_clauses(*clauses);
            auto next_units_of_work = clauses->front()->structure_for_processing(std::move(entity_id_vectors));

            std::vector<folly::Future<std::vector<EntityId>>> work_futures;
            for(auto&& unit_of_work : next_units_of_work) {
                ARCTICDB_RUNTIME_DEBUG(log::memory(), "Scheduling work for entity ids: {}", unit_of_work);
                work_futures.emplace_back(async::submit_cpu_task(async::MemSegmentProcessingTask{*clauses, std::move(unit_of_work)}));
            }

            return folly::collect(work_futures).via(&async::io_executor());
        });
    }

    return std::move(entity_ids_vec_fut).thenValueInline([](std::vector<std::vector<EntityId>>&& entity_id_vectors) {
        return flatten_entities(std::move(entity_id_vectors));
    });
}

/*
 * Processes the slices in the given pipeline_context.
 *
 * Slices are processed in an order defined by the first clause in the pipeline, with slices corresponding to the same
 * processing unit collected into a single ProcessingUnit. Slices contained within a single ProcessingUnit are processed
 * within a single thread.
 *
 * The processing of a ProcessingUnit is scheduled via the Async Store. Within a single thread, the
 * segments will be retrieved from storage and decompressed before being passed to a MemSegmentProcessingTask which
 * will process all clauses up until a clause that requires a repartition.
 */
    folly::Future<std::vector<SliceAndKey>> read_and_process(
        const std::shared_ptr<Store>& store,
        const std::shared_ptr<PipelineContext>& pipeline_context,
        const std::shared_ptr<ReadQuery>& read_query,
        const ReadOptions& read_options) {
    auto component_manager = std::make_shared<ComponentManager>();
    ProcessingConfig processing_config{opt_false(read_options.dynamic_schema()), pipeline_context->rows_};
    for (auto& clause: read_query->clauses_) {
        clause->set_processing_config(processing_config);
        clause->set_component_manager(component_manager);
    }

    auto ranges_and_keys = generate_ranges_and_keys(*pipeline_context);

    // Each element of the vector corresponds to one processing unit containing the list of indexes in ranges_and_keys required for that processing unit
    // i.e. if the first processing unit needs ranges_and_keys[0] and ranges_and_keys[1], and the second needs ranges_and_keys[2] and ranges_and_keys[3]
    // then the structure will be {{0, 1}, {2, 3}}
    std::vector<std::vector<size_t>> processing_unit_indexes = read_query->clauses_[0]->structure_for_processing(ranges_and_keys);

    // Start reading as early as possible
    auto segment_and_slice_futures = generate_segment_and_slice_futures(store, pipeline_context, processing_config, std::move(ranges_and_keys));

    return schedule_clause_processing(
        component_manager,
        std::move(segment_and_slice_futures),
        std::move(processing_unit_indexes),
        std::make_shared<std::vector<std::shared_ptr<Clause>>>(read_query->clauses_))
        .via(&async::cpu_executor())
        .thenValue([component_manager, read_query, pipeline_context](std::vector<EntityId>&& processed_entity_ids) {
            auto proc = gather_entities<std::shared_ptr<SegmentInMemory>, std::shared_ptr<RowRange>, std::shared_ptr<ColRange>>(*component_manager, std::move(processed_entity_ids));

            if (std::any_of(read_query->clauses_.begin(), read_query->clauses_.end(), [](const std::shared_ptr<Clause>& clause) {
                return clause->clause_info().modifies_output_descriptor_;
            })) {
                set_output_descriptors(proc, read_query->clauses_, pipeline_context);
            }
            return collect_segments(std::move(proc));
        });
}

void copy_frame_data_to_buffer(
    const SegmentInMemory& destination,
    size_t target_index,
    SegmentInMemory& source,
    size_t source_index,
    const RowRange& row_range,
    const DecodePathData& shared_data,
    std::any& handler_data) {
    const auto num_rows = row_range.diff();
    if (num_rows == 0) {
        return;
    }
    auto& src_column = source.column(static_cast<position_t>(source_index));
    auto& dst_column = destination.column(static_cast<position_t>(target_index));
    auto& buffer = dst_column.data().buffer();
    auto dst_rawtype_size = data_type_size(dst_column.type(), DataTypeMode::EXTERNAL);
    auto offset = dst_rawtype_size * (row_range.first - destination.offset());
    auto total_size = dst_rawtype_size * num_rows;
    buffer.assert_size(offset + total_size);

    auto src_data = src_column.data();
    auto dst_ptr = buffer.data() + offset;

    auto type_promotion_error_msg = fmt::format("Can't promote type {} to type {} in field {}",
                                                src_column.type(), dst_column.type(), destination.field(target_index).name());
    if(auto handler = get_type_handler(src_column.type(), dst_column.type()); handler) {
        handler->convert_type(src_column, buffer, num_rows, offset, src_column.type(), dst_column.type(), shared_data, handler_data, source.string_pool_ptr());
    } else if (is_empty_type(src_column.type().data_type())) {
        dst_column.type().visit_tag([&](auto dst_desc_tag) {
            util::default_initialize<decltype(dst_desc_tag)>(dst_ptr, num_rows * dst_rawtype_size);
        });
        // Do not use src_column.is_sparse() here, as that misses columns that are dense, but have fewer than num_rows values
    } else if (src_column.opt_sparse_map().has_value() && has_valid_type_promotion(src_column.type(), dst_column.type())) {
        details::visit_type(dst_column.type().data_type(), [&](auto dst_tag) {
            using dst_type_info = ScalarTypeInfo<decltype(dst_tag)>;
            util::default_initialize<typename dst_type_info::TDT>(dst_ptr, num_rows * dst_rawtype_size);
            auto typed_dst_ptr = reinterpret_cast<typename dst_type_info::RawType*>(dst_ptr);
            details::visit_type(src_column.type().data_type(), [&](auto src_tag) {
                using src_type_info = ScalarTypeInfo<decltype(src_tag)>;
                Column::for_each_enumerated<typename src_type_info::TDT>(src_column, [typed_dst_ptr](auto enumerating_it) {
                    typed_dst_ptr[enumerating_it.idx()] = static_cast<typename dst_type_info::RawType>(enumerating_it.value());
                });
            });
        });
    } else if (trivially_compatible_types(src_column.type(), dst_column.type())) {
        details::visit_type(src_column.type().data_type() ,[&src_data, &dst_ptr] (auto src_desc_tag) {
            using SourceTDT = ScalarTagType<decltype(src_desc_tag)>;
            using SourceType =  typename decltype(src_desc_tag)::DataTypeTag::raw_type;
            while (auto block = src_data.template next<SourceTDT>()) {
                const auto row_count = block->row_count();
                memcpy(dst_ptr, block->data(), row_count * sizeof(SourceType));
                dst_ptr += row_count * sizeof(SourceType);
            }
        });
    } else if (has_valid_type_promotion(src_column.type(), dst_column.type())) {
        details::visit_type(dst_column.type().data_type() ,[&src_data, &dst_ptr, &src_column, &type_promotion_error_msg] (auto dest_desc_tag) {
            using DestinationType =  typename decltype(dest_desc_tag)::DataTypeTag::raw_type;
            auto typed_dst_ptr = reinterpret_cast<DestinationType *>(dst_ptr);
            details::visit_type(src_column.type().data_type() ,[&src_data, &typed_dst_ptr, &type_promotion_error_msg] (auto src_desc_tag) {
                using source_type_info = ScalarTypeInfo<decltype(src_desc_tag)>;
                if constexpr(std::is_arithmetic_v<typename source_type_info::RawType> && std::is_arithmetic_v<DestinationType>) {
                    const auto src_cend = src_data.cend<typename source_type_info::TDT>();
                    for (auto src_it = src_data.cbegin<typename source_type_info::TDT>(); src_it != src_cend; ++src_it) {
                        *typed_dst_ptr++ = static_cast<DestinationType>(*src_it);
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
    SegmentInMemory&& source_segment_;
    SegmentInMemory target_segment_;
    FrameSlice frame_slice_;
    DecodePathData shared_data_;
    std::any& handler_data_;
    bool fetch_index_;

    CopyToBufferTask(
        SegmentInMemory&& source_segment,
        SegmentInMemory target_segment,
        FrameSlice frame_slice,
        DecodePathData shared_data,
        std::any& handler_data,
        bool fetch_index) :
        source_segment_(std::move(source_segment)),
        target_segment_(std::move(target_segment)),
        frame_slice_(std::move(frame_slice)),
        shared_data_(std::move(shared_data)),
        handler_data_(handler_data),
        fetch_index_(fetch_index) {

    }

    folly::Unit operator()() {
        const auto index_field_count = get_index_field_count(target_segment_);
        for (auto idx = 0u; idx < index_field_count && fetch_index_; ++idx) {
            copy_frame_data_to_buffer(target_segment_, idx, source_segment_, idx, frame_slice_.row_range, shared_data_, handler_data_);
        }

        auto field_count = frame_slice_.col_range.diff() + index_field_count;
        internal::check<ErrorCode::E_ASSERTION_FAILURE>(
            field_count == source_segment_.descriptor().field_count(),
            "Column range does not match segment descriptor field count in copy_segments_to_frame: {} != {}",
            field_count, source_segment_.descriptor().field_count());

        const auto& fields = source_segment_.descriptor().fields();
        for (auto field_col = index_field_count; field_col < field_count; ++field_col) {
            const auto& field = fields.at(field_col);
            const auto& field_name = field.name();
            auto frame_loc_opt = target_segment_.column_index(field_name);
            if (!frame_loc_opt)
                continue;

            copy_frame_data_to_buffer(target_segment_, *frame_loc_opt, source_segment_, field_col, frame_slice_.row_range, shared_data_, handler_data_);
        }
        return folly::Unit{};
    }
};

folly::Future<folly::Unit> copy_segments_to_frame(
    const std::shared_ptr<Store>& store,
    const std::shared_ptr<PipelineContext>& pipeline_context,
    SegmentInMemory frame,
    std::any& handler_data) {
    std::vector<folly::Future<folly::Unit>> copy_tasks;
    DecodePathData shared_data;
    for (auto context_row : folly::enumerate(*pipeline_context)) {
        auto &slice_and_key = context_row->slice_and_key();
        auto &segment = slice_and_key.segment(store);

        copy_tasks.emplace_back(async::submit_cpu_task(
            CopyToBufferTask{
                std::move(segment),
                frame,
                context_row->slice_and_key().slice(),
                shared_data,
                handler_data,
                context_row->fetch_index()}));
    }
    return folly::collect(copy_tasks).via(&async::cpu_executor()).unit();
}

folly::Future<SegmentInMemory> prepare_output_frame(
    std::vector<SliceAndKey>&& items,
    const std::shared_ptr<PipelineContext>& pipeline_context,
    const std::shared_ptr<Store>& store,
    const ReadOptions& read_options,
    std::any& handler_data) {
    pipeline_context->clear_vectors();
    pipeline_context->slice_and_keys_ = std::move(items);
    std::sort(std::begin(pipeline_context->slice_and_keys_), std::end(pipeline_context->slice_and_keys_), [] (const auto& left, const auto& right) {
        return std::tie(left.slice_.row_range, left.slice_.col_range) < std::tie(right.slice_.row_range, right.slice_.col_range);
    });
    adjust_slice_rowcounts(pipeline_context);
    const auto dynamic_schema = opt_false(read_options.dynamic_schema());
    mark_index_slices(pipeline_context, dynamic_schema, pipeline_context->bucketize_dynamic_);
    pipeline_context->ensure_vectors();

    for(auto row : *pipeline_context) {
        row.set_compacted(false);
        row.set_descriptor(row.slice_and_key().segment(store).descriptor_ptr());
        row.set_string_pool(row.slice_and_key().segment(store).string_pool_ptr());
    }

    auto frame = allocate_frame(pipeline_context);
    return copy_segments_to_frame(store, pipeline_context, frame, handler_data).thenValue([frame](auto&&){ return frame; });
}

folly::Future<SegmentInMemory> do_direct_read_or_process(
    const std::shared_ptr<Store>& store,
    const std::shared_ptr<ReadQuery>& read_query,
    const ReadOptions& read_options,
    const std::shared_ptr<PipelineContext>& pipeline_context,
    const DecodePathData& shared_data,
    std::any& handler_data) {
    if(!read_query->clauses_.empty()) {
        ARCTICDB_SAMPLE(RunPipelineAndOutput, 0)
        util::check_rte(!pipeline_context->is_pickled(),"Cannot filter pickled data");
        return read_and_process(store, pipeline_context, read_query, read_options)
            .thenValue([store, pipeline_context, &read_options, &handler_data](std::vector<SliceAndKey>&& segs) {
                return prepare_output_frame(std::move(segs), pipeline_context, store, read_options, handler_data);
            });
    } else {
        ARCTICDB_SAMPLE(MarkAndReadDirect, 0)
        util::check_rte(!(pipeline_context->is_pickled() && std::holds_alternative<RowRange>(read_query->row_filter)), "Cannot use head/tail/row_range with pickled data, use plain read instead");
        mark_index_slices(pipeline_context, opt_false(read_options.dynamic_schema()), pipeline_context->bucketize_dynamic_);
        auto frame = allocate_frame(pipeline_context);
        util::print_total_mem_usage(__FILE__, __LINE__, __FUNCTION__);
        ARCTICDB_DEBUG(log::version(), "Fetching frame data");
        return fetch_data(std::move(frame), pipeline_context, store, opt_false(read_options.dynamic_schema()), shared_data, handler_data);
    }
}

} // namespace read
