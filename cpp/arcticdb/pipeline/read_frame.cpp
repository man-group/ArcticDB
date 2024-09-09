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
#include <arcticdb/python/python_strings.hpp>
#include <arcticdb/pipeline/string_reducers.hpp>

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

StreamDescriptor get_filtered_descriptor(StreamDescriptor&& descriptor, const std::shared_ptr<FieldCollection>& filter_columns) {
    // We assume here that filter_columns_ will always contain the index.

    auto desc = std::move(descriptor);
    auto index = stream::index_type_from_descriptor(desc);
    return util::variant_match(index, [&desc, &filter_columns] (const auto& idx) {
        const std::shared_ptr<FieldCollection>& fields = filter_columns ? filter_columns : desc.fields_ptr();
        return StreamDescriptor{index_descriptor_from_range(desc.id(), idx, *fields)};
    });
}

StreamDescriptor get_filtered_descriptor(const std::shared_ptr<PipelineContext>& context) {
    return get_filtered_descriptor(context->descriptor().clone(), context->filter_columns_);
}

SegmentInMemory allocate_frame(const std::shared_ptr<PipelineContext>& context) {
    ARCTICDB_SAMPLE_DEFAULT(AllocFrame)
    auto [offset, row_count] = offset_and_row_count(context);
    ARCTICDB_DEBUG(log::version(), "Allocated frame with offset {} and row count {}", offset, row_count);
    SegmentInMemory output{get_filtered_descriptor(context),  row_count, AllocationType::PRESIZED, Sparsity::NOT_PERMITTED, DataTypeMode::EXTERNAL};
    output.set_offset(static_cast<position_t>(offset));
    output.set_row_data(static_cast<ssize_t>(row_count - 1));
    output.init_column_map();

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

    return output;
}

size_t get_index_field_count(const SegmentInMemory& frame) {
    return frame.descriptor().index().field_count();
}

const uint8_t* skip_heading_fields(const SegmentHeader & hdr, const uint8_t*& data) {
    const auto has_magic_numbers = hdr.encoding_version() == EncodingVersion::V2;
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
        EncodingVersion encoding_version) {
    if (get_index_field_count(frame)) {
        if (!context.fetch_index()) {
            // not selected, skip decompression
            auto size = encoding_sizes::ndarray_field_compressed_size(field.ndarray());
            if (encoding_version == EncodingVersion::V2)
                size += sizeof(ColumnMagic);

            data += size;
        } else {
            auto &buffer = frame.column(0).data().buffer(); // TODO assert size
            auto &frame_field_descriptor = frame.field(0); //TODO better method
            auto sz = data_type_size(frame_field_descriptor.type(), DataTypeMode::EXTERNAL);
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

void decode_or_expand(
    const uint8_t*& data,
    ChunkedBuffer& dest_buffer,
    const EncodedFieldImpl& encoded_field_info,
    const DecodePathData& shared_data,
    std::any& handler_data,
	EncodingVersion encoding_version,
    const ColumnMapping& m,
    const std::shared_ptr<StringPool>& string_pool) {
    if(auto handler = get_type_handler(m.source_type_desc_, m.dest_type_desc_); handler) {
        handler->handle_type(data, dest_buffer, encoded_field_info, m, shared_data, handler_data, encoding_version, string_pool);
    } else {
        auto* dest = dest_buffer.data() + m.offset_bytes_;  // TODO only works with contiguous data
        const auto dest_bytes = m.dest_bytes_;
        std::optional<util::BitMagic> bv;
        if (encoded_field_info.has_ndarray() && encoded_field_info.ndarray().sparse_map_bytes() > 0) {
            const auto &ndarray = encoded_field_info.ndarray();
            const auto bytes = encoding_sizes::data_uncompressed_size(ndarray);
            ChunkedBuffer sparse{bytes};
            SliceDataSink sparse_sink{sparse.data(), bytes};
            data += decode_field(m.source_type_desc_, encoded_field_info, data, sparse_sink, bv, encoding_version);
            m.source_type_desc_.visit_tag([dest, dest_bytes, &bv, &sparse](const auto tdt) {
                using TagType = decltype(tdt);
                using RawType = typename TagType::DataTypeTag::raw_type;
                util::default_initialize<TagType>(dest, dest_bytes);
                util::expand_dense_buffer_using_bitmap<RawType>(bv.value(), sparse.data(), dest);
            });
        } else {
            SliceDataSink sink(dest, dest_bytes);
            const auto &ndarray = encoded_field_info.ndarray();
            if (const auto bytes = encoding_sizes::data_uncompressed_size(ndarray); bytes < dest_bytes) {
                m.source_type_desc_.visit_tag([dest, bytes, dest_bytes](const auto tdt) {
                    using TagType = decltype(tdt);
                    util::default_initialize<TagType>(dest + bytes, dest_bytes - bytes);
                });
            }
            data += decode_field(m.source_type_desc_, encoded_field_info, data, sink, bv, encoding_version);
        }
    }
}

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
        ssize_t prev_col_offset,
        size_t source_col,
        size_t first_col_offset,
        size_t index_fieldcount,
        const EncodedFieldCollection& fields,
        const SegmentHeader& hdr) {
    const auto next_col = prev_col_offset + 1;
    auto skipped_cols = source_col - next_col;
    if(skipped_cols) {
        const auto bytes_to_skip = get_field_range_compressed_size((next_col - first_col_offset) + index_fieldcount, skipped_cols, hdr, fields);
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

void decode_into_frame_static(
    SegmentInMemory &frame,
    PipelineContextRow &context,
    Segment &&s,
    const DecodePathData& shared_data,
    std::any& handler_data) {
    auto seg = std::move(s);
    ARCTICDB_SAMPLE_DEFAULT(DecodeIntoFrame)
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
        decode_index_field(frame, index_field, data, begin, end, context, encoding_version);

        StaticColumnMappingIterator it(context, index_fieldcount);
        if(it.invalid())
            return;

        while (it.has_next()) {
            advance_skipped_cols(data, static_cast<ssize_t>(it.prev_col_offset()), it.source_col(), it.first_slice_col_offset(), index_fieldcount, fields, hdr);
            if(has_magic_nums)
                util::check_magic_in_place<ColumnMagic>(data);

            auto& encoded_field = fields.at(it.source_field_pos());
            util::check(it.source_field_pos() < size_t(fields.size()), "Field index out of range: {} !< {}", it.source_field_pos(), fields.size());
            auto field_name = context.descriptor().fields(it.source_field_pos()).name();
            auto& buffer = frame.column(static_cast<ssize_t>(it.dest_col())).data().buffer();
            ColumnMapping m{frame, it.dest_col(), it.source_field_pos(), context};
            const bool types_trivially_compatible = trivially_compatible_types(m.source_type_desc_, m.dest_type_desc_);
            const bool any_type_is_empty = is_empty_type(m.source_type_desc_.data_type()) || is_empty_type(m.dest_type_desc_.data_type());
            util::check(
                types_trivially_compatible || any_type_is_empty,
                "Column type conversion from {} to {} not implemented in column {}:{} -> {}:{}",
                m.source_type_desc_,
                m.dest_type_desc_,
                it.source_col(),
                field_name,
                it.dest_col(),
                m.frame_field_descriptor_.name()
            );
            util::check(
                data != end || remaining_fields_empty(it, context),
                "Reached end of input block with {} fields to decode",
                it.remaining_fields()
            );
            decode_or_expand(
                data,
                buffer,
                encoded_field,
                shared_data,
                handler_data,
                encoding_version,
                m,
                context.string_pool_ptr()
            );
            ARCTICDB_TRACE(log::codec(), "Decoded column {} to position {}", field_name, data - begin);

            it.advance();

            if(it.at_end_of_selected()) {
                advance_skipped_cols(data,
                    static_cast<ssize_t>(it.prev_col_offset()),
                    it.last_slice_col_offset(),
                    it.first_slice_col_offset(),
                    it.index_fieldcount(),
                    fields,
                    hdr);
                break;
            } else {
                if(has_magic_nums)
                    util::check_magic_in_place<ColumnMagic>(data);
            }
        }
    }
    ARCTICDB_TRACE(log::codec(), "Frame decoded into static schema");
}

void decode_into_frame_dynamic(
    SegmentInMemory& frame,
    PipelineContextRow& context,
    Segment&& s,
    const DecodePathData& shared_data,
    std::any& handler_data
) {
    ARCTICDB_SAMPLE_DEFAULT(DecodeIntoFrame)
    auto seg = std::move(s);
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
        decode_index_field(frame, index_field, data, begin, end, context, encoding_version);

        auto field_count = context.slice_and_key().slice_.col_range.diff() + index_fieldcount;
        for (auto field_col = index_fieldcount; field_col < field_count; ++field_col) {
            auto field_name = context.descriptor().fields(field_col).name();
            auto& encoded_field = fields.at(field_col);
            auto frame_loc_opt = frame.column_index(field_name);
            if (!frame_loc_opt) {
                // Column is not selected in the output frame.
                advance_field_size(encoded_field, data, has_magic_numbers);
                continue;
            }

            auto dst_col = *frame_loc_opt;
            auto& buffer = frame.column(static_cast<position_t>(dst_col)).data().buffer();
            ColumnMapping m{frame, dst_col, field_col, context};

            util::check(
                static_cast<bool>(has_valid_type_promotion(m.source_type_desc_, m.dest_type_desc_)),
                "Can't promote type {} to type {} in field {}",
                m.source_type_desc_,
                m.dest_type_desc_,
                m.frame_field_descriptor_.name()
            );
            ARCTICDB_TRACE(
                log::storage(),
                "Creating data slice at {} with total size {} ({} rows)",
                m.offset_bytes_,
                m.dest_bytes_,
                context.slice_and_key().slice_.row_range.diff()
            );
            const bool source_is_empty = is_empty_type(m.source_type_desc_.data_type());
            util::check(
                data != end || source_is_empty,
                "Reached end of input block with {} fields to decode",
                field_count - field_col
            );
            decode_or_expand(
                data,
                buffer,
                encoded_field,
                shared_data,
                handler_data,
                encoding_version,
                m,
                context.string_pool_ptr()
            );

            if (!trivially_compatible_types(m.source_type_desc_, m.dest_type_desc_) && !source_is_empty) {
                m.dest_type_desc_.visit_tag([&buffer, &m, shared_data] (auto dest_desc_tag) {
                    using DestinationType =  typename decltype(dest_desc_tag)::DataTypeTag::raw_type;
                    m.source_type_desc_.visit_tag([&buffer, &m] (auto src_desc_tag ) {
                        using SourceType =  typename decltype(src_desc_tag)::DataTypeTag::raw_type;
                        if constexpr(std::is_arithmetic_v<SourceType> && std::is_arithmetic_v<DestinationType>) {
                            // If the source and destination types are different, then sizeof(destination type) >= sizeof(source type)
                            // We have decoded the column of source type directly onto the output buffer above
                            // We therefore need to iterate backwards through the source values, static casting them to the destination
                            // type to avoid overriding values we haven't cast yet.
                            const auto src_ptr_offset = data_type_size(m.source_type_desc_, DataTypeMode::INTERNAL) * (m.num_rows_ - 1);
                            const auto dest_ptr_offset = data_type_size(m.dest_type_desc_, DataTypeMode::INTERNAL) * (m.num_rows_ - 1);
                            auto src_ptr = reinterpret_cast<SourceType *>(buffer.data() + m.offset_bytes_ + src_ptr_offset);
                            auto dest_ptr = reinterpret_cast<DestinationType *>(buffer.data() + m.offset_bytes_ + dest_ptr_offset);
                            for (auto i = 0u; i < m.num_rows_; ++i) {
                                *dest_ptr-- = static_cast<DestinationType>(*src_ptr--);
                            }
                        } else {
                            util::raise_rte("Can't promote type {} to type {} in field {}", m.source_type_desc_, m.dest_type_desc_, m.frame_field_descriptor_.name());
                        }
                    });
                });
            }
            ARCTICDB_TRACE(log::codec(), "Decoded column {} to position {}", frame.field(dst_col).name(), data - begin);
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

public:
    NullValueReducer(
        Column &column,
        std::shared_ptr<PipelineContext> &context,
        SegmentInMemory frame,
        DecodePathData shared_data,
        std::any& handler_data) :
        column_(column),
        context_(context),
        frame_(std::move(frame)),
        pos_(frame_.offset()),
        shared_data_(std::move(shared_data)),
        handler_data_(handler_data){
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
            if (const std::shared_ptr<TypeHandler>& handler = arcticdb::TypeHandlerRegistry::instance()->get_handler(column_.type()); handler) {
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
            if (const std::shared_ptr<TypeHandler>& handler = arcticdb::TypeHandlerRegistry::instance()->get_handler(column_.type()); handler) {
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
    bool dynamic_schema_;

    ReduceColumnTask(
        SegmentInMemory frame,
        size_t c,
        std::shared_ptr<FrameSliceMap> slice_map,
        std::shared_ptr<PipelineContext>& context,
        DecodePathData shared_data,
        std::any& handler_data,
        bool dynamic_schema) :
        frame_(std::move(frame)),
        column_index_(c),
        slice_map_(std::move(slice_map)),
        context_(context),
        shared_data_(std::move(shared_data)),
        handler_data_(handler_data),
        dynamic_schema_(dynamic_schema) {
    }

    folly::Unit operator()() {
        const auto &frame_field = frame_.field(column_index_);
        const auto field_type = frame_field.type().data_type();
        auto &column = frame_.column(static_cast<position_t>(column_index_));

        const auto column_data = slice_map_->columns_.find(frame_field.name());
        if(dynamic_schema_ && column_data == slice_map_->columns_.end()) {
            if (const std::shared_ptr<TypeHandler>& handler = arcticdb::TypeHandlerRegistry::instance()->get_handler(column.type()); handler) {
                handler->default_initialize(column.buffer(), 0, frame_.row_count() * handler->type_size(), shared_data_, handler_data_);
            } else {
                column.default_initialize_rows(0, frame_.row_count(), false);
            }
        } else if (column_data != slice_map_->columns_.end()) {
            if(dynamic_schema_) {
                NullValueReducer null_reducer{column, context_, frame_, shared_data_, handler_data_};
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
        } else if (!dynamic_schema_ && column_data == slice_map_->columns_.end() && is_sequence_type(column.type().data_type())) {
            internal::raise<ErrorCode::E_ASSERTION_FAILURE>("Column with index {} is not in static schema slice map.", column_index_);
        }
        return folly::Unit{};
    }
};

    void reduce_and_fix_columns(
        std::shared_ptr<PipelineContext> &context,
        SegmentInMemory &frame,
        const ReadOptions& read_options,
        std::any& handler_data
) {
    ARCTICDB_SAMPLE_DEFAULT(ReduceAndFixStringCol)
    ARCTICDB_DEBUG(log::version(), "Reduce and fix columns");
    if(frame.empty())
        return;

    bool dynamic_schema = opt_false(read_options.dynamic_schema_);
    auto slice_map = std::make_shared<FrameSliceMap>(context, dynamic_schema);
    DecodePathData shared_data;

    static const auto batch_size = ConfigsMap::instance()->get_int("ReduceColumns.BatchSize", 100);
    folly::collect(folly::window(frame.descriptor().fields().size(), [&] (size_t field) {
        return async::submit_cpu_task(ReduceColumnTask(frame, field, slice_map, context, shared_data, handler_data, dynamic_schema));
    }, batch_size)).via(&async::io_executor()).get();

}

folly::Future<std::vector<VariantKey>> fetch_data(
    const SegmentInMemory& frame,
    const std::shared_ptr<PipelineContext> &context,
    const std::shared_ptr<stream::StreamSource>& ssource,
    bool dynamic_schema,
    DecodePathData shared_data,
    std::any& handler_data
    ) {
    ARCTICDB_SAMPLE_DEFAULT(FetchSlices)
    if (frame.empty())
        return {std::vector<VariantKey>{}};

    std::vector<std::pair<VariantKey, stream::StreamSource::ReadContinuation>> keys_and_continuations;
    keys_and_continuations.reserve(context->slice_and_keys_.size());
    context->ensure_vectors();
    {
        ARCTICDB_SUBSAMPLE_DEFAULT(QueueReadContinuations)
        for ( auto& row : *context) {
            keys_and_continuations.emplace_back(row.slice_and_key().key(),
            [row=row, frame=frame, dynamic_schema=dynamic_schema, shared_data, &handler_data](auto &&ks) mutable {
                auto key_seg = std::forward<storage::KeySegmentPair>(ks);
                if(dynamic_schema) {
                    decode_into_frame_dynamic(frame, row, std::move(key_seg.segment()), shared_data, handler_data);
                } else {
                    decode_into_frame_static(frame, row, std::move(key_seg.segment()), shared_data, handler_data);
                }

                return key_seg.variant_key();
            });
        }
    }
    ARCTICDB_SUBSAMPLE_DEFAULT(DoBatchReadCompressed)
    return ssource->batch_read_compressed(std::move(keys_and_continuations), BatchReadArgs{});
}

} // namespace read
