/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/util/variant.hpp>

#include <arcticdb/codec/encoding_sizes.hpp>
#include <arcticdb/codec/codec.hpp>
#include <arcticdb/pipeline/index_segment_reader.hpp>
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
#include <arcticdb/util/third_party/emilib_map.hpp>
#include <arcticdb/codec/variant_encoded_field_collection.hpp>

#include <google/protobuf/util/message_differencer.h>
#include <folly/SpinLock.h>
#include <folly/gen/Base.h>

namespace arcticdb::pipelines {

/*
   _  _  _
  |i||_||_|    We mark the first slice of each row of the grid as requiring a fetch_index
  |i||_||_|    This helps us calculating the row_count during allocation. Note that all slices
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
        if(filter_columns) {
            return StreamDescriptor{index_descriptor(desc.id(), idx, *filter_columns)};
        }
        else {
            return StreamDescriptor{index_descriptor(desc.id(), idx, *desc.fields_ptr())};
        }
    });
}

StreamDescriptor get_filtered_descriptor(const std::shared_ptr<PipelineContext>& context) {
    return get_filtered_descriptor(context->descriptor().clone(), context->filter_columns_);
}

SegmentInMemory allocate_frame(const std::shared_ptr<PipelineContext>& context) {
    ARCTICDB_SAMPLE_DEFAULT(AllocFrame)
    auto [offset, row_count] = offset_and_row_count(context);
    ARCTICDB_DEBUG(log::version(), "Allocated frame with offset {} and row count {}", offset, row_count);
    SegmentInMemory output{get_filtered_descriptor(context),  row_count, true};
    output.set_offset(static_cast<position_t>(offset));
    output.set_row_data(static_cast<ssize_t>(row_count - 1));
    output.init_column_map();

    if(context->orig_desc_) {
        for(const auto& field : context->orig_desc_.value().fields()) {
            auto col_index = output.column_index(field.name());
            if(!col_index)
                continue;

            auto& column = output.column(static_cast<position_t>(col_index.value()));
            if(field.type().data_type() != column.type().data_type())
                column.set_orig_type(field.type());
        }
    }

    return output;
}

size_t get_index_field_count(const SegmentInMemory& frame) {
    return frame.descriptor().index().field_count();
}

const uint8_t* skip_heading_fields(const arcticdb::proto::encoding::SegmentHeader & hdr, const uint8_t*& data) {
    const auto has_magic_numbers = EncodingVersion(hdr.encoding_version()) == EncodingVersion::V2;
    if(has_magic_numbers)
        check_magic<MetadataMagic>(data);

    if (hdr.has_metadata_field()) {
        auto metadata_size = encoding_sizes::ndarray_field_compressed_size(hdr.metadata_field().ndarray());
        ARCTICDB_DEBUG(log::version(), "Skipping {} bytes of metadata", metadata_size);
        data += metadata_size;
    }

    if(has_magic_numbers)
        check_magic<DescriptorMagic>(data);

    if(hdr.has_descriptor_field()) {
        auto descriptor_field_size = encoding_sizes::ndarray_field_compressed_size(hdr.descriptor_field().ndarray());
        ARCTICDB_DEBUG(log::version(), "Skipping {} bytes of descriptor", descriptor_field_size);
        data += descriptor_field_size;
    }

    if(has_magic_numbers)
        check_magic<IndexMagic>(data);

    if(hdr.has_index_descriptor_field()) {
        auto index_fields_size = encoding_sizes::ndarray_field_compressed_size(hdr.index_descriptor_field().ndarray());
        ARCTICDB_DEBUG(log::version(), "Skipping {} bytes of index descriptor", index_fields_size);
            data += index_fields_size;
    }
    return data;
}

void decode_string_pool(const arcticdb::proto::encoding::SegmentHeader & hdr, const uint8_t*& data, const uint8_t *begin ARCTICDB_UNUSED, const uint8_t* end, PipelineContextRow &context) {
    if (hdr.has_string_pool_field()) {
        ARCTICDB_DEBUG(log::codec(), "Decoding string pool at position: {}", data - begin);
        util::check(data != end, "Reached end of input block with string pool fields to decode");
        context.allocate_string_pool();
        std::optional<util::BitMagic> bv;

        // Note that this will decode the entire string pool into a ChunkedBuffer with exactly 1 chunk
        if(EncodingVersion(hdr.encoding_version()) == EncodingVersion::V2)
            check_magic<StringPoolMagic>(data);

        data += decode_field(string_pool_descriptor().type(),
                       hdr.string_pool_field(),
                       data,
                       context.string_pool(),
                       bv);

        ARCTICDB_TRACE(log::codec(), "Decoded string pool to position {}", data - begin);
    }
}

template<typename EncodedFieldType>
void decode_index_field_impl(
        SegmentInMemory &frame,
        const EncodedFieldType& field,
        const uint8_t*& data,
        const uint8_t *begin ARCTICDB_UNUSED,
        const uint8_t* end ARCTICDB_UNUSED,
        PipelineContextRow &context) {
    if (get_index_field_count(frame)) {
        if (!context.fetch_index()) {
            // not selected, skip decompression
            auto size = encoding_sizes::ndarray_field_compressed_size(field.ndarray());
            if constexpr(std::is_same_v<EncodedFieldType, arcticdb::EncodedField>)
                size += sizeof(ColumnMagic);

            data += size;
        } else {
            auto &buffer = frame.column(0).data().buffer(); // TODO assert size
            auto &frame_field_descriptor = frame.field(0); //TODO better method
            auto sz = sizeof_datatype(frame_field_descriptor.type());
            auto offset = sz * (context.slice_and_key().slice_.row_range.first - frame.offset());
            auto tot_size = sz * context.slice_and_key().slice_.row_range.diff();

            SliceDataSink sink(buffer.data() + offset, tot_size);
            ARCTICDB_DEBUG(log::storage(), "Creating index slice with total size {} ({} - {})", tot_size, sz,
                                 context.slice_and_key().slice_.row_range.diff());

            const auto fields_match = frame_field_descriptor.type() == context.descriptor().fields(0).type();
            util::check(fields_match, "Cannot coerce index type from {} to {}",
                        context.descriptor().fields(0).type(), frame_field_descriptor.type());

            std::optional<util::BitMagic> bv;
            data += decode_field(frame_field_descriptor.type(), field, data, sink, bv);
            util::check(!bv, "Unexpected sparse vector in index field");
            ARCTICDB_DEBUG(log::codec(), "Decoded index column {} to position {}", 0, data - begin);
        }
    }
}

void decode_index_field(
        SegmentInMemory &frame,
        VariantField field,
        const uint8_t*& data,
        const uint8_t *begin ARCTICDB_UNUSED,
        const uint8_t* end ARCTICDB_UNUSED,
        PipelineContextRow &context) {
    util::variant_match(field, [&] (auto field) {
        decode_index_field_impl(frame, *field, data, begin, end, context);
    });
}

template <typename EncodedFieldType>
void decode_or_expand_impl(
    const uint8_t*& data,
    uint8_t* dest,
    const EncodedFieldType& encoded_field_info,
    const TypeDescriptor& type_descriptor,
    size_t dest_bytes,
    std::shared_ptr<BufferHolder> buffers) {
    if(auto handler = TypeHandlerRegistry::instance()->get_handler(type_descriptor.data_type()); handler) {
        handler->handle_type(data, dest, VariantField{&encoded_field_info}, type_descriptor, dest_bytes, buffers);
    } else {
        std::optional<util::BitMagic> bv;
        if (encoded_field_info.has_ndarray() && encoded_field_info.ndarray().sparse_map_bytes() > 0) {
            const auto &ndarray = encoded_field_info.ndarray();
            const auto bytes = encoding_sizes::data_uncompressed_size(ndarray);
            ChunkedBuffer sparse{bytes};
            SliceDataSink sparse_sink{sparse.data(), bytes};
            data += decode_field(type_descriptor, encoded_field_info, data, sparse_sink, bv);
            type_descriptor.visit_tag([dest, dest_bytes, &bv, &sparse](const auto tdt) {
                using TagType = decltype(tdt);
                using RawType = typename TagType::DataTypeTag::raw_type;
                util::default_initialize<TagType>(dest, dest_bytes);
                util::expand_dense_buffer_using_bitmap<RawType>(bv.value(), sparse.data(), dest);
            });
        } else {
            SliceDataSink sink(dest, dest_bytes);
            const auto &ndarray = encoded_field_info.ndarray();
            if (const auto bytes = encoding_sizes::data_uncompressed_size(ndarray); bytes < dest_bytes) {
                type_descriptor.visit_tag([dest, bytes, dest_bytes](const auto tdt) {
                    using TagType = decltype(tdt);
                    util::default_initialize<TagType>(dest + bytes, dest_bytes - bytes);
                });
            }
            data += decode_field(type_descriptor, encoded_field_info, data, sink, bv);
        }
    }
}

size_t get_field_range_compressed_size(size_t start_idx, size_t num_fields,
                                       const arcticdb::proto::encoding::SegmentHeader& hdr,
                                       const VariantEncodedFieldCollection& fields) {
    size_t total = 0ULL;
    const size_t magic_num_size = EncodingVersion(hdr.encoding_version()) == EncodingVersion::V2 ? sizeof(ColumnMagic) : 0u;
    ARCTICDB_DEBUG(log::version(), "Skipping between {} and {}", start_idx, start_idx + num_fields);
    for(auto i = start_idx; i < start_idx + num_fields; ++i) {
        util::variant_match(fields.at(i), [&total, magic_num_size] (const auto& field) {
            ARCTICDB_DEBUG(log::version(), "Adding {}", encoding_sizes::ndarray_field_compressed_size(field->ndarray()) + magic_num_size);
            total += encoding_sizes::ndarray_field_compressed_size(field->ndarray()) + magic_num_size;
        });
    }
    ARCTICDB_DEBUG(log::version(), "Fields {} to {} contain {} bytes", start_idx, start_idx + num_fields, total);
    return total;
}

void decode_or_expand(
    const uint8_t*& data,
    uint8_t* dest,
    const VariantField& field,
    const TypeDescriptor& type_descriptor,
    size_t dest_bytes,
    std::shared_ptr<BufferHolder> buffers) {
    util::variant_match(field, [&] (auto field) {
        decode_or_expand_impl(data, dest, *field, type_descriptor, dest_bytes, buffers);
    });
}

void advance_field_size(
    const VariantField& variant_field,
    const uint8_t*& data,
    bool has_magic_numbers
    ) {
    util::variant_match(variant_field, [&data, has_magic_numbers] (auto field) {
    const size_t magic_num_size = has_magic_numbers ? sizeof(ColumnMagic) : 0ULL;
    data += encoding_sizes::ndarray_field_compressed_size(field->ndarray()) + magic_num_size;
  });
}

void advance_skipped_cols(
        const uint8_t*& data,
        ssize_t prev_col_offset,
        size_t source_col,
        size_t first_col_offset,
        size_t index_fieldcount,
        const VariantEncodedFieldCollection& fields,
        const arcticdb::proto::encoding::SegmentHeader& hdr) {
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
    }
    return true;
}

void decode_into_frame_static(
    SegmentInMemory &frame,
    PipelineContextRow &context,
    Segment &&s,
    const std::shared_ptr<BufferHolder> buffers) {
    auto seg = std::move(s);
    ARCTICDB_SAMPLE_DEFAULT(DecodeIntoFrame)
    const uint8_t *data = seg.buffer().data();
    const uint8_t *begin = data;
    const uint8_t *end = begin + seg.buffer().bytes();
    auto &hdr = seg.header();
    auto index_fieldcount = get_index_field_count(frame);
    data = skip_heading_fields(hdr, data);
    context.set_descriptor(StreamDescriptor{ std::make_shared<StreamDescriptor::Proto>(std::move(*hdr.mutable_stream_descriptor())), seg.fields_ptr() });
    context.set_compacted(hdr.compacted());
    ARCTICDB_DEBUG(log::version(), "Num fields: {}", seg.header().fields_size());
    const bool has_magic_nums = EncodingVersion(hdr.encoding_version()) == EncodingVersion::V2;

    if (data != end) {
        VariantEncodedFieldCollection fields(seg);
        auto index_field = fields.at(0u);
        decode_index_field(frame, index_field, data, begin, end, context);

        StaticColumnMappingIterator it(context, index_fieldcount);
        if(it.invalid())
            return;

        while (it.has_next()) {
            advance_skipped_cols(data, static_cast<ssize_t>(it.prev_col_offset()), it.source_col(), it.first_slice_col_offset(), index_fieldcount, fields, hdr);
            if(has_magic_nums)
                check_magic_in_place<ColumnMagic>(data);

            auto encoded_field = fields.at(it.source_field_pos());
            util::check(it.source_field_pos() < size_t(fields.size()), "Field index out of range: {} !< {}", it.source_field_pos(), fields.size());
            auto field_name = context.descriptor().fields(it.source_field_pos()).name();
            auto& buffer = frame.column(static_cast<ssize_t>(it.dest_col())).data().buffer();
            ColumnMapping m{frame, it.dest_col(), it.source_field_pos(), context};
            util::check(trivially_compatible_types(m.source_type_desc_, m.dest_type_desc_), "Column type conversion from {} to {} not implemented in column {}:{} -> {}:{}",
                        m.source_type_desc_,
                        m.dest_type_desc_,
                        it.source_col(),
                        field_name,
                        it.dest_col(),
                        m.frame_field_descriptor_.name());

            util::check(data != end || remaining_fields_empty(it, context), "Reached end of input block with {} fields to decode", it.remaining_fields());
            decode_or_expand(data, buffer.data() + m.offset_bytes_, encoded_field, m.source_type_desc_,  m.dest_bytes_, buffers);
            ARCTICDB_TRACE(log::codec(), "Decoded column {} to position {}", field_name, data - begin);

            it.advance();

            if(it.at_end_of_selected()) {
                advance_skipped_cols(data, static_cast<ssize_t>(it.prev_col_offset()), it.last_slice_col_offset(), it.first_slice_col_offset(), it.index_fieldcount(), fields, hdr);
                break;
            } else {
                if(has_magic_nums)
                    check_magic_in_place<ColumnMagic>(data);
            }
        }

        decode_string_pool(hdr, data, begin, end, context);
    }
}

void decode_into_frame_dynamic(
        SegmentInMemory &frame,
        PipelineContextRow &context,
        Segment &&s,
        const std::shared_ptr<BufferHolder>& buffers) {
    ARCTICDB_SAMPLE_DEFAULT(DecodeIntoFrame)
    auto seg = std::move(s);
    const uint8_t *data = seg.buffer().data();
    const uint8_t *begin = data;
    const uint8_t *end = begin + seg.buffer().bytes();
    auto &hdr = seg.header();
    auto index_fieldcount = get_index_field_count(frame);
    data = skip_heading_fields(hdr, data);
    context.set_descriptor(StreamDescriptor{std::make_shared<StreamDescriptor::Proto>(std::move(*hdr.mutable_stream_descriptor())), seg.fields_ptr()});
    context.set_compacted(hdr.compacted());
    const bool has_magic_numbers = EncodingVersion(hdr.encoding_version()) == EncodingVersion::V2;

    if (data != end) {
        VariantEncodedFieldCollection fields(seg);
        auto index_field = fields.at(0u);
        decode_index_field(frame, index_field, data, begin, end, context);

        auto field_count = context.slice_and_key().slice_.col_range.diff() + index_fieldcount;
        for (auto field_col = index_fieldcount; field_col < field_count; ++field_col) {
            auto field_name = context.descriptor().fields(field_col).name();
            auto encoded_field = fields.at(field_col);
            auto frame_loc_opt = frame.column_index(field_name);
            if (!frame_loc_opt) {
                // Column is not selected in the output frame.
                advance_field_size(encoded_field, data, has_magic_numbers);
                continue;
            }

            auto dst_col = frame_loc_opt.value();
            auto& buffer = frame.column(static_cast<position_t>(dst_col)).data().buffer();
            if(ColumnMapping m{frame, dst_col, field_col, context};!trivially_compatible_types(m.source_type_desc_, m.dest_type_desc_)) {
                util::check(static_cast<bool>(has_valid_type_promotion(m.source_type_desc_, m.dest_type_desc_)), "Can't promote type {} to type {} in field {}",
                            m.source_type_desc_, m.dest_type_desc_, m.frame_field_descriptor_.name());

                    m.dest_type_desc_.visit_tag([&buffer, &m, &data, encoded_field, buffers] (auto dest_desc_tag) {
                        using DestinationType =  typename decltype(dest_desc_tag)::DataTypeTag::raw_type;
                        m.source_type_desc_.visit_tag([&buffer, &m, &data, &encoded_field, &buffers] (auto src_desc_tag ) {
                            using SourceType =  typename decltype(src_desc_tag)::DataTypeTag::raw_type;
                            if constexpr(std::is_arithmetic_v<SourceType> && std::is_arithmetic_v<DestinationType>) {
                                const auto src_bytes = sizeof_datatype(m.source_type_desc_) * m.num_rows_;
                                Buffer tmp_buf{src_bytes};
                                decode_or_expand(data, tmp_buf.data(), encoded_field, m.source_type_desc_, src_bytes, buffers);
                                auto src_ptr = reinterpret_cast<SourceType *>(tmp_buf.data());
                                auto dest_ptr = reinterpret_cast<DestinationType *>(buffer.data() + m.offset_bytes_);
                                for (auto i = 0u; i < m.num_rows_; ++i) {
                                    *dest_ptr++ = static_cast<DestinationType>(*src_ptr++);
                                }
                            }
                            else {
                                util::raise_rte("Can't promote type {} to type {} in field {}", m.source_type_desc_, m.dest_type_desc_, m.frame_field_descriptor_.name());
                            }
                        });
                    });
                    ARCTICDB_TRACE(log::codec(), "Decoded column {} to position {}", m.frame_field_descriptor_.name(), data - begin);
            } else {
                ARCTICDB_TRACE(log::storage(), "Creating data slice at {} with total size {} ({} rows)", m.offset_bytes_, m.dest_bytes_,
                                     context.slice_and_key().slice_.row_range.diff());
                util::check(data != end,
                            "Reached end of input block with {} fields to decode",
                            field_count - field_col);

                decode_or_expand(data, buffer.data() + m.offset_bytes_, encoded_field, m.source_type_desc_, m.dest_bytes_, buffers);
            }
            ARCTICDB_TRACE(log::codec(), "Decoded column {} to position {}", frame.field(dst_col).name(), data - begin);
        }

        decode_string_pool(hdr, data, begin, end, context);
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

public:
    NullValueReducer(
        Column &column,
        std::shared_ptr<PipelineContext> &context,
        SegmentInMemory frame) :
        column_(column),
        context_(context),
        frame_(std::move(frame)),
        pos_(frame_.offset()) {
    }

    [[nodiscard]] size_t cursor(const PipelineContextRow &context_row) const {
        return context_row.slice_and_key().slice_.row_range.first;
    }

    void reduce(PipelineContextRow &context_row){
        auto &slice_and_key = context_row.slice_and_key();
        auto sz_to_advance = slice_and_key.slice_.row_range.diff();
        auto current_pos = context_row.slice_and_key().slice_.row_range.first;
        if (current_pos != pos_) {
            auto num_rows = current_pos - pos_;
            column_.default_initialize_rows(pos_ - frame_.offset(), num_rows, false);
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
            auto num_rows =  end - pos_;
            column_.default_initialize_rows(pos_ - frame_.offset(), num_rows, false);
        }
    }
};

size_t get_max_string_size_in_column(
        ChunkedBuffer &src_buffer,
        std::shared_ptr<PipelineContext> &context,
        SegmentInMemory &frame,
        const Field &frame_field,
        const FrameSliceMap& slice_map,
        bool check_all) {
    const auto column_data = slice_map.columns_.find(frame_field.name());
    util::check(column_data != slice_map.columns_.end(), "Data for column {} was not generated in map", frame_field.name());
    auto column_width = size_t{0};

    for (const auto &row : column_data->second) {
        PipelineContextRow context_row{context, row.second.context_index_};
        size_t string_size = 0;
        if(check_all || context_row.compacted()) {
            string_size = get_max_string_size(context_row, src_buffer, frame.offset());
        } else {
            string_size = get_first_string_size(context_row, src_buffer, frame.offset());
        }

        column_width = std::max(
            column_width,
            std::max(column_width, string_size)
        );
    }

    return column_width;
}

class StringReducer {
protected:
    Column& column_;
    std::shared_ptr<PipelineContext> context_;
    SegmentInMemory frame_;
    const Field& frame_field_;
    size_t row_ ;
    ChunkedBuffer& src_buffer_;
    size_t column_width_;
    ChunkedBuffer dest_buffer_;
    uint8_t *dst_;

public:
    StringReducer(
        Column& column,
        std::shared_ptr<PipelineContext> context,
        SegmentInMemory frame,
        const Field& frame_field,
        size_t alloc_width) :
        column_(column),
        context_(std::move(context)),
        frame_(std::move(frame)),
        frame_field_(frame_field),
        row_(0),
        src_buffer_(column.data().buffer()),
        column_width_(alloc_width),
        dest_buffer_(ChunkedBuffer::presized(frame_.row_count() * column_width_)),
        dst_(dest_buffer_.data()) {
        std::memset(dest_buffer_.data(), 0, dest_buffer_.bytes());
    }

    virtual void finalize() {
    }

    virtual ~StringReducer() {
        src_buffer_ = std::move(dest_buffer_);
        column_.set_inflated(frame_.row_count());
    }

public:
    virtual void reduce(PipelineContextRow& context_row, size_t column_index) = 0;
};

using LockType = folly::SpinLock;

class EmptyDynamicStringReducer {
protected:
    Column& column_;
    SegmentInMemory frame_;
    const Field& frame_field_;
    size_t row_ ;
    ChunkedBuffer& src_buffer_;
    size_t column_width_;
    ChunkedBuffer dest_buffer_;
    uint8_t *dst_;
    PyObject** ptr_dest_;
    std::shared_ptr<LockType> lock_;
    std::shared_ptr<PyObject> py_nan_;

public:
    EmptyDynamicStringReducer(
        Column& column,
        SegmentInMemory frame,
        const Field& frame_field,
        size_t alloc_width,
        std::shared_ptr<LockType> spinlock) :
        column_(column),
        frame_(std::move(frame)),
        frame_field_(frame_field),
        row_(0),
        src_buffer_(column.data().buffer()),
        column_width_(alloc_width),
        dest_buffer_(ChunkedBuffer::presized(frame_.row_count() * column_width_)),
        dst_(dest_buffer_.data()),
        ptr_dest_(reinterpret_cast<PyObject**>(dst_)),
        lock_(std::move(spinlock)),
        py_nan_(create_py_nan(lock_),[lock=lock_](PyObject* py_obj) {
            lock->lock();
            Py_DECREF(py_obj);
            lock->unlock(); }) {
    }

    ~EmptyDynamicStringReducer() {
        src_buffer_ = std::move(dest_buffer_);
        column_.set_inflated(frame_.row_count());
    }

    void reduce(size_t end) {
        auto none = py::none{};
        auto ptr_src = get_offset_ptr_at(row_, src_buffer_);
        auto non_counter = 0u;
        for (; row_ < end; ++row_, ++ptr_src, ++ptr_dest_) {
            auto offset = *ptr_src;
            if (offset == not_a_string()) {
                ++non_counter;
                *ptr_dest_ = none.ptr();
            } else if (offset == nan_placeholder()) {
                *ptr_dest_ = py_nan_.get();
                Py_INCREF(py_nan_.get());
            } else {
                util::raise_rte("Got unexpected offset in default initialization column");
            }
        }

        if(non_counter != 0u) {
            lock_->lock();
            for(auto j = 0u; j < non_counter; ++j)
                none.inc_ref();
            lock_->unlock();
        }
    }
};

class FixedStringReducer : public StringReducer{
public:
    FixedStringReducer(
        Column& column,
        std::shared_ptr<PipelineContext> &context,
        SegmentInMemory frame,
        const Field& frame_field,
        size_t alloc_width) :
        StringReducer(column, context, std::move(frame), frame_field, alloc_width) {
    }

    void reduce(PipelineContextRow& context_row, size_t) override {
        size_t end = context_row.slice_and_key().slice_.row_range.second - frame_.offset();
        for (; row_ < end; ++row_) {
            auto val = get_string_from_buffer(row_, src_buffer_, context_row.string_pool());
                util::variant_match(val,
                    [&] (std::string_view sv) {
                        std::memcpy(dst_, sv.data(), sv.size());
                },
                [&] (StringPool::offset_t ) {
                    memset(dst_, 0, column_width_);
                });
            dst_ += column_width_;
        }
    }
};

class UnicodeConvertingStringReducer : public StringReducer {
    static constexpr size_t UNICODE_PREFIX = 4;
    arcticdb::PortableEncodingConversion conv_;
    uint8_t *buf_;

public:
    UnicodeConvertingStringReducer(
        Column &column,
        std::shared_ptr<PipelineContext> context,
        SegmentInMemory frame,
        const Field& frame_field,
        size_t alloc_width) :
        StringReducer(column, std::move(context), std::move(frame), std::move(frame_field), alloc_width * UNICODE_WIDTH),
        conv_("UTF32", "UTF8"),
        buf_(new uint8_t[column_width_ + UNICODE_PREFIX]) {
    }

    void reduce(PipelineContextRow &context_row, size_t) override {
        size_t end = context_row.slice_and_key().slice_.row_range.second - frame_.offset();
        for (; row_ < end; ++row_) {
            auto val = get_string_from_buffer(row_, src_buffer_, context_row.string_pool());
            util::variant_match(val,
                                [&] (std::string_view sv) {
                                    memset(buf_, 0, column_width_);
                                    auto success = conv_.convert(sv.data(), sv.size(), buf_, column_width_);
                                    util::check(success, "Failed to convert utf8 to utf32 for string {}", sv);
                                    memcpy(dst_, buf_, column_width_);
                                },
                                [&] (StringPool::offset_t ) {
                                    memset(dst_, 0, column_width_);
                                });

            dst_ += column_width_;
        }
    }

    ~UnicodeConvertingStringReducer() override {
        delete[] buf_;
    }
};


namespace {

enum class PyStringConstructor {
    Unicode_FromUnicode,
    Unicode_FromStringAndSize,
    Bytes_FromStringAndSize
};

inline PyStringConstructor get_string_constructor(bool has_type_conversion, bool is_utf) {
    if (is_utf) {
        if (has_type_conversion) {
            return PyStringConstructor::Unicode_FromUnicode;

        } else {
            return PyStringConstructor::Unicode_FromStringAndSize;

        }
    } else {
        return PyStringConstructor::Bytes_FromStringAndSize;
    }
}
}

using UniqueStringMapType = folly::ConcurrentHashMap<std::string_view, PyObject*>;

class DynamicStringReducer : public StringReducer {
    PyObject** ptr_dest_;
    std::shared_ptr<UniqueStringMapType> unique_string_map_;
    std::shared_ptr<PyObject> py_nan_;
    std::shared_ptr<LockType> lock_;
    bool do_lock_ = false;

    struct UnicodeFromUnicodeCreator {
        static PyObject* create(std::string_view sv, bool) {
            const auto actual_length = std::min(sv.size() / UNICODE_WIDTH, wcslen(reinterpret_cast<const wchar_t *>(sv.data())));
            return PyUnicode_FromKindAndData(PyUnicode_4BYTE_KIND, reinterpret_cast<const UnicodeType*>(sv.data()), actual_length);
        }
    };

    struct UnicodeFromStringAndSizeCreator {
        static PyObject* create(std::string_view sv, bool) {
            const auto actual_length = sv.size();
            return PyUnicode_FromStringAndSize(sv.data(), actual_length);
        }
    };

    struct BytesFromStringAndSizeCreator {
        static PyObject* create(std::string_view sv, bool has_type_conversion) {
            const auto actual_length = has_type_conversion ? std::min(sv.size(), strlen(sv.data())) : sv.size();
            return PYBIND11_BYTES_FROM_STRING_AND_SIZE(sv.data(), actual_length);
        }
    };

    template<typename StringCreator, typename LockPolicy>
    void assign_strings_shared(size_t end, const StringPool::offset_t* ptr_src, bool has_type_conversion, const StringPool& string_pool) {
        LockPolicy::lock(*lock_);
        auto none = std::make_unique<py::none>(py::none{});
        LockPolicy::unlock(*lock_);
        size_t none_count = 0u;
        for (; row_ < end; ++row_, ++ptr_src, ++ptr_dest_) {
            auto offset = *ptr_src;
            if(offset == not_a_string()) {
                *ptr_dest_ = none->ptr();
                ++none_count;
            } else if (offset == nan_placeholder()) {
                *ptr_dest_ = py_nan_.get();
                Py_INCREF(py_nan_.get());
            } else {
                const auto sv = get_string_from_pool(offset, string_pool);
                if (auto it = unique_string_map_->find(sv); it != unique_string_map_->end()) {
                    *ptr_dest_ = it->second;
                    LockPolicy::lock(*lock_);
                    Py_INCREF(*ptr_dest_);
                    LockPolicy::unlock(*lock_);
                } else {
                    LockPolicy::lock(*lock_);
                    *ptr_dest_ = StringCreator::create(sv, has_type_conversion) ;
                    Py_INCREF(*ptr_dest_);
                    LockPolicy::unlock(*lock_);
                    unique_string_map_->emplace(sv, *ptr_dest_);
                }
            }
        }
        LockPolicy::lock(*lock_);
        for(auto i = 0u; i < none_count; ++i)
            Py_INCREF(none->ptr());

        none.reset();
        LockPolicy::unlock(*lock_);
    }


    template<typename StringCreator, typename LockPolicy>
    void assign_strings_local(size_t end, const StringPool::offset_t* ptr_src, bool has_type_conversion, const StringPool& string_pool) {
        LockPolicy::lock(*lock_);
        auto none = std::make_unique<py::none>(py::none{});
        LockPolicy::unlock(*lock_);
        size_t none_count = 0u;
        emilib::HashMap<StringPool::offset_t, std::pair<PyObject*, folly::SpinLock>> local_map;
        local_map.reserve(end - row_);
        // TODO this is no good for non-contigous blocks, but we currently expect
        // output data to be contiguous
        for (; row_ < end; ++row_, ++ptr_src, ++ptr_dest_) {
            auto offset = *ptr_src;
            if(offset == not_a_string()) {
                *ptr_dest_ = none->ptr();
                ++none_count;
            } else if (offset == nan_placeholder()) {
                *ptr_dest_ = py_nan_.get();
                Py_INCREF(py_nan_.get());
            } else {
                auto it = local_map.find(offset);
                if(it != local_map.end()) {
                    *ptr_dest_ = it->second.first;
                    LockPolicy::lock(it->second.second);
                    Py_INCREF(*ptr_dest_);
                    LockPolicy::unlock(it->second.second);
                } else {
                    const auto sv = get_string_from_pool(offset, string_pool);
                    LockPolicy::lock(*lock_);
                    *ptr_dest_ = StringCreator::create(sv, has_type_conversion);
                    LockPolicy::unlock(*lock_);
                    PyObject* dest = *ptr_dest_;
                    local_map.insert_unique(std::move(offset), std::make_pair(std::move(dest), folly::SpinLock{}));
                }
            }
        }

        LockPolicy::lock(*lock_);
        for(auto i = 0u; i < none_count; ++i)
            Py_INCREF(none->ptr());

        none.reset();
        LockPolicy::unlock(*lock_);
    }

    template<typename LockPolicy>
    inline void process_string_views(
        bool has_type_conversion,
        bool is_utf,
        size_t end,
        const StringPool::offset_t* ptr_src,
        const StringPool& string_pool) {
        auto string_constructor = get_string_constructor(has_type_conversion, is_utf);

        switch(string_constructor) {
        case PyStringConstructor::Unicode_FromUnicode:
            if(unique_string_map_)
                assign_strings_shared<UnicodeFromUnicodeCreator, LockPolicy>(end, ptr_src, has_type_conversion, string_pool);
            else
                assign_strings_local<UnicodeFromUnicodeCreator, LockPolicy>(end, ptr_src, has_type_conversion, string_pool);
            break;
        case PyStringConstructor::Unicode_FromStringAndSize:
            if(unique_string_map_)
                assign_strings_shared<UnicodeFromStringAndSizeCreator, LockPolicy>(end, ptr_src, has_type_conversion, string_pool);
            else
                assign_strings_local<UnicodeFromStringAndSizeCreator, LockPolicy>(end, ptr_src, has_type_conversion, string_pool);
            break;
        case PyStringConstructor::Bytes_FromStringAndSize:
            if(unique_string_map_)
                assign_strings_shared<BytesFromStringAndSizeCreator, LockPolicy>(end, ptr_src, has_type_conversion, string_pool);
            else
                assign_strings_local<BytesFromStringAndSizeCreator, LockPolicy>(end, ptr_src, has_type_conversion, string_pool);
            break;
        }
    }

public:
    DynamicStringReducer(
        Column& column,
        std::shared_ptr<PipelineContext> &context,
        SegmentInMemory frame,
        const Field& frame_field,
        std::shared_ptr<UniqueStringMapType> unique_string_map,
        std::shared_ptr<PyObject> py_nan,
        std::shared_ptr<LockType> lock,
        bool do_lock) :
        StringReducer(column, context, std::move(frame), frame_field, sizeof(StringPool::offset_t)),
        ptr_dest_(reinterpret_cast<PyObject**>(dst_)),
        unique_string_map_(std::move(unique_string_map)),
        py_nan_(py_nan),
        lock_(std::move(lock)),
        do_lock_(do_lock) {
            if(!py_nan_)
                py_nan_ = std::shared_ptr<PyObject>(create_py_nan(lock_),[lock=lock_](PyObject* py_obj) {
                    lock->lock();
                    Py_DECREF(py_obj);
                    lock->unlock();
                });
            util::check(static_cast<bool>(py_nan_), "Got null nan in string reducer");
    }

    struct LockActive {
        static void lock(LockType& lock) {
            lock.lock();
        }

        static void unlock(LockType& lock) {
            lock.unlock();
        }
    };

    struct LockDisabled {
        static void lock(LockType&) { }

        static void unlock(LockType&) { }
    };

    void reduce(PipelineContextRow& context_row, size_t column_index) override {
        const auto& segment_descriptor = context_row.descriptor();
        const auto& segment_field = segment_descriptor[column_index];

        auto has_type_conversion = frame_field_.type() != segment_field.type();
        util::check(!has_type_conversion || trivially_compatible_types(frame_field_.type(), segment_field.type()),
                    "Cannot convert from type {} to {} in frame field", frame_field_.type(), segment_field.type());

        auto is_utf = is_utf_type(slice_value_type(frame_field_.type().data_type()));
        size_t end =  context_row.slice_and_key().slice_.row_range.second - frame_.offset();

        auto ptr_src = get_offset_ptr_at(row_, src_buffer_);
        if(do_lock_)
            process_string_views<LockActive>(has_type_conversion, is_utf, end, ptr_src, context_row.string_pool());
        else
            process_string_views<LockDisabled>(has_type_conversion, is_utf, end, ptr_src, context_row.string_pool());
    }

    void finalize() override {
        auto total_rows = frame_.row_count();
        if(row_!= total_rows) {
            auto none = py::none{};
            const auto diff = total_rows - row_;
            for(; row_ < total_rows; ++row_, ++ptr_dest_) {
                *ptr_dest_ = none.ptr();
            }

            lock_->lock();
            for(auto i = 0u; i < diff; ++i) {
                none.inc_ref();
            }
            lock_->unlock();
        }
    }
};

bool was_coerced_from_dynamic_to_fixed(DataType field_type, const Column& column) {
    return field_type == DataType::UTF_FIXED64
        && column.has_orig_type()
        && column.orig_type().data_type() == DataType::UTF_DYNAMIC64;
}

std::unique_ptr<StringReducer> get_string_reducer(
    Column& column,
    std::shared_ptr<PipelineContext>& context,
    SegmentInMemory frame,
    const Field& frame_field,
    const FrameSliceMap& slice_map,
    std::shared_ptr<UniqueStringMapType>& unique_string_map,
    std::shared_ptr<PyObject> py_nan,
    std::shared_ptr<LockType>& spinlock,
    bool do_lock
    ) {
    const auto& field_type = frame_field.type().data_type();
    std::unique_ptr<StringReducer> string_reducer;

    if (is_fixed_string_type(field_type)) {
        if (was_coerced_from_dynamic_to_fixed(field_type, column)) {
            const auto alloc_width = get_max_string_size_in_column(column.data().buffer(), context, frame, frame_field, slice_map, true);
            string_reducer = std::make_unique<UnicodeConvertingStringReducer>(column, context, frame, frame_field, alloc_width);
        } else {
            const auto alloc_width = get_max_string_size_in_column(column.data().buffer(), context, frame, frame_field, slice_map, false);
            string_reducer = std::make_unique<FixedStringReducer>(column, context, frame, frame_field, alloc_width);
        }
    } else {
        string_reducer = std::make_unique<DynamicStringReducer>(column, context, frame, frame_field, unique_string_map, py_nan, spinlock, do_lock);
    }
    return string_reducer;
}

struct ReduceColumnTask : async::BaseTask {
    SegmentInMemory frame_;
    size_t column_index_;
    std::shared_ptr<FrameSliceMap> slice_map_;
    std::shared_ptr<PipelineContext> context_;
    std::shared_ptr<UniqueStringMapType> unique_string_map_;
    std::shared_ptr<PyObject> py_nan_;
    std::shared_ptr<LockType> lock_;
    bool dynamic_schema_;
    bool do_lock_;

    ReduceColumnTask(
        const SegmentInMemory& frame,
        size_t c,
        std::shared_ptr<FrameSliceMap> slice_map,
        std::shared_ptr<PipelineContext>& context,
        std::shared_ptr<UniqueStringMapType> unique_string_map,
        std::shared_ptr<PyObject> py_nan,
        std::shared_ptr<LockType> lock,
        bool dynamic_schema,
        bool do_lock) :
        frame_(frame),
        column_index_(c),
        slice_map_(std::move(slice_map)),
        context_(context),
        unique_string_map_(std::move(unique_string_map)),
        py_nan_(py_nan),
        lock_(std::move(lock)),
        dynamic_schema_(dynamic_schema),
        do_lock_(do_lock) {
    }

    folly::Unit operator()() {
        const auto &frame_field = frame_.field(column_index_);
        const auto field_type = frame_field.type().data_type();
        auto &column = frame_.column(static_cast<position_t>(column_index_));

        const auto column_data = slice_map_->columns_.find(frame_field.name());
        if(dynamic_schema_ && column_data == slice_map_->columns_.end()) {
            column.default_initialize_rows(0, frame_.row_count(), false);
            bool dynamic_type = is_dynamic_string_type(field_type);
            if(dynamic_type) {
                EmptyDynamicStringReducer reducer(column, frame_, frame_field, sizeof(StringPool::offset_t), lock_);
                reducer.reduce(frame_.row_count());
            }
        } else {
            if(dynamic_schema_) {
                NullValueReducer null_reducer{column, context_, frame_};
                for (const auto &row : column_data->second) {
                    PipelineContextRow context_row{context_, row.second.context_index_};
                    null_reducer.reduce(context_row);
                }
                null_reducer.finalize();
            }
            if (is_sequence_type(field_type)) {
                auto string_reducer = get_string_reducer(column, context_, frame_, frame_field, *slice_map_, unique_string_map_, py_nan_, lock_, do_lock_);
                for (const auto &row : column_data->second) {
                    PipelineContextRow context_row{context_, row.second.context_index_};
                    if(context_row.slice_and_key().slice().row_range.diff() > 0)
                        string_reducer->reduce(context_row, row.second.column_index_);
                }
                string_reducer->finalize();
            }
        }
        return folly::Unit{};
    }
};

void reduce_and_fix_columns(
        std::shared_ptr<PipelineContext> &context,
        SegmentInMemory &frame,
        const ReadOptions& read_options
) {
    ARCTICDB_SAMPLE_DEFAULT(ReduceAndFixStringCol)
    ARCTICDB_DEBUG(log::version(), "Reduce and fix columns");
    if(frame.empty())
        return;

    bool dynamic_schema = opt_false(read_options.dynamic_schema_);
    auto slice_map = std::make_shared<FrameSliceMap>(context, dynamic_schema);
    static auto spinlock = std::make_shared<LockType>();
    std::shared_ptr<UniqueStringMapType> unique_string_map;
    std::shared_ptr<PyObject> py_nan;

    if (opt_false(read_options.optimise_string_memory_)) {
        ARCTICDB_DEBUG(log::version(), "Optimising dynamic string memory consumption");
        unique_string_map = std::make_shared<UniqueStringMapType>();
        py_nan = std::shared_ptr<PyObject>(create_py_nan(spinlock),[lock=spinlock](PyObject* py_obj) {
            lock->lock();
            Py_DECREF(py_obj);
            lock->unlock(); });
    } else {
        ARCTICDB_DEBUG(log::version(), "Not optimising dynamic string memory consumption");
    }

    constexpr bool parallel_strings = false;
    if(parallel_strings) {
        std::vector<folly::Future<folly::Unit>> jobs;
        static const auto batch_size = ConfigsMap::instance()->get_int("StringAllocation.BatchSize", 50);
        for (size_t c = 0; c < static_cast<size_t>(frame.descriptor().fields().size()); ++c) {
            jobs.emplace_back(async::submit_cpu_task(ReduceColumnTask(frame, c, slice_map, context, unique_string_map, py_nan, spinlock, dynamic_schema, true)));
            if(jobs.size() == static_cast<size_t>(batch_size)) {
                folly::collect(jobs).get();
                jobs.clear();
            }
        }

        if(!jobs.empty())
            folly::collect(jobs).get();
    } else {
        for (size_t c = 0; c < static_cast<size_t>(frame.descriptor().fields().size()); ++c) {
            ReduceColumnTask(frame, c, slice_map, context, unique_string_map, py_nan, spinlock, dynamic_schema, false)();
        }
    }

    if (unique_string_map) {
        ARCTICDB_DEBUG(log::version(), "Found {} unique dynamic strings in reduce_and_fix_columns", unique_string_map->size());
    }
}

folly::Future<std::vector<VariantKey>> fetch_data(
    const SegmentInMemory& frame,
    const std::shared_ptr<PipelineContext> &context,
    const std::shared_ptr<stream::StreamSource>& ssource,
    bool dynamic_schema,
    std::shared_ptr<BufferHolder> buffers
    ) {
    ARCTICDB_SAMPLE_DEFAULT(FetchSlices)
    if (frame.empty())
        return folly::Future<std::vector<VariantKey>>(std::vector<VariantKey>{});

    std::vector<VariantKey> keys;
    keys.reserve(context->slice_and_keys_.size());
    std::vector<stream::StreamSource::ReadContinuation> continuations;
    continuations.reserve(keys.capacity());
    context->ensure_vectors();
    {
        ARCTICDB_SUBSAMPLE_DEFAULT(QueueReadContinuations)
        for ( auto& row : *context) {
            keys.push_back(row.slice_and_key().key());
            continuations.emplace_back([
                row = row,
                frame = frame,
                dynamic_schema=dynamic_schema,
                buffers](auto &&ks) mutable {
                auto key_seg = std::forward<storage::KeySegmentPair>(ks);
                if(dynamic_schema)
                    decode_into_frame_dynamic(frame, row, std::move(key_seg.segment()), buffers);
                else
                    decode_into_frame_static(frame, row, std::move(key_seg.segment()), buffers);
                return std::get<AtomKey>(key_seg.variant_key());
            });
        }
    }
    ARCTICDB_SUBSAMPLE_DEFAULT(DoBatchReadCompressed)
    return ssource->batch_read_compressed(std::move(keys), std::move(continuations), BatchReadArgs{});
}

} // namespace read
