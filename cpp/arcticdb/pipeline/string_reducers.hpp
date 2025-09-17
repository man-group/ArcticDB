#pragma once

#include <arcticdb/util/encoding_conversion.hpp>
#include <arcticdb/column_store/chunked_buffer.hpp>
#include <arcticdb/entity/types.hpp>
#include <arcticdb/column_store/column.hpp>
#include <arcticdb/pipeline/frame_slice_map.hpp>
#include <arcticdb/pipeline/frame_utils.hpp>
#include <arcticdb/pipeline/pipeline_context.hpp>

namespace arcticdb {

size_t get_max_string_size_in_column(
        ChunkedBuffer& src_buffer, std::shared_ptr<pipelines::PipelineContext>& context, SegmentInMemory& frame,
        const entity::Field& frame_field, const pipelines::FrameSliceMap& slice_map, bool check_all
) {
    const auto column_info = slice_map.columns_.find(frame_field.name());
    util::check(
            column_info != slice_map.columns_.end(), "Data for column {} was not generated in map", frame_field.name()
    );
    auto column_width = size_t{0};

    for (const auto& row : column_info->second) {
        pipelines::PipelineContextRow context_row{context, row.second.context_index_};
        size_t string_size;
        if (check_all || context_row.compacted()) {
            string_size = get_max_string_size(context_row, src_buffer, frame.offset());
        } else {
            string_size = get_first_string_size(context_row, src_buffer, frame.offset());
        }

        column_width = std::max(column_width, std::max(column_width, string_size));
    }

    return column_width;
}

class StringReducer {
  protected:
    std::shared_ptr<pipelines::PipelineContext> context_;
    SegmentInMemory frame_;
    size_t row_ = 0U;
    ChunkedBuffer& src_buffer_;
    size_t column_width_;
    ChunkedBuffer dest_buffer_;
    uint8_t* dst_;

  public:
    StringReducer(
            Column& column, std::shared_ptr<pipelines::PipelineContext> context, SegmentInMemory frame,
            size_t alloc_width
    ) :
        context_(std::move(context)),
        frame_(std::move(frame)),
        src_buffer_(column.data().buffer()),
        column_width_(alloc_width),
        dest_buffer_(frame_.row_count() * column_width_, entity::AllocationType::DETACHABLE),
        dst_(dest_buffer_.data()) {
        if (dest_buffer_.bytes() > 0) {
            std::memset(dest_buffer_.data(), 0, dest_buffer_.bytes());
        }
    }

    virtual void finalize() {}

    virtual ~StringReducer() { src_buffer_ = std::move(dest_buffer_); }

  public:
    virtual void reduce(pipelines::PipelineContextRow& context_row, size_t column_index) = 0;
};

class FixedStringReducer : public StringReducer {
  public:
    FixedStringReducer(
            Column& column, std::shared_ptr<pipelines::PipelineContext>& context, SegmentInMemory frame,
            size_t alloc_width
    ) :
        StringReducer(column, context, std::move(frame), alloc_width) {}

    void reduce(pipelines::PipelineContextRow& context_row, size_t) override {
        size_t end = context_row.slice_and_key().slice_.row_range.second - frame_.offset();
        for (; row_ < end; ++row_) {
            auto val = get_string_from_buffer(row_, src_buffer_, context_row.string_pool());
            util::variant_match(
                    val,
                    [&](std::string_view sv) { std::memcpy(dst_, sv.data(), sv.size()); },
                    [&](entity::position_t) { memset(dst_, 0, column_width_); }
            );
            dst_ += column_width_;
        }
    }
};

class UnicodeConvertingStringReducer : public StringReducer {
    static constexpr size_t UNICODE_PREFIX = 4;
    arcticdb::PortableEncodingConversion conv_;
    uint8_t* buf_;

  public:
    UnicodeConvertingStringReducer(
            Column& column, std::shared_ptr<pipelines::PipelineContext> context, SegmentInMemory frame,
            size_t alloc_width
    ) :
        StringReducer(column, std::move(context), std::move(frame), alloc_width * UNICODE_WIDTH),
        conv_("UTF32", "UTF8"),
        buf_(new uint8_t[column_width_ + UNICODE_PREFIX]) {}

    void reduce(pipelines::PipelineContextRow& context_row, size_t) override {
        size_t end = context_row.slice_and_key().slice_.row_range.second - frame_.offset();
        for (; row_ < end; ++row_) {
            auto val = get_string_from_buffer(row_, src_buffer_, context_row.string_pool());
            util::variant_match(
                    val,
                    [&](std::string_view sv) {
                        memset(buf_, 0, column_width_);
                        auto success = conv_.convert(sv.data(), sv.size(), buf_, column_width_);
                        util::check(success, "Failed to convert utf8 to utf32 for string {}", sv);
                        memcpy(dst_, buf_, column_width_);
                    },
                    [&](entity::position_t) { memset(dst_, 0, column_width_); }
            );

            dst_ += column_width_;
        }
    }

    ~UnicodeConvertingStringReducer() override { delete[] buf_; }
};

bool was_coerced_from_dynamic_to_fixed(DataType field_type, const Column& column) {
    return field_type == DataType::UTF_FIXED64 && column.has_orig_type() &&
           column.orig_type().data_type() == DataType::UTF_DYNAMIC64;
}

std::unique_ptr<StringReducer> get_fixed_string_reducer(
        Column& column, std::shared_ptr<pipelines::PipelineContext>& context, SegmentInMemory frame,
        const entity::Field& frame_field, const pipelines::FrameSliceMap& slice_map
) {
    const auto& field_type = frame_field.type().data_type();
    std::unique_ptr<StringReducer> string_reducer;

    util::check(is_fixed_string_type(field_type), "Expected fixed string type in reducer, got {}", field_type);

    if (was_coerced_from_dynamic_to_fixed(field_type, column)) {
        const auto alloc_width =
                get_max_string_size_in_column(column.data().buffer(), context, frame, frame_field, slice_map, true);
        string_reducer = std::make_unique<UnicodeConvertingStringReducer>(column, context, frame, alloc_width);
    } else {
        const auto alloc_width =
                get_max_string_size_in_column(column.data().buffer(), context, frame, frame_field, slice_map, false);
        string_reducer = std::make_unique<FixedStringReducer>(column, context, frame, alloc_width);
    }

    return string_reducer;
}
} // namespace arcticdb
