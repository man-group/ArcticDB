#pragma once

#include <arcticdb/column_store/memory_segment.hpp>
#include <arcticdb/pipeline/input_tensor_frame.hpp>

namespace arcticdb {

inline constexpr ssize_t to_tensor_dim(Dimension dim) {
    return static_cast<int>(dim) + 1;
}

inline NativeTensor tensor_from_column(const Column &column) {
    return column.type().visit_tag([&column](auto &&tag) {
        using TypeDescriptorTag = std::decay_t<decltype(tag)>;
        shape_t scalar_shape = 0;
        const shape_t *shape_ptr;
        constexpr auto dim = TypeDescriptorTag::DimensionTag::value;
        constexpr auto data_type = TypeDescriptorTag::DataTypeTag::data_type;
        if constexpr (dim == Dimension::Dim0) {
            scalar_shape = column.row_count();
            shape_ptr = &scalar_shape;
        } else {
            shape_ptr = column.shape_ptr();
        }

        auto tensor = NativeTensor{
            static_cast<ssize_t>(column.bytes()),
            to_tensor_dim(dim),
            nullptr,
            shape_ptr,
            data_type,
            get_type_size(data_type),
            column.ptr(),
            to_tensor_dim(dim)
        };
        return tensor;
    });
}


struct SegmentToInputFrameAdapter {
    SegmentInMemory segment_;
    std::shared_ptr<pipelines::InputTensorFrame> input_frame_ = std::make_shared<pipelines::InputTensorFrame>();

    explicit SegmentToInputFrameAdapter(SegmentInMemory &&segment) :
        segment_(std::move(segment)) {
        input_frame_->desc = segment_.descriptor();
        input_frame_->num_rows = segment_.row_count();
        size_t col{0};
        if (segment_.descriptor().index().type() != IndexDescriptor::ROWCOUNT) {
            for (size_t i = 0; i < segment_.descriptor().index().field_count(); ++i) {
                input_frame_->index_tensor = tensor_from_column(segment_.column(col));
                ++col;
            }
        }

        while (col < segment_.num_columns())
            input_frame_->field_tensors.emplace_back(tensor_from_column(segment_.column(col++)));

        input_frame_->set_index_range();
    }

    void synthesize_norm_meta() {
        if (segment_.metadata()) {
            auto segment_tsd = segment_.index_descriptor();
            input_frame_->norm_meta.CopyFrom(segment_tsd.proto().normalization());
        }
        ensure_timeseries_norm_meta(input_frame_->norm_meta, input_frame_->desc.id(), false);
    }
};
} //namespace arcticdb