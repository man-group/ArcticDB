/* Copyright 2025 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <sparrow/record_batch.hpp>

#include <arcticdb/pipeline/input_frame.hpp>
#include <arcticdb/entity/types.hpp>
#include <arcticdb/stream/index.hpp>

namespace arcticdb::pipelines {

InputFrame::InputFrame() : index(stream::empty_index()) {}

InputFrame::InputFrame(SegmentInMemory&& seg) : index(stream::empty_index()) { set_segment(std::move(seg)); }

InputFrame::InputFrame(
        StreamDescriptor&& desc, std::vector<entity::NativeTensor>&& field_tensors,
        std::optional<entity::NativeTensor>&& index_tensor
) :
    index(stream::empty_index()) {
    set_from_tensors(std::move(desc), std::move(field_tensors), std::move(index_tensor));
}

void InputFrame::set_segment(SegmentInMemory&& seg) {
    num_rows = seg.row_count();
    util::check(norm_meta.has_experimental_arrow(), "Unexpected non-Arrow norm metadata provided with Arrow data");
    if (norm_meta.experimental_arrow().has_index()) {
        user_input::check<ErrorCode::E_INVALID_USER_ARGUMENT>(
                !seg.columns().empty(), "Arrow index column specified but there are zero columns"
        );
        user_input::check<ErrorCode::E_INVALID_USER_ARGUMENT>(
                is_time_type(seg.column(0).type().data_type()),
                "Specified Arrow index column has non-time type {}",
                seg.column(0).type().data_type()
        );
        seg.descriptor().set_index({IndexDescriptorImpl::Type::TIMESTAMP, 1});
        index = stream::TimeseriesIndex{std::string(seg.descriptor().field(0).name())};
        seg.descriptor().set_sorted(SortedValue::ASCENDING);
    } else {
        seg.descriptor().set_index({IndexDescriptorImpl::Type::ROWCOUNT, 0});
        index = stream::RowCountIndex{};
        seg.descriptor().set_sorted(SortedValue::UNKNOWN);
    }
    input_data.emplace<InputSegment>(std::move(seg));
}

void InputFrame::set_from_tensors(
        StreamDescriptor&& desc, std::vector<entity::NativeTensor>&& field_tensors,
        std::optional<entity::NativeTensor>&& index_tensor
) {
    input_data.emplace<InputTensors>(std::move(index_tensor), std::move(field_tensors), std::move(desc));
}

StreamDescriptor& InputFrame::desc() {
    if (has_tensors()) {
        return std::get<InputTensors>(input_data).desc;
    } else {
        return std::get<InputSegment>(input_data).seg.descriptor();
    }
}

const StreamDescriptor& InputFrame::desc() const { return const_cast<InputFrame*>(this)->desc(); }

const StreamDescriptor& InputFrame::desc_for_tsd() {
    if (has_segment()) {
        return std::get<InputSegment>(input_data).desc_for_tsd;
    } else {
        return desc();
    }
}

void InputFrame::set_offset(ssize_t off) const { offset = off; }

bool InputFrame::has_index() const { return desc().index().field_count() != 0ULL; }

bool InputFrame::empty() const { return num_rows == 0; }

timestamp InputFrame::index_value_at(size_t row) {
    util::check(has_index(), "InputFrame::index_value_at should only be called on timeseries data");
    return util::variant_match(
            input_data,
            [row](const InputSegment& input_segment) {
                const auto& seg = input_segment.seg;
                util::check(
                        row < seg.row_count(),
                        "Out of range row {} requested in InputFrame::index_value_at with segment of length",
                        row,
                        seg.row_count()
                );
                const auto& index_column = seg.column(0);
                // Note that scalar_at is O(log(n)) where n is the number of chunks in the underlying buffer, which is
                // equal to the number of input record batches for Arrow
                return *index_column.scalar_at<timestamp>(row);
            },
            [row](const InputTensors& input_tensors) {
                util::check(
                        input_tensors.index_tensor.has_value(), "InputFrame::index_value_at call with null index tensor"
                );
                util::check(
                        input_tensors.index_tensor->data_type() == DataType::NANOSECONDS_UTC64,
                        "Expected timestamp index in append, got type {}",
                        input_tensors.index_tensor->data_type()
                );
                return *input_tensors.index_tensor->ptr_cast<timestamp>(row);
            }
    );
}

void InputFrame::set_index_range() {
    // Fill index range
    // Note RowCountIndex will normally have an index field count of 0
    if (num_rows == 0) {
        index_range.start_ = IndexValue{NumericIndex{0}};
        index_range.end_ = IndexValue{NumericIndex{0}};
    } else if (desc().index().field_count() == 1) {
        index_range.start_ = index_value_at(0);
        index_range.end_ = index_value_at(num_rows - 1);
    } else {
        index_range.start_ = IndexValue{NumericIndex{0}};
        index_range.end_ = IndexValue{static_cast<timestamp>(num_rows) - 1};
    }
}

void InputFrame::set_bucketize_dynamic(bool bucketize) { bucketize_dynamic = bucketize; }

bool InputFrame::has_segment() const { return std::holds_alternative<InputSegment>(input_data); }

bool InputFrame::has_tensors() const { return std::holds_alternative<InputTensors>(input_data); }

const std::optional<entity::NativeTensor>& InputFrame::opt_index_tensor() const {
    util::check(has_tensors(), "InputFrame index_tensor requested but holds SegmentInMemory");
    return std::get<InputTensors>(input_data).index_tensor;
}

const std::vector<entity::NativeTensor>& InputFrame::field_tensors() const {
    util::check(has_tensors(), "InputFrame field_tensors requested but holds SegmentInMemory");
    return std::get<InputTensors>(input_data).field_tensors;
}

const SegmentInMemory& InputFrame::segment() const {
    util::check(has_segment(), "InputFrame segment requested but holds InputTensors");
    return std::get<InputSegment>(input_data).seg;
}

} // namespace arcticdb::pipelines
