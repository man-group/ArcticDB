/* Copyright 2025 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#include <sparrow/record_batch.hpp>

#include <arcticdb/pipeline/input_frame.hpp>
#include <arcticdb/entity/types.hpp>
#include <arcticdb/stream/index.hpp>

namespace arcticdb::pipelines {


InputFrame::InputFrame() :
    index(stream::empty_index())
{
}

void InputFrame::set_segment(SegmentInMemory&& seg) {
    num_rows = seg.row_count();
    util::check(norm_meta.has_arrow_table(), "Unexpected non-Arrow norm metadata provided with Arrow data");
    if (norm_meta.arrow_table().has_index()) {
        user_input::check<ErrorCode::E_INVALID_USER_ARGUMENT>(
                !seg.columns().empty(),
                "Arrow index column specified but there are zero columns");
        user_input::check<ErrorCode::E_INVALID_USER_ARGUMENT>(
                is_time_type(seg.column(0).type().data_type()),
                "Specified Arrow index column has non-time type {}", seg.column(0).type().data_type());
        seg.descriptor().set_index({IndexDescriptorImpl::Type::TIMESTAMP, 1});
        index = stream::TimeseriesIndex{std::string(seg.descriptor().field(0).name())};
    } else {
        seg.descriptor().set_index({IndexDescriptorImpl::Type::ROWCOUNT, 0});
        index = stream::RowCountIndex{};
    }
    input_data = std::move(seg);
    set_sorted(SortedValue::ASCENDING);
}

void InputFrame::set_from_tensors(StreamDescriptor&& desc,
                                  std::vector<entity::NativeTensor>&& field_tensors,
                                  std::optional<entity::NativeTensor>&& index_tensor) {
    input_data = InputTensors{std::move(index_tensor), std::move(field_tensors), std::move(desc)};
}

StreamDescriptor& InputFrame::desc() {
    if (has_tensors()) {
        return std::get<InputTensors>(input_data).desc;
    } else {
        return std::get<SegmentInMemory>(input_data).descriptor();
    }
}

const StreamDescriptor& InputFrame::desc() const {
    return const_cast<InputFrame*>(this)->desc();
}

void InputFrame::set_offset(ssize_t off) const {
    offset = off;
}

void InputFrame::set_sorted(SortedValue sorted) {
    switch (sorted) {
        case SortedValue::UNSORTED:desc().set_sorted(SortedValue::UNSORTED);break;
        case SortedValue::DESCENDING:desc().set_sorted(SortedValue::DESCENDING);break;
        case SortedValue::ASCENDING:desc().set_sorted(SortedValue::ASCENDING);break;
        default:desc().set_sorted(SortedValue::UNKNOWN);
    }
}

bool InputFrame::has_index() const { return desc().index().field_count() != 0ULL; }

bool InputFrame::empty() const { return num_rows == 0; }

timestamp InputFrame::index_value_at(size_t row) {
    util::check(has_index(), "InputFrame::index_value_at should only be called on timeseries data");
    return util::variant_match(
            input_data,
            [row](SegmentInMemory& seg) {
                util::check(row < seg.row_count(), "Out of range row {} requsted in InputFrame::index_value_at with segment of length",
                            row, seg.row_count());
                const auto& index_column = seg.column(0);
                return *index_column.scalar_at<timestamp>(row);
            },
            [row](InputTensors& input_tensors) {
                util::check(input_tensors.index_tensor.has_value(), "InputFrame::index_value_at call with null index tensor");
                util::check(input_tensors.index_tensor->data_type() == DataType::NANOSECONDS_UTC64,
                            "Expected timestamp index in append, got type {}", input_tensors.index_tensor->data_type());
                return *input_tensors.index_tensor->ptr_cast<timestamp>(row);
            }
            );
}

void InputFrame::set_index_range() {
    // Fill index range
    // Note RowCountIndex will normally have an index field count of 0
    if(num_rows == 0) {
        index_range.start_ = IndexValue{ NumericIndex{0} };
        index_range.end_ = IndexValue{ NumericIndex{0} };
    } else if (desc().index().field_count() == 1) {
        visit_field(desc().field(0), [&](auto &&tag) {
            using DT = std::decay_t<decltype(tag)>;
            using RawType = typename DT::DataTypeTag::raw_type;
            if constexpr (std::is_integral_v<RawType> || std::is_floating_point_v<RawType>) {
                util::variant_match(
                        input_data,
                        [this](SegmentInMemory& segment) {
                            const auto& index_column = segment.column(0);
                            index_range.start_ = IndexValue(*index_column.scalar_at<timestamp>(0));
                            index_range.end_ = IndexValue(*index_column.scalar_at<timestamp>(num_rows - 1));
                        },
                        [this](InputTensors& input_tensors) {
                            util::check(static_cast<bool>(input_tensors.index_tensor), "Got null index tensor in set_index_range");
                            util::check(input_tensors.index_tensor->nbytes() > 0, "Empty index tensor");
                            auto &tensor = input_tensors.index_tensor.value();
                            auto start_t = tensor.ptr_cast<RawType>(0);
                            auto end_t = tensor.ptr_cast<RawType>(static_cast<size_t>(tensor.shape(0) - 1));
                            index_range.start_  = IndexValue(static_cast<timestamp>(*start_t));
                            index_range.end_ = IndexValue(static_cast<timestamp>(*end_t));
                        }
                        );
            } else {
                throw std::runtime_error("Unsupported non-integral index type");
            }
            });
    } else {
        index_range.start_ = IndexValue{ NumericIndex{0} };
        index_range.end_ = IndexValue{static_cast<timestamp>(num_rows) - 1};
    }
}

bool InputFrame::has_segment() const {
    return std::holds_alternative<SegmentInMemory>(input_data);
}

bool InputFrame::has_tensors() const {
    return std::holds_alternative<InputTensors>(input_data);
}

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
    return std::get<SegmentInMemory>(input_data);
}

} //namespace arcticdb::pipelines
