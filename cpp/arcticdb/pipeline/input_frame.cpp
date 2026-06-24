/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <sparrow/record_batch.hpp>

#include <arcticdb/pipeline/input_frame.hpp>
#include <arcticdb/entity/types.hpp>
#include <arcticdb/entity/stream_descriptor.hpp>
#include <arcticdb/stream/index.hpp>
#include <variant>

namespace arcticdb::pipelines {

namespace {
SortedValue compute_index_sortedness(const Column& index_column) {
    using TimeseriesTDT = stream::TimeseriesIndex::TypeDescTag;
    auto column_data = index_column.data();
    auto it = column_data.cbegin<TimeseriesTDT>();
    const auto end = column_data.cend<TimeseriesTDT>();
    if (it == end) {
        return SortedValue::ASCENDING;
    }
    bool ascending = true;
    bool descending = true;
    auto prev = *it;
    for (++it; it != end; ++it) {
        const auto curr = *it;
        if (curr < prev) {
            ascending = false;
        } else if (curr > prev) {
            descending = false;
        }
        if (!ascending && !descending) {
            return SortedValue::UNSORTED;
        }
        prev = curr;
    }
    return ascending ? SortedValue::ASCENDING : SortedValue::DESCENDING;
}
} // namespace

InputFrame::InputFrame() : index(stream::empty_index()) {}

void InputFrame::set_from_columns(
        std::vector<Column>&& cols, StreamDescriptor&& desc, std::vector<sparrow::record_batch>&& arrow_buffer_owners,
        SortednessScan sortedness_scan
) {
    util::check(norm_meta.has_experimental_arrow(), "Unexpected non-Arrow norm metadata provided with Arrow data");
    desc_ = std::move(desc);
    if (norm_meta.experimental_arrow().has_index()) {
        user_input::check<ErrorCode::E_INVALID_USER_ARGUMENT>(
                !cols.empty(), "Arrow index column specified but there are zero columns"
        );
        user_input::check<ErrorCode::E_INVALID_USER_ARGUMENT>(
                is_time_type(cols[0].type().data_type()),
                "Specified Arrow index column has non-time type {}",
                cols[0].type().data_type()
        );
        desc_.set_index({IndexDescriptorImpl::Type::TIMESTAMP, 1});
        index = stream::TimeseriesIndex{std::string(desc_.field(0).name())};
        // Arrow input does not record sortedness up front (unlike pandas), so we have to compute it when required.
        desc_.set_sorted(
                sortedness_scan == SortednessScan::SCAN_IF_UNKNOWN ? compute_index_sortedness(cols[0])
                                                                   : SortedValue::UNKNOWN
        );
    } else {
        desc_.set_index({IndexDescriptorImpl::Type::ROWCOUNT, 0});
        index = stream::RowCountIndex{};
        desc_.set_sorted(SortedValue::UNKNOWN);
    }

    num_rows = cols.empty() ? 0 : cols[0].row_count();
    desc_for_tsd_ = desc_.clone();
    for (auto& field : desc_for_tsd_->fields()) {
        if (field.type().data_type() == DataType::UTF_DYNAMIC32) {
            field.mutable_type() = TypeDescriptor(DataType::UTF_DYNAMIC64, field.type().dimension());
        }
    }

    columns_ = std::vector<FieldData>(std::make_move_iterator(cols.begin()), std::make_move_iterator(cols.end()));
    arrow_buffer_owners_ = std::move(arrow_buffer_owners);
    has_only_tensors_ = false;
}

StreamDescriptor& InputFrame::desc() { return desc_; }

const StreamDescriptor& InputFrame::desc() const { return const_cast<InputFrame*>(this)->desc(); }

const StreamDescriptor& InputFrame::desc_for_tsd() { return desc_for_tsd_.has_value() ? *desc_for_tsd_ : desc_; }

void InputFrame::set_offset(ssize_t off) const { offset = off; }

bool InputFrame::has_index() const { return desc().index().field_count() != 0ULL; }

bool InputFrame::empty() const { return num_rows == 0; }

timestamp InputFrame::index_value_at(size_t row) const {
    util::check(has_index(), "InputFrame::index_value_at should only be called on timeseries data");
    util::check(!columns_.empty(), "InputFrame::index_value_at called but no columns are present");
    return util::variant_match(
            columns_[0],
            [row](const NativeTensor& tensor) {
                util::check(
                        tensor.data_type() == DataType::NANOSECONDS_UTC64,
                        "Expected timestamp index in append, got type {}",
                        tensor.data_type()
                );
                return *tensor.ptr_cast<timestamp>(row);
            },
            [row](const Column& col) {
                util::check(
                        static_cast<position_t>(row) < col.row_count(),
                        "Out of range row {} requested in InputFrame::index_value_at with column of length {}",
                        row,
                        col.row_count()
                );
                // Note that scalar_at is O(log(n)) where n is the number of chunks in the underlying buffer, which is
                // equal to the number of input record batches for Arrow
                return *col.scalar_at<timestamp>(row);
            }
    );
};

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

bool InputFrame::has_only_tensors() const { return has_only_tensors_; };

size_t InputFrame::num_columns() const { return columns_.size(); }

const InputFrame::FieldData& InputFrame::field_data(size_t idx) const {
    util::check(idx < columns_.size(), "InputFrame::field_data index {} out of range (size {})", idx, columns_.size());
    return columns_[idx];
}

const NativeTensor& InputFrame::get_tensor(size_t idx) const {
    util::check(idx < columns_.size(), "InputFrame::get_tensor index {} out of range (size {})", idx, columns_.size());
    auto& variant_column = columns_[idx];
    util::check(
            std::holds_alternative<NativeTensor>(variant_column),
            "InputFrame::get_tensor called for index {}, but that is not a NativeTensor.",
            idx
    );
    return std::get<NativeTensor>(variant_column);
}

const Column& InputFrame::get_column(size_t idx) const {
    util::check(idx < columns_.size(), "InputFrame::get_column index {} out of range (size {})", idx, columns_.size());
    auto& variant_column = columns_[idx];
    util::check(
            std::holds_alternative<Column>(variant_column),
            "InputFrame::get_column called for index {}, but that is not a Column.",
            idx
    );
    return std::get<Column>(variant_column);
}

} // namespace arcticdb::pipelines
