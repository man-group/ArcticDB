/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/column_store/memory_segment_impl.hpp>
#include <arcticdb/entity/type_utils.hpp>
#include <arcticdb/stream/index.hpp>
#include <arcticdb/util/format_date.hpp>

namespace arcticdb {
// Append any columns that exist both in this segment and in the 'other' segment onto the
// end of the column in this segment. Any columns that exist in this segment but not in the
// one being appended will be default-valued (or sparse) in this segment. Any columns in the
// segment being appended not in this one will be ignored. This means that if you want to
// append a lot of segments together into a merged segment, you need to merge the descriptors
// first and create the target segment with the merged descriptor. Could be modified in the future to
// retroactively insert and default-initialize or sparsify the new column, but that's not required
//  at the moment.
void SegmentInMemoryImpl::append(const SegmentInMemoryImpl& other) {
    if (other.row_count() == 0)
        return;

    other.init_column_map();
    for(auto col = 0u; col < num_columns(); ++col) {
        auto col_name = descriptor().field(col).name();
        auto other_col_index = other.column_index(col_name);
        if(other_col_index.has_value()) {
            ARCTICDB_DEBUG(log::version(), "Appending column {} at index {}", col_name, *other_col_index);
            auto this_type = column_unchecked(col).type();
            auto other_type =  other.column_unchecked(*other_col_index).type();
            auto opt_common_type = has_valid_common_type(this_type, other_type);
            internal::check<ErrorCode::E_INVALID_ARGUMENT>(
                    opt_common_type.has_value(),
                    "Could not append type {} to type {} for column {}, this index {}, other index {}",
                    other_type, this_type, col_name, col, *other_col_index);

            if (this_type != *opt_common_type) {
                column_unchecked(col).change_type(opt_common_type->data_type_);
            }
            if (other_type != *opt_common_type) {
                auto type_promoted_col = other.column_unchecked(*other_col_index).clone();
                type_promoted_col.change_type(opt_common_type->data_type_);
                column_unchecked(col).append(type_promoted_col, row_id_ + 1);
            } else {
                column_unchecked(col).append(other.column(*other_col_index), row_id_ + 1);
            }
        } else {
            ARCTICDB_DEBUG(log::version(), "Marking {} absent rows for column {}", other.row_count(), col_name);
            column_unchecked(col).mark_absent_rows(other.row_count());
        }
    }
    set_row_id(row_id_ + other.row_count());
}

// Combine 2 segments that hold different columns associated with the same rows
// If unique_column_names is true, any columns from other with names matching those in this are ignored
void SegmentInMemoryImpl::concatenate(SegmentInMemoryImpl&& other, bool unique_column_names) {
        internal::check<ErrorCode::E_INVALID_ARGUMENT>(
                row_count() == other.row_count(),
                "Cannot concatenate segments with differing row counts: {} {}",
                row_count(), other.row_count());
    for (const auto& field: folly::enumerate(other.fields())) {
        if (!unique_column_names || !column_index(field->name()).has_value()) {
            add_column(*field, other.column_ptr(field.index));
        }
    }
}

position_t SegmentInMemoryImpl::add_column(const Field &field, size_t num_rows, bool presize) {
    util::check_arg(!field.name().empty(), "Empty name in field: {}", field);
    if(!column_map_)
        init_column_map();

    columns_.emplace_back(std::make_shared<Column>(field.type(), num_rows, presize, allow_sparse_));
    auto new_field_name = descriptor_->add_field(FieldRef{field.type(), field.name()});

    std::lock_guard<std::mutex> lock{*column_map_mutex_};
    column_map_->insert(new_field_name, descriptor_->field_count() - 1);
    return columns_.size() - 1;
}

position_t SegmentInMemoryImpl::add_column(const Field &field, const std::shared_ptr<Column>& column) {
    if(!column_map_)
        init_column_map();

    columns_.emplace_back(std::move(column));
    auto new_field_name = descriptor_->add_field(FieldRef{field.type(), field.name()});

    std::lock_guard<std::mutex> lock{*column_map_mutex_};
    column_map_->insert(new_field_name, descriptor_->field_count() - 1);
    return columns_.size() - 1;
}

void SegmentInMemoryImpl::change_schema(StreamDescriptor descriptor) {
    //util::check(vector_is_unique(descriptor.fields()), "Non-unique fields in descriptor: {}", descriptor.fields());
    init_column_map();
    std::vector<std::shared_ptr<Column>> new_columns(descriptor.field_count());
    for(auto col = 0u; col < descriptor.field_count(); ++col) {
        auto col_name = descriptor.field(col).name();
        auto col_index = column_index(col_name);
        const auto& other_type = descriptor.field(col).type();
        if(col_index) {
            auto this_type = column_unchecked(col_index.value()).type();
            util::check(this_type == other_type, "Could not convert type {} to type {} for column {}, this index {}, other index {}",
                        other_type, this_type, col_name, col_index.value(), col);
            new_columns[col] = std::move(columns_[col_index.value()]);
        } else {
            auto new_column = std::make_shared<Column>(other_type, row_count(), false, allow_sparse_);
            new_column->default_initialize_rows(size_t{0}, row_count(), true);
            new_columns[col] = std::move(new_column);
        }
    }
    std::swap(*descriptor_, descriptor);
    std::swap(columns_, new_columns);
    generate_column_map();
}

std::optional<std::string_view> SegmentInMemoryImpl::string_at(position_t row, position_t col) const {
    auto td = descriptor_->field(col).type();
    util::check(is_sequence_type(td.data_type()), "Not a sequence type");
    util::check_arg(size_t(row) < row_count(), "Segment index {} out of bounds in string", row);
    const auto& col_ref = column(col);

    if(is_fixed_string_type(td.data_type()) && col_ref.is_inflated()) {
        auto string_size = col_ref.bytes() / row_count();
        auto ptr = col_ref.data().buffer().ptr_cast<char>(row * string_size, string_size);
        return std::string_view(ptr, string_size);
    } else {

        auto offset = col_ref.scalar_at<StringPool::offset_t>(row);
        if (offset != std::nullopt && offset.value() != not_a_string() && offset.value() != nan_placeholder())
            return string_pool_->get_view(offset.value());
        else
            return std::nullopt;
    }
}

size_t SegmentInMemoryImpl::num_blocks() const {
    return std::accumulate(std::begin(columns_), std::end(columns_), 0, [] (size_t n, const auto& col) {
        return n + col->num_blocks();
    });
}

std::optional<Column::StringArrayData> SegmentInMemoryImpl::string_array_at(position_t row, position_t col) {
    util::check_arg(size_t(row) < row_count(), "Segment index {} out of bounds in string array", row);
    return column(col).string_array_at(row, *string_pool_);
}

size_t SegmentInMemoryImpl::num_bytes() const {
    return std::accumulate(std::begin(columns_), std::end(columns_), 0, [] (size_t n, const auto& col) {
        return n + col->bytes();
    });
}

void SegmentInMemoryImpl::sort(const std::string& column_name) {
    init_column_map();
    auto idx = column_index(std::string_view(column_name));
    util::check(static_cast<bool>(idx), "Column {} not found in sort", column_name);
    sort(idx.value());
}

void SegmentInMemoryImpl::sort(position_t idx) {
    auto& sort_col = column_unchecked(idx);
    util::check(!sort_col.is_sparse(), "Can't sort on sparse column idx {} because it is not supported yet. The user should either fill the column data or filter the empty columns out",idx);
    auto table = sort_col.type().visit_tag([&sort_col] (auto tdt) {
        using TagType = decltype(tdt);
        using RawType = typename TagType::DataTypeTag::raw_type;
        return create_jive_table<RawType>(sort_col);
    });

    for (auto field_col = 0u; field_col < descriptor().field_count(); ++field_col) {
        column(field_col).sort_external(table);
    }
}

}
