/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/util/string_utils.hpp>

#include <arcticdb/column_store/string_pool.hpp>
#include <arcticdb/column_store/segment_reslicer.hpp>
#include <arcticdb/entity/type_utils.hpp>

namespace arcticdb {

SegmentReslicer::SegmentReslicer(uint64_t max_rows_per_segment) : max_rows_per_segment_(max_rows_per_segment) {
    util::check(max_rows_per_segment_ > 0, "SegmentReslicer max rows per segment must be >0");
}

std::vector<std::optional<Column>> SegmentReslicer::reslice_dense_numeric_static_schema_columns(
        std::vector<std::shared_ptr<Column>>&& cols, const SlicingInfo& slicing_info
) {
    std::vector<std::optional<Column>> res;
    res.reserve(slicing_info.num_segments);
    const auto& type = cols.front()->type();
    const auto type_size = get_type_size(type.data_type());
    for (size_t idx = 0; idx < slicing_info.num_segments; ++idx) {
        res.emplace_back(std::make_optional<Column>(
                type, slicing_info.rows_in_slice(idx), AllocationType::PRESIZED, Sparsity::NOT_PERMITTED
        ));
    }
    auto output_col = res.begin();
    auto dest_ptr = output_col->value().buffer().data();
    uint64_t output_col_capacity = type_size * output_col->value().row_count();
    for (auto& input_col : cols) {
        const auto& input_blocks = input_col->buffer().blocks();
        for (const auto& block : input_blocks) {
            auto src_ptr = block->data();
            uint64_t remaining_bytes = block->physical_bytes();
            while (remaining_bytes > 0) {
                auto bytes_to_copy = std::min(remaining_bytes, output_col_capacity);
                memcpy(dest_ptr, src_ptr, bytes_to_copy);
                src_ptr += bytes_to_copy;
                dest_ptr += bytes_to_copy;
                remaining_bytes -= bytes_to_copy;
                output_col_capacity -= bytes_to_copy;
                if (output_col_capacity == 0) {
                    ++output_col;
                    if (output_col != res.end()) {
                        dest_ptr = output_col->value().buffer().data();
                        output_col_capacity = type_size * output_col->value().row_count();
                    }
                }
            }
        }
        ARCTICDB_DEBUG_CHECK(
                ErrorCode::E_ASSERTION_FAILURE,
                input_col.use_count() == 1,
                "Unexpected column shared_ptr use count {} > 1 in SegmentReslicer",
                input_col.use_count()
        );
        input_col.reset();
    }
    return res;
}

std::vector<std::optional<Column>> SegmentReslicer::reslice_dense_numeric_dynamic_schema_columns(
        std::vector<std::shared_ptr<Column>>&& cols, const SlicingInfo& slicing_info, const TypeDescriptor& type
) {
    std::vector<std::optional<Column>> res;
    res.reserve(slicing_info.num_segments);
    for (size_t idx = 0; idx < slicing_info.num_segments; ++idx) {
        res.emplace_back(std::make_optional<Column>(
                type, slicing_info.rows_in_slice(idx), AllocationType::PRESIZED, Sparsity::NOT_PERMITTED
        ));
    }
    auto output_col = res.begin();
    auto output_data = output_col->value().data();
    details::visit_type(type.data_type(), [&](auto output_tag) {
        using output_type_info = ScalarTypeInfo<decltype(output_tag)>;
        // This is true by construction, just to prevent generation of invalid/useless code generation
        if constexpr (is_numeric_type(output_type_info::data_type) || is_bool_type(output_type_info::data_type)) {
            auto output_it = output_data.begin<typename output_type_info::TDT>();
            auto output_end_it = output_data.end<typename output_type_info::TDT>();
            for (auto& input_col : cols) {
                details::visit_type(input_col->type().data_type(), [&](auto input_tag) {
                    using input_type_info = ScalarTypeInfo<decltype(input_tag)>;
                    // This is true by construction, just to prevent generation of invalid/useless code generation
                    if constexpr (is_numeric_type(input_type_info::data_type) ||
                                  is_bool_type(input_type_info::data_type)) {
                        auto input_data = input_col->data();
                        auto input_end_it = input_data.cend<typename input_type_info::TDT>();
                        for (auto input_it = input_data.cbegin<typename input_type_info::TDT>();
                             input_it != input_end_it;
                             ++input_it, ++output_it) {
                            if (output_it == output_end_it) {
                                ++output_col;
                                output_data = output_col->value().data();
                                output_it = output_data.begin<typename output_type_info::TDT>();
                                output_end_it = output_data.end<typename output_type_info::TDT>();
                            }
                            *output_it = static_cast<output_type_info::RawType>(*input_it);
                        }
                    }
                });
                ARCTICDB_DEBUG_CHECK(
                        ErrorCode::E_ASSERTION_FAILURE,
                        input_col.use_count() == 1,
                        "Unexpected column shared_ptr use count {} > 1 in SegmentReslicer",
                        input_col.use_count()
                );
                input_col.reset();
            }
        }
    });
    return res;
}

std::vector<std::optional<Column>> SegmentReslicer::reslice_dense_string_columns(
        std::vector<ColumnWithStrings>&& cols_with_strings, const SlicingInfo& slicing_info,
        std::vector<StringPool>& string_pools
) {
    std::vector<std::optional<Column>> res;
    res.reserve(slicing_info.num_segments);
    // Regardless of input column types, compact into dynamic-width UTF-8
    // Anything else is a nightmare, as even with static schema, you can have a mixture of dynamic and fixed width
    // strings in the same column across different segments, or fixed-width columns where the width differs from segment
    // to segment
    const auto type = make_scalar_type(DataType::UTF_DYNAMIC64);
    for (size_t idx = 0; idx < slicing_info.num_segments; ++idx) {
        res.emplace_back(std::make_optional<Column>(
                type, slicing_info.rows_in_slice(idx), AllocationType::PRESIZED, Sparsity::NOT_PERMITTED
        ));
    }
    auto output_col = res.begin();
    auto output_data = output_col->value().data();
    auto string_pool = string_pools.begin();
    details::visit_type(type.data_type(), [&](auto output_tag) {
        using output_type_info = ScalarTypeInfo<decltype(output_tag)>;
        // This is true by construction, just to prevent generation of invalid/useless code generation
        if constexpr (output_type_info::data_type == DataType::UTF_DYNAMIC64) {
            auto output_it = output_data.begin<typename output_type_info::TDT>();
            auto output_end_it = output_data.end<typename output_type_info::TDT>();
            for (auto& input_col_with_strings : cols_with_strings) {
                details::visit_type(input_col_with_strings.column_->type().data_type(), [&](auto input_tag) {
                    using input_type_info = ScalarTypeInfo<decltype(input_tag)>;
                    // This is true by construction, just to prevent generation of invalid/useless code generation
                    if constexpr (is_sequence_type(input_type_info::data_type)) {
                        auto input_data = input_col_with_strings.column_->data();
                        auto input_end_it = input_data.cend<typename input_type_info::TDT>();
                        for (auto input_it = input_data.cbegin<typename input_type_info::TDT>();
                             input_it != input_end_it;
                             ++input_it, ++output_it) {
                            if (output_it == output_end_it) {
                                ++output_col;
                                output_data = output_col->value().data();
                                output_it = output_data.begin<typename output_type_info::TDT>();
                                output_end_it = output_data.end<typename output_type_info::TDT>();
                                ++string_pool;
                            }
                            // Trailing nulls are stripping in utf32_to_u8, so we only need to strip them for
                            // fixed-width ASCII
                            auto opt_str = input_col_with_strings.string_at_offset(
                                    *input_it, input_type_info::data_type == DataType::ASCII_FIXED64
                            );
                            if (opt_str.has_value()) {
                                if constexpr (is_fixed_string_type(input_type_info::data_type) &&
                                              is_utf_type(input_type_info::data_type)) {
                                    auto utf8_str = util::utf32_to_u8(*opt_str);
                                    *output_it = string_pool->get(utf8_str).offset();
                                } else {
                                    *output_it = string_pool->get(*opt_str).offset();
                                }
                            } else {
                                // This is only possible if the input column has a dynamic string type, and so *input_it
                                // will represent either None or NaN
                                ARCTICDB_DEBUG_CHECK(
                                        ErrorCode::E_ASSERTION_FAILURE,
                                        !is_a_string(*input_it),
                                        "Invalid non-string offset {}",
                                        *input_it
                                );
                                *output_it = *input_it;
                            }
                        }
                    }
                });
                ARCTICDB_DEBUG_CHECK(
                        ErrorCode::E_ASSERTION_FAILURE,
                        input_col_with_strings.column_.use_count() == 1,
                        "Unexpected column shared_ptr use count {} > 1 in SegmentReslicer",
                        input_col_with_strings.column_.use_count()
                );
                // Drops reference to string pool as well
                input_col_with_strings.column_.reset();
            }
        }
    });
    cols_with_strings.clear();
    return res;
}

std::vector<std::optional<Column>> SegmentReslicer::reslice_columns(
        std::vector<std::variant<ColumnWithStrings, size_t>>&& cols_with_strings, const SlicingInfo& slicing_info,
        std::vector<StringPool>& string_pools
) {
    std::optional<TypeDescriptor> type;
    bool numeric_types_all_same{true};
    for (const auto& col_with_strings : cols_with_strings) {
        schema::check<ErrorCode::E_DESCRIPTOR_MISMATCH>(
                std::holds_alternative<ColumnWithStrings>(col_with_strings),
                "compact_data not yet supported with dynamic schema"
        );
        const auto& col = *std::get<ColumnWithStrings>(col_with_strings).column_;
        schema::check<ErrorCode::E_UNSUPPORTED_COLUMN_TYPE>(
                !col.is_sparse(), "compact_data not yet supported with sparse data"
        );
        if (type.has_value()) {
            if (type->data_type() == DataType::UTF_DYNAMIC64) {
                schema::check<ErrorCode::E_DESCRIPTOR_MISMATCH>(
                        is_sequence_type(col.type().data_type()),
                        "Cannot compact data: no common type between {} and {}",
                        type->data_type(),
                        col.type().data_type()
                );
            } else {
                if (col.type() != *type) {
                    numeric_types_all_same = false;
                }
                auto opt_common_type = has_valid_common_type(*type, col.type(), IntToFloatConversion::PERMISSIVE);
                schema::check<ErrorCode::E_DESCRIPTOR_MISMATCH>(
                        opt_common_type.has_value(),
                        "Cannot compact data: no common type between {} and {}",
                        type->data_type(),
                        col.type().data_type()
                );
                type = opt_common_type;
            }
        } else {
            type = is_sequence_type(col.type().data_type()) ? make_scalar_type(DataType::UTF_DYNAMIC64) : col.type();
        }
    }
    if (is_sequence_type(type->data_type())) {
        std::vector<ColumnWithStrings> cols;
        for (auto& var : cols_with_strings) {
            cols.emplace_back(std::move(std::get<ColumnWithStrings>(var)));
        }
        return reslice_dense_string_columns(std::move(cols), slicing_info, string_pools);
    } else {
        std::vector<std::shared_ptr<Column>> cols;
        for (auto& var : cols_with_strings) {
            cols.emplace_back(std::get<ColumnWithStrings>(var).column_);
        }
        cols_with_strings.clear();
        if (numeric_types_all_same) {
            return reslice_dense_numeric_static_schema_columns(std::move(cols), slicing_info);
        } else {
            return reslice_dense_numeric_dynamic_schema_columns(std::move(cols), slicing_info, *type);
        }
    }
}

std::vector<SegmentInMemory> SegmentReslicer::reslice_segments(std::vector<SegmentInMemory>&& segments) {
    if (segments.empty()) {
        return {};
    }
    // Use string over string_view for the keys in this map, as we are going to free the segments soon, which would
    // then invalidate the pointers in those string views
    // The size_t variant is used with dynamic schema when a column is missing from a row slice, to represent how many
    // rows are in the missing slice
    ankerl::unordered_dense::map<
            std::string,
            std::vector<std::variant<ColumnWithStrings, size_t>>,
            util::TransparentStringHash,
            std::equal_to<>>
            column_map;
    // Build up the set of all columns in all segments
    // Also track the order they were added in
    std::vector<std::string> col_names_in_order;
    uint64_t total_rows{0};
    for (const auto& segment : segments) {
        total_rows += segment.row_count();
        for (const auto& field : segment.descriptor().fields()) {
            if (column_map.emplace(std::piecewise_construct, std::make_tuple(field.name()), std::make_tuple()).second) {
                col_names_in_order.emplace_back(field.name());
            }
        }
    }
    SlicingInfo slicing_info{total_rows, max_rows_per_segment_};
    std::vector<SegmentInMemory> res(slicing_info.num_segments);
    const auto& desc = segments.front().descriptor();
    for (auto& segment : res) {
        // add_column also adds to the FieldCollection, so start with an empty one
        segment.attach_descriptor(std::make_shared<StreamDescriptor>(
                desc.segment_desc_, std::make_shared<FieldCollection>(), desc.stream_id_
        ));
    }
    // For each segment, append the column to the corresponding vector in columns if it is present, or the number of
    // rows in this segment if it is missing from this row slice (dynamic schema)
    for (const auto& segment : segments) {
        for (const auto& col_name : col_names_in_order) {
            if (auto col_idx = segment.column_index(col_name); col_idx.has_value()) {
                column_map.at(col_name).emplace_back(
                        ColumnWithStrings{segment.column_ptr(*col_idx), segment.string_pool_ptr(), col_name}
                );
            } else {
                column_map.at(col_name).emplace_back(segment.row_count());
            }
        }
    }
    // This ensures that the refcount of the column shared pointers is 1 when they are reset after having their data
    // copied into the result column, and so the memory is freed as early as possible
    segments.clear();
    std::vector<StringPool> string_pools(slicing_info.num_segments);
    // We can use string_view keys here as they point to the keys in column_map, which are still live while this
    // variable is in use
    ankerl::unordered_dense::
            map<std::string_view, std::vector<std::optional<Column>>, util::TransparentStringHash, std::equal_to<>>
                    resliced_column_map;
    for (auto&& [col_name, columns] : column_map) {
        resliced_column_map.emplace(col_name, reslice_columns(std::move(columns), slicing_info, string_pools));
    }
    for (const auto& col_name : col_names_in_order) {
        auto& sliced_cols = resliced_column_map.at(col_name);
        util::check(
                sliced_cols.size() == slicing_info.num_segments,
                "Mismatched sliced column size {} != {}",
                sliced_cols.size(),
                slicing_info.num_segments
        );
        for (size_t idx = 0; idx < slicing_info.num_segments; ++idx) {
            if (sliced_cols.at(idx).has_value()) {
                res.at(idx).add_column(col_name, std::make_shared<Column>(std::move(*sliced_cols.at(idx))));
            }
        }
    }
    for (size_t idx = 0; idx < slicing_info.num_segments; ++idx) {
        auto rows_to_consume = std::min(slicing_info.rows_in_slice(idx), total_rows);
        res.at(idx).set_row_data(rows_to_consume - 1);
        total_rows -= rows_to_consume;
        res.at(idx).set_string_pool(std::make_shared<StringPool>(std::move(string_pools.at(idx))));
    }
    return res;
}

} // namespace arcticdb
