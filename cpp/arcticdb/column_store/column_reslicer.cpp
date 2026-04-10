/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/util/string_utils.hpp>

#include <arcticdb/column_store/string_pool.hpp>
#include <arcticdb/column_store/column_reslicer.hpp>

namespace arcticdb {

ColumnReslicer::ColumnReslicer(const ReslicingInfo& reslicing_info) : reslicing_info_(reslicing_info) {}

void ColumnReslicer::push_back(std::shared_ptr<Column> column, std::shared_ptr<StringPool> string_pool) {
    if (column->is_sparse()) {
        sparse_ = true;
        // Will be implemented in a future PR
        schema::raise<ErrorCode::E_UNSUPPORTED_COLUMN_TYPE>("compact_data not yet supported with sparse data");
    }
    if (type_.has_value()) {
        if (type_->data_type() == DataType::UTF_DYNAMIC64) {
            schema::check<ErrorCode::E_DESCRIPTOR_MISMATCH>(
                    is_sequence_type(column->type().data_type()),
                    "Cannot compact data: no common type between {} and {}",
                    type_->data_type(),
                    column->type().data_type()
            );
        } else {
            if (column->type() != *type_) {
                numeric_types_all_same_ = false;
            }
            // IntToFloatConversion::STRICT matches the behaviour when appending/updating data with dynamic schema
            auto opt_common_type = has_valid_common_type(*type_, column->type(), IntToFloatConversion::STRICT);
            // This should not be possible by construction, as has_valid_common_type is also used when
            // appending/updating with dynamic schema
            schema::check<ErrorCode::E_DESCRIPTOR_MISMATCH>(
                    opt_common_type.has_value(),
                    "Cannot compact data: no common type between {} and {}",
                    type_->data_type(),
                    column->type().data_type()
            );
            type_ = opt_common_type;
        }
    } else {
        type_ = is_sequence_type(column->type().data_type()) ? make_scalar_type(DataType::UTF_DYNAMIC64)
                                                             : column->type();
    }
    cols_or_row_counts_.emplace_back(ColumnWithStrings{std::move(column), std::move(string_pool)});
}

void ColumnReslicer::push_back(size_t row_count) {
    cols_or_row_counts_.emplace_back(row_count);
    sparse_ = true;
}

std::vector<std::optional<Column>> ColumnReslicer::reslice_columns(std::vector<StringPool>& string_pools) {
    util::check(type_.has_value(), "ColumnReslicer::reslice_columns called without any calls to push_back");
    auto res = [this, &string_pools]() {
        if (!is_sequence_type(type_->data_type()) && numeric_types_all_same_) {
            return reslice_by_memcpy();
        } else {
            return reslice_by_iteration(string_pools);
        }
    }();
    // This was a destructive process, reset the class so that it can be safely reused
    cols_or_row_counts_.clear();
    type_.reset();
    sparse_ = false;
    numeric_types_all_same_ = true;
    return res;
}

std::vector<std::optional<Column>> ColumnReslicer::reslice_by_memcpy() {
    // In theory, this could be done zero copy by adding external blocks to the output columns, referencing data in the
    // input columns. There are 2 arguments against this:
    // 1 - there will then be multiple (potentially very small) blocks written to disk, which will then need decoding
    //     separately at read time, which could negatively impact performance
    // 2 - it would massively complicate the implementation. Now, the segments are consumed and freed by the reslicing
    //     process. If this were done zero copy, then columns resliced by this method (or at least their underlying
    //     buffers) would need to be kept alive until the new segments have been encoded.
    auto output_columns = initialise_output_columns();
    auto type_size = get_type_size(type_->data_type());
    auto output_col = output_columns.begin();
    auto advance_output_col = [&]() {
        while (output_col != output_columns.end() && !output_col->has_value()) {
            ++output_col;
        }
    };
    advance_output_col();
    // This is safe as at least one output column must not be std::nullopt by construction
    auto dest_ptr = output_col->value().buffer().data();
    uint64_t output_col_capacity = type_size * output_col->value().row_count();
    for (auto& col_or_row_count : cols_or_row_counts_) {
        if (auto* col_with_strings = std::get_if<ColumnWithStrings>(&col_or_row_count)) {
            const auto& input_blocks = col_with_strings->column_->buffer().blocks();
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
                        advance_output_col();
                        if (output_col != output_columns.end()) {
                            dest_ptr = output_col->value().buffer().data();
                            output_col_capacity = type_size * output_col->value().row_count();
                        }
                    }
                }
            }
            ARCTICDB_DEBUG_CHECK(
                    ErrorCode::E_ASSERTION_FAILURE,
                    col_with_strings->column_.use_count() == 1,
                    "Unexpected column shared_ptr use count {} > 1 in SegmentReslicer",
                    col_with_strings->column_.use_count()
            );
            col_with_strings->column_.reset();
        }
    }
    // As we have allocated exactly as much space in the output data as was present in the input data, in the final
    // block of the final input column, we should hit the output_col_capacity == 0 condition and advance the output_col
    // iterator exactly to the end
    util::check(
            output_col == output_columns.end(),
            "ColumnReslicer::reslice_by_memcpy input iteration finished without reaching end of output columns"
    );
    return output_columns;
}

std::vector<std::optional<Column>> ColumnReslicer::reslice_by_iteration(std::vector<StringPool>& string_pools) {
    auto output_columns = initialise_output_columns();
    util::check(
            output_columns.size() == string_pools.size(),
            "ColumnReslicer::reslice_by_iteration number of output columns does not match number of input string pools "
            "{} != {}",
            output_columns.size(),
            string_pools.size()
    );
    auto output_col = output_columns.begin();
    auto string_pool = string_pools.begin();
    auto advance_output_col = [&]() {
        while (output_col != output_columns.end() && !output_col->has_value()) {
            ++output_col;
            ++string_pool;
        }
    };
    advance_output_col();
    // This is safe as at least one output column must not be std::nullopt by construction
    auto output_data = output_col->value().data();
    details::visit_type(type_->data_type(), [&](auto output_tag) {
        using output_type_info = ScalarTypeInfo<decltype(output_tag)>;
        auto output_it = output_data.begin<typename output_type_info::TDT>();
        auto output_end_it = output_data.end<typename output_type_info::TDT>();
        for (auto& col_or_row_count : cols_or_row_counts_) {
            if (auto* col_with_strings = std::get_if<ColumnWithStrings>(&col_or_row_count)) {
                details::visit_type(col_with_strings->column_->type().data_type(), [&](auto input_tag) {
                    using input_type_info = ScalarTypeInfo<decltype(input_tag)>;
                    auto input_data = col_with_strings->column_->data();
                    auto input_end_it = input_data.cend<typename input_type_info::TDT>();
                    for (auto input_it = input_data.cbegin<typename input_type_info::TDT>(); input_it != input_end_it;
                         ++input_it, ++output_it) {
                        if (output_it == output_end_it) {
                            ++output_col;
                            ++string_pool;
                            advance_output_col();
                            if (output_col != output_columns.end()) {
                                output_data = output_col->value().data();
                                output_it = output_data.begin<typename output_type_info::TDT>();
                                output_end_it = output_data.end<typename output_type_info::TDT>();
                            }
                        }
                        if constexpr (is_sequence_type(output_type_info::data_type)) {
                            // Trailing nulls are stripped in utf32_to_u8, so we only need to strip them for
                            // fixed-width ASCII
                            auto opt_str = col_with_strings->string_at_offset(
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
                                // This is only possible if the input column has a dynamic string type, and so
                                // *input_it will represent either None or NaN
                                ARCTICDB_DEBUG_CHECK(
                                        ErrorCode::E_ASSERTION_FAILURE,
                                        !is_a_string(*input_it),
                                        "Invalid non-string offset {}",
                                        *input_it
                                );
                                *output_it = *input_it;
                            }
                        } else { // Numeric, datetime, or bool
                            *output_it = static_cast<output_type_info::RawType>(*input_it);
                        }
                    }
                });
                ARCTICDB_DEBUG_CHECK(
                        ErrorCode::E_ASSERTION_FAILURE,
                        col_with_strings->column_.use_count() == 1,
                        "Unexpected column shared_ptr use count {} > 1 in SegmentReslicer",
                        col_with_strings->column_.use_count()
                );
                col_with_strings->column_.reset();
            }
        }
        // As we have allocated exactly as much space in the output data as was present in the input data, in the final
        // input column, we should advance output_it exactly to the end of the final output column
        // This is safe, as at least one output column must have a value, so the find_if will never return rend
        auto last_output_col =
                std::next(std::find_if(output_columns.rbegin(), output_columns.rend(), [](const auto& opt_col) {
                    return opt_col.has_value();
                })).base();
        util::check(
                output_col == last_output_col && output_it == output_end_it,
                "ColumnReslicer::reslice_by_iteration input iteration finished without reaching end of output columns"
        );
    });
    return output_columns;
}

std::vector<std::optional<Column>> ColumnReslicer::initialise_output_columns() const {
    std::vector<std::optional<Column>> output_columns;
    output_columns.reserve(reslicing_info_.num_segments);
    const auto& type = *type_;
    if (sparse_) {
        // This should "just work" for sparse columns, up to corner cases like all columns in an output segment having
        // no values. Will need a lot of testing though, so push to future PR.
        uint64_t input_values{0};
        // Represents a global bitset for all of the input columns, with 0s where an entire row slice is missing
        util::BitSet bitset(reslicing_info_.total_rows);
        util::BitSet::bulk_insert_iterator inserter(bitset);
        size_t idx{0};
        for (const auto& col_or_rows : cols_or_row_counts_) {
            util::variant_match(
                    col_or_rows,
                    [&](const ColumnWithStrings& col_with_strings) {
                        const auto& col = *col_with_strings.column_;
                        input_values += col.row_count();
                        if (col.is_sparse()) {
                            const auto& sparse_map = col.sparse_map();
                            auto end_bit = sparse_map.end();
                            for (auto set_bit = sparse_map.first(); set_bit < end_bit; ++set_bit) {
                                inserter = *set_bit + idx;
                            }
                        } else {
                            for (auto local_idx = 0; local_idx < col.last_row() + 1; ++local_idx) {
                                inserter = local_idx + idx;
                            }
                        }
                        idx += col.last_row() + 1;
                    },
                    [&](size_t rows) { idx += rows; }
            );
        }
        inserter.flush();
        util::BitIndex bit_index;
        bitset.build_rs_index(&bit_index);
        util::check(
                input_values == bitset.count(),
                "ColumnReslicer::initialise_output_columns: mismatch between input_values and bitset.count() {} != {}",
                input_values,
                bitset.count()
        );
        uint64_t output_values{0};
        uint64_t output_rows{0};
        for (idx = 0; idx < reslicing_info_.num_segments; ++idx) {
            auto num_rows = reslicing_info_.rows_in_slice(idx);
            auto set_bits = bitset.count_range(output_rows, output_rows + num_rows - 1, bit_index);
            output_values += set_bits;
            if (set_bits == 0) {               // No values in the range of this output column
                output_columns.emplace_back(); // std::nullopt
            } else if (set_bits == num_rows) {
                output_columns.emplace_back(
                        std::make_optional<Column>(type, num_rows, AllocationType::PRESIZED, Sparsity::NOT_PERMITTED)
                );
            } else { // Output column is sparse
                output_columns.emplace_back(
                        std::make_optional<Column>(type, set_bits, AllocationType::PRESIZED, Sparsity::PERMITTED)
                );
                output_columns.back()->set_sparse_map(
                        util::truncate_sparse_map(bitset, output_rows, output_rows + num_rows)
                );
            }
            output_rows += num_rows;
        }
        util::check(
                input_values == output_values,
                "ColumnReslicer::initialise_output_columns: mismatch between input_values and output_values {} != {}",
                input_values,
                output_values
        );
    } else {
        for (size_t idx = 0; idx < reslicing_info_.num_segments; ++idx) {
            output_columns.emplace_back(std::make_optional<Column>(
                    type, reslicing_info_.rows_in_slice(idx), AllocationType::PRESIZED, Sparsity::NOT_PERMITTED
            ));
        }
    }
    return output_columns;
}

} // namespace arcticdb
