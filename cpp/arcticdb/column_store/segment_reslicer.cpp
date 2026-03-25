/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <algorithm>

#include <arcticdb/column_store/string_pool.hpp>
#include <arcticdb/column_store/segment_reslicer.hpp>

namespace arcticdb {

SegmentReslicer::SegmentReslicer(uint64_t max_rows_per_segment) : max_rows_per_segment_(max_rows_per_segment) {
    util::check(max_rows_per_segment > 0, "SegmentReslicer max rows per segment must be >0");
}

std::vector<std::optional<Column>> SegmentReslicer::reslice_dense_numeric_static_schema_columns(
        std::vector<std::optional<std::shared_ptr<Column>>>&& columns, const SlicingInfo& slicing_info
) {
    std::vector<std::optional<Column>> res;
    res.reserve(slicing_info.num_segments);
    const auto& type = columns.front().value()->type();
    const auto type_size = get_type_size(type.data_type());
    auto remaining_rows{slicing_info.total_rows};
    while (remaining_rows > 0) {
        auto rows_to_consume = std::min(remaining_rows, slicing_info.rows_per_segment);
        res.emplace_back(
                std::make_optional<Column>(type, rows_to_consume, AllocationType::PRESIZED, Sparsity::NOT_PERMITTED)
        );
        remaining_rows -= rows_to_consume;
    }
    auto output_col = res.begin();
    auto output_ptr = output_col->value().buffer().data();
    auto capacity = type_size * output_col->value().row_count();
    for (auto& input_col : columns) {
        const auto& input_blocks = input_col.value()->buffer().blocks();
        for (const auto& block : input_blocks) {
            auto input_ptr = block->data();
            auto remaining_bytes = block->physical_bytes();
            while (remaining_bytes > 0) {
                auto bytes_to_copy = std::min(remaining_bytes, capacity);
                memcpy(output_ptr, input_ptr, bytes_to_copy);
                input_ptr += bytes_to_copy;
                output_ptr += bytes_to_copy;
                remaining_bytes -= bytes_to_copy;
                capacity -= bytes_to_copy;
                if (capacity == 0) {
                    ++output_col;
                    if (output_col != res.end()) {
                        output_ptr = output_col->value().buffer().data();
                        capacity = type_size * output_col->value().row_count();
                    }
                }
            }
        }
        input_col->reset();
    }
    return res;
}

std::vector<std::optional<Column>> SegmentReslicer::reslice_columns(
        std::vector<std::optional<std::shared_ptr<Column>>>&& columns, const SlicingInfo& slicing_info,
        ARCTICDB_UNUSED std::vector<StringPool>& string_pools
) {
    std::optional<TypeDescriptor> type;
    for (const auto& col : columns) {
        schema::check<ErrorCode::E_DESCRIPTOR_MISMATCH>(
                col.has_value(), "compact_data not yet supported with dynamic schema"
        );
        schema::check<ErrorCode::E_UNSUPPORTED_COLUMN_TYPE>(
                !col.value()->is_sparse(), "compact_data not yet supported with sparse data"
        );
        if (type.has_value()) {
            schema::check<ErrorCode::E_DESCRIPTOR_MISMATCH>(
                    col.value()->type() == *type, "compact_data not yet supported with dynamic schema"
            );
        } else {
            type = col.value()->type();
        }
    }
    schema::check<ErrorCode::E_UNSUPPORTED_COLUMN_TYPE>(
            !is_sequence_type(type->data_type()), "compact_data not yet supported with string columns"
    );
    return reslice_dense_numeric_static_schema_columns(std::move(columns), slicing_info);
}

std::vector<SegmentInMemory> SegmentReslicer::reslice_segments(std::vector<SegmentInMemory>&& segments) {
    if (segments.empty()) {
        return {};
    }
    ankerl::unordered_dense::map<std::string_view, std::vector<std::optional<std::shared_ptr<Column>>>> column_map;
    // Build up the set of all columns in all segments
    // Also track the order they were added in
    std::vector<std::string_view> col_names_in_order;
    uint64_t total_rows{0};
    for (const auto& segment : segments) {
        total_rows += segment.row_count();
        for (const auto& field : segment.descriptor().fields()) {
            if (auto it = column_map.find(field.name()); it == column_map.end()) {
                col_names_in_order.emplace_back(field.name());
                column_map.emplace(field.name(), std::vector<std::optional<std::shared_ptr<Column>>>{});
            }
        }
    }
    SlicingInfo slicing_info{total_rows, max_rows_per_segment_};
    // For each segment, append the column to the corresponding vector in columns if it is present, or a nullopt if it
    // is missing from this segment (dynamic schema)
    for (const auto& segment : segments) {
        for (const auto col_name : col_names_in_order) {
            if (auto col_idx = segment.column_index(col_name); col_idx.has_value()) {
                column_map.at(col_name).emplace_back(segment.column_ptr(*col_idx));
            } else {
                column_map.at(col_name).emplace_back(std::nullopt);
            }
        }
    }
    std::vector<StringPool> string_pools(slicing_info.num_segments);
    ankerl::unordered_dense::map<std::string_view, std::vector<std::optional<Column>>> resliced_column_map;
    for (auto&& [col_name, columns] : column_map) {
        resliced_column_map.emplace(col_name, reslice_columns(std::move(columns), slicing_info, string_pools));
    }
    std::vector<SegmentInMemory> res(slicing_info.num_segments);
    for (auto& segment : res) {
        // This won't be sufficient with dynamic schema
        segment.attach_descriptor(std::make_shared<StreamDescriptor>(segments.front().descriptor().clone()));
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
        auto rows_to_consume = std::min(slicing_info.rows_per_segment, total_rows);
        res.at(idx).set_row_data(rows_to_consume - 1);
        total_rows -= rows_to_consume;
        res.at(idx).set_string_pool(std::make_shared<StringPool>(std::move(string_pools.at(idx))));
    }
    return res;
}

} // namespace arcticdb
