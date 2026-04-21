/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/column_store/segment_reslicer.hpp>

#include <arcticdb/column_store/column_reslicer.hpp>
#include <arcticdb/util/string_utils.hpp>

namespace arcticdb {

SegmentReslicer::SegmentReslicer(uint64_t max_rows_per_segment) : max_rows_per_segment_(max_rows_per_segment) {
    util::check(max_rows_per_segment_ > 0, "SegmentReslicer max rows per segment must be >0");
}

std::vector<SegmentInMemory> SegmentReslicer::reslice_segments(std::vector<SegmentInMemory>&& segments) {
    if (segments.empty()) {
        return {};
    }
    // Use string over string_view for the keys in this map, as we are going to free the segments soon, which would
    // then invalidate the pointers in those string views
    ankerl::unordered_dense::map<std::string, ColumnReslicer, util::TransparentStringHash, std::equal_to<>> column_map;
    // Build up the set of all columns in all segments
    // Also track the order they were added in
    std::vector<std::string> col_names_in_order;
    uint64_t total_rows = std::accumulate(
            segments.cbegin(),
            segments.cend(),
            uint64_t(0),
            [](uint64_t n, const SegmentInMemory& segment) { return n + segment.row_count(); }
    );
    ReslicingInfo reslicing_info{total_rows, max_rows_per_segment_};
    for (const auto& segment : segments) {
        for (const auto& field : segment.descriptor().fields()) {
            if (column_map.emplace(field.name(), ColumnReslicer(reslicing_info)).second) {
                col_names_in_order.emplace_back(field.name());
            }
        }
    }
    std::vector<SegmentInMemory> res(reslicing_info.num_segments);
    const auto& desc = segments.front().descriptor();
    for (auto& segment : res) {
        // add_column also adds to the FieldCollection, so start with an empty one
        segment.attach_descriptor(std::make_shared<StreamDescriptor>(
                desc.segment_desc_, std::make_shared<FieldCollection>(), desc.stream_id_
        ));
    }
    // For each segment, add the column to the corresponding ColumnReslicer in column_map if it is present, or the
    // number of rows in this segment if it is missing from this row slice (dynamic schema)
    for (const auto& segment : segments) {
        for (const auto& col_name : col_names_in_order) {
            if (auto col_idx = segment.column_index(col_name); col_idx.has_value()) {
                column_map.at(col_name).push_back(segment.column_ptr(*col_idx), segment.string_pool_ptr());
            } else {
                column_map.at(col_name).push_back(segment.row_count());
            }
        }
    }
    // This ensures that the refcount of the column shared pointers is 1 when they are reset after having their data
    // copied into the result column, and so the memory is freed as early as possible
    segments.clear();
    std::vector<StringPool> string_pools(reslicing_info.num_segments);
    // We can use string_view keys here as they point to the keys in column_map, which are still live while this
    // variable is in use
    ankerl::unordered_dense::
            map<std::string_view, std::vector<std::optional<Column>>, util::TransparentStringHash, std::equal_to<>>
                    resliced_column_map;
    for (auto&& [col_name, column_reslicer] : column_map) {
        resliced_column_map.emplace(col_name, column_reslicer.reslice_columns(string_pools));
    }
    for (const auto& col_name : col_names_in_order) {
        auto& sliced_cols = resliced_column_map.at(col_name);
        util::check(
                sliced_cols.size() == reslicing_info.num_segments,
                "Mismatched sliced column size {} != {}",
                sliced_cols.size(),
                reslicing_info.num_segments
        );
        for (size_t idx = 0; idx < reslicing_info.num_segments; ++idx) {
            if (sliced_cols.at(idx).has_value()) {
                res.at(idx).add_column(col_name, std::make_shared<Column>(std::move(*sliced_cols.at(idx))));
            }
        }
    }
    for (size_t idx = 0; idx < reslicing_info.num_segments; ++idx) {
        res.at(idx).set_row_id(reslicing_info.rows_in_slice(idx) - 1);
        res.at(idx).set_string_pool(std::make_shared<StringPool>(std::move(string_pools.at(idx))));
    }
    return res;
}

} // namespace arcticdb
