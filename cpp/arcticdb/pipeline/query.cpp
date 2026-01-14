/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/pipeline/query.hpp>
#include <arcticdb/pipeline/index_utils.hpp>
#include <arcticdb/pipeline/frame_slice.hpp>
#include <arcticdb/pipeline/test/test_container.hpp>

namespace arcticdb::pipelines {

using namespace arcticdb::stream;
using namespace arcticdb::pipelines::index;

RowRange slice_row_range_at(const IndexSegmentReader& isr, std::size_t row) {
    auto start_row = isr.column(index::Fields::start_row).scalar_at<std::size_t>(row).value();
    auto end_row = isr.column(index::Fields::end_row).scalar_at<std::size_t>(row).value();
    return {start_row, end_row};
}

RowRange slice_row_range_at(const std::vector<SliceAndKey>& sk, std::size_t row) { return sk[row].slice_.row_range; }

bool is_slice_in_row_range(const RowRange& slice_row_range, const RowRange& row_filter) {
    // If the row_filter is empty we return false (i.e. don't read the slice).
    // This is required for cases like a range (3, 2) which falls completely within an index row (0, 10). We
    // know that if the range is empty we don't need to read the data key for (0, 10) because it won't contain
    // any elements within the empty range.
    return slice_row_range.first < row_filter.second && slice_row_range.second > row_filter.first &&
           !row_filter.empty();
}

bool is_slice_in_index_range(IndexRange slice_index_range, const IndexRange& index_filter, bool is_read_operation) {
    // Typically slice_index_range should be end exclusive, however due to old bugs we have old data written with
    // inclusive end_index. So, when we are reading we explicitly set the interval as closed to be able to read
    // old_data. The same fix should be done for updates, but that is not implemented yet and should be added with
    // https://github.com/man-group/ArcticDB/issues/2655
    slice_index_range.end_closed_ = is_read_operation;
    return closed_aware_intersects(slice_index_range, index_filter) && !index_filter.empty();
}

template<typename ContainerType, typename IdxType>
std::unique_ptr<util::BitSet> build_bitset_for_index(
        const ContainerType& container,
        IndexRange rg, // IndexRange is expected to be inclusive on both ends
        bool dynamic_schema, bool column_groups, bool is_read_operation, std::unique_ptr<util::BitSet>&& input
) {
    auto res = std::make_unique<util::BitSet>(static_cast<util::BitSetSizeType>(container.size()));
    if (container.empty())
        return res;

    using IndexTagType = typename IdxType::TypeDescTag;

    const auto& start_idx_col = container.seg().column(position_t(index::Fields::start_index));
    const auto& end_idx_col = container.seg().column(position_t(index::Fields::end_index));
    ARCTICDB_DEBUG(log::version(), "Searching for match in index range {}", rg);

    auto end_index_col_begin = end_idx_col.template begin<IndexTagType>();
    auto end_index_col_end = end_idx_col.template end<IndexTagType>();

    if (dynamic_schema && !column_groups) {
        const auto range_start = std::get<timestamp>(rg.start_);
        const auto range_end = std::get<timestamp>(rg.end_);

        // End index column is exclusive. We want to find the last position where `range_start` is < end_index at
        // position. This is equivalent to finding the first position where range_start + 1 >= end_index at position.
        // If we are reading, we want to include the start index, in order to support backwards compatibility with older
        // versions. The same fix should be done for updates, but that is not implemented yet and should be added with
        // https://github.com/man-group/ArcticDB/issues/2655
        const auto adjusted_range_start = is_read_operation ? range_start : range_start + 1;
        auto start_pos = std::lower_bound(end_index_col_begin, end_index_col_end, adjusted_range_start);

        if (start_pos == end_idx_col.template end<IndexTagType>()) {
            ARCTICDB_DEBUG(log::version(), "Returning as start pos is at end");
            return res;
        }
        auto begin_offset = std::distance(end_index_col_begin, start_pos);

        auto end_pos = std::upper_bound(
                start_idx_col.template begin<IndexTagType>(), start_idx_col.template end<IndexTagType>(), range_end
        );

        if (end_pos == start_idx_col.template begin<IndexTagType>()) {
            ARCTICDB_DEBUG(log::version(), "Returning as end pos is at beginning");
            return res;
        }

        // `upper_bound` gives us the exclusive end of positions with values < range_end. We need the inclusive end.
        --end_pos;
        auto end_offset = std::distance(start_idx_col.template begin<IndexTagType>(), end_pos);

        if (begin_offset > end_offset) {
            ARCTICDB_DEBUG(log::version(), "Returning as start and end pos crossed, no intersecting ranges");
            return res;
        }

        ARCTICDB_DEBUG(log::version(), "Ready to set offset between {} and {}", begin_offset, end_offset);
        res->set_range(begin_offset, end_offset);

    } else {
        interval_timer timer;
        timer.start_timer();
        auto start_idx_pos = start_idx_col.template begin<IndexTagType>();
        auto end_idx_pos = end_idx_col.template begin<IndexTagType>();

        for (auto i = 0u; i < container.size(); ++i) {
            const auto intersects =
                    is_slice_in_index_range(IndexRange(*start_idx_pos, *end_idx_pos), rg, is_read_operation);
            (*res)[i] = intersects;
            if (intersects)
                ARCTICDB_DEBUG(log::version(), "range intersects at {}", i);

            ++start_idx_pos;
            ++end_idx_pos;
        }
        timer.stop_timer();
        ARCTICDB_DEBUG(log::version(), timer.display_all());
    }

    if (input)
        *res &= *input;

    ARCTICDB_DEBUG(log::version(), "Res count = {}", res->count());
    return res;
}

template std::unique_ptr<util::BitSet> build_bitset_for_index<
        IndexSegmentReader,
        TimeseriesIndex>(const index::IndexSegmentReader&, IndexRange, bool, bool, bool, std::unique_ptr<util::BitSet>&&);
template std::unique_ptr<util::BitSet> build_bitset_for_index<
        IndexSegmentReader,
        TableIndex>(const index::IndexSegmentReader&, IndexRange, bool, bool, bool, std::unique_ptr<util::BitSet>&&);
template std::unique_ptr<util::BitSet> build_bitset_for_index<
        TestContainer,
        TimeseriesIndex>(const TestContainer&, IndexRange, bool, bool, bool, std::unique_ptr<util::BitSet>&&);
} // namespace arcticdb::pipelines
