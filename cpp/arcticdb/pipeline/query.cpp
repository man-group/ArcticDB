/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/pipeline/query.hpp>

#include <arcticdb/pipeline/index_utils.hpp>
#include <arcticdb/pipeline/frame_slice.hpp>
#include <arcticdb/pipeline/test/test_container.hpp>

namespace arcticdb::pipelines {

using namespace arcticdb::stream;
using namespace arcticdb::pipelines::index;

IndexValue start_index(const std::vector<SliceAndKey> &sk, std::size_t row) {
    return sk[row].key().start_index();
}

IndexValue start_index(const index::IndexSegmentReader &isr, std::size_t row) {
    return index::index_value_from_segment(isr.seg(), row, index::Fields::start_index);
}

IndexValue end_index(const index::IndexSegmentReader &isr, std::size_t row) {
    return index::index_value_from_segment(isr.seg(), row, index::Fields::end_index);
}

IndexValue end_index(const std::vector<SliceAndKey> &sk, std::size_t row) {
    return sk[row].key().end_index();
}

template<typename ContainerType, typename IdxType>
std::unique_ptr<util::BitSet> build_bitset_for_index(
        const ContainerType& container,
        IndexRange rg,
        bool dynamic_schema,
        bool column_groups,
        std::unique_ptr<util::BitSet>&& input) {
    auto res = std::make_unique<util::BitSet>(static_cast<util::BitSetSizeType>(container.size()));
    if (container.empty())
        return res;

    using IndexTagType = typename IdxType::TypeDescTag;

    const auto &start_idx_col = container.seg().column(position_t(index::Fields::start_index));
    const auto &end_idx_col = container.seg().column(position_t(index::Fields::end_index));
    ARCTICDB_DEBUG(log::version(), "Searching for match in index range {}", rg);

    auto end_index_col_begin = end_idx_col.template begin<IndexTagType>();
    auto end_index_col_end = end_idx_col.template end<IndexTagType>();

    if (dynamic_schema && !column_groups) {
        const auto range_start = std::get<timestamp>(rg.start_);
        const auto range_end = std::get<timestamp>(rg.end_);

        auto start_pos = std::lower_bound(
            end_index_col_begin,
            end_index_col_end,
            range_start
        );

        if(start_pos == end_idx_col.template end<IndexTagType>()) {
            ARCTICDB_DEBUG(log::version(), "Returning as start pos is at end");
            return res;
        }

        auto end_pos = std::upper_bound(
            start_idx_col.template begin<IndexTagType>(),
            start_idx_col.template end<IndexTagType>(),
            range_end
        );

        if(end_pos == start_idx_col.template begin<IndexTagType >()) {
            ARCTICDB_DEBUG(log::version(), "Returning as end pos is at beginning");
            return res;
        }

        // TODO: This logic is so hard to follow and inefficient. We can only rely on lower_bound and upper_bound. Will do this in a follow up commit.
        // Currently with the lower bound we get some value before the one we want and with the upper bound we get some
        // value after the one we want. Then we need to iterate row by row to get the row we want.
        using RawType = typename IndexTagType::DataTypeTag::raw_type;
        auto begin_offset = std::distance(end_index_col_begin, start_pos);
        ARCTICDB_DEBUG(log::version(), "start_pos at {} of {}", begin_offset, end_idx_col.row_count());
        auto end_offset = std::distance(start_idx_col.template begin<IndexTagType>(), end_pos);
        if(end_offset == start_idx_col.row_count()) {
            --end_pos;
            --end_offset;
        }
        ARCTICDB_DEBUG(log::version(), "end offset at {} of {}", end_offset, start_idx_col.row_count());

        auto start_range_begin = start_idx_col.template begin<IndexTagType>();
        std::advance(start_range_begin, begin_offset);
        while(begin_offset <= end_offset && !range_intersects<RawType>(range_start, range_end, *start_range_begin, *start_pos - 1)) {
            ARCTICDB_DEBUG(log::version(), "increasing start index");
            ++start_range_begin;
            ++start_pos;
            ++begin_offset;
        }

        auto end_range_last = end_idx_col.template begin<IndexTagType>();
        std::advance(end_range_last, end_offset);
        while(!range_intersects<RawType>(range_start, range_end, *end_pos, *end_range_last - 1) && begin_offset < end_offset) {
            ARCTICDB_DEBUG(log::version(), "decreasing end index");
            --end_pos;
            --end_range_last;
            --end_offset;
        }

        if(begin_offset > end_offset) {
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

        using RawType = typename IndexTagType::DataTypeTag::raw_type;
        const auto range_start = std::get<timestamp>(rg.start_);
        const auto range_end = std::get<timestamp>(rg.end_);
        for(auto i = 0u; i < container.size(); ++i) {
            const auto intersects = range_intersects<RawType>(range_start, range_end, *start_idx_pos, *end_idx_pos - 1);
            (*res)[i] = intersects;
            if(intersects)
                ARCTICDB_DEBUG(log::version(), "range intersects at {}", i);

            ++start_idx_pos;
            ++end_idx_pos;
        }
        timer.stop_timer();
        ARCTICDB_DEBUG(log::version(), timer.display_all());
    }

    if(input)
        *res &= *input;

    ARCTICDB_DEBUG(log::version(), "Res count = {}", res->count());
    return res;
}

template std::unique_ptr<util::BitSet> build_bitset_for_index<IndexSegmentReader, TimeseriesIndex>(const index::IndexSegmentReader&,  IndexRange, bool, bool, std::unique_ptr<util::BitSet>&&);
template std::unique_ptr<util::BitSet> build_bitset_for_index<IndexSegmentReader, TableIndex>(const index::IndexSegmentReader&,  IndexRange, bool, bool, std::unique_ptr<util::BitSet>&&);
template std::unique_ptr<util::BitSet> build_bitset_for_index<TestContainer, TimeseriesIndex>(const TestContainer&,  IndexRange, bool, bool, std::unique_ptr<util::BitSet>&&);
} //namespace arcticdb
