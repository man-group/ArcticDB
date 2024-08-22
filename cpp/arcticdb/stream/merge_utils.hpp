#pragma once

#include <arcticdb/column_store/chunked_buffer.hpp>
#include <arcticdb/column_store/string_pool.hpp>
#include <arcticdb/column_store/memory_segment.hpp>
#include <arcticdb/util/memory_tracing.hpp>

#include <arcticdb/pipeline/string_pool_utils.hpp>

namespace arcticdb {
inline void merge_string_column(
    ChunkedBuffer& src_buffer,
    const std::shared_ptr<StringPool>& src_pool,
    const std::shared_ptr<StringPool>& merged_pool,
    CursoredBuffer<ChunkedBuffer>& output,
    bool verify
) {
    using OffsetType = entity::position_t;
    constexpr auto offset_size =  sizeof(OffsetType);
    auto num_strings = src_buffer.bytes() / offset_size;
    for(auto row = 0ULL; row < num_strings; ++row) {
        auto offset = get_offset_string_at(row, src_buffer);
        entity::position_t new_value;
        if (offset != not_a_string() && offset != nan_placeholder()) {
            auto sv = get_string_from_pool(offset, *src_pool);
            new_value = merged_pool->get(sv).offset();
        } else {
            new_value = offset;
        }
        output.ensure<OffsetType>(1);
        util::check(new_value >= 0, "Unexpected negative number {} in merge string column", new_value);
        output.typed_cursor<OffsetType>() = new_value;
        output.commit();
    }
    if (verify) {
        const auto& out_buffer = output.buffer();
        auto num_out = out_buffer.bytes() /offset_size;
        util::check(num_strings == num_out, "Mismatch in input/output size {} != {}", num_strings, num_out);
        for(auto row = size_t(0); row < num_out; ++row) {
            auto offset = get_offset_string_at(row, out_buffer);
            if (offset != not_a_string() && offset != nan_placeholder()) {
                auto sv ARCTICDB_UNUSED = get_string_from_pool(offset, *merged_pool);
                ARCTICDB_DEBUG(log::version(), "Accessed {} from string pool", sv);
            }
        }
    }
}

inline void merge_string_columns(const SegmentInMemory& segment, const std::shared_ptr<StringPool>& merged_pool, bool verify) {
    for (size_t c = 0; c < segment.descriptor().field_count(); ++c) {
        auto &frame_field = segment.field(c);
        const auto& field_type = frame_field.type();

        if (!is_sequence_type(field_type.data_type_))
            continue;

        auto &src = segment.column(static_cast<position_t>(c)).data().buffer();
        CursoredBuffer<ChunkedBuffer> cursor{src.bytes(), false};
        merge_string_column(src, segment.string_pool_ptr(), merged_pool, cursor, verify);
        std::swap(src, cursor.buffer());
    }
}

inline bool is_row_identical(const SegmentInMemoryImpl::Row& row1, const SegmentInMemoryImpl::Row& row2) {
    for (auto row1_it = row1.begin(), row2_it = row2.begin(); row1_it != row1.end() && row2_it != row2.end(); row1_it++, row2_it++) {
        auto row1_dt = row1_it->get_field().type().data_type(), row2_dt = row2_it->get_field().type().data_type();
        if (row1_dt != row2_dt) {
            return false;
        }
        if (row1_it->get_field().name() != row2_it->get_field().name()) {
            return false;
        }
        if (row1_it->has_value() != row2_it->has_value()) {
            return false;
        }
        if (row1_it->has_value() && row2_it->has_value()) {
            if (is_sequence_type(row1_dt)) {
                std::optional<std::string_view> row1_val, row2_val;
                row1_it->visit_string([&row1_val](std::optional<std::string_view> val) {
                    if (static_cast<bool>(val)) {
                        row1_val = val.value();
                    }
                });
                row2_it->visit_string([&row2_val](std::optional<std::string_view> val) {
                    if (static_cast<bool>(val)) {
                        row2_val = val.value();
                    }
                });
                if (row1_val && row2_val) {
                    if (row1_val.value() != row2_val.value()) {
                        return false;
                    }
                }
                else if (row1_val != row2_val) {
                    return false;
                }
            } else {
                bool row1_it_has_value = true;
                bool ans = false;
                row1_it->visit([&row1_it_has_value, &row1_dt](auto val) {
                    row1_it_has_value = static_cast<bool>(val) && (!is_floating_point_type(row1_dt) || !std::isnan(static_cast<float>(val.value())));
                });
                row2_it->visit([&row1_it, &ans, &row1_it_has_value, &row2_dt](auto val) {
                    bool row2_it_has_value = static_cast<bool>(val) && (!is_floating_point_type(row2_dt) || !std::isnan(static_cast<float>(val.value())));
                    if (row1_it_has_value != row2_it_has_value) {
                        ans = false;
                    }
                    else if (row1_it_has_value && row2_it_has_value) {
                        using ValType = std::decay_t<decltype(val.value())>;
                        if (val.value() == row1_it->value<ValType>()) {
                            ans = true;
                        }
                    }
                });
                if (!ans) {
                    return false;
                }
            }
        }
    }
    return true;
}

inline util::BitSet get_duplicates_bitset(const SegmentInMemoryImpl &merged, const SegmentInMemoryImpl& to_be_merged) {
    util::BitSet duplicates(to_be_merged.row_count());
    if (duplicates.size() == 0 || merged.row_count() == 0) {
        duplicates.set();
        return duplicates;
    }
    auto prev_row_it = to_be_merged.begin();
    duplicates.set(prev_row_it->row_id_, true);
    for (auto row_it = prev_row_it + 1; row_it != to_be_merged.end(); row_it++, prev_row_it++) {
        if (!is_row_identical(*row_it, *prev_row_it)) {
            duplicates.set(row_it->row_id_, true);
        }
    }
    auto to_be_merged_row_it = to_be_merged.begin();
    
    auto t = to_be_merged_row_it->template index<stream::TimeseriesIndex>();
    log::version().info("to_be_merged_row_it {}", t);
    auto merged_it = std::lower_bound(merged.begin(), merged.end(), *to_be_merged_row_it,
        [](const auto& lhs, const auto& rhs) {
            auto ltime = lhs.template index<stream::TimeseriesIndex>(), rtime = rhs.template index<stream::TimeseriesIndex>();
            log::version().info("lhs {} rhs {}", ltime, rtime);
            return lhs.template index<stream::TimeseriesIndex>() < rhs.template index<stream::TimeseriesIndex>();
        }
    ); // find last matching row with the same index

    while (merged_it != merged.end() && to_be_merged_row_it != to_be_merged.end()) {
        if (!duplicates[to_be_merged_row_it->row_id_]) {
            to_be_merged_row_it++;
        }
        else if (is_row_identical(*merged_it, *to_be_merged_row_it)) {
            duplicates.set(to_be_merged_row_it->row_id_, false);
            merged_it++;
            to_be_merged_row_it++;
        }
        else if (merged_it->template index<stream::TimeseriesIndex>() <= to_be_merged_row_it->template index<stream::TimeseriesIndex>()) {
            merged_it++;
        }
        else {
            break;
        }        
    }
    return duplicates;
}

inline bool dedup_rows_from_last_committed_segment(
    SegmentInMemory& last_committed_segment,
    SegmentInMemory& to_be_merged_segment,
    std::vector<size_t>& segments_final_sizes
) {
    auto last_committed_filter_bitset = get_duplicates_bitset(*last_committed_segment.impl(), *to_be_merged_segment.impl());
    auto set_rows = last_committed_filter_bitset.count();
    ARCTICDB_DEBUG(log::version(), "filter_bitset.count() with last_committed is {} ", set_rows);
    if (set_rows > 0) {
        // Now we have atleast one row which doesn't match with previous committed
        // segment, thus no more segments after this will match since segments are sorted
        // by time
        log::version().info("Non zero rows found - stopping dedup with last_committed_segment now");
        if (set_rows != to_be_merged_segment.row_count()) {
            // filter segment in case it's needed
            to_be_merged_segment = filter_segment(to_be_merged_segment, std::move(last_committed_filter_bitset));
        }
        return false;
    } else {
        segments_final_sizes.push_back(0);
        return true;
    }
}

inline bool dedup_rows_from_segment(
    SegmentInMemory& merged,
    SegmentInMemory& to_be_merged_segment,
    std::vector<size_t>& segments_final_sizes
    ) {
    // TODO: Currently, the original segment will be copied twice - once while filtering and once when
    // TODO: appending to merged. Combine the append and filter functionality.
    auto filter_bitset = get_duplicates_bitset(*merged.impl(), *to_be_merged_segment.impl());
    auto set_rows = filter_bitset.count();
    ARCTICDB_DEBUG(log::version(), "filter_bitset.count() is {} ", set_rows);
    if (set_rows == 0) {
        segments_final_sizes.push_back(0);
        return true;
    }
    if (set_rows < to_be_merged_segment.row_count()) {
        ARCTICDB_DEBUG(log::version(), "Some rows are dup-ed, removing them");
        to_be_merged_segment = filter_segment(to_be_merged_segment, std::move(filter_bitset));
    }
    return false;
}

inline void merge_segments(
    std::vector<SegmentInMemory>&& segments,
    SegmentInMemory& merged,
    bool dedup_rows,
    std::optional<SegmentInMemory>& last_committed_segment,
    std::vector<size_t>& segments_final_sizes,
    bool is_sparse) {
    ARCTICDB_DEBUG(log::version(), "Appending {} segments", segments.size());
    timestamp min_idx = std::numeric_limits<timestamp>::max();
    timestamp max_idx = std::numeric_limits<timestamp>::min();
    bool dedup_with_last_committed = true;
    for (auto &&segment : segments) {
        SegmentInMemory& to_be_merged_segment = segment;
        if (dedup_rows){
            if (dedup_with_last_committed && last_committed_segment) {
                if (dedup_rows_from_last_committed_segment(*last_committed_segment, to_be_merged_segment, segments_final_sizes)) {
                    continue;
                }
                else {
                    dedup_with_last_committed = false;
                }
            }
            if (dedup_rows_from_segment(merged, to_be_merged_segment, segments_final_sizes)) {
                continue;
            }
        }
        ARCTICDB_DEBUG(log::version(), "Appending segment with {} rows", to_be_merged_segment.row_count());
        for(const auto& field : to_be_merged_segment.descriptor().fields()) {
            if(!merged.column_index(field.name())){//TODO: Bottleneck for wide segments
                auto pos = merged.add_column(field, 0, false);
                if (!is_sparse){
                    merged.column(pos).mark_absent_rows(merged.row_count());
                }
            }
        }

        if (to_be_merged_segment.row_count() && to_be_merged_segment.descriptor().index().type() == IndexDescriptorImpl::Type::TIMESTAMP) {
            min_idx = std::min(min_idx, to_be_merged_segment.begin()->begin()->value<timestamp>());
            max_idx = std::max(max_idx, (to_be_merged_segment.end() - 1)->begin()->value<timestamp>());

            if (dedup_rows && merged.row_count()) {
                util::check(to_be_merged_segment.begin()->begin()->value<timestamp>() >= (merged.end() - 1)->begin()->value<timestamp>(),
                            "Timestamp in segment_agg merge is decreasing with merged");
            }
            if (dedup_rows && last_committed_segment && last_committed_segment->row_count()) {
                util::check(to_be_merged_segment.begin()->begin()->value<timestamp>() >= (last_committed_segment->end() - 1)->begin()->value<timestamp>(),
                            "Timestamp in segment_agg merge is decreasing with last_segment_dedup");
            }
        }

        merge_string_columns(to_be_merged_segment, merged.string_pool_ptr(), false);
        segments_final_sizes.push_back(to_be_merged_segment.row_count());
        merged.append(std::move(to_be_merged_segment));
        merged.set_compacted(true);
        util::print_total_mem_usage(__FILE__, __LINE__, __FUNCTION__);
    }
}

inline ssize_t fix_slices_row_range(
    std::vector<pipelines::FrameSlice>& slices,
    const std::vector<size_t>& final_sizes,
    ssize_t previous_reduction_in_size) {

    util::check(slices.size() == final_sizes.size(), "Mismatch between slices and final_sizes size");
    ssize_t reduction_in_size = previous_reduction_in_size;
    for (auto slice: folly::enumerate(slices)) {
        auto original_size = slice->row_range.diff();
        slice->row_range.first -= reduction_in_size;
        reduction_in_size += (original_size - final_sizes[slice.index]);
        slice->row_range.second -= reduction_in_size;
    }
    return reduction_in_size;
}

inline std::pair<pipelines::FrameSlice, ssize_t> merge_slices(
    std::vector<pipelines::FrameSlice>& slices,
    const StreamDescriptor& desc,
    bool dedup_rows,
    const std::vector<size_t>& final_sizes,
    ssize_t previous_reduction_in_size) {
    util::check(!slices.empty(), "Expected to merge non-empty slices_vector");

    if (dedup_rows) {
        previous_reduction_in_size = fix_slices_row_range(slices, final_sizes, previous_reduction_in_size);
    }

    pipelines::FrameSlice output{slices[0]};
    for(const auto& slice : slices) {
        output.row_range.first = std::min(output.row_range.first, slice.row_range.first);
        output.row_range.second = std::max(output.row_range.second, slice.row_range.second);
    }

    output.col_range.first = desc.index().field_count();
    output.col_range.second = desc.field_count();
    return {output, previous_reduction_in_size};
}
}
