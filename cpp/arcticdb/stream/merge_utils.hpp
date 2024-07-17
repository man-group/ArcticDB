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

inline bool dedup_rows_from_segment(
    bool dedup_with_last_committed,
    SegmentInMemory& merged,
    std::optional<SegmentInMemory> last_committed_segment,
    std::vector<SegmentInMemory>& history,
    std::vector<size_t>& segments_final_sizes
    ) {
    //TODO: should definitely moved this to a separate function and not called here
    if (dedup_with_last_committed && last_committed_segment) {
        auto last_committed_filter_bitset = last_committed_segment->get_duplicates_bitset(*history.rbegin());
        auto set_rows = last_committed_filter_bitset.count();
        ARCTICDB_DEBUG(log::version(), "filter_bitset.count() with last_committed is {} ", set_rows);
        if (set_rows > 0) {
            // Now we have atleast one row which doesn't match with previous committed
            // segment, thus no more segments after this will match since segments are sorted
            // by time
            log::version().info("Non zero rows found - stopping dedup with last_committed_segment now");
            dedup_with_last_committed = false;
        } else {
            segments_final_sizes.push_back(0);
            return true;
        }
        if (set_rows != history.rbegin()->row_count()) {
            // filter segment in case it's needed
            history.emplace_back(filter_segment(*history.rbegin(), std::move(last_committed_filter_bitset)));
        }
    }
    // TODO: Currently, the original segment will be copied twice - once while filtering and once when
    // TODO: appending to merged. Combine the append and filter functionality.
    auto filter_bitset = merged.get_duplicates_bitset(*history.rbegin());
    auto set_rows = filter_bitset.count();
    ARCTICDB_DEBUG(log::version(), "filter_bitset.count() is {} ", set_rows);
    if (set_rows == 0) {
        segments_final_sizes.push_back(0);
        return true;
    }
    if (set_rows < history.rbegin()->row_count()) {
        ARCTICDB_DEBUG(log::version(), "Some rows are dup-ed, removing them");
        history.emplace_back(filter_segment(*history.rbegin(), std::move(filter_bitset)));
    }
    return false;
}

inline void merge_segments(
    std::vector<SegmentInMemory>& segments,
    SegmentInMemory& merged,
    bool dedup_rows,
    std::optional<SegmentInMemory>& last_committed_segment,
    std::vector<size_t>& segments_final_sizes,
    bool is_sparse) {
    ARCTICDB_DEBUG(log::version(), "Appending {} segments", segments.size());
    timestamp min_idx = std::numeric_limits<timestamp>::max();
    timestamp max_idx = std::numeric_limits<timestamp>::min();
    bool dedup_with_last_committed = true;
    for (auto &segment : segments) {
        std::vector<SegmentInMemory> history{(segment)}; //TODO - a single SegmentInMemory suffices
        if (dedup_rows && dedup_rows_from_segment(dedup_with_last_committed, merged, last_committed_segment, history, segments_final_sizes)){
            continue;
        }
        ARCTICDB_DEBUG(log::version(), "Appending segment with {} rows", history.rbegin()->row_count());
        for(const auto& field : history.rbegin()->descriptor().fields()) {
            if(!merged.column_index(field.name())){//TODO: Bottleneck for wide segments
                auto pos = merged.add_column(field, 0, false);
                if (!is_sparse){
                    merged.column(pos).mark_absent_rows(merged.row_count());
                }
            }
        }

        if (history.rbegin()->row_count() && history.rbegin()->descriptor().index().type() == IndexDescriptorImpl::Type::TIMESTAMP) {
            min_idx = std::min(min_idx, history.rbegin()->begin()->begin()->value<timestamp>());
            max_idx = std::max(max_idx, (history.rbegin()->end() - 1)->begin()->value<timestamp>());

            if (dedup_rows && merged.row_count()) {
                util::check(history.rbegin()->begin()->begin()->value<timestamp>() >= (merged.end() - 1)->begin()->value<timestamp>(),
                            "Timestamp in segment_agg merge is decreasing with merged");
            }
            if (dedup_rows && last_committed_segment && last_committed_segment->row_count()) {
                util::check(history.rbegin()->begin()->begin()->value<timestamp>() >= (last_committed_segment->end() - 1)->begin()->value<timestamp>(),
                            "Timestamp in segment_agg merge is decreasing with last_segment_dedup");
            }
        }

        merge_string_columns(*history.rbegin(), merged.string_pool_ptr(), false);
        merged.append(*history.rbegin());
        merged.set_compacted(true);
        segments_final_sizes.push_back(history.rbegin()->row_count());
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
