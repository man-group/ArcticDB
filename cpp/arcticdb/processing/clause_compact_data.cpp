/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/processing/clause.hpp>

#include <cstdint>
#include <set>

#include <arcticdb/column_store/column_reslicer.hpp>
#include <arcticdb/column_store/segment_reslicer.hpp>
#include <arcticdb/pipeline/frame_slice.hpp>
#include <arcticdb/pipeline/input_frame.hpp>
#include <arcticdb/pipeline/write_frame.hpp>
#include <arcticdb/processing/clause_utils.hpp>
#include <arcticdb/util/collection_utils.hpp>

namespace arcticdb {

CompactDataClause::CompactDataClause(uint64_t rows_per_segment, std::shared_ptr<InputFrame> frame) :
    rows_per_segment_(rows_per_segment) {
    if (frame && frame->num_rows > 0) {
        frame_ = std::move(frame);
    }
    // Magic range of +-33% chosen so that:
    // - 2 row slices with < min_rows_per_segment_ cannot be combined into one row slice with > max_rows_per_segment_
    // - 1 row slice with a little more than max_rows_per_segment_ when split in half will still have
    //   >= min_rows_per_segment_ in each resulting row slice
    // If rows_per_segment_ == 1 min_rows_per_segment_ would be 0 without the std::max
    min_rows_per_segment_ = std::max((2 * rows_per_segment_) / 3, uint64_t(1));
    // If rows_per_segment_ == 2 max_rows_per_segment_ would be 2 without the std::max
    max_rows_per_segment_ = std::max((4 * rows_per_segment_) / 3, rows_per_segment_ + 1);
    clause_info_.input_structure_ = ProcessingStructure::ONE_COL_SLICE_MULTIPLE_ROW_SLICES;
    clause_info_.output_structure_ = ProcessingStructure::ONE_COL_SLICE_MULTIPLE_ROW_SLICES;
    clause_info_.can_combine_with_column_selection_ = false;
}

bool CompactDataClause::row_ranges_all_acceptable_lengths(const std::set<RowRange>& row_ranges) const {
    return std::ranges::all_of(row_ranges, [this](const RowRange& row_range) {
        return min_rows_per_segment_ <= row_range.diff() && row_range.diff() <= max_rows_per_segment_;
    });
}

// Given the set of row ranges in the input data:
// {[0, x1), [x1, x2), ..., [xn-1, xn), [xn, total_rows)}
// produces a covering set of output row ranges
// {[0, y1), [y1, y2), ..., [ym-1, ym), [ym, total_rows)}
// The output ranges will then either:
// - Discarded from the set of segments to process if they are in the input ranges, as they do not need to change
// - Passed to process to be combined/split as appropriate
std::set<RowRange> CompactDataClause::structure_row_ranges(const std::set<RowRange>& row_ranges) const {
    if (row_ranges.empty()) {
        return {};
    }
    // Greedy algorithm - keep adding row ranges until there are at least min_rows_per_segment_
    // If possible, keep adding more to get as close as possible to rows_per_segment_
    // If it is necessary to get above min_rows_per_segment_ in a slice, we may have to exceed max_rows_per_segment_
    // In this case, CompactDataClause::process will split into multiple row slices
    std::set<RowRange> res;
    RowRange current = *row_ranges.cbegin();
    for (auto row_range = std::next(row_ranges.cbegin()); row_range != row_ranges.cend(); ++row_range) {
        const bool current_below_min_rows_per_segment = current.diff() < min_rows_per_segment_;
        const bool below_rows_per_segment_with_this_slice = current.diff() + row_range->diff() <= rows_per_segment_;
        // This is equivalent to:
        // |(current.diff() + row_range->diff()) - rows_per_segment_| < rows_per_segment_ - current.diff()
        // but without any subtractions which can underflow with unsigned integers
        const bool closer_to_rows_per_segment_with_this_slice_than_without =
                (2 * current.diff()) + row_range->diff() < 2 * rows_per_segment_;
        if (current_below_min_rows_per_segment || below_rows_per_segment_with_this_slice ||
            closer_to_rows_per_segment_with_this_slice_than_without) {
            current.second = row_range->second;
        } else {
            res.emplace(current);
            current = *row_range;
        }
    }
    if (current.diff() >= min_rows_per_segment_ || res.empty()) {
        res.emplace(current);
    } else {
        // Extend last entry of result to include current
        auto last_it = std::prev(res.end());
        auto last_row_range = *last_it;
        last_row_range.second = current.second;
        res.erase(last_it);
        res.emplace(last_row_range);
    }
    // It is possible that the last slice has fewer than min_rows_per_segment_, so combine with previous slice if this
    // is the case
    auto last_it = std::prev(res.end());
    auto last_row_range = *last_it;
    if (last_row_range.diff() < min_rows_per_segment_ && last_it != res.begin()) {
        auto penultimate_it = std::prev(last_it);
        last_row_range.first = penultimate_it->first;
        res.erase(penultimate_it, res.end());
        res.emplace(last_row_range);
    }
    return res;
}

std::vector<std::vector<size_t>> CompactDataClause::structure_for_processing(std::vector<RangesAndKey>& ranges_and_keys
) {
    log::version().debug("CompactDataClause structuring {} data keys for processing", ranges_and_keys.size());
    // Extract the unique row ranges
    std::set<RowRange> row_ranges;
    for (const auto& range_and_key : ranges_and_keys) {
        row_ranges.insert(range_and_key.row_range());
    }
    if (frame_) {
        // frame_ is non null when we have arrived here via the compact_data_inline argument to append[_batch]
        // In this case, we treat the frame being appended as if it is ONE row-slice (even if it is larger than the
        // library's default slicing policy). A consequence of this is that the resulting structure on-disk may not be
        // identical to calling append with compact_data_inline=false, followed by an explicit compact_data call.
        // However, the invariant that all row-slices will have rows_per_segments+-33% will be maintained in both cases
        util::check(
                frame_->offset == (row_ranges.empty() ? 0 : row_ranges.rbegin()->second),
                "CompactDataClause expects the input frame offset to match the end of the existing data"
        );
        row_ranges.emplace(frame_->offset, frame_->offset + frame_->num_rows);
    }
    // The greedy algorithm in structure_row_ranges can reslice data where all segments have an acceptable number of
    // rows, which is not desirable, so short-circuit out if this is the case
    if (row_ranges_all_acceptable_lengths(row_ranges)) {
        log::version().debug("No work to do in CompactDataClause, existing data is already compacted");
        ranges_and_keys.clear();
        return {};
    }
    auto processing_row_ranges = structure_row_ranges(row_ranges);
    // We can eliminate elements of ranges_and_key where their row range is in processing_row_ranges unless they have
    // more than max_rows_per_segment_ rows, as it implies they do not need to change
    auto retained_processing_row_ranges = processing_row_ranges;
    std::erase_if(
            ranges_and_keys,
            [this, &processing_row_ranges, &retained_processing_row_ranges](const RangesAndKey& range_and_key) {
                if (processing_row_ranges.contains(range_and_key.row_range()) &&
                    range_and_key.row_range().diff() <= max_rows_per_segment_) {
                    retained_processing_row_ranges.erase(range_and_key.row_range());
                    return true;
                } else {
                    return false;
                }
            }
    );
    if (ranges_and_keys.empty()) {
        log::version().debug("No work to do in CompactDataClause, existing data is already compacted");
        return {};
    }
    // Order by column slice (i.e. all segments in first column slice from top to bottom, then second column slice, etc)
    std::ranges::sort(ranges_and_keys, [](const RangesAndKey& left, const RangesAndKey& right) {
        return std::tie(left.col_range().first, left.row_range().first) <
               std::tie(right.col_range().first, right.row_range().first);
    });
    std::vector<std::vector<size_t>> res;
    std::vector<size_t> current;
    auto row_range = retained_processing_row_ranges.cbegin();
    // Use first element of col_range rather than full col range equality so that it works with dynamic schema where
    // number of columns in each row slice can be different
    auto col_range_start = ranges_and_keys.front().col_range().first;
    for (const auto& [idx, range_and_key] : folly::enumerate(ranges_and_keys)) {
        if (range_and_key.col_range().first == col_range_start) {
            if (row_range->contains(range_and_key.row_range().first)) {
                current.emplace_back(idx);
            } else {
                res.emplace_back(std::move(current));
                current = std::vector<size_t>{idx};
                ++row_range;
            }
        } else {
            // Moving to the next column slice
            res.emplace_back(std::move(current));
            current = std::vector<size_t>{idx};
            col_range_start = range_and_key.col_range().first;
            row_range = retained_processing_row_ranges.cbegin();
        }
    }
    if (!current.empty()) {
        res.emplace_back(std::move(current));
    }
    log::version().debug("CompactDataClause processing {} data keys in {} batches", ranges_and_keys.size(), res.size());
    return res;
}

std::vector<std::vector<EntityId>> CompactDataClause::structure_for_processing(std::vector<std::vector<EntityId>>&&) {
    internal::raise<ErrorCode::E_ASSERTION_FAILURE>("CompactData clause should be the first clause in the pipeline");
}

std::vector<EntityId> CompactDataClause::process(std::vector<EntityId>&& entity_ids) const {
    auto input_segment_count = entity_ids.size();
    util::check(input_segment_count != 0, "Unexpected empty entity_ids in CompactDataClause::process");
    auto proc = gather_entities<std::shared_ptr<SegmentInMemory>, std::shared_ptr<RowRange>, std::shared_ptr<ColRange>>(
            *component_manager_, entity_ids
    );
    auto col_range_start = proc.col_ranges_->front()->first;
    log::version().debug(
            "CompactDataClause processing {} segments with starting column {} and with row ranges spanning [{:d}, "
            "{:d}]",
            input_segment_count,
            col_range_start,
            proc.row_ranges_->front()->first,
            proc.row_ranges_->back()->second
    );
    std::vector<SegmentInMemory> segments = util::extract_from_pointers(std::move(*proc.segments_));
    // Presence of frame implies we are doing inline compaction in append. The second condition means we are processing
    // the last row-slice of the existing data, and so it must be combined with the frame
    if (frame_ && frame_->offset == proc.row_ranges_->back()->second) {
        add_segment_from_frame(proc, col_range_start, segments);
    }

    SegmentReslicer reslicer{max_rows_per_segment_};
    segments = reslicer.reslice_segments(std::move(segments));

    std::vector<std::shared_ptr<SegmentInMemory>> segment_ptrs = util::extract_to_pointers(std::move(segments));
    std::vector<std::shared_ptr<RowRange>> row_ranges;
    std::vector<std::shared_ptr<ColRange>> col_ranges;
    auto row_range_start = proc.row_ranges_->front()->first;
    const auto index_field_count = segment_ptrs.front()->descriptor().index().field_count();
    for (const auto& segment : segment_ptrs) {
        col_ranges.emplace_back(std::make_shared<ColRange>(
                col_range_start, col_range_start + segment->num_columns() - index_field_count
        ));
        row_ranges.emplace_back(std::make_shared<RowRange>(row_range_start, row_range_start + segment->row_count()));
        row_range_start += segment->row_count();
    }
    proc.set_segments(std::move(segment_ptrs));
    proc.set_row_ranges(std::move(row_ranges));
    proc.set_col_ranges(std::move(col_ranges));
    log::version().debug(
            "CompactDataClause compacted {} segments into {} segments with starting column {} and with row ranges "
            "spanning [{:d}, {:d}]",
            input_segment_count,
            proc.segments_->size(),
            col_range_start,
            proc.row_ranges_->front()->first,
            proc.row_ranges_->back()->second
    );
    return push_entities(*component_manager_, std::move(proc));
}

void CompactDataClause::add_segment_from_frame(
        const ProcessingUnit& proc, size_t col_range_start, std::vector<SegmentInMemory>& segments
) const {
    uint64_t total_rows_without_frame = std::accumulate(
            segments.cbegin(),
            segments.cend(),
            uint64_t(0),
            [](uint64_t n, const SegmentInMemory& segment) { return n + segment.row_count(); }
    );
    auto total_rows = total_rows_without_frame + frame_->num_rows;
    ReslicingInfo reslicing_info{total_rows, max_rows_per_segment_};
    // Work out how many rows of the frame we need to combine with segments from disk
    uint64_t row_count{0};
    for (size_t idx = 0; idx < reslicing_info.num_segments; ++idx) {
        row_count += reslicing_info.rows_in_slice(idx);
        if (row_count > total_rows_without_frame) {
            break;
        }
    }
    // Note that this RowRange does not automatically include all of the rows in the frame. This is for 3 reasons:
    // - WriteToSegmentTask can be parallelised in the post-processing step that writes the remainder of the frame
    // - At time of writing, WriteClause compresses all of the segments it receives in 1 thread, which can also be
    //   parallelised if we do it later
    // - If no compaction of existing data is needed, there is no obvious set of entity ids to call
    //   CompactDataClause::process with to slice+write the frame
    const SpecificSlicer slicer{
            {RowRange{frame_->offset, frame_->offset + (row_count - total_rows_without_frame)}},
            {ColRange{
                    col_range_start, dynamic_schema_ ? frame_->desc().field_count() : proc.col_ranges_->front()->second
            }}
    };
    // There is a check in SpecificSlicer that exactly 1 slice will be produced in this case
    auto frame_slice = slicer(*frame_)[0];
    WriteToSegmentTask write_to_segment_task{frame_, std::move(frame_slice)};
    segments.emplace_back(std::get<SegmentInMemory>(write_to_segment_task()));
}

const ClauseInfo& CompactDataClause::clause_info() const { return clause_info_; }

void CompactDataClause::set_processing_config(const ProcessingConfig& processing_config) {
    dynamic_schema_ = processing_config.dynamic_schema_;
}

void CompactDataClause::set_component_manager(std::shared_ptr<ComponentManager> component_manager) {
    component_manager_ = std::move(component_manager);
}

OutputSchema CompactDataClause::modify_schema(OutputSchema&& output_schema) const { return output_schema; }

OutputSchema CompactDataClause::join_schemas(std::vector<OutputSchema>&&) const {
    util::raise_rte("CompactDataClause::join_schemas should never be called");
}

std::string CompactDataClause::to_string() const {
    return fmt::format("COMPACT_DATA(rows_per_segment={})", rows_per_segment_);
}

} // namespace arcticdb
