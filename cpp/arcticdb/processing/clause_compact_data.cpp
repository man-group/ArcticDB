/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/processing/clause.hpp>

#include <cstdint>
#include <map>
#include <numeric>
#include <set>
#include <tuple>

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
    min_rows_per_segment_ = min_rows_per_segment(rows_per_segment_);
    max_rows_per_segment_ = max_rows_per_segment(rows_per_segment_);
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
        // frame_ is non null when we have arrived here via the compact_data argument to append[_batch]
        // In this case, we treat the frame being appended as if it is ONE row-slice (even if it is larger than the
        // library's default slicing policy). A consequence of this is that the resulting structure on-disk may not be
        // identical to calling append with compact_data=false, followed by an explicit compact_data call.
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
    const std::vector<RowRange> output_row_ranges(
            retained_processing_row_ranges.cbegin(), retained_processing_row_ranges.cend()
    );
    auto output_row_range_idx = [&output_row_ranges](const RowRange& row_range) -> size_t {
        auto it = std::ranges::upper_bound(output_row_ranges, row_range.first, {}, [](const RowRange& output_range) {
            return output_range.first;
        });
        internal::check<ErrorCode::E_ASSERTION_FAILURE>(
                it != output_row_ranges.cbegin() && std::prev(it)->contains(row_range.first),
                "CompactDataClause could not find output row range for row range {}",
                row_range
        );
        return std::distance(output_row_ranges.cbegin(), std::prev(it));
    };
    // Row-slices in the same output row range may be column-sliced differently, e.g. if columns_per_segment was
    // changed between writes. Such output row ranges are processed as one unit spanning all of the columns.
    // Use first element of col_range rather than full col range equality so that it works with dynamic schema where
    // number of columns in each row slice can be different
    std::vector<std::map<size_t, std::set<size_t>>> col_starts_by_row_slice(output_row_ranges.size());
    for (const auto& range_and_key : ranges_and_keys) {
        col_starts_by_row_slice[output_row_range_idx(range_and_key.row_range())][range_and_key.row_range().first]
                .insert(range_and_key.col_range().first);
    }
    std::vector<bool> mixed_column_slicing(output_row_ranges.size(), false);
    for (const auto& [idx, col_starts] : folly::enumerate(col_starts_by_row_slice)) {
        mixed_column_slicing[idx] = std::ranges::any_of(col_starts, [&col_starts](const auto& row_slice_col_starts) {
            return row_slice_col_starts.second != col_starts.cbegin()->second;
        });
    }
    // Keyed on (mixed column slicing, column slice start, output row range index), with column slice start 0 for
    // mixed column slicing
    std::map<std::tuple<bool, size_t, size_t>, std::vector<size_t>> groups;
    for (const auto& [idx, range_and_key] : folly::enumerate(ranges_and_keys)) {
        const auto row_range_idx = output_row_range_idx(range_and_key.row_range());
        const auto key = mixed_column_slicing[row_range_idx]
                                 ? std::make_tuple(true, size_t{0}, row_range_idx)
                                 : std::make_tuple(false, range_and_key.col_range().first, row_range_idx);
        groups[key].emplace_back(idx);
    }
    std::vector<std::vector<size_t>> res;
    res.reserve(groups.size());
    for (auto& [key, indexes] : groups) {
        res.emplace_back(std::move(indexes));
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
    if (std::ranges::any_of(*proc.col_ranges_, [col_range_start](const std::shared_ptr<ColRange>& col_range) {
            return col_range->first != col_range_start;
        })) {
        return process_mixed_column_slicing(std::move(proc));
    }
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
        add_segment_from_frame(
                ColRange{
                        col_range_start,
                        dynamic_schema_ ? frame_->desc().field_count() : proc.col_ranges_->front()->second
                },
                segments
        );
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

namespace {
// Builds a segment holding the index column(s) and data columns [begin, end) (excluding the index) of segment, moving
// the columns out of segment so that SegmentReslicer can free them as it goes. The index column(s) are moved only on
// the last use, and copied otherwise.
SegmentInMemory take_columns(SegmentInMemory& segment, size_t begin, size_t end, bool last_use) {
    const auto& desc = segment.descriptor();
    const auto index_field_count = desc.index().field_count();
    const auto row_count = segment.row_count();
    SegmentInMemory res;
    res.attach_descriptor(
            std::make_shared<StreamDescriptor>(desc.segment_desc_, std::make_shared<FieldCollection>(), desc.stream_id_)
    );
    for (size_t idx = 0; idx < index_field_count; ++idx) {
        auto column = last_use ? std::move(segment.columns().at(idx))
                               : std::make_shared<Column>(segment.column(static_cast<position_t>(idx)).clone());
        res.add_column(segment.field(idx), column);
    }
    for (size_t idx = index_field_count + begin; idx < index_field_count + end; ++idx) {
        res.add_column(segment.field(idx), std::move(segment.columns().at(idx)));
    }
    res.set_string_pool(segment.string_pool_ptr());
    res.set_row_id(static_cast<ssize_t>(row_count) - 1);
    return res;
}
} // namespace

std::vector<EntityId> CompactDataClause::process_mixed_column_slicing(ProcessingUnit&& proc) const {
    // Only static schema data can reach here. Stock writers never column-slice dynamic schema data.
    internal::check<ErrorCode::E_ASSERTION_FAILURE>(
            !dynamic_schema_, "CompactDataClause cannot combine differently column-sliced data with dynamic schema"
    );
    const auto num_segments = proc.segments_->size();
    // Output column slices are the common refinement of the input column slices, so every input segment splits into
    // whole output column slices
    std::set<size_t> boundaries;
    for (const auto& col_range : *proc.col_ranges_) {
        boundaries.insert(col_range->first);
        boundaries.insert(col_range->second);
    }
    const std::vector<size_t> col_starts(boundaries.cbegin(), boundaries.cend());
    std::vector<size_t> order(num_segments);
    std::iota(order.begin(), order.end(), 0);
    std::ranges::sort(order, [&proc](size_t left, size_t right) {
        return std::tie(proc.row_ranges_->at(left)->first, proc.col_ranges_->at(left)->first) <
               std::tie(proc.row_ranges_->at(right)->first, proc.col_ranges_->at(right)->first);
    });
    const auto row_range_start = proc.row_ranges_->at(order.front())->first;
    const auto row_range_end = proc.row_ranges_->at(order.back())->second;
    log::version().debug(
            "CompactDataClause processing {} differently column-sliced segments with row ranges spanning [{:d}, {:d}] "
            "into {} column slices",
            num_segments,
            row_range_start,
            row_range_end,
            col_starts.size() - 1
    );
    std::vector<std::vector<SegmentInMemory>> segments_by_output_col_slice(col_starts.size() - 1);
    for (auto idx : order) {
        auto segment = std::move(proc.segments_->at(idx));
        const auto& col_range = *proc.col_ranges_->at(idx);
        auto output_idx = static_cast<size_t>(
                std::distance(col_starts.cbegin(), std::ranges::lower_bound(col_starts, col_range.first))
        );
        for (auto start = col_range.first; start < col_range.second; ++output_idx) {
            const auto end = col_starts.at(output_idx + 1);
            segments_by_output_col_slice.at(output_idx)
                    .emplace_back(take_columns(
                            *segment, start - col_range.first, end - col_range.first, end == col_range.second
                    ));
            start = end;
        }
    }
    std::vector<std::shared_ptr<SegmentInMemory>> segment_ptrs;
    std::vector<std::shared_ptr<RowRange>> row_ranges;
    std::vector<std::shared_ptr<ColRange>> col_ranges;
    for (auto&& [output_idx, segments] : folly::enumerate(segments_by_output_col_slice)) {
        const ColRange col_range{col_starts.at(output_idx), col_starts.at(output_idx + 1)};
        if (frame_ && frame_->offset == row_range_end) {
            add_segment_from_frame(col_range, segments);
        }
        SegmentReslicer reslicer{max_rows_per_segment_};
        auto resliced = reslicer.reslice_segments(std::move(segments));
        auto row_start = row_range_start;
        for (auto& segment : resliced) {
            row_ranges.emplace_back(std::make_shared<RowRange>(row_start, row_start + segment.row_count()));
            col_ranges.emplace_back(std::make_shared<ColRange>(col_range));
            row_start += segment.row_count();
            segment_ptrs.emplace_back(std::make_shared<SegmentInMemory>(std::move(segment)));
        }
    }
    proc.set_segments(std::move(segment_ptrs));
    proc.set_row_ranges(std::move(row_ranges));
    proc.set_col_ranges(std::move(col_ranges));
    return push_entities(*component_manager_, std::move(proc));
}

void CompactDataClause::add_segment_from_frame(const ColRange& col_range, std::vector<SegmentInMemory>& segments)
        const {
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
    for (size_t idx = 0; idx < reslicing_info.num_segments(); ++idx) {
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
            {RowRange{frame_->offset, frame_->offset + (row_count - total_rows_without_frame)}}, {col_range}
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
