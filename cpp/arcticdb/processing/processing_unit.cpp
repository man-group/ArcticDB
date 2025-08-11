/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/processing/processing_unit.hpp>

namespace arcticdb {

void ProcessingUnit::apply_filter(
    util::BitSet&& bitset,
    PipelineOptimisation optimisation) {
    internal::check<ErrorCode::E_ASSERTION_FAILURE>(segments_.has_value() && row_ranges_.has_value() && col_ranges_.has_value(),
                                                    "ProcessingUnit::apply_filter requires all of segments, row_ranges, and col_ranges to be present");
    auto filter_down_stringpool = optimisation == PipelineOptimisation::MEMORY;

    for (auto&& [idx, segment]: folly::enumerate(*segments_)) {
        auto seg = filter_segment(*segment, bitset, filter_down_stringpool);
        auto num_rows = seg.is_null() ? 0 : seg.row_count();
        auto& row_range = row_ranges_->at(idx);
        row_range = std::make_shared<pipelines::RowRange>(row_range->first, row_range->first + num_rows);
        auto num_cols = seg.is_null() ? 0 : seg.descriptor().field_count() - seg.descriptor().index().field_count();
        auto& col_range = col_ranges_->at(idx);
        col_range = std::make_shared<pipelines::ColRange>(col_range->first, col_range->first + num_cols);
        segments_->at(idx) = std::make_shared<SegmentInMemory>(std::move(seg));
    }
}

// Inclusive of start_row, exclusive of end_row
void ProcessingUnit::truncate(size_t start_row, size_t end_row) {
    internal::check<ErrorCode::E_ASSERTION_FAILURE>(segments_.has_value() && row_ranges_.has_value() && col_ranges_.has_value(),
                                                    "ProcessingUnit::truncate requires all of segments, row_ranges, and col_ranges to be present");

    for (auto&& [idx, segment]: folly::enumerate(*segments_)) {
        auto seg = segment->truncate(start_row, end_row, false);
        auto num_rows = seg.is_null() ? 0 : seg.row_count();
        row_ranges_->at(idx) = std::make_shared<pipelines::RowRange>(row_ranges_->at(idx)->first, row_ranges_->at(idx)->first + num_rows);
        auto num_cols = seg.is_null() ? 0 : seg.descriptor().field_count() - seg.descriptor().index().field_count();
        col_ranges_->at(idx) = std::make_shared<pipelines::ColRange>(col_ranges_->at(idx)->first, col_ranges_->at(idx)->first + num_cols);
        segments_->at(idx) = std::make_shared<SegmentInMemory>(std::move(seg));
    }
}

VariantData ProcessingUnit::get(const VariantNode &name) {
    internal::check<ErrorCode::E_ASSERTION_FAILURE>(segments_.has_value(), "ProcessingUnit::get requires segments to be present");
    return util::variant_match(name,
        [&](const ColumnName &column_name) {
        for (const auto& segment: *segments_) {
            segment->init_column_map();
            if (const auto opt_idx = segment->column_index_with_name_demangling(column_name.value)) {
                return VariantData(ColumnWithStrings(
                        segment->column_ptr(static_cast<position_t>(*opt_idx)),
                        segment->string_pool_ptr(),
                        column_name.value));
            }
        }

        if (expression_context_ && !expression_context_->dynamic_schema_) {
            internal::raise<ErrorCode::E_ASSERTION_FAILURE>("Column {} not found in {}",
                                                            column_name,
                                                            segments_->at(0)->descriptor());
        } else {
            log::version().debug("Column {} not found in {}", column_name, segments_->at(0)->descriptor());
            return VariantData{EmptyResult{}};
        }
        },
        [&](const ValueName &value_name) {
        return VariantData(expression_context_->values_.get_value(value_name.value));
        },
        [&](const ValueSetName &value_set_name) {
        return VariantData(expression_context_->value_sets_.get_value(value_set_name.value));
        },
        [&](const RegexName &regex_name) {
        return VariantData(expression_context_->regex_matches_.get_value(regex_name.value));
        },
        [&](const ExpressionName &expression_name) {
        if (auto computed = computed_data_.find(expression_name.value); computed != std::end(computed_data_)) {
            return computed->second;
        } else {
            auto expr = expression_context_->expression_nodes_.get_value(expression_name.value);
            auto data = expr->compute(*this);
            computed_data_.try_emplace(expression_name.value, data);
            return data;
        }
        },
        [&](const std::monostate&) -> VariantData {
        util::raise_rte("ProcessingUnit::get called with monostate VariantNode");
    }
    );
}

std::vector<ProcessingUnit> split_by_row_slice(ProcessingUnit&& proc) {
    auto input = std::move(proc);
    internal::check<ErrorCode::E_ASSERTION_FAILURE>(input.segments_.has_value(), "split_by_row_slice needs Segments");
    internal::check<ErrorCode::E_ASSERTION_FAILURE>(input.row_ranges_.has_value(), "split_by_row_slice needs RowRanges");
    internal::check<ErrorCode::E_ASSERTION_FAILURE>(input.col_ranges_.has_value(), "split_by_row_slice needs ColRanges");
    auto include_entity_fetch_count = input.entity_fetch_count_.has_value();

    std::vector<ProcessingUnit> output;
    // Some clauses (e.g. AggregationClause) are lossy about row-ranges. We can assume that if all of the input column
    // ranges start with zero, that every segment belongs to a different logical row-slice
    if (std::all_of(input.col_ranges_->begin(), input.col_ranges_->end(), [](const auto& col_range) { return col_range->start() == 0; })) {
        output.reserve(input.segments_->size());
        for (size_t idx = 0; idx < input.segments_->size(); ++idx) {
            ProcessingUnit proc_tmp(std::move(*input.segments_->at(idx)), std::move(*input.row_ranges_->at(idx)), std::move(*input.col_ranges_->at(idx)));
            if (include_entity_fetch_count) {
                proc_tmp.set_entity_fetch_count({input.entity_fetch_count_->at(idx)});
            }
            output.emplace_back(std::move(proc_tmp));
        }
    } else {
        std::map<RowRange, ProcessingUnit> output_map;
        for (auto [idx, row_range_ptr]: folly::enumerate(*input.row_ranges_)) {
            if (auto it = output_map.find(*row_range_ptr); it != output_map.end()) {
                it->second.segments_->emplace_back(input.segments_->at(idx));
                it->second.row_ranges_->emplace_back(input.row_ranges_->at(idx));
                it->second.col_ranges_->emplace_back(input.col_ranges_->at(idx));
                if (include_entity_fetch_count) {
                    it->second.entity_fetch_count_->emplace_back(input.entity_fetch_count_->at(idx));
                }
            } else {
                auto [inserted_it, _] = output_map.emplace(*row_range_ptr, ProcessingUnit{});
                inserted_it->second.segments_.emplace(1, input.segments_->at(idx));
                inserted_it->second.row_ranges_.emplace(1, input.row_ranges_->at(idx));
                inserted_it->second.col_ranges_.emplace(1, input.col_ranges_->at(idx));
                if (include_entity_fetch_count) {
                    inserted_it->second.entity_fetch_count_.emplace(1, input.entity_fetch_count_->at(idx));
                }
            }
        }
        output.reserve(output_map.size());
        for (auto &&[_, processing_unit]: output_map) {
            output.emplace_back(std::move(processing_unit));
        }
    }

    internal::check<ErrorCode::E_ASSERTION_FAILURE>(!output.empty(), "Unexpected empty output in split_by_row_slice");
    if (include_entity_fetch_count) {
        // The expected get counts for all segments in a row slice should be the same
        // This should always be 1 or 2 for the first/last row slice, and 1 for all of the others
        for (auto row_slice = output.cbegin(); row_slice != output.cend(); ++row_slice) {
            auto entity_fetch_count = row_slice->entity_fetch_count_->front();
            uint64_t max_entity_fetch_count = row_slice == output.cbegin() || row_slice == std::prev(output.cend()) ? 2 : 1;
            internal::check<ErrorCode::E_ASSERTION_FAILURE>(0 < entity_fetch_count && entity_fetch_count <= max_entity_fetch_count,
                                                            "entity_fetch_count in split_by_row_slice should be 1 or 2, got {}",
                                                            entity_fetch_count);
            internal::check<ErrorCode::E_ASSERTION_FAILURE>(
                    std::all_of(row_slice->entity_fetch_count_->begin(),
                                row_slice->entity_fetch_count_->end(),
                                [&entity_fetch_count](uint64_t i) { return i == entity_fetch_count; }),
                    "All segments in same row slice should have same entity_fetch_count in split_by_row_slice");
        }
    }

    return output;
}

} //namespace arcticdb