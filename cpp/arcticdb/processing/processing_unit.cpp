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
        auto seg = filter_segment(*segment,
                                  std::move(bitset),
                                  filter_down_stringpool);
        auto num_rows = seg.is_null() ? 0 : seg.row_count();
        row_ranges_->at(idx) = std::make_shared<pipelines::RowRange>(row_ranges_->at(idx)->first, row_ranges_->at(idx)->first + num_rows);
        auto num_cols = seg.is_null() ? 0 : seg.descriptor().field_count() - seg.descriptor().index().field_count();
        col_ranges_->at(idx) = std::make_shared<pipelines::ColRange>(col_ranges_->at(idx)->first, col_ranges_->at(idx)->first + num_cols);
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
            if (auto opt_idx = segment->column_index(column_name.value)) {
                return VariantData(ColumnWithStrings(
                        segment->column_ptr(
                        position_t(position_t(opt_idx.value()))),
                        segment->string_pool_ptr(),
                        column_name.value));
            }
        }
        // Try multi-index column names
        std::string multi_index_column_name = fmt::format("__idx__{}",
                                                          column_name.value);
        for (const auto& segment: *segments_) {
            if (auto opt_idx = segment->column_index(multi_index_column_name)) {
                return VariantData(ColumnWithStrings(
                        segment->column_ptr(
                        position_t(*opt_idx)),
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
        [&](const ExpressionName &expression_name) {
        if (auto computed = computed_data_.find(expression_name.value);
        computed != std::end(computed_data_)) {
            return computed->second;
        } else {
            auto expr = expression_context_->expression_nodes_.get_value(expression_name.value);
            auto data = expr->compute(self());
            computed_data_.try_emplace(expression_name.value, data);
            return data;
        }
        },
        [&]([[maybe_unused]] const std::monostate &unused) -> VariantData {
        util::raise_rte("ProcessingUnit::get called with monostate VariantNode");
    }
    );
}

} //namespace arcticdb