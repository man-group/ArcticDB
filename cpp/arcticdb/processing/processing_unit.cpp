/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/processing/processing_unit.hpp>

namespace arcticdb {
void ProcessingUnit::apply_filter(
    const util::BitSet& bitset,
    const std::shared_ptr<Store>& store,
    PipelineOptimisation optimisation) {
    auto filter_down_stringpool = optimisation == PipelineOptimisation::MEMORY;

    for (auto& slice_and_key: data_) {
        auto seg = filter_segment(slice_and_key.segment(store),
                                  bitset,
                                  filter_down_stringpool);
        if(!seg.is_null()) {
            slice_and_key.slice_.adjust_rows(seg.row_count());
            slice_and_key.slice_.adjust_columns(seg.descriptor().field_count() - seg.descriptor().index().field_count());
        } else {
            slice_and_key.slice_.adjust_rows(0u);
            slice_and_key.slice_.adjust_columns(0u);
        }
        slice_and_key.segment_ = std::move(seg);
    }
}

// Inclusive of start_row, exclusive of end_row
void ProcessingUnit::truncate(size_t start_row, size_t end_row, const std::shared_ptr<Store>& store) {
    for (auto& slice_and_key: data_) {
        auto seg = truncate_segment(slice_and_key.segment(store), start_row, end_row);
        if(!seg.is_null()) {
            slice_and_key.slice_.adjust_rows(seg.row_count());
            slice_and_key.slice_.adjust_columns(seg.descriptor().field_count() - seg.descriptor().index().field_count());
        } else {
            slice_and_key.slice_.adjust_rows(0u);
            slice_and_key.slice_.adjust_columns(0u);
        }
        slice_and_key.segment_ = std::move(seg);
    }
}

VariantData ProcessingUnit::get(const VariantNode &name, const std::shared_ptr<Store> &store) {
    return util::variant_match(name,
        [&](const ColumnName &column_name) {
        for (auto &slice_and_key: data_) {
            slice_and_key.segment(store).init_column_map();
            if (auto opt_idx = slice_and_key.segment(store).column_index(column_name.value)) {
                return VariantData(ColumnWithStrings(
                        slice_and_key.segment(store).column_ptr(
                        position_t(position_t(opt_idx.value()))),
                        slice_and_key.segment(store).string_pool_ptr()));
            }
        }
        // Try multi-index column names
        std::string multi_index_column_name = fmt::format("__idx__{}",
                                                          column_name.value);
        for (auto &slice_and_key: data_) {
            if (auto opt_idx = slice_and_key.segment(store).column_index(multi_index_column_name)) {
                return VariantData(ColumnWithStrings(
                        slice_and_key.segment(store).column_ptr(
                        position_t(opt_idx.value())),
                        slice_and_key.segment(store).string_pool_ptr()));
            }
        }

        if (expression_context_ && !expression_context_->dynamic_schema_) {
            internal::raise<ErrorCode::E_ASSERTION_FAILURE>("Column {} not found in {}",
                                                            column_name,
                                                            data_[0].segment(store).descriptor());
        } else {
            log::version().debug("Column {} not found in {}", column_name, data_[0].segment(store).descriptor());
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
            auto data = expr->compute(self(), store);
            computed_data_.try_emplace(expression_name.value, data);
            return data;
        }
        },
        [&]([[maybe_unused]] const std::monostate &unused) -> VariantData {
        util::raise_rte("ProcessingUnit::get called with monostate VariantNode");
    }
    );
    util::raise_rte("Unexpected value, or expression");
}

} //namespace arcticdb