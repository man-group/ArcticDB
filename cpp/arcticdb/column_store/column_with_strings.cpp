/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/column_store/column_with_strings.hpp>
#include <arcticdb/column_store/column.hpp>
#include <arcticdb/column_store/string_pool.hpp>
#include <arcticdb/util/offset_string.hpp>
#include <arcticdb/util/preconditions.hpp>

namespace arcticdb {

ColumnWithStrings::ColumnWithStrings(std::unique_ptr<Column>&& col, std::string_view col_name) :
    column_(std::move(col)),
    column_name_(col_name) {}

ColumnWithStrings::ColumnWithStrings(
        std::unique_ptr<Column> column, std::shared_ptr<StringPool> string_pool, std::string_view col_name
) :
    column_(std::move(column)),
    string_pool_(std::move(string_pool)),
    column_name_(col_name) {}

ColumnWithStrings::ColumnWithStrings(Column&& col, std::shared_ptr<StringPool> string_pool, std::string_view col_name) :
    column_(std::make_shared<Column>(std::move(col))),
    string_pool_(std::move(string_pool)),
    column_name_(col_name) {}

ColumnWithStrings::ColumnWithStrings(
        std::shared_ptr<Column> column, const std::shared_ptr<StringPool>& string_pool, std::string_view col_name
) :
    column_(std::move(column)),
    string_pool_(string_pool),
    column_name_(col_name) {}

[[nodiscard]] std::optional<std::string_view> ColumnWithStrings::string_at_offset(
        entity::position_t offset, bool strip_fixed_width_trailing_nulls
) const {
    if (UNLIKELY(!column_ || !string_pool_))
        return std::nullopt;
    util::check(!column_->is_inflated(), "Unexpected inflated column in filtering");
    if (!is_a_string(offset)) {
        return std::nullopt;
    }
    std::string_view raw = string_pool_->get_view(offset);
    if (strip_fixed_width_trailing_nulls && is_fixed_string_type(column_->type().data_type())) {
        auto char_width = is_utf_type(slice_value_type(column_->type().data_type())) ? UTF32_WIDTH : ASCII_WIDTH;
        const std::string_view null_char_view("\0\0\0\0", char_width);
        while (!raw.empty() && raw.substr(raw.size() - char_width) == null_char_view) {
            raw.remove_suffix(char_width);
        }
    }
    return raw;
}

[[nodiscard]] std::optional<size_t> ColumnWithStrings::get_fixed_width_string_size() const {
    if (!column_ || !string_pool_)
        return std::nullopt;

    util::check(!column_->is_inflated(), "Unexpected inflated column in filtering");
    for (position_t i = 0; i < column_->row_count(); ++i) {
        auto offset = column_->scalar_at<entity::position_t>(i);
        if (offset != std::nullopt) {
            std::string_view raw = string_pool_->get_view(*offset);
            return raw.size();
        }
    }
    return std::nullopt;
}

} // namespace arcticdb
