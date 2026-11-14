/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/entity/types.hpp>

#include <memory>
#include <optional>
#include <string>
#include <string_view>

namespace arcticdb {

class Column;
class StringPool;

/*
 * Wrapper class combining a column with the string pool containing strings for that column
 */
struct ColumnWithStrings {
    std::shared_ptr<Column> column_;
    const std::shared_ptr<StringPool> string_pool_;
    std::string column_name_;

    ColumnWithStrings(std::unique_ptr<Column>&& col, std::string_view col_name);

    ColumnWithStrings(
            std::unique_ptr<Column> column, std::shared_ptr<StringPool> string_pool, std::string_view col_name
    );

    ColumnWithStrings(Column&& col, std::shared_ptr<StringPool> string_pool, std::string_view col_name);

    ColumnWithStrings(
            std::shared_ptr<Column> column, const std::shared_ptr<StringPool>& string_pool,
            std::string_view col_name = ""
    );

    [[nodiscard]] std::optional<std::string_view> string_at_offset(
            entity::position_t offset, bool strip_fixed_width_trailing_nulls = false
    ) const;

    [[nodiscard]] std::optional<size_t> get_fixed_width_string_size() const;
};

} // namespace arcticdb
