/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */
#pragma once

#include <memory>
#include <random>

#include <benchmark/benchmark.h>

#include <arcticdb/column_store/column.hpp>
#include <arcticdb/column_store/string_pool.hpp>
#include <arcticdb/processing/expression_node.hpp>

namespace arcticdb {

std::string generate_string();
ColumnWithStrings generate_string_dense_column(
        const size_t num_rows, const size_t unique_strings, DataType dt = DataType::UTF_DYNAMIC64
);
ColumnWithStrings generate_numeric_sparse_column(const size_t num_rows);
util::BitSet generate_bitset(const size_t num_rows);
Value generate_numeric_value();
Value generate_string_value();
ColumnWithStrings generate_numeric_dense_column(const size_t num_rows);
ColumnWithStrings generate_string_sparse_column(const size_t num_rows, const size_t unique_strings);
} // namespace arcticdb