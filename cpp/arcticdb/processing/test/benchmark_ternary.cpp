/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#include <random>

#include <benchmark/benchmark.h>

#include <arcticdb/column_store/column.hpp>
#include <arcticdb/processing/operation_dispatch_ternary.hpp>

using namespace arcticdb;

// run like: --benchmark_time_unit=ms --benchmark_filter=.* --benchmark_min_time=5x

static void BM_ternary_numeric_col_col_dense(benchmark::State& state){
    const auto num_rows = static_cast<size_t>(state.range(0));
    util::BitSet condition;
    condition.resize(num_rows);

    std::vector<int64_t> left_data;
    std::vector<int64_t> right_data;
    left_data.reserve(num_rows);
    right_data.reserve(num_rows);

    std::random_device rd;
    std::mt19937 gen(rd());
    // uniform_int_distribution (validly) undefined for int8_t in MSVC, hence the casting backwards and forwards
    std::uniform_int_distribution<int64_t> dis(std::numeric_limits<int64_t>::lowest(), std::numeric_limits<int64_t>::max());
    for (size_t idx = 0; idx < num_rows; ++idx) {
        left_data.emplace_back(dis(gen));
        right_data.emplace_back(dis(gen));
        if (dis(gen) < std::numeric_limits<int64_t>::max() / 2) {
            condition.set_bit(idx);
        }
    }

    Column left_col(make_scalar_type(DataType::INT64), num_rows, AllocationType::PRESIZED, Sparsity::NOT_PERMITTED);
    Column right_col(make_scalar_type(DataType::INT64), num_rows, AllocationType::PRESIZED, Sparsity::NOT_PERMITTED);
    memcpy(left_col.ptr(), left_data.data(), num_rows * sizeof(int64_t));
    memcpy(right_col.ptr(), right_data.data(), num_rows * sizeof(int64_t));
    left_col.set_row_data(num_rows - 1);
    right_col.set_row_data(num_rows - 1);

    ColumnWithStrings left(std::move(left_col), {}, "");
    ColumnWithStrings right(std::move(right_col), {}, "");

    for (auto _ : state) {
        ternary_operator(condition, left, right);
    }
}

BENCHMARK(BM_ternary_numeric_col_col_dense)->Args({100'000});
