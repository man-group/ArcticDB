/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/processing/operation_dispatch_ternary.hpp>
#include <arcticdb/processing/test/benchmark_common.hpp>

using namespace arcticdb;

// run like: --benchmark_time_unit=ms --benchmark_filter=.* --benchmark_min_time=5x
static void BM_ternary_bitset_bitset(benchmark::State& state) {
    const auto num_rows = static_cast<size_t>(state.range(0));
    const auto condition = generate_bitset(num_rows);
    const auto left = generate_bitset(num_rows);
    const auto right = generate_bitset(num_rows);
    for (auto _ : state) {
        ternary_operator(condition, left, right);
    }
}

static void BM_ternary_bitset_bool(benchmark::State& state) {
    const auto num_rows = static_cast<size_t>(state.range(0));
    const bool right = state.range(1);
    const bool arguments_reversed = state.range(2);
    const auto condition = generate_bitset(num_rows);
    const auto left = generate_bitset(num_rows);
    for (auto _ : state) {
        if (arguments_reversed) {
            ternary_operator<true>(condition, left, right);
        } else {
            ternary_operator<false>(condition, left, right);
        }
    }
}

static void BM_ternary_numeric_dense_col_dense_col(benchmark::State& state) {
    const auto num_rows = static_cast<size_t>(state.range(0));
    const auto condition = generate_bitset(num_rows);
    const auto left = generate_numeric_dense_column(num_rows);
    const auto right = generate_numeric_dense_column(num_rows);
    for (auto _ : state) {
        ternary_operator(condition, left, right);
    }
}

static void BM_ternary_numeric_sparse_col_sparse_col(benchmark::State& state) {
    const auto num_rows = static_cast<size_t>(state.range(0));
    const auto condition = generate_bitset(num_rows);
    const auto left = generate_numeric_sparse_column(num_rows);
    const auto right = generate_numeric_sparse_column(num_rows);
    for (auto _ : state) {
        ternary_operator(condition, left, right);
    }
}

static void BM_ternary_numeric_dense_col_sparse_col(benchmark::State& state) {
    const auto num_rows = static_cast<size_t>(state.range(0));
    const bool arguments_reversed = state.range(1);
    const auto condition = generate_bitset(num_rows);
    const auto dense = generate_numeric_dense_column(num_rows);
    const auto sparse = generate_numeric_sparse_column(num_rows);
    for (auto _ : state) {
        if (arguments_reversed) {
            ternary_operator(condition, sparse, dense);
        } else {
            ternary_operator(condition, dense, sparse);
        }
    }
}

static void BM_ternary_string_dense_col_dense_col(benchmark::State& state) {
    const auto num_rows = static_cast<size_t>(state.range(0));
    const auto num_unique_strings = static_cast<size_t>(state.range(1));
    const bool same_string_col = state.range(2);
    const auto condition = generate_bitset(num_rows);
    const auto left = generate_string_dense_column(num_rows, num_unique_strings);
    const auto tmp = generate_string_dense_column(num_rows, num_unique_strings);
    std::optional<ColumnWithStrings> right;
    if (same_string_col) {
        right.emplace(tmp.column_, left.string_pool_, "");
    } else {
        right.emplace(std::move(tmp));
    }
    for (auto _ : state) {
        ternary_operator(condition, left, *right);
    }
}

static void BM_ternary_string_sparse_col_sparse_col(benchmark::State& state) {
    const auto num_rows = static_cast<size_t>(state.range(0));
    const auto num_unique_strings = static_cast<size_t>(state.range(1));
    const bool same_string_col = state.range(2);
    const auto condition = generate_bitset(num_rows);
    const auto left = generate_string_sparse_column(num_rows, num_unique_strings);
    const auto tmp = generate_string_sparse_column(num_rows, num_unique_strings);
    std::optional<ColumnWithStrings> right;
    if (same_string_col) {
        right.emplace(tmp.column_, left.string_pool_, "");
    } else {
        right.emplace(std::move(tmp));
    }
    for (auto _ : state) {
        ternary_operator(condition, left, *right);
    }
}

static void BM_ternary_string_dense_col_sparse_col(benchmark::State& state) {
    const auto num_rows = static_cast<size_t>(state.range(0));
    const auto num_unique_strings = static_cast<size_t>(state.range(1));
    const bool same_string_col = state.range(2);
    const auto condition = generate_bitset(num_rows);
    const auto left = generate_string_dense_column(num_rows, num_unique_strings);
    const auto tmp = generate_string_sparse_column(num_rows, num_unique_strings);
    std::optional<ColumnWithStrings> right;
    if (same_string_col) {
        right.emplace(tmp.column_, left.string_pool_, "");
    } else {
        right.emplace(std::move(tmp));
    }
    for (auto _ : state) {
        ternary_operator(condition, left, *right);
    }
}

static void BM_ternary_numeric_dense_col_val(benchmark::State& state) {
    const auto num_rows = static_cast<size_t>(state.range(0));
    const bool arguments_reversed = state.range(1);
    const auto condition = generate_bitset(num_rows);
    const auto left = generate_numeric_dense_column(num_rows);
    const auto right = generate_numeric_value();
    for (auto _ : state) {
        if (arguments_reversed) {
            ternary_operator<true>(condition, left, right);
        } else {
            ternary_operator<false>(condition, left, right);
        }
    }
}

static void BM_ternary_numeric_sparse_col_val(benchmark::State& state) {
    const auto num_rows = static_cast<size_t>(state.range(0));
    const bool arguments_reversed = state.range(1);
    const auto condition = generate_bitset(num_rows);
    const auto left = generate_numeric_sparse_column(num_rows);
    const auto right = generate_numeric_value();
    for (auto _ : state) {
        if (arguments_reversed) {
            ternary_operator<true>(condition, left, right);
        } else {
            ternary_operator<false>(condition, left, right);
        }
    }
}

static void BM_ternary_string_dense_col_val(benchmark::State& state) {
    const auto num_rows = static_cast<size_t>(state.range(0));
    const bool arguments_reversed = state.range(1);
    const auto num_unique_strings = static_cast<size_t>(state.range(2));
    const auto condition = generate_bitset(num_rows);
    const auto left = generate_string_dense_column(num_rows, num_unique_strings);
    const auto right = generate_string_value();
    for (auto _ : state) {
        if (arguments_reversed) {
            ternary_operator<true>(condition, left, right);
        } else {
            ternary_operator<false>(condition, left, right);
        }
    }
}

static void BM_ternary_string_sparse_col_val(benchmark::State& state) {
    const auto num_rows = static_cast<size_t>(state.range(0));
    const bool arguments_reversed = state.range(1);
    const auto num_unique_strings = static_cast<size_t>(state.range(2));
    const auto condition = generate_bitset(num_rows);
    const auto left = generate_string_sparse_column(num_rows, num_unique_strings);
    const auto right = generate_string_value();
    for (auto _ : state) {
        if (arguments_reversed) {
            ternary_operator<true>(condition, left, right);
        } else {
            ternary_operator<false>(condition, left, right);
        }
    }
}

static void BM_ternary_numeric_dense_col_empty(benchmark::State& state) {
    const auto num_rows = static_cast<size_t>(state.range(0));
    const bool arguments_reversed = state.range(1);
    const auto condition = generate_bitset(num_rows);
    const auto left = generate_numeric_dense_column(num_rows);
    const EmptyResult right;
    for (auto _ : state) {
        if (arguments_reversed) {
            ternary_operator<true>(condition, left, right);
        } else {
            ternary_operator<false>(condition, left, right);
        }
    }
}

static void BM_ternary_numeric_sparse_col_empty(benchmark::State& state) {
    const auto num_rows = static_cast<size_t>(state.range(0));
    const bool arguments_reversed = state.range(1);
    const auto condition = generate_bitset(num_rows);
    const auto left = generate_numeric_sparse_column(num_rows);
    const EmptyResult right;
    for (auto _ : state) {
        if (arguments_reversed) {
            ternary_operator<true>(condition, left, right);
        } else {
            ternary_operator<false>(condition, left, right);
        }
    }
}

static void BM_ternary_string_dense_col_empty(benchmark::State& state) {
    const auto num_rows = static_cast<size_t>(state.range(0));
    const bool arguments_reversed = state.range(1);
    const auto num_unique_strings = static_cast<size_t>(state.range(2));
    const auto condition = generate_bitset(num_rows);
    const auto left = generate_string_dense_column(num_rows, num_unique_strings);
    const EmptyResult right;
    for (auto _ : state) {
        if (arguments_reversed) {
            ternary_operator<true>(condition, left, right);
        } else {
            ternary_operator<false>(condition, left, right);
        }
    }
}

static void BM_ternary_string_sparse_col_empty(benchmark::State& state) {
    const auto num_rows = static_cast<size_t>(state.range(0));
    const bool arguments_reversed = state.range(1);
    const auto num_unique_strings = static_cast<size_t>(state.range(2));
    const auto condition = generate_bitset(num_rows);
    const auto left = generate_string_sparse_column(num_rows, num_unique_strings);
    const EmptyResult right;
    for (auto _ : state) {
        if (arguments_reversed) {
            ternary_operator<true>(condition, left, right);
        } else {
            ternary_operator<false>(condition, left, right);
        }
    }
}

static void BM_ternary_numeric_val_val(benchmark::State& state) {
    const auto num_rows = static_cast<size_t>(state.range(0));
    const auto condition = generate_bitset(num_rows);
    const auto left = generate_numeric_value();
    const auto right = generate_numeric_value();
    for (auto _ : state) {
        ternary_operator(condition, left, right);
    }
}

static void BM_ternary_string_val_val(benchmark::State& state) {
    const auto num_rows = static_cast<size_t>(state.range(0));
    const auto condition = generate_bitset(num_rows);
    const auto left = generate_string_value();
    const auto right = generate_string_value();
    for (auto _ : state) {
        ternary_operator(condition, left, right);
    }
}

static void BM_ternary_numeric_val_empty(benchmark::State& state) {
    const auto num_rows = static_cast<size_t>(state.range(0));
    const auto arguments_reversed = state.range(1);
    const auto condition = generate_bitset(num_rows);
    const auto left = generate_numeric_value();
    const EmptyResult right;
    for (auto _ : state) {
        if (arguments_reversed) {
            ternary_operator<true>(condition, left, right);
        } else {
            ternary_operator<false>(condition, left, right);
        }
    }
}

static void BM_ternary_string_val_empty(benchmark::State& state) {
    const auto num_rows = static_cast<size_t>(state.range(0));
    const auto arguments_reversed = state.range(1);
    const auto condition = generate_bitset(num_rows);
    const auto left = generate_string_value();
    const EmptyResult right;
    for (auto _ : state) {
        if (arguments_reversed) {
            ternary_operator<true>(condition, left, right);
        } else {
            ternary_operator<false>(condition, left, right);
        }
    }
}

static void BM_ternary_bool_bool(benchmark::State& state) {
    const auto num_rows = static_cast<size_t>(state.range(0));
    const bool left = state.range(1);
    const bool right = state.range(2);
    const auto condition = generate_bitset(num_rows);
    for (auto _ : state) {
        ternary_operator(condition, left, right);
    }
}

BENCHMARK(BM_ternary_bitset_bitset)->Args({100'000});
BENCHMARK(BM_ternary_bitset_bool)
        ->Args({100'000, true, true})
        ->Args({100'000, true, false})
        ->Args({100'000, false, true})
        ->Args({100'000, false, false});
BENCHMARK(BM_ternary_numeric_dense_col_dense_col)->Args({100'000});
BENCHMARK(BM_ternary_numeric_sparse_col_sparse_col)->Args({100'000});
BENCHMARK(BM_ternary_numeric_dense_col_sparse_col)->Args({100'000, true})->Args({100'000, false});
BENCHMARK(BM_ternary_string_dense_col_dense_col)
        ->Args({100'000, 100'000, true})
        ->Args({100'000, 100'000, false})
        ->Args({100'000, 2, true})
        ->Args({100'000, 2, false});
BENCHMARK(BM_ternary_string_sparse_col_sparse_col)
        ->Args({100'000, 100'000, true})
        ->Args({100'000, 100'000, false})
        ->Args({100'000, 2, true})
        ->Args({100'000, 2, false});
BENCHMARK(BM_ternary_string_dense_col_sparse_col)
        ->Args({100'000, 100'000, true})
        ->Args({100'000, 100'000, false})
        ->Args({100'000, 2, true})
        ->Args({100'000, 2, false});
BENCHMARK(BM_ternary_numeric_dense_col_val)->Args({100'000, true})->Args({100'000, false});
BENCHMARK(BM_ternary_numeric_sparse_col_val)->Args({100'000, true})->Args({100'000, false});
BENCHMARK(BM_ternary_string_dense_col_val)
        ->Args({100'000, true, 100'000})
        ->Args({100'000, false, 100'000})
        ->Args({100'000, true, 2})
        ->Args({100'000, false, 2});
BENCHMARK(BM_ternary_string_sparse_col_val)
        ->Args({100'000, true, 100'000})
        ->Args({100'000, false, 100'000})
        ->Args({100'000, true, 2})
        ->Args({100'000, false, 2});
BENCHMARK(BM_ternary_numeric_dense_col_empty)->Args({100'000, true})->Args({100'000, false});
BENCHMARK(BM_ternary_numeric_sparse_col_empty)->Args({100'000, true})->Args({100'000, false});
BENCHMARK(BM_ternary_string_dense_col_empty)
        ->Args({100'000, true, 100'000})
        ->Args({100'000, false, 100'000})
        ->Args({100'000, true, 2})
        ->Args({100'000, false, 2});
BENCHMARK(BM_ternary_string_sparse_col_empty)
        ->Args({100'000, true, 100'000})
        ->Args({100'000, false, 100'000})
        ->Args({100'000, true, 2})
        ->Args({100'000, false, 2});
BENCHMARK(BM_ternary_numeric_val_val)->Args({100'000});
BENCHMARK(BM_ternary_string_val_val)->Args({100'000});
BENCHMARK(BM_ternary_numeric_val_empty)->Args({100'000, true})->Args({100'000, false});
BENCHMARK(BM_ternary_string_val_empty)->Args({100'000, true})->Args({100'000, false});
BENCHMARK(BM_ternary_bool_bool)
        ->Args({100'000, true, true})
        ->Args({100'000, true, false})
        ->Args({100'000, false, true})
        ->Args({100'000, false, false});
