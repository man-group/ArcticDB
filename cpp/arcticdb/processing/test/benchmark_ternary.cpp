


/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/processing/operation_dispatch_ternary.hpp>
#include <arcticdb/processing/test/benchmark_common.hpp>

using namespace arcticdb;

// run like: --benchmark_time_unit=ms --benchmark_filter=.* --benchmark_min_time=5x

static std::random_device rd;
static std::mt19937 gen(rd());

util::BitSet generate_bitset(const size_t num_rows) {
    util::BitSet bitset;
    bitset.resize(num_rows);
    util::BitSet::bulk_insert_iterator inserter(bitset);
    std::uniform_int_distribution<> dis(0, 1);
    for (size_t idx = 0; idx < num_rows; ++idx) {
        if (dis(gen) == 0) {
            inserter = idx;
        }
    }
    inserter.flush();
    return bitset;
}

Value generate_numeric_value() {
    std::uniform_int_distribution<int64_t> dis(std::numeric_limits<int64_t>::lowest(), std::numeric_limits<int64_t>::max());
    return construct_value<int64_t>(dis(gen));
}

std::string generate_string() {
    static const std::string characters =
            "0123456789"
            "abcdefghijklmnopqrstuvwxyz"
            "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
    std::uniform_int_distribution<std::string::size_type > dis(0, characters.size() - 1);
    std::string res;
    for (size_t idx = 0; idx < 10; ++idx) {
        res += characters.at(dis(gen));
    }
    return res;
}

Value generate_string_value() {
    return construct_string_value(generate_string());
}


ColumnWithStrings generate_numeric_dense_column(const size_t num_rows) {
    std::vector<int64_t> data;
    data.reserve(num_rows);
    std::uniform_int_distribution<int64_t> dis(std::numeric_limits<int64_t>::lowest(), std::numeric_limits<int64_t>::max());
    for (size_t idx = 0; idx < num_rows; ++idx) {
        data.emplace_back(dis(gen));
    }
    Column col(make_scalar_type(DataType::INT64), num_rows, AllocationType::PRESIZED, Sparsity::NOT_PERMITTED);
    memcpy(col.ptr(), data.data(), num_rows * sizeof(int64_t));
    col.set_row_data(num_rows - 1);
    return {std::move(col), {}, ""};
}

ColumnWithStrings generate_string_dense_column(const size_t num_rows, const size_t unique_strings) {
    auto string_pool = std::make_shared<StringPool>();
    std::vector<entity::position_t> offsets;
    offsets.reserve(unique_strings);
    for (size_t _ = 0; _ < unique_strings; _++) {
        auto str = generate_string();
        offsets.emplace_back(string_pool->get(str, false).offset());
    }

    std::vector<int64_t> data;
    data.reserve(num_rows);
    std::uniform_int_distribution<uint64_t> dis(0, unique_strings - 1);
    for (size_t idx = 0; idx < num_rows; ++idx) {
        data.emplace_back(offsets.at(dis(gen)));
    }
    Column col(make_scalar_type(DataType::UTF_DYNAMIC64), num_rows, AllocationType::PRESIZED, Sparsity::NOT_PERMITTED);
    memcpy(col.ptr(), data.data(), num_rows * sizeof(int64_t));
    col.set_row_data(num_rows - 1);
    return {std::move(col), string_pool, ""};
}

ColumnWithStrings generate_numeric_sparse_column(const size_t num_rows) {
    Column col(make_scalar_type(DataType::INT64), 0, AllocationType::DYNAMIC, Sparsity::PERMITTED);
    std::uniform_int_distribution<int64_t> dis(std::numeric_limits<int64_t>::lowest(), std::numeric_limits<int64_t>::max());
    for (size_t idx = 0; idx < num_rows; ++idx) {
        if (dis(gen) < 0) {
            col.set_scalar<int64_t>(static_cast<ssize_t>(idx), dis(gen));
        }
    }
    col.set_row_data(num_rows - 1);
    return {std::move(col), {}, ""};
}

ColumnWithStrings generate_string_sparse_column(const size_t num_rows, const size_t unique_strings) {
    Column col(make_scalar_type(DataType::UTF_DYNAMIC64), 0, AllocationType::DYNAMIC, Sparsity::PERMITTED);
    auto string_pool = std::make_shared<StringPool>();
    std::vector<entity::position_t> offsets;
    offsets.reserve(unique_strings);
    for (size_t _ = 0; _ < unique_strings; _++) {
        auto str = generate_string();
        offsets.emplace_back(string_pool->get(str, false).offset());
    }
    std::uniform_int_distribution<uint64_t> dis(0, unique_strings - 1);
    for (size_t idx = 0; idx < num_rows; ++idx) {
        if (dis(gen) < unique_strings / 2) {
            col.set_scalar<int64_t>(static_cast<ssize_t>(idx), offsets.at(dis(gen)));
        }
    }
    col.set_row_data(num_rows - 1);
    return {std::move(col), string_pool, ""};
}

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
BENCHMARK(BM_ternary_numeric_dense_col_sparse_col)
        ->Args({100'000, true})
        ->Args({100'000, false});
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
BENCHMARK(BM_ternary_numeric_dense_col_val)
        ->Args({100'000, true})
        ->Args({100'000, false});
BENCHMARK(BM_ternary_numeric_sparse_col_val)
        ->Args({100'000, true})
        ->Args({100'000, false});
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
BENCHMARK(BM_ternary_numeric_dense_col_empty)
        ->Args({100'000, true})
        ->Args({100'000, false});
BENCHMARK(BM_ternary_numeric_sparse_col_empty)
        ->Args({100'000, true})
        ->Args({100'000, false});
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
BENCHMARK(BM_ternary_numeric_val_empty)
        ->Args({100'000, true})
        ->Args({100'000, false});
BENCHMARK(BM_ternary_string_val_empty)
        ->Args({100'000, true})
        ->Args({100'000, false});
BENCHMARK(BM_ternary_bool_bool)
        ->Args({100'000, true, true})
        ->Args({100'000, true, false})
        ->Args({100'000, false, true})
        ->Args({100'000, false, false});
