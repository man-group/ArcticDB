
#include <memory>
#include <random>

#include <benchmark/benchmark.h>

#include <arcticdb/column_store/column.hpp>
#include <arcticdb/column_store/string_pool.hpp>
#include <arcticdb/processing/operation_dispatch_ternary.hpp>

using namespace arcticdb;

// run like: --benchmark_time_unit=ms --benchmark_filter=.* --benchmark_min_time=5x

std::random_device rd;
std::mt19937 gen(rd());

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

static void BM_ternary_bitset_bitset(benchmark::State& state) {
    const auto num_rows = static_cast<size_t>(state.range(0));
    const auto condition = generate_bitset(num_rows);
    const auto left = generate_bitset(num_rows);
    const auto right = generate_bitset(num_rows);
    for (auto _ : state) {
        ternary_operator(condition, left, right);
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

BENCHMARK(BM_ternary_bitset_bitset)->Args({100'000});
BENCHMARK(BM_ternary_numeric_dense_col_dense_col)->Args({100'000});
BENCHMARK(BM_ternary_numeric_sparse_col_sparse_col)->Args({100'000});
