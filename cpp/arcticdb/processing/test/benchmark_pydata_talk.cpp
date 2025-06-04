
#include <memory>
#include <random>

#include <benchmark/benchmark.h>

#include <arcticdb/column_store/column.hpp>
#include <arcticdb/column_store/string_pool.hpp>
#include <arcticdb/processing/operation_dispatch_binary.hpp>

using namespace arcticdb;

// run like: --benchmark_time_unit=ms --benchmark_filter=.* --benchmark_min_time=5x

std::random_device rd;
std::mt19937 gen(rd());

util::BitSet generate_bitset_random(const size_t num_rows, const int dense_percentage) {
    util::BitSet bitset;
    bitset.resize(num_rows);
    util::BitSet::bulk_insert_iterator inserter(bitset);
    std::uniform_int_distribution<> dis(1, 100);
    for (size_t idx = 0; idx < num_rows; ++idx) {
        if (dis(gen) <= dense_percentage) {
            inserter = idx;
        }
    }
    inserter.flush();
    return bitset;
}

ColumnWithStrings generate_numeric_dense_column(const size_t num_rows) {
    std::vector<int64_t> data;
    data.reserve(num_rows);
    std::uniform_int_distribution<int64_t> dis(0, 99);
    for (size_t idx = 0; idx < num_rows; ++idx) {
        data.emplace_back(dis(gen));
    }
    Column col(make_scalar_type(DataType::INT64), num_rows, AllocationType::PRESIZED, Sparsity::NOT_PERMITTED);
    memcpy(col.ptr(), data.data(), num_rows * sizeof(int64_t));
    col.set_row_data(num_rows - 1);
    return {std::move(col), {}, ""};
}

//ColumnWithStrings generate_numeric_sparse_column(const size_t num_rows, const int dense_percentage) {
//    Column col(make_scalar_type(DataType::INT64), 0, AllocationType::DYNAMIC, Sparsity::PERMITTED);
//    std::uniform_int_distribution<int64_t> dis(0, 99);
//    for (size_t idx = 0; idx < num_rows; ++idx) {
//        if (dis(gen) < dense_percentage) {
//            col.set_scalar<int64_t>(static_cast<ssize_t>(idx), dis(gen));
//        }
//    }
//    col.set_row_data(num_rows - 1);
//    return {std::move(col), {}, ""};
//}

ColumnWithStrings generate_numeric_sparse_column(const size_t num_rows, const int dense_percentage) {
    auto sparse_map = generate_bitset_random(num_rows, dense_percentage);
    std::vector<int64_t> data;
    auto num_values = sparse_map.count();
    data.reserve(num_values);
    std::uniform_int_distribution<int64_t> dis(0, 99);
    for (size_t idx = 0; idx < num_values; ++idx) {
        data.emplace_back(dis(gen));
    }
    Column col(make_scalar_type(DataType::INT64), num_values, AllocationType::PRESIZED, Sparsity::PERMITTED);
    memcpy(col.ptr(), data.data(), num_values * sizeof(int64_t));
    col.set_sparse_map(std::move(sparse_map));
    col.set_row_data(num_rows - 1);
    return {std::move(col), {}, ""};
}

static void BM_single_column_projection(benchmark::State& state) {
    const auto dense_percentage = state.range(0);
    std::optional<ColumnWithStrings> col;
    if (dense_percentage == 100) {
        col.emplace(generate_numeric_dense_column(100'000'000));
    } else {
        col.emplace(generate_numeric_sparse_column(100'000'000, dense_percentage));
    }
    const auto val = construct_value<int64_t>(42);
    for (auto _ : state) {
        binary_operator(*col, val, PlusOperator{});
    }
}

static void BM_two_column_projection(benchmark::State& state) {
    const auto dense_percentage = state.range(0);
    std::optional<ColumnWithStrings> col1;
    std::optional<ColumnWithStrings> col2;
    if (dense_percentage == 100) {
        col1.emplace(generate_numeric_dense_column(100'000'000));
        col2.emplace(generate_numeric_dense_column(100'000'000));
    } else {
        col1.emplace(generate_numeric_sparse_column(100'000'000, dense_percentage));
        col2.emplace(generate_numeric_sparse_column(100'000'000, dense_percentage));
    }
    for (auto _ : state) {
        binary_operator(*col1, *col2, PlusOperator{});
    }
}

static void BM_single_column_aggregation(benchmark::State& state) {
    const auto dense_percentage = state.range(0);
    std::optional<ColumnWithStrings> col;
    if (dense_percentage == 100) {
        col.emplace(generate_numeric_dense_column(100'000'000));
    } else {
        col.emplace(generate_numeric_sparse_column(100'000'000, dense_percentage));
    }
    for (auto _ : state) {
        auto col_data = col->column_->data();
        auto begin_it = col_data.cbegin<TypeDescriptorTag<DataTypeTag<DataType::INT64>, DimensionTag<Dimension::Dim0>>>();
        auto end_it = col_data.cend<TypeDescriptorTag<DataTypeTag<DataType::INT64>, DimensionTag<Dimension::Dim0>>>();
        std::accumulate(begin_it, end_it, int64_t(0));
    }
}

//BENCHMARK(BM_single_column_projection)->Args({100});
BENCHMARK(BM_single_column_projection)->Args({100})->Args({99})->Args({90})->Args({50})->Args({10})->Args({1});
BENCHMARK(BM_two_column_projection)->Args({100})->Args({99})->Args({90})->Args({50})->Args({10})->Args({1});
//BENCHMARK(BM_two_column_projection)->Args({10});
BENCHMARK(BM_single_column_aggregation)->Args({100})->Args({99})->Args({90})->Args({50})->Args({10})->Args({1});
