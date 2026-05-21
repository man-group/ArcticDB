/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <random>

#include <benchmark/benchmark.h>
#include <arcticdb/column_store/column_reslicer.hpp>

using namespace arcticdb;

// run like: --benchmark_time_unit=ms --benchmark_filter=.* --benchmark_min_time=5x

static std::random_device rd;
static std::mt19937 gen(rd());

template<typename T>
static void BM_column_reslicer_combine_dense_numeric_same_type(benchmark::State& state) {
    auto data_type = data_type_from_raw_type<T>();
    auto num_input_columns = state.range(0);
    auto rows_per_input_column = state.range(1);
    auto total_rows = static_cast<uint64_t>(num_input_columns * rows_per_input_column);
    std::vector<std::shared_ptr<Column>> columns;
    auto string_pool = std::make_shared<StringPool>();
    std::vector<StringPool> string_pools(1);
    for (auto idx = 0; idx < num_input_columns; ++idx) {
        auto column = std::make_shared<Column>(
                make_scalar_type(data_type), rows_per_input_column, AllocationType::PRESIZED, Sparsity::NOT_PERMITTED
        );
        // Buffer will be full of uninitialised garbage, which is fine for this benchmark
        column->set_row_data(rows_per_input_column - 1);
        columns.push_back(column);
    }
    for (auto _ : state) {
        state.PauseTiming();
        ColumnReslicer reslicer(num_input_columns, ReslicingInfo{total_rows, total_rows});
        for (auto& column : columns) {
            reslicer.push_back(column, string_pool);
        }
        state.ResumeTiming();
        reslicer.reslice_columns(string_pools);
    }
}

BENCHMARK(BM_column_reslicer_combine_dense_numeric_same_type<uint8_t>)->Args({10, 10'000});
BENCHMARK(BM_column_reslicer_combine_dense_numeric_same_type<uint64_t>)->Args({10, 10'000});

template<typename T>
static void BM_column_reslicer_combine_dense_numeric_same_type_small_appends(benchmark::State& state) {
    auto data_type = data_type_from_raw_type<T>();
    auto num_input_columns = state.range(0);
    auto rows_per_input_column = state.range(1);
    auto total_rows = static_cast<uint64_t>(100'000 + (num_input_columns * rows_per_input_column));
    std::vector<std::shared_ptr<Column>> columns;
    auto string_pool = std::make_shared<StringPool>();
    std::vector<StringPool> string_pools(1);
    auto column = std::make_shared<Column>(
            make_scalar_type(data_type), 100'000, AllocationType::PRESIZED, Sparsity::NOT_PERMITTED
    );
    column->set_row_data(99'999);
    columns.push_back(column);
    for (auto idx = 0; idx < num_input_columns; ++idx) {
        column = std::make_shared<Column>(
                make_scalar_type(data_type), rows_per_input_column, AllocationType::PRESIZED, Sparsity::NOT_PERMITTED
        );
        // Buffer will be full of uninitialised garbage, which is fine for this benchmark
        column->set_row_data(rows_per_input_column - 1);
        columns.push_back(column);
    }
    for (auto _ : state) {
        state.PauseTiming();
        ColumnReslicer reslicer(num_input_columns, ReslicingInfo{total_rows, total_rows});
        for (auto& column : columns) {
            reslicer.push_back(column, string_pool);
        }
        state.ResumeTiming();
        reslicer.reslice_columns(string_pools);
    }
}

BENCHMARK(BM_column_reslicer_combine_dense_numeric_same_type_small_appends<uint8_t>)->Args({100, 1});
BENCHMARK(BM_column_reslicer_combine_dense_numeric_same_type_small_appends<uint64_t>)->Args({100, 1});

template<typename T, typename U>
static void BM_column_reslicer_combine_dense_numeric_type_promotion(benchmark::State& state) {
    auto data_type_1 = data_type_from_raw_type<T>();
    auto data_type_2 = data_type_from_raw_type<U>();
    auto num_input_columns = state.range(0);
    auto rows_per_input_column = state.range(1);
    auto total_rows = static_cast<uint64_t>(num_input_columns * rows_per_input_column);
    std::vector<std::shared_ptr<Column>> columns;
    auto string_pool = std::make_shared<StringPool>();
    std::vector<StringPool> string_pools(1);
    for (auto idx = 0; idx < num_input_columns; ++idx) {
        auto column = std::make_shared<Column>(
                make_scalar_type((idx % 2 == 0) ? data_type_1 : data_type_2),
                rows_per_input_column,
                AllocationType::PRESIZED,
                Sparsity::NOT_PERMITTED
        );
        // Buffer will be full of uninitialised garbage, which is fine for this benchmark
        column->set_row_data(rows_per_input_column - 1);
        columns.push_back(column);
    }
    for (auto _ : state) {
        state.PauseTiming();
        ColumnReslicer reslicer(num_input_columns, ReslicingInfo{total_rows, total_rows});
        for (auto& column : columns) {
            reslicer.push_back(column, string_pool);
        }
        state.ResumeTiming();
        reslicer.reslice_columns(string_pools);
    }
}

BENCHMARK(BM_column_reslicer_combine_dense_numeric_type_promotion<uint8_t, int8_t>)->Args({10, 10'000});
BENCHMARK(BM_column_reslicer_combine_dense_numeric_type_promotion<uint32_t, int32_t>)->Args({10, 10'000});

template<typename T>
static void BM_column_reslicer_split_dense_numeric_same_type(benchmark::State& state) {
    auto data_type = data_type_from_raw_type<T>();
    auto num_input_rows = static_cast<uint64_t>(state.range(0));
    auto num_output_columns = state.range(1);
    auto string_pool = std::make_shared<StringPool>();
    std::vector<StringPool> string_pools(num_output_columns);
    auto column = std::make_shared<Column>(
            make_scalar_type(data_type), num_input_rows, AllocationType::PRESIZED, Sparsity::NOT_PERMITTED
    );
    // Buffer will be full of uninitialised garbage, which is fine for this benchmark
    column->set_row_data(num_input_rows - 1);
    for (auto _ : state) {
        state.PauseTiming();
        ColumnReslicer reslicer(1, ReslicingInfo{num_input_rows, num_input_rows / num_output_columns});
        reslicer.push_back(column, string_pool);
        state.ResumeTiming();
        reslicer.reslice_columns(string_pools);
    }
}

BENCHMARK(BM_column_reslicer_split_dense_numeric_same_type<uint8_t>)->Args({100'000, 10});
BENCHMARK(BM_column_reslicer_split_dense_numeric_same_type<uint64_t>)->Args({100'000, 10});

static void BM_column_reslicer_combine_dense_strings(benchmark::State& state) {
    auto num_input_columns = state.range(0);
    auto rows_per_input_column = state.range(1);
    auto unique_string_count = state.range(2);
    auto total_rows = static_cast<uint64_t>(num_input_columns * rows_per_input_column);
    std::vector<std::shared_ptr<Column>> columns;
    std::vector<std::shared_ptr<StringPool>> input_string_pools;
    std::vector<std::string> unique_strings;
    for (auto idx = 0; idx < unique_string_count; ++idx) {
        unique_strings.emplace_back(std::to_string(idx));
    }
    std::uniform_int_distribution<size_t> dis(0, unique_string_count - 1);
    for (auto idx = 0; idx < num_input_columns; ++idx) {
        StringPool string_pool;
        auto column = std::make_shared<Column>(
                make_scalar_type(DataType::UTF_DYNAMIC64),
                rows_per_input_column,
                AllocationType::PRESIZED,
                Sparsity::NOT_PERMITTED
        );
        column->set_row_data(rows_per_input_column - 1);
        auto ptr = column->ptr_cast<StringPool::offset_t>(0, rows_per_input_column * sizeof(StringPool::offset_t));
        for (auto row = 0; row < rows_per_input_column; ++row, ++ptr) {
            *ptr = string_pool.get(unique_strings.at(dis(gen)), false).offset();
        }
        columns.push_back(column);
        input_string_pools.emplace_back(std::make_shared<StringPool>(std::move(string_pool)));
    }
    for (auto _ : state) {
        state.PauseTiming();
        ColumnReslicer reslicer(num_input_columns, ReslicingInfo{total_rows, total_rows});
        for (auto idx = 0; idx < num_input_columns; ++idx) {
            reslicer.push_back(columns.at(idx), input_string_pools.at(idx));
        }
        std::vector<StringPool> output_string_pools(1);
        state.ResumeTiming();
        reslicer.reslice_columns(output_string_pools);
    }
}

BENCHMARK(BM_column_reslicer_combine_dense_strings)->Args({10, 10'000, 1})->Args({10, 10'000, 100'000});

static void BM_column_reslicer_split_dense_strings(benchmark::State& state) {
    auto num_input_rows = static_cast<uint64_t>(state.range(0));
    auto num_output_columns = state.range(1);
    auto unique_string_count = state.range(2);
    std::vector<std::string> unique_strings;
    for (auto idx = 0; idx < unique_string_count; ++idx) {
        unique_strings.emplace_back(std::to_string(idx));
    }
    std::uniform_int_distribution<size_t> dis(0, unique_string_count - 1);
    auto input_string_pool = std::make_shared<StringPool>();
    auto column = std::make_shared<Column>(
            make_scalar_type(DataType::UTF_DYNAMIC64), num_input_rows, AllocationType::PRESIZED, Sparsity::NOT_PERMITTED
    );
    column->set_row_data(num_input_rows - 1);
    auto ptr = column->ptr_cast<StringPool::offset_t>(0, num_input_rows * sizeof(StringPool::offset_t));
    for (size_t row = 0; row < num_input_rows; ++row, ++ptr) {
        *ptr = input_string_pool->get(unique_strings.at(dis(gen))).offset();
    }
    for (auto _ : state) {
        state.PauseTiming();
        ColumnReslicer reslicer(1, ReslicingInfo{num_input_rows, num_input_rows / num_output_columns});
        reslicer.push_back(column, input_string_pool);
        std::vector<StringPool> output_string_pools(10);
        state.ResumeTiming();
        reslicer.reslice_columns(output_string_pools);
    }
}

BENCHMARK(BM_column_reslicer_split_dense_strings)->Args({100'000, 10, 1});

template<typename T>
static void BM_initialise_output_columns_dense_static_schema(benchmark::State& state) {
    auto data_type = data_type_from_raw_type<T>();
    auto total_rows = static_cast<uint64_t>(state.range(0));
    auto rows_per_segment = static_cast<uint64_t>(state.range(1));
    auto column = std::make_shared<Column>(make_scalar_type(data_type));
    ColumnReslicer reslicer(1, ReslicingInfo{total_rows, rows_per_segment});
    reslicer.push_back(column, std::make_shared<StringPool>());
    for (auto _ : state) {
        reslicer.initialise_output_columns();
    }
}

BENCHMARK(BM_initialise_output_columns_dense_static_schema<uint8_t>)->Args({100'000, 100'000});
BENCHMARK(BM_initialise_output_columns_dense_static_schema<uint64_t>)->Args({100'000, 100'000});

template<typename T>
static void BM_initialise_output_columns_dense_dynamic_schema(benchmark::State& state) {
    auto data_type = data_type_from_raw_type<T>();
    auto total_rows = static_cast<uint64_t>(state.range(0));
    auto rows_per_segment = static_cast<uint64_t>(state.range(1));
    auto column = std::make_shared<Column>(
            make_scalar_type(data_type), total_rows / 4, AllocationType::PRESIZED, Sparsity::NOT_PERMITTED
    );
    column->set_row_data(total_rows / 4 - 1);
    ColumnReslicer reslicer(4, ReslicingInfo{total_rows, rows_per_segment});
    reslicer.push_back(column, std::make_shared<StringPool>());
    reslicer.push_back(total_rows / 4);
    reslicer.push_back(column, std::make_shared<StringPool>());
    reslicer.push_back(total_rows / 4);
    for (auto _ : state) {
        reslicer.initialise_output_columns();
    }
}

BENCHMARK(BM_initialise_output_columns_dense_dynamic_schema<uint8_t>)->Args({100'000, 100'000});
BENCHMARK(BM_initialise_output_columns_dense_dynamic_schema<uint64_t>)->Args({100'000, 100'000});

template<typename T>
static void BM_initialise_output_columns_sparse(benchmark::State& state) {
    auto data_type = data_type_from_raw_type<T>();
    auto total_rows = static_cast<uint64_t>(state.range(0));
    auto rows_per_segment = static_cast<uint64_t>(state.range(1));
    util::BitSet sparse_map(total_rows / 10);
    std::uniform_int_distribution<int> dis(0, 1);
    for (size_t idx = 0; idx < total_rows / 10; ++idx) {
        if (dis(gen) == 0) {
            sparse_map.set_bit(idx);
        }
    }
    auto column = std::make_shared<Column>(
            make_scalar_type(data_type), sparse_map.count(), AllocationType::PRESIZED, Sparsity::PERMITTED
    );
    column->set_row_data(total_rows / 10 - 1);
    column->set_sparse_map(std::move(sparse_map));
    ColumnReslicer reslicer(10, ReslicingInfo{total_rows, rows_per_segment});
    for (size_t _ = 0; _ < 10; ++_) {
        reslicer.push_back(column, std::make_shared<StringPool>());
    }
    for (auto _ : state) {
        reslicer.initialise_output_columns();
    }
}

BENCHMARK(BM_initialise_output_columns_sparse<uint8_t>)->Args({100'000, 100'000});
BENCHMARK(BM_initialise_output_columns_sparse<uint64_t>)->Args({100'000, 100'000});