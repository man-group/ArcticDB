/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */
#include <arcticdb/processing/test/benchmark_common.hpp>
#include <arcticdb/processing/operation_dispatch_binary.hpp>
#include <arcticdb/util/regex_filter.hpp>

using namespace arcticdb;

// run like: --benchmark_time_unit=ms --benchmark_filter=.* --benchmark_min_time=5x

static void BM_regex_match(benchmark::State& state) {
    const auto num_rows = static_cast<size_t>(state.range(0));
    const auto left = generate_string_dense_column(
            num_rows, state.range(1), state.range(2) ? DataType::UTF_DYNAMIC64 : DataType::UTF_FIXED64
    );
    const auto right = util::RegexGeneric(".*");
    for (auto _ : state) {
        binary_comparator(left, right, RegexMatchOperator{});
    }
}

BENCHMARK(BM_regex_match)
        ->Args({100'000, 1'000, true})
        ->Args({100'000, 1'000, false})
        ->Args({100'000, 10'000, true})
        ->Args({100'000, 10'000, false});