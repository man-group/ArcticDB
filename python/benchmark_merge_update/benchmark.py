import arcticdb
import pandas as pd
import polars as pl
import numpy as np
import subprocess
import benchmarks.seaweed_utils
import time
import statistics
from arcticdb.version_store.library import MergeStrategy


def float_col_with_nan(rows, num_sparse_rows):
    col = np.array(range(rows), dtype="float64")
    rng = np.random.default_rng(42)
    sparse_rows = rng.choice(rows, size=num_sparse_rows, replace=False)
    for sparse in sparse_rows:
        col[sparse] = np.nan
    return col


def generate_target_non_repeated_index(rows, cols):
    index = pd.date_range(pd.Timestamp(0), freq="1ms", periods=rows, name="ts")
    return pd.DataFrame({f"col_{i}": float_col_with_nan(rows, (rows // 10) * i) for i in range(cols)}, index=index)


def generate_source(target, source_rows, num_matched_rows):
    rng = np.random.default_rng(24)
    matched_rows = rng.choice(len(target), size=num_matched_rows, replace=False)
    matched_source = target.iloc[matched_rows]
    num_rows_to_add = source_rows - num_matched_rows
    unmatched_rows = generate_target_non_repeated_index(num_rows_to_add, len(target.columns))
    return pd.concat([matched_source, unmatched_rows]).sort_index()


def reset_bucket(seaweed, bucket_name):
    seaweed.delete_bucket(bucket_name)
    seaweed.create_bucket(bucket_name)


def run_single(library, target, source, on, strategy):
    library.write("benchmark_sym", target)
    start = time.time_ns()
    library.merge("benchmark_sym", source, strategy=strategy, on=on)
    end = time.time_ns()
    return end - start


def generate_stats(times):
    return {
        "mean": statistics.mean(times),
        "median": statistics.mean(times),
        "min": min(times),
        "max": max(times),
        "sd": statistics.stdev(times),
        "data": times,
    }


def benchmark(seaweed, target, source, on, strategy, repeats):
    bucket_name = "benchmark_bucket"
    uri = seaweed.arctic_uri(bucket_name)
    time = []
    for _ in range(repeats):
        reset_bucket(seaweed, bucket_name)
        ac = arcticdb.Arctic(uri)
        lib = ac.get_library("benchmark_lib", create_if_missing=True)
        time.append(run_single(lib, target, source, on, strategy))
    reset_bucket(seaweed, bucket_name)
    print(generate_stats(time))


def test_1(seaweed):
    num_rows = 10_0000_000
    num_cols = 10
    num_source_rows = 5_000_001
    num_matched_rows = 2_500_000
    on = [f"col_{i}" for i in range(5)]
    strategy = MergeStrategy(matched="update", not_matched_by_target="insert")
    target = generate_target_non_repeated_index(num_rows, num_cols)
    source = generate_source(target, num_source_rows, num_matched_rows)
    benchmark(seaweed, target, source, on, strategy, 5)


def get_seaweed():
    seaweed_path = "/home/vasil/Documents/source/ArcticDB/build_tooling/start_seaweed.sh"
    subprocess.run(["bash", seaweed_path, "stop"], check=True)
    subprocess.run(["bash", seaweed_path, "start"], check=True)
    return benchmarks.seaweed_utils.SeaweedClient()


def main():
    seaweed = get_seaweed()
    test_1(seaweed)


if __name__ == "__main__":
    main()
