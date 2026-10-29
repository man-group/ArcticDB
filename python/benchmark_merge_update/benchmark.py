import arcticdb
import json
import pandas as pd
import polars as pl
import numpy as np
import subprocess
import benchmarks.seaweed_utils
import time
import statistics
from arcticdb.version_store.library import MergeStrategy
from arcticdb.util.test import random_strings_of_length
from arcticdb_ext.tracing import TracingSession, TracingSessionConfig

SEAWEED_PATH = "/home/vasil/Documents/source/ArcticDB/build_tooling/start_seaweed.sh"


def float_col_with_nan(rows, num_sparse_rows):
    col = np.array(range(rows), dtype="float64")
    rng = np.random.default_rng(42)
    sparse_rows = rng.choice(rows, size=num_sparse_rows, replace=False)
    for sparse in sparse_rows:
        col[sparse] = np.nan
    return col


def string_col_with_none(rows, num_sparse_rows):
    pool = random_strings_of_length(10_000, 10, True)
    rng = np.random.default_rng(7)
    col = rng.choice(pool, rows)
    sparse_rows = rng.choice(rows, size=num_sparse_rows, replace=False)
    for sparse in sparse_rows:
        col[sparse] = np.nan
    return col


def generate_col_with_na(rows, num_sparse_rows, dtype):
    if dtype == "float64":
        return float_col_with_nan(rows, num_sparse_rows)
    elif dtype == "object":
        return string_col_with_none(rows, num_sparse_rows)
    else:
        raise ValueError(f"Unknown dtype: {dtype} used to generate a column")


def generate_target_non_repeated_index(rows, cols, index_start=pd.Timestamp(0), dtype="float64"):
    index = pd.date_range(index_start, freq="1ms", periods=rows, name="ts")
    return pd.DataFrame(
        {f"col_{i}": generate_col_with_na(rows, (rows // 10) * i, dtype) for i in range(cols)}, index=index
    )


def generate_source(target, source_rows, num_matched_rows):
    rng = np.random.default_rng(24)
    matched_rows = rng.choice(len(target), size=num_matched_rows, replace=False)
    matched_source = target.iloc[matched_rows]
    num_rows_to_add = source_rows - num_matched_rows
    unmatched_rows = generate_target_non_repeated_index(num_rows_to_add, len(target.columns))
    # Half a step after a target timestamp, so inserts interleave with the whole target without colliding
    unmatched_positions = np.sort(rng.choice(len(target), size=num_rows_to_add, replace=False))
    unmatched_rows.index = (target.index[unmatched_positions] + pd.Timedelta(microseconds=500)).rename("ts")
    return pd.concat([matched_source, unmatched_rows]).sort_index()


def reset_bucket(seaweed, bucket_name):
    seaweed.delete_bucket(bucket_name)
    seaweed.create_bucket(bucket_name)


def run_single(library, target, source, on, strategy, run_idx):
    library.write("benchmark_sym", target)
    print(json.dumps({"tag": "merge_run", "event": "start", "run": run_idx}), flush=True)
    start = time.perf_counter_ns()
    library.merge("benchmark_sym", source, strategy=strategy, on=on, match_na=True)
    end = time.perf_counter_ns()
    print(json.dumps({"tag": "merge_run", "event": "end", "run": run_idx, "wall_ns": end - start}), flush=True)
    return end - start


def generate_stats(times):
    return {
        "mean": statistics.mean(times),
        "median": statistics.median(times),
        "min": min(times),
        "max": max(times),
        "sd": statistics.stdev(times),
        "data": times,
    }


def stats_to_s(stats):
    res = {}
    for k, v in stats.items():
        if k != "data":
            res[k] = ns_to_s(v)
        else:
            res[k] = [ns_to_s(el) for el in v]
    return res


def ns_to_s(ns):
    return ns * 0.000000001


def benchmark(seaweed, target, source, on, strategy, repeats):
    bucket_name = "benchmark-bucket"
    uri = seaweed.arctic_uri(bucket_name)
    time = []
    for run_idx in range(repeats):
        reset_bucket(seaweed, bucket_name)
        ac = arcticdb.Arctic(uri)
        lib = ac.get_library("benchmark_lib", create_if_missing=True)
        time.append(run_single(lib, target, source, on, strategy, run_idx))
    reset_bucket(seaweed, bucket_name)
    stats_ns = generate_stats(time)
    stats_s = stats_to_s(stats_ns)
    print(stats_ns)
    print(stats_s)


def test_01(seaweed):
    num_rows = 10_0000_000
    num_cols = 10
    num_source_rows = 5_000_001
    num_matched_rows = 2_500_000
    on = [f"col_{i}" for i in range(5)]
    strategy = MergeStrategy(matched="update", not_matched_by_target="insert")
    target = generate_target_non_repeated_index(num_rows, num_cols)
    source = generate_source(target, num_source_rows, num_matched_rows)
    config = TracingSessionConfig()
    config.buffer_size_kb = 256 * 1024
    session = TracingSession(config)
    session.start()
    benchmark(seaweed, target, source, on, strategy, 10)
    session.stop_and_flush("test_01.pftrace")


def test_02(seaweed):
    num_rows = 10_0000_000
    num_cols = 10
    num_source_rows = 5_000_001
    num_matched_rows = 2_500_000
    on = [f"col_{i}" for i in range(5)]
    strategy = MergeStrategy(matched="update", not_matched_by_target="insert")
    target = generate_target_non_repeated_index(num_rows, num_cols)
    source = generate_source(target, num_source_rows, num_matched_rows)
    benchmark(seaweed, target, source, on, strategy, 10)


def start_seaweed(seaweed_path):
    subprocess.run(["bash", seaweed_path, "start"], check=True)


def stop_seaweed(seaweed_path):
    subprocess.run(["bash", seaweed_path, "stop"], check=True)


def get_seaweed():
    stop_seaweed(SEAWEED_PATH)
    start_seaweed(SEAWEED_PATH)
    return benchmarks.seaweed_utils.SeaweedClient()


def main():
    seaweed = get_seaweed()
    test_01(seaweed)
    stop_seaweed(SEAWEED_PATH)


if __name__ == "__main__":
    main()
