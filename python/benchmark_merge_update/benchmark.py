import arcticdb
import json
import pandas as pd
import polars as pl
import numpy as np
import subprocess
import benchmarks.seaweed_utils
import time
import random
import statistics
from arcticdb.version_store.library import MergeStrategy
from arcticdb.util.test import random_strings_of_length
from arcticdb_ext.tracing import TracingSession, TracingSessionConfig

SEAWEED_PATH = "/home/vasil/Documents/source/ArcticDB/build_tooling/start_seaweed.sh"
ROWS_PER_SEGMENT = 100_000


def float_col_with_nan(rows, num_sparse_rows, rng, low):
    col = rng.uniform(low, low + 1, rows)
    col[rng.choice(rows, size=num_sparse_rows, replace=False)] = np.nan
    return col


def string_col_with_none(rows, num_sparse_rows, rng, pool):
    col = rng.choice(pool, rows)
    col[rng.choice(rows, size=num_sparse_rows, replace=False)] = None
    return col


def generate_columns(rows, cols, dtype, seed, float_low=0, string_prefix=""):
    # Column i draws from its own generator seeded with [seed, i]
    rngs = [np.random.default_rng([seed, i]) for i in range(cols)]
    if dtype == "float64":
        return {f"col_{i}": float_col_with_nan(rows, (rows // 10) * i, rngs[i], float_low) for i in range(cols)}
    elif dtype == "object":
        random.seed(seed)
        # Sorted because random_strings_of_length dedups through a set whose order depends on PYTHONHASHSEED
        pool = np.array([string_prefix + s for s in sorted(random_strings_of_length(10_000, 10, True))], dtype=object)
        return {f"col_{i}": string_col_with_none(rows, (rows // 10) * i, rngs[i], pool) for i in range(cols)}
    else:
        raise ValueError(f"Unknown dtype: {dtype} used to generate a column")


def generate_target(index, cols, dtype):
    return pd.DataFrame(generate_columns(len(index), cols, dtype, seed=42), index=index)


def generate_target_non_repeated_index(rows, cols, dtype):
    return generate_target(pd.date_range(pd.Timestamp(0), freq="1ms", periods=rows, name="ts"), cols, dtype)


def generate_target_repeated_index_one_value_per_slice(rows, cols, dtype):
    index = pd.DatetimeIndex(np.repeat(np.arange(rows // ROWS_PER_SEGMENT), ROWS_PER_SEGMENT), name="ts")
    return generate_target(index, cols, dtype)


def generate_target_repeated_index_one_value(rows, cols, dtype):
    return generate_target(pd.DatetimeIndex(np.zeros(rows, dtype="int64"), name="ts"), cols, dtype)


def generate_source(target, source_rows, num_matched_rows, dtype, on):
    rng = np.random.default_rng(24)
    matched_rows = rng.choice(len(target), size=num_matched_rows, replace=False)
    matched_source = target.iloc[matched_rows]
    if dtype == "object":
        # Target string rows can be equal on the on columns, so copies of two such rows would match the same target rows
        matched_source = matched_source[~matched_source.reset_index().duplicated(subset=["ts"] + on).to_numpy()]
    num_unmatched_rows = source_rows - num_matched_rows
    # Target floats are in [0, 1) and target strings never contain "_" so unmatched rows cannot match
    unmatched_rows = pd.DataFrame(
        generate_columns(num_unmatched_rows, len(target.columns), dtype, seed=24, float_low=1, string_prefix="_"),
        index=pd.DatetimeIndex(
            rng.integers(target.index[0].value, target.index[-1].value, size=num_unmatched_rows, endpoint=True),
            name="ts",
        ),
    )
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


def count_matched_source_rows(target, source, on):
    # pandas merge treats NaN/None as equal to each other, same as match_na=True
    keys = ["ts"] + on
    target_keys = target.reset_index()[keys].drop_duplicates()
    joined = source.reset_index()[keys].merge(target_keys, on=keys, how="left", indicator=True)
    return int((joined["_merge"] == "both").sum())


def benchmark(seaweed, target, source, on, strategy, repeats, trace_path):
    num_matched = count_matched_source_rows(target, source, on)
    print(
        json.dumps(
            {
                "tag": "merge_data",
                "target_rows": len(target),
                "source_rows": len(source),
                "matched_source_rows": num_matched,
                "unmatched_source_rows": len(source) - num_matched,
            }
        ),
        flush=True,
    )
    print("========================================================")
    print(f"Start processing for {trace_path}")
    config = TracingSessionConfig()
    config.buffer_size_kb = 256 * 1024
    session = TracingSession(config)
    session.start()
    bucket_name = "benchmark-bucket"
    uri = seaweed.arctic_uri(bucket_name)
    times = []
    for run_idx in range(repeats + 1):
        reset_bucket(seaweed, bucket_name)
        ac = arcticdb.Arctic(uri)
        lib = ac.get_library("benchmark_lib", create_if_missing=True)
        current_time = run_single(lib, target, source, on, strategy, run_idx)
        if run_idx != 0:
            times.append(current_time)
    reset_bucket(seaweed, bucket_name)
    stats_ns = generate_stats(times)
    stats_s = stats_to_s(stats_ns)
    session.stop_and_flush(trace_path)
    print(stats_ns)
    print(stats_s)
    print(f"End processing for {trace_path}")
    print("========================================================")


def test_update_insert_random_float_data(seaweed):
    """Test update and insert with float columns data is randomly scattered"""
    num_rows = 10_000_000
    num_cols = 10
    num_source_rows = 6_000_000
    num_matched_rows = 3_000_000
    on = [f"col_{i}" for i in range(5)]
    dtype = "float64"
    strategy = MergeStrategy(matched="update", not_matched_by_target="insert")
    target = generate_target_non_repeated_index(num_rows, num_cols, dtype)
    source = generate_source(target, num_source_rows, num_matched_rows, dtype, on)
    benchmark(seaweed, target, source, on, strategy, 10, "test_update_insert_random_float_data.pftrace")


def test_update_insert_random_string_data(seaweed):
    """Test update and insert with string columns data is randomly scattered"""
    num_rows = 10_000_000
    num_cols = 10
    num_source_rows = 6_000_000
    num_matched_rows = 3_000_000
    dtype = "object"
    on = [f"col_{i}" for i in range(5)]
    strategy = MergeStrategy(matched="update", not_matched_by_target="insert")
    target = generate_target_non_repeated_index(num_rows, num_cols, dtype)
    source = generate_source(target, num_source_rows, num_matched_rows, dtype, on)
    benchmark(seaweed, target, source, on, strategy, 10, "test_update_insert_random_string_data.pftrace")


def test_update_random_float_data(seaweed):
    """Test update with float columns data is randomly scattered"""
    num_rows = 10_000_000
    num_cols = 10
    num_source_rows = 6_000_000
    on = [f"col_{i}" for i in range(5)]
    dtype = "float64"
    strategy = MergeStrategy(matched="update", not_matched_by_target="do_nothing")
    target = generate_target_non_repeated_index(num_rows, num_cols, dtype)
    source = generate_source(target, num_source_rows, num_source_rows, dtype, on)
    benchmark(seaweed, target, source, on, strategy, 10, "test_update_random_float_data.pftrace")


def test_update_random_string_data(seaweed):
    """Update with string columns"""
    num_rows = 10_000_000
    num_cols = 10
    num_source_rows = 6_000_000
    on = [f"col_{i}" for i in range(5)]
    strategy = MergeStrategy(matched="update", not_matched_by_target="do_nothing")
    dtype = "object"
    target = generate_target_non_repeated_index(num_rows, num_cols, dtype)
    source = generate_source(target, num_source_rows, num_source_rows, dtype, on)
    benchmark(seaweed, target, source, on, strategy, 10, "test_update_random_string_data.pftrace")


def test_one_index_value_per_segment_float(seaweed):
    num_rows = 10_000_000
    num_cols = 10
    num_source_rows = 6_000_000
    num_matched_rows = 3_000_000
    dtype = "float64"
    on = [f"col_{i}" for i in range(5)]
    strategy = MergeStrategy(matched="update", not_matched_by_target="insert")
    target = generate_target_repeated_index_one_value_per_slice(num_rows, num_cols, dtype)
    source = generate_source(target, num_source_rows, num_matched_rows, dtype, on)
    benchmark(seaweed, target, source, on, strategy, 10, "test_one_index_value_per_segment_float.pftrace")


def test_one_index_value_per_segment_string(seaweed):
    num_rows = 10_000_000
    num_cols = 10
    num_source_rows = 6_000_000
    num_matched_rows = 3_000_000
    dtype = "object"
    on = [f"col_{i}" for i in range(5)]
    strategy = MergeStrategy(matched="update", not_matched_by_target="insert")
    target = generate_target_repeated_index_one_value_per_slice(num_rows, num_cols, dtype)
    source = generate_source(target, num_source_rows, num_matched_rows, dtype, on)
    benchmark(seaweed, target, source, on, strategy, 10, "test_one_index_value_per_segment_string.pftrace")


def test_one_index_value_float(seaweed):
    num_rows = 1_000_000
    num_cols = 10
    num_source_rows = 600_000
    num_matched_rows = 300_000
    dtype = "float64"
    on = [f"col_{i}" for i in range(5)]
    strategy = MergeStrategy(matched="update", not_matched_by_target="insert")
    target = generate_target_repeated_index_one_value(num_rows, num_cols, dtype)
    source = generate_source(target, num_source_rows, num_matched_rows, dtype, on)
    benchmark(seaweed, target, source, on, strategy, 10, "test_one_index_value_float.pftrace")


def test_one_index_value_string(seaweed):
    num_rows = 1_000_000
    num_cols = 10
    num_source_rows = 600_000
    num_matched_rows = 300_000
    dtype = "object"
    on = [f"col_{i}" for i in range(5)]
    strategy = MergeStrategy(matched="update", not_matched_by_target="insert")
    target = generate_target_repeated_index_one_value(num_rows, num_cols, dtype)
    source = generate_source(target, num_source_rows, num_matched_rows, dtype, on)
    benchmark(seaweed, target, source, on, strategy, 10, "test_one_index_value_string.pftrace")


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
    test_update_insert_random_float_data(seaweed)
    test_update_insert_random_string_data(seaweed)
    test_update_random_float_data(seaweed)
    test_update_random_string_data(seaweed)
    test_one_index_value_per_segment_float(seaweed)
    test_one_index_value_per_segment_string(seaweed)
    test_one_index_value_float(seaweed)
    test_one_index_value_string(seaweed)
    stop_seaweed(SEAWEED_PATH)


if __name__ == "__main__":
    main()
