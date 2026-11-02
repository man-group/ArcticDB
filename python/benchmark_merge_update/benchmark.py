import arcticdb
import json
import pandas as pd
import polars as pl
import numpy as np
import os
import pickle
import subprocess
import sys
import benchmarks.seaweed_utils
import time
import random
import statistics
import zipfile
from arcticdb.version_store.library import MergeStrategy
from arcticdb.util.test import random_strings_of_length
from arcticdb_ext.tracing import TracingSession, TracingSessionConfig

SEAWEED_PATH = "/home/vasil/Documents/source/ArcticDB/build_tooling/start_seaweed.sh"
# Kept on disk next to this script: the default /tmp can be a tmpfs, which would hold the data in RAM
SEAWEED_DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "seaweed")
ROWS_PER_SEGMENT = 100_000
# Each run is a separate process; the first (chronologically) run is treated as the warm-up during analysis
RUNS = 11
BUCKET_NAME = "benchmark-bucket"
# Generated test data is cached here so the run processes do not regenerate it
DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")


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
    source = pd.concat([matched_source, unmatched_rows]).sort_index()
    # Concatenating with an empty frame can leave the float block row-major, which merge rejects as Fortran-style
    return pd.DataFrame({c: np.ascontiguousarray(source[c].to_numpy()) for c in source.columns}, index=source.index)


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


def data_path(test_name):
    return os.path.join(DATA_DIR, f"{test_name}.pkl")


def run_trace_name(test_name, run_idx):
    return f"{test_name}_run_{run_idx:02d}.pftrace"


def generate_data(test_name):
    """Generates the test's target and source once and caches them for the run processes."""
    target, source, on, strategy = TESTS[test_name]()
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
    os.makedirs(DATA_DIR, exist_ok=True)
    with open(data_path(test_name), "wb") as f:
        pickle.dump((target, source, on, strategy), f, protocol=pickle.HIGHEST_PROTOCOL)


def run_once(test_name, run_idx):
    """Runs one traced merge of the test in this process and writes its trace to the working directory."""
    with open(data_path(test_name), "rb") as f:
        target, source, on, strategy = pickle.load(f)
    seaweed = benchmarks.seaweed_utils.SeaweedClient()
    reset_bucket(seaweed, BUCKET_NAME)
    lib = arcticdb.Arctic(seaweed.arctic_uri(BUCKET_NAME)).get_library("benchmark_lib", create_if_missing=True)
    config = TracingSessionConfig()
    config.buffer_size_kb = 256 * 1024
    session = TracingSession(config)
    session.start()
    run_single(lib, target, source, on, strategy, run_idx)
    session.stop_and_flush(run_trace_name(test_name, run_idx))
    reset_bucket(seaweed, BUCKET_NAME)


def wall_times_from_log(lines):
    return {record["run"]: record["wall_ns"] for record in map(json.loads, lines) if record.get("event") == "end"}


def benchmark(test_name):
    """Runs each of the RUNS merges in its own process, then zips the per run traces into <test_name>.zip."""
    print("========================================================")
    print(f"Start processing for {test_name}", flush=True)
    subprocess.run([sys.executable, __file__, "generate", test_name], check=True)
    times = []
    for run_idx in range(RUNS):
        result = subprocess.run(
            [sys.executable, __file__, "run", test_name, str(run_idx)], stdout=subprocess.PIPE, text=True
        )
        # Printed before checking the exit code so that a killed run still leaves its output in the log
        sys.stdout.write(result.stdout)
        sys.stdout.flush()
        result.check_returncode()
        merge_run_lines = [line for line in result.stdout.splitlines() if line.startswith('{"tag": "merge_run"')]
        times.append(wall_times_from_log(merge_run_lines)[run_idx])
    traces = [run_trace_name(test_name, run_idx) for run_idx in range(RUNS)]
    with zipfile.ZipFile(f"{test_name}.zip", "w", zipfile.ZIP_DEFLATED) as archive:
        for trace in traces:
            archive.write(trace)
    for trace in traces:
        os.remove(trace)
    os.remove(data_path(test_name))
    stats_ns = generate_stats(times[1:])
    print(stats_ns)
    print(stats_to_s(stats_ns))
    print(f"End processing for {test_name}")
    print("========================================================", flush=True)


def test_update_insert_random_float_data():
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
    return target, source, on, strategy


def test_update_insert_random_string_data():
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
    return target, source, on, strategy


def test_update_random_float_data():
    """Test update with float columns data is randomly scattered"""
    num_rows = 10_000_000
    num_cols = 10
    num_source_rows = 6_000_000
    on = [f"col_{i}" for i in range(5)]
    dtype = "float64"
    strategy = MergeStrategy(matched="update", not_matched_by_target="do_nothing")
    target = generate_target_non_repeated_index(num_rows, num_cols, dtype)
    source = generate_source(target, num_source_rows, num_source_rows, dtype, on)
    return target, source, on, strategy


def test_update_random_string_data():
    """Update with string columns"""
    num_rows = 10_000_000
    num_cols = 10
    num_source_rows = 6_000_000
    on = [f"col_{i}" for i in range(5)]
    strategy = MergeStrategy(matched="update", not_matched_by_target="do_nothing")
    dtype = "object"
    target = generate_target_non_repeated_index(num_rows, num_cols, dtype)
    source = generate_source(target, num_source_rows, num_source_rows, dtype, on)
    return target, source, on, strategy


def test_one_index_value_per_segment_float():
    num_rows = 10_000_000
    num_cols = 10
    num_source_rows = 68_000
    num_matched_rows = 34_000
    dtype = "float64"
    on = [f"col_{i}" for i in range(5)]
    strategy = MergeStrategy(matched="update", not_matched_by_target="insert")
    target = generate_target_repeated_index_one_value_per_slice(num_rows, num_cols, dtype)
    source = generate_source(target, num_source_rows, num_matched_rows, dtype, on)
    return target, source, on, strategy


def test_one_index_value_per_segment_string():
    num_rows = 10_000_000
    num_cols = 10
    num_source_rows = 68_000
    num_matched_rows = 34_000
    dtype = "object"
    on = [f"col_{i}" for i in range(5)]
    strategy = MergeStrategy(matched="update", not_matched_by_target="insert")
    target = generate_target_repeated_index_one_value_per_slice(num_rows, num_cols, dtype)
    source = generate_source(target, num_source_rows, num_matched_rows, dtype, on)
    return target, source, on, strategy


def test_one_index_value_float():
    num_rows = 100_000
    num_cols = 10
    num_source_rows = 73_000
    num_matched_rows = 36_500
    dtype = "float64"
    on = [f"col_{i}" for i in range(5)]
    strategy = MergeStrategy(matched="update", not_matched_by_target="insert")
    target = generate_target_repeated_index_one_value(num_rows, num_cols, dtype)
    source = generate_source(target, num_source_rows, num_matched_rows, dtype, on)
    return target, source, on, strategy


def test_one_index_value_string():
    num_rows = 100_000
    num_cols = 10
    num_source_rows = 73_000
    num_matched_rows = 36_500
    dtype = "object"
    on = [f"col_{i}" for i in range(5)]
    strategy = MergeStrategy(matched="update", not_matched_by_target="insert")
    target = generate_target_repeated_index_one_value(num_rows, num_cols, dtype)
    source = generate_source(target, num_source_rows, num_matched_rows, dtype, on)
    return target, source, on, strategy


def start_seaweed(seaweed_path):
    subprocess.run(["bash", seaweed_path, "start", SEAWEED_DATA_DIR], check=True)


def stop_seaweed(seaweed_path):
    subprocess.run(["bash", seaweed_path, "stop", SEAWEED_DATA_DIR], check=True)


def get_seaweed():
    stop_seaweed(SEAWEED_PATH)
    start_seaweed(SEAWEED_PATH)
    return benchmarks.seaweed_utils.SeaweedClient()


TESTS = {
    test.__name__: test
    for test in [
        test_update_insert_random_float_data,
        test_update_insert_random_string_data,
        test_update_random_float_data,
        test_update_random_string_data,
        test_one_index_value_per_segment_float,
        test_one_index_value_per_segment_string,
        test_one_index_value_float,
        test_one_index_value_string,
    ]
}


def run_tests(test_names):
    """Benchmarks the named tests against the already running SeaweedFS server."""
    unknown = [name for name in test_names if name not in TESTS]
    if unknown:
        raise ValueError(f"Unknown tests: {unknown}. Known tests: {list(TESTS)}")
    for name in test_names:
        benchmark(name)


def main():
    """Every merge runs in its own process, so memory held by one run cannot add to the peak of the next."""
    get_seaweed()
    try:
        run_tests(list(TESTS))
    finally:
        stop_seaweed(SEAWEED_PATH)


if __name__ == "__main__":
    if len(sys.argv) == 1:
        main()
    elif sys.argv[1] == "generate":
        generate_data(sys.argv[2])
    elif sys.argv[1] == "run":
        run_once(sys.argv[2], int(sys.argv[3]))
    else:
        run_tests(sys.argv[1:])
