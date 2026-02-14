#!/usr/bin/env python3
"""
Profile the lazy iterator pipeline: per-segment C++ timing, Python overhead,
and DuckDB scan time.

Useful for diagnosing where time is spent when lib.sql() is slow: is it the
C++ prepare_segment_for_arrow, the Python batch iteration, or DuckDB itself?

Usage:
    python -u python/benchmarks/non_asv/duckdb/profile_iterator_pipeline.py
"""

import tempfile
import time

import numpy as np
import pandas as pd
import pyarrow as pa

from arcticdb import Arctic
from arcticdb.version_store.duckdb.arrow_reader import ArcticRecordBatchReader
from arcticdb.version_store.processing import QueryBuilder

duckdb = __import__("duckdb")


def generate_numeric_df(n):
    np.random.seed(42)
    return pd.DataFrame(
        {
            "a": np.random.randint(0, 1000, n),
            "b": np.random.randint(0, 1000, n),
            "c": np.random.uniform(0, 100, n),
            "d": np.random.uniform(0, 100, n),
            "e": np.random.randint(0, 10, n),
            "f": np.random.randint(0, 100000, n),
        }
    )


def generate_mixed_df(n, freq="min", end_timestamp="1/1/2023"):
    np.random.seed(42)
    timestamps = pd.date_range(end=end_timestamp, periods=n, freq=freq)
    k = n // 10
    dt = pd.DataFrame()
    dt["id1"] = np.random.choice([f"id{str(i).zfill(3)}" for i in range(1, k + 1)], n)
    dt["id2"] = np.random.choice([f"id{str(i).zfill(3)}" for i in range(1, k + 1)], n)
    dt["id3"] = np.random.choice([f"id{str(i).zfill(10)}" for i in range(1, n // k + 1)], n)
    dt["v1"] = np.random.choice(range(1, 6), n)
    dt["v2"] = np.random.choice(range(1, 16), n)
    dt["v3"] = np.round(np.random.uniform(0, 100, n), 6)
    dt.index = timestamps
    return dt


def time_cpp_iterator(lib, sym):
    """Consume all batches from C++ iterator, return per-batch timings."""
    cpp_iter = lib._nvs.read_as_lazy_record_batch_iterator(sym)
    n_batches = cpp_iter.num_batches()
    batch_times = []
    batch_rows = []
    total_rows = 0
    t0 = time.perf_counter()
    while True:
        tb = time.perf_counter()
        data = cpp_iter.next()
        elapsed = time.perf_counter() - tb
        if data is None:
            break
        batch = pa.RecordBatch._import_from_c(data.array(), data.schema())
        batch_times.append(elapsed)
        batch_rows.append(batch.num_rows)
        total_rows += batch.num_rows
    total = time.perf_counter() - t0
    return total, n_batches, total_rows, batch_times, batch_rows


def profile_symbol(lib, sym, label, group_col="e", agg_col="c"):
    print(f"\n{'='*70}")
    print(f"  {label} — {sym}")
    print(f"{'='*70}")

    # --- Warm the cache ---
    print("  Warming cache...")
    for _ in range(3):
        time_cpp_iterator(lib, sym)

    # --- 1. C++ iterator per-segment timing ---
    print("\n  --- C++ iterator (prepare_segment_for_arrow per segment) ---")
    for run in range(3):
        total, n_b, n_r, bt, br = time_cpp_iterator(lib, sym)
        avg_ms = sum(bt) / len(bt) * 1000
        print(f"    Run {run+1}: {total:.3f}s  ({n_b} segs, {n_r:,} rows, avg={avg_ms:.1f}ms/seg)")
        if run == 0:
            print(f"    Per-seg: min={min(bt)*1000:.1f}ms  max={max(bt)*1000:.1f}ms")
            print(f"    Rows/seg: min={min(br):,}  max={max(br):,}")

    # --- 2. lib.sql('SELECT *') ---
    print("\n  --- lib.sql('SELECT *') ---")
    lib.sql(f"SELECT * FROM {sym}")  # warmup
    for run in range(3):
        t0 = time.perf_counter()
        result = lib.sql(f"SELECT * FROM {sym}")
        t = time.perf_counter() - t0
        print(f"    Run {run+1}: {t:.3f}s ({len(result):,} rows)")

    # --- 3. lib.sql('GROUP BY') ---
    gb_sql = f'SELECT "{group_col}", SUM("{agg_col}") FROM {sym} GROUP BY "{group_col}"'
    print(f"\n  --- lib.sql('GROUP BY {group_col}, SUM({agg_col})') ---")
    lib.sql(gb_sql)
    for run in range(3):
        t0 = time.perf_counter()
        result = lib.sql(gb_sql)
        t = time.perf_counter() - t0
        print(f"    Run {run+1}: {t:.3f}s ({len(result)} rows)")

    # --- 4. lib.read() baseline ---
    print("\n  --- lib.read() (pandas baseline) ---")
    lib.read(sym)
    for run in range(3):
        t0 = time.perf_counter()
        lib.read(sym)
        t = time.perf_counter() - t0
        print(f"    Run {run+1}: {t:.3f}s")

    # --- 5. QB GROUP BY ---
    print(f"\n  --- QueryBuilder groupby({group_col}).sum ---")
    q = QueryBuilder()
    q = q.groupby(group_col).agg({agg_col: "sum"})
    lib.read(sym, query_builder=q)
    for run in range(3):
        q = QueryBuilder()
        q = q.groupby(group_col).agg({agg_col: "sum"})
        t0 = time.perf_counter()
        lib.read(sym, query_builder=q)
        t = time.perf_counter() - t0
        print(f"    Run {run+1}: {t:.3f}s")

    # --- 6. Streaming GROUP BY vs pre-materialized ---
    print("\n  --- DuckDB: streaming vs pre-materialized ---")

    gb_duck = f'SELECT "{group_col}", SUM("{agg_col}") as total FROM stream GROUP BY "{group_col}"'
    gb_duck_mat = f'SELECT "{group_col}", SUM("{agg_col}") as total FROM arrow_table GROUP BY "{group_col}"'

    # Streaming
    cpp_iter2 = lib._nvs.read_as_lazy_record_batch_iterator(sym)
    reader = ArcticRecordBatchReader(cpp_iter2)
    pa_reader = reader.to_pyarrow_reader()
    conn = duckdb.connect()
    t0 = time.perf_counter()
    conn.register("stream", pa_reader)
    result = conn.execute(gb_duck).fetchdf()
    t_stream = time.perf_counter() - t0
    conn.close()
    print(f"    Streaming GROUP BY:        {t_stream:.3f}s ({len(result)} rows)")

    # Pre-materialized
    cpp_iter3 = lib._nvs.read_as_lazy_record_batch_iterator(sym)
    reader3 = ArcticRecordBatchReader(cpp_iter3)
    pa_reader3 = reader3.to_pyarrow_reader()
    t0 = time.perf_counter()
    arrow_table = pa_reader3.read_all()
    t_mat = time.perf_counter() - t0

    conn2 = duckdb.connect()
    t0 = time.perf_counter()
    result2 = conn2.execute(gb_duck_mat).fetchdf()
    t_duck = time.perf_counter() - t0
    conn2.close()
    print(f"    Materialize: {t_mat:.3f}s, then DuckDB: {t_duck:.3f}s")


def main():
    lmdb_dir = tempfile.mkdtemp(prefix="profile_iterator_")
    ac = Arctic(f"lmdb://{lmdb_dir}")
    lib = ac.create_library("bench")

    for n, label in [(1_000_000, "1M"), (10_000_000, "10M")]:
        sym = f"num_{n}"
        print(f"Writing {n:,} numeric rows...")
        lib.write(sym, generate_numeric_df(n))

    sym_str = "mixed_10M"
    print("Writing 10M mixed rows...")
    lib.write(sym_str, generate_mixed_df(10_000_000))

    profile_symbol(lib, "num_1000000", "NUMERIC 1M rows x 6 cols")
    profile_symbol(lib, "num_10000000", "NUMERIC 10M rows x 6 cols")
    profile_symbol(lib, "mixed_10M", "MIXED 10M rows x 6 cols (3 str + 3 num)", group_col="v1", agg_col="v3")


if __name__ == "__main__":
    main()
