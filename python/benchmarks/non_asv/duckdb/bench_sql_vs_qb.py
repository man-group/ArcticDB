"""
Head-to-head comparison of SQL (lib.sql via DuckDB) vs QueryBuilder (lib.read + QueryBuilder).

Tests equivalent operations on the same data at 1M and 10M rows.
Measures wall-clock time, peak RSS memory delta, and result shape.
"""

import gc
import os
import time
import tracemalloc

import numpy as np
import pandas as pd

from arcticdb import Arctic
from arcticdb.version_store.processing import QueryBuilder


def generate_benchmark_df(n, freq="min", end_timestamp="1/1/2023"):
    """Same generator as ASV benchmarks."""
    np.random.seed(42)
    timestamps = pd.date_range(end=end_timestamp, periods=n, freq=freq)
    k = n // 10
    dt = pd.DataFrame()
    dt["id1"] = np.random.choice([f"id{str(i).zfill(3)}" for i in range(1, k + 1)], n)
    dt["id2"] = np.random.choice([f"id{str(i).zfill(3)}" for i in range(1, k + 1)], n)
    dt["id3"] = np.random.choice([f"id{str(i).zfill(10)}" for i in range(1, n // k + 1)], n)
    dt["id4"] = np.random.choice(range(1, k + 1), n)
    dt["id5"] = np.random.choice(range(1, k + 1), n)
    dt["id6"] = np.random.choice(range(1, n // k + 1), n)
    dt["v1"] = np.random.choice(range(1, 6), n)
    dt["v2"] = np.random.choice(range(1, 16), n)
    dt["v3"] = np.round(np.random.uniform(0, 100, n), 6)
    dt.index = timestamps
    return dt


def measure(func, label, warmup=1, runs=3):
    """Run func multiple times, report min time and peak memory."""
    # Warmup
    for _ in range(warmup):
        result = func()
        del result
        gc.collect()

    times = []
    peak_mems = []
    result_shape = None

    for _ in range(runs):
        gc.collect()
        tracemalloc.start()
        t0 = time.perf_counter()
        result = func()
        elapsed = time.perf_counter() - t0
        _, peak = tracemalloc.get_traced_memory()
        tracemalloc.stop()

        times.append(elapsed)
        peak_mems.append(peak)
        if result_shape is None:
            if isinstance(result, pd.DataFrame):
                result_shape = result.shape
            elif hasattr(result, "data"):
                result_shape = result.data.shape
            else:
                result_shape = str(type(result))
        del result
        gc.collect()

    return {
        "label": label,
        "min_time_s": min(times),
        "median_time_s": sorted(times)[len(times) // 2],
        "peak_mem_mb": max(peak_mems) / (1024 * 1024),
        "result_shape": result_shape,
    }


def run_comparison(lib, num_rows, symbol):
    print(f"\n{'='*80}")
    print(f"  BENCHMARK: {num_rows:,} rows  |  symbol: {symbol}")
    print(f"{'='*80}\n")

    results = []

    # --- 1. SELECT ALL ---
    print("  [1/8] SELECT ALL ...")
    results.append(measure(lambda: lib.sql(f"SELECT * FROM {symbol}"), "SQL: SELECT *"))
    results.append(measure(lambda: lib.read(symbol).data, "QB: read (all)"))

    # --- 2. Column projection ---
    print("  [2/8] Column projection (v1, v2, v3) ...")
    results.append(measure(lambda: lib.sql(f"SELECT v1, v2, v3 FROM {symbol}"), "SQL: SELECT v1,v2,v3"))
    results.append(measure(lambda: lib.read(symbol, columns=["v1", "v2", "v3"]).data, "QB: read(columns=[v1,v2,v3])"))

    # --- 3. Numeric filter (~1% selectivity) ---
    print("  [3/8] Numeric filter (v3 < 1.0, ~1%) ...")
    results.append(measure(lambda: lib.sql(f"SELECT v3 FROM {symbol} WHERE v3 < 1.0"), "SQL: WHERE v3 < 1.0"))

    def qb_filter_numeric():
        q = QueryBuilder()
        q = q[q["v3"] < 1.0]
        return lib.read(symbol, columns=["v3"], query_builder=q).data

    results.append(measure(qb_filter_numeric, "QB: filter v3 < 1.0"))

    # --- 4. String equality filter ---
    print("  [4/8] String filter (id1 = 'id001') ...")
    results.append(
        measure(lambda: lib.sql(f"SELECT v1, v3 FROM {symbol} WHERE id1 = 'id001'"), "SQL: WHERE id1='id001'")
    )

    def qb_filter_string():
        q = QueryBuilder()
        q = q[q["id1"] == "id001"]
        return lib.read(symbol, columns=["v1", "v3"], query_builder=q).data

    results.append(measure(qb_filter_string, "QB: filter id1=='id001'"))

    # --- 5. GROUP BY SUM (low cardinality - id6 has ~10 values) ---
    print("  [5/8] GROUP BY SUM (low cardinality, id6) ...")
    results.append(
        measure(lambda: lib.sql(f"SELECT id6, SUM(v1) as total FROM {symbol} GROUP BY id6"), "SQL: GROUP BY id6 SUM")
    )

    def qb_groupby_low():
        q = QueryBuilder()
        q = q.groupby("id6").agg({"v1": "sum"})
        return lib.read(symbol, query_builder=q).data

    results.append(measure(qb_groupby_low, "QB: groupby(id6).sum"))

    # --- 6. GROUP BY SUM (high cardinality - id1 has ~N/10 values) ---
    print("  [6/8] GROUP BY SUM (high cardinality, id1) ...")
    results.append(
        measure(lambda: lib.sql(f"SELECT id1, SUM(v1) as total FROM {symbol} GROUP BY id1"), "SQL: GROUP BY id1 SUM")
    )

    def qb_groupby_high():
        q = QueryBuilder()
        q = q.groupby("id1").agg({"v1": "sum"})
        return lib.read(symbol, query_builder=q).data

    results.append(measure(qb_groupby_high, "QB: groupby(id1).sum"))

    # --- 7. Multi-aggregation GROUP BY ---
    print("  [7/8] Multi-agg GROUP BY (id1: sum v1, sum v3) ...")
    results.append(
        measure(
            lambda: lib.sql(f"SELECT id1, SUM(v1) as s1, SUM(v3) as s3 FROM {symbol} GROUP BY id1"),
            "SQL: GROUP BY id1 multi-agg",
        )
    )

    def qb_multi_agg():
        q = QueryBuilder()
        q = q.groupby("id1").agg({"v1": "sum", "v3": "sum"})
        return lib.read(symbol, query_builder=q).data

    results.append(measure(qb_multi_agg, "QB: groupby(id1).agg(sum,sum)"))

    # --- 8. Filter + GROUP BY ---
    print("  [8/8] Filter + GROUP BY (v3 < 10 then GROUP BY id1) ...")
    results.append(
        measure(
            lambda: lib.sql(f"SELECT id1, SUM(v3) as total FROM {symbol} WHERE v3 < 10.0 GROUP BY id1"),
            "SQL: WHERE + GROUP BY",
        )
    )

    def qb_filter_group():
        q = QueryBuilder()
        q = q[q["v3"] < 10.0]
        q = q.groupby("id1").agg({"v3": "sum"})
        return lib.read(symbol, query_builder=q).data

    results.append(measure(qb_filter_group, "QB: filter+groupby"))

    # --- Print results table ---
    print(f"\n{'─'*90}")
    print(f"  {'Operation':<35} {'Time (s)':>10} {'Median':>10} {'Peak MB':>10} {'Shape':>20}")
    print(f"{'─'*90}")
    for r in results:
        shape_str = f"{r['result_shape']}" if isinstance(r["result_shape"], tuple) else str(r["result_shape"])
        print(
            f"  {r['label']:<35} {r['min_time_s']:>10.3f} {r['median_time_s']:>10.3f} {r['peak_mem_mb']:>10.1f} {shape_str:>20}"
        )
    print(f"{'─'*90}")

    # Group SQL vs QB pairs
    print(
        f"\n  {'Comparison':<30} {'SQL (s)':>10} {'QB (s)':>10} {'Ratio':>10} {'SQL MB':>10} {'QB MB':>10} {'Mem Ratio':>10}"
    )
    print(f"{'─'*100}")
    for i in range(0, len(results), 2):
        sql_r = results[i]
        qb_r = results[i + 1]
        time_ratio = sql_r["min_time_s"] / qb_r["min_time_s"] if qb_r["min_time_s"] > 0 else float("inf")
        mem_ratio = sql_r["peak_mem_mb"] / qb_r["peak_mem_mb"] if qb_r["peak_mem_mb"] > 0 else float("inf")
        op_name = sql_r["label"].replace("SQL: ", "")
        print(
            f"  {op_name:<30} {sql_r['min_time_s']:>10.3f} {qb_r['min_time_s']:>10.3f} {time_ratio:>10.2f}x {sql_r['peak_mem_mb']:>10.1f} {qb_r['peak_mem_mb']:>10.1f} {mem_ratio:>10.2f}x"
        )
    print(f"{'─'*100}")
    print("  (Ratio > 1.0 means SQL is slower/more memory; < 1.0 means SQL is faster/less memory)")

    return results


def main():
    import tempfile

    lmdb_path = tempfile.mkdtemp(prefix="bench_sql_vs_qb_")
    conn = f"lmdb://{lmdb_path}"

    ac = Arctic(conn)
    lib_name = "sql_vs_qb"

    # Check if data already exists to skip regeneration
    try:
        lib = ac.get_library(lib_name)
        existing = set(lib.list_symbols())
    except Exception:
        ac.delete_library(lib_name)
        lib = ac.create_library(lib_name)
        existing = set()

    for num_rows in [1_000_000, 10_000_000]:
        sym = f"bench_{num_rows}"
        if sym not in existing:
            print(f"Generating {num_rows:,} rows for {sym}...")
            df = generate_benchmark_df(num_rows)
            print(f"  DataFrame shape: {df.shape}, writing to ArcticDB...")
            lib.write(sym, df)
            print(f"  Written.")
        else:
            print(f"  {sym} already exists, skipping write.")

    # Run 1M first (fast)
    all_results = {}
    for num_rows in [1_000_000, 10_000_000]:
        sym = f"bench_{num_rows}"
        all_results[num_rows] = run_comparison(lib, num_rows, sym)

    print("\n\nDone.")


if __name__ == "__main__":
    main()
