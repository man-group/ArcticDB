#!/usr/bin/env python3
"""
Head-to-head: lib.sql() vs lib.read()+QueryBuilder on mixed and numeric data.

Tests equivalent operations (SELECT, projection, filter, GROUP BY) at 1M and
10M rows.  Reports wall-clock time, peak memory, and result shape.

Usage:
    python -u python/benchmarks/non_asv/duckdb/bench_sql_vs_querybuilder.py
"""

import gc
import tempfile
import time
import tracemalloc

import numpy as np
import pandas as pd

from arcticdb import Arctic
from arcticdb.version_store.processing import QueryBuilder


def generate_mixed_df(n, freq="min", end_timestamp="1/1/2023"):
    """ASV-style mixed data: 3 string + 3 int + 3 float columns."""
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


def measure(func, label, warmup=1, runs=3):
    for _ in range(warmup):
        r = func()
        del r
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
            result_shape = result.shape if isinstance(result, pd.DataFrame) else getattr(getattr(result, "data", None), "shape", "?")
        del result
        gc.collect()

    return {
        "label": label,
        "min_s": min(times),
        "med_s": sorted(times)[len(times) // 2],
        "peak_mb": max(peak_mems) / (1024 * 1024),
        "shape": result_shape,
    }


def run_mixed_comparison(lib, sym, n_label):
    print(f"\n{'='*90}")
    print(f"  MIXED (3 str + 6 numeric): {n_label} rows — {sym}")
    print(f"{'='*90}")

    results = []

    print("  [1/8] SELECT * ...")
    results.append(measure(lambda: lib.sql(f"SELECT * FROM {sym}"), "SQL: SELECT *"))
    results.append(measure(lambda: lib.read(sym).data, "QB: read(all)"))

    print("  [2/8] Column projection (v1, v2, v3) ...")
    results.append(measure(lambda: lib.sql(f"SELECT v1, v2, v3 FROM {sym}"), "SQL: SELECT v1,v2,v3"))
    results.append(measure(lambda: lib.read(sym, columns=["v1", "v2", "v3"]).data, "QB: cols=[v1,v2,v3]"))

    print("  [3/8] Numeric filter (v3 < 1.0, ~1%) ...")
    results.append(measure(lambda: lib.sql(f"SELECT v3 FROM {sym} WHERE v3 < 1.0"), "SQL: WHERE v3<1"))
    results.append(measure(lambda: lib.read(sym, columns=["v3"], query_builder=QueryBuilder()[QueryBuilder()["v3"] < 1.0]).data, "QB: filter v3<1"))

    print("  [4/8] String filter (id1 = 'id001') ...")
    results.append(measure(lambda: lib.sql(f"SELECT v1, v3 FROM {sym} WHERE id1 = 'id001'"), "SQL: WHERE id1='id001'"))
    results.append(measure(lambda: lib.read(sym, columns=["v1", "v3"], query_builder=QueryBuilder()[QueryBuilder()["id1"] == "id001"]).data, "QB: filter id1"))

    print("  [5/8] GROUP BY low cardinality (id6, ~10 groups) ...")
    results.append(measure(lambda: lib.sql(f"SELECT id6, SUM(v1) as total FROM {sym} GROUP BY id6"), "SQL: GB id6 SUM"))
    results.append(measure(lambda: lib.read(sym, query_builder=QueryBuilder().groupby("id6").agg({"v1": "sum"})).data, "QB: gb(id6).sum"))

    print("  [6/8] GROUP BY high cardinality (id1, ~N/10 groups) ...")
    results.append(measure(lambda: lib.sql(f"SELECT id1, SUM(v1) as total FROM {sym} GROUP BY id1"), "SQL: GB id1 SUM"))
    results.append(measure(lambda: lib.read(sym, query_builder=QueryBuilder().groupby("id1").agg({"v1": "sum"})).data, "QB: gb(id1).sum"))

    print("  [7/8] Multi-agg GROUP BY ...")
    results.append(measure(lambda: lib.sql(f"SELECT id1, SUM(v1) as s1, SUM(v3) as s3 FROM {sym} GROUP BY id1"), "SQL: GB multi-agg"))
    results.append(measure(lambda: lib.read(sym, query_builder=QueryBuilder().groupby("id1").agg({"v1": "sum", "v3": "sum"})).data, "QB: gb multi-agg"))

    print("  [8/8] Filter + GROUP BY ...")
    results.append(measure(lambda: lib.sql(f"SELECT id1, SUM(v3) as total FROM {sym} WHERE v3 < 10.0 GROUP BY id1"), "SQL: WHERE+GB"))

    def qb_fg():
        q = QueryBuilder()
        q = q[q["v3"] < 10.0]
        q = q.groupby("id1").agg({"v3": "sum"})
        return lib.read(sym, query_builder=q).data

    results.append(measure(qb_fg, "QB: filter+gb"))

    _print_comparison(results)
    return results


def run_numeric_comparison(lib, sym, n_label):
    print(f"\n{'='*90}")
    print(f"  NUMERIC-ONLY (6 int/float cols): {n_label} rows — {sym}")
    print(f"{'='*90}")

    results = []

    print("  [1/5] SELECT * ...")
    results.append(measure(lambda: lib.sql(f"SELECT * FROM {sym}"), "SQL: SELECT *"))
    results.append(measure(lambda: lib.read(sym).data, "QB: read(all)"))

    print("  [2/5] Column projection (c, d) ...")
    results.append(measure(lambda: lib.sql(f"SELECT c, d FROM {sym}"), "SQL: SELECT c,d"))
    results.append(measure(lambda: lib.read(sym, columns=["c", "d"]).data, "QB: cols=[c,d]"))

    print("  [3/5] Filter (c < 1.0) ...")
    results.append(measure(lambda: lib.sql(f"SELECT c FROM {sym} WHERE c < 1.0"), "SQL: WHERE c<1"))
    results.append(measure(lambda: lib.read(sym, columns=["c"], query_builder=QueryBuilder()[QueryBuilder()["c"] < 1.0]).data, "QB: filter c<1"))

    print("  [4/5] GROUP BY low cardinality (e, 10 groups) ...")
    results.append(measure(lambda: lib.sql(f"SELECT e, SUM(c) FROM {sym} GROUP BY e"), "SQL: GB e"))
    results.append(measure(lambda: lib.read(sym, query_builder=QueryBuilder().groupby("e").agg({"c": "sum"})).data, "QB: gb(e).sum"))

    print("  [5/5] GROUP BY high cardinality (f, 100K groups) ...")
    results.append(measure(lambda: lib.sql(f"SELECT f, SUM(c) FROM {sym} GROUP BY f"), "SQL: GB f"))
    results.append(measure(lambda: lib.read(sym, query_builder=QueryBuilder().groupby("f").agg({"c": "sum"})).data, "QB: gb(f).sum"))

    _print_comparison(results)
    return results


def _print_comparison(results):
    print(f"\n  {'Comparison':<30} {'SQL (s)':>10} {'QB (s)':>10} {'Ratio':>10} {'SQL MB':>10} {'QB MB':>10}")
    print(f"  {'─'*90}")
    for i in range(0, len(results), 2):
        sql_r, qb_r = results[i], results[i + 1]
        ratio = sql_r["min_s"] / qb_r["min_s"] if qb_r["min_s"] > 0 else float("inf")
        label = sql_r["label"].replace("SQL: ", "")
        print(
            f"  {label:<30} {sql_r['min_s']:>10.3f} {qb_r['min_s']:>10.3f} {ratio:>10.2f}x "
            f"{sql_r['peak_mb']:>10.1f} {qb_r['peak_mb']:>10.1f}"
        )
    print(f"  {'─'*90}")
    print("  (Ratio > 1.0 = SQL slower, < 1.0 = SQL faster)")


def main():
    lmdb_dir = tempfile.mkdtemp(prefix="bench_sql_vs_qb_")
    ac = Arctic(f"lmdb://{lmdb_dir}")
    lib = ac.create_library("bench")

    for n in [1_000_000, 10_000_000]:
        sym_mixed = f"mixed_{n}"
        sym_num = f"num_{n}"
        print(f"Generating {n:,} rows...")
        lib.write(sym_mixed, generate_mixed_df(n))
        lib.write(sym_num, generate_numeric_df(n))
        print(f"  Written {sym_mixed} and {sym_num}.")

    for n, label in [(1_000_000, "1M"), (10_000_000, "10M")]:
        run_mixed_comparison(lib, f"mixed_{n}", label)
        run_numeric_comparison(lib, f"num_{n}", label)


if __name__ == "__main__":
    main()
