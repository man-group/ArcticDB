#!/usr/bin/env python3
"""
Benchmark: how lib.sql() and lib.read() scale with table width.

Generates tables of increasing column count (9 to 385 columns) with a mix of
string and numeric columns, then runs SELECT *, column projection, filter,
string filter, and GROUP BY at each width.  Compares lib.sql() vs
lib.read()+QueryBuilder.

Usage:
    python -u python/benchmarks/non_asv/duckdb/bench_sql_scaling.py
"""

import gc
import tempfile
import time

import numpy as np
import pandas as pd

from arcticdb import Arctic
from arcticdb.version_store.processing import QueryBuilder


def generate_wide_df(n_rows, n_str_cols, n_num_cols, seed=42):
    np.random.seed(seed)
    data = {}
    str_pool = [f"val_{i:04d}" for i in range(100)]
    for i in range(n_str_cols):
        data[f"str_{i}"] = np.random.choice(str_pool, n_rows)
    for i in range(n_num_cols):
        data[f"num_{i}"] = np.random.uniform(0, 100, n_rows)
    df = pd.DataFrame(data)
    df.index = pd.date_range(end="2025-01-01", periods=n_rows, freq="min")
    return df


def timeit(func, label, warmup=True, runs=3):
    if warmup:
        try:
            r = func()
            del r
            gc.collect()
        except Exception as e:
            print(f"  {label}: ERROR: {e}")
            return None, None
    times = []
    shape = None
    for _ in range(runs):
        gc.collect()
        t0 = time.perf_counter()
        r = func()
        times.append(time.perf_counter() - t0)
        if shape is None:
            shape = r.shape if hasattr(r, "shape") else (hasattr(r, "data") and r.data.shape)
        del r
        gc.collect()
    return min(times), shape


def run_benchmarks(lib, sym, n_str, n_num):
    pairs = []

    # 1. SELECT *
    print("  [1/6] SELECT * ...")
    sql_t, sql_s = timeit(lambda: lib.sql(f"SELECT * FROM {sym}"), "SQL *")
    qb_t, qb_s = timeit(lambda: lib.read(sym).data, "QB *")
    pairs.append(("SELECT *", sql_t, qb_t, sql_s, qb_s))

    # 2. Column projection (3 numeric cols)
    print("  [2/6] SELECT 3 cols ...")
    sql_t, sql_s = timeit(lambda: lib.sql(f"SELECT num_0, num_1, num_2 FROM {sym}"), "SQL 3c")
    qb_t, qb_s = timeit(lambda: lib.read(sym, columns=["num_0", "num_1", "num_2"]).data, "QB 3c")
    pairs.append(("SELECT 3 cols", sql_t, qb_t, sql_s, qb_s))

    # 3. Numeric filter (1% selectivity) + 3 cols
    print("  [3/6] WHERE num_0 < 1.0 (3 cols) ...")
    sql_t, sql_s = timeit(
        lambda: lib.sql(f"SELECT num_0, num_1, num_2 FROM {sym} WHERE num_0 < 1.0"), "SQL filt"
    )

    def qb_filter_3():
        q = QueryBuilder()
        q = q[q["num_0"] < 1.0]
        return lib.read(sym, columns=["num_0", "num_1", "num_2"], query_builder=q).data

    qb_t, qb_s = timeit(qb_filter_3, "QB filt")
    pairs.append(("WHERE + 3 cols", sql_t, qb_t, sql_s, qb_s))

    # 4. Numeric filter returning ALL columns
    print("  [4/6] WHERE num_0 < 1.0 (all cols) ...")
    sql_t, sql_s = timeit(lambda: lib.sql(f"SELECT * FROM {sym} WHERE num_0 < 1.0"), "SQL filt*")

    def qb_filter_all():
        q = QueryBuilder()
        q = q[q["num_0"] < 1.0]
        return lib.read(sym, query_builder=q).data

    qb_t, qb_s = timeit(qb_filter_all, "QB filt*")
    pairs.append(("WHERE + all cols", sql_t, qb_t, sql_s, qb_s))

    # 5. String filter + all cols
    print("  [5/6] WHERE str_0 = 'val_0001' (all cols) ...")
    sql_t, sql_s = timeit(
        lambda: lib.sql(f"SELECT * FROM {sym} WHERE str_0 = 'val_0001'"), "SQL sfilt"
    )

    def qb_str_filter():
        q = QueryBuilder()
        q = q[q["str_0"] == "val_0001"]
        return lib.read(sym, query_builder=q).data

    qb_t, qb_s = timeit(qb_str_filter, "QB sfilt")
    pairs.append(("str filter + all", sql_t, qb_t, sql_s, qb_s))

    # 6. GROUP BY + SUM
    print("  [6/6] GROUP BY str_0, SUM(num_0) ...")
    sql_t, sql_s = timeit(
        lambda: lib.sql(f"SELECT str_0, SUM(num_0) as total FROM {sym} GROUP BY str_0"), "SQL gb"
    )

    def qb_groupby():
        q = QueryBuilder()
        q = q.groupby("str_0").agg({"num_0": "sum"})
        return lib.read(sym, query_builder=q).data

    qb_t, qb_s = timeit(qb_groupby, "QB gb")
    pairs.append(("GROUP BY str_0", sql_t, qb_t, sql_s, qb_s))

    # Print results
    print(f"\n  {'Operation':<25} {'SQL (s)':>10} {'QB (s)':>10} {'Ratio':>10} {'SQL shape':>20} {'QB shape':>20}")
    print(f"  {'─'*95}")
    for op, st, qt, ss, qs in pairs:
        if st is not None and qt is not None and qt > 0:
            ratio = st / qt
            print(f"  {op:<25} {st:>10.3f} {qt:>10.3f} {ratio:>9.2f}x {str(ss):>20} {str(qs):>20}")
        else:
            print(f"  {op:<25} {'ERR':>10} {'ERR':>10} {'---':>10}")

    return pairs


def main():
    configs = [
        (1_000_000, 3, 6, "9 cols (1M)"),
        (1_000_000, 20, 30, "50 cols (1M)"),
        (1_000_000, 40, 60, "100 cols (1M)"),
        (500_000, 85, 115, "200 cols (500K)"),
        (250_000, 170, 215, "385 cols (250K)"),
    ]

    lmdb_dir = tempfile.mkdtemp(prefix="bench_sql_scaling_")
    ac = Arctic(f"lmdb://{lmdb_dir}")
    lib = ac.create_library("bench")

    for n_rows, n_str, n_num, label in configs:
        sym = f"wide_{n_str + n_num}"
        print(f"Generating {label}: {n_rows:,} rows x {n_str + n_num} cols...")
        df = generate_wide_df(n_rows, n_str, n_num)
        t0 = time.time()
        lib.write(sym, df)
        print(f"  Written in {time.time() - t0:.1f}s ({df.memory_usage(deep=True).sum() / 1024**2:.0f} MB)")
        del df
        gc.collect()

    all_pairs = {}
    for n_rows, n_str, n_num, label in configs:
        sym = f"wide_{n_str + n_num}"
        print(f"\n{'='*80}")
        print(f"  {label}")
        print(f"{'='*80}")
        all_pairs[label] = run_benchmarks(lib, sym, n_str, n_num)

    # Cross-config summary
    print(f"\n\n{'='*120}")
    print("CROSS-CONFIG SUMMARY: SQL/QB Time Ratio (>1 means SQL slower)")
    print(f"{'='*120}")
    ops = [p[0] for p in list(all_pairs.values())[0]]
    labels = [l for _, _, _, l in configs]
    print(f"  {'Operation':<25}", end="")
    for l in labels:
        print(f" {l:>19}", end="")
    print()
    print(f"  {'─'*120}")
    for op_idx, op in enumerate(ops):
        print(f"  {op:<25}", end="")
        for label in labels:
            pairs = all_pairs[label]
            st, qt = pairs[op_idx][1], pairs[op_idx][2]
            if st and qt and qt > 0:
                print(f" {st/qt:>18.2f}x", end="")
            else:
                print(f" {'ERR':>19}", end="")
        print()


if __name__ == "__main__":
    main()
