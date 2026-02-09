"""
Quick benchmark: SQL vs QueryBuilder on NUMERIC-ONLY data.
This isolates string dictionary encoding overhead.
"""

import gc
import time
import tracemalloc

import numpy as np
import pandas as pd

from arcticdb import Arctic
from arcticdb.version_store.processing import QueryBuilder


def measure(func, label, warmup=1, runs=3):
    for _ in range(warmup):
        result = func()
        del result
        gc.collect()

    times = []
    peak_mems = []
    result = None
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
        del result
        gc.collect()

    return {
        "label": label,
        "min_time_s": min(times),
        "median_time_s": sorted(times)[len(times) // 2],
        "peak_mem_mb": max(peak_mems) / (1024 * 1024),
    }


def main():
    import tempfile

    conn = f"lmdb://{tempfile.mkdtemp(prefix='bench_numeric_')}"
    ac = Arctic(conn)
    lib_name = "numeric_bench"

    try:
        lib = ac.get_library(lib_name)
        existing = set(lib.list_symbols())
    except Exception:
        ac.delete_library(lib_name)
        lib = ac.create_library(lib_name)
        existing = set()

    for n in [1_000_000, 10_000_000]:
        sym = f"num_{n}"
        if sym not in existing:
            print(f"Generating {n:,} numeric-only rows...")
            np.random.seed(42)
            df = pd.DataFrame(
                {
                    "a": np.random.randint(0, 1000, n),
                    "b": np.random.randint(0, 1000, n),
                    "c": np.random.uniform(0, 100, n),
                    "d": np.random.uniform(0, 100, n),
                    "e": np.random.randint(0, 10, n),
                    "f": np.random.randint(0, 100000, n),
                }
            )
            lib.write(sym, df)
            print(f"  Written {sym}.")
        else:
            print(f"  {sym} exists.")

    for n in [1_000_000, 10_000_000]:
        sym = f"num_{n}"
        print(f"\n{'='*80}")
        print(f"  NUMERIC-ONLY: {n:,} rows x 6 int/float columns")
        print(f"{'='*80}")

        results = []

        # SELECT ALL
        print("  SELECT * ...")
        results.append(measure(lambda: lib.sql(f"SELECT * FROM {sym}"), "SQL: SELECT *"))
        results.append(measure(lambda: lib.read(sym).data, "QB: read(all)"))

        # Column projection
        print("  SELECT c,d ...")
        results.append(measure(lambda: lib.sql(f"SELECT c, d FROM {sym}"), "SQL: SELECT c,d"))
        results.append(measure(lambda: lib.read(sym, columns=["c", "d"]).data, "QB: read(cols=[c,d])"))

        # Filter
        print("  WHERE c < 1.0 ...")
        results.append(measure(lambda: lib.sql(f"SELECT c FROM {sym} WHERE c < 1.0"), "SQL: WHERE c<1"))

        def qb_filt():
            q = QueryBuilder()
            q = q[q["c"] < 1.0]
            return lib.read(sym, columns=["c"], query_builder=q).data

        results.append(measure(qb_filt, "QB: filter c<1"))

        # GROUP BY low cardinality (e has 10 values)
        print("  GROUP BY e (10 groups) ...")
        results.append(measure(lambda: lib.sql(f"SELECT e, SUM(c) FROM {sym} GROUP BY e"), "SQL: GROUP BY e"))

        def qb_gb():
            q = QueryBuilder()
            q = q.groupby("e").agg({"c": "sum"})
            return lib.read(sym, query_builder=q).data

        results.append(measure(qb_gb, "QB: groupby(e).sum"))

        # GROUP BY high cardinality (f has 100K values)
        print("  GROUP BY f (100K groups) ...")
        results.append(measure(lambda: lib.sql(f"SELECT f, SUM(c) FROM {sym} GROUP BY f"), "SQL: GROUP BY f"))

        def qb_gb_high():
            q = QueryBuilder()
            q = q.groupby("f").agg({"c": "sum"})
            return lib.read(sym, query_builder=q).data

        results.append(measure(qb_gb_high, "QB: groupby(f).sum"))

        # Print results
        print(f"\n  {'Comparison':<30} {'SQL (s)':>10} {'QB (s)':>10} {'Ratio':>10} {'SQL MB':>10} {'QB MB':>10}")
        print(f"  {'─'*90}")
        for i in range(0, len(results), 2):
            sql_r = results[i]
            qb_r = results[i + 1]
            ratio = sql_r["min_time_s"] / qb_r["min_time_s"] if qb_r["min_time_s"] > 0 else 0
            label = sql_r["label"].replace("SQL: ", "")
            print(
                f"  {label:<30} {sql_r['min_time_s']:>10.3f} {qb_r['min_time_s']:>10.3f} {ratio:>10.2f}x {sql_r['peak_mem_mb']:>10.1f} {qb_r['peak_mem_mb']:>10.1f}"
            )
        print(f"  {'─'*90}")


if __name__ == "__main__":
    main()
