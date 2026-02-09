"""
Profile with warm LMDB cache to get true prepare_segment_for_arrow() cost.
"""

import tempfile
import time

import numpy as np
import pandas as pd
import pyarrow as pa

from arcticdb import Arctic
from arcticdb.version_store.processing import QueryBuilder


def _generate_numeric_df(n):
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


def time_cpp_iterator(lib, sym):
    """Return total time to consume all batches from C++ iterator."""
    cpp_iter = lib._nvs.read_as_lazy_record_batch_iterator(sym)
    n_batches = cpp_iter.num_batches()
    total_rows = 0
    batch_times = []
    t0 = time.perf_counter()
    while True:
        tb = time.perf_counter()
        data = cpp_iter.next()
        batch_t = time.perf_counter() - tb
        if data is None:
            break
        batch = pa.RecordBatch._import_from_c(data.array(), data.schema())
        batch_times.append(batch_t)
        total_rows += batch.num_rows
    total = time.perf_counter() - t0
    return total, n_batches, total_rows, batch_times


def main():
    conn = f"lmdb://{tempfile.mkdtemp(prefix='profile_warm_')}"
    ac = Arctic(conn)
    lib = ac.create_library("bench")

    for n in [1_000_000, 10_000_000]:
        sym = f"num_{n}"
        print(f"Writing {n:,} rows...")
        lib.write(sym, _generate_numeric_df(n))

    for sym, label in [("num_1000000", "1M"), ("num_10000000", "10M")]:
        print(f"\n{'='*70}")
        print(f"  WARM CACHE PROFILE: {label} numeric rows")
        print(f"{'='*70}")

        # Warm the cache with 3 reads
        print("  Warming LMDB page cache...")
        for _ in range(3):
            t, _, _, _ = time_cpp_iterator(lib, sym)
        print(f"  Cache warm (last warmup: {t:.3f}s)")

        # Now take 3 timed runs
        print("\n  --- C++ iterator (prepare_segment_for_arrow) ---")
        for run in range(3):
            total, n_b, n_r, bt = time_cpp_iterator(lib, sym)
            print(
                f"    Run {run+1}: {total:.3f}s  ({n_b} segs, {n_r:,} rows, " f"avg={sum(bt)/len(bt)*1000:.1f}ms/seg)"
            )

        # Warm lib.sql()
        print("\n  --- lib.sql('SELECT *') ---")
        lib.sql(f"SELECT * FROM {sym}")  # warmup
        for run in range(3):
            t0 = time.perf_counter()
            lib.sql(f"SELECT * FROM {sym}")
            print(f"    Run {run+1}: {time.perf_counter()-t0:.3f}s")

        # Warm lib.sql GROUP BY
        print("\n  --- lib.sql('GROUP BY e, SUM(c)') ---")
        lib.sql(f"SELECT e, SUM(c) FROM {sym} GROUP BY e")
        for run in range(3):
            t0 = time.perf_counter()
            lib.sql(f"SELECT e, SUM(c) FROM {sym} GROUP BY e")
            print(f"    Run {run+1}: {time.perf_counter()-t0:.3f}s")

        # Warm lib.read()
        print("\n  --- lib.read() (pandas) ---")
        lib.read(sym)
        for run in range(3):
            t0 = time.perf_counter()
            lib.read(sym)
            print(f"    Run {run+1}: {time.perf_counter()-t0:.3f}s")

        # QB GROUP BY
        print("\n  --- QB groupby(e).sum ---")
        q = QueryBuilder()
        q = q.groupby("e").agg({"c": "sum"})
        lib.read(sym, query_builder=q)
        for run in range(3):
            q = QueryBuilder()
            q = q.groupby("e").agg({"c": "sum"})
            t0 = time.perf_counter()
            lib.read(sym, query_builder=q)
            print(f"    Run {run+1}: {time.perf_counter()-t0:.3f}s")


if __name__ == "__main__":
    main()
