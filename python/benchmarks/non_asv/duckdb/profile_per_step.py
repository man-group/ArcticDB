"""
Detailed per-step timing for the lazy iterator path.
Instruments each phase: read+decode, prepare_for_arrow, to_arrow, DuckDB scan.

Generates its own numeric and string-heavy data so it's self-contained.
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


def _generate_numeric_df(n):
    np.random.seed(42)
    return pd.DataFrame({
        "a": np.random.randint(0, 1000, n),
        "b": np.random.randint(0, 1000, n),
        "c": np.random.uniform(0, 100, n),
        "d": np.random.uniform(0, 100, n),
        "e": np.random.randint(0, 10, n),
        "f": np.random.randint(0, 100000, n),
    })


def _generate_benchmark_df(n, freq="min", end_timestamp="1/1/2023"):
    """Same as ASV generate_benchmark_df — mixed string + numeric columns."""
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


def main():
    conn = f"lmdb://{tempfile.mkdtemp(prefix='profile_steps_')}"
    ac = Arctic(conn)
    lib = ac.create_library("bench")

    # Write numeric data
    sym_num = "num_10000000"
    print(f"Writing 10M numeric rows...")
    lib.write(sym_num, _generate_numeric_df(10_000_000))

    # Write string-heavy data
    sym_str = "bench_10000000"
    print(f"Writing 10M string-heavy rows...")
    lib.write(sym_str, _generate_benchmark_df(10_000_000))

    # --- NUMERIC DATA ---
    print("\n" + "=" * 70)
    print("  NUMERIC-ONLY: 10M rows × 6 int/float columns")
    print("=" * 70)

    print("  Warming cache...")
    for _ in range(2):
        cpp_iter = lib._nvs.read_as_lazy_record_batch_iterator(sym_num)
        while cpp_iter.has_next():
            cpp_iter.next()

    print("\n  --- C++ next() per segment (first 10) ---")
    cpp_iter = lib._nvs.read_as_lazy_record_batch_iterator(sym_num)
    print(f"  Segments: {cpp_iter.num_batches()}")

    seg_times = []
    for i in range(min(10, cpp_iter.num_batches())):
        t0 = time.perf_counter()
        data = cpp_iter.next()
        elapsed = time.perf_counter() - t0
        seg_times.append(elapsed)
        if data is not None:
            batch = pa.RecordBatch._import_from_c(data.array(), data.schema())
            print(f"  Seg {i}: {elapsed*1000:.1f}ms, {batch.num_rows:,} rows, {batch.num_columns} cols, "
                  f"{batch.nbytes / 1024:.0f}KB")

    print(f"\n  Average per segment: {sum(seg_times)/len(seg_times)*1000:.1f}ms")
    print(f"  Projected total for {cpp_iter.num_batches()} segments: "
          f"{sum(seg_times)/len(seg_times)*cpp_iter.num_batches():.2f}s")

    # GROUP BY comparison
    print("\n  --- GROUP BY comparison ---")

    # Via streaming RecordBatchReader
    t0 = time.perf_counter()
    cpp_iter2 = lib._nvs.read_as_lazy_record_batch_iterator(sym_num)
    reader = ArcticRecordBatchReader(cpp_iter2)
    pa_reader = reader.to_pyarrow_reader()
    conn_db = duckdb.connect()
    result = conn_db.execute("SELECT e, SUM(c) as total FROM pa_reader GROUP BY e").fetchdf()
    t_streaming_gb = time.perf_counter() - t0
    conn_db.close()
    print(f"  Streaming GROUP BY: {t_streaming_gb:.3f}s ({len(result)} rows)")

    # Via pre-materialized arrow table
    t0 = time.perf_counter()
    cpp_iter3 = lib._nvs.read_as_lazy_record_batch_iterator(sym_num)
    reader3 = ArcticRecordBatchReader(cpp_iter3)
    pa_reader3 = reader3.to_pyarrow_reader()
    arrow_table = pa_reader3.read_all()
    t_materialize = time.perf_counter() - t0

    conn_db2 = duckdb.connect()
    t0 = time.perf_counter()
    result2 = conn_db2.execute("SELECT e, SUM(c) as total FROM arrow_table GROUP BY e").fetchdf()
    t_duckdb_gb = time.perf_counter() - t0
    conn_db2.close()
    print(f"  Materialize all: {t_materialize:.3f}s, then DuckDB GROUP BY: {t_duckdb_gb:.3f}s")

    # Via lib.read() + pandas
    t0 = time.perf_counter()
    df = lib.read(sym_num).data
    t_read = time.perf_counter() - t0
    t0 = time.perf_counter()
    result3 = df.groupby("e")["c"].sum()
    t_pandas_gb = time.perf_counter() - t0
    print(f"  lib.read(): {t_read:.3f}s, pandas groupby: {t_pandas_gb:.3f}s")

    # Via lib.read() then DuckDB on pandas df
    t0 = time.perf_counter()
    df2 = lib.read(sym_num).data
    conn_db3 = duckdb.connect()
    result4 = conn_db3.execute("SELECT e, SUM(c) as total FROM df2 GROUP BY e").fetchdf()
    t_read_then_duckdb = time.perf_counter() - t0
    conn_db3.close()
    print(f"  lib.read() + DuckDB on df: {t_read_then_duckdb:.3f}s")

    # --- STRING-HEAVY DATA ---
    print("\n\n" + "=" * 70)
    print("  STRING-HEAVY: 10M rows × 9 columns (3 string + 6 numeric)")
    print("=" * 70)

    # Warm
    lib.sql(f"SELECT v1 FROM {sym_str} LIMIT 1")
    cpp_iter4 = lib._nvs.read_as_lazy_record_batch_iterator(sym_str)
    while cpp_iter4.has_next():
        cpp_iter4.next()

    # Time C++ iterator
    print("  C++ iterator (all segments):")
    for run in range(3):
        cpp_iter5 = lib._nvs.read_as_lazy_record_batch_iterator(sym_str)
        n_b = cpp_iter5.num_batches()
        t0 = time.perf_counter()
        while True:
            d = cpp_iter5.next()
            if d is None:
                break
        t = time.perf_counter() - t0
        print(f"    Run {run+1}: {t:.3f}s ({n_b} segments)")

    # Full SQL SELECT *
    print("  lib.sql('SELECT *'):")
    lib.sql(f"SELECT * FROM {sym_str}")  # warmup
    for run in range(3):
        t0 = time.perf_counter()
        lib.sql(f"SELECT * FROM {sym_str}")
        print(f"    Run {run+1}: {time.perf_counter()-t0:.3f}s")

    # lib.read() baseline
    print("  lib.read():")
    lib.read(sym_str)
    for run in range(3):
        t0 = time.perf_counter()
        lib.read(sym_str)
        print(f"    Run {run+1}: {time.perf_counter()-t0:.3f}s")


if __name__ == "__main__":
    main()
