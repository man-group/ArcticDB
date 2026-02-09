"""
Profile the lazy iterator to understand where time is spent.
Breaks down: C++ next() calls vs Python overhead vs DuckDB scan time.
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


def main():
    conn = f"lmdb://{tempfile.mkdtemp(prefix='profile_lazy_')}"
    ac = Arctic(conn)
    lib = ac.create_library("bench")

    for n in [1_000_000, 10_000_000]:
        sym = f"num_{n}"
        print(f"Writing {n:,} rows...")
        lib.write(sym, _generate_numeric_df(n))

    for n_label, sym in [("1M", "num_1000000"), ("10M", "num_10000000")]:
        print(f"\n{'='*70}")
        print(f"  PROFILING: {n_label} numeric rows — {sym}")
        print(f"{'='*70}")

        # 1. Measure raw C++ iterator: how long does next() take per segment?
        cpp_iter = lib._nvs.read_as_lazy_record_batch_iterator(sym)
        n_batches = cpp_iter.num_batches()
        print(f"\n  Segments: {n_batches}")

        batch_times = []
        batch_rows = []
        total_rows = 0
        t0 = time.perf_counter()
        while True:
            t_batch = time.perf_counter()
            data = cpp_iter.next()
            elapsed_batch = time.perf_counter() - t_batch
            if data is None:
                break
            batch = pa.RecordBatch._import_from_c(data.array(), data.schema())
            batch_times.append(elapsed_batch)
            batch_rows.append(batch.num_rows)
            total_rows += batch.num_rows
        total_cpp = time.perf_counter() - t0

        print(f"  Total rows: {total_rows:,}")
        print(f"  Total C++ iterator time: {total_cpp:.3f}s")
        if batch_times:
            print(f"  Per-batch: min={min(batch_times)*1000:.1f}ms, max={max(batch_times)*1000:.1f}ms, "
                  f"avg={sum(batch_times)/len(batch_times)*1000:.1f}ms")
            print(f"  Rows per batch: min={min(batch_rows):,}, max={max(batch_rows):,}")

        # 2. Measure ArcticRecordBatchReader -> PyArrow RecordBatchReader conversion
        cpp_iter2 = lib._nvs.read_as_lazy_record_batch_iterator(sym)
        reader = ArcticRecordBatchReader(cpp_iter2)
        t0 = time.perf_counter()
        pa_reader = reader.to_pyarrow_reader()
        t_reader_create = time.perf_counter() - t0
        print(f"\n  to_pyarrow_reader() creation: {t_reader_create*1000:.1f}ms")

        # 3. Consume all batches through PyArrow reader
        t0 = time.perf_counter()
        table = pa_reader.read_all()
        t_read_all = time.perf_counter() - t0
        print(f"  pa_reader.read_all(): {t_read_all:.3f}s ({table.num_rows:,} rows)")

        # 4. Measure full lib.sql() time
        t0 = time.perf_counter()
        result = lib.sql(f"SELECT * FROM {sym}")
        t_sql = time.perf_counter() - t0
        print(f"\n  lib.sql('SELECT *'): {t_sql:.3f}s ({len(result):,} rows)")

        # 5. Measure DuckDB scan of pre-materialized Arrow table
        cpp_iter3 = lib._nvs.read_as_lazy_record_batch_iterator(sym)
        reader3 = ArcticRecordBatchReader(cpp_iter3)
        pa_reader3 = reader3.to_pyarrow_reader()
        arrow_table = pa_reader3.read_all()  # Pre-materialize

        conn_db = duckdb.connect()
        t0 = time.perf_counter()
        result2 = conn_db.execute("SELECT * FROM arrow_table").fetchdf()
        t_duckdb_only = time.perf_counter() - t0
        print(f"  DuckDB scan pre-loaded table: {t_duckdb_only:.3f}s ({len(result2):,} rows)")
        conn_db.close()

        # 6. Measure lib.read() baseline (pandas via C++)
        t0 = time.perf_counter()
        baseline = lib.read(sym).data
        t_read = time.perf_counter() - t0
        print(f"  lib.read() (pandas): {t_read:.3f}s ({len(baseline):,} rows)")

        # 7. Measure GROUP BY (aggregation, tiny result)
        t0 = time.perf_counter()
        result_gb = lib.sql(f"SELECT e, SUM(c) FROM {sym} GROUP BY e")
        t_gb = time.perf_counter() - t0
        print(f"\n  lib.sql('GROUP BY e'): {t_gb:.3f}s ({len(result_gb)} rows)")

        # 8. lib.read + QueryBuilder GROUP BY
        t0 = time.perf_counter()
        q = QueryBuilder()
        q = q.groupby("e").agg({"c": "sum"})
        qb_result = lib.read(sym, query_builder=q).data
        t_qb_gb = time.perf_counter() - t0
        print(f"  QB groupby(e).sum: {t_qb_gb:.3f}s ({len(qb_result)} rows)")

        # Time breakdown
        print(f"\n  --- TIME BREAKDOWN for SELECT * ---")
        print(f"  C++ iterator (prepare_segment_for_arrow per seg): {total_cpp:.3f}s")
        print(f"  lib.sql() total:                                   {t_sql:.3f}s")
        print(f"  Implied DuckDB + Python overhead:                  {t_sql - total_cpp:.3f}s")
        print(f"  lib.read() (pandas) baseline:                      {t_read:.3f}s")
        print(f"  SQL/read ratio:                                    {t_sql/t_read:.1f}x")


if __name__ == "__main__":
    main()
