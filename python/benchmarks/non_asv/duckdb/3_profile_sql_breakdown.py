#!/usr/bin/env python3
"""
Break down where time is spent inside lib.sql() on a wide table.

Instruments each phase of the SQL query path:
  1. Pushdown extraction (SQL parsing)
  2. _read_as_record_batch_reader (C++ iterator creation)
  3. Batch materialization (C++ prepare_segment_for_arrow per segment)
  4. DuckDB scan (streaming vs pre-materialized)

Generates a 100K-row, 400-column table with a named DatetimeIndex to test
date_range + value filter pushdown, matching the CTA workload pattern.

Usage:
    python -u python/benchmarks/non_asv/duckdb/profile_sql_breakdown.py
"""

import tempfile
import time

import numpy as np
import pandas as pd
import pyarrow as pa

from arcticdb import Arctic, QueryBuilder
from arcticdb.version_store.duckdb.pushdown import _get_sql_ast, extract_pushdown_from_sql

duckdb = __import__("duckdb")


def create_wide_dataframe(n_rows=100_000, n_float_cols=350, n_string_cols=50):
    rng = np.random.default_rng(42)
    dates = pd.date_range("2024-01-01", periods=n_rows, freq="min")
    data = {f"f{i}": rng.standard_normal(n_rows) for i in range(n_float_cols)}
    cats = ["A", "B", "C", "D", "E"]
    for i in range(n_string_cols):
        data[f"s{i}"] = rng.choice(cats, n_rows)
    df = pd.DataFrame(data, index=pd.DatetimeIndex(dates, name="Date"))
    print(f"DataFrame: {df.shape}, {df.memory_usage(deep=True).sum()/1024**2:.0f} MB")
    return df


def run_test(lib, label, sql_query, qb_func):
    """Run a single test: measure QB, lib.sql(), and break down SQL internals."""
    print(f"\n{'='*80}")
    print(f"  {label}")
    print(f"{'='*80}")

    # QB baseline
    t0 = time.perf_counter()
    qb_result = qb_func()
    t_qb = time.perf_counter() - t0
    print(f"  QueryBuilder:  {t_qb:.4f}s  {len(qb_result)} rows")

    # lib.sql() total
    t0 = time.perf_counter()
    sql_result = lib.sql(sql_query)
    t_sql = time.perf_counter() - t0
    print(f"  lib.sql():     {t_sql:.4f}s  {len(sql_result)} rows  ({t_sql/t_qb:.1f}x QB)")

    # --- Break down SQL internals ---
    print(f"\n  SQL Step Breakdown:")

    # 1. Pushdown extraction
    ast = _get_sql_ast(sql_query)
    index_columns = lib._resolve_index_columns_for_sql(ast)
    t0 = time.perf_counter()
    pushdown_by_table, symbols = extract_pushdown_from_sql(sql_query, index_columns=index_columns)
    t_pushdown = time.perf_counter() - t0
    pushdown = pushdown_by_table.get("sym")
    if pushdown:
        print(f"    1. Pushdown extraction:     {t_pushdown*1000:.1f}ms")
        print(f"       date_range={pushdown.date_range}")
        print(f"       columns={pushdown.columns[:5] if pushdown.columns else None}{'...' if pushdown.columns and len(pushdown.columns) > 5 else ''}")
        print(f"       query_builder={pushdown.query_builder}")

    # 2. _read_as_record_batch_reader
    lib_dynamic = lib.options().dynamic_schema
    t0 = time.perf_counter()
    reader = lib._read_as_record_batch_reader(
        "sym",
        date_range=pushdown.date_range if pushdown else None,
        columns=pushdown.columns if pushdown else None,
        dynamic_schema=lib_dynamic,
    )
    t_reader = time.perf_counter() - t0
    print(f"    2. _read_as_record_batch_reader: {t_reader*1000:.1f}ms")

    # 3. to_pyarrow_reader
    t0 = time.perf_counter()
    pa_reader = reader.to_pyarrow_reader()
    t_pa = time.perf_counter() - t0
    print(f"    3. to_pyarrow_reader():     {t_pa*1000:.1f}ms")

    # 4. Materialize batches
    t0 = time.perf_counter()
    batches = list(pa_reader)
    t_mat = time.perf_counter() - t0
    n_rows_total = sum(len(b) for b in batches)
    print(f"    4. Materialize batches:     {t_mat*1000:.1f}ms ({len(batches)} batches, {n_rows_total:,} rows)")

    # 5. DuckDB scan from Arrow table
    if batches:
        table = pa.Table.from_batches(batches, schema=batches[0].schema)
        conn = duckdb.connect(":memory:")
        conn.register("sym", table)
        t0 = time.perf_counter()
        _ = conn.execute("SELECT * FROM sym").fetch_arrow_table()
        t_duck = time.perf_counter() - t0
        print(f"    5. DuckDB scan (table):     {t_duck*1000:.1f}ms")
        conn.close()

    # 6. DuckDB on streaming reader
    reader2 = lib._read_as_record_batch_reader(
        "sym",
        date_range=pushdown.date_range if pushdown else None,
        columns=pushdown.columns if pushdown else None,
        dynamic_schema=lib_dynamic,
    )
    pa_reader2 = reader2.to_pyarrow_reader()
    conn = duckdb.connect(":memory:")
    conn.register("sym", pa_reader2)
    t0 = time.perf_counter()
    _ = conn.execute("SELECT * FROM sym").fetch_arrow_table()
    t_duck_stream = time.perf_counter() - t0
    print(f"    6. DuckDB scan (stream):    {t_duck_stream*1000:.1f}ms")
    conn.close()

    print(f"\n  Summary:")
    print(f"    QB total:              {t_qb*1000:.1f}ms")
    print(f"    SQL total:             {t_sql*1000:.1f}ms")
    print(f"    Batch materialization: {t_mat*1000:.1f}ms ({t_mat/max(t_sql,0.001)*100:.0f}% of SQL)")
    print(f"    Overhead vs QB:        {(t_sql - t_qb)*1000:.1f}ms")


def main():
    lmdb_dir = tempfile.mkdtemp(prefix="profile_sql_bd_")
    ac = Arctic(f"lmdb://{lmdb_dir}")
    lib = ac.create_library("test")

    df = create_wide_dataframe()
    t0 = time.perf_counter()
    lib.write("sym", df)
    print(f"Write: {time.perf_counter()-t0:.2f}s")

    date_lo, date_hi = "2024-01-10", "2024-01-15"

    # Warmup
    lib.read("sym", date_range=(pd.Timestamp(date_lo), pd.Timestamp(date_hi)))

    # Test 1: Date-range filter
    run_test(
        lib,
        "Date-range filter: SELECT * WHERE Date >= ... AND Date <= ...",
        f"SELECT * FROM sym WHERE Date >= '{date_lo}' AND Date <= '{date_hi}'",
        lambda: lib.read("sym", date_range=(pd.Timestamp(date_lo), pd.Timestamp(date_hi))).data,
    )

    # Test 2: Date-range + value filter
    run_test(
        lib,
        "Date + value filter: SELECT * WHERE Date range AND s0 = 'A'",
        f"SELECT * FROM sym WHERE Date >= '{date_lo}' AND Date <= '{date_hi}' AND s0 = 'A'",
        lambda: lib.read(
            "sym",
            date_range=(pd.Timestamp(date_lo), pd.Timestamp(date_hi)),
            query_builder=QueryBuilder()[QueryBuilder()["s0"] == "A"],
        ).data,
    )

    # Test 3: Column projection
    run_test(
        lib,
        "Column projection: SELECT 3 cols with date filter",
        f"SELECT f0, f1, s0 FROM sym WHERE Date >= '{date_lo}' AND Date <= '{date_hi}'",
        lambda: lib.read(
            "sym",
            columns=["f0", "f1", "s0"],
            date_range=(pd.Timestamp(date_lo), pd.Timestamp(date_hi)),
        ).data,
    )


if __name__ == "__main__":
    main()
