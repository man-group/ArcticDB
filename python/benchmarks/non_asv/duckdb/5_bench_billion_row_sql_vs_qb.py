"""
Copyright 2026 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

"""
Billion Row Challenge: SQL vs QueryBuilder with Polars Streaming Output
========================================================================

Compares CPU time and peak memory for the billion-row-challenge queries using:

  1. QueryBuilder (C++ pushdown) → Polars output
  2. SQL via lib.sql() (DuckDB streaming) → Polars output
  3. SQL via lib.sql() → Pandas output (baseline)
  4. Polars LazyFrame scanning the Arrow RecordBatchReader directly

The original billion row challenge uses 1B rows. For practical benchmarking
we default to 100M rows (configurable via --rows). The dataset schema mirrors
the original: City (string, 10K unique) and Temperature (float64).

Usage:
    python -m benchmarks.non_asv.duckdb.5_bench_billion_row_sql_vs_qb [--rows N] [--skip-write] [--repeats R]

Requirements:
    pip install polars  (in addition to arcticdb + duckdb)
"""

import argparse
import gc
import os
import sys
import time
import numpy as np
import pandas as pd

import arcticdb as adb
from arcticdb import QueryBuilder

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def get_rss_mb():
    """Current RSS in MB (Linux /proc, fallback to resource module)."""
    try:
        with open(f"/proc/{os.getpid()}/status") as f:
            for line in f:
                if line.startswith("VmRSS:"):
                    return int(line.split()[1]) / 1024  # kB → MB
    except Exception:
        pass
    import resource

    return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024


def measure(fn, repeats=3, label="", warmup=True):
    """Run fn() `repeats` times, report median wall-time and peak/delta RSS."""
    gc.collect()
    gc.collect()

    if warmup:
        result = fn()
        del result
        gc.collect()
        gc.collect()

    times = []
    rss_deltas = []
    peak_rss_values = []
    for i in range(repeats):
        gc.collect()
        gc.collect()
        time.sleep(0.1)  # Let OS reclaim pages
        rss_pre = get_rss_mb()

        t0 = time.perf_counter()
        result = fn()
        t1 = time.perf_counter()

        rss_holding = get_rss_mb()  # RSS while result is alive
        n_rows = len(result) if hasattr(result, "__len__") else "?"

        del result
        gc.collect()
        gc.collect()
        time.sleep(0.1)

        elapsed = t1 - t0
        times.append(elapsed)
        rss_deltas.append(rss_holding - rss_pre)
        peak_rss_values.append(rss_holding)
        print(f"    [{label}] iter {i+1}/{repeats}: {elapsed:.2f}s  rss={rss_holding:.0f}MB", flush=True)

    times.sort()
    rss_deltas.sort()
    median_s = times[len(times) // 2]
    median_rss_delta = rss_deltas[len(rss_deltas) // 2]
    return {
        "label": label,
        "median_s": median_s,
        "min_s": times[0],
        "max_s": times[-1],
        "rss_delta_mb": median_rss_delta,
        "peak_rss_mb": max(peak_rss_values),
        "result_rows": n_rows,
    }


def fmt_result(r):
    return (
        f"  {r['label']:45s}  "
        f"time={r['median_s']:8.2f}s  "
        f"(min={r['min_s']:.2f} max={r['max_s']:.2f})  "
        f"rss_delta={r['rss_delta_mb']:+8.0f}MB  "
        f"peak_rss={r['peak_rss_mb']:8.0f}MB  "
        f"rows={r['result_rows']}"
    )


# ---------------------------------------------------------------------------
# Data setup
# ---------------------------------------------------------------------------


def write_data(lib, sym, num_rows, rows_per_segment=100_000):
    """Write the billion-row-challenge dataset (City + Temperature)."""
    rng = np.random.default_rng(42)
    num_cities = 10_000
    cities = [f"City {idx:04}" for idx in range(num_cities)]

    num_segments = num_rows // rows_per_segment
    remainder = num_rows % rows_per_segment

    print(f"Writing {num_rows:,} rows in {num_segments + (1 if remainder else 0)} segments...", flush=True)
    t0 = time.perf_counter()

    for idx in range(num_segments):
        if (idx + 1) % 100 == 0 or idx == 0:
            elapsed = time.perf_counter() - t0
            print(f"  segment {idx + 1}/{num_segments}  ({elapsed:.1f}s elapsed)", flush=True)
        temperature = 100_000 * rng.random(rows_per_segment)
        city_col = rng.choice(cities, rows_per_segment)
        df = pd.DataFrame({"City": city_col, "Temperature": temperature})
        if idx == 0:
            lib.write(sym, df)
        else:
            lib.append(sym, df)

    if remainder:
        temperature = 100_000 * rng.random(remainder)
        city_col = rng.choice(cities, remainder)
        df = pd.DataFrame({"City": city_col, "Temperature": temperature})
        if num_segments == 0:
            lib.write(sym, df)
        else:
            lib.append(sym, df)

    elapsed = time.perf_counter() - t0
    print(f"Write complete: {num_rows:,} rows in {elapsed:.1f}s", flush=True)


# ---------------------------------------------------------------------------
# Benchmark functions
# ---------------------------------------------------------------------------


def bench_groupby(lib, sym, repeats, num_rows=0):
    """GROUP BY City with min/max/mean Temperature."""
    print(f"\n{'='*80}")
    print("  GROUPBY: City → MIN/MAX/MEAN(Temperature)")
    print(f"{'='*80}")

    # At >500M rows the Polars LazyFrame path OOMs (materializes all data in Arrow)
    skip_lazy_full = num_rows > 500_000_000

    results = []

    # 1. QueryBuilder → Polars
    def qb_polars():
        q = QueryBuilder()
        q = q.groupby("City").agg(
            {
                "Min": ("Temperature", "min"),
                "Max": ("Temperature", "max"),
                "Mean": ("Temperature", "mean"),
            }
        )
        return lib.read(sym, query_builder=q, output_format="polars").data

    results.append(measure(qb_polars, repeats, "QueryBuilder → Polars", warmup=False))

    # 2. SQL → Polars
    sql = (
        f"SELECT City, MIN(Temperature) AS Min, MAX(Temperature) AS Max, "
        f"AVG(Temperature) AS Mean FROM {sym} GROUP BY City"
    )

    def sql_polars():
        return lib.sql(sql, output_format="polars")

    results.append(measure(sql_polars, repeats, "SQL (lib.sql) → Polars"))

    # 3. SQL → Pandas (baseline)
    def sql_pandas():
        return lib.sql(sql, output_format="pandas")

    results.append(measure(sql_pandas, repeats, "SQL (lib.sql) → Pandas"))

    # 4. Polars LazyFrame scanning Arrow RecordBatchReader
    if not skip_lazy_full:

        def polars_lazy_scan():
            import polars as pl

            reader, _ = lib._read_as_record_batch_reader(sym)
            pa_reader = reader.to_pyarrow_reader()
            lf = pl.LazyFrame(pa_reader)
            return (
                lf.group_by("City")
                .agg(
                    pl.col("Temperature").min().alias("Min"),
                    pl.col("Temperature").max().alias("Max"),
                    pl.col("Temperature").mean().alias("Mean"),
                )
                .collect()
            )

        results.append(measure(polars_lazy_scan, repeats, "Polars LazyFrame (streaming)", warmup=False))
    else:
        print("  [SKIP] Polars LazyFrame (streaming) — would OOM at this scale")

    # 5. QueryBuilder → Pandas (original benchmark equivalent)
    def qb_pandas():
        q = QueryBuilder()
        q = q.groupby("City").agg(
            {
                "Min": ("Temperature", "min"),
                "Max": ("Temperature", "max"),
                "Mean": ("Temperature", "mean"),
            }
        )
        return lib.read(sym, query_builder=q).data

    results.append(measure(qb_pandas, repeats, "QueryBuilder → Pandas", warmup=False))

    for r in results:
        print(fmt_result(r))

    return results


def bench_filter(lib, sym, repeats, num_rows=0):
    """Filter: City == 'City 9999'."""
    print(f"\n{'='*80}")
    print("  FILTER: City = 'City 9999'")
    print(f"{'='*80}")

    skip_lazy_full = num_rows > 500_000_000

    results = []

    # 1. QueryBuilder → Polars
    def qb_polars():
        q = QueryBuilder()
        q = q[q["City"] == "City 9999"]
        return lib.read(sym, query_builder=q, output_format="polars").data

    results.append(measure(qb_polars, repeats, "QueryBuilder → Polars"))

    # 2. SQL → Polars
    sql = f"SELECT * FROM {sym} WHERE City = 'City 9999'"

    def sql_polars():
        return lib.sql(sql, output_format="polars")

    results.append(measure(sql_polars, repeats, "SQL (lib.sql) → Polars"))

    # 3. SQL → Pandas
    def sql_pandas():
        return lib.sql(sql, output_format="pandas")

    results.append(measure(sql_pandas, repeats, "SQL (lib.sql) → Pandas"))

    # 4. Polars LazyFrame scan
    if not skip_lazy_full:

        def polars_lazy_scan():
            import polars as pl

            reader, _ = lib._read_as_record_batch_reader(sym)
            pa_reader = reader.to_pyarrow_reader()
            lf = pl.LazyFrame(pa_reader)
            return lf.filter(pl.col("City") == "City 9999").collect()

        results.append(measure(polars_lazy_scan, repeats, "Polars LazyFrame (streaming)"))
    else:
        print("  [SKIP] Polars LazyFrame (streaming) — would OOM at this scale")

    # 5. QueryBuilder → Pandas
    def qb_pandas():
        q = QueryBuilder()
        q = q[q["City"] == "City 9999"]
        return lib.read(sym, query_builder=q).data

    results.append(measure(qb_pandas, repeats, "QueryBuilder → Pandas"))

    for r in results:
        print(fmt_result(r))

    return results


def bench_project_filter(lib, sym, repeats, num_rows=0):
    """Project Temperature*1.5 then filter City == 'City 9999'."""
    print(f"\n{'='*80}")
    print("  PROJECT + FILTER: Temperature*1.5, City = 'City 9999'")
    print(f"{'='*80}")

    skip_lazy_full = num_rows > 500_000_000

    results = []

    # 1. QueryBuilder → Polars
    def qb_polars():
        q = QueryBuilder()
        q = q.apply("new_col", q["Temperature"] * 1.5)
        q = q[q["City"] == "City 9999"]
        return lib.read(sym, query_builder=q, output_format="polars").data

    results.append(measure(qb_polars, repeats, "QueryBuilder → Polars"))

    # 2. SQL → Polars (projection in SQL)
    sql = f"SELECT City, Temperature, Temperature * 1.5 AS new_col " f"FROM {sym} WHERE City = 'City 9999'"

    def sql_polars():
        return lib.sql(sql, output_format="polars")

    results.append(measure(sql_polars, repeats, "SQL (lib.sql) → Polars"))

    # 3. SQL → Pandas
    def sql_pandas():
        return lib.sql(sql, output_format="pandas")

    results.append(measure(sql_pandas, repeats, "SQL (lib.sql) → Pandas"))

    # 4. Polars LazyFrame scan with lazy expression
    if not skip_lazy_full:

        def polars_lazy_scan():
            import polars as pl

            reader, _ = lib._read_as_record_batch_reader(sym)
            pa_reader = reader.to_pyarrow_reader()
            lf = pl.LazyFrame(pa_reader)
            return (
                lf.with_columns((pl.col("Temperature") * 1.5).alias("new_col"))
                .filter(pl.col("City") == "City 9999")
                .collect()
            )

        results.append(measure(polars_lazy_scan, repeats, "Polars LazyFrame (streaming)"))
    else:
        print("  [SKIP] Polars LazyFrame (streaming) — would OOM at this scale")

    # 5. QueryBuilder → Pandas (original benchmark)
    def qb_pandas():
        q = QueryBuilder()
        q = q.apply("new_col", q["Temperature"] * 1.5)
        q = q[q["City"] == "City 9999"]
        return lib.read(sym, query_builder=q).data

    results.append(measure(qb_pandas, repeats, "QueryBuilder → Pandas"))

    for r in results:
        print(fmt_result(r))

    return results


def bench_full_scan(lib, sym, repeats, num_rows=0):
    """SELECT * — full table read (stress test for streaming memory)."""
    print(f"\n{'='*80}")
    print("  FULL SCAN: SELECT *")
    print(f"{'='*80}")

    # Full scan at >500M rows will OOM — skip materializing paths
    skip_materialize = num_rows > 500_000_000
    if skip_materialize:
        print(f"  NOTE: {num_rows:,} rows — skipping full-materialize paths to avoid OOM")

    results = []

    if not skip_materialize:
        # 1. QueryBuilder → Polars (just a plain read)
        def qb_polars():
            return lib.read(sym, output_format="polars").data

        results.append(measure(qb_polars, repeats, "read() → Polars", warmup=False))

        # 2. SQL → Polars
        sql_full = f"SELECT * FROM {sym}"

        def sql_polars():
            return lib.sql(sql_full, output_format="polars")

        results.append(measure(sql_polars, repeats, "SQL (lib.sql) → Polars", warmup=False))

        # 3. Polars LazyFrame scan (streams through Arrow batches)
        def polars_lazy_scan():
            import polars as pl

            reader, _ = lib._read_as_record_batch_reader(sym)
            pa_reader = reader.to_pyarrow_reader()
            return pl.LazyFrame(pa_reader).collect()

        results.append(measure(polars_lazy_scan, repeats, "Polars LazyFrame (streaming)", warmup=False))

        # 4. read() → Pandas (baseline)
        def qb_pandas():
            return lib.read(sym).data

        results.append(measure(qb_pandas, repeats, "read() → Pandas", warmup=False))
    else:
        # At billion-row scale, only measure row-count via streaming (no materialize)
        def streaming_count():
            """Count rows via streaming — never materializes full table."""
            reader, _ = lib._read_as_record_batch_reader(sym)
            total = 0
            while True:
                batch = reader.read_next_batch()
                if batch is None:
                    break
                total += batch.num_rows
            return [total]  # Return list so len() works

        results.append(measure(streaming_count, repeats, "Streaming row count (Arrow)", warmup=False))

    for r in results:
        print(fmt_result(r))

    return results


# ---------------------------------------------------------------------------
# Summary report
# ---------------------------------------------------------------------------


def print_summary(all_results):
    """Print a comparison table."""
    print(f"\n\n{'#'*80}")
    print("  SUMMARY: CPU time (median seconds) and peak traced memory (MB)")
    print(f"{'#'*80}\n")

    # Group by benchmark
    benchmarks = {}
    for benchmark_name, results in all_results:
        benchmarks[benchmark_name] = results

    # Header
    labels = set()
    for results in benchmarks.values():
        for r in results:
            labels.add(r["label"])
    labels = sorted(labels)

    # Table
    header = f"{'Benchmark':30s}"
    for label in labels:
        short = label[:22]
        header += f"  {short:>24s}"
    print(header)
    print("-" * len(header))

    for bench_name, results in benchmarks.items():
        row_time = f"{'  ' + bench_name + ' (time)':30s}"
        row_mem = f"{'  ' + bench_name + ' (rss Δ)':30s}"
        result_map = {r["label"]: r for r in results}
        for label in labels:
            r = result_map.get(label)
            if r:
                row_time += f"  {r['median_s']:>22.2f}s "
                row_mem += f"  {r['rss_delta_mb']:>+20.0f}MB "
            else:
                row_time += f"  {'N/A':>24s}"
                row_mem += f"  {'N/A':>24s}"
        print(row_time)
        print(row_mem)
        print()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main():
    parser = argparse.ArgumentParser(description="Billion Row Challenge: SQL vs QueryBuilder")
    parser.add_argument("--rows", type=int, default=100_000_000, help="Number of rows (default: 100M)")
    parser.add_argument("--skip-write", action="store_true", help="Skip data generation (reuse existing data)")
    parser.add_argument("--repeats", type=int, default=3, help="Number of measurement repeats (default: 3)")
    parser.add_argument(
        "--uri", type=str, default="lmdb:///tmp/arcticdb_billion_row_bench", help="ArcticDB connection URI"
    )
    parser.add_argument(
        "--benchmarks",
        type=str,
        default="all",
        help="Comma-separated list: groupby,filter,project_filter,full_scan,all",
    )
    args = parser.parse_args()

    # Check polars is available
    try:
        import polars as pl

        print(f"Polars version: {pl.__version__}")
    except ImportError:
        print("ERROR: polars is required. Install with: pip install polars")
        sys.exit(1)

    import duckdb as ddb

    print(f"DuckDB version: {ddb.__version__}")
    print(f"ArcticDB version: {adb.__version__}")
    print(f"Rows: {args.rows:,}")
    print(f"Repeats: {args.repeats}")
    print(f"URI: {args.uri}")

    ac = adb.Arctic(args.uri)
    lib = ac.get_library("billion_row_bench", create_if_missing=True)
    sym = "billion_row_challenge"

    if not args.skip_write:
        if lib.has_symbol(sym):
            lib.delete(sym)
        write_data(lib, sym, args.rows)
    else:
        if not lib.has_symbol(sym):
            print(f"ERROR: Symbol '{sym}' not found. Run without --skip-write first.")
            sys.exit(1)
        info = lib.get_description(sym)
        print(f"Reusing existing data: {info.row_count:,} rows, {len(info.columns)} columns")

    # Select benchmarks
    bench_names = args.benchmarks.split(",")
    run_all = "all" in bench_names

    all_results = []

    if run_all or "groupby" in bench_names:
        results = bench_groupby(lib, sym, args.repeats, num_rows=args.rows)
        all_results.append(("groupby", results))

    if run_all or "filter" in bench_names:
        results = bench_filter(lib, sym, args.repeats, num_rows=args.rows)
        all_results.append(("filter", results))

    if run_all or "project_filter" in bench_names:
        results = bench_project_filter(lib, sym, args.repeats, num_rows=args.rows)
        all_results.append(("project+filter", results))

    if run_all or "full_scan" in bench_names:
        results = bench_full_scan(lib, sym, args.repeats, num_rows=args.rows)
        all_results.append(("full_scan", results))

    print_summary(all_results)


if __name__ == "__main__":
    main()
