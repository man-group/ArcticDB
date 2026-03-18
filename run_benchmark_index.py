"""
Run column stats index key benchmarks.

Measures the performance of reading/deleting with column stats stored in:
  - A dedicated COLUMN_STATS key (current approach)
  - Embedded in the TABLE_INDEX key (new approach, requires C++ implementation)

Measurements:
  0. read          - Single read, no filter
  1. filtered      - Single read with QueryBuilder filter (~1% of data)
  2. delete        - Single delete (re-writes before each timed delete)
  3. batch_read    - Batch read (no filter), M requests for the same symbol
  4. write         - Single write (no stats, separate stats, embedded stats)
  5. batch_write   - Batch write of M symbols (same three modes)
  6. batch_delete  - Batch delete of M symbols
  7. sizes         - Report TABLE_INDEX, COLUMN_STATS, and TABLE_DATA key sizes

Usage:
    python run_benchmark_index.py --backends lmdb --sizes small --columns 2 --runs 3
    python run_benchmark_index.py --backends vast --sizes medium --columns 10 --runs 10
    python run_benchmark_index.py --backends vast --sizes medium --columns 10 --benchmarks filtered
"""

import argparse
import os
import statistics
import sys
import time

import numpy as np
import pandas as pd
from arcticdb import Arctic
from arcticdb.options import LibraryOptions
from arcticdb.version_store.library import WritePayload
from arcticdb.version_store.processing import QueryBuilder
import arcticdb_ext
from arcticdb_ext import set_config_int, unset_config_int
from arcticdb_ext.storage import KeyType

# Verify arcticdb_ext is loaded from the expected symlink
_expected_so = os.path.join(os.path.dirname(__file__), "python", "arcticdb_ext.cpython-310-x86_64-linux-gnu.so")
_actual = os.path.realpath(arcticdb_ext.__file__)
_expected = os.path.realpath(_expected_so)
if _actual != _expected:
    print(f"ERROR: arcticdb_ext loaded from unexpected location", file=sys.stderr)
    print(f"  Expected (via symlink): {_expected}", file=sys.stderr)
    print(f"  Actual:                 {_actual}", file=sys.stderr)
    sys.exit(1)

# ── Configuration (must match create_benchmark_data_index.py) ──────────────────

BACKENDS = {
    "vast": {
        "uri": (
            "s3://arctic-data.vast.gdc.storage.res.m:"
            "alpha-data-dev-arcticnative-ahl-research-3"
            "?aws_auth=true&path_prefix=aseaton_tst"
        ),
        "env": {"AWS_PROFILE": "research-3"},
    },
    "aws": {
        "uri": (
            "s3://s3.eu-west-2.amazonaws.com:aseaton-tickdata"
            "?aws_auth=true"
            "&path_prefix=aseaton_tst"
        ),
    },
    "lmdb": {
        "uri": "lmdb:///tmp/arcticdb_benchmark",
    },
}

SIZES = {
    "small": 100,
    "medium": 1_000_000,
    "large": 10_000_000,
}

ROWS_PER_SEGMENT = 100_000
BASE_DATE = pd.Timestamp("2020-01-01")

LIB_SEPARATE = "csbench_separate"
LIB_EMBEDDED = "csbench_embedded"


# ── Helpers ────────────────────────────────────────────────────────────────────

def symbol_name(nrows, ncols):
    return f"bench_{nrows}_{ncols}"


def generate_dataframe(nrows, ncols):
    """Generate a benchmark DataFrame (same logic as create script)."""
    index = pd.date_range(BASE_DATE, periods=nrows, freq="864ms")
    pub_dates = pd.date_range(BASE_DATE, periods=nrows, freq="864ms")

    data = {"publication_date": pub_dates}
    for i in range(ncols - 1):
        data[f"value-{i}"] = np.random.randn(nrows)

    return pd.DataFrame(data, index=index)


def get_arctic(backend_name):
    cfg = BACKENDS[backend_name]
    for k, v in cfg.get("env", {}).items():
        os.environ[k] = v
    return Arctic(cfg["uri"])


def compute_query_window(nrows):
    """Return (start, end) timestamps selecting ~1% of the publication_date range."""
    num_segments = max(1, nrows // ROWS_PER_SEGMENT)
    total_days = num_segments
    query_days = max(0.01, total_days * 0.01)
    mid_day = total_days / 2.0
    start = BASE_DATE + pd.Timedelta(days=mid_day - query_days / 2)
    end = BASE_DATE + pd.Timedelta(days=mid_day + query_days / 2)
    return start, end


def make_filter(query_start, query_end):
    q = QueryBuilder()
    return q[(q["publication_date"] >= query_start) & (q["publication_date"] < query_end)]


def print_timing(label, timings):
    avg = statistics.mean(timings)
    sd = statistics.stdev(timings) if len(timings) > 1 else 0.0
    print(f"  {label:50s} avg={avg:.3f}s  min={min(timings):.3f}s  max={max(timings):.3f}s  stddev={sd:.3f}s")


def print_batch_timing(label, timings, batch_size):
    avg = statistics.mean(timings)
    sd = statistics.stdev(timings) if len(timings) > 1 else 0.0
    per_sym = avg / batch_size
    print(
        f"  {label:50s} avg={avg:.3f}s  min={min(timings):.3f}s  max={max(timings):.3f}s"
        f"  stddev={sd:.3f}s  per_symbol={per_sym:.4f}s"
    )


# ── Benchmark functions ────────────────────────────────────────────────────────

def benchmark_single_read(lib, symbol, num_runs):
    """Measurement 0: single read, no filter."""
    timings = []
    for _ in range(num_runs):
        t0 = time.perf_counter()
        lib.read(symbol).data
        timings.append(time.perf_counter() - t0)
    return timings


def benchmark_filtered_read(lib, symbol, nrows, num_runs, use_for_queries, embed_in_index):
    """Measurement 1: single read with QueryBuilder filter on publication_date."""
    set_config_int("ColumnStats.UseForQueries", 1 if use_for_queries else 0)
    if embed_in_index:
        set_config_int("ColumnStats.EmbedInIndex", 1)

    query_start, query_end = compute_query_window(nrows)
    timings = []
    for _ in range(num_runs):
        q = make_filter(query_start, query_end)
        t0 = time.perf_counter()
        lib.read(symbol, query_builder=q).data
        timings.append(time.perf_counter() - t0)

    set_config_int("ColumnStats.UseForQueries", 0)
    if embed_in_index:
        unset_config_int("ColumnStats.EmbedInIndex")

    return timings


def benchmark_single_delete(lib, symbol, df, num_runs, is_separate, embed_in_index=False):
    """Measurement 2: single delete (re-writes symbol before each timed delete)."""
    timings = []
    for _ in range(num_runs):
        if embed_in_index:
            set_config_int("ColumnStats.EmbedInIndex", 1)
        lib.write(symbol, df)
        if is_separate:
            lib._nvs.create_column_stats(symbol, {"publication_date": {"MINMAX"}})
        if embed_in_index:
            unset_config_int("ColumnStats.EmbedInIndex")

        t0 = time.perf_counter()
        lib.delete(symbol)
        timings.append(time.perf_counter() - t0)
    return timings


def benchmark_batch_read(lib, symbol, num_runs, batch_size):
    """Measurement 3: batch read (no filter), M requests for the same symbol."""
    timings = []
    for _ in range(num_runs):
        t0 = time.perf_counter()
        lib.read_batch([symbol] * batch_size)
        timings.append(time.perf_counter() - t0)

    return timings


def benchmark_batch_delete(lib, symbol_prefix, df, batch_size, num_runs, is_separate, embed_in_index=False):
    """Measurement 6: batch delete of M symbols (writes them fresh before each timed delete)."""
    timings = []
    for _ in range(num_runs):
        batch_symbols = [f"{symbol_prefix}_batch_{i}" for i in range(batch_size)]
        if embed_in_index:
            set_config_int("ColumnStats.EmbedInIndex", 1)
        payloads = [WritePayload(sym, df) for sym in batch_symbols]
        lib.write_batch(payloads)
        if is_separate:
            for sym in batch_symbols:
                lib._nvs.create_column_stats(sym, {"publication_date": {"MINMAX"}})
        if embed_in_index:
            unset_config_int("ColumnStats.EmbedInIndex")

        t0 = time.perf_counter()
        lib.delete_batch(batch_symbols)
        timings.append(time.perf_counter() - t0)
    return timings


def benchmark_single_write(lib, df, sym_prefix, num_runs, mode):
    """Measurement 4: single write.
    mode: 'none' (no column stats), 'separate' (write + create_column_stats),
          'embedded' (write with EmbedInIndex=1)
    """
    if mode == 'embedded':
        set_config_int("Statistics.GenerateOnWrite", 1)
        set_config_int("ColumnStats.EmbedInIndex", 1)

    timings = []
    for i in range(num_runs):
        sym = f"{sym_prefix}_write_{mode}_{i}"
        t0 = time.perf_counter()
        lib.write(sym, df)
        if mode == 'separate':
            lib._nvs.create_column_stats(sym, {"publication_date": {"MINMAX"}})
        timings.append(time.perf_counter() - t0)

    if mode == 'embedded':
        unset_config_int("Statistics.GenerateOnWrite")
        unset_config_int("ColumnStats.EmbedInIndex")

    return timings


def benchmark_batch_write(lib, df, sym_prefix, num_runs, batch_size, mode):
    """Measurement 5: batch write.
    mode: 'none' (no column stats), 'separate' (write_batch + create_column_stats per symbol),
          'embedded' (write_batch with EmbedInIndex=1)
    """
    if mode == 'embedded':
        set_config_int("Statistics.GenerateOnWrite", 1)
        set_config_int("ColumnStats.EmbedInIndex", 1)

    timings = []
    for i in range(num_runs):
        syms = [f"{sym_prefix}_bwrite_{mode}_{i}_{j}" for j in range(batch_size)]
        payloads = [WritePayload(sym, df) for sym in syms]
        t0 = time.perf_counter()
        lib.write_batch(payloads)
        if mode == 'separate':
            for sym in syms:
                lib._nvs.create_column_stats(sym, {"publication_date": {"MINMAX"}})
        timings.append(time.perf_counter() - t0)

    if mode == 'embedded':
        unset_config_int("Statistics.GenerateOnWrite")
        unset_config_int("ColumnStats.EmbedInIndex")

    return timings


def report_key_sizes(lib, symbol, label):
    """Report compressed sizes of TABLE_INDEX, COLUMN_STATS, and TABLE_DATA keys."""
    sizes = lib._nvs.version_store.scan_object_sizes_for_stream(symbol)
    by_type = {}
    for s in sizes:
        by_type[s.key_type] = (s.count, s.compressed_size)

    print(f"  {label}")
    for kt in (KeyType.TABLE_INDEX, KeyType.COLUMN_STATS, KeyType.TABLE_DATA):
        count, compressed = by_type.get(kt, (0, 0))
        if compressed >= 1_000_000:
            size_str = f"{compressed / 1_000_000:.2f} MB"
        elif compressed >= 1_000:
            size_str = f"{compressed / 1_000:.2f} KB"
        else:
            size_str = f"{compressed} B"
        print(f"    {kt.name:20s}  count={count:>4d}  compressed={size_str}")


# ── Main benchmark runner ──────────────────────────────────────────────────────

def run_benchmarks(backend_name, nrows, ncols, num_runs, batch_size, benchmarks):
    ac = get_arctic(backend_name)
    lib_opts = LibraryOptions(rows_per_segment=ROWS_PER_SEGMENT)
    lib_sep = ac.get_library(LIB_SEPARATE, create_if_missing=True, library_options=lib_opts)
    lib_emb = ac.get_library(LIB_EMBEDDED, create_if_missing=True, library_options=lib_opts)

    sym = symbol_name(nrows, ncols)
    num_segments = max(1, nrows // ROWS_PER_SEGMENT)

    header = f"Backend: {backend_name} | Rows: {nrows:,} | Columns: {ncols} | Segments: {num_segments}"
    print("=" * len(header))
    print(header)
    print("=" * len(header))

    # ── Key Sizes ──
    if "sizes" in benchmarks:
        print(f"\n--- Key Sizes ---")
        report_key_sizes(lib_sep, sym, "Separate library:")
        report_key_sizes(lib_emb, sym, "Embedded library:")

    # ── Measurement 0: Single Read (no filter) ──
    if "read" in benchmarks:
        print(f"\n--- Single Read (no filter, {num_runs} runs) ---")
        timings = benchmark_single_read(lib_sep, sym, num_runs)
        print_timing("Separate library:", timings)

        timings = benchmark_single_read(lib_emb, sym, num_runs)
        print_timing("Embedded library:", timings)

    # ── Measurement 1: Single Read (filtered) ──
    if "filtered" in benchmarks:
        query_start, query_end = compute_query_window(nrows)
        print(f"\n--- Single Read (filtered ~1%, {num_runs} runs) ---")
        print(f"    Query: publication_date in [{query_start}, {query_end})")

        timings = benchmark_filtered_read(lib_sep, sym, nrows, num_runs, False, False)
        print_timing("Column stats OFF:", timings)

        timings = benchmark_filtered_read(lib_sep, sym, nrows, num_runs, True, False)
        print_timing("Column stats ON:", timings)

        timings = benchmark_filtered_read(lib_emb, sym, nrows, num_runs, False, False)
        print_timing("Index-embedded, column stats OFF:", timings)

        timings = benchmark_filtered_read(lib_emb, sym, nrows, num_runs, True, True)
        print_timing("Index-embedded, column stats ON:", timings)

    # ── Measurement 3: Batch Read (no filter) ──
    # Skip batch benchmarks for large data to avoid OOM (>100 cols or >1M rows)
    batch_eligible = ncols <= 100 and nrows <= 1_000_000
    if "batch_read" in benchmarks and batch_eligible:
        print(f"\n--- Batch Read (M={batch_size}, no filter, {num_runs} runs) ---")

        timings = benchmark_batch_read(lib_sep, sym, num_runs, batch_size)
        print_batch_timing("Separate library:", timings, batch_size)

        timings = benchmark_batch_read(lib_emb, sym, num_runs, batch_size)
        print_batch_timing("Embedded library:", timings, batch_size)
    elif "batch_read" in benchmarks:
        print(f"\n--- Batch Read: SKIPPED (ncols={ncols}, nrows={nrows:,} exceeds batch limits) ---")

    # Generate DataFrame for write/delete benchmarks
    df = None
    need_df = ("write" in benchmarks or "delete" in benchmarks
               or ("batch_write" in benchmarks and batch_eligible)
               or ("batch_delete" in benchmarks and batch_eligible))
    if need_df:
        print(f"\n  (generating DataFrame for write/delete benchmarks...)")
        df = generate_dataframe(nrows, ncols)

    # ── Measurement 4: Single Write ──
    if "write" in benchmarks:
        print(f"\n--- Single Write ({num_runs} runs) ---")

        timings = benchmark_single_write(lib_sep, df, sym, num_runs, 'none')
        print_timing("No column stats:", timings)

        timings = benchmark_single_write(lib_sep, df, sym, num_runs, 'separate')
        print_timing("Separate column stats (write + create_column_stats):", timings)

        timings = benchmark_single_write(lib_emb, df, sym, num_runs, 'embedded')
        print_timing("Embedded column stats:", timings)

    # ── Measurement 5: Batch Write ──
    if "batch_write" in benchmarks and batch_eligible:
        print(f"\n--- Batch Write (M={batch_size}, {num_runs} runs) ---")

        timings = benchmark_batch_write(lib_sep, df, sym, num_runs, batch_size, 'none')
        print_batch_timing("No column stats:", timings, batch_size)

        timings = benchmark_batch_write(lib_sep, df, sym, num_runs, batch_size, 'separate')
        print_batch_timing("Separate column stats (write + create_column_stats):", timings, batch_size)

        timings = benchmark_batch_write(lib_emb, df, sym, num_runs, batch_size, 'embedded')
        print_batch_timing("Embedded column stats:", timings, batch_size)
    elif "batch_write" in benchmarks:
        print(f"\n--- Batch Write: SKIPPED (ncols={ncols}, nrows={nrows:,} exceeds batch limits) ---")

    # ── Measurement 2: Single Delete ──
    if "delete" in benchmarks:
        print(f"\n--- Single Delete ({num_runs} runs, re-writes before each) ---")

        timings = benchmark_single_delete(lib_sep, sym, df, num_runs, True, False)
        print_timing("Separate library:", timings)

        timings = benchmark_single_delete(lib_emb, sym, df, num_runs, False, True)
        print_timing("Embedded library:", timings)

    # ── Measurement 6: Batch Delete ──
    if "batch_delete" in benchmarks and batch_eligible:
        print(f"\n--- Batch Delete (M={batch_size}, {num_runs} runs) ---")

        timings = benchmark_batch_delete(lib_sep, sym, df, batch_size, num_runs, True, False)
        print_batch_timing("Separate library:", timings, batch_size)

        timings = benchmark_batch_delete(lib_emb, sym, df, batch_size, num_runs, False, True)
        print_batch_timing("Embedded library:", timings, batch_size)
    elif "batch_delete" in benchmarks:
        print(f"\n--- Batch Delete: SKIPPED (ncols={ncols}, nrows={nrows:,} exceeds batch limits) ---")

    print()


def main():
    parser = argparse.ArgumentParser(description="Run column stats index key benchmarks")
    parser.add_argument("--backends", nargs="+", choices=list(BACKENDS.keys()), required=True)
    parser.add_argument("--sizes", nargs="+", choices=list(SIZES.keys()), required=True)
    parser.add_argument("--columns", nargs="+", type=int, required=True)
    parser.add_argument("--runs", type=int, default=10, help="Number of timed repetitions per measurement")
    parser.add_argument("--batch-size", type=int, default=50, help="M for batch operations")
    parser.add_argument(
        "--benchmarks", nargs="+",
        default=["sizes", "read", "filtered", "write", "batch_write", "delete", "batch_read", "batch_delete"],
        choices=["sizes", "read", "filtered", "write", "batch_write", "delete", "batch_read", "batch_delete"],
        help="Which measurements to run",
    )
    args = parser.parse_args()

    set_config_int("VersionStore.NumCPUThreads", 16)
    set_config_int("VersionStore.NumIOThreads", 24)

    for backend in args.backends:
        for size_label in args.sizes:
            nrows = SIZES[size_label]
            for ncols in args.columns:
                run_benchmarks(backend, nrows, ncols, args.runs, args.batch_size, args.benchmarks)


if __name__ == "__main__":
    main()
