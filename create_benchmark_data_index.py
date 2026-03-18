"""
Create benchmark data for column stats in index key vs dedicated COLUMN_STATS key.

Writes to two libraries per backend:
  - csbench_separate: column stats stored in a dedicated COLUMN_STATS key
  - csbench_embedded: column stats embedded in the TABLE_INDEX key (requires C++ implementation)

Usage:
    python create_benchmark_data_index.py --backends lmdb --sizes small --columns 2
    python create_benchmark_data_index.py --backends vast aws --sizes small medium --columns 2 10 100
"""

import argparse
import os
import sys
import time

import numpy as np
import pandas as pd
from arcticdb import Arctic
from arcticdb.options import LibraryOptions
import arcticdb_ext
from arcticdb_ext import set_config_int, unset_config_int

# Verify arcticdb_ext is loaded from the expected symlink
_expected_so = os.path.join(os.path.dirname(__file__), "python", "arcticdb_ext.cpython-310-x86_64-linux-gnu.so")
_actual = os.path.realpath(arcticdb_ext.__file__)
_expected = os.path.realpath(_expected_so)
if _actual != _expected:
    print(f"ERROR: arcticdb_ext loaded from unexpected location", file=sys.stderr)
    print(f"  Expected (via symlink): {_expected}", file=sys.stderr)
    print(f"  Actual:                 {_actual}", file=sys.stderr)
    sys.exit(1)

# ── Backend Configuration ──────────────────────────────────────────────────────

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


def symbol_name(nrows, ncols):
    return f"bench_{nrows}_{ncols}"


def generate_dataframe(nrows, ncols):
    """Generate a benchmark DataFrame with a DatetimeIndex, a monotonically increasing
    publication_date column, and (ncols - 1) float64 value columns."""
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


def write_symbol(lib, sym, total_rows, ncols, embed_in_index=False):
    """Write data in a single call. When embed_in_index is True, sets both
    Statistics.GenerateOnWrite and ColumnStats.EmbedInIndex so that column
    stats are embedded in the TABLE_INDEX key at write time."""
    if embed_in_index:
        set_config_int("Statistics.GenerateOnWrite", 1)
        set_config_int("ColumnStats.EmbedInIndex", 1)

    df = generate_dataframe(total_rows, ncols)
    lib.write(sym, df)

    if embed_in_index:
        unset_config_int("Statistics.GenerateOnWrite")
        unset_config_int("ColumnStats.EmbedInIndex")


def create_data(backend_name, nrows, ncols):
    ac = get_arctic(backend_name)
    lib_opts = LibraryOptions(rows_per_segment=ROWS_PER_SEGMENT)
    lib_sep = ac.get_library(LIB_SEPARATE, create_if_missing=True, library_options=lib_opts)
    lib_emb = ac.get_library(LIB_EMBEDDED, create_if_missing=True, library_options=lib_opts)

    sym = symbol_name(nrows, ncols)
    est_gb = nrows * (ncols + 1) * 8 / 1e9
    print(f"[{backend_name}] {nrows:,} rows x {ncols} cols (~{est_gb:.1f} GB in memory)")

    # ── Separate-stats library: write + create_column_stats ──
    print(f"  Writing to {LIB_SEPARATE}/{sym}...")
    t0 = time.perf_counter()
    write_symbol(lib_sep, sym, nrows, ncols)
    print(f"    Write: {time.perf_counter() - t0:.1f}s")

    print(f"  Creating COLUMN_STATS key on {LIB_SEPARATE}/{sym}...")
    t0 = time.perf_counter()
    lib_sep._nvs.create_column_stats(sym, {"publication_date": {"MINMAX"}})
    print(f"    Column stats: {time.perf_counter() - t0:.1f}s")

    # ── Embedded-stats library: write with EmbedInIndex flag ──
    # When the C++ implementation is complete, this embeds stats in the TABLE_INDEX key
    # at write time. No separate create_column_stats call is needed.
    print(f"  Writing to {LIB_EMBEDDED}/{sym} (EmbedInIndex=1)...")
    t0 = time.perf_counter()
    write_symbol(lib_emb, sym, nrows, ncols, embed_in_index=True)
    print(f"    Write: {time.perf_counter() - t0:.1f}s")

    print(f"  Done: {backend_name}/{sym}\n")


def main():
    parser = argparse.ArgumentParser(description="Create benchmark data for column stats experiments")
    parser.add_argument("--backends", nargs="+", choices=list(BACKENDS.keys()), required=True)
    parser.add_argument("--sizes", nargs="+", choices=list(SIZES.keys()), required=True)
    parser.add_argument("--columns", nargs="+", type=int, required=True)
    parser.add_argument("--clear", action="store_true", help="Clear both libraries before writing")
    args = parser.parse_args()

    for backend in args.backends:
        if args.clear:
            ac = get_arctic(backend)
            lib_opts = LibraryOptions(rows_per_segment=ROWS_PER_SEGMENT)
            for lib_name in (LIB_SEPARATE, LIB_EMBEDDED):
                lib = ac.get_library(lib_name, create_if_missing=True, library_options=lib_opts)
                print(f"Clearing {lib_name} on {backend}...")
                lib._nvs.version_store.clear()
        for size_label in args.sizes:
            nrows = SIZES[size_label]
            for ncols in args.columns:
                create_data(backend, nrows, ncols)

    print("All done.")


if __name__ == "__main__":
    main()
