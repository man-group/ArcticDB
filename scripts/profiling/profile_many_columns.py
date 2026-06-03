"""Profile a zero-match QueryBuilder filter against many int64 columns with column stats.

With --setup, writes a 200k-row dataframe with 1000 positive int64 columns into a library
configured with rows_per_segment=2 and builds MINMAX column stats over every column.

Without --setup (the default), times reads with a QueryBuilder filter that requires 10 of
the columns to be negative — so column stats prune every segment and the read returns no
rows. The task scheduler is forced single-threaded only on this read/profile path.
"""
import argparse
import shutil
import time
from pathlib import Path

import numpy as np
import pandas as pd

from arcticdb import Arctic, QueryBuilder
from arcticdb.options import LibraryOptions
from arcticdb_ext import set_config_int, unset_config_int
from arcticdb_ext import cpp_async as adb_async


LIB_NAME = "profile_lib"


def _column_names(num_cols: int):
    return [f"int64_col_{i}" for i in range(num_cols)]


def _symbol(rows: int, num_cols: int, rows_per_segment: int) -> str:
    return f"many_cols_{rows}r_{num_cols}c_seg{rows_per_segment}"


def _generate_dataframe(rows: int, num_cols: int) -> pd.DataFrame:
    timestamps = pd.date_range(end="1/1/2023", periods=rows, freq="s")
    base = np.arange(1, rows + 1, dtype=np.int64)
    data = {name: base + i for i, name in enumerate(_column_names(num_cols))}
    df = pd.DataFrame(data)
    df.index = timestamps
    df.index.name = "ts"
    return df


def _setup(lmdb_path: Path, rows: int, num_cols: int, rows_per_segment: int, reset: bool) -> None:
    if reset and lmdb_path.exists():
        shutil.rmtree(lmdb_path)
    lmdb_path.mkdir(parents=True, exist_ok=True)
    ac = Arctic(f"lmdb://{lmdb_path}")
    if LIB_NAME in ac.list_libraries():
        ac.delete_library(LIB_NAME)
    ac.create_library(
        LIB_NAME,
        library_options=LibraryOptions(rows_per_segment=rows_per_segment),
    )
    lib = ac[LIB_NAME]
    symbol = _symbol(rows, num_cols, rows_per_segment)
    print(f"Generating {rows} rows x {num_cols} cols...", flush=True)
    t0 = time.perf_counter()
    df = _generate_dataframe(rows, num_cols)
    print(f"  generated in {time.perf_counter() - t0:.2f}s", flush=True)
    t0 = time.perf_counter()
    lib.write(symbol, df)
    print(f"  wrote in {time.perf_counter() - t0:.2f}s", flush=True)
    column_stats = {name: {"MINMAX"} for name in _column_names(num_cols)}
    t0 = time.perf_counter()
    lib._nvs.create_column_stats(symbol, column_stats)
    print(f"  column stats in {time.perf_counter() - t0:.2f}s", flush=True)


def _profile(
    lmdb_path: Path,
    rows: int,
    num_cols: int,
    rows_per_segment: int,
    filter_cols: int,
    warmup: int,
    iters: int,
    column_stats_on: bool,
) -> None:
    if column_stats_on:
        set_config_int("ColumnStats.UseForQueries", 1)
    else:
        unset_config_int("ColumnStats.UseForQueries")

    # Force single-threaded scheduler for the read/profile path only.
    set_config_int("VersionStore.NumIOThreads", 1)
    set_config_int("VersionStore.NumCPUThreads", 1)
    adb_async.reinit_task_scheduler()

    ac = Arctic(f"lmdb://{lmdb_path}")
    lib = ac[LIB_NAME]
    symbol = _symbol(rows, num_cols, rows_per_segment)

    # All values are >= 1, so requiring any column to be < 0 prunes every segment.
    filter_columns = _column_names(num_cols)[:filter_cols]
    q = QueryBuilder()
    clause = q[filter_columns[0]] < 0
    for name in filter_columns[1:]:
        clause = clause & (q[name] < 0)
    q = q[clause]

    read_columns = filter_columns

    def _do_read():
        return lib.read(symbol, columns=read_columns, query_builder=q)

    print(f"warmup x{warmup}", flush=True)
    for _ in range(warmup):
        _do_read()

    print(f"timed x{iters}", flush=True)
    times = []
    t_block = time.perf_counter()
    for _ in range(iters):
        t0 = time.perf_counter()
        result = _do_read()
        dt = time.perf_counter() - t0
        times.append(dt)
        assert len(result.data) == 0, f"expected zero rows, got {len(result.data)}"
    block_dt = time.perf_counter() - t_block

    times_ms = np.array(times) * 1000.0
    print(
        f"\nlib.read zero-match (rows={rows}, num_cols={num_cols}, "
        f"rows_per_segment={rows_per_segment}, filter_cols={filter_cols}, "
        f"iters={iters}, column_stats={'on' if column_stats_on else 'off'}):"
    )
    print(f"  total block      = {block_dt * 1000:.1f} ms")
    print(f"  mean             = {times_ms.mean():.2f} ms")
    print(f"  median           = {np.median(times_ms):.2f} ms")
    print(f"  min              = {times_ms.min():.2f} ms")
    print(f"  max              = {times_ms.max():.2f} ms")


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--setup", action="store_true", help="create the library + symbol + column stats and exit")
    p.add_argument("--rows", type=int, default=20_000)
    p.add_argument("--num-cols", type=int, default=100)
    p.add_argument("--rows-per-segment", type=int, default=2)
    p.add_argument("--filter-cols", type=int, default=10, help="number of columns in the AND filter")
    p.add_argument("--iters", type=int, default=20)
    p.add_argument("--warmup", type=int, default=2)
    p.add_argument("--reset", action="store_true", help="(--setup only) wipe LMDB before recreating")
    p.add_argument(
        "--lmdb-path",
        default=str(Path.home() / ".tmp" / "profile_many_columns_lmdb"),
        help="LMDB directory",
    )
    p.add_argument("--no-column-stats", action="store_true", help="disable ColumnStats.UseForQueries")
    args = p.parse_args()

    lmdb_path = Path(args.lmdb_path)

    if args.setup:
        _setup(lmdb_path, args.rows, args.num_cols, args.rows_per_segment, args.reset)
        return

    _profile(
        lmdb_path,
        args.rows,
        args.num_cols,
        args.rows_per_segment,
        args.filter_cols,
        args.warmup,
        args.iters,
        column_stats_on=not args.no_column_stats,
    )


if __name__ == "__main__":
    main()
