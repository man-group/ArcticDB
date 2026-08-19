"""
Copyright 2026 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.

Small-machine variant of profile_s3_connection_pool.py, sized for a quick (~5-10 minute) smoke
test on a small Windows box (e.g. a 2-processor Xeon Platinum 8168 VM) rather than a full sweep
against a simulated headnode. Kept as a separate script rather than parameterising the original,
since the two intentionally disagree on what the fixed thread counts should be:

- The Linux driver pins VersionStore.NumCPUThreads/NumIOThreads to headnode-sized values (48/72)
  regardless of the machine it runs on, so pool-size effects are comparable across test machines.
- This script instead pins them to values sized for the actual small machine (8/12), because on a
  2-core box, 48 CPU threads would oversubscribe the CPU-bound work (compression etc.) heavily
  enough to confound the pool-size comparison.

All data shapes here are ~10x smaller than the original (fewer rows per segment, fewer segments,
fewer columns for "wide" shapes) and the default grid (pool sizes, ops, shapes, repeats) is cut
down for a quick smoke test. Everything is still overridable via CLI flags for a longer run.

Usage:
    ARCTICDB_REAL_S3_ACCESS_KEY=AAAA ARCTICDB_REAL_S3_SECRET_KEY=BBBB \
    python profile_s3_connection_pool_windows_small.py driver \
        --endpoint vast.example.com --bucket my-bucket \
        --backend-label vast --output-csv results_vast_win.csv

    python profile_s3_connection_pool_windows_small.py driver --dry-run --endpoint x --bucket y \
        --backend-label vast --output-csv out.csv

Pass credentials via ARCTICDB_REAL_S3_ACCESS_KEY / ARCTICDB_REAL_S3_SECRET_KEY (as an environment
variable, not --access-key/--secret-key) so they never appear as a literal argv and leak via
Task Manager / `wmic process get Caption,CommandLine` on shared machines. --access-key/--secret-key
exist as a fallback but should be avoided on any host other users can inspect running processes on.
`driver` forwards credentials to each `worker` subprocess via ARCTICDB_BENCH_ACCESS_KEY/
ARCTICDB_BENCH_SECRET_KEY the same way.
"""

import argparse
import dataclasses
import json
import os
import subprocess
import sys
import time
import uuid
from typing import Dict, List, Optional

import pandas as pd

from arcticdb import Arctic, LibraryOptions, WritePayload
from arcticdb_ext.cpp_async import io_thread_count
from benchmarks.common import generate_random_floats_dataframe

# Sized for the actual small test machine (2 processors), not a simulated headnode - see module
# docstring for why this differs from profile_s3_connection_pool.py's FIXED_CPU_THREADS/IO_THREADS.
FIXED_CPU_THREADS = 8
FIXED_IO_THREADS = 12

RESULT_PREFIX = "RESULT "
ACCESS_KEY_ENV_VAR = "ARCTICDB_BENCH_ACCESS_KEY"
SECRET_KEY_ENV_VAR = "ARCTICDB_BENCH_SECRET_KEY"

DEFAULT_POOL_SIZES = [1, 4, 16, 25]
DEFAULT_OPS = ["write", "read", "batch_write", "batch_read"]
DEFAULT_BATCH_SIZES = [8, 16]
DEFAULT_REPEATS = 3
DEFAULT_PATH_PREFIX = "aseaton-conn-bench-win-small20260819"


@dataclasses.dataclass(frozen=True)
class Shape:
    label: str
    rows_per_segment: int
    segment_count: int
    column_count: int

    @property
    def total_rows(self):
        return self.rows_per_segment * self.segment_count


# ~10x smaller than profile_s3_connection_pool.py's SHAPES on every axis (rows_per_segment,
# segment_count, and "wide" column_count), keeping the same narrow/wide/many-small/large-single
# categories so the comparison shape is preserved.
SHAPES: Dict[str, Shape] = {
    "narrow_1seg": Shape("narrow_1seg", 10_000, 1, 1),
    "narrow_10seg": Shape("narrow_10seg", 10_000, 10, 1),
    "narrow_100seg": Shape("narrow_100seg", 10_000, 100, 1),
    "wide_1seg": Shape("wide_1seg", 10_000, 1, 16),
    "wide_10seg": Shape("wide_10seg", 10_000, 10, 16),
    "wide_100seg": Shape("wide_100seg", 10_000, 100, 16),
    "many_small_narrow": Shape("many_small_narrow", 500, 1, 2),
    "large_single_object": Shape("large_single_object", 100_000, 1, 16),
}

BATCH_SHAPES = ["many_small_narrow", "large_single_object"]
# Smoke-test defaults deliberately only cover one "flat" control shape (1 segment) and one
# "many segments" shape per op, to keep the default grid small. The full SHAPES dict above is
# still selectable via --shapes for a longer run.
DEFAULT_SHAPES_FOR_OP = {
    "write": ["narrow_1seg", "narrow_100seg"],
    "read": ["narrow_1seg", "narrow_100seg"],
    "batch_write": BATCH_SHAPES,
    "batch_read": BATCH_SHAPES,
}

READ_TARGET_SYMBOL = "read_target"


@dataclasses.dataclass(frozen=True)
class WorkItem:
    pool_size: int
    op: str
    shape_label: str
    batch_size: Optional[int]


def _parse_int_list(s: str) -> List[int]:
    return [int(x) for x in s.split(",") if x.strip()]


def _parse_str_list(s: str) -> List[str]:
    return [x.strip() for x in s.split(",") if x.strip()]


def add_storage_args(parser: argparse.ArgumentParser, include_credentials: bool) -> None:
    parser.add_argument("--endpoint", required=True, help="S3-compatible endpoint hostname, no scheme/port")
    parser.add_argument("--bucket", required=True)
    parser.add_argument("--port", type=int, default=None)
    parser.add_argument("--region", default=None)
    parser.add_argument("--path-prefix", default=DEFAULT_PATH_PREFIX)
    parser.add_argument("--https", dest="https", action="store_true", default=True)
    parser.add_argument("--no-https", dest="https", action="store_false")
    parser.add_argument("--use-virtual-addressing", action="store_true", default=False)
    parser.add_argument("--backend-label", required=True, help="Free-text tag stamped into every output row")
    if include_credentials:
        parser.add_argument("--access-key", default=os.environ.get("ARCTICDB_REAL_S3_ACCESS_KEY"))
        parser.add_argument("--secret-key", default=os.environ.get("ARCTICDB_REAL_S3_SECRET_KEY"))


def build_arctic_uri(args, access_key: str, secret_key: str) -> str:
    scheme = "s3s" if args.https else "s3"
    uri = f"{scheme}://{args.endpoint}:{args.bucket}?access={access_key}&secret={secret_key}"
    if args.region:
        uri += f"&region={args.region}"
    if args.path_prefix:
        uri += f"&path_prefix={args.path_prefix}"
    if args.port:
        uri += f"&port={args.port}"
    if args.use_virtual_addressing:
        uri += "&use_virtual_addressing=true"
    return uri


def lib_name_for_shape(run_id: str, shape_label: str) -> str:
    return f"bench_{run_id}_{shape_label}"


def get_shape_library(ac: Arctic, run_id: str, shape: Shape):
    return ac.get_library(
        lib_name_for_shape(run_id, shape.label),
        create_if_missing=True,
        library_options=LibraryOptions(rows_per_segment=shape.rows_per_segment),
    )


def make_chunk_df(rows: int, cols: int, chunk_index: int) -> pd.DataFrame:
    df = generate_random_floats_dataframe(rows, cols)
    df.index = pd.date_range(
        start=pd.Timestamp("2000-01-01") + pd.Timedelta(seconds=chunk_index * rows), periods=rows, freq="s"
    )
    return df


def write_shape_symbol(lib, symbol: str, shape: Shape) -> None:
    for chunk_index in range(shape.segment_count):
        df = make_chunk_df(shape.rows_per_segment, shape.column_count, chunk_index)
        if chunk_index == 0:
            lib.write(symbol, df)
        else:
            lib.append(symbol, df)


def build_grid(args) -> List[WorkItem]:
    shapes_override = _parse_str_list(args.shapes) if args.shapes else None
    grid = []
    for pool_size in _parse_int_list(args.pool_sizes):
        for op in _parse_str_list(args.ops):
            valid_shapes = DEFAULT_SHAPES_FOR_OP[op]
            shapes = [s for s in shapes_override or valid_shapes if s in valid_shapes]
            for shape_label in shapes:
                if op in ("batch_write", "batch_read"):
                    for batch_size in _parse_int_list(args.batch_sizes):
                        grid.append(WorkItem(pool_size, op, shape_label, batch_size))
                else:
                    grid.append(WorkItem(pool_size, op, shape_label, None))
    return grid


def setup_phase(ac: Arctic, run_id: str, grid: List[WorkItem]) -> None:
    read_shapes = {item.shape_label for item in grid if item.op == "read"}
    for shape_label in read_shapes:
        shape = SHAPES[shape_label]
        lib = get_shape_library(ac, run_id, shape)
        if not lib.has_symbol(READ_TARGET_SYMBOL):
            print(f"[setup] writing read target for shape={shape_label}")
            write_shape_symbol(lib, READ_TARGET_SYMBOL, shape)

    batch_read_items = [item for item in grid if item.op == "batch_read"]
    max_batch_size_by_shape: Dict[str, int] = {}
    for item in batch_read_items:
        max_batch_size_by_shape[item.shape_label] = max(
            max_batch_size_by_shape.get(item.shape_label, 0), item.batch_size
        )
    for shape_label, max_batch_size in max_batch_size_by_shape.items():
        shape = SHAPES[shape_label]
        lib = get_shape_library(ac, run_id, shape)
        for i in range(max_batch_size):
            symbol = f"batch_member_{i}"
            if not lib.has_symbol(symbol):
                print(f"[setup] writing batch_read member {i} for shape={shape_label}")
                write_shape_symbol(lib, symbol, shape)


def run_op_write(lib, shape: Shape, repeats: int, run_id: str):
    for repeat_index in range(repeats):
        symbol = f"write_r{repeat_index}"
        t0 = time.perf_counter()
        write_shape_symbol(lib, symbol, shape)
        t1 = time.perf_counter()
        bytes_transferred = shape.total_rows * shape.column_count * 8
        yield repeat_index, t1 - t0, bytes_transferred


def run_op_read(lib, shape: Shape, repeats: int, run_id: str):
    for repeat_index in range(repeats):
        t0 = time.perf_counter()
        df = lib.read(READ_TARGET_SYMBOL).data
        t1 = time.perf_counter()
        yield repeat_index, t1 - t0, len(df) * shape.column_count * 8


def run_op_batch_write(lib, shape: Shape, batch_size: int, repeats: int, run_id: str):
    for repeat_index in range(repeats):
        payloads = [
            WritePayload(f"bwrite_r{repeat_index}_m{i}", make_chunk_df(shape.rows_per_segment, shape.column_count, 0))
            for i in range(batch_size)
        ]
        t0 = time.perf_counter()
        lib.write_batch(payloads)
        t1 = time.perf_counter()
        bytes_transferred = batch_size * shape.rows_per_segment * shape.column_count * 8
        yield repeat_index, t1 - t0, bytes_transferred


def run_op_batch_read(lib, shape: Shape, batch_size: int, repeats: int, run_id: str):
    symbols = [f"batch_member_{i}" for i in range(batch_size)]
    for repeat_index in range(repeats):
        t0 = time.perf_counter()
        results = lib.read_batch(symbols)
        t1 = time.perf_counter()
        bytes_transferred = sum(len(r.data) for r in results) * shape.column_count * 8
        yield repeat_index, t1 - t0, bytes_transferred


OP_RUNNERS = {
    "write": run_op_write,
    "read": run_op_read,
    "batch_write": run_op_batch_write,
    "batch_read": run_op_batch_read,
}


def run_worker(args) -> None:
    access_key = os.environ[ACCESS_KEY_ENV_VAR]
    secret_key = os.environ[SECRET_KEY_ENV_VAR]
    uri = build_arctic_uri(args, access_key, secret_key)
    ac = Arctic(uri)
    shape = SHAPES[args.shape]
    lib = get_shape_library(ac, args.run_id, shape)
    actual_io_threads = io_thread_count()

    if args.op in ("batch_write", "batch_read"):
        gen = OP_RUNNERS[args.op](lib, shape, args.batch_size, args.repeats, args.run_id)
    else:
        gen = OP_RUNNERS[args.op](lib, shape, args.repeats, args.run_id)

    for repeat_index, wall_time_s, bytes_transferred in gen:
        record = {
            "backend_label": args.backend_label,
            "pool_size": args.pool_size,
            "io_thread_count_actual": actual_io_threads,
            "op": args.op,
            "shape_label": args.shape,
            "segment_count": shape.segment_count,
            "column_count": shape.column_count,
            "rows_per_segment": shape.rows_per_segment,
            "total_rows": shape.total_rows,
            "batch_size": args.batch_size,
            "repeat_index": repeat_index,
            "wall_time_s": wall_time_s,
            "bytes_transferred": bytes_transferred,
            "throughput_MBps": (bytes_transferred / wall_time_s / 1e6) if wall_time_s > 0 else None,
            "worker_pid": os.getpid(),
            "timestamp_utc": time.time(),
        }
        print(RESULT_PREFIX + json.dumps(record), flush=True)


def spawn_worker(args, item: WorkItem, run_id: str) -> List[dict]:
    env = os.environ.copy()
    env["ARCTICDB_S3Storage_MaxConnections_int"] = str(item.pool_size)
    env["ARCTICDB_VersionStore_NumCPUThreads_int"] = str(FIXED_CPU_THREADS)
    env["ARCTICDB_VersionStore_NumIOThreads_int"] = str(FIXED_IO_THREADS)
    env[ACCESS_KEY_ENV_VAR] = args.access_key
    env[SECRET_KEY_ENV_VAR] = args.secret_key

    cmd = [
        sys.executable,
        os.path.abspath(__file__),
        "worker",
        "--endpoint",
        args.endpoint,
        "--bucket",
        args.bucket,
        "--backend-label",
        args.backend_label,
        "--pool-size",
        str(item.pool_size),
        "--op",
        item.op,
        "--shape",
        item.shape_label,
        "--repeats",
        str(args.repeats),
        "--run-id",
        run_id,
    ]
    if args.https:
        cmd.append("--https")
    else:
        cmd.append("--no-https")
    if args.region:
        cmd += ["--region", args.region]
    if args.path_prefix:
        cmd += ["--path-prefix", args.path_prefix]
    if args.port:
        cmd += ["--port", str(args.port)]
    if args.use_virtual_addressing:
        cmd.append("--use-virtual-addressing")
    if item.batch_size is not None:
        cmd += ["--batch-size", str(item.batch_size)]

    proc = subprocess.run(cmd, env=env, capture_output=True, text=True, timeout=args.worker_timeout_s)
    if proc.returncode != 0:
        print(f"[WARN] worker failed for {item}: exit {proc.returncode}\n{proc.stderr[-4000:]}", file=sys.stderr)
        return []

    records = []
    for line in proc.stdout.splitlines():
        if line.startswith(RESULT_PREFIX):
            records.append(json.loads(line[len(RESULT_PREFIX) :]))
    return records


def run_driver(args) -> None:
    grid = build_grid(args)
    print(
        f"Grid has {len(grid)} work items, each running {args.repeats} repeats -> {len(grid) * args.repeats} timed operations"
    )
    if args.dry_run:
        for item in grid:
            print(item)
        return

    if not args.access_key or not args.secret_key:
        raise SystemExit("--access-key/--secret-key required (or set ARCTICDB_REAL_S3_ACCESS_KEY/SECRET_KEY)")

    run_id = uuid.uuid4().hex[:8]
    print(f"run_id={run_id}")

    setup_uri = build_arctic_uri(args, args.access_key, args.secret_key)
    ac = Arctic(setup_uri)
    setup_phase(ac, run_id, grid)

    all_records = []
    for i, item in enumerate(grid):
        print(
            f"[{i + 1}/{len(grid)}] pool_size={item.pool_size} op={item.op} shape={item.shape_label} batch_size={item.batch_size}"
        )
        all_records.extend(spawn_worker(args, item, run_id))

    lib_names = sorted({lib_name_for_shape(run_id, item.shape_label) for item in grid})
    symbols_sidecar = args.output_csv + ".libraries.txt"
    with open(symbols_sidecar, "w") as f:
        f.write("\n".join(lib_names) + "\n")

    df = pd.DataFrame(all_records)
    df.to_csv(args.output_csv, index=False)
    print(f"Wrote {len(df)} rows to {args.output_csv}")

    if args.cleanup:
        cleanup_libraries(ac, lib_names)
    else:
        print(f"Skipping cleanup (--no-cleanup); library names recorded in {symbols_sidecar}")


def cleanup_libraries(ac: Arctic, lib_names: List[str]) -> None:
    for lib_name in lib_names:
        print(f"[cleanup] deleting library {lib_name}")
        ac.delete_library(lib_name)


def run_cleanup(args) -> None:
    with open(args.libraries_file) as f:
        lib_names = [line.strip() for line in f if line.strip()]
    uri = build_arctic_uri(args, args.access_key, args.secret_key)
    ac = Arctic(uri)
    cleanup_libraries(ac, lib_names)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    subparsers = parser.add_subparsers(dest="mode", required=True)

    driver = subparsers.add_parser("driver")
    add_storage_args(driver, include_credentials=True)
    driver.add_argument("--pool-sizes", default=",".join(str(x) for x in DEFAULT_POOL_SIZES))
    driver.add_argument("--ops", default=",".join(DEFAULT_OPS))
    driver.add_argument("--shapes", default=None, help="Comma list, defaults to the standard set per op")
    driver.add_argument("--batch-sizes", default=",".join(str(x) for x in DEFAULT_BATCH_SIZES))
    driver.add_argument("--repeats", type=int, default=DEFAULT_REPEATS)
    driver.add_argument("--output-csv", required=True)
    driver.add_argument("--cleanup", dest="cleanup", action="store_true", default=True)
    driver.add_argument("--no-cleanup", dest="cleanup", action="store_false")
    driver.add_argument("--worker-timeout-s", type=float, default=600.0)
    driver.add_argument("--dry-run", action="store_true", default=False)
    driver.set_defaults(func=run_driver)

    worker = subparsers.add_parser("worker")
    add_storage_args(worker, include_credentials=False)
    worker.add_argument("--pool-size", type=int, required=True)
    worker.add_argument("--op", required=True, choices=list(OP_RUNNERS.keys()))
    worker.add_argument("--shape", required=True, choices=list(SHAPES.keys()))
    worker.add_argument("--batch-size", type=int, default=None)
    worker.add_argument("--repeats", type=int, required=True)
    worker.add_argument("--run-id", required=True)
    worker.set_defaults(func=run_worker)

    cleanup = subparsers.add_parser("cleanup", help="Re-run library deletion from a saved .libraries.txt sidecar file")
    add_storage_args(cleanup, include_credentials=True)
    cleanup.add_argument("--libraries-file", required=True)
    cleanup.set_defaults(func=run_cleanup)

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
