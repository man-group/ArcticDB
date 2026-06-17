"""Stage B: measure peak RSS for create_column_stats on the symbol written by write_data.py.

Prints VmRSS just before the call (baseline) and ru_maxrss at exit (peak high-water).
Best run wrapped in `/usr/bin/time -v` as a cross-check.
"""
import argparse
import resource
import time
from pathlib import Path

from arcticdb import Arctic
from arcticdb_ext import set_config_int
from arcticdb_ext import cpp_async as adb_async

from write_data import LIB_NAME, SYMBOL, SYMBOL_ZEROS, DEFAULT_LMDB


def read_vmrss_kb() -> int:
    with open("/proc/self/status") as f:
        for line in f:
            if line.startswith("VmRSS:"):
                return int(line.split()[1])
    return -1


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--num-cols", type=int, default=100)
    p.add_argument("--lmdb-path", default=str(DEFAULT_LMDB))
    p.add_argument(
        "--no-cpu-executor",
        action="store_true",
        help="set Storage.ProcessOnCpuExecutor=0",
    )
    p.add_argument("--io-threads", type=int, default=None)
    p.add_argument("--cpu-threads", type=int, default=None)
    p.add_argument("--zeros", action="store_true", help="use the compressible-zeros symbol")
    args = p.parse_args()
    symbol = SYMBOL_ZEROS if args.zeros else SYMBOL

    if args.no_cpu_executor:
        set_config_int("Storage.ProcessOnCpuExecutor", 0)
    if args.io_threads is not None:
        set_config_int("VersionStore.NumIOThreads", args.io_threads)
    if args.cpu_threads is not None:
        set_config_int("VersionStore.NumCPUThreads", args.cpu_threads)
    if args.io_threads is not None or args.cpu_threads is not None:
        adb_async.reinit_task_scheduler()

    ac = Arctic(f"lmdb://{Path(args.lmdb_path)}")
    lib = ac[LIB_NAME]
    nvs = lib._nvs

    nvs.drop_column_stats(symbol)

    column_stats = {f"f_{i}": {"MINMAX"} for i in range(args.num_cols)}

    baseline_kb = read_vmrss_kb()
    print(f"baseline VmRSS = {baseline_kb / 1024:.1f} MiB", flush=True)

    t0 = time.perf_counter()
    nvs.create_column_stats(symbol, column_stats)
    dt = time.perf_counter() - t0

    peak_kb = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    print(f"create_column_stats wall = {dt:.2f}s")
    print(f"peak ru_maxrss        = {peak_kb / 1024:.1f} MiB")
    print(f"peak - baseline       = {(peak_kb - baseline_kb) / 1024:.1f} MiB")


if __name__ == "__main__":
    main()
