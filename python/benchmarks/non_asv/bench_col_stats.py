import argparse
import atexit
import shutil
import signal
import time

import numpy as np
import pandas as pd
import psutil

from arcticdb import Arctic

LMDB_PATH = "/tmp/arcticdb_bench_col_stats"


def _cleanup():
    shutil.rmtree(LMDB_PATH, ignore_errors=True)


atexit.register(_cleanup)
signal.signal(signal.SIGINT, lambda *_: exit(130))

SCENARIOS = [
    # (rows, cols)
    (10, 10),
    (1_000, 1_000),
    (100_000, 1_000),
    (100_000, 10_000),
    (1_000_000, 1_000),
    (1_000_000, 10_000),
    (10_000_000, 1_000),
]

parser = argparse.ArgumentParser()
parser.add_argument("--simple_tests", action="store_true", help="Run only the first 4 scenarios")
args = parser.parse_args()

scenarios = SCENARIOS[:2] if args.simple_tests else SCENARIOS

ac = Arctic(f"lmdb://{LMDB_PATH}")
if not ac.has_library("bench"):
    ac.create_library("bench")
lib = ac.get_library("bench")
nvs = lib._nvs

results = []

for rows, cols in scenarios:
    sym = f"r{rows}_c{cols}"

    df = pd.DataFrame(
        np.random.rand(rows, cols).astype(np.float64),
        columns=[f"col_{i}" for i in range(cols)],
    )

    t0 = time.time()
    lib.write(sym, df)
    write_time = time.time() - t0

    col_stats = {f"col_{i}": {"MINMAX"} for i in range(cols)}

    mem_before = psutil.Process().memory_info().rss / 1e6
    t0 = time.time()
    nvs.create_column_stats(sym, col_stats)
    stats_time = time.time() - t0
    mem_after = psutil.Process().memory_info().rss / 1e6

    nvs.drop_column_stats(sym)
    lib.delete(sym)

    results.append((rows, cols, write_time, stats_time, mem_after - mem_before))
    print(f"rows={rows:>10,}  cols={cols:>6,}  write={write_time:6.2f}s  stats={stats_time:6.2f}s  mem_delta={mem_after - mem_before:+.1f} MB")

print()
print(f"{'rows':>12}  {'cols':>6}  {'write_symbol_time':>8}  {'stats_create_time':>8}  {'consumed_memory_mb':>14}")
print("-" * 60)

for rows, cols, write_time, stats_time, mem_delta in results:
    print(f"{rows:>12,}  {cols:>6,}  {write_time:>8.2f}  {stats_time:>8.2f}  {mem_delta:>+14.1f}")
