"""Stage A: write 10M rows x 100 float64 columns into an LMDB library.

Run once. The library is reused by measure_create_column_stats.py.
"""

import argparse
import shutil
import time
from pathlib import Path

import numpy as np
import pandas as pd

from arcticdb import Arctic

LIB_NAME = "col_stats_mem"
SYMBOL = "f64_10m_100c"
SYMBOL_ZEROS = "f64_10m_100c_zeros"
DEFAULT_LMDB = Path.home() / ".tmp" / "col_stats_rss_lmdb"


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--rows", type=int, default=10_000_000)
    p.add_argument("--num-cols", type=int, default=100)
    p.add_argument("--lmdb-path", default=str(DEFAULT_LMDB))
    p.add_argument("--reset", action="store_true", help="wipe LMDB before writing")
    p.add_argument("--zeros", action="store_true", help="write all zeros (compressible)")
    args = p.parse_args()

    lmdb_path = Path(args.lmdb_path)
    if args.reset and lmdb_path.exists():
        shutil.rmtree(lmdb_path)
    lmdb_path.mkdir(parents=True, exist_ok=True)

    ac = Arctic(f"lmdb://{lmdb_path}")
    if LIB_NAME not in ac.list_libraries():
        ac.create_library(LIB_NAME)
    lib = ac[LIB_NAME]

    symbol = SYMBOL_ZEROS if args.zeros else SYMBOL
    print(f"Generating {args.rows} rows x {args.num_cols} float64 cols (zeros={args.zeros})...", flush=True)
    t0 = time.perf_counter()
    if args.zeros:
        data = {f"f_{i}": np.zeros(args.rows, dtype=np.float64) for i in range(args.num_cols)}
    else:
        rng = np.random.default_rng(0)
        data = {f"f_{i}": rng.random(args.rows, dtype=np.float64) for i in range(args.num_cols)}
    df = pd.DataFrame(data)
    df.index = pd.date_range(end="1/1/2023", periods=args.rows, freq="s")
    df.index.name = "ts"
    print(f"  generated in {time.perf_counter() - t0:.2f}s", flush=True)

    t0 = time.perf_counter()
    lib.write(symbol, df)
    print(f"  wrote in {time.perf_counter() - t0:.2f}s", flush=True)
    print(f"lmdb={lmdb_path} lib={LIB_NAME} symbol={symbol}")


if __name__ == "__main__":
    main()
