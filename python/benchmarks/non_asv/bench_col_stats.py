import argparse
import json
import signal
import threading
import time

import numpy as np
import pandas as pd
import psutil
from arcticdb import Arctic

LMDB_PATH = "/tmp/arcticdb_bench_col_stats"
SYMBOL_NAME = "test_symbol"

signal.signal(signal.SIGINT, lambda *_: exit(130))

parser = argparse.ArgumentParser()
parser.add_argument("--scenario", required=True, help="Scenario as ROWSxCOLS, e.g. 100000x1000")
parser.add_argument("--operation", required=True, choices=["write_symbol", "create_stats"])
args = parser.parse_args()

rows, cols = map(int, args.scenario.split("x"))

ac = Arctic(f"lmdb://{LMDB_PATH}")
if not ac.has_library("bench"):
    ac.create_library("bench")
lib = ac.get_library("bench")
nvs = lib._nvs


def measure_peak_rss(operation_fn):
    process = psutil.Process()
    rss_baseline_mb = process.memory_info().rss / 1e6
    peak_rss_delta_mb = [0.0]
    stop_sampling = threading.Event()

    def rss_sampler():
        while not stop_sampling.is_set():
            delta_mb = process.memory_info().rss / 1e6 - rss_baseline_mb
            if delta_mb > peak_rss_delta_mb[0]:
                peak_rss_delta_mb[0] = delta_mb
            time.sleep(0.01)

    sampler_thread = threading.Thread(target=rss_sampler, daemon=True)
    sampler_thread.start()
    start_time = time.time()
    operation_fn()
    elapsed_seconds = time.time() - start_time
    stop_sampling.set()
    sampler_thread.join()
    return elapsed_seconds, peak_rss_delta_mb[0]


if args.operation == "write_symbol":
    dataframe = pd.DataFrame(
        np.random.rand(rows, cols).astype(np.float64),
        columns=[f"col_{i}" for i in range(cols)],
    )
    elapsed_seconds, peak_rss_delta_mb = measure_peak_rss(lambda: lib.write(SYMBOL_NAME, dataframe))
else:
    column_stats_spec = {f"col_{i}": {"MINMAX"} for i in range(cols)}
    elapsed_seconds, peak_rss_delta_mb = measure_peak_rss(
        lambda: nvs.create_column_stats(SYMBOL_NAME, column_stats_spec)
    )
    nvs.drop_column_stats(SYMBOL_NAME)

print(json.dumps({"elapsed_seconds": elapsed_seconds, "peak_rss_delta_mb": peak_rss_delta_mb}))
