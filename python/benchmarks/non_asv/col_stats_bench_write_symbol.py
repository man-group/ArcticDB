import json
import resource
import sys
import time

import numpy as np
import pandas as pd
from arcticdb import Arctic

LMDB_PATH = "/tmp/arcticdb_bench_col_stats"
SYMBOL_NAME = "test_symbol"
CHUNK_ROWS = 100_000


def main():
    rows, cols = int(sys.argv[1]), int(sys.argv[2])
    column_names = [f"col_{i}" for i in range(cols)]

    ac = Arctic(f"lmdb://{LMDB_PATH}")
    if not ac.has_library("bench"):
        ac.create_library("bench")
    lib = ac.get_library("bench")


    for chunk_start in range(0, rows, CHUNK_ROWS):
        chunk_row_count = min(CHUNK_ROWS, rows - chunk_start)
        
        chunk = pd.DataFrame(
            np.random.rand(chunk_row_count, cols).astype(np.float64),
            columns=column_names,
        )

        start_time = time.time()

        if chunk_start == 0:
            lib.write(SYMBOL_NAME, chunk)
        else:
            lib.append(SYMBOL_NAME, chunk)

    elapsed_seconds = time.time() - start_time
    peak_rss_mb = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024  # ru_maxrss is KB on Linux

    print(json.dumps({"elapsed_seconds": elapsed_seconds, "peak_rss_mb": peak_rss_mb}))


if __name__ == "__main__":
    main()
