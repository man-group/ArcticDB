import json
import logging
import resource
import sys
import time

import numpy as np
import pandas as pd
from ahl.mongo import NativeMongoose

logging.getLogger("man.vault.client").setLevel(logging.WARNING)
logging.getLogger("man.secrets.api").setLevel(logging.WARNING)


CHUNK_ROWS = 100_000

def main():
    rows, cols = int(sys.argv[1]), int(sys.argv[2])
    column_names = [f"col_{i}" for i in range(cols)]

    lib = NativeMongoose("mktdatad").get_library("pmarkovski.columns_stats", api="v2")

    total_elapsed = 0.0

    for chunk_start in range(0, rows, CHUNK_ROWS):
        chunk_row_count = min(CHUNK_ROWS, rows - chunk_start)

        chunk = pd.DataFrame(
            np.random.rand(chunk_row_count, cols).astype(np.float64),
            columns=column_names,
        )

        chunk_mb = chunk.memory_usage(deep=True).sum() / 1024 / 1024
        print(f"  chunk [{chunk_start}:{chunk_start + chunk_row_count}] {chunk.shape} {chunk_mb:.1f} MB", file=sys.stderr, flush=True)

        start_time = time.time()

        if chunk_start == 0:
            lib.write("test_symbol", chunk)
        else:
            lib.append("test_symbol", chunk)

        total_elapsed += (time.time() - start_time)

    peak_rss_mb = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024  # ru_maxrss is KB on Linux

    print(json.dumps({
        "elapsed_seconds": total_elapsed,
        "peak_rss_mb": peak_rss_mb,
    }))


if __name__ == "__main__":
    main()
