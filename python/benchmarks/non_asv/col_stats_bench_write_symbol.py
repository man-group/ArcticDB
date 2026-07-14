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


def main():
    rows, cols = int(sys.argv[1]), int(sys.argv[2])
    column_names = [f"col_{i}" for i in range(cols)]

    lib = NativeMongoose("mktdatad").get_library("pmarkovski.columns_stats", api="v2")

    chunk = pd.DataFrame(
        np.random.rand(rows, cols).astype(np.float64),
        columns=column_names,
    )

    start_time = time.time()
    lib.write("test_symbol", chunk)
    end_time = time.time()

    peak_rss_mb = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024  # ru_maxrss is KB on Linux

    print(json.dumps({
        "elapsed_seconds": end_time - start_time,
        "peak_rss_mb": peak_rss_mb,
    }))


if __name__ == "__main__":
    main()
