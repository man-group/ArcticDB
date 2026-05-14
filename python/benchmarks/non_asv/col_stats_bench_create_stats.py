import json
import resource
import sys
import time

from arcticdb import Arctic

LMDB_PATH = "/tmp/arcticdb_bench_col_stats"
SYMBOL_NAME = "test_symbol"


def main():
    cols = int(sys.argv[1])

    ac = Arctic(f"lmdb://{LMDB_PATH}")
    lib = ac.get_library("bench")
    nvs = lib._nvs
    column_stats_spec = {f"col_{i}": {"MINMAX"} for i in range(cols)}

    start = time.time()
    nvs.create_column_stats(SYMBOL_NAME, column_stats_spec)
    end = time.time()
    
    peak_rss_mb = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss

    nvs.drop_column_stats(SYMBOL_NAME)

    print(json.dumps({
        "elapsed_seconds": end - start,
        "peak_rss_mb": peak_rss_mb / 1024,
    }))


if __name__ == "__main__":
    main()
