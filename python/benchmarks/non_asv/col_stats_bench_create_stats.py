import json
import logging
import resource
import sys
import time

from ahl.mongo import NativeMongoose

logging.getLogger("man.vault.client").setLevel(logging.WARNING)
logging.getLogger("man.secrets.api").setLevel(logging.WARNING)


def main():
    cols = int(sys.argv[1])

    lib = NativeMongoose("mktdatad").get_library("pmarkovski.columns_stats", api="v2")
    nvs = lib._nvs
    column_stats_spec = {f"col_{i}": {"MINMAX"} for i in range(cols)}

    start = time.time()
    nvs.create_column_stats("test_symbol", column_stats_spec)
    end = time.time()

    peak_rss_mb = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss

    nvs.drop_column_stats("test_symbol")

    print(json.dumps({
        "elapsed_seconds": end - start,
        "peak_rss_mb": peak_rss_mb / 1024,
    }))


if __name__ == "__main__":
    main()
