"""
Read 10 days of benchmark data using QueryBuilder filter on publication_date, with column stats enabled.
Reports timing stats (min, max, avg, stddev) over 10 runs and the number of TABLE_DATA segments read from storage.
"""

import statistics
import time

import pandas as pd
from arcticdb import Arctic
from arcticdb.version_store.processing import QueryBuilder
from arcticdb_ext import set_config_int
import arcticdb.toolbox.query_stats as qs

import os

os.environ["AWS_PROFILE"] = "research-3"

URI = "s3://arctic-data.vast.gdc.storage.res.m:alpha-data-dev-arcticnative-ahl-research-3?aws_auth=true&path_prefix=aseaton_tst"
LIB_NAME = "column_stats_benchmark"
SYMBOL = "benchmark_10m"

NUM_RUNS = 10

# 10-day query window (days 45-54 inclusive, middle of the dataset)
QUERY_START = pd.Timestamp("2020-02-15")
QUERY_END = pd.Timestamp("2020-02-25")

# Configure thread pools
set_config_int("VersionStore.NumCPUThreads", 16)
set_config_int("VersionStore.NumIOThreads", 24)

ac = Arctic(URI)
lib = ac.get_library(LIB_NAME)


def get_table_data_read_count():
    stats = qs.get_query_stats()
    return (stats or {}).get("storage_operations", {}).get("S3_GetObject", {}).get("TABLE_DATA", {}).get("count", 0)


def read_with_column_stats(enabled: bool):
    set_config_int("ColumnStats.UseForQueries", 1 if enabled else 0)

    timings = []

    for i in range(NUM_RUNS):
        q = QueryBuilder()
        q = q[(q["publication_date"] >= QUERY_START) & (q["publication_date"] < QUERY_END)]

        t0 = time.perf_counter()
        lib.read(SYMBOL, query_builder=q).data
        elapsed = time.perf_counter() - t0

        timings.append(elapsed)

    label = "WITH" if enabled else "WITHOUT"
    avg = statistics.mean(timings)
    stddev = statistics.stdev(timings) if len(timings) > 1 else 0.0
    print(f"Read {label} column stats ({NUM_RUNS} runs):")
    print(f"  Min:    {min(timings):.3f}s")
    print(f"  Max:    {max(timings):.3f}s")
    print(f"  Avg:    {avg:.3f}s")
    print(f"  Stddev: {stddev:.3f}s")
    print()


print(f"Querying {SYMBOL}: publication_date in [{QUERY_START}, {QUERY_END})")
print(f"Threads: 16 CPU, 24 IO")
print(f"Runs: {NUM_RUNS}")
print()

read_with_column_stats(enabled=False)
read_with_column_stats(enabled=True)
