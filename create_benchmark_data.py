"""
Create benchmark data: 10M rows with a monotonically increasing publication_date column.
100k rows per day, 100 days total. Column stats (MINMAX) are created on publication_date.
"""

import numpy as np
import pandas as pd
from arcticdb import Arctic
from arcticdb.options import LibraryOptions
import os

os.environ["AWS_PROFILE"] = "research-3"

URI = "s3://arctic-data.vast.gdc.storage.res.m:alpha-data-dev-arcticnative-ahl-research-3?aws_auth=true&path_prefix=aseaton_tst"
LIB_NAME = "column_stats_benchmark"
SYMBOL = "benchmark_10m"

ROWS_PER_DAY = 100_000
NUM_DAYS = 1000
TOTAL_ROWS = ROWS_PER_DAY * NUM_DAYS

ac = Arctic(URI)
lib = ac.get_library(
    LIB_NAME,
    create_if_missing=True,
    library_options=LibraryOptions(rows_per_segment=ROWS_PER_DAY),
)

print(f"Building DataFrame: {TOTAL_ROWS:,} rows, {NUM_DAYS} days, {ROWS_PER_DAY:,} rows/day")

base_date = pd.Timestamp("2020-01-01")
# Each day has 100k timestamps spaced 864ms apart (86400s / 100000)
timestamps = np.concatenate([
    pd.date_range(base_date + pd.Timedelta(days=d), periods=ROWS_PER_DAY, freq="864ms").values
    for d in range(NUM_DAYS)
])

df = pd.DataFrame(
    {
        "publication_date": timestamps,
        "value": np.random.randn(TOTAL_ROWS),
    },
    index=pd.date_range(base_date, periods=TOTAL_ROWS, freq="864ms"),
)

print(f"Writing {TOTAL_ROWS:,} rows to {LIB_NAME}/{SYMBOL}...")
lib.write(SYMBOL, df)
print("Write complete.")

print("Creating column stats (MINMAX) on publication_date...")
lib._nvs.create_column_stats(SYMBOL, {"publication_date": {"MINMAX"}})
print("Column stats created.")

print("Done.")
