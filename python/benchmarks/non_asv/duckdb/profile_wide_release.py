"""Profile Arrow read for wide tables in release mode."""
import arcticdb as adb
import numpy as np
import pandas as pd
import time
import sys

ac = adb.Arctic("lmdb:///tmp/arrow_timing_wide?map_size=4GB")
lib = ac.get_library("profiling", create_if_missing=True)

n_rows = 1_000_000
n_str_cols = 20
n_num_cols = 30
n_unique = 100

data = {}
str_values = [f"string_value_{i:04d}" for i in range(n_unique)]
for i in range(n_str_cols):
    data[f"str_{i}"] = np.random.choice(str_values, n_rows)
for i in range(n_num_cols):
    data[f"num_{i}"] = np.random.randn(n_rows)

df = pd.DataFrame(data, index=pd.date_range("2024-01-01", periods=n_rows, freq="s"))
lib.write("test_sym", df)

print(f"\n=== Wide table: {n_str_cols} str + {n_num_cols} num cols, {n_rows} rows ===")

store = lib._nvs

# Arrow read
version_query, read_options, read_query, output_format = store._get_queries(
    as_of=None, date_range=None, row_range=None, columns=None, query_builder=None,
    output_format="pyarrow"
)
t0 = time.time()
raw = store.version_store.read_dataframe_version("test_sym", version_query, read_query, read_options)
t1 = time.time()
print(f"Arrow C++ total: {t1-t0:.4f}s")

# Extract batches + make table
import pyarrow as pa
from arcticdb.version_store._store import ReadResult
read_result = ReadResult(*raw)

t2 = time.time()
record_batches = []
for record_batch in read_result.frame_data.extract_record_batches():
    record_batches.append(pa.RecordBatch._import_from_c(record_batch.array(), record_batch.schema()))
t3 = time.time()
print(f"Extract + import batches: {t3-t2:.4f}s ({len(record_batches)} batches)")

t4 = time.time()
table = pa.Table.from_batches(record_batches)
t5 = time.time()
print(f"Table.from_batches: {t5-t4:.4f}s")

t6 = time.time()
data_out = store._normalizer.denormalize(table, read_result.norm)
t7 = time.time()
print(f"Denormalize: {t7-t6:.4f}s")

print(f"\nTotal Arrow: C++ {t1-t0:.4f}s + Python {t7-t2:.4f}s = {t1-t0 + t7-t2:.4f}s")

# Pandas read for comparison
version_query_pd, read_options_pd, read_query_pd, _ = store._get_queries(
    as_of=None, date_range=None, row_range=None, columns=None, query_builder=None
)
t0 = time.time()
raw_pd = store.version_store.read_dataframe_version("test_sym", version_query_pd, read_query_pd, read_options_pd)
t1 = time.time()
print(f"\nPandas C++ total: {t1-t0:.4f}s")

t2 = time.time()
read_result_pd = ReadResult(*raw_pd)
result = store._normalizer.denormalize(read_result_pd.frame_data, read_result_pd.norm)
t3 = time.time()
print(f"Pandas Python overhead: {t3-t2:.4f}s")
print(f"Pandas total: {t1-t0 + t3-t2:.4f}s")

ac.delete_library("profiling")
print("\nDone.")
sys.stderr.flush()
