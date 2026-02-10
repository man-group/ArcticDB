"""
Detailed profiling to find where time is spent in Arrow read path.

The Arrow read path:
  1. C++: read_dataframe_version() -> allocate_frame + fetch_data + decode + reduce_and_fix_columns
  2. C++: segment_to_arrow_data() -> ArrowOutputFrame
  3. C++: extract_record_batches() -> RecordBatchData (Arrow C arrays)
  4. Python: _import_from_c() for each batch
  5. Python: pa.Table.from_batches()
  6. Python: denormalize()

Steps 2 and 3 have timing instrumentation in C++ (fprintf to stderr).
This script instruments steps 4-6 in Python.
"""
import sys
import time
import numpy as np
import pandas as pd
import pyarrow as pa
from arcticdb import Arctic
from arcticdb.version_store._store import NativeVersionStore


np.random.seed(42)
ac = Arctic("lmdb://profile_detailed")
try:
    ac.delete_library("profile")
except Exception:
    pass
lib = ac.create_library("profile")

# Write test data
print("Writing test data...")
n_rows = 1_000_000

# Pure numeric 50 cols
df_num50 = pd.DataFrame({f"num_{i}": np.random.uniform(0, 100, n_rows) for i in range(50)})
df_num50.index = pd.date_range(end="2025-01-01", periods=n_rows, freq="min")
lib.write("pure_numeric_50", df_num50)

# Mixed 9 cols
str_pool = [f"val_{i:04d}_{'x' * np.random.randint(4, 16)}" for i in range(100)]
data = {}
for i in range(3):
    data[f"str_{i}"] = np.random.choice(str_pool, n_rows)
for i in range(6):
    data[f"num_{i}"] = np.random.uniform(0, 100, n_rows)
df_mix = pd.DataFrame(data)
df_mix.index = pd.date_range(end="2025-01-01", periods=n_rows, freq="min")
lib.write("mixed_9", df_mix)

# Pure numeric 9 cols
df_num9 = pd.DataFrame({f"num_{i}": np.random.uniform(0, 100, n_rows) for i in range(9)})
df_num9.index = pd.date_range(end="2025-01-01", periods=n_rows, freq="min")
lib.write("pure_numeric_9", df_num9)

print("Done writing.\n")

# Get the underlying NativeVersionStore
store = lib._nvs

# Monkeypatch _adapt_frame_data to get detailed timing
from arcticdb.version_store._store import ArrowOutputFrame

def detailed_arrow_read(store, symbol):
    """Do the read with detailed timing of each step."""
    from arcticdb.version_store._store import ReadResult as InternalReadResult

    # Build the read args the same way the library does
    version_query, read_options, read_query, output_format = store._get_queries(
        as_of=None, date_range=None, row_range=None, columns=None, query_builder=None,
        output_format="pyarrow"
    )

    # Warmup
    store.read(symbol, output_format="pyarrow")
    store.read(symbol, output_format="pyarrow")

    # Step 1: The full C++ read (including segment_to_arrow_data, which we timed via fprintf)
    t0 = time.perf_counter()
    raw_result = store.version_store.read_dataframe_version(symbol, version_query, read_query, read_options)
    read_result = InternalReadResult(*raw_result)
    t_cpp = time.perf_counter()

    # Step 2: extract_record_batches (C++ via pybind11)
    frame_data = read_result.frame_data
    assert isinstance(frame_data, ArrowOutputFrame), f"Expected ArrowOutputFrame, got {type(frame_data)}"

    t_extract_start = time.perf_counter()
    raw_batches = frame_data.extract_record_batches()
    t_extract = time.perf_counter()

    # Step 3: _import_from_c for each batch
    record_batches = []
    t_import_start = time.perf_counter()
    for rb in raw_batches:
        record_batches.append(pa.RecordBatch._import_from_c(rb.array(), rb.schema()))
    t_import = time.perf_counter()

    # Step 4: pa.Table.from_batches
    t_table_start = time.perf_counter()
    table = pa.Table.from_batches(record_batches)
    t_table = time.perf_counter()

    # Step 5: denormalize
    t_denorm_start = time.perf_counter()
    data = store._normalizer.denormalize(table, read_result.norm)
    t_denorm = time.perf_counter()

    n_batches = len(record_batches)
    n_cols = record_batches[0].num_columns if n_batches > 0 else 0

    print(f"  Batches: {n_batches}, columns per batch: {n_cols}")
    print(f"  C++ read_dataframe_version:  {(t_cpp - t0)*1000:8.1f}ms")
    print(f"  extract_record_batches:      {(t_extract - t_extract_start)*1000:8.1f}ms")
    print(f"  _import_from_c ({n_batches}x):       {(t_import - t_import_start)*1000:8.1f}ms ({(t_import - t_import_start)*1000/max(n_batches,1):.2f}ms/batch)")
    print(f"  Table.from_batches:          {(t_table - t_table_start)*1000:8.1f}ms")
    print(f"  denormalize:                 {(t_denorm - t_denorm_start)*1000:8.1f}ms")
    print(f"  TOTAL:                       {(t_denorm - t0)*1000:8.1f}ms")

    return data


# Also compare with pandas path timing
def detailed_pandas_read(store, symbol):
    """Do the pandas read with timing."""
    # Warmup
    store.read(symbol)
    store.read(symbol)

    t0 = time.perf_counter()
    result = store.read(symbol)
    t1 = time.perf_counter()
    print(f"  pandas TOTAL:                {(t1 - t0)*1000:8.1f}ms")
    return result


for sym_name, sym_label in [("pure_numeric_9", "Pure Numeric 9 cols"),
                              ("pure_numeric_50", "Pure Numeric 50 cols"),
                              ("mixed_9", "Mixed 9 cols (3 str + 6 num)")]:
    print(f"=== {sym_label} ===")
    print("Arrow path:")
    detailed_arrow_read(store, sym_name)
    print("Pandas path:")
    detailed_pandas_read(store, sym_name)
    print()

print("Done!")
