"""Profile Arrow string handler timing: only Arrow read."""
import arcticdb as adb
import numpy as np
import pandas as pd
import time
import sys

ac = adb.Arctic("lmdb:///tmp/arrow_timing_test2?map_size=2GB")
lib = ac.get_library("profiling", create_if_missing=True)

n_rows = 1_000_000
n_str_cols = 3
n_unique = 100

# Create data with string columns
data = {}
str_values = [f"string_value_{i:04d}" for i in range(n_unique)]
for i in range(n_str_cols):
    data[f"str_{i}"] = np.random.choice(str_values, n_rows)

df = pd.DataFrame(data, index=pd.date_range("2024-01-01", periods=n_rows, freq="s"))
lib.write("test_sym", df)

print(f"\n=== Test: {n_str_cols} string cols, {n_rows} rows, {n_unique} unique ===")

# ONLY read with LARGE_STRING (default) - no pandas read to avoid cross-contamination
t0 = time.time()
result = lib.read("test_sym", output_format="pyarrow")
t1 = time.time()
print(f"LARGE_STRING total: {t1-t0:.3f}s")

ac.delete_library("profiling")
print("\nDone. Check stderr for timing output.")
sys.stderr.flush()
