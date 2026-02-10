"""
Profile the Arrow read path to identify exactly where time is spent.
"""
import sys
import time
import numpy as np
import pandas as pd
from arcticdb import Arctic


def generate_df(n_rows, n_str_cols, n_num_cols):
    data = {}
    str_pool = [f"val_{i:04d}_{'x' * np.random.randint(4, 16)}" for i in range(100)]
    for i in range(n_str_cols):
        data[f"str_{i}"] = np.random.choice(str_pool, n_rows)
    for i in range(n_num_cols):
        data[f"num_{i}"] = np.random.uniform(0, 100, n_rows)
    df = pd.DataFrame(data)
    df.index = pd.date_range(end="2025-01-01", periods=n_rows, freq="min")
    return df


def timeit(func, label, n=3):
    times = []
    for _ in range(n):
        t0 = time.time()
        func()
        times.append(time.time() - t0)
    best = min(times)
    print(f"  {label:<50} {best:.3f}s (best of {n})")
    return best


np.random.seed(42)
ac = Arctic("lmdb://profile_arrow")
try:
    ac.delete_library("profile")
except Exception:
    pass
lib = ac.create_library("profile")

# Test 1: Pure numeric (no string handler overhead)
print("=== Test 1: Pure numeric 9 cols, 1M rows ===")
df_num = pd.DataFrame({f"num_{i}": np.random.uniform(0, 100, 1_000_000) for i in range(9)})
df_num.index = pd.date_range(end="2025-01-01", periods=1_000_000, freq="min")
lib.write("pure_numeric", df_num)

timeit(lambda: lib.read("pure_numeric"), "pandas read")
timeit(lambda: lib.read("pure_numeric", output_format="pyarrow"), "pyarrow read")

# Test 2: Pure string (string handler is the only difference)
print("\n=== Test 2: Pure string 9 cols, 1M rows ===")
str_pool = [f"val_{i:04d}" for i in range(100)]
df_str = pd.DataFrame({f"str_{i}": np.random.choice(str_pool, 1_000_000) for i in range(9)})
df_str.index = pd.date_range(end="2025-01-01", periods=1_000_000, freq="min")
lib.write("pure_string", df_str)

timeit(lambda: lib.read("pure_string"), "pandas read")
timeit(lambda: lib.read("pure_string", output_format="pyarrow"), "pyarrow read")

# Test 3: Mixed (3 str + 6 num), 1M rows - matching original benchmark
print("\n=== Test 3: Mixed 9 cols (3 str + 6 num), 1M rows ===")
df_mix = generate_df(1_000_000, 3, 6)
lib.write("mixed_9", df_mix)

timeit(lambda: lib.read("mixed_9"), "pandas read")
timeit(lambda: lib.read("mixed_9", output_format="pyarrow"), "pyarrow read")

# Test 4: Pure numeric wide (50 cols)
print("\n=== Test 4: Pure numeric 50 cols, 1M rows ===")
df_num50 = pd.DataFrame({f"num_{i}": np.random.uniform(0, 100, 1_000_000) for i in range(50)})
df_num50.index = pd.date_range(end="2025-01-01", periods=1_000_000, freq="min")
lib.write("pure_numeric_50", df_num50)

timeit(lambda: lib.read("pure_numeric_50"), "pandas read")
timeit(lambda: lib.read("pure_numeric_50", output_format="pyarrow"), "pyarrow read")

# Test 5: Pure string wide (50 cols)
print("\n=== Test 5: Pure string 50 cols, 1M rows ===")
df_str50 = pd.DataFrame({f"str_{i}": np.random.choice(str_pool, 1_000_000) for i in range(50)})
df_str50.index = pd.date_range(end="2025-01-01", periods=1_000_000, freq="min")
lib.write("pure_string_50", df_str50)

timeit(lambda: lib.read("pure_string_50"), "pandas read")
timeit(lambda: lib.read("pure_string_50", output_format="pyarrow"), "pyarrow read")

print("\nDone!")
