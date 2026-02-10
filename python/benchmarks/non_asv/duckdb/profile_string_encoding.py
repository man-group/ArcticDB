"""
Profile string encoding specifically: how does string format affect performance?

Default is LARGE_STRING (not CATEGORICAL as I assumed).
"""
import sys
import time
import numpy as np
import pandas as pd
import pyarrow as pa
from arcticdb import Arctic
from arcticdb.options import ArrowOutputStringFormat

np.random.seed(42)
ac = Arctic("lmdb://profile_strings2")
try:
    ac.delete_library("profile")
except Exception:
    pass
lib = ac.create_library("profile")

store = lib._nvs
n_rows = 1_000_000


def timeit(func, label, n=3):
    # warmup
    func()
    func()
    times = []
    for _ in range(n):
        t0 = time.perf_counter()
        func()
        times.append(time.perf_counter() - t0)
    best = min(times)
    print(f"  {label:<55} {best:.3f}s (best of {n})")
    return best


# Test 1: 3 string cols, 100 unique strings
print("=== Test 1: 3 str cols, 1M rows, 100 unique strings ===")
str_pool = [f"val_{i:04d}" for i in range(100)]
df = pd.DataFrame({f"str_{i}": np.random.choice(str_pool, n_rows) for i in range(3)})
df.index = pd.date_range(end="2025-01-01", periods=n_rows, freq="min")
lib.write("str_3_100u", df)

timeit(lambda: lib.read("str_3_100u"), "pandas")
timeit(lambda: lib.read("str_3_100u", output_format="pyarrow"), "pyarrow (large_string default)")
timeit(lambda: lib.read("str_3_100u", output_format="pyarrow",
                         arrow_string_format_default=ArrowOutputStringFormat.CATEGORICAL),
       "pyarrow (categorical)")

# Test 2: 3 string cols, 100K unique strings
print("\n=== Test 2: 3 str cols, 1M rows, 100K unique strings ===")
big_pool = [f"val_{i:08d}" for i in range(100_000)]
df2 = pd.DataFrame({f"str_{i}": np.random.choice(big_pool, n_rows) for i in range(3)})
df2.index = pd.date_range(end="2025-01-01", periods=n_rows, freq="min")
lib.write("str_3_100ku", df2)

timeit(lambda: lib.read("str_3_100ku"), "pandas")
timeit(lambda: lib.read("str_3_100ku", output_format="pyarrow"), "pyarrow (large_string default)")
timeit(lambda: lib.read("str_3_100ku", output_format="pyarrow",
                         arrow_string_format_default=ArrowOutputStringFormat.CATEGORICAL),
       "pyarrow (categorical)")

# Test 3: Mixed 9 cols (3 str + 6 num)
print("\n=== Test 3: Mixed 9 cols (3 str + 6 num), 1M rows, 100 unique ===")
data = {}
for i in range(3):
    data[f"str_{i}"] = np.random.choice(str_pool, n_rows)
for i in range(6):
    data[f"num_{i}"] = np.random.uniform(0, 100, n_rows)
df3 = pd.DataFrame(data)
df3.index = pd.date_range(end="2025-01-01", periods=n_rows, freq="min")
lib.write("mixed_9", df3)

timeit(lambda: lib.read("mixed_9"), "pandas")
timeit(lambda: lib.read("mixed_9", output_format="pyarrow"), "pyarrow (large_string default)")
timeit(lambda: lib.read("mixed_9", output_format="pyarrow",
                         arrow_string_format_default=ArrowOutputStringFormat.CATEGORICAL),
       "pyarrow (categorical)")

# Test 4: 20 string cols
print("\n=== Test 4: 20 str cols, 1M rows, 100 unique ===")
df4 = pd.DataFrame({f"str_{i}": np.random.choice(str_pool, n_rows) for i in range(20)})
df4.index = pd.date_range(end="2025-01-01", periods=n_rows, freq="min")
lib.write("str_20_100u", df4)

timeit(lambda: lib.read("str_20_100u"), "pandas")
timeit(lambda: lib.read("str_20_100u", output_format="pyarrow"), "pyarrow (large_string default)")
timeit(lambda: lib.read("str_20_100u", output_format="pyarrow",
                         arrow_string_format_default=ArrowOutputStringFormat.CATEGORICAL),
       "pyarrow (categorical)")

print("\nDone!")
