import time
from timeit import default_timer as timer

import numpy as np
import pandas as pd
import pyarrow as pa
import polars as pl


def test_pandas_concat():
    df0 = pd.DataFrame({"floats": [0.5], "strings": ["hello"], "timestamps": [pd.Timestamp("2025-01-01")], "ints": [0]})
    print(df0.dtypes)
    df1 = pd.DataFrame({"other col": [0]})
    res = pd.concat([df0, df1])
    # int column is now doubles :(
    # And still takes up n_rows * typesize bytes
    print(res)
    print(res.dtypes)


def test_pandas_sparse_array():
    # Stores dense array of values, and an int32 array of indexes that have values
    arr1 = pd.arrays.SparseArray([0, 1, 2, 3, 4, 5, 0])
    # Stores offsets to blocks of missing values
    arr2 = pd.arrays.SparseArray([0, 0, 0, 1, 2, 3, 4], kind="block")
    # Still cannot tell the difference between 0s and missing values
    print("fin")


def test_pyarrow():
    arr1 = pa.array([0, 1, 2, 3, 4])
    arr2 = pa.array([None, 0, 1, 2, 3, 4, None, None, 5])
    buf1 = arr1.buffers()
    buf2 = arr2.buffers()
    size1 = arr1.get_total_buffer_size()
    size2 = arr2.get_total_buffer_size()
    # Bool array adds 1.5% - 12.5% in storage space for 8 to 1 byte values respectively
    # But null values are also "stored", so a 99% sparse array still needs the same storage as a 1% sparse array

    rng = np.random.default_rng()
    num_rows = 100_000_000
    int_arr_np = rng.integers(0, 100, num_rows)
    num_repeats = 10
    for null_percentage in [0, 10, 50, 90, 99]:
        mask = None
        if null_percentage > 0:
            mask = rng.choice([True, False], num_rows, p=[null_percentage / 100, 1 - null_percentage / 100], shuffle=False)
        int_table = pa.table({"col": pa.Array.from_pandas(int_arr_np, mask)})
        int_df = pl.from_arrow(int_table)
        start = timer()
        for _ in range(num_repeats):
            new_int_arr = int_df.lazy().with_columns((2 * pl.col("col")).alias("2 * col")).collect()
        end = timer()
        print(f"Multiplication with {null_percentage}% nulls took {(end - start) / num_repeats}s")
        # Performance does not improve as nullness percentage increases
        # Same in pyarrow
    print("fin")


def test_polars_projection():
    rng = np.random.default_rng()
    num_rows = 100_000_000
    int_arr_np = rng.integers(0, 100, num_rows)
    num_repeats = 100
    for null_percentage in [0, 1, 10, 50, 90, 99]:
        mask = None
        if null_percentage > 0:
            mask = rng.choice([True, False], num_rows, p=[null_percentage / 100, 1 - null_percentage / 100], shuffle=False)
        int_table = pa.table({"col": pa.Array.from_pandas(int_arr_np, mask)})
        int_df = pl.from_arrow(int_table)
        start = timer()
        for _ in range(num_repeats):
            new_int_arr = int_df.lazy().with_columns((42 + pl.col("col")).alias("42 +  col")).collect()
        end = timer()
        print(f"Multiplication with {100 - null_percentage}% dense took {(end - start) / num_repeats}s")
    print("fin")


def test_polars_projection_2():
    rng = np.random.default_rng()
    num_rows = 100_000_000
    int_arr_np_1 = rng.integers(0, 100, num_rows)
    int_arr_np_2 = rng.integers(0, 100, num_rows)
    num_repeats = 100
    for null_percentage in [0, 1, 10, 50, 90, 99]:
        mask_1 = None
        mask_2 = None
        if null_percentage > 0:
            mask_1 = rng.choice([True, False], num_rows, p=[null_percentage / 100, 1 - null_percentage / 100], shuffle=False)
            mask_2 = rng.choice([True, False], num_rows, p=[null_percentage / 100, 1 - null_percentage / 100], shuffle=False)
        int_table = pa.table({"col1": pa.Array.from_pandas(int_arr_np_1, mask_1), "col2": pa.Array.from_pandas(int_arr_np_2, mask_2)})
        int_df = pl.from_arrow(int_table)
        start = timer()
        for _ in range(num_repeats):
            new_int_arr = int_df.lazy().with_columns((pl.col("col1") + pl.col("col2")).alias("col1 + col2")).collect()
        end = timer()
        print(f"Column addition with {100 - null_percentage}% dense took {(end - start) / num_repeats}s")


def test_polars_aggregation():
    rng = np.random.default_rng()
    num_rows = 100_000_000
    int_arr_np = rng.integers(0, 100, num_rows)
    num_repeats = 100
    for null_percentage in [0, 1, 10, 50, 90, 99]:
        mask = None
        if null_percentage > 0:
            mask = rng.choice([True, False], num_rows, p=[null_percentage / 100, 1 - null_percentage / 100], shuffle=False)
        int_table = pa.table({"col": pa.Array.from_pandas(int_arr_np, mask)})
        int_df = pl.from_arrow(int_table)
        start = timer()
        for _ in range(num_repeats):
            int_df.select(pl.sum("col"))
        end = timer()
        print(f"Summing column with {100 - null_percentage}% dense took {(end - start) / num_repeats}s")