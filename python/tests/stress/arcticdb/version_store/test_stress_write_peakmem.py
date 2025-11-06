import pandas as pd
import numpy as np
import pyarrow as pa
import pytest
import tracemalloc


def create_numeric_df(num_rows=100_000, num_columns=100):
    df = pd.DataFrame(
        np.arange(num_rows * num_columns).reshape((num_rows, num_columns)),
        columns=[f"col_{i}" for i in range(num_columns)],
    )
    return df


def assert_write_allocates_small_fraction(lib, sym, create_obj_fn, acceptable_ratio=1.5):
    tracemalloc.start()

    obj = create_obj_fn()

    memory_after_creation, peak_memory_during_creation = tracemalloc.get_traced_memory()
    tracemalloc.reset_peak()

    lib.write(sym, obj)

    memory_after_write, peak_memory_during_write = tracemalloc.get_traced_memory()

    tracemalloc.stop()

    print(f"Memory after creation: {memory_after_creation/1000/1000:.2f}MB")
    print(f"Peak memory during write: {peak_memory_during_write/1000/1000:.2f}MB")
    print(f"Peak memory during write ratio: {peak_memory_during_write / memory_after_creation:.4f}")

    # Asserts that during write we use views of the data and we don't allocate surprising amounts of extra data.
    assert memory_after_write / memory_after_creation < acceptable_ratio
    assert peak_memory_during_write / memory_after_creation < acceptable_ratio

    return obj


def test_peakmem_write_basic(lmdb_version_store_v1):
    lib = lmdb_version_store_v1
    sym = "sym"

    original_df_after_write = assert_write_allocates_small_fraction(lib, sym, create_numeric_df)
    df_copy = create_numeric_df()

    # Verify that original dataframe was not modified during write.
    # We use `pd.testing.assert_frame_equal` to ensure even block structure is the same
    pd.testing.assert_frame_equal(original_df_after_write, df_copy)


def test_peakmem_write_arrow_basic(lmdb_version_store_arrow):
    lib = lmdb_version_store_arrow
    sym = "sym"

    def create_pyarrow_table():
        return pa.Table.from_pandas(create_numeric_df())

    original_table_after_write = assert_write_allocates_small_fraction(lib, sym, create_pyarrow_table)
    table_copy = create_pyarrow_table()

    # Verify that original table was not modified during write.
    original_table_after_write.equals(table_copy)


def test_peakmem_write_multiindex(lmdb_version_store_v1):
    lib = lmdb_version_store_v1
    sym = "sym"

    def create_multiindex(num_rows=100_000):
        df = create_numeric_df(num_rows=num_rows)
        num_indices = 3
        indices_df = create_numeric_df(num_rows, num_indices)
        df.index = pd.MultiIndex.from_frame(indices_df)
        df.index.names = [f"index_{i}" for i in range(num_indices)]
        return df

    original_df_after_write = assert_write_allocates_small_fraction(lib, sym, create_multiindex)
    df_copy = create_multiindex()

    # Verify that original dataframe was not modified during write.
    # We use `pd.testing.assert_frame_equal` to ensure even block structure is the same
    pd.testing.assert_frame_equal(original_df_after_write, df_copy)
