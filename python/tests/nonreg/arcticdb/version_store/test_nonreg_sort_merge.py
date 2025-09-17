import pandas as pd
import numpy as np


# This was a added as a bug repro for GH issue #1795.
def test_two_columns_with_different_dtypes(lmdb_library_dynamic_schema):
    lib = lmdb_library_dynamic_schema

    idx1 = pd.DatetimeIndex([pd.Timestamp("2024-01-02")])
    df1 = pd.DataFrame({"a": np.array([1], dtype="float"), "b": np.array([2], dtype="int64")}, index=idx1)

    b = np.array([3, 4], dtype="int64")

    idx = pd.DatetimeIndex([pd.Timestamp("2024-01-03"), pd.Timestamp("2024-01-01")])

    df2 = pd.DataFrame({"b": b}, index=idx)

    lib.write("sym", df1, staged=True, validate_index=False)
    lib.write("sym", df2, staged=True, validate_index=False)
    lib.sort_and_finalize_staged_data("sym")
    lib.read("sym")
