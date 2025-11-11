"""
Copyright 2025 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

import numpy as np
import pandas as pd
import pyarrow as pa
import pytest

from arcticdb.exceptions import InternalException, NormalizationException
from arcticdb.util.test import assert_frame_equal


def test_write_arrow_read_pandas_no_index(lmdb_version_store_arrow):
    lib = lmdb_version_store_arrow
    sym = "test_write_arrow_read_pandas_no_index"
    table = pa.table({"col0": pa.array([0, 1], pa.int64()), "col1": pa.array(["a", "bb"], pa.string())})
    lib.write(sym, table)
    received = lib.read(sym, output_format="pandas").data
    assert isinstance(received, pd.DataFrame)
    expected = pd.DataFrame({"col0": np.arange(2, dtype=np.int64), "col1": ["a", "bb"]})
    assert_frame_equal(expected, received)


def test_write_arrow_read_pandas_with_index(lmdb_version_store_arrow):
    lib = lmdb_version_store_arrow
    sym = "test_write_arrow_read_pandas_with_index"
    table = pa.table(
        {
            "col": pa.array([0, 1], pa.int64()),
            "ts": pa.Array.from_pandas(pd.date_range("2025-01-01", periods=2), type=pa.timestamp("ns")),
        }
    )
    lib.write(sym, table, index_column="ts")
    received = lib.read(sym, output_format="pandas").data
    assert isinstance(received, pd.DataFrame)
    expected = pd.DataFrame({"col": np.arange(2, dtype=np.int64)}, index=pd.date_range("2025-01-01", periods=2))
    expected.index.name = "ts"
    assert_frame_equal(expected, received)


def test_write_pandas_df_with_specified_index_column(lmdb_version_store_v1):
    lib = lmdb_version_store_v1
    sym = "test_write_pandas_df_with_specified_index_column"
    df = pd.DataFrame({"col": [0, 1]})
    lib.write(sym, df, index_column="col")
    received = lib.read(sym).data
    assert_frame_equal(df, received)
    df.index = pd.date_range("2025-01-01", periods=2)
    df.index.name = "ts"
    lib.write(sym, df, index_column="col")
    received = lib.read(sym).data
    assert_frame_equal(df, received)
    lib.write(sym, df, index_column="ts")
    received = lib.read(sym).data
    assert_frame_equal(df, received)


def test_append_arrow_with_pandas(lmdb_version_store_arrow):
    lib = lmdb_version_store_arrow
    sym = "test_append_arrow_with_pandas"
    df = pd.DataFrame({"col1": np.arange(2, dtype=np.int64), "col2": np.arange(2, dtype=np.float32)})
    table = pa.table({"col1": pa.array([2, 3], pa.int64()), "col2": pa.array([2, 3], pa.float32())})

    lib.write(sym, df)
    with pytest.raises(NormalizationException) as e:
        lib.append(sym, table)
    assert "arrow table" in str(e.value).lower()

    lib.write(sym, table)
    with pytest.raises(NormalizationException) as e:
        lib.append(sym, df)
    assert "arrow table" in str(e.value).lower()


def test_update_arrow_with_pandas(lmdb_version_store_arrow):
    lib = lmdb_version_store_arrow
    sym = "test_update_arrow_with_pandas"
    df = pd.DataFrame({"col1": np.arange(2, dtype=np.int64)}, index=pd.date_range("2025-01-01", periods=2))
    df.index.name = "ts"
    table = pa.table(
        {
            "ts": pa.Array.from_pandas(pd.date_range("2025-01-02", periods=2), type=pa.timestamp("ns")),
            "col1": pa.array([2, 3], pa.int64()),
        }
    )

    lib.write(sym, df)
    with pytest.raises(NormalizationException) as e:
        lib.update(sym, table, index_column="ts")
    assert "arrow table" in str(e.value).lower()

    lib.write(sym, table)
    # Ideally this would be a NormalizationException as well, but we'll make this work before Arrow writes are fully
    # supported anyway so not a big deal
    with pytest.raises(InternalException):
        lib.update(sym, df)
