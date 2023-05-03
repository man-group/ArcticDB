"""
Copyright 2023 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""
import numpy as np
import pandas as pd
import pytest
from arcticdb.exceptions import SortingException, NormalizationException
from pandas import MultiIndex


def test_write_numpy_array(lmdb_version_store):
    symbol = "test_write_numpy_arr"
    arr = np.random.rand(2, 2, 2)
    lmdb_version_store.write(symbol, arr)

    np.array_equal(arr, lmdb_version_store.read(symbol).data)


def test_write_ascending_sorted_dataframe(lmdb_version_store):
    symbol = "write_sorted_asc"

    num_initial_rows = 20
    dtidx = np.arange(0, num_initial_rows)
    df = pd.DataFrame({"c": np.arange(0, num_initial_rows, dtype=np.int64)}, index=dtidx)

    lmdb_version_store.write(symbol, df)
    assert df.index.is_monotonic_increasing == True
    info = lmdb_version_store.get_info(symbol)
    assert info["sorted"] == "ASCENDING"


def test_write_descending_sorted_dataframe(lmdb_version_store):
    symbol = "write_sorted_desc"

    num_initial_rows = 20
    dtidx = np.arange(0, num_initial_rows)

    df = pd.DataFrame({"c": np.arange(0, num_initial_rows, dtype=np.int64)}, index=np.flip(dtidx, 0))

    lmdb_version_store.write(symbol, df)
    assert df.index.is_monotonic_decreasing == True
    info = lmdb_version_store.get_info(symbol)
    assert info["sorted"] == "DESCENDING"


def test_write_unsorted_sorted_dataframe(lmdb_version_store):
    symbol = "write_sorted_uns"

    num_initial_rows = 20
    dtidx = np.arange(0, num_initial_rows)

    df = pd.DataFrame({"c": np.arange(0, num_initial_rows, dtype=np.int64)}, index=np.roll(dtidx, 3))

    lmdb_version_store.write(symbol, df)
    assert df.index.is_monotonic_decreasing == False
    assert df.index.is_monotonic_increasing == False
    info = lmdb_version_store.get_info(symbol)
    assert info["sorted"] == "UNSORTED"


def test_write_unknown_sorted_dataframe(lmdb_version_store):
    symbol = "write_sorted_undef"
    lmdb_version_store.write(symbol, 1)
    info = lmdb_version_store.get_info(symbol)
    assert info["sorted"] == "UNKNOWN"


def test_write_non_sorted_exception(lmdb_version_store):
    symbol = "bad_write"
    num_initial_rows = 20
    num_rows = 20
    initial_timestamp = pd.Timestamp("2020-01-01")
    dtidx = np.roll(pd.date_range(initial_timestamp, periods=num_initial_rows), 3)
    df = pd.DataFrame({"c": np.arange(0, num_rows, dtype=np.int64)}, index=dtidx)
    assert df.index.is_monotonic_increasing == False

    with pytest.raises(SortingException):
        lmdb_version_store.write(symbol, df, validate_index=True)


def test_write_non_sorted_non_validate_index(lmdb_version_store):
    symbol = "bad_write"
    num_initial_rows = 20
    num_rows = 20
    initial_timestamp = pd.Timestamp("2020-01-01")
    dtidx = np.roll(pd.date_range(initial_timestamp, periods=num_initial_rows), 3)
    df = pd.DataFrame({"c": np.arange(0, num_rows, dtype=np.int64)}, index=dtidx)
    assert df.index.is_monotonic_increasing == False

    lmdb_version_store.write(symbol, df)


def test_write_non_sorted_multi_index_exception(lmdb_version_store):
    symbol = "bad_write"
    num_initial_rows = 20
    num_rows = 20
    initial_timestamp = pd.Timestamp("2020-01-01")
    dtidx1 = np.roll(pd.date_range(initial_timestamp, periods=num_initial_rows), 3)
    dtidx2 = np.arange(0, num_initial_rows)
    dtidx = np.roll(pd.date_range(initial_timestamp, periods=num_initial_rows), 3)
    df = pd.DataFrame(
        {"c": np.arange(0, num_rows, dtype=np.int64)},
        index=pd.MultiIndex.from_arrays([dtidx1, dtidx2], names=["datetime", "level"]),
    )
    assert isinstance(df.index, MultiIndex) == True
    assert df.index.is_monotonic_increasing == False

    with pytest.raises(SortingException):
        lmdb_version_store.write(symbol, df, validate_index=True)


def test_write_non_sorted_range_index_exception(lmdb_version_store):
    symbol = "bad_write"
    num_rows = 20
    dtidx = np.roll(pd.RangeIndex(0, num_rows, 1), 3)
    df = pd.DataFrame({"c": np.arange(0, num_rows, dtype=np.int64)}, index=dtidx)
    assert df.index.is_monotonic_increasing == False
    lmdb_version_store.write(symbol, df, validate_index=True)
