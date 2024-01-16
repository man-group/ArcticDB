"""
Copyright 2023 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""
import numpy as np
import pandas as pd
import pytest
from arcticdb.exceptions import SortingException, NormalizationException
from arcticdb.util._versions import IS_PANDAS_TWO
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
    assert info["sorted"] == "UNKNOWN"


def test_write_descending_sorted_dataframe(lmdb_version_store):
    symbol = "write_sorted_desc"

    num_initial_rows = 20
    dtidx = np.arange(0, num_initial_rows)

    df = pd.DataFrame({"c": np.arange(0, num_initial_rows, dtype=np.int64)}, index=np.flip(dtidx, 0))

    lmdb_version_store.write(symbol, df)
    assert df.index.is_monotonic_decreasing == True
    info = lmdb_version_store.get_info(symbol)
    assert info["sorted"] == "UNKNOWN"


def test_write_unsorted_sorted_dataframe(lmdb_version_store):
    symbol = "write_sorted_uns"

    num_initial_rows = 20
    dtidx = np.arange(0, num_initial_rows)

    df = pd.DataFrame({"c": np.arange(0, num_initial_rows, dtype=np.int64)}, index=np.roll(dtidx, 3))

    lmdb_version_store.write(symbol, df)
    assert df.index.is_monotonic_decreasing == False
    assert df.index.is_monotonic_increasing == False
    info = lmdb_version_store.get_info(symbol)
    assert info["sorted"] == "UNKNOWN"


def test_write_unknown_sorted_dataframe(lmdb_version_store):
    symbol = "write_sorted_undef"
    lmdb_version_store.write(symbol, 1)
    info = lmdb_version_store.get_info(symbol)
    assert info["sorted"] == "UNKNOWN"


def test_write_not_sorted_exception(lmdb_version_store):
    symbol = "bad_write"
    num_initial_rows = 20
    num_rows = 20
    initial_timestamp = pd.Timestamp("2020-01-01")
    dtidx = np.roll(pd.date_range(initial_timestamp, periods=num_initial_rows), 3)
    df = pd.DataFrame({"c": np.arange(0, num_rows, dtype=np.int64)}, index=dtidx)
    assert df.index.is_monotonic_increasing == False

    with pytest.raises(SortingException):
        lmdb_version_store.write(symbol, df, validate_index=True)


def test_write_not_sorted_non_validate_index(lmdb_version_store):
    symbol = "bad_write"
    num_initial_rows = 20
    num_rows = 20
    initial_timestamp = pd.Timestamp("2020-01-01")
    dtidx = np.roll(pd.date_range(initial_timestamp, periods=num_initial_rows), 0)
    df = pd.DataFrame({"c": np.arange(0, num_rows, dtype=np.int64)}, index=dtidx)
    # assert df.index.is_monotonic_increasing == False

    lmdb_version_store.write(symbol, df)


def test_write_not_sorted_multi_index_exception(lmdb_version_store):
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


@pytest.mark.parametrize("index_type", ["range", "int64"])
@pytest.mark.parametrize("sorted", [True, False])
@pytest.mark.parametrize("validate_index", [True, False])
def test_write_non_timestamp_index(lmdb_version_store, index_type, sorted, validate_index):
    lib = lmdb_version_store
    symbol = "test_write_range_index"
    num_rows = 20
    shift = 0 if sorted else 3
    if index_type == "range":
        idx = np.roll(pd.RangeIndex(0, num_rows, 1), shift)
    elif index_type == "int64":
        idx = np.roll(pd.Index(range(20), dtype=np.int64) if IS_PANDAS_TWO else pd.Int64Index(range(20)), shift)
    df = pd.DataFrame({"c": np.arange(0, num_rows, dtype=np.int64)}, index=idx)
    assert df.index.is_monotonic_increasing == sorted
    lib.write(symbol, df, validate_index=validate_index)
    info = lib.get_info(symbol)
    assert info["sorted"] == "UNKNOWN"


