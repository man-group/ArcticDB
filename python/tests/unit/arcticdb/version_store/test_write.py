"""
Copyright 2023 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

import numpy as np
import pandas as pd
import pytest
from arcticdb_ext.exceptions import SortingException, ArcticException as ArcticNativeException
from arcticdb.util._versions import IS_PANDAS_TWO
from arcticdb.util.test import assert_frame_equal
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


class TestMissingStringPlaceholders:
    @pytest.mark.parametrize("dtype", [None, object, np.float32, np.double])
    def test_write_with_nan_none(self, lmdb_version_store, dtype):
        lib = lmdb_version_store
        sym = "nan"
        lib.write(sym, pd.DataFrame({"a": [None, np.nan]}, dtype=dtype))
        data = lib.read(sym).data
        assert_frame_equal(data, pd.DataFrame({"a": [None, np.nan]}, dtype=dtype))

    @pytest.mark.parametrize("dtype", [None, object])
    def test_write_with_nan_none_and_a_string(self, lmdb_version_store, dtype):
        lib = lmdb_version_store
        sym = "nan"
        lib.write(sym, pd.DataFrame({"a": [None, np.nan, "string"]}, dtype=dtype))
        data = lib.read(sym).data
        assert_frame_equal(data, pd.DataFrame({"a": [None, np.nan, "string"]}, dtype=dtype))

    @pytest.mark.parametrize("dtype", [None, object, np.double, np.float32])
    def test_write_only_nan_column(self, lmdb_version_store, dtype):
        lib = lmdb_version_store
        sym = "nan"
        lib.write(sym, pd.DataFrame({"a": [np.nan]}, dtype=dtype))
        data = lib.read(sym).data
        assert_frame_equal(data, pd.DataFrame({"a": [np.nan]}, dtype=dtype))


def test_write_bool_named_columns(lmdb_version_store):
    symbol = "bad_write"
    ts = pd.Timestamp("2020-01-01")

    df = pd.DataFrame({True: [1, 2, 3]}, index=pd.date_range(ts, periods=3))

    # The normalization exception is getting reraised as an ArcticNativeException so we check for that
    with pytest.raises(ArcticNativeException):
        lmdb_version_store.write(symbol, df)

    assert lmdb_version_store.list_symbols() == []
    assert lmdb_version_store.has_symbol(symbol) is False


@pytest.mark.parametrize(
    "idx", [pd.date_range(pd.Timestamp("2020-01-01"), periods=3), pd.RangeIndex(start=0, stop=3, step=1)]
)
def test_write_bool_named_index(lmdb_version_store, idx):
    symbol = "bad_write"

    df = pd.DataFrame({"col": [1, 2, 3]}, index=idx)
    df.index.name = True

    # The normalization exception is getting reraised as an ArcticNativeException so we check for that
    with pytest.raises(ArcticNativeException):
        lmdb_version_store.write(symbol, df)

    assert lmdb_version_store.list_symbols() == []
    assert lmdb_version_store.has_symbol(symbol) is False


@pytest.mark.parametrize(
    "idx", [pd.date_range(pd.Timestamp("2020-01-01"), periods=3), pd.RangeIndex(start=0, stop=3, step=1)]
)
@pytest.mark.parametrize("idx_names", [["index", True], [True, "index"]])
def test_write_bool_named_multi_index(lmdb_version_store, idx, idx_names):
    symbol = "bad_write"

    df = pd.DataFrame({"col": [1, 2, 3]}, index=pd.MultiIndex.from_arrays([idx, idx], names=idx_names))

    lmdb_version_store.write(symbol, df)

    # We do allow for the boolean index names in multiindex and they get normalized to strings
    # so this just tests the current behaviour and that we can read back the data correctly
    df.index.names = [str(n) for n in idx_names]

    assert_frame_equal(lmdb_version_store.read(symbol).data, df)


@pytest.mark.parametrize("first", [None, np.nan])
def test_write_fortran_style_data_starting_with_none(lmdb_version_store_v1, first):
    lib = lmdb_version_store_v1
    data = np.array([[first, "string"], ["aaa", "bbb"], ["ccc", "ddd"]])
    df0 = pd.DataFrame(data, columns=["a", "b"], index=pd.date_range("2025-01-01", periods=3))
    lib.write("fortran_style", df0)
    assert_frame_equal(lib.read("fortran_style").data, df0)

    df1 = pd.DataFrame(data, columns=["a", "b"], index=pd.date_range("2025-01-04", periods=3))
    lib.append("fortran_style", df1)
    assert_frame_equal(lib.read("fortran_style").data, pd.concat([df0, df1]))

    data_update = np.array([[first, "string"], ["aaa", "bbb"]])
    df2 = pd.DataFrame(data_update, columns=["a", "b"], index=pd.date_range("2025-01-02", periods=2))
    lib.update("fortran_style", df2)

    res_data = np.array(
        [[first, "string"], [first, "string"], ["aaa", "bbb"], [first, "string"], ["aaa", "bbb"], ["ccc", "ddd"]],
    )
    res = pd.DataFrame(res_data, columns=["a", "b"], index=pd.date_range("2025-01-01", periods=6))
    assert_frame_equal(lib.read("fortran_style").data, res)
