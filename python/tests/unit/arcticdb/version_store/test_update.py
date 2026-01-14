"""
Copyright 2026 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

import pandas as pd
import numpy as np
import pytest
from itertools import product
import datetime
import random
from arcticdb import DataError

from arcticdb.util.test import (
    random_strings_of_length,
    random_string,
    random_floats,
    assert_frame_equal,
)
from arcticdb.exceptions import InternalException, SortingException, NormalizationException, SchemaException
from arcticdb_ext.version_store import StreamDescriptorMismatch
from tests.util.date import DateRange
from pandas import MultiIndex
import arcticdb
from arcticdb.version_store import VersionedItem
from arcticdb.version_store.library import UpdatePayload
from arcticdb.exceptions import ErrorCode, ErrorCategory
from arcticdb.toolbox.library_tool import LibraryTool
from arcticdb_ext.storage import KeyType


def test_update_single_dates(lmdb_version_store_dynamic_schema):
    lib = lmdb_version_store_dynamic_schema
    df1 = pd.DataFrame(index=[pd.Timestamp(2022, 1, 3)], data=2220103.0, columns=["a"])
    df2 = pd.DataFrame(index=[pd.Timestamp(2021, 12, 22)], data=211222.0, columns=["a"])
    df3 = pd.DataFrame(index=[pd.Timestamp(2021, 12, 29)], data=2211229.0, columns=["a"])
    sym = "data6"
    lib.update(sym, df1, upsert=True)
    lib.update(sym, df2, upsert=True)
    lib.update(sym, df3, upsert=True)

    expected = pd.concat((df2, df3, df1))
    assert_frame_equal(lib.read(sym).data, expected)


def test_update(version_store_factory):
    lmdb_version_store = version_store_factory(col_per_group=2, row_per_segment=2)
    symbol = "update_no_daterange"

    idx = pd.date_range("1970-01-01", periods=100, freq="D")
    df = pd.DataFrame({"a": np.arange(len(idx), dtype="float")}, index=idx)
    lmdb_version_store.write(symbol, df)

    idx2 = pd.date_range("1970-01-12", periods=10, freq="D")
    df2 = pd.DataFrame({"a": np.arange(1000, 1000 + len(idx2), dtype="float")}, index=idx2)
    lmdb_version_store.update(symbol, df2)

    vit = lmdb_version_store.read(symbol)
    df.update(df2)

    assert_frame_equal(vit.data, df)


def test_update_long_strides(s3_version_store):
    lib = s3_version_store
    symbol = "test_update_long_strides"

    write_df = pd.DataFrame({"A": 7 * [1]}, index=pd.date_range("2023-02-01", periods=7))
    assert write_df.index.values.strides[0] == 8
    lib.write(symbol, write_df)

    update_df = write_df[write_df.index.isin([pd.Timestamp(2023, 2, 1), pd.Timestamp(2023, 2, 6)])].copy()
    update_df["A"] = 999
    assert update_df.index.values.strides[0] in (8, 40)

    lib.update(symbol, update_df)

    expected = pd.DataFrame(
        {"A": [999, 999, 1]},
        index=[
            pd.Timestamp(2023, 2, 1),
            pd.Timestamp(2023, 2, 6),
            pd.Timestamp(2023, 2, 7),
        ],
    )
    received = lib.read(symbol).data
    pd.testing.assert_frame_equal(expected, received)


def gen_params():
    p = [
        list(range(2, 4)),
        list(range(-1, 1)),
        list(range(-1, 1)),
        list(range(17, 19)),
        list(range(7, 8)),
        list(range(5, 6)),
    ]
    return list(product(*p))


@pytest.mark.parametrize(
    "col_per_group, start_increment, end_increment, update_start, iterations, start_dist",
    gen_params(),
)
def test_update_repeatedly_dynamic_schema(
    version_store_factory,
    col_per_group,
    start_increment,
    end_increment,
    update_start,
    iterations,
    start_dist,
):
    lmdb_version_store = version_store_factory(col_per_group=col_per_group, row_per_segment=2, dynamic_schema=True)

    symbol = "update_dynamic_schema"

    idx = pd.date_range("1970-01-01", periods=100, freq="D")
    df = pd.DataFrame({"a": np.arange(len(idx), dtype="float")}, index=idx)
    lmdb_version_store.write(symbol, df)
    update_end = update_start + start_dist

    for x in range(iterations):
        adjust_start = x * start_increment
        adjust_end = x * end_increment
        update_date = "1970-01-{}".format(update_start + adjust_start)
        periods = (update_end + adjust_end) - update_start
        if periods <= 0:
            continue

        idx2 = pd.date_range(update_date, periods=periods, freq="D")
        df2 = pd.DataFrame({"a": np.arange(1000 + x, 1000 + x + len(idx2), dtype="float")}, index=idx2)
        lmdb_version_store.update(symbol, df2)

        vit = lmdb_version_store.read(symbol)
        df.update(df2)
        assert_frame_equal(vit.data, df)


@pytest.mark.parametrize(
    "col_per_group, start_increment, end_increment, update_start, iterations, start_dist",
    gen_params(),
)
def test_update_repeatedly_dynamic_schema_hashed(
    version_store_factory,
    col_per_group,
    start_increment,
    end_increment,
    update_start,
    iterations,
    start_dist,
):
    lmdb_version_store = version_store_factory(col_per_group=col_per_group, row_per_segment=2, dynamic_schema=True)

    symbol = "update_dynamic_schema"

    idx = pd.date_range("1970-01-01", periods=100, freq="D")
    l = len(idx)
    df = pd.DataFrame(
        {
            "a": np.arange(l, dtype="float"),
            "b": np.arange(1, l + 1, dtype="float"),
            "c": np.arange(2, l + 2, dtype="float"),
            "d": np.arange(3, l + 3, dtype="float"),
            "e": np.arange(4, l + 4, dtype="float"),
        },
        index=idx,
    )

    lmdb_version_store.write(symbol, df)
    update_end = update_start + start_dist

    for x in range(iterations):
        adjust_start = x * start_increment
        adjust_end = x * end_increment
        update_date = "1970-01-{}".format(update_start + adjust_start)
        periods = (update_end + adjust_end) - update_start
        if periods <= 0:
            continue

        idx2 = pd.date_range(update_date, periods=periods, freq="D")
        l = len(idx2)
        df2 = pd.DataFrame(
            {
                "a": np.arange(x, l + x, dtype="float"),
                "b": np.arange(1 + x, l + 1 + x, dtype="float"),
                "c": np.arange(2 + x, l + 2 + x, dtype="float"),
                "d": np.arange(3 + x, l + 3 + x, dtype="float"),
                "e": np.arange(4 + x, l + 4 + x, dtype="float"),
            },
            index=idx2,
        )

        lmdb_version_store.update(symbol, df2)

        vit = lmdb_version_store.read(symbol)
        df.update(df2)
        assert_frame_equal(vit.data, df)


@pytest.mark.parametrize(
    "col_per_group, start_increment, end_increment, update_start, iterations, start_dist",
    gen_params(),
)
def test_update_repeatedly(
    version_store_factory,
    col_per_group,
    start_increment,
    end_increment,
    update_start,
    iterations,
    start_dist,
):
    lmdb_version_store = version_store_factory(col_per_group=col_per_group, row_per_segment=2)

    symbol = "update_no_daterange"

    idx = pd.date_range("1970-01-01", periods=100, freq="D")
    df = pd.DataFrame({"a": np.arange(len(idx), dtype="float")}, index=idx)
    lmdb_version_store.write(symbol, df)
    update_end = update_start + start_dist

    for x in range(iterations):
        adjust_start = x * start_increment
        adjust_end = x * end_increment
        update_date = "1970-01-{}".format(update_start + adjust_start)
        periods = (update_end + adjust_end) - update_start
        if periods <= 0:
            continue

        idx2 = pd.date_range(update_date, periods=periods, freq="D")
        df2 = pd.DataFrame({"a": np.arange(1000 + x, 1000 + x + len(idx2), dtype="float")}, index=idx2)
        lmdb_version_store.update(symbol, df2)

        vit = lmdb_version_store.read(symbol)
        df.update(df2)
        assert_frame_equal(vit.data, df)


@pytest.mark.parametrize(
    "col_per_group, start_increment, end_increment, update_start, iterations, start_dist",
    gen_params(),
)
def test_update_repeatedly_with_strings(
    version_store_factory,
    col_per_group,
    start_increment,
    end_increment,
    update_start,
    iterations,
    start_dist,
):
    lmdb_version_store = version_store_factory(col_per_group=col_per_group, row_per_segment=2)

    symbol = "update_no_daterange"

    idx = pd.date_range("1970-01-01", periods=100, freq="D")
    df = pd.DataFrame({"a": [random_string(10) for _ in range(len(idx))]}, index=idx)
    lmdb_version_store.write(symbol, df)
    update_end = update_start + start_dist

    for x in range(iterations):
        adjust_start = x * start_increment
        adjust_end = x * end_increment
        update_date = "1970-01-{}".format(update_start + adjust_start)
        periods = (update_end + adjust_end) - update_start
        if periods <= 0:
            continue

        idx2 = pd.date_range(update_date, periods=periods, freq="D")
        df2 = pd.DataFrame({"a": [random_string(10) for _ in range(len(idx2))]}, index=idx2)
        lmdb_version_store.update(symbol, df2)

        vit = lmdb_version_store.read(symbol)
        df.update(df2)
        assert_frame_equal(vit.data, df)


def test_update_with_snapshot(version_store_factory):
    lmdb_version_store = version_store_factory(col_per_group=2, row_per_segment=2)

    symbol = "update_no_daterange"

    idx = pd.date_range("1970-01-01", periods=100, freq="D")
    df = pd.DataFrame({"a": np.arange(len(idx), dtype="float")}, index=idx)
    original_df = df.copy(deep=True)
    lmdb_version_store.write(symbol, df)

    lmdb_version_store.snapshot("my_snap")

    idx2 = pd.date_range("1970-01-12", periods=10, freq="D")
    df2 = pd.DataFrame({"a": np.arange(1000, 1000 + len(idx2), dtype="float")}, index=idx2)
    lmdb_version_store.update(symbol, df2)

    assert_frame_equal(lmdb_version_store.read(symbol, as_of=0).data, original_df)
    assert_frame_equal(lmdb_version_store.read(symbol, as_of="my_snap").data, original_df)

    df.update(df2)

    vit = lmdb_version_store.read(symbol)
    assert_frame_equal(vit.data, df)
    assert_frame_equal(lmdb_version_store.read(symbol, as_of=1).data, df)
    assert_frame_equal(lmdb_version_store.read(symbol, as_of="my_snap").data, original_df)

    lmdb_version_store.delete(symbol)
    assert lmdb_version_store.list_versions() == []

    assert_frame_equal(lmdb_version_store.read(symbol, as_of="my_snap").data, original_df)


def generate_dataframe(columns, dt, num_days, num_rows_per_day):
    dataframes = []
    for _ in range(num_days):
        index = pd.Index([dt + datetime.timedelta(seconds=s) for s in range(num_rows_per_day)])
        vals = {c: random_floats(num_rows_per_day) for c in columns}
        new_df = pd.DataFrame(data=vals, index=index)
        dataframes.append(new_df)
        dt = dt + datetime.timedelta(days=1)

    return pd.concat(dataframes)


def test_update_with_daterange(lmdb_version_store):
    lib = lmdb_version_store

    def get_frame_for_date_range(start, end):
        df = pd.DataFrame(index=pd.date_range(start, end, freq="D"))
        df["value"] = df.index.day
        return df

    df1 = get_frame_for_date_range("2020-01-01", "2021-01-01")
    lib.write("test", df1)

    df2 = get_frame_for_date_range("2020-06-01", "2021-06-01")
    date_range = DateRange("2020-01-01", "2022-01-01")
    lib.update("test", df2, date_range=date_range)
    stored_df = lib.read("test").data
    assert stored_df.index.min() == df2.index.min()
    assert stored_df.index.max() == df2.index.max()


def test_update_schema_change(lmdb_version_store_dynamic_schema):
    dt = datetime.datetime(2019, 4, 8, 0, 0, 0)
    column_length = 6
    num_days = 10
    num_rows_per_day = 2
    num_columns = 8
    columns = random_strings_of_length(num_columns, column_length, True)
    symbol = "test_update_schema_change"

    df1 = generate_dataframe(random.sample(columns, 6), dt, num_days, num_rows_per_day)
    lmdb_version_store_dynamic_schema.write(symbol, df1)
    df2 = generate_dataframe(columns, dt, num_days, num_rows_per_day)

    block_size = 6
    # offsets =  np.random.randint(len(df2), size=num_samples)
    offsets = [7]
    samples = [df2.iloc[x : x + block_size] for x in offsets]

    for sample in samples:
        missing = df1.columns.symmetric_difference(sample.columns)
        for col in missing:
            df1[col] = np.nan

        df1.update(sample)
        lmdb_version_store_dynamic_schema.update(symbol, sample)

    vit = lmdb_version_store_dynamic_schema.read(symbol)
    df1.sort_index(axis=1, inplace=True)
    result = vit.data
    result.sort_index(axis=1, inplace=True)
    assert_frame_equal(df1, vit.data)


def test_update_schema_change_with_params(lmdb_version_store_dynamic_schema):
    lib = lmdb_version_store_dynamic_schema

    dt = datetime.datetime(2019, 4, 8, 0, 0, 0)
    column_length = 6
    num_days = 10
    num_rows_per_day = 2
    num_columns = 8
    columns = random_strings_of_length(num_columns, column_length, True)
    symbol = "test_update_schema_change"

    df1 = generate_dataframe(random.sample(columns, 6), dt, num_days, num_rows_per_day)
    lib.write(symbol, df1)
    df2 = generate_dataframe(columns, dt, num_days, num_rows_per_day)

    block_size = 6
    num_samples = 1
    # offsets =  np.random.randint(len(df2), size=num_samples)
    offsets = [7]
    samples = [df2.iloc[x : x + block_size] for x in offsets]

    for sample in samples:
        missing = df1.columns.symmetric_difference(sample.columns)
        for col in missing:
            df1[col] = np.nan

        df1.update(sample)
        lib.update(symbol, sample, dynamic_schema=True)

    vit = lib.read(symbol, dynamic_schema=True)
    df1.sort_index(axis=1, inplace=True)
    result = vit.data
    result.sort_index(axis=1, inplace=True)
    assert_frame_equal(df1, vit.data)


def test_update_single_line(lmdb_version_store_dynamic_schema):
    dt = datetime.datetime(2019, 4, 8, 0, 0, 0)
    column_length = 6
    num_days = 10
    num_rows_per_day = 1
    num_columns = 8
    columns = random_strings_of_length(num_columns, column_length, True)
    symbol = "test_update_single_line"

    df = generate_dataframe(columns, dt, num_days, num_rows_per_day)
    x = [False] * len(df)

    first = True

    while not all(x):
        start = random.randrange(0, len(df))
        try:
            idx = x.index(False, start)
        except ValueError:
            pass

        x[idx] = True
        if first:
            lmdb_version_store_dynamic_schema.write(symbol, df[idx:])
            info = lmdb_version_store_dynamic_schema.get_info(symbol)
            print(info["sorted"])
            assert info["sorted"] == "ASCENDING"
            first = False
        else:
            info = lmdb_version_store_dynamic_schema.get_info(symbol)
            print(info["sorted"])
            assert info["sorted"] == "ASCENDING"
            lmdb_version_store_dynamic_schema.update(symbol, df[idx:])
            info = lmdb_version_store_dynamic_schema.get_info(symbol)
            print(info["sorted"])
            assert info["sorted"] == "ASCENDING"

    vit = lmdb_version_store_dynamic_schema.read(symbol)
    df.sort_index(axis=1, inplace=True)
    result = vit.data
    result.sort_index(axis=1, inplace=True)
    info = lmdb_version_store_dynamic_schema.get_info(symbol)
    print(info["sorted"])
    assert info["sorted"] == "ASCENDING"
    assert_frame_equal(df, vit.data)


def test_update_pickled_data(lmdb_version_store):
    symbol = "test_update_pickled_data"
    idx = pd.date_range("2000-01-01", periods=3)
    df = pd.DataFrame({"a": [[1, 2], [3, 4], [5, 6]]}, index=idx)
    lmdb_version_store.write(symbol, df, pickle_on_failure=True)
    assert lmdb_version_store.is_symbol_pickled(symbol)
    df2 = pd.DataFrame({"a": [1000]}, index=idx[1:2])
    with pytest.raises(InternalException) as e_info:
        lmdb_version_store.update(symbol, df2)


def test_non_cstyle_numpy_update(lmdb_version_store):
    symbol = "test_non_cstyle_numpy_update"
    not_sorted_arr_1 = [
        [1673740800, 846373.91],
        [1673654400, 2243057.35],
        [1673568000, 1091657.66],
        [1673481600, 1523618.28],
    ]
    not_sorted_arr_2 = [
        [1674000000, 990047.95],
        [1673913600, 873934.74],
        [1673827200, 1602216.77],
        [1673740800, 846373.91],
    ]

    def _create_product_candles_df(arr):
        timestamps = [pd.to_datetime(t[0], unit="s") for t in arr]
        sorted_df = pd.DataFrame(data=arr, index=timestamps, columns=["time_start", "volume"])
        return sorted_df.sort_index()

    sorted_df_1 = _create_product_candles_df(not_sorted_arr_1)
    sorted_df_2 = _create_product_candles_df(not_sorted_arr_2)

    lmdb_version_store.write(symbol, sorted_df_1)
    lmdb_version_store.update(symbol, sorted_df_2)
    after_arctic = lmdb_version_store.read(symbol).data
    before_arctic = pd.concat([sorted_df_1.iloc[:-1], sorted_df_2])
    assert_frame_equal(after_arctic, before_arctic)


@pytest.mark.parametrize("existing_df_sortedness", ("ASCENDING", "DESCENDING", "UNSORTED"))
@pytest.mark.parametrize("update_df_sortedness", ("ASCENDING", "DESCENDING", "UNSORTED"))
@pytest.mark.parametrize("date_range_arg_provided", (True, False))
def test_update_sortedness_checks(
    lmdb_version_store,
    existing_df_sortedness,
    update_df_sortedness,
    date_range_arg_provided,
):
    lib = lmdb_version_store
    symbol = "test_update_sortedness_checks"
    num_rows = 10
    data = np.arange(num_rows)
    ascending_idx = pd.date_range("2024-01-15", periods=num_rows)
    ascending_df = pd.DataFrame({"col": data}, index=ascending_idx)
    descending_df = pd.DataFrame({"col": data}, index=pd.DatetimeIndex(reversed(ascending_idx)))
    unsorted_df = pd.DataFrame({"col": data}, index=pd.DatetimeIndex(np.roll(ascending_idx, num_rows // 2)))

    date_range = (pd.Timestamp("2024-01-13"), pd.Timestamp("2024-01-17")) if date_range_arg_provided else None

    if existing_df_sortedness == "ASCENDING":
        write_df = ascending_df
    elif existing_df_sortedness == "DESCENDING":
        write_df = descending_df
    else:
        # existing_df_sortedness == "UNSORTED":
        write_df = unsorted_df
    lib.write(symbol, write_df)
    assert lib.get_info(symbol)["sorted"] == existing_df_sortedness

    if update_df_sortedness == "ASCENDING":
        update_df = ascending_df
    elif update_df_sortedness == "DESCENDING":
        update_df = descending_df
    else:
        # update_df_sortedness == "UNSORTED":
        update_df = unsorted_df

    if existing_df_sortedness == "ASCENDING" and update_df_sortedness == "ASCENDING":
        lib.update(symbol, update_df, date_range=date_range)
        assert lib.get_info(symbol)["sorted"] == "ASCENDING"
    else:
        with pytest.raises(SortingException):
            lib.update(symbol, update_df, date_range=date_range)


def test_update_not_sorted_input_multi_index_exception(lmdb_version_store):
    symbol = "bad_write"
    num_initial_rows = 20
    num_rows = 20
    initial_timestamp = pd.Timestamp("2020-01-01")
    dtidx1 = pd.date_range(initial_timestamp, periods=num_initial_rows)
    dtidx2 = np.arange(0, num_initial_rows)
    df = pd.DataFrame(
        {"c": np.arange(0, num_rows, dtype=np.int64)},
        index=pd.MultiIndex.from_arrays([dtidx1, dtidx2], names=["datetime", "level"]),
    )
    assert isinstance(df.index, MultiIndex) == True
    assert df.index.is_monotonic_increasing == True
    lmdb_version_store.write(symbol, df)

    num_rows = 20
    initial_timestamp = pd.Timestamp("2020-01-01")
    dtidx1 = np.roll(pd.date_range(initial_timestamp, periods=num_initial_rows), 3)
    dtidx2 = np.arange(0, num_initial_rows)
    df = pd.DataFrame(
        {"c": np.arange(0, num_rows, dtype=np.int64)},
        index=pd.MultiIndex.from_arrays([dtidx1, dtidx2], names=["datetime", "level"]),
    )
    assert isinstance(df.index, MultiIndex) == True
    assert df.index.is_monotonic_increasing == False

    with pytest.raises(SortingException):
        lmdb_version_store.update(symbol, df)


def test_update_not_sorted_existing_multi_index_exception(lmdb_version_store):
    symbol = "bad_write"
    num_initial_rows = 20
    num_rows = 20
    initial_timestamp = pd.Timestamp("2020-01-01")
    dtidx1 = np.roll(pd.date_range(initial_timestamp, periods=num_initial_rows), 3)
    dtidx2 = np.arange(0, num_initial_rows)
    df = pd.DataFrame(
        {"c": np.arange(0, num_rows, dtype=np.int64)},
        index=pd.MultiIndex.from_arrays([dtidx1, dtidx2], names=["datetime", "level"]),
    )
    assert isinstance(df.index, MultiIndex) == True
    assert df.index.is_monotonic_increasing == False
    lmdb_version_store.write(symbol, df)

    num_rows = 20
    initial_timestamp = pd.Timestamp("2020-01-01")
    dtidx1 = pd.date_range(initial_timestamp, periods=num_initial_rows)
    dtidx2 = np.arange(0, num_initial_rows)
    df = pd.DataFrame(
        {"c": np.arange(0, num_rows, dtype=np.int64)},
        index=pd.MultiIndex.from_arrays([dtidx1, dtidx2], names=["datetime", "level"]),
    )
    assert isinstance(df.index, MultiIndex) == True
    assert df.index.is_monotonic_increasing == True

    with pytest.raises(SortingException):
        lmdb_version_store.update(symbol, df)


def test_update_not_sorted_range_index_exception(lmdb_version_store):
    symbol = "bad_write"
    num_rows = 20
    dtidx = pd.RangeIndex(0, num_rows, 1)
    df = pd.DataFrame({"c": np.arange(0, num_rows, dtype=np.int64)}, index=dtidx)
    assert df.index.is_monotonic_increasing == True
    lmdb_version_store.write(symbol, df)

    symbol = "bad_write"
    num_rows = 20
    dtidx = pd.RangeIndex(0, num_rows, 1)
    df = pd.DataFrame({"c": np.arange(0, num_rows, dtype=np.int64)}, index=dtidx)
    assert df.index.is_monotonic_increasing == True
    with pytest.raises(InternalException):
        lmdb_version_store.update(symbol, df)


class TestBatchUpdate:
    def test_success(self, lmdb_library):
        lib = lmdb_library

        initial_data = {
            "symbol_1": pd.DataFrame({"a": range(20)}, index=pd.date_range("2024-01-01", "2024-01-20")),
            "symbol_2": pd.DataFrame({"b": range(30, 60)}, index=pd.date_range("2024-02-01", periods=30)),
            "symbol_3": pd.DataFrame({"c": range(70, 80)}, index=pd.date_range("2024-03-01", periods=10)),
        }
        for symbol, data in initial_data.items():
            lib.write(symbol, data)

        batch_update_queries = {
            "symbol_1": UpdatePayload(
                "symbol_1", pd.DataFrame({"a": range(0, -5, -1)}, index=pd.date_range("2024-01-10", periods=5))
            ),
            "symbol_2": UpdatePayload(
                "symbol_2",
                pd.DataFrame({"b": range(-10, -20, -1)}, index=pd.date_range("2024-02-05", periods=10, freq="h")),
            ),
        }

        result = lib.update_batch(batch_update_queries.values())
        assert len(result) == len(batch_update_queries)
        for i in range(len(result)):
            versioned_item = result[i]
            assert isinstance(versioned_item, VersionedItem)
            assert versioned_item.symbol == list(batch_update_queries.keys())[i]

        expected = {
            "symbol_1": pd.concat(
                [
                    pd.DataFrame({"a": range(0, 9)}, pd.date_range("2024-01-01", periods=9)),
                    batch_update_queries["symbol_1"].data,
                    pd.DataFrame({"a": range(14, 20)}, pd.date_range("2024-01-15", periods=6)),
                ]
            ),
            "symbol_2": pd.concat(
                [
                    pd.DataFrame({"b": range(30, 34)}, pd.date_range("2024-02-01", "2024-02-04")),
                    batch_update_queries["symbol_2"].data,
                    pd.DataFrame({"b": range(35, 60)}, pd.date_range("2024-02-06", periods=25)),
                ]
            ),
            "symbol_3": initial_data["symbol_3"],
        }

        updated = [lib.read(symbol) for symbol in expected]
        for vit in updated:
            if vit.symbol in batch_update_queries.values():
                assert vit.version == 1
            assert_frame_equal(vit.data, expected[vit.symbol])

    def test_date_range(self, lmdb_library):
        lib = lmdb_library
        lib.write("symbol_1", pd.DataFrame({"a": range(5)}, index=pd.date_range("2024-01-01", "2024-01-05")))
        lib.write("symbol_2", pd.DataFrame({"b": range(10, 16)}, index=pd.date_range("2024-01-05", "2024-01-10")))
        update_queries = [
            UpdatePayload(
                "symbol_1",
                data=pd.DataFrame({"a": range(-10, 0)}, index=pd.date_range("2024-01-01", "2024-01-10")),
                date_range=(pd.Timestamp("2024-01-02"), pd.Timestamp("2024-01-02")),
            ),
            UpdatePayload(
                "symbol_2",
                data=pd.DataFrame({"b": range(100, 120)}, index=pd.date_range("2024-01-01", "2024-01-20")),
                date_range=(pd.Timestamp("2024-01-05"), pd.Timestamp("2024-01-11")),
            ),
        ]
        lib.update_batch(update_queries)
        symbol1, symbol2 = lib.read("symbol_1").data, lib.read("symbol_2").data
        expected1 = pd.DataFrame({"a": [0, -9, 2, 3, 4]}, index=pd.date_range("2024-01-01", "2024-01-05"))
        assert_frame_equal(symbol1, expected1)
        expected2 = pd.DataFrame({"b": range(104, 111)}, index=pd.date_range("2024-01-05", "2024-01-11"))
        assert_frame_equal(symbol2, expected2)

    def test_metadata(self, lmdb_library):
        lib = lmdb_library
        lib.write(
            "symbol_1",
            pd.DataFrame({"a": [1]}, index=pd.DatetimeIndex([pd.Timestamp("2024-01-01")])),
            metadata={"meta": "data"},
        )
        lib.write(
            "symbol_2",
            pd.DataFrame({"b": [2]}, index=pd.DatetimeIndex([pd.Timestamp("2024-01-01")])),
            metadata={"meta": [1]},
        )
        update_result = lib.update_batch(
            [
                UpdatePayload(
                    "symbol_1",
                    pd.DataFrame({"a": [2]}, index=pd.DatetimeIndex([pd.Timestamp("2024-01-01")])),
                    metadata={1, 2},
                ),
                UpdatePayload(
                    "symbol_2",
                    pd.DataFrame({"b": [3]}, index=pd.DatetimeIndex([pd.Timestamp("2024-01-01")])),
                    metadata=[4, 5],
                ),
            ]
        )
        assert update_result[0].metadata == {1, 2}
        assert lib.read("symbol_1").metadata == {1, 2}
        assert update_result[1].metadata == [4, 5]
        assert lib.read("symbol_2").metadata == [4, 5]

    def test_empty_payload_list(self, lmdb_library):
        lib = lmdb_library
        symbol_1_data = pd.DataFrame({"a": [1]}, index=pd.DatetimeIndex([pd.Timestamp("2024-01-01")]))
        lib.write("symbol_1", symbol_1_data)
        symbol_2_data = pd.DataFrame({"b": [2]}, index=pd.DatetimeIndex([pd.Timestamp("2024-01-01")]))
        lib.write("symbol_2", symbol_2_data)
        update_result = lib.update_batch([])
        assert update_result == []
        symbol_1_vit, symbol_2_vit = lib.read("symbol_1"), lib.read("symbol_2")
        assert_frame_equal(symbol_1_vit.data, symbol_1_data)
        assert_frame_equal(symbol_2_vit.data, symbol_2_data)
        assert symbol_1_vit.version == 0
        assert symbol_2_vit.version == 0

    def test_missing_symbol_is_error(self, lmdb_library):
        lib = lmdb_library
        lib.write("symbol_1", pd.DataFrame({"a": [1]}, index=pd.DatetimeIndex([pd.Timestamp("2024-01-01")])))
        lib.write("symbol_2", pd.DataFrame({"b": [2]}, index=pd.DatetimeIndex([pd.Timestamp("2024-01-01")])))
        update_result = lib.update_batch(
            [
                UpdatePayload(
                    symbol="symbol_3", data=pd.DataFrame({"a": [1, 2]}, index=pd.date_range("2024-01-02", periods=2))
                ),
                UpdatePayload(
                    symbol="symbol_1", data=pd.DataFrame({"a": [2, 3]}, index=pd.date_range("2024-01-02", periods=2))
                ),
            ]
        )
        assert set(lib.list_symbols()) == {"symbol_1", "symbol_2"}
        assert isinstance(update_result[0], DataError)
        assert update_result[0].symbol == "symbol_3"
        assert update_result[0].error_code == ErrorCode.E_NO_SUCH_VERSION
        assert update_result[0].error_category == ErrorCategory.MISSING_DATA
        assert all(
            expected in update_result[0].exception_string for expected in ["upsert", "Cannot update", "symbol_3"]
        )
        assert isinstance(update_result[1], VersionedItem)

        symbol_1_vit = lib.read("symbol_1")
        assert symbol_1_vit.version == 1
        assert len(lib.list_versions("symbol_1")) == 2
        assert_frame_equal(
            symbol_1_vit.data, pd.DataFrame({"a": [1, 2, 3]}, index=pd.date_range("2024-01-01", periods=3))
        )

    def test_update_batch_upsert_creates_symbol(self, lmdb_library):
        lib = lmdb_library
        lib.write("symbol_1", pd.DataFrame({"a": [1]}, index=pd.DatetimeIndex([pd.Timestamp("2024-01-01")])))

        lib.update_batch(
            [
                UpdatePayload(
                    symbol="symbol_2", data=pd.DataFrame({"b": [10, 11]}, index=pd.date_range("2024-01-04", periods=2))
                ),
                UpdatePayload(
                    symbol="symbol_1", data=pd.DataFrame({"a": [2, 3]}, index=pd.date_range("2024-01-02", periods=2))
                ),
            ],
            upsert=True,
        )

        assert set(lib.list_symbols()) == {"symbol_1", "symbol_2"}
        symbol_1_vit, symbol_2_vit = lib.read("symbol_1"), lib.read("symbol_2")
        assert symbol_1_vit.version == 1
        assert len(lib.list_versions("symbol_1")) == 2
        assert_frame_equal(
            symbol_1_vit.data, pd.DataFrame({"a": [1, 2, 3]}, index=pd.date_range("2024-01-01", periods=3))
        )
        assert symbol_2_vit.version == 0
        assert len(lib.list_versions("symbol_2")) == 1
        assert_frame_equal(
            symbol_2_vit.data, pd.DataFrame({"b": [10, 11]}, index=pd.date_range("2024-01-04", periods=2))
        )

    def test_prune_previous(self, lmdb_library):
        lib = lmdb_library
        lib.write("symbol_1", pd.DataFrame({"a": [1]}, index=pd.DatetimeIndex([pd.Timestamp("2024-01-01")])))
        lib.write("symbol_2", pd.DataFrame({"b": [10]}, index=pd.DatetimeIndex([pd.Timestamp("2024-01-01")])))
        lib.update_batch(
            [
                UpdatePayload(
                    symbol="symbol_1", data=pd.DataFrame({"a": [1, 2, 3]}, index=pd.date_range("2024-01-01", periods=3))
                ),
                UpdatePayload(
                    symbol="symbol_2", data=pd.DataFrame({"b": [8, 9]}, index=pd.date_range("2023-01-01", periods=2))
                ),
            ],
            prune_previous_versions=True,
        )
        symbol_1_vit, symbol_2_vit = lib.read("symbol_1"), lib.read("symbol_2")
        assert_frame_equal(
            symbol_1_vit.data, pd.DataFrame({"a": [1, 2, 3]}, index=pd.date_range("2024-01-01", periods=3))
        )
        assert len(lib.list_versions("symbol_1")) == 1

        symbol_2_expected_data = pd.DataFrame(
            {"b": [8, 9, 10]},
            index=pd.DatetimeIndex(
                [pd.Timestamp("2023-01-01"), pd.Timestamp("2023-01-02"), pd.Timestamp("2024-01-01")]
            ),
        )
        assert_frame_equal(symbol_2_vit.data, symbol_2_expected_data)
        assert len(lib.list_versions("symbol_2")) == 1

    def test_repeating_symbol_in_payload_list_throws(self, lmdb_library):
        lib = lmdb_library
        lib.write("symbol_1", pd.DataFrame({"a": [1]}, index=pd.DatetimeIndex([pd.Timestamp("2024-01-01")])))
        with pytest.raises(arcticdb.version_store.library.ArcticDuplicateSymbolsInBatchException):
            lib.update_batch(
                [
                    UpdatePayload(
                        symbol="symbol_1",
                        data=pd.DataFrame({"a": [1, 2, 3]}, index=pd.date_range("2024-01-01", periods=3)),
                    ),
                    UpdatePayload(
                        symbol="symbol_1",
                        data=pd.DataFrame({"a": [8, 9]}, index=pd.date_range("2023-01-01", periods=2)),
                    ),
                ]
            )

    def test_non_normalizable_data_throws(self, lmdb_library):
        lib = lmdb_library
        lib.write("symbol_1", pd.DataFrame({"a": [1]}, index=pd.DatetimeIndex([pd.Timestamp("2024-01-01")])))
        lib.write("symbol_2", pd.DataFrame({"a": [1]}, index=pd.DatetimeIndex([pd.Timestamp("2024-01-01")])))
        with pytest.raises(arcticdb.version_store.library.ArcticUnsupportedDataTypeException) as ex_info:
            lib.update_batch(
                [
                    UpdatePayload(symbol="symbol_1", data={1, 2, 3}),
                    UpdatePayload(
                        symbol="symbol_2",
                        data=pd.DataFrame({"a": [8, 9]}, index=pd.date_range("2023-01-01", periods=2)),
                    ),
                ]
            )
        assert "symbol_1" in str(ex_info.value)
        assert "symbol_2" not in str(ex_info.value)

    @pytest.mark.parametrize("upsert", [True, False])
    def test_empty_dataframe_does_not_increase_version(self, lmdb_library, upsert):
        lib = lmdb_library
        lib_tool = lib._dev_tools.library_tool()
        df1 = pd.DataFrame({"a": range(5)}, index=pd.date_range("2024-01-01", periods=5))
        df2 = pd.DataFrame({"b": range(5)}, index=pd.date_range("2023-01-01", periods=5))
        lib.write_batch([UpdatePayload("symbol_1", df1), UpdatePayload("symbol_2", df2)])
        for symbol in ["symbol_1", "symbol_2"]:
            assert len(lib_tool.find_keys_for_symbol(KeyType.VERSION, symbol)) == 1
            assert len(lib_tool.find_keys_for_symbol(KeyType.TABLE_INDEX, symbol)) == 1
            assert len(lib_tool.find_keys_for_symbol(KeyType.TABLE_DATA, symbol)) == 1
        # One symbol list entry for symbol_1 and one for symbol_2
        assert len(lib_tool.find_keys(KeyType.SYMBOL_LIST)) == 2
        update_1 = pd.DataFrame({"a": []}, index=pd.date_range("2024-01-01", periods=0))
        update_2 = pd.DataFrame({"b": [10, 20]}, index=pd.date_range("2023-01-02", periods=2))
        res = lib.update_batch(
            [UpdatePayload("symbol_1", update_1), UpdatePayload("symbol_2", update_2)], upsert=upsert
        )

        assert res[0].version == 0
        assert res[1].version == 1

        sym_1_vit, sym_2_vit = lib.read("symbol_1"), lib.read("symbol_2")

        assert sym_1_vit.version == 0
        assert_frame_equal(sym_1_vit.data, df1)
        assert len(lib_tool.find_keys_for_symbol(KeyType.VERSION, "symbol_1")) == 1
        assert len(lib_tool.find_keys_for_symbol(KeyType.TABLE_INDEX, "symbol_1")) == 1
        assert len(lib_tool.find_keys_for_symbol(KeyType.TABLE_DATA, "symbol_1")) == 1

        assert sym_2_vit.version == 1
        assert_frame_equal(
            sym_2_vit.data, pd.DataFrame({"b": [0, 10, 20, 3, 4]}, index=pd.date_range("2023-01-01", periods=5))
        )
        assert len(lib_tool.find_keys_for_symbol(KeyType.VERSION, "symbol_2")) == 2
        assert len(lib_tool.find_keys_for_symbol(KeyType.TABLE_INDEX, "symbol_2")) == 2
        # Update happens in the middle of the dataframe. Data prior the update range (value 0) is in one segment, then
        # there's one segment for the new data (values 10, 20) then there's one segment for the data that's pas the
        # update range (values 3, 4). The fourth segment is the original data segment
        assert len(lib_tool.find_keys_for_symbol(KeyType.TABLE_DATA, "symbol_2")) == 4
        assert len(lib_tool.read_index("symbol_2")) == 3

        # This result is wrong. The correct value is 2. This is due to a bug Monday: 9682041273, append_batch and
        # update_batch should not create symbol list keys for already existing symbols. Since update_batch is noop when
        # the input is empty, there is no key for symbol_1, but there is a new key for symbol_2 and that is wrong.
        assert len(lib_tool.find_keys(KeyType.SYMBOL_LIST)) == 3

    def test_empty_dataframe_with_daterange_does_not_delete_data(self, lmdb_library):
        sym = "symbol_1"
        input_df = pd.DataFrame({"a": [1, 2]}, index=pd.date_range(start=pd.Timestamp("2024-01-02"), periods=2))
        lmdb_library.write(sym, input_df)
        payload = UpdatePayload(
            sym,
            pd.DataFrame({"a": []}, index=pd.DatetimeIndex([])),
            date_range=(pd.Timestamp("2024-01-01"), pd.Timestamp("2024-01-04")),
        )
        lmdb_library.update_batch([payload])
        vit = lmdb_library.read(sym)
        assert vit.version == 0
        assert_frame_equal(vit.data, input_df)


def test_regular_update_dynamic_schema_named_index(
    lmdb_version_store_tiny_segment_dynamic,
):
    lib = lmdb_version_store_tiny_segment_dynamic
    sym = "test_parallel_update_dynamic_schema_named_index"
    df_0 = pd.DataFrame({"col_0": [0], "col_1": [0.5]}, index=pd.date_range("2024-01-01", periods=1))
    df_0.index.name = "date"
    df_1 = pd.DataFrame({"col_0": [1]}, index=pd.date_range("2024-01-02", periods=1))
    lib.write(sym, df_0)
    with pytest.raises(StreamDescriptorMismatch) as exception_info:
        lib.update(sym, df_1, upsert=True)

    assert "date" in str(exception_info.value)


@pytest.mark.parametrize(
    "to_write, to_update",
    [
        (
            pd.DataFrame({"a": [1]}, index=pd.DatetimeIndex([pd.Timestamp(0)])),
            pd.Series([2], index=pd.DatetimeIndex([pd.Timestamp(0)])),
        ),
        (pd.DataFrame({"a": [1]}, index=pd.DatetimeIndex([pd.Timestamp(0)])), np.array([2])),
        (
            pd.Series([1], index=pd.DatetimeIndex([pd.Timestamp(0)])),
            pd.DataFrame({"a": [2]}, index=pd.DatetimeIndex([pd.Timestamp(0)])),
        ),
        (pd.Series([1], index=pd.DatetimeIndex([pd.Timestamp(0)])), np.array([2])),
        (np.array([1]), pd.DataFrame({"a": [2]}, index=pd.DatetimeIndex([pd.Timestamp(0)]))),
        (np.array([1]), pd.Series([2], index=pd.DatetimeIndex([pd.Timestamp(0)]))),
    ],
)
def test_update_mismatched_object_kind(to_write, to_update, lmdb_version_store_dynamic_schema_v1):
    lib = lmdb_version_store_dynamic_schema_v1
    lib.write("sym", to_write)
    if isinstance(to_update, np.ndarray) or isinstance(to_write, np.ndarray):
        with pytest.raises(Exception) as e:
            assert "Index mismatch" in str(e.value)
    else:
        with pytest.raises(NormalizationException) as e:
            lib.update("sym", to_update)
        assert "Update" in str(e.value)


def test_update_series_with_different_column_name_throws(lmdb_version_store_dynamic_schema_v1):
    # It makes sense to create a new column and turn the whole thing into a dataframe. This would require changes in the
    # logic for storing normalization metadata which is tricky. Noone has requested this, so we just throw.
    lib = lmdb_version_store_dynamic_schema_v1
    lib.write(
        "sym",
        pd.Series(
            [1, 2, 3], name="name_1", index=pd.DatetimeIndex([pd.Timestamp(0), pd.Timestamp(1), pd.Timestamp(2)])
        ),
    )
    with pytest.raises(SchemaException) as e:
        lib.update("sym", pd.Series([1], name="name_2", index=pd.DatetimeIndex([pd.Timestamp(0)])))
    assert "name_1" in str(e.value) and "name_2" in str(e.value)


def test_update_index_has_the_same_start_end(version_store_factory):
    lib = version_store_factory(column_group_size=2, segment_row_size=2, dynamic_strings=True)
    row_count = 10
    columns = [f"col_{i}" for i in range(10)]
    df = pd.DataFrame(
        {col_name: range(row_count) for col_name in columns}, index=pd.date_range("2024-01-01", periods=row_count)
    )
    lib.write("sym", df)
    lib_tool = lib.library_tool()
    assert len(lib_tool.find_keys_for_symbol(KeyType.TABLE_DATA, "sym")) == 25

    update_df = pd.DataFrame(
        {col_name: range(10, row_count + 10) for col_name in columns},
        index=pd.date_range("2024-01-01", periods=row_count),
    )
    lib.update("sym", update_df)
    assert_frame_equal(lib.read("sym").data, update_df)

    lib_tool = lib.library_tool()
    assert len(lib_tool.find_keys_for_symbol(KeyType.TABLE_DATA, "sym")) == 50
    assert len(lib_tool.read_index("sym")) == 25


def test_update_new_data_contains_old(version_store_factory):
    lib = version_store_factory(column_group_size=2, segment_row_size=2, dynamic_strings=True)
    row_count = 10
    columns = [f"col_{i}" for i in range(10)]
    df = pd.DataFrame(
        {col_name: range(row_count) for col_name in columns}, index=pd.date_range("2024-01-01", periods=row_count)
    )
    lib.write("sym", df)
    lib_tool = lib.library_tool()
    assert len(lib_tool.find_keys_for_symbol(KeyType.TABLE_DATA, "sym")) == 25

    new_row_count = 12
    update_df = pd.DataFrame(
        {col_name: range(10, new_row_count + 10) for col_name in columns},
        index=pd.date_range("2023-12-31", periods=new_row_count),
    )
    lib.update("sym", update_df)
    assert_frame_equal(lib.read("sym").data, update_df)

    lib_tool = lib.library_tool()
    assert len(lib_tool.find_keys_for_symbol(KeyType.TABLE_DATA, "sym")) == 55
    assert len(lib_tool.read_index("sym")) == 30
