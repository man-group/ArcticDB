"""
Copyright 2023 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

import pandas as pd
import numpy as np
import pytest
from itertools import product
import datetime
import random

from arcticdb.util.test import random_strings_of_length, random_string, random_floats, assert_frame_equal
from arcticdb.exceptions import InternalException, SortingException
from tests.util.date import DateRange
from pandas import MultiIndex
from arcticdb.version_store import VersionedItem

from arcticdb.version_store.library import UpdatePayload


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


def test_update_unicode(lmdb_version_store):
    symbol = "test_append_unicode"
    uc = "\u0420\u043e\u0441\u0441\u0438\u044f"

    df1 = pd.DataFrame(
        index=[pd.Timestamp("2018-01-02"), pd.Timestamp("2018-01-03")],
        data={"a": ["123", uc]},
    )
    lmdb_version_store.update(symbol, df1, upsert=True)
    vit = lmdb_version_store.read(symbol)
    assert_frame_equal(vit.data, df1)

    df2 = pd.DataFrame(
        index=[pd.Timestamp("2018-01-04"), pd.Timestamp("2018-01-05")],
        data={"a": ["123", uc]},
    )
    lmdb_version_store.update(symbol, df2)
    vit = lmdb_version_store.read(symbol)
    expected = pd.concat([df1, df2])
    assert_frame_equal(vit.data, expected)

    uc_new = "\u0420\u043e\u0441\u0441\u0438\u044f_new"
    df1_new = pd.DataFrame(
        index=[pd.Timestamp("2018-01-02"), pd.Timestamp("2018-01-03")],
        data={"a": ["123", uc_new]},
    )
    lmdb_version_store.update(symbol, df1_new)
    vit = lmdb_version_store.read(symbol)
    expected = pd.concat([df1_new, df2])
    assert_frame_equal(vit.data, expected)


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
        {"A": [999, 999, 1]}, index=[pd.Timestamp(2023, 2, 1), pd.Timestamp(2023, 2, 6), pd.Timestamp(2023, 2, 7)]
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
    "col_per_group, start_increment, end_increment, update_start, iterations, start_dist", gen_params()
)
def test_update_repeatedly_dynamic_schema(
    version_store_factory, col_per_group, start_increment, end_increment, update_start, iterations, start_dist
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
    "col_per_group, start_increment, end_increment, update_start, iterations, start_dist", gen_params()
)
def test_update_repeatedly_dynamic_schema_hashed(
    version_store_factory, col_per_group, start_increment, end_increment, update_start, iterations, start_dist
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
    "col_per_group, start_increment, end_increment, update_start, iterations, start_dist", gen_params()
)
def test_update_repeatedly(
    version_store_factory, col_per_group, start_increment, end_increment, update_start, iterations, start_dist
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
    "col_per_group, start_increment, end_increment, update_start, iterations, start_dist", gen_params()
)
def test_update_repeatedly_with_strings(
    version_store_factory, col_per_group, start_increment, end_increment, update_start, iterations, start_dist
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
    def test_update_batch_success(self, lmdb_library):
        lib = lmdb_library

        initial_data = {
            "symbol_1": pd.DataFrame({"a": range(20)}, index=pd.date_range("2024-01-01", "2024-01-20")),
            "symbol_2": pd.DataFrame({"b": range(30, 60)}, index=pd.date_range("2024-02-01", periods=30)),
            "symbol_3": pd.DataFrame({"c": range(70, 80)}, index=pd.date_range("2024-03-01", periods=10))
        }
        for symbol, data in initial_data.items():
            lib.write(symbol, data)

        batch_update_queries = {
            "symbol_1": UpdatePayload("symbol_1", pd.DataFrame({"a": range(0, -5, -1)}, index=pd.date_range("2024-01-10", periods=5))),
            "symbol_2": UpdatePayload("symbol_2", pd.DataFrame({"b": range(-10, -20, -1)}, index=pd.date_range("2024-02-05", periods=10, freq='h'))),
        }

        result = lib.update_batch(batch_update_queries.values())
        assert(len(result) == len(batch_update_queries))
        for i in range(len(result)):
            versioned_item = result[i]
            assert (isinstance(versioned_item, VersionedItem))
            assert versioned_item.symbol == list(batch_update_queries.keys())[i]

        expected = {
            "symbol_1": pd.concat([
                pd.DataFrame({"a": range(0, 9)}, pd.date_range("2024-01-01", periods=9)),
                batch_update_queries["symbol_1"].data,
                pd.DataFrame({"a": range(14, 20)}, pd.date_range("2024-01-15", periods=6)),
            ]),
            "symbol_2": pd.concat([
                pd.DataFrame({"b": range(30, 34)}, pd.date_range("2024-02-01", "2024-02-04")),
                batch_update_queries["symbol_2"].data,
                pd.DataFrame({"b": range(35, 60)}, pd.date_range("2024-02-06", periods=25)),
            ]),
            "symbol_3": initial_data["symbol_3"]
        }

        updated = [lib.read(symbol) for symbol in expected]
        for vit in updated:
            if vit.symbol in batch_update_queries.values():
                assert vit.version == 1
            assert_frame_equal(vit.data, expected[vit.symbol])


