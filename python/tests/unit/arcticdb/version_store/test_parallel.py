"""
Copyright 2023 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""
import numpy as np
import pandas as pd
import random
import datetime
import pytest

from arcticdb.exceptions import SortingException, SchemaException
from arcticdb.util.test import (
    assert_frame_equal,
    random_strings_of_length,
    random_integers,
    random_floats,
    random_dates,
)
from arcticdb.util._versions import IS_PANDAS_TWO
from arcticdb_ext.storage import KeyType


def test_remove_incomplete(basic_store):
    lib = basic_store
    lib_tool = lib.library_tool()
    assert lib_tool.find_keys(KeyType.APPEND_DATA) == []
    assert lib.list_symbols_with_incomplete_data() == []

    sym1 = "test_remove_incomplete_1"
    sym2 = "test_remove_incomplete_2"
    num_chunks = 10
    df1 = pd.DataFrame({"col": np.arange(10)}, index=pd.date_range("2000-01-01", periods=num_chunks))
    df2 = pd.DataFrame({"col": np.arange(100, 110)}, index=pd.date_range("2001-01-01", periods=num_chunks))
    for idx in range(num_chunks):
        lib.write(sym1, df1.iloc[idx : idx + 1, :], parallel=True)
        lib.write(sym2, df2.iloc[idx : idx + 1, :], parallel=True)

    assert len(lib_tool.find_keys_for_symbol(KeyType.APPEND_DATA, sym1)) == num_chunks
    assert len(lib_tool.find_keys_for_symbol(KeyType.APPEND_DATA, sym2)) == num_chunks
    assert sorted(lib.list_symbols_with_incomplete_data()) == [sym1, sym2]

    lib.remove_incomplete(sym1)
    assert lib_tool.find_keys_for_symbol(KeyType.APPEND_DATA, sym1) == []
    assert len(lib_tool.find_keys_for_symbol(KeyType.APPEND_DATA, sym2)) == num_chunks
    assert lib.list_symbols_with_incomplete_data() == [sym2]

    lib.remove_incomplete(sym2)
    assert lib_tool.find_keys(KeyType.APPEND_DATA) == []
    assert lib.list_symbols_with_incomplete_data() == []

    # Removing incompletes from a symbol that doesn't exist, or a symbol with no incompletes, is a no-op
    lib.remove_incomplete("non-existent-symbol")
    sym3 = "test_remove_incomplete_3"
    lib.write(sym3, df1)
    lib.remove_incomplete(sym3)


def test_parallel_write(basic_store):
    sym = "parallel"
    basic_store.remove_incomplete(sym)

    num_rows = 1111
    dtidx = pd.date_range("1970-01-01", periods=num_rows)
    test = pd.DataFrame(
        {"uint8": random_integers(num_rows, np.uint8), "uint32": random_integers(num_rows, np.uint32)}, index=dtidx
    )
    chunk_size = 100
    list_df = [test[i : i + chunk_size] for i in range(0, test.shape[0], chunk_size)]
    random.shuffle(list_df)

    for df in list_df:
        basic_store.write(sym, df, parallel=True)

    user_meta = {"thing": 7}
    basic_store.compact_incomplete(sym, False, False, metadata=user_meta)
    vit = basic_store.read(sym)
    assert_frame_equal(test, vit.data)
    assert vit.metadata["thing"] == 7


def test_roundtrip_nan(lmdb_version_store):
    df = pd.DataFrame(np.nan, index=[0, 1, 2, 3], columns=["A", "B"])
    lmdb_version_store.write("all_nans", df)
    vit = lmdb_version_store.read("all_nans")


def test_roundtrip_nat(lmdb_version_store):
    nats = np.repeat(pd.NaT, 4)
    d = {"col1": nats}
    df = pd.DataFrame(index=[0, 1, 2, 3], data=d)
    lmdb_version_store.write("all_nats", df)
    vit = lmdb_version_store.read("all_nats")


def test_floats_to_nans(lmdb_version_store_dynamic_schema):
    num_rows_per_day = 10
    num_days = 10
    num_columns = 8
    column_length = 8
    dt = datetime.datetime(2019, 4, 8, 0, 0, 0)
    columns = random_strings_of_length(num_columns, column_length, True)
    symbol = "test_write_parallel_stress_schema_change"
    dataframes = []
    df = pd.DataFrame()

    for _ in range(num_days):
        cols = random.sample(columns, 4)
        index = pd.Index([dt + datetime.timedelta(seconds=s) for s in range(num_rows_per_day)])
        vals = {c: random_floats(num_rows_per_day) for c in cols}
        new_df = pd.DataFrame(data=vals, index=index)

        dataframes.append(new_df)
        df = pd.concat((df, new_df))
        dt = dt + datetime.timedelta(days=1)

    random.shuffle(dataframes)
    for d in dataframes:
        lmdb_version_store_dynamic_schema.write(symbol, d, parallel=True)

    lmdb_version_store_dynamic_schema.version_store.compact_incomplete(symbol, False, False)
    vit = lmdb_version_store_dynamic_schema.read(symbol)
    df.sort_index(axis=1, inplace=True)
    result = vit.data
    result.sort_index(axis=1, inplace=True)
    assert_frame_equal(vit.data, df)


def test_sort_merge_write(basic_store):
    num_rows_per_day = 10
    num_days = 10
    num_columns = 8
    column_length = 8
    dt = datetime.datetime(2019, 4, 8, 0, 0, 0)
    columns = random_strings_of_length(num_columns, column_length, True)
    symbol = "test_write_parallel_sort_merge"
    dataframes = []
    df = pd.DataFrame()

    for _ in range(num_days):
        cols = random.sample(columns, 4)
        index = pd.Index([dt + datetime.timedelta(seconds=s) for s in range(num_rows_per_day)])
        vals = {c: random_floats(num_rows_per_day) for c in cols}
        new_df = pd.DataFrame(data=vals, index=index)

        dataframes.append(new_df)
        df = pd.concat((df, new_df))
        dt = dt + datetime.timedelta(days=1)

    random.shuffle(dataframes)
    for d in dataframes:
        basic_store.write(symbol, d, parallel=True)
    basic_store.version_store.sort_merge(symbol)
    vit = basic_store.read(symbol)
    df.sort_index(axis=1, inplace=True)
    result = vit.data
    result.sort_index(axis=1, inplace=True)
    assert_frame_equal(vit.data, df)


def test_sort_merge_append(basic_store_dynamic_schema):
    lib = basic_store_dynamic_schema
    num_rows_per_day = 10
    num_days = 10
    num_columns = 8
    column_length = 8
    dt = datetime.datetime(2019, 4, 8, 0, 0, 0)
    columns = random_strings_of_length(num_columns, column_length, True)
    symbol = "test_append_parallel_sort_merge"
    dataframes = []
    df = pd.DataFrame()
    for _ in range(num_days):
        cols = random.sample(columns, 4)
        index = pd.Index([dt + datetime.timedelta(seconds=s) for s in range(num_rows_per_day)])
        vals = {c: random_floats(num_rows_per_day) for c in cols}
        new_df = pd.DataFrame(data=vals, index=index)
        dataframes.append(new_df)
        df = pd.concat((df, new_df))
        dt = dt + datetime.timedelta(days=1)

    half_way = len(dataframes) / 2
    idx_dataframe = 0
    for d in dataframes:
        if idx_dataframe == 0:
            lib.write(symbol, d)
        else:
            lib.append(symbol, d)
        idx_dataframe += 1
        if idx_dataframe == half_way:
            break

    dataframes = dataframes[int(half_way) :]
    random.shuffle(dataframes)
    for d in dataframes:
        lib.write(symbol, d, parallel=True)

    lib.version_store.sort_merge(symbol, None, True)
    vit = lib.read(symbol)
    df.sort_index(axis=1, inplace=True)
    result = vit.data
    result.sort_index(axis=1, inplace=True)
    assert_frame_equal(vit.data, df)


def test_datetimes_to_nats(lmdb_version_store_dynamic_schema):
    num_rows_per_day = 10
    num_days = 10
    num_columns = 8
    column_length = 8
    dt = datetime.datetime(2019, 4, 8, 0, 0, 0)
    columns = random_strings_of_length(num_columns, column_length, True)
    symbol = "test_write_parallel_stress_schema_change"
    dataframes = []
    df = pd.DataFrame()

    for _ in range(num_days):
        cols = random.sample(columns, 4)
        index = pd.Index([dt + datetime.timedelta(seconds=s) for s in range(num_rows_per_day)])
        vals = {c: random_dates(num_rows_per_day) for c in cols}
        new_df = pd.DataFrame(data=vals, index=index)
        dataframes.append(new_df)
        df = pd.concat((df, new_df))
        dt = dt + datetime.timedelta(days=1)

    random.shuffle(dataframes)
    for d in dataframes:
        lmdb_version_store_dynamic_schema.write(symbol, d, parallel=True)

    lmdb_version_store_dynamic_schema.version_store.compact_incomplete(symbol, False, True)
    vit = lmdb_version_store_dynamic_schema.read(symbol)
    df.sort_index(axis=1, inplace=True)
    result = vit.data
    result.sort_index(axis=1, inplace=True)

    if IS_PANDAS_TWO:
        # In Pandas < 2.0, `datetime64[ns]` was _always_ used. `datetime64[ns]` is also used by ArcticDB.
        # In Pandas >= 2.0, the `datetime64` can be used with other resolutions (namely 's', 'ms', and 'us').
        # See: https://pandas.pydata.org/docs/dev/whatsnew/v2.0.0.html#construction-with-datetime64-or-timedelta64-dtype-with-unsupported-resolution  # noqa
        # Hence, we convert to the largest resolution (which is guaranteed to be the ones of the original `df`).
        result = result.astype(df.dtypes)

    assert_frame_equal(result, df)


@pytest.mark.parametrize("append", (True, False))
@pytest.mark.parametrize("arg", (True, False, None))
@pytest.mark.parametrize("lib_config", (True, False))
def test_compact_incomplete_prune_previous(lib_config, arg, append, version_store_factory):
    lib = version_store_factory(prune_previous_version=lib_config)
    lib.write("sym", pd.DataFrame({"col": [3]}, index=pd.DatetimeIndex([0])))
    lib.append("sym", pd.DataFrame({"col": [4]}, index=pd.DatetimeIndex([1])), incomplete=True)

    lib.compact_incomplete("sym", append, convert_int_to_float=False, prune_previous_version=arg)
    assert lib.read_metadata("sym").version == 1

    should_prune = lib_config if arg is None else arg
    assert lib.has_symbol("sym", 0) != should_prune


def test_compact_incomplete_sets_sortedness(lmdb_version_store):
    lib = lmdb_version_store
    sym = "test_compact_incomplete_sets_sortedness"
    df_0 = pd.DataFrame({"col": [1, 2]}, index=[pd.Timestamp("2024-01-01"), pd.Timestamp("2024-01-02")])
    df_1 = pd.DataFrame({"col": [3, 4]}, index=[pd.Timestamp("2024-01-03"), pd.Timestamp("2024-01-04")])
    lib.write(sym, df_1, parallel=True)
    lib.write(sym, df_0, parallel=True)
    lib.compact_incomplete(sym, False, False)
    assert lib.get_info(sym)["sorted"] == "ASCENDING"

    df_2 = pd.DataFrame({"col": [5, 6]}, index=[pd.Timestamp("2024-01-05"), pd.Timestamp("2024-01-06")])
    df_3 = pd.DataFrame({"col": [7, 8]}, index=[pd.Timestamp("2024-01-07"), pd.Timestamp("2024-01-08")])
    lib.append(sym, df_3, incomplete=True)
    lib.append(sym, df_2, incomplete=True)
    lib.compact_incomplete(sym, True, False)
    assert lib.get_info(sym)["sorted"] == "ASCENDING"


@pytest.mark.parametrize("append", (True, False))
def test_parallel_sortedness_checks(lmdb_version_store, append):
    lib = lmdb_version_store
    sym = "test_parallel_sortedness_checks"
    if append:
        df_0 = pd.DataFrame({"col": [1, 2]}, index=[pd.Timestamp("2024-01-01"), pd.Timestamp("2024-01-02")])
        lib.write(sym, df_0)
    df_1 = pd.DataFrame({"col": [3, 4]}, index=[pd.Timestamp("2024-01-04"), pd.Timestamp("2024-01-03")])
    with pytest.raises(SortingException):
        if append:
            lib.append(sym, df_1, incomplete=True)
        else:
            lib.write(sym, df_1, parallel=True)


@pytest.mark.parametrize("append", (True, False))
def test_parallel_non_timestamp_index(lmdb_version_store, append):
    lib = lmdb_version_store
    sym = "test_parallel_non_timestamp_index"
    if append:
        df_0 = pd.DataFrame({"col": [1, 2]}, index=np.arange(0, 2))
        lib.write(sym, df_0)
        assert lib.get_info(sym)["sorted"] == "UNKNOWN"
    df_1 = pd.DataFrame({"col": [3, 4]}, index=np.arange(2, 4))
    df_2 = pd.DataFrame({"col": [5, 6]}, index=np.arange(4, 6))
    if append:
        lib.append(sym, df_2, incomplete=True)
        lib.append(sym, df_1, incomplete=True)
    else:
        lib.write(sym, df_2, parallel=True)
        lib.write(sym, df_1, parallel=True)
    lib.compact_incomplete(sym, append, False)
    assert lib.get_info(sym)["sorted"] == "UNKNOWN"

    read_df = lib.read(sym).data
    if append:
        expected_df = pd.concat([df_0, df_1, df_2]) if read_df["col"].iloc[-1] == 6 else pd.concat([df_0, df_2, df_1])
    else:
        expected_df = pd.concat([df_1, df_2]) if read_df["col"].iloc[-1] == 6 else pd.concat([df_2, df_1])
    assert_frame_equal(expected_df, read_df)


@pytest.mark.parametrize("append", (True, False))
def test_parallel_overlapping_incomplete_segments(lmdb_version_store, append):
    lib = lmdb_version_store
    sym = "test_parallel_overlapping_incomplete_segments"
    if append:
        df_0 = pd.DataFrame({"col": [1, 2]}, index=[pd.Timestamp("2024-01-01"), pd.Timestamp("2024-01-02")])
        lib.write(sym, df_0)
    df_1 = pd.DataFrame({"col": [3, 4]}, index=[pd.Timestamp("2024-01-03"), pd.Timestamp("2024-01-04")])
    df_2 = pd.DataFrame({"col": [5, 6]}, index=[pd.Timestamp("2024-01-03T12"), pd.Timestamp("2024-01-05")])
    if append:
        lib.append(sym, df_2, incomplete=True)
        lib.append(sym, df_1, incomplete=True)
    else:
        lib.write(sym, df_2, parallel=True)
        lib.write(sym, df_1, parallel=True)
    with pytest.raises(SortingException):
        lib.compact_incomplete(sym, append, False)


def test_parallel_append_overlapping_with_existing(lmdb_version_store):
    lib = lmdb_version_store
    sym = "test_parallel_append_overlapping_with_existing"
    df_0 = pd.DataFrame({"col": [1, 2]}, index=[pd.Timestamp("2024-01-01"), pd.Timestamp("2024-01-02")])
    lib.write(sym, df_0)
    df_1 = pd.DataFrame({"col": [3, 4]}, index=[pd.Timestamp("2024-01-01T12"), pd.Timestamp("2024-01-03")])
    lib.append(sym, df_1, incomplete=True)
    with pytest.raises(SortingException):
        lib.compact_incomplete(sym, True, False)


@pytest.mark.parametrize("sortedness", ("DESCENDING", "UNSORTED"))
def test_parallel_append_existing_data_unsorted(lmdb_version_store, sortedness):
    lib = lmdb_version_store
    sym = "test_parallel_append_existing_data_unsorted"
    last_index_date = "2024-01-01" if sortedness == "DESCENDING" else "2024-01-03"
    df_0 = pd.DataFrame(
        {"col": [1, 2, 3]},
        index=[pd.Timestamp("2024-01-04"), pd.Timestamp("2024-01-02"), pd.Timestamp(last_index_date)]
    )
    lib.write(sym, df_0)
    assert lib.get_info(sym)["sorted"] == sortedness
    df_1 = pd.DataFrame({"col": [3, 4]}, index=[pd.Timestamp("2024-01-05"), pd.Timestamp("2024-01-06")])
    lib.append(sym, df_1, incomplete=True)
    with pytest.raises(SortingException):
        lib.compact_incomplete(sym, True, False)


def test_parallel_no_column_slicing(lmdb_version_store_tiny_segment):
    lib = lmdb_version_store_tiny_segment
    sym = "test_parallel_column_slicing"
    lib_tool = lib.library_tool()
    # lmdb_version_store_tiny_segment has 2 columns per slice
    df = pd.DataFrame({"col_0": [0], "col_1": [1], "col_2": [2]})
    lib.write(sym, df, parallel=True)
    assert len(lib_tool.find_keys(KeyType.APPEND_DATA)) == 1
    lib.remove_incomplete(sym)
    lib.append(sym, df, incomplete=True)
    assert len(lib_tool.find_keys(KeyType.APPEND_DATA)) == 1


@pytest.mark.parametrize("rows_per_incomplete", (1, 2))
def test_parallel_write_static_schema_type_changing(lmdb_version_store_tiny_segment, rows_per_incomplete):
    lib = lmdb_version_store_tiny_segment
    sym = "test_parallel_write_static_schema_type_changing"
    lib_tool = lib.library_tool()
    df_0 = pd.DataFrame({"col": np.arange(rows_per_incomplete, dtype=np.uint8)}, index=pd.date_range("2024-01-01", periods=rows_per_incomplete))
    df_1 = pd.DataFrame({"col": np.arange(rows_per_incomplete, 2 * rows_per_incomplete, dtype=np.uint16)}, index=pd.date_range("2024-01-03", periods=rows_per_incomplete))
    lib.write(sym, df_0, parallel=True)
    lib.write(sym, df_1, parallel=True)
    with pytest.raises(SchemaException):
        lib.compact_incomplete(sym, False, False)


@pytest.mark.parametrize("rows_per_incomplete", (1, 2))
def test_parallel_write_dynamic_schema_type_changing(lmdb_version_store_tiny_segment_dynamic, rows_per_incomplete):
    lib = lmdb_version_store_tiny_segment_dynamic
    sym = "test_parallel_write_dynamic_schema_type_changing"
    df_0 = pd.DataFrame({"col": np.arange(rows_per_incomplete, dtype=np.uint8)}, index=pd.date_range("2024-01-01", periods=rows_per_incomplete))
    df_1 = pd.DataFrame({"col": np.arange(rows_per_incomplete, 2 * rows_per_incomplete, dtype=np.uint16)}, index=pd.date_range("2024-01-02", periods=rows_per_incomplete))
    lib.write(sym, df_0, parallel=True)
    lib.write(sym, df_1, parallel=True)
    lib.compact_incomplete(sym, False, False)
    expected = pd.concat([df_0, df_1])
    received = lib.read(sym).data
    assert_frame_equal(expected, received)


def test_parallel_write_static_schema_missing_column(lmdb_version_store_tiny_segment):
    lib = lmdb_version_store_tiny_segment
    sym = "test_parallel_write_static_schema_missing_column"
    df_0 = pd.DataFrame({"col_0": [0], "col_1": [0.5]}, index=pd.date_range("2024-01-01", periods=1))
    df_1 = pd.DataFrame({"col_0": [1]}, index=pd.date_range("2024-01-02", periods=1))
    lib.write(sym, df_0, parallel=True)
    lib.write(sym, df_1, parallel=True)
    with pytest.raises(SchemaException):
        lib.compact_incomplete(sym, False, False)


def test_parallel_write_dynamic_schema_missing_column(lmdb_version_store_tiny_segment_dynamic):
    lib = lmdb_version_store_tiny_segment_dynamic
    sym = "test_parallel_write_dynamic_schema_missing_column"
    df_0 = pd.DataFrame({"col_0": [0], "col_1": [0.5]}, index=pd.date_range("2024-01-01", periods=1))
    df_1 = pd.DataFrame({"col_0": [1]}, index=pd.date_range("2024-01-02", periods=1))
    lib.write(sym, df_0, parallel=True)
    lib.write(sym, df_1, parallel=True)
    lib.compact_incomplete(sym, False, False)
    expected = pd.concat([df_0, df_1])
    received = lib.read(sym).data
    assert_frame_equal(expected, received)


def test_parallel_append_static_schema_type_changing(lmdb_version_store_tiny_segment):
    lib = lmdb_version_store_tiny_segment
    sym = "test_parallel_append_static_schema_type_changing"
    df_0 = pd.DataFrame({"col": np.arange(1, dtype=np.uint8)}, index=pd.date_range("2024-01-01", periods=1))
    df_1 = pd.DataFrame({"col": np.arange(1, 2, dtype=np.uint16)}, index=pd.date_range("2024-01-02", periods=1))
    lib.write(sym, df_0)
    lib.append(sym, df_1, incomplete=True)
    with pytest.raises(SchemaException):
        lib.compact_incomplete(sym, True, False)


def test_parallel_append_static_schema_missing_column(lmdb_version_store_tiny_segment):
    lib = lmdb_version_store_tiny_segment
    sym = "test_parallel_append_static_schema_missing_column"
    df_0 = pd.DataFrame({"col_0": [0], "col_1": [0.5]}, index=pd.date_range("2024-01-01", periods=1))
    df_1 = pd.DataFrame({"col_0": [1]}, index=pd.date_range("2024-01-02", periods=1))
    lib.write(sym, df_0)
    lib.append(sym, df_1, incomplete=True)
    with pytest.raises(SchemaException):
        lib.compact_incomplete(sym, True, False)


def test_parallel_append_dynamic_schema_missing_column(lmdb_version_store_tiny_segment_dynamic):
    lib = lmdb_version_store_tiny_segment_dynamic
    sym = "test_parallel_append_dynamic_schema_missing_column"
    df_0 = pd.DataFrame({"col_0": [0], "col_1": [0.5]}, index=pd.date_range("2024-01-01", periods=1))
    df_1 = pd.DataFrame({"col_0": [1]}, index=pd.date_range("2024-01-02", periods=1))
    lib.write(sym, df_0)
    lib.append(sym, df_1, incomplete=True)
    lib.compact_incomplete(sym, True, False)
    expected = pd.concat([df_0, df_1])
    received = lib.read(sym).data
    assert_frame_equal(expected, received)


