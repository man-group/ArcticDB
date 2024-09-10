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


@pytest.mark.parametrize("prune_previous_versions", [True, False])
def test_write_parallel_sort_merge(basic_arctic_library, prune_previous_versions):
    lib = basic_arctic_library

    num_rows_per_day = 10
    num_days = 10
    num_columns = 4
    column_length = 8
    dt = datetime.datetime(2019, 4, 8, 0, 0, 0)
    cols = random_strings_of_length(num_columns, column_length, True)
    symbol = "test_write_parallel_sort_merge"
    dataframes = []
    df = pd.DataFrame()

    for _ in range(num_days):
        index = pd.Index([dt + datetime.timedelta(seconds=s) for s in range(num_rows_per_day)])
        vals = {c: random_floats(num_rows_per_day) for c in cols}
        new_df = pd.DataFrame(data=vals, index=index)

        dataframes.append(new_df)
        df = pd.concat((df, new_df))
        dt = dt + datetime.timedelta(days=1)

    random.shuffle(dataframes)
    lib.write(symbol, dataframes[0])
    for d in dataframes:
        lib.write(symbol, d, staged=True)
    lib.sort_and_finalize_staged_data(symbol, prune_previous_versions=prune_previous_versions)
    vit = lib.read(symbol)
    df.sort_index(axis=1, inplace=True)
    result = vit.data
    result.sort_index(axis=1, inplace=True)
    assert_frame_equal(vit.data, df)
    if prune_previous_versions:
        assert 0 not in [version["version"] for version in lib._nvs.list_versions(symbol)]
    else:
        assert_frame_equal(lib.read(symbol, as_of=0).data, dataframes[0])


@pytest.mark.parametrize("prune_previous_versions", [True, False])
def test_sort_merge_append(basic_store_dynamic_schema, prune_previous_versions):
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

    old_dataframes = dataframes[: int(half_way)]
    dataframes = dataframes[int(half_way) :]
    random.shuffle(dataframes)
    for d in dataframes:
        lib.write(symbol, d, parallel=True)

    lib.version_store.sort_merge(symbol, None, True, prune_previous_versions=prune_previous_versions)
    vit = lib.read(symbol)
    df.sort_index(axis=1, inplace=True)
    result = vit.data
    result.sort_index(axis=1, inplace=True)
    assert_frame_equal(vit.data, df)

    versions = [version["version"] for version in lib.list_versions(symbol)]
    if prune_previous_versions:
        assert len(versions) == 1 and versions[0] == int(half_way)
    else:
        assert len(versions) == int(half_way) + 1
        for version in range(int(half_way)):
            result = lib.read(symbol, as_of=version).data
            result.sort_index(axis=1, inplace=True)
            df = pd.concat(old_dataframes[0 : version+1])
            df.sort_index(axis=1, inplace=True)
            assert_frame_equal(result, df)


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
@pytest.mark.parametrize("validate_index", (True, False, None))
def test_parallel_sortedness_checks_unsorted_data(lmdb_version_store, append, validate_index):
    lib = lmdb_version_store
    sym = "test_parallel_sortedness_checks_unsorted_data"
    if append:
        df_0 = pd.DataFrame({"col": [1, 2]}, index=[pd.Timestamp("2024-01-01"), pd.Timestamp("2024-01-02")])
        lib.write(sym, df_0)
    df_1 = pd.DataFrame({"col": [3, 4]}, index=[pd.Timestamp("2024-01-04"), pd.Timestamp("2024-01-03")])
    if validate_index:
        with pytest.raises(SortingException):
            if append:
                lib.append(sym, df_1, incomplete=True, validate_index=True)
            else:
                lib.write(sym, df_1, parallel=True, validate_index=True)
    else:
        if validate_index is None:
            # Test default behaviour when arg isn't provided
            if append:
                lib.append(sym, df_1, incomplete=True)
            else:
                lib.write(sym, df_1, parallel=True)
        else:
            if append:
                lib.append(sym, df_1, incomplete=True, validate_index=False)
            else:
                lib.write(sym, df_1, parallel=True, validate_index=False)


@pytest.mark.parametrize("append", (True, False))
@pytest.mark.parametrize("validate_index", (True, False, None))
def test_parallel_sortedness_checks_sorted_data(lmdb_version_store, append, validate_index):
    lib = lmdb_version_store
    sym = "test_parallel_sortedness_checks_unsorted_data"
    if append:
        df_0 = pd.DataFrame({"col": [1, 2]}, index=[pd.Timestamp("2024-01-01"), pd.Timestamp("2024-01-02")])
        lib.write(sym, df_0)
    df_1 = pd.DataFrame({"col": [3, 4]}, index=[pd.Timestamp("2024-01-03"), pd.Timestamp("2024-01-04")])
    df_2 = pd.DataFrame({"col": [5, 6]}, index=[pd.Timestamp("2024-01-05"), pd.Timestamp("2024-01-06")])
    if append:
        lib.append(sym, df_1, incomplete=True, validate_index=validate_index)
        lib.append(sym, df_2, incomplete=True, validate_index=validate_index)
    else:
        lib.write(sym, df_1, parallel=True, validate_index=validate_index)
        lib.write(sym, df_2, parallel=True, validate_index=validate_index)
    lib.compact_incomplete(sym, append, False, validate_index=validate_index)
    expected = pd.concat([df_0, df_1, df_2]) if append else pd.concat([df_1, df_2])
    received = lib.read(sym).data
    assert_frame_equal(expected, received)


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
def test_parallel_all_same_index_values(lmdb_version_store, append):
    lib = lmdb_version_store
    sym = "test_parallel_all_same_index_values"
    if append:
        df_0 = pd.DataFrame({"col": [1, 2]}, index=[pd.Timestamp("2024-01-01"), pd.Timestamp("2024-01-01")])
        lib.write(sym, df_0)
    df_1 = pd.DataFrame({"col": [3, 4]}, index=[pd.Timestamp("2024-01-02"), pd.Timestamp("2024-01-02")])
    df_2 = pd.DataFrame({"col": [5, 6]}, index=[pd.Timestamp("2024-01-02"), pd.Timestamp("2024-01-02")])
    if append:
        lib.append(sym, df_2, incomplete=True)
        lib.append(sym, df_1, incomplete=True)
    else:
        lib.write(sym, df_2, parallel=True)
        lib.write(sym, df_1, parallel=True)
    lib.compact_incomplete(sym, append, False)
    received = lib.read(sym).data
    # Index values in incompletes all the same, so order of values in col could be [3, 4, 5, 6] or [5, 6, 3, 4]
    if append:
        expected = pd.concat([df_0, df_1, df_2]) if received["col"][2] == 3 else pd.concat([df_0, df_2, df_1])
        assert_frame_equal(expected, received)
    else:
        expected = pd.concat([df_1, df_2]) if received["col"][0] == 3 else pd.concat([df_2, df_1])
        assert_frame_equal(expected, received)
    assert lib.get_info(sym)["sorted"] == "ASCENDING"


@pytest.mark.parametrize("append", (True, False))
@pytest.mark.parametrize("validate_index", (True, False, None))
def test_parallel_overlapping_incomplete_segments(lmdb_version_store, append, validate_index):
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
    if validate_index:
        with pytest.raises(SortingException):
            lib.compact_incomplete(sym, append, False, validate_index=True)
    else:
        if validate_index is None:
            # Test default behaviour when arg isn't provided
            lib.compact_incomplete(sym, append, False)
        else:
            lib.compact_incomplete(sym, append, False, validate_index=False)
        received = lib.read(sym).data
        expected = pd.concat([df_0, df_1, df_2]) if append else pd.concat([df_1, df_2])
        assert_frame_equal(received, expected)


def test_parallel_append_exactly_matches_existing(lmdb_version_store):
    lib = lmdb_version_store
    sym = "test_parallel_append_exactly_matches_existing"
    df_0 = pd.DataFrame({"col": [1, 2]}, index=[pd.Timestamp("2024-01-01"), pd.Timestamp("2024-01-02")])
    lib.write(sym, df_0)
    df_1 = pd.DataFrame({"col": [3, 4]}, index=[pd.Timestamp("2024-01-02"), pd.Timestamp("2024-01-03")])
    lib.append(sym, df_1, incomplete=True)
    lib.compact_incomplete(sym, True, False)
    expected = pd.concat([df_0, df_1])
    received = lib.read(sym).data
    assert_frame_equal(expected, received)
    assert lib.get_info(sym)["sorted"] == "ASCENDING"


@pytest.mark.parametrize("append", (True, False))
@pytest.mark.parametrize("validate_index", (True, False, None))
def test_parallel_all_incomplete_segments_same_index(lmdb_version_store_v1, append, validate_index):
    lib = lmdb_version_store_v1
    sym = "test_parallel_all_incomplete_segments_same_index"
    if append:
        df_0 = pd.DataFrame({"col": [1, 2]}, index=[pd.Timestamp("2024-01-01"), pd.Timestamp("2024-01-02")])
        lib.write(sym, df_0)
    df_1 = pd.DataFrame({"col": [3, 4]}, index=[pd.Timestamp("2024-01-03"), pd.Timestamp("2024-01-04")])
    df_2 = pd.DataFrame({"col": [5, 6]}, index=[pd.Timestamp("2024-01-03"), pd.Timestamp("2024-01-04")])
    if append:
        lib.append(sym, df_2, incomplete=True)
        lib.append(sym, df_1, incomplete=True)
    else:
        lib.write(sym, df_2, parallel=True)
        lib.write(sym, df_1, parallel=True)
    if validate_index:
        with pytest.raises(SortingException):
            lib.compact_incomplete(sym, append, False, validate_index=True)
    else:
        if validate_index is None:
            # Test default behaviour when arg isn't provided
            lib.compact_incomplete(sym, append, False)
        else:
            lib.compact_incomplete(sym, append, False, validate_index=False)
        received = lib.read(sym).data
        # Order is arbitrary if all index values are the same
        if received["col"].iloc[-1] == 6:
            expected = pd.concat([df_0, df_1, df_2]) if append else pd.concat([df_1, df_2])
        else:
            expected = pd.concat([df_0, df_2, df_1]) if append else pd.concat([df_2, df_1])
        assert_frame_equal(received, expected)


@pytest.mark.parametrize("validate_index", (True, False, None))
def test_parallel_append_overlapping_with_existing(lmdb_version_store, validate_index):
    lib = lmdb_version_store
    sym = "test_parallel_append_overlapping_with_existing"
    df_0 = pd.DataFrame({"col": [1, 2]}, index=[pd.Timestamp("2024-01-01"), pd.Timestamp("2024-01-02")])
    lib.write(sym, df_0)
    df_1 = pd.DataFrame({"col": [3, 4]}, index=[pd.Timestamp("2024-01-01T12"), pd.Timestamp("2024-01-03")])
    lib.append(sym, df_1, incomplete=True)
    if validate_index:
        with pytest.raises(SortingException):
            lib.compact_incomplete(sym, True, False, validate_index=validate_index)
    else:
        if validate_index is None:
            # Test default behaviour when arg isn't provided
            lib.compact_incomplete(sym, True, False)
        else:
            lib.compact_incomplete(sym, True, False, validate_index=False)
        received = lib.read(sym).data
        assert_frame_equal(received, pd.concat([df_0, df_1]))


@pytest.mark.parametrize("sortedness", ("DESCENDING", "UNSORTED"))
@pytest.mark.parametrize("validate_index", (True, False, None))
def test_parallel_append_existing_data_unsorted(lmdb_version_store, sortedness, validate_index):
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
    if validate_index:
        with pytest.raises(SortingException):
            lib.compact_incomplete(sym, True, False, validate_index=True)
    else:
        if validate_index is None:
            # Test the default case with no arg provided
            lib.compact_incomplete(sym, True, False)
        else:
            lib.compact_incomplete(sym, True, False, validate_index=False)
        expected = pd.concat([df_0, df_1])
        received = lib.read(sym).data
        assert_frame_equal(expected, received)


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


@pytest.mark.xfail(reason="See https://github.com/man-group/ArcticDB/issues/1466")
def test_parallel_append_static_schema_type_changing(lmdb_version_store_tiny_segment):
    lib = lmdb_version_store_tiny_segment
    sym = "test_parallel_append_static_schema_type_changing"
    df_0 = pd.DataFrame({"col": np.arange(1, dtype=np.uint8)}, index=pd.date_range("2024-01-01", periods=1))
    df_1 = pd.DataFrame({"col": np.arange(1, 2, dtype=np.uint16)}, index=pd.date_range("2024-01-02", periods=1))
    lib.write(sym, df_0)
    lib.append(sym, df_1, incomplete=True)
    with pytest.raises(SchemaException):
        lib.compact_incomplete(sym, True, False)


@pytest.mark.xfail(reason="See https://github.com/man-group/ArcticDB/issues/1466")
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


class TestFinalizeStagedDataStaticSchemaMismatch:
    def test_append_throws_with_missmatched_column_set(self, lmdb_version_store_v1):
        lib = lmdb_version_store_v1

        initial_df = pd.DataFrame({"col_0": [1]}, index=pd.DatetimeIndex([pd.Timestamp(2024, 1, 1)]))
        lib.write("sym", initial_df)

        appended_df = pd.DataFrame({"col_1": [1]}, index=pd.DatetimeIndex([pd.Timestamp(2024, 1, 2)]))
        lib.write("sym", appended_df, parallel=True)
        with pytest.raises(SchemaException) as exception_info:
            lib.compact_incomplete("sym", True, False)
        assert "col_1" in str(exception_info.value)

    def test_append_throws_column_subset(self, lmdb_version_store_v1):
        lib = lmdb_version_store_v1

        df1 = pd.DataFrame(
            {"a": np.array([1.1], dtype="float"), "b": np.array([2], dtype="int64")},
            index=pd.DatetimeIndex([pd.Timestamp("2024-01-01")])
        )
        lib.write("sym", df1)
        df2 = pd.DataFrame({"b": [1]}, index=pd.DatetimeIndex([pd.Timestamp("2024-01-02")]))
        lib.write("sym", df2, parallel=True)
        with pytest.raises(SchemaException) as exception_info: 
            lib.compact_incomplete("sym", True, False)
        assert "a" in str(exception_info.value)
        assert "b" in str(exception_info.value)

    def test_append_throws_on_incompatible_dtype(self, lmdb_version_store_v1):
        lib = lmdb_version_store_v1

        initial_df = pd.DataFrame({"col_0": np.array([1], dtype="int64")}, index=pd.DatetimeIndex([pd.Timestamp(2024, 1, 1)]))
        lib.write("sym", initial_df)

        appended_df = pd.DataFrame({"col_0": ["asd"]}, index=pd.DatetimeIndex([pd.Timestamp(2024, 1, 2)]))
        lib.write("sym", appended_df, parallel=True)
        with pytest.raises(SchemaException) as exception_info:
            lib.compact_incomplete("sym", True, False)
        assert "col_0" in str(exception_info.value)
        assert "INT64" in str(exception_info.value)

    def test_type_mismatch_in_staged_segments_throws(self, lmdb_version_store_v1):
        lib = lmdb_version_store_v1
        lib.write("sym", pd.DataFrame({"col": [1]}, index=pd.DatetimeIndex([np.datetime64('2023-01-01')])), parallel=True)
        lib.write("sym", pd.DataFrame({"col": ["a"]}, index=pd.DatetimeIndex([np.datetime64('2023-01-02')])), parallel=True)
        with pytest.raises(Exception) as exception_info:
            lib.compact_incomplete("sym", False, False)
        assert all(x in str(exception_info.value) for x in ["INT64", "type"])

    def test_types_cant_be_promoted(self, lmdb_version_store_v1):
        lib = lmdb_version_store_v1
        lib.write("sym", pd.DataFrame({"col": np.array([1], dtype="int64")}, index=pd.DatetimeIndex([pd.Timestamp(2024, 1, 1)])))

        lib.write("sym", pd.DataFrame({"col": np.array([1], dtype="int32")}, index=pd.DatetimeIndex([pd.Timestamp(2024, 1, 2)])), parallel=True)
        with pytest.raises(SchemaException) as exception_info:
            lib.compact_incomplete("sym", True, False)
        assert "INT32" in str(exception_info.value)
        assert "INT64" in str(exception_info.value)

    def test_appending_reordered_column_set_throws(self, lmdb_version_store_v1):
        lib = lmdb_version_store_v1

        lib.write("sym", pd.DataFrame({"col_0": [1], "col_1": ["test"], "col_2": [1.2]}, index=pd.DatetimeIndex([pd.Timestamp(2024, 1, 1)])))

        lib.write("sym", pd.DataFrame({"col_1": ["asd"], "col_2": [2.5], "col_0": [2]}, index=pd.DatetimeIndex([pd.Timestamp(2024, 1, 2)])), parallel=True)
        with pytest.raises(SchemaException) as exception_info:
            lib.compact_incomplete("sym", True, False)
        assert "col_0" in str(exception_info.value)
        assert "col_1" in str(exception_info.value)
        assert "col_2" in str(exception_info.value)

    @pytest.mark.parametrize("mode", [True, False])
    def test_staged_segments_can_be_reordered(self, lmdb_version_store_v1, mode):
        lib = lmdb_version_store_v1
        df1 = pd.DataFrame({"col_0": [1], "col_1": ["test"], "col_2": [1.2]}, index=pd.DatetimeIndex([pd.Timestamp(2024, 1, 1)]))
        df2 = pd.DataFrame({"col_1": ["asd"], "col_2": [2.5], "col_0": [2]}, index=pd.DatetimeIndex([pd.Timestamp(2024, 1, 2)]))
        lib.write("sym", df1, parallel=True)
        lib.write("sym", df2, parallel=True)
        expected = pd.concat([df1, df2]).sort_index()
        with pytest.raises(SchemaException) as exception_info:
            lib.compact_incomplete("sym", mode, False)
        assert "col_0" in str(exception_info.value)
        assert "col_1" in str(exception_info.value)
        assert "col_2" in str(exception_info.value)

# finalize_method True -> append, finalize_method False -> write
@pytest.mark.parametrize("finalize_method", (True, False))
class TestFinalizeWithEmptySegments:
    def test_staged_segment_is_only_empty_dfs(self, lmdb_version_store_v1, finalize_method):
        lib = lmdb_version_store_v1
        lib.write("sym", pd.DataFrame([]), parallel=True)
        lib.write("sym", pd.DataFrame([]), parallel=True)
        lib.compact_incomplete("sym", finalize_method, False)
        assert_frame_equal(lib.read("sym").data, pd.DataFrame([], index=pd.DatetimeIndex([])))

    def test_staged_segment_has_empty_df(self, lmdb_version_store_v1, finalize_method):
        lib = lmdb_version_store_v1
        index = pd.DatetimeIndex([pd.Timestamp(2024, 1, 1), pd.Timestamp(2024, 1, 3), pd.Timestamp(2024, 1, 4)])
        df1 = pd.DataFrame({"col": [1, 2, 3]}, index=index)
        df2 = pd.DataFrame({})
        df3 = pd.DataFrame({"col": [4]}, index=pd.DatetimeIndex([pd.Timestamp(2024, 1, 5)]))
        lib.write("sym", df1, parallel=True)
        lib.write("sym", df2, parallel=True)
        lib.write("sym", df3, parallel=True)
        with pytest.raises(SchemaException):
            lib.compact_incomplete("sym", finalize_method, False)

    def test_df_without_rows(self, lmdb_version_store_v1, finalize_method):
        lib = lmdb_version_store_v1
        df = pd.DataFrame({"col": []}, index=pd.DatetimeIndex([]))
        lib.write("sym", df, parallel=True)
        lib.compact_incomplete("sym", finalize_method, False)
        assert_frame_equal(lib.read("sym").data, df)