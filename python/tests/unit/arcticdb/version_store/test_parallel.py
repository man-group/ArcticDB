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


from arcticdb.exceptions import (
    SortingException,
    SchemaException,
    UserInputException, ArcticDbNotYetImplemented,
)
from arcticdb.util.test import (
    assert_frame_equal,
    random_strings_of_length,
    random_integers,
    random_floats,
    random_dates,
)
from arcticdb.util._versions import IS_PANDAS_TWO
from arcticdb.version_store.library import Library
from arcticdb_ext.exceptions import UnsortedDataException
from arcticdb_ext.storage import KeyType

from arcticdb import util, LibraryOptions

from arcticdb.util.test import config_context_multi

import arcticdb_ext.cpp_async as adb_async


def get_append_keys(lib, sym):
    lib_tool = lib.library_tool()
    keys = lib_tool.find_keys_for_symbol(KeyType.APPEND_DATA, sym)
    return keys


def get_data_keys(lib, sym):
    lib_tool = lib.library_tool()
    keys = lib_tool.find_keys_for_symbol(KeyType.TABLE_DATA, sym)
    return keys


def get_index_keys(lib, sym):
    lib_tool = lib.library_tool()
    keys = lib_tool.find_keys_for_symbol(KeyType.TABLE_INDEX, sym)
    return keys


def test_staging_doesnt_write_append_ref(lmdb_version_store_v1):
    lib = lmdb_version_store_v1
    lib_tool = lib.library_tool()
    sym = "test_staging_doesnt_write_append_ref"
    df = pd.DataFrame({"col": [0]})
    lib.write(sym, df, parallel=True)
    assert not len(lib_tool.find_keys_for_symbol(KeyType.APPEND_REF, sym))
    lib.version_store.clear()
    lib.write(sym, df, incomplete=True)
    assert not len(lib_tool.find_keys_for_symbol(KeyType.APPEND_REF, sym))
    lib.version_store.clear()
    lib.append(sym, df, incomplete=True)
    assert not len(lib_tool.find_keys_for_symbol(KeyType.APPEND_REF, sym))


@pytest.mark.storage
@pytest.mark.parametrize("batch", (True, False))
@pytest.mark.parametrize("batch_size", (1000, 7))
def test_remove_incomplete(arctic_library_v1, batch, batch_size, lib_name):
    lib = arctic_library_v1._nvs
    if lib.get_backing_store() == "mongo_storage":
        with pytest.raises(ArcticDbNotYetImplemented):
            arctic_library_v1._dev_tools.remove_incompletes(["sym"])
        return # remove_incompletes not implemented on Mongo 8784267430

    with config_context_multi({"Storage.DeleteBatchSize": batch_size, "S3Storage.DeleteBatchSize": 2 * batch_size}):
        lib_tool = lib.library_tool()
        assert lib_tool.find_keys(KeyType.APPEND_DATA) == []
        assert lib.list_symbols_with_incomplete_data() == []

        if batch:
            def remove(sym):
                arctic_library_v1._dev_tools.remove_incompletes([sym])
        else:
            def remove(sym):
                lib.remove_incomplete(sym)

        sym1 = "test_remove_incomplete_1"
        sym2 = "test_remove_incomplete_2"
        num_chunks = 10
        df1 = pd.DataFrame({"col": np.arange(10)}, index=pd.date_range("2000-01-01", periods=num_chunks))
        df2 = pd.DataFrame(
            {"col": np.arange(100, 110)},
            index=pd.date_range("2001-01-01", periods=num_chunks),
        )
        for idx in range(num_chunks):
            lib.write(sym1, df1.iloc[idx : idx + 1, :], parallel=True)
            lib.write(sym2, df2.iloc[idx : idx + 1, :], parallel=True)

        assert len(lib_tool.find_keys_for_symbol(KeyType.APPEND_DATA, sym1)) == num_chunks
        assert len(lib_tool.find_keys_for_symbol(KeyType.APPEND_DATA, sym2)) == num_chunks
        assert sorted(lib.list_symbols_with_incomplete_data()) == [sym1, sym2]

        remove(sym1)
        assert lib_tool.find_keys_for_symbol(KeyType.APPEND_DATA, sym1) == []
        assert len(lib_tool.find_keys_for_symbol(KeyType.APPEND_DATA, sym2)) == num_chunks
        assert lib.list_symbols_with_incomplete_data() == [sym2]

        remove(sym2)
        assert lib_tool.find_keys(KeyType.APPEND_DATA) == []
        assert lib.list_symbols_with_incomplete_data() == []

        # Removing incompletes from a symbol that doesn't exist, or a symbol with no incompletes, is a no-op
        remove("non-existent-symbol")
        sym3 = "test_remove_incomplete_3"
        lib.write(sym3, df1)
        remove(sym3)


@pytest.mark.parametrize("batch_size", (1000, 7))
def test_remove_incompletes(arctic_library_v1, batch_size):
    if arctic_library_v1._nvs.get_backing_store() == "mongo_storage":
        with pytest.raises(ArcticDbNotYetImplemented):
            arctic_library_v1._dev_tools.remove_incompletes(["sym"])
        return # remove_incompletes not implemented on Mongo 8784267430

    with config_context_multi({"Storage.DeleteBatchSize": batch_size, "S3Storage.DeleteBatchSize": 2 * batch_size}):
        lib = arctic_library_v1
        lib_tool = lib._dev_tools.library_tool()
        assert lib_tool.find_keys(KeyType.APPEND_DATA) == []
        assert lib.get_staged_symbols() == []

        n_symbols = 20
        n_to_delete = 9
        syms = [f"sym_{i}" for i in range(n_symbols)]
        other_syms = [f"other_prefix_{i}" for i in range(n_symbols)]
        to_delete = list(np.random.choice(syms, n_to_delete))

        num_chunks = 10
        df = pd.DataFrame({"col": np.arange(10)}, index=pd.date_range("2000-01-01", periods=num_chunks))
        for s in syms + other_syms:
            for idx in range(num_chunks):
                lib.write(s, df.iloc[idx : idx + 1, :], staged=True)

        assert len(lib_tool.find_keys_for_symbol(KeyType.APPEND_DATA, "sym_0")) == num_chunks
        assert sorted(lib.get_staged_symbols()) == sorted(syms + other_syms)

        lib._dev_tools.remove_incompletes(to_delete)
        for s in to_delete:
            assert lib_tool.find_keys_for_symbol(KeyType.APPEND_DATA, s) == []

        survivors = set(syms + other_syms) - set(to_delete)
        for s in survivors:
            assert len(lib_tool.find_keys_for_symbol(KeyType.APPEND_DATA, s)) == num_chunks
        assert sorted(lib.get_staged_symbols()) == sorted(list(survivors))

        with pytest.raises(UserInputException, match="E_NO_STAGED_SEGMENTS"):
            lib.finalize_staged_data(to_delete[0])

        to_finalize = survivors.pop()
        lib.finalize_staged_data(to_finalize)
        assert_frame_equal(lib.read(to_finalize).data, df)


def test_remove_incompletes_no_common_prefix(basic_store):
    lib = Library("desc", basic_store)
    lib_tool = lib._dev_tools.library_tool()
    assert lib_tool.find_keys(KeyType.APPEND_DATA) == []
    assert lib.get_staged_symbols() == []

    df = pd.DataFrame({"a": [1]})
    df.index = [pd.Timestamp(0)]
    lib.write("sym", df, staged=True)
    lib.write("tzm", df, staged=True)
    lib.write("uan", df, staged=True)

    assert len(lib_tool.find_keys(KeyType.APPEND_DATA)) == 3
    assert sorted(lib.get_staged_symbols()) == ["sym", "tzm", "uan"]

    lib._dev_tools.remove_incompletes(["sym", "uan"])
    assert len(lib_tool.find_keys(KeyType.APPEND_DATA)) == 1
    assert sorted(lib.get_staged_symbols()) == ["tzm"]


@pytest.mark.parametrize("num_segments_live_during_compaction, num_io_threads, num_cpu_threads", [
    (1, 1, 1),
    (10, 1, 1),
    (1, 10, 1),
    (None, None, None)
])
@pytest.mark.storage
def test_parallel_write(basic_store_tiny_segment, num_segments_live_during_compaction, num_io_threads, num_cpu_threads):
    try:
        with config_context_multi({"VersionStore.NumSegmentsLiveDuringCompaction": num_segments_live_during_compaction,
                                   "VersionStore.NumIOThreads": num_io_threads,
                                   "VersionStore.NumCPUThreads": num_cpu_threads}):
            adb_async.reinit_task_scheduler()
            if num_io_threads:
                assert adb_async.io_thread_count() == num_io_threads
            if num_cpu_threads:
                assert adb_async.cpu_thread_count() == num_cpu_threads

            store = basic_store_tiny_segment
            sym = "parallel"
            store.remove_incomplete(sym)

            num_rows = 1111
            dtidx = pd.date_range("1970-01-01", periods=num_rows)
            test = pd.DataFrame(
                {
                    "uint8": random_integers(num_rows, np.uint8),
                    "uint32": random_integers(num_rows, np.uint32),
                },
                index=dtidx,
            )
            chunk_size = 100
            list_df = [test[i : i + chunk_size] for i in range(0, test.shape[0], chunk_size)]
            random.shuffle(list_df)

            for df in list_df:
                store.write(sym, df, parallel=True)

            user_meta = {"thing": 7}
            store.compact_incomplete(sym, False, False, metadata=user_meta)
            vit = store.read(sym)
            assert_frame_equal(test, vit.data)
            assert vit.metadata["thing"] == 7
            assert len(get_append_keys(store, sym)) == 0
    finally:
        adb_async.reinit_task_scheduler()


@pytest.mark.parametrize(
    "index, expect_ordered",
    [
        (pd.date_range(datetime.datetime(2000, 1, 1), periods=10, freq="s"), True),
        (np.arange(10), False),
        ([chr(ord("a") + i) for i in range(10)], False),
    ],
)
def test_parallel_write_chunking(lmdb_version_store_tiny_segment, index, expect_ordered):
    lib = lmdb_version_store_tiny_segment  # row size 2 column size 2
    lib_tool = lib.library_tool()
    sym = "sym"
    df = util.test.sample_dataframe(size=10)
    df.index = index
    lib.write(sym, df.iloc[:7], parallel=True)
    lib.write(sym, df.iloc[7:], parallel=True)

    data_keys = lib_tool.find_keys_for_symbol(KeyType.APPEND_DATA, sym)
    # We don't apply column slicing when staging incompletes, do apply row slicing
    assert len(data_keys) == 6

    ref_keys = lib_tool.find_keys_for_symbol(KeyType.APPEND_REF, sym)
    assert not ref_keys

    lib.compact_incomplete(sym, append=False, convert_int_to_float=False)

    assert_frame_equal(df, lib.read(sym).data, check_like=not expect_ordered)


def test_parallel_write_chunking_dynamic(lmdb_version_store_tiny_segment_dynamic):
    lib = lmdb_version_store_tiny_segment_dynamic  # row size 2 column size 2
    lib_tool = lib.library_tool()
    sym = "sym"

    df1 = pd.DataFrame(
        {
            "timestamp": pd.date_range("2023-01-01", periods=7, freq="H"),
            "col1": np.arange(1, 8, dtype=np.uint8),
            "col2": [f"a{i:02d}" for i in range(1, 8)],
            "col3": np.arange(1, 8, dtype=np.int32),
        }
    ).set_index("timestamp")

    df2 = pd.DataFrame(
        {
            "timestamp": pd.date_range("2023-01-04", periods=7, freq="H"),
            "col1": np.arange(8, 15, dtype=np.int32),
            "col2": [f"b{i:02d}" for i in range(8, 15)],
            "col3": np.arange(8, 15, dtype=np.uint16),
        }
    ).set_index("timestamp")

    lib.write(sym, df1, parallel=True)
    lib.write(sym, df2, parallel=True)

    data_keys = lib_tool.find_keys_for_symbol(KeyType.APPEND_DATA, sym)
    assert len(data_keys) == 8

    ref_keys = lib_tool.find_keys_for_symbol(KeyType.APPEND_REF, sym)
    assert not ref_keys

    lib.compact_incomplete(sym, append=False, convert_int_to_float=False)

    expected = pd.concat([df1, df2])
    assert_frame_equal(expected, lib.read(sym).data)


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


@pytest.mark.parametrize(
    "num_segments_live_during_compaction, num_io_threads, num_cpu_threads", [(1, 1, 1), (10, 1, 1), (None, None, None)]
)
@pytest.mark.parametrize("prune_previous_versions", (True, False))
@pytest.mark.storage
def test_parallel_write_sort_merge(
    basic_store_tiny_segment,
    lib_name,
    prune_previous_versions,
    num_segments_live_during_compaction,
    num_io_threads,
    num_cpu_threads,
):
    with config_context_multi(
        {
            "VersionStore.NumSegmentsLiveDuringCompaction": num_segments_live_during_compaction,
            "VersionStore.NumIOThreads": num_io_threads,
            "VersionStore.NumCPUThreads": num_cpu_threads,
        }
    ):
        lib = Library(lib_name, basic_store_tiny_segment)

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
@pytest.mark.storage
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
            df = pd.concat(old_dataframes[0 : version + 1])
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
        df_0 = pd.DataFrame(
            {"col": [1, 2]},
            index=[pd.Timestamp("2024-01-01"), pd.Timestamp("2024-01-02")],
        )
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
        df_0 = pd.DataFrame(
            {"col": [1, 2]},
            index=[pd.Timestamp("2024-01-01"), pd.Timestamp("2024-01-02")],
        )
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
        df_0 = pd.DataFrame(
            {"col": [1, 2]},
            index=[pd.Timestamp("2024-01-01"), pd.Timestamp("2024-01-01")],
        )
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


@pytest.mark.parametrize("delete_staged_data_on_failure", [True, False])
@pytest.mark.parametrize("append", (True, False))
@pytest.mark.parametrize("validate_index", (True, False, None))
def test_parallel_overlapping_incomplete_segments(
    lmdb_version_store, append, validate_index, delete_staged_data_on_failure
):
    lib = lmdb_version_store
    sym = "test_parallel_overlapping_incomplete_segments"
    if append:
        df_0 = pd.DataFrame(
            {"col": [1, 2]},
            index=[pd.Timestamp("2024-01-01"), pd.Timestamp("2024-01-02")],
        )
        lib.write(sym, df_0)
    df_1 = pd.DataFrame({"col": [3, 4]}, index=[pd.Timestamp("2024-01-03"), pd.Timestamp("2024-01-04")])
    df_2 = pd.DataFrame(
        {"col": [5, 6]},
        index=[pd.Timestamp("2024-01-03T12"), pd.Timestamp("2024-01-05")],
    )
    if append:
        lib.append(sym, df_2, incomplete=True)
        lib.append(sym, df_1, incomplete=True)
    else:
        lib.write(sym, df_2, parallel=True)
        lib.write(sym, df_1, parallel=True)
    if validate_index:
        with pytest.raises(SortingException):
            lib.compact_incomplete(
                sym,
                append,
                False,
                validate_index=True,
                delete_staged_data_on_failure=delete_staged_data_on_failure,
            )
        expected_key_count = 0 if delete_staged_data_on_failure else 2
        assert len(get_append_keys(lib, sym)) == expected_key_count
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


@pytest.mark.parametrize("delete_staged_data_on_failure", [True, False])
@pytest.mark.parametrize("append", (True, False))
@pytest.mark.parametrize("validate_index", (True, False, None))
def test_parallel_all_incomplete_segments_same_index(
    lmdb_version_store_v1, append, validate_index, delete_staged_data_on_failure
):
    lib = lmdb_version_store_v1
    sym = "test_parallel_all_incomplete_segments_same_index"
    if append:
        df_0 = pd.DataFrame(
            {"col": [1, 2]},
            index=[pd.Timestamp("2024-01-01"), pd.Timestamp("2024-01-02")],
        )
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
            lib.compact_incomplete(
                sym,
                append,
                False,
                validate_index=True,
                delete_staged_data_on_failure=delete_staged_data_on_failure,
            )
        expected_key_count = 0 if delete_staged_data_on_failure else 2
        assert len(get_append_keys(lib, sym)) == expected_key_count
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


@pytest.mark.parametrize("delete_staged_data_on_failure", [True, False])
@pytest.mark.parametrize("validate_index", (True, False, None))
def test_parallel_append_overlapping_with_existing(lmdb_version_store, validate_index, delete_staged_data_on_failure):
    lib = lmdb_version_store
    sym = "test_parallel_append_overlapping_with_existing"
    df_0 = pd.DataFrame({"col": [1, 2]}, index=[pd.Timestamp("2024-01-01"), pd.Timestamp("2024-01-02")])
    lib.write(sym, df_0)
    df_1 = pd.DataFrame(
        {"col": [3, 4]},
        index=[pd.Timestamp("2024-01-01T12"), pd.Timestamp("2024-01-03")],
    )
    lib.append(sym, df_1, incomplete=True)
    if validate_index:
        with pytest.raises(SortingException):
            lib.compact_incomplete(
                sym,
                True,
                False,
                validate_index=validate_index,
                delete_staged_data_on_failure=delete_staged_data_on_failure,
            )
        expected_key_count = 0 if delete_staged_data_on_failure else 1
        assert len(get_append_keys(lib, sym)) == expected_key_count
    else:
        if validate_index is None:
            # Test default behaviour when arg isn't provided
            lib.compact_incomplete(sym, True, False)
        else:
            lib.compact_incomplete(sym, True, False, validate_index=False)
        received = lib.read(sym).data
        assert_frame_equal(received, pd.concat([df_0, df_1]))


@pytest.mark.parametrize("delete_staged_data_on_failure", [True, False])
@pytest.mark.parametrize("sortedness", ("DESCENDING", "UNSORTED"))
@pytest.mark.parametrize("validate_index", (True, False, None))
def test_parallel_append_existing_data_unsorted(
    lmdb_version_store, sortedness, validate_index, delete_staged_data_on_failure
):
    lib = lmdb_version_store
    sym = "test_parallel_append_existing_data_unsorted"
    last_index_date = "2024-01-01" if sortedness == "DESCENDING" else "2024-01-03"
    df_0 = pd.DataFrame(
        {"col": [1, 2, 3]},
        index=[
            pd.Timestamp("2024-01-04"),
            pd.Timestamp("2024-01-02"),
            pd.Timestamp(last_index_date),
        ],
    )
    lib.write(sym, df_0)
    assert lib.get_info(sym)["sorted"] == sortedness
    df_1 = pd.DataFrame({"col": [3, 4]}, index=[pd.Timestamp("2024-01-05"), pd.Timestamp("2024-01-06")])
    lib.append(sym, df_1, incomplete=True)
    if validate_index:
        with pytest.raises(SortingException):
            lib.compact_incomplete(
                sym,
                True,
                False,
                validate_index=True,
                delete_staged_data_on_failure=delete_staged_data_on_failure,
            )
        expected_key_count = 0 if delete_staged_data_on_failure else 1
        assert len(get_append_keys(lib, sym)) == expected_key_count
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


@pytest.mark.parametrize("delete_staged_data_on_failure", [True, False])
@pytest.mark.parametrize("rows_per_incomplete", (1, 2))
def test_parallel_write_static_schema_type_changing(
    lmdb_version_store_tiny_segment, rows_per_incomplete, delete_staged_data_on_failure
):
    lib = lmdb_version_store_tiny_segment
    sym = "test_parallel_write_static_schema_type_changing"
    df_0 = pd.DataFrame(
        {"col": np.arange(rows_per_incomplete, dtype=np.uint8)},
        index=pd.date_range("2024-01-01", periods=rows_per_incomplete),
    )
    df_1 = pd.DataFrame(
        {"col": np.arange(rows_per_incomplete, 2 * rows_per_incomplete, dtype=np.uint16)},
        index=pd.date_range("2024-01-03", periods=rows_per_incomplete),
    )
    lib.write(sym, df_0, parallel=True)
    lib.write(sym, df_1, parallel=True)
    with pytest.raises(SchemaException):
        lib.compact_incomplete(
            sym,
            False,
            False,
            delete_staged_data_on_failure=delete_staged_data_on_failure,
        )
    expected_key_count = 0 if delete_staged_data_on_failure else 2
    assert len(get_append_keys(lib, sym)) == expected_key_count


@pytest.mark.parametrize("rows_per_incomplete", (1, 2))
def test_parallel_write_dynamic_schema_type_changing(lmdb_version_store_tiny_segment_dynamic, rows_per_incomplete):
    lib = lmdb_version_store_tiny_segment_dynamic
    sym = "test_parallel_write_dynamic_schema_type_changing"
    df_0 = pd.DataFrame(
        {"col": np.arange(rows_per_incomplete, dtype=np.uint8)},
        index=pd.date_range("2024-01-01", periods=rows_per_incomplete),
    )
    df_1 = pd.DataFrame(
        {"col": np.arange(rows_per_incomplete, 2 * rows_per_incomplete, dtype=np.uint16)},
        index=pd.date_range("2024-01-02", periods=rows_per_incomplete),
    )
    lib.write(sym, df_0, parallel=True)
    lib.write(sym, df_1, parallel=True)
    lib.compact_incomplete(sym, False, False)
    expected = pd.concat([df_0, df_1])
    received = lib.read(sym).data
    assert_frame_equal(expected, received)
    assert len(get_data_keys(lib, sym)) == rows_per_incomplete
    assert len(get_index_keys(lib, sym)) == 1


@pytest.mark.parametrize("delete_staged_data_on_failure", [True, False])
@pytest.mark.parametrize("symbol_already_exists", [True, False])
def test_parallel_write_static_schema_type_changing_cleans_up_data_keys(
    lmdb_version_store_tiny_segment,
    delete_staged_data_on_failure,
    symbol_already_exists,
):
    lib = lmdb_version_store_tiny_segment
    sym = "test_parallel_write_static_schema_type_changing"
    rows_per_incomplete = 2  # tiny segment store uses 2 rows per segment
    if symbol_already_exists:
        df_0 = pd.DataFrame(
            {"col": np.arange(rows_per_incomplete, dtype=np.uint8)},
            index=pd.date_range("2024-01-01", periods=rows_per_incomplete),
        )
        lib.write(sym, df_0)

    df_1 = pd.DataFrame(
        {"col": np.arange(rows_per_incomplete, dtype=np.uint8)},
        index=pd.date_range("2024-01-03", periods=rows_per_incomplete),
    )
    df_2 = pd.DataFrame(
        {"col": np.arange(rows_per_incomplete, 2 * rows_per_incomplete, dtype=np.uint16)},
        index=pd.date_range("2024-01-05", periods=rows_per_incomplete),
    )
    lib.write(sym, df_1, parallel=True)
    lib.write(sym, df_2, parallel=True)
    with pytest.raises(SchemaException):
        lib.compact_incomplete(
            sym,
            False,
            False,
            delete_staged_data_on_failure=delete_staged_data_on_failure,
        )
    expected_key_count = 0 if delete_staged_data_on_failure else 2
    assert len(get_append_keys(lib, sym)) == expected_key_count

    expected_key_count = 1 if symbol_already_exists else 0
    assert len(get_data_keys(lib, sym)) == expected_key_count
    assert len(get_index_keys(lib, sym)) == expected_key_count


@pytest.mark.parametrize("delete_staged_data_on_failure", [True, False])
def test_parallel_write_static_schema_missing_column(lmdb_version_store_tiny_segment, delete_staged_data_on_failure):
    lib = lmdb_version_store_tiny_segment
    sym = "test_parallel_write_static_schema_missing_column"
    df_0 = pd.DataFrame({"col_0": [0], "col_1": [0.5]}, index=pd.date_range("2024-01-01", periods=1))
    df_1 = pd.DataFrame({"col_0": [1]}, index=pd.date_range("2024-01-02", periods=1))
    lib.write(sym, df_0, parallel=True)
    lib.write(sym, df_1, parallel=True)
    with pytest.raises(SchemaException):
        lib.compact_incomplete(
            sym,
            False,
            False,
            delete_staged_data_on_failure=delete_staged_data_on_failure,
        )
    expected_key_count = 0 if delete_staged_data_on_failure else 2
    assert len(get_append_keys(lib, sym)) == expected_key_count


def test_parallel_write_dynamic_schema_missing_column(
    lmdb_version_store_tiny_segment_dynamic,
):
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


def test_parallel_append_dynamic_schema_missing_column(
    lmdb_version_store_tiny_segment_dynamic,
):
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


@pytest.mark.parametrize("delete_staged_data_on_failure", [True, False])
@pytest.mark.parametrize("append", (True, False))
def test_parallel_dynamic_schema_named_index(
    lmdb_version_store_tiny_segment_dynamic, delete_staged_data_on_failure, append
):
    lib = lmdb_version_store_tiny_segment_dynamic
    sym = "test_parallel_append_dynamic_schema_named_index"
    df_0 = pd.DataFrame({"col_0": [0], "col_1": [0.5]}, index=pd.date_range("2024-01-01", periods=1))
    df_0.index.name = "date"
    df_1 = pd.DataFrame({"col_0": [1]}, index=pd.date_range("2024-01-02", periods=1))
    if append:
        lib.write(sym, df_0)
        lib.append(sym, df_1, incomplete=True)
    else:
        lib.write(sym, df_0, parallel=True)
        lib.write(sym, df_1, parallel=True)

    with pytest.raises(SchemaException) as exception_info:
        lib.compact_incomplete(
            sym,
            append,
            False,
            delete_staged_data_on_failure=delete_staged_data_on_failure,
        )

    assert "date" in str(exception_info.value)
    staged_keys = 1 if append else 2
    expected_key_count = 0 if delete_staged_data_on_failure else staged_keys
    assert len(get_append_keys(lib, sym)) == expected_key_count


@pytest.mark.parametrize("delete_staged_data_on_failure", [True, False])
class TestFinalizeStagedDataStaticSchemaMismatch:
    def test_append_throws_with_missmatched_column_set(self, lmdb_version_store_v1, delete_staged_data_on_failure):
        lib = lmdb_version_store_v1

        initial_df = pd.DataFrame({"col_0": [1]}, index=pd.DatetimeIndex([pd.Timestamp(2024, 1, 1)]))
        lib.write("sym", initial_df)

        appended_df = pd.DataFrame({"col_1": [1]}, index=pd.DatetimeIndex([pd.Timestamp(2024, 1, 2)]))
        lib.write("sym", appended_df, parallel=True)
        with pytest.raises(SchemaException) as exception_info:
            lib.compact_incomplete(
                "sym",
                True,
                False,
                delete_staged_data_on_failure=delete_staged_data_on_failure,
            )
        assert "col_1" in str(exception_info.value)
        expected_key_count = 0 if delete_staged_data_on_failure else 1
        assert len(get_append_keys(lib, "sym")) == expected_key_count

    def test_append_throws_column_subset(self, lmdb_version_store_v1, delete_staged_data_on_failure):
        lib = lmdb_version_store_v1

        df1 = pd.DataFrame(
            {"a": np.array([1.1], dtype="float"), "b": np.array([2], dtype="int64")},
            index=pd.DatetimeIndex([pd.Timestamp("2024-01-01")]),
        )
        lib.write("sym", df1)
        df2 = pd.DataFrame({"b": [1]}, index=pd.DatetimeIndex([pd.Timestamp("2024-01-02")]))
        lib.write("sym", df2, parallel=True)
        with pytest.raises(SchemaException) as exception_info:
            lib.compact_incomplete(
                "sym",
                True,
                False,
                delete_staged_data_on_failure=delete_staged_data_on_failure,
            )
        assert "a" in str(exception_info.value)
        assert "b" in str(exception_info.value)
        expected_key_count = 0 if delete_staged_data_on_failure else 1
        assert len(get_append_keys(lib, "sym")) == expected_key_count

    def test_append_throws_on_incompatible_dtype(self, lmdb_version_store_v1, delete_staged_data_on_failure):
        lib = lmdb_version_store_v1

        initial_df = pd.DataFrame(
            {"col_0": np.array([1], dtype="int64")},
            index=pd.DatetimeIndex([pd.Timestamp(2024, 1, 1)]),
        )
        lib.write("sym", initial_df)

        appended_df = pd.DataFrame({"col_0": ["asd"]}, index=pd.DatetimeIndex([pd.Timestamp(2024, 1, 2)]))
        lib.write("sym", appended_df, parallel=True)
        with pytest.raises(SchemaException) as exception_info:
            lib.compact_incomplete(
                "sym",
                True,
                False,
                delete_staged_data_on_failure=delete_staged_data_on_failure,
            )
        assert "col_0" in str(exception_info.value)
        assert "INT64" in str(exception_info.value)
        expected_key_count = 0 if delete_staged_data_on_failure else 1
        assert len(get_append_keys(lib, "sym")) == expected_key_count

    def test_type_mismatch_in_staged_segments_throws(self, lmdb_version_store_v1, delete_staged_data_on_failure):
        lib = lmdb_version_store_v1
        lib.write(
            "sym",
            pd.DataFrame({"col": [1]}, index=pd.DatetimeIndex([np.datetime64("2023-01-01")])),
            parallel=True,
        )
        lib.write(
            "sym",
            pd.DataFrame({"col": ["a"]}, index=pd.DatetimeIndex([np.datetime64("2023-01-02")])),
            parallel=True,
        )
        with pytest.raises(Exception) as exception_info:
            lib.compact_incomplete(
                "sym",
                False,
                False,
                delete_staged_data_on_failure=delete_staged_data_on_failure,
            )
        assert all(x in str(exception_info.value) for x in ["INT64", "type"])
        expected_key_count = 0 if delete_staged_data_on_failure else 2
        assert len(get_append_keys(lib, "sym")) == expected_key_count

    def test_types_cant_be_promoted(self, lmdb_version_store_v1, delete_staged_data_on_failure):
        lib = lmdb_version_store_v1
        lib.write(
            "sym",
            pd.DataFrame(
                {"col": np.array([1], dtype="int64")},
                index=pd.DatetimeIndex([pd.Timestamp(2024, 1, 1)]),
            ),
        )

        lib.write(
            "sym",
            pd.DataFrame(
                {"col": np.array([1], dtype="int32")},
                index=pd.DatetimeIndex([pd.Timestamp(2024, 1, 2)]),
            ),
            parallel=True,
        )
        with pytest.raises(SchemaException) as exception_info:
            lib.compact_incomplete(
                "sym",
                True,
                False,
                delete_staged_data_on_failure=delete_staged_data_on_failure,
            )
        assert "INT32" in str(exception_info.value)
        assert "INT64" in str(exception_info.value)
        expected_key_count = 0 if delete_staged_data_on_failure else 1
        assert len(get_append_keys(lib, "sym")) == expected_key_count

    def test_appending_reordered_column_set_throws(self, lmdb_version_store_v1, delete_staged_data_on_failure):
        lib = lmdb_version_store_v1

        lib.write(
            "sym",
            pd.DataFrame(
                {"col_0": [1], "col_1": ["test"], "col_2": [1.2]},
                index=pd.DatetimeIndex([pd.Timestamp(2024, 1, 1)]),
            ),
        )

        lib.write(
            "sym",
            pd.DataFrame(
                {"col_1": ["asd"], "col_2": [2.5], "col_0": [2]},
                index=pd.DatetimeIndex([pd.Timestamp(2024, 1, 2)]),
            ),
            parallel=True,
        )
        with pytest.raises(SchemaException) as exception_info:
            lib.compact_incomplete(
                "sym",
                True,
                False,
                delete_staged_data_on_failure=delete_staged_data_on_failure,
            )
        assert "col_0" in str(exception_info.value)
        assert "col_1" in str(exception_info.value)
        assert "col_2" in str(exception_info.value)
        expected_key_count = 0 if delete_staged_data_on_failure else 1
        assert len(get_append_keys(lib, "sym")) == expected_key_count

    @pytest.mark.parametrize("mode", [True, False])
    def test_staged_segments_can_be_reordered(self, lmdb_version_store_v1, mode, delete_staged_data_on_failure):
        lib = lmdb_version_store_v1
        df1 = pd.DataFrame(
            {"col_0": [1], "col_1": ["test"], "col_2": [1.2]},
            index=pd.DatetimeIndex([pd.Timestamp(2024, 1, 1)]),
        )
        df2 = pd.DataFrame(
            {"col_1": ["asd"], "col_2": [2.5], "col_0": [2]},
            index=pd.DatetimeIndex([pd.Timestamp(2024, 1, 2)]),
        )
        lib.write("sym", df1, parallel=True)
        lib.write("sym", df2, parallel=True)
        expected = pd.concat([df1, df2]).sort_index()
        with pytest.raises(SchemaException) as exception_info:
            lib.compact_incomplete(
                "sym",
                mode,
                False,
                delete_staged_data_on_failure=delete_staged_data_on_failure,
            )
        assert "col_0" in str(exception_info.value)
        assert "col_1" in str(exception_info.value)
        assert "col_2" in str(exception_info.value)
        expected_key_count = 0 if delete_staged_data_on_failure else 2
        assert len(get_append_keys(lib, "sym")) == expected_key_count


# finalize_method True -> append, finalize_method False -> write
@pytest.mark.parametrize("finalize_method", (True, False))
class TestFinalizeWithEmptySegments:
    def test_staged_segment_is_only_empty_dfs(self, lmdb_version_store_v1, finalize_method):
        lib = lmdb_version_store_v1
        lib.write("sym", pd.DataFrame([]), parallel=True)
        lib.write("sym", pd.DataFrame([]), parallel=True)
        lib.write("sym", pd.DataFrame(), parallel=True)
        lib.compact_incomplete("sym", finalize_method, False)
        assert_frame_equal(lib.read("sym").data, pd.DataFrame([], index=pd.DatetimeIndex([])))

    @pytest.mark.parametrize("delete_staged_data_on_failure", [True, False])
    def test_staged_segment_has_empty_df(self, lmdb_version_store_v1, finalize_method, delete_staged_data_on_failure):
        lib = lmdb_version_store_v1
        index = pd.DatetimeIndex(
            [
                pd.Timestamp(2024, 1, 1),
                pd.Timestamp(2024, 1, 3),
                pd.Timestamp(2024, 1, 4),
            ]
        )
        df1 = pd.DataFrame({"col": [1, 2, 3]}, index=index)
        df2 = pd.DataFrame({})
        df3 = pd.DataFrame({"col": [4]}, index=pd.DatetimeIndex([pd.Timestamp(2024, 1, 5)]))
        lib.write("sym", df1, parallel=True)
        lib.write("sym", df2, parallel=True)
        lib.write("sym", df3, parallel=True)
        with pytest.raises(SchemaException):
            lib.compact_incomplete(
                "sym",
                finalize_method,
                False,
                delete_staged_data_on_failure=delete_staged_data_on_failure,
            )
        expected_key_count = 0 if delete_staged_data_on_failure else 3
        assert len(get_append_keys(lib, "sym")) == expected_key_count

    def test_df_without_rows(self, lmdb_version_store_v1, finalize_method):
        lib = lmdb_version_store_v1
        df = pd.DataFrame({"col": []}, index=pd.DatetimeIndex([]))
        lib.write("sym", df, parallel=True)
        lib.compact_incomplete("sym", finalize_method, False)
        assert_frame_equal(lib.read("sym").data, df)


class TestSlicing:
    def test_append_long_segment(self, lmdb_version_store_tiny_segment):
        lib = lmdb_version_store_tiny_segment
        df_0 = pd.DataFrame({"col_0": [1, 2, 3]}, index=pd.date_range("2024-01-01", "2024-01-03"))
        lib.write("sym", df_0)

        index = pd.date_range("2024-01-05", "2024-01-15")
        df_1 = pd.DataFrame({"col_0": range(0, len(index))}, index=index)
        lib.write("sym", df_1, parallel=True)
        lib.compact_incomplete("sym", True, False)

        assert_frame_equal(lib.read("sym").data, pd.concat([df_0, df_1]))

    def test_write_long_segment(self, lmdb_version_store_tiny_segment):
        lib = lmdb_version_store_tiny_segment
        index = pd.date_range("2024-01-05", "2024-01-15")
        df = pd.DataFrame({"col_0": range(0, len(index))}, index=index)
        lib.write("sym", df, parallel=True)
        lib.compact_incomplete("sym", False, False)
        assert_frame_equal(lib.read("sym").data, df)

    def test_write_several_segments_triggering_slicing(self, lmdb_version_store_tiny_segment):
        lib = lmdb_version_store_tiny_segment
        combined_staged_index = pd.date_range(pd.Timestamp(2024, 1, 1), pd.Timestamp(2024, 1, 15))
        staged_values = range(0, len(combined_staged_index))
        for value, date in zip(staged_values, combined_staged_index):
            df = pd.DataFrame({"a": [value]}, index=pd.DatetimeIndex([date]))
            lib.write("sym", df, parallel=True)
        lib.compact_incomplete("sym", False, False)
        expected = pd.DataFrame({"a": staged_values}, index=combined_staged_index)
        assert_frame_equal(lib.read("sym").data, expected)

    def test_append_several_segments_trigger_slicing(self, lmdb_version_store_tiny_segment):
        lib = lmdb_version_store_tiny_segment
        df_0 = pd.DataFrame(
            {"a": [1, 2, 3]},
            index=pd.date_range(pd.Timestamp(2024, 1, 1), pd.Timestamp(2024, 1, 3)),
        )
        lib.write("sym", df_0)
        combined_staged_index = pd.date_range(pd.Timestamp(2024, 1, 5), pd.Timestamp(2024, 1, 20))
        staged_values = range(0, len(combined_staged_index))
        for value, date in zip(staged_values, combined_staged_index):
            df = pd.DataFrame({"a": [value]}, index=pd.DatetimeIndex([date]))
            lib.write("sym", df, parallel=True)
        lib.compact_incomplete("sym", True, False)
        expected = pd.concat([df_0, pd.DataFrame({"a": staged_values}, index=combined_staged_index)])
        assert_frame_equal(lib.read("sym").data, expected)

    @pytest.mark.parametrize("delete_staged_data_on_failure", [True, False])
    @pytest.mark.parametrize("mode", [True, False])
    def test_wide_segment_with_no_prior_slicing(
        self, lmdb_version_store_tiny_segment, mode, delete_staged_data_on_failure
    ):
        lib = lmdb_version_store_tiny_segment
        df_0 = pd.DataFrame(
            {f"col_{i}": [i] for i in range(0, 10)},
            index=pd.DatetimeIndex([pd.Timestamp(2024, 1, 1)]),
        )
        lib.write("sym", df_0, parallel=True)
        lib.compact_incomplete("sym", mode, False)
        assert_frame_equal(lib.read("sym").data, df_0)

        df_1 = pd.DataFrame(
            {f"col_{i}": [i] for i in range(0, 10)},
            index=pd.DatetimeIndex([pd.Timestamp(2024, 1, 2)]),
        )
        lib.append("sym", df_1)
        assert_frame_equal(lib.read("sym").data, pd.concat([df_0, df_1]))

        # Cannot perform another sort and finalize append when column sliced data has been written even though the first
        # write is done using sort and finalize
        with pytest.raises(UserInputException) as exception_info:
            lib.write(
                "sym",
                pd.DataFrame(
                    {f"col_{i}": [i] for i in range(0, 10)},
                    index=pd.DatetimeIndex([pd.Timestamp(2024, 1, 3)]),
                ),
                parallel=True,
            )
            lib.compact_incomplete(
                "sym",
                True,
                False,
                delete_staged_data_on_failure=delete_staged_data_on_failure,
            )
        assert "append" in str(exception_info.value).lower()
        assert "column" in str(exception_info.value).lower()
        assert "sliced" in str(exception_info.value).lower()
        expected_key_count = 0 if delete_staged_data_on_failure else 1
        assert len(get_append_keys(lib, "sym")) == expected_key_count

    @pytest.mark.parametrize("delete_staged_data_on_failure", [True, False])
    def test_appending_wide_segment_throws_with_prior_slicing(
        self, lmdb_version_store_tiny_segment, lib_name, delete_staged_data_on_failure
    ):
        lib = lmdb_version_store_tiny_segment
        df_0 = pd.DataFrame(
            {f"col_{i}": [i] for i in range(0, 10)},
            index=pd.DatetimeIndex([pd.Timestamp(2024, 1, 1)]),
        )
        lib.write("sym", df_0)

        df_1 = pd.DataFrame(
            {f"col_{i}": [i] for i in range(0, 10)},
            index=pd.DatetimeIndex([pd.Timestamp(2024, 1, 2)]),
        )
        lib.write("sym", df_1, parallel=True)
        with pytest.raises(UserInputException) as exception_info:
            lib.compact_incomplete(
                "sym",
                True,
                True,
                delete_staged_data_on_failure=delete_staged_data_on_failure,
            )
        assert "append" in str(exception_info.value).lower()
        assert "column" in str(exception_info.value).lower()
        assert "sliced" in str(exception_info.value).lower()
        expected_key_count = 0 if delete_staged_data_on_failure else 1
        assert len(get_append_keys(lib, "sym")) == expected_key_count

    def test_writing_wide_segment_over_sliced_data(self, lmdb_version_store_tiny_segment):
        lib = lmdb_version_store_tiny_segment
        df_0 = pd.DataFrame(
            {f"col_{i}": [i] for i in range(0, 10)},
            index=pd.DatetimeIndex([pd.Timestamp(2024, 1, 1)]),
        )
        lib.write("sym", df_0)

        df_1 = pd.DataFrame(
            {f"col_{i}": [i] for i in range(0, 10)},
            index=pd.DatetimeIndex([pd.Timestamp(2024, 1, 2)]),
        )
        lib.write("sym", df_1, parallel=True)

        lib.compact_incomplete("sym", False, False)

        assert_frame_equal(lib.read("sym").data, df_1)


def test_chunks_overlap(lmdb_storage, lib_name):
    """Given - we stage chunks with indexes:

    b:test:0:0xdfde242de44bdf38@1739968386409923711[0,1001]
    b:test:0:0x95750a82cfa088df@1739968386410180283[1000,1001]

    When - We finalize the staged segments

    Then - We should succeed even though the segments seem to overlap by 1ns because the end time in the key is 1
    greater than the last index value in the segment
    """
    lib: Library = lmdb_storage.create_arctic().create_library(
        lib_name, library_options=LibraryOptions(rows_per_segment=2)
    )

    idx = [
        pd.Timestamp(0),
        pd.Timestamp(1000),
        pd.Timestamp(1000),
        pd.Timestamp(1000),
    ]

    data = pd.DataFrame({"a": len(idx)}, index=idx)
    lib.write("test", data, staged=True)

    lt = lib._nvs.library_tool()
    append_keys = lt.find_keys_for_id(KeyType.APPEND_DATA, "test")
    assert len(append_keys) == 2
    assert sorted([key.start_index for key in append_keys]) == [0, 1000]
    assert [key.end_index for key in append_keys] == [1001, 1001]

    lib.finalize_staged_data("test")

    df = lib.read("test").data
    assert_frame_equal(df, data)


def test_chunks_overlap_1ns(lmdb_storage, lib_name):
    """Given - we stage chunks that overlap by 1ns

    When - We finalize the staged segments

    Then - We should raise a validation error
    """
    lib: Library = lmdb_storage.create_arctic().create_library(
        lib_name, library_options=LibraryOptions(rows_per_segment=2)
    )

    idx = [pd.Timestamp(0), pd.Timestamp(1), pd.Timestamp(2)]
    first = pd.DataFrame({"a": len(idx)}, index=idx)
    lib.write("test", first, staged=True)

    idx = [pd.Timestamp(1), pd.Timestamp(3)]
    second = pd.DataFrame({"a": len(idx)}, index=idx)
    lib.write("test", second, staged=True)

    with pytest.raises(UnsortedDataException):
        lib.finalize_staged_data("test")


def test_chunks_match_at_ends(lmdb_storage, lib_name):
    """Given - we stage chunks that match at the ends

    When - We finalize the staged segments

    Then - Should be OK to finalize
    """
    lib: Library = lmdb_storage.create_arctic().create_library(
        lib_name, library_options=LibraryOptions(rows_per_segment=2)
    )

    first_idx = [pd.Timestamp(0), pd.Timestamp(1), pd.Timestamp(2)]
    first = pd.DataFrame({"a": np.arange(3)}, index=first_idx)
    lib.write("test", first, staged=True)

    second_idx = [pd.Timestamp(2), pd.Timestamp(2), pd.Timestamp(2), pd.Timestamp(3)]
    second = pd.DataFrame({"a": np.arange(3, 7)}, index=second_idx)
    lib.write("test", second, staged=True)

    lib.finalize_staged_data("test")

    result = lib.read("test").data
    index_result = result.index
    assert index_result.equals(pd.Index(first_idx + second_idx))
    assert result.index.is_monotonic_increasing
    # There is some non-determinism about where the overlap will end up
    assert set(result["a"].values) == set(range(7))
    assert result["a"][0] == 0
    assert result["a"][-1] == 6


@pytest.mark.parametrize("n_runs", range(10))
def test_chunks_the_same(lmdb_storage, lib_name, n_runs):
    """Given - we stage chunks with indexes:

    b:test:0:h1@1739968588832977666[1000,2001]
    b:test:0:h2@1739968588832775570[1000,1001]
    b:test:0:h3@1739968588832621000[1000,1001]

    When - We finalize the staged segments

    Then - We should succeed even though the segments seem to be identical, since they are just covering a duplicated
    index value
    """
    lib: Library = lmdb_storage.create_arctic().create_library(
        lib_name, library_options=LibraryOptions(rows_per_segment=2)
    )

    idx = [
        pd.Timestamp(1000),
        pd.Timestamp(1000),
        pd.Timestamp(1000),
        pd.Timestamp(1000),
        pd.Timestamp(1000),
        pd.Timestamp(2000),
    ]

    data = pd.DataFrame({"a": np.arange(len(idx))}, index=idx, dtype=np.int64)
    lib.write("test", data, staged=True)

    lt = lib._nvs.library_tool()
    append_keys = lt.find_keys_for_id(KeyType.APPEND_DATA, "test")
    assert len(append_keys) == 3
    assert sorted([key.start_index for key in append_keys]) == [1000, 1000, 1000]
    assert sorted([key.end_index for key in append_keys]) == [1001, 1001, 2001]

    lib.finalize_staged_data("test")

    df = lib.read("test").data
    try:
        assert_frame_equal(df, data)
    except AssertionError:
        # Where the identical segments end up is arbitrary
        other_order = pd.DataFrame({"a": [2, 3, 0, 1, 4, 5]}, index=idx, dtype=np.int64)
        assert_frame_equal(df, other_order)

    assert df.index.is_monotonic_increasing


def test_staging_in_chunks_default_settings(lmdb_storage, lib_name):
    lib: Library = lmdb_storage.create_arctic().create_library(lib_name)
    idx = pd.date_range(pd.Timestamp(0), periods=int(31e5), freq="us")

    data = pd.DataFrame({"a": len(idx)}, index=idx)
    lib.write("test", data, staged=True)

    lt = lib._nvs.library_tool()
    append_keys = lt.find_keys_for_id(KeyType.APPEND_DATA, "test")
    assert len(append_keys) == 31
    lib.finalize_staged_data("test")

    df = lib.read("test").data
    assert_frame_equal(df, data)
    assert df.index.is_monotonic_increasing

class TestConvertIntToFloat:
    @pytest.mark.parametrize("dtype", [np.int32, np.uint16, np.int8, np.int64, np.uint64, np.float64, np.float32])
    @pytest.mark.parametrize("version_store", ["lmdb_version_store_v1", "lmdb_version_store_dynamic_schema_v1"])
    def test_write_convert_same_types(self, version_store, dtype, request):
        lib = request.getfixturevalue(version_store)
        sym = "sym"
        df1 = pd.DataFrame({"a": np.array([1, 2, 3], dtype=dtype)}, index=pd.date_range(pd.Timestamp(0), periods=3, freq="ns"))
        df2 = pd.DataFrame({"a": np.array([4, 5, 6], dtype=dtype)}, index=pd.date_range(pd.Timestamp(3), periods=3, freq="ns"))
        lib.write(sym, df1, parallel=True)
        lib.write(sym, df2, parallel=True)
        lib.compact_incomplete(sym, append=False, convert_int_to_float=True)
        # convert_int_to_float is applied only to integer dtypes and it always converts the dtype to np.float64
        # If the dtype is np.float32 it should not be changed
        expected_dtype = np.float32 if dtype == np.float32 else np.float64
        expected = pd.DataFrame({"a": np.arange(1, 7, dtype=expected_dtype)}, index=pd.date_range(pd.Timestamp(0), periods=6, freq="ns"))
        assert_frame_equal(expected, lib.read(sym).data, check_dtype=True)

    @pytest.mark.parametrize("dtype1", [np.int32, np.uint16, np.int8, np.int64, np.uint64, np.float64, np.float32])
    @pytest.mark.parametrize("dtype2", [np.int32, np.uint16, np.int8, np.int64, np.uint64, np.float64, np.float32])
    @pytest.mark.parametrize("append", [True, False])
    def test_write_convert_different_types_dynamic_schema(self, lmdb_version_store_dynamic_schema_v1, dtype1, dtype2, append):
        lib = lmdb_version_store_dynamic_schema_v1
        sym = "sym"
        df1 = pd.DataFrame({"a": np.array([1, 2, 3], dtype=dtype1)}, index=pd.date_range(pd.Timestamp(0), periods=3, freq="ns"))
        df2 = pd.DataFrame({"a": np.array([4, 5, 6], dtype=dtype2)}, index=pd.date_range(pd.Timestamp(3), periods=3, freq="ns"))
        lib.write(sym, df1, parallel=True)
        lib.write(sym, df2, parallel=True)
        lib.compact_incomplete(sym, append=append, convert_int_to_float=True)
        # Convert int to float should not affect columns of float32 type. However, if one column is of float32 type and
        # the other is of integer type, the integer column will be promoted to float64 and then the float32 will
        # be promoted to float64.
        expected_dtype = np.float32 if dtype1 == dtype2 == np.float32 else np.float64
        expected = pd.DataFrame({"a": np.arange(1, 7, dtype=expected_dtype)}, index=pd.date_range(pd.Timestamp(0), periods=6, freq="ns"))
        assert_frame_equal(expected, lib.read(sym).data, check_dtype=True)

    @pytest.mark.parametrize("dtype1", [np.int32, np.uint16, np.int8, np.int64, np.uint64, np.float64, np.float32])
    @pytest.mark.parametrize("dtype2", [np.int32, np.uint16, np.int8, np.int64, np.uint64, np.float64, np.float32])
    @pytest.mark.parametrize("append", [True, False])
    def test_write_convert_different_types_static_schema(self, lmdb_version_store_v1, dtype1, dtype2, append):
        lib = lmdb_version_store_v1
        sym = "sym"
        df1 = pd.DataFrame({"a": np.array([1, 2, 3], dtype=dtype1)}, index=pd.date_range(pd.Timestamp(0), periods=3, freq="ns"))
        df2 = pd.DataFrame({"a": np.array([4, 5, 6], dtype=dtype2)}, index=pd.date_range(pd.Timestamp(3), periods=3, freq="ns"))
        lib.write(sym, df1, parallel=True)
        lib.write(sym, df2, parallel=True)
        # Convert int to float should not affect columns of float32 type. However, if one column is of float32 type and
        # the other is of integer type, the integer column will be promoted to float64 and there will be conflicting
        # types
        exactly_one_float = (dtype1 == np.float32) ^ (dtype2 == np.float32)
        if exactly_one_float:
            with pytest.raises(SchemaException) as exception_info:
                lib.compact_incomplete(sym, append=append, convert_int_to_float=True)
                assert "FLOAT32" in str(exception_info.value)
                assert "FLOAT64" in str(exception_info.value)
        else:
            lib.compact_incomplete(sym, append=append, convert_int_to_float=True)
            expected_dtype = np.float32 if dtype1 == dtype2 == np.float32 else np.float64
            expected = pd.DataFrame({"a": np.arange(1, 7, dtype=expected_dtype)}, index=pd.date_range(pd.Timestamp(0), periods=6, freq="ns"))
            assert_frame_equal(expected, lib.read(sym).data, check_dtype=True)

    @pytest.mark.parametrize("dtype2", [np.int32, np.uint16, np.int8, np.int64, np.uint64, np.float64])
    @pytest.mark.parametrize("version_store", ["lmdb_version_store_v1", "lmdb_version_store_dynamic_schema_v1"])
    def test_append_to_existing_convert_different_types(self, version_store, dtype2, request):
        lib = request.getfixturevalue(version_store)
        sym = "sym"
        df1 = pd.DataFrame({"a": np.array([1, 2, 3], dtype=np.float64)}, index=pd.date_range(pd.Timestamp(0), periods=3, freq="ns"))
        df2 = pd.DataFrame({"a": np.array([4, 5, 6], dtype=dtype2)}, index=pd.date_range(pd.Timestamp(3), periods=3, freq="ns"))
        lib.write(sym, df1)
        lib.write(sym, df2, parallel=True)
        lib.compact_incomplete(sym, append=True, convert_int_to_float=True)
        expected = pd.DataFrame({"a": np.arange(1, 7, dtype=np.double)}, index=pd.date_range(pd.Timestamp(0), periods=6, freq="ns"))
        assert_frame_equal(expected, lib.read(sym).data, check_dtype=True)
    @pytest.mark.parametrize("dtype", [np.int32, np.uint16, np.int8, np.int64, np.uint64, np.float64])
    def test_float32_is_not_converted_write(self, lmdb_version_store_v1, dtype):
        lib = lmdb_version_store_v1
        sym = "sym"
        df1 = pd.DataFrame({"a": np.array([1, 2, 3], dtype=dtype)}, index=pd.date_range(pd.Timestamp(0), periods=3, freq="ns"))
        df2 = pd.DataFrame({"a": np.array([4, 5, 6], dtype=np.float32)}, index=pd.date_range(pd.Timestamp(3), periods=3, freq="ns"))
        lib.write(sym, df1, parallel=True)
        lib.write(sym, df2, parallel=True)
        with pytest.raises(SchemaException) as exception_info:
            lib.compact_incomplete(sym, append=False, convert_int_to_float=True)
        assert "FLOAT32" in str(exception_info.value)
        assert "FLOAT64" in str(exception_info.value)

    def test_float32_is_not_converted_append(self, lmdb_version_store_v1):
        lib = lmdb_version_store_v1
        sym = "sym"
        df1 = pd.DataFrame({"a": np.array([1, 2, 3], dtype=np.double)}, index=pd.date_range(pd.Timestamp(0), periods=3, freq="ns"))
        df2 = pd.DataFrame({"a": np.array([4, 5, 6], dtype=np.float32)}, index=pd.date_range(pd.Timestamp(3), periods=3, freq="ns"))
        lib.write(sym, df1)
        lib.write(sym, df2, parallel=True)
        with pytest.raises(SchemaException) as exception_info:
            lib.compact_incomplete(sym, append=True, convert_int_to_float=True)
        assert "FLOAT32" in str(exception_info.value)
        assert "FLOAT64" in str(exception_info.value)

    @pytest.mark.parametrize("data_dtype", [("string_value", object), (pd.Timestamp(0), "datetime64[ns]"), (3.14, np.float32)])
    @pytest.mark.parametrize("append", [True, False])
    def test_non_int_columns_are_not_affected(self, lmdb_version_store_v1, data_dtype, append):
        lib = lmdb_version_store_v1
        sym = "sym"
        df1 = pd.DataFrame({"a": np.array([data_dtype[0]], dtype=data_dtype[1])}, index=pd.DatetimeIndex([pd.Timestamp(0)]))
        lib.write(sym, df1, parallel=True)
        lib.compact_incomplete(sym, append=append, convert_int_to_float=True)
        assert_frame_equal(lib.read(sym).data, df1, check_dtype=True)

    @pytest.mark.parametrize("dtype", [np.int32, np.uint16, np.int8, np.int64, np.uint64, np.float64])
    @pytest.mark.parametrize("version_store", ["lmdb_version_store_v1", "lmdb_version_store_dynamic_schema_v1"])
    @pytest.mark.parametrize("append", [True, False])
    def test_single_segment(self, version_store, dtype, request, append):
        lib = request.getfixturevalue(version_store)
        sym = "sym"
        df1 = pd.DataFrame({"a": np.array([1, 2, 3], dtype=dtype)}, index=pd.date_range(pd.Timestamp(0), periods=3, freq="ns"))
        lib.write(sym, df1, parallel=True)
        lib.compact_incomplete(sym, append=append, convert_int_to_float=True)
        expected = pd.DataFrame({"a": np.array([1, 2, 3], dtype=np.float64)}, index=pd.date_range(pd.Timestamp(0), periods=3, freq="ns"))
        assert_frame_equal(lib.read(sym).data, expected, check_dtype=True)

class TestEmptyDataFrames:
    """
    Tests the behavior of appending with compact incomplete when the dataframe on disk is an empty dataframe. It should
    behave the same way is if there is data. Static schema must check index and column names and types, dynamic schema
    should allow appending with differing names and types which are promotable. Index names must match regardless of
    schema type.

    Note with introduction of empty index and empty types (feature flagged at the moment) the tests might have to be
    changed. Refer to TestEmptyIndexPreservesIndexNames class comment in python/tests/unit/arcticdb/version_store/test_empty_writes.py
    """
    def test_append_to_empty(self, lmdb_version_store_v1):
        lib = lmdb_version_store_v1
        symbol = "symbol"
        lib.write(symbol, pd.DataFrame({"a": np.array([], np.int64)}, index=pd.DatetimeIndex([])))
        df = pd.DataFrame({"a": [1]}, index=pd.DatetimeIndex([pd.Timestamp(0)]))
        lib.write(symbol, df, parallel=True)
        lib.compact_incomplete(symbol, append=True, convert_int_to_float=False)
        assert_frame_equal(lib.read(symbol).data, df)

    @pytest.mark.parametrize("version_store", ["lmdb_version_store_v1", "lmdb_version_store_dynamic_schema_v1"])
    def test_appending_to_empty_with_differing_index_name_fails(self, version_store, request):
        lib = request.getfixturevalue(version_store)
        symbol = "symbol"
        empty = pd.DataFrame({"a": np.array([], np.int64)}, index=pd.DatetimeIndex([], name="my_initial_index"))
        lib.write(symbol, empty)
        df = pd.DataFrame({"a": [1]}, index=pd.DatetimeIndex([pd.Timestamp(0)], name="my_new_index"))
        lib.write(symbol, df, parallel=True)
        with pytest.raises(SchemaException) as exception_info:
            lib.compact_incomplete(symbol, append=True, convert_int_to_float=False)
        assert "index" in str(exception_info.value)
        assert "my_initial_index" in str(exception_info.value)
        assert "my_new_index" in str(exception_info.value)

    @pytest.mark.parametrize(
        "to_append",
        [
            pd.DataFrame({"wrong_col": [1]}, pd.DatetimeIndex([pd.Timestamp(0)])),
            pd.DataFrame({"a": [1], "wrong_col": [2]}, pd.DatetimeIndex([pd.Timestamp(0)]))
        ]
    )
    def test_appending_to_empty_with_differing_columns_fails(self, lmdb_version_store_v1, to_append):
        lib = lmdb_version_store_v1
        symbol = "symbol"
        empty = pd.DataFrame({"a": np.array([], np.int64)}, index=pd.DatetimeIndex([]))
        lib.write(symbol, empty)
        lib.write(symbol, to_append, parallel=True)
        with pytest.raises(SchemaException, match="wrong_col"):
            lib.compact_incomplete(symbol, append=True, convert_int_to_float=False)
