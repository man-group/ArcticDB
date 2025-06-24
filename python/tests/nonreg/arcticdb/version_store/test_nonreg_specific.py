"""
Copyright 2023 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""
import numpy as np
import pandas as pd
import datetime
import pytest
import sys

from arcticdb import QueryBuilder
from arcticdb.exceptions import UserInputException
from arcticdb.util.test import assert_frame_equal, assert_series_equal
from arcticdb.util.utils import delete_nvs
from arcticdb.version_store.library import Library
from arcticdb_ext import set_config_int
from arcticdb_ext.storage import KeyType
from arcticc.pb2.descriptors_pb2 import TypeDescriptor
from tests.util.date import DateRange


@pytest.mark.storage
def test_read_keys(object_and_mem_and_lmdb_version_store_dynamic_schema):
    lib = object_and_mem_and_lmdb_version_store_dynamic_schema
    symbol = "test_update_float_int"
    data1 = pd.DataFrame({"a": [np.float64(1.0)]}, index=[datetime.datetime(2019, 4, 9, 10, 5, 2, 1)])
    data2 = pd.DataFrame({"a": [np.int64(2)]}, index=[datetime.datetime(2019, 4, 8, 10, 5, 2, 1)])
    expected = pd.concat((data1, data2))
    expected.sort_index(inplace=True)

    lib.write(symbol, data1)
    lib.update(symbol, data2, dynamic_schema=True)
    result = lib.read(symbol, dynamic_schema=True).data
    result.sort_index(inplace=True)

    assert_frame_equal(expected, result)


@pytest.mark.storage
def test_update_int_float(object_and_mem_and_lmdb_version_store_dynamic_schema):
    lib = object_and_mem_and_lmdb_version_store_dynamic_schema
    symbol = "test_update_int_float"
    data1 = pd.DataFrame({"a": [np.int64(2)]}, index=[datetime.datetime(2019, 4, 9, 10, 5, 2, 1)])
    data2 = pd.DataFrame({"a": [np.float64(1.0)]}, index=[datetime.datetime(2019, 4, 8, 10, 5, 2, 1)])
    expected = pd.concat((data1, data2))
    expected.sort_index(inplace=True)

    lib.write(symbol, data1)
    lib.update(symbol, data2, dynamic_schema=True)
    result = lib.read(symbol, dynamic_schema=True).data
    result.sort_index(inplace=True)

    assert_frame_equal(expected, result)


@pytest.mark.storage
def test_update_nan_int(object_and_mem_and_lmdb_version_store_dynamic_schema):
    lib = object_and_mem_and_lmdb_version_store_dynamic_schema
    symbol = "test_update_nan_int"
    data1 = pd.DataFrame({"a": [np.nan]}, index=[datetime.datetime(2019, 4, 9, 10, 5, 2, 1)])
    data2 = pd.DataFrame({"a": [np.int64(2)]}, index=[datetime.datetime(2019, 4, 8, 10, 5, 2, 1)])
    expected = pd.concat((data1, data2))
    expected.sort_index(inplace=True)

    lib.write(symbol, data1)
    lib.update(symbol, data2, dynamic_schema=True)
    result = lib.read(symbol, dynamic_schema=True).data
    result.sort_index(inplace=True)

    assert_frame_equal(expected, result)


@pytest.mark.storage
def test_update_int_nan(object_and_mem_and_lmdb_version_store_dynamic_schema):
    lib = object_and_mem_and_lmdb_version_store_dynamic_schema
    symbol = "test_update_int_nan"
    data1 = pd.DataFrame({"a": [np.int64(2)]}, index=[datetime.datetime(2019, 4, 9, 10, 5, 2, 1)])
    data2 = pd.DataFrame({"a": [np.nan]}, index=[datetime.datetime(2019, 4, 8, 10, 5, 2, 1)])
    expected = pd.concat((data1, data2))
    expected.sort_index(inplace=True)

    lib.write(symbol, data1)
    lib.update(symbol, data2, dynamic_schema=True)
    result = lib.read(symbol, dynamic_schema=True).data
    result.sort_index(inplace=True)

    assert_frame_equal(expected, result)


@pytest.mark.skipif(sys.platform == "win32", reason="SKIP_WIN Only dynamic strings are supported on Windows")
@pytest.mark.storage
def test_append_dynamic_to_fixed_width_strings(object_and_mem_and_lmdb_version_store_dynamic_schema):
    lib = object_and_mem_and_lmdb_version_store_dynamic_schema
    symbol = "test_append_dynamic_to_fixed_width_strings"

    fixed_width_strings_index = pd.date_range("2000-1-1", periods=3)
    fixed_width_strings_data = pd.DataFrame({"a": ["hello", "bonjour", "gutentag"]}, index=fixed_width_strings_index)
    lib.write(symbol, fixed_width_strings_data, dynamic_strings=False)

    info = lib.get_info(symbol)
    assert TypeDescriptor.ValueType.Name(info["dtype"][0].value_type) == "UTF8_STRING"

    dynamic_strings_index = pd.date_range("2000-1-4", periods=3)
    dynamic_strings_data = pd.DataFrame({"a": ["nihao", "konichiwa", "annyeonghaseyo"]}, index=dynamic_strings_index)
    lib.append(symbol, dynamic_strings_data, dynamic_strings=True)

    info = lib.get_info(symbol)
    assert TypeDescriptor.ValueType.Name(info["dtype"][0].value_type) == "DYNAMIC_STRING"

    expected_df = pd.concat((fixed_width_strings_data, dynamic_strings_data))
    read_df = lib.read(symbol).data
    assert_frame_equal(expected_df, read_df)


@pytest.mark.storage
def test_append_fixed_width_to_dynamic_strings(object_and_mem_and_lmdb_version_store_dynamic_schema):
    lib = object_and_mem_and_lmdb_version_store_dynamic_schema
    symbol = "test_append_fixed_width_to_dynamic_strings"

    dynamic_strings_index = pd.date_range("2000-1-1", periods=3)
    dynamic_strings_data = pd.DataFrame({"a": ["hello", "bonjour", "gutentag"]}, index=dynamic_strings_index)
    lib.write(symbol, dynamic_strings_data, dynamic_strings=True)

    info = lib.get_info(symbol)
    assert TypeDescriptor.ValueType.Name(info["dtype"][0].value_type) == "DYNAMIC_STRING"

    fixed_width_strings_index = pd.date_range("2000-1-4", periods=3)
    fixed_width_strings_data = pd.DataFrame(
        {"a": ["nihao", "konichiwa", "annyeonghaseyo"]}, index=fixed_width_strings_index
    )
    lib.append(symbol, fixed_width_strings_data, dynamic_strings=False)

    info = lib.get_info(symbol)
    assert TypeDescriptor.ValueType.Name(info["dtype"][0].value_type) == "DYNAMIC_STRING"

    expected_df = pd.concat((dynamic_strings_data, fixed_width_strings_data))
    read_df = lib.read(symbol).data
    assert_frame_equal(expected_df, read_df)


@pytest.mark.skipif(sys.platform == "win32", reason="SKIP_WIN Only dynamic strings are supported on Windows")
@pytest.mark.storage
def test_update_dynamic_to_fixed_width_strings(object_and_mem_and_lmdb_version_store_dynamic_schema):
    lib = object_and_mem_and_lmdb_version_store_dynamic_schema
    symbol = "test_update_dynamic_to_fixed_width_strings"

    fixed_width_strings_index = pd.date_range("2000-1-1", periods=3)
    fixed_width_strings_data = pd.DataFrame({"a": ["hello", "bonjour", "gutentag"]}, index=fixed_width_strings_index)
    lib.write(symbol, fixed_width_strings_data, dynamic_strings=False)

    info = lib.get_info(symbol)
    assert TypeDescriptor.ValueType.Name(info["dtype"][0].value_type) == "UTF8_STRING"

    dynamic_strings_index = pd.date_range("2000-1-2", periods=1)
    dynamic_strings_data = pd.DataFrame({"a": ["annyeonghaseyo"]}, index=dynamic_strings_index)
    lib.update(symbol, dynamic_strings_data, dynamic_strings=True)

    info = lib.get_info(symbol)
    assert TypeDescriptor.ValueType.Name(info["dtype"][0].value_type) == "DYNAMIC_STRING"

    fixed_width_strings_data.update(dynamic_strings_data)
    expected_df = fixed_width_strings_data
    read_df = lib.read(symbol).data
    assert_frame_equal(expected_df, read_df)


@pytest.mark.storage
def test_update_fixed_width_to_dynamic_strings(object_and_mem_and_lmdb_version_store_dynamic_schema):
    lib = object_and_mem_and_lmdb_version_store_dynamic_schema
    symbol = "test_update_fixed_width_to_dynamic_strings"

    dynamic_strings_index = pd.date_range("2000-1-1", periods=3)
    dynamic_strings_data = pd.DataFrame({"a": ["hello", "bonjour", "gutentag"]}, index=dynamic_strings_index)
    lib.write(symbol, dynamic_strings_data, dynamic_strings=True)

    fixed_width_strings_index = pd.date_range("2000-1-2", periods=1)
    fixed_width_strings_data = pd.DataFrame({"a": ["annyeonghaseyo"]}, index=fixed_width_strings_index)
    lib.update(symbol, fixed_width_strings_data, dynamic_strings=False)

    dynamic_strings_data.update(fixed_width_strings_data)
    expected_df = dynamic_strings_data
    read_df = lib.read(symbol).data
    assert_frame_equal(expected_df, read_df)


# https://github.com/man-group/ArcticDB/issues/767
# Batch write and append call aggregator_set_data from threads running in the CPU threadpool (i.e. not the main thread)
# With unicode strings, a PyObject allocation is needed, and therefore the thread must be holding the GIL
# This test ensures that the correct thread is holding the GIL when performing these allocations, and that there is no
# deadlock.
# This is not an issue with non-batch methods as aggregator_set_data is called from the main thread, which pybind11
# ensures is holding the GIL on entry to the C++ layer.
def test_batch_write_unicode_strings(lmdb_version_store):
    lib = lmdb_version_store
    syms = ["sym1", "sym2"]
    # 10 was too small to trigger problem
    num_rows = 100

    index = np.arange(num_rows)
    u_umlaut = b"\xc3\x9c".decode("utf-8")
    unicode_vals = [u_umlaut] * num_rows

    data = [
        pd.Series(data=unicode_vals, index=index),
        pd.Series(data=unicode_vals, index=index),
    ]

    # The problem was not always triggered on the first call
    for _ in range(5):
        lib.batch_write(syms, data)
        lib.batch_append(syms, data)


@pytest.mark.parametrize("PandasType, assert_pandas_container_equal", [
    (pd.Series, assert_series_equal),
    (pd.DataFrame, assert_frame_equal),
])
def test_update_with_empty_series_or_dataframe(lmdb_version_store_empty_types_v1, PandasType, assert_pandas_container_equal):
    # Non-regression test for https://github.com/man-group/ArcticDB/issues/892
    lib = lmdb_version_store_empty_types_v1

    kwargs = { "name": "a" } if PandasType == pd.Series else { "columns": ["a"] }
    data = np.array([1.0]) if PandasType == pd.Series else np.array([[1.0]])

    empty = PandasType(data=[], dtype=float, index=pd.DatetimeIndex([]), **kwargs)
    one_row = PandasType(
        data=data,
        dtype=float,
        index=pd.DatetimeIndex([
            datetime.datetime(2019, 4, 9, 10, 5, 2, 1)
        ]),
        **kwargs,
    )

    symbol = "test_update_with_empty_series_or_dataframe_first"

    first_operation = lib.write(symbol, empty)

    assert first_operation.version == 0

    # Has no effect, but must not fail.
    second_operation = lib.append(symbol, empty)

    # No new version is created.
    assert second_operation.version == first_operation.version

    third_operation = lib.update(symbol, one_row)

    # A new version is created in this case.
    assert third_operation.version == second_operation.version + 1

    received = lib.read(symbol).data
    assert_pandas_container_equal(one_row, received)

    symbol = "test_update_with_empty_series_or_dataframe_second"

    first_operation = lib.write(symbol, one_row)

    # Has no effect, but must not fail.
    second_operation = lib.append(symbol, empty)

    # No new version is created.
    assert first_operation.version == second_operation.version

    # Has no effect, but must not fail.
    third_operation = lib.update(symbol, empty)

    # No new version is created as well.
    assert third_operation.version == first_operation.version

    received = lib.read(symbol).data
    assert_pandas_container_equal(one_row, received)


def test_update_with_empty_dataframe_with_index(lmdb_version_store):
    # Non-regression test for https://github.com/man-group/ArcticDB/issues/940
    lib = lmdb_version_store

    symbol = "test_update_with_empty_dataframe_with_index"

    series = pd.Series(dtype="datetime64[ns]")
    lib.write(symbol, series)

    # This must not fail.
    lib.read(symbol, as_of=0).data


def test_date_range_multi_index(lmdb_version_store):
    # Non-regression test for https://github.com/man-group/ArcticDB/issues/1122
    lib = lmdb_version_store
    sym = "test_date_range_multi_index"

    existing_df = pd.DataFrame(
        {"col": [1, 2, 3]},
        index=pd.MultiIndex.from_arrays(
            [pd.date_range("2023-11-28", "2023-11-30", freq="D"), ["a", "b", "c"]], names=["dt_level", "str_level"]
        ),
    )
    lib.write(sym, existing_df)

    expected_df = pd.DataFrame(
        {"col": pd.Series([], dtype=np.int64)},
        index=pd.MultiIndex.from_arrays([pd.DatetimeIndex([]), []], names=["dt_level", "str_level"]),
    )
    result_df = lib.read(
        sym, date_range=DateRange(pd.Timestamp("2099-01-01"), pd.Timestamp("2099-01-02"))
    ).data
    assert_frame_equal(result_df, expected_df)


@pytest.mark.parametrize(
    "method",
    ("write", "append", "update", "write_metadata", "batch_write", "batch_append", "batch_write_metadata")
)
@pytest.mark.parametrize("lib_config", (True, False))
@pytest.mark.parametrize("env_var", (True, False))
@pytest.mark.parametrize("arg", (True, False, None))
def test_prune_previous_general(version_store_factory, monkeypatch, method, lib_config, env_var, arg):
    lib = version_store_factory(prune_previous_version=lib_config, use_tombstones=True)

    try:
        should_be_pruned = lib_config
        if env_var:
            monkeypatch.setenv("PRUNE_PREVIOUS_VERSION", "true")
            should_be_pruned = True
        if arg is not None:
            should_be_pruned = arg

        lt = lib.library_tool()
        sym = f"test_prune_previous_general"
        df_0 = pd.DataFrame({"col": np.arange(10)}, index=pd.date_range("2024-01-01", periods=10))
        lib.write(sym, df_0)

        df_1 = pd.DataFrame({"col": np.arange(10)}, index=pd.date_range("2024-01-11", periods=10))
        arg_0 = [sym] if method.startswith("batch") else sym
        arg_1 = [df_1] if method.startswith("batch") else df_1

        getattr(lib, method)(arg_0, arg_1, prune_previous_version=arg)

        assert len(lt.find_keys(KeyType.TABLE_INDEX)) == 1 if should_be_pruned else 2
    finally:
        delete_nvs(lib)

@pytest.mark.parametrize("append", (True, False))
@pytest.mark.parametrize("lib_config", (True, False))
@pytest.mark.parametrize("env_var", (True, False))
@pytest.mark.parametrize("arg", (True, False, None))
def test_prune_previous_compact_incomplete(version_store_factory, monkeypatch, append, lib_config, env_var, arg):
    lib = version_store_factory(prune_previous_version=lib_config, use_tombstones=True)

    try:
        should_be_pruned = lib_config
        if env_var:
            monkeypatch.setenv("PRUNE_PREVIOUS_VERSION", "true")
            should_be_pruned = True
        if arg is not None:
            should_be_pruned = arg

        lt = lib.library_tool()
        sym = f"test_prune_previous_compact_incomplete"
        df_0 = pd.DataFrame({"col": np.arange(10)}, index=pd.date_range("2024-01-01", periods=10))
        lib.write(sym, df_0)

        df_1 = pd.DataFrame({"col": np.arange(10)}, index=pd.date_range("2024-01-11", periods=10))
        lib.write(sym, df_1, parallel=True)

        lib.compact_incomplete(sym, append, False, prune_previous_version=arg)

        assert len(lt.find_keys(KeyType.TABLE_INDEX)) == 1 if should_be_pruned else 2
    finally:
        delete_nvs(lib)


@pytest.mark.parametrize("lib_config", (True, False))
@pytest.mark.parametrize("env_var", (True, False))
@pytest.mark.parametrize("arg", (True, False, None))
def test_prune_previous_delete_date_range(version_store_factory, monkeypatch, lib_config, env_var, arg):
    lib = version_store_factory(prune_previous_version=lib_config, use_tombstones=True)

    try:
        should_be_pruned = lib_config
        if env_var:
            monkeypatch.setenv("PRUNE_PREVIOUS_VERSION", "true")
            should_be_pruned = True
        if arg is not None:
            should_be_pruned = arg

        lt = lib.library_tool()
        sym = f"test_prune_previous_delete_date_range"
        df_0 = pd.DataFrame({"col": np.arange(10)}, index=pd.date_range("2024-01-01", periods=10))
        lib.write(sym, df_0)

        lib.delete(sym, (pd.Timestamp("2024-01-05"), pd.Timestamp("2024-01-07")), prune_previous_version=arg)

        assert len(lt.find_keys(KeyType.TABLE_INDEX)) == 1 if should_be_pruned else 2
    finally:
        delete_nvs(lib)


@pytest.mark.parametrize("lib_config", (True, False))
@pytest.mark.parametrize("env_var", (True, False))
@pytest.mark.parametrize("arg", (True, False, None))
def test_prune_previous_defragment_symbol_data(version_store_factory, monkeypatch, lib_config, env_var, arg):
    lib = version_store_factory(prune_previous_version=lib_config, use_tombstones=True)

    try:
        should_be_pruned = lib_config
        if env_var:
            monkeypatch.setenv("PRUNE_PREVIOUS_VERSION", "true")
            should_be_pruned = True
        if arg is not None:
            should_be_pruned = arg

        lt = lib.library_tool()
        sym = f"test_prune_previous_defragment_symbol_data"
        df_0 = pd.DataFrame({"col": np.arange(10)}, index=pd.date_range("2024-01-01", periods=10))
        lib.write(sym, df_0)
        df_1 = pd.DataFrame({"col": np.arange(10)}, index=pd.date_range("2024-01-11", periods=10))
        lib.append(sym, df_1, prune_previous_version=arg)

        assert len(lt.find_keys(KeyType.TABLE_INDEX)) == 1 if should_be_pruned else 2

        set_config_int("SymbolDataCompact.SegmentCount", 1)
        lib.defragment_symbol_data(sym, prune_previous_version=arg)

        assert len(lt.find_keys(KeyType.TABLE_INDEX)) == 1 if should_be_pruned else 3
    finally:
        delete_nvs(lib)


@pytest.mark.parametrize("index_start", range(9))
def test_update_index_overlap_corner_cases(lmdb_version_store_tiny_segment, index_start):
    lib = lmdb_version_store_tiny_segment
    sym = "test_update_index_overlap_corner_cases"

    index = [pd.Timestamp(index_start), pd.Timestamp(index_start + 1)]

    # Gap of 2 nanoseconds so we can insert inbetween the 2 tiny segments
    initial_df = pd.DataFrame({"col": [1, 2, 3, 4]}, index=[pd.Timestamp(2), pd.Timestamp(3), pd.Timestamp(6), pd.Timestamp(7)])
    update_df = pd.DataFrame({"col": [100, 200]}, index=index)
    lib.write(sym, initial_df)
    lib.update(sym, update_df)

    # TODO: Use dataframe_arctic_update once #1951 is merged
    chunks = []
    chunks.append(initial_df[initial_df.index < index[0]])
    chunks.append(update_df)
    chunks.append(initial_df[initial_df.index > index[1]])
    expected_df = pd.concat(chunks)
    received_df = lib.read(sym).data
    assert_frame_equal(expected_df, received_df)


def test_delete_snapshot_regression(nfs_clean_bucket):
    lib = nfs_clean_bucket.create_version_store_factory("test_delete_snapshot_regression")()
    try:
        lib.write("sym", 1)
        lib.snapshot("snap")
        assert "snap" in lib.list_snapshots()
        lib.delete_snapshot("snap")
        assert "snap" not in lib.list_snapshots()
    finally:
        delete_nvs(lib)


def test_resampling_non_timeseries(lmdb_version_store_v1):
    lib = lmdb_version_store_v1
    sym = "test_resampling_non_timeseries"

    df = pd.DataFrame({"col": np.arange(10)})
    lib.write(sym, df)
    q = QueryBuilder().resample('1min').agg({"col": "sum"})
    with pytest.raises(UserInputException):
        lib.read(sym, query_builder=q)
    q = QueryBuilder().date_range((pd.Timestamp("2025-01-01"), pd.Timestamp("2025-02-01"))).resample('1min').agg({"col": "sum"})
    with pytest.raises(UserInputException) as e:
        lib.read(sym, query_builder=q)
    assert "std::length_error(vector::reserve)" not in str(e.value)


@pytest.mark.parametrize("date_range", [None, (pd.Timestamp(4), pd.Timestamp(17))])
def test_update_data_key_timestamps(lmdb_version_store_v1, date_range):
    lib = lmdb_version_store_v1
    sym = "test_resampling_non_timeseries"
    initial_df = pd.DataFrame({"col": [0, 1, 2]}, index=[pd.Timestamp(0), pd.Timestamp(10), pd.Timestamp(20)])
    lib.write(sym, initial_df)
    update_df = pd.DataFrame({"col": [3, 4]}, index=[pd.Timestamp(5), pd.Timestamp(15)])
    lib.update(sym, update_df, date_range=date_range)
    expected_df = pd.DataFrame(
        {"col": [0, 3, 4, 2]},
        index=[pd.Timestamp(0), pd.Timestamp(5), pd.Timestamp(15), pd.Timestamp(20)],
    )
    assert_frame_equal(expected_df, lib.read(sym).data)
    index_df = lib.read_index(sym)
    assert (index_df.index.to_numpy() == np.array([0, 5, 20], dtype="datetime64[ns]")).all()
    assert (index_df["end_index"].to_numpy() == np.array([1, 16, 21], dtype="datetime64[ns]")).all()


def test_use_norm_failure_handler_known_types(lmdb_version_store_allows_pickling):
    # The change in PR https://github.com/man-group/ArcticDB/pull/2392 did not take into account that
    # self._nvs._normalizer.df could be not only DataFrameNormalizer, but also KnownTypeFallbackOnError if
    # use_norm_failure_handler_known_types (aka pickle_on_failure) was set to True. Skipping of df consolidation
    # is set in Library.__init__, hence why this test stresses the correct codepath
    nvs = lmdb_version_store_allows_pickling
    Library("dummy", nvs)
