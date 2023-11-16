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

from arcticdb.util.test import assert_frame_equal, assert_series_equal
from arcticdb.util._versions import IS_PANDAS_TWO
from arcticc.pb2.descriptors_pb2 import TypeDescriptor


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
def test_update_with_empty_series_or_dataframe(lmdb_version_store, PandasType, assert_pandas_container_equal):
    # Non-regression test for https://github.com/man-group/ArcticDB/issues/892
    lib = lmdb_version_store

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


@pytest.mark.parametrize(
    "input_empty_col_dtype, output_empty_col_dtype, value_type, size_bits",
    [
        (np.uint8, np.uint8, TypeDescriptor.ValueType.UINT, TypeDescriptor.SizeBits.S8),
        (int, int, TypeDescriptor.ValueType.INT, TypeDescriptor.SizeBits.S64),
        ("datetime64[ns]", "datetime64[ns]", TypeDescriptor.ValueType.NANOSECONDS_UTC, TypeDescriptor.SizeBits.S64),

        # For rationale see: https://github.com/man-group/ArcticDB/pull/1049
        (float, float, TypeDescriptor.ValueType.FLOAT if IS_PANDAS_TWO else TypeDescriptor.ValueType.EMPTY, TypeDescriptor.SizeBits.S64),  # noqa: E501
        (object, object if IS_PANDAS_TWO else float, TypeDescriptor.ValueType.EMPTY, TypeDescriptor.SizeBits.S64),
])
def test_empty_column_handling(lmdb_version_store, input_empty_col_dtype, output_empty_col_dtype, value_type, size_bits):
    # Non-regression test for https://github.com/man-group/ArcticDB/issues/987
    lib = lmdb_version_store

    symbol_type_descriptor_series_index = 1 if IS_PANDAS_TWO else 0
    def get_symbol_type_descriptor(symbol):
        symbol_info = lib.get_info(symbol)
        return symbol_info["dtype"][symbol_type_descriptor_series_index]

    symbol = "empty"
    series = pd.Series([], dtype=input_empty_col_dtype)
    lib.write(symbol, series)
    symbol_type_info = get_symbol_type_descriptor(symbol)
    assert symbol_type_info.value_type == value_type
    assert symbol_type_info.size_bits == size_bits
    result = lib.read(symbol).data
    assert result.dtype == output_empty_col_dtype
