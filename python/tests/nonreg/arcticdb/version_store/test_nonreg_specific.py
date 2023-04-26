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

from arcticdb.util.test import assert_frame_equal
from arcticc.pb2.descriptors_pb2 import TypeDescriptor


@pytest.mark.parametrize(
    "lib_type",
    [
        "lmdb_version_store_dynamic_schema_v1",
        "lmdb_version_store_dynamic_schema_v2",
        "s3_version_store_dynamic_schema_v1",
        "s3_version_store_dynamic_schema_v2",
    ],
)
def test_read_keys(lib_type, request):
    lib = request.getfixturevalue(lib_type)
    symbol = "test_update_float_int"
    data1 = pd.DataFrame({"a": [np.float64(1.0)]}, index=[datetime.datetime(2019, 4, 9, 10, 5, 2, 1)])
    data2 = pd.DataFrame({"a": [np.int64(2)]}, index=[datetime.datetime(2019, 4, 8, 10, 5, 2, 1)])
    expected = data1.append(data2)
    expected.sort_index(inplace=True)

    lib.write(symbol, data1)
    lib.update(symbol, data2, dynamic_schema=True)
    result = lib.read(symbol, dynamic_schema=True).data
    result.sort_index(inplace=True)

    assert_frame_equal(expected, result)


@pytest.mark.parametrize(
    "lib_type",
    [
        "lmdb_version_store_dynamic_schema_v1",
        "lmdb_version_store_dynamic_schema_v2",
        "s3_version_store_dynamic_schema_v1",
        "s3_version_store_dynamic_schema_v2",
    ],
)
def test_update_int_float(lib_type, request):
    lib = request.getfixturevalue(lib_type)
    symbol = "test_update_int_float"
    data1 = pd.DataFrame({"a": [np.int64(2)]}, index=[datetime.datetime(2019, 4, 9, 10, 5, 2, 1)])
    data2 = pd.DataFrame({"a": [np.float64(1.0)]}, index=[datetime.datetime(2019, 4, 8, 10, 5, 2, 1)])
    expected = data1.append(data2)
    expected.sort_index(inplace=True)

    lib.write(symbol, data1)
    lib.update(symbol, data2, dynamic_schema=True)
    result = lib.read(symbol, dynamic_schema=True).data
    result.sort_index(inplace=True)

    assert_frame_equal(expected, result)


@pytest.mark.parametrize(
    "lib_type",
    [
        "lmdb_version_store_dynamic_schema_v1",
        "lmdb_version_store_dynamic_schema_v2",
        "s3_version_store_dynamic_schema_v1",
        "s3_version_store_dynamic_schema_v2",
    ],
)
def test_update_nan_int(lib_type, request):
    lib = request.getfixturevalue(lib_type)
    symbol = "test_update_nan_int"
    data1 = pd.DataFrame({"a": [np.nan]}, index=[datetime.datetime(2019, 4, 9, 10, 5, 2, 1)])
    data2 = pd.DataFrame({"a": [np.int64(2)]}, index=[datetime.datetime(2019, 4, 8, 10, 5, 2, 1)])
    expected = data1.append(data2)
    expected.sort_index(inplace=True)

    lib.write(symbol, data1)
    lib.update(symbol, data2, dynamic_schema=True)
    result = lib.read(symbol, dynamic_schema=True).data
    result.sort_index(inplace=True)

    assert_frame_equal(expected, result)


@pytest.mark.parametrize(
    "lib_type",
    [
        "lmdb_version_store_dynamic_schema_v1",
        "lmdb_version_store_dynamic_schema_v2",
        "s3_version_store_dynamic_schema_v1",
        "s3_version_store_dynamic_schema_v2",
    ],
)
def test_update_int_nan(lib_type, request):
    lib = request.getfixturevalue(lib_type)
    symbol = "test_update_int_nan"
    data1 = pd.DataFrame({"a": [np.int64(2)]}, index=[datetime.datetime(2019, 4, 9, 10, 5, 2, 1)])
    data2 = pd.DataFrame({"a": [np.nan]}, index=[datetime.datetime(2019, 4, 8, 10, 5, 2, 1)])
    expected = data1.append(data2)
    expected.sort_index(inplace=True)

    lib.write(symbol, data1)
    lib.update(symbol, data2, dynamic_schema=True)
    result = lib.read(symbol, dynamic_schema=True).data
    result.sort_index(inplace=True)

    assert_frame_equal(expected, result)


@pytest.mark.skipif(sys.platform == "win32", reason="SKIP_WIN Only dynamic strings are supported on Windows")
@pytest.mark.parametrize(
    "lib_type",
    [
        "lmdb_version_store_dynamic_schema_v1",
        "lmdb_version_store_dynamic_schema_v2",
        "s3_version_store_dynamic_schema_v1",
        "s3_version_store_dynamic_schema_v2",
    ],
)
def test_append_dynamic_to_fixed_width_strings(lib_type, request):
    lib = request.getfixturevalue(lib_type)
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

    expected_df = fixed_width_strings_data.append(dynamic_strings_data)
    read_df = lib.read(symbol).data
    assert_frame_equal(expected_df, read_df)


@pytest.mark.parametrize(
    "lib_type",
    [
        "lmdb_version_store_dynamic_schema_v1",
        "lmdb_version_store_dynamic_schema_v2",
        "s3_version_store_dynamic_schema_v1",
        "s3_version_store_dynamic_schema_v2",
    ],
)
def test_append_fixed_width_to_dynamic_strings(lib_type, request):
    lib = request.getfixturevalue(lib_type)
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

    expected_df = dynamic_strings_data.append(fixed_width_strings_data)
    read_df = lib.read(symbol).data
    assert_frame_equal(expected_df, read_df)


@pytest.mark.skipif(sys.platform == "win32", reason="SKIP_WIN Only dynamic strings are supported on Windows")
@pytest.mark.parametrize(
    "lib_type",
    [
        "lmdb_version_store_dynamic_schema_v1",
        "lmdb_version_store_dynamic_schema_v2",
        "s3_version_store_dynamic_schema_v1",
        "s3_version_store_dynamic_schema_v2",
    ],
)
def test_update_dynamic_to_fixed_width_strings(lib_type, request):
    lib = request.getfixturevalue(lib_type)
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


@pytest.mark.parametrize(
    "lib_type",
    [
        "lmdb_version_store_dynamic_schema_v1",
        "lmdb_version_store_dynamic_schema_v2",
        "s3_version_store_dynamic_schema_v1",
        "s3_version_store_dynamic_schema_v2",
    ],
)
def test_update_fixed_width_to_dynamic_strings(lib_type, request):
    lib = request.getfixturevalue(lib_type)
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
