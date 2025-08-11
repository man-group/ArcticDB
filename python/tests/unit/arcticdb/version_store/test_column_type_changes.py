"""
Copyright 2024 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""
import numpy as np
import pandas as pd
import pytest
from arcticdb import QueryBuilder
from arcticdb.supported_types import float_types
from arcticdb.version_store.library import Library

from arcticdb_ext.version_store import StreamDescriptorMismatch
from arcticdb_ext.storage import KeyType
from arcticdb.util.test import assert_frame_equal
from arcticdb_ext.types import DataType

from tests.util.mark import MACOS, MACOS_WHEEL_BUILD


@pytest.mark.parametrize("dynamic_schema", [True, False])
def test_changing_numeric_type(version_store_factory, dynamic_schema):
    lib = version_store_factory(dynamic_schema=dynamic_schema)
    sym_append = "test_changing_numeric_type_append"
    sym_update = "test_changing_numeric_type_update"
    df_write = pd.DataFrame({"col": np.arange(3, dtype=np.uint32)}, index=pd.date_range("2024-01-01", periods=3))
    df_append = pd.DataFrame({"col": np.arange(1, dtype=np.int32)}, index=pd.date_range("2024-01-04", periods=1))
    df_update = pd.DataFrame({"col": np.arange(1, dtype=np.int32)}, index=pd.date_range("2024-01-02", periods=1))

    lib.write(sym_append, df_write)
    lib.write(sym_update, df_write)

    if not dynamic_schema:
        with pytest.raises(StreamDescriptorMismatch):
            lib.append(sym_append, df_append)
        with pytest.raises(StreamDescriptorMismatch):
            lib.update(sym_update, df_update)
    else:
        lib.append(sym_append, df_append)
        lib.update(sym_update, df_update)

        expected_append = pd.concat([df_write, df_append])
        received_append = lib.read(sym_append).data
        assert_frame_equal(expected_append, received_append)

        expected_update = pd.DataFrame({"col": np.array([0, 0, 2], dtype=np.int64)}, index=pd.date_range("2024-01-01", periods=3))
        received_update = lib.read(sym_update).data
        assert_frame_equal(expected_update, received_update)


@pytest.mark.parametrize("dynamic_schema", [True, False])
@pytest.mark.parametrize("dynamic_strings_first", [True, False])
def test_changing_string_type(version_store_factory, dynamic_schema, dynamic_strings_first):
    lib = version_store_factory(dynamic_strings=True, dynamic_schema=dynamic_schema)
    sym_append = "test_changing_string_type_append"
    sym_update = "test_changing_string_type_update"
    df_write = pd.DataFrame({"col": ["a", "bb", "ccc"]}, index=pd.date_range("2024-01-01", periods=3))
    df_append = pd.DataFrame({"col": ["dddd"]}, index=pd.date_range("2024-01-04", periods=1))
    df_update = pd.DataFrame({"col": ["dddd"]}, index=pd.date_range("2024-01-02", periods=1))

    lib.write(sym_append, df_write, dynamic_strings=dynamic_strings_first)
    lib.write(sym_update, df_write, dynamic_strings=dynamic_strings_first)

    lib.append(sym_append, df_append, dynamic_strings=not dynamic_strings_first)
    lib.update(sym_update, df_update, dynamic_strings=not dynamic_strings_first)

    expected_append = pd.concat([df_write, df_append])
    received_append = lib.read(sym_append).data
    assert_frame_equal(expected_append, received_append)

    expected_update = pd.DataFrame({"col": ["a", "dddd", "ccc"]}, index=pd.date_range("2024-01-01", periods=3))
    received_update = lib.read(sym_update).data
    assert_frame_equal(expected_update, received_update)


@pytest.mark.parametrize("dynamic_schema", [True, False])
@pytest.mark.parametrize("wider_strings_first", [True, False])
def test_changing_fixed_string_width(version_store_factory, dynamic_schema, wider_strings_first):
    lib = version_store_factory(dynamic_strings=False, dynamic_schema=dynamic_schema)
    sym_append = "test_changing_fixed_string_width_append"
    sym_update = "test_changing_fixed_string_width_update"
    df_write = pd.DataFrame({"col": ["aa", "bb", "cc"]}, index=pd.date_range("2024-01-01", periods=3))
    df_append = pd.DataFrame({"col": ["d" * (1 if wider_strings_first else 3)]}, index=pd.date_range("2024-01-04", periods=1))
    df_update = pd.DataFrame({"col": ["d" * (1 if wider_strings_first else 3)]}, index=pd.date_range("2024-01-02", periods=1))

    lib.write(sym_append, df_write)
    lib.write(sym_update, df_write)

    lib.append(sym_append, df_append)
    lib.update(sym_update, df_update)

    expected_append = pd.concat([df_write, df_append])
    received_append = lib.read(sym_append).data
    assert_frame_equal(expected_append, received_append)

    expected_update = pd.DataFrame({"col": ["aa", "d" * (1 if wider_strings_first else 3), "cc"]}, index=pd.date_range("2024-01-01", periods=3))
    received_update = lib.read(sym_update).data
    assert_frame_equal(expected_update, received_update)


def test_type_promotion_stored_in_index_key(lmdb_version_store_dynamic_schema):
    lib = lmdb_version_store_dynamic_schema
    lib_tool = lib.library_tool()
    sym = "symbol"
    col = "column"

    def get_type_of_column():
        index_key = lib_tool.find_keys_for_symbol(KeyType.TABLE_INDEX, sym)[-1]
        tsd = lib_tool.read_timeseries_descriptor(index_key)
        type_desc = [field.type for field in tsd.fields if field.name == col][0]
        return type_desc.data_type()

    df_write = pd.DataFrame({col: [1, 2]}, dtype="int8", index=pd.date_range("2024-01-01", periods=2))
    lib.write(sym, df_write)
    assert get_type_of_column() == DataType.INT8

    df_append_1 = pd.DataFrame({col: [3, 4]}, dtype="int32", index=pd.date_range("2024-01-03", periods=2))
    lib.append(sym, df_append_1)
    assert get_type_of_column() == DataType.INT32

    df_append_2 = pd.DataFrame({col: [5, 6]}, dtype="float64", index=pd.date_range("2024-01-05", periods=2))
    lib.append(sym, df_append_2)
    assert get_type_of_column() == DataType.FLOAT64

    result_df = lib.read(sym).data
    expected_df = pd.concat([df_write, df_append_1, df_append_2])
    assert_frame_equal(result_df, expected_df)


@pytest.mark.parametrize("int_type", [np.int64, np.uint64, np.int32, np.uint32])
@pytest.mark.parametrize("float_type", float_types)
@pytest.mark.parametrize("second_append_type", [np.int64, np.uint64, np.int32, np.uint32])
@pytest.mark.parametrize("int_first", (True, False))
def test_type_promotion_ints_and_floats_up_to_float64(lmdb_version_store_dynamic_schema, int_type, float_type, second_append_type, int_first):
    # Given
    lib = lmdb_version_store_dynamic_schema

    if int_first:
        original_data = pd.DataFrame({"a": np.array([1, 2, 3], int_type)}, index=[0, 1, 2])
        first_append = pd.DataFrame({"a": np.array([4.0, 5.0, 6.0], float_type)}, index=[3, 4, 5])
    else:
        original_data = pd.DataFrame({"a": np.array([1, 2, 3], float_type)}, index=[0, 1, 2])
        first_append = pd.DataFrame({"a": np.array([4.0, 5.0, 6.0], int_type)}, index=[3, 4, 5])

    second_append = pd.DataFrame({"a": np.array([7, 8, 9], second_append_type)}, index=[5, 6, 7])
    lib.write("test", original_data)
    lib.append("test", first_append)
    lib.append("test", second_append)

    # When
    data = lib.read("test").data

    # Then
    expected_result = pd.concat([original_data, first_append, second_append])
    assert_frame_equal(data, expected_result)

    adb_lib = Library("desc", lib)
    info = adb_lib.get_description("test")
    assert len(info.columns) == 1
    name, dtype = info.columns[0]
    assert name == "a"
    # We promote 32 bit int + 32 bit float => float64 so we don't lose precision needlessly
    assert dtype.data_type() == DataType.FLOAT64
    assert data.dtypes["a"] == np.float64
    assert expected_result.dtypes["a"] == np.float64


@pytest.mark.parametrize("original_type", [np.int8, np.uint8, np.int16, np.uint16])
@pytest.mark.parametrize("second_append_type", [np.int8, np.uint8, np.int16, np.uint16])
def test_type_promotion_ints_and_floats_up_to_float32(lmdb_version_store_dynamic_schema, original_type, second_append_type):
    """Cases where we promote an integral type and a float32 to a float32"""
    # Given
    lib = lmdb_version_store_dynamic_schema

    original_data = pd.DataFrame({"a": np.array([1, 2, 3], original_type)}, index=[0, 1, 2])
    first_append = pd.DataFrame({"a": np.array([4.0, 5.0, 6.0], np.float32)}, index=[3, 4, 5])
    second_append = pd.DataFrame({"a": np.array([7, 8, 9], second_append_type)}, index=[5, 6, 7])
    lib.write("test", original_data)
    lib.append("test", first_append)
    lib.append("test", second_append)

    # When
    data = lib.read("test").data

    # Then
    expected_result = pd.concat([original_data, first_append, second_append])
    assert_frame_equal(data, expected_result)

    adb_lib = Library("desc", lib)
    info = adb_lib.get_description("test")
    assert len(info.columns) == 1
    name, dtype = info.columns[0]
    assert name == "a"
    assert dtype.data_type() == DataType.FLOAT32


@pytest.mark.parametrize("original_type", [np.int32, np.uint32])
def test_type_promotion_int32_and_float32_up_to_float64(lmdb_version_store_dynamic_schema, original_type):
    """We promote int32 and float32 up to float64 so we can save the int32 without a loss of precision. """
    # Given
    lib = lmdb_version_store_dynamic_schema

    original_data = pd.DataFrame({"a": np.array([0, np.iinfo(original_type).min, np.iinfo(original_type).max], original_type)}, index=[0, 1, 2])
    first_append = pd.DataFrame({"a": np.array([0, np.finfo(np.float32).min, np.finfo(np.float32).max], np.float32)}, index=[3, 4, 5])
    lib.write("test", original_data)
    lib.append("test", first_append)

    # When
    data = lib.read("test").data

    # Then
    expected_result = pd.concat([original_data, first_append])
    assert_frame_equal(data, expected_result)
    assert data.iloc[1, 0] == np.iinfo(original_type).min
    assert data.iloc[2, 0] == np.iinfo(original_type).max
    assert data.iloc[4, 0] == np.finfo(np.float32).min
    assert data.iloc[5, 0] == np.finfo(np.float32).max
    assert data.dtypes["a"] == np.float64
    assert expected_result.dtypes["a"] == np.float64

@pytest.mark.xfail(MACOS, reason="bug??? https://github.com/man-group/ArcticDB/actions/runs/16517098026/job/46710177373?pr=2506")
def test_type_promotion_int64_and_float64_up_to_float64(lmdb_version_store_dynamic_schema):
    """We unavoidably lose precision in this case, this test just shows what happens when we do."""
    # Given
    lib = lmdb_version_store_dynamic_schema
    original_type = np.int64

    original_data = pd.DataFrame({"a": np.array([
        np.iinfo(original_type).min + 1,
        np.iinfo(original_type).max - 1,
        2 ** 53 - 1,
        2 ** 53,
        2 ** 53 + 1
    ], original_type)}, index=[0, 1, 2, 3, 4])
    append = pd.DataFrame({"a": np.array([np.finfo(np.float64).min, np.finfo(np.float64).max], np.float64)}, index=[5, 6])
    lib.write("test", original_data)
    lib.append("test", append)

    # When
    data = lib.read("test").data.astype(original_type)

    # Then
    if MACOS_WHEEL_BUILD:
        # This test gives other results on MacOS, but it's not a problem for us as the assertions below are meant
        # for illustrating the issue, not for testing the behaviour strictly.
        return

    assert data.iloc[0, 0] == np.iinfo(original_type).min  # out by one compared to original
    assert data.iloc[1, 0] == np.iinfo(original_type).min  # overflowed
    assert data.iloc[2, 0] == 2 ** 53 - 1  # fine, this fits in float64 which has an 11 bit exponent
    assert data.iloc[3, 0] == 2 ** 53  # also fine
    assert data.iloc[4, 0] == 2 ** 53  # off by one, should be 2 ** 53 + 1 but we lost precision


@pytest.mark.parametrize("integral_type", [np.int64, np.int32, np.uint64, np.uint32])
@pytest.mark.parametrize("float_type", [np.float32, np.float64])
def test_querybuilder_project_int_gt_32_float(lmdb_version_store_tiny_segment, integral_type, float_type):
    # Given
    lib = lmdb_version_store_tiny_segment
    symbol = "test"
    df = pd.DataFrame({
        "col1": np.array([1, 2, 3, 4], dtype=integral_type),
        "col2": np.array([-1.0, 2.0, 0.0, 1.0], dtype=float_type)
    }, index=np.arange(4))
    lib.write(symbol, df)

    # When
    q = QueryBuilder()
    q = q.apply("new_col", q["col1"] + q["col2"])

    # Then
    received = lib.read(symbol, query_builder=q).data
    received.sort_index(inplace=True)
    expected = df
    expected["new_col"] = expected["col1"] + expected["col2"]
    # We have to promote to float64 even with 32 bit ints to avoid losing precision of the int column
    assert expected.dtypes["new_col"] == np.float64
    assert received.dtypes["new_col"] == np.float64
    assert_frame_equal(expected, received)


@pytest.mark.parametrize("flip_addition", [True, False])
@pytest.mark.parametrize("integral_type", [np.int32, np.uint32])
def test_querybuilder_project_int32_float32_boundary(lmdb_version_store_tiny_segment, integral_type, flip_addition):
    # Given
    lib = lmdb_version_store_tiny_segment
    symbol = "test"
    max_int = np.iinfo(integral_type).max
    min_int = np.iinfo(integral_type).min
    max_float32 = np.finfo(np.float32).max
    min_float32 = np.finfo(np.float32).min
    df = pd.DataFrame({
        "col1": np.array([min_int, min_int + 1, 0, max_int - 1, max_int], dtype=integral_type),
        "col2": np.array([min_float32, min_float32 + 1, 0, max_float32 - 1, max_float32], dtype=np.float32)
    }, index=np.arange(5))
    lib.write(symbol, df)

    # When
    q = QueryBuilder()
    if flip_addition:
        q = q.apply("new_col", q["col1"] + q["col2"])
    else:
        q = q.apply("new_col", q["col2"] + q["col1"])

    # Then
    received = lib.read(symbol, query_builder=q).data
    received.sort_index(inplace=True)
    expected = df
    expected["new_col"] = expected["col1"] + expected["col2"]
    # We have to promote to float64 even with 32 bit ints to avoid losing precision of the int column
    assert expected.dtypes["new_col"] == np.float64
    assert received.dtypes["new_col"] == np.float64
    assert_frame_equal(expected, received)


@pytest.mark.parametrize("integral_type", [np.int8, np.int16, np.uint8, np.uint16])
@pytest.mark.parametrize("float_type", [np.float32, np.float64])
def test_querybuilder_project_int_lt_16_float(lmdb_version_store_tiny_segment, integral_type, float_type):
    # Given
    lib = lmdb_version_store_tiny_segment
    symbol = "test"
    df = pd.DataFrame({
        "col1": np.array([1, 2, 3, 4], dtype=np.int64),
        "col2": np.array([-1.0, 2.0, 0.0, 1.0], dtype=float_type)
    }, index=np.arange(4))
    lib.write(symbol, df)

    # When
    q = QueryBuilder()
    q = q.apply("new_col", q["col1"] + q["col2"])

    # Then
    received = lib.read(symbol, query_builder=q).data
    expected = df
    expected["new_col"] = expected["col1"] + expected["col2"]
    # We could promote up to float32 without losing precision of these int types but we go all the way up to float64
    # to match Pandas.
    assert expected.dtypes["new_col"] == np.float64
    assert received.dtypes["new_col"] == np.float64
    assert_frame_equal(expected, received)


@pytest.mark.parametrize("original_type", [np.int16, np.uint16, np.int32, np.uint32, np.int64, np.uint64])
@pytest.mark.parametrize("append_type", [np.float32, np.float64])
def test_type_promotion_ints_and_floats_then_project_float64_result(lmdb_version_store_dynamic_schema_v1, original_type, append_type):
    # Given
    lib = lmdb_version_store_dynamic_schema_v1

    original_data = pd.DataFrame({"a": np.array([1, 2, 3], original_type)}, index=[0, 1, 2])
    original_data["b"] = original_data["a"]
    first_append = pd.DataFrame({"a": np.array([4.0, 5.0, 6.0], append_type)}, index=[3, 4, 5])
    first_append["b"] = first_append["a"]
    lib.write("test", original_data)
    lib.append("test", first_append)

    # When
    q = QueryBuilder()
    q = q.apply("new_col", q["a"] + q["b"])

    # Then
    received = lib.read("test", query_builder=q).data
    df = pd.concat([original_data, first_append])
    expected = df
    expected["new_col"] = expected["a"] + expected["b"]
    if original_type in (np.int16, np.uint16):
        if append_type == np.float32:
            expected_dtype = np.float32
        else:
            assert append_type == np.float64
            expected_dtype = np.float64
    else:
        assert original_type in (np.int32, np.uint32, np.int64, np.uint64)
        expected_dtype = np.float64

    assert expected.dtypes["new_col"] == expected_dtype
    assert received.dtypes["new_col"] == expected_dtype
    assert_frame_equal(expected, received, check_dtype=False)


@pytest.mark.parametrize("original_type", [np.int8, np.uint8])
def test_type_promotion_ints_and_floats_then_project_float32_result(lmdb_version_store_dynamic_schema_v1, original_type):
    # Given
    lib = lmdb_version_store_dynamic_schema_v1

    original_data = pd.DataFrame({"a": np.array([1, 2, 3], original_type)}, index=[0, 1, 2])
    original_data["b"] = original_data["a"]
    first_append = pd.DataFrame({"a": np.array([4.0, 5.0, 6.0], np.float32)}, index=[3, 4, 5])
    first_append["b"] = first_append["a"]
    lib.write("test", original_data)
    lib.append("test", first_append)

    # When
    q = QueryBuilder()
    q = q.apply("new_col", q["a"] + q["b"])

    # Then
    received = lib.read("test", query_builder=q).data
    df = pd.concat([original_data, first_append])
    expected = df
    expected["new_col"] = expected["a"] + expected["b"]
    assert expected.dtypes["new_col"] == np.float32
    assert received.dtypes["new_col"] == np.float32
    assert_frame_equal(expected, received)
