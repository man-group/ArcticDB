"""
Copyright 2026 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

import numpy as np
import pandas as pd
import pytest

from arcticdb_ext.exceptions import SchemaException, StorageException, UserInputException, InternalException
from arcticdb_ext.storage import KeyType, NoDataFoundException
from arcticdb_ext.version_store import NoSuchVersionException

pytestmark = pytest.mark.pipeline


df0 = pd.DataFrame(
    {"col_0": ["a", "b"], "col_1": [1, 2], "col_2": [6, 5]}, index=pd.date_range("2000-01-01", periods=2)
)
df1 = pd.DataFrame(
    {"col_0": ["c", "d"], "col_1": [3, 4], "col_2": [8, 7]}, index=pd.date_range("2000-01-03", periods=2)
)
df2 = pd.DataFrame(
    {"col_0": ["e", "f"], "col_1": [5, 6], "col_2": [10, 9]}, index=pd.date_range("2000-01-05", periods=2)
)


def generate_symbol(lib, sym):
    lib.write(sym, df0)
    lib.append(sym, df1)
    expected_column_stats = lib.read_index(sym)
    expected_column_stats.drop(
        expected_column_stats.columns.difference(["start_index", "end_index"]),
        axis=1,
        inplace=True,
    )
    expected_column_stats = expected_column_stats.iloc[[0, 1]]
    expected_column_stats["v1.0_MIN(col_1)"] = [df0["col_1"].min(), df1["col_1"].min()]
    expected_column_stats["v1.0_MAX(col_1)"] = [df0["col_1"].max(), df1["col_1"].max()]
    expected_column_stats["v1.0_MIN(col_2)"] = [df0["col_2"].min(), df1["col_2"].min()]
    expected_column_stats["v1.0_MAX(col_2)"] = [df0["col_2"].max(), df1["col_2"].max()]
    return expected_column_stats


def assert_stats_equal(received, expected):
    # Use check_like because we don't care about column order
    pd.testing.assert_frame_equal(received, expected, check_like=True, check_dtype=False)
    # But also check the index, as we do care about the row order
    pd.testing.assert_index_equal(received.index, expected.index)


def test_column_stats_basic_flow(lmdb_version_store_tiny_segment, any_output_format):
    lib = lmdb_version_store_tiny_segment
    lib._set_output_format_for_pipeline_tests(any_output_format)
    sym = "test_column_stats_basic_flow"
    expected_column_stats = generate_symbol(lib, sym)
    expected_column_stats.drop(
        expected_column_stats.columns.difference(["start_index", "end_index", "v1.0_MIN(col_1)", "v1.0_MAX(col_1)"]),
        axis=1,
        inplace=True,
    )

    column_stats_dict = {"col_1": {"MINMAX"}}

    # These should just log warnings if no column stats exist
    lib.drop_column_stats(sym)
    lib.drop_column_stats(sym, column_stats_dict)

    lib.create_column_stats(sym, column_stats_dict)
    assert lib.get_column_stats_info(sym) == column_stats_dict

    column_stats = lib.read_column_stats(sym)
    assert_stats_equal(column_stats, expected_column_stats)

    lib.drop_column_stats(sym)
    with pytest.raises(StorageException):
        lib.get_column_stats_info(sym)
    with pytest.raises(StorageException):
        lib.read_column_stats(sym)


def test_column_stats_infinity(lmdb_version_store_tiny_segment, any_output_format):
    lib = lmdb_version_store_tiny_segment
    lib._set_output_format_for_pipeline_tests(any_output_format)
    sym = "test_column_stats_infinity"
    df0 = pd.DataFrame({"col_1": [np.inf, 0.5]}, index=pd.date_range("2000-01-01", periods=2))
    df1 = pd.DataFrame({"col_1": [1.5, -np.inf]}, index=pd.date_range("2000-01-03", periods=2))
    df2 = pd.DataFrame({"col_1": [np.inf, -np.inf]}, index=pd.date_range("2000-01-05", periods=2))
    lib.write(sym, df0)
    lib.append(sym, df1)
    lib.append(sym, df2)
    expected_column_stats = lib.read_index(sym)
    expected_column_stats.drop(
        expected_column_stats.columns.difference(["start_index", "end_index"]),
        axis=1,
        inplace=True,
    )
    expected_column_stats = expected_column_stats.iloc[[0, 1, 2]]
    expected_column_stats["v1.0_MIN(col_1)"] = [df0["col_1"].min(), df1["col_1"].min(), df2["col_1"].min()]
    expected_column_stats["v1.0_MAX(col_1)"] = [df0["col_1"].max(), df1["col_1"].max(), df2["col_1"].max()]
    column_stats_dict = {"col_1": {"MINMAX"}}

    lib.create_column_stats(sym, column_stats_dict)

    column_stats = lib.read_column_stats(sym)
    assert_stats_equal(column_stats, expected_column_stats)


def test_column_stats_as_of(lmdb_version_store_tiny_segment, any_output_format):
    lib = lmdb_version_store_tiny_segment
    lib._set_output_format_for_pipeline_tests(any_output_format)
    sym = "test_column_stats_as_of"
    expected_column_stats = generate_symbol(lib, sym)
    expected_column_stats = expected_column_stats.iloc[[0]]
    expected_column_stats.drop(
        expected_column_stats.columns.difference(["start_index", "end_index", "v1.0_MIN(col_1)", "v1.0_MAX(col_1)"]),
        axis=1,
        inplace=True,
    )
    column_stats_dict = {"col_1": {"MINMAX"}}
    lib.create_column_stats(sym, column_stats_dict, as_of=0)
    assert lib.get_column_stats_info(sym, as_of=0) == column_stats_dict
    with pytest.raises(StorageException):
        lib.get_column_stats_info(sym)

    column_stats = lib.read_column_stats(sym, as_of=0)
    assert_stats_equal(column_stats, expected_column_stats)
    with pytest.raises(StorageException):
        lib.read_column_stats(sym)

    lib.drop_column_stats(sym, as_of=0)
    with pytest.raises(StorageException):
        lib.get_column_stats_info(sym, as_of=0)
    with pytest.raises(StorageException):
        lib.read_column_stats(sym, as_of=0)


def test_column_stats_as_of_version_doesnt_exist(lmdb_version_store_tiny_segment, any_output_format):
    lib = lmdb_version_store_tiny_segment
    lib._set_output_format_for_pipeline_tests(any_output_format)
    sym = "test_column_stats_as_of_version_doesnt_exist"
    generate_symbol(lib, sym)

    lib.delete_version(sym, 0)

    column_stats_dict = {"col_1": {"MINMAX"}}
    with pytest.raises(NoSuchVersionException):
        lib.create_column_stats(sym, column_stats_dict, as_of=0)
    with pytest.raises(NoSuchVersionException):
        lib.get_column_stats_info(sym, as_of=0)
    with pytest.raises(NoSuchVersionException):
        lib.read_column_stats(sym, as_of=0)


# TODO: When more than one column stat type is implemented, change this to add multiple indexes across multiple columns
def test_column_stats_multiple_indexes_different_columns(lmdb_version_store_tiny_segment, any_output_format):
    lib = lmdb_version_store_tiny_segment
    lib._set_output_format_for_pipeline_tests(any_output_format)
    sym = "test_column_stats_multiple_indexes"
    expected_column_stats = generate_symbol(lib, sym)

    column_stats_dict = {"col_1": {"MINMAX"}, "col_2": {"MINMAX"}}
    lib.create_column_stats(sym, column_stats_dict)
    assert lib.get_column_stats_info(sym) == column_stats_dict

    column_stats = lib.read_column_stats(sym)
    assert_stats_equal(column_stats, expected_column_stats)

    lib.drop_column_stats(sym, {"col_2": {"MINMAX"}})
    assert lib.get_column_stats_info(sym) == {"col_1": {"MINMAX"}}

    expected_column_stats.drop(
        expected_column_stats.columns.difference(["start_index", "end_index", "v1.0_MIN(col_1)", "v1.0_MAX(col_1)"]),
        axis=1,
        inplace=True,
    )
    column_stats = lib.read_column_stats(sym)
    assert_stats_equal(column_stats, expected_column_stats)


def test_column_stats_empty_dict(lmdb_version_store_tiny_segment, any_output_format):
    lib = lmdb_version_store_tiny_segment
    lib._set_output_format_for_pipeline_tests(any_output_format)
    sym = "test_column_stats_empty_dict"
    expected_column_stats = generate_symbol(lib, sym)

    column_stats_empty_dict = dict()
    lib.create_column_stats(sym, column_stats_empty_dict)
    with pytest.raises(StorageException):
        lib.get_column_stats_info(sym)
    with pytest.raises(StorageException):
        lib.read_column_stats(sym)

    column_stats_dict = {"col_1": {"MINMAX"}, "col_2": {"MINMAX"}}
    lib.create_column_stats(sym, column_stats_dict)
    lib.drop_column_stats(sym, column_stats_empty_dict)
    assert lib.get_column_stats_info(sym) == column_stats_dict
    assert_stats_equal(lib.read_column_stats(sym), expected_column_stats)


def test_column_stats_empty_set(lmdb_version_store_tiny_segment, any_output_format):
    lib = lmdb_version_store_tiny_segment
    lib._set_output_format_for_pipeline_tests(any_output_format)
    sym = "test_column_stats_empty_set"
    expected_column_stats = generate_symbol(lib, sym)

    column_stats_empty_set = {"col_1": set()}
    lib.create_column_stats(sym, column_stats_empty_set)
    with pytest.raises(StorageException):
        lib.get_column_stats_info(sym)
    with pytest.raises(StorageException):
        lib.read_column_stats(sym)

    column_stats_dict = {"col_1": {"MINMAX"}, "col_2": {"MINMAX"}}
    lib.create_column_stats(sym, column_stats_dict)
    lib.drop_column_stats(sym, column_stats_empty_set)
    assert lib.get_column_stats_info(sym) == column_stats_dict
    assert_stats_equal(lib.read_column_stats(sym), expected_column_stats)


def test_column_stats_non_existent_column(lmdb_version_store_tiny_segment, any_output_format):
    lib = lmdb_version_store_tiny_segment
    lib._set_output_format_for_pipeline_tests(any_output_format)
    sym = "test_column_stats_non_existent_column"
    expected_column_stats = generate_symbol(lib, sym)

    column_stats_non_existent_column = {"non_existent_column": {"MINMAX"}}
    # Idealy this should throw either a SchemaException or UserInputException
    with pytest.raises(InternalException) as exception_info:
        lib.create_column_stats(sym, column_stats_non_existent_column)
    assert "non_existent_column" in str(exception_info.value)

    column_stats_dict = {"col_1": {"MINMAX"}, "col_2": {"MINMAX"}}
    lib.create_column_stats(sym, column_stats_dict)
    lib.drop_column_stats(sym, column_stats_non_existent_column)
    assert lib.get_column_stats_info(sym) == column_stats_dict
    assert_stats_equal(lib.read_column_stats(sym), expected_column_stats)


def test_column_stats_non_existent_stat_type(lmdb_version_store_tiny_segment, any_output_format):
    lib = lmdb_version_store_tiny_segment
    lib._set_output_format_for_pipeline_tests(any_output_format)
    sym = "test_column_stats_non_existent_stat_type"
    expected_column_stats = generate_symbol(lib, sym)

    column_stats_non_existent_index_type = {"col_1": {"NON_EXISTENT_INDEX_TYPE"}}
    with pytest.raises(UserInputException):
        lib.create_column_stats(sym, column_stats_non_existent_index_type)

    column_stats_dict = {"col_1": {"MINMAX"}, "col_2": {"MINMAX"}}
    lib.create_column_stats(sym, column_stats_dict)
    with pytest.raises(UserInputException):
        lib.drop_column_stats(sym, column_stats_non_existent_index_type)
    assert lib.get_column_stats_info(sym) == column_stats_dict
    assert_stats_equal(lib.read_column_stats(sym), expected_column_stats)


def test_column_stats_pickled_symbol(lmdb_version_store_tiny_segment, any_output_format):
    lib = lmdb_version_store_tiny_segment
    lib._set_output_format_for_pipeline_tests(any_output_format)
    sym = "test_column_stats_pickled_symbol"
    lib.write(sym, 1)
    assert lib.is_symbol_pickled(sym)

    column_stats_dict = {"col_1": {"MINMAX"}}
    with pytest.raises(SchemaException):
        lib.create_column_stats(sym, column_stats_dict)


def test_column_stats_multiple_creates(lmdb_version_store_tiny_segment, any_output_format):
    lib = lmdb_version_store_tiny_segment
    lib._set_output_format_for_pipeline_tests(any_output_format)
    sym = "test_column_stats_multiple_creates"
    base_expected_column_stats = generate_symbol(lib, sym)

    column_stats_dict_1 = {"col_1": {"MINMAX"}}
    lib.create_column_stats(sym, column_stats_dict_1)
    assert lib.get_column_stats_info(sym) == column_stats_dict_1

    expected_column_stats = base_expected_column_stats.copy()
    expected_column_stats.drop(
        expected_column_stats.columns.difference(["start_index", "end_index", "v1.0_MIN(col_1)", "v1.0_MAX(col_1)"]),
        axis=1,
        inplace=True,
    )
    column_stats = lib.read_column_stats(sym)
    assert_stats_equal(column_stats, expected_column_stats)

    # Re-adding the same index should be idempotent
    lib.create_column_stats(sym, column_stats_dict_1)
    assert lib.get_column_stats_info(sym) == column_stats_dict_1
    column_stats = lib.read_column_stats(sym)
    assert_stats_equal(column_stats, expected_column_stats)

    column_stats_dict_2 = {"col_2": {"MINMAX"}}
    lib.create_column_stats(sym, column_stats_dict_2)
    column_stats_dict_merged = column_stats_dict_1.copy()
    column_stats_dict_merged.update(column_stats_dict_2)
    assert lib.get_column_stats_info(sym) == column_stats_dict_merged

    column_stats = lib.read_column_stats(sym)
    assert_stats_equal(column_stats, base_expected_column_stats)


def test_column_stats_string_column_minmax(lmdb_version_store_tiny_segment, any_output_format):
    lib = lmdb_version_store_tiny_segment
    lib._set_output_format_for_pipeline_tests(any_output_format)
    sym = "test_column_stats_string_column_minmax"
    generate_symbol(lib, sym)

    column_stats_dict = {"col_0": {"MINMAX"}}
    with pytest.raises(SchemaException):
        lib.create_column_stats(sym, column_stats_dict)


def test_column_stats_duplicated_primary_index(lmdb_version_store_tiny_segment, any_output_format):
    lib = lmdb_version_store_tiny_segment
    lib._set_output_format_for_pipeline_tests(any_output_format)
    sym = "test_column_stats_duplicated_primary_index"

    total_df = pd.concat((df0, df1))
    lib.write(sym, total_df)
    expected_column_stats = lib.read_index(sym)
    expected_column_stats.drop(
        expected_column_stats.columns.difference(["start_index", "end_index"]),
        axis=1,
        inplace=True,
    )
    expected_column_stats = expected_column_stats.iloc[[0, 1]]
    expected_column_stats["v1.0_MIN(col_1)"] = [df0["col_1"].min(), df1["col_1"].min()]
    expected_column_stats["v1.0_MAX(col_1)"] = [df0["col_1"].max(), df1["col_1"].max()]

    column_stats_dict = {"col_1": {"MINMAX"}}
    lib.create_column_stats(sym, column_stats_dict)
    assert lib.get_column_stats_info(sym) == column_stats_dict

    column_stats = lib.read_column_stats(sym)
    assert_stats_equal(column_stats, expected_column_stats)


def test_column_stats_dynamic_schema_missing_data(lmdb_version_store_tiny_segment_dynamic, any_output_format):
    lib = lmdb_version_store_tiny_segment_dynamic
    lib._set_output_format_for_pipeline_tests(any_output_format)
    sym = "test_column_stats_dynamic_schema_missing_data"

    df0 = pd.DataFrame({"col_1": [0.1, 0.2], "col_2": [0.3, 0.4]}, index=pd.date_range("2000-01-01", periods=2))
    df1 = pd.DataFrame({"col_2": [0.5, 0.6]}, index=pd.date_range("2000-01-03", periods=2))
    df2 = pd.DataFrame({"col_1": [0.7, 0.8]}, index=pd.date_range("2000-01-05", periods=2))
    df3 = pd.DataFrame({"col_0": ["a", "b"]}, index=pd.date_range("2000-01-07", periods=2))
    df4 = pd.DataFrame(
        {"col_1": [0.9, np.nan], "col_2": [np.nan, np.nan]}, index=pd.date_range("2000-01-09", periods=2)
    )

    lib.write(sym, df0)
    lib.append(sym, df1)
    lib.append(sym, df2)
    lib.append(sym, df3)
    lib.append(sym, df4)

    df = lib.read(sym).data

    expected_column_stats = lib.read_index(sym)
    expected_column_stats.drop(
        expected_column_stats.columns.difference(["start_index", "end_index"]),
        axis=1,
        inplace=True,
    )
    expected_column_stats = expected_column_stats.iloc[[0, 1, 2, 3, 4]]
    expected_column_stats["v1.0_MIN(col_1)"] = [
        df0["col_1"].min(),
        np.nan,
        df2["col_1"].min(),
        np.nan,
        df4["col_1"].min(),
    ]
    expected_column_stats["v1.0_MAX(col_1)"] = [
        df0["col_1"].max(),
        np.nan,
        df2["col_1"].max(),
        np.nan,
        df4["col_1"].max(),
    ]
    expected_column_stats["v1.0_MIN(col_2)"] = [
        df0["col_2"].min(),
        df1["col_2"].min(),
        np.nan,
        np.nan,
        df4["col_2"].min(),
    ]
    expected_column_stats["v1.0_MAX(col_2)"] = [
        df0["col_2"].max(),
        df1["col_2"].max(),
        np.nan,
        np.nan,
        df4["col_2"].max(),
    ]
    column_stats_dict = {"col_1": {"MINMAX"}, "col_2": {"MINMAX"}}
    lib.create_column_stats(sym, column_stats_dict)
    assert lib.get_column_stats_info(sym) == column_stats_dict

    column_stats = lib.read_column_stats(sym)
    assert_stats_equal(column_stats, expected_column_stats)


def test_column_stats_dynamic_schema_types_changing(lmdb_version_store_tiny_segment_dynamic, any_output_format):
    lib = lmdb_version_store_tiny_segment_dynamic
    lib._set_output_format_for_pipeline_tests(any_output_format)
    sym = "test_column_stats_dynamic_schema_types_changing"

    df0 = pd.DataFrame(
        {
            "int_widening": np.arange(0, 2, dtype=np.uint8),
            "int_narrowing": np.arange(-1002, -1000, dtype=np.int16),
            "unsigned_to_wider_signed_int": np.arange(1000, 1002, dtype=np.uint16),
            "wider_signed_to_unsigned_int": np.arange(-1002, -1000, dtype=np.int32),
            "unsigned_to_signed_int_same_width": np.arange(1000, 1002, dtype=np.uint16),
            "int_to_float": np.arange(1000, 1002, dtype=np.uint16),
            "float_to_int": [1.5, 2.5],
        },
        index=pd.date_range("2000-01-01", periods=2),
    )

    df1 = pd.DataFrame(
        {
            "int_widening": np.arange(1000, 1002, dtype=np.uint16),
            "int_narrowing": np.arange(-2, 0, dtype=np.int8),
            "unsigned_to_wider_signed_int": np.arange(-1002, -1000, dtype=np.int32),
            "wider_signed_to_unsigned_int": np.arange(1000, 1002, dtype=np.uint16),
            "unsigned_to_signed_int_same_width": np.arange(-1002, -1000, dtype=np.int16),
            "int_to_float": [1.5, 2.5],
            "float_to_int": np.arange(1000, 1002, dtype=np.uint16),
        },
        index=pd.date_range("2000-01-03", periods=2),
    )

    lib.write(sym, df0)
    lib.append(sym, df1)

    expected_column_stats = lib.read_index(sym)
    expected_column_stats.drop(
        expected_column_stats.columns.difference(["start_index", "end_index"]),
        axis=1,
        inplace=True,
    )
    expected_column_stats = expected_column_stats.iloc[[0, 1]]
    expected_column_stats["v1.0_MIN(int_widening)"] = [df0["int_widening"].min(), df1["int_widening"].min()]
    expected_column_stats["v1.0_MAX(int_widening)"] = [df0["int_widening"].max(), df1["int_widening"].max()]

    expected_column_stats["v1.0_MIN(int_narrowing)"] = [df0["int_narrowing"].min(), df1["int_narrowing"].min()]
    expected_column_stats["v1.0_MAX(int_narrowing)"] = [df0["int_narrowing"].max(), df1["int_narrowing"].max()]

    expected_column_stats["v1.0_MIN(unsigned_to_wider_signed_int)"] = [
        df0["unsigned_to_wider_signed_int"].min(),
        df1["unsigned_to_wider_signed_int"].min(),
    ]
    expected_column_stats["v1.0_MAX(unsigned_to_wider_signed_int)"] = [
        df0["unsigned_to_wider_signed_int"].max(),
        df1["unsigned_to_wider_signed_int"].max(),
    ]

    expected_column_stats["v1.0_MIN(wider_signed_to_unsigned_int)"] = [
        df0["wider_signed_to_unsigned_int"].min(),
        df1["wider_signed_to_unsigned_int"].min(),
    ]
    expected_column_stats["v1.0_MAX(wider_signed_to_unsigned_int)"] = [
        df0["wider_signed_to_unsigned_int"].max(),
        df1["wider_signed_to_unsigned_int"].max(),
    ]

    expected_column_stats["v1.0_MIN(unsigned_to_signed_int_same_width)"] = [
        df0["unsigned_to_signed_int_same_width"].min(),
        df1["unsigned_to_signed_int_same_width"].min(),
    ]
    expected_column_stats["v1.0_MAX(unsigned_to_signed_int_same_width)"] = [
        df0["unsigned_to_signed_int_same_width"].max(),
        df1["unsigned_to_signed_int_same_width"].max(),
    ]

    expected_column_stats["v1.0_MIN(int_to_float)"] = [df0["int_to_float"].min(), df1["int_to_float"].min()]
    expected_column_stats["v1.0_MAX(int_to_float)"] = [df0["int_to_float"].max(), df1["int_to_float"].max()]

    expected_column_stats["v1.0_MIN(float_to_int)"] = [df0["float_to_int"].min(), df1["float_to_int"].min()]
    expected_column_stats["v1.0_MAX(float_to_int)"] = [df0["float_to_int"].max(), df1["float_to_int"].max()]
    column_stats_dict = {
        "int_widening": {"MINMAX"},
        "int_narrowing": {"MINMAX"},
        "unsigned_to_wider_signed_int": {"MINMAX"},
        "wider_signed_to_unsigned_int": {"MINMAX"},
        "unsigned_to_signed_int_same_width": {"MINMAX"},
        "int_to_float": {"MINMAX"},
        "float_to_int": {"MINMAX"},
    }
    lib.create_column_stats(sym, column_stats_dict)
    assert lib.get_column_stats_info(sym) == column_stats_dict

    column_stats = lib.read_column_stats(sym)
    assert_stats_equal(column_stats, expected_column_stats)
    assert column_stats.dtypes["v1.0_MIN(int_widening)"] == np.uint16
    assert column_stats.dtypes["v1.0_MAX(int_widening)"] == np.uint16

    assert column_stats.dtypes["v1.0_MIN(int_narrowing)"] == np.int16
    assert column_stats.dtypes["v1.0_MAX(int_narrowing)"] == np.int16

    assert column_stats.dtypes["v1.0_MIN(unsigned_to_wider_signed_int)"] == np.int32
    assert column_stats.dtypes["v1.0_MAX(unsigned_to_wider_signed_int)"] == np.int32

    assert column_stats.dtypes["v1.0_MIN(wider_signed_to_unsigned_int)"] == np.int32
    assert column_stats.dtypes["v1.0_MAX(wider_signed_to_unsigned_int)"] == np.int32

    assert column_stats.dtypes["v1.0_MIN(unsigned_to_signed_int_same_width)"] == np.int32
    assert column_stats.dtypes["v1.0_MAX(unsigned_to_signed_int_same_width)"] == np.int32

    assert column_stats.dtypes["v1.0_MIN(int_to_float)"] == np.float64
    assert column_stats.dtypes["v1.0_MAX(int_to_float)"] == np.float64

    assert column_stats.dtypes["v1.0_MIN(float_to_int)"] == np.float64
    assert column_stats.dtypes["v1.0_MAX(float_to_int)"] == np.float64


def test_column_stats_object_deleted_with_index_key(lmdb_version_store, any_output_format):
    def clear():
        nonlocal expected_count
        lib.version_store.clear()
        expected_count = 0

    def create_stats():
        nonlocal expected_count
        lib.create_column_stats(sym, column_stats_dict)
        expected_count += 1

    def assert_column_stats_key_count():
        assert lib_tool.count_keys(KeyType.COLUMN_STATS) == expected_count

    def test_delete():
        nonlocal expected_count
        lib.write(sym, df0)
        create_stats()
        assert_column_stats_key_count()
        lib.append(sym, df1)
        create_stats()
        assert_column_stats_key_count()
        lib.delete(sym)
        expected_count = 0
        assert_column_stats_key_count()

    def test_delete_version():
        nonlocal expected_count
        lib.write(sym, df0)
        create_stats()
        assert_column_stats_key_count()
        lib.append(sym, df1)
        create_stats()
        assert_column_stats_key_count()
        lib.delete_version(sym, 0)
        expected_count = 1
        assert_column_stats_key_count()

    def test_delete_versions():
        nonlocal expected_count
        lib.write(sym, df0)
        create_stats()
        assert_column_stats_key_count()
        lib.append(sym, df1)
        create_stats()
        assert_column_stats_key_count()
        lib.append(sym, df2)
        create_stats()
        assert_column_stats_key_count()
        lib.delete_versions(sym, [0, 1])
        expected_count = 1
        assert_column_stats_key_count()

    def test_delete_snapshot():
        nonlocal expected_count
        lib.write(sym, df0)
        create_stats()
        assert_column_stats_key_count()
        snapshot_name = "snapshot_name"
        lib.snapshot(snapshot_name)
        lib.delete(sym)
        # Version now kept alive by snapshot
        assert_column_stats_key_count()
        lib.delete_snapshot(snapshot_name)
        # Last reference to version 0 has been deleted, so column stats key should be deleted as well
        expected_count = 0
        assert_column_stats_key_count()

    def test_add_to_snapshot():
        nonlocal expected_count
        lib.write(sym, df0)
        create_stats()
        assert_column_stats_key_count()
        snapshot_name = "snapshot_name"
        lib.snapshot(snapshot_name)
        lib.delete(sym)
        # Version now kept alive by snapshot
        assert_column_stats_key_count()
        lib.write(sym, df1)
        lib.add_to_snapshot(snapshot_name, [sym])
        # Last reference to version 0 has been deleted, so column stats key should be deleted as well
        expected_count = 0
        assert_column_stats_key_count()

    def test_remove_from_snapshot():
        nonlocal expected_count
        lib.write(sym, df0)
        create_stats()
        assert_column_stats_key_count()
        snapshot_name = "snapshot_name"
        lib.snapshot(snapshot_name)
        lib.delete(sym)
        # Version now kept alive by snapshot
        assert_column_stats_key_count()
        lib.remove_from_snapshot(snapshot_name, [sym], [0])
        # Last reference to version 0 has been deleted, so column stats key should be deleted as well
        expected_count = 0
        assert_column_stats_key_count()

    def test_prune_previous_kwarg():
        nonlocal expected_count
        for operation in ["write", "append", "update", "write_metadata"]:
            lib.write(sym, df0)
            create_stats()
            assert_column_stats_key_count()
            getattr(lib, operation)(sym, df1, prune_previous_version=True)
            expected_count = 0
            assert_column_stats_key_count()
            clear()

    def test_prune_previous_api():
        nonlocal expected_count
        lib.write(sym, df0)
        create_stats()
        assert_column_stats_key_count()
        lib.append(sym, df1)
        create_stats()
        assert_column_stats_key_count()
        lib.prune_previous_versions(sym)
        expected_count = 1
        assert_column_stats_key_count()

    lib = lmdb_version_store
    lib._set_output_format_for_pipeline_tests(any_output_format)
    lib_tool = lib.library_tool()
    sym = "test_column_stats_object_deleted_with_index_key"
    column_stats_dict = {"col_1": {"MINMAX"}}
    expected_count = 0

    for test in [
        test_delete,
        test_delete_version,
        test_delete_versions,
        test_delete_snapshot,
        test_add_to_snapshot,
        test_remove_from_snapshot,
        test_prune_previous_kwarg,
        test_prune_previous_api,
    ]:
        test()
        clear()


@pytest.mark.xfail(
    reason=(
        "ArcticDB/issues/230 This test can be folded in with test_column_stats_object_deleted_with_index_key once the"
        " issue is resolved"
    )
)
def test_column_stats_object_deleted_with_index_key_batch_methods(lmdb_version_store, any_output_format):
    def clear():
        nonlocal expected_count
        lib.version_store.clear()
        expected_count = 0

    def create_stats():
        nonlocal expected_count
        lib.create_column_stats(sym, column_stats_dict)
        expected_count += 1

    def assert_column_stats_key_count():
        assert lib_tool.count_keys(KeyType.COLUMN_STATS) == expected_count

    def test_prune_previous_kwarg_batch_methods():
        nonlocal expected_count
        for operation in ["batch_write", "batch_append", "batch_write_metadata"]:
            lib.write(sym, df0)
            create_stats()
            assert_column_stats_key_count()
            getattr(lib, operation)([sym], [df1], prune_previous_version=True)
            expected_count = 0
            assert_column_stats_key_count()
            clear()

    lib = lmdb_version_store
    lib._set_output_format_for_pipeline_tests(any_output_format)
    lib_tool = lib.library_tool()
    sym = "test_column_stats_object_deleted_with_index_key_batch_methods"
    column_stats_dict = {"col_1": {"MINMAX"}}
    expected_count = 0

    for test in [test_prune_previous_kwarg_batch_methods]:
        test()
        clear()


@pytest.mark.parametrize("output_format", ["PANDAS", "PYARROW"])
def test_column_stats_query_optimization(s3_store_factory, output_format):
    """
    Test that column stats are used to optimize QueryBuilder queries by pruning segments.

    Uses S3 storage so that query_stats can be used to verify the number of TABLE_DATA
    keys read (LMDB doesn't support query_stats).

    The data is structured so that:
    - Segment 0: col_1 values [1, 2] (min=1, max=2)
    - Segment 1: col_1 values [3, 4] (min=3, max=4)

    Query col_1 > 2 should only need to read segment 1 (segment 0 max=2 <= 2).
    """
    from arcticdb.version_store.processing import QueryBuilder
    import arcticdb.toolbox.query_stats as qs
    from arcticdb import OutputFormat

    # Create S3 store with tiny segments (2 rows per segment)
    lib = s3_store_factory(column_group_size=2, segment_row_size=2)
    if output_format == "PYARROW":
        lib._set_output_format_for_pipeline_tests(OutputFormat.PYARROW)
    sym = "test_column_stats_query_optimization"

    # Segment 0: col_1 = [1, 2]
    df0 = pd.DataFrame({"col_1": [1, 2], "col_2": ["a", "b"]}, index=pd.date_range("2000-01-01", periods=2))
    # Segment 1: col_1 = [3, 4]
    df1 = pd.DataFrame({"col_1": [3, 4], "col_2": ["c", "d"]}, index=pd.date_range("2000-01-03", periods=2))

    lib.write(sym, df0)
    lib.append(sym, df1)

    # Create column stats for col_1
    lib.create_column_stats(sym, {"col_1": {"MINMAX"}})

    def get_table_data_read_count():
        """Get the number of TABLE_DATA keys read from query stats."""
        stats = qs.get_query_stats()
        if not stats or "storage_operations" not in stats:
            return 0
        storage_ops = stats["storage_operations"]
        if "S3_GetObject" not in storage_ops:
            return 0
        get_ops = storage_ops["S3_GetObject"]
        if "TABLE_DATA" not in get_ops:
            return 0
        return get_ops["TABLE_DATA"]["count"]

    qs.enable()
    try:
        # Test 1: col_1 > 2 should only read segment 1 (segment 0 max=2 <= 2, pruned)
        # Result should be [3, 4] from segment 1
        q = QueryBuilder()
        q = q[q["col_1"] > 2]
        qs.reset_stats()
        result = lib.read(sym, query_builder=q).data
        table_data_reads = get_table_data_read_count()
        assert len(result) == 2
        assert list(result["col_1"]) == [3, 4]
        assert table_data_reads == 1, f"Expected 1 TABLE_DATA read (segment 1 only), got {table_data_reads}"

        # Test 2: col_1 > 4 should read no segments (both segments pruned: max <= 4)
        q = QueryBuilder()
        q = q[q["col_1"] > 4]
        qs.reset_stats()
        result = lib.read(sym, query_builder=q).data
        table_data_reads = get_table_data_read_count()
        assert len(result) == 0
        assert table_data_reads == 0, f"Expected 0 TABLE_DATA reads (all segments pruned), got {table_data_reads}"

        # Test 3: col_1 < 2 should only read segment 0 (segment 1 min=3 >= 2, pruned)
        q = QueryBuilder()
        q = q[q["col_1"] < 2]
        qs.reset_stats()
        result = lib.read(sym, query_builder=q).data
        table_data_reads = get_table_data_read_count()
        assert len(result) == 1
        assert list(result["col_1"]) == [1]
        assert table_data_reads == 1, f"Expected 1 TABLE_DATA read (segment 0 only), got {table_data_reads}"

        # Test 4: col_1 == 3 should only read segment 1 (segment 0 max=2 < 3, pruned)
        q = QueryBuilder()
        q = q[q["col_1"] == 3]
        qs.reset_stats()
        result = lib.read(sym, query_builder=q).data
        table_data_reads = get_table_data_read_count()
        assert len(result) == 1
        assert list(result["col_1"]) == [3]
        assert table_data_reads == 1, f"Expected 1 TABLE_DATA read (segment 1 only), got {table_data_reads}"

        # Test 5: col_1 >= 1 should read both segments (no pruning possible)
        q = QueryBuilder()
        q = q[q["col_1"] >= 1]
        qs.reset_stats()
        result = lib.read(sym, query_builder=q).data
        table_data_reads = get_table_data_read_count()
        assert len(result) == 4
        assert list(result["col_1"]) == [1, 2, 3, 4]
        assert table_data_reads == 2, f"Expected 2 TABLE_DATA reads (both segments), got {table_data_reads}"
    finally:
        qs.disable()


def test_column_stats_query_optimization_column_not_in_stats(lmdb_version_store_tiny_segment, any_output_format):
    """
    Test that queries work when column stats exist but not for the filtered column.
    """
    from arcticdb.version_store.processing import QueryBuilder

    lib = lmdb_version_store_tiny_segment
    lib._set_output_format_for_pipeline_tests(any_output_format)
    sym = "test_column_stats_query_optimization_column_not_in_stats"

    df0 = pd.DataFrame({"col_1": [1, 2], "col_2": [10, 20]}, index=pd.date_range("2000-01-01", periods=2))
    df1 = pd.DataFrame({"col_1": [3, 4], "col_2": [30, 40]}, index=pd.date_range("2000-01-03", periods=2))

    lib.write(sym, df0)
    lib.append(sym, df1)

    # Create column stats for col_2 only, not col_1
    lib.create_column_stats(sym, {"col_2": {"MINMAX"}})

    # Query on col_1 (no stats) should still work
    q = QueryBuilder()
    q = q[q["col_1"] > 2]
    result = lib.read(sym, query_builder=q).data
    assert len(result) == 2
    assert list(result["col_1"]) == [3, 4]


@pytest.mark.parametrize("output_format", ["PANDAS", "PYARROW"])
def test_column_stats_query_optimization_multiple_filters(s3_store_factory, output_format):
    """
    Test that column stats pruning works correctly with multiple filters on different columns.

    The data is structured so that:
    - Segment 0: col_1 = [1, 2], col_2 = [10, 20]  (col_1 min=1, max=2; col_2 min=10, max=20)
    - Segment 1: col_1 = [3, 4], col_2 = [30, 40]  (col_1 min=3, max=4; col_2 min=30, max=40)
    - Segment 2: col_1 = [5, 6], col_2 = [50, 60]  (col_1 min=5, max=6; col_2 min=50, max=60)

    Test various combinations of filters on both columns.
    """
    from arcticdb.version_store.processing import QueryBuilder
    import arcticdb.toolbox.query_stats as qs
    from arcticdb import OutputFormat

    # Create S3 store with tiny segments (2 rows per segment)
    lib = s3_store_factory(column_group_size=2, segment_row_size=2)
    if output_format == "PYARROW":
        lib._set_output_format_for_pipeline_tests(OutputFormat.PYARROW)
    sym = "test_column_stats_query_optimization_multiple_filters"

    # Create 3 segments with distinct ranges for both columns
    df0 = pd.DataFrame({"col_1": [1, 2], "col_2": [10, 20]}, index=pd.date_range("2000-01-01", periods=2))
    df1 = pd.DataFrame({"col_1": [3, 4], "col_2": [30, 40]}, index=pd.date_range("2000-01-03", periods=2))
    df2 = pd.DataFrame({"col_1": [5, 6], "col_2": [50, 60]}, index=pd.date_range("2000-01-05", periods=2))

    lib.write(sym, df0)
    lib.append(sym, df1)
    lib.append(sym, df2)

    # Create column stats for both columns
    lib.create_column_stats(sym, {"col_1": {"MINMAX"}, "col_2": {"MINMAX"}})

    def get_table_data_read_count():
        """Get the number of TABLE_DATA keys read from query stats."""
        stats = qs.get_query_stats()
        if not stats or "storage_operations" not in stats:
            return 0
        storage_ops = stats["storage_operations"]
        if "S3_GetObject" not in storage_ops:
            return 0
        get_ops = storage_ops["S3_GetObject"]
        if "TABLE_DATA" not in get_ops:
            return 0
        return get_ops["TABLE_DATA"]["count"]

    qs.enable()
    try:
        # Test 1: col_1 > 2 AND col_2 < 50 should only read segment 1
        # Segment 0: col_1 max=2 <= 2, pruned by col_1 filter
        # Segment 1: col_1 in [3,4], col_2 in [30,40], both filters pass
        # Segment 2: col_2 min=50 >= 50, pruned by col_2 filter
        q = QueryBuilder()
        q = q[(q["col_1"] > 2) & (q["col_2"] < 50)]
        qs.reset_stats()
        result = lib.read(sym, query_builder=q).data
        table_data_reads = get_table_data_read_count()
        assert len(result) == 2
        assert list(result["col_1"]) == [3, 4]
        assert list(result["col_2"]) == [30, 40]
        assert table_data_reads == 1, f"Expected 1 TABLE_DATA read (segment 1 only), got {table_data_reads}"

        # Test 2: col_1 >= 3 AND col_2 > 35 should read segments 1 and 2
        # Segment 0: col_1 max=2 < 3, pruned by col_1 filter
        # Segment 1: might have col_2 > 35 (has 40)
        # Segment 2: both filters pass
        q = QueryBuilder()
        q = q[(q["col_1"] >= 3) & (q["col_2"] > 35)]
        qs.reset_stats()
        result = lib.read(sym, query_builder=q).data
        table_data_reads = get_table_data_read_count()
        assert len(result) == 3  # [4] from seg 1, [5, 6] from seg 2
        assert list(result["col_1"]) == [4, 5, 6]
        assert table_data_reads == 2, f"Expected 2 TABLE_DATA reads (segments 1 and 2), got {table_data_reads}"

        # Test 3: col_1 == 1 AND col_2 == 10 should only read segment 0
        # Only segment 0 can have col_1=1 (range [1,2]) and col_2=10 (range [10,20])
        q = QueryBuilder()
        q = q[(q["col_1"] == 1) & (q["col_2"] == 10)]
        qs.reset_stats()
        result = lib.read(sym, query_builder=q).data
        table_data_reads = get_table_data_read_count()
        assert len(result) == 1
        assert list(result["col_1"]) == [1]
        assert list(result["col_2"]) == [10]
        assert table_data_reads == 1, f"Expected 1 TABLE_DATA read (segment 0 only), got {table_data_reads}"

        # Test 4: col_1 > 6 AND col_2 > 0 should read no segments
        # All segments have col_1 max <= 6, so all pruned
        q = QueryBuilder()
        q = q[(q["col_1"] > 6) & (q["col_2"] > 0)]
        qs.reset_stats()
        result = lib.read(sym, query_builder=q).data
        table_data_reads = get_table_data_read_count()
        assert len(result) == 0
        assert table_data_reads == 0, f"Expected 0 TABLE_DATA reads (all segments pruned), got {table_data_reads}"

        # Test 5: Filter only on col_1 with stats on both - col_1 > 4 should only read segment 2
        q = QueryBuilder()
        q = q[q["col_1"] > 4]
        qs.reset_stats()
        result = lib.read(sym, query_builder=q).data
        table_data_reads = get_table_data_read_count()
        assert len(result) == 2
        assert list(result["col_1"]) == [5, 6]
        assert table_data_reads == 1, f"Expected 1 TABLE_DATA read (segment 2 only), got {table_data_reads}"

    finally:
        qs.disable()


@pytest.mark.parametrize(
    "dtype,values_seg0,values_seg1,filter_val,expected_reads",
    [
        # Integer types
        (np.int8, [-100, -50], [50, 100], 0, 1),  # filter > 0 prunes seg0
        (np.int16, [-1000, -500], [500, 1000], 0, 1),
        (np.int32, [-100000, -50000], [50000, 100000], 0, 1),
        (np.int64, [-(10**15), -(10**14)], [10**14, 10**15], 0, 1),
        # Unsigned integer types
        (np.uint8, [1, 10], [200, 250], 100, 1),  # filter > 100 prunes seg0
        (np.uint16, [1, 100], [50000, 60000], 1000, 1),
        (np.uint32, [1, 1000], [3000000000, 4000000000], 2000000000, 1),
        (np.uint64, [1, 1000], [10**18, 10**18 + 1000], 10**17, 1),
        # Float types
        (np.float32, [1.5, 2.5], [10.5, 11.5], 5.0, 1),  # filter > 5.0 prunes seg0
        (np.float64, [1.5e10, 2.5e10], [10.5e10, 11.5e10], 5.0e10, 1),
    ],
)
def test_column_stats_query_optimization_different_types(
    s3_store_factory, dtype, values_seg0, values_seg1, filter_val, expected_reads
):
    """
    Test that column stats pruning works correctly with different numeric column types.

    This ensures that native type comparisons work correctly without precision loss
    from converting to double.
    """
    from arcticdb.version_store.processing import QueryBuilder
    import arcticdb.toolbox.query_stats as qs

    # Create S3 store with tiny segments (2 rows per segment)
    lib = s3_store_factory(column_group_size=2, segment_row_size=2)
    sym = f"test_column_stats_types_{dtype.__name__}"

    # Create two segments with the specified dtype
    df0 = pd.DataFrame(
        {"col": np.array(values_seg0, dtype=dtype)}, index=pd.date_range("2000-01-01", periods=len(values_seg0))
    )
    df1 = pd.DataFrame(
        {"col": np.array(values_seg1, dtype=dtype)}, index=pd.date_range("2000-01-03", periods=len(values_seg1))
    )

    lib.write(sym, df0)
    lib.append(sym, df1)

    # Create column stats
    lib.create_column_stats(sym, {"col": {"MINMAX"}})

    def get_table_data_read_count():
        stats = qs.get_query_stats()
        if not stats or "storage_operations" not in stats:
            return 0
        storage_ops = stats["storage_operations"]
        if "S3_GetObject" not in storage_ops:
            return 0
        get_ops = storage_ops["S3_GetObject"]
        if "TABLE_DATA" not in get_ops:
            return 0
        return get_ops["TABLE_DATA"]["count"]

    qs.enable()
    try:
        # Filter should prune segment 0 based on the filter_val
        q = QueryBuilder()
        q = q[q["col"] > filter_val]
        qs.reset_stats()
        result = lib.read(sym, query_builder=q).data
        table_data_reads = get_table_data_read_count()

        # Verify pruning happened correctly
        assert (
            table_data_reads == expected_reads
        ), f"Expected {expected_reads} TABLE_DATA read(s) for dtype {dtype.__name__}, got {table_data_reads}"
        # Verify we got the right data (from segment 1)
        assert len(result) == len(values_seg1)
    finally:
        qs.disable()
