"""
Copyright 2026 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

import copy

from hypothesis import assume, given, settings, strategies
from hypothesis.extra.pandas import columns, data_frames
import numpy as np
import pandas as pd
import pytest

from arcticdb import QueryBuilder, where, OutputFormat
from arcticdb_ext.exceptions import InternalException, SchemaException, UserInputException
from arcticdb.util.hypothesis import use_of_function_scoped_fixtures_in_hypothesis_checked
from arcticdb.util.test import assert_frame_equal
from tests.util.mark import WINDOWS

pytestmark = pytest.mark.pipeline


# A lot of the tests in here are designed to test specific code paths in operation_dispatch_ternary.cpp. In particular,
# references to FullResult, EmptyResult, bitset, value, and column are exercising those code paths with the variant
# holding those particular types


def test_project_ternary_condition_as_full_and_empty_result(lmdb_version_store_v1, any_output_format):
    lib = lmdb_version_store_v1
    lib._set_output_format_for_pipeline_tests(any_output_format)
    symbol = "test_project_ternary_condition_as_full_and_empty_result"
    df = pd.DataFrame(
        {
            "conditional": [0] * 6,
            "col1": np.arange(6, dtype=np.int64),
            "col2": np.arange(10, 16, dtype=np.int64),
        },
        index=pd.date_range("2024-01-01", periods=6),
    )
    lib.write(symbol, df)

    expected = df

    # FullResult
    expected["new_col"] = np.where((~(df["conditional"] != 0)).to_numpy(), df["col1"].to_numpy(), df["col2"].to_numpy())
    q = QueryBuilder()
    q = q.apply("new_col", where(~(q["conditional"] != 0), q["col1"], q["col2"]))
    received = lib.read(symbol, query_builder=q).data
    assert_frame_equal(expected, received)

    # EmptyResult
    expected["new_col"] = np.where((df["conditional"] != 0).to_numpy(), df["col1"].to_numpy(), df["col2"].to_numpy())
    q = QueryBuilder()
    q = q.apply("new_col", where(q["conditional"] != 0, q["col1"], q["col2"]))
    received = lib.read(symbol, query_builder=q).data
    assert_frame_equal(expected, received)


def test_project_ternary_column_column_numeric(lmdb_version_store_v1, any_output_format):
    lib = lmdb_version_store_v1
    lib._set_output_format_for_pipeline_tests(any_output_format)
    symbol = "test_project_ternary_column_column_numeric"
    df = pd.DataFrame(
        {
            "conditional": [True, False, False, True, False, True],
            "int64_1": np.arange(6, dtype=np.int64),
            "int64_2": np.arange(10, 16, dtype=np.int64),
            "int8": np.arange(-6, 0, dtype=np.int8),
            "uint8": np.arange(249, 255, dtype=np.uint8),
            "uint64": np.arange(1000, 1006, dtype=np.uint64),
        },
        index=pd.date_range("2024-01-01", periods=6),
    )
    lib.write(symbol, df)

    # Identical types
    expected = copy.deepcopy(df)
    expected["new_col"] = np.where(df["conditional"].to_numpy(), df["int64_1"].to_numpy(), df["int64_2"].to_numpy())
    q = QueryBuilder()
    q = q.apply("new_col", where(q["conditional"], q["int64_1"], q["int64_2"]))
    received = lib.read(symbol, query_builder=q).data
    assert_frame_equal(expected, received)

    # One type a subset of the other
    expected = copy.deepcopy(df)
    expected["new_col"] = np.where(df["conditional"].to_numpy(), df["int64_1"].to_numpy(), df["int8"].to_numpy())
    q = QueryBuilder()
    q = q.apply("new_col", where(q["conditional"], q["int64_1"], q["int8"]))
    received = lib.read(symbol, query_builder=q).data
    assert_frame_equal(expected, received)

    # Promotable type exists
    expected = copy.deepcopy(df)
    expected["new_col"] = np.where(df["conditional"].to_numpy(), df["int8"].to_numpy(), df["uint8"].to_numpy())
    q = QueryBuilder()
    q = q.apply("new_col", where(q["conditional"], q["int8"], q["uint8"]))
    received = lib.read(symbol, query_builder=q).data
    assert_frame_equal(expected, received)

    # uint64/int64 mix
    expected = copy.deepcopy(df)
    expected["new_col"] = np.where(df["conditional"].to_numpy(), df["int8"].to_numpy(), df["uint64"].to_numpy())
    q = QueryBuilder()
    q = q.apply("new_col", where(q["conditional"], q["int8"], q["uint64"]))
    received = lib.read(symbol, query_builder=q).data
    assert_frame_equal(expected, received)


def test_project_ternary_column_column_dynamic_strings(lmdb_version_store_v1, any_output_format):
    lib = lmdb_version_store_v1
    lib._set_output_format_for_pipeline_tests(any_output_format)
    symbol = "test_project_ternary_column_column_dynamic_strings"
    df = pd.DataFrame(
        {
            "conditional": [True, False, False, True, False, True],
            "col1": ["a", "b", "c", None, "e", "f"],
            "col2": ["g", "h", "i", "j", np.nan, "l"],
        },
        index=pd.date_range("2024-01-01", periods=6),
    )
    lib.write(symbol, df)

    expected = df
    expected["new_col"] = np.where(df["conditional"].to_numpy(), df["col1"].to_numpy(), df["col2"].to_numpy())
    q = QueryBuilder()
    q = q.apply("new_col", where(q["conditional"], q["col1"], q["col2"]))
    received = lib.read(symbol, query_builder=q).data
    assert_frame_equal(expected, received)


@pytest.mark.skipif(WINDOWS, reason="We do not support fixed-width strings on Windows")
def test_project_ternary_fixed_width_strings(version_store_factory):
    # Explicitly not tested with arrow because arrow doesn't support fixed width strings
    lib = version_store_factory(dynamic_strings=False)
    symbol = "test_project_ternary_fixed_width_strings"
    df = pd.DataFrame(
        {
            "conditional": [True, False, False, True, False, True],
            "width_1": ["a", "b", "c", "d", "e", "f"],
            "width_2": ["gg", "hh", "ii", "jj", "kk", "ll"],
        },
        index=pd.date_range("2024-01-01", periods=6),
    )
    lib.write(symbol, df)

    # Column/value
    q = QueryBuilder()
    q = q.apply("new_col", where(q["conditional"], q["width_1"], "hello"))
    with pytest.raises(SchemaException):
        lib.read(symbol, query_builder=q)

    # Column/column
    q = QueryBuilder()
    q = q.apply("new_col", where(q["conditional"], q["width_1"], q["width_2"]))
    with pytest.raises(SchemaException):
        lib.read(symbol, query_builder=q)


def test_project_ternary_column_value_numeric(lmdb_version_store_v1, any_output_format):
    lib = lmdb_version_store_v1
    lib._set_output_format_for_pipeline_tests(any_output_format)
    symbol = "test_project_ternary_column_value_numeric"
    df = pd.DataFrame(
        {
            "conditional": [True, False, False, True, False, True],
            "col1": np.arange(6),
        },
        index=pd.date_range("2024-01-01", periods=6),
    )
    lib.write(symbol, df)

    expected = df
    expected["new_col"] = np.where(df["conditional"].to_numpy(), df["col1"].to_numpy(), 10)
    q = QueryBuilder()
    q = q.apply("new_col", where(q["conditional"], q["col1"], 10))
    received = lib.read(symbol, query_builder=q).data
    assert_frame_equal(expected, received)

    # Swap operands
    expected["new_col"] = np.where(df["conditional"].to_numpy(), 10, df["col1"].to_numpy())
    q = QueryBuilder()
    q = q.apply("new_col", where(q["conditional"], 10, q["col1"]))
    received = lib.read(symbol, query_builder=q).data
    assert_frame_equal(expected, received)


def test_project_ternary_column_value_strings(lmdb_version_store_v1, any_output_format):
    lib = lmdb_version_store_v1
    lib._set_output_format_for_pipeline_tests(any_output_format)
    symbol = "test_project_ternary_column_value_strings"
    df = pd.DataFrame(
        {
            "conditional": [True, False, False, True, False, True],
            "col1": ["a", "b", "c", "d", "e", "f"],
        },
        index=pd.date_range("2024-01-01", periods=6),
    )
    lib.write(symbol, df)

    expected = df
    expected["new_col"] = np.where(df["conditional"].to_numpy(), df["col1"].to_numpy(), "h")
    q = QueryBuilder()
    q = q.apply("new_col", where(q["conditional"], q["col1"], "h"))
    received = lib.read(symbol, query_builder=q).data
    assert_frame_equal(expected, received)

    # Swap operands
    expected["new_col"] = np.where(df["conditional"].to_numpy(), "h", df["col1"].to_numpy())
    q = QueryBuilder()
    q = q.apply("new_col", where(q["conditional"], "h", q["col1"]))
    received = lib.read(symbol, query_builder=q).data
    assert_frame_equal(expected, received)


def test_project_ternary_value_value_numeric(lmdb_version_store_v1, any_output_format):
    lib = lmdb_version_store_v1
    lib._set_output_format_for_pipeline_tests(any_output_format)
    symbol = "test_project_ternary_value_value_numeric"
    df = pd.DataFrame(
        {
            "conditional": [True, False, False, True, False, True],
        },
        index=pd.date_range("2024-01-01", periods=6),
    )
    lib.write(symbol, df)

    expected = df
    expected["new_col"] = np.where(df["conditional"].to_numpy(), 0, 1)
    expected["new_col"] = expected["new_col"].astype("uint8")
    q = QueryBuilder()
    q = q.apply("new_col", where(q["conditional"], 0, 1))
    received = lib.read(symbol, query_builder=q).data
    assert_frame_equal(expected, received)


def test_project_ternary_value_value_string(lmdb_version_store_v1, any_output_format):
    lib = lmdb_version_store_v1
    lib._set_output_format_for_pipeline_tests(any_output_format)
    symbol = "test_project_ternary_value_value_string"
    df = pd.DataFrame(
        {
            "conditional": [True, False, False, True, False, True],
        },
        index=pd.date_range("2024-01-01", periods=6),
    )
    lib.write(symbol, df)

    expected = df
    expected["new_col"] = np.where(df["conditional"].to_numpy(), "hello", "goodbye")
    q = QueryBuilder()
    q = q.apply("new_col", where(q["conditional"], "hello", "goodbye"))
    received = lib.read(symbol, query_builder=q).data
    assert_frame_equal(expected, received)


@pytest.mark.parametrize(
    "index",
    [
        None,
        pd.MultiIndex.from_arrays(
            [3 * [pd.Timestamp(0)] + 3 * [pd.Timestamp(1)], [0, 1, 2, 0, 1, 2]], names=["datetime", "level"]
        ),
    ],
)
def test_project_ternary_column_sliced(version_store_factory, index, any_output_format):
    # Cannot use lmdb_version_store_tiny_segment as it has fixed-width strings, which are not supported with the ternary
    # operator
    lib = version_store_factory(dynamic_strings=True, column_group_size=2, segment_row_size=2)
    lib._set_output_format_for_pipeline_tests(any_output_format)
    symbol = "test_project_ternary_column_sliced_range_index"
    # This fixture has 2 columns per slice, so the column groups will be:
    # - ["conditional", num_1]
    # - ["num_2", "str1"]
    # - ["str_2"]
    # i.e. the numeric columns and (more importantly) the string columns are in different segments from one another,
    # testing the string pool handling
    df = pd.DataFrame(
        {
            "conditional": [True, False, False, True, False, True],
            "num_1": np.arange(0, 6, dtype=np.int64),
            "num_2": np.arange(10, 16, dtype=np.int64),
            "str_1": ["one", "two", "three", "four", "five", "six"],
            "str_2": ["eleven", "twelve", "thirteen", "fourteen", "fifteen", "sixteen"],
        },
        index=index,
    )
    lib.write(symbol, df)

    # Numeric
    expected = df
    expected["new_col"] = np.where(df["conditional"].to_numpy(), df["num_1"].to_numpy(), df["num_2"].to_numpy())
    q = QueryBuilder()
    q = q.apply("new_col", where(q["conditional"], q["num_1"], q["num_2"]))
    received = lib.read(symbol, query_builder=q).data
    assert_frame_equal(expected, received)

    # String
    expected = df
    expected["new_col"] = np.where(df["conditional"].to_numpy(), df["str_1"].to_numpy(), df["str_2"].to_numpy())
    q = QueryBuilder()
    q = q.apply("new_col", where(q["conditional"], q["str_1"], q["str_2"]))
    received = lib.read(symbol, query_builder=q).data
    assert_frame_equal(expected, received)


def test_project_ternary_dynamic_missing_columns(lmdb_version_store_dynamic_schema_v1, any_output_format):
    lib = lmdb_version_store_dynamic_schema_v1
    lib._set_output_format_for_pipeline_tests(any_output_format)
    symbol = "test_project_ternary_dynamic_missing_columns"
    all_columns_df = pd.DataFrame(
        {
            "conditional": [True, False],
            "col1": np.arange(2, dtype=np.int64),
            "col2": np.arange(10, 12, dtype=np.int64),
        },
        index=pd.date_range("2024-01-01", periods=2),
    )
    lib.write(symbol, all_columns_df)

    base_update_df = pd.DataFrame(
        {
            "conditional": [True, False],
            "col1": np.arange(2, 4, dtype=np.int64),
            "col2": np.arange(12, 14, dtype=np.int64),
        },
        index=pd.date_range("2024-01-03", periods=2),
    )

    # left column missing with value
    update_df = base_update_df.drop(columns="col1")
    lib.update(symbol, update_df)
    q = QueryBuilder()
    q = q.apply("new_col", where(q["conditional"], q["col1"], 100))
    received = lib.read(symbol, query_builder=q).data
    expected = pd.concat([all_columns_df, update_df]).fillna(0)
    expected["col1"] = expected["col1"].astype("int64")
    expected["new_col"] = np.where(expected["conditional"].to_numpy(), expected["col1"].to_numpy(), 100)
    assert_frame_equal(expected, received)

    # right column missing with value
    update_df = base_update_df.drop(columns="col2")
    lib.update(symbol, update_df)
    q = QueryBuilder()
    q = q.apply("new_col", where(q["conditional"], 100, q["col2"]))
    received = lib.read(symbol, query_builder=q).data
    expected = pd.concat([all_columns_df, update_df]).fillna(0)
    expected["col2"] = expected["col2"].astype("int64")
    expected["new_col"] = np.where(expected["conditional"].to_numpy(), 100, expected["col2"].to_numpy())
    assert_frame_equal(expected, received)

    # conditional column missing
    update_df = base_update_df.drop(columns="conditional")
    lib.update(symbol, update_df)
    q = QueryBuilder()
    q = q.apply("new_col", where(q["conditional"], q["col1"], q["col2"]))
    received = lib.read(symbol, query_builder=q).data
    expected = pd.concat([all_columns_df, update_df]).fillna(False)
    expected["new_col"] = np.where(
        expected["conditional"].to_numpy(), expected["col1"].to_numpy(), expected["col2"].to_numpy()
    )
    assert_frame_equal(expected, received)

    # left column missing with column
    update_df = base_update_df.drop(columns="col1")
    lib.update(symbol, update_df)
    received = lib.read(symbol, query_builder=q).data
    expected = pd.concat([all_columns_df, update_df]).fillna(0)
    expected["col1"] = expected["col1"].astype("int64")
    expected["new_col"] = np.where(
        expected["conditional"].to_numpy(), expected["col1"].to_numpy(), expected["col2"].to_numpy()
    )
    assert_frame_equal(expected, received)

    # right column missing with column
    update_df = base_update_df.drop(columns="col2")
    lib.update(symbol, update_df)
    received = lib.read(symbol, query_builder=q).data
    expected = pd.concat([all_columns_df, update_df]).fillna(0)
    expected["col2"] = expected["col2"].astype("int64")
    expected["new_col"] = np.where(
        expected["conditional"].to_numpy(), expected["col1"].to_numpy(), expected["col2"].to_numpy()
    )
    assert_frame_equal(expected, received)

    # conditional and left columns missing
    update_df = base_update_df.drop(columns=["conditional", "col1"])
    lib.update(symbol, update_df)
    received = lib.read(symbol, query_builder=q).data
    expected = pd.concat([all_columns_df, update_df])
    expected["conditional"].fillna(False, inplace=True)
    expected["col1"].fillna(0, inplace=True)
    expected["col1"] = expected["col1"].astype("int64")
    expected["new_col"] = np.where(
        expected["conditional"].to_numpy(), expected["col1"].to_numpy(), expected["col2"].to_numpy()
    )
    assert_frame_equal(expected, received)

    # conditional and right columns missing
    update_df = base_update_df.drop(columns=["conditional", "col2"])
    lib.update(symbol, update_df)
    received = lib.read(symbol, query_builder=q).data
    expected = pd.concat([all_columns_df, update_df])
    expected["conditional"].fillna(False, inplace=True)
    expected["col2"].fillna(0, inplace=True)
    expected["col2"] = expected["col2"].astype("int64")
    expected["new_col"] = np.where(
        expected["conditional"].to_numpy(), expected["col1"].to_numpy(), expected["col2"].to_numpy()
    )
    assert_frame_equal(expected, received)

    # left and right columns missing
    update_df = base_update_df.drop(columns=["col1", "col2"])
    lib.update(symbol, update_df)
    received = lib.read(symbol, query_builder=q).data
    expected = pd.concat([all_columns_df, update_df]).fillna(0)
    expected["col1"] = expected["col1"].astype("int64")
    expected["col2"] = expected["col2"].astype("int64")
    expected["new_col"] = np.where(
        expected["conditional"].to_numpy(), expected["col1"].to_numpy(), expected["col2"].to_numpy()
    )
    assert_frame_equal(expected, received)


def test_project_ternary_dynamic_missing_columns_strings(lmdb_version_store_dynamic_schema_v1, any_output_format):
    lib = lmdb_version_store_dynamic_schema_v1
    lib._set_output_format_for_pipeline_tests(any_output_format)
    symbol = "test_project_ternary_dynamic_missing_columns_strings"
    all_columns_df = pd.DataFrame(
        {
            "conditional": [True, False],
            "col1": ["a", "bb"],
            "col2": ["ccc", "dddd"],
        },
        index=pd.date_range("2024-01-01", periods=2),
    )
    lib.write(symbol, all_columns_df)

    base_update_df = pd.DataFrame(
        {
            "conditional": [True, False],
            "col1": ["aa", "bbb"],
            "col2": ["c", "dd"],
        },
        index=pd.date_range("2024-01-03", periods=2),
    )

    # left column missing with value
    update_df = base_update_df.drop(columns="col1")
    lib.update(symbol, update_df)
    q = QueryBuilder()
    q = q.apply("new_col", where(q["conditional"], q["col1"], "e"))
    received = lib.read(symbol, query_builder=q).data
    expected = pd.concat([all_columns_df, update_df])
    expected["new_col"] = np.where(expected["conditional"].to_numpy(), expected["col1"].to_numpy(), "e")
    assert_frame_equal(expected, received)

    # right column missing with value
    update_df = base_update_df.drop(columns="col2")
    lib.update(symbol, update_df)
    q = QueryBuilder()
    q = q.apply("new_col", where(q["conditional"], "e", q["col2"]))
    received = lib.read(symbol, query_builder=q).data
    expected = pd.concat([all_columns_df, update_df])
    expected["new_col"] = np.where(expected["conditional"].to_numpy(), "e", expected["col2"].to_numpy())
    assert_frame_equal(expected, received)

    # conditional column missing
    update_df = base_update_df.drop(columns="conditional")
    lib.update(symbol, update_df)
    q = QueryBuilder()
    q = q.apply("new_col", where(q["conditional"], q["col1"], q["col2"]))
    received = lib.read(symbol, query_builder=q).data
    expected = pd.concat([all_columns_df, update_df]).fillna(False)
    expected["new_col"] = np.where(
        expected["conditional"].to_numpy(), expected["col1"].to_numpy(), expected["col2"].to_numpy()
    )
    assert_frame_equal(expected, received)

    # left column missing with column
    update_df = base_update_df.drop(columns="col1")
    lib.update(symbol, update_df)
    received = lib.read(symbol, query_builder=q).data
    expected = pd.concat([all_columns_df, update_df])
    expected["new_col"] = np.where(
        expected["conditional"].to_numpy(), expected["col1"].to_numpy(), expected["col2"].to_numpy()
    )
    assert_frame_equal(expected, received)

    # right column missing with column
    update_df = base_update_df.drop(columns="col2")
    lib.update(symbol, update_df)
    received = lib.read(symbol, query_builder=q).data
    expected = pd.concat([all_columns_df, update_df])
    expected["new_col"] = np.where(
        expected["conditional"].to_numpy(), expected["col1"].to_numpy(), expected["col2"].to_numpy()
    )
    assert_frame_equal(expected, received)

    # conditional and left columns missing
    update_df = base_update_df.drop(columns=["conditional", "col1"])
    lib.update(symbol, update_df)
    received = lib.read(symbol, query_builder=q).data
    expected = pd.concat([all_columns_df, update_df])
    expected["conditional"].fillna(False, inplace=True)
    expected["new_col"] = np.where(
        expected["conditional"].to_numpy(), expected["col1"].to_numpy(), expected["col2"].to_numpy()
    )
    assert_frame_equal(expected, received)

    # conditional and right columns missing
    update_df = base_update_df.drop(columns=["conditional", "col2"])
    lib.update(symbol, update_df)
    received = lib.read(symbol, query_builder=q).data
    expected = pd.concat([all_columns_df, update_df])
    expected["conditional"].fillna(False, inplace=True)
    expected["new_col"] = np.where(
        expected["conditional"].to_numpy(), expected["col1"].to_numpy(), expected["col2"].to_numpy()
    )
    assert_frame_equal(expected, received)

    # left and right columns missing
    update_df = base_update_df.drop(columns=["col1", "col2"])
    lib.update(symbol, update_df)
    received = lib.read(symbol, query_builder=q).data
    expected = pd.concat([all_columns_df, update_df])
    expected["new_col"] = np.where(
        expected["conditional"].to_numpy(), expected["col1"].to_numpy(), expected["col2"].to_numpy()
    )
    assert_frame_equal(expected, received)


def test_project_ternary_sparse_col_val(lmdb_version_store_v1, any_output_format):
    lib = lmdb_version_store_v1
    lib._set_output_format_for_pipeline_tests(any_output_format)
    sym = "test_project_ternary_sparse_col_val"
    df = pd.DataFrame(
        {
            "condition": [1.0, 0.0, 1.0, 0.0, 1.0, 0.0, 1.0, 0.0],
            "col": [np.nan, 0.0, 1.0, np.nan, np.nan, 2.0, 3.0, np.nan],
        },
        index=pd.date_range("2024-01-01", periods=8),
    )
    lib.write(sym, df, sparsify_floats=True)

    # Col/val
    # Sparse output
    expected = df
    expected["projected"] = np.where((expected["condition"] == 1.0).to_numpy(), expected["col"].to_numpy(), 5.0)
    q = QueryBuilder()
    q = q.apply("projected", where(q["condition"] == 1.0, q["col"], 5))
    received = lib.read(sym, query_builder=q).data
    assert_frame_equal(expected, received)
    # Dense output
    expected = df
    expected["projected"] = np.where(expected["col"].notnull().to_numpy(), expected["col"].to_numpy(), 5.0)
    q = QueryBuilder()
    q = q.apply("projected", where(q["col"].notnull(), q["col"], 5))
    received = lib.read(sym, query_builder=q).data
    assert_frame_equal(expected, received)

    # Val/col
    # Sparse output
    expected = df
    expected["projected"] = np.where((expected["condition"] == 1.0).to_numpy(), 5.0, expected["col"].to_numpy())
    q = QueryBuilder()
    q = q.apply("projected", where(q["condition"] == 1.0, 5, q["col"]))
    received = lib.read(sym, query_builder=q).data
    assert_frame_equal(expected, received)
    # Dense output
    expected = df
    expected["projected"] = np.where((expected["col"].isnull()).to_numpy(), 5.0, expected["col"].to_numpy())
    q = QueryBuilder()
    q = q.apply("projected", where(q["col"].isnull(), 5.0, q["col"]))
    received = lib.read(sym, query_builder=q).data
    assert_frame_equal(expected, received)


def test_project_ternary_sparse_col_col(lmdb_version_store_v1, any_output_format):
    lib = lmdb_version_store_v1
    lib._set_output_format_for_pipeline_tests(any_output_format)
    sym = "test_project_ternary_sparse_col_col"
    df = pd.DataFrame(
        {
            "condition1": [1.0, 0.0, 1.0, 0.0, 1.0, 0.0, 1.0, 0.0],
            "condition2": [1.0, 0.0, 0.0, 1.0, 1.0, 0.0, 0.0, 1.0],
            "col1": [np.nan, 0.0, 1.0, np.nan, np.nan, 2.0, 3.0, np.nan],
            "col2": [np.nan, np.nan, 10.0, 12.0, 13.0, 14.0, np.nan, np.nan],
            "!col1": [20.0, np.nan, np.nan, 21.0, 22.0, np.nan, np.nan, 23.0],
        },
        index=pd.date_range("2024-01-01", periods=8),
    )
    lib.write(sym, df, sparsify_floats=True)

    # Sparse output
    # Both inputs sparse
    expected = df
    expected["projected"] = np.where(
        (expected["condition1"] == 1.0).to_numpy(), expected["col1"].to_numpy(), expected["col2"].to_numpy()
    )
    q = QueryBuilder()
    q = q.apply("projected", where(q["condition1"] == 1.0, q["col1"], q["col2"]))
    received = lib.read(sym, query_builder=q).data
    assert_frame_equal(expected, received)
    # Left input sparse, right input dense
    expected = df
    expected["projected"] = np.where(
        (expected["condition1"] == 1.0).to_numpy(), expected["col1"].to_numpy(), expected["condition2"].to_numpy()
    )
    q = QueryBuilder()
    q = q.apply("projected", where(q["condition1"] == 1.0, q["col1"], q["condition2"]))
    received = lib.read(sym, query_builder=q).data
    assert_frame_equal(expected, received)
    # Left input dense, right input sparse
    expected = df
    expected["projected"] = np.where(
        (expected["condition1"] == 1.0).to_numpy(), expected["condition2"].to_numpy(), expected["col2"].to_numpy()
    )
    q = QueryBuilder()
    q = q.apply("projected", where(q["condition1"] == 1.0, q["condition2"], q["col2"]))
    received = lib.read(sym, query_builder=q).data
    assert_frame_equal(expected, received)
    # Both inputs dense
    expected = df
    expected["projected"] = np.where(
        (expected["condition1"] == 1.0).to_numpy(), expected["condition2"].to_numpy(), expected["condition2"].to_numpy()
    )
    q = QueryBuilder()
    q = q.apply("projected", where(q["condition1"] == 1.0, q["condition2"], q["condition2"]))
    received = lib.read(sym, query_builder=q).data
    assert_frame_equal(expected, received)

    # Dense output
    expected = df
    expected["projected"] = np.where(
        (expected["condition2"] == 0.0).to_numpy(), expected["col1"].to_numpy(), expected["!col1"].to_numpy()
    )
    q = QueryBuilder()
    q = q.apply("projected", where(q["condition2"] == 0.0, q["col1"], q["!col1"]))
    received = lib.read(sym, query_builder=q).data
    assert_frame_equal(expected, received)

    # Empty output
    expected = df
    expected["projected"] = np.where(
        (expected["condition2"] == 1.0).to_numpy(), expected["col1"].to_numpy(), expected["!col1"].to_numpy()
    )
    q = QueryBuilder()
    q = q.apply("projected", where(q["condition2"] == 1.0, q["col1"], q["!col1"]))
    received = lib.read(sym, query_builder=q).data
    assert_frame_equal(expected, received)


def test_project_ternary_condition_empty(lmdb_version_store_v1, any_output_format):
    lib = lmdb_version_store_v1
    lib._set_output_format_for_pipeline_tests(any_output_format)
    sym = "test_project_ternary_condition_empty"
    df = pd.DataFrame(
        {"condition": [0.0, 0.0, 0.0], "col1": [0.0, np.nan, np.nan], "col2": [0.0, np.nan, np.nan]},
        index=pd.date_range("2024-01-01", periods=3),
    )
    lib.write(sym, df, sparsify_floats=True)
    expected = df
    expected["projected"] = np.where(expected["condition"].isnull().to_numpy(), expected["col1"].to_numpy(), 2000.0)
    q = QueryBuilder()
    q = q.apply("projected", where(q["condition"].isnull(), q["col1"], np.float64(2000)))
    received = lib.read(sym, query_builder=q).data
    assert_frame_equal(expected, received)


def test_filter_ternary_bitset_bitset(lmdb_version_store_v1, any_output_format):
    lib = lmdb_version_store_v1
    lib._set_output_format_for_pipeline_tests(any_output_format)
    symbol = "test_filter_ternary_bitset_bitset"
    df = pd.DataFrame(
        {
            "conditional": [True, False, False, True, False, True],
            "col1": np.arange(6),
            "col2": np.arange(6),
        },
        index=pd.date_range("2024-01-01", periods=6),
    )
    lib.write(symbol, df)

    expected = df[np.where(df["conditional"].to_numpy(), (df["col1"] < 4).to_numpy(), (df["col2"] == 4).to_numpy())]
    q = QueryBuilder()
    q = q[where(q["conditional"], q["col1"] < 4, q["col2"] == 4)]
    received = lib.read(symbol, query_builder=q).data
    assert_frame_equal(expected, received)


def test_filter_ternary_bitset_column(lmdb_version_store_v1, any_output_format):
    lib = lmdb_version_store_v1
    lib._set_output_format_for_pipeline_tests(any_output_format)
    symbol = "test_filter_ternary_bitset_column"
    df = pd.DataFrame(
        {
            "conditional": [True, False, False, True, False, True],
            "col1": np.arange(6),
            "col2": [True, False, True, False, True, False],
        },
        index=pd.date_range("2024-01-01", periods=6),
    )
    lib.write(symbol, df)

    expected = df[np.where(df["conditional"].to_numpy(), (df["col1"] < 4).to_numpy(), df["col2"].to_numpy())]
    q = QueryBuilder()
    q = q[where(q["conditional"], q["col1"] < 4, q["col2"])]
    received = lib.read(symbol, query_builder=q).data
    assert_frame_equal(expected, received)

    expected = df[np.where(df["conditional"].to_numpy(), df["col2"].to_numpy(), (df["col1"] < 4).to_numpy())]
    q = QueryBuilder()
    q = q[where(q["conditional"], q["col2"], q["col1"] < 4)]
    received = lib.read(symbol, query_builder=q).data
    assert_frame_equal(expected, received)


def test_filter_ternary_bool_columns(lmdb_version_store_v1, any_output_format):
    lib = lmdb_version_store_v1
    lib._set_output_format_for_pipeline_tests(any_output_format)
    symbol = "test_filter_ternary_bool_columns"
    df = pd.DataFrame(
        {
            "conditional": [True, False, False, True, False, True],
            "col1": [True, True, True, True, False, False],
            "col2": [True, False, True, False, True, False],
        },
        index=pd.date_range("2024-01-01", periods=6),
    )
    lib.write(symbol, df)

    expected = df[np.where(df["conditional"].to_numpy(), df["col1"].to_numpy(), df["col2"].to_numpy())]
    q = QueryBuilder()
    q = q[where(q["conditional"], q["col1"], q["col2"])]
    received = lib.read(symbol, query_builder=q).data
    assert_frame_equal(expected, received)

    expected = df[np.where(df["conditional"].to_numpy(), df["col2"].to_numpy(), df["col1"].to_numpy())]
    q = QueryBuilder()
    q = q[where(q["conditional"], q["col2"], q["col1"])]
    received = lib.read(symbol, query_builder=q).data
    assert_frame_equal(expected, received)

    expected = df[np.where(df["conditional"].to_numpy(), df["col1"].to_numpy(), True)]
    q = QueryBuilder()
    q = q[where(q["conditional"], q["col1"], True)]
    received = lib.read(symbol, query_builder=q).data
    assert_frame_equal(expected, received)

    expected = df[np.where(df["conditional"].to_numpy(), False, df["col2"].to_numpy())]
    q = QueryBuilder()
    q = q[where(q["conditional"], False, q["col2"])]
    received = lib.read(symbol, query_builder=q).data
    assert_frame_equal(expected, received)


def test_filter_ternary_bitset_value(lmdb_version_store_v1, any_output_format):
    lib = lmdb_version_store_v1
    lib._set_output_format_for_pipeline_tests(any_output_format)
    symbol = "test_filter_ternary_bitset_value"
    df = pd.DataFrame(
        {
            "conditional": [True, False, False, True, False, True],
            "col1": np.arange(6),
        },
        index=pd.date_range("2024-01-01", periods=6),
    )
    lib.write(symbol, df)

    expected = df[np.where(df["conditional"].to_numpy(), (df["col1"] < 4).to_numpy(), False)]
    q = QueryBuilder()
    q = q[where(q["conditional"], q["col1"] < 4, False)]
    received = lib.read(symbol, query_builder=q).data
    assert_frame_equal(expected, received)

    expected = df[np.where(df["conditional"].to_numpy(), (df["col1"] < 4).to_numpy(), True)]
    q = QueryBuilder()
    q = q[where(q["conditional"], q["col1"] < 4, True)]
    received = lib.read(symbol, query_builder=q).data
    assert_frame_equal(expected, received)

    expected = df[np.where(df["conditional"].to_numpy(), False, (df["col1"] < 4).to_numpy())]
    q = QueryBuilder()
    q = q[where(q["conditional"], False, q["col1"] < 4)]
    received = lib.read(symbol, query_builder=q).data
    assert_frame_equal(expected, received)

    expected = df[np.where(df["conditional"].to_numpy(), True, (df["col1"] < 4).to_numpy())]
    q = QueryBuilder()
    q = q[where(q["conditional"], True, q["col1"] < 4)]
    received = lib.read(symbol, query_builder=q).data
    assert_frame_equal(expected, received)


def test_filter_ternary_bitset_full_and_empty_results(lmdb_version_store_v1, any_output_format):
    lib = lmdb_version_store_v1
    lib._set_output_format_for_pipeline_tests(any_output_format)
    symbol = "test_filter_ternary_bitset_full_and_empty_results"
    df = pd.DataFrame(
        {
            "conditional": [True, False, False, True, False, True],
            "col1": np.arange(6),
        },
        index=pd.date_range("2024-01-01", periods=6),
    )
    lib.write(symbol, df)

    # Empty result as right operand
    expected = df[np.where(df["conditional"].to_numpy(), (df["col1"] < 4).to_numpy(), (df["col1"] < 0).to_numpy())]
    q = QueryBuilder()
    q = q[where(q["conditional"], q["col1"] < 4, q["col1"] < 0)]
    received = lib.read(symbol, query_builder=q).data
    assert_frame_equal(expected, received)

    # Empty result as left operand
    expected = df[np.where(df["conditional"].to_numpy(), (df["col1"] < 0).to_numpy(), (df["col1"] < 4).to_numpy())]
    q = QueryBuilder()
    q = q[where(q["conditional"], q["col1"] < 0, q["col1"] < 4)]
    received = lib.read(symbol, query_builder=q).data
    assert_frame_equal(expected, received)

    # Full result as right operand
    expected = df[np.where(df["conditional"].to_numpy(), (df["col1"] < 4).to_numpy(), (~(df["col1"] < 0)).to_numpy())]
    q = QueryBuilder()
    q = q[where(q["conditional"], q["col1"] < 4, ~(q["col1"] < 0))]
    received = lib.read(symbol, query_builder=q).data
    assert_frame_equal(expected, received)

    # Full result as left operand
    expected = df[np.where(df["conditional"].to_numpy(), (~(df["col1"] < 0)).to_numpy(), (df["col1"] < 4).to_numpy())]
    q = QueryBuilder()
    q = q[where(q["conditional"], ~(q["col1"] < 0), q["col1"] < 4)]
    received = lib.read(symbol, query_builder=q).data
    assert_frame_equal(expected, received)


def test_filter_ternary_column_full_and_empty_results(lmdb_version_store_v1, any_output_format):
    lib = lmdb_version_store_v1
    lib._set_output_format_for_pipeline_tests(any_output_format)
    symbol = "test_filter_ternary_column_full_and_empty_results"
    df = pd.DataFrame(
        {
            "conditional": [True, False, False, True, False, True],
            "col1": [True, False] * 3,
            "col2": [0] * 6,
        },
        index=pd.date_range("2024-01-01", periods=6),
    )
    lib.write(symbol, df)

    # Empty result as right operand
    expected = df[np.where(df["conditional"].to_numpy(), df["col1"].to_numpy(), (df["col2"] < 0).to_numpy())]
    q = QueryBuilder()
    q = q[where(q["conditional"], q["col1"], q["col2"] < 0)]
    received = lib.read(symbol, query_builder=q).data
    assert_frame_equal(expected, received)

    # Empty result as left operand
    expected = df[np.where(df["conditional"].to_numpy(), (df["col2"] < 0).to_numpy(), df["col1"].to_numpy())]
    q = QueryBuilder()
    q = q[where(q["conditional"], q["col2"] < 0, q["col1"])]
    received = lib.read(symbol, query_builder=q).data
    assert_frame_equal(expected, received)

    # Full result as right operand
    expected = df[np.where(df["conditional"].to_numpy(), df["col1"].to_numpy(), (~(df["col2"] < 0)).to_numpy())]
    q = QueryBuilder()
    q = q[where(q["conditional"], q["col1"], ~(q["col2"] < 0))]
    received = lib.read(symbol, query_builder=q).data
    assert_frame_equal(expected, received)

    # Full result as left operand
    expected = df[np.where(df["conditional"].to_numpy(), (~(df["col2"] < 0)).to_numpy(), df["col1"].to_numpy())]
    q = QueryBuilder()
    q = q[where(q["conditional"], ~(q["col2"] < 0), q["col1"])]
    received = lib.read(symbol, query_builder=q).data
    assert_frame_equal(expected, received)


@pytest.mark.parametrize("value", [True, False])
def test_filter_ternary_value_full_and_empty_results(lmdb_version_store_v1, value, any_output_format):
    lib = lmdb_version_store_v1
    lib._set_output_format_for_pipeline_tests(any_output_format)
    symbol = "test_filter_ternary_value_full_and_empty_results"
    df = pd.DataFrame(
        {
            "conditional": [True, False, False, True, False, True],
            "col2": [0] * 6,
        },
        index=pd.date_range("2024-01-01", periods=6),
    )
    lib.write(symbol, df)

    # Empty result as right operand
    expected = df[np.where(df["conditional"].to_numpy(), value, (df["col2"] < 0).to_numpy())]
    q = QueryBuilder()
    q = q[where(q["conditional"], value, q["col2"] < 0)]
    received = lib.read(symbol, query_builder=q).data
    assert_frame_equal(expected, received)

    # Empty result as left operand
    expected = df[np.where(df["conditional"].to_numpy(), (df["col2"] < 0).to_numpy(), value)]
    q = QueryBuilder()
    q = q[where(q["conditional"], q["col2"] < 0, value)]
    received = lib.read(symbol, query_builder=q).data
    assert_frame_equal(expected, received)

    # Full result as right operand
    expected = df[np.where(df["conditional"].to_numpy(), value, (~(df["col2"] < 0)).to_numpy())]
    q = QueryBuilder()
    q = q[where(q["conditional"], value, ~(q["col2"] < 0))]
    received = lib.read(symbol, query_builder=q).data
    assert_frame_equal(expected, received)

    # Full result as left operand
    expected = df[np.where(df["conditional"].to_numpy(), (~(df["col2"] < 0)).to_numpy(), value)]
    q = QueryBuilder()
    q = q[where(q["conditional"], ~(q["col2"] < 0), value)]
    received = lib.read(symbol, query_builder=q).data
    assert_frame_equal(expected, received)


def test_filter_ternary_full_and_empty_results_squared(lmdb_version_store_v1, any_output_format):
    lib = lmdb_version_store_v1
    lib._set_output_format_for_pipeline_tests(any_output_format)
    symbol = "test_filter_ternary_full_and_empty_results_squared"
    df = pd.DataFrame(
        {
            "conditional": [True, False, False, True, False, True],
            "col2": [0] * 6,
        },
        index=pd.date_range("2024-01-01", periods=6),
    )
    lib.write(symbol, df)

    # Full/Full
    expected = df[
        np.where(df["conditional"].to_numpy(), (~(df["col2"] < 0)).to_numpy(), (~(df["col2"] < 0)).to_numpy())
    ]
    q = QueryBuilder()
    q = q[where(q["conditional"], ~(q["col2"] < 0), ~(q["col2"] < 0))]
    received = lib.read(symbol, query_builder=q).data
    assert_frame_equal(expected, received)

    # Full/Empty
    expected = df[np.where(df["conditional"].to_numpy(), (~(df["col2"] < 0)).to_numpy(), (df["col2"] < 0).to_numpy())]
    q = QueryBuilder()
    q = q[where(q["conditional"], ~(q["col2"] < 0), q["col2"] < 0)]
    received = lib.read(symbol, query_builder=q).data
    assert_frame_equal(expected, received)

    # Empty/Full
    expected = df[np.where(df["conditional"].to_numpy(), (df["col2"] < 0).to_numpy(), (~(df["col2"] < 0)).to_numpy())]
    q = QueryBuilder()
    q = q[where(q["conditional"], q["col2"] < 0, ~(q["col2"] < 0))]
    received = lib.read(symbol, query_builder=q).data
    assert_frame_equal(expected, received)

    # Empty/Empty
    expected = df[np.where(df["conditional"].to_numpy(), (df["col2"] < 0).to_numpy(), (df["col2"] < 0).to_numpy())]
    q = QueryBuilder()
    q = q[where(q["conditional"], q["col2"] < 0, q["col2"] < 0)]
    received = lib.read(symbol, query_builder=q).data
    assert_frame_equal(expected, received)


def test_filter_ternary_invalid_conditions(lmdb_version_store_v1, any_output_format):
    lib = lmdb_version_store_v1
    lib._set_output_format_for_pipeline_tests(any_output_format)
    symbol = "test_filter_ternary_invalid_conditions"
    # Non-bool column should throw if provided as condition
    df = pd.DataFrame({"conditional": [0]})
    lib.write(symbol, df)

    # Non-bool column
    q = QueryBuilder()
    q = q[where(q["conditional"], q["conditional"] < 0, q["conditional"] >= 0)]
    with pytest.raises(InternalException):
        lib.read(symbol, query_builder=q)

    # Value
    q = QueryBuilder()
    q = q[where(True, q["conditional"] < 0, q["conditional"] >= 0)]
    with pytest.raises(InternalException):
        lib.read(symbol, query_builder=q)


def test_filter_ternary_invalid_arguments(lmdb_version_store_v1, any_output_format):
    lib = lmdb_version_store_v1
    lib._set_output_format_for_pipeline_tests(any_output_format)
    symbol = "test_filter_ternary_invalid_arguments"
    df = pd.DataFrame(
        {"conditional": [True], "col1": [0], "col2": ["hello"]},
    )
    lib.write(symbol, df)

    # Non-bool column as left arg
    q = QueryBuilder()
    q = q[where(q["conditional"], q["col1"], q["conditional"])]
    with pytest.raises(UserInputException):
        lib.read(symbol, query_builder=q)
    # Above reversed
    q = QueryBuilder()
    q = q[where(q["conditional"], q["conditional"], q["col1"])]
    with pytest.raises(UserInputException):
        lib.read(symbol, query_builder=q)

    # Non-bool value as left arg
    q = QueryBuilder()
    q = q[where(q["conditional"], 0, q["col1"] == 0)]
    with pytest.raises(UserInputException):
        lib.read(symbol, query_builder=q)
    # Above reversed
    q = QueryBuilder()
    q = q[where(q["conditional"], q["col1"] == 0, 0)]
    with pytest.raises(UserInputException):
        lib.read(symbol, query_builder=q)

    # Incompatible column types
    q = QueryBuilder()
    q = q[where(q["conditional"], q["col1"], q["col2"])]
    with pytest.raises(UserInputException) as e:
        lib.read(symbol, query_builder=q)

    # Incompatible column/value types
    q = QueryBuilder()
    q = q[where(q["conditional"], q["col1"], "hello")]
    with pytest.raises(UserInputException):
        lib.read(symbol, query_builder=q)

    # Incompatible value types
    q = QueryBuilder()
    q = q[where(q["conditional"], 0, "hello")]
    with pytest.raises(UserInputException):
        lib.read(symbol, query_builder=q)


def test_filter_ternary_pythonic_syntax():
    q = QueryBuilder()
    with pytest.raises(UserInputException):
        q[q["col1"] if q["conditional"] else q["col2"]]


def test_filter_ternary_dynamic_missing_columns(lmdb_version_store_dynamic_schema_v1, any_output_format):
    lib = lmdb_version_store_dynamic_schema_v1
    lib._set_output_format_for_pipeline_tests(any_output_format)
    symbol = "test_filter_ternary_dynamic_missing_columns"
    all_columns_df = pd.DataFrame(
        {
            "conditional": [True, False, True, False],
            "col1": np.array([1, 1, 2, 2], dtype=np.int64),
            "col2": np.array([11, 11, 12, 12], dtype=np.int64),
        },
        index=pd.date_range("2024-01-01", periods=4),
    )
    lib.write(symbol, all_columns_df)

    base_update_df = pd.DataFrame(
        {
            "conditional": [True, False, True, False],
            "col1": np.array([2, 2, 1, 1], dtype=np.int64),
            "col2": np.array([12, 12, 11, 11], dtype=np.int64),
        },
        index=pd.date_range("2024-01-05", periods=4),
    )

    # left column missing with value
    update_df = base_update_df.drop(columns="col1")
    lib.update(symbol, update_df)
    q = QueryBuilder()
    q = q[where(q["conditional"], q["col1"] == 1, True)]
    received = lib.read(symbol, query_builder=q).data
    expected = pd.concat([all_columns_df, update_df]).fillna(0)
    expected["col1"] = expected["col1"].astype("int64")
    expected = expected[np.where(expected["conditional"].to_numpy(), (expected["col1"] == 1).to_numpy(), True)]
    assert_frame_equal(expected, received)

    # right column missing with value
    update_df = base_update_df.drop(columns="col2")
    lib.update(symbol, update_df)
    q = QueryBuilder()
    q = q[where(q["conditional"], False, q["col2"] == 12)]
    received = lib.read(symbol, query_builder=q).data
    expected = pd.concat([all_columns_df, update_df]).fillna(0)
    expected["col2"] = expected["col2"].astype("int64")
    expected = expected[np.where(expected["conditional"].to_numpy(), False, (expected["col2"] == 12).to_numpy())]
    assert_frame_equal(expected, received)

    # conditional column missing
    update_df = base_update_df.drop(columns="conditional")
    lib.update(symbol, update_df)
    q = QueryBuilder()
    q = q[where(q["conditional"], q["col1"] == 1, q["col2"] == 12)]
    received = lib.read(symbol, query_builder=q).data
    expected = pd.concat([all_columns_df, update_df]).fillna(False)
    expected = expected[
        np.where(
            expected["conditional"].to_numpy(), (expected["col1"] == 1).to_numpy(), (expected["col2"] == 12).to_numpy()
        )
    ]
    assert_frame_equal(expected, received)

    # left column missing
    update_df = base_update_df.drop(columns="col1")
    lib.update(symbol, update_df)
    received = lib.read(symbol, query_builder=q).data
    expected = pd.concat([all_columns_df, update_df]).fillna(0)
    expected["col1"] = expected["col1"].astype("int64")
    expected = expected[
        np.where(
            expected["conditional"].to_numpy(), (expected["col1"] == 1).to_numpy(), (expected["col2"] == 12).to_numpy()
        )
    ]
    assert_frame_equal(expected, received)

    # right column missing
    update_df = base_update_df.drop(columns="col2")
    lib.update(symbol, update_df)
    received = lib.read(symbol, query_builder=q).data
    expected = pd.concat([all_columns_df, update_df]).fillna(0)
    expected["col2"] = expected["col2"].astype("int64")
    expected = expected[
        np.where(
            expected["conditional"].to_numpy(), (expected["col1"] == 1).to_numpy(), (expected["col2"] == 12).to_numpy()
        )
    ]
    assert_frame_equal(expected, received)

    # conditional and left column missing
    update_df = base_update_df.drop(columns=["conditional", "col1"])
    lib.update(symbol, update_df)
    received = lib.read(symbol, query_builder=q).data
    expected = pd.concat([all_columns_df, update_df])
    expected["conditional"].fillna(False, inplace=True)
    expected["col1"].fillna(0, inplace=True)
    expected["col1"] = expected["col1"].astype("int64")
    expected = expected[
        np.where(
            expected["conditional"].to_numpy(), (expected["col1"] == 1).to_numpy(), (expected["col2"] == 12).to_numpy()
        )
    ]
    assert_frame_equal(expected, received)

    # conditional and right column missing
    update_df = base_update_df.drop(columns=["conditional", "col2"])
    lib.update(symbol, update_df)
    received = lib.read(symbol, query_builder=q).data
    expected = pd.concat([all_columns_df, update_df])
    expected["conditional"].fillna(False, inplace=True)
    expected["col2"].fillna(0, inplace=True)
    expected["col2"] = expected["col2"].astype("int64")
    expected = expected[
        np.where(
            expected["conditional"].to_numpy(), (expected["col1"] == 1).to_numpy(), (expected["col2"] == 12).to_numpy()
        )
    ]
    assert_frame_equal(
        expected,
        received,
    )

    # left and right column missing
    update_df = base_update_df.drop(columns=["col1", "col2"])
    lib.update(symbol, update_df)
    received = lib.read(symbol, query_builder=q).data
    expected = pd.concat([all_columns_df, update_df]).fillna(0)
    expected["col1"] = expected["col1"].astype("int64")
    expected["col2"] = expected["col2"].astype("int64")
    expected = expected[
        np.where(
            expected["conditional"].to_numpy(), (expected["col1"] == 1).to_numpy(), (expected["col2"] == 12).to_numpy()
        )
    ]
    assert_frame_equal(expected, received)


# TODO: Assert that the projected column is of type float64 after modify_schema change is merged
@use_of_function_scoped_fixtures_in_hypothesis_checked
@settings(deadline=None)
@given(
    df=data_frames(
        columns(
            ["condition", "col1", "col2"],
            elements=strategies.floats(min_value=0, max_value=1000, allow_nan=False, allow_subnormal=False),
            fill=strategies.just(np.nan),
        ),
    ),
)
def test_ternary_hypothesis(lmdb_version_store_v1, df, any_output_format):
    assume(
        not df.empty
        and not df["condition"].isnull().all()
        and not df["col1"].isnull().all()
        and not df["col2"].isnull().all()
    )
    lib = lmdb_version_store_v1
    lib._set_output_format_for_pipeline_tests(any_output_format)
    dense_sym = "test_ternary_hypothesis_dense"
    sparse_sym = "test_ternary_hypothesis_sparse"

    df.index = pd.date_range("2024-01-01", periods=len(df))

    lib.write(dense_sym, df)
    lib.write(sparse_sym, df, sparsify_floats=True)

    # Projection
    # col/col
    expected = df.copy(deep=True)
    expected["projected"] = np.where(
        expected["condition"].isnull().to_numpy(), expected["col1"].to_numpy(), expected["col2"].to_numpy()
    )
    q = QueryBuilder()
    q = q.apply("projected", where(q["condition"].isnull(), q["col1"], q["col2"]))
    assert_frame_equal(expected, lib.read(dense_sym, query_builder=q).data, check_dtype=False)
    assert_frame_equal(expected, lib.read(sparse_sym, query_builder=q).data, check_dtype=False)
    # col/val
    expected = df.copy(deep=True)
    expected["projected"] = np.where(expected["condition"].isnull().to_numpy(), expected["col1"].to_numpy(), 2000.0)
    q = QueryBuilder()
    q = q.apply("projected", where(q["condition"].isnull(), q["col1"], 2000))
    assert_frame_equal(expected, lib.read(dense_sym, query_builder=q).data, check_dtype=False)
    assert_frame_equal(expected, lib.read(sparse_sym, query_builder=q).data, check_dtype=False)
    # val/col
    expected = df.copy(deep=True)
    expected["projected"] = np.where(expected["condition"].isnull().to_numpy(), 2000.0, expected["col2"].to_numpy())
    q = QueryBuilder()
    q = q.apply("projected", where(q["condition"].isnull(), 2000, q["col2"]))
    assert_frame_equal(expected, lib.read(dense_sym, query_builder=q).data, check_dtype=False)
    assert_frame_equal(expected, lib.read(sparse_sym, query_builder=q).data, check_dtype=False)
    # val/val
    expected = df.copy(deep=True)
    expected["projected"] = np.where(expected["condition"].isnull().to_numpy(), 2000.0, 3000.0)
    q = QueryBuilder()
    q = q.apply("projected", where(q["condition"].isnull(), 2000, 3000))
    assert_frame_equal(expected, lib.read(dense_sym, query_builder=q).data, check_dtype=False)
    assert_frame_equal(expected, lib.read(sparse_sym, query_builder=q).data, check_dtype=False)

    # Filters
    # Only test col/col, col/val etc can be achieved more efficiently without using the ternary operator
    expected = df.copy(deep=True)
    expected = expected[
        np.where(
            expected["condition"].isnull().to_numpy(),
            expected["col1"].isnull().to_numpy(),
            expected["col2"].isnull().to_numpy(),
        )
    ]
    q = QueryBuilder()
    q = q[where(q["condition"].isnull(), q["col1"].isnull(), q["col2"].isnull())]
    assert_frame_equal(expected, lib.read(dense_sym, query_builder=q).data, check_dtype=False)
    assert_frame_equal(expected, lib.read(sparse_sym, query_builder=q).data, check_dtype=False)
