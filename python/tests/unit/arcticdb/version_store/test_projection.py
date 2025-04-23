"""
Copyright 2024 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""
import copy

import numpy as np
import pandas as pd
import pytest

from arcticdb_ext.exceptions import InternalException, SchemaException, UserInputException
from arcticdb import QueryBuilder, where
from arcticdb.util.test import assert_frame_equal, make_dynamic, regularize_dataframe


pytestmark = pytest.mark.pipeline


@pytest.mark.parametrize("lib_type", ["lmdb_version_store_v1", "lmdb_version_store_dynamic_schema_v1"])
def test_project_empty_dataframe(request, lib_type):
    lib = request.getfixturevalue(lib_type)
    df = pd.DataFrame({"a": []})
    q = QueryBuilder()
    q = q.apply("new", q["a"] + 1)
    symbol = "test_project_empty_dataframe"
    lib.write(symbol, df)
    vit = lib.read(symbol, query_builder=q)
    assert vit.data.empty


def test_project_column_not_present(lmdb_version_store_v1):
    lib = lmdb_version_store_v1
    df = pd.DataFrame({"a": np.arange(2)}, index=np.arange(2))
    q = QueryBuilder()
    q = q.apply("new", q["b"] + 1)
    symbol = "test_project_column_not_present"
    lib.write(symbol, df)
    with pytest.raises(InternalException):
        _ = lib.read(symbol, query_builder=q)


def test_project_string_binary_arithmetic(lmdb_version_store_v1):
    lib = lmdb_version_store_v1
    symbol = "test_project_string_arithmetic"
    lib.write(symbol, pd.DataFrame({"col_a": [0], "col_b": ["hello"], "col_c": ["bonjour"]}))
    operands = ["col_a", "col_b", "col_c", "0", 0]
    for lhs in operands:
        for rhs in operands:
            if ((lhs == "col_a" and rhs in ["col_a", 0]) or
                (rhs == "col_a" and lhs in ["col_a", 0]) or
                (lhs in ["0", 0] and rhs in ["0", 0])):
                continue
            q = QueryBuilder()
            q = q.apply("d", (q[lhs] if isinstance(lhs, str) and lhs.startswith("col_") else lhs) + (q[rhs] if isinstance(rhs, str) and rhs.startswith("col_") else rhs))
            with pytest.raises(UserInputException):
                lib.read(symbol, query_builder=q)


def test_project_string_unary_arithmetic(lmdb_version_store_v1):
    lib = lmdb_version_store_v1
    symbol = "test_project_string_unary_arithmetic"
    lib.write(symbol, pd.DataFrame({"a": ["hello"]}))
    q = QueryBuilder()
    q = q.apply("b", abs(q["a"]))
    with pytest.raises(UserInputException):
        lib.read(symbol, query_builder=q)
    q = QueryBuilder()
    q = q.apply("b", -q["a"])
    with pytest.raises(UserInputException):
        lib.read(symbol, query_builder=q)


def test_project_ternary_condition_as_full_and_empty_result(lmdb_version_store_v1):
    lib = lmdb_version_store_v1
    symbol = "test_project_ternary_condition_as_full_and_empty_result"
    df = pd.DataFrame(
        {
            "conditional": [0] * 6,
            "col1": np.arange(6),
            "col2": np.arange(10, 16),
        },
        index=pd.date_range("2024-01-01", periods=6)
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


def test_project_ternary_column_column_numeric(lmdb_version_store_v1):
    lib = lmdb_version_store_v1
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
        index=pd.date_range("2024-01-01", periods=6)
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


def test_project_ternary_column_column_dynamic_strings(lmdb_version_store_v1):
    lib = lmdb_version_store_v1
    symbol = "test_project_ternary_column_column_dynamic_strings"
    df = pd.DataFrame(
        {
            "conditional": [True, False, False, True, False, True],
            "col1": ["a", "b", "c", None, "e", "f"],
            "col2": ["g", "h", "i", "j", np.nan, "l"],
        },
        index=pd.date_range("2024-01-01", periods=6)
    )
    lib.write(symbol, df)

    expected = df
    expected["new_col"] = np.where(df["conditional"].to_numpy(), df["col1"].to_numpy(), df["col2"].to_numpy())
    q = QueryBuilder()
    q = q.apply("new_col", where(q["conditional"], q["col1"], q["col2"]))
    received = lib.read(symbol, query_builder=q).data
    assert_frame_equal(expected, received)


def test_project_ternary_fixed_width_strings(version_store_factory):
    lib = version_store_factory(dynamic_strings=False)
    symbol = "test_project_ternary_fixed_width_strings"
    df = pd.DataFrame(
        {
            "conditional": [True, False, False, True, False, True],
            "width_1": ["a", "b", "c", "d", "e", "f"],
            "width_2": ["gg", "hh", "ii", "jj", "kk", "ll"],
        },
        index=pd.date_range("2024-01-01", periods=6)
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


def test_project_ternary_column_value_numeric(lmdb_version_store_v1):
    lib = lmdb_version_store_v1
    symbol = "test_project_ternary_column_value_numeric"
    df = pd.DataFrame(
        {
            "conditional": [True, False, False, True, False, True],
            "col1": np.arange(6),
        },
        index=pd.date_range("2024-01-01", periods=6)
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


def test_project_ternary_column_value_strings(lmdb_version_store_v1):
    lib = lmdb_version_store_v1
    symbol = "test_project_ternary_column_value_strings"
    df = pd.DataFrame(
        {
            "conditional": [True, False, False, True, False, True],
            "col1": ["a", "b", "c", "d", "e", "f"],
        },
        index=pd.date_range("2024-01-01", periods=6)
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


def test_project_ternary_value_value_numeric(lmdb_version_store_v1):
    lib = lmdb_version_store_v1
    symbol = "test_project_ternary_value_value_numeric"
    df = pd.DataFrame(
        {
            "conditional": [True, False, False, True, False, True],
        },
        index=pd.date_range("2024-01-01", periods=6)
    )
    lib.write(symbol, df)

    expected = df
    expected["new_col"] = np.where(df["conditional"].to_numpy(), 0, 1)
    q = QueryBuilder()
    q = q.apply("new_col", where(q["conditional"], 0, 1))
    received = lib.read(symbol, query_builder=q).data
    assert_frame_equal(expected, received, check_dtype=False)


def test_project_ternary_value_value_string(lmdb_version_store_v1):
    lib = lmdb_version_store_v1
    symbol = "test_project_ternary_value_value_string"
    df = pd.DataFrame(
        {
            "conditional": [True, False, False, True, False, True],
        },
        index=pd.date_range("2024-01-01", periods=6)
    )
    lib.write(symbol, df)

    expected = df
    expected["new_col"] = np.where(df["conditional"].to_numpy(), "hello", "goodbye")
    q = QueryBuilder()
    q = q.apply("new_col", where(q["conditional"], "hello", "goodbye"))
    received = lib.read(symbol, query_builder=q).data
    assert_frame_equal(expected, received, check_dtype=False)


@pytest.mark.parametrize(
    "index",
    [
        None,
        pd.MultiIndex.from_arrays(
            [3 * [pd.Timestamp(0)] + 3 * [pd.Timestamp(1)], [0, 1, 2, 0, 1, 2]],
            names=["datetime", "level"]
        )
    ]
)
def test_project_ternary_column_sliced(version_store_factory, index):
    # Cannot use lmdb_version_store_tiny_segment as it has fixed-width strings, which are not supported with the ternary
    # operator
    lib = version_store_factory(dynamic_strings=True, column_group_size=2, segment_row_size=2)
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
            "num_1": np.arange(0, 6),
            "num_2": np.arange(10, 16),
            "str_1": ["one", "two", "three", "four", "five", "six"],
            "str_2": ["eleven", "twelve", "thirteen", "fourteen", "fifteen", "sixteen"],
        },
        index=index
    )
    lib.write(symbol, df)

    # Numeric
    expected = df
    expected["new_col"] = np.where(df["conditional"].to_numpy(), df["num_1"].to_numpy(), df["num_2"].to_numpy())
    q = QueryBuilder()
    q = q.apply("new_col", where(q["conditional"], q["num_1"], q["num_2"]))
    received = lib.read(symbol, query_builder=q).data
    assert_frame_equal(expected, received, check_dtype=False)

    # String
    expected = df
    expected["new_col"] = np.where(df["conditional"].to_numpy(), df["str_1"].to_numpy(), df["str_2"].to_numpy())
    q = QueryBuilder()
    q = q.apply("new_col", where(q["conditional"], q["str_1"], q["str_2"]))
    received = lib.read(symbol, query_builder=q).data
    assert_frame_equal(expected, received, check_dtype=False)


def test_docstring_example_query_builder_apply(lmdb_version_store_v1):
    lib = lmdb_version_store_v1
    df = pd.DataFrame(
        {
            "VWAP": np.arange(0, 10, dtype=np.float64),
            "ASK": np.arange(10, 20, dtype=np.uint16),
            "VOL_ACC": np.arange(20, 30, dtype=np.int32),
        },
        index=np.arange(10),
    )

    lib.write("expression", df)

    q = QueryBuilder()
    q = q.apply("ADJUSTED", q["ASK"] * q["VOL_ACC"] + 7)
    data = lib.read("expression", query_builder=q).data

    df["ADJUSTED"] = df["ASK"] * df["VOL_ACC"] + 7
    assert_frame_equal(df.astype({"ADJUSTED": "int64"}), data)


##################################
# DYNAMIC SCHEMA TESTS FROM HERE #
##################################


def test_project_dynamic(lmdb_version_store_dynamic_schema_v1):
    lib = lmdb_version_store_dynamic_schema_v1
    symbol = "test_project_dynamic"

    df = pd.DataFrame(
        {
            "VWAP": np.arange(0, 10, dtype=np.float64),
            "ASK": np.arange(10, 20, dtype=np.uint16),
            "ACVOL": np.arange(20, 30, dtype=np.int32),
        },
        index=np.arange(10),
    )

    expected, slices = make_dynamic(df)
    for df_slice in slices:
        lib.append(symbol, df_slice, write_if_missing=True)

    q = QueryBuilder()
    q = q.apply("ADJUSTED", q["ASK"] * q["ACVOL"] + 7)
    vit = lib.read(symbol, query_builder=q)

    expected["ADJUSTED"] = expected["ASK"] * expected["ACVOL"] + 7
    received = regularize_dataframe(vit.data)
    expected = regularize_dataframe(expected)
    assert_frame_equal(expected, received)


def test_project_column_types_changing_and_missing(lmdb_version_store_dynamic_schema):
    lib = lmdb_version_store_dynamic_schema
    symbol = "test_project_column_types_changing_and_missing"
    # Floats
    expected = pd.DataFrame({"col_to_project": [0.5, 1.5], "data_col": [0, 1]}, index=np.arange(0, 2))
    lib.write(symbol, expected)
    # uint8
    df = pd.DataFrame({"col_to_project": np.arange(2, dtype=np.uint8), "data_col": [2, 3]}, index=np.arange(2, 4))
    lib.append(symbol, df)
    expected = pd.concat((expected, df))
    # Missing
    df = pd.DataFrame({"data_col": [4, 5]}, index=np.arange(4, 6))
    lib.append(symbol, df)
    expected = pd.concat((expected, df))
    # int16
    df = pd.DataFrame(
        {"col_to_project": np.arange(200, 202, dtype=np.int16), "data_col": [6, 7]}, index=np.arange(6, 8)
    )
    lib.append(symbol, df)

    expected = pd.concat((expected, df))
    expected["projected_col"] = expected["col_to_project"] * 2
    q = QueryBuilder()
    q = q.apply("projected_col", q["col_to_project"] * 2)
    received = lib.read(symbol, query_builder=q).data
    assert_frame_equal(expected, received)


def test_project_ternary_dynamic_missing_columns(lmdb_version_store_dynamic_schema_v1):
    lib = lmdb_version_store_dynamic_schema_v1
    symbol = "test_project_ternary_dynamic_missing_columns"
    all_columns_df = pd.DataFrame(
        {
            "conditional": [True, False],
            "col1": np.arange(2),
            "col2": np.arange(10, 12),
        },
        index=pd.date_range("2024-01-01", periods=2),
    )
    lib.write(symbol, all_columns_df)

    base_update_df = pd.DataFrame(
        {
            "conditional": [True, False],
            "col1": np.arange(2, 4),
            "col2": np.arange(12, 14),
        },
        index=pd.date_range("2024-01-03", periods=2),
    )

    # conditional column missing
    update_df = base_update_df.drop(columns="conditional")
    lib.update(symbol, update_df)
    q = QueryBuilder()
    q = q.apply("new_col", where(q["conditional"], q["col1"], q["col2"]))
    received = lib.read(symbol, query_builder=q).data
    expected = pd.concat([all_columns_df, update_df]).fillna(False)
    expected["new_col"] = np.where(expected["conditional"].to_numpy(), expected["col1"].to_numpy(), expected["col2"].to_numpy())
    assert_frame_equal(expected, received, check_dtype=False)

    # left column missing
    update_df = base_update_df.drop(columns="col1")
    lib.update(symbol, update_df)
    q = QueryBuilder()
    q = q.apply("new_col", where(q["conditional"], q["col1"], q["col2"]))
    with pytest.raises(UserInputException):
        lib.read(symbol, query_builder=q)

    # right column missing
    update_df = base_update_df.drop(columns="col2")
    lib.update(symbol, update_df)
    q = QueryBuilder()
    q = q.apply("new_col", where(q["conditional"], q["col1"], q["col2"]))
    with pytest.raises(UserInputException):
        lib.read(symbol, query_builder=q)

    # conditional and left columns missing
    update_df = base_update_df.drop(columns=["conditional", "col1"])
    lib.update(symbol, update_df)
    q = QueryBuilder()
    q = q.apply("new_col", where(q["conditional"], q["col1"], q["col2"]))
    received = lib.read(symbol, query_builder=q).data
    expected = pd.concat([all_columns_df, update_df])
    expected["conditional"].fillna(False, inplace=True)
    expected["col1"].fillna(0, inplace=True)
    expected["new_col"] = np.where(expected["conditional"].to_numpy(), expected["col1"].to_numpy(), expected["col2"].to_numpy())
    assert_frame_equal(expected, received, check_dtype=False)

    # conditional and right columns missing
    update_df = base_update_df.drop(columns=["conditional", "col2"])
    lib.update(symbol, update_df)
    q = QueryBuilder()
    q = q.apply("new_col", where(q["conditional"], q["col1"], q["col2"]))
    received = lib.read(symbol, query_builder=q).data
    expected = pd.concat([all_columns_df, update_df])
    expected["conditional"].fillna(False, inplace=True)
    expected["col2"].fillna(0, inplace=True)
    expected["new_col"] = np.where(expected["conditional"].to_numpy(), expected["col1"].to_numpy(), expected["col2"].to_numpy())
    assert_frame_equal(expected, received, check_dtype=False)

    # left and right columns missing
    update_df = base_update_df.drop(columns=["col1", "col2"])
    lib.update(symbol, update_df)
    q = QueryBuilder()
    q = q.apply("new_col", where(q["conditional"], q["col1"], q["col2"]))
    received = lib.read(symbol, query_builder=q).data
    expected = pd.concat([all_columns_df, update_df]).fillna(0)
    expected["new_col"] = np.where(expected["conditional"].to_numpy(), expected["col1"].to_numpy(), expected["col2"].to_numpy())
    assert_frame_equal(expected, received, check_dtype=False)
