"""
Copyright 2024 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""
import numpy as np
import pandas as pd
import pytest

from arcticdb_ext.exceptions import InternalException, UserInputException
from arcticdb.exceptions import ArcticNativeException
from arcticdb.version_store.processing import QueryBuilder
from arcticdb.util.test import assert_frame_equal, make_dynamic, regularize_dataframe


pytestmark = pytest.mark.pipeline


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


@pytest.mark.parametrize("index", [None, pd.date_range("2025-01-01", periods=3)])
@pytest.mark.parametrize("value", [5, "hello"])
def test_project_fixed_value(lmdb_version_store_tiny_segment, index, value):
    lib = lmdb_version_store_tiny_segment
    sym = "test_project_fixed_value"
    df = pd.DataFrame({"col1": [0, 1, 2], "col2": [3, 4, 5], "col3": [6, 7, 8]}, index=index)
    lib.write(sym, df)
    df["new_col"] = value
    q = QueryBuilder().apply("new_col", value)
    received = lib.read(sym, query_builder=q).data
    assert_frame_equal(df, received, check_dtype=False)


def test_project_value_set():
    with pytest.raises(ArcticNativeException):
        QueryBuilder().apply("new_col", [0, 1, 2])


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


@pytest.mark.parametrize("index", [None, "timeseries"])
@pytest.mark.parametrize("value", [5, "hello"])
def test_project_fixed_value_dynamic(lmdb_version_store_dynamic_schema_v1, index, value):
    lib = lmdb_version_store_dynamic_schema_v1
    sym = "test_project_fixed_value_dynamic"
    df0 = pd.DataFrame({"col1": [0, 0.1, 0.2], "col2": [0.3, 0.4, 0.5]}, index=pd.date_range("2025-01-01", periods=3) if index == "timeseries" else None)
    df1 = pd.DataFrame({"col2": [0.6, 0.7, 0.8]}, index=pd.date_range("2025-01-04", periods=3) if index == "timeseries" else None)
    lib.write(sym, df0)
    lib.append(sym, df1)
    expected = pd.concat([df0, df1])
    expected["new_col"] = value
    if index is None:
        expected.index = pd.RangeIndex(6)
    q = QueryBuilder().apply("new_col", value)
    received = lib.read(sym, query_builder=q).data
    assert_frame_equal(expected, received, check_dtype=False)
