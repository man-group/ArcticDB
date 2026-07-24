"""
Copyright 2026 Man Group Operations Limited

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


def test_project_column_not_present(lmdb_version_store_v1, any_output_format):
    lib = lmdb_version_store_v1
    lib._set_output_format_for_pipeline_tests(any_output_format)
    df = pd.DataFrame({"a": np.arange(2)}, index=np.arange(2))
    q = QueryBuilder()
    q = q.apply("new", q["b"] + 1)
    symbol = "test_project_column_not_present"
    lib.write(symbol, df)
    with pytest.raises(InternalException):
        _ = lib.read(symbol, query_builder=q)


def test_project_string_binary_arithmetic(lmdb_version_store_v1, any_output_format):
    lib = lmdb_version_store_v1
    lib._set_output_format_for_pipeline_tests(any_output_format)
    symbol = "test_project_string_arithmetic"
    lib.write(symbol, pd.DataFrame({"col_a": [0], "col_b": ["hello"], "col_c": ["bonjour"]}))
    operands = ["col_a", "col_b", "col_c", "0", 0]
    for lhs in operands:
        for rhs in operands:
            if (
                (lhs == "col_a" and rhs in ["col_a", 0])
                or (rhs == "col_a" and lhs in ["col_a", 0])
                or (lhs in ["0", 0] and rhs in ["0", 0])
            ):
                continue
            q = QueryBuilder()
            q = q.apply(
                "d",
                (q[lhs] if isinstance(lhs, str) and lhs.startswith("col_") else lhs)
                + (q[rhs] if isinstance(rhs, str) and rhs.startswith("col_") else rhs),
            )
            with pytest.raises(UserInputException):
                lib.read(symbol, query_builder=q)


def test_project_string_unary_arithmetic(lmdb_version_store_v1, any_output_format):
    lib = lmdb_version_store_v1
    lib._set_output_format_for_pipeline_tests(any_output_format)
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
def test_project_fixed_value(lmdb_version_store_tiny_segment, index, value, any_output_format):
    lib = lmdb_version_store_tiny_segment
    lib._set_output_format_for_pipeline_tests(any_output_format)
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


def test_docstring_example_query_builder_apply(lmdb_version_store_v1, any_output_format):
    lib = lmdb_version_store_v1
    lib._set_output_format_for_pipeline_tests(any_output_format)
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


###############################################
# POW OPERATOR TESTS                          #
###############################################


@pytest.mark.parametrize(
    "base_dtype, exp_dtype, result_dtype",
    [
        pytest.param(np.uint16, np.uint8, np.uint64, id="uint_uint"),  # uint ^ uint -> uint64
        pytest.param(np.int32, np.uint16, np.int64, id="int_uint"),  # int ^ uint -> int64
        pytest.param(np.uint16, np.int32, np.float64, id="uint_int"),  # uint ^ int -> float64 (negative exp)
        pytest.param(np.int32, np.int16, np.float64, id="int_int"),  # int ^ int -> float64 (negative exp)
    ],
)
def test_project_pow_col_col(lmdb_version_store_v1, any_output_format, base_dtype, exp_dtype, result_dtype):
    lib = lmdb_version_store_v1
    lib._set_output_format_for_pipeline_tests(any_output_format)
    base_vals = np.arange(1, 11) if np.issubdtype(base_dtype, np.unsignedinteger) else np.arange(-5, 5)
    exp_vals = np.arange(1, 11) if np.issubdtype(exp_dtype, np.unsignedinteger) else np.arange(-5, 5)
    df = pd.DataFrame(
        {"BASE": base_vals.astype(base_dtype), "EXP": exp_vals.astype(exp_dtype)},
        index=np.arange(10),
    )
    lib.write("test_project_pow_col_col", df)

    q = QueryBuilder()
    q = q.apply("RESULT", q["BASE"] ** q["EXP"])
    data = lib.read("test_project_pow_col_col", query_builder=q).data

    df["RESULT"] = df["BASE"].astype(result_dtype) ** df["EXP"].astype(result_dtype)
    assert_frame_equal(df, data)


def test_project_pow_col_value(lmdb_version_store_v1, any_output_format):
    # Column ^ scalar value, exercising all four promotion combinations
    lib = lmdb_version_store_v1
    lib._set_output_format_for_pipeline_tests(any_output_format)
    df = pd.DataFrame(
        {
            "UINT_COL": np.arange(1, 11, dtype=np.uint16),
            "INT_COL": np.arange(-5, 5, dtype=np.int32),
        },
        index=np.arange(10),
    )
    lib.write("pow_col_value", df)

    q = QueryBuilder()
    q = q.apply("UINT_POW_UINT_VAL", q["UINT_COL"] ** np.uint8(3))
    q = q.apply("UINT_POW_INT_VAL", q["UINT_COL"] ** np.int8(2))
    q = q.apply("INT_POW_UINT_VAL", q["INT_COL"] ** np.uint8(3))
    q = q.apply("INT_POW_INT_VAL", q["INT_COL"] ** np.int8(2))
    data = lib.read("pow_col_value", query_builder=q).data

    df["UINT_POW_UINT_VAL"] = df["UINT_COL"].astype(np.uint64) ** np.uint64(3)
    df["UINT_POW_INT_VAL"] = df["UINT_COL"].astype(np.float64) ** np.float64(2)
    df["INT_POW_UINT_VAL"] = df["INT_COL"].astype(np.int64) ** np.int64(3)
    df["INT_POW_INT_VAL"] = df["INT_COL"].astype(np.float64) ** np.float64(2)

    assert_frame_equal(df, data)


def test_project_pow_float_base(lmdb_version_store_v1, any_output_format):
    # float ^ uint -> float64, float ^ int -> float64
    lib = lmdb_version_store_v1
    lib._set_output_format_for_pipeline_tests(any_output_format)
    df = pd.DataFrame(
        {
            "BASE": np.arange(1.0, 11.0, dtype=np.float64),
            "UINT_EXP": np.arange(1, 11, dtype=np.uint8),
            "INT_EXP": np.arange(-5, 5, dtype=np.int32),
        },
        index=np.arange(10),
    )
    lib.write("pow_float_base", df)

    q = QueryBuilder()
    q = q.apply("FLOAT_POW_UINT", q["BASE"] ** q["UINT_EXP"])
    q = q.apply("FLOAT_POW_INT", q["BASE"] ** q["INT_EXP"])
    data = lib.read("pow_float_base", query_builder=q).data

    df["FLOAT_POW_UINT"] = df["BASE"].astype(np.float64) ** df["UINT_EXP"].astype(np.float64)
    df["FLOAT_POW_INT"] = df["BASE"].astype(np.float64) ** df["INT_EXP"].astype(np.float64)
    assert_frame_equal(df, data)


def test_project_pow_string_raises(lmdb_version_store_v1, any_output_format):
    # Pow with string operand should raise UserInputException (matching other arithmetic ops)
    lib = lmdb_version_store_v1
    lib._set_output_format_for_pipeline_tests(any_output_format)
    symbol = "test_project_pow_string_raises"
    lib.write(symbol, pd.DataFrame({"num": [1, 2, 3], "s": ["a", "b", "c"]}))

    q = QueryBuilder()
    q = q.apply("result", q["num"] ** q["s"])
    with pytest.raises(UserInputException):
        lib.read(symbol, query_builder=q)

    q = QueryBuilder()
    q = q.apply("result", q["s"] ** q["num"])
    with pytest.raises(UserInputException):
        lib.read(symbol, query_builder=q)


def test_project_pow_float_exponent_raises(lmdb_version_store_v1, any_output_format):
    lib = lmdb_version_store_v1
    lib._set_output_format_for_pipeline_tests(any_output_format)
    symbol = "test_project_pow_float_exponent_raises"
    lib.write(
        symbol,
        pd.DataFrame({"num": np.array([1, 2, 3], dtype=np.int32), "f": np.array([1.0, 2.0, 3.0], dtype=np.float64)}),
    )

    q = QueryBuilder()
    q = q.apply("result", q["num"] ** q["f"])
    with pytest.raises(UserInputException):
        lib.read(symbol, query_builder=q)

    q = QueryBuilder()
    q = q.apply("result", q["num"] ** np.float64(2.0))
    with pytest.raises(UserInputException):
        lib.read(symbol, query_builder=q)


def test_project_pow_dynamic(lmdb_version_store_dynamic_schema_v1, any_output_format):
    # Pow under dynamic schema, analogous to test_project_dynamic
    lib = lmdb_version_store_dynamic_schema_v1
    lib._set_output_format_for_pipeline_tests(any_output_format)
    symbol = "test_project_pow_dynamic"

    df = pd.DataFrame(
        {
            "BASE": np.arange(1, 11, dtype=np.uint16),
            "EXP": np.arange(1, 11, dtype=np.uint8),
        },
        index=np.arange(10),
    )

    expected, slices = make_dynamic(df)
    for df_slice in slices:
        lib.append(symbol, df_slice)

    q = QueryBuilder()
    q = q.apply("RESULT", q["BASE"] ** q["EXP"])
    vit = lib.read(symbol, query_builder=q)

    expected["RESULT"] = expected["BASE"].astype(np.float64) ** expected["EXP"].astype(np.float64)
    # ArcticDB propagates NaN for sparse (missing) column values; IEEE 754 has special cases like
    # 1^NaN=1 and x^0=1 that pandas uses but ArcticDB does not (it treats missing as NULL).
    expected.loc[expected["BASE"].isna() | expected["EXP"].isna(), "RESULT"] = np.nan
    received = regularize_dataframe(vit.data)
    expected = regularize_dataframe(expected)
    assert_frame_equal(expected, received)


###############################################
# TIMESTAMP / NUMERIC TYPE MISMATCH TESTS    #
###############################################


@pytest.mark.parametrize("dtype", (np.int64, np.uint64, np.float64, np.float32))
@pytest.mark.parametrize("op", ("__add__", "__sub__", "__mul__", "__truediv__"))
@pytest.mark.xfail(reason="Bug - see 8065794446")
def test_project_datetime_col_with_numeric_scalar(dtype, op, lmdb_version_store_v1, any_output_format):
    lib = lmdb_version_store_v1
    lib._set_output_format_for_pipeline_tests(any_output_format)
    symbol = "sym"
    df = pd.DataFrame({"a": [pd.Timestamp(0), pd.Timestamp(1)]})
    lib.write(symbol, df)
    value = dtype(1)
    q = QueryBuilder()
    q = q.apply("result", getattr(q["a"], op)(value))
    with pytest.raises(UserInputException):
        lib.read(symbol, query_builder=q)


@pytest.mark.parametrize("dtype", (np.int64, np.uint64, np.float64, np.float32))
@pytest.mark.parametrize("op", ("__add__", "__sub__", "__mul__", "__truediv__"))
@pytest.mark.xfail(reason="Bug - see 8065794446")
def test_project_numeric_col_with_datetime_scalar(dtype, op, lmdb_version_store_v1, any_output_format):
    lib = lmdb_version_store_v1
    lib._set_output_format_for_pipeline_tests(any_output_format)
    symbol = "sym"
    df = pd.DataFrame({"a": np.array([0, 1], dtype=dtype)})
    lib.write(symbol, df)
    value = pd.Timestamp(1)
    q = QueryBuilder()
    q = q.apply("result", getattr(q["a"], op)(value))
    with pytest.raises(UserInputException):
        lib.read(symbol, query_builder=q)


@pytest.mark.parametrize("dtype", (np.int64, np.uint64, np.float64, np.float32))
@pytest.mark.parametrize("op", ("__add__", "__sub__", "__mul__", "__truediv__"))
@pytest.mark.xfail(reason="Bug - see 8065794446")
def test_project_datetime_col_with_numeric_col(dtype, op, lmdb_version_store_v1, any_output_format):
    lib = lmdb_version_store_v1
    lib._set_output_format_for_pipeline_tests(any_output_format)
    symbol = "sym"
    df = pd.DataFrame({"a": [pd.Timestamp(0), pd.Timestamp(1)], "b": np.array([0, 1], dtype=dtype)})
    lib.write(symbol, df)
    q = QueryBuilder()
    q = q.apply("result", getattr(q["a"], op)(q["b"]))
    with pytest.raises(UserInputException):
        lib.read(symbol, query_builder=q)


@pytest.mark.parametrize("dtype", (np.int64, np.uint64, np.float64, np.float32))
@pytest.mark.parametrize("op", ("__add__", "__sub__", "__mul__", "__truediv__"))
@pytest.mark.xfail(reason="Bug - see 8065794446")
def test_project_numeric_col_with_datetime_col(dtype, op, lmdb_version_store_v1, any_output_format):
    lib = lmdb_version_store_v1
    lib._set_output_format_for_pipeline_tests(any_output_format)
    symbol = "sym"
    df = pd.DataFrame({"a": np.array([0, 1], dtype=dtype), "b": [pd.Timestamp(0), pd.Timestamp(1)]})
    lib.write(symbol, df)
    q = QueryBuilder()
    q = q.apply("result", getattr(q["a"], op)(q["b"]))
    with pytest.raises(UserInputException):
        lib.read(symbol, query_builder=q)


@pytest.mark.xfail(reason="Bug - see 8065794446")
def test_project_abs_datetime_col(lmdb_version_store_v1, any_output_format):
    lib = lmdb_version_store_v1
    lib._set_output_format_for_pipeline_tests(any_output_format)
    symbol = "sym"
    df = pd.DataFrame({"a": [pd.Timestamp(0), pd.Timestamp(1)]})
    lib.write(symbol, df)
    q = QueryBuilder()
    q = q.apply("result", abs(q["a"]))
    with pytest.raises(UserInputException):
        lib.read(symbol, query_builder=q)


@pytest.mark.xfail(reason="Bug - see 8065794446")
def test_project_neg_datetime_col(lmdb_version_store_v1, any_output_format):
    lib = lmdb_version_store_v1
    lib._set_output_format_for_pipeline_tests(any_output_format)
    symbol = "sym"
    df = pd.DataFrame({"a": [pd.Timestamp(0), pd.Timestamp(1)]})
    lib.write(symbol, df)
    q = QueryBuilder()
    q = q.apply("result", -q["a"])
    with pytest.raises(UserInputException):
        lib.read(symbol, query_builder=q)


##################################
# DYNAMIC SCHEMA TESTS FROM HERE #
##################################


def test_project_dynamic(lmdb_version_store_dynamic_schema_v1, any_output_format):
    lib = lmdb_version_store_dynamic_schema_v1
    lib._set_output_format_for_pipeline_tests(any_output_format)
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
        lib.append(symbol, df_slice)

    q = QueryBuilder()
    q = q.apply("ADJUSTED", q["ASK"] * q["ACVOL"] + 7)
    vit = lib.read(symbol, query_builder=q)

    expected["ADJUSTED"] = expected["ASK"] * expected["ACVOL"] + 7
    received = regularize_dataframe(vit.data)
    expected = regularize_dataframe(expected)
    assert_frame_equal(expected, received)


def test_project_column_types_changing_and_missing(lmdb_version_store_dynamic_schema, any_output_format):
    lib = lmdb_version_store_dynamic_schema
    lib._set_output_format_for_pipeline_tests(any_output_format)
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
def test_project_fixed_value_dynamic(lmdb_version_store_dynamic_schema_v1, index, value, any_output_format):
    lib = lmdb_version_store_dynamic_schema_v1
    lib._set_output_format_for_pipeline_tests(any_output_format)
    sym = "test_project_fixed_value_dynamic"
    df0 = pd.DataFrame(
        {"col1": [0, 0.1, 0.2], "col2": [0.3, 0.4, 0.5]},
        index=pd.date_range("2025-01-01", periods=3) if index == "timeseries" else None,
    )
    df1 = pd.DataFrame(
        {"col2": [0.6, 0.7, 0.8]}, index=pd.date_range("2025-01-04", periods=3) if index == "timeseries" else None
    )
    lib.write(sym, df0)
    lib.append(sym, df1)
    expected = pd.concat([df0, df1])
    expected["new_col"] = value
    if index is None:
        expected.index = pd.RangeIndex(6)
    q = QueryBuilder().apply("new_col", value)
    received = lib.read(sym, query_builder=q).data
    assert_frame_equal(expected, received, check_dtype=False)


def test_projection_repeated_subexpression(lmdb_version_store_v1):
    lib = lmdb_version_store_v1
    sym = "test_projection_repeated_subexpression"
    df = pd.DataFrame({"a": np.arange(10, dtype=np.int64), "b": np.arange(10, 20, dtype=np.int64)})
    lib.write(sym, df)

    q = QueryBuilder()
    q = q.apply("x", (q["a"] + q["b"]) * (q["a"] + q["b"]))

    expected = df.copy()
    expected["x"] = (df["a"] + df["b"]) * (df["a"] + df["b"])
    received = lib.read(sym, query_builder=q).data
    assert_frame_equal(expected, received)
