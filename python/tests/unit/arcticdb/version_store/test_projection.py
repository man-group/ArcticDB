"""
Copyright 2026 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

import numpy as np
import pandas as pd
import pytest

from arcticdb_ext.exceptions import ArcticException, InternalException, UserInputException
from arcticdb.exceptions import ArcticNativeException
from arcticdb.version_store.processing import QueryBuilder
from arcticdb.util.test import assert_frame_equal, make_dynamic, regularize_dataframe

pytestmark = pytest.mark.pipeline


def _assert_projection_matches(lib, symbol, query_builder, expected):
    received = regularize_dataframe(lib.read(symbol, query_builder=query_builder).data)
    assert_frame_equal(regularize_dataframe(expected), received)


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


def test_projection_modulo_value_and_column_operands(lmdb_version_store_tiny_segment, any_output_format):
    lib = lmdb_version_store_tiny_segment
    lib._set_output_format_for_pipeline_tests(any_output_format)
    symbol = "test_projection_modulo_value_and_column_operands"
    df = pd.DataFrame(
        {
            "a": np.arange(1, 11, dtype=np.int64),
            "b": np.arange(11, 21, dtype=np.int64),
        },
        index=np.arange(10),
    )
    lib.write(symbol, df)

    q = QueryBuilder()
    q = q.apply("a_mod_3", q["a"] % 3)
    q = q.apply("20_mod_a", 20 % q["a"])
    q = q.apply("a_mod_b", q["a"] % q["b"])

    expected = df.copy()
    expected["a_mod_3"] = expected["a"] % 3
    expected["20_mod_a"] = 20 % expected["a"]
    expected["a_mod_b"] = expected["a"] % expected["b"]

    _assert_projection_matches(lib, symbol, q, expected)


def test_projection_modulo_negative_integers_and_floats(lmdb_version_store_tiny_segment, any_output_format):
    lib = lmdb_version_store_tiny_segment
    lib._set_output_format_for_pipeline_tests(any_output_format)
    symbol = "test_projection_modulo_negative_integers_and_floats"
    df = pd.DataFrame(
        {
            "int_col": np.array([-5, -4, -3, 3, 4, 5], dtype=np.int64),
            "float_col": np.array([-5.5, -4.5, -3.5, 3.5, 4.5, 5.5], dtype=np.float64),
        },
        index=np.arange(6),
    )
    lib.write(symbol, df)

    q = QueryBuilder()
    q = q.apply("int_mod_pos", q["int_col"] % 2)
    q = q.apply("int_mod_neg", q["int_col"] % -2)
    q = q.apply("float_mod_pos", q["float_col"] % 2.0)
    q = q.apply("float_mod_neg", q["float_col"] % -2.0)

    expected = df.copy()
    expected["int_mod_pos"] = expected["int_col"] % 2
    expected["int_mod_neg"] = expected["int_col"] % -2
    expected["float_mod_pos"] = expected["float_col"] % 2.0
    expected["float_mod_neg"] = expected["float_col"] % -2.0

    _assert_projection_matches(lib, symbol, q, expected)


def test_projection_modulo_special_float_rhs_values(lmdb_version_store_tiny_segment, any_output_format):
    lib = lmdb_version_store_tiny_segment
    lib._set_output_format_for_pipeline_tests(any_output_format)
    symbol = "test_projection_modulo_special_float_rhs_values"
    df = pd.DataFrame({"float_col": np.array([1.0, -1.0, 2.5, -2.5, np.nan], dtype=np.float64)}, index=np.arange(5))
    lib.write(symbol, df)

    q = QueryBuilder()
    q = q.apply("mod_zero", q["float_col"] % 0.0)
    q = q.apply("mod_nan", q["float_col"] % np.nan)

    expected = df.copy()
    expected["mod_zero"] = expected["float_col"] % 0.0
    expected["mod_nan"] = expected["float_col"] % np.nan

    _assert_projection_matches(lib, symbol, q, expected)


def test_projection_modulo_infinite_rhs_raises():
    q = QueryBuilder()
    with pytest.raises(ArcticException, match="Infinite values not supported in queries"):
        q.apply("mod_inf", q["col"] % np.inf)
    with pytest.raises(ArcticException, match="Infinite values not supported in queries"):
        q.apply("mod_neg_inf", q["col"] % -np.inf)


def test_projection_modulo_integer_by_zero_raises(lmdb_version_store_tiny_segment, any_output_format):
    lib = lmdb_version_store_tiny_segment
    lib._set_output_format_for_pipeline_tests(any_output_format)
    symbol = "test_projection_modulo_integer_by_zero_raises"
    df = pd.DataFrame({"int_col": np.arange(5, dtype=np.int64)}, index=np.arange(5))
    lib.write(symbol, df)

    q = QueryBuilder()
    q = q.apply("mod_zero", q["int_col"] % 0)
    with pytest.raises(UserInputException):
        lib.read(symbol, query_builder=q)


def test_projection_modulo_mixed_type_non_representable_values(lmdb_version_store_tiny_segment, any_output_format):
    lib = lmdb_version_store_tiny_segment
    lib._set_output_format_for_pipeline_tests(any_output_format)
    symbol = "test_projection_modulo_mixed_type_non_representable_values"
    df = pd.DataFrame(
        {
            "u8_nonzero": np.array([10, 255, 1], dtype=np.uint8),
            "i64_large": np.array([300, 301, 302], dtype=np.int64),
            "f64_large": np.array([300.25, 301.25, 302.25], dtype=np.float64),
        },
        index=np.arange(3),
    )
    lib.write(symbol, df)

    q = QueryBuilder()
    q = q.apply("u8_mod_i64", q["u8_nonzero"] % q["i64_large"])
    q = q.apply("i64_mod_u8", q["i64_large"] % q["u8_nonzero"])
    q = q.apply("u8_mod_f64", q["u8_nonzero"] % q["f64_large"])
    q = q.apply("f64_mod_u8", q["f64_large"] % q["u8_nonzero"])

    expected = df.copy()
    expected["u8_mod_i64"] = expected["u8_nonzero"] % expected["i64_large"]
    expected["i64_mod_u8"] = expected["i64_large"] % expected["u8_nonzero"]
    expected["u8_mod_f64"] = expected["u8_nonzero"] % expected["f64_large"]
    expected["f64_mod_u8"] = expected["f64_large"] % expected["u8_nonzero"]

    _assert_projection_matches(lib, symbol, q, expected)


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
        lib.append(symbol, df_slice, write_if_missing=True)

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
