"""
Copyright 2024 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""
from datetime import datetime
from itertools import cycle
import math
import numpy as np
from packaging.version import Version
import pandas as pd
import pytest
from pytz import timezone
import random
import string
import sys

from arcticdb.exceptions import ArcticNativeException
from arcticdb.version_store.processing import QueryBuilder
from arcticdb_ext.exceptions import InternalException, UserInputException
from arcticdb.util.test import (
    assert_frame_equal,
    config_context,
    get_wide_dataframe,
    make_dynamic,
    regularize_dataframe,
    DYNAMIC_STRINGS_SUFFIX,
    FIXED_STRINGS_SUFFIX,
    generic_filter_test,
    generic_filter_test_strings,
    generic_filter_test_nans,
)
from arcticdb.util._versions import IS_PANDAS_TWO, PANDAS_VERSION, IS_NUMPY_TWO


pytestmark = pytest.mark.pipeline


def test_filter_column_not_present(lmdb_version_store_v1):
    lib = lmdb_version_store_v1
    df = pd.DataFrame({"a": np.arange(2)}, index=np.arange(2))
    q = QueryBuilder()
    q = q[q["b"] < 5]
    symbol = "test_filter_column_not_present"
    lib.write(symbol, df)
    with pytest.raises(InternalException):
        _ = lib.read(symbol, query_builder=q)


def test_filter_column_attribute_syntax(lmdb_version_store_v1):
    lib = lmdb_version_store_v1
    symbol = "test_filter_column_attribute_syntax"
    df = pd.DataFrame({"a": [np.uint8(1), np.uint8(0)]})
    lib.write(symbol, df)
    q = QueryBuilder()
    q = q[q.a < np.uint8(1)]
    expected = df[df["a"] < np.uint8(1)]
    generic_filter_test(lib, symbol, q, expected)


def test_filter_infinite_value():
    q = QueryBuilder()
    with pytest.raises(ArcticNativeException):
        q = q[q["a"] < math.inf]


def test_filter_categorical(lmdb_version_store_v1):
    lib = lmdb_version_store_v1
    df = pd.DataFrame({"a": ["hello", "hi", "hello"]}, index=np.arange(3))
    df.a = df.a.astype("category")
    q = QueryBuilder()
    q = q[q.a == "hi"]
    symbol = "test_filter_categorical"
    lib.write(symbol, df)
    with pytest.raises(UserInputException):
        _ = lib.read(symbol, query_builder=q)


def test_filter_date_range_row_indexed(lmdb_version_store_tiny_segment):
    lib = lmdb_version_store_tiny_segment
    symbol = "test_filter_date_range_row_indexed"
    df = pd.DataFrame({"a": np.arange(3)}, index=np.arange(3))
    lib.write(symbol, df)
    with pytest.raises(InternalException):
        lib.read(symbol, date_range=(pd.Timestamp("2000-01-01"), pd.Timestamp("2000-01-02")))


def test_filter_explicit_index(lmdb_version_store_v1):
    lib = lmdb_version_store_v1
    df = pd.DataFrame({"a": [np.uint8(1), np.uint8(0)]}, index=np.arange(2))
    q = QueryBuilder()
    q = q[q["a"] < np.uint8(1)]
    pandas_query = "a < 1"
    symbol = "test_filter_explicit_index"
    lib.write(symbol, df)
    assert_frame_equal(df.query(pandas_query), lib.read(symbol, query_builder=q).data)


def test_filter_clashing_values(lmdb_version_store_v1):
    lib = lmdb_version_store_v1
    base_symbol = "test_filter_clashing_values"
    df = pd.DataFrame({"a": [10, 11, 12], "b": ["11", "12", "13"]}, index=np.arange(3))
    lib.write(f"{base_symbol}_{DYNAMIC_STRINGS_SUFFIX}", df, dynamic_strings=True)
    lib.write(f"{base_symbol}_{FIXED_STRINGS_SUFFIX}", df, dynamic_strings=False)
    q = QueryBuilder()
    q = q[(q.a == 11) | (q.b == "11")]
    expected = df[(df["a"] == 11) | (df["b"] == "11")]
    generic_filter_test_strings(lib, base_symbol, q, expected)


def test_filter_bool_nonbool_comparison(lmdb_version_store_v1):
    symbol = "test_filter_bool_nonbool_comparison"
    lib = lmdb_version_store_v1
    df = pd.DataFrame({"string": ["True", "False"], "numeric": [1, 0], "bool": [True, False]}, index=np.arange(2))
    lib.write(symbol, df)

    # bool column to string column
    q = QueryBuilder()
    q = q[q["bool"] == q["string"]]
    with pytest.raises(UserInputException):
        lib.read(symbol, query_builder=q)
    # bool column to numeric column
    q = QueryBuilder()
    q = q[q["bool"] == q["numeric"]]
    with pytest.raises(UserInputException):
        lib.read(symbol, query_builder=q)
    # bool column to string value
    q = QueryBuilder()
    q = q[q["bool"] == "test"]
    with pytest.raises(UserInputException):
        lib.read(symbol, query_builder=q)
    # bool column to numeric value
    q = QueryBuilder()
    q = q[q["bool"] == 0]
    with pytest.raises(UserInputException):
        lib.read(symbol, query_builder=q)
    # string column to bool value
    q = QueryBuilder()
    q = q[q["string"] == True]
    with pytest.raises(UserInputException):
        lib.read(symbol, query_builder=q)
    # numeric column to bool value
    q = QueryBuilder()
    q = q[q["numeric"] == True]
    with pytest.raises(UserInputException):
        lib.read(symbol, query_builder=q)


def test_filter_bool_column(lmdb_version_store_v1):
    lib = lmdb_version_store_v1
    symbol = "test_filter_bool_column"
    df = pd.DataFrame({"a": [True, False]}, index=np.arange(2))
    lib.write(symbol, df)
    q = QueryBuilder()
    q = q[q["a"]]
    expected = df[df["a"]]
    generic_filter_test(lib, symbol, q, expected)


def test_filter_bool_column_not(lmdb_version_store_v1):
    lib = lmdb_version_store_v1
    symbol = "test_filter_bool_column_not"
    df = pd.DataFrame({"a": [True, False]}, index=np.arange(2))
    lib.write(symbol, df)
    q = QueryBuilder()
    q = q[~q["a"]]
    expected = df[~df["a"]]
    generic_filter_test(lib, symbol, q, expected)


def test_filter_bool_column_binary_boolean(lmdb_version_store_v1):
    lib = lmdb_version_store_v1
    symbol = "test_filter_bool_column_binary_boolean"
    df = pd.DataFrame({"a": [True, True, False, False], "b": [True, False, True, False]}, index=np.arange(4))
    lib.write(symbol, df)
    q = QueryBuilder()
    q = q[q["a"] & q["b"]]
    expected = df[df["a"] & df["b"]]
    generic_filter_test(lib, symbol, q, expected)


def test_filter_bool_column_comparison(lmdb_version_store_v1):
    lib = lmdb_version_store_v1
    symbol = "test_filter_bool_column_comparison"
    df = pd.DataFrame({"a": [True, False]}, index=np.arange(2))
    lib.write(symbol, df)
    comparators = ["==", "!=", "<", "<=", ">", ">="]
    for comparator in comparators:
        for bool_value in [True, False]:
            pandas_query = f"a {comparator} {bool_value}"
            q = QueryBuilder()
            if comparator == "==":
                q = q[q["a"] == bool_value]
                expected = df[df["a"] == bool_value]
            elif comparator == "!=":
                q = q[q["a"] != bool_value]
                expected = df[df["a"] != bool_value]
            elif comparator == "<":
                q = q[q["a"] < bool_value]
                expected = df[df["a"] < bool_value]
            elif comparator == "<=":
                q = q[q["a"] <= bool_value]
                expected = df[df["a"] <= bool_value]
            elif comparator == ">":
                q = q[q["a"] > bool_value]
                expected = df[df["a"] > bool_value]
            elif comparator == ">=":
                q = q[q["a"] >= bool_value]
                expected = df[df["a"] >= bool_value]
            generic_filter_test(lib, symbol, q, expected)


def test_filter_datetime_naive(lmdb_version_store_v1):
    lib = lmdb_version_store_v1
    symbol = "test_filter_datetime_simple"
    df = pd.DataFrame({"a": pd.date_range("2000-01-01", periods=10)})
    lib.write(symbol, df)
    pd_ts = pd.Timestamp("2000-01-05")
    for ts in [pd_ts, pd_ts.to_pydatetime()]:
        q = QueryBuilder()
        q = q[q["a"] < ts]
        expected = df[df["a"] < ts]
        generic_filter_test(lib, symbol, q, expected)


def test_filter_datetime_isin(lmdb_version_store_v1):
    lib = lmdb_version_store_v1
    symbol = "test_filter_datetime_isin"
    df = pd.DataFrame({"a": pd.date_range("2000-01-01", periods=10)})
    lib.write(symbol, df)
    pd_ts = pd.Timestamp("2000-01-05")
    for ts in [pd_ts, pd_ts.to_pydatetime()]:
        q = QueryBuilder()
        q = q[q["a"] == [ts]]
        expected = df[df["a"].isin([ts])]
        generic_filter_test(lib, symbol, q, expected)


def test_filter_datetime_timedelta(lmdb_version_store_v1):
    lib = lmdb_version_store_v1
    symbol = "test_filter_datetime_timedelta"
    df = pd.DataFrame({"a": pd.date_range("2000-01-01", periods=10)})
    pd_ts = pd.Timestamp("2000-01-05")
    pd_td = pd.Timedelta(1, "d")
    for ts in [pd_ts, pd_ts.to_pydatetime()]:
        for td in [pd_td, pd_td.to_pytimedelta()]:
            q = QueryBuilder()
            q = q[(q["a"] + td) < ts]
            lib.write(symbol, df)
            expected = df[(df["a"] + td) < ts]
            received = lib.read(symbol, query_builder=q).data
            if not np.array_equal(expected, received) and (not expected.empty and not received.empty):
                print("ts\n{}".format(ts))
                print("td\n{}".format(td))
                print("Original dataframe\n{}".format(df))
                print("Expected\n{}".format(expected))
                print("Received\n{}".format(received))
                assert False
            assert True


def test_filter_datetime_timezone_aware(lmdb_version_store_v1):
    lib = lmdb_version_store_v1
    symbol = "test_filter_datetime_timezone_aware"
    df = pd.DataFrame({"a": pd.date_range("2000-01-01", periods=10, tz=timezone("Europe/Amsterdam"))})
    lib.write(symbol, df)
    pd_ts = pd.Timestamp("2000-01-05", tz=timezone("GMT"))
    for ts in [pd_ts, pd_ts.to_pydatetime()]:
        q = QueryBuilder()
        q = q[q["a"] < ts]
        expected = df[df["a"] < ts]
        # Convert to UTC and strip tzinfo to match behaviour of roundtripping through Arctic
        expected["a"] = expected["a"].apply(lambda x: x.tz_convert(timezone("utc")).tz_localize(None))
        generic_filter_test(lib, symbol, q, expected)


def test_df_query_wrong_type(lmdb_version_store_v1):
    lib = lmdb_version_store_v1

    df1 = pd.DataFrame({"col1": [1, 2, 3], "col2": [2, 3, 4], "col3": [4, 5, 6],
                        "col_str": ["1", "2", "3"], "col_bool": [True, False, True]})
    sym = "symbol"
    lib.write(sym, df1)

    str_vals = np.array(["2", "3", "4", "5"])
    q = QueryBuilder()
    q = q[q["col1"].isin(str_vals)]
    with pytest.raises(UserInputException, match="Cannot check membership 'IS IN' of col1.*type=INT.*in set of.*type=STRING"):
        lib.read(sym, query_builder=q)

    q = QueryBuilder()
    q = q[q["col_bool"].isnull()]
    with pytest.raises(UserInputException, match="Cannot perform null check: ISNULL\(col_bool\).*type=BOOL"):
        lib.read(sym, query_builder=q)

    q = QueryBuilder()
    q = q[q["col1"] / q["col_str"] == 3]
    with pytest.raises(UserInputException, match="Non-numeric column provided to binary operation: col1.*type=INT.*/.*col_str.*type=STRING"):
        lib.read(sym, query_builder=q)

    q = QueryBuilder()
    q = q[q["col1"] + "1" == 3]
    with pytest.raises(UserInputException, match="Non-numeric type provided to binary operation: col1.*type=INT.*\+ \"1\".*type=STRING"):
        lib.read(sym, query_builder=q)

    q = QueryBuilder()
    q = q[-q["col_str"] == 3]
    with pytest.raises(UserInputException, match="Cannot perform unary operation -\(col_str\).*type=STRING"):
        lib.read(sym, query_builder=q)

    q = QueryBuilder()
    q = q[q["col1"] - 1 >= "1"]
    with pytest.raises(UserInputException, match="Invalid comparison.*col1 - 1.*type=INT.*>=.*\"1\".*type=STRING"):
        lib.read(sym, query_builder=q)

    q = QueryBuilder()
    q = q[1 + q["col1"] * q["col2"] - q["col3"] == q["col_str"]]
    # check that ((1 + (col1 * col2)) + col3) is generated as a column name and shown in the error message
    with pytest.raises(UserInputException, match="Invalid comparison.*\(1 \+ \(col1 \* col2\)\) - col3.*type=INT.*==.*col_str .*type=STRING"):
        lib.read(sym, query_builder=q)


def test_filter_datetime_nanoseconds(lmdb_version_store_v1):
    lib = lmdb_version_store_v1
    sym = "test_filter_datetime_nanoseconds"

    # Dataframe has three rows and a single column containing timestamps 1 nanosecond apart
    timestamp_1 = pd.Timestamp("2023-03-15 10:30:00")
    timestamp_0 = timestamp_1 - pd.Timedelta(1, unit="ns")
    timestamp_2 = timestamp_1 + pd.Timedelta(1, unit="ns")
    df = pd.DataFrame(data=[timestamp_0, timestamp_1, timestamp_2], columns=["col"])

    lib.write(sym, df)

    # Try to read all rows
    qb_all = QueryBuilder()
    qb_all = qb_all[(qb_all["col"] >= timestamp_0) & (qb_all["col"] <= timestamp_2)]
    all_rows_result = lib.read(sym, query_builder=qb_all).data
    assert_frame_equal(all_rows_result, df)

    # Try to read only the first row
    qb_first = QueryBuilder()
    qb_first = qb_first[(qb_first["col"] >= timestamp_0) & (qb_first["col"] <= timestamp_0)]
    first_row_result = lib.read(sym, query_builder=qb_first).data
    assert_frame_equal(first_row_result, df.iloc[[0]])

    # Try to read first and second rows
    qb_first_and_second = QueryBuilder()
    qb_first_and_second = qb_first_and_second[
        (qb_first_and_second["col"] >= timestamp_0) & (qb_first_and_second["col"] <= timestamp_1)
    ]
    first_and_second_row_result = lib.read(sym, query_builder=qb_first_and_second).data
    assert_frame_equal(first_and_second_row_result, df.iloc[[0, 1]])

    # Try to read second and third rows
    qb_second_and_third = QueryBuilder()
    qb_second_and_third = qb_second_and_third[
        (qb_second_and_third["col"] >= timestamp_1) & (qb_second_and_third["col"] <= timestamp_2)
    ]
    second_and_third_row_result = lib.read(sym, query_builder=qb_second_and_third).data
    assert_frame_equal(second_and_third_row_result, df.iloc[[1, 2]].reset_index(drop=True))


def test_filter_isin_clashing_sets(lmdb_version_store_v1):
    lib = lmdb_version_store_v1
    symbol = "test_filter_isin_clashing_sets"
    a_unique_val = 100000
    b_unique_val = 200000
    df = pd.DataFrame({"a": [-1, a_unique_val, -1], "b": [-1, -1, b_unique_val]}, index=np.arange(3))
    lib.write(symbol, df)
    q = QueryBuilder()
    vals1 = np.arange(10000, dtype=np.uint64)
    np.put(vals1, 5000, a_unique_val)
    vals2 = np.arange(10000, dtype=np.uint64)
    np.put(vals2, 5000, b_unique_val)
    assert str(vals1) == str(vals2)
    q = q[(q["a"].isin(vals1)) | (q["b"].isin(vals2))]
    expected = df[(df["a"].isin(vals1)) | (df["b"].isin(vals2))]
    generic_filter_test(lib, symbol, q, expected)


@pytest.mark.parametrize(
    "df_col,isin_vals,expected_col",
    [
        ([0, 1, 2**62], [0, 1, -1], [0, 1]),
        ([0, 1, 2**63 - 1], [0, 1, -1], [0, 1]),
        ([0, 1, 2**62], [0, 1, -1], [0, 1]),
        ([-1, 0, 1], [0, 1, 2**62], [0, 1]),
    ],
)
def test_filter_numeric_isin_hashing_overflows(lmdb_version_store_v1, df_col, isin_vals, expected_col):
    lib = lmdb_version_store_v1
    df = pd.DataFrame({"a": df_col})
    lib.write("test_filter_numeric_isin_hashing_overflows", df)

    q = QueryBuilder()
    q = q[q["a"].isin(isin_vals)]
    result = lib.read("test_filter_numeric_isin_hashing_overflows", query_builder=q).data

    expected = pd.DataFrame({"a": expected_col})
    assert_frame_equal(expected, result)


def test_filter_numeric_isin_unsigned(lmdb_version_store_v1):
    lib = lmdb_version_store_v1
    df = pd.DataFrame({"a": [0, 1, 2**64 - 1]})
    lib.write("test_filter_numeric_isin_unsigned", df)

    q = QueryBuilder()
    q = q[q["a"].isin([0, 1, 2])]
    result = lib.read("test_filter_numeric_isin_unsigned", query_builder=q).data

    expected = pd.DataFrame({"a": [0, 1]}, dtype=np.uint64)
    assert_frame_equal(expected, result)


def test_filter_numeric_isnotin_mixed_types_exception():
    vals = [np.int64(-1), np.uint64(4294967296)]
    q = QueryBuilder()
    with pytest.raises(UserInputException):
        q = q[q["a"].isnotin(vals)]


def test_filter_numeric_isnotin_hashing_overflow(lmdb_version_store_v1):
    lib = lmdb_version_store_v1
    df = pd.DataFrame({"a": [256]})
    lib.write("test_filter_numeric_isnotin_hashing_overflow", df)

    q = QueryBuilder()
    isnotin_vals = np.array([], np.uint8)
    q = q[q["a"].isnotin(isnotin_vals)]
    result = lib.read("test_filter_numeric_isnotin_hashing_overflow", query_builder=q).data

    assert_frame_equal(df, result)


_uint64_max = np.iinfo(np.uint64).max


@pytest.mark.parametrize("op", ("in", "not in"))
@pytest.mark.parametrize("signed_type", (np.int8, np.int16, np.int32, np.int64))
@pytest.mark.parametrize("uint64_in", ("df", "vals") if PANDAS_VERSION >= Version("1.2") else ("vals",))
def test_filter_numeric_membership_mixing_int64_and_uint64(lmdb_version_store_v1, op, signed_type, uint64_in):
    lib = lmdb_version_store_v1
    symbol = "test_filter_numeric_membership_mixing_int64_and_uint64"
    signed = signed_type(-1)
    if uint64_in == "df":
        df, vals = pd.DataFrame({"a": [_uint64_max]}), [signed]
    else:
        df, vals = pd.DataFrame({"a": [signed]}), [_uint64_max]
    lib.write(symbol, df)

    q = QueryBuilder()
    q = q[q["a"].isin(vals) if op == "in" else q["a"].isnotin(vals)]
    expected = df[df["a"].isin(vals) if op == "in" else ~df["a"].isin(vals)]
    generic_filter_test(lib, symbol, q, expected)


def test_filter_nones_and_nans_retained_in_string_column(lmdb_version_store_v1):
    lib = lmdb_version_store_v1
    sym = "test_filter_nones_and_nans_retained_in_string_column"
    df = pd.DataFrame({"filter_column": [1, 2, 1, 2, 1, 2], "string_column": ["1", "2", np.nan, "4", None, "6"]})
    lib.write(sym, df)
    q = QueryBuilder()
    q = q[q["filter_column"] == 1]
    q.optimise_for_memory()
    expected = df.query("filter_column == 1")
    received = lib.read(sym, query_builder=q).data
    assert np.array_equal(expected["filter_column"], received["filter_column"])
    assert received["string_column"].iloc[0] == "1"
    assert np.isnan(received["string_column"].iloc[1])
    assert received["string_column"].iloc[2] is None


# Tests that false matches aren't generated when list members truncate to column values
def test_filter_fixed_width_string_isin_truncation(lmdb_version_store_v1):
    lib = lmdb_version_store_v1
    symbol = "test_filter_fixed_width_string_isin_truncation"
    df = pd.DataFrame({"a": ["1"]}, index=np.arange(1))
    lib.write(symbol, df, dynamic_strings=False)
    vals = ["12"]
    q = QueryBuilder()
    q = q[q["a"].isin(vals)]
    expected = df[df["a"].isin(vals)]
    generic_filter_test(lib, symbol, q, expected)


def test_filter_stringpool_shrinking_basic(lmdb_version_store_tiny_segment):
    # Construct a dataframe and QueryBuilder pair with the following properties:
    # - original dataframe spanning multiple segments horizontally and vertically (tiny segment == 2x2)
    # - strings of varying lengths to exercise fixed width strings more completely
    # - repeated strings within a segment
    # - at least one segment will need all of the strings in it's pool after filtering
    # - at least one segment will need none of the strings in it's pool after filtering
    # - at least one segment will need some, but not all of the strings in it's pool after filtering
    lib = lmdb_version_store_tiny_segment
    base_symbol = "test_filter_stringpool_shrinking_basic"
    df = pd.DataFrame(
        {
            "a": ["a1", "a2", "a3", "a4", "a5"],
            "b": ["b11", "b22", "b3", "b4", "b5"],
            "c": ["c1", "c2", "c3", "c4", "c5"],
            "d": ["d11", "d2", "d3", "d4", "d5"],
        }
    )
    lib.write(f"{base_symbol}_{DYNAMIC_STRINGS_SUFFIX}", df, dynamic_strings=True)
    lib.write(f"{base_symbol}_{FIXED_STRINGS_SUFFIX}", df, dynamic_strings=False)
    q = QueryBuilder()
    q = q[q["a"] != "a1"]
    q.optimise_for_memory()
    expected = df[df["a"] != "a1"]
    generic_filter_test_strings(lib, base_symbol, q, expected)


def test_filter_stringpool_shrinking_block_alignment(lmdb_version_store_v1):
    lib = lmdb_version_store_v1
    base_symbol = "test_filter_stringpool_shrinking_block_alignment"
    # Create a dataframe with more than one block (3968 bytes) worth of strings for the stringpool
    string_length = 10
    num_rows = 1000
    data = ["".join(random.choice(string.ascii_uppercase) for _ in range(string_length)) for unused in range(num_rows)]
    df = pd.DataFrame({"a": data})
    lib.write(f"{base_symbol}_{DYNAMIC_STRINGS_SUFFIX}", df, dynamic_strings=True)
    lib.write(f"{base_symbol}_{FIXED_STRINGS_SUFFIX}", df, dynamic_strings=False)
    q = QueryBuilder()
    string_to_find = data[3]
    q = q[q["a"] == string_to_find]
    expected = df[df["a"] == string_to_find]
    generic_filter_test_strings(lib, base_symbol, q, expected)


def test_filter_explicit_type_promotion(lmdb_version_store_v1):
    lib = lmdb_version_store_v1
    df = pd.DataFrame(
        {
            "uint8": [np.iinfo(np.uint8).min, np.iinfo(np.uint8).max],
            "uint16": [np.iinfo(np.uint16).min, np.iinfo(np.uint16).max],
            "uint32": [np.iinfo(np.uint32).min, np.iinfo(np.uint32).max],
            "int8": [np.iinfo(np.int8).min, np.iinfo(np.int8).max],
            "int16": [np.iinfo(np.int16).min, np.iinfo(np.int16).max],
            "int32": [np.iinfo(np.int32).min, np.iinfo(np.int32).max],
        },
        index=np.arange(2),
    )
    symbol = "test_filter_explicit_type_promotion"
    lib.write(symbol, df)
    # Plus
    q = QueryBuilder()
    q = q[
        ((q.uint8 + 1) == 256)
        & ((q.uint16 + 1) == 65536)
        & ((q.uint32 + 1) == 4294967296)
        & ((q.int8 + 1) == 128)
        & ((q.int16 + 1) == 32768)
        & ((q.int32 + 1) == 2147483648)
    ]
    assert np.array_equal(lib.read(symbol, query_builder=q).data, df.loc[[1]])
    # Minus
    q = QueryBuilder()
    q = q[
        ((q.uint8 - 1) == -1)
        & ((q.uint16 - 1) == -1)
        & ((q.uint32 - 1) == -1)
        & ((q.int8 - 1) == -129)
        & ((q.int16 - 1) == -32769)
        & ((q.int32 - 1) == -2147483649)
    ]
    assert np.array_equal(lib.read(symbol, query_builder=q).data, df.loc[[0]])
    # Times
    q = QueryBuilder()
    q = q[
        ((q.uint8 * 2) == 510)
        & ((q.uint16 * 2) == 131070)
        & ((q.uint32 * 2) == 8589934590)
        & ((q.int8 * 2) == 254)
        & ((q.int16 * 2) == 65534)
        & ((q.int32 * 2) == 4294967294)
    ]
    assert np.array_equal(lib.read(symbol, query_builder=q).data, df.loc[[1]])
    # Divide
    q = QueryBuilder()
    q = q[
        ((q.uint8 / -1) == -255)
        & ((q.uint16 / -1) == -65535)
        & ((q.uint32 / -1) == -4294967295)
        & ((q.int8 / -1) == -127)
        & ((q.int16 / -1) == -32767)
        & ((q.int32 / -1) == -2147483647)
    ]
    assert np.array_equal(lib.read(symbol, query_builder=q).data, df.loc[[1]])
    # Abs
    q = QueryBuilder()
    q = q[
        (abs(q.uint8 + 1) == 1)
        & (abs(q.uint16 + 1) == 1)
        & (abs(q.uint32 + 1) == 1)
        & (abs(q.int8 + 1) == 127)
        & (abs(q.int16 + 1) == 32767)
        & (abs(q.int32 + 1) == 2147483647)
    ]
    assert np.array_equal(lib.read(symbol, query_builder=q).data, df.loc[[0]])
    # Neg
    q = QueryBuilder()
    q = q[
        (-q.uint8 == -255)
        & (-q.uint16 == -65535)
        & (-q.uint32 == -4294967295)
        & (-q.int8 == -127)
        & (-q.int16 == -32767)
        & (-q.int32 == -2147483647)
    ]
    assert np.array_equal(lib.read(symbol, query_builder=q).data, df.loc[[1]])


def test_filter_column_slicing_different_segments(lmdb_version_store_tiny_segment):
    lib = lmdb_version_store_tiny_segment
    df = pd.DataFrame({"a": np.arange(0, 10), "b": np.arange(10, 20), "c": np.arange(20, 30)}, index=np.arange(10))
    symbol = "test_filter_column_slicing_different_segments"
    lib.write(symbol, df)
    # Filter on column c (in second column slice), but only display column a (in first column slice)
    q = QueryBuilder()
    q = q[q["c"] == 22]
    pandas_query = "c == 22"
    expected = df.query(pandas_query).loc[:, ["a"]]
    received = lib.read(symbol, columns=["a"], query_builder=q).data
    assert np.array_equal(expected, received)
    # Filter on column c (in second column slice), and display all columns
    q = QueryBuilder()
    q = q[q["c"] == 22]
    pandas_query = "c == 22"
    expected = df.query(pandas_query)
    received = lib.read(symbol, query_builder=q).data
    assert np.array_equal(expected, received)
    # Filter on column c (in second column slice), and only display column c
    q = QueryBuilder()
    q = q[q["c"] == 22]
    pandas_query = "c == 22"
    expected = df.query(pandas_query).loc[:, ["c"]]
    received = lib.read(symbol, columns=["c"], query_builder=q).data
    assert np.array_equal(expected, received)


def test_filter_with_multi_index(lmdb_version_store_v1):
    lib = lmdb_version_store_v1
    symbol = "test_filter_with_multi_index"
    dt1 = datetime(2019, 4, 8, 10, 5, 2, 1)
    dt2 = datetime(2019, 4, 9, 10, 5, 2, 1)
    arr1 = [dt1, dt1, dt2, dt2]
    arr2 = [0, 1, 0, 1]
    df = pd.DataFrame(
        data={"a": np.arange(10, 14)}, index=pd.MultiIndex.from_arrays([arr1, arr2], names=["datetime", "level"])
    )
    lib.write(symbol, df)
    q = QueryBuilder()
    q = q[(q["a"] == 11) | (q["a"] == 13)]
    expected = df[(df["a"] == 11) | (df["a"] == 13)]
    generic_filter_test(lib, symbol, q, expected)


def test_filter_on_multi_index(lmdb_version_store_v1):
    lib = lmdb_version_store_v1
    symbol = "test_filter_on_multi_index"
    dt1 = datetime(2019, 4, 8, 10, 5, 2, 1)
    dt2 = datetime(2019, 4, 9, 10, 5, 2, 1)
    arr1 = [dt1, dt1, dt2, dt2]
    arr2 = [0, 1, 0, 1]
    df = pd.DataFrame(
        data={"a": np.arange(10, 14)}, index=pd.MultiIndex.from_arrays([arr1, arr2], names=["datetime", "level"])
    )
    lib.write(symbol, df)
    q = QueryBuilder()
    q = q[q["level"] == 1]
    expected = df.query("level == 1")
    generic_filter_test(lib, symbol, q, expected)


def test_filter_complex_expression(lmdb_version_store_tiny_segment):
    lib = lmdb_version_store_tiny_segment
    symbol = "test_filter_complex_expression"
    df = pd.DataFrame(
        {
            "a": np.arange(0, 10, dtype=np.float64),
            "b": np.arange(10, 20, dtype=np.uint16),
            "c": np.arange(20, 30, dtype=np.int32),
        },
        index=np.arange(10),
    )
    lib.write(symbol, df)
    q = QueryBuilder()
    q = q[(((q["a"] * q["b"]) / 5) < (0.7 * q["c"])) & (q["b"] != 12)]
    expected = df[(((df["a"] * df["b"]) / 5) < (0.7 * df["c"])) & (df["b"] != 12)]
    generic_filter_test(lib, symbol, q, expected)


def test_filter_string_backslash(lmdb_version_store_v1):
    lib = lmdb_version_store_v1
    df = pd.DataFrame({"a": ["", "\\"]}, index=np.arange(2))
    q = QueryBuilder()
    q = q[q["a"] == "\\"]
    symbol = "test_filter_string_backslash"
    lib.write(symbol, df, dynamic_strings=True)
    expected = pd.DataFrame({"a": ["\\"]}, index=np.arange(1, 2))
    received = lib.read(symbol, query_builder=q).data
    assert np.array_equal(expected, received)


def test_filter_string_single_quote(lmdb_version_store_v1):
    lib = lmdb_version_store_v1
    df = pd.DataFrame({"a": ["", "'"]}, index=np.arange(2))
    q = QueryBuilder()
    q = q[q["a"] == "'"]
    symbol = "test_filter_string_single_quote"
    lib.write(symbol, df, dynamic_strings=True)
    expected = pd.DataFrame({"a": ["'"]}, index=np.arange(1, 2))
    received = lib.read(symbol, query_builder=q).data
    assert np.array_equal(expected, received)


def test_filter_string_less_than(lmdb_version_store_v1):
    lib = lmdb_version_store_v1
    base_symbol = "test_filter_string_less_than"
    df = pd.DataFrame({"a": ["row1", "row2"]}, index=np.arange(2))
    lib.write(f"{base_symbol}_{DYNAMIC_STRINGS_SUFFIX}", df, dynamic_strings=True)
    lib.write(f"{base_symbol}_{FIXED_STRINGS_SUFFIX}", df, dynamic_strings=False)
    q = QueryBuilder()
    q = q[q["a"] < "row2"]
    with pytest.raises(InternalException):
        lib.read(f"{base_symbol}_{DYNAMIC_STRINGS_SUFFIX}", query_builder=q).data
    with pytest.raises(InternalException):
        lib.read(f"{base_symbol}_{FIXED_STRINGS_SUFFIX}", query_builder=q).data


def test_filter_string_less_than_equal(lmdb_version_store_v1):
    lib = lmdb_version_store_v1
    base_symbol = "test_filter_string_less_than_equal"
    df = pd.DataFrame({"a": ["row1", "row2"]}, index=np.arange(2))
    lib.write(f"{base_symbol}_{DYNAMIC_STRINGS_SUFFIX}", df, dynamic_strings=True)
    lib.write(f"{base_symbol}_{FIXED_STRINGS_SUFFIX}", df, dynamic_strings=False)
    q = QueryBuilder()
    q = q[q["a"] <= "row2"]
    with pytest.raises(InternalException):
        lib.read(f"{base_symbol}_{DYNAMIC_STRINGS_SUFFIX}", query_builder=q).data
    with pytest.raises(InternalException):
        lib.read(f"{base_symbol}_{FIXED_STRINGS_SUFFIX}", query_builder=q).data


def test_filter_string_greater_than(lmdb_version_store_v1):
    lib = lmdb_version_store_v1
    base_symbol = "test_filter_string_greater_than"
    df = pd.DataFrame({"a": ["row1", "row2"]}, index=np.arange(2))
    lib.write(f"{base_symbol}_{DYNAMIC_STRINGS_SUFFIX}", df, dynamic_strings=True)
    lib.write(f"{base_symbol}_{FIXED_STRINGS_SUFFIX}", df, dynamic_strings=False)
    q = QueryBuilder()
    q = q[q["a"] > "row2"]
    with pytest.raises(InternalException):
        lib.read(f"{base_symbol}_{DYNAMIC_STRINGS_SUFFIX}", query_builder=q).data
    with pytest.raises(InternalException):
        lib.read(f"{base_symbol}_{FIXED_STRINGS_SUFFIX}", query_builder=q).data


def test_filter_string_greater_than_equal(lmdb_version_store_v1):
    lib = lmdb_version_store_v1
    base_symbol = "test_filter_string_greater_than_equal"
    df = pd.DataFrame({"a": ["row1", "row2"]}, index=np.arange(2))
    lib.write(f"{base_symbol}_{DYNAMIC_STRINGS_SUFFIX}", df, dynamic_strings=True)
    lib.write(f"{base_symbol}_{FIXED_STRINGS_SUFFIX}", df, dynamic_strings=False)
    q = QueryBuilder()
    q = q[q["a"] >= "row2"]
    with pytest.raises(InternalException):
        lib.read(f"{base_symbol}_{DYNAMIC_STRINGS_SUFFIX}", query_builder=q).data
    with pytest.raises(InternalException):
        lib.read(f"{base_symbol}_{FIXED_STRINGS_SUFFIX}", query_builder=q).data


def test_filter_string_nans_col_val(lmdb_version_store_v1):
    lib = lmdb_version_store_v1
    symbol = "test_filter_string_nans_col_val"
    df = pd.DataFrame({"a": ["row1", "row2", None, np.nan, math.nan]}, index=np.arange(5))
    lib.write(symbol, df, dynamic_strings=True)

    q = QueryBuilder()
    q = q[q["a"] == "row2"]
    expected = df[df["a"] == "row2"]
    generic_filter_test_nans(lib, symbol, q, expected)

    q = QueryBuilder()
    q = q[q["a"] != "row2"]
    expected = df[df["a"] != "row2"]
    generic_filter_test_nans(lib, symbol, q, expected)

    q = QueryBuilder()
    q = q[q["a"] == ["row2"]]
    expected = df[df["a"].isin(["row2"])]
    generic_filter_test_nans(lib, symbol, q, expected)

    q = QueryBuilder()
    q = q[q["a"] != ["row2"]]
    expected = df[~df["a"].isin(["row2"])]
    generic_filter_test_nans(lib, symbol, q, expected)

    q = QueryBuilder()
    q = q[q["a"] == []]
    expected = df[df["a"].isin([])]
    generic_filter_test_nans(lib, symbol, q, expected)

    q = QueryBuilder()
    q = q[q["a"] != []]
    expected = df[~df["a"].isin([])]
    generic_filter_test_nans(lib, symbol, q, expected)


def test_filter_string_nans_col_col(lmdb_version_store_v1):
    lib = lmdb_version_store_v1
    symbol = "test_filter_string_nans_col_col"
    # Compare all combinations of string, None, np.nan, and math.nan to one another
    df = pd.DataFrame(
        {
            "a": ["row1", "row2", "row3", "row4", None, None, None, np.nan, np.nan, math.nan],
            "b": ["row1", None, np.nan, math.nan, None, np.nan, math.nan, np.nan, math.nan, math.nan],
        },
        index=np.arange(10),
    )
    lib.write(symbol, df)

    q = QueryBuilder()
    q = q[q["a"] == q["b"]]
    expected = df[df["a"] == df["b"]]
    generic_filter_test_nans(lib, symbol, q, expected)

    q = QueryBuilder()
    q = q[q["a"] != q["b"]]
    expected = df[df["a"] != df["b"]]
    generic_filter_test_nans(lib, symbol, q, expected)


@pytest.mark.parametrize("method", ("isna", "notna", "isnull", "notnull"))
@pytest.mark.parametrize("dtype", (np.int64, np.float32, np.float64, np.datetime64, str))
def test_filter_null_filtering(lmdb_version_store_v1, method, dtype):
    lib = lmdb_version_store_v1
    symbol = "test_filter_null_filtering"
    num_rows = 5
    if dtype is np.int64:
        data = np.arange(num_rows, dtype=dtype)
        # These are the values used to represent:
        # - NaT in timestamp columns
        # - None in string columns
        # - NaN in string columns
        # respectively, so are most likely to cause issues
        iinfo = np.iinfo(np.int64)
        null_values = cycle([iinfo.min, iinfo.max, iinfo.max - 1])
    elif dtype in (np.float32, np.float64):
        data = np.arange(num_rows, dtype=dtype)
        null_values = cycle([np.nan])
    elif dtype is np.datetime64:
        data = np.arange(np.datetime64("2024-01-01"), np.datetime64(f"2024-01-0{num_rows + 1}"), np.timedelta64(1, "D")).astype("datetime64[ns]")
        null_values = cycle([np.datetime64("nat")])
    else:  # str
        data = [str(idx) for idx in range(num_rows)]
        null_values = cycle([None, np.nan])
    for idx in range(num_rows):
        if idx % 2 == 0:
            data[idx] = next(null_values)

    df = pd.DataFrame({"a": data}, index=np.arange(num_rows))
    lib.write(symbol, df)

    expected = df[getattr(df["a"], method)()]

    q = QueryBuilder()
    q = q[getattr(q["a"], method)()]
    received = lib.read(symbol, query_builder=q).data
    assert_frame_equal(expected, received)


def test_filter_numeric_membership_equivalence():
    q_list_isin = QueryBuilder()
    q_set_isin = QueryBuilder()
    q_frozenset_isin = QueryBuilder()
    q_tuple_isin = QueryBuilder()
    q_ndarray_isin = QueryBuilder()
    q_args_isin = QueryBuilder()
    q_list = QueryBuilder()
    q_set = QueryBuilder()
    q_frozenset = QueryBuilder()
    q_tuple = QueryBuilder()
    q_ndarray = QueryBuilder()

    errors = []

    l = [1, 2, 3, 1]
    s = set(l)
    f = frozenset(l)
    t = tuple(l)
    a = np.array(l)

    q_list_isin = q_list_isin[q_list_isin["a"].isin(l)]
    q_set_isin = q_set_isin[q_set_isin["a"].isin(s)]
    q_frozenset_isin = q_frozenset_isin[q_frozenset_isin["a"].isin(f)]
    q_tuple_isin = q_tuple_isin[q_tuple_isin["a"].isin(t)]
    q_ndarray_isin = q_ndarray_isin[q_ndarray_isin["a"].isin(a)]
    q_args_isin = q_args_isin[q_args_isin["a"].isin(1, 2, 3, 1)]
    q_list = q_list[q_list["a"] == l]
    q_set = q_set[q_set["a"] == s]
    q_frozenset = q_frozenset[q_frozenset["a"] == f]
    q_tuple = q_tuple[q_tuple["a"] == t]
    q_ndarray = q_ndarray[q_ndarray["a"] == a]

    if q_list_isin != q_set_isin:
        errors.append("set isin")
    if q_list_isin != q_frozenset_isin:
        errors.append("frozenset isin")
    if q_list_isin != q_tuple_isin:
        errors.append("tuple isin")
    if q_list_isin != q_ndarray_isin:
        errors.append("ndarray isin")
    if q_list_isin != q_args_isin:
        errors.append("args isin")
    if q_list_isin != q_list:
        errors.append("list")
    if q_list_isin != q_set:
        errors.append("set")
    if q_list_isin != q_frozenset:
        errors.append("frozenset")
    if q_list_isin != q_tuple:
        errors.append("tuple")
    if q_list_isin != q_ndarray:
        errors.append("ndarray")

    q_list_isnotin = QueryBuilder()
    q_list_not_equal = QueryBuilder()
    q_list_isnotin = q_list_isnotin[q_list_isnotin["a"].isnotin(l)]
    q_list_not_equal = q_list_not_equal[q_list_not_equal["a"] != l]

    if q_list_isnotin != q_list_not_equal:
        errors.append("isnotin")

    assert not errors


def test_filter_bool_short_circuiting():
    def _clear(first, second):
        first.clauses.clear()
        first._python_clauses.clear()
        second.clauses.clear()
        second._python_clauses.clear()

    # Original query
    q1 = QueryBuilder()
    # Expected short-circuited version
    q2 = QueryBuilder()
    errors = []

    q1 = q1[(q1["a"] < 5) & True]
    q2 = q2[(q2["a"] < 5)]
    if q1 != q2:
        errors.append("and true")
    _clear(q1, q2)

    with pytest.raises(ArcticNativeException):
        q1 = q1[(q1["a"] < 5) & False]
    _clear(q1, q2)

    q1 = q1[True & (q1["a"] < 5)]
    q2 = q2[(q2["a"] < 5)]
    if q1 != q2:
        errors.append("rand true")
    _clear(q1, q2)

    with pytest.raises(ArcticNativeException):
        q1 = q1[False & (q1["a"] < 5)]
    _clear(q1, q2)

    with pytest.raises(ArcticNativeException):
        q1 = q1[(q1["a"] < 5) | True]
    _clear(q1, q2)

    q1 = q1[(q1["a"] < 5) | False]
    q2 = q2[q1["a"] < 5]
    if q1 != q2:
        errors.append("or false")
    _clear(q1, q2)

    with pytest.raises(ArcticNativeException):
        q1 = q1[True | (q1["a"] < 5)]
    _clear(q1, q2)

    q1 = q1[False | (q1["a"] < 5)]
    q2 = q2[q1["a"] < 5]
    if q1 != q2:
        errors.append("ror false")
    _clear(q1, q2)

    q1 = q1[(q1["a"] < 5) ^ True]
    q2 = q2[~(q1["a"] < 5)]
    if q1 != q2:
        errors.append("xor true")
    _clear(q1, q2)

    q1 = q1[(q1["a"] < 5) ^ False]
    q2 = q2[q1["a"] < 5]
    if q1 != q2:
        errors.append("xor false")
    _clear(q1, q2)

    q1 = q1[True ^ (q1["a"] < 5)]
    q2 = q2[~(q1["a"] < 5)]
    if q1 != q2:
        errors.append("rxor true")
    _clear(q1, q2)

    q1 = q1[False ^ (q1["a"] < 5)]
    q2 = q2[q1["a"] < 5]
    if q1 != q2:
        errors.append("rxor false")
    _clear(q1, q2)

    assert not errors


def test_filter_string_number_comparison(lmdb_version_store_v1):
    lib = lmdb_version_store_v1
    symbol = "test_filter_string_number_comparison"
    lib.write(symbol, pd.DataFrame({"a": [0], "b": ["hello"]}))
    q = QueryBuilder()
    q = q[q["a"] == "0"]
    with pytest.raises(UserInputException):
        lib.read(symbol, query_builder=q)
    q = QueryBuilder()
    q = q[q["b"] == 0]
    with pytest.raises(UserInputException):
        lib.read(symbol, query_builder=q)
    q = QueryBuilder()
    q = q[q["a"] == q["b"]]
    with pytest.raises(UserInputException):
        lib.read(symbol, query_builder=q)
    q = QueryBuilder()
    q = q["0" == q["a"]]
    with pytest.raises(UserInputException):
        lib.read(symbol, query_builder=q)
    q = QueryBuilder()
    q = q[0 == q["b"]]
    with pytest.raises(UserInputException):
        lib.read(symbol, query_builder=q)
    q = QueryBuilder()
    q = q[q["b"] == q["a"]]
    with pytest.raises(UserInputException):
        lib.read(symbol, query_builder=q)


def test_filter_string_number_set_membership(lmdb_version_store_v1):
    lib = lmdb_version_store_v1
    symbol = "test_filter_string_number_set_membership"
    lib.write(symbol, pd.DataFrame({"a": [0], "b": ["hello"]}))
    q = QueryBuilder()
    q = q[q["a"].isin(["0"])]
    with pytest.raises(UserInputException):
        lib.read(symbol, query_builder=q)
    q = QueryBuilder()
    q = q[q["b"].isin([0])]
    with pytest.raises(UserInputException):
        lib.read(symbol, query_builder=q)


# float32 comparisons are excluded from the hypothesis tests due to a bug in Pandas, so cover these here instead
# https://github.com/pandas-dev/pandas/issues/59524
def test_float32_binary_comparison(lmdb_version_store_v1):
    lib = lmdb_version_store_v1
    symbol = "test_float32_binary_comparison"
    df = pd.DataFrame(
        {
            "uint8": np.arange(1000, dtype=np.uint8),
            "uint16": np.arange(1000, dtype=np.uint16),
            "uint32": np.arange(1000, dtype=np.uint32),
            "uint64": np.arange(1000, dtype=np.uint64),
            "int8": np.arange(1000, dtype=np.int8),
            "int16": np.arange(1000, dtype=np.int16),
            "int32": np.arange(1000, dtype=np.int32),
            "int64": np.arange(1000, dtype=np.int64),
            "float32": np.arange(1000, dtype=np.float32),
            "float64": np.arange(1000, dtype=np.float64),
        }
    )
    lib.write(symbol, df)
    for op in ["<", "<=", ">", ">=", "==", "!="]:
        for other_col in ["uint8", "uint16", "uint32", "uint64", "int8", "int16", "int32", "int64", "float32", "float64"]:
            q = QueryBuilder()
            qb_lhs = q["float32"]
            qb_rhs = q[other_col]
            pandas_lhs = df["float32"]
            pandas_rhs = df[other_col]
            if op == "<":
                q = q[qb_lhs < qb_rhs]
                expected = df[pandas_lhs < pandas_rhs]
            elif op == "<=":
                q = q[qb_lhs <= qb_rhs]
                expected = df[pandas_lhs <= pandas_rhs]
            elif op == ">":
                q = q[qb_lhs > qb_rhs]
                expected = df[pandas_lhs > pandas_rhs]
            elif op == ">=":
                q = q[qb_lhs >= qb_rhs]
                expected = df[pandas_lhs >= pandas_rhs]
            elif op == "==":
                q = q[qb_lhs == qb_rhs]
                expected = df[pandas_lhs == pandas_rhs]
            elif op == "!=":
                q = q[qb_lhs != qb_rhs]
                expected = df[pandas_lhs != pandas_rhs]
            generic_filter_test(lib, symbol, q, expected)


################################
# MIXED SCHEMA TESTS FROM HERE #
################################


@pytest.mark.parametrize("lib_type", ["lmdb_version_store_v1", "lmdb_version_store_dynamic_schema_v1"])
def test_filter_empty_dataframe(request, lib_type):
    lib = request.getfixturevalue(lib_type)
    df = pd.DataFrame({"a": []})
    q = QueryBuilder()
    q = q[q["a"] < 5]
    symbol = "test_filter_empty_dataframe"
    lib.write(symbol, df)
    vit = lib.read(symbol, query_builder=q)
    assert vit.data.empty


@pytest.mark.parametrize("lib_type", ["lmdb_version_store_v1", "lmdb_version_store_dynamic_schema_v1"])
def test_filter_pickled_symbol(request, lib_type):
    lib = request.getfixturevalue(lib_type)
    symbol = "test_filter_pickled_symbol"
    lib.write(symbol, np.arange(100).tolist())
    assert lib.is_symbol_pickled(symbol)
    q = QueryBuilder()
    q = q[q.a == 0]
    with pytest.raises(InternalException):
        _ = lib.read(symbol, query_builder=q)


@pytest.mark.parametrize("lib_type", ["lmdb_version_store_v1", "lmdb_version_store_dynamic_schema_v1"])
def test_filter_date_range_pickled_symbol(request, lib_type):
    lib = request.getfixturevalue(lib_type)
    symbol = "test_filter_date_range_pickled_symbol"
    idx = pd.date_range("2000-01-01", periods=4)
    df = pd.DataFrame({"a": [[1, 2], [3, 4], [5, 6], [7, 8]]}, index=idx)
    lib.write(symbol, df, pickle_on_failure=True)
    assert lib.is_symbol_pickled(symbol)
    with pytest.raises(InternalException):
        lib.read(symbol, date_range=(idx[1], idx[2]))


##################################
# DYNAMIC SCHEMA TESTS FROM HERE #
##################################


def test_numeric_filter_dynamic_schema(lmdb_version_store_tiny_segment_dynamic):
    lib = lmdb_version_store_tiny_segment_dynamic
    symbol = "test_numeric_filter_dynamic_schema"
    df = get_wide_dataframe(100)
    expected, slices = make_dynamic(df)
    for df_slice in slices:
        lib.append(symbol, df_slice, write_if_missing=True)
    val = 0
    q = QueryBuilder()
    q = q[q["int8"] < val]
    pandas_query = "int8 < {}".format(val)
    expected = expected.query(pandas_query)
    received = lib.read(symbol, query_builder=q).data
    expected = regularize_dataframe(expected)
    received = regularize_dataframe(received)
    assert_frame_equal(expected, received)


def test_filter_column_not_present_dynamic(lmdb_version_store_dynamic_schema_v1):
    lib = lmdb_version_store_dynamic_schema_v1
    symbol = "test_filter_column_not_present_static"
    df = pd.DataFrame({"a": np.arange(2)}, index=np.arange(2), dtype="int64")
    q = QueryBuilder()
    q = q[q["b"] < 5]

    lib.write(symbol, df)
    vit = lib.read(symbol, query_builder=q)

    if (not IS_NUMPY_TWO) and (IS_PANDAS_TWO and sys.platform.startswith("win32")):
        # Pandas 2.0.0 changed the behavior of Index creation from numpy arrays:
        # "Previously, all indexes created from numpy numeric arrays were forced to 64-bit.
        # Now, for example, Index(np.array([1, 2, 3])) will be int32 on 32-bit systems,
        # where it previously would have been int64 even on 32-bit systems.
        # Instantiating Index using a list of numbers will still return 64bit dtypes,
        # e.g. Index([1, 2, 3]) will have a int64 dtype, which is the same as previously."
        # See: https://pandas.pydata.org/docs/dev/whatsnew/v2.0.0.html#index-can-now-hold-numpy-numeric-dtypes
        index_dtype = "int32"
    else:
        index_dtype = "int64"

    expected = pd.DataFrame({"a": pd.Series(dtype="int64")}, index=pd.Index([], dtype=index_dtype))
    assert_frame_equal(vit.data, expected)


def test_filter_column_type_change(lmdb_version_store_dynamic_schema_v1):
    lib = lmdb_version_store_dynamic_schema_v1
    symbol = "test_filter_column_type_change"

    # Write a column of float type
    df1 = pd.DataFrame({"col": [0.0]}, index=pd.date_range("2000-01-01", periods=1))
    lib.write(symbol, df1)
    # Append a column of int type
    df2 = pd.DataFrame({"col": [np.uint8(1)]}, index=pd.date_range("2000-01-02", periods=1))
    lib.append(symbol, df2)

    q = QueryBuilder()
    q = q[q["col"] == 1]
    received = lib.read(symbol, query_builder=q).data
    expected = pd.concat((df1, df2)).query("col == 1")
    assert np.array_equal(expected, received)

    # Fixed width strings, width 1
    df1 = pd.DataFrame({"col": ["a", "b"]}, index=pd.date_range("2000-01-01", periods=2))
    lib.write(symbol, df1, dynamic_strings=False)
    # Fixed width strings, width 2
    df2 = pd.DataFrame({"col": ["a", "bb"]}, index=pd.date_range("2000-01-03", periods=2))
    lib.append(symbol, df2, dynamic_strings=False)
    # Dynamic strings
    df3 = pd.DataFrame({"col": ["a", "bbb"]}, index=pd.date_range("2000-01-05", periods=2))
    lib.append(symbol, df3, dynamic_strings=True)

    q = QueryBuilder()
    q = q[q["col"] == "a"]
    received = lib.read(symbol, query_builder=q).data
    expected = pd.concat((df1, df2, df3)).query("col == 'a'")
    assert np.array_equal(expected, received)


@pytest.mark.parametrize("method", ("isna", "notna", "isnull", "notnull"))
@pytest.mark.parametrize("dtype", (np.int64, np.float32, np.float64, np.datetime64, str))
def test_filter_null_filtering_dynamic(lmdb_version_store_dynamic_schema_v1, method, dtype):
    lib = lmdb_version_store_dynamic_schema_v1
    symbol = "lmdb_version_store_dynamic_schema"
    num_rows = 3
    if dtype is np.int64:
        data = np.arange(num_rows, dtype=dtype)
        # Cannot use int64 min/max here as with the static schema tests, as pd.concat with missing columns promotes the
        # np.int64 columns to np.float64 columns, and astype(np.int64) on this produces incorrect results (presumably
        # due to loss of precision)
        null_values = cycle([100])
    elif dtype in (np.float32, np.float64):
        data = np.arange(num_rows, dtype=dtype)
        null_values = cycle([np.nan])
    elif dtype is np.datetime64:
        data = np.arange(np.datetime64("2024-01-01"), np.datetime64(f"2024-01-0{num_rows + 1}"), np.timedelta64(1, "D")).astype("datetime64[ns]")
        null_values = cycle([np.datetime64("nat")])
    else: # str
        data = [str(idx) for idx in range(num_rows)]
        null_values = cycle([None, np.nan])
    for idx in range(num_rows):
        if idx % 2 == 0:
            data[idx] = next(null_values)

    df_0 = pd.DataFrame({"a": data}, index=np.arange(num_rows))
    lib.write(symbol, df_0)

    df_1 = pd.DataFrame({"b": data}, index=np.arange(num_rows, 2 * num_rows))
    lib.append(symbol, df_1)

    df_2 = pd.DataFrame({"a": data}, index=np.arange(2 * num_rows, 3 * num_rows))
    lib.append(symbol, df_2)

    df = pd.concat([df_0, df_1, df_2])
    expected = df[getattr(df["a"], method)()]
    # We backfill missing int columns with 0s to keep the original dtype, whereas Pandas promotes to float64 in concat
    # when an int column is missing
    if dtype is np.int64:
        expected = expected.fillna(0).astype(np.int64)

    q = QueryBuilder()
    q = q[getattr(q["a"], method)()]
    received = lib.read(symbol, query_builder=q).data
    assert_frame_equal(expected, received)


# Defrag removes column slicing and therefore basically makes any symbol dynamic
def test_filter_with_column_slicing_defragmented(lmdb_version_store_tiny_segment):
    lib = lmdb_version_store_tiny_segment
    symbol = "test_filter_with_column_slicing_defragmented"
    with config_context("SymbolDataCompact.SegmentCount", 0):
        df = pd.DataFrame(
            index=pd.date_range(pd.Timestamp(0), periods=3),
            data={
                "a": pd.date_range("2000-01-01", periods=3),
                "b": pd.date_range("2000-01-01", periods=3),
                "c": pd.date_range("2000-01-01", periods=3),
            },
        )
        pd_ts = pd.Timestamp("2000-01-05")
        for ts in [pd_ts, pd_ts.to_pydatetime()]:
            q = QueryBuilder()
            q = q[q["a"] < ts]
            pandas_query = "a < @ts"
            lib.write(symbol, df[:1])
            lib.append(symbol, df[1:2])
            lib.defragment_symbol_data(symbol, None)
            lib.append(symbol, df[2:])
            received = lib.read(symbol, query_builder=q).data
            expected = df.query(pandas_query)
            assert np.array_equal(expected, received) and (not expected.empty and not received.empty)


def test_filter_unsupported_boolean_operators():
    with pytest.raises(UserInputException):
        q = QueryBuilder()
        q = q[q["a"] and q["b"]]
    with pytest.raises(UserInputException):
        q = QueryBuilder()
        q = q[q["a"] or q["b"]]
    with pytest.raises(UserInputException):
        q = QueryBuilder()
        q = q[not q["a"]]
