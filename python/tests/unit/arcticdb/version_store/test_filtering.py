"""
Copyright 2023 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""
import sys
import copy
import pickle
import math
from datetime import datetime
from hypothesis import assume, given, settings
from hypothesis.extra.pandas import column, data_frames, range_indexes
from hypothesis.extra.pytz import timezones as timezone_st
import hypothesis.strategies as st
from math import inf
import numpy as np
from pandas import DataFrame
import pandas as pd
import pytest
from pytz import timezone
from packaging.version import Version
import random
import string

from arcticdb.config import MACOS_CONDA_BUILD
from arcticdb.exceptions import ArcticNativeException
from arcticdb_ext.storage import KeyType, NoDataFoundException
from arcticdb.version_store.processing import QueryBuilder
from arcticdb_ext.exceptions import InternalException, StorageException, UserInputException
from arcticdb.util.test import assert_frame_equal, PANDAS_VERSION
from arcticdb.util._versions import PANDAS_VERSION
from arcticdb.util.hypothesis import (
    use_of_function_scoped_fixtures_in_hypothesis_checked,
    integral_type_strategies,
    unsigned_integral_type_strategies,
    signed_integral_type_strategies,
    numeric_type_strategies,
    non_zero_numeric_type_strategies,
    string_strategy,
    dataframes_with_names_and_dtypes,
)
from arcticdb_ext import set_config_int


@pytest.fixture(scope="module", autouse=True)
def _restrict_to(only_test_encoding_version_v1):
    pass


def generic_filter_test(version_store, symbol, df, arctic_query, pandas_query, dynamic_strings=True):
    version_store.write(symbol, df, dynamic_strings=dynamic_strings)
    expected = df.query(pandas_query)
    received = version_store.read(symbol, query_builder=arctic_query).data
    if not np.array_equal(expected, received):
        print(f"\nOriginal dataframe:\n{df}\ndtypes:\n{df.dtypes}")
        print(f"\nPandas query: {pandas_query}")
        print(f"\nPandas returns:\n{expected}")
        print(f"\nQueryBuilder returns:\n{received}")
        assert False
    assert True


# For string queries, test both with and without dynamic strings, and with the query both optimised for speed and memory
def generic_filter_test_strings(version_store, symbol, df, arctic_query, pandas_query):
    for dynamic_strings in [True, False]:
        arctic_query.optimise_for_speed()
        generic_filter_test(version_store, symbol, df, arctic_query, pandas_query, dynamic_strings)
        arctic_query.optimise_for_memory()
        generic_filter_test(version_store, symbol, df, arctic_query, pandas_query, dynamic_strings)


def test_querybuilder_shallow_copy(lmdb_version_store):
    df = DataFrame({"a": [0, 1]}, index=np.arange(2))
    q = QueryBuilder()
    q = q[q["a"] > 1]
    q_copy = copy.copy(q)
    pandas_query = "a > 1"
    generic_filter_test(lmdb_version_store, "test_querybuilder_shallow_copy", df, q, pandas_query)
    generic_filter_test(lmdb_version_store, "test_querybuilder_shallow_copy", df, q_copy, pandas_query)


def test_querybuilder_deepcopy(lmdb_version_store):
    df = DataFrame({"a": [0, 1]}, index=np.arange(2))
    q = QueryBuilder()
    q = q[q["a"] > 1]
    q_copy = copy.deepcopy(q)
    pandas_query = "a > 1"
    generic_filter_test(lmdb_version_store, "test_querybuilder_deepcopy", df, q, pandas_query)
    del q
    generic_filter_test(lmdb_version_store, "test_querybuilder_deepcopy", df, q_copy, pandas_query)


def test_querybuilder_pickle(lmdb_version_store):
    df = DataFrame({"a": [0, 1]}, index=np.arange(2))
    q = QueryBuilder()
    q = q[q["a"] > 1]
    q_pickled = pickle.dumps(q)
    pandas_query = "a > 1"
    generic_filter_test(lmdb_version_store, "test_querybuilder_pickle", df, q, pandas_query)
    del q
    q_unpickled = pickle.loads(q_pickled)
    generic_filter_test(lmdb_version_store, "test_querybuilder_pickle", df, q_unpickled, pandas_query)


@pytest.mark.parametrize(
    "lib_type",
    [
        "lmdb_version_store_v1",
        "lmdb_version_store_v2",
        "lmdb_version_store_dynamic_schema_v1",
        "lmdb_version_store_dynamic_schema_v2",
    ],
)
def test_filter_empty_dataframe(request, lib_type):
    lib = request.getfixturevalue(lib_type)
    df = DataFrame({"a": []})
    q = QueryBuilder()
    q = q[q["a"] < 5]
    symbol = "test_filter_empty_dataframe"
    lib.write(symbol, df)
    vit = lib.read(symbol, query_builder=q)
    assert vit.data.empty


def test_filter_column_not_present_static(lmdb_version_store):
    df = DataFrame({"a": np.arange(2)}, index=np.arange(2))
    q = QueryBuilder()
    q = q[q["b"] < 5]
    symbol = "test_filter_column_not_present_static"
    lmdb_version_store.write(symbol, df)
    with pytest.raises(InternalException) as e_info:
        _ = lmdb_version_store.read(symbol, query_builder=q)


@pytest.mark.parametrize(
    "lib_type",
    [
        "lmdb_version_store_v1",
        "lmdb_version_store_v2",
        "lmdb_version_store_dynamic_schema_v1",
        "lmdb_version_store_dynamic_schema_v2",
    ],
)
def test_filter_column_attribute_syntax(request, lib_type):
    lib = request.getfixturevalue(lib_type)
    df = DataFrame({"a": [np.uint8(1), np.uint8(0)]})
    q = QueryBuilder()
    q = q[q.a < np.uint8(1)]
    pandas_query = "a < 1"
    generic_filter_test(lib, "test_filter_column_attribute_syntax", df, q, pandas_query)


# TODO: Remove if square bracket syntax is sufficient for all use cases
# def test_filter_where_syntax(lmdb_version_store):
#     df = DataFrame({"a": [np.uint8(1), np.uint8(0)]})
#     q = QueryBuilder()
#     q = q.where(q["a"] < np.uint8(1))
#     pandas_query = "a < 1"
#     generic_filter_test(lmdb_version_store, "test_filter_where_syntax", df, q, pandas_query)


@pytest.mark.parametrize(
    "lib_type",
    [
        "lmdb_version_store_v1",
        "lmdb_version_store_v2",
        "lmdb_version_store_dynamic_schema_v1",
        "lmdb_version_store_dynamic_schema_v2",
    ],
)
def test_filter_explicit_index(request, lib_type):
    lib = request.getfixturevalue(lib_type)
    df = DataFrame({"a": [np.uint8(1), np.uint8(0)]}, index=np.arange(2))
    q = QueryBuilder()
    q = q[q["a"] < np.uint8(1)]
    pandas_query = "a < 1"
    symbol = "test_filter_explicit_index"
    lib.write(symbol, df)
    assert_frame_equal(df.query(pandas_query), lib.read(symbol, query_builder=q).data)


@pytest.mark.parametrize(
    "lib_type",
    [
        "lmdb_version_store_v1",
        "lmdb_version_store_v2",
        "lmdb_version_store_dynamic_schema_v1",
        "lmdb_version_store_dynamic_schema_v2",
    ],
)
def test_filter_infinite_value(request, lib_type):
    lib = request.getfixturevalue(lib_type)
    df = DataFrame({"a": np.arange(1)})
    q = QueryBuilder()
    with pytest.raises(ArcticNativeException):
        q = q[q["a"] < inf]


@pytest.mark.parametrize(
    "lib_type",
    [
        "lmdb_version_store_v1",
        "lmdb_version_store_v2",
        "lmdb_version_store_dynamic_schema_v1",
        "lmdb_version_store_dynamic_schema_v2",
    ],
)
def test_filter_categorical(request, lib_type):
    lib = request.getfixturevalue(lib_type)
    df = DataFrame({"a": ["hello", "hi", "hello"]}, index=np.arange(3))
    df.a = df.a.astype("category")
    q = QueryBuilder()
    q = q[q.a == "hi"]
    symbol = "test_filter_categorical"
    lib.write(symbol, df)
    with pytest.raises(InternalException) as e_info:
        _ = lib.read(symbol, query_builder=q)


@pytest.mark.parametrize(
    "lib_type",
    [
        "lmdb_version_store_v1",
        "lmdb_version_store_v2",
        "lmdb_version_store_dynamic_schema_v1",
        "lmdb_version_store_dynamic_schema_v2",
    ],
)
def test_filter_pickled_symbol(request, lib_type):
    lib = request.getfixturevalue(lib_type)
    symbol = "test_filter_pickled_symbol"
    lib.write(symbol, np.arange(100).tolist())
    assert lib.is_symbol_pickled(symbol)
    q = QueryBuilder()
    q = q[q.a == 0]
    with pytest.raises(InternalException):
        _ = lib.read(symbol, query_builder=q)


@pytest.mark.parametrize(
    "lib_type",
    [
        "lmdb_version_store_v1",
        "lmdb_version_store_v2",
        "lmdb_version_store_dynamic_schema_v1",
        "lmdb_version_store_dynamic_schema_v2",
    ],
)
def test_filter_date_range_pickled_symbol(request, lib_type):
    lib = request.getfixturevalue(lib_type)
    symbol = "test_filter_date_range_pickled_symbol"
    idx = pd.date_range("2000-01-01", periods=4)
    df = pd.DataFrame({"a": [[1, 2], [3, 4], [5, 6], [7, 8]]}, index=idx)
    lib.write(symbol, df, pickle_on_failure=True)
    assert lib.is_symbol_pickled(symbol)
    with pytest.raises(InternalException) as e_info:
        lib.read(symbol, date_range=(idx[1], idx[2]))


def test_filter_date_range_row_indexed(lmdb_version_store_tiny_segment):
    symbol = "test_filter_date_range_row_indexed"
    df = pd.DataFrame({"a": np.arange(3)}, index=np.arange(3))
    lmdb_version_store_tiny_segment.write(symbol, df)
    with pytest.raises(InternalException) as e_info:
        lmdb_version_store_tiny_segment.read(
            symbol, date_range=(pd.Timestamp("2000-01-01"), pd.Timestamp("2000-01-02"))
        )


def test_filter_clashing_values(lmdb_version_store):
    df = DataFrame({"a": [10, 11, 12], "b": ["11", "12", "13"]}, index=np.arange(3))
    q = QueryBuilder()
    q = q[(q.a == 11) | (q.b == "11")]
    pandas_query = "(a == 11) | (b == '11')"
    generic_filter_test_strings(lmdb_version_store, "test_filter_clashing_values", df, q, pandas_query)


def test_filter_bool_nonbool_comparison(lmdb_version_store):
    symbol = "test_filter_bool_nonbool_comparison"
    lib = lmdb_version_store
    df = DataFrame({"string": ["True", "False"], "numeric": [1, 0], "bool": [True, False]}, index=np.arange(2))
    lib.write(symbol, df)

    # bool column to string column
    q = QueryBuilder()
    q = q[q["bool"] == q["string"]]
    with pytest.raises(InternalException) as e_info:
        lib.read(symbol, query_builder=q)
    # bool column to numeric column
    q = QueryBuilder()
    q = q[q["bool"] == q["numeric"]]
    with pytest.raises(InternalException) as e_info:
        lib.read(symbol, query_builder=q)
    # bool column to string value
    q = QueryBuilder()
    q = q[q["bool"] == "test"]
    with pytest.raises(InternalException) as e_info:
        lib.read(symbol, query_builder=q)
    # bool column to numeric value
    q = QueryBuilder()
    q = q[q["bool"] == 0]
    with pytest.raises(InternalException) as e_info:
        lib.read(symbol, query_builder=q)
    # string column to bool value
    q = QueryBuilder()
    q = q[q["string"] == True]
    with pytest.raises(InternalException) as e_info:
        lib.read(symbol, query_builder=q)
    # numeric column to bool value
    q = QueryBuilder()
    q = q[q["numeric"] == True]
    with pytest.raises(InternalException) as e_info:
        lib.read(symbol, query_builder=q)


def test_filter_bool_column(lmdb_version_store):
    df = DataFrame({"a": [True, False]}, index=np.arange(2))
    q = QueryBuilder()
    q = q[q["a"]]
    pandas_query = "a"
    generic_filter_test(lmdb_version_store, "test_filter_bool_column", df, q, pandas_query)


def test_filter_bool_column_not(lmdb_version_store):
    df = DataFrame({"a": [True, False]}, index=np.arange(2))
    q = QueryBuilder()
    q = q[~q["a"]]
    pandas_query = "~a"
    generic_filter_test(lmdb_version_store, "test_filter_bool_column_not", df, q, pandas_query)


def test_filter_bool_column_binary_boolean(lmdb_version_store):
    df = DataFrame({"a": [True, True, False, False], "b": [True, False, True, False]}, index=np.arange(4))
    q = QueryBuilder()
    q = q[q["a"] & q["b"]]
    pandas_query = "a & b"
    generic_filter_test(lmdb_version_store, "test_filter_bool_column_binary_boolean", df, q, pandas_query)


def test_filter_bool_column_comparison(lmdb_version_store):
    df = DataFrame({"a": [True, False]}, index=np.arange(2))
    comparators = ["==", "!=", "<", "<=", ">", ">="]
    for comparator in comparators:
        for bool_value in [True, False]:
            pandas_query = f"a {comparator} {bool_value}"
            q = QueryBuilder()
            if comparator == "==":
                q = q[q["a"] == bool_value]
            elif comparator == "!=":
                q = q[q["a"] != bool_value]
            elif comparator == "<":
                q = q[q["a"] < bool_value]
            elif comparator == "<=":
                q = q[q["a"] <= bool_value]
            elif comparator == ">":
                q = q[q["a"] > bool_value]
            elif comparator == ">=":
                q = q[q["a"] >= bool_value]
            generic_filter_test(lmdb_version_store, "test_filter_bool_column_comparison", df, q, pandas_query)


@use_of_function_scoped_fixtures_in_hypothesis_checked
@settings(deadline=None)
@given(
    df=data_frames([column("a", elements=numeric_type_strategies())], index=range_indexes()),
    val=numeric_type_strategies(),
)
def test_filter_less_than_col_val(lmdb_version_store, df, val):
    assume(not df.empty)
    q = QueryBuilder()
    q = q[q["a"] < val]
    pandas_query = "a < {}".format(val)
    generic_filter_test(lmdb_version_store, "test_filter_less_than_col_val", df, q, pandas_query)


@use_of_function_scoped_fixtures_in_hypothesis_checked
@settings(deadline=None)
@given(
    df=data_frames([column("a", elements=numeric_type_strategies())], index=range_indexes()),
    val=numeric_type_strategies(),
)
def test_filter_less_than_val_col(lmdb_version_store, df, val):
    assume(not df.empty)
    q = QueryBuilder()
    q = q[val < q["a"]]
    pandas_query = "{} < a".format(val)
    generic_filter_test(lmdb_version_store, "test_filter_less_than_val_col", df, q, pandas_query)


@use_of_function_scoped_fixtures_in_hypothesis_checked
@settings(deadline=None)
@given(
    df=data_frames(
        [column("a", elements=numeric_type_strategies()), column("b", elements=numeric_type_strategies())],
        index=range_indexes(),
    )
)
def test_filter_less_than_col_col(lmdb_version_store, df):
    assume(not df.empty)
    q = QueryBuilder()
    q = q[q["a"] < q["b"]]
    pandas_query = "a < b"
    generic_filter_test(lmdb_version_store, "test_filter_less_than_col_col", df, q, pandas_query)


@use_of_function_scoped_fixtures_in_hypothesis_checked
@settings(deadline=None)
@given(
    df=data_frames([column("a", elements=numeric_type_strategies())], index=range_indexes()),
    val=numeric_type_strategies(),
)
def test_filter_less_than_equals_col_val(lmdb_version_store, df, val):
    assume(not df.empty)
    q = QueryBuilder()
    q = q[q["a"] <= val]
    pandas_query = "a <= {}".format(val)
    generic_filter_test(lmdb_version_store, "test_filter_less_than_equals_col_val", df, q, pandas_query)


@use_of_function_scoped_fixtures_in_hypothesis_checked
@settings(deadline=None)
@given(
    df=data_frames([column("a", elements=numeric_type_strategies())], index=range_indexes()),
    val=numeric_type_strategies(),
)
def test_filter_less_than_equals_val_col(lmdb_version_store, df, val):
    assume(not df.empty)
    q = QueryBuilder()
    q = q[val <= q["a"]]
    pandas_query = "{} <= a".format(val)
    generic_filter_test(lmdb_version_store, "test_filter_less_than_equals_val_col", df, q, pandas_query)


@use_of_function_scoped_fixtures_in_hypothesis_checked
@settings(deadline=None)
@given(
    df=data_frames(
        [column("a", elements=numeric_type_strategies()), column("b", elements=numeric_type_strategies())],
        index=range_indexes(),
    )
)
def test_filter_less_than_equals_col_col(lmdb_version_store, df):
    assume(not df.empty)
    q = QueryBuilder()
    q = q[q["a"] <= q["b"]]
    pandas_query = "a <= b"
    generic_filter_test(lmdb_version_store, "test_filter_less_than_equals_col_col", df, q, pandas_query)


@use_of_function_scoped_fixtures_in_hypothesis_checked
@settings(deadline=None)
@given(
    df=data_frames([column("a", elements=numeric_type_strategies())], index=range_indexes()),
    val=numeric_type_strategies(),
)
def test_filter_greater_than_col_val(lmdb_version_store, df, val):
    assume(not df.empty)
    q = QueryBuilder()
    q = q[q["a"] > val]
    pandas_query = "a > {}".format(val)
    generic_filter_test(lmdb_version_store, "test_filter_greater_than_col_val", df, q, pandas_query)


@use_of_function_scoped_fixtures_in_hypothesis_checked
@settings(deadline=None)
@given(
    df=data_frames([column("a", elements=numeric_type_strategies())], index=range_indexes()),
    val=numeric_type_strategies(),
)
def test_filter_greater_than_val_col(lmdb_version_store, df, val):
    assume(not df.empty)
    q = QueryBuilder()
    q = q[val > q["a"]]
    pandas_query = "{} > a".format(val)
    generic_filter_test(lmdb_version_store, "test_filter_greater_than_val_col", df, q, pandas_query)


@use_of_function_scoped_fixtures_in_hypothesis_checked
@settings(deadline=None)
@given(
    df=data_frames(
        [column("a", elements=numeric_type_strategies()), column("b", elements=numeric_type_strategies())],
        index=range_indexes(),
    )
)
def test_filter_greater_than_col_col(lmdb_version_store, df):
    assume(not df.empty)
    q = QueryBuilder()
    q = q[q["a"] > q["b"]]
    pandas_query = "a > b"
    generic_filter_test(lmdb_version_store, "test_filter_greater_than_col_col", df, q, pandas_query)


@use_of_function_scoped_fixtures_in_hypothesis_checked
@settings(deadline=None)
@given(
    df=data_frames([column("a", elements=numeric_type_strategies())], index=range_indexes()),
    val=numeric_type_strategies(),
)
def test_filter_greater_than_equals_col_val(lmdb_version_store, df, val):
    assume(not df.empty)
    q = QueryBuilder()
    q = q[q["a"] >= val]
    pandas_query = "a >= {}".format(val)
    generic_filter_test(lmdb_version_store, "test_filter_greater_than_equals_col_val", df, q, pandas_query)


@use_of_function_scoped_fixtures_in_hypothesis_checked
@settings(deadline=None)
@given(
    df=data_frames([column("a", elements=numeric_type_strategies())], index=range_indexes()),
    val=numeric_type_strategies(),
)
def test_filter_greater_than_equals_val_col(lmdb_version_store, df, val):
    assume(not df.empty)
    q = QueryBuilder()
    q = q[val >= q["a"]]
    pandas_query = "{} >= a".format(val)
    generic_filter_test(lmdb_version_store, "test_filter_greater_than_equals_val_col", df, q, pandas_query)


@use_of_function_scoped_fixtures_in_hypothesis_checked
@settings(deadline=None)
@given(
    df=data_frames(
        [column("a", elements=numeric_type_strategies()), column("b", elements=numeric_type_strategies())],
        index=range_indexes(),
    )
)
def test_filter_greater_than_equals_col_col(lmdb_version_store, df):
    assume(not df.empty)
    q = QueryBuilder()
    q = q[q["a"] >= q["b"]]
    pandas_query = "a >= b"
    generic_filter_test(lmdb_version_store, "test_filter_greater_than_equals_col_col", df, q, pandas_query)


@use_of_function_scoped_fixtures_in_hypothesis_checked
@settings(deadline=None)
@given(
    df=data_frames([column("a", elements=integral_type_strategies())], index=range_indexes()),
    val=integral_type_strategies(),
)
def test_filter_equals_col_val(lmdb_version_store, df, val):
    assume(not df.empty)
    q = QueryBuilder()
    q = q[q["a"] == val]
    pandas_query = "a == {}".format(val)
    generic_filter_test(lmdb_version_store, "test_filter_equals_col_val", df, q, pandas_query)


@use_of_function_scoped_fixtures_in_hypothesis_checked
@settings(deadline=None)
@given(
    df=data_frames([column("a", elements=integral_type_strategies())], index=range_indexes()),
    val=integral_type_strategies(),
)
def test_filter_equals_val_col(lmdb_version_store, df, val):
    assume(not df.empty)
    q = QueryBuilder()
    q = q[val == q["a"]]
    pandas_query = "{} == a".format(val)
    generic_filter_test(lmdb_version_store, "test_filter_equals_val_col", df, q, pandas_query)


@use_of_function_scoped_fixtures_in_hypothesis_checked
@settings(deadline=None)
@given(
    df=data_frames(
        [column("a", elements=integral_type_strategies()), column("b", elements=integral_type_strategies())],
        index=range_indexes(),
    )
)
def test_filter_equals_col_col(lmdb_version_store, df):
    assume(not df.empty)
    q = QueryBuilder()
    q = q[q["a"] == q["b"]]
    pandas_query = "a == b"
    generic_filter_test(lmdb_version_store, "test_filter_equals_col_col", df, q, pandas_query)


@use_of_function_scoped_fixtures_in_hypothesis_checked
@settings(deadline=None)
@given(
    df=data_frames([column("a", elements=integral_type_strategies())], index=range_indexes()),
    val=integral_type_strategies(),
)
def test_filter_not_equals_col_val(lmdb_version_store, df, val):
    assume(not df.empty)
    q = QueryBuilder()
    q = q[q["a"] != val]
    pandas_query = "a != {}".format(val)
    generic_filter_test(lmdb_version_store, "test_filter_not_equals_col_val", df, q, pandas_query)


@use_of_function_scoped_fixtures_in_hypothesis_checked
@settings(deadline=None)
@given(
    df=data_frames([column("a", elements=integral_type_strategies())], index=range_indexes()),
    val=integral_type_strategies(),
)
def test_filter_not_equals_val_col(lmdb_version_store, df, val):
    assume(not df.empty)
    q = QueryBuilder()
    q = q[val != q["a"]]
    pandas_query = "{} != a".format(val)
    generic_filter_test(lmdb_version_store, "test_filter_not_equals_val_col", df, q, pandas_query)


@use_of_function_scoped_fixtures_in_hypothesis_checked
@settings(deadline=None)
@given(
    df=data_frames(
        [column("a", elements=integral_type_strategies()), column("b", elements=integral_type_strategies())],
        index=range_indexes(),
    )
)
def test_filter_not_equals_col_col(lmdb_version_store, df):
    assume(not df.empty)
    q = QueryBuilder()
    q = q[q["a"] != q["b"]]
    pandas_query = "a != b"
    generic_filter_test(lmdb_version_store, "test_filter_not_equals_col_col", df, q, pandas_query)


def test_filter_datetime_naive(lmdb_version_store):
    symbol = "test_filter_datetime_simple"
    df = pd.DataFrame({"a": pd.date_range("2000-01-01", periods=10)})
    pd_ts = pd.Timestamp("2000-01-05")
    for ts in [pd_ts, pd_ts.to_pydatetime()]:
        q = QueryBuilder()
        q = q[q["a"] < ts]
        pandas_query = "a < @ts"
        # Cannot use generic_filter_test as pandas query involves variable
        lmdb_version_store.write(symbol, df)
        expected = df.query(pandas_query)
        received = lmdb_version_store.read(symbol, query_builder=q).data
        if not np.array_equal(expected, received) and (not expected.empty and not received.empty):
            print("ts\n{}".format(ts))
            print("Original dataframe\n{}".format(df))
            print("Pandas query\n{}".format(pandas_query))
            print("Expected\n{}".format(expected))
            print("Received\n{}".format(received))
            assert False
        assert True


def test_filter_datetime_isin(lmdb_version_store):
    symbol = "test_filter_datetime_isin"
    df = pd.DataFrame({"a": pd.date_range("2000-01-01", periods=10)})
    pd_ts = pd.Timestamp("2000-01-05")
    for ts in [pd_ts, pd_ts.to_pydatetime()]:
        q = QueryBuilder()
        q = q[q["a"] == [ts]]
        pandas_query = "a in [@ts]"
        # Cannot use generic_filter_test as pandas query involves variable
        lmdb_version_store.write(symbol, df)
        expected = df.query(pandas_query)
        received = lmdb_version_store.read(symbol, query_builder=q).data
        if not np.array_equal(expected, received) and (not expected.empty and not received.empty):
            print("ts\n{}".format(ts))
            print("Original dataframe\n{}".format(df))
            print("Pandas query\n{}".format(pandas_query))
            print("Expected\n{}".format(expected))
            print("Received\n{}".format(received))
            assert False
        assert True


def test_filter_datetime_timedelta(lmdb_version_store):
    symbol = "test_filter_datetime_timedelta"
    df = pd.DataFrame({"a": pd.date_range("2000-01-01", periods=10)})
    pd_ts = pd.Timestamp("2000-01-05")
    pd_td = pd.Timedelta(1, "d")
    for ts in [pd_ts, pd_ts.to_pydatetime()]:
        for td in [pd_td, pd_td.to_pytimedelta()]:
            q = QueryBuilder()
            q = q[(q["a"] + td) < ts]
            lmdb_version_store.write(symbol, df)
            expected = df[(df["a"] + td) < ts]
            received = lmdb_version_store.read(symbol, query_builder=q).data
            if not np.array_equal(expected, received) and (not expected.empty and not received.empty):
                print("ts\n{}".format(ts))
                print("td\n{}".format(td))
                print("Original dataframe\n{}".format(df))
                print("Expected\n{}".format(expected))
                print("Received\n{}".format(received))
                assert False
            assert True


def test_filter_datetime_timezone_aware(lmdb_version_store):
    symbol = "test_filter_datetime_timezone_aware"
    df = pd.DataFrame({"a": pd.date_range("2000-01-01", periods=10, tz=timezone("Europe/Amsterdam"))})
    pd_ts = pd.Timestamp("2000-01-05", tz=timezone("GMT"))
    print("pd_ts\n{}".format(pd_ts))
    for ts in [pd_ts, pd_ts.to_pydatetime()]:
        q = QueryBuilder()
        q = q[q["a"] < ts]
        pandas_query = "a < @ts"
        # Cannot use generic_filter_test as roundtripping a dataframe with datetime64 columns does not preserve tz info
        lmdb_version_store.write(symbol, df)
        expected = df.query(pandas_query)
        # Convert to UTC and strip tzinfo to match behaviour of roundtripping through Arctic
        expected["a"] = expected["a"].apply(lambda x: x.tz_convert(timezone("utc")).tz_localize(None))
        received = lmdb_version_store.read(symbol, query_builder=q).data
        if not np.array_equal(expected, received) and (not expected.empty and not received.empty):
            print("ts\n{}".format(ts))
            print("Original dataframe\n{}".format(df))
            print("Expected\n{}".format(expected))
            print("Received\n{}".format(received))
            assert False
        assert True


@use_of_function_scoped_fixtures_in_hypothesis_checked
# Restrict datetime range to a couple of years, as this should be sufficient to catch most weird corner cases
@settings(deadline=None)
@given(
    df_dt=st.datetimes(min_value=datetime(2020, 1, 1), max_value=datetime(2022, 1, 1), timezones=timezone_st()),
    comparison_dt=st.datetimes(min_value=datetime(2020, 1, 1), max_value=datetime(2022, 1, 1), timezones=timezone_st()),
)
def test_filter_datetime_timezone_aware_hypothesis(version_store_factory, df_dt, comparison_dt):
    lmdb_version_store = version_store_factory(name="_unique_")
    symbol = "test_filter_datetime_timezone_aware_hypothesis"
    df = pd.DataFrame({"a": [df_dt]})
    for ts in [comparison_dt, pd.Timestamp(comparison_dt)]:
        q = QueryBuilder()
        q = q[q["a"] < ts]
        pandas_query = "a < @ts"
        # Cannot use generic_filter_test as roundtripping a dataframe with datetime64 columns does not preserve tz info
        lmdb_version_store.write(symbol, df)
        expected = df.query(pandas_query)
        # Convert to UTC and strip tzinfo to match behaviour of roundtripping through Arctic
        expected["a"] = expected["a"].apply(lambda x: x.tz_convert(timezone("utc")).tz_localize(None))
        received = lmdb_version_store.read(symbol, query_builder=q).data
        if not np.array_equal(expected, received) and (not expected.empty and not received.empty):
            print("ts\n{}".format(ts))
            print("Original dataframe\n{}".format(df))
            print("Expected\n{}".format(expected))
            print("Received\n{}".format(received))
            assert False
        assert True


def test_filter_datetime_nanoseconds(lmdb_version_store):
    sym = "test_filter_datetime_nanoseconds"

    # Dataframe has three rows and a single column containing timestamps 1 nanosecond apart
    timestamp_1 = pd.Timestamp("2023-03-15 10:30:00")
    timestamp_0 = timestamp_1 - pd.Timedelta(1, unit="ns")
    timestamp_2 = timestamp_1 + pd.Timedelta(1, unit="ns")
    df = pd.DataFrame(data=[timestamp_0, timestamp_1, timestamp_2], columns=["col"])

    lmdb_version_store.write(sym, df)

    # Try to read all rows
    qb_all = QueryBuilder()
    qb_all = qb_all[(qb_all["col"] >= timestamp_0) & (qb_all["col"] <= timestamp_2)]
    all_rows_result = lmdb_version_store.read(sym, query_builder=qb_all).data
    assert_frame_equal(all_rows_result, df)

    # Try to read only the first row
    qb_first = QueryBuilder()
    qb_first = qb_first[(qb_first["col"] >= timestamp_0) & (qb_first["col"] <= timestamp_0)]
    first_row_result = lmdb_version_store.read(sym, query_builder=qb_first).data
    assert_frame_equal(first_row_result, df.iloc[[0]])

    # Try to read first and second rows
    qb_first_and_second = QueryBuilder()
    qb_first_and_second = qb_first_and_second[
        (qb_first_and_second["col"] >= timestamp_0) & (qb_first_and_second["col"] <= timestamp_1)
    ]
    first_and_second_row_result = lmdb_version_store.read(sym, query_builder=qb_first_and_second).data
    assert_frame_equal(first_and_second_row_result, df.iloc[[0, 1]])

    # Try to read second and third rows
    qb_second_and_third = QueryBuilder()
    qb_second_and_third = qb_second_and_third[
        (qb_second_and_third["col"] >= timestamp_1) & (qb_second_and_third["col"] <= timestamp_2)
    ]
    second_and_third_row_result = lmdb_version_store.read(sym, query_builder=qb_second_and_third).data
    assert_frame_equal(second_and_third_row_result, df.iloc[[1, 2]].reset_index(drop=True))


@use_of_function_scoped_fixtures_in_hypothesis_checked
@settings(deadline=None)
@given(df=data_frames([column("a", elements=string_strategy)], index=range_indexes()), val=numeric_type_strategies())
def test_filter_compare_string_number_col_val(lmdb_version_store, df, val):
    assume(not df.empty)
    q = QueryBuilder()
    q = q[q["a"] < val]
    symbol = "test_filter_compare_string_number_col_val"
    lmdb_version_store.write(symbol, df)
    with pytest.raises(InternalException) as e_info:
        _ = lmdb_version_store.read(symbol, query_builder=q)
    q = QueryBuilder()
    q = q[val < q["a"]]
    with pytest.raises(InternalException) as e_info:
        _ = lmdb_version_store.read(symbol, query_builder=q)


@use_of_function_scoped_fixtures_in_hypothesis_checked
@settings(deadline=None)
@given(df=data_frames([column("a", elements=numeric_type_strategies())], index=range_indexes()), val=string_strategy)
def test_filter_compare_string_number_val_col(lmdb_version_store, df, val):
    assume(not df.empty)
    q = QueryBuilder()
    q = q[val < q["a"]]
    symbol = "test_filter_compare_string_number_val_col"
    lmdb_version_store.write(symbol, df)
    with pytest.raises(InternalException) as e_info:
        _ = lmdb_version_store.read(symbol, query_builder=q)
    q = QueryBuilder()
    q = q[q["a"] < val]
    with pytest.raises(InternalException) as e_info:
        _ = lmdb_version_store.read(symbol, query_builder=q)


@use_of_function_scoped_fixtures_in_hypothesis_checked
@settings(deadline=None)
@given(
    df=data_frames(
        [column("a", elements=string_strategy), column("b", elements=numeric_type_strategies())], index=range_indexes()
    )
)
def test_filter_compare_string_number_col_col(lmdb_version_store, df):
    assume(not df.empty)
    q = QueryBuilder()
    q = q[q["a"] < q["b"]]
    symbol = "test_filter_compare_string_number_col_col"
    lmdb_version_store.write(symbol, df)
    with pytest.raises(InternalException) as e_info:
        _ = lmdb_version_store.read(symbol, query_builder=q)
    q = QueryBuilder()
    q = q[q["b"] < q["a"]]
    with pytest.raises(InternalException) as e_info:
        _ = lmdb_version_store.read(symbol, query_builder=q)


@use_of_function_scoped_fixtures_in_hypothesis_checked
# Note min_size=1 for the sets in the following two tests, as an empty set does not have an associated type
@settings(deadline=None)
@given(
    df=data_frames([column("a", elements=string_strategy)], index=range_indexes()),
    vals=st.frozensets(signed_integral_type_strategies(), min_size=1),
)
def test_filter_isin_string_number_signed(lmdb_version_store, df, vals):
    assume(not df.empty)
    q = QueryBuilder()
    q = q[q["a"].isin(vals)]
    symbol = "test_filter_isin_string_number"
    lmdb_version_store.write(symbol, df, dynamic_strings=True)
    with pytest.raises(InternalException) as e_info:
        _ = lmdb_version_store.read(symbol, query_builder=q)


@use_of_function_scoped_fixtures_in_hypothesis_checked
# Note min_size=1 for the sets in the following two tests, as an empty set does not have an associated type
@settings(deadline=None)
@given(
    df=data_frames([column("a", elements=string_strategy)], index=range_indexes()),
    vals=st.frozensets(unsigned_integral_type_strategies(), min_size=1),
)
def test_filter_isin_string_number_unsigned(lmdb_version_store, df, vals):
    assume(not df.empty)
    q = QueryBuilder()
    q = q[q["a"].isin(vals)]
    symbol = "test_filter_isin_string_number"
    lmdb_version_store.write(symbol, df, dynamic_strings=True)
    with pytest.raises(InternalException) as e_info:
        _ = lmdb_version_store.read(symbol, query_builder=q)


@use_of_function_scoped_fixtures_in_hypothesis_checked
@settings(deadline=None)
@given(
    df=data_frames([column("a", elements=integral_type_strategies())], index=range_indexes()),
    vals=st.frozensets(string_strategy, min_size=1),
)
def test_filter_isin_number_string(lmdb_version_store, df, vals):
    assume(not df.empty)
    q = QueryBuilder()
    q = q[q["a"].isin(vals)]
    symbol = "test_filter_isin_number_string"
    lmdb_version_store.write(symbol, df)
    with pytest.raises(InternalException) as e_info:
        _ = lmdb_version_store.read(symbol, query_builder=q)


def test_filter_isin_clashing_sets(lmdb_version_store):
    a_unique_val = 100000
    b_unique_val = 200000
    df = DataFrame({"a": [-1, a_unique_val, -1], "b": [-1, -1, b_unique_val]}, index=np.arange(3))
    q = QueryBuilder()
    vals1 = np.arange(10000, dtype=np.uint64)
    np.put(vals1, 5000, a_unique_val)
    vals2 = np.arange(10000, dtype=np.uint64)
    np.put(vals2, 5000, b_unique_val)
    assert str(vals1) == str(vals2)
    q = q[(q["a"].isin(vals1)) | (q["b"].isin(vals2))]
    pandas_query = "(a in {}) | (b in {})".format([a_unique_val], [b_unique_val])
    generic_filter_test(lmdb_version_store, "test_filter_isin_clashing_sets", df, q, pandas_query)


@use_of_function_scoped_fixtures_in_hypothesis_checked
@settings(deadline=None)
@given(
    df=dataframes_with_names_and_dtypes(["a"], integral_type_strategies()),
    vals=st.frozensets(signed_integral_type_strategies(), min_size=1),
)
def test_filter_numeric_isin_signed(lmdb_version_store, df, vals):
    assume(not df.empty)
    q = QueryBuilder()
    q = q[q["a"].isin(vals)]
    pandas_query = "a in {}".format(list(vals))
    generic_filter_test(lmdb_version_store, "test_filter_numeric_isin", df, q, pandas_query)


@use_of_function_scoped_fixtures_in_hypothesis_checked
@settings(deadline=None)
@given(
    df=dataframes_with_names_and_dtypes(["a"], integral_type_strategies()),
    vals=st.frozensets(unsigned_integral_type_strategies(), min_size=1),
)
def test_filter_numeric_isin_unsigned(lmdb_version_store, df, vals):
    assume(not df.empty)
    q = QueryBuilder()
    q = q[q["a"].isin(vals)]
    pandas_query = "a in {}".format(list(vals))
    generic_filter_test(lmdb_version_store, "test_filter_numeric_isin", df, q, pandas_query)


@pytest.mark.parametrize(
    "df_col,isin_vals,expected_col",
    [
        ([0, 1, 2**62], [0, 1, -1], [0, 1]),
        ([0, 1, 2**63 - 1], [0, 1, -1], [0, 1]),
        ([0, 1, 2**62], [0, 1, -1], [0, 1]),
        ([-1, 0, 1], [0, 1, 2**62], [0, 1]),
    ],
)
def test_filter_numeric_isin_hashing_overflows(lmdb_version_store, df_col, isin_vals, expected_col):
    df = pd.DataFrame({"a": df_col})
    lmdb_version_store.write("test_filter_numeric_isin_hashing_overflows", df)

    q = QueryBuilder()
    q = q[q["a"].isin(isin_vals)]
    result = lmdb_version_store.read("test_filter_numeric_isin_hashing_overflows", query_builder=q).data

    expected = pd.DataFrame({"a": expected_col})
    assert_frame_equal(expected, result)


def test_filter_numeric_isin_unsigned(lmdb_version_store):
    df = pd.DataFrame({"a": [0, 1, 2**64 - 1]})
    lmdb_version_store.write("test_filter_numeric_isin_unsigned", df)

    q = QueryBuilder()
    q = q[q["a"].isin([0, 1, 2])]
    result = lmdb_version_store.read("test_filter_numeric_isin_unsigned", query_builder=q).data

    expected = pd.DataFrame({"a": [0, 1]}, dtype=np.uint64)
    assert_frame_equal(expected, result)


@use_of_function_scoped_fixtures_in_hypothesis_checked
@settings(deadline=None)
@given(
    df=dataframes_with_names_and_dtypes(["a"], integral_type_strategies()),
    vals=st.frozensets(unsigned_integral_type_strategies(), min_size=1),
)
@pytest.mark.skipif(PANDAS_VERSION < Version("1.2"), reason="Early Pandas filtering does not handle unsigned well")
def test_filter_numeric_isnotin_unsigned(lmdb_version_store, df, vals):
    assume(not df.empty)
    q = QueryBuilder()
    q = q[q["a"].isnotin(vals)]
    pandas_query = "a not in {}".format(list(vals))
    generic_filter_test(lmdb_version_store, "test_filter_numeric_isnotin", df, q, pandas_query)


@use_of_function_scoped_fixtures_in_hypothesis_checked
@settings(deadline=None)
@given(
    df=dataframes_with_names_and_dtypes(["a"], integral_type_strategies()),
    vals=st.frozensets(signed_integral_type_strategies(), min_size=1),
)
def test_filter_numeric_isnotin_signed(lmdb_version_store, df, vals):
    assume(not df.empty)
    q = QueryBuilder()
    q = q[q["a"].isnotin(vals)]
    pandas_query = "a not in {}".format(list(vals))
    generic_filter_test(lmdb_version_store, "test_filter_numeric_isnotin", df, q, pandas_query)


def test_filter_numeric_isnotin_mixed_types_exception():
    vals = [np.int64(-1), np.uint64(4294967296)]
    q = QueryBuilder()
    with pytest.raises(UserInputException) as e_info:
        q = q[q["a"].isnotin(vals)]


def test_filter_numeric_isnotin_hashing_overflow(lmdb_version_store):
    df = pd.DataFrame({"a": [256]})
    lmdb_version_store.write("test_filter_numeric_isnotin_hashing_overflow", df)

    q = QueryBuilder()
    isnotin_vals = np.array([], np.uint8)
    q = q[q["a"].isnotin(isnotin_vals)]
    result = lmdb_version_store.read("test_filter_numeric_isnotin_hashing_overflow", query_builder=q).data

    assert_frame_equal(df, result)


_uint64_max = np.iinfo(np.uint64).max


@pytest.mark.parametrize("op", ("in", "not in"))
@pytest.mark.parametrize("signed_type", (np.int8, np.int16, np.int32, np.int64))
@pytest.mark.parametrize("uint64_in", ("df", "vals") if PANDAS_VERSION >= Version("1.2") else ("vals",))
def test_filter_numeric_membership_mixing_int64_and_uint64(lmdb_version_store, op, signed_type, uint64_in):
    signed = signed_type(-1)
    if uint64_in == "df":
        df, vals = pd.DataFrame({"a": [_uint64_max]}), [signed]
    else:
        df, vals = pd.DataFrame({"a": [signed]}), [_uint64_max]

    q = QueryBuilder()
    q = q[q["a"].isin(vals) if op == "in" else q["a"].isnotin(vals)]
    pandas_query = f"a {op} {vals}"
    generic_filter_test(lmdb_version_store, "test_filter_numeric_mixing", df, q, pandas_query)


@use_of_function_scoped_fixtures_in_hypothesis_checked
@settings(deadline=None)
@given(df=dataframes_with_names_and_dtypes(["a"], integral_type_strategies()))
def test_filter_numeric_isnotin_empty_set(lmdb_version_store, df):
    assume(not df.empty)
    q = QueryBuilder()
    vals = []
    q = q[q["a"].isnotin(vals)]
    pandas_query = "a not in {}".format(vals)
    generic_filter_test(lmdb_version_store, "test_filter_numeric_isnotin_empty_set", df, q, pandas_query)


def test_filter_nones_and_nans_retained_in_string_column(lmdb_version_store):
    lib = lmdb_version_store
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


@use_of_function_scoped_fixtures_in_hypothesis_checked
@settings(deadline=None)
@given(
    df=data_frames([column("a", elements=string_strategy)], index=range_indexes()),
    vals=st.frozensets(string_strategy, min_size=1),
)
def test_filter_string_isin(lmdb_version_store, df, vals):
    assume(not df.empty)
    q = QueryBuilder()
    q = q[q["a"].isin(vals)]
    pandas_query = "a in {}".format(list(vals))
    generic_filter_test_strings(lmdb_version_store, "test_filter_string_isin", df, q, pandas_query)


# Tests that false matches aren't generated when list members truncate to column values
def test_filter_fixed_width_string_isin_truncation(lmdb_version_store):
    df = DataFrame({"a": ["1"]}, index=np.arange(1))
    vals = ["12"]
    q = QueryBuilder()
    q = q[q["a"].isin(vals)]
    pandas_query = "a in {}".format(list(vals))
    generic_filter_test(
        lmdb_version_store, "test_filter_fixed_width_string_isin_truncation", df, q, pandas_query, dynamic_strings=False
    )


@use_of_function_scoped_fixtures_in_hypothesis_checked
@settings(deadline=None)
@given(df=data_frames([column("a", elements=string_strategy)], index=range_indexes()))
def test_filter_string_isin_empty_set(lmdb_version_store, df):
    assume(not df.empty)
    vals = []
    q = QueryBuilder()
    q = q[q["a"].isin(vals)]
    pandas_query = "a in {}".format(list(vals))
    generic_filter_test_strings(lmdb_version_store, "test_filter_string_isin_empty_set", df, q, pandas_query)


@use_of_function_scoped_fixtures_in_hypothesis_checked
@settings(deadline=None)
@given(
    df=data_frames([column("a", elements=string_strategy)], index=range_indexes()),
    vals=st.frozensets(string_strategy, min_size=1),
)
def test_filter_string_isnotin(lmdb_version_store, df, vals):
    assume(not df.empty)
    q = QueryBuilder()
    q = q[q["a"].isnotin(vals)]
    pandas_query = "a not in {}".format(list(vals))
    generic_filter_test_strings(lmdb_version_store, "test_filter_string_isnotin", df, q, pandas_query)


@use_of_function_scoped_fixtures_in_hypothesis_checked
@settings(deadline=None)
@given(df=data_frames([column("a", elements=string_strategy)], index=range_indexes()))
def test_filter_string_isnotin_empty_set(lmdb_version_store, df):
    assume(not df.empty)
    vals = []
    q = QueryBuilder()
    q = q[q["a"].isnotin(vals)]
    pandas_query = "a not in {}".format(list(vals))
    generic_filter_test_strings(lmdb_version_store, "test_filter_string_isnotin_empty_set", df, q, pandas_query)


def test_filter_stringpool_shrinking_basic(lmdb_version_store_tiny_segment):
    # Construct a dataframe and QueryBuilder pair with the following properties:
    # - original dataframe spanning multiple segments horizontally and vertically (tiny segment == 2x2)
    # - strings of varying lengths to exercise fixed width strings more completely
    # - repeated strings within a segment
    # - at least one segment will need all of the strings in it's pool after filtering
    # - at least one segment will need none of the strings in it's pool after filtering
    # - at least one segment will need some, but not all of the strings in it's pool after filtering
    df = DataFrame(
        {
            "a": ["a1", "a2", "a3", "a4", "a5"],
            "b": ["b11", "b22", "b3", "b4", "b5"],
            "c": ["c1", "c2", "c3", "c4", "c5"],
            "d": ["d11", "d2", "d3", "d4", "d5"],
        }
    )
    q = QueryBuilder()
    q = q[q["a"] != "a1"]
    q.optimise_for_memory()
    pandas_query = "a != 'a1'"
    generic_filter_test_strings(
        lmdb_version_store_tiny_segment, "test_filter_stringpool_shrinking_1", df, q, pandas_query
    )


def test_filter_stringpool_shrinking_block_alignment(lmdb_version_store):
    # Create a dataframe with more than one block (3968 bytes) worth of strings for the stringpool
    string_length = 10
    num_rows = 1000
    data = ["".join(random.choice(string.ascii_uppercase) for _ in range(string_length)) for unused in range(num_rows)]
    df = pd.DataFrame({"a": data})
    q = QueryBuilder()
    string_to_find = data[3]
    q = q[q["a"] == string_to_find]
    pandas_query = f"a == '{string_to_find}'"
    generic_filter_test_strings(
        lmdb_version_store, "test_filter_stringpool_shrinking_block_alignment", df, q, pandas_query
    )


@use_of_function_scoped_fixtures_in_hypothesis_checked
@settings(deadline=None)
@given(
    df=data_frames(
        [column("a", elements=numeric_type_strategies()), column("b", elements=numeric_type_strategies())],
        index=range_indexes(),
    )
)
def test_filter_and(lmdb_version_store, df):
    assume(not df.empty)
    q = QueryBuilder()
    q = q[(q["a"] < 5) & (q["b"] > 10)]
    pandas_query = "(a < 5) & (b > 10)"
    generic_filter_test(lmdb_version_store, "test_filter_and", df, q, pandas_query)


@use_of_function_scoped_fixtures_in_hypothesis_checked
@settings(deadline=None)
@given(
    df=data_frames(
        [column("a", elements=numeric_type_strategies()), column("b", elements=numeric_type_strategies())],
        index=range_indexes(),
    )
)
def test_filter_or(lmdb_version_store, df):
    assume(not df.empty)
    q = QueryBuilder()
    q = q[(q["a"] < 5) | (q["b"] > 10)]
    pandas_query = "(a < 5) | (b > 10)"
    generic_filter_test(lmdb_version_store, "test_filter_or", df, q, pandas_query)


@use_of_function_scoped_fixtures_in_hypothesis_checked
@settings(deadline=None)
@given(
    df=data_frames(
        [column("a", elements=numeric_type_strategies()), column("b", elements=numeric_type_strategies())],
        index=range_indexes(),
    )
)
def test_filter_xor(lmdb_version_store, df):
    assume(not df.empty)
    q = QueryBuilder()
    q = q[(q["a"] < 5) ^ (q["b"] > 10)]
    # Pandas doesn't support '^' for xor
    pandas_query = "((a < 5) & ~(b > 10)) | (~(a < 5) & (b > 10))"
    generic_filter_test(lmdb_version_store, "test_filter_xor", df, q, pandas_query)


@use_of_function_scoped_fixtures_in_hypothesis_checked
@settings(deadline=None)
@given(
    df=data_frames([column("a", elements=numeric_type_strategies())], index=range_indexes()),
    val=numeric_type_strategies(),
)
def test_filter_add_col_val(lmdb_version_store, df, val):
    assume(not df.empty)
    q = QueryBuilder()
    q = q[(q["a"] + val) < 10]
    pandas_query = "(a + {}) < 10".format(val)
    generic_filter_test(lmdb_version_store, "test_filter_add_col_val", df, q, pandas_query)


@use_of_function_scoped_fixtures_in_hypothesis_checked
@settings(deadline=None)
@given(
    df=data_frames([column("a", elements=numeric_type_strategies())], index=range_indexes()),
    val=numeric_type_strategies(),
)
def test_filter_add_val_col(lmdb_version_store, df, val):
    assume(not df.empty)
    q = QueryBuilder()
    q = q[(val + q["a"]) < 10]
    pandas_query = "({} + a) < 10".format(val)
    generic_filter_test(lmdb_version_store, "test_filter_add_val_col", df, q, pandas_query)


@use_of_function_scoped_fixtures_in_hypothesis_checked
@settings(deadline=None)
@given(
    df=data_frames(
        [column("a", elements=numeric_type_strategies()), column("b", elements=numeric_type_strategies())],
        index=range_indexes(),
    )
)
def test_filter_add_col_col(lmdb_version_store, df):
    assume(not df.empty)
    q = QueryBuilder()
    q = q[(q["a"] + q["b"]) < 10]
    pandas_query = "(a + b) < 10"
    generic_filter_test(lmdb_version_store, "test_filter_add_col_col", df, q, pandas_query)


@use_of_function_scoped_fixtures_in_hypothesis_checked
@settings(deadline=None)
@given(
    df=data_frames([column("a", elements=numeric_type_strategies())], index=range_indexes()),
    val=numeric_type_strategies(),
)
def test_filter_sub_col_val(lmdb_version_store, df, val):
    assume(not df.empty)
    q = QueryBuilder()
    q = q[(q["a"] - val) < 10]
    pandas_query = "(a - {}) < 10".format(val)
    generic_filter_test(lmdb_version_store, "test_filter_sub_col_val", df, q, pandas_query)


@use_of_function_scoped_fixtures_in_hypothesis_checked
@settings(deadline=None)
@given(
    df=data_frames([column("a", elements=numeric_type_strategies())], index=range_indexes()),
    val=numeric_type_strategies(),
)
def test_filter_sub_val_col(lmdb_version_store, df, val):
    assume(not df.empty)
    q = QueryBuilder()
    q = q[(val - q["a"]) < 10]
    pandas_query = "({} - a) < 10".format(val)
    generic_filter_test(lmdb_version_store, "test_filter_sub_val_col", df, q, pandas_query)


@use_of_function_scoped_fixtures_in_hypothesis_checked
@settings(deadline=None)
@given(
    df=data_frames(
        [column("a", elements=numeric_type_strategies()), column("b", elements=numeric_type_strategies())],
        index=range_indexes(),
    )
)
def test_filter_sub_col_col(lmdb_version_store, df):
    assume(not df.empty)
    q = QueryBuilder()
    q = q[(q["a"] - q["b"]) < 10]
    pandas_query = "(a - b) < 10"
    generic_filter_test(lmdb_version_store, "test_filter_sub_col_col", df, q, pandas_query)


@use_of_function_scoped_fixtures_in_hypothesis_checked
@settings(deadline=None)
@given(
    df=data_frames([column("a", elements=numeric_type_strategies())], index=range_indexes()),
    val=numeric_type_strategies(),
)
def test_filter_times_col_val(lmdb_version_store, df, val):
    assume(not df.empty)
    q = QueryBuilder()
    q = q[(q["a"] * val) < 10]
    pandas_query = "(a * {}) < 10".format(val)
    generic_filter_test(lmdb_version_store, "test_filter_times_col_val", df, q, pandas_query)


@use_of_function_scoped_fixtures_in_hypothesis_checked
@settings(deadline=None)
@given(
    df=data_frames([column("a", elements=numeric_type_strategies())], index=range_indexes()),
    val=numeric_type_strategies(),
)
def test_filter_times_val_col(lmdb_version_store, df, val):
    assume(not df.empty)
    q = QueryBuilder()
    q = q[(val * q["a"]) < 10]
    pandas_query = "({} * a) < 10".format(val)
    generic_filter_test(lmdb_version_store, "test_filter_times_val_col", df, q, pandas_query)


@use_of_function_scoped_fixtures_in_hypothesis_checked
@settings(deadline=None)
@given(
    df=data_frames(
        [column("a", elements=numeric_type_strategies()), column("b", elements=numeric_type_strategies())],
        index=range_indexes(),
    )
)
def test_filter_times_col_col(lmdb_version_store, df):
    assume(not df.empty)
    q = QueryBuilder()
    q = q[(q["a"] * q["b"]) < 10]
    pandas_query = "(a * b) < 10"
    generic_filter_test(lmdb_version_store, "test_filter_times_col_col", df, q, pandas_query)


@use_of_function_scoped_fixtures_in_hypothesis_checked
@settings(deadline=None)
@given(
    df=data_frames([column("a", elements=numeric_type_strategies())], index=range_indexes()),
    val=non_zero_numeric_type_strategies(),
)
def test_filter_divide_col_val(lmdb_version_store, df, val):
    assume(not df.empty)
    q = QueryBuilder()
    q = q[(q["a"] / val) < 10]
    pandas_query = "(a / {}) < 10".format(val)
    generic_filter_test(lmdb_version_store, "test_filter_divide_col_val", df, q, pandas_query)


@use_of_function_scoped_fixtures_in_hypothesis_checked
@settings(deadline=None)
@given(
    df=data_frames([column("a", elements=non_zero_numeric_type_strategies())], index=range_indexes()),
    val=numeric_type_strategies(),
)
def test_filter_divide_val_col(lmdb_version_store, df, val):
    assume(not df.empty)
    q = QueryBuilder()
    q = q[(val / q["a"]) < 10]
    pandas_query = "({} / a) < 10".format(val)
    generic_filter_test(lmdb_version_store, "test_filter_divide_val_col", df, q, pandas_query)


@use_of_function_scoped_fixtures_in_hypothesis_checked
@settings(deadline=None)
@given(
    df=data_frames(
        [column("a", elements=numeric_type_strategies()), column("b", elements=non_zero_numeric_type_strategies())],
        index=range_indexes(),
    )
)
def test_filter_divide_col_col(lmdb_version_store, df):
    assume(not df.empty)
    q = QueryBuilder()
    q = q[(q["a"] / q["b"]) < 10]
    pandas_query = "(a / b) < 10"
    generic_filter_test(lmdb_version_store, "test_filter_divide_col_col", df, q, pandas_query)


@use_of_function_scoped_fixtures_in_hypothesis_checked
@settings(deadline=None)
@given(df=data_frames([column("a", elements=string_strategy)], index=range_indexes()), val=numeric_type_strategies())
def test_filter_arithmetic_string_number_col_val(lmdb_version_store, df, val):
    assume(not df.empty)
    q = QueryBuilder()
    q = q[(q["a"] + val) < 10]
    symbol = "test_filter_arithmetic_string_number_col_val"
    lmdb_version_store.write(symbol, df, dynamic_strings=True)
    with pytest.raises(InternalException) as e_info:
        _ = lmdb_version_store.read(symbol, query_builder=q)
    q = QueryBuilder()
    q = q[(val + q["a"]) < 10]
    with pytest.raises(InternalException) as e_info:
        _ = lmdb_version_store.read(symbol, query_builder=q)


@use_of_function_scoped_fixtures_in_hypothesis_checked
@settings(deadline=None)
@given(df=data_frames([column("a", elements=numeric_type_strategies())], index=range_indexes()), val=string_strategy)
def test_filter_arithmetic_string_number_val_col(lmdb_version_store, df, val):
    assume(not df.empty)
    q = QueryBuilder()
    q = q[(q["a"] + val) < 10]
    symbol = "test_filter_arithmetic_string_number_val_col"
    lmdb_version_store.write(symbol, df)
    with pytest.raises(InternalException) as e_info:
        _ = lmdb_version_store.read(symbol, query_builder=q)
    q = QueryBuilder()
    q = q[(val + q["a"]) < 10]
    with pytest.raises(InternalException) as e_info:
        _ = lmdb_version_store.read(symbol, query_builder=q)


@use_of_function_scoped_fixtures_in_hypothesis_checked
@settings(deadline=None)
@given(
    df=data_frames(
        [column("a", elements=string_strategy), column("b", elements=numeric_type_strategies())], index=range_indexes()
    )
)
def test_filter_arithmetic_string_number_col_col(lmdb_version_store, df):
    assume(not df.empty)
    q = QueryBuilder()
    q = q[(q["a"] + q["b"]) < 10]
    symbol = "test_filter_arithmetic_string_number_val_col"
    lmdb_version_store.write(symbol, df, dynamic_strings=True)
    with pytest.raises(InternalException) as e_info:
        _ = lmdb_version_store.read(symbol, query_builder=q)
    q = QueryBuilder()
    q = q[(q["b"] + q["a"]) < 10]
    with pytest.raises(InternalException) as e_info:
        _ = lmdb_version_store.read(symbol, query_builder=q)


@use_of_function_scoped_fixtures_in_hypothesis_checked
@settings(deadline=None)
@given(df=data_frames([column("a", elements=string_strategy)], index=range_indexes()), val=string_strategy)
def test_filter_arithmetic_string_string_col_val(lmdb_version_store, df, val):
    assume(not df.empty)
    q = QueryBuilder()
    q = q[(q["a"] + val) < 10]
    symbol = "test_filter_arithmetic_string_string_col_val"
    lmdb_version_store.write(symbol, df, dynamic_strings=True)
    with pytest.raises(InternalException) as e_info:
        _ = lmdb_version_store.read(symbol, query_builder=q)
    q = QueryBuilder()
    q = q[(val + q["a"]) < 10]
    with pytest.raises(InternalException) as e_info:
        _ = lmdb_version_store.read(symbol, query_builder=q)


@use_of_function_scoped_fixtures_in_hypothesis_checked
@settings(deadline=None)
@given(df=data_frames([column("a", elements=string_strategy)], index=range_indexes()), val=string_strategy)
def test_filter_arithmetic_string_string_val_col(lmdb_version_store, df, val):
    assume(not df.empty)
    q = QueryBuilder()
    q = q[(q["a"] + val) < 10]
    symbol = "test_filter_arithmetic_string_string_val_col"
    lmdb_version_store.write(symbol, df, dynamic_strings=True)
    with pytest.raises(InternalException) as e_info:
        _ = lmdb_version_store.read(symbol, query_builder=q)
    q = QueryBuilder()
    q = q[(val + q["a"]) < 10]
    with pytest.raises(InternalException) as e_info:
        _ = lmdb_version_store.read(symbol, query_builder=q)


@use_of_function_scoped_fixtures_in_hypothesis_checked
@settings(deadline=None)
@given(
    df=data_frames(
        [column("a", elements=string_strategy), column("b", elements=string_strategy)], index=range_indexes()
    )
)
def test_filter_arithmetic_string_string_col_col(lmdb_version_store, df):
    assume(not df.empty)
    q = QueryBuilder()
    q = q[(q["a"] + q["b"]) < 10]
    symbol = "test_filter_arithmetic_string_string_col_col"
    lmdb_version_store.write(symbol, df, dynamic_strings=True)
    with pytest.raises(InternalException) as e_info:
        _ = lmdb_version_store.read(symbol, query_builder=q)
    q = QueryBuilder()
    q = q[(q["b"] + q["a"]) < 10]
    with pytest.raises(InternalException) as e_info:
        _ = lmdb_version_store.read(symbol, query_builder=q)


@use_of_function_scoped_fixtures_in_hypothesis_checked
@settings(deadline=None)
@given(
    df=data_frames([column("a", elements=numeric_type_strategies())], index=range_indexes()),
    val=numeric_type_strategies(),
)
def test_filter_abs(lmdb_version_store, df, val):
    assume(not df.empty)
    q = QueryBuilder()
    q = q[abs(q["a"]) < val]
    pandas_query = "abs(a) < {}".format(val)
    generic_filter_test(lmdb_version_store, "test_filter_abs", df, q, pandas_query)


@use_of_function_scoped_fixtures_in_hypothesis_checked
@settings(deadline=None)
@given(df=data_frames([column("a", elements=string_strategy)], index=range_indexes()), val=string_strategy)
def test_filter_abs_string(lmdb_version_store, df, val):
    assume(not df.empty)
    q = QueryBuilder()
    q = q[abs(q["a"]) < val]
    symbol = "test_filter_abs_string"
    lmdb_version_store.write(symbol, df, dynamic_strings=True)
    with pytest.raises(InternalException) as e_info:
        _ = lmdb_version_store.read(symbol, query_builder=q)


@use_of_function_scoped_fixtures_in_hypothesis_checked
@settings(deadline=None)
@given(
    df=data_frames([column("a", elements=numeric_type_strategies())], index=range_indexes()),
    val=numeric_type_strategies(),
)
def test_filter_neg(lmdb_version_store, df, val):
    assume(not df.empty)
    q = QueryBuilder()
    q = q[-q["a"] < val]
    pandas_query = "-a < {}".format(val)
    generic_filter_test(lmdb_version_store, "test_filter_neg", df, q, pandas_query)


@use_of_function_scoped_fixtures_in_hypothesis_checked
@settings(deadline=None)
@given(df=data_frames([column("a", elements=string_strategy)], index=range_indexes()), val=string_strategy)
def test_filter_neg_string(lmdb_version_store, df, val):
    assume(not df.empty)
    q = QueryBuilder()
    q = q[-q["a"] < val]
    symbol = "test_filter_neg_string"
    lmdb_version_store.write(symbol, df, dynamic_strings=True)
    with pytest.raises(InternalException) as e_info:
        _ = lmdb_version_store.read(symbol, query_builder=q)


def test_filter_explicit_type_promotion(lmdb_version_store):
    df = DataFrame(
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
    lmdb_version_store.write(symbol, df)
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
    assert np.array_equal(lmdb_version_store.read(symbol, query_builder=q).data, df.loc[[1]])
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
    assert np.array_equal(lmdb_version_store.read(symbol, query_builder=q).data, df.loc[[0]])
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
    assert np.array_equal(lmdb_version_store.read(symbol, query_builder=q).data, df.loc[[1]])
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
    assert np.array_equal(lmdb_version_store.read(symbol, query_builder=q).data, df.loc[[1]])
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
    assert np.array_equal(lmdb_version_store.read(symbol, query_builder=q).data, df.loc[[0]])
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
    assert np.array_equal(lmdb_version_store.read(symbol, query_builder=q).data, df.loc[[1]])


@use_of_function_scoped_fixtures_in_hypothesis_checked
@settings(deadline=None)
@given(
    df=data_frames(
        [
            column("a", elements=numeric_type_strategies()),
            column("b", elements=numeric_type_strategies()),
            column("c", elements=numeric_type_strategies()),
        ],
        index=range_indexes(),
    )
)
def test_filter_more_columns_than_fit_in_one_segment(lmdb_version_store_tiny_segment, df):
    assume(not df.empty)
    q = QueryBuilder()
    q = q[(q["a"] < q["c"]) | (q["a"] < q["b"]) | (q["b"] < q["c"])]
    pandas_query = "(a < c) | (a < b) | (b < c)"
    generic_filter_test(
        lmdb_version_store_tiny_segment, "test_filter_more_columns_than_fit_in_one_segment", df, q, pandas_query
    )


@use_of_function_scoped_fixtures_in_hypothesis_checked
@settings(deadline=None)
@given(
    df=data_frames(
        [
            column("a", elements=numeric_type_strategies()),
            column("b", elements=numeric_type_strategies()),
            column("c", elements=numeric_type_strategies()),
        ],
        index=range_indexes(),
    )
)
def test_filter_with_column_slicing(lmdb_version_store_tiny_segment, df):
    assume(not df.empty)
    q = QueryBuilder()
    q = q[(q["a"] < q["c"]) | (q["a"] < q["b"]) | (q["b"] < q["c"])]
    pandas_query = "(a < c) | (a < b) | (b < c)"
    symbol = "test_filter_with_column_filtering"
    lmdb_version_store_tiny_segment.write(symbol, df)
    expected = df.query(pandas_query).loc[:, ["a", "c"]]
    received = lmdb_version_store_tiny_segment.read(symbol, columns=["a", "c"], query_builder=q).data
    assert np.array_equal(expected, received)


def test_filter_column_slicing_different_segments(lmdb_version_store_tiny_segment):
    df = DataFrame({"a": np.arange(0, 10), "b": np.arange(10, 20), "c": np.arange(20, 30)}, index=np.arange(10))
    symbol = "test_filter_column_slicing_different_segments"
    lmdb_version_store_tiny_segment.write(symbol, df)
    # Filter on column c (in second column slice), but only display column a (in first column slice)
    q = QueryBuilder()
    q = q[q["c"] == 22]
    pandas_query = "c == 22"
    expected = df.query(pandas_query).loc[:, ["a"]]
    received = lmdb_version_store_tiny_segment.read(symbol, columns=["a"], query_builder=q).data
    assert np.array_equal(expected, received)
    # Filter on column c (in second column slice), and display all columns
    q = QueryBuilder()
    q = q[q["c"] == 22]
    pandas_query = "c == 22"
    expected = df.query(pandas_query)
    received = lmdb_version_store_tiny_segment.read(symbol, query_builder=q).data
    assert np.array_equal(expected, received)
    # Filter on column c (in second column slice), and only display column c
    q = QueryBuilder()
    q = q[q["c"] == 22]
    pandas_query = "c == 22"
    expected = df.query(pandas_query).loc[:, ["c"]]
    received = lmdb_version_store_tiny_segment.read(symbol, columns=["c"], query_builder=q).data
    assert np.array_equal(expected, received)


def test_filter_with_multi_index(lmdb_version_store):
    dt1 = datetime(2019, 4, 8, 10, 5, 2, 1)
    dt2 = datetime(2019, 4, 9, 10, 5, 2, 1)
    arr1 = [dt1, dt1, dt2, dt2]
    arr2 = [0, 1, 0, 1]
    df = DataFrame(
        data={"a": np.arange(10, 14)}, index=pd.MultiIndex.from_arrays([arr1, arr2], names=["datetime", "level"])
    )
    q = QueryBuilder()
    q = q[(q["a"] == 11) | (q["a"] == 13)]
    pandas_query = "(a == 11) | (a == 13)"
    generic_filter_test(lmdb_version_store, "test_filter_with_multi_index", df, q, pandas_query)


def test_filter_on_multi_index(lmdb_version_store):
    dt1 = datetime(2019, 4, 8, 10, 5, 2, 1)
    dt2 = datetime(2019, 4, 9, 10, 5, 2, 1)
    arr1 = [dt1, dt1, dt2, dt2]
    arr2 = [0, 1, 0, 1]
    df = DataFrame(
        data={"a": np.arange(10, 14)}, index=pd.MultiIndex.from_arrays([arr1, arr2], names=["datetime", "level"])
    )
    q = QueryBuilder()
    q = q[(q["level"] == 1)]
    pandas_query = "level == 1"
    generic_filter_test(lmdb_version_store, "test_filter_on_multi_index", df, q, pandas_query)


@use_of_function_scoped_fixtures_in_hypothesis_checked
@settings(deadline=None)
@given(
    df=data_frames([column("a", elements=numeric_type_strategies())], index=range_indexes()),
    val=numeric_type_strategies(),
)
def test_filter_not(lmdb_version_store, df, val):
    assume(not df.empty)
    q = QueryBuilder()
    q = q[~(q["a"] < val)]
    pandas_query = "~(a < {})".format(val)
    generic_filter_test(lmdb_version_store, "test_filter_not", df, q, pandas_query)


def test_filter_complex_expression(lmdb_version_store_tiny_segment):
    df = DataFrame(
        {
            "a": np.arange(0, 10, dtype=np.float64),
            "b": np.arange(10, 20, dtype=np.uint16),
            "c": np.arange(20, 30, dtype=np.int32),
        },
        index=np.arange(10),
    )
    q = QueryBuilder()
    q = q[(((q["a"] * q["b"]) / 5) < (0.7 * q["c"])) & (q["b"] != 12)]
    pandas_query = "(((a * b) / 5) < (0.7 * c)) & (b != 12)"
    generic_filter_test(lmdb_version_store_tiny_segment, "test_filter_complex_expression", df, q, pandas_query)


def test_filter_string_backslash(lmdb_version_store):
    df = DataFrame({"a": ["", "\\"]}, index=np.arange(2))
    q = QueryBuilder()
    q = q[q["a"] == "\\"]
    symbol = "test_filter_string_backslash"
    lmdb_version_store.write(symbol, df, dynamic_strings=True)
    expected = DataFrame({"a": ["\\"]}, index=np.arange(1, 2))
    received = lmdb_version_store.read(symbol, query_builder=q).data
    assert np.array_equal(expected, received)


def test_filter_string_single_quote(lmdb_version_store):
    df = DataFrame({"a": ["", "'"]}, index=np.arange(2))
    q = QueryBuilder()
    q = q[q["a"] == "'"]
    symbol = "test_filter_string_single_quote"
    lmdb_version_store.write(symbol, df, dynamic_strings=True)
    expected = DataFrame({"a": ["'"]}, index=np.arange(1, 2))
    received = lmdb_version_store.read(symbol, query_builder=q).data
    assert np.array_equal(expected, received)


@use_of_function_scoped_fixtures_in_hypothesis_checked
@settings(deadline=None)
@given(df=data_frames([column("a", elements=string_strategy)], index=range_indexes()), val=string_strategy)
@pytest.mark.skipif(MACOS_CONDA_BUILD, reason="This test might segfault on MacOS conda-forge builds.")
def test_filter_string_equals_col_val(lmdb_version_store, df, val):
    assume(not df.empty)
    q = QueryBuilder()
    q = q[q["a"] == val]
    pandas_query = "a == '{}'".format(val)
    generic_filter_test_strings(lmdb_version_store, "test_filter_string_equals_col_val", df, q, pandas_query)


@use_of_function_scoped_fixtures_in_hypothesis_checked
@settings(deadline=None)
@given(df=data_frames([column("a", elements=string_strategy)], index=range_indexes()), val=string_strategy)
@pytest.mark.skipif(MACOS_CONDA_BUILD, reason="This test might segfault on MacOS conda-forge builds.")
def test_filter_string_equals_val_col(lmdb_version_store, df, val):
    assume(not df.empty)
    q = QueryBuilder()
    q = q[val == q["a"]]
    pandas_query = "'{}' == a".format(val)
    generic_filter_test_strings(lmdb_version_store, "test_filter_string_equals_val_col", df, q, pandas_query)


@use_of_function_scoped_fixtures_in_hypothesis_checked
@settings(deadline=None)
@given(
    df=data_frames(
        [column("a", elements=string_strategy), column("b", elements=string_strategy)], index=range_indexes()
    )
)
@pytest.mark.skipif(MACOS_CONDA_BUILD, reason="This test might segfault on MacOS conda-forge builds.")
def test_filter_string_equals_col_col(lmdb_version_store, df):
    assume(not df.empty)
    q = QueryBuilder()
    q = q[q["a"] == q["b"]]
    pandas_query = "a == b"
    generic_filter_test_strings(lmdb_version_store, "test_filter_string_equals_col_col", df, q, pandas_query)


@use_of_function_scoped_fixtures_in_hypothesis_checked
@settings(deadline=None)
@given(df=data_frames([column("a", elements=string_strategy)], index=range_indexes()), val=string_strategy)
@pytest.mark.skipif(MACOS_CONDA_BUILD, reason="This test might segfault on MacOS conda-forge builds.")
def test_filter_string_not_equals_col_val(lmdb_version_store, df, val):
    assume(not df.empty)
    q = QueryBuilder()
    q = q[q["a"] != val]
    pandas_query = "a != '{}'".format(val)
    generic_filter_test_strings(lmdb_version_store, "test_filter_string_not_equals_col_val", df, q, pandas_query)


@use_of_function_scoped_fixtures_in_hypothesis_checked
@settings(deadline=None)
@given(df=data_frames([column("a", elements=string_strategy)], index=range_indexes()), val=string_strategy)
@pytest.mark.skipif(MACOS_CONDA_BUILD, reason="This test might segfault on MacOS conda-forge builds.")
def test_filter_string_not_equals_val_col(lmdb_version_store, df, val):
    assume(not df.empty)
    q = QueryBuilder()
    q = q[val != q["a"]]
    pandas_query = "'{}' != a".format(val)
    generic_filter_test_strings(lmdb_version_store, "test_filter_string_not_equals_val_col", df, q, pandas_query)


@use_of_function_scoped_fixtures_in_hypothesis_checked
@settings(deadline=None)
@given(
    df=data_frames(
        [column("a", elements=string_strategy), column("b", elements=string_strategy)], index=range_indexes()
    )
)
@pytest.mark.skipif(MACOS_CONDA_BUILD, reason="This test might segfault on MacOS conda-forge builds.")
def test_filter_string_not_equals_col_col(lmdb_version_store, df):
    assume(not df.empty)
    q = QueryBuilder()
    q = q[q["a"] != q["b"]]
    pandas_query = "a != b"
    generic_filter_test_strings(lmdb_version_store, "test_filter_string_not_equals_col_col", df, q, pandas_query)


def test_filter_string_less_than(lmdb_version_store):
    df = DataFrame({"a": ["row1", "row2"]}, index=np.arange(2))
    q = QueryBuilder()
    q = q[q["a"] < "row2"]
    pandas_query = "a < 'row2'"
    with pytest.raises(InternalException) as e_info:
        generic_filter_test(lmdb_version_store, "test_filter_string_less_than", df, q, pandas_query)


def test_filter_string_less_than_equal(lmdb_version_store):
    df = DataFrame({"a": ["row1", "row2"]}, index=np.arange(2))
    q = QueryBuilder()
    q = q[q["a"] <= "row2"]
    pandas_query = "a <= 'row2'"
    with pytest.raises(InternalException) as e_info:
        generic_filter_test(lmdb_version_store, "test_filter_string_less_than_equal", df, q, pandas_query)


def test_filter_string_greater_than(lmdb_version_store):
    df = DataFrame({"a": ["row1", "row2"]}, index=np.arange(2))
    q = QueryBuilder()
    q = q[q["a"] > "row2"]
    pandas_query = "a > 'row2'"
    with pytest.raises(InternalException) as e_info:
        generic_filter_test(lmdb_version_store, "test_filter_string_greater_than", df, q, pandas_query)


def test_filter_string_greater_than_equal(lmdb_version_store):
    df = DataFrame({"a": ["row1", "row2"]}, index=np.arange(2))
    q = QueryBuilder()
    q = q[q["a"] >= "row2"]
    pandas_query = "a >= 'row2'"
    with pytest.raises(InternalException) as e_info:
        generic_filter_test(lmdb_version_store, "test_filter_string_greater_than_equal", df, q, pandas_query)


# TODO: Replace with np.array_equal with equal_nan argument (added in 1.19.0)
def generic_filter_test_nans(version_store, symbol, df, arctic_query, pandas_query):
    version_store.write(symbol, df, dynamic_strings=True)
    expected = df.query(pandas_query)
    received = version_store.read(symbol, query_builder=arctic_query).data
    assert expected.shape == received.shape
    for col in expected.columns:
        expected_col = expected.loc[:, col]
        received_col = received.loc[:, col]
        for idx, expected_val in expected_col.items():
            received_val = received_col[idx]
            if isinstance(expected_val, str):
                assert isinstance(received_val, str) and expected_val == received_val
            elif expected_val is None:
                assert received_val is None
            elif np.isnan(expected_val):
                assert np.isnan(received_val)


def test_filter_string_nans_col_val(lmdb_version_store):
    symbol = "test_filter_string_nans_col_val"
    df = pd.DataFrame({"a": ["row1", "row2", None, np.nan, math.nan]}, index=np.arange(5))

    q = QueryBuilder()
    q = q[q["a"] == "row2"]
    pandas_query = "a == 'row2'"
    generic_filter_test_nans(lmdb_version_store, symbol, df, q, pandas_query)

    q = QueryBuilder()
    q = q[q["a"] != "row2"]
    pandas_query = "a != 'row2'"
    generic_filter_test_nans(lmdb_version_store, symbol, df, q, pandas_query)

    q = QueryBuilder()
    q = q[q["a"] == ["row2"]]
    pandas_query = f"a in {['row2']}"
    generic_filter_test_nans(lmdb_version_store, symbol, df, q, pandas_query)

    q = QueryBuilder()
    q = q[q["a"] != ["row2"]]
    pandas_query = f"a not in {['row2']}"
    generic_filter_test_nans(lmdb_version_store, symbol, df, q, pandas_query)

    q = QueryBuilder()
    q = q[q["a"] == []]
    pandas_query = f"a in {[]}"
    generic_filter_test_nans(lmdb_version_store, symbol, df, q, pandas_query)

    q = QueryBuilder()
    q = q[q["a"] != []]
    pandas_query = f"a not in {[]}"
    generic_filter_test_nans(lmdb_version_store, symbol, df, q, pandas_query)


def test_filter_string_nans_col_col(lmdb_version_store):
    symbol = "test_filter_string_nans_col_col"
    # Compare all combinations of string, None, np.nan, and math.nan to one another
    df = pd.DataFrame(
        {
            "a": ["row1", "row2", "row3", "row4", None, None, None, np.nan, np.nan, math.nan],
            "b": ["row1", None, np.nan, math.nan, None, np.nan, math.nan, np.nan, math.nan, math.nan],
        },
        index=np.arange(10),
    )

    q = QueryBuilder()
    q = q[q["a"] == q["b"]]
    pandas_query = "a == b"
    generic_filter_test_nans(lmdb_version_store, symbol, df, q, pandas_query)

    q = QueryBuilder()
    q = q[q["a"] != q["b"]]
    pandas_query = "a != b"
    generic_filter_test_nans(lmdb_version_store, symbol, df, q, pandas_query)


def test_filter_batch_one_query(lmdb_version_store):
    sym1 = "sym1"
    sym2 = "sym2"
    df1 = DataFrame({"a": [1, 2]}, index=np.arange(2))
    df2 = DataFrame({"a": [2, 3]}, index=np.arange(2))
    lmdb_version_store.write(sym1, df1)
    lmdb_version_store.write(sym2, df2)

    q = QueryBuilder()
    q = q[q["a"] == 2]
    pandas_query = "a == 2"
    batch_res = lmdb_version_store.batch_read([sym1, sym2], query_builder=q)
    res1 = batch_res[sym1].data
    res2 = batch_res[sym2].data
    assert np.array_equal(df1.query(pandas_query), res1)
    assert np.array_equal(df2.query(pandas_query), res2)


def test_filter_batch_multiple_queries(lmdb_version_store):
    sym1 = "sym1"
    sym2 = "sym2"
    df1 = DataFrame({"a": [1, 2]}, index=np.arange(2))
    df2 = DataFrame({"a": [2, 3]}, index=np.arange(2))
    lmdb_version_store.write(sym1, df1)
    lmdb_version_store.write(sym2, df2)

    q1 = QueryBuilder()
    q1 = q1[q1["a"] == 1]
    pandas_query1 = "a == 1"
    q2 = QueryBuilder()
    q2 = q2[q2["a"] == 3]
    pandas_query2 = "a == 3"
    batch_res = lmdb_version_store.batch_read([sym1, sym2], query_builder=[q1, q2])
    res1 = batch_res[sym1].data
    res2 = batch_res[sym2].data
    assert np.array_equal(df1.query(pandas_query1), res1)
    assert np.array_equal(df2.query(pandas_query2), res2)


def test_filter_batch_multiple_queries_with_none(lmdb_version_store):
    sym1 = "sym1"
    sym2 = "sym2"
    df1 = DataFrame({"a": [1, 2]}, index=np.arange(2))
    df2 = DataFrame({"a": [2, 3]}, index=np.arange(2))
    lmdb_version_store.write(sym1, df1)
    lmdb_version_store.write(sym2, df2)

    q2 = QueryBuilder()
    q2 = q2[q2["a"] == 3]
    pandas_query2 = "a == 3"
    batch_res = lmdb_version_store.batch_read([sym1, sym2], query_builder=[None, q2])
    res1 = batch_res[sym1].data
    res2 = batch_res[sym2].data
    assert np.array_equal(df1, res1)
    assert np.array_equal(df2.query(pandas_query2), res2)


def test_filter_batch_incorrect_query_count(lmdb_version_store):
    sym1 = "sym1"
    sym2 = "sym2"
    df1 = DataFrame({"a": [1, 2]}, index=np.arange(2))
    df2 = DataFrame({"a": [2, 3]}, index=np.arange(2))
    lmdb_version_store.write(sym1, df1)
    lmdb_version_store.write(sym2, df2)

    q = QueryBuilder()
    q = q[q["a"] == 3]
    with pytest.raises(ArcticNativeException):
        batch_res = lmdb_version_store.batch_read([sym1, sym2], query_builder=[q])
    with pytest.raises(ArcticNativeException):
        batch_res = lmdb_version_store.batch_read([sym1, sym2], query_builder=[q, q, q])


def test_filter_batch_symbol_doesnt_exist(lmdb_version_store):
    sym1 = "sym1"
    sym2 = "sym2"
    df1 = DataFrame({"a": [1, 2]}, index=np.arange(2))
    lmdb_version_store.write(sym1, df1)
    q = QueryBuilder()
    q = q[q["a"] == 2]
    with pytest.raises(NoDataFoundException):
        batch_res = lmdb_version_store.batch_read([sym1, sym2], query_builder=q)


def test_filter_batch_version_doesnt_exist(lmdb_version_store):
    sym1 = "sym1"
    sym2 = "sym2"
    df1 = DataFrame({"a": [1, 2]}, index=np.arange(2))
    df2 = DataFrame({"a": [2, 3]}, index=np.arange(2))
    lmdb_version_store.write(sym1, df1)
    lmdb_version_store.write(sym2, df2)

    q = QueryBuilder()
    q = q[q["a"] == 2]
    # pandas_query = "a == 2"
    with pytest.raises(NoDataFoundException):
        batch_res = lmdb_version_store.batch_read([sym1, sym2], as_ofs=[0, 1], query_builder=q)


def test_filter_batch_missing_keys(lmdb_version_store):
    lib = lmdb_version_store

    df1 = pd.DataFrame({"a": [3, 5, 7]})
    df2 = pd.DataFrame({"a": [4, 6, 8]})
    df3 = pd.DataFrame({"a": [5, 7, 9]})
    lib.write("s1", df1)
    lib.write("s2", df2)
    # Need two versions for this symbol as we're going to delete a version key, and the optimisation of storing the
    # latest index key in the version ref key means it will still work if we just write one version key and then delete
    # it
    lib.write("s3", df3)
    lib.write("s3", df3)
    lib_tool = lib.library_tool()
    s1_index_key = lib_tool.find_keys_for_id(KeyType.TABLE_INDEX, "s1")[0]
    s2_data_key = lib_tool.find_keys_for_id(KeyType.TABLE_DATA, "s2")[0]
    s3_version_keys = lib_tool.find_keys_for_id(KeyType.VERSION, "s3")
    s3_key_to_delete = [key for key in s3_version_keys if key.version_id == 0][0]
    lib_tool.remove(s1_index_key)
    lib_tool.remove(s2_data_key)
    lib_tool.remove(s3_key_to_delete)

    q = QueryBuilder()
    q = q[q["a"] == 2]

    # The exception thrown is different for missing version keys to everything else, and so depends on which symbol is
    # processed first
    with pytest.raises((NoDataFoundException, StorageException)):
        _ = lib.batch_read(["s1", "s2", "s3"], [None, None, 0], query_builder=q)


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


def test_filter_with_column_slicing_defragmented(lmdb_version_store_tiny_segment):
    set_config_int("SymbolDataCompact.SegmentCount", 0)

    df = pd.DataFrame(
        index=pd.date_range(pd.Timestamp(0), periods=3),
        data={
            "a": pd.date_range("2000-01-01", periods=3),
            "b": pd.date_range("2000-01-01", periods=3),
            "c": pd.date_range("2000-01-01", periods=3),
        },
    )
    pd_ts = pd.Timestamp("2000-01-05")
    symbol = "test_filter_with_column_filtering"
    for ts in [pd_ts, pd_ts.to_pydatetime()]:
        q = QueryBuilder()
        q = q[q["a"] < ts]
        pandas_query = "a < @ts"
        lmdb_version_store_tiny_segment.write(symbol, df[:1])
        lmdb_version_store_tiny_segment.append(symbol, df[1:2])
        lmdb_version_store_tiny_segment.defragment_symbol_data(symbol, None)
        lmdb_version_store_tiny_segment.append(symbol, df[2:])
        received = lmdb_version_store_tiny_segment.read(symbol, query_builder=q).data
        expected = df.query(pandas_query)
        assert np.array_equal(expected, received) and (not expected.empty and not received.empty)
