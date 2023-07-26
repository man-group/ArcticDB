"""
Copyright 2023 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""
import sys

from hypothesis import assume, given, settings
from hypothesis.extra.pandas import column, data_frames, range_indexes
import hypothesis.strategies as st
import numpy as np
import pandas as pd

from arcticdb.util._versions import IS_PANDAS_TWO

try:
    from pandas.errors import UndefinedVariableError
except ImportError:
    from pandas.core.computation.ops import UndefinedVariableError


from arcticdb.version_store.processing import QueryBuilder
from arcticdb.util.test import get_wide_dataframe, make_dynamic, regularize_dataframe, assert_frame_equal
from arcticdb.util.hypothesis import (
    use_of_function_scoped_fixtures_in_hypothesis_checked,
    integral_type_strategies,
    signed_integral_type_strategies,
    unsigned_integral_type_strategies,
    numeric_type_strategies,
    string_strategy,
)


def generic_dynamic_filter_test(version_store, symbol, df, arctic_query, pandas_query, dynamic_strings=True):
    expected, slices = make_dynamic(df)
    for df_slice in slices:
        version_store.append(symbol, df_slice, write_if_missing=True)

    try:
        expected = expected.query(pandas_query)
        received = version_store.read(symbol, query_builder=arctic_query).data
        expected = regularize_dataframe(expected)
        received = regularize_dataframe(received)
        if not len(expected) == 0 and len(received) == 0:
            if not np.array_equal(expected, received):
                print("Original dataframe\n{}".format(expected))
                print("Pandas query\n{}".format(pandas_query))
                print("Expected\n{}".format(expected))
                print("Received\n{}".format(received))
                assert False
    except UndefinedVariableError:
        # Might have edited out the query columns entirely
        pass

    assert True


@use_of_function_scoped_fixtures_in_hypothesis_checked
@settings(deadline=None)
@given(
    df=data_frames(
        [
            column("a", elements=numeric_type_strategies()),
            column("b", elements=numeric_type_strategies()),
            column("c", elements=numeric_type_strategies()),
            column("d", elements=string_strategy),
        ],
        index=range_indexes(),
    )
)
def test_filter_less_than_col_col(lmdb_version_store_dynamic_schema, df):
    assume(not df.empty)
    q = QueryBuilder()
    q = q[q["a"] < q["b"]]
    pandas_query = "a < b"
    generic_dynamic_filter_test(lmdb_version_store_dynamic_schema, "test_filter_less_than_col_col", df, q, pandas_query)


@use_of_function_scoped_fixtures_in_hypothesis_checked
@settings(deadline=None)
@given(
    df=data_frames(
        [
            column("a", elements=numeric_type_strategies()),
            column("b", elements=numeric_type_strategies()),
            column("c", elements=numeric_type_strategies()),
            column("d", elements=string_strategy),
        ],
        index=range_indexes(),
    ),
    val=numeric_type_strategies(),
)
def test_filter_less_than_equals_col_val(lmdb_version_store_dynamic_schema, df, val):
    assume(not df.empty)
    q = QueryBuilder()
    q = q[q["a"] <= val]
    pandas_query = "a <= {}".format(val)
    generic_dynamic_filter_test(
        lmdb_version_store_dynamic_schema, "test_filter_less_than_equals_col_val", df, q, pandas_query
    )


@use_of_function_scoped_fixtures_in_hypothesis_checked
@settings(deadline=None)
@given(
    df=data_frames(
        [
            column("a", elements=numeric_type_strategies()),
            column("b", elements=numeric_type_strategies()),
            column("c", elements=numeric_type_strategies()),
            column("d", elements=string_strategy),
        ],
        index=range_indexes(),
    ),
    val=numeric_type_strategies(),
)
def test_filter_less_than_equals_val_col(lmdb_version_store_dynamic_schema, df, val):
    assume(not df.empty)
    q = QueryBuilder()
    q = q[val <= q["a"]]
    pandas_query = "{} <= a".format(val)
    generic_dynamic_filter_test(
        lmdb_version_store_dynamic_schema, "test_filter_less_than_equals_val_col", df, q, pandas_query
    )


@use_of_function_scoped_fixtures_in_hypothesis_checked
@settings(deadline=None)
@given(
    df=data_frames(
        [
            column("a", elements=numeric_type_strategies()),
            column("b", elements=numeric_type_strategies()),
            column("c", elements=numeric_type_strategies()),
            column("d", elements=string_strategy),
        ],
        index=range_indexes(),
    )
)
def test_filter_less_than_equals_col_col(lmdb_version_store_dynamic_schema, df):
    assume(not df.empty)
    q = QueryBuilder()
    q = q[q["a"] <= q["b"]]
    pandas_query = "a <= b"
    generic_dynamic_filter_test(
        lmdb_version_store_dynamic_schema, "test_filter_less_than_equals_col_col", df, q, pandas_query
    )


@use_of_function_scoped_fixtures_in_hypothesis_checked
@settings(deadline=None)
@given(
    df=data_frames(
        [
            column("a", elements=numeric_type_strategies()),
            column("b", elements=numeric_type_strategies()),
            column("c", elements=numeric_type_strategies()),
            column("d", elements=string_strategy),
        ],
        index=range_indexes(),
    ),
    val=numeric_type_strategies(),
)
def test_filter_greater_than_col_val(lmdb_version_store_dynamic_schema, df, val):
    assume(not df.empty)
    q = QueryBuilder()
    q = q[q["a"] > val]
    pandas_query = "a > {}".format(val)
    generic_dynamic_filter_test(
        lmdb_version_store_dynamic_schema, "test_filter_greater_than_col_val", df, q, pandas_query
    )


@use_of_function_scoped_fixtures_in_hypothesis_checked
@settings(deadline=None)
@given(
    df=data_frames(
        [column("a", elements=integral_type_strategies()), column("b", elements=integral_type_strategies())],
        index=range_indexes(),
    ),
    vals=st.frozensets(signed_integral_type_strategies(), min_size=1),
)
def test_filter_numeric_isin_signed(lmdb_version_store_dynamic_schema, df, vals):
    assume(not df.empty)
    q = QueryBuilder()
    q = q[q["a"].isin(vals)]
    pandas_query = "a in {}".format(list(vals))
    generic_dynamic_filter_test(lmdb_version_store_dynamic_schema, "test_filter_numeric_isin", df, q, pandas_query)


@use_of_function_scoped_fixtures_in_hypothesis_checked
@settings(deadline=None)
@given(
    df=data_frames(
        [column("a", elements=integral_type_strategies()), column("b", elements=integral_type_strategies())],
        index=range_indexes(),
    ),
    vals=st.frozensets(unsigned_integral_type_strategies(), min_size=1),
)
def test_filter_numeric_isin_unsigned(lmdb_version_store_dynamic_schema, df, vals):
    assume(not df.empty)
    q = QueryBuilder()
    q = q[q["a"].isin(vals)]
    pandas_query = "a in {}".format(list(vals))
    generic_dynamic_filter_test(lmdb_version_store_dynamic_schema, "test_filter_numeric_isin", df, q, pandas_query)


@use_of_function_scoped_fixtures_in_hypothesis_checked
@settings(deadline=None)
@given(
    df=data_frames(
        [column("a", elements=integral_type_strategies()), column("b", elements=integral_type_strategies())],
        index=range_indexes(),
    ),
    vals=st.frozensets(signed_integral_type_strategies(), min_size=1),
)
def test_filter_numeric_isnotin_signed(lmdb_version_store_dynamic_schema, df, vals):
    assume(not df.empty)
    q = QueryBuilder()
    q = q[q["a"].isnotin(vals)]
    pandas_query = "a not in {}".format(list(vals))
    generic_dynamic_filter_test(lmdb_version_store_dynamic_schema, "test_filter_numeric_isnotin", df, q, pandas_query)


@use_of_function_scoped_fixtures_in_hypothesis_checked
@settings(deadline=None)
@given(
    df=data_frames(
        [column("a", elements=integral_type_strategies()), column("b", elements=integral_type_strategies())],
        index=range_indexes(),
    ),
    vals=st.frozensets(unsigned_integral_type_strategies(), min_size=1),
)
def test_filter_numeric_isnotin_unsigned(lmdb_version_store_dynamic_schema, df, vals):
    assume(not df.empty)
    q = QueryBuilder()
    q = q[q["a"].isnotin(vals)]
    pandas_query = "a not in {}".format(list(vals))
    generic_dynamic_filter_test(lmdb_version_store_dynamic_schema, "test_filter_numeric_isnotin", df, q, pandas_query)


@use_of_function_scoped_fixtures_in_hypothesis_checked
@settings(deadline=None)
@given(
    df=data_frames(
        [column("a", elements=string_strategy), column("b", elements=string_strategy)], index=range_indexes()
    ),
    vals=st.frozensets(string_strategy, min_size=1),
)
def test_filter_string_isin(lmdb_version_store_dynamic_schema, df, vals):
    assume(not df.empty)
    q = QueryBuilder()
    q = q[q["a"].isin(vals)]
    pandas_query = "a in {}".format(list(vals))
    generic_dynamic_filter_test(lmdb_version_store_dynamic_schema, "test_filter_string_isin", df, q, pandas_query)


@use_of_function_scoped_fixtures_in_hypothesis_checked
@settings(deadline=None)
@given(
    df=data_frames(
        [column("a", elements=string_strategy), column("b", elements=string_strategy)], index=range_indexes()
    )
)
def test_filter_string_isin_empty_set(lmdb_version_store_dynamic_schema, df):
    assume(not df.empty)
    vals = []
    q = QueryBuilder()
    q = q[q["a"].isin(vals)]
    pandas_query = "a in {}".format(list(vals))
    generic_dynamic_filter_test(
        lmdb_version_store_dynamic_schema, "test_filter_string_isin_empty_set", df, q, pandas_query
    )


@use_of_function_scoped_fixtures_in_hypothesis_checked
@settings(deadline=None)
@given(
    df=data_frames(
        [column("a", elements=string_strategy), column("b", elements=string_strategy)], index=range_indexes()
    ),
    vals=st.frozensets(string_strategy, min_size=1),
)
def test_filter_string_isnotin(lmdb_version_store_dynamic_schema, df, vals):
    assume(not df.empty)
    q = QueryBuilder()
    q = q[q["a"].isnotin(vals)]
    pandas_query = "a not in {}".format(list(vals))
    generic_dynamic_filter_test(lmdb_version_store_dynamic_schema, "test_filter_string_isnotin", df, q, pandas_query)


def test_numeric_filter_dynamic_schema(lmdb_version_store_tiny_segment_dynamic):
    symbol = "test_numeric_filter_dynamic_schema"
    lib = lmdb_version_store_tiny_segment_dynamic
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


def test_filter_column_not_present_dynamic(lmdb_version_store_dynamic_schema):
    df = pd.DataFrame({"a": np.arange(2)}, index=np.arange(2), dtype="int64")
    q = QueryBuilder()
    q = q[q["b"] < 5]
    symbol = "test_filter_column_not_present_static"
    lmdb_version_store_dynamic_schema.write(symbol, df)
    vit = lmdb_version_store_dynamic_schema.read(symbol, query_builder=q)

    if IS_PANDAS_TWO and sys.platform.startswith("win32"):
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


def test_filter_column_type_change(lmdb_version_store_dynamic_schema):
    lib = lmdb_version_store_dynamic_schema
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
