"""
Copyright 2023 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""
from datetime import datetime
from hypothesis import assume, given, settings
from hypothesis.extra.pandas import column, data_frames, range_indexes
from hypothesis.extra.pytz import timezones as timezone_st
import hypothesis.strategies as st
import numpy as np
import pandas as pd
import pytest
from pytz import timezone

from arcticdb.version_store.processing import QueryBuilder
from arcticdb.util.test import (
    DYNAMIC_STRINGS_SUFFIX,
    FIXED_STRINGS_SUFFIX,
    generic_filter_test,
    generic_filter_test_strings,
    generic_filter_test_dynamic,
    generic_filter_test_strings_dynamic,
)
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


pytestmark = pytest.mark.pipeline


@use_of_function_scoped_fixtures_in_hypothesis_checked
@settings(deadline=None)
@given(
    df=data_frames(
        [column("a", elements=numeric_type_strategies()), column("b", elements=numeric_type_strategies())],
        index=range_indexes(),
    ),
    val=numeric_type_strategies(),
)
def test_filter_numeric_binary_comparison(lmdb_version_store_v1, df, val):
    assume(not df.empty)
    lib = lmdb_version_store_v1
    symbol = "test_filter_numeric_binary_comparison"
    lib.write(symbol, df)
    # Would be cleaner to use pytest.parametrize, but the expensive bit is generating/writing the df, so make sure we
    # only do these operations once to save time
    for op in ["<", "<=", ">", ">=", "==", "!="]:
        for comp in ["col op col", "col op val", "val op col"]:
            q = QueryBuilder()
            qb_lhs = q["a"] if comp.startswith("col") else val
            qb_rhs = q["b"] if comp.endswith("col") else val
            if op == "<":
                q = q[qb_lhs < qb_rhs]
            elif op == "<=":
                q = q[qb_lhs <= qb_rhs]
            elif op == ">":
                q = q[qb_lhs > qb_rhs]
            elif op == ">=":
                q = q[qb_lhs >= qb_rhs]
            elif op == "==":
                q = q[qb_lhs == qb_rhs]
            elif op == "!=":
                q = q[qb_lhs != qb_rhs]
            pandas_lhs = "a" if comp.startswith("col") else val
            pandas_rhs = "b" if comp.endswith("col") else val
            pandas_query = f"{pandas_lhs} {op} {pandas_rhs}"
            generic_filter_test(lib, symbol, df, q, pandas_query)


@use_of_function_scoped_fixtures_in_hypothesis_checked
@settings(deadline=None)
@given(
    df=data_frames(
        [column("a", elements=string_strategy), column("b", elements=string_strategy)], index=range_indexes()
    ),
    val=string_strategy,
)
def test_filter_string_binary_comparison(lmdb_version_store_v1, df, val):
    assume(not df.empty)
    lib = lmdb_version_store_v1
    base_symbol = "test_filter_string_binary_comparison"
    lib.write(f"{base_symbol}_{DYNAMIC_STRINGS_SUFFIX}", df, dynamic_strings=True)
    lib.write(f"{base_symbol}_{FIXED_STRINGS_SUFFIX}", df, dynamic_strings=False)
    # Would be cleaner to use pytest.parametrize, but the expensive bit is generating/writing the df, so make sure we
    # only do these operations once to save time
    for op in ["==", "!="]:
        for comp in ["col op col", "col op val", "val op col"]:
            q = QueryBuilder()
            qb_lhs = q["a"] if comp.startswith("col") else val
            qb_rhs = q["b"] if comp.endswith("col") else val
            q = q[qb_lhs == qb_rhs] if op == "==" else q[qb_lhs != qb_rhs]
            pandas_lhs = "a" if comp.startswith("col") else f"'{val}'"
            pandas_rhs = "b" if comp.endswith("col") else f"'{val}'"
            pandas_query = f"{pandas_lhs} {op} {pandas_rhs}"
            generic_filter_test_strings(lib, base_symbol, df, q, pandas_query)


@use_of_function_scoped_fixtures_in_hypothesis_checked
@settings(deadline=None)
@given(
    df=dataframes_with_names_and_dtypes(["a"], integral_type_strategies()),
    signed_vals=st.frozensets(signed_integral_type_strategies(), min_size=1),
    unsigned_vals=st.frozensets(unsigned_integral_type_strategies(), min_size=1),
)
def test_filter_numeric_set_membership(lmdb_version_store_v1, df, signed_vals, unsigned_vals):
    assume(not df.empty)
    lib = lmdb_version_store_v1
    symbol = "test_filter_numeric_set_membership"
    lib.write(symbol, df)
    # Would be cleaner to use pytest.parametrize, but the expensive bit is generating/writing the df, so make sure we
    # only do these operations once to save time
    for op in ["isin", "isnotin"]:
        for vals in [signed_vals, unsigned_vals]:
            q = QueryBuilder()
            q = q[getattr(q["a"], op)(vals)]
            pandas_query = f"a {'not ' if op == 'isnotin' else ''}in {list(vals)}"
            generic_filter_test(lib, symbol, df, q, pandas_query)


@use_of_function_scoped_fixtures_in_hypothesis_checked
@settings(deadline=None)
@given(
    df=data_frames([column("a", elements=string_strategy)], index=range_indexes()),
    vals=st.frozensets(string_strategy, min_size=1),
)
def test_filter_string_set_membership(lmdb_version_store_v1, df, vals):
    assume(not df.empty)
    lib = lmdb_version_store_v1
    base_symbol = "test_filter_string_set_membership"
    lib.write(f"{base_symbol}_{DYNAMIC_STRINGS_SUFFIX}", df, dynamic_strings=True)
    lib.write(f"{base_symbol}_{FIXED_STRINGS_SUFFIX}", df, dynamic_strings=False)
    # Would be cleaner to use pytest.parametrize, but the expensive bit is generating/writing the df, so make sure we
    # only do these operations once to save time
    for op in ["isin", "isnotin"]:
        q = QueryBuilder()
        q = q[getattr(q["a"], op)(vals)]
        pandas_query = f"a {'not ' if op == 'isnotin' else ''}in {list(vals)}"
        generic_filter_test_strings(lib, base_symbol, df, q, pandas_query)


@use_of_function_scoped_fixtures_in_hypothesis_checked
@settings(deadline=None)
@given(
    df=dataframes_with_names_and_dtypes(["a"], integral_type_strategies()),
)
def test_filter_numeric_empty_set_membership(lmdb_version_store_v1, df):
    assume(not df.empty)
    lib = lmdb_version_store_v1
    symbol = "test_filter_numeric_empty_set_membership"
    lib.write(symbol, df)
    # Would be cleaner to use pytest.parametrize, but the expensive bit is generating/writing the df, so make sure we
    # only do these operations once to save time
    for op in ["isin", "isnotin"]:
        q = QueryBuilder()
        q = q[getattr(q["a"], op)([])]
        pandas_query = f"a {'not ' if op == 'isnotin' else ''}in {[]}"
        generic_filter_test(lib, symbol, df, q, pandas_query)


@use_of_function_scoped_fixtures_in_hypothesis_checked
@settings(deadline=None)
@given(df=data_frames([column("a", elements=string_strategy)], index=range_indexes()))
def test_filter_string_empty_set_membership(lmdb_version_store_v1, df):
    assume(not df.empty)
    lib = lmdb_version_store_v1
    base_symbol = "test_filter_string_empty_set_membership"
    lib.write(f"{base_symbol}_{DYNAMIC_STRINGS_SUFFIX}", df, dynamic_strings=True)
    lib.write(f"{base_symbol}_{FIXED_STRINGS_SUFFIX}", df, dynamic_strings=False)
    # Would be cleaner to use pytest.parametrize, but the expensive bit is generating/writing the df, so make sure we
    # only do these operations once to save time
    for op in ["isin", "isnotin"]:
        q = QueryBuilder()
        q = q[getattr(q["a"], op)([])]
        pandas_query = f"a {'not ' if op == 'isnotin' else ''}in {list([])}"
        generic_filter_test_strings(lib, base_symbol, df, q, pandas_query)


@use_of_function_scoped_fixtures_in_hypothesis_checked
# Restrict datetime range to a couple of years, as this should be sufficient to catch most weird corner cases
@settings(deadline=None)
@given(
    df_dt=st.datetimes(min_value=datetime(2020, 1, 1), max_value=datetime(2022, 1, 1), timezones=timezone_st()),
    comparison_dt=st.datetimes(min_value=datetime(2020, 1, 1), max_value=datetime(2022, 1, 1), timezones=timezone_st()),
)
def test_filter_datetime_timezone_aware_hypothesis(lmdb_version_store_v1, df_dt, comparison_dt):
    lib = lmdb_version_store_v1
    symbol = "test_filter_datetime_timezone_aware_hypothesis"
    df = pd.DataFrame({"a": [df_dt]})
    lib.write(symbol, df)
    for ts in [comparison_dt, pd.Timestamp(comparison_dt)]:
        q = QueryBuilder()
        q = q[q["a"] < ts]
        pandas_query = "a < @ts"
        # Cannot use generic_filter_test as roundtripping a dataframe with datetime64 columns does not preserve tz info
        expected = df.query(pandas_query)
        # Convert to UTC and strip tzinfo to match behaviour of roundtripping through Arctic
        expected["a"] = expected["a"].apply(lambda x: x.tz_convert(timezone("utc")).tz_localize(None))
        received = lib.read(symbol, query_builder=q).data
        if not np.array_equal(expected, received) and (not expected.empty and not received.empty):
            print("ts\n{}".format(ts))
            print("Original dataframe\n{}".format(df))
            print("Expected\n{}".format(expected))
            print("Received\n{}".format(received))
            assert False
        assert True


@use_of_function_scoped_fixtures_in_hypothesis_checked
@settings(deadline=None)
@given(
    df=data_frames(
        [column("a", elements=numeric_type_strategies()), column("b", elements=numeric_type_strategies())],
        index=range_indexes(),
    )
)
def test_filter_binary_boolean(lmdb_version_store_v1, df):
    assume(not df.empty)
    lib = lmdb_version_store_v1
    symbol = "test_filter_binary_boolean"
    lib.write(symbol, df)
    # Would be cleaner to use pytest.parametrize, but the expensive bit is generating/writing the df, so make sure we
    # only do these operations once to save time
    for op in ["&", "|", "^"]:
        q = QueryBuilder()
        if op == "&":
            q = q[(q["a"] < 5) & (q["b"] > 10)]
            pandas_query = "(a < 5) & (b > 10)"
            generic_filter_test(lib, symbol, df, q, pandas_query)
        elif op == "|":
            q = q[(q["a"] < 5) | (q["b"] > 10)]
            pandas_query = "(a < 5) | (b > 10)"
            generic_filter_test(lib, symbol, df, q, pandas_query)
        elif op == "^":
            q = q[(q["a"] < 5) ^ (q["b"] > 10)]
            # Pandas doesn't support '^' for xor
            pandas_query = "((a < 5) & ~(b > 10)) | (~(a < 5) & (b > 10))"
            generic_filter_test(lib, symbol, df, q, pandas_query)


@use_of_function_scoped_fixtures_in_hypothesis_checked
@settings(deadline=None)
@given(
    df=data_frames([column("a", elements=numeric_type_strategies())], index=range_indexes()),
    val=numeric_type_strategies(),
)
def test_filter_not(lmdb_version_store_v1, df, val):
    assume(not df.empty)
    lib = lmdb_version_store_v1
    symbol = "test_filter_not"
    lib.write(symbol, df)
    q = QueryBuilder()
    q = q[~(q["a"] < val)]
    pandas_query = "~(a < {})".format(val)
    generic_filter_test(lib, symbol, df, q, pandas_query)


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
    lib = lmdb_version_store_tiny_segment
    symbol = "test_filter_more_columns_than_fit_in_one_segment"
    lib.write(symbol, df)
    q = QueryBuilder()
    q = q[(q["a"] < q["c"]) | (q["a"] < q["b"]) | (q["b"] < q["c"])]
    pandas_query = "(a < c) | (a < b) | (b < c)"
    generic_filter_test(lib, symbol, df, q, pandas_query)


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
    lib = lmdb_version_store_tiny_segment
    symbol = "test_filter_with_column_filtering"
    q = QueryBuilder()
    q = q[(q["a"] < q["c"]) | (q["a"] < q["b"]) | (q["b"] < q["c"])]
    pandas_query = "(a < c) | (a < b) | (b < c)"
    lib.write(symbol, df)
    expected = df.query(pandas_query).loc[:, ["a", "c"]]
    received = lib.read(symbol, columns=["a", "c"], query_builder=q).data
    assert np.array_equal(expected, received)


##################################
# DYNAMIC SCHEMA TESTS FROM HERE #
##################################


@use_of_function_scoped_fixtures_in_hypothesis_checked
@settings(deadline=None)
@given(
    df=data_frames(
        [column("a", elements=numeric_type_strategies()), column("b", elements=numeric_type_strategies())],
        index=range_indexes(),
    ),
    val=numeric_type_strategies(),
)
def test_filter_numeric_binary_comparison_dynamic(lmdb_version_store_dynamic_schema_v1, df, val):
    assume(len(df) >= 3)
    lib = lmdb_version_store_dynamic_schema_v1
    symbol = "test_filter_numeric_binary_comparison_dynamic"
    lib.delete(symbol)
    slices = [
        df[:len(df) // 3],
        df[len(df) // 3: 2 * len(df) // 3].drop(columns=["a"]),
        df[2 * len(df) // 3:].drop(columns=["b"]),
    ]
    for slice in slices:
        lib.append(symbol, slice)
    # Would be cleaner to use pytest.parametrize, but the expensive bit is generating/writing the df, so make sure we
    # only do these operations once to save time
    for op in ["<", "<=", ">", ">=", "==", "!="]:
        for comp in ["col op col", "col op val", "val op col"]:
            q = QueryBuilder()
            qb_lhs = q["a"] if comp.startswith("col") else val
            qb_rhs = q["b"] if comp.endswith("col") else val
            if op == "<":
                q = q[qb_lhs < qb_rhs]
            elif op == "<=":
                q = q[qb_lhs <= qb_rhs]
            elif op == ">":
                q = q[qb_lhs > qb_rhs]
            elif op == ">=":
                q = q[qb_lhs >= qb_rhs]
            pandas_lhs = "a" if comp.startswith("col") else val
            pandas_rhs = "b" if comp.endswith("col") else val
            pandas_query = f"{pandas_lhs} {op} {pandas_rhs}"
            generic_filter_test_dynamic(lib, symbol, slices, q, pandas_query)


@use_of_function_scoped_fixtures_in_hypothesis_checked
@settings(deadline=None)
@given(
    df=data_frames(
        [column("a", elements=string_strategy), column("b", elements=string_strategy)], index=range_indexes()
    ),
    val=string_strategy,
)
def test_filter_string_binary_comparison_dynamic(lmdb_version_store_dynamic_schema_v1, df, val):
    assume(len(df) >= 3)
    lib = lmdb_version_store_dynamic_schema_v1
    base_symbol = "test_filter_string_binary_comparison_dynamic"

    slices = [
        df[:len(df) // 3],
        df[len(df) // 3: 2 * len(df) // 3].drop(columns=["a"]),
        df[2 * len(df) // 3:].drop(columns=["b"]),
    ]

    for dynamic_strings in [True, False]:
        symbol = f"{base_symbol}_{DYNAMIC_STRINGS_SUFFIX}" if dynamic_strings else f"{base_symbol}_{FIXED_STRINGS_SUFFIX}"
        lib.delete(symbol)
        for slice in slices:
            lib.append(symbol, slice, dynamic_strings=dynamic_strings)

    # Would be cleaner to use pytest.parametrize, but the expensive bit is generating/writing the df, so make sure we
    # only do these operations once to save time
    for op in ["==", "!="]:
        for comp in ["col op col", "col op val", "val op col"]:
            q = QueryBuilder()
            qb_lhs = q["a"] if comp.startswith("col") else val
            qb_rhs = q["b"] if comp.endswith("col") else val
            q = q[qb_lhs == qb_rhs] if op == "==" else q[qb_lhs != qb_rhs]
            pandas_lhs = "a" if comp.startswith("col") else f"'{val}'"
            pandas_rhs = "b" if comp.endswith("col") else f"'{val}'"
            pandas_query = f"{pandas_lhs} {op} {pandas_rhs}"
            generic_filter_test_strings_dynamic(lib, base_symbol, slices, q, pandas_query)


@use_of_function_scoped_fixtures_in_hypothesis_checked
@settings(deadline=None)
@given(
    df=dataframes_with_names_and_dtypes(["a"], integral_type_strategies()),
    signed_vals=st.frozensets(signed_integral_type_strategies(), min_size=1),
    unsigned_vals=st.frozensets(unsigned_integral_type_strategies(), min_size=1),
)
def test_filter_numeric_set_membership_dynamic(lmdb_version_store_dynamic_schema_v1, df, signed_vals, unsigned_vals):
    assume(len(df) >= 2)
    lib = lmdb_version_store_dynamic_schema_v1
    symbol = "test_filter_numeric_set_membership_dynamic"
    lib.delete(symbol)
    slices = [
        df[:len(df) // 2],
        df[len(df) // 2:].drop(columns=["a"]),
    ]
    for slice in slices:
        lib.append(symbol, slice)
    # Would be cleaner to use pytest.parametrize, but the expensive bit is generating/writing the df, so make sure we
    # only do these operations once to save time
    for op in ["isin", "isnotin"]:
        for vals in [signed_vals, unsigned_vals]:
            q = QueryBuilder()
            q = q[getattr(q["a"], op)(vals)]
            pandas_query = f"a {'not ' if op == 'isnotin' else ''}in {list(vals)}"
            generic_filter_test_dynamic(lib, symbol, slices, q, pandas_query)


@use_of_function_scoped_fixtures_in_hypothesis_checked
@settings(deadline=None)
@given(
    df=data_frames([column("a", elements=string_strategy)], index=range_indexes()),
    vals=st.frozensets(string_strategy, min_size=1),
)
def test_filter_string_set_membership_dynamic(lmdb_version_store_dynamic_schema_v1, df, vals):
    assume(len(df) >= 2)
    lib = lmdb_version_store_dynamic_schema_v1
    base_symbol = "test_filter_string_set_membership_dynamic"
    slices = [
        df[:len(df) // 2],
        df[len(df) // 2:].drop(columns=["a"]),
    ]

    for dynamic_strings in [True, False]:
        symbol = f"{base_symbol}_{DYNAMIC_STRINGS_SUFFIX}" if dynamic_strings else f"{base_symbol}_{FIXED_STRINGS_SUFFIX}"
        lib.delete(symbol)
        for slice in slices:
            lib.append(symbol, slice, dynamic_strings=dynamic_strings)
    # Would be cleaner to use pytest.parametrize, but the expensive bit is generating/writing the df, so make sure we
    # only do these operations once to save time
    for op in ["isin", "isnotin"]:
        q = QueryBuilder()
        q = q[getattr(q["a"], op)(vals)]
        pandas_query = f"a {'not ' if op == 'isnotin' else ''}in {list(vals)}"
        generic_filter_test_strings_dynamic(lib, base_symbol, slices, q, pandas_query)