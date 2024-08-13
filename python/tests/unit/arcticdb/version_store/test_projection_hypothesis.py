"""
Copyright 2023 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""
from hypothesis import assume, given, settings
from hypothesis.extra.pandas import column, data_frames, range_indexes
import pandas as pd

from arcticdb.version_store.processing import QueryBuilder
from arcticdb.util.test import assert_frame_equal
from arcticdb.util.hypothesis import (
    use_of_function_scoped_fixtures_in_hypothesis_checked,
    numeric_type_strategies,
    non_zero_numeric_type_strategies,
    string_strategy,
)


import pytest
pytestmark = pytest.mark.pipeline


@use_of_function_scoped_fixtures_in_hypothesis_checked
@settings(deadline=None)
@given(
    df=data_frames(
        [column("a", elements=numeric_type_strategies()), column("b", elements=non_zero_numeric_type_strategies())],
        index=range_indexes(),
    ),
    val=non_zero_numeric_type_strategies(),
)
def test_project_numeric_binary_operation(lmdb_version_store_v1, df, val):
    assume(not df.empty)
    lib = lmdb_version_store_v1
    symbol = "test_project_numeric_binary_operation"
    lib.write(symbol, df)
    # Would be cleaner to use pytest.parametrize, but the expensive bit is generating/writing the df, so make sure we
    # only do these operations once to save time
    for op in ["+", "-", "*", "/"]:
        for comp in ["col op col", "col op val", "val op col"]:
            q = QueryBuilder()
            qb_lhs = q["a"] if comp.startswith("col") else val
            qb_rhs = q["b"] if comp.endswith("col") else val
            pandas_lhs = df["a"] if comp.startswith("col") else val
            pandas_rhs = df["b"] if comp.endswith("col") else val
            if op == "+":
                q = q.apply("c", qb_lhs + qb_rhs)
                df["c"] = pandas_lhs + pandas_rhs
            elif op == "-":
                q = q.apply("c", qb_lhs - qb_rhs)
                df["c"] = pandas_lhs - pandas_rhs
            elif op == "*":
                q = q.apply("c", qb_lhs * qb_rhs)
                df["c"] = pandas_lhs * pandas_rhs
            elif op == "/":
                q = q.apply("c", qb_lhs / qb_rhs)
                df["c"] = pandas_lhs / pandas_rhs
            received = lib.read(symbol, query_builder=q).data
            assert_frame_equal(df, received)


@use_of_function_scoped_fixtures_in_hypothesis_checked
@settings(deadline=None)
@given(
    df=data_frames(
        [column("a", elements=numeric_type_strategies())],
        index=range_indexes(),
    ),
)
def test_project_numeric_unary_operation(lmdb_version_store_v1, df):
    assume(not df.empty)
    lib = lmdb_version_store_v1
    symbol = "test_project_numeric_unary_operation"
    lib.write(symbol, df)
    q = QueryBuilder()
    q = q.apply("b", abs(q["a"]))
    df["b"] = abs(df["a"])
    received = lib.read(symbol, query_builder=q).data
    assert_frame_equal(df, received)
    q = QueryBuilder()
    q = q.apply("b", -q["a"])
    df["b"] = -df["a"]
    received = lib.read(symbol, query_builder=q).data
    assert_frame_equal(df, received)


##################################
# DYNAMIC SCHEMA TESTS FROM HERE #
##################################


@use_of_function_scoped_fixtures_in_hypothesis_checked
@settings(deadline=None)
@given(
    df=data_frames(
        [column("a", elements=numeric_type_strategies()), column("b", elements=non_zero_numeric_type_strategies())],
        index=range_indexes(),
    ),
    val=non_zero_numeric_type_strategies(),
)
def test_project_numeric_binary_operation_dynamic(lmdb_version_store_dynamic_schema_v1, df, val):
    assume(len(df) >= 3)
    lib = lmdb_version_store_dynamic_schema_v1
    symbol = "test_project_numeric_binary_operation_dynamic"
    lib.delete(symbol)
    slices = [
        df[:len(df) // 3],
        df[len(df) // 3: 2 * len(df) // 3].drop(columns=["a"]),
        df[2 * len(df) // 3:].drop(columns=["b"]),
    ]
    for slice in slices:
        lib.append(symbol, slice)
    df = pd.concat(slices)
    # Would be cleaner to use pytest.parametrize, but the expensive bit is generating/writing the df, so make sure we
    # only do these operations once to save time
    for op in ["+", "-", "*", "/"]:
        for comp in ["col op col", "col op val", "val op col"]:
            q = QueryBuilder()
            qb_lhs = q["a"] if comp.startswith("col") else val
            qb_rhs = q["b"] if comp.endswith("col") else val
            pandas_lhs = df["a"] if comp.startswith("col") else val
            pandas_rhs = df["b"] if comp.endswith("col") else val
            if op == "+":
                q = q.apply("c", qb_lhs + qb_rhs)
                df["c"] = pandas_lhs + pandas_rhs
            elif op == "-":
                q = q.apply("c", qb_lhs - qb_rhs)
                df["c"] = pandas_lhs - pandas_rhs
            elif op == "*":
                q = q.apply("c", qb_lhs * qb_rhs)
                df["c"] = pandas_lhs * pandas_rhs
            elif op == "/":
                q = q.apply("c", qb_lhs / qb_rhs)
                df["c"] = pandas_lhs / pandas_rhs
            received = lib.read(symbol, query_builder=q).data
            assert_frame_equal(df, received)


@use_of_function_scoped_fixtures_in_hypothesis_checked
@settings(deadline=None)
@given(
    df=data_frames(
        [column("a", elements=numeric_type_strategies())],
        index=range_indexes(),
    ),
)
def test_project_numeric_unary_operation_dynamic(lmdb_version_store_dynamic_schema_v1, df):
    assume(len(df) >= 2)
    lib = lmdb_version_store_dynamic_schema_v1
    symbol = "test_project_numeric_unary_operation_dynamic"
    lib.delete(symbol)
    slices = [
        df[:len(df) // 2],
        df[len(df) // 2:],
    ]
    for slice in slices:
        lib.append(symbol, slice)
    df = pd.concat(slices)
    q = QueryBuilder()
    q = q.apply("b", abs(q["a"]))
    df["b"] = abs(df["a"])
    received = lib.read(symbol, query_builder=q).data
    assert_frame_equal(df, received)
    q = QueryBuilder()
    q = q.apply("b", -q["a"])
    df["b"] = -df["a"]
    received = lib.read(symbol, query_builder=q).data
    assert_frame_equal(df, received)