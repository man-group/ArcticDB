"""
Copyright 2024 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

from hypothesis import assume, given, settings
import numpy as np
import pandas as pd
import pytest

from arcticdb.version_store.processing import QueryBuilder
from arcticdb.util.test import assert_frame_equal
from arcticdb.util.hypothesis import (
    use_of_function_scoped_fixtures_in_hypothesis_checked,
    supported_numeric_dtypes,
    supported_floating_dtypes,
    dataframe_strategy,
    column_strategy,
    numeric_type_strategies,
)


pytestmark = pytest.mark.pipeline


@use_of_function_scoped_fixtures_in_hypothesis_checked
@settings(deadline=None)
@given(
    df=dataframe_strategy(
        [
            column_strategy("a", supported_numeric_dtypes(), restrict_range=True),
            column_strategy("b", supported_numeric_dtypes(), restrict_range=True),
        ],
    ),
    val=numeric_type_strategies(),
)
def test_project_numeric_binary_operation(lmdb_version_store_v1, df, val):
    assume(not df.empty)
    lib = lmdb_version_store_v1
    symbol = "test_project_numeric_binary_operation"
    lib.write(symbol, df)
    # Would be cleaner to use pytest.parametrize, but the expensive bit is generating/writing the df, so make sure we
    # only do these operations once to save time
    # Have to cast all Pandas values to doubles before computing, otherwise it gets the types wrong and over/underflows
    # a lot: https://github.com/pandas-dev/pandas/issues/59524
    for op in ["+", "-", "*", "/"]:
        for comp in ["col op col", "col op val", "val op col"]:
            q = QueryBuilder()
            qb_lhs = q["a"] if comp.startswith("col") else val
            qb_rhs = q["b"] if comp.endswith("col") else val
            pandas_lhs = df["a"].astype(np.float64) if comp.startswith("col") else np.float64(val)
            pandas_rhs = df["b"].astype(np.float64) if comp.endswith("col") else np.float64(val)
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
            try:
                assert_frame_equal(df, received, check_dtype=False)
            except AssertionError as e:
                original_df = lib.read(symbol).data
                print(
                    f"""Original df:\n{original_df}\nwith dtypes:\n{original_df.dtypes}\nval:\n{val}\nwith dtype:\n{val.dtype}\nquery:\n{q}"""
                    f"""\nPandas result:\n{df}\n"ArcticDB result:\n{received}"""
                )
                raise e


@use_of_function_scoped_fixtures_in_hypothesis_checked
@settings(deadline=None)
@given(df=dataframe_strategy([column_strategy("a", supported_numeric_dtypes(), restrict_range=True)]))
def test_project_numeric_unary_operation(lmdb_version_store_v1, df):
    assume(not df.empty)
    lib = lmdb_version_store_v1
    symbol = "test_project_numeric_unary_operation"
    lib.write(symbol, df)
    q = QueryBuilder()
    q = q.apply("b", abs(q["a"]))
    df["b"] = abs(df["a"].astype(np.float64))
    received = lib.read(symbol, query_builder=q).data
    assert_frame_equal(df, received, check_dtype=False)
    q = QueryBuilder()
    q = q.apply("b", -q["a"])
    df["b"] = -(df["a"].astype(np.float64))
    received = lib.read(symbol, query_builder=q).data
    try:
        assert_frame_equal(df, received, check_dtype=False)
    except AssertionError as e:
        original_df = lib.read(symbol).data
        print(
            f"""Original df:\n{original_df}\nwith dtypes:\n{original_df.dtypes}\nquery:\n{q}"""
            f"""\nPandas result:\n{df}\n"ArcticDB result:\n{received}"""
        )
        raise e


##################################
# DYNAMIC SCHEMA TESTS FROM HERE #
##################################


# It is very complex to mimic our behaviour for backfilling missing columns in Pandas with integral columns, so use floats
@use_of_function_scoped_fixtures_in_hypothesis_checked
@settings(deadline=None)
@given(
    df=dataframe_strategy(
        [
            column_strategy("a", supported_floating_dtypes(), restrict_range=True),
            column_strategy("b", supported_floating_dtypes(), restrict_range=True),
        ],
    ),
    val=numeric_type_strategies(),
)
def test_project_numeric_binary_operation_dynamic(lmdb_version_store_dynamic_schema_v1, df, val):
    assume(len(df) >= 3)
    lib = lmdb_version_store_dynamic_schema_v1
    symbol = "test_project_numeric_binary_operation_dynamic"
    lib.delete(symbol)
    slices = [
        df[: len(df) // 3],
        df[len(df) // 3 : 2 * len(df) // 3].drop(columns=["a"]),
        df[2 * len(df) // 3 :].drop(columns=["b"]),
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
            try:
                assert_frame_equal(df, received, check_dtype=False)
            except AssertionError as e:
                original_df = lib.read(symbol).data
                print(
                    f"""Original df:\n{original_df}\nwith dtypes:\n{original_df.dtypes}\nval:\n{val}\nwith dtype:\n{val.dtype}\nquery:\n{q}"""
                    f"""\nPandas result:\n{df}\n"ArcticDB result:\n{received}"""
                )
                raise e


# It is very complex to mimic our behaviour for backfilling missing columns in Pandas with integral columns, so use floats
@use_of_function_scoped_fixtures_in_hypothesis_checked
@settings(deadline=None)
@given(df=dataframe_strategy([column_strategy("a", supported_floating_dtypes(), restrict_range=True)]))
def test_project_numeric_unary_operation_dynamic(lmdb_version_store_dynamic_schema_v1, df):
    assume(len(df) >= 2)
    lib = lmdb_version_store_dynamic_schema_v1
    symbol = "test_project_numeric_unary_operation_dynamic"
    lib.delete(symbol)
    slices = [
        df[: len(df) // 2],
        df[len(df) // 2 :].rename(columns={"a": "b"}),
    ]
    for slice in slices:
        lib.append(symbol, slice)
    df = pd.concat(slices).astype(np.float64)
    q = QueryBuilder()
    q = q.apply("c", abs(q["a"]))
    df["c"] = abs(df["a"])
    received = lib.read(symbol, query_builder=q).data
    assert_frame_equal(df, received, check_dtype=False)
    q = QueryBuilder()
    q = q.apply("c", -q["a"])
    df["c"] = -df["a"]
    received = lib.read(symbol, query_builder=q).data
    assert_frame_equal(df, received, check_dtype=False)
