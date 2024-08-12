"""
Copyright 2023 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""
from hypothesis import assume, given, settings
from hypothesis.extra.pandas import column, data_frames, range_indexes
import numpy as np
import pandas as pd

from arcticdb.version_store.processing import QueryBuilder
from arcticdb.util.test import assert_frame_equal
from arcticdb.util.hypothesis import (
    use_of_function_scoped_fixtures_in_hypothesis_checked,
    numeric_type_strategies,
    string_strategy,
)


import pytest
pytestmark = pytest.mark.pipeline


def test_project(local_object_version_store):
    lib = local_object_version_store
    df = pd.DataFrame(
        {
            "VWAP": np.arange(0, 10, dtype=np.float64),
            "ASK": np.arange(10, 20, dtype=np.uint16),
            "ACVOL": np.arange(20, 30, dtype=np.int32),
        },
        index=np.arange(10),
    )

    lib.write("expression", df)
    df["ADJUSTED"] = df["ASK"] * df["ACVOL"] + 7
    df["ADJUSTED"] = df["ADJUSTED"].astype("int64")
    q = QueryBuilder()
    q = q.apply("ADJUSTED", q["ASK"] * q["ACVOL"] + 7)
    vit = lib.read("expression", query_builder=q)
    assert_frame_equal(df, vit.data)


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
def test_project_add_col_col(lmdb_version_store, df):
    assume(not df.empty)
    symbol = "test_project_add_col_col"
    q = QueryBuilder()
    lmdb_version_store.write(symbol, df)

    q = QueryBuilder()
    q = q.apply("x", q["a"] + q["b"])
    vit = lmdb_version_store.read(symbol, query_builder=q)

    df["x"] = df["a"] + df["b"]
    df["x"] = df["x"].astype(vit.data["x"].dtypes)
    assert_frame_equal(df, vit.data)


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
def test_project_multiply_col_val(lmdb_version_store, df):
    assume(not df.empty)
    symbol = "test_project_add_col_col"
    q = QueryBuilder()
    lmdb_version_store.write(symbol, df)

    q = QueryBuilder()
    q = q.apply("x", q["a"] * 7)
    vit = lmdb_version_store.read(symbol, query_builder=q)

    df["x"] = df["a"] * 7
    df["x"] = df["x"].astype(vit.data["x"].dtypes)
    assert_frame_equal(df, vit.data)


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
def test_project_divide_val_col(lmdb_version_store, df):
    assume(not df.empty)
    symbol = "test_project_add_col_col"
    q = QueryBuilder()
    lmdb_version_store.write(symbol, df)

    q = QueryBuilder()
    q = q.apply("x", 1000 / q["c"])
    vit = lmdb_version_store.read(symbol, query_builder=q)

    df["x"] = 1000 / df["c"]
    df["x"] = df["x"].astype(vit.data["x"].dtypes)
    assert_frame_equal(df, vit.data)


# @use_of_function_scoped_fixtures_in_hypothesis_checked
# @settings(deadline=None)
# @given(
#     df=data_frames([column("a", elements=numeric_type_strategies())], index=range_indexes()),
#     val=numeric_type_strategies(),
# )
# def test_filter_add_col_val(lmdb_version_store_v1, df, val):
#     assume(not df.empty)
#     q = QueryBuilder()
#     q = q[(q["a"] + val) < 10]
#     pandas_query = "(a + {}) < 10".format(val)
#     generic_filter_test(lmdb_version_store_v1, "test_filter_add_col_val", df, q, pandas_query)
#
#
# @use_of_function_scoped_fixtures_in_hypothesis_checked
# @settings(deadline=None)
# @given(
#     df=data_frames([column("a", elements=numeric_type_strategies())], index=range_indexes()),
#     val=numeric_type_strategies(),
# )
# def test_filter_add_val_col(lmdb_version_store_v1, df, val):
#     assume(not df.empty)
#     q = QueryBuilder()
#     q = q[(val + q["a"]) < 10]
#     pandas_query = "({} + a) < 10".format(val)
#     generic_filter_test(lmdb_version_store_v1, "test_filter_add_val_col", df, q, pandas_query)
#
#
# @use_of_function_scoped_fixtures_in_hypothesis_checked
# @settings(deadline=None)
# @given(
#     df=data_frames(
#         [column("a", elements=numeric_type_strategies()), column("b", elements=numeric_type_strategies())],
#         index=range_indexes(),
#     )
# )
# def test_filter_add_col_col(lmdb_version_store_v1, df):
#     assume(not df.empty)
#     q = QueryBuilder()
#     q = q[(q["a"] + q["b"]) < 10]
#     pandas_query = "(a + b) < 10"
#     generic_filter_test(lmdb_version_store_v1, "test_filter_add_col_col", df, q, pandas_query)
#
#
# @use_of_function_scoped_fixtures_in_hypothesis_checked
# @settings(deadline=None)
# @given(
#     df=data_frames([column("a", elements=numeric_type_strategies())], index=range_indexes()),
#     val=numeric_type_strategies(),
# )
# def test_filter_sub_col_val(lmdb_version_store_v1, df, val):
#     assume(not df.empty)
#     q = QueryBuilder()
#     q = q[(q["a"] - val) < 10]
#     pandas_query = "(a - {}) < 10".format(val)
#     generic_filter_test(lmdb_version_store_v1, "test_filter_sub_col_val", df, q, pandas_query)
#
#
# @use_of_function_scoped_fixtures_in_hypothesis_checked
# @settings(deadline=None)
# @given(
#     df=data_frames([column("a", elements=numeric_type_strategies())], index=range_indexes()),
#     val=numeric_type_strategies(),
# )
# def test_filter_sub_val_col(lmdb_version_store_v1, df, val):
#     assume(not df.empty)
#     q = QueryBuilder()
#     q = q[(val - q["a"]) < 10]
#     pandas_query = "({} - a) < 10".format(val)
#     generic_filter_test(lmdb_version_store_v1, "test_filter_sub_val_col", df, q, pandas_query)
#
#
# @use_of_function_scoped_fixtures_in_hypothesis_checked
# @settings(deadline=None)
# @given(
#     df=data_frames(
#         [column("a", elements=numeric_type_strategies()), column("b", elements=numeric_type_strategies())],
#         index=range_indexes(),
#     )
# )
# def test_filter_sub_col_col(lmdb_version_store_v1, df):
#     assume(not df.empty)
#     q = QueryBuilder()
#     q = q[(q["a"] - q["b"]) < 10]
#     pandas_query = "(a - b) < 10"
#     generic_filter_test(lmdb_version_store_v1, "test_filter_sub_col_col", df, q, pandas_query)
#
#
# @use_of_function_scoped_fixtures_in_hypothesis_checked
# @settings(deadline=None)
# @given(
#     df=data_frames([column("a", elements=numeric_type_strategies())], index=range_indexes()),
#     val=numeric_type_strategies(),
# )
# def test_filter_times_col_val(lmdb_version_store_v1, df, val):
#     assume(not df.empty)
#     q = QueryBuilder()
#     q = q[(q["a"] * val) < 10]
#     pandas_query = "(a * {}) < 10".format(val)
#     generic_filter_test(lmdb_version_store_v1, "test_filter_times_col_val", df, q, pandas_query)
#
#
# @use_of_function_scoped_fixtures_in_hypothesis_checked
# @settings(deadline=None)
# @given(
#     df=data_frames([column("a", elements=numeric_type_strategies())], index=range_indexes()),
#     val=numeric_type_strategies(),
# )
# def test_filter_times_val_col(lmdb_version_store_v1, df, val):
#     assume(not df.empty)
#     q = QueryBuilder()
#     q = q[(val * q["a"]) < 10]
#     pandas_query = "({} * a) < 10".format(val)
#     generic_filter_test(lmdb_version_store_v1, "test_filter_times_val_col", df, q, pandas_query)
#
#
# @use_of_function_scoped_fixtures_in_hypothesis_checked
# @settings(deadline=None)
# @given(
#     df=data_frames(
#         [column("a", elements=numeric_type_strategies()), column("b", elements=numeric_type_strategies())],
#         index=range_indexes(),
#     )
# )
# def test_filter_times_col_col(lmdb_version_store_v1, df):
#     assume(not df.empty)
#     q = QueryBuilder()
#     q = q[(q["a"] * q["b"]) < 10]
#     pandas_query = "(a * b) < 10"
#     generic_filter_test(lmdb_version_store_v1, "test_filter_times_col_col", df, q, pandas_query)
#
#
# @use_of_function_scoped_fixtures_in_hypothesis_checked
# @settings(deadline=None)
# @given(
#     df=data_frames([column("a", elements=numeric_type_strategies())], index=range_indexes()),
#     val=non_zero_numeric_type_strategies(),
# )
# def test_filter_divide_col_val(lmdb_version_store_v1, df, val):
#     assume(not df.empty)
#     q = QueryBuilder()
#     q = q[(q["a"] / val) < 10]
#     pandas_query = "(a / {}) < 10".format(val)
#     generic_filter_test(lmdb_version_store_v1, "test_filter_divide_col_val", df, q, pandas_query)
#
#
# @use_of_function_scoped_fixtures_in_hypothesis_checked
# @settings(deadline=None)
# @given(
#     df=data_frames([column("a", elements=non_zero_numeric_type_strategies())], index=range_indexes()),
#     val=numeric_type_strategies(),
# )
# def test_filter_divide_val_col(lmdb_version_store_v1, df, val):
#     assume(not df.empty)
#     q = QueryBuilder()
#     q = q[(val / q["a"]) < 10]
#     pandas_query = "({} / a) < 10".format(val)
#     generic_filter_test(lmdb_version_store_v1, "test_filter_divide_val_col", df, q, pandas_query)
#
#
# @use_of_function_scoped_fixtures_in_hypothesis_checked
# @settings(deadline=None)
# @given(
#     df=data_frames(
#         [column("a", elements=numeric_type_strategies()), column("b", elements=non_zero_numeric_type_strategies())],
#         index=range_indexes(),
#     )
# )
# def test_filter_divide_col_col(lmdb_version_store_v1, df):
#     assume(not df.empty)
#     q = QueryBuilder()
#     q = q[(q["a"] / q["b"]) < 10]
#     pandas_query = "(a / b) < 10"
#     generic_filter_test(lmdb_version_store_v1, "test_filter_divide_col_col", df, q, pandas_query)
#
#
# @use_of_function_scoped_fixtures_in_hypothesis_checked
# @settings(deadline=None)
# @given(df=data_frames([column("a", elements=string_strategy)], index=range_indexes()), val=numeric_type_strategies())
# def test_filter_arithmetic_string_number_col_val(lmdb_version_store_v1, df, val):
#     lib = lmdb_version_store_v1
#     assume(not df.empty)
#     q = QueryBuilder()
#     q = q[(q["a"] + val) < 10]
#     symbol = "test_filter_arithmetic_string_number_col_val"
#     lib.write(symbol, df, dynamic_strings=True)
#     with pytest.raises(UserInputException) as e_info:
#         lib.read(symbol, query_builder=q)
#     q = QueryBuilder()
#     q = q[(val + q["a"]) < 10]
#     with pytest.raises(UserInputException) as e_info:
#         lib.read(symbol, query_builder=q)
#
#
# @use_of_function_scoped_fixtures_in_hypothesis_checked
# @settings(deadline=None)
# @given(df=data_frames([column("a", elements=numeric_type_strategies())], index=range_indexes()), val=string_strategy)
# def test_filter_arithmetic_string_number_val_col(lmdb_version_store_v1, df, val):
#     lib = lmdb_version_store_v1
#     assume(not df.empty)
#     q = QueryBuilder()
#     q = q[(q["a"] + val) < 10]
#     symbol = "test_filter_arithmetic_string_number_val_col"
#     lib.write(symbol, df)
#     with pytest.raises(UserInputException) as e_info:
#         lib.read(symbol, query_builder=q)
#     q = QueryBuilder()
#     q = q[(val + q["a"]) < 10]
#     with pytest.raises(UserInputException) as e_info:
#         lib.read(symbol, query_builder=q)
#
#
# @use_of_function_scoped_fixtures_in_hypothesis_checked
# @settings(deadline=None)
# @given(
#     df=data_frames(
#         [column("a", elements=string_strategy), column("b", elements=numeric_type_strategies())], index=range_indexes()
#     )
# )
# def test_filter_arithmetic_string_number_col_col(lmdb_version_store_v1, df):
#     lib = lmdb_version_store_v1
#     assume(not df.empty)
#     q = QueryBuilder()
#     q = q[(q["a"] + q["b"]) < 10]
#     symbol = "test_filter_arithmetic_string_number_val_col"
#     lib.write(symbol, df, dynamic_strings=True)
#     with pytest.raises(UserInputException) as e_info:
#         lib.read(symbol, query_builder=q)
#     q = QueryBuilder()
#     q = q[(q["b"] + q["a"]) < 10]
#     with pytest.raises(UserInputException) as e_info:
#         lib.read(symbol, query_builder=q)
#
#
# @use_of_function_scoped_fixtures_in_hypothesis_checked
# @settings(deadline=None)
# @given(df=data_frames([column("a", elements=string_strategy)], index=range_indexes()), val=string_strategy)
# def test_filter_arithmetic_string_string_col_val(lmdb_version_store_v1, df, val):
#     lib = lmdb_version_store_v1
#     assume(not df.empty)
#     q = QueryBuilder()
#     q = q[(q["a"] + val) < 10]
#     symbol = "test_filter_arithmetic_string_string_col_val"
#     lib.write(symbol, df, dynamic_strings=True)
#     with pytest.raises(UserInputException) as e_info:
#         lib.read(symbol, query_builder=q)
#     q = QueryBuilder()
#     q = q[(val + q["a"]) < 10]
#     with pytest.raises(UserInputException) as e_info:
#         lib.read(symbol, query_builder=q)
#
#
# @use_of_function_scoped_fixtures_in_hypothesis_checked
# @settings(deadline=None)
# @given(df=data_frames([column("a", elements=string_strategy)], index=range_indexes()), val=string_strategy)
# def test_filter_arithmetic_string_string_val_col(lmdb_version_store_v1, df, val):
#     lib = lmdb_version_store_v1
#     assume(not df.empty)
#     q = QueryBuilder()
#     q = q[(q["a"] + val) < 10]
#     symbol = "test_filter_arithmetic_string_string_val_col"
#     lib.write(symbol, df, dynamic_strings=True)
#     with pytest.raises(UserInputException) as e_info:
#         lib.read(symbol, query_builder=q)
#     q = QueryBuilder()
#     q = q[(val + q["a"]) < 10]
#     with pytest.raises(UserInputException) as e_info:
#         lib.read(symbol, query_builder=q)
#
#
# @use_of_function_scoped_fixtures_in_hypothesis_checked
# @settings(deadline=None)
# @given(
#     df=data_frames(
#         [column("a", elements=string_strategy), column("b", elements=string_strategy)], index=range_indexes()
#     )
# )
# def test_filter_arithmetic_string_string_col_col(lmdb_version_store_v1, df):
#     lib = lmdb_version_store_v1
#     assume(not df.empty)
#     q = QueryBuilder()
#     q = q[(q["a"] + q["b"]) < 10]
#     symbol = "test_filter_arithmetic_string_string_col_col"
#     lib.write(symbol, df, dynamic_strings=True)
#     with pytest.raises(UserInputException) as e_info:
#         lib.read(symbol, query_builder=q)
#     q = QueryBuilder()
#     q = q[(q["b"] + q["a"]) < 10]
#     with pytest.raises(UserInputException) as e_info:
#         lib.read(symbol, query_builder=q)
#
#
# @use_of_function_scoped_fixtures_in_hypothesis_checked
# @settings(deadline=None)
# @given(
#     df=data_frames([column("a", elements=numeric_type_strategies())], index=range_indexes()),
#     val=numeric_type_strategies(),
# )
# def test_filter_abs(lmdb_version_store_v1, df, val):
#     assume(not df.empty)
#     q = QueryBuilder()
#     q = q[abs(q["a"]) < val]
#     pandas_query = "abs(a) < {}".format(val)
#     generic_filter_test(lmdb_version_store_v1, "test_filter_abs", df, q, pandas_query)
#
#
# @use_of_function_scoped_fixtures_in_hypothesis_checked
# @settings(deadline=None)
# @given(df=data_frames([column("a", elements=string_strategy)], index=range_indexes()), val=string_strategy)
# def test_filter_abs_string(lmdb_version_store_v1, df, val):
#     lib = lmdb_version_store_v1
#     assume(not df.empty)
#     q = QueryBuilder()
#     q = q[abs(q["a"]) < val]
#     symbol = "test_filter_abs_string"
#     lib.write(symbol, df, dynamic_strings=True)
#     with pytest.raises(UserInputException) as e_info:
#         lib.read(symbol, query_builder=q)
#
#
# @use_of_function_scoped_fixtures_in_hypothesis_checked
# @settings(deadline=None)
# @given(
#     df=data_frames([column("a", elements=numeric_type_strategies())], index=range_indexes()),
#     val=numeric_type_strategies(),
# )
# def test_filter_neg(lmdb_version_store_v1, df, val):
#     assume(not df.empty)
#     q = QueryBuilder()
#     q = q[-q["a"] < val]
#     pandas_query = "-a < {}".format(val)
#     generic_filter_test(lmdb_version_store_v1, "test_filter_neg", df, q, pandas_query)
#
#
# @use_of_function_scoped_fixtures_in_hypothesis_checked
# @settings(deadline=None)
# @given(df=data_frames([column("a", elements=string_strategy)], index=range_indexes()), val=string_strategy)
# def test_filter_neg_string(lmdb_version_store_v1, df, val):
#     lib = lmdb_version_store_v1
#     assume(not df.empty)
#     q = QueryBuilder()
#     q = q[-q["a"] < val]
#     symbol = "test_filter_neg_string"
#     lib.write(symbol, df, dynamic_strings=True)
#     with pytest.raises(UserInputException) as e_info:
#         lib.read(symbol, query_builder=q)