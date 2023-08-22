"""
Copyright 2023 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""
import pytest
import numpy as np
import pandas as pd
from pandas import DataFrame

from arcticdb.version_store.processing import QueryBuilder
from arcticdb_ext.exceptions import InternalException, SchemaException
from arcticdb.util.test import assert_frame_equal
from arcticdb.util.hypothesis import (
    use_of_function_scoped_fixtures_in_hypothesis_checked,
    non_zero_numeric_type_strategies,
    string_strategy,
)

from hypothesis import assume, given, settings
from hypothesis.extra.pandas import column, data_frames, range_indexes


def test_group_on_float_column_with_nans(lmdb_version_store):
    lib = lmdb_version_store
    sym = "test_group_on_float_column_with_nans"
    df = pd.DataFrame({"grouping_column": [1.0, 2.0, np.nan, 1.0, 2.0, 2.0], "agg_column": [1, 2, 3, 4, 5, 6]})
    lib.write(sym, df)
    expected = df.groupby("grouping_column").agg({"agg_column": "sum"})
    q = QueryBuilder()
    q = q.groupby("grouping_column").agg({"agg_column": "sum"})
    received = lib.read(sym, query_builder=q).data
    received.sort_index(inplace=True)
    assert_frame_equal(expected, received)


@use_of_function_scoped_fixtures_in_hypothesis_checked
@settings(deadline=None)
@given(
    df=data_frames(
        [
            column("grouping_column", elements=string_strategy, fill=string_strategy),
            column("a", elements=non_zero_numeric_type_strategies()),
        ],
        index=range_indexes(),
    )
)
def test_hypothesis_mean_agg(lmdb_version_store, df):
    lib = lmdb_version_store
    assume(not df.empty)

    q = QueryBuilder()
    q = q.groupby("grouping_column").agg({"a": "mean"})
    expected = df.groupby("grouping_column").agg({"a": "mean"})
    expected.replace(
        np.nan, np.inf, inplace=True
    )  # New version of pandas treats values which exceeds limits as np.nan rather than np.inf, as in old version and arcticdb

    symbol = "mean_agg"
    lib.write(symbol, df)
    vit = lib.read(symbol, query_builder=q)
    vit.data.sort_index(inplace=True)
    assert_frame_equal(expected, vit.data)


@use_of_function_scoped_fixtures_in_hypothesis_checked
@settings(deadline=None)
@given(
    df=data_frames(
        [
            column("grouping_column", elements=string_strategy, fill=string_strategy),
            column("a", elements=non_zero_numeric_type_strategies()),
        ],
        index=range_indexes(),
    )
)
def test_hypothesis_sum_agg(lmdb_version_store, df):
    lib = lmdb_version_store
    assume(not df.empty)

    q = QueryBuilder()
    q = q.groupby("grouping_column").agg({"a": "sum"})
    expected = df.groupby("grouping_column").agg({"a": "sum"})
    expected.replace(
        np.nan, np.inf, inplace=True
    )  # New version of pandas treats values which exceeds limits as np.nan rather than np.inf, as in old version and arcticdb

    symbol = "sum_agg"
    lib.write(symbol, df)
    vit = lib.read(symbol, query_builder=q)
    vit.data.sort_index(inplace=True)
    assert_frame_equal(expected, vit.data)


@use_of_function_scoped_fixtures_in_hypothesis_checked
@settings(deadline=None)
@given(
    df=data_frames(
        [
            column("grouping_column", elements=string_strategy, fill=string_strategy),
            column("a", elements=non_zero_numeric_type_strategies()),
            column("b", elements=non_zero_numeric_type_strategies()),
        ],
        index=range_indexes(),
    )
)
def test_hypothesis_max_min_agg(lmdb_version_store, df):
    lib = lmdb_version_store
    assume(not df.empty)

    q = QueryBuilder()
    q = q.groupby("grouping_column").agg({"a": "max", "b": "min"})
    expected = df.groupby("grouping_column").agg({"a": "max", "b": "min"})

    symbol = "max_min_agg"
    lib.write(symbol, df)
    vit = lib.read(symbol, query_builder=q)
    vit.data.sort_index(inplace=True)
    if not np.array_equal(expected, vit.data):
        print("Original dataframe\n{}".format(df))
        print("Expected\n{}".format(expected))
        print("Received\n{}".format(vit.data))
    assert_frame_equal(expected, vit.data)


def test_sum_aggregation(local_object_version_store):
    df = DataFrame(
        {"grouping_column": ["group_1", "group_1", "group_1", "group_2", "group_2"], "to_sum": [1, 1, 2, 2, 2]},
        index=np.arange(5),
    )
    q = QueryBuilder()
    q = q.groupby("grouping_column").agg({"to_sum": "sum"})
    symbol = "test_sum_aggregation"
    local_object_version_store.write(symbol, df)

    res = local_object_version_store.read(symbol, query_builder=q)
    res.data.sort_index(inplace=True)

    df = pd.DataFrame({"to_sum": [4, 4]}, index=["group_1", "group_2"])
    df.index.rename("grouping_column", inplace=True)
    res.data.sort_index(inplace=True)

    assert_frame_equal(res.data, df)


def test_mean_aggregation(local_object_version_store):
    df = DataFrame(
        {"grouping_column": ["group_1", "group_1", "group_1", "group_2", "group_2"], "to_mean": [1, 1, 2, 2, 2]},
        index=np.arange(5),
    )
    q = QueryBuilder()
    q = q.groupby("grouping_column").agg({"to_mean": "mean"})
    symbol = "test_aggregation"
    local_object_version_store.write(symbol, df)

    res = local_object_version_store.read(symbol, query_builder=q)
    res.data.sort_index(inplace=True)

    df = pd.DataFrame({"to_mean": [4 / 3, 2]}, index=["group_1", "group_2"])
    df.index.rename("grouping_column", inplace=True)
    res.data.sort_index(inplace=True)

    assert_frame_equal(res.data, df)


def test_mean_aggregation_float(local_object_version_store):
    df = DataFrame(
        {
            "grouping_column": ["group_1", "group_1", "group_1", "group_2", "group_2"],
            "to_mean": [1.1, 1.4, 2.5, 2.2, 2.2],
        },
        index=np.arange(5),
    )
    q = QueryBuilder()
    q = q.groupby("grouping_column").agg({"to_mean": "mean"})
    symbol = "test_aggregation"
    local_object_version_store.write(symbol, df)

    res = local_object_version_store.read(symbol, query_builder=q)
    res.data.sort_index(inplace=True)

    df = pd.DataFrame({"to_mean": [(1.1 + 1.4 + 2.5) / 3, 2.2]}, index=["group_1", "group_2"])
    df.index.rename("grouping_column", inplace=True)
    res.data.sort_index(inplace=True)

    assert_frame_equal(res.data, df)


def test_mean_aggregation_float_nan(lmdb_version_store_v2):
    df = DataFrame(
        {
            "grouping_column": ["group_1", "group_1", "group_1", "group_2", "group_2"],
            "to_mean": [1.1, 1.4, 2.5, np.nan, 2.2],
        },
        index=np.arange(5),
    )
    q = QueryBuilder()
    q = q.groupby("grouping_column").agg({"to_mean": "mean"})
    symbol = "test_aggregation"
    lmdb_version_store_v2.write(symbol, df)

    res = lmdb_version_store_v2.read(symbol, query_builder=q)

    df = pd.DataFrame({"to_mean": [(1.1 + 1.4 + 2.5) / 3, np.nan]}, index=["group_1", "group_2"])
    df.index.rename("grouping_column", inplace=True)
    res.data.sort_index(inplace=True)

    assert_frame_equal(res.data, df)


def test_max_minus_one(lmdb_version_store):
    symbol = "minus_one"
    lib = lmdb_version_store
    df = pd.DataFrame({"grouping_column": ["thing"], "a": [-1]})
    lib.write(symbol, df)
    q = QueryBuilder()
    q = q.groupby("grouping_column").agg({"a": "max"})
    expected = df.groupby("grouping_column").agg({"a": "max"})

    vit = lib.read(symbol, query_builder=q)
    vit.data.sort_index(inplace=True)
    print(vit.data)
    if not np.array_equal(expected, vit.data):
        print("Original dataframe\n{}".format(df))
        print("Expected\n{}".format(expected))
        print("Received\n{}".format(vit.data))
    assert_frame_equal(expected, vit.data)


def test_group_empty_dataframe(lmdb_version_store):
    df = DataFrame({"grouping_column": [], "to_mean": []})
    q = QueryBuilder()

    q = q.groupby("grouping_column").agg({"to_mean": "mean"})

    symbol = "test_group_empty_dataframe"
    lmdb_version_store.write(symbol, df)
    with pytest.raises(SchemaException):
        _ = lmdb_version_store.read(symbol, query_builder=q)


def test_group_pickled_symbol(lmdb_version_store):
    symbol = "test_group_pickled_symbol"
    lmdb_version_store.write(symbol, np.arange(100).tolist())
    assert lmdb_version_store.is_symbol_pickled(symbol)
    q = QueryBuilder()
    q = q.groupby("grouping_column").agg({"to_mean": "mean"})
    with pytest.raises(InternalException):
        _ = lmdb_version_store.read(symbol, query_builder=q)


def test_group_column_not_present(lmdb_version_store):
    df = DataFrame({"a": np.arange(2)}, index=np.arange(2))
    q = QueryBuilder()
    q = q.groupby("grouping_column").agg({"to_mean": "mean"})
    symbol = "test_group_column_not_present"
    lmdb_version_store.write(symbol, df)
    with pytest.raises(InternalException) as e_info:
        _ = lmdb_version_store.read(symbol, query_builder=q)


def test_group_column_splitting(lmdb_version_store_tiny_segment):
    lib = lmdb_version_store_tiny_segment
    symbol = "test_group_column_splitting"
    df = DataFrame(
        {
            "grouping_column": [1, 2, 3, 4, 1, 2, 3, 4],
            "sum1": [1, 2, 3, 4, 1, 2, 3, 4],
            "max1": [1, 2, 3, 4, 1, 2, 3, 4],
            "sum2": [2, 3, 4, 5, 2, 3, 4, 5],
            "max2": [2, 3, 4, 5, 2, 3, 4, 5],
        }
    )

    lib.write(symbol, df)
    q = QueryBuilder()
    q = q.groupby("grouping_column").agg({"sum1": "sum", "max1": "max", "sum2": "sum", "max2": "max"})
    expected = df.groupby("grouping_column").agg({"sum1": "sum", "max1": "max", "sum2": "sum", "max2": "max"})

    vit = lib.read(symbol, query_builder=q)
    vit.data.sort_index(inplace=True)
    assert_frame_equal(expected, vit.data)


def test_group_column_splitting_strings(lmdb_version_store_tiny_segment):
    lib = lmdb_version_store_tiny_segment
    symbol = "test_group_column_splitting"
    df = DataFrame(
        {
            "grouping_column": ["group_1", "group_1", "group_1", "group_2", "group_2", "group_1", "group_2", "group_1"],
            "sum1": [1, 2, 3, 4, 1, 2, 3, 4],
            "max1": [1, 2, 3, 4, 1, 2, 3, 4],
            "sum2": [2, 3, 4, 5, 2, 3, 4, 5],
            "max2": [2, 3, 4, 5, 2, 3, 4, 5],
        }
    )

    lib.write(symbol, df)
    q = QueryBuilder()
    q = q.groupby("grouping_column").agg({"sum1": "sum", "max1": "max", "sum2": "sum", "max2": "max"})
    expected = df.groupby("grouping_column").agg({"sum1": "sum", "max1": "max", "sum2": "sum", "max2": "max"})

    vit = lib.read(symbol, query_builder=q)
    vit.data.sort_index(inplace=True)
    assert_frame_equal(expected, vit.data)


def test_aggregation_with_nones_and_nans_in_string_grouping_column(version_store_factory):
    lib = version_store_factory(column_group_size=2, segment_row_size=2, dynamic_strings=True)
    symbol = "test_aggregation_with_nones_and_nans_in_string_grouping_column"
    # Structured so that the row-slices of the grouping column contain:
    # 1 - All strings
    # 2 - Strings and Nones
    # 3 - Strings and NaNs
    # 4 - All Nones
    # 5 - All NaNs
    # 6 - Nones and NaNs
    df = DataFrame(
        {
            "grouping_column": [
                "group_1",
                "group_2",
                "group_1",
                None,
                np.nan,
                "group_2",
                None,
                None,
                np.nan,
                np.nan,
                None,
                np.nan,
            ],
            "to_sum": np.arange(12),
        },
        index=np.arange(12),
    )
    lib.write(symbol, df, dynamic_strings=True)

    expected = df.groupby("grouping_column").agg({"to_sum": "sum"})

    q = QueryBuilder()
    q = q.groupby("grouping_column").agg({"to_sum": "sum"})
    res = lib.read(symbol, query_builder=q)
    res.data.sort_index(inplace=True)

    assert_frame_equal(res.data, expected)


def test_docstring_example_query_builder_apply(lmdb_version_store):
    lib = lmdb_version_store
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


def test_doctring_example_query_builder_groupby_max(lmdb_version_store):
    df = DataFrame({"grouping_column": ["group_1", "group_1", "group_1"], "to_max": [1, 5, 4]}, index=np.arange(3))
    q = QueryBuilder()
    q = q.groupby("grouping_column").agg({"to_max": "max"})

    lmdb_version_store.write("symbol", df)
    res = lmdb_version_store.read("symbol", query_builder=q)
    df = pd.DataFrame({"to_max": [5]}, index=["group_1"])
    df.index.rename("grouping_column", inplace=True)
    assert_frame_equal(res.data, df)


def test_docstring_example_query_builder_groupby_max_and_mean(lmdb_version_store):
    df = DataFrame(
        {"grouping_column": ["group_1", "group_1", "group_1"], "to_mean": [1.1, 1.4, 2.5], "to_max": [1.1, 1.4, 2.5]},
        index=np.arange(3),
    )
    q = QueryBuilder()
    q = q.groupby("grouping_column").agg({"to_max": "max", "to_mean": "mean"})

    lmdb_version_store.write("symbol", df)
    res = lmdb_version_store.read("symbol", query_builder=q)
    df = pd.DataFrame({"to_mean": (1.1 + 1.4 + 2.5) / 3, "to_max": [2.5]}, index=["group_1"])
    df.index.rename("grouping_column", inplace=True)
    assert_frame_equal(res.data, df)
