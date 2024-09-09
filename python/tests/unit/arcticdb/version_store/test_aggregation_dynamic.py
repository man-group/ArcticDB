"""
Copyright 2023 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

import uuid
from hypothesis import assume, given, settings
from hypothesis.extra.pandas import column, data_frames, range_indexes
import pytest
import numpy as np
import pandas as pd
from pandas import DataFrame

try:
    from pandas.errors import SpecificationError
except:
    from pandas.core.base import SpecificationError

from arcticdb.version_store.processing import QueryBuilder
from arcticdb_ext.exceptions import InternalException, SchemaException
from arcticdb.util.test import make_dynamic, assert_frame_equal
from arcticdb.util.hypothesis import (
    use_of_function_scoped_fixtures_in_hypothesis_checked,
    numeric_type_strategies,
    string_strategy,
)
from tests.util.mark import MACOS_CONDA_BUILD


def assert_equal_value(data, expected):
    received = data.reindex(sorted(data.columns), axis=1)
    received.sort_index(inplace=True)
    expected = expected.reindex(sorted(expected.columns), axis=1)
    assert_frame_equal(received.astype("float"), expected)


@pytest.mark.xfail(MACOS_CONDA_BUILD, reason="Conda Pandas returns nan instead of inf like other platforms")
@use_of_function_scoped_fixtures_in_hypothesis_checked
@settings(deadline=None)
@given(
    df=data_frames(
        [
            column("grouping_column", elements=string_strategy, fill=string_strategy),
            column("a", elements=numeric_type_strategies()),
        ],
        index=range_indexes(),
    )
)
def test_hypothesis_mean_agg_dynamic(lmdb_version_store_dynamic_schema_v1, df):
    lib = lmdb_version_store_dynamic_schema_v1
    assume(not df.empty)

    symbol = f"mean_agg-{uuid.uuid4().hex}"
    expected, slices = make_dynamic(df)
    for df_slice in slices:
        lib.append(symbol, df_slice, write_if_missing=True)

    try:
        q = QueryBuilder()
        q = q.groupby("grouping_column").agg({"a": "mean"})
        expected_df = expected.groupby("grouping_column").agg({"a": "mean"})

        vit = lib.read(symbol, query_builder=q)
        received_df = vit.data

        # Older versions of Pandas treat values which exceeds limits as `np.inf` or `-np.inf`.
        # ArcticDB adopted this behaviour.
        #
        # Yet, new version of Pandas treats values which exceeds limits as `np.nan` instead.
        # To be able to compare the results, we need to replace `np.inf` and `-np.inf` with `np.nan`.
        received_df.replace(-np.inf, np.nan, inplace=True)
        received_df.replace(np.inf, np.nan, inplace=True)

        expected_df.replace(-np.inf, np.nan, inplace=True)
        expected_df.replace(np.inf, np.nan, inplace=True)
        assert_equal_value(received_df, expected_df)
    # pandas 1.0 raises SpecificationError rather than KeyError if the column in "agg" doesn't exist
    except (KeyError, SpecificationError):
        pass


@use_of_function_scoped_fixtures_in_hypothesis_checked
@settings(deadline=None)
@given(
    df=data_frames(
        [
            column("grouping_column", elements=string_strategy, fill=string_strategy),
            column("a", elements=numeric_type_strategies()),
        ],
        index=range_indexes(),
    )
)
def test_hypothesis_sum_agg_dynamic(s3_version_store_dynamic_schema_v2, df):
    lib = s3_version_store_dynamic_schema_v2
    assume(not df.empty)

    symbol = f"sum_agg-{uuid.uuid4().hex}"
    expected, slices = make_dynamic(df)
    for df_slice in slices:
        lib.append(symbol, df_slice, write_if_missing=True)

    try:
        q = QueryBuilder()
        q = q.groupby("grouping_column").agg({"a": "sum"})
        expected_df = expected.groupby("grouping_column").agg({"a": "sum"})

        vit = lib.read(symbol, query_builder=q)
        received_df = vit.data

        # Older versions of Pandas treat values which exceeds limits as `np.inf` or `-np.inf`.
        # ArcticDB adopted this behaviour.
        #
        # Yet, new version of Pandas treats values which exceeds limits as `np.nan` instead.
        # To be able to compare the results, we need to replace `np.inf` and `-np.inf` with `np.nan`.
        received_df.replace(-np.inf, np.nan, inplace=True)
        received_df.replace(np.inf, np.nan, inplace=True)

        expected_df.replace(-np.inf, np.nan, inplace=True)
        expected_df.replace(np.inf, np.nan, inplace=True)
        assert_equal_value(received_df, expected_df)
    # pandas 1.0 raises SpecificationError rather than KeyError if the column in "agg" doesn't exist
    except (KeyError, SpecificationError):
        pass


@use_of_function_scoped_fixtures_in_hypothesis_checked
@settings(deadline=None)
@given(
    df=data_frames(
        [
            column("grouping_column", elements=string_strategy, fill=string_strategy),
            column("a", elements=numeric_type_strategies()),
        ],
        index=range_indexes(),
    )
)
def test_hypothesis_max_agg_dynamic(lmdb_version_store_dynamic_schema_v1, df):
    lib = lmdb_version_store_dynamic_schema_v1
    assume(not df.empty)

    symbol = f"max_agg-{uuid.uuid4().hex}"
    expected, slices = make_dynamic(df)
    for df_slice in slices:
        lib.append(symbol, df_slice, write_if_missing=True)

    try:
        q = QueryBuilder()
        q = q.groupby("grouping_column").agg({"a": "max"})
        expected = expected.groupby("grouping_column").agg({"a": "max"})

        vit = lib.read(symbol, query_builder=q)
        assert_equal_value(vit.data, expected)
    # pandas 1.0 raises SpecificationError rather than KeyError if the column in "agg" doesn't exist
    except (KeyError, SpecificationError):
        pass


def count_agg_dynamic(lmdb_version_store_dynamic_schema_v1, df):
    lib = lmdb_version_store_dynamic_schema_v1
    assume(not df.empty)

    symbol = f"count_agg-{uuid.uuid4().hex}"
    expected, slices = make_dynamic(df)
    for df_slice in slices:
        lib.append(symbol, df_slice, write_if_missing=True)

    try:
        q = QueryBuilder()
        q = q.groupby("grouping_column").agg({"a": "count"})

        vit = lib.read(symbol, query_builder=q)
        vit.data.sort_index(inplace=True)

        expected = expected.groupby("grouping_column").agg({"a": "count"})
        expected = expected.astype(np.uint64)

        assert_frame_equal(vit.data, expected)
    # pandas 1.0 raises SpecificationError rather than KeyError if the column in "agg" doesn't exist
    except (KeyError, SpecificationError):
        pass


@use_of_function_scoped_fixtures_in_hypothesis_checked
@settings(deadline=None)
@given(
    df=data_frames(
        [
            column("grouping_column", elements=string_strategy, fill=string_strategy),
            column("a", elements=numeric_type_strategies()),
        ],
        index=range_indexes(),
    )
)
def test_hypothesis_count_agg_dynamic_numeric(lmdb_version_store_dynamic_schema_v1, df):
    count_agg_dynamic(lmdb_version_store_dynamic_schema_v1, df)


@use_of_function_scoped_fixtures_in_hypothesis_checked
@settings(deadline=None)
@given(
    df=data_frames(
        [
            column("grouping_column", elements=string_strategy, fill=string_strategy),
            column("a", elements=string_strategy),
        ],
        index=range_indexes(),
    )
)
def test_hypothesis_count_agg_dynamic_strings(lmdb_version_store_dynamic_schema_v1, df):
    count_agg_dynamic(lmdb_version_store_dynamic_schema_v1, df)


def test_count_aggregation_dynamic(s3_version_store_dynamic_schema_v2):
    lib = s3_version_store_dynamic_schema_v2
    df = DataFrame(
        {
            "grouping_column": ["group_1", "group_1", "group_1", "group_2", "group_2", "group_3"],
            "to_count": [100, 1, 3, 2, 2, np.nan],
        },
        index=np.arange(6),
    )
    symbol = "test_count_aggregation_dynamic"
    expected, slices = make_dynamic(df)
    for df_slice in slices:
        lib.append(symbol, df_slice, write_if_missing=True)

    q = QueryBuilder()
    q = q.groupby("grouping_column").agg({"to_count": "count"})

    received = lib.read(symbol, query_builder=q).data
    received.sort_index(inplace=True)

    expected = expected.groupby("grouping_column").agg({"to_count": "count"})
    expected = expected.astype(np.uint64)

    assert_frame_equal(received, expected)


@use_of_function_scoped_fixtures_in_hypothesis_checked
@settings(deadline=None)
@given(
    df=data_frames(
        [
            column("grouping_column", elements=string_strategy, fill=string_strategy),
            column("a", elements=numeric_type_strategies()),
        ],
        index=range_indexes(),
    )
)
@pytest.mark.xfail(reason="Not supported yet")
def test_hypothesis_first_agg_dynamic_numeric(lmdb_version_store_dynamic_schema_v1, df):
    lib = lmdb_version_store_dynamic_schema_v1
    assume(not df.empty)

    symbol = f"first_agg-{uuid.uuid4().hex}"
    expected, slices = make_dynamic(df)
    for df_slice in slices:
        lib.append(symbol, df_slice, write_if_missing=True)

    try:
        q = QueryBuilder()
        q = q.groupby("grouping_column").agg({"a": "first"})

        vit = lib.read(symbol, query_builder=q)
        vit.data.sort_index(inplace=True)

        expected = expected.groupby("grouping_column").agg({"a": "first"})

        assert_frame_equal(vit.data, expected)
    # pandas 1.0 raises SpecificationError rather than KeyError if the column in "agg" doesn't exist
    except (KeyError, SpecificationError):
        pass


@pytest.mark.xfail(reason="Not supported yet")
def test_first_aggregation_dynamic(s3_version_store_dynamic_schema_v2):
    lib = s3_version_store_dynamic_schema_v2
    df = DataFrame(
        {
            "grouping_column": ["group_1", "group_2", "group_4", "group_2", "group_1", "group_3", "group_1"],
            "get_first": [100.0, np.nan, np.nan, 2.7, 1.4, 5.8, 3.45],
        },
        index=np.arange(7),
    )
    symbol = "test_first_aggregation_dynamic"
    expected, slices = make_dynamic(df)
    for df_slice in slices:
        lib.append(symbol, df_slice, write_if_missing=True)

    q = QueryBuilder()
    q = q.groupby("grouping_column").agg({"get_first": "first"})

    received = lib.read(symbol, query_builder=q).data
    received.sort_index(inplace=True)

    expected = expected.groupby("grouping_column").agg({"get_first": "first"})

    assert_frame_equal(received, expected)


@use_of_function_scoped_fixtures_in_hypothesis_checked
@settings(deadline=None)
@given(
    df=data_frames(
        [
            column("grouping_column", elements=string_strategy, fill=string_strategy),
            column("a", elements=numeric_type_strategies()),
        ],
        index=range_indexes(),
    )
)
@pytest.mark.xfail(reason="Not supported yet")
def test_hypothesis_last_agg_dynamic_numeric(lmdb_version_store_dynamic_schema_v1, df):
    lib = lmdb_version_store_dynamic_schema_v1
    assume(not df.empty)

    symbol = f"last_agg-{uuid.uuid4().hex}"
    expected, slices = make_dynamic(df)
    for df_slice in slices:
        lib.append(symbol, df_slice, write_if_missing=True)

    try:
        q = QueryBuilder()
        q = q.groupby("grouping_column").agg({"a": "last"})

        vit = lib.read(symbol, query_builder=q)
        vit.data.sort_index(inplace=True)

        expected = expected.groupby("grouping_column").agg({"a": "last"})

        assert_frame_equal(vit.data, expected)
    # pandas 1.0 raises SpecificationError rather than KeyError if the column in "agg" doesn't exist
    except (KeyError, SpecificationError):
        pass


@pytest.mark.xfail(reason="Not supported yet")
def test_last_aggregation_dynamic(s3_version_store_dynamic_schema_v2):
    lib = s3_version_store_dynamic_schema_v2
    df = DataFrame(
        {
            "grouping_column": [
                "group_1",
                "group_2",
                "group_4",
                "group_5",
                "group_2",
                "group_1",
                "group_3",
                "group_1",
                "group_5",
            ],
            "get_last": [100.0, 2.7, np.nan, np.nan, np.nan, 1.4, 5.8, 3.45, 6.9],
        },
        index=np.arange(9),
    )
    symbol = "test_last_aggregation_dynamic"
    expected, slices = make_dynamic(df)
    for df_slice in slices:
        lib.append(symbol, df_slice, write_if_missing=True)

    q = QueryBuilder()
    q = q.groupby("grouping_column").agg({"get_last": "last"})

    received = lib.read(symbol, query_builder=q).data
    received.sort_index(inplace=True)

    expected = expected.groupby("grouping_column").agg({"get_last": "last"})

    assert_frame_equal(received, expected)


def test_sum_aggregation_dynamic(s3_version_store_dynamic_schema_v2):
    lib = s3_version_store_dynamic_schema_v2
    df = DataFrame(
        {"grouping_column": ["group_1", "group_1", "group_1", "group_2", "group_2"], "to_sum": [1, 1, 2, 2, 2]},
        index=np.arange(5),
    )
    symbol = "test_sum_aggregation_dynamic"
    expected, slices = make_dynamic(df)
    for df_slice in slices:
        lib.append(symbol, df_slice, write_if_missing=True)

    q = QueryBuilder()
    q = q.groupby("grouping_column").agg({"to_sum": "sum"})

    received = lib.read(symbol, query_builder=q).data
    received.sort_index(inplace=True)
    expected = expected.groupby("grouping_column").agg({"to_sum": "sum"})
    assert_equal_value(received, expected)


def test_sum_aggregation_with_range_index_dynamic(lmdb_version_store_dynamic_schema_v1):
    lib = lmdb_version_store_dynamic_schema_v1
    df = DataFrame(
        {"grouping_column": ["group_1", "group_1", "group_1", "group_2", "group_2"], "to_sum": [1, 1, 2, 2, 2]}
    )
    symbol = "test_sum_aggregation_dynamic"
    expected, slices = make_dynamic(df)
    for df_slice in slices:
        lib.append(symbol, df_slice, write_if_missing=True)

    q = QueryBuilder()
    q = q.groupby("grouping_column").agg({"to_sum": "sum"})

    received = lib.read(symbol, query_builder=q).data
    received.sort_index(inplace=True)
    expected = expected.groupby("grouping_column").agg({"to_sum": "sum"})
    assert_equal_value(received, expected)


def test_group_empty_dataframe_dynamic(lmdb_version_store_dynamic_schema):
    df = DataFrame({"grouping_column": [], "to_mean": []})
    q = QueryBuilder()

    q = q.groupby("grouping_column").agg({"to_mean": "mean"})

    symbol = "test_group_empty_dataframe"
    lmdb_version_store_dynamic_schema.write(symbol, df)
    with pytest.raises(SchemaException):
        _ = lmdb_version_store_dynamic_schema.read(symbol, query_builder=q)


def test_group_pickled_symbol_dynamic(lmdb_version_store_dynamic_schema):
    symbol = "test_group_pickled_symbol"
    lmdb_version_store_dynamic_schema.write(symbol, np.arange(100).tolist())
    assert lmdb_version_store_dynamic_schema.is_symbol_pickled(symbol)
    q = QueryBuilder()
    q = q.groupby("grouping_column").agg({"to_mean": "mean"})
    with pytest.raises(InternalException):
        _ = lmdb_version_store_dynamic_schema.read(symbol, query_builder=q)


def test_group_column_not_present_dynamic(lmdb_version_store_dynamic_schema):
    df = DataFrame({"a": np.arange(2)}, index=np.arange(2))
    q = QueryBuilder()
    q = q.groupby("grouping_column").agg({"to_mean": "mean"})
    symbol = "test_group_column_not_present"
    lmdb_version_store_dynamic_schema.write(symbol, df)
    with pytest.raises(SchemaException):
        _ = lmdb_version_store_dynamic_schema.read(symbol, query_builder=q)


def test_group_column_splitting_dynamic(lmdb_version_store_tiny_segment_dynamic):
    lib = lmdb_version_store_tiny_segment_dynamic
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

    expected, slices = make_dynamic(df)
    for df_slice in slices:
        lib.append(symbol, df_slice, write_if_missing=True)

    q = QueryBuilder()
    q = q.groupby("grouping_column").agg({"sum1": "sum", "max1": "max", "sum2": "sum", "max2": "max"})
    expected = expected.groupby("grouping_column").agg({"sum1": "sum", "max1": "max", "sum2": "sum", "max2": "max"})
    vit = lib.read(symbol, query_builder=q)

    assert_equal_value(vit.data, expected)


def test_group_column_splitting_strings_dynamic(lmdb_version_store_tiny_segment_dynamic):
    lib = lmdb_version_store_tiny_segment_dynamic
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

    expected, slices = make_dynamic(df)
    for df_slice in slices:
        lib.append(symbol, df_slice, write_if_missing=True)

    q = QueryBuilder()
    q = q.groupby("grouping_column").agg({"sum1": "sum", "max1": "max", "sum2": "sum", "max2": "max"})
    expected = expected.groupby("grouping_column").agg({"sum1": "sum", "max1": "max", "sum2": "sum", "max2": "max"})

    vit = lib.read(symbol, query_builder=q)
    assert_equal_value(vit.data, expected)


def test_segment_without_aggregation_column(lmdb_version_store_dynamic_schema):
    lib = lmdb_version_store_dynamic_schema
    sym = "test_segment_without_aggregation_column"

    write_df = pd.DataFrame({"grouping_column": ["group_0"], "aggregation_column": [10330.0]})
    lib.write(sym, write_df)
    append_df = pd.DataFrame({"grouping_column": ["group_1"]})
    lib.append(sym, append_df)
    df = pd.concat((write_df, append_df))

    for aggregation_operation in ["max", "min", "mean", "sum"]:
        expected = df.groupby("grouping_column").agg({"aggregation_column": aggregation_operation})
        q = QueryBuilder()
        q = q.groupby("grouping_column").agg({"aggregation_column": aggregation_operation})
        received = lib.read(sym, query_builder=q).data
        assert_equal_value(received, expected)


def test_minimal_repro_type_change(lmdb_version_store_dynamic_schema):
    lib = lmdb_version_store_dynamic_schema
    sym = "test_minimal_repro_type_change"

    write_df = pd.DataFrame({"grouping_column": ["group_1"], "to_sum": [np.uint8(1)]})
    lib.write(sym, write_df)
    append_df = pd.DataFrame({"grouping_column": ["group_1"], "to_sum": [1.5]})
    lib.append(sym, append_df)
    df = pd.concat([write_df, append_df])
    expected = df.groupby("grouping_column").agg({"to_sum": "sum"})

    q = QueryBuilder()
    q = q.groupby("grouping_column").agg({"to_sum": "sum"})
    received = lib.read(sym, query_builder=q).data
    assert_equal_value(received, expected)


def test_minimal_repro_type_change_max(lmdb_version_store_dynamic_schema):
    lib = lmdb_version_store_dynamic_schema
    sym = "test_minimal_repro_type_change_max"

    write_df = pd.DataFrame({"grouping_column": ["group_1"], "to_max": [np.uint8(1)]})
    lib.write(sym, write_df)

    append_df = pd.DataFrame({"grouping_column": ["group_1"], "to_max": [0.5]})
    lib.append(sym, append_df)
    df = pd.concat((write_df, append_df))

    expected = df.groupby("grouping_column").agg({"to_max": "max"})

    q = QueryBuilder()
    q = q.groupby("grouping_column").agg({"to_max": "max"})
    received = lib.read(sym, query_builder=q).data
    assert_equal_value(received, expected)


def test_minimal_repro_type_sum_similar_string_group_values(lmdb_version_store_dynamic_schema):
    lib = lmdb_version_store_dynamic_schema
    sym = "test_minimal_repro_type_change_max"

    write_df = pd.DataFrame({"grouping_column": ["0", "000"], "to_sum": [1.0, 1.0]})
    lib.write(sym, write_df)
    expected = write_df.groupby("grouping_column").agg({"to_sum": "sum"})

    q = QueryBuilder()
    q = q.groupby("grouping_column").agg({"to_sum": "sum"})
    received = lib.read(sym, query_builder=q).data
    assert_equal_value(received, expected)


def test_aggregation_grouping_column_missing_from_row_group(lmdb_version_store_dynamic_schema):
    lib = lmdb_version_store_dynamic_schema
    df0 = DataFrame(
        {"to_sum": [1, 2], "grouping_column": ["group_1", "group_2"]},
        index=np.arange(2),
    )
    df1 = DataFrame(
        {"to_sum": [3, 4]},
        index=np.arange(2, 4),
    )
    expected = pd.concat((df0, df1)).groupby("grouping_column").agg({"to_sum": "sum"})

    symbol = "test_aggregation_grouping_column_missing_from_row_group"
    lib.write(symbol, df0)
    lib.append(symbol, df1)

    q = QueryBuilder()
    q = q.groupby("grouping_column").agg({"to_sum": "sum"})

    res = lib.read(symbol, query_builder=q)
    res.data.sort_index(inplace=True)

    assert_frame_equal(res.data, expected)
