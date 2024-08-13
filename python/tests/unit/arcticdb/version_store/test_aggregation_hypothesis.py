"""
Copyright 2023 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""
import pytest
import numpy as np
import pandas as pd
from hypothesis import assume, given, settings
from hypothesis.extra.pandas import column, data_frames, range_indexes

try:
    from pandas.errors import SpecificationError
except:
    from pandas.core.base import SpecificationError

from arcticdb.version_store.processing import QueryBuilder
from arcticdb.util.test import assert_frame_equal, make_dynamic
from arcticdb.util.hypothesis import (
    use_of_function_scoped_fixtures_in_hypothesis_checked,
    numeric_type_strategies,
    string_strategy,
)

pytestmark = pytest.mark.pipeline


@use_of_function_scoped_fixtures_in_hypothesis_checked
@settings(deadline=None)
@given(
    df=data_frames(
        [
            column("grouping_column", elements=string_strategy),
            column("agg_column", elements=numeric_type_strategies()),
        ],
        index=range_indexes(),
    )
)
def test_aggregation_numeric(lmdb_version_store_v1, df):
    assume(not df.empty)
    lib = lmdb_version_store_v1
    symbol = "test_aggregation_numeric"
    lib.write(symbol, df)

    expected = df.groupby("grouping_column").agg(
        mean=pd.NamedAgg("agg_column", "mean"),
        sum=pd.NamedAgg("agg_column", "sum"),
        min=pd.NamedAgg("agg_column", "min"),
        max=pd.NamedAgg("agg_column", "max"),
        count=pd.NamedAgg("agg_column", "count"),
        # Uncomment when un-feature flagged
        # first=pd.NamedAgg("agg_column", "first"),
        # last=pd.NamedAgg("agg_column", "last"),
    )
    expected = expected.reindex(columns=sorted(expected.columns))
    q = QueryBuilder().groupby("grouping_column").agg(
        {
            "mean": ("agg_column", "mean"),
            "sum": ("agg_column", "sum"),
            "min": ("agg_column", "min"),
            "max": ("agg_column", "max"),
            "count": ("agg_column", "count"),
            # Uncomment when un-feature flagged
            # "first": ("agg_column", "first"),
            # "last": ("agg_column", "last"),
        }
    )
    received = lib.read(symbol, query_builder=q).data
    received = received.reindex(columns=sorted(received.columns))
    received.sort_index(inplace=True)

    # Older versions of Pandas treat values which exceeds limits as `np.inf` or `-np.inf`.
    # ArcticDB adopted this behaviour.
    #
    # Yet, new version of Pandas treats values which exceeds limits as `np.nan` instead.
    # To be able to compare the results, we need to replace `np.inf` and `-np.inf` with `np.nan`.
    received.replace(-np.inf, np.nan, inplace=True)
    received.replace(np.inf, np.nan, inplace=True)

    expected.replace(-np.inf, np.nan, inplace=True)
    expected.replace(np.inf, np.nan, inplace=True)

    assert_frame_equal(expected, received, check_dtype=False)


@use_of_function_scoped_fixtures_in_hypothesis_checked
@settings(deadline=None)
@given(
    df=data_frames(
        [
            column("grouping_column", elements=string_strategy),
            column("agg_column", elements=string_strategy),
        ],
        index=range_indexes(),
    )
)
def test_aggregation_strings(lmdb_version_store_v1, df):
    assume(not df.empty)
    lib = lmdb_version_store_v1
    symbol = "test_aggregation_strings"
    lib.write(symbol, df)

    expected = df.groupby("grouping_column").agg(
        count=pd.NamedAgg("agg_column", "count"),
        # Uncomment when un-feature flagged
        # first=pd.NamedAgg("agg_column", "first"),
        # last=pd.NamedAgg("agg_column", "last"),
    )
    expected = expected.reindex(columns=sorted(expected.columns))
    q = QueryBuilder().groupby("grouping_column").agg(
        {
            "count": ("agg_column", "count"),
            # Uncomment when un-feature flagged
            # "first": ("agg_column", "first"),
            # "last": ("agg_column", "last"),
        }
    )
    received = lib.read(symbol, query_builder=q).data
    received = received.reindex(columns=sorted(received.columns))
    received.sort_index(inplace=True)
    assert_frame_equal(expected, received, check_dtype=False)


##################################
# DYNAMIC SCHEMA TESTS FROM HERE #
##################################


@use_of_function_scoped_fixtures_in_hypothesis_checked
@settings(deadline=None)
@given(
    df=data_frames(
        [
            column("grouping_column", elements=string_strategy),
            column("agg_column", elements=numeric_type_strategies()),
        ],
        index=range_indexes(),
    )
)
def test_aggregation_numeric_dynamic(lmdb_version_store_dynamic_schema_v1, df):
    assume(len(df) >= 3)
    lib = lmdb_version_store_dynamic_schema_v1
    symbol = "test_aggregation_numeric_dynamic"
    lib.delete(symbol)
    slices = [
        df[:len(df) // 3],
        df[len(df) // 3: 2 * len(df) // 3].drop(columns=["grouping_column"]),
        df[2 * len(df) // 3:].drop(columns=["agg_column"]),
    ]
    for slice in slices:
        lib.append(symbol, slice)

    expected = pd.concat(slices).groupby("grouping_column").agg(
        mean=pd.NamedAgg("agg_column", "mean"),
        sum=pd.NamedAgg("agg_column", "sum"),
        min=pd.NamedAgg("agg_column", "min"),
        max=pd.NamedAgg("agg_column", "max"),
        count=pd.NamedAgg("agg_column", "count"),
        # Uncomment when un-feature flagged
        # first=pd.NamedAgg("agg_column", "first"),
        # last=pd.NamedAgg("agg_column", "last"),
    )
    expected = expected.reindex(columns=sorted(expected.columns))
    q = QueryBuilder().groupby("grouping_column").agg(
        {
            "mean": ("agg_column", "mean"),
            "sum": ("agg_column", "sum"),
            "min": ("agg_column", "min"),
            "max": ("agg_column", "max"),
            "count": ("agg_column", "count"),
            # Uncomment when un-feature flagged
            # "first": ("agg_column", "first"),
            # "last": ("agg_column", "last"),
        }
    )
    received = lib.read(symbol, query_builder=q).data
    received = received.reindex(columns=sorted(received.columns))
    received.sort_index(inplace=True)

    # Older versions of Pandas treat values which exceeds limits as `np.inf` or `-np.inf`.
    # ArcticDB adopted this behaviour.
    #
    # Yet, new version of Pandas treats values which exceeds limits as `np.nan` instead.
    # To be able to compare the results, we need to replace `np.inf` and `-np.inf` with `np.nan`.
    received.replace(-np.inf, np.nan, inplace=True)
    received.replace(np.inf, np.nan, inplace=True)

    expected.replace(-np.inf, np.nan, inplace=True)
    expected.replace(np.inf, np.nan, inplace=True)

    assert_frame_equal(expected, received, check_dtype=False)


@use_of_function_scoped_fixtures_in_hypothesis_checked
@settings(deadline=None)
@given(
    df=data_frames(
        [
            column("grouping_column", elements=string_strategy),
            column("agg_column", elements=string_strategy),
        ],
        index=range_indexes(),
    )
)
def test_aggregation_strings_dynamic(lmdb_version_store_dynamic_schema_v1, df):
    assume(len(df) >= 3)
    lib = lmdb_version_store_dynamic_schema_v1
    symbol = "test_aggregation_strings_dynamic"
    lib.delete(symbol)
    slices = [
        df[:len(df) // 3],
        df[len(df) // 3: 2 * len(df) // 3].drop(columns=["grouping_column"]),
        df[2 * len(df) // 3:].drop(columns=["agg_column"]),
    ]
    for slice in slices:
        lib.append(symbol, slice)

    expected = pd.concat(slices).groupby("grouping_column").agg(
        count=pd.NamedAgg("agg_column", "count"),
        # Uncomment when un-feature flagged
        # first=pd.NamedAgg("agg_column", "first"),
        # last=pd.NamedAgg("agg_column", "last"),
    )
    expected = expected.reindex(columns=sorted(expected.columns))
    q = QueryBuilder().groupby("grouping_column").agg(
        {
            "count": ("agg_column", "count"),
            # Uncomment when un-feature flagged
            # "first": ("agg_column", "first"),
            # "last": ("agg_column", "last"),
        }
    )
    received = lib.read(symbol, query_builder=q).data
    received = received.reindex(columns=sorted(received.columns))
    received.sort_index(inplace=True)

    assert_frame_equal(expected, received, check_dtype=False)
