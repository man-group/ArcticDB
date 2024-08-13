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


pytestmark = pytest.mark.pipeline


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
