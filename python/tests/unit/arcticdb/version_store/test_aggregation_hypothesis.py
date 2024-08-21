"""
Copyright 2023 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""
import pandas as pd
from hypothesis import assume, given, settings

from arcticdb.util.test import generic_named_aggregation_test
from arcticdb.util.hypothesis import (
    use_of_function_scoped_fixtures_in_hypothesis_checked,
    supported_numeric_dtypes,
    dataframe_strategy,
    column_strategy,
    supported_string_dtypes,
)

import pytest
pytestmark = pytest.mark.pipeline


@use_of_function_scoped_fixtures_in_hypothesis_checked
@settings(deadline=None)
@given(
    df=dataframe_strategy(
        [
            column_strategy("grouping_column", supported_string_dtypes()),
            column_strategy("agg_column", supported_numeric_dtypes(), restrict_range=True),
        ],
    ),
)
def test_aggregation_numeric(lmdb_version_store_v1, df):
    assume(not df.empty)
    lib = lmdb_version_store_v1
    symbol = "test_aggregation_numeric"
    lib.write(symbol, df)

    generic_named_aggregation_test(
        lib,
        symbol,
        df,
        "grouping_column",
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


@use_of_function_scoped_fixtures_in_hypothesis_checked
@settings(deadline=None)
@given(
    df=dataframe_strategy(
        [
            column_strategy("grouping_column", supported_string_dtypes()),
            column_strategy("agg_column", supported_string_dtypes()),
        ],
    ),
)
def test_aggregation_strings(lmdb_version_store_v1, df):
    assume(not df.empty)
    lib = lmdb_version_store_v1
    symbol = "test_aggregation_strings"
    lib.write(symbol, df)

    generic_named_aggregation_test(
        lib,
        symbol,
        df,
        "grouping_column",
        {
            "count": ("agg_column", "count"),
            # Uncomment when un-feature flagged
            # "first": ("agg_column", "first"),
            # "last": ("agg_column", "last"),
        }
    )


##################################
# DYNAMIC SCHEMA TESTS FROM HERE #
##################################


@use_of_function_scoped_fixtures_in_hypothesis_checked
@settings(deadline=None)
@given(
    df=dataframe_strategy(
        [
            column_strategy("grouping_column", supported_string_dtypes()),
            column_strategy("agg_column", supported_numeric_dtypes(), restrict_range=True),
        ],
    ),
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

    generic_named_aggregation_test(
        lib,
        symbol,
        pd.concat(slices),
        "grouping_column",
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


@use_of_function_scoped_fixtures_in_hypothesis_checked
@settings(deadline=None)
@given(
    df=dataframe_strategy(
        [
            column_strategy("grouping_column", supported_string_dtypes()),
            column_strategy("agg_column", supported_string_dtypes()),
        ],
    ),
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

    generic_named_aggregation_test(
        lib,
        symbol,
        pd.concat(slices),
        "grouping_column",
        {
            "count": ("agg_column", "count"),
            # Uncomment when un-feature flagged
            # "first": ("agg_column", "first"),
            # "last": ("agg_column", "last"),
        }
    )
