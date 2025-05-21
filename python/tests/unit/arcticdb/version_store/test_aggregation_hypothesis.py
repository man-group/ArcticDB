"""
Copyright 2024 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""
import pandas as pd
from hypothesis import assume, given, settings
import pytest
import numpy as np

from arcticdb.util.test import generic_named_aggregation_test
from arcticdb.util.hypothesis import (
    use_of_function_scoped_fixtures_in_hypothesis_checked,
    supported_numeric_dtypes,
    dataframe_strategy,
    column_strategy,
    supported_string_dtypes,
)


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

def largest_numeric_type(dtype):
    if pd.api.types.is_float_dtype(dtype):
        return np.float64
    elif pd.api.types.is_signed_integer_dtype(dtype):
        return np.int64
    elif pd.api.types.is_unsigned_integer_dtype(dtype):
        return np.uint64
    return dtype


def test_aggregation_numeric_dynamic(lmdb_version_store_dynamic_schema_v1):
    df = pd.DataFrame({
        'grouping_column': [1, 0, 0],
        'agg_column': np.array([0, 0, 0], dtype=np.uint64)
    })
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
    required_types = {
        "mean": np.float64,
        "sum": largest_numeric_type(df.dtypes["agg_column"]),
        "grouping_column": df.dtypes["grouping_column"],
        "count": np.uint64,
        # Min and Max are only temporary set to float to keep API the API. See:
        # https://github.com/man-group/ArcticDB/blob/67d2bbe530f96a0aa5412f479e123da480ba2d99/cpp/arcticdb/processing/unsorted_aggregation.cpp#L319
        # The output type must be the largest common type of all segments. If it's an integer, the default is 0. With
        # Arrow, it'll be Arrow's missing value.
        "min": float,
        "max": float,
    }
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
        },
        agg_dtypes=required_types
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
