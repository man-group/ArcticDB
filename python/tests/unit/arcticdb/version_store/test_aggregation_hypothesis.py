"""
Copyright 2024 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""
import functools
import itertools

import pandas as pd
from hypothesis import assume, given, settings
import hypothesis.strategies as st
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

def larget_common_type(left, right):
    if left is None or right is None:
        return None
    if left == right:
        return left
    if pd.api.types.is_float_dtype(left):
        if pd.api.types.is_float_dtype(right):
            return left if left.itemsize > right.itemsize else right
        elif pd.api.types.is_integer_dtype(right):
            return left
        return None
    elif pd.api.types.is_signed_integer_dtype(left):
        if pd.api.types.is_float_dtype(right):
            return right
        elif pd.api.types.is_signed_integer_dtype(right):
            return left if left.itemsize > right.itemsize else right
        elif pd.api.types.is_unsigned_integer_dtype(right):
            int_dtypes = {1: np.dtype("int8"), 2: np.dtype("int16"), 4: np.dtype("int32"), 8: np.dtype("int64")}
            if right.itemsize >= 8:
                return None
            return int_dtypes[right.itemsize * 2]
    elif pd.api.types.is_unsigned_integer_dtype(left):
        if pd.api.types.is_float_dtype(right):
            return right
        elif pd.api.types.is_unsigned_integer_dtype(right):
            return left if left.itemsize > right.itemsize else right
        elif pd.api.types.is_signed_integer_dtype(left):
            int_dtypes = {1: np.dtype("int8"), 2: np.dtype("int16"), 4: np.dtype("int32"), 8: np.dtype("int64")}
            if left.itemsize >= 8:
                return None
            else:
                return int_dtypes[left.itemsize * 2]
    return None

@st.composite
def aggregation_dataframe_strategy(draw):
    include_grouping = draw(st.booleans())
    include_aggregation = draw(st.booleans())
    columns = []
    if include_grouping:
        columns.append(column_strategy("grouping_column", supported_string_dtypes()))
    if include_aggregation:
        columns.append(column_strategy("agg_column", supported_numeric_dtypes(), restrict_range=True))
    return draw(dataframe_strategy(columns, min_size=1))

@st.composite
def aggregation_dataframe_list_strategy(draw):
    return draw(st.lists(aggregation_dataframe_strategy()))

@use_of_function_scoped_fixtures_in_hypothesis_checked
@settings(deadline=None)
@given(dfs=aggregation_dataframe_list_strategy())
def test_aggregation_numeric_dynamic(lmdb_version_store_dynamic_schema_v1, dfs):
    agg_column_dtypes = [df['agg_column'].dtype for df in dfs if 'agg_column' in df.columns]
    common_agg_type = functools.reduce(larget_common_type, agg_column_dtypes) if len(agg_column_dtypes) > 0 else None
    assume(any('grouping_column' in df.columns for df in dfs) and common_agg_type is not None)
    print("===========================")
    print([df.dtypes for df in dfs])
    print(common_agg_type)
    print("===========================")
    lib = lmdb_version_store_dynamic_schema_v1
    symbol = "test_aggregation_numeric_dynamic"
    lib.delete(symbol)
    for df in dfs:
        lib.append(symbol, df)
    required_types = {
        #"mean": np.float64,
        "sum": common_agg_type,
        "grouping_column": object,
        #"count": np.uint64,
        #"min": common_agg_type,
        #"max": common_agg_type,
    }

    generic_named_aggregation_test(
        lib,
        symbol,
        pd.concat(dfs),
        "grouping_column",
        {
            #"mean": ("agg_column", "mean"),
            "sum": ("agg_column", "sum"),
            #"min": ("agg_column", "min"),
            #"max": ("agg_column", "max"),
            #"count": ("agg_column", "count"),
            # Uncomment when un-feature flagged
            # "first": ("agg_column", "first")
            # "last": ("agg_column", "last"),
        },
        agg_dtypes=required_types
    )

def test_aggregation_missing_in_middle(lmdb_version_store_dynamic_schema_v1):
    lib = lmdb_version_store_dynamic_schema_v1
    sym = "test_aggregation_missing_in_middle"
    #dfs = [
    #    pd.DataFrame({"agg_column": [0.0, 0.0]}),
    #    pd.DataFrame({"grouping_column": ["123"]}),
    #    pd.DataFrame({"grouping_column": ["1"], "agg_column": [0]})
    #]

    #dfs = [
    #    pd.DataFrame({"grouping_column": ["0"]}),
    #    pd.DataFrame({"grouping_column": ["00"], "agg_column": np.array([1], np.uint16)})
    #]

    dfs = [
        pd.DataFrame({"agg_column": np.array([0.0], np.float64)}),
        pd.DataFrame({"grouping_column": ["0"], "agg_column": np.array([0], np.int8)}),
        pd.DataFrame({"grouping_column": ["0"], "agg_column": np.array([0], np.uint64)})
    ]

    #dfs = [
    #    pd.DataFrame({"agg_column": np.array([1], np.int8)}),
    #    pd.DataFrame({"grouping_column": ["0"], "agg_column": np.array([1], np.uint8)})
    #]

    for df in dfs:
        lmdb_version_store_dynamic_schema_v1.append(sym, df)

    required_types = {
        #"mean": np.float64,
        "sum": np.int16,
        "grouping_column": object,
        #"count": np.uint64,
        #"min": common_agg_type,
        #"max": np.uint16
    }

    generic_named_aggregation_test(
        lib,
        sym,
        pd.concat(dfs),
        "grouping_column",
        {
            #"mean": ("agg_column", "mean"),
            "sum": ("agg_column", "sum"),
            #"min": ("agg_column", "min"),
            #"max": ("agg_column", "max"),
            #"count": ("agg_column", "count"),
        },
        #agg_dtypes=required_types
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
