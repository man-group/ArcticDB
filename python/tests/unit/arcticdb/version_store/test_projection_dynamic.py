"""
Copyright 2023 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""
from hypothesis import assume, given, settings
from hypothesis.extra.pandas import column, data_frames, range_indexes
import numpy as np
from pandas.testing import assert_frame_equal
import pandas as pd

from arcticdb.version_store.processing import QueryBuilder
from arcticdb.util.test import make_dynamic, regularize_dataframe
from arcticdb.util.hypothesis import (
    use_of_function_scoped_fixtures_in_hypothesis_checked,
    numeric_type_strategies,
    string_strategy,
)


def test_project_dynamic(lmdb_version_store_dynamic_schema):
    lib = lmdb_version_store_dynamic_schema
    symbol = "test_project_dynamic"

    df = pd.DataFrame(
        {
            "VWAP": np.arange(0, 10, dtype=np.float64),
            "ASK": np.arange(10, 20, dtype=np.uint16),
            "ACVOL": np.arange(20, 30, dtype=np.int32),
        },
        index=np.arange(10),
    )

    expected, slices = make_dynamic(df)
    for df_slice in slices:
        lib.append(symbol, df_slice, write_if_missing=True)

    q = QueryBuilder()
    q = q.apply("ADJUSTED", q["ASK"] * q["ACVOL"] + 7)
    vit = lib.read(symbol, query_builder=q)

    expected["ADJUSTED"] = expected["ASK"] * expected["ACVOL"] + 7
    received = regularize_dataframe(vit.data)
    expected = regularize_dataframe(expected)
    assert_frame_equal(expected, received)


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
def test_project_add_col_col_dynamic(lmdb_version_store_dynamic_schema, df):
    lib = lmdb_version_store_dynamic_schema
    assume(not df.empty)
    symbol = "test_project_add_col_col"

    expected, slices = make_dynamic(df)
    for df_slice in slices:
        lib.append(symbol, df_slice, write_if_missing=True)

    q = QueryBuilder()
    try:
        q = q.apply("x", q["a"] + q["b"])
        vit = lib.read(symbol, query_builder=q)

        expected["x"] = expected["a"] + expected["b"]

        received = regularize_dataframe(vit.data)
        expected = regularize_dataframe(expected)
        assert_frame_equal(expected, received)
    except KeyError:
        pass

    lib.delete(symbol)


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
def test_project_multiply_col_val(lmdb_version_store_dynamic_schema, df):
    lib = lmdb_version_store_dynamic_schema
    assume(not df.empty)
    symbol = "test_project_multiply_col_val"

    expected, slices = make_dynamic(df)
    for df_slice in slices:
        lib.append(symbol, df_slice, write_if_missing=True)

    q = QueryBuilder()
    try:
        q = q.apply("x", q["a"] * 7)
        vit = lib.read(symbol, query_builder=q)

        expected["x"] = expected["a"] * 7
        received = regularize_dataframe(vit.data)
        expected = regularize_dataframe(expected)
        assert_frame_equal(expected, received)
    except KeyError:
        pass

    lib.delete(symbol)


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
def test_project_divide_val_col(lmdb_version_store_dynamic_schema, df):
    lib = lmdb_version_store_dynamic_schema
    assume(not df.empty)
    symbol = "test_project_divide_val_col"

    expected, slices = make_dynamic(df)
    for df_slice in slices:
        lib.append(symbol, df_slice, write_if_missing=True)

    q = QueryBuilder()
    try:
        q = q.apply("x", 100 / q["c"])
        vit = lib.read(symbol, query_builder=q)

        expected["x"] = 100 / expected["c"]
        received = regularize_dataframe(vit.data)
        expected = regularize_dataframe(expected)
        assert_frame_equal(expected, received)
    except KeyError:
        pass

    lib.delete(symbol)
