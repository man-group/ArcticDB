"""
Copyright 2023 Man Group Operations Ltd.
NO WARRANTY, EXPRESSED OR IMPLIED.
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


def test_project(s3_version_store):
    lib = s3_version_store
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
