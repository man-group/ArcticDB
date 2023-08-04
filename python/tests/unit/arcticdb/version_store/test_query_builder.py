"""
Copyright 2023 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""
import numpy as np
import pandas as pd

from arcticdb.version_store.processing import QueryBuilder
from arcticdb.util.test import assert_frame_equal


def test_reuse_querybuilder(lmdb_version_store_tiny_segment):
    lib = lmdb_version_store_tiny_segment
    symbol = "test_reuse_querybuilder"
    df = pd.DataFrame(
        {"col1": np.arange(10, dtype=np.int64), "col2": np.arange(100, 110, dtype=np.int64)}, index=np.arange(10)
    )
    lib.write(symbol, df)

    q = QueryBuilder()
    q = q[q["col1"].isin(2, 3, 7)]
    expected = df.query("col1 in [2, 3, 7]")
    received = lib.read(symbol, query_builder=q).data
    assert_frame_equal(expected, received)

    q = q.apply("new_col", (q["col1"] * q["col2"]) + 13)
    expected = df.query("col1 in [2, 3, 7]")
    received = lib.read(symbol, query_builder=q).data

    expected["new_col"] = (expected["col1"] * expected["col2"]) + 13
    assert_frame_equal(expected, received)


def test_querybuilder_filter_then_filter(lmdb_version_store_tiny_segment):
    lib = lmdb_version_store_tiny_segment
    symbol = "test_querybuilder_filter_then_filter"
    df = pd.DataFrame({"col1": np.arange(10), "col2": np.arange(100, 110)}, index=np.arange(10))
    lib.write(symbol, df)

    q = QueryBuilder()
    q = q[q["col1"].isin(2, 3, 7)]
    q = q[q["col1"].isin(2, 3)]
    received = lib.read(symbol, query_builder=q).data

    expected = df.query("col1 in [2, 3]")
    assert_frame_equal(expected, received)


def test_querybuilder_filter_then_project(lmdb_version_store_tiny_segment):
    lib = lmdb_version_store_tiny_segment
    symbol = "test_querybuilder_filter_then_project"
    df = pd.DataFrame(
        {"col1": np.arange(10, dtype=np.int64), "col2": np.arange(100, 110, dtype=np.int64)}, index=np.arange(10)
    )
    lib.write(symbol, df)

    q = QueryBuilder()
    q = q[q["col1"].isin(2, 3, 7)]
    q = q.apply("new_col", (q["col1"] * q["col2"]) + 13)
    received = lib.read(symbol, query_builder=q).data

    expected = df.query("col1 in [2, 3, 7]")
    expected["new_col"] = expected["col1"] * expected["col2"] + 13
    assert_frame_equal(expected, received)


def test_querybuilder_filter_then_groupby(lmdb_version_store_tiny_segment):
    lib = lmdb_version_store_tiny_segment
    symbol = "test_querybuilder_filter_then_groupby"
    df = pd.DataFrame(
        {"col1": ["a", "b", "c", "a", "b", "c", "a", "b", "c", "d"], "col2": [1, 2, 3, 2, 1, 3, 1, 1, 3, 4]},
        index=np.arange(10),
    )
    lib.write(symbol, df)
    q = QueryBuilder()
    q = q[q["col1"] != "b"]
    q = q.groupby("col1").agg({"col2": "sum"})
    received = lib.read(symbol, query_builder=q).data
    received.sort_index(inplace=True)
    expected = df.query("col1 != 'b'").groupby("col1").agg({"col2": "sum"})
    assert_frame_equal(expected, received)


def test_querybuilder_project_then_filter(lmdb_version_store_tiny_segment):
    lib = lmdb_version_store_tiny_segment
    symbol = "test_querybuilder_project_then_filter"
    df = pd.DataFrame(
        {"col1": np.arange(10, dtype=np.int64), "col2": np.arange(100, 110, dtype=np.int64)}, index=np.arange(10)
    )
    lib.write(symbol, df)

    q = QueryBuilder()
    q = q.apply("new_col", (q["col1"] * q["col2"]) + 13)
    q = q[q["new_col"].isin(13, 114, 538)]
    received = lib.read(symbol, query_builder=q).data

    expected = df
    expected["new_col"] = expected["col1"] * expected["col2"] + 13
    expected = expected.query("new_col in [13, 114, 538]")
    assert_frame_equal(expected, received)


def test_querybuilder_project_then_project(lmdb_version_store_tiny_segment):
    lib = lmdb_version_store_tiny_segment
    symbol = "test_querybuilder_project_then_project"
    df = pd.DataFrame(
        {"col1": np.arange(10, dtype=np.int64), "col2": np.arange(100, 110, dtype=np.int64)}, index=np.arange(10)
    )
    lib.write(symbol, df)

    q = QueryBuilder()
    q = q.apply("new_col1", (q["col1"] * q["col2"]) + 13)
    q = q.apply("new_col2", (q["new_col1"] * q["col2"]) - 5)
    received = lib.read(symbol, query_builder=q).data

    expected = df
    expected["new_col1"] = expected["col1"] * expected["col2"] + 13
    expected["new_col2"] = expected["new_col1"] * expected["col2"] - 5
    assert_frame_equal(expected, received)


def test_querybuilder_project_then_groupby(lmdb_version_store_tiny_segment):
    lib = lmdb_version_store_tiny_segment
    symbol = "test_querybuilder_project_then_groupby"
    df = pd.DataFrame(
        {"col1": [1, 2, 2, 3, 3, 3, 4, 4, 4, 4], "col2": np.arange(0, 1, 0.1, dtype=np.float64)}, index=np.arange(10)
    )
    lib.write(symbol, df)
    q = QueryBuilder()
    q = q.apply("new_col1", q["col1"] * 3)
    q = q.apply("new_col2", q["col2"] + 2.5)
    q = q.groupby("new_col1").agg({"new_col2": "sum"})
    received = lib.read(symbol, query_builder=q).data
    received.sort_index(inplace=True)
    expected = df
    expected["new_col1"] = expected["col1"] * 3
    expected["new_col2"] = expected["col2"] + 2.5
    expected = expected.groupby("new_col1").agg({"new_col2": "sum"})
    assert_frame_equal(expected, received)


def test_querybuilder_groupby_then_filter(lmdb_version_store_tiny_segment):
    lib = lmdb_version_store_tiny_segment
    symbol = "test_querybuilder_groupby_then_filter"
    df = pd.DataFrame(
        {"col1": ["a", "b", "c", "a", "b", "c", "a", "b", "c", "d"], "col2": [1, 2, 3, 2, 1, 3, 1, 1, 3, 4]},
        index=np.arange(10),
    )
    lib.write(symbol, df)
    q = QueryBuilder()
    q = q.groupby("col1").agg({"col2": "sum"})
    q = q[(q["col1"] != "b") & (q["col2"] != 9)]
    received = lib.read(symbol, query_builder=q).data
    received.sort_index(inplace=True)
    expected = df.groupby("col1").agg({"col2": "sum"}).query("(col1 != 'b') & (col2 != 9)")
    assert_frame_equal(expected, received)


def test_querybuilder_groupby_then_project(lmdb_version_store_tiny_segment):
    lib = lmdb_version_store_tiny_segment
    symbol = "test_querybuilder_groupby_then_project"
    df = pd.DataFrame(
        {"col1": [5, 23, 42, 5, 23, 42, 5, 23, 42, 0], "col2": [1, 2, 3, 2, 1, 3, 1, 1, 3, 4]}, index=np.arange(10)
    )
    lib.write(symbol, df)
    q = QueryBuilder()
    q = q.groupby("col1").agg({"col2": "sum"})
    q = q.apply("new_col", q["col2"] * 3)
    received = lib.read(symbol, query_builder=q).data
    received.sort_index(inplace=True)
    expected = df.groupby("col1").agg({"col2": "sum"})
    expected["new_col"] = expected["col2"] * 3
    assert_frame_equal(expected, received)


def test_querybuilder_groupby_then_groupby(lmdb_version_store_tiny_segment):
    lib = lmdb_version_store_tiny_segment
    symbol = "test_querybuilder_groupby_then_groupby"
    df = pd.DataFrame(
        {
            "col1": ["a", "b", "c", "a", "b", "c", "a", "b", "c", "d"],
            "col2": [1, 2, 3, 2, 1, 3, 1, 1, 3, 4],
            "col3": np.arange(100, 110),
        },
        index=np.arange(10),
    )
    lib.write(symbol, df)
    q = QueryBuilder()
    q = q.groupby("col1").agg({"col2": "sum", "col3": "mean"})
    q = q.groupby("col2").agg({"col3": "mean"})
    received = lib.read(symbol, query_builder=q).data
    received.sort_index(inplace=True)
    expected = df.groupby("col1").agg({"col2": "sum", "col3": "mean"}).groupby("col2").agg({"col3": "mean"})
    assert_frame_equal(expected, received)
