"""
Copyright 2023 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""
import numpy as np
import pandas as pd
import pytest

from arcticdb.version_store.processing import QueryBuilder
from arcticdb.util.test import assert_frame_equal
from arcticdb_ext.exceptions import SchemaException


def test_query_builder_equality_checks():
    q1 = QueryBuilder()
    q2 = QueryBuilder()
    q1 = q1[q1["date"] >= pd.Timestamp("2020-01-01")]
    q2 = q2[q2["date"] >= pd.Timestamp("2020-01-01")]
    assert q1 == q2

    q2 = QueryBuilder()
    q2 = q2[q2["date"] >= pd.Timestamp("2021-01-01")]
    assert q1 != q2


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


@pytest.mark.parametrize("batch", [True, False])
@pytest.mark.parametrize("use_date_range_clause", [True, False])
def test_querybuilder_date_range_then_filter(lmdb_version_store_tiny_segment, batch, use_date_range_clause):
    lib = lmdb_version_store_tiny_segment
    symbol = "test_querybuilder_date_range_then_filter"
    df = pd.DataFrame(
        {"col1": np.arange(10), "col2": np.arange(100, 110)}, index=pd.date_range("2000-01-01", periods=10)
    )
    lib.write(symbol, df)

    date_range = (pd.Timestamp("2000-01-04"), pd.Timestamp("2000-01-07"))

    q = QueryBuilder()
    if use_date_range_clause:
        q = q.date_range(date_range)
    q = q[q["col1"].isin(0, 3, 6, 9)]

    if use_date_range_clause:
        if batch:
            received = lib.batch_read([symbol], query_builder=q)[symbol].data
        else:
            received = lib.read(symbol, query_builder=q).data
    else:
        if batch:
            received = lib.batch_read([symbol], date_ranges=[date_range], query_builder=q)[symbol].data
        else:
            received = lib.read(symbol, date_range=date_range, query_builder=q).data
    expected = df.query("col1 in [3, 6]")
    assert_frame_equal(expected, received)


@pytest.mark.parametrize("batch", [True, False])
@pytest.mark.parametrize("use_date_range_clause", [True, False])
def test_querybuilder_date_range_then_project(lmdb_version_store_tiny_segment, batch, use_date_range_clause):
    lib = lmdb_version_store_tiny_segment
    symbol = "test_querybuilder_date_range_then_project"
    df = pd.DataFrame(
        {"col1": np.arange(10, dtype=np.int64), "col2": np.arange(100, 110, dtype=np.int64)},
        index=pd.date_range("2000-01-01", periods=10),
    )
    lib.write(symbol, df)

    date_range = (pd.Timestamp("2000-01-04"), pd.Timestamp("2000-01-07"))

    q = QueryBuilder()
    if use_date_range_clause:
        q = q.date_range(date_range)
    q = q.apply("new_col", (q["col1"] * q["col2"]) + 13)

    if use_date_range_clause:
        if batch:
            received = lib.batch_read([symbol], query_builder=q)[symbol].data
        else:
            received = lib.read(symbol, query_builder=q).data
    else:
        if batch:
            received = lib.batch_read([symbol], date_ranges=[date_range], query_builder=q)[symbol].data
        else:
            received = lib.read(symbol, date_range=date_range, query_builder=q).data
    expected = df.iloc[3:-3]
    expected["new_col"] = expected["col1"] * expected["col2"] + 13
    assert_frame_equal(expected, received)


@pytest.mark.parametrize("batch", [True, False])
@pytest.mark.parametrize("use_date_range_clause", [True, False])
def test_querybuilder_date_range_then_groupby(lmdb_version_store_tiny_segment, batch, use_date_range_clause):
    lib = lmdb_version_store_tiny_segment
    symbol = "test_querybuilder_date_range_then_groupby"
    df = pd.DataFrame(
        {
            "col1": ["a", "b", "c", "a", "b", "c", "a", "b", "c", "d"],
            "col2": [1, 2, 3, 2, 1, 3, 1, 1, 3, 4],
        },
        index=pd.date_range("2000-01-01", periods=10),
    )
    lib.write(symbol, df)

    date_range = (pd.Timestamp("2000-01-04"), pd.Timestamp("2000-01-07"))

    q = QueryBuilder()
    if use_date_range_clause:
        q = q.date_range(date_range)
    q = q.groupby("col1").agg({"col2": "sum"})

    if use_date_range_clause:
        if batch:
            received = lib.batch_read([symbol], query_builder=q)[symbol].data
        else:
            received = lib.read(symbol, query_builder=q).data
    else:
        if batch:
            received = lib.batch_read([symbol], date_ranges=[date_range], query_builder=q)[symbol].data
        else:
            received = lib.read(symbol, date_range=date_range, query_builder=q).data
    received.sort_index(inplace=True)

    expected = df.iloc[3:-3]
    expected = expected.groupby("col1").agg({"col2": "sum"})
    assert_frame_equal(expected, received)


@pytest.mark.parametrize("batch", [True, False])
@pytest.mark.parametrize("use_row_range_clause", [True, False])
def test_querybuilder_row_range(lmdb_version_store_tiny_segment, batch, use_row_range_clause):
    lib = lmdb_version_store_tiny_segment
    symbol = "test_querybuilder_row_range"
    df = pd.DataFrame({"col1": np.arange(10), "col2": np.arange(100, 110)}, index=np.arange(10))
    lib.write(symbol, df)

    row_range = (3, 7)

    q = QueryBuilder()
    if use_row_range_clause:
        q = q._row_range(row_range)

    if use_row_range_clause:
        if batch:
            received = lib.batch_read([symbol], query_builder=q)[symbol].data
        else:
            received = lib.read(symbol, query_builder=q).data
    else:
        if batch:
            received = lib.batch_read([symbol], row_ranges=[row_range])[symbol].data
        else:
            received = lib.read(symbol, row_range=row_range).data
    expected = df.iloc[3:7]
    assert_frame_equal(expected, received)


@pytest.mark.parametrize("batch", [True, False])
@pytest.mark.parametrize("use_row_range_clause", [True, False])
def test_querybuilder_row_range_then_filter(lmdb_version_store_tiny_segment, batch, use_row_range_clause):
    lib = lmdb_version_store_tiny_segment
    symbol = "test_querybuilder_row_range_then_filter"
    df = pd.DataFrame({"col1": np.arange(10), "col2": np.arange(100, 110)}, index=np.arange(10))
    lib.write(symbol, df)

    row_range = (3, 7)

    q = QueryBuilder()
    if use_row_range_clause:
        q = q._row_range(row_range)
    q = q[q["col1"].isin(0, 3, 6, 9)]

    if use_row_range_clause:
        if batch:
            received = lib.batch_read([symbol], query_builder=q)[symbol].data
        else:
            received = lib.read(symbol, query_builder=q).data
    else:
        if batch:
            received = lib.batch_read([symbol], row_ranges=[row_range], query_builder=q)[symbol].data
        else:
            received = lib.read(symbol, row_range=row_range, query_builder=q).data
    expected = df.query("col1 in [3, 6]")
    assert_frame_equal(expected, received)


@pytest.mark.parametrize("batch", [True, False])
@pytest.mark.parametrize("use_row_range_clause", [True, False])
def test_querybuilder_row_range_then_project(lmdb_version_store_tiny_segment, batch, use_row_range_clause):
    lib = lmdb_version_store_tiny_segment
    symbol = "test_querybuilder_row_range_then_project"
    df = pd.DataFrame(
        {"col1": np.arange(10, dtype=np.int64), "col2": np.arange(100, 110, dtype=np.int64)},
        index=np.arange(10),
    )
    lib.write(symbol, df)

    row_range = (3, 7)

    q = QueryBuilder()
    if use_row_range_clause:
        q = q._row_range(row_range)
    q = q.apply("new_col", (q["col1"] * q["col2"]) + 13)

    if use_row_range_clause:
        if batch:
            received = lib.batch_read([symbol], query_builder=q)[symbol].data
        else:
            received = lib.read(symbol, query_builder=q).data
    else:
        if batch:
            received = lib.batch_read([symbol], row_ranges=[row_range], query_builder=q)[symbol].data
        else:
            received = lib.read(symbol, row_range=row_range, query_builder=q).data
    expected = df.iloc[3:-3]
    expected["new_col"] = expected["col1"] * expected["col2"] + 13
    assert_frame_equal(expected, received)


@pytest.mark.parametrize("batch", [True, False])
@pytest.mark.parametrize("use_row_range_clause", [True, False])
def test_querybuilder_row_range_then_groupby(lmdb_version_store_tiny_segment, batch, use_row_range_clause):
    lib = lmdb_version_store_tiny_segment
    symbol = "test_querybuilder_row_range_then_groupby"
    df = pd.DataFrame(
        {
            "col1": ["a", "b", "c", "a", "b", "c", "a", "b", "c", "d"],
            "col2": [1, 2, 3, 2, 1, 3, 1, 1, 3, 4],
        },
        index=np.arange(10),
    )
    lib.write(symbol, df)

    row_range = (3, 7)

    q = QueryBuilder()
    if use_row_range_clause:
        q = q._row_range(row_range)
    q = q.groupby("col1").agg({"col2": "sum"})

    if use_row_range_clause:
        if batch:
            received = lib.batch_read([symbol], query_builder=q)[symbol].data
        else:
            received = lib.read(symbol, query_builder=q).data
    else:
        if batch:
            received = lib.batch_read([symbol], row_ranges=[row_range], query_builder=q)[symbol].data
        else:
            received = lib.read(symbol, row_range=row_range, query_builder=q).data
    received.sort_index(inplace=True)

    expected = df.iloc[3:-3]
    expected = expected.groupby("col1").agg({"col2": "sum"})
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


def test_querybuilder_pickling():
    """QueryBuilder must be pickleable with all possible clauses."""

    # TODO: Also check that `PythonRowRangeClause` is pickleable once `QueryBuilder.row_range` is implemented.
    q = QueryBuilder()
    # PythonDateRangeClause
    q = q.date_range((pd.Timestamp("2000-01-04"), pd.Timestamp("2000-01-07")))

    # PythonProjectionClause
    q = q[q["col1"].isin(2, 3, 7)]

    # PythonFilterClause
    q = q.apply("new_col", (q["col1"] * q["col2"]) + 13)

    # PythonGroupByClause
    q = q.groupby("col1")

    # PythonAggregationClause
    q = q.agg({"col2": "sum"})

    import pickle

    assert pickle.loads(pickle.dumps(q)) == q


# Remove the following test and replace with more extensive ones once this issue is fixed:
# https://github.com/man-group/ArcticDB/issues/1404
def test_query_builder_sparse(lmdb_version_store):
    lib = lmdb_version_store
    sym = "test_filter_sparse"
    df = pd.DataFrame(
        {
            "sparse_col": [0.0, np.nan, 0.0],
            "dense_col": [0.0, 1.0, 2.0]
         },
        index=pd.date_range("2024-01-01", periods=3)
    )
    lib.write(sym, df, sparsify_floats=True)

    # Filters
    # These 2 filters exercise different code paths
    q = QueryBuilder()
    q = q[q["sparse_col"].isnull()]
    with pytest.raises(SchemaException):
        lib.read(sym, query_builder=q)
    q = QueryBuilder()
    q = q[q["sparse_col"] == np.float64(0.0)]
    with pytest.raises(SchemaException):
        lib.read(sym, query_builder=q)

    # Projections
    q = QueryBuilder().apply("projected_col", q["sparse_col"] + 1)
    with pytest.raises(SchemaException):
        lib.read(sym, query_builder=q)

    # Groupbys
    q = QueryBuilder().groupby("sparse_col").agg({"dense_col": "sum"})
    with pytest.raises(SchemaException):
        lib.read(sym, query_builder=q)

    # Aggregations
    q = QueryBuilder().groupby("dense_col").agg({"sparse_col": "sum"})
    with pytest.raises(SchemaException):
        lib.read(sym, query_builder=q)

    # Date range
    q = QueryBuilder().date_range((pd.Timestamp("2024-01-01"), pd.Timestamp("2024-01-02")))
    with pytest.raises(SchemaException):
        lib.read(sym, query_builder=q)

    # Head
    q = QueryBuilder()._head(2)
    with pytest.raises(SchemaException):
        lib.read(sym, query_builder=q)

    # Tail
    q = QueryBuilder()._tail(2)
    with pytest.raises(SchemaException):
        lib.read(sym, query_builder=q)

    # Row range
    q = QueryBuilder()._row_range((1, 2))
    with pytest.raises(SchemaException):
        lib.read(sym, query_builder=q)
