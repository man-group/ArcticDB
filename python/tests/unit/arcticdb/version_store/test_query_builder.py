"""
Copyright 2023 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""
import numpy as np
import pandas as pd
import pickle
import pytest
import datetime
import dateutil

from arcticdb.version_store.processing import QueryBuilder
from arcticdb.util.test import assert_frame_equal
from arcticdb_ext.exceptions import SchemaException


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


@pytest.mark.parametrize("use_date_range_clause", [True, False])
def test_querybuilder_date_range_then_filter(lmdb_version_store_tiny_segment, use_date_range_clause):
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
        received = lib.read(symbol, query_builder=q).data
    else:
        received = lib.read(symbol, date_range=date_range, query_builder=q).data
    expected = df.query("col1 in [3, 6]")
    assert_frame_equal(expected, received)


def test_querybuilder_filter_datetime_with_timezone(lmdb_version_store_tiny_segment):
    lib = lmdb_version_store_tiny_segment
    symbol = "symbol"
    def can_read_back(write_with_time, filter_with_time):
        df = pd.DataFrame({"col": [write_with_time]})
        lib.delete(symbol)
        lib.write(symbol, df)

        q = QueryBuilder()
        q = q[q["col"] == filter_with_time]
        read_df = lib.read(symbol, query_builder=q).data

        return len(read_df) == 1

    notz_winter_time = datetime.datetime(2024, 1, 1)
    notz_summer_time = datetime.datetime(2024, 6, 1)
    utc_time = datetime.datetime(2024, 6, 1, tzinfo=dateutil.tz.tzutc())
    us_time = datetime.datetime(2024, 6, 1, tzinfo=dateutil.tz.gettz('America/New_York'))

    # Reading back the same time should always succeed
    assert can_read_back(notz_winter_time, notz_winter_time)
    assert can_read_back(notz_summer_time, notz_summer_time)
    assert can_read_back(utc_time, utc_time)
    assert can_read_back(us_time, us_time)

    # If tzinfo is not specified we assume UTC
    assert can_read_back(notz_summer_time, utc_time)
    assert can_read_back(utc_time, notz_summer_time)
    assert not can_read_back(notz_summer_time, us_time)
    assert not can_read_back(us_time, notz_summer_time)



@pytest.mark.parametrize("use_date_range_clause", [True, False])
def test_querybuilder_date_range_then_project(lmdb_version_store_tiny_segment, use_date_range_clause):
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
        received = lib.read(symbol, query_builder=q).data
    else:
        received = lib.read(symbol, date_range=date_range, query_builder=q).data
    expected = df.iloc[3:-3]
    expected["new_col"] = expected["col1"] * expected["col2"] + 13
    assert_frame_equal(expected, received)


@pytest.mark.parametrize("use_date_range_clause", [True, False])
def test_querybuilder_date_range_then_groupby(lmdb_version_store_tiny_segment, use_date_range_clause):
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
        received = lib.read(symbol, query_builder=q).data
    else:
        received = lib.read(symbol, date_range=date_range, query_builder=q).data
    received.sort_index(inplace=True)

    expected = df.iloc[3:-3]
    expected = expected.groupby("col1").agg({"col2": "sum"})
    assert_frame_equal(expected, received)


@pytest.mark.parametrize("use_row_range_clause", [True, False])
def test_querybuilder_row_range(lmdb_version_store_tiny_segment, use_row_range_clause):
    lib = lmdb_version_store_tiny_segment
    symbol = "test_querybuilder_row_range_then_filter"
    df = pd.DataFrame({"col1": np.arange(10), "col2": np.arange(100, 110)}, index=np.arange(10))
    lib.write(symbol, df)

    row_range = (3, 7)

    q = QueryBuilder()
    if use_row_range_clause:
        q = q._row_range(row_range)

    if use_row_range_clause:
        received = lib.read(symbol, query_builder=q).data
    else:
        received = lib.read(symbol, row_range=row_range).data

    expected = df.iloc[3:7]
    assert_frame_equal(expected, received)


@pytest.mark.parametrize("use_row_range_clause", [True, False])
def test_querybuilder_row_range_then_filter(lmdb_version_store_tiny_segment, use_row_range_clause):
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
        received = lib.read(symbol, query_builder=q).data
    else:
        received = lib.read(symbol, row_range=row_range, query_builder=q).data
    expected = df.query("col1 in [3, 6]")
    assert_frame_equal(expected, received)


@pytest.mark.parametrize("use_row_range_clause", [True, False])
def test_querybuilder_row_range_then_project(lmdb_version_store_tiny_segment, use_row_range_clause):
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
        received = lib.read(symbol, query_builder=q).data
    else:
        received = lib.read(symbol, row_range=row_range, query_builder=q).data
    expected = df.iloc[3:-3]
    expected["new_col"] = expected["col1"] * expected["col2"] + 13
    assert_frame_equal(expected, received)


@pytest.mark.parametrize("use_row_range_clause", [True, False])
def test_querybuilder_row_range_then_groupby(lmdb_version_store_tiny_segment, use_row_range_clause):
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
        received = lib.read(symbol, query_builder=q).data
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


def test_querybuilder_resample_then_filter(lmdb_version_store_tiny_segment):
    lib = lmdb_version_store_tiny_segment
    symbol = "test_querybuilder_resample_then_filter"
    idx = [0, 1, 2, 3, 1000, 1001]
    idx = np.array(idx, dtype="datetime64[ns]")
    df = pd.DataFrame({"col": np.arange(6)}, index=idx)
    lib.write(symbol, df)

    q = QueryBuilder()
    q = q.resample("us").agg({"col": "sum"})
    q = q[q["col"] == 9]

    received = lib.read(symbol, query_builder=q).data

    expected = df.resample("us").agg({"col": "sum"})
    expected = expected.query("col == 9")
    assert_frame_equal(expected, received, check_dtype=False)


def test_querybuilder_resample_then_project(lmdb_version_store_tiny_segment):
    lib = lmdb_version_store_tiny_segment
    symbol = "test_querybuilder_resample_then_project"
    idx = [0, 1, 2, 3, 1000, 1001]
    idx = np.array(idx, dtype="datetime64[ns]")
    df = pd.DataFrame({"col": np.arange(6)}, index=idx)
    lib.write(symbol, df)

    q = QueryBuilder()
    q = q.resample("us").agg({"col": "sum"})
    q = q.apply("new_col", q["col"] * 3)

    received = lib.read(symbol, query_builder=q).data

    expected = df.resample("us").agg({"col": "sum"})
    expected["new_col"] = expected["col"] * 3
    assert_frame_equal(expected, received, check_dtype=False)


def test_querybuilder_resample_then_groupby(lmdb_version_store_tiny_segment):
    lib = lmdb_version_store_tiny_segment
    symbol = "test_querybuilder_resample_then_groupby"
    idx = [0, 1, 1000, 1001, 2000, 2001, 3000, 3001]
    idx = np.array(idx, dtype="datetime64[ns]")
    # After downsampling and summing, grouping_col will be [0, 1, 1, 0]
    df = pd.DataFrame(
        {
            "grouping_col": [0, 0, 10, -9, 20, -19, 30, -30],
            "agg_col": np.arange(8),
        },
        index=idx)
    lib.write(symbol, df)

    q = QueryBuilder()
    q = q.resample("us").agg({"grouping_col": "sum", "agg_col": "sum"})
    q = q.groupby("grouping_col").agg({"agg_col": "sum"})

    received = lib.read(symbol, query_builder=q).data
    received.sort_index(inplace=True)

    expected = df.resample("us").agg({"grouping_col": "sum", "agg_col": "sum"})
    expected = expected.groupby("grouping_col").agg({"agg_col": "sum"})
    assert_frame_equal(expected, received, check_dtype=False)


def test_querybuilder_pickling():
    """QueryBuilder must be pickleable with all possible clauses."""

    # TODO: Also check that `PythonRowRangeClause` is pickleable once `QueryBuilder.row_range` is implemented.
    q = QueryBuilder()
    # PythonDateRangeClause
    q = q.date_range((pd.Timestamp("2000-01-04"), pd.Timestamp("2000-01-07")))

    # PythonFilterClause
    q = q[q["col1"].isin(2, 3, 7)]

    # PythonProjectionClause
    q = q.apply("new_col", (q["col1"] * q["col2"]) + 13)

    # PythonGroupByClause
    q = q.groupby("col1")

    # PythonAggregationClause
    q = q.agg({"col2": "sum", "new_col": ("col2", "mean")})

    assert pickle.loads(pickle.dumps(q)) == q

    # PythonResampleClause
    q = QueryBuilder()
    q = q.resample("T", "right", "left")

    assert pickle.loads(pickle.dumps(q)) == q

    q = q.agg({"col2": "sum", "new_col": ("col2", "sum")})

    assert pickle.loads(pickle.dumps(q)) == q
