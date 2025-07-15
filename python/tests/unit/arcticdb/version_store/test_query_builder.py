"""
Copyright 2024 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""
import copy
import itertools
from functools import partial

import arcticdb
import numpy as np
import pandas as pd
import pytest
import pickle
import datetime
import dateutil

from arcticdb.version_store.processing import QueryBuilder
from arcticdb.util.test import assert_frame_equal, subset_permutations, powerset

pytestmark = pytest.mark.pipeline


def test_query_builder_equality_checks():
    q1 = QueryBuilder()
    q2 = QueryBuilder()
    q1 = q1[q1["date"] >= pd.Timestamp("2020-01-01")]
    q2 = q2[q2["date"] >= pd.Timestamp("2020-01-01")]
    assert q1 == q2

    q2 = QueryBuilder()
    q2 = q2[q2["date"] >= pd.Timestamp("2021-01-01")]
    assert q1 != q2


def test_querybuilder_getitem_idempotency(lmdb_version_store_v1):
    lib = lmdb_version_store_v1
    sym = "test_querybuilder_getitem_idempotency"
    df = pd.DataFrame({"a": [0, 1]}, index=np.arange(2))
    lib.write(sym, df)
    q = QueryBuilder()
    q_copy = q
    q = q[q["a"] == 1]
    q_copy = q_copy[q_copy["a"] == 0]
    expected = df[df["a"] == 1]
    expected_copy = df[df["a"] == 0]
    assert_frame_equal(expected, lib.read(sym, query_builder=q).data)
    assert_frame_equal(expected_copy, lib.read(sym, query_builder=q_copy).data)


def test_querybuilder_shallow_copy(lmdb_version_store_v1):
    lib = lmdb_version_store_v1
    sym = "test_querybuilder_shallow_copy"
    df = pd.DataFrame({"a": [0, 1]}, index=np.arange(2))
    lib.write(sym, df)
    q = QueryBuilder()
    q = q[q["a"] == 1]
    q_copy = copy.copy(q)
    expected = df[df["a"] == 1]
    assert_frame_equal(expected, lib.read(sym, query_builder=q).data)
    assert_frame_equal(expected, lib.read(sym, query_builder=q_copy).data)


def test_querybuilder_deepcopy(lmdb_version_store_v1):
    lib = lmdb_version_store_v1
    sym = "test_querybuilder_deepcopy"
    df = pd.DataFrame({"a": [0, 1]}, index=np.arange(2))
    lib.write(sym, df)
    q = QueryBuilder()
    q = q[q["a"] == 1]
    q_copy = copy.deepcopy(q)
    expected = df[df["a"] == 1]
    assert_frame_equal(expected, lib.read(sym, query_builder=q).data)
    assert_frame_equal(expected, lib.read(sym, query_builder=q_copy).data)


def test_querybuilder_pickle(lmdb_version_store_v1):
    lib = lmdb_version_store_v1
    sym = "test_querybuilder_pickle"
    df = pd.DataFrame({"a": [0, 1]}, index=np.arange(2))
    lib.write(sym, df)
    q = QueryBuilder()
    q = q[q["a"] == 1]
    q_pickled = pickle.dumps(q)
    expected = df[df["a"] == 1]
    assert_frame_equal(expected, lib.read(sym, query_builder=q).data)
    del q
    q_unpickled = pickle.loads(q_pickled)
    assert_frame_equal(expected, lib.read(sym, query_builder=q_unpickled).data)


def test_querybuilder_pickling_all_clauses():
    """QueryBuilder must be pickleable with all possible clauses."""
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

    # PythonConcatClause
    q = QueryBuilder().concat("INNER")
    assert pickle.loads(pickle.dumps(q)) == q


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


def test_reuse_querybuilder_date_range(lmdb_version_store_tiny_segment):
    lib = lmdb_version_store_tiny_segment
    symbol = "test_reuse_querybuilder_date_range"
    df = pd.DataFrame(
        {"col1": np.arange(1, 11, dtype=np.int64)}, index=pd.date_range("2000-01-01", periods=10)
    )
    lib.write(symbol, df)

    q = QueryBuilder()
    q = q[q["col1"].isin(2, 3, 7)]

    expected_0 = df.query("col1 in [2, 3]")
    received_0 = lib.read(symbol, date_range=(None, pd.Timestamp("2000-01-06")), query_builder=q).data
    assert_frame_equal(expected_0, received_0)

    received_1 = lib.read(symbol, date_range=(None, pd.Timestamp("2000-01-06")), query_builder=q).data
    assert_frame_equal(expected_0, received_1)

    expected_2 = df.query("col1 in [2, 3, 7]")
    received_2 = lib.read(symbol, query_builder=q).data
    assert_frame_equal(expected_2, received_2)

    expected_3 = df.query("col1 in [7]")
    received_3 = lib.read(symbol, date_range=(pd.Timestamp("2000-01-06"), pd.Timestamp("2000-01-08")), query_builder=q).data
    assert_frame_equal(expected_3, received_3)


def test_reuse_querybuilder_date_range_batch(lmdb_version_store_tiny_segment):
    lib = lmdb_version_store_tiny_segment
    symbol = "test_reuse_querybuilder_date_range_batch"
    df = pd.DataFrame(
        {"col1": np.arange(1, 11, dtype=np.int64)}, index=pd.date_range("2000-01-01", periods=10)
    )
    lib.write(symbol, df)

    q = QueryBuilder()
    q = q[q["col1"].isin(2, 3, 7)]

    expected_0 = df.query("col1 in [2, 3]")
    received_0 = lib.batch_read([symbol], date_ranges=[(None, pd.Timestamp("2000-01-06"))], query_builder=q)[symbol].data
    assert_frame_equal(expected_0, received_0)

    received_1 = lib.batch_read([symbol], date_ranges=[(None, pd.Timestamp("2000-01-06"))], query_builder=[q])[symbol].data
    assert_frame_equal(expected_0, received_1)

    expected_2 = df.query("col1 in [2, 3, 7]")
    received_2 = lib.read(symbol, query_builder=q).data
    assert_frame_equal(expected_2, received_2)


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


@pytest.mark.parametrize("batch", [True, False])
@pytest.mark.parametrize("use_date_range_clause", [True, False])
def test_querybuilder_date_range_then_date_range(lmdb_version_store_tiny_segment, batch, use_date_range_clause):
    lib = lmdb_version_store_tiny_segment
    symbol = "test_querybuilder_date_range_then_date_range"
    df = pd.DataFrame({"col": np.arange(1, 11)}, index=pd.date_range("2000-01-01", periods=10))
    lib.write(symbol, df)

    first_date_range = (pd.Timestamp("2000-01-02"), pd.Timestamp("2000-01-09"))
    second_date_range = (pd.Timestamp("2000-01-07"), pd.Timestamp("2000-01-08"))

    q = QueryBuilder()
    if use_date_range_clause:
        q = q.date_range(first_date_range)
    q = q.date_range(second_date_range)

    if use_date_range_clause:
        if batch:
            received = lib.batch_read([symbol], query_builder=q)[symbol].data
        else:
            received = lib.read(symbol, query_builder=q).data
    else:
        if batch:
            received = lib.batch_read([symbol], date_ranges=[first_date_range], query_builder=q)[symbol].data
        else:
            received = lib.read(symbol, date_range=first_date_range, query_builder=q).data
    expected = df.query("col in [7, 8]")
    assert_frame_equal(expected, received)


@pytest.mark.parametrize("batch", [True, False])
@pytest.mark.parametrize("use_date_range_clause", [True, False])
def test_querybuilder_date_range_then_row_range(lmdb_version_store_tiny_segment, batch, use_date_range_clause):
    lib = lmdb_version_store_tiny_segment
    symbol = "test_querybuilder_date_range_then_row_range"
    df = pd.DataFrame({"col": np.arange(1, 11)}, index=pd.date_range("2000-01-01", periods=10))
    lib.write(symbol, df)

    date_range = (pd.Timestamp("2000-01-02"), pd.Timestamp("2000-01-09"))

    q = QueryBuilder()
    if use_date_range_clause:
        q = q.date_range(date_range)
    q = q.row_range((1, 7))

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
    expected = df.iloc[2:8]
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


def test_querybuilder_date_range_then_filter_then_resample(lmdb_version_store_tiny_segment):
    # Pandas recommended way to resample and exclude buckets with no index values, which is our behaviour
    # See https://pandas.pydata.org/pandas-docs/stable/user_guide/timeseries.html#sparse-resampling
    def round(t, freq):
        freq = pd.tseries.frequencies.to_offset(freq)
        td = pd.Timedelta(freq)
        return pd.Timestamp((t.value // td.value) * td.value)

    lib = lmdb_version_store_tiny_segment
    symbol = "test_querybuilder_date_range_then_filter_then_resample"
    rng = np.random.default_rng()
    df = pd.DataFrame(
        {"filter_col": rng.integers(0, 2, 100), "agg_col": rng.integers(0, 1000, 100)},
        index=pd.date_range("2000-01-01", periods=100, freq="h")
    )
    lib.write(symbol, df)

    date_range=(pd.Timestamp("2000-01-02"), pd.Timestamp("2000-01-04"))
    q = QueryBuilder()
    q = q[q["filter_col"] == 0]
    q = q.resample("3h").agg({"agg_col": "sum"})
    received = lib.read(symbol, date_range=date_range, query_builder=q).data
    expected = lib.read(symbol, date_range=date_range).data.query("filter_col == 0")
    expected = expected.groupby(partial(round, freq="3h")).agg({"agg_col": "sum"})
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
        q = q.row_range(row_range)

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
def test_querybuilder_row_range_then_date_range(lmdb_version_store_tiny_segment, batch, use_row_range_clause):
    lib = lmdb_version_store_tiny_segment
    symbol = "test_querybuilder_row_range_then_date_range"
    df = pd.DataFrame({"col": np.arange(1, 11)}, index=pd.date_range("2024-01-01", periods=10))
    lib.write(symbol, df)

    row_range = (3, 7)

    q = QueryBuilder()
    if use_row_range_clause:
        q = q.row_range(row_range)
    q = q.date_range((pd.Timestamp("2024-01-04"), pd.Timestamp("2024-01-06")))

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
    expected = df.query("col in [4, 5, 6]")
    assert_frame_equal(expected, received)


@pytest.mark.parametrize("batch", [True, False])
@pytest.mark.parametrize("use_row_range_clause", [True, False])
def test_querybuilder_row_range_then_row_range(lmdb_version_store_tiny_segment, batch, use_row_range_clause):
    lib = lmdb_version_store_tiny_segment
    symbol = "test_querybuilder_row_range_then_row_range"
    df = pd.DataFrame({"col": np.arange(1, 11)}, index=pd.date_range("2024-01-01", periods=10))
    lib.write(symbol, df)

    first_row_range = (3, 7)
    second_row_range = (1, 3)

    q = QueryBuilder()
    if use_row_range_clause:
        q = q.row_range(first_row_range)
    q = q.row_range(second_row_range)

    if use_row_range_clause:
        if batch:
            received = lib.batch_read([symbol], query_builder=q)[symbol].data
        else:
            received = lib.read(symbol, query_builder=q).data
    else:
        if batch:
            received = lib.batch_read([symbol], row_ranges=[first_row_range], query_builder=q)[symbol].data
        else:
            received = lib.read(symbol, row_range=first_row_range, query_builder=q).data
    expected = df.iloc[4:6]
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
        q = q.row_range(row_range)
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
        q = q.row_range(row_range)
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
        q = q.row_range(row_range)
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


@pytest.mark.parametrize("batch", [True, False])
@pytest.mark.parametrize("use_row_range_clause", [True, False])
def test_querybuilder_row_range_then_resample(lmdb_version_store_tiny_segment, batch, use_row_range_clause):
    lib = lmdb_version_store_tiny_segment
    symbol = "test_querybuilder_row_range_then_resample"
    idx = [0, 1, 2, 1000, 1001, 1002]
    idx = np.array(idx, dtype="datetime64[ns]")
    df = pd.DataFrame({"col": np.arange(6)}, index=idx)
    lib.write(symbol, df)

    row_range = (1, 5)

    q = QueryBuilder()
    if use_row_range_clause:
        q = q.row_range(row_range)
    q = q.resample("us").agg({"col": "sum"})

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
    expected = df.query("col in [1, 2, 3, 4]")
    expected = expected.resample("us").agg({"col": "sum"})
    assert_frame_equal(expected, received, check_dtype=False)


def test_querybuilder_filter_then_date_range(lmdb_version_store_tiny_segment):
    lib = lmdb_version_store_tiny_segment
    symbol = "test_querybuilder_filter_then_date_range"
    df = pd.DataFrame({"col": np.arange(1, 11)}, index=pd.date_range("2024-01-01", periods=10))
    lib.write(symbol, df)
    q = QueryBuilder()
    q = q[q["col"].isin(2, 3, 7)]
    q = q.date_range((pd.Timestamp("2024-01-03"), pd.Timestamp("2024-01-08")))
    received = lib.read(symbol, query_builder=q).data

    expected = df.query("col in [3, 7]")
    assert_frame_equal(expected, received)


@pytest.mark.parametrize("n", range(-7, 8))
def test_querybuilder_filter_then_head(lmdb_version_store_tiny_segment, n):
    lib = lmdb_version_store_tiny_segment
    symbol = "test_querybuilder_filter_then_head"
    df = pd.DataFrame({"col": np.arange(1, 11)}, index=pd.date_range("2024-01-01", periods=10))
    lib.write(symbol, df)
    q = QueryBuilder()
    q = q[q["col"].isin(4, 5, 6, 8, 10)]
    q = q.head(n)
    received = lib.read(symbol, query_builder=q).data

    expected = df.query("col in [4, 5, 6, 8, 10]")
    expected = expected.head(n)
    assert_frame_equal(expected, received)


@pytest.mark.parametrize("n", range(-7, 8))
def test_querybuilder_filter_then_tail(lmdb_version_store_tiny_segment, n):
    lib = lmdb_version_store_tiny_segment
    symbol = "test_querybuilder_filter_then_head"
    df = pd.DataFrame({"col": np.arange(1, 11)}, index=pd.date_range("2024-01-01", periods=10))
    lib.write(symbol, df)
    q = QueryBuilder()
    q = q[q["col"].isin(4, 5, 6, 8, 10)]
    q = q.tail(n)
    received = lib.read(symbol, query_builder=q).data

    expected = df.query("col in [4, 5, 6, 8, 10]")
    expected = expected.tail(n)
    assert_frame_equal(expected, received)


def test_querybuilder_filter_then_row_range(lmdb_version_store_tiny_segment):
    lib = lmdb_version_store_tiny_segment
    symbol = "test_querybuilder_filter_then_row_range"
    df = pd.DataFrame({"col": np.arange(1, 11)}, index=pd.date_range("2024-01-01", periods=10))
    lib.write(symbol, df)
    q = QueryBuilder()
    q = q[q["col"].isin(4, 5, 6, 8, 10)]
    q = q.row_range((1, 4))
    received = lib.read(symbol, query_builder=q).data

    expected = df.query("col in [4, 5, 6, 8, 10]")
    expected = expected.iloc[1:4]
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


def test_querybuilder_filter_then_resample(lmdb_version_store_tiny_segment):
    lib = lmdb_version_store_tiny_segment
    symbol = "test_querybuilder_filter_then_resample"
    idx = [0, 1, 2, 1000, 1001, 1002]
    idx = np.array(idx, dtype="datetime64[ns]")
    df = pd.DataFrame({"col": np.arange(6)}, index=idx)
    lib.write(symbol, df)

    q = QueryBuilder()
    q = q[(q["col"] != 1) & (q["col"] != 5)]
    q = q.resample("us").agg({"col": "sum"})

    received = lib.read(symbol, query_builder=q).data

    expected = df.query("(col != 1) & (col != 5)")
    expected = expected.resample("us").agg({"col": "sum"})
    assert_frame_equal(expected, received, check_dtype=False)


def test_querybuilder_project_then_date_range(lmdb_version_store_tiny_segment):
    lib = lmdb_version_store_tiny_segment
    symbol = "test_querybuilder_project_then_date_range"
    df = pd.DataFrame({"col": np.arange(1, 11)}, index=pd.date_range("2024-01-01", periods=10))
    lib.write(symbol, df)
    q = QueryBuilder()
    q = q.apply("new_col", q["col"] * 3)
    q = q.date_range((pd.Timestamp("2024-01-03"), pd.Timestamp("2024-01-08")))
    received = lib.read(symbol, query_builder=q).data

    expected = df
    expected["new_col"] = expected["col"] * 3
    expected = expected.query("col in [3, 4, 5, 6, 7, 8]")
    assert_frame_equal(expected, received, check_dtype=False)


def test_querybuilder_project_then_row_range(lmdb_version_store_tiny_segment):
    lib = lmdb_version_store_tiny_segment
    symbol = "test_querybuilder_project_then_row_range"
    df = pd.DataFrame({"col": np.arange(1, 11)}, index=pd.date_range("2024-01-01", periods=10))
    lib.write(symbol, df)
    q = QueryBuilder()
    q = q.apply("new_col", q["col"] * 3)
    q = q.row_range((3, 9))
    received = lib.read(symbol, query_builder=q).data

    expected = df
    expected["new_col"] = expected["col"] * 3
    expected = expected.iloc[3:9]
    assert_frame_equal(expected, received, check_dtype=False)


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


def test_querybuilder_project_then_resample(lmdb_version_store_tiny_segment):
    lib = lmdb_version_store_tiny_segment
    symbol = "test_querybuilder_project_then_resample"
    idx = [0, 1, 2, 1000, 1001, 1002]
    idx = np.array(idx, dtype="datetime64[ns]")
    df = pd.DataFrame({"col": np.arange(6)}, index=idx)
    lib.write(symbol, df)

    q = QueryBuilder()
    q = q.apply("new_col", q["col"] * 3)
    q = q.resample("us").agg({"new_col": "sum"})

    received = lib.read(symbol, query_builder=q).data

    expected = df
    expected["new_col"] = expected["col"] * 3
    expected = expected.resample("us").agg({"new_col": "sum"})
    assert_frame_equal(expected, received, check_dtype=False)


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


def test_querybuilder_resample_then_date_range(lmdb_version_store_tiny_segment):
    lib = lmdb_version_store_tiny_segment
    symbol = "test_querybuilder_resample_then_date_range"
    df = pd.DataFrame({"col": np.arange(30)}, index=pd.date_range("1970-01-01", periods=30, freq="D"))
    lib.write(symbol, df)

    q = QueryBuilder()
    q = q.resample("2D").agg({"col": "sum"})
    q = q.date_range((pd.Timestamp("1970-01-03"), pd.Timestamp("1970-01-27")))

    received = lib.read(symbol, query_builder=q).data

    expected = df.resample("2D").agg({"col": "sum"})
    expected = expected.iloc[1:-1]
    assert_frame_equal(expected, received, check_dtype=False)


def test_querybuilder_resample_then_row_range(lmdb_version_store_tiny_segment):
    lib = lmdb_version_store_tiny_segment
    symbol = "test_querybuilder_resample_then_row_range"
    df = pd.DataFrame({"col": np.arange(30)}, index=pd.date_range("1970-01-01", periods=30, freq="D"))
    lib.write(symbol, df)

    q = QueryBuilder()
    q = q.resample("2D").agg({"col": "sum"})
    q = q.row_range((5, 8))

    received = lib.read(symbol, query_builder=q).data

    expected = df.resample("2D").agg({"col": "sum"})
    expected = expected.iloc[5:8]
    assert_frame_equal(expected, received, check_dtype=False)


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


def test_querybuilder_resample_then_resample(lmdb_version_store_tiny_segment):
    lib = lmdb_version_store_tiny_segment
    symbol = "test_querybuilder_resample_then_resample"
    df = pd.DataFrame(
        {
            "col": np.arange(240),
        },
        index=pd.date_range("2024-01-01", periods=240, freq="min")
    )
    lib.write(symbol, df)
    q = QueryBuilder()
    q = q.resample("h").agg({"new_col": ("col", "mean")})
    q = q.resample("2h").agg({"new_col": "mean"})
    received = lib.read(symbol, query_builder=q).data
    # Pandas 1.X needs None as the first argument to agg with named aggregators
    expected = df.resample("h").agg(None, new_col=pd.NamedAgg("col", "mean"))
    expected = expected.resample("2h").agg({"new_col": "mean"})
    assert_frame_equal(expected, received, check_dtype=False)


def test_query_builder_vwap(lmdb_version_store_v1):
    lib = lmdb_version_store_v1
    symbol = "test_query_builder_vwap"
    rng = np.random.default_rng()
    index = pd.date_range("2024-01-01", "2024-01-03", freq="min")
    df = pd.DataFrame(
        {
            "price": rng.random(len(index)),
            "volume": rng.integers(1, 100, len(index)),
        },
        index=index
    )
    lib.write(symbol, df)

    date_range = (pd.Timestamp("2024-01-01T12:00:00"), pd.Timestamp("2024-01-02T12:00:00"))
    freq = "h"
    aggs = {"volume": "sum", "product": "sum"}
    q = QueryBuilder()
    q["product"] = q["price"] * q["volume"]
    q = q.resample(freq).agg(aggs)
    q["vwap"] = q["product"] / q["volume"]
    received = lib.read(symbol, date_range=date_range, query_builder=q).data
    expected = lib.read(symbol, date_range=date_range).data
    expected["product"] = expected["price"] * expected["volume"]
    expected = expected.resample(freq).agg(aggs)
    expected["vwap"] = expected["product"] / expected["volume"]
    expected.sort_index(inplace=True, axis=1)
    received.sort_index(inplace=True, axis=1)
    assert_frame_equal(expected, received, check_dtype=False)


def test_to_strings():
    q = QueryBuilder().row_range((1, 10))
    assert str(q) == "ROWRANGE: RANGE, start=1, end=10"

    q = QueryBuilder().head(10)
    assert str(q) == "ROWRANGE: HEAD, n=10"

    q = QueryBuilder().tail(9)
    assert str(q) == "ROWRANGE: TAIL, n=9"

    q = QueryBuilder().date_range((pd.Timestamp(1000), pd.Timestamp(2000)))
    assert str(q) == "DATE RANGE 1000 - 2000"

    q = QueryBuilder().date_range((None, pd.Timestamp(2000)))
    assert str(q) == f"DATE RANGE {pd.Timestamp.min.value} - 2000"

    q = QueryBuilder().date_range((pd.Timestamp(1000), None))
    assert str(q) == f"DATE RANGE 1000 - {pd.Timestamp.max.value}"

    q = QueryBuilder()
    q["def"] = 2 * q["abc"]
    assert str(q) == 'PROJECT Column["def"] = (Num(2) MUL Column["abc"])'

    q = QueryBuilder()
    q = q[q["abc"] > 3]
    assert str(q) == 'WHERE (Column["abc"] GT Num(3))'

    q = QueryBuilder()
    q = q[q["abc"] > 3]
    q = q[q["def"] > q["ghi"]]
    q.row_range((1, 10))
    assert str(q) == 'WHERE (Column["abc"] GT Num(3)) | WHERE (Column["def"] GT Column["ghi"]) | ROWRANGE: RANGE, start=1, end=10'

    q = QueryBuilder().resample('1min').agg({"col": "sum"})
    assert str(q) == 'RESAMPLE(1min) | AGGREGATE {col: (col, sum), }'

def input_segments_for_column_selection_by_schema(dynamic_schema, column_names):
    """
    Generate a list of tuples, both elements are dataframes that are going to be appended. The columns in the dataframes
    are generated so that they are all permutations of all subsets of column_names. For static schema both segments have
    the same columns and we don't do permutations.
    """
    column_values_seg_1 = {col: np.array([i], np.dtype("float64")) for (i, col) in enumerate(column_names)}
    column_values_seg_2 = {col: np.array([len(column_names) - i - 1], np.dtype("float64")) for (i, col) in enumerate(column_names)}
    if dynamic_schema:
        return ((pd.DataFrame({col: column_values_seg_1[col] for col in seg1}), pd.DataFrame({col: column_values_seg_2[col] for col in seg2})) for seg1 in subset_permutations(column_names) for seg2 in subset_permutations(column_names))
    else:
        return ((pd.DataFrame({col: column_values_seg_1[col] for col in seg1}), pd.DataFrame({col: column_values_seg_2[col] for col in seg1})) for seg1 in powerset(column_names))

def input_segments_for_processing(column_names):
    dynamic_schema_segments = ((True, segment_pair) for segment_pair in input_segments_for_column_selection_by_schema(True, column_names))
    static_schema_segments = ((False, segment_pair) for segment_pair in input_segments_for_column_selection_by_schema(False, column_names))
    return itertools.chain(dynamic_schema_segments, static_schema_segments)

def input_segments_for_concat(column_names):
    sym_1_dynamic = input_segments_for_column_selection_by_schema(dynamic_schema=True, column_names=column_names)
    sym_2_dynamic = input_segments_for_column_selection_by_schema(dynamic_schema=True, column_names=column_names)
    sym_1_2_dynamic = ((True, (sym_1_segments, sym_2_segments)) for sym_1_segments in sym_1_dynamic for sym_2_segments in sym_2_dynamic)

    sym_1_static = input_segments_for_column_selection_by_schema(dynamic_schema=False, column_names=column_names)
    sym_2_static = input_segments_for_column_selection_by_schema(dynamic_schema=False, column_names=column_names)
    sym_1_2_static = ((False, (sym_1_segments, sym_2_segments)) for sym_1_segments in sym_1_static for sym_2_segments in sym_2_static)
    return itertools.chain(sym_1_2_dynamic, sym_1_2_static)

class TestColumnSelection:

    @pytest.mark.parametrize("dynamic_schema, input_segments", input_segments_for_processing(["a", "b", "c"]))
    def test_filter(self, version_store_factory, dynamic_schema, input_segments):
        """
        Test filtering by each column on disk and then try all possible combinations of column selection. The output
        must contain only the columns in the columns parameter of read even if the column that's being filtered is not
        in that list. The filtering must be applied even if the column that's being filtered is in the columns list.
        """
        lib = version_store_factory(dynamic_schema=dynamic_schema, col_per_group=2)
        df_seg_1, df_seg_2 = input_segments
        lib.write("sym", df_seg_1)
        lib.append("sym", df_seg_2)
        pandas_df = pd.concat([df_seg_1, df_seg_2])
        columns_in_df = set(list(df_seg_1.columns) + list(df_seg_2.columns))
        for column_to_filter in columns_in_df:
            for columns_to_select in subset_permutations(columns_in_df):
                q = QueryBuilder()
                q = q[q[column_to_filter] >= 2.0]
                result = lib.read("sym", query_builder=q, columns=columns_to_select).data
                result.sort_index(axis=1, inplace=True)
                expected = pandas_df[pandas_df[column_to_filter] >= 2][list(columns_to_select)]
                expected.sort_index(axis=1, inplace=True)
                assert set(columns_to_select) == set(result.columns)
                assert_frame_equal(expected, result)

    @pytest.mark.parametrize("dynamic_schema, input_segments", input_segments_for_processing(["a", "b", "c"]))
    def test_project(self, version_store_factory, dynamic_schema, input_segments):
        """
        Test projection each column on disk onto a new_column that's the value of the column + 2. Then try selecting all
        possible combinations of selection columns.
        """
        lib = version_store_factory(dynamic_schema=dynamic_schema, col_per_group=2)
        df_seg_1, df_seg_2 = input_segments
        lib.write("sym", df_seg_1)
        lib.append("sym", df_seg_2)
        pandas_df = pd.concat([df_seg_1, df_seg_2])
        pandas_df.index = pd.RangeIndex(start=0, stop=2)
        columns_in_df = set(list(df_seg_1.columns) + list(df_seg_2.columns))
        for column_to_project in columns_in_df:
            columns_in_output = columns_in_df | {"new_column"}
            for columns_to_select in subset_permutations(columns_in_output):
                q = QueryBuilder()
                q = q.apply("new_column", q[column_to_project] + 2)
                result = lib.read("sym", query_builder=q, columns=columns_to_select).data
                result.sort_index(axis=1, inplace=True)
                expected = pandas_df.copy(True)
                expected["new_column"] = expected[column_to_project] + 2
                expected = expected[list(columns_to_select)]
                expected.sort_index(axis=1, inplace=True)
                if "new_column" in columns_to_select:
                    # This is a known bug. See Monday issue: 9492480789
                    buggy = expected.copy(True)
                    buggy.drop(["new_column"], axis=1, inplace=True)
                    if len(buggy.columns) == 0:
                        buggy = pd.DataFrame()
                    assert_frame_equal(result, buggy)
                else:
                    assert set(columns_to_select) == set(result.columns)
                    assert_frame_equal(expected, result)

    @pytest.mark.parametrize("dynamic_schema, input_segments", input_segments_for_processing(["a", "b", "c"]))
    def test_filter_projected(self, version_store_factory, dynamic_schema, input_segments):
        """
        Test projection each column on disk onto a new_column that's the value of the column + 2 and then filtering on
        the value of that new column. Then try selecting all possible combinations of selection columns.
        """
        lib = version_store_factory(dynamic_schema=dynamic_schema, col_per_group=2)
        df_seg_1, df_seg_2 = input_segments
        lib.write("sym", df_seg_1)
        lib.append("sym", df_seg_2)
        pandas_df = pd.concat([df_seg_1, df_seg_2])
        columns_in_df = set(list(df_seg_1.columns) + list(df_seg_2.columns))
        for column_to_project in columns_in_df:
            columns_in_output = columns_in_df | {"new_column"}
            for columns_to_select in subset_permutations(columns_in_output):
                q = QueryBuilder()
                q = q.apply("new_column", q[column_to_project] + 2)
                q = q[q["new_column"] >= 3.0]
                result = lib.read("sym", query_builder=q, columns=columns_to_select).data
                result.sort_index(axis=1, inplace=True)
                expected = pandas_df.copy(True)
                expected["new_column"] = expected[column_to_project] + 2
                expected = expected[expected["new_column"] >= 3.0][list(columns_to_select)]
                expected.sort_index(axis=1, inplace=True)
                expected.index = pd.RangeIndex(start=0, stop=len(expected.index))
                if "new_column" in columns_to_select:
                    # This is a known bug. See Monday issue: 9492480789
                    buggy = expected.copy(True)
                    buggy.drop(["new_column"], axis=1, inplace=True)
                    if len(buggy.columns) == 0:
                        buggy = pd.DataFrame()
                    assert_frame_equal(result, buggy)
                else:
                    assert set(columns_to_select) == set(result.columns)
                    assert_frame_equal(expected, result)

    @pytest.mark.xfail(reason="Column selection not working properly. Check also tests: test_symbol_concat_column_slicing and test_symbol_concat_filtering_with_column_selection")
    @pytest.mark.parametrize("dynamic_schema, input_symbols", input_segments_for_concat(["a", "b", "c"]))
    @pytest.mark.parametrize("join", ["inner"])
    def test_concat(self, lmdb_library_factory, dynamic_schema, input_symbols, join):
        lib = lmdb_library_factory(library_options=arcticdb.LibraryOptions(dynamic_schema=dynamic_schema, columns_per_segment=2))
        symbol_names = ["sym_0", "sym_1"]
        input_dfs = []
        for (index, symbol_name) in enumerate(symbol_names):
            for segment in input_symbols[index]:
                lib.append(symbol_name, segment)
            input_dfs.append(pd.concat(input_symbols[index]))

        for columns_to_select_0 in subset_permutations(set(list(input_dfs[0]))):
            for columns_to_select_1 in subset_permutations(set(list(input_dfs[1]))):
                lazy_df_0 = lib.read("sym_0", columns=columns_to_select_0, lazy=True)
                lazy_df_1 = lib.read("sym_1", columns=columns_to_select_1, lazy=True)

                received = arcticdb.concat([lazy_df_0, lazy_df_1], join).collect().data
                received.sort_index(axis=1, inplace=True)

                expected = pd.concat([input_dfs[0].loc[:, columns_to_select_0], input_dfs[0].loc[:, columns_to_select_1]])
                expected.index = pd.RangeIndex(len(expected))

                assert_frame_equal(expected, received)
