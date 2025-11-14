"""
Copyright 2024 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

import copy
from functools import partial
import numpy as np
import pandas as pd
import pytest
import pickle
import datetime
import dateutil

from arcticdb import OutputFormat
from arcticdb.version_store.processing import QueryBuilder
from arcticdb.util.test import assert_frame_equal
import arcticdb.toolbox.query_stats as qs

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


def test_querybuilder_getitem_idempotency(lmdb_version_store_v1, any_output_format):
    lib = lmdb_version_store_v1
    lib._set_output_format_for_pipeline_tests(any_output_format)
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


def test_querybuilder_shallow_copy(lmdb_version_store_v1, any_output_format):
    lib = lmdb_version_store_v1
    lib._set_output_format_for_pipeline_tests(any_output_format)
    sym = "test_querybuilder_shallow_copy"
    df = pd.DataFrame({"a": [0, 1]}, index=np.arange(2))
    lib.write(sym, df)
    q = QueryBuilder()
    q = q[q["a"] == 1]
    q_copy = copy.copy(q)
    expected = df[df["a"] == 1]
    assert_frame_equal(expected, lib.read(sym, query_builder=q).data)
    assert_frame_equal(expected, lib.read(sym, query_builder=q_copy).data)


def test_querybuilder_deepcopy(lmdb_version_store_v1, any_output_format):
    lib = lmdb_version_store_v1
    lib._set_output_format_for_pipeline_tests(any_output_format)
    sym = "test_querybuilder_deepcopy"
    df = pd.DataFrame({"a": [0, 1]}, index=np.arange(2))
    lib.write(sym, df)
    q = QueryBuilder()
    q = q[q["a"] == 1]
    q_copy = copy.deepcopy(q)
    expected = df[df["a"] == 1]
    assert_frame_equal(expected, lib.read(sym, query_builder=q).data)
    assert_frame_equal(expected, lib.read(sym, query_builder=q_copy).data)


def test_querybuilder_pickle(lmdb_version_store_v1, any_output_format):
    lib = lmdb_version_store_v1
    lib._set_output_format_for_pipeline_tests(any_output_format)
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


def test_reuse_querybuilder(lmdb_version_store_tiny_segment, any_output_format):
    lib = lmdb_version_store_tiny_segment
    lib._set_output_format_for_pipeline_tests(any_output_format)
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


def test_reuse_querybuilder_date_range(lmdb_version_store_tiny_segment, any_output_format):
    lib = lmdb_version_store_tiny_segment
    lib._set_output_format_for_pipeline_tests(any_output_format)
    symbol = "test_reuse_querybuilder_date_range"
    df = pd.DataFrame({"col1": np.arange(1, 11, dtype=np.int64)}, index=pd.date_range("2000-01-01", periods=10))
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
    received_3 = lib.read(
        symbol, date_range=(pd.Timestamp("2000-01-06"), pd.Timestamp("2000-01-08")), query_builder=q
    ).data
    assert_frame_equal(expected_3, received_3)


def test_reuse_querybuilder_date_range_batch(lmdb_version_store_tiny_segment, any_output_format):
    lib = lmdb_version_store_tiny_segment
    lib._set_output_format_for_pipeline_tests(any_output_format)
    symbol = "test_reuse_querybuilder_date_range_batch"
    df = pd.DataFrame({"col1": np.arange(1, 11, dtype=np.int64)}, index=pd.date_range("2000-01-01", periods=10))
    lib.write(symbol, df)

    q = QueryBuilder()
    q = q[q["col1"].isin(2, 3, 7)]

    expected_0 = df.query("col1 in [2, 3]")
    received_0 = lib.batch_read([symbol], date_ranges=[(None, pd.Timestamp("2000-01-06"))], query_builder=q)[
        symbol
    ].data
    assert_frame_equal(expected_0, received_0)

    received_1 = lib.batch_read([symbol], date_ranges=[(None, pd.Timestamp("2000-01-06"))], query_builder=[q])[
        symbol
    ].data
    assert_frame_equal(expected_0, received_1)

    expected_2 = df.query("col1 in [2, 3, 7]")
    received_2 = lib.read(symbol, query_builder=q).data
    assert_frame_equal(expected_2, received_2)


def test_querybuilder_filter_datetime_with_timezone(lmdb_version_store_tiny_segment, any_output_format):
    lib = lmdb_version_store_tiny_segment
    lib._set_output_format_for_pipeline_tests(any_output_format)
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
    us_time = datetime.datetime(2024, 6, 1, tzinfo=dateutil.tz.gettz("America/New_York"))

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
def test_querybuilder_date_range_then_date_range(
    lmdb_version_store_tiny_segment, batch, use_date_range_clause, any_output_format
):
    lib = lmdb_version_store_tiny_segment
    lib._set_output_format_for_pipeline_tests(any_output_format)
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
def test_querybuilder_date_range_then_row_range(
    lmdb_version_store_tiny_segment, batch, use_date_range_clause, any_output_format
):
    lib = lmdb_version_store_tiny_segment
    lib._set_output_format_for_pipeline_tests(any_output_format)
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
def test_querybuilder_date_range_then_filter(
    lmdb_version_store_tiny_segment, batch, use_date_range_clause, any_output_format
):
    lib = lmdb_version_store_tiny_segment
    lib._set_output_format_for_pipeline_tests(any_output_format)
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


def test_querybuilder_date_range_then_filter_then_resample(lmdb_version_store_tiny_segment, any_output_format):
    # Pandas recommended way to resample and exclude buckets with no index values, which is our behaviour
    # See https://pandas.pydata.org/pandas-docs/stable/user_guide/timeseries.html#sparse-resampling
    def round(t, freq):
        freq = pd.tseries.frequencies.to_offset(freq)
        td = pd.Timedelta(freq)
        return pd.Timestamp((t.value // td.value) * td.value)

    lib = lmdb_version_store_tiny_segment
    lib._set_output_format_for_pipeline_tests(any_output_format)
    symbol = "test_querybuilder_date_range_then_filter_then_resample"
    rng = np.random.default_rng()
    df = pd.DataFrame(
        {"filter_col": rng.integers(0, 2, 100), "agg_col": rng.integers(0, 1000, 100)},
        index=pd.date_range("2000-01-01", periods=100, freq="h"),
    )
    lib.write(symbol, df)

    date_range = (pd.Timestamp("2000-01-02"), pd.Timestamp("2000-01-04"))
    q = QueryBuilder()
    q = q[q["filter_col"] == 0]
    q = q.resample("3h").agg({"agg_col": "sum"})
    received = lib.read(symbol, date_range=date_range, query_builder=q).data
    expected = lib.read(symbol, date_range=date_range, output_format=OutputFormat.PANDAS).data.query("filter_col == 0")
    expected = expected.groupby(partial(round, freq="3h")).agg({"agg_col": "sum"})
    assert_frame_equal(expected, received)


@pytest.mark.parametrize("batch", [True, False])
@pytest.mark.parametrize("use_date_range_clause", [True, False])
def test_querybuilder_date_range_then_project(
    lmdb_version_store_tiny_segment, batch, use_date_range_clause, any_output_format
):
    lib = lmdb_version_store_tiny_segment
    lib._set_output_format_for_pipeline_tests(any_output_format)
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
def test_querybuilder_date_range_then_groupby(
    lmdb_version_store_tiny_segment_dynamic_strings, batch, use_date_range_clause, any_output_format
):
    lib = lmdb_version_store_tiny_segment_dynamic_strings
    lib._set_output_format_for_pipeline_tests(any_output_format)
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


def test_querybuilder_empty_date_range_then_groupby(lmdb_version_store_v1, any_output_format):
    lib = lmdb_version_store_v1
    lib._set_output_format_for_pipeline_tests(any_output_format)
    symbol = "test_querybuilder_empty_date_range_then_groupby"
    df = pd.DataFrame(
        {
            "col1": ["a", "b", "c", "a", "b", "c", "a", "b", "c", "d"],
            "col2": [1, 2, 3, 2, 1, 3, 1, 1, 3, 4],
        },
        index=pd.date_range("2000-01-01", periods=10),
    )
    lib.write(symbol, df)

    date_range = (pd.Timestamp("2000-01-04 12:00:00"), pd.Timestamp("2000-01-04 13:00:00"))

    q = QueryBuilder().date_range(date_range).groupby("col1").agg({"col2": "sum"})

    received = lib.read(symbol, query_builder=q).data
    assert not len(received)
    assert received.index.name == "col1"
    assert len(received.columns) == 1
    assert "col2" in received.columns


@pytest.mark.parametrize("batch", [True, False])
@pytest.mark.parametrize("use_row_range_clause", [True, False])
def test_querybuilder_row_range(lmdb_version_store_tiny_segment, batch, use_row_range_clause, any_output_format):
    lib = lmdb_version_store_tiny_segment
    lib._set_output_format_for_pipeline_tests(any_output_format)
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
def test_querybuilder_row_range_then_date_range(
    lmdb_version_store_tiny_segment, batch, use_row_range_clause, any_output_format
):
    lib = lmdb_version_store_tiny_segment
    lib._set_output_format_for_pipeline_tests(any_output_format)
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
def test_querybuilder_row_range_then_row_range(
    lmdb_version_store_tiny_segment, batch, use_row_range_clause, any_output_format
):
    lib = lmdb_version_store_tiny_segment
    lib._set_output_format_for_pipeline_tests(any_output_format)
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
def test_querybuilder_row_range_then_filter(
    lmdb_version_store_tiny_segment, batch, use_row_range_clause, any_output_format
):
    lib = lmdb_version_store_tiny_segment
    lib._set_output_format_for_pipeline_tests(any_output_format)
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
def test_querybuilder_row_range_then_project(
    lmdb_version_store_tiny_segment, batch, use_row_range_clause, any_output_format
):
    lib = lmdb_version_store_tiny_segment
    lib._set_output_format_for_pipeline_tests(any_output_format)
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
def test_querybuilder_row_range_then_groupby(
    lmdb_version_store_tiny_segment_dynamic_strings, batch, use_row_range_clause, any_output_format
):
    lib = lmdb_version_store_tiny_segment_dynamic_strings
    lib._set_output_format_for_pipeline_tests(any_output_format)
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
def test_querybuilder_row_range_then_resample(
    lmdb_version_store_tiny_segment, batch, use_row_range_clause, any_output_format
):
    lib = lmdb_version_store_tiny_segment
    lib._set_output_format_for_pipeline_tests(any_output_format)
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


def test_querybuilder_filter_then_date_range(lmdb_version_store_tiny_segment, any_output_format):
    lib = lmdb_version_store_tiny_segment
    lib._set_output_format_for_pipeline_tests(any_output_format)
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
def test_querybuilder_filter_then_head(lmdb_version_store_tiny_segment, n, any_output_format):
    lib = lmdb_version_store_tiny_segment
    lib._set_output_format_for_pipeline_tests(any_output_format)
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
def test_querybuilder_filter_then_tail(lmdb_version_store_tiny_segment, n, any_output_format):
    lib = lmdb_version_store_tiny_segment
    lib._set_output_format_for_pipeline_tests(any_output_format)
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


def test_querybuilder_filter_then_row_range(lmdb_version_store_tiny_segment, any_output_format):
    lib = lmdb_version_store_tiny_segment
    lib._set_output_format_for_pipeline_tests(any_output_format)
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


def test_querybuilder_filter_then_filter(lmdb_version_store_tiny_segment, any_output_format):
    lib = lmdb_version_store_tiny_segment
    lib._set_output_format_for_pipeline_tests(any_output_format)
    symbol = "test_querybuilder_filter_then_filter"
    df = pd.DataFrame({"col1": np.arange(10), "col2": np.arange(100, 110)}, index=np.arange(10))
    lib.write(symbol, df)

    q = QueryBuilder()
    q = q[q["col1"].isin(2, 3, 7)]
    q = q[q["col1"].isin(2, 3)]
    received = lib.read(symbol, query_builder=q).data

    expected = df.query("col1 in [2, 3]")
    assert_frame_equal(expected, received)


def test_querybuilder_filter_then_project(lmdb_version_store_tiny_segment, any_output_format):
    lib = lmdb_version_store_tiny_segment
    lib._set_output_format_for_pipeline_tests(any_output_format)
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


def test_querybuilder_filter_then_groupby(lmdb_version_store_tiny_segment_dynamic_strings, any_output_format):
    lib = lmdb_version_store_tiny_segment_dynamic_strings
    lib._set_output_format_for_pipeline_tests(any_output_format)
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


def test_querybuilder_filter_then_resample(lmdb_version_store_tiny_segment, any_output_format):
    lib = lmdb_version_store_tiny_segment
    lib._set_output_format_for_pipeline_tests(any_output_format)
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


def test_querybuilder_project_then_date_range(lmdb_version_store_tiny_segment, any_output_format):
    lib = lmdb_version_store_tiny_segment
    lib._set_output_format_for_pipeline_tests(any_output_format)
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


def test_querybuilder_project_then_row_range(lmdb_version_store_tiny_segment, any_output_format):
    lib = lmdb_version_store_tiny_segment
    lib._set_output_format_for_pipeline_tests(any_output_format)
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


def test_querybuilder_project_then_filter(lmdb_version_store_tiny_segment, any_output_format):
    lib = lmdb_version_store_tiny_segment
    lib._set_output_format_for_pipeline_tests(any_output_format)
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


def test_querybuilder_project_then_project(lmdb_version_store_tiny_segment, any_output_format):
    lib = lmdb_version_store_tiny_segment
    lib._set_output_format_for_pipeline_tests(any_output_format)
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


def test_querybuilder_project_then_groupby(lmdb_version_store_tiny_segment, any_output_format):
    lib = lmdb_version_store_tiny_segment
    lib._set_output_format_for_pipeline_tests(any_output_format)
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


def test_querybuilder_project_then_resample(lmdb_version_store_tiny_segment, any_output_format):
    lib = lmdb_version_store_tiny_segment
    lib._set_output_format_for_pipeline_tests(any_output_format)
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


def test_querybuilder_groupby_then_filter(lmdb_version_store_tiny_segment_dynamic_strings, any_output_format):
    lib = lmdb_version_store_tiny_segment_dynamic_strings
    lib._set_output_format_for_pipeline_tests(any_output_format)
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


def test_querybuilder_groupby_then_project(lmdb_version_store_tiny_segment, any_output_format):
    lib = lmdb_version_store_tiny_segment
    lib._set_output_format_for_pipeline_tests(any_output_format)
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


def test_querybuilder_groupby_then_groupby(lmdb_version_store_tiny_segment, any_output_format):
    lib = lmdb_version_store_tiny_segment
    lib._set_output_format_for_pipeline_tests(any_output_format)
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


def test_querybuilder_resample_then_date_range(lmdb_version_store_tiny_segment, any_output_format):
    lib = lmdb_version_store_tiny_segment
    lib._set_output_format_for_pipeline_tests(any_output_format)
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


def test_querybuilder_resample_then_row_range(lmdb_version_store_tiny_segment, any_output_format):
    lib = lmdb_version_store_tiny_segment
    lib._set_output_format_for_pipeline_tests(any_output_format)
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


def test_querybuilder_resample_then_filter(lmdb_version_store_tiny_segment, any_output_format):
    lib = lmdb_version_store_tiny_segment
    lib._set_output_format_for_pipeline_tests(any_output_format)
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


def test_querybuilder_resample_then_project(lmdb_version_store_tiny_segment, any_output_format):
    lib = lmdb_version_store_tiny_segment
    lib._set_output_format_for_pipeline_tests(any_output_format)
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


def test_querybuilder_resample_then_groupby(lmdb_version_store_tiny_segment, any_output_format):
    lib = lmdb_version_store_tiny_segment
    lib._set_output_format_for_pipeline_tests(any_output_format)
    symbol = "test_querybuilder_resample_then_groupby"
    idx = [0, 1, 1000, 1001, 2000, 2001, 3000, 3001]
    idx = np.array(idx, dtype="datetime64[ns]")
    # After downsampling and summing, grouping_col will be [0, 1, 1, 0]
    df = pd.DataFrame(
        {
            "grouping_col": [0, 0, 10, -9, 20, -19, 30, -30],
            "agg_col": np.arange(8),
        },
        index=idx,
    )
    lib.write(symbol, df)

    q = QueryBuilder()
    q = q.resample("us").agg({"grouping_col": "sum", "agg_col": "sum"})
    q = q.groupby("grouping_col").agg({"agg_col": "sum"})

    received = lib.read(symbol, query_builder=q).data
    received.sort_index(inplace=True)

    expected = df.resample("us").agg({"grouping_col": "sum", "agg_col": "sum"})
    expected = expected.groupby("grouping_col").agg({"agg_col": "sum"})
    assert_frame_equal(expected, received, check_dtype=False)


def test_querybuilder_resample_then_resample(lmdb_version_store_tiny_segment, any_output_format):
    lib = lmdb_version_store_tiny_segment
    lib._set_output_format_for_pipeline_tests(any_output_format)
    symbol = "test_querybuilder_resample_then_resample"
    df = pd.DataFrame(
        {
            "col": np.arange(240),
        },
        index=pd.date_range("2024-01-01", periods=240, freq="min"),
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


def test_query_builder_vwap(lmdb_version_store_v1, any_output_format):
    lib = lmdb_version_store_v1
    lib._set_output_format_for_pipeline_tests(any_output_format)
    symbol = "test_query_builder_vwap"
    rng = np.random.default_rng()
    index = pd.date_range("2024-01-01", "2024-01-03", freq="min")
    df = pd.DataFrame(
        {
            "price": rng.random(len(index)),
            "volume": rng.integers(1, 100, len(index)),
        },
        index=index,
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
    expected = lib.read(symbol, date_range=date_range, output_format=OutputFormat.PANDAS).data
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
    assert (
        str(q)
        == 'WHERE (Column["abc"] GT Num(3)) | WHERE (Column["def"] GT Column["ghi"]) | ROWRANGE: RANGE, start=1, end=10'
    )

    q = QueryBuilder().resample("1min").agg({"col": "sum"})
    assert str(q) == "RESAMPLE(1min) | AGGREGATE {col: (col, sum), }"


@pytest.mark.parametrize("dynamic_schema", [True, False])
def test_column_select_projected_column(s3_store_factory, dynamic_schema, any_output_format):
    lib = s3_store_factory(dynamic_schema=dynamic_schema, column_group_size=2)
    lib._set_output_format_for_pipeline_tests(any_output_format)
    sym = "sym_0"
    lib.write(sym, pd.DataFrame({"a": [1, 2], "b": ["a", "b"], "c": [5, 6]}))
    qb = QueryBuilder()
    qb = qb.apply("new_column", qb["a"] + 2)
    with qs.query_stats():
        result = lib.read(sym, columns=["new_column"], query_builder=qb).data
        stats = qs.get_query_stats()
    qs.reset_stats()
    expected = pd.DataFrame({"new_column": [3, 4]})
    assert_frame_equal(expected, result)
    assert stats["storage_operations"]["S3_GetObject"]["TABLE_DATA"]["count"] == 1


@pytest.mark.parametrize("dynamic_schema", [True, False])
def test_column_select_projected_column_and_filter_it(s3_store_factory, dynamic_schema, any_output_format):
    lib = s3_store_factory(dynamic_schema=dynamic_schema, column_group_size=2)
    lib._set_output_format_for_pipeline_tests(any_output_format)
    sym = "sym_0"
    lib.write(sym, pd.DataFrame({"b": ["a", "b"], "c": [5, 6], "a": [1, 2]}))
    qb = QueryBuilder()
    qb = qb.apply("new_column", qb["a"] + 2)
    qb = qb[qb["new_column"] > 3]
    with qs.query_stats():
        result = lib.read(sym, columns=["new_column"], query_builder=qb).data
        stats = qs.get_query_stats()
    qs.reset_stats()
    expected = pd.DataFrame({"new_column": [4]})
    assert_frame_equal(expected, result)
    assert stats["storage_operations"]["S3_GetObject"]["TABLE_DATA"]["count"] == 1


@pytest.mark.parametrize("dynamic_schema", [True, False])
@pytest.mark.parametrize("column_to_read", ["b", "c"])
def test_filter_synthetic_column_and_select_on_disk_column(
    s3_store_factory, dynamic_schema, column_to_read, any_output_format
):
    lib = s3_store_factory(dynamic_schema=dynamic_schema, column_group_size=2)
    lib._set_output_format_for_pipeline_tests(any_output_format)
    sym = "sym_0"
    df = pd.DataFrame({"a": [1, 2], "b": [7, 8], "c": [5, 6]})
    lib.write(sym, df)
    qb = QueryBuilder()
    qb = qb.apply("new_column", qb["a"] + 2)
    qb = qb[qb["new_column"] > 3]
    with qs.query_stats():
        result = lib.read(sym, columns=[column_to_read], query_builder=qb).data
        stats = qs.get_query_stats()
    qs.reset_stats()
    expected = pd.DataFrame({column_to_read: [df[column_to_read][1]]})
    assert_frame_equal(expected, result)
    if dynamic_schema or column_to_read == "b":
        data_keys_count = 1
    elif column_to_read == "c" and not dynamic_schema:
        # Column c is in the second column slice. This means that we must read the first column slice to perform the
        # filter and then read the second column slice to return the requested column
        data_keys_count = 2
    assert stats["storage_operations"]["S3_GetObject"]["TABLE_DATA"]["count"] == data_keys_count


from arcticdb import Arctic, LibraryOptions
import time


start_date = pd.Timestamp("2005-01-01")
end_date = pd.Timestamp("2025-01-01")
years = pd.date_range(start_date, end_date, freq="YS")
quarters = []
for ts in years:
    year = ts.year
    for q in [1, 2, 3, 4]:
        quarters.append(f"{year} Q{q}")
num_companies = 70_000
companies = [f"company_{i}" for i in range(num_companies)]


def test_write():
    adb = Arctic("lmdb:///tmp/query_performance?map_size=10GB")
    lib = adb.get_library(
        "query_performance", create_if_missing=True, library_options=LibraryOptions(dynamic_schema=True)
    )
    lib._nvs.version_store.clear()
    rng = np.random.default_rng(0)
    index = pd.date_range(start_date, end_date, freq="d")
    data = {"FISCAL_YEAR_PERIOD": rng.choice(quarters, size=len(index))}
    for company in companies:
        data[company] = np.arange(len(index), dtype=np.float64)
    df = pd.DataFrame(data, index=index)
    df.index.name = "DL_SNAPSHOT_DATE"
    lib.write("sym", df)


def test_read_without_querybuilder():
    adb = Arctic("lmdb:///tmp/query_performance?map_size=10GB")
    lib = adb.get_library("query_performance")
    rng = np.random.default_rng(0)
    filter_companies = list(rng.choice(companies, size=500, replace=False))
    filter_quarters = rng.choice(quarters, size=40, replace=False)
    start = time.time()
    df = lib.read(
        "sym",
        date_range=(pd.Timestamp("2010-01-01"), pd.Timestamp("2015-01-01")),
        columns=["FISCAL_YEAR_PERIOD"] + filter_companies,
    ).data
    df = df[df["FISCAL_YEAR_PERIOD"].isin(filter_quarters)]
    end = time.time()
    print(f"Read without QB and filter in Pandas took {end - start:.2f}s")


def test_read_with_querybuilder():
    adb = Arctic("lmdb:///tmp/query_performance?map_size=10GB")
    lib = adb.get_library("query_performance")
    rng = np.random.default_rng(0)
    filter_companies = list(rng.choice(companies, size=500, replace=False))
    filter_quarters = rng.choice(quarters, size=40, replace=False)
    q = QueryBuilder()
    q = q[q["FISCAL_YEAR_PERIOD"].isin(filter_quarters)]
    start = time.time()
    df = lib.read(
        "sym",
        date_range=(pd.Timestamp("2010-01-01"), pd.Timestamp("2015-01-01")),
        columns=["FISCAL_YEAR_PERIOD"] + filter_companies,
        query_builder=q,
    ).data
    end = time.time()
    print(f"Read and filter with QB took {end - start:.2f}s")
