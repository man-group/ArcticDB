"""
Copyright 2024 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""
from functools import partial
from typing import Union

import numpy as np
import pandas as pd
import datetime as dt
import pytest

from arcticdb import QueryBuilder
from arcticdb.exceptions import ArcticDbNotYetImplemented, SchemaException, UserInputException
from arcticdb.util.test import assert_frame_equal, generic_resample_test
from packaging.version import Version
from arcticdb.util._versions import IS_PANDAS_TWO, PANDAS_VERSION
import itertools
from pandas.api.types import is_float_dtype

pytestmark = pytest.mark.pipeline


ALL_AGGREGATIONS = ["sum", "mean", "min", "max", "first", "last", "count"]
DATETIME_AGGREGATIONS = ["mean", "min", "max", "first", "last", "count"]

def default_aggregation_value(aggregation: str, dtype: Union[np.dtype, str]):
    assert aggregation in ALL_AGGREGATIONS
    if is_float_dtype(dtype):
        return 0 if aggregation == "count" else np.nan
    elif np.issubdtype(dtype, np.integer):
        return np.nan if aggregation == "mean" else 0
    elif np.issubdtype(dtype, np.datetime64):
        return 0 if aggregation == "count" else pd.NaT
    elif np.issubdtype(dtype, np.str_):
        pass
    elif pd.api.types.is_bool_dtype(dtype):
        return np.nan if aggregation == "mean" else 0
    else:
        raise "Unknown dtype"

def all_aggregations_dict(col):
    return {f"to_{agg}": (col, agg) for agg in ALL_AGGREGATIONS}

def datetime_aggregations_dict(col):
    return {f"to_{agg}": (col, agg) for agg in DATETIME_AGGREGATIONS}

# Pandas recommended way to resample and exclude buckets with no index values, which is our behaviour
# See https://pandas.pydata.org/pandas-docs/stable/user_guide/timeseries.html#sparse-resampling
def round(t, freq):
    freq = pd.tseries.frequencies.to_offset(freq)
    td = pd.Timedelta(freq)
    return pd.Timestamp((t.value // td.value) * td.value)

def generic_resample_test_with_empty_buckets(lib, sym, rule, aggregations, date_range=None):
    """
    Perform a resampling in ArcticDB and compare it against the same query in Pandas.

    This will remove all empty buckets mirroring ArcticDB's behavior. It cannot take additional parameters such as
    orign and offset. In case such parameters are needed arcticdb.util.test.generic_resample_test can be used.

    This can drop buckets even all columns are of float type while generic_resample_test needs at least one non-float
    column.
    """
    # Pandas doesn't have a good date_range equivalent in resample, so just use read for that
    expected = lib.read(sym, date_range=date_range).data
    # Pandas 1.X needs None as the first argument to agg with named aggregators
    expected = expected.groupby(partial(round, freq=rule)).agg(None, **aggregations)
    expected = expected.reindex(columns=sorted(expected.columns))
    q = QueryBuilder()
    q = q.resample(rule).agg(aggregations)
    received = lib.read(sym, date_range=date_range, query_builder=q).data
    received = received.reindex(columns=sorted(received.columns))

    assert_frame_equal(expected, received, check_dtype=False)


@pytest.mark.parametrize("freq", ("min", "h", "D", "1h30min"))
@pytest.mark.parametrize("date_range", (None, (pd.Timestamp("2024-01-02T12:00:00"), pd.Timestamp("2024-01-03T12:00:00"))))
@pytest.mark.parametrize("closed", ("left", "right"))
@pytest.mark.parametrize("label", ("left", "right"))
def test_resampling(lmdb_version_store_v1, freq, date_range, closed, label):
    lib = lmdb_version_store_v1
    sym = "test_resampling"
    # Want an index with data every minute for 2 days, with additional data points 1 nanosecond before and after each
    # minute to catch off-by-one errors
    idx_start_base = pd.Timestamp("2024-01-02")
    idx_end_base = pd.Timestamp("2024-01-04")

    idx = pd.date_range(idx_start_base, idx_end_base, freq="min")
    idx_1_nano_before = pd.date_range(idx_start_base - pd.Timedelta(1), idx_end_base - pd.Timedelta(1), freq="min")
    idx_1_nano_after = pd.date_range(idx_start_base + pd.Timedelta(1), idx_end_base + pd.Timedelta(1), freq="min")
    idx = idx.join(idx_1_nano_before, how="outer").join(idx_1_nano_after, how="outer")
    rng = np.random.default_rng()
    df = pd.DataFrame({"col": rng.integers(0, 100, len(idx))}, index=idx)
    lib.write(sym, df)

    generic_resample_test(
        lib,
        sym,
        freq,
        {
            "sum": ("col", "sum"),
            "min": ("col", "min"),
            "max": ("col", "max"),
            "mean": ("col", "mean"),
            "count": ("col", "count"),
            "first": ("col", "first"),
            "last": ("col", "last"),
        },
        date_range=date_range,
        closed=closed,
        label=label
    )


@pytest.mark.parametrize("closed", ("left", "right"))
def test_resampling_duplicated_index_value_on_segment_boundary(lmdb_version_store_v1, closed):
    lib = lmdb_version_store_v1
    sym = "test_resampling_duplicated_index_value_on_segment_boundary"
    # Will group on microseconds
    df_0 = pd.DataFrame({"col": np.arange(4)}, index=np.array([0, 1, 2, 1000], dtype="datetime64[ns]"))
    df_1 = pd.DataFrame({"col": np.arange(4, 8)}, index=np.array([1000, 1000, 1000, 1000], dtype="datetime64[ns]"))
    df_2 = pd.DataFrame({"col": np.arange(8, 12)}, index=np.array([1000, 1001, 2000, 2001], dtype="datetime64[ns]"))
    lib.write(sym, df_0)
    lib.append(sym, df_1)
    lib.append(sym, df_2)

    generic_resample_test(
        lib,
        sym,
        "us",
        {"sum": ("col", "sum")},
        closed=closed,
    )


class TestResamplingBucketInsideSegment:

    def test_all_buckets_have_values(self, lmdb_version_store_v1):
        lib = lmdb_version_store_v1
        sym = "test_inner_buckets_are_empty"
        start = dt.datetime(2023, 12, 7, 23, 59, 47, 500000);
        idx = [start + i * pd.Timedelta('1s') for i in range(0, 8)]
        df = pd.DataFrame({"mid": [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0]}, index=idx)
        lib.write(sym, df)
    
        date_range = (dt.datetime(2023, 12, 7, 23, 59, 48), dt.datetime(2023, 12, 7, 23, 59, 52))
        generic_resample_test_with_empty_buckets(lib, sym, 's', {'high': ('mid', 'max')}, date_range=date_range)

    @pytest.mark.parametrize("closed", ("left", "right"))
    def test_first_bucket_is_empy(self, lmdb_version_store_v1, closed):
        lib = lmdb_version_store_v1
        sym = "test_first_bucket_is_empy"
        idx = pd.DatetimeIndex([
            dt.datetime(2023, 12, 7, 23, 59, 48, 342000),
            dt.datetime(2023, 12, 7, 23, 59, 49, 717000),
            dt.datetime(2023, 12, 7, 23, 59, 49, 921000),
            dt.datetime(2023, 12, 7, 23, 59, 50, 75000),
            dt.datetime(2023, 12, 7, 23, 59, 50, 76000),
            dt.datetime(2023, 12, 7, 23, 59, 55, 75000)
        ])
        df = pd.DataFrame({"mid": [1, 2, 3, 4, 5, 6]}, index=idx)
        lib.write(sym, df)
    
        date_range = (dt.datetime(2023, 12, 7, 23, 59, 49), dt.datetime(2023, 12, 7, 23, 59, 50))
        generic_resample_test(lib, sym, 's', {'high': ('mid', 'max')}, date_range=date_range, closed=closed)

    @pytest.mark.parametrize("closed", ("left", "right"))
    def test_last_bucket_is_empty(self, lmdb_version_store_v1, closed):
        lib = lmdb_version_store_v1
        sym = "test_last_bucket_is_empty"
        idx = pd.DatetimeIndex([
            dt.datetime(2023, 12, 7, 23, 59, 47, 342000),
            dt.datetime(2023, 12, 7, 23, 59, 48, 342000),
            dt.datetime(2023, 12, 7, 23, 59, 49, 717000),
            dt.datetime(2023, 12, 7, 23, 59, 49, 921000),
            dt.datetime(2023, 12, 7, 23, 59, 50, 75000),
            dt.datetime(2023, 12, 7, 23, 59, 50, 76000),
            dt.datetime(2023, 12, 7, 23, 59, 55, 75000)
        ])
        df = pd.DataFrame({"mid": [1, 2, 3, 4, 5, 6, 7]}, index=idx)
        lib.write(sym, df)
    
        date_range = (dt.datetime(2023, 12, 7, 23, 59, 48), dt.datetime(2023, 12, 7, 23, 59, 49, 500000))
        generic_resample_test(lib, sym, 's', {'high': ('mid', 'max')}, date_range=date_range, closed=closed)
    
    def test_inner_buckets_are_empty(self, lmdb_version_store_v1):
        lib = lmdb_version_store_v1
        sym = "test_inner_buckets_are_empty"
        idx = pd.DatetimeIndex([
            dt.datetime(2023, 12, 7, 23, 59, 48, 342000),
            dt.datetime(2023, 12, 7, 23, 59, 49, 717000),
            dt.datetime(2023, 12, 7, 23, 59, 49, 921000),
            dt.datetime(2023, 12, 7, 23, 59, 52, 75000),
            dt.datetime(2023, 12, 7, 23, 59, 53, 76000),
            dt.datetime(2023, 12, 7, 23, 59, 55, 75000)
        ])
        df = pd.DataFrame({"mid": [1.0, 2.0, 3.0, 4.0, 5.0, 6.0]}, index=idx)
        lib.write(sym, df)
    
        date_range = (dt.datetime(2023, 12, 7, 23, 59, 48), dt.datetime(2023, 12, 7, 23, 59, 55))
        generic_resample_test_with_empty_buckets(lib, sym, 's', {'high': ('mid', 'max')}, date_range=date_range)
        


def test_resampling_timezones(lmdb_version_store_v1):
    lib = lmdb_version_store_v1
    sym = "test_resampling_timezones"
    # UK clocks go forward at 1am on March 31st in 2024
    index = pd.date_range("2024-03-31T00:00:00", freq="min", periods=240, tz="Europe/London")
    df = pd.DataFrame({"col": np.arange(len(index))}, index=index)
    lib.write(sym, df)
    generic_resample_test(
        lib,
        sym,
        "h",
        {"sum": ("col", "sum")},
    )

    # UK clocks go back at 2am on October 27th in 2024
    index = pd.date_range("2024-10-27T00:00:00", freq="min", periods=240, tz="Europe/London")
    df = pd.DataFrame({"col": np.arange(len(index))}, index=index)
    lib.write(sym, df)
    generic_resample_test(
        lib,
        sym,
        "h",
        {"sum": ("col", "sum")},
    )


def test_resampling_nan_correctness(version_store_factory):
    lib = version_store_factory(
        column_group_size=2,
        segment_row_size=2,
        dynamic_strings=True,
        lmdb_config={"map_size": 2**30}
    )
    sym = "test_resampling_nan_correctness"
    # NaN here means NaT for datetime columns and NaN/None in string columns
    # Create 5 buckets worth of data, each containing 3 values:
    # - No nans
    # - All nans
    # - First value nan
    # - Middle value nan
    # - Last value nan
    # Will group on microseconds
    idx = [0, 1, 2, 1000, 1001, 1002, 2000, 2001, 2002, 3000, 3001, 3002, 4000, 4001, 4002]
    idx = np.array(idx, dtype="datetime64[ns]")
    float_col = np.arange(15, dtype=np.float64)
    string_col = [f"str {str(i)}" for i in range(15)]
    datetime_col = np.array(np.arange(0, 30, 2), dtype="datetime64[ns]")
    for i in [3, 4, 5, 6, 10, 14]:
        float_col[i] = np.nan
        string_col[i] = None if i % 2 == 0 else np.nan
        datetime_col[i] = np.datetime64('NaT')

    df = pd.DataFrame({"float_col": float_col, "string_col": string_col, "datetime_col": datetime_col}, index=idx)
    lib.write(sym, df)

    agg_dict = {
        "float_sum": ("float_col", "sum"),
        "float_mean": ("float_col", "mean"),
        "float_min": ("float_col", "min"),
        "float_max": ("float_col", "max"),
        "float_first": ("float_col", "first"),
        "float_last": ("float_col", "last"),
        "float_count": ("float_col", "count"),
    }

    # Pandas 1.X does not support all of these aggregators, or behaves in a less intuitive way than Pandas 2.X
    if IS_PANDAS_TWO:
        agg_dict.update(
            {
                "string_first": ("string_col", "first"),
                "string_last": ("string_col", "last"),
                "string_count": ("string_col", "count"),
                "datetime_mean": ("datetime_col", "mean"),
                "datetime_min": ("datetime_col", "min"),
                "datetime_max": ("datetime_col", "max"),
                "datetime_first": ("datetime_col", "first"),
                "datetime_last": ("datetime_col", "last"),
                "datetime_count": ("datetime_col", "count"),
            }
        )

    generic_resample_test(lib, sym, "us", agg_dict)


def test_resampling_bool_columns(lmdb_version_store_tiny_segment):
    lib = lmdb_version_store_tiny_segment
    sym = "test_resampling_bool_columns"

    idx = [0, 1, 1000, 1001, 2000, 2001, 3000, 3001]
    idx = np.array(idx, dtype="datetime64[ns]")

    col = [True, True, True, False, False, True, False, False]

    df = pd.DataFrame({"col": col}, index=idx)
    lib.write(sym, df)

    generic_resample_test(
        lib,
        sym,
        "us",
        {
            "sum": ("col", "sum"),
            "mean": ("col", "mean"),
            "min": ("col", "min"),
            "max": ("col", "max"),
            "first": ("col", "first"),
            "last": ("col", "last"),
            "count": ("col", "count"),
        },
    )


def test_resampling_dynamic_schema_types_changing(lmdb_version_store_dynamic_schema_v1):
    lib = lmdb_version_store_dynamic_schema_v1
    sym = "test_resampling_dynamic_schema_types_changing"
    # Will group on microseconds
    idx_0 = [0, 1, 2, 1000]
    idx_0 = np.array(idx_0, dtype="datetime64[ns]")
    col_0 = np.arange(4, dtype=np.uint8)
    df_0 = pd.DataFrame({"col": col_0}, index=idx_0)
    lib.write(sym, df_0)

    idx_1 = [1001, 1002, 2000, 2001]
    idx_1 = np.array(idx_1, dtype="datetime64[ns]")
    col_1 = np.arange(1000, 1004, dtype=np.int64)
    df_1 = pd.DataFrame({"col": col_1}, index=idx_1)
    lib.append(sym, df_1)

    generic_resample_test(
        lib,
        sym,
        "us",
        {
            "sum": ("col", "sum"),
            "mean": ("col", "mean"),
            "min": ("col", "min"),
            "max": ("col", "max"),
            "first": ("col", "first"),
            "last": ("col", "last"),
            "count": ("col", "count"),
        },
    )


def test_resampling_empty_bucket_in_range(lmdb_version_store_v1):
    lib = lmdb_version_store_v1
    sym = "test_resampling_empty_bucket_in_range"
    # Group on microseconds, so bucket 1000-1999 will be empty
    idx = [0, 1, 2000, 2001]
    idx = np.array(idx, dtype="datetime64[ns]")
    col = np.arange(4, dtype=np.float64)

    df = pd.DataFrame({"col": col}, index=idx)
    rng = np.random.default_rng()
    df = pd.DataFrame(
        {
            "to_sum": rng.integers(0, 100, len(idx)),
            "to_min": rng.integers(0, 100, len(idx)),
            "to_max": rng.integers(0, 100, len(idx)),
            "to_mean": rng.integers(0, 100, len(idx)),
            "to_count": rng.integers(0, 100, len(idx)),
            "to_first": rng.integers(0, 100, len(idx)),
            "to_last": rng.integers(0, 100, len(idx)),
        },
        index=idx,
    )
    lib.write(sym, df)

    generic_resample_test_with_empty_buckets(
        lib,
        sym,
        "us",
        {
            "to_sum": ("to_sum", "sum"),
            "to_mean": ("to_mean", "mean"),
            "to_min": ("to_min", "min"),
            "to_max": ("to_max", "max"),
            "to_first": ("to_first", "first"),
            "to_last": ("to_last", "last"),
            "to_count": ("to_count", "count"),
        }
    )


@pytest.mark.parametrize("tz", (None, "Europe/London"))
@pytest.mark.parametrize("named_levels", (True, False))
def test_resample_multiindex(lmdb_version_store_v1, tz, named_levels):
    lib = lmdb_version_store_v1
    sym = "test_resample_multiindex"
    multiindex = pd.MultiIndex.from_product([pd.date_range("2024-01-01", freq="h", periods=5, tz=tz), [0, 1], ["hello", "goodbye"]])
    if named_levels:
        multiindex.names = ["datetime", "sequence number", "another index level"]
    df = pd.DataFrame(
        data={
            "to_sum": np.arange(len(multiindex)),
            "to_mean": np.arange(len(multiindex)) * 10,
        },
        index=multiindex,
    )
    lib.write(sym, df)
    freq = "2h"
    aggs = {"to_sum": "sum", "to_mean": "mean"}

    # Pandas doesn't support resampling multiindexed dataframes, but our behaviour is equivalent to there only being a
    # top-level timeseries index
    df.index = multiindex.droplevel(2).droplevel(1)
    expected = df.resample(freq).agg(aggs)

    q = QueryBuilder()
    q = q.resample(freq).agg(aggs)
    received = lib.read(sym, query_builder=q).data
    assert_frame_equal(expected, received, check_dtype=False)


@pytest.mark.parametrize("use_date_range", (True, False))
@pytest.mark.parametrize("single_query", (True, False))
def test_resampling_batch_read_query(lmdb_version_store_v1, use_date_range, single_query):
    lib = lmdb_version_store_v1
    sym_0 = "test_resampling_batch_read_query_0"
    sym_1 = "test_resampling_batch_read_query_1"

    if use_date_range:
        date_range_0 = (pd.Timestamp("2024-01-01T01:00:00"), pd.Timestamp("2024-01-01T12:00:00"))
        date_range_1 = (pd.Timestamp("2024-01-02T01:00:00"), pd.Timestamp("2024-01-02T11:00:00"))
        date_ranges = [date_range_0, date_range_1]
    else:
        date_range_0 = None
        date_range_1 = None
        date_ranges = None

    df_0 = pd.DataFrame({"col": np.arange(2000)}, index=pd.date_range("2024-01-01", freq="min", periods=2000))
    df_1 = pd.DataFrame({"col": np.arange(1000)}, index=pd.date_range("2024-01-02", freq="min", periods=1000))
    lib.batch_write([sym_0, sym_1], [df_0, df_1])

    if single_query:
        agg_dict_0 = {"col": "sum"}
        agg_dict_1 = agg_dict_0
        q = QueryBuilder().resample("h").agg(agg_dict_0)
    else:
        agg_dict_0 = {"col": "sum"}
        agg_dict_1 = {"col": "mean"}
        q_0 = QueryBuilder().resample("h").agg(agg_dict_0)
        q_1 = QueryBuilder().resample("h").agg(agg_dict_1)
        q = [q_0, q_1]

    # Date range filtering in Pandas is painful, so use our read call for that bit
    expected_0 = lib.read(sym_0, date_range=date_range_0).data.resample("h").agg(agg_dict_0)
    expected_1 = lib.read(sym_1, date_range=date_range_1).data.resample("h").agg(agg_dict_1)
    expected_0 = expected_0.reindex(columns=sorted(expected_0.columns))
    expected_1 = expected_1.reindex(columns=sorted(expected_1.columns))

    res = lib.batch_read([sym_0, sym_1], date_ranges=date_ranges, query_builder=q)
    received_0 = res[sym_0].data
    received_1 = res[sym_1].data

    received_0 = received_0.reindex(columns=sorted(received_0.columns))
    received_1 = received_1.reindex(columns=sorted(received_1.columns))
    assert_frame_equal(expected_0, received_0, check_dtype=False)
    assert_frame_equal(expected_1, received_1, check_dtype=False)


# All following tests cover that an appropriate exception is thrown when unsupported operations are attempted

@pytest.mark.parametrize("freq", ("B", "W", "M", "Q", "Y", "cbh", "bh", "BYS", "YS", "BYE", "YE", "BQS", "QS", "BQE",
                                  "QE", "CBMS", "BMS", "SMS", "MS", "CBME", "BME", "SME", "ME", "C"))
def test_resample_rejects_unsupported_frequency_strings(freq):
    with pytest.raises(ArcticDbNotYetImplemented):
        QueryBuilder().resample(freq)
    with pytest.raises(ArcticDbNotYetImplemented):
        QueryBuilder().resample("2" + freq)
    # Pandas 1.X throws an attribute error here
    if IS_PANDAS_TWO:
        with pytest.raises(ArcticDbNotYetImplemented):
            QueryBuilder().resample(freq + "1h")


def test_resampling_unsupported_aggregation_type_combos(lmdb_version_store_v1):
    lib = lmdb_version_store_v1
    sym = "test_resampling_unsupported_aggregation_type_combos"

    df = pd.DataFrame({"string": ["hello"], "datetime": [pd.Timestamp(0)]}, index=[pd.Timestamp(0)])
    lib.write(sym, df)

    for agg in ["sum", "mean", "min", "max"]:
        q = QueryBuilder()
        q = q.resample("min").agg({"string": agg})
        with pytest.raises(SchemaException):
            lib.read(sym, query_builder=q)

    q = QueryBuilder()
    q = q.resample("min").agg({"datetime": "sum"})
    with pytest.raises(SchemaException):
        lib.read(sym, query_builder=q)


def test_resampling_dynamic_schema_missing_column(lmdb_version_store_dynamic_schema_v1):
    lib = lmdb_version_store_dynamic_schema_v1
    sym = "test_resampling_dynamic_schema_missing_column"

    lib.write(sym, pd.DataFrame({"col_0": [0]}, index=[pd.Timestamp(0)]))
    lib.append(sym, pd.DataFrame({"col_1": [1000]}, index=[pd.Timestamp(2000)]))

    # Schema exception should be thrown regardless of whether there are any buckets that span segments or not
    q = QueryBuilder()
    q = q.resample("us").agg({"col_0": "sum"})
    with pytest.raises(SchemaException):
        lib.read(sym, query_builder=q)

    q = QueryBuilder()
    q = q.resample("s").agg({"col_1": "sum"})
    with pytest.raises(SchemaException):
        lib.read(sym, query_builder=q)


def test_resampling_sparse_data(lmdb_version_store_v1):
    lib = lmdb_version_store_v1
    sym = "test_resampling_sparse_data"

    # col_1 will be dense, but with fewer rows than the index column, and so semantically sparse
    data = {
        "col_0": [np.nan, 1.0],
        "col_1": [2.0, np.nan]
    }
    lib.write(sym, pd.DataFrame(data, index=[pd.Timestamp(0), pd.Timestamp(1000)]), sparsify_floats=True)

    q = QueryBuilder()
    q = q.resample("us").agg({"col_0": "sum"})
    with pytest.raises(SchemaException):
        lib.read(sym, query_builder=q)

    q = QueryBuilder()
    q = q.resample("s").agg({"col_1": "sum"})
    with pytest.raises(SchemaException):
        lib.read(sym, query_builder=q)


def test_resampling_empty_type_column(lmdb_version_store_empty_types_v1):
    lib = lmdb_version_store_empty_types_v1
    sym = "test_resampling_empty_type_column"

    lib.write(sym, pd.DataFrame({"col": ["hello"]}, index=[pd.Timestamp(0)]))
    lib.append(sym, pd.DataFrame({"col": [None]}, index=[pd.Timestamp(2000)]))

    # Schema exception should be thrown regardless of whether there are any buckets that span segments or not
    q = QueryBuilder()
    q = q.resample("us").agg({"col": "first"})
    with pytest.raises(SchemaException):
        lib.read(sym, query_builder=q)

    q = QueryBuilder()
    q = q.resample("s").agg({"col": "first"})
    with pytest.raises(SchemaException):
        lib.read(sym, query_builder=q)

@pytest.mark.skipif(PANDAS_VERSION < Version("1.1.0"), reason="Pandas < 1.1.0 do not have offset param")
@pytest.mark.parametrize("closed", ["left", "right"])
class TestResamplingOffset:

    @pytest.mark.parametrize("offset", ("30s", pd.Timedelta(seconds=30)))
    def test_offset_smaller_than_freq(self, lmdb_version_store_v1, closed, offset):
        lib = lmdb_version_store_v1
        sym = "test_offset_smaller_than_freq"
        idx = pd.date_range(pd.Timestamp("2024-01-02"), pd.Timestamp("2024-01-04"), freq="min")
        rng = np.random.default_rng()
        df = pd.DataFrame({"col": rng.integers(0, 100, len(idx))}, index=idx)
        lib.write(sym, df)
        generic_resample_test(
            lib,
            sym,
            "2min",
            all_aggregations_dict("col"),
            closed=closed,
            offset="30s"
        )

    @pytest.mark.parametrize("offset", ("2min37s", pd.Timedelta(minutes=2, seconds=37)))
    def test_offset_larger_than_freq(self, lmdb_version_store_v1, closed, offset):
        lib = lmdb_version_store_v1
        sym = "test_offset_larger_than_freq"
        idx = pd.date_range(pd.Timestamp("2024-01-02"), pd.Timestamp("2024-01-04"), freq="min")
        rng = np.random.default_rng()
        df = pd.DataFrame({"col": rng.integers(0, 100, len(idx))}, index=idx)
        lib.write(sym, df)
        generic_resample_test(
            lib,
            sym,
            "2min",
            all_aggregations_dict("col"),
            closed=closed,
            offset=offset
        )

    @pytest.mark.parametrize("offset", ("30s", pd.Timedelta(seconds=30)))
    def test_values_on_offset_boundary(self, lmdb_version_store_v1, closed, offset):
        lib = lmdb_version_store_v1
        sym = "test_offset_larger_than_freq"
        start = pd.Timestamp("2024-01-02")
        end = pd.Timestamp("2024-01-04")
        idx = pd.date_range(start, end, freq="30s")
        idx_1_nano_before = pd.date_range(start - pd.Timedelta(1), end - pd.Timedelta(1), freq="min")
        idx_1_nano_after = pd.date_range(start + pd.Timedelta(1), end + pd.Timedelta(1), freq="min")
        idx = idx.join(idx_1_nano_before, how="outer").join(idx_1_nano_after, how="outer")
        rng = np.random.default_rng()
        df = pd.DataFrame({"col": rng.integers(0, 100, len(idx))}, index=idx)
        lib.write(sym, df)
        generic_resample_test(
            lib,
            sym,
            "2min",
            all_aggregations_dict("col"),
            closed=closed,
            offset=offset
        )

    @pytest.mark.parametrize("offset", ("30s", pd.Timedelta(seconds=30)))
    @pytest.mark.parametrize("date_range", [
        (dt.datetime(2024, 1, 2, 5, 0, 30), dt.datetime(2024, 1, 3, 5, 0, 30)),
        (dt.datetime(2024, 1, 2, 5, 0, 45), dt.datetime(2024, 1, 3, 5, 0, 50)),
        (dt.datetime(2024, 1, 2, 5, 0, 30, 1), dt.datetime(2024, 1, 3, 5, 0, 29, 999999))
    ])
    def test_with_date_range(self, lmdb_version_store_v1, closed, date_range, offset):
        lib = lmdb_version_store_v1
        sym = "test_offset_larger_than_freq"
        start = pd.Timestamp("2024-01-02")
        end = pd.Timestamp("2024-01-04")
        idx = pd.date_range(start, end, freq="30s")
        idx_1_nano_before = pd.date_range(start - pd.Timedelta(1), end - pd.Timedelta(1), freq="min")
        idx_1_nano_after = pd.date_range(start + pd.Timedelta(1), end + pd.Timedelta(1), freq="min")
        idx = idx.join(idx_1_nano_before, how="outer").join(idx_1_nano_after, how="outer")
        rng = np.random.default_rng()
        df = pd.DataFrame({"col": rng.integers(0, 100, len(idx))}, index=idx)
        lib.write(sym, df)
        generic_resample_test(
            lib,
            sym,
            "2min",
            all_aggregations_dict("col"),
            closed=closed,
            offset=offset,
            date_range=date_range
        )

@pytest.mark.skipif(PANDAS_VERSION < Version("1.1.0"), reason="Pandas < 1.1.0 do not have offset param")
@pytest.mark.parametrize("closed", ["left", "right"])
class TestResamplingOrigin:

    # Timestamps: pre start, between start and end, post end, first date in the index, last date in the index
    @pytest.mark.parametrize(
        "origin",
        [
            "start",
            "start_day",
            pytest.param("end", marks=pytest.mark.skipif(PANDAS_VERSION < Version("1.3.0"), reason="Not supported")),
            pytest.param("end_day", marks=pytest.mark.skipif(PANDAS_VERSION < Version("1.3.0"), reason="Not supported")),
            "epoch",
            pd.Timestamp("2024-01-01"),
            pd.Timestamp("2025-01-01 15:00:00"),
            pd.Timestamp("2025-01-03 15:00:00"),
            pd.Timestamp("2025-01-01 10:00:33"),
            pd.Timestamp("2025-01-02 12:00:13")
        ]
    )
    def test_origin(self, lmdb_version_store_v1, closed, origin):
        lib = lmdb_version_store_v1
        sym = "test_origin_special_values"
        # Start and end are picked so that #bins * rule + start != end on purpose to test
        # the bin generation in case of end and end_day
        start = pd.Timestamp("2025-01-01 10:00:33")
        end = pd.Timestamp("2025-01-02 12:00:20")
        idx = pd.date_range(start, end, freq='10s')
        rng = np.random.default_rng()
        df = pd.DataFrame({"col": rng.integers(0, 100, len(idx))}, index=idx)
        lib.write(sym, df)
        generic_resample_test(
            lib,
            sym,
            "2min",
            all_aggregations_dict("col"),
            closed=closed,
            origin=origin
        )

    @pytest.mark.parametrize("origin", [
        "start",
        "start_day",
        pytest.param("end", marks=pytest.mark.skipif(PANDAS_VERSION < Version("1.3.0"), reason="Not supported")),
        pytest.param("end_day", marks=pytest.mark.skipif(PANDAS_VERSION < Version("1.3.0"), reason="Not supported"))
    ])
    @pytest.mark.parametrize("date_range", [
        (pd.Timestamp("2025-01-01 10:00:00"), pd.Timestamp("2025-01-02 12:00:00")), # start and end are multiples of rule
        (pd.Timestamp("2025-01-01 10:00:00"), pd.Timestamp("2025-01-02 12:00:03")), # start is multiple of rule
        (pd.Timestamp("2025-01-01 10:00:03"), pd.Timestamp("2025-01-02 12:00:00")) # end is multiple of rule
    ])
    def test_origin_is_multiple_of_freq(self, lmdb_version_store_v1, closed, origin, date_range):
        lib = lmdb_version_store_v1
        sym = "test_origin_special_values"
        start, end = date_range
        idx = pd.date_range(start, end, freq='10s')
        rng = np.random.default_rng()
        df = pd.DataFrame({"col": rng.integers(0, 100, len(idx))}, index=idx)
        lib.write(sym, df)
        generic_resample_test(
            lib,
            sym,
            "2min",
            all_aggregations_dict("col"),
            closed=closed,
            origin=origin,
            drop_empty_buckets_for="col"
        )

    @pytest.mark.parametrize("origin", [
        "start",
        "start_day",
        pytest.param("end", marks=pytest.mark.skipif(PANDAS_VERSION < Version("1.3.0"), reason="Not supported")),
        pytest.param("end_day", marks=pytest.mark.skipif(PANDAS_VERSION < Version("1.3.0"), reason="Not supported")),
        "epoch"
    ])
    def test_pre_epoch_data(self, lmdb_version_store_v1, closed, origin):
        lib = lmdb_version_store_v1
        sym = "test_origin_special_values"
        start = pd.Timestamp("1800-01-01 10:00:00")
        end = pd.Timestamp("1800-01-02 10:00:00")
        idx = pd.date_range(start, end, freq='30s')
        rng = np.random.default_rng()
        df = pd.DataFrame({"col": rng.integers(0, 100, len(idx))}, index=idx)
        lib.write(sym, df)
        generic_resample_test(
            lib,
            sym,
            "2min",
            all_aggregations_dict("col"),
            closed=closed,
            origin=origin,
            drop_empty_buckets_for="col"
        )

    @pytest.mark.parametrize("origin", [
        "start",
        "start_day",
        pytest.param("end", marks=pytest.mark.skipif(PANDAS_VERSION < Version("1.3.0"), reason="Not supported")),
        pytest.param("end_day", marks=pytest.mark.skipif(PANDAS_VERSION < Version("1.3.0"), reason="Not supported")),
    ])
    @pytest.mark.parametrize("date_range",
        list(itertools.product(
            [pd.Timestamp("2024-01-01") - pd.Timedelta(1), pd.Timestamp("2024-01-01") + pd.Timedelta(1)],
            [pd.Timestamp("2024-01-02") - pd.Timedelta(1), pd.Timestamp("2024-01-02") + pd.Timedelta(1)]))
    )
    def test_origin_off_by_one_on_boundary(self, lmdb_version_store_v1, closed, origin, date_range):
        lib = lmdb_version_store_v1
        sym = "test_origin_special_values"
        start, end = date_range
        idx = pd.date_range(start, end, freq='10s')
        rng = np.random.default_rng()
        df = pd.DataFrame({"col": rng.integers(0, 100, len(idx))}, index=idx)
        lib.write(sym, df)
        generic_resample_test(
            lib,
            sym,
            "2min",
            all_aggregations_dict("col"),
            closed=closed,
            origin=origin,
            drop_empty_buckets_for="col"
        )

    @pytest.mark.parametrize("origin", [
        "start_day",
        "start",
        pytest.param("end", marks=pytest.mark.skipif(PANDAS_VERSION < Version("1.3.0"), reason="Not supported")),
        pytest.param("end_day", marks=pytest.mark.skipif(PANDAS_VERSION < Version("1.3.0"), reason="Not supported"))
    ])
    def test_non_epoch_origin_throws_with_daterange(self, lmdb_version_store_v1, origin, closed):
        lib = lmdb_version_store_v1
        sym = "test_origin_start_throws_with_daterange"

        lib.write(sym, pd.DataFrame({"col": [1, 2, 3]}, index=pd.DatetimeIndex([pd.Timestamp("2024-01-01"), pd.Timestamp("2024-01-02"), pd.Timestamp("2024-01-03")])))
        q = QueryBuilder()
        q = q.resample('1min', origin=origin, closed=closed).agg({"col_min":("col", "min")})
        with pytest.raises(UserInputException) as exception_info:
            lib.read(sym, query_builder=q, date_range=(pd.Timestamp("2024-01-02"), pd.Timestamp("2024-01-03")))
        assert all(w in str(exception_info.value) for w in [origin, "origin"])

    @pytest.mark.parametrize("origin", ["epoch", pd.Timestamp("2025-01-03 12:00:00")])
    def test_epoch_and_ts_origin_works_with_date_range(self, lmdb_version_store_v1, closed, origin):
        lib = lmdb_version_store_v1
        sym = "test_origin_special_values"
        # Start and end are picked so that #bins * rule + start != end on purpose to test
        # the bin generation in case of end and end_day
        start = pd.Timestamp("2025-01-01 00:00:00")
        end = pd.Timestamp("2025-01-04 00:00:00")
        idx = pd.date_range(start, end, freq='3s')
        rng = np.random.default_rng()
        df = pd.DataFrame({"col": rng.integers(0, 100, len(idx))}, index=idx)
        lib.write(sym, df)
        generic_resample_test(
            lib,
            sym,
            "2min",
            all_aggregations_dict("col"),
            closed=closed,
            origin=origin,
            date_range=(pd.Timestamp("2025-01-02 00:00:00"), pd.Timestamp("2025-01-03 00:00:00"))
        )

@pytest.mark.skipif(PANDAS_VERSION < Version("1.1.0"), reason="Pandas < 1.1.0 do not have offset param")
@pytest.mark.parametrize("closed", ["left", "right"])
@pytest.mark.parametrize("label", ["left", "right"])
@pytest.mark.parametrize("origin",[
    "start",
    "start_day",
    pytest.param("end", marks=pytest.mark.skipif(PANDAS_VERSION < Version("1.3.0"), reason="Not supported")),
    pytest.param("end_day", marks=pytest.mark.skipif(PANDAS_VERSION < Version("1.3.0"), reason="Not supported")),
    "epoch",
    pd.Timestamp("2024-01-01"),
    pd.Timestamp("2025-01-01 15:00:00"),
    pd.Timestamp("2025-01-03 15:00:00")
])
@pytest.mark.parametrize("offset", ['10s', '13s', '2min'])
def test_origin_offset_combined(lmdb_version_store_v1, closed, origin, label, offset):
    lib = lmdb_version_store_v1
    sym = "test_origin_special_values"
    # Start and end are picked so that #bins * rule + start != end on purpose to test
    # the bin generation in case of end and end_day
    start = pd.Timestamp("2025-01-01 10:00:33")
    end = pd.Timestamp("2025-01-02 12:00:20")
    idx = pd.date_range(start, end, freq='10s')
    rng = np.random.default_rng()
    df = pd.DataFrame({"col": range(len(idx))}, index=idx)
    lib.write(sym, df)
    generic_resample_test(
        lib,
        sym,
        "2min",
        all_aggregations_dict("col"),
        closed=closed,
        origin=origin,
        drop_empty_buckets_for="col",
        label=label,
        offset=offset
    )

def test_max_with_one_infinity_element(lmdb_version_store_v1):
    lib = lmdb_version_store_v1
    sym = "test_max_with_one_infinity_element"

    lib.write(sym, pd.DataFrame({"col": [np.inf]}, index=pd.DatetimeIndex([pd.Timestamp("2024-01-01")])))
    q = QueryBuilder()
    q = q.resample('1min').agg({"col_max":("col", "max")})
    assert np.isinf(lib.read(sym, query_builder=q).data['col_max'][0])

def test_min_with_one_infinity_element(lmdb_version_store_v1):
    lib = lmdb_version_store_v1
    sym = "test_min_with_one_infinity_element"

    lib.write(sym, pd.DataFrame({"col": [-np.inf]}, index=pd.DatetimeIndex([pd.Timestamp("2024-01-01")])))
    q = QueryBuilder()
    q = q.resample('1min').agg({"col_min":("col", "min")})
    assert np.isneginf(lib.read(sym, query_builder=q).data['col_min'][0])

class TestDynamicSchema:
    @pytest.mark.parametrize("dtype", [np.float32, np.float64, np.uint32, np.int32, np.int64, bool])
    def test_missing_column_segment_does_not_cross_bucket(self, lmdb_version_store_dynamic_schema_v1, dtype):
        lib = lmdb_version_store_dynamic_schema_v1
        sym = "sym"

        idx = pd.date_range(pd.Timestamp(0), periods=20, freq='ns')
        initial_df = pd.DataFrame({"a": range(len(idx))}, index=idx)
        lib.write(sym, initial_df)

        idx_to_append = pd.date_range(pd.Timestamp(40), periods=20, freq='ns')
        data_to_append = {"a": range(len(idx_to_append)), "b": np.array(range(len(idx_to_append)), dtype=dtype)}
        df_to_append = pd.DataFrame(data_to_append, index=idx_to_append)
        lib.append(sym, df_to_append)

        q = QueryBuilder()
        q = q.resample('10ns').agg(all_aggregations_dict("b"))
        arctic_resampled = lib.read(sym, query_builder=q).data
        arctic_resampled = arctic_resampled.reindex(columns=sorted(arctic_resampled.columns))

        expected_slice_with_missing_column_idx = [pd.Timestamp(0), pd.Timestamp(10)]
        expected_slice_with_missing_column = pd.DataFrame({
            f"to_{agg}": [default_aggregation_value(agg, dtype) for _ in range(len(expected_slice_with_missing_column_idx))] for agg in ALL_AGGREGATIONS
        }, index=expected_slice_with_missing_column_idx)
        expected_slice_containing_column = df_to_append.resample('10ns').agg(None, **all_aggregations_dict("b"))
        expected = pd.concat([expected_slice_with_missing_column, expected_slice_containing_column])
        expected = expected.reindex(columns=sorted(expected.columns))
        assert_frame_equal(arctic_resampled, expected, check_dtype=False)

    def test_missing_column_segment_does_not_cross_bucket_date(self, lmdb_version_store_dynamic_schema_v1):
        # Datetime types do not support all aggregation types that is why they are separated in a separate test
        lib = lmdb_version_store_dynamic_schema_v1
        sym = "sym"

        idx = pd.date_range(pd.Timestamp(0), periods=20, freq='ns')
        initial_df = pd.DataFrame({"a": range(len(idx))}, index=idx)
        lib.write(sym, initial_df)

        idx_to_append = pd.date_range(pd.Timestamp(40), periods=20, freq='ns')
        data_to_append = {"a": range(len(idx_to_append)), "b": np.array([pd.Timestamp(i) for i in range(len(idx_to_append))])}
        df_to_append = pd.DataFrame(data_to_append, index=idx_to_append)
        lib.append(sym, df_to_append)

        q = QueryBuilder()
        q = q.resample('10ns').agg(datetime_aggregations_dict("b"))
        arctic_resampled = lib.read(sym, query_builder=q).data
        arctic_resampled = arctic_resampled.reindex(columns=sorted(arctic_resampled.columns))

        expected_slice_with_missing_column_idx = [pd.Timestamp(0), pd.Timestamp(10)]
        expected_slice_with_missing_column = pd.DataFrame({
            f"to_{agg}": [default_aggregation_value(agg, np.datetime64) for _ in range(len(expected_slice_with_missing_column_idx))] for agg in DATETIME_AGGREGATIONS
        }, index=expected_slice_with_missing_column_idx)
        expected_slice_containing_column = df_to_append.resample('10ns').agg(None, **datetime_aggregations_dict("b"))
        expected = pd.concat([expected_slice_with_missing_column, expected_slice_containing_column])
        expected = expected.reindex(columns=sorted(expected.columns))
        assert_frame_equal(arctic_resampled, expected, check_dtype=False)
    @pytest.mark.parametrize("dtype", [np.float32, np.float64, np.uint32, np.int32, np.int64, bool])
    def test_date_range_select_missing_column(self, lmdb_version_store_dynamic_schema_v1, dtype):
        lib = lmdb_version_store_dynamic_schema_v1
        sym = "sym"

        idx = pd.date_range(pd.Timestamp(0), periods=20, freq='ns')
        initial_df = pd.DataFrame({"a": range(len(idx))}, index=idx)
        lib.write(sym, initial_df)

        idx_to_append = pd.date_range(pd.Timestamp(40), periods=20, freq='ns')
        data_to_append = {"a": range(len(idx_to_append)), "b": np.array(range(len(idx_to_append)), dtype=dtype)}
        df_to_append = pd.DataFrame(data_to_append, index=idx_to_append)
        lib.append(sym, df_to_append)

        q = QueryBuilder()
        q = q.resample('10ns').agg(all_aggregations_dict("b"))
        arctic_resampled = lib.read(sym, query_builder=q, date_range=(None, pd.Timestamp(20))).data
        arctic_resampled = arctic_resampled.reindex(columns=sorted(arctic_resampled.columns))
        expected = pd.DataFrame({}, index=pd.DatetimeIndex([pd.Timestamp(0), pd.Timestamp(10)]))
        assert_frame_equal(arctic_resampled, expected)

    @pytest.mark.parametrize("dtype", [np.float32])
    def test_bucket_spans_two_segments(self, lmdb_version_store_dynamic_schema_v1, dtype):

        pd.set_option('display.max_rows', None)
        pd.set_option('display.max_columns', None)
        pd.set_option('display.width', None)
        pd.set_option('display.max_colwidth', None)

        lib = lmdb_version_store_dynamic_schema_v1
        sym = "sym"

        idx = pd.date_range(pd.Timestamp(1), periods=5, freq='ns')
        initial_df = pd.DataFrame({"a": range(len(idx))}, index=idx)
        lib.write(sym, initial_df)

        idx_to_append = pd.date_range(pd.Timestamp(6), periods=4, freq='ns')
        data_to_append = {"a": range(len(idx_to_append)), "b": np.array(range(-len(idx_to_append),0), dtype=dtype)}
        df_to_append = pd.DataFrame(data_to_append, index=idx_to_append)
        lib.append(sym, df_to_append)

        print()
        print(lib.read(sym).data)

        q = QueryBuilder()
        q = q.resample('10ns').agg({"mx": ("b", "max")})
        arctic_resampled = lib.read(sym, query_builder=q).data
        arctic_resampled = arctic_resampled.reindex(columns=sorted(arctic_resampled.columns))
        print()
        print(arctic_resampled)