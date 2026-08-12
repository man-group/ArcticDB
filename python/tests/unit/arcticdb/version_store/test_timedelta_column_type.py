"""
Copyright 2026 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

import numpy as np
import pandas as pd
import pyarrow as pa
import pytest
from pandas.testing import assert_frame_equal

from arcticdb.exceptions import (
    ArcticDbNotYetImplemented,
    NormalizationException,
    SchemaException,
    StreamDescriptorMismatch,
)
from arcticdb.options import OutputFormat
from arcticdb.util.test import config_context
from arcticdb.version_store.processing import QueryBuilder

ENABLE_WRITE = "Timedelta.EnableWrite"


def timedelta_df(values, index=None):
    return pd.DataFrame({"dur": pd.to_timedelta(values)}, index=index)


def test_write_read_round_trip(lmdb_version_store_v1):
    lib = lmdb_version_store_v1
    df = timedelta_df(["1 days", "2 hours", "3 seconds", "-4 minutes"])
    with config_context(ENABLE_WRITE, 1):
        lib.write("sym", df)
    received = lib.read("sym").data
    assert received["dur"].dtype == np.dtype("timedelta64[ns]")
    assert_frame_equal(df, received)


def test_read_does_not_require_the_flag(lmdb_version_store_v1):
    lib = lmdb_version_store_v1
    df = timedelta_df(["1 days", "2 hours"])
    with config_context(ENABLE_WRITE, 1):
        lib.write("sym", df)
    with config_context(ENABLE_WRITE, 0):
        assert_frame_equal(df, lib.read("sym").data)
        assert lib.read("sym", output_format=OutputFormat.PYARROW).data.column("dur").type == pa.duration("ns")


def test_write_rejected_without_the_flag(lmdb_version_store_v1):
    lib = lmdb_version_store_v1
    df = timedelta_df(["1 days"])
    with pytest.raises(NormalizationException) as e:
        lib.write("sym", df)
    assert "dur" in str(e.value)


def test_nat_round_trip(lmdb_version_store_v1):
    lib = lmdb_version_store_v1
    df = timedelta_df(["1 days", pd.NaT, "3 seconds", pd.NaT])
    with config_context(ENABLE_WRITE, 1):
        lib.write("sym", df)
    received = lib.read("sym").data
    assert_frame_equal(df, received)
    assert received["dur"].isna().tolist() == [False, True, False, True]


def test_all_nat_round_trip(lmdb_version_store_v1):
    lib = lmdb_version_store_v1
    df = timedelta_df([pd.NaT, pd.NaT, pd.NaT])
    with config_context(ENABLE_WRITE, 1):
        lib.write("sym", df)
    assert_frame_equal(df, lib.read("sym").data)


def test_empty_timedelta_column(lmdb_version_store_v1):
    lib = lmdb_version_store_v1
    df = pd.DataFrame({"dur": np.array([], dtype="timedelta64[ns]")})
    with config_context(ENABLE_WRITE, 1):
        lib.write("sym", df)
    received = lib.read("sym").data
    assert received["dur"].dtype == np.dtype("timedelta64[ns]")
    assert len(received) == 0


@pytest.mark.parametrize("unit", ["us", "ms", "s"])
def test_non_nanosecond_resolution_coerced(lmdb_version_store_v1, unit):
    lib = lmdb_version_store_v1
    df = pd.DataFrame({"dur": np.array([1, 2, 3], dtype=f"timedelta64[{unit}]")})
    original_dtype = df["dur"].dtype
    with config_context(ENABLE_WRITE, 1):
        lib.write("sym", df)
    assert df["dur"].dtype == original_dtype, "Caller's DataFrame was modified in place"
    received = lib.read("sym").data
    assert received["dur"].dtype == np.dtype("timedelta64[ns]")
    assert_frame_equal(df.astype({"dur": "timedelta64[ns]"}), received)


@pytest.mark.parametrize("flag", [0, 1])
def test_timedelta_index_rejected(lmdb_version_store_v1, flag):
    lib = lmdb_version_store_v1
    df = pd.DataFrame({"col": [1, 2, 3]}, index=pd.to_timedelta(["1 days", "2 days", "3 days"]))
    with config_context(ENABLE_WRITE, flag):
        with pytest.raises(ArcticDbNotYetImplemented):
            lib.write("sym", df)


@pytest.mark.parametrize("flag", [0, 1])
@pytest.mark.parametrize("level", [0, 1])
def test_timedelta_multiindex_level_rejected(lmdb_version_store_v1, flag, level):
    lib = lmdb_version_store_v1
    timestamps = pd.to_datetime(["2025-01-01", "2025-01-02"])
    durations = pd.to_timedelta(["1 days", "2 days"])
    arrays = [durations, timestamps] if level == 0 else [timestamps, durations]
    df = pd.DataFrame({"col": [1, 2]}, index=pd.MultiIndex.from_arrays(arrays))
    with config_context(ENABLE_WRITE, flag):
        with pytest.raises(ArcticDbNotYetImplemented):
            lib.write("sym", df)


@pytest.mark.parametrize("unit", ["ns", "us", "ms", "s"])
def test_ndarray_round_trip(lmdb_version_store_v1, unit):
    lib = lmdb_version_store_v1
    arr = np.array([1, 2, 3], dtype=f"timedelta64[{unit}]")
    with config_context(ENABLE_WRITE, 1):
        lib.write("sym", arr)
    received = lib.read("sym").data
    assert received.dtype == np.dtype("timedelta64[ns]")
    np.testing.assert_array_equal(arr.astype("timedelta64[ns]"), received)


def test_ndarray_write_rejected_without_the_flag(lmdb_version_store_v1):
    lib = lmdb_version_store_v1
    with pytest.raises(NormalizationException):
        lib.write("sym", np.array([1, 2, 3], dtype="timedelta64[ns]"))


def test_stage_rejected_without_the_flag(lmdb_version_store_v1):
    lib = lmdb_version_store_v1
    with pytest.raises(NormalizationException):
        lib.stage("sym", timedelta_df(["1 days"]))


def test_series_round_trip(lmdb_version_store_v1):
    lib = lmdb_version_store_v1
    series = pd.Series(pd.to_timedelta(["1 days", pd.NaT]), name="dur")
    with config_context(ENABLE_WRITE, 1):
        lib.write("sym", series)
    received = lib.read("sym").data
    assert received.dtype == np.dtype("timedelta64[ns]")
    pd.testing.assert_series_equal(series, received)


def test_append_matching_types(lmdb_version_store_v1):
    lib = lmdb_version_store_v1
    first = timedelta_df(["1 days", "2 days"])
    second = timedelta_df(["3 days", pd.NaT])
    with config_context(ENABLE_WRITE, 1):
        lib.write("sym", first)
        lib.append("sym", second)
    assert_frame_equal(pd.concat([first, second]).reset_index(drop=True), lib.read("sym").data)


def test_update_matching_types(lmdb_version_store_v1):
    lib = lmdb_version_store_v1
    index = pd.to_datetime(["2025-01-01", "2025-01-02", "2025-01-03", "2025-01-04"])
    df = timedelta_df(["1 days", "2 days", "3 days", "4 days"], index=index)
    update = timedelta_df(["10 days", "20 days"], index=index[1:3])
    with config_context(ENABLE_WRITE, 1):
        lib.write("sym", df)
        lib.update("sym", update)
    expected = df.copy()
    expected.iloc[1:3] = update
    assert_frame_equal(expected, lib.read("sym").data)


@pytest.mark.parametrize("other_dtype", ["int64", "datetime64[ns]"])
def test_append_incompatible_type_static_schema(lmdb_version_store_v1, other_dtype):
    lib = lmdb_version_store_v1
    with config_context(ENABLE_WRITE, 1):
        lib.write("sym", timedelta_df(["1 days"]))
        with pytest.raises(StreamDescriptorMismatch):
            lib.append("sym", pd.DataFrame({"dur": np.array([1], dtype=other_dtype)}))


@pytest.mark.parametrize("other_dtype", ["int64", "datetime64[ns]"])
def test_append_incompatible_type_dynamic_schema(lmdb_version_store_dynamic_schema_v1, other_dtype):
    lib = lmdb_version_store_dynamic_schema_v1
    with config_context(ENABLE_WRITE, 1):
        lib.write("sym", timedelta_df(["1 days"]))
        with pytest.raises(SchemaException):
            lib.append("sym", pd.DataFrame({"dur": np.array([1], dtype=other_dtype)}))


def test_dynamic_schema_missing_column_backfills_nat(lmdb_version_store_dynamic_schema_v1):
    lib = lmdb_version_store_dynamic_schema_v1
    with config_context(ENABLE_WRITE, 1):
        lib.write("sym", pd.DataFrame({"other": [1, 2]}))
        lib.append("sym", timedelta_df(["1 days", "2 days"]).assign(other=[3, 4]))
    received = lib.read("sym").data
    assert received["dur"].dtype == np.dtype("timedelta64[ns]")
    assert received["dur"].isna().tolist() == [True, True, False, False]


def test_arrow_read(lmdb_version_store_v1):
    lib = lmdb_version_store_v1
    df = timedelta_df(["1 days", pd.NaT, "3 seconds"])
    with config_context(ENABLE_WRITE, 1):
        lib.write("sym", df)
    table = lib.read("sym", output_format=OutputFormat.PYARROW).data
    assert table.column("dur").type == pa.duration("ns")
    assert table.column("dur").is_null().to_pylist() == [False, True, False]
    assert_frame_equal(df, table.to_pandas())


def test_arrow_read_empty(lmdb_version_store_v1):
    lib = lmdb_version_store_v1
    df = pd.DataFrame({"dur": np.array([], dtype="timedelta64[ns]")})
    with config_context(ENABLE_WRITE, 1):
        lib.write("sym", df)
    table = lib.read("sym", output_format=OutputFormat.PYARROW).data
    assert table.column("dur").type == pa.duration("ns")
    assert table.num_rows == 0


def test_arrow_read_all_nat(lmdb_version_store_v1):
    lib = lmdb_version_store_v1
    with config_context(ENABLE_WRITE, 1):
        lib.write("sym", timedelta_df([pd.NaT, pd.NaT]))
    table = lib.read("sym", output_format=OutputFormat.PYARROW).data
    assert table.column("dur").type == pa.duration("ns")
    assert table.column("dur").is_null().to_pylist() == [True, True]


def test_arrow_write(in_memory_version_store_arrow):
    lib = in_memory_version_store_arrow
    table = pa.table({"dur": pa.array([1, None, 3], type=pa.duration("ns"))})
    with config_context(ENABLE_WRITE, 1):
        lib.write("sym", table)
    received = lib.read("sym").data
    assert received.column("dur").type == pa.duration("ns")
    assert received.column("dur").to_pylist() == table.column("dur").to_pylist()


def test_arrow_write_rejected_without_the_flag(in_memory_version_store_arrow):
    lib = in_memory_version_store_arrow
    table = pa.table({"dur": pa.array([1, 2], type=pa.duration("ns"))})
    with pytest.raises(NormalizationException) as e:
        lib.write("sym", table)
    assert "not yet supported" in str(e.value)


@pytest.fixture
def timedelta_symbol(lmdb_version_store_v1):
    lib = lmdb_version_store_v1
    with config_context(ENABLE_WRITE, 1):
        lib.write("sym", timedelta_df(["1 days", "2 days", "3 days"]).assign(other=[1, 2, 3]))
    return lib


def assert_rejected_in_queries(lib, query_builder):
    with pytest.raises(SchemaException) as e:
        lib.read("sym", query_builder=query_builder)
    assert "not yet supported in queries" in str(e.value)
    assert "dur" in str(e.value)


def test_filter_rejected(timedelta_symbol):
    q = QueryBuilder()
    assert_rejected_in_queries(timedelta_symbol, q[q["dur"] > pd.Timedelta("1 days")])


def test_filter_against_int_rejected(timedelta_symbol):
    q = QueryBuilder()
    assert_rejected_in_queries(timedelta_symbol, q[q["dur"] > 0])


def test_projection_rejected(timedelta_symbol):
    q = QueryBuilder()
    assert_rejected_in_queries(timedelta_symbol, q.apply("new", q["dur"] + 1))


def test_isnull_rejected(timedelta_symbol):
    q = QueryBuilder()
    assert_rejected_in_queries(timedelta_symbol, q[q["dur"].isnull()])


def test_filter_on_another_column_returns_durations(timedelta_symbol):
    # Exercises SegmentInMemoryImpl::filter carrying a duration column through an operation driven by another column.
    q = QueryBuilder()
    received = timedelta_symbol.read("sym", query_builder=q[q["other"] > 1]).data
    assert received["dur"].dtype == np.dtype("timedelta64[ns]")
    assert received["dur"].tolist() == pd.to_timedelta(["2 days", "3 days"]).tolist()


def test_row_range_returns_durations(timedelta_symbol):
    q = QueryBuilder()
    received = timedelta_symbol.read("sym", query_builder=q.row_range((1, 3))).data
    assert received["dur"].tolist() == pd.to_timedelta(["2 days", "3 days"]).tolist()


def test_groupby_key_rejected(timedelta_symbol):
    q = QueryBuilder()
    assert_rejected_in_queries(timedelta_symbol, q.groupby("dur").agg({"other": "sum"}))


@pytest.mark.parametrize("aggregation", ["min", "max", "sum", "mean"])
def test_groupby_aggregation_rejected(timedelta_symbol, aggregation):
    q = QueryBuilder()
    assert_rejected_in_queries(timedelta_symbol, q.groupby("other").agg({"dur": aggregation}))


@pytest.mark.parametrize("aggregation", ["min", "max", "sum", "mean", "first", "last", "count"])
def test_resample_aggregation_rejected(lmdb_version_store_v1, aggregation):
    lib = lmdb_version_store_v1
    df = timedelta_df(["1 days", "2 days"], index=pd.date_range("2025-01-01", periods=2))
    with config_context(ENABLE_WRITE, 1):
        lib.write("sym", df)
    q = QueryBuilder()
    with pytest.raises(SchemaException) as e:
        lib.read("sym", query_builder=q.resample("D").agg({"dur": aggregation}))
    assert "dur" in str(e.value)
    assert "TIMEDELTA_NS64" in str(e.value)


def test_column_stats_round_trip(lmdb_version_store_v1):
    lib = lmdb_version_store_v1
    df = timedelta_df(["2 days", pd.NaT, "1 days"], index=pd.date_range("2025-01-01", periods=3))
    with config_context(ENABLE_WRITE, 1):
        lib.write("sym", df)
    lib.create_column_stats_experimental("sym")
    stats = lib.read_column_stats_experimental("sym")
    assert stats.column("v1_MIN(dur)").type == pa.duration("ns")
    assert stats.column("v1_MIN(dur)").to_pylist() == [pd.Timedelta("1 days").to_pytimedelta()]
    assert stats.column("v1_MAX(dur)").to_pylist() == [pd.Timedelta("2 days").to_pytimedelta()]
    assert stats.column("v1_NAN_COUNT(dur)").to_pylist() == [1]
