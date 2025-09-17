"""
Copyright 2023 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

import pandas as pd
import numpy as np
import pytest
from arcticdb_ext.exceptions import SortingException, InternalException
from arcticdb.version_store import _store as store

try:
    from arctic.date import DateRange
except ModuleNotFoundError:
    from tests.util.date import DateRange  # Verbatim local copy


def assert_range_eq(result, start, end):
    assert result.start_ts == start
    assert result.end_ts == end


def test_pandas_date_range():
    result = store._normalize_dt_range(pd.date_range("2020-01-01", "2021-03-01"))
    assert_range_eq(result, 1577836800000000000, 1614556800000000000)
    assert pd.Timestamp(result.start_ts, unit="ns") == pd.Timestamp("2020-01-01")
    assert pd.Timestamp(result.end_ts, unit="ns") == pd.Timestamp("2021-03-01")


def test_arctic_date_range():
    result = store._normalize_dt_range(DateRange("2020-01-01", "2021-03-01"))
    assert_range_eq(result, 1577836800000000000, 1614556800000000000)
    assert pd.Timestamp(result.start_ts, unit="ns") == pd.Timestamp("2020-01-01")
    assert pd.Timestamp(result.end_ts, unit="ns") == pd.Timestamp("2021-03-01")


def test_arctic_date_range_max():
    result = store._normalize_dt_range(DateRange(None, None))
    assert str(result.start_ts).startswith("-9223372036854775")  # Older Pandas trucated min for some reason
    assert result.end_ts == 9223372036854775807
    assert pd.Timestamp(result.start_ts, unit="ns") == pd.Timestamp.min
    assert pd.Timestamp(result.end_ts, unit="ns") == pd.Timestamp.max


def test_read_unsorted_date_range_dataframe(lmdb_version_store):
    symbol = "read_unsorted_incr"

    num_initial_rows = 20
    initial_timestamp = pd.Timestamp("2019-01-01")
    dtidx = pd.date_range(initial_timestamp, periods=num_initial_rows)

    df = pd.DataFrame({"c": np.arange(0, num_initial_rows, dtype=np.int64)}, index=np.roll(dtidx, 3))

    assert df.index.is_monotonic_increasing == False
    assert df.index.is_monotonic_decreasing == False
    lmdb_version_store.write(symbol, df)
    info = lmdb_version_store.get_info(symbol)
    assert info["sorted"] == "UNSORTED"
    with pytest.raises(SortingException) as e_info:
        data = lmdb_version_store.read(
            symbol, date_range=(DateRange(pd.Timestamp("2019-01-03"), pd.Timestamp("2019-01-06")))
        ).data


def test_batch_read_unsorted_date_range_dataframe(lmdb_version_store):
    symbol1 = "read_batch_unsorted_incr1"
    symbol2 = "read_batch_unsorted_incr2"
    num_initial_rows = 20
    initial_timestamp = pd.Timestamp("2019-01-01")
    dtidx = pd.date_range(initial_timestamp, periods=num_initial_rows)
    df1 = pd.DataFrame({"c": np.arange(0, num_initial_rows, dtype=np.int64)}, index=np.roll(dtidx, 3))

    df2 = pd.DataFrame({"c": np.arange(0, num_initial_rows, dtype=np.int64)}, index=np.roll(dtidx, 6))

    assert df1.index.is_monotonic_increasing == False
    assert df1.index.is_monotonic_decreasing == False
    assert df2.index.is_monotonic_increasing == False
    assert df2.index.is_monotonic_decreasing == False
    lmdb_version_store.write(symbol1, df1)
    lmdb_version_store.write(symbol2, df2)
    info1 = lmdb_version_store.get_info(symbol1)
    info2 = lmdb_version_store.get_info(symbol2)
    assert info1["sorted"] == "UNSORTED"
    assert info2["sorted"] == "UNSORTED"
    with pytest.raises(SortingException) as e_info:
        batch_res = lmdb_version_store.batch_read(
            [symbol1, symbol2],
            date_ranges=[
                (DateRange(pd.Timestamp("2019-01-03"), pd.Timestamp("2019-01-06"))),
                (DateRange(pd.Timestamp("2019-01-06"), pd.Timestamp("2019-01-09"))),
            ],
        )


def test_read_date_range_not_date_time_dataframe(lmdb_version_store):
    symbol = "read_unsorted_incr"

    num_initial_rows = 20
    dtidx = np.arange(0, num_initial_rows)

    df = pd.DataFrame({"c": np.arange(0, num_initial_rows, dtype=np.int64)}, index=dtidx)

    lmdb_version_store.write(symbol, df)
    with pytest.raises(InternalException) as e_info:
        data = lmdb_version_store.read(
            symbol, date_range=(DateRange(pd.Timestamp("2019-01-03"), pd.Timestamp("2019-01-06")))
        ).data


def test_read_unsorted_date_range_dataframe_multi_index(lmdb_version_store):
    symbol = "read_unsorted_incr"

    num_initial_rows = 20
    initial_timestamp = pd.Timestamp("2019-01-01")
    dtidx1 = np.roll(pd.date_range(initial_timestamp, periods=num_initial_rows), 3)
    dtidx2 = np.roll(np.arange(0, num_initial_rows), 3)
    df = pd.DataFrame(
        {"c": np.arange(0, num_initial_rows, dtype=np.int64)},
        index=pd.MultiIndex.from_arrays([dtidx1, dtidx2], names=["datetime", "level"]),
    )

    assert df.index.is_monotonic_increasing == False
    assert df.index.is_monotonic_decreasing == False
    lmdb_version_store.write(symbol, df)
    info = lmdb_version_store.get_info(symbol)
    assert info["sorted"] == "UNSORTED"
    with pytest.raises(SortingException) as e_info:
        data = lmdb_version_store.read(
            symbol, date_range=(DateRange(pd.Timestamp("2019-01-03"), pd.Timestamp("2019-01-06")))
        ).data
