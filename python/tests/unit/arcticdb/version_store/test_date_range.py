"""
Copyright 2023 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""
import pandas as pd

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
