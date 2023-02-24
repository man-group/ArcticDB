"""
Copyright 2023 Man Group Operations Ltd.
NO WARRANTY, EXPRESSED OR IMPLIED.
"""
import pandas as pd
from arcticdb.version_store import _store as store
from arctic.date import DateRange


def range_eq(result, start, end):
    return result.start_ts == start and result.end_ts == end


def test_pandas_date_range():
    result = store._normalize_dt_range(pd.date_range("2020-01-01", "2021-03-01"))
    assert range_eq(result, 1577836800000000000, 1614556800000000000)
    assert pd.Timestamp(result.start_ts, unit="ns") == pd.Timestamp("2020-01-01")
    assert pd.Timestamp(result.end_ts, unit="ns") == pd.Timestamp("2021-03-01")


def test_arctic_date_range():
    result = store._normalize_dt_range(DateRange("2020-01-01", "2021-03-01"))
    assert range_eq(result, 1577836800000000000, 1614556800000000000)
    assert pd.Timestamp(result.start_ts, unit="ns") == pd.Timestamp("2020-01-01")
    assert pd.Timestamp(result.end_ts, unit="ns") == pd.Timestamp("2021-03-01")


def test_arctic_date_range_max():
    result = store._normalize_dt_range(DateRange(None, None))
    assert range_eq(result, -9223372036854775000, 9223372036854775807)
    assert pd.Timestamp(result.start_ts, unit="ns") == pd.Timestamp.min
    assert pd.Timestamp(result.end_ts, unit="ns") == pd.Timestamp.max
