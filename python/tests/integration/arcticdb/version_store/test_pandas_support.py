"""
Copyright 2023 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""
import numpy as np
import pandas as pd
import pytest
from datetime import datetime as dt
from numpy.random import RandomState
from pandas import DataFrame, DatetimeIndex

from arcticdb.util.test import assert_frame_equal


# In the following lines, the naming convention is
# test_rt_df stands for roundtrip dataframe (implicitly pandas given file name)


def test_rt_df_with_datetimeindex_with_timezone(basic_store):
    df = DataFrame(
        data=["A", "BC", "DEF"],
        index=DatetimeIndex(
            np.array([dt(2013, 1, 1), dt(2013, 1, 2), dt(2013, 1, 3)]).astype("datetime64[ns]"), tz="America/Chicago"
        ),
    )

    basic_store.write("pandas", df)
    saved_df = basic_store.read("pandas").data
    assert df.index.tz == saved_df.index.tz
    assert all(df.index == saved_df.index)
    assert_frame_equal(df, saved_df, check_names=False)


def test_rt_df_range_index_with_name(basic_store):
    df = DataFrame(data=["A", "B", "D"])
    df.index.name = "xxx"
    basic_store.write("pandas", df)
    saved_df = basic_store.read("pandas").data
    assert df.index.name == saved_df.index.name
    assert all(df.index == saved_df.index)
    assert_frame_equal(df, saved_df)


@pytest.mark.parametrize("has_index", [True, False])
@pytest.mark.parametrize("N", [1, 5, 10])
def test_rt_df_small_col_dtidx(basic_store, N, has_index):
    rnd = RandomState(0x42)

    if has_index:
        idx = pd.date_range(pd.Timestamp("2019-01-01"), periods=N)
        idx.freq = None
        idx.name = "datetime"
    else:
        idx = None

    symbol = "df_{}".format(N)
    df = DataFrame(data={"A": rnd.rand(N), "B": np.repeat(np.nan, N), "C": rnd.rand(N)}, index=idx)
    basic_store.write(symbol, df)
    saved_df = basic_store.read(symbol).data
    assert df.index.name == saved_df.index.name
    assert all(df.index == saved_df.index)
    assert_frame_equal(df, saved_df)


def create_params():
    params = []

    dtidx = pd.date_range(pd.Timestamp("2016-01-01"), periods=3)
    vals = np.arange(3, dtype=np.uint32)
    df = pd.DataFrame([1, 4, 9], index=pd.MultiIndex.from_arrays([dtidx, vals]))
    params.append(("midx_no_names", df))

    df = pd.DataFrame({"a": [1, 4, 9]}, index=pd.MultiIndex.from_arrays([dtidx, vals]))
    df.index.names = ["ts", "val"]
    params.append(("midx_names", df))
    return params


@pytest.mark.parametrize("symbol, item", create_params())
def test_rt_df(basic_store, symbol, item):
    basic_store.write("xxx", item.copy())
    df2 = basic_store.read("xxx").data
    assert_frame_equal(item, df2)


def test_empty_df(basic_store):
    item = pd.DataFrame()
    basic_store.write("xxx", item)
    df2 = basic_store.read("xxx").data
    assert df2.empty


def test_df_datetime_multi_index_with_timezones(object_and_mem_and_lmdb_version_store):
    zone = "America/Chicago"
    df = DataFrame(
        data=["A", "BC", "DEF"],
        index=[
            DatetimeIndex(np.array([dt(2013, 1, 1), dt(2013, 1, 2), dt(2013, 1, 3)]).astype("datetime64[ns]"), tz=zone),
            DatetimeIndex(np.array([dt(2014, 1, 1), dt(2014, 1, 2), dt(2014, 1, 3)]).astype("datetime64[ns]"), tz=zone),
        ],
    )
    first_index, second_index = df.index.levels
    assert first_index.tzinfo.zone == second_index.tzinfo.zone == zone
    object_and_mem_and_lmdb_version_store.write("pandastz", df)
    saved_df = object_and_mem_and_lmdb_version_store.read("pandastz").data
    first_index_s, second_index_s = saved_df.index.levels
    assert first_index_s.tzinfo.zone == second_index_s.tzinfo.zone == zone
