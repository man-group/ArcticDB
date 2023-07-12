"""
Copyright 2023 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""
import pandas as pd
import numpy as np

from arcticdb.version_store._common import TimeFrame
from arcticdb.util.test import assert_frame_equal, assert_series_equal
from arcticdb.util._versions import IS_PANDAS_TWO


def test_write_no_rows(lmdb_version_store, sym):
    column_names = ["a", "b", "c"]
    df = pd.DataFrame(columns=column_names)
    df["b"] = df["b"].astype("int64")
    lmdb_version_store.write(sym, df, dynamic_strings=True, coerce_columns={"a": float, "b": int, "c": str})
    assert not lmdb_version_store.is_symbol_pickled(sym)
    df.index = df.index.astype("datetime64[ns]")
    df["a"] = df["a"].astype("float64")
    assert_frame_equal(lmdb_version_store.read(sym).data, df)

    df2 = pd.DataFrame([[1.3, 6, "test"]], columns=column_names, index=[pd.Timestamp(0)])
    df2 = pd.concat((df, df2))
    # coercing not needed
    lmdb_version_store.append(sym, df2, dynamic_strings=True)
    assert_frame_equal(lmdb_version_store.read(sym).data, df2)

    df3 = pd.DataFrame(
        [[3.3, 8, None], [2.3, 10, "test2"]], columns=column_names, index=[pd.Timestamp(1), pd.Timestamp(2)]
    )
    df2 = pd.concat((df2, df3))
    # coercing not needed
    lmdb_version_store.append(sym, df3, dynamic_strings=True)
    assert_frame_equal(lmdb_version_store.read(sym).data, df2)


def test_write_no_columns_dynamic_schema(lmdb_version_store_dynamic_schema, sym):
    column_names = ["a", "b", "c"]
    df = pd.DataFrame(index=[pd.Timestamp(0), pd.Timestamp(1)])
    lmdb_version_store_dynamic_schema.write(sym, df)
    assert not lmdb_version_store_dynamic_schema.is_symbol_pickled(sym)
    assert_frame_equal(lmdb_version_store_dynamic_schema.read(sym).data, df)

    df2 = pd.DataFrame([[1.3, 6, "test"]], columns=column_names, index=[pd.Timestamp(2)])
    df3 = pd.concat([df, df2])
    # pandas will cast 'b' to float64 to fill the previous rows with NaNs
    df3["b"] = [0, 0, 6]
    df3["b"] = df3["b"].astype("int64")
    lmdb_version_store_dynamic_schema.append(sym, df2)
    ans = lmdb_version_store_dynamic_schema.read(sym).data
    assert_frame_equal(ans, df3)

    df4 = pd.DataFrame(
        [[3.3, 8, None, 3.5], [2.3, 10, "test2"]],
        columns=column_names + ["d"],
        index=[pd.Timestamp(3), pd.Timestamp(4)],
    )
    df5 = pd.concat((df3, df4))
    lmdb_version_store_dynamic_schema.append(sym, df4, dynamic_strings=True)
    assert_frame_equal(lmdb_version_store_dynamic_schema.read(sym).data, df5)


def test_write_no_columns_static_schema(lmdb_version_store, sym):
    df = pd.DataFrame(index=[pd.Timestamp(0), pd.Timestamp(1)])
    lmdb_version_store.write(sym, df)
    assert not lmdb_version_store.is_symbol_pickled(sym)
    assert_frame_equal(lmdb_version_store.read(sym).data, df)

    df2 = pd.DataFrame(index=[pd.Timestamp(2)])
    df3 = pd.concat((df, df2))

    lmdb_version_store.append(sym, df2)
    ans = lmdb_version_store.read(sym).data
    assert_frame_equal(ans, df3)

    df4 = pd.DataFrame(index=[pd.Timestamp(3), pd.Timestamp(4)])
    df5 = pd.concat((df3, df4))
    lmdb_version_store.append(sym, df4, dynamic_strings=True)
    assert_frame_equal(lmdb_version_store.read(sym).data, df5)


def test_write_no_rows_and_columns(lmdb_version_store_dynamic_schema, sym):
    column_names = ["a", "b", "c"]
    df = pd.DataFrame()
    lmdb_version_store_dynamic_schema.write(sym, df)
    assert not lmdb_version_store_dynamic_schema.is_symbol_pickled(sym)
    df.index = df.index.astype("datetime64[ns]")
    assert_frame_equal(lmdb_version_store_dynamic_schema.read(sym).data, df)

    df2 = pd.DataFrame([[1.3, 6, "test"]], columns=column_names, index=[pd.Timestamp(2)])
    lmdb_version_store_dynamic_schema.append(sym, df2)
    ans = lmdb_version_store_dynamic_schema.read(sym).data
    assert_frame_equal(ans, df2)

    df4 = pd.DataFrame(
        [[3.3, 8, None, 3.5], [2.3, 10, "test2"]],
        columns=column_names + ["d"],
        index=[pd.Timestamp(3), pd.Timestamp(4)],
    )
    df5 = pd.concat((df2, df4))
    lmdb_version_store_dynamic_schema.append(sym, df4, dynamic_strings=True)
    assert_frame_equal(lmdb_version_store_dynamic_schema.read(sym).data, df5)


def test_update_no_columns_dynamic_schema(lmdb_version_store_dynamic_schema, sym):
    column_names = ["a", "b", "c"]
    df = pd.DataFrame(index=[pd.Timestamp(0), pd.Timestamp(1)])
    lmdb_version_store_dynamic_schema.write(sym, df)
    assert not lmdb_version_store_dynamic_schema.is_symbol_pickled(sym)
    assert_frame_equal(lmdb_version_store_dynamic_schema.read(sym).data, df)

    df2 = pd.DataFrame([[1.3, 6, "test"]], columns=column_names, index=[pd.Timestamp(0)])
    lmdb_version_store_dynamic_schema.update(sym, df2)
    # update in arctic native (outer join) behaves a bit differently than DataFrame.update (left join)
    df2 = pd.concat((df2, pd.DataFrame([[np.nan, 0, np.nan]], columns=column_names, index=[pd.Timestamp(1)])))
    ans = lmdb_version_store_dynamic_schema.read(sym).data
    assert_frame_equal(ans, df2)


def test_empty_timeframe(lmdb_version_store_dynamic_schema, sym):
    tz = "America/New_York"
    dtidx = pd.date_range("2019-02-06 11:43", periods=6).tz_localize(tz)
    tf = TimeFrame(dtidx.values, columns_names=[], columns_values=[])
    lmdb_version_store_dynamic_schema.write(sym, tf)
    vit = lmdb_version_store_dynamic_schema.read(sym)

    assert tf == vit.data


def test_empty_series(lmdb_version_store_dynamic_schema, sym):
    ser = pd.Series([])
    lmdb_version_store_dynamic_schema.write(sym, ser)
    assert not lmdb_version_store_dynamic_schema.is_symbol_pickled(sym)
    if IS_PANDAS_TWO:
        # In Pandas 2.0, RangeIndex is used by default when an empty dataframe or series is created.
        # The index is converted to a DatetimeIndex for preserving the behavior of ArcticDB with
        # Pandas 1.0.
        ser.index = ser.index.astype("datetime64[ns]")

    assert_series_equal(lmdb_version_store_dynamic_schema.read(sym).data, ser)


def test_fallback_to_pickle(lmdb_version_store, sym):
    column_names = ["a", "b", "c"]
    df = pd.DataFrame(columns=column_names)
    lmdb_version_store.write(sym, df)

    # In Pandas 2.0, RangeIndex is used by default when an empty dataframe or series is created.
    # The index is converted to a DatetimeIndex for preserving the behavior of ArcticDB with Pandas 1.0.
    assert isinstance(df.index, pd.RangeIndex if IS_PANDAS_TWO else pd.Index)

    if IS_PANDAS_TWO:
        # In Pandas 2.0, RangeIndex is used by default when an empty dataframe or series is created.
        # The index has to be converted to a DatetimeIndex by ArcticDB to perform updates.
        df.index = df.index.astype("datetime64[ns]")

    # In Pandas 2.0, the DataFrame can be stored without pickling.
    # In Pandas 1.0, the DataFrame has to be pickled.
    assert IS_PANDAS_TWO ^ lmdb_version_store.is_symbol_pickled(sym)

    assert_frame_equal(df, lmdb_version_store.read(sym).data)
