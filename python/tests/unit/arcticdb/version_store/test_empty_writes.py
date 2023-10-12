"""
Copyright 2023 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""
import pytest
import pandas as pd
import numpy as np

from arcticdb.version_store._common import TimeFrame
from arcticdb.util.test import assert_frame_equal, assert_series_equal


def test_write_no_rows(lmdb_version_store, sym):
    column_names = ["a", "b", "c"]
    df = pd.DataFrame(columns=column_names)
    df["b"] = df["b"].astype("int64")
    lmdb_version_store.write(sym, df, dynamic_strings=True, coerce_columns={"a": float, "b": int, "c": str})
    assert not lmdb_version_store.is_symbol_pickled(sym)
    df.index = df.index.astype("datetime64[ns]")
    df["a"] = df["a"].astype("float64")

    # ArcticDB stores empty columns under a dedicated `EMPTYVAL` type, so the types are not going to match with pandas
    # until the first append.
    assert_frame_equal(lmdb_version_store.read(sym).data, df, check_index_type=False, check_dtype=False)

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
    # ArcticDB stores empty columns under a dedicated `EMPTYVAL` type, so the types are not going to match with pandas
    # until the first append.
    assert_frame_equal(lmdb_version_store_dynamic_schema.read(sym).data, df, check_index_type=False, check_dtype=False)

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

    # ArcticDB stores empty columns under a dedicated `EMPTYVAL` type, so the types are not going to match with pandas
    # until the first append.
    assert_series_equal(lmdb_version_store_dynamic_schema.read(sym).data, ser, check_index_type=False)


@pytest.mark.parametrize("dtype", ["int64", "float64"])
def test_append_empty_series(lmdb_version_store_dynamic_schema, sym, dtype):
    ser = pd.Series([])
    lmdb_version_store_dynamic_schema.write(sym, ser)
    assert not lmdb_version_store_dynamic_schema.is_symbol_pickled(sym)

    # ArcticDB stores empty columns under a dedicated `EMPTYVAL` type, so the types are not going to match with pandas
    # until the first append.
    assert_series_equal(lmdb_version_store_dynamic_schema.read(sym).data, ser, check_index_type=False)

    new_ser = pd.Series([1, 2, 3], dtype=dtype)
    lmdb_version_store_dynamic_schema.append(sym, new_ser)
    assert_series_equal(lmdb_version_store_dynamic_schema.read(sym).data, new_ser)
