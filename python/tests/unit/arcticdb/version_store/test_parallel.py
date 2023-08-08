"""
Copyright 2023 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""
import numpy as np
import pandas as pd
import random
import datetime
import pytest

from arcticdb.util.test import (
    assert_frame_equal,
    random_strings_of_length,
    random_integers,
    random_floats,
    random_dates,
)


def test_parallel_write(lmdb_version_store):
    sym = "parallel"
    lmdb_version_store.version_store.remove_incomplete(sym)

    num_rows = 1111
    dtidx = pd.date_range("1970-01-01", periods=num_rows)
    test = pd.DataFrame(
        {"uint8": random_integers(num_rows, np.uint8), "uint32": random_integers(num_rows, np.uint32)}, index=dtidx
    )
    chunk_size = 100
    list_df = [test[i : i + chunk_size] for i in range(0, test.shape[0], chunk_size)]
    random.shuffle(list_df)

    for df in list_df:
        lmdb_version_store.write(sym, df, parallel=True)

    user_meta = {"thing": 7}
    lmdb_version_store.compact_incomplete(sym, False, False, metadata=user_meta)
    vit = lmdb_version_store.read(sym)
    assert_frame_equal(test, vit.data)
    assert vit.metadata["thing"] == 7


def test_roundtrip_nan(lmdb_version_store):
    df = pd.DataFrame(np.nan, index=[0, 1, 2, 3], columns=["A", "B"])
    lmdb_version_store.write("all_nans", df)
    vit = lmdb_version_store.read("all_nans")


def test_roundtrip_nat(lmdb_version_store):
    nats = np.repeat(pd.NaT, 4)
    d = {"col1": nats}
    df = pd.DataFrame(index=[0, 1, 2, 3], data=d)
    lmdb_version_store.write("all_nats", df)
    vit = lmdb_version_store.read("all_nats")


def test_floats_to_nans(lmdb_version_store):
    num_rows_per_day = 10
    num_days = 10
    num_columns = 8
    column_length = 8
    dt = datetime.datetime(2019, 4, 8, 0, 0, 0)
    columns = random_strings_of_length(num_columns, column_length, True)
    symbol = "test_write_parallel_stress_schema_change"
    dataframes = []
    df = pd.DataFrame()

    for _ in range(num_days):
        cols = random.sample(columns, 4)
        index = pd.Index([dt + datetime.timedelta(seconds=s) for s in range(num_rows_per_day)])
        vals = {c: random_floats(num_rows_per_day) for c in cols}
        new_df = pd.DataFrame(data=vals, index=index)

        dataframes.append(new_df)
        df = pd.concat((df, new_df))
        dt = dt + datetime.timedelta(days=1)

    random.shuffle(dataframes)
    for d in dataframes:
        lmdb_version_store.write(symbol, d, parallel=True)

    lmdb_version_store.version_store.compact_incomplete(symbol, False, False)
    vit = lmdb_version_store.read(symbol)
    df.sort_index(axis=1, inplace=True)
    result = vit.data
    result.sort_index(axis=1, inplace=True)
    assert_frame_equal(vit.data, df)


def test_sort_merge_write(lmdb_version_store):
    num_rows_per_day = 10
    num_days = 10
    num_columns = 8
    column_length = 8
    dt = datetime.datetime(2019, 4, 8, 0, 0, 0)
    columns = random_strings_of_length(num_columns, column_length, True)
    symbol = "test_write_parallel_sort_merge"
    dataframes = []
    df = pd.DataFrame()

    for _ in range(num_days):
        cols = random.sample(columns, 4)
        index = pd.Index([dt + datetime.timedelta(seconds=s) for s in range(num_rows_per_day)])
        vals = {c: random_floats(num_rows_per_day) for c in cols}
        new_df = pd.DataFrame(data=vals, index=index)

        dataframes.append(new_df)
        df = pd.concat((df, new_df))
        dt = dt + datetime.timedelta(days=1)

    random.shuffle(dataframes)
    for d in dataframes:
        lmdb_version_store.write(symbol, d, parallel=True)
    lmdb_version_store.version_store.sort_merge(symbol)
    vit = lmdb_version_store.read(symbol)
    df.sort_index(axis=1, inplace=True)
    result = vit.data
    result.sort_index(axis=1, inplace=True)
    assert_frame_equal(vit.data, df)


def test_sort_merge_append(lmdb_version_store_dynamic_schema):
    lib = lmdb_version_store_dynamic_schema
    num_rows_per_day = 10
    num_days = 10
    num_columns = 8
    column_length = 8
    dt = datetime.datetime(2019, 4, 8, 0, 0, 0)
    columns = random_strings_of_length(num_columns, column_length, True)
    symbol = "test_append_parallel_sort_merge"
    dataframes = []
    df = pd.DataFrame()
    for _ in range(num_days):
        cols = random.sample(columns, 4)
        index = pd.Index([dt + datetime.timedelta(seconds=s) for s in range(num_rows_per_day)])
        vals = {c: random_floats(num_rows_per_day) for c in cols}
        new_df = pd.DataFrame(data=vals, index=index)
        dataframes.append(new_df)
        df = pd.concat((df, new_df))
        dt = dt + datetime.timedelta(days=1)

    half_way = len(dataframes) / 2
    idx_dataframe = 0
    for d in dataframes:
        if idx_dataframe == 0:
            lib.write(symbol, d)
        else:
            lib.append(symbol, d)
        idx_dataframe += 1
        if idx_dataframe == half_way:
            break

    dataframes = dataframes[int(half_way) :]
    random.shuffle(dataframes)
    for d in dataframes:
        lib.write(symbol, d, parallel=True)

    lib.version_store.sort_merge(symbol, None, True)
    vit = lib.read(symbol)
    df.sort_index(axis=1, inplace=True)
    result = vit.data
    result.sort_index(axis=1, inplace=True)
    assert_frame_equal(vit.data, df)


def test_datetimes_to_nats(lmdb_version_store):
    num_rows_per_day = 10
    num_days = 10
    num_columns = 8
    column_length = 8
    dt = datetime.datetime(2019, 4, 8, 0, 0, 0)
    columns = random_strings_of_length(num_columns, column_length, True)
    symbol = "test_write_parallel_stress_schema_change"
    dataframes = []
    df = pd.DataFrame()

    for _ in range(num_days):
        cols = random.sample(columns, 4)
        index = pd.Index([dt + datetime.timedelta(seconds=s) for s in range(num_rows_per_day)])
        vals = {c: random_dates(num_rows_per_day) for c in cols}
        new_df = pd.DataFrame(data=vals, index=index)

        dataframes.append(new_df)
        df = pd.concat((df, new_df))
        dt = dt + datetime.timedelta(days=1)

    random.shuffle(dataframes)
    for d in dataframes:
        lmdb_version_store.write(symbol, d, parallel=True)

    lmdb_version_store.version_store.compact_incomplete(symbol, False, True)
    vit = lmdb_version_store.read(symbol)
    df.sort_index(axis=1, inplace=True)
    result = vit.data
    result.sort_index(axis=1, inplace=True)
    assert_frame_equal(vit.data, df)
