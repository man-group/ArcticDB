"""
Copyright 2023 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""
import datetime
import random
import pandas as pd

from arcticdb.util.test import (
    assert_frame_equal,
    dataframe_for_date,
    random_strings_of_length,
    random_floats,
    random_strings_of_length_with_nan,
)


def test_append_stress(object_version_store):
    num_rows_per_day = 1000
    num_days = 10
    identifier_length = 8
    dt = datetime.datetime(2019, 4, 8, 0, 0, 0)
    identifiers = random_strings_of_length(num_rows_per_day, identifier_length, True)

    symbol = "test_append_stress"
    df = dataframe_for_date(dt, identifiers)
    object_version_store.write(symbol, df)

    for i in range(num_days):
        dt = dt + datetime.timedelta(days=1)
        new_df = dataframe_for_date(dt, identifiers)
        object_version_store.append(symbol, new_df)
        df = pd.concat((df, new_df))
        vit = object_version_store.read(symbol)
        assert_frame_equal(vit.data, df)


def test_write_parallel_stress(object_version_store):
    num_rows_per_day = 1000
    num_days = 100
    identifier_length = 8
    dt = datetime.datetime(2019, 4, 8, 0, 0, 0)
    identifiers = random_strings_of_length(num_rows_per_day, identifier_length, True)

    symbol = "test_write_parallel_stress"
    df = dataframe_for_date(dt, identifiers)
    dataframes = [df]

    for _ in range(num_days):
        dt = dt + datetime.timedelta(days=1)
        new_df = dataframe_for_date(dt, identifiers)
        dataframes.append(new_df)
        df = pd.concat((df, new_df))

    random.shuffle(dataframes)
    for d in dataframes:
        object_version_store.write(symbol, d, parallel=True)

    object_version_store.version_store.compact_incomplete(symbol, False, False)
    vit = object_version_store.read(symbol)
    assert_frame_equal(vit.data, df)


def test_write_parallel_stress_schema_change(object_version_store):
    num_rows_per_day = 1000
    num_days = 100
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
        object_version_store.write(symbol, d, parallel=True)

    object_version_store.version_store.compact_incomplete(symbol, False, False)
    vit = object_version_store.read(symbol)
    df.sort_index(axis=1, inplace=True)
    result = vit.data
    result.sort_index(axis=1, inplace=True)
    assert_frame_equal(vit.data, df)


def test_write_parallel_stress_schema_change_strings(basic_store_large_data):
    num_rows_per_day = 1000
    num_days = 100
    num_columns = 8
    column_length = 8
    dt = datetime.datetime(2019, 4, 8, 0, 0, 0)
    string_length = 6
    columns = random_strings_of_length(num_columns, column_length, True)
    symbol = "test_write_parallel_stress_schema_change_strings"
    dataframes = []
    df = pd.DataFrame()

    for _ in range(num_days):
        cols = random.sample(columns, 4)
        index = pd.Index([dt + datetime.timedelta(seconds=s) for s in range(num_rows_per_day)])
        vals = {c: random_strings_of_length(num_rows_per_day, string_length, False) for c in cols}
        new_df = pd.DataFrame(data=vals, index=index)

        dataframes.append(new_df)
        df = pd.concat((df, new_df))
        dt = dt + datetime.timedelta(days=1)

    random.shuffle(dataframes)
    for d in dataframes:
        basic_store_large_data.write(symbol, d, parallel=True)

    basic_store_large_data.version_store.compact_incomplete(symbol, False, False)
    vit = basic_store_large_data.read(symbol)
    df.sort_index(axis=1, inplace=True)
    result = vit.data
    result.sort_index(axis=1, inplace=True)
    assert_frame_equal(vit.data, df)


def test_write_parallel_stress_schema_change_strings_with_nan(basic_store_large_data):
    num_rows_per_day = 1000
    num_days = 100
    num_columns = 8
    column_length = 8
    dt = datetime.datetime(2019, 4, 8, 0, 0, 0)
    string_length = 6
    columns = random_strings_of_length(num_columns, column_length, True)
    symbol = "test_write_parallel_stress_schema_change_strings"
    dataframes = []
    df = pd.DataFrame()

    for _ in range(num_days):
        cols = random.sample(columns, 4)
        index = pd.Index([dt + datetime.timedelta(seconds=s) for s in range(num_rows_per_day)])
        vals = {c: random_strings_of_length_with_nan(num_rows_per_day, string_length) for c in cols}
        new_df = pd.DataFrame(data=vals, index=index)

        dataframes.append(new_df)
        df = pd.concat((df, new_df))
        dt = dt + datetime.timedelta(days=1)

    random.shuffle(dataframes)
    for d in dataframes:
        basic_store_large_data.write(symbol, d, parallel=True)

    basic_store_large_data.version_store.compact_incomplete(symbol, False, False)
    vit = basic_store_large_data.read(symbol)
    df.sort_index(axis=1, inplace=True)
    result = vit.data
    result.sort_index(axis=1, inplace=True)
    assert_frame_equal(vit.data, df)


def test_change_to_dynamic_strings(object_version_store):
    num_rows_per_day = 1000
    num_days = 100
    num_columns = 5
    column_length = 8
    dt = datetime.datetime(2019, 4, 8, 0, 0, 0)
    string_length = 6
    columns = random_strings_of_length(num_columns, column_length, True)
    symbol = "test_change_to_dynamic_strings"
    df = pd.DataFrame()

    for i in range(num_days):
        index = pd.Index([dt + datetime.timedelta(seconds=s) for s in range(num_rows_per_day)])
        vals = {c: random_strings_of_length(num_rows_per_day, string_length, False) for c in columns}
        new_df = pd.DataFrame(data=vals, index=index)
        if i < num_days / 2:
            object_version_store.append(symbol, new_df, dynamic_strings=False, write_if_missing=True)
        else:
            object_version_store.append(symbol, new_df, dynamic_strings=True)
        df = pd.concat((df, new_df))
        dt = dt + datetime.timedelta(days=1)

    vit = object_version_store.read(symbol)
    assert_frame_equal(vit.data, df)


def test_write_parallel_stress_many_chunks(object_version_store):
    num_rows_per_day = 10
    num_days = 10
    num_columns = 8
    column_length = 4
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
        object_version_store.write(symbol, d, parallel=True)

    object_version_store.version_store.compact_incomplete(symbol, False, False)
    vit = object_version_store.read(symbol)
    df.sort_index(axis=1, inplace=True)
    result = vit.data
    result.sort_index(axis=1, inplace=True)
    assert_frame_equal(vit.data, df)
