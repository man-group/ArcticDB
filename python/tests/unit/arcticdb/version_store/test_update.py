"""
Copyright 2023 Man Group Operations Ltd.
NO WARRANTY, EXPRESSED OR IMPLIED.
"""
from arcticdb.config import Defaults
import pandas as pd
import numpy as np
from arcticdb.version_store.helper import ArcticMemoryConfig
from pandas.testing import assert_frame_equal
import pandas.util.testing as tm
import pytest
from itertools import product
from arctic.date import DateRange
import datetime
import random
from arcticdb.util.test import random_strings_of_length, random_floats
from arcticdb_ext.exceptions import ArcticNativeCxxException


def test_update_single_dates(lmdb_version_store_dynamic_schema):
    lib = lmdb_version_store_dynamic_schema
    df1 = pd.DataFrame(index=[pd.Timestamp(2022, 1, 3)], data=2220103.0, columns=["a"])
    df2 = pd.DataFrame(index=[pd.Timestamp(2021, 12, 22)], data=211222.0, columns=["a"])
    df3 = pd.DataFrame(index=[pd.Timestamp(2021, 12, 29)], data=2211229.0, columns=["a"])
    sym = "data6"
    lib.update(sym, df1, upsert=True)
    lib.update(sym, df2, upsert=True)
    lib.update(sym, df3, upsert=True)

    expected = df2.append(df3).append(df1)
    assert_frame_equal(lib.read(sym).data, expected)


def test_update(arcticdb_test_lmdb_config, lib_name):
    col_per_group = 2
    row_per_segment = 2
    local_lib_cfg = arcticdb_test_lmdb_config(lib_name)
    lib = local_lib_cfg.env_by_id[Defaults.ENV].lib_by_path[lib_name]
    lib.version.write_options.column_group_size = col_per_group
    lib.version.write_options.segment_row_size = row_per_segment
    lmdb_version_store = ArcticMemoryConfig(local_lib_cfg, Defaults.ENV)[lib_name]

    symbol = "update_no_daterange"

    idx = pd.date_range("1970-01-01", periods=100, freq="D")
    df = pd.DataFrame({"a": range(len(idx))}, index=idx)
    lmdb_version_store.write(symbol, df)

    idx2 = pd.date_range("1970-01-12", periods=10, freq="D")
    df2 = pd.DataFrame({"a": range(1000, 1000 + len(idx2))}, index=idx2)
    lmdb_version_store.update(symbol, df2)

    vit = lmdb_version_store.read(symbol)
    df.update(df2)

    assert_frame_equal(vit.data.astype("float"), df)


def test_update_long_strides(lmdb_version_store):
    lib = lmdb_version_store
    symbol = "test_update_long_strides"

    write_df = pd.DataFrame({"A": 7 * [1]}, index=pd.date_range("2023-02-01", periods=7))
    assert write_df.index.values.strides[0] == 8
    lib.write(symbol, write_df)

    update_df = write_df[write_df.index.isin([pd.Timestamp(2023, 2, 1), pd.Timestamp(2023, 2, 6)])].copy()
    update_df["A"] = 999
    assert update_df.index.values.strides[0] == 40

    lib.update(symbol, update_df)

    expected = pd.DataFrame(
        {"A": [999, 999, 1]}, index=[pd.Timestamp(2023, 2, 1), pd.Timestamp(2023, 2, 6), pd.Timestamp(2023, 2, 7)]
    )
    received = lib.read(symbol).data
    pd.testing.assert_frame_equal(expected, received)


def gen_params():
    p = [
        list(range(2, 4)),
        list(range(-1, 1)),
        list(range(-1, 1)),
        list(range(17, 19)),
        list(range(7, 8)),
        list(range(5, 6)),
    ]
    return list(product(*p))


@pytest.mark.parametrize(
    "col_per_group, start_increment, end_increment, update_start, iterations, start_dist", gen_params()
)
def test_update_repeatedly_dynamic_schema(
    arcticdb_test_lmdb_config,
    col_per_group,
    start_increment,
    end_increment,
    update_start,
    iterations,
    start_dist,
    lib_name,
):
    row_per_segment = 2
    local_lib_cfg = arcticdb_test_lmdb_config(lib_name)
    lib = local_lib_cfg.env_by_id[Defaults.ENV].lib_by_path[lib_name]
    lib.version.write_options.column_group_size = col_per_group
    lib.version.write_options.segment_row_size = row_per_segment
    lib.version.write_options.dynamic_schema = True
    lmdb_version_store = ArcticMemoryConfig(local_lib_cfg, Defaults.ENV)[lib_name]

    symbol = "update_dynamic_schema"

    idx = pd.date_range("1970-01-01", periods=100, freq="D")
    df = pd.DataFrame({"a": range(len(idx))}, index=idx)
    lmdb_version_store.write(symbol, df)
    update_end = update_start + start_dist

    for x in range(iterations):
        adjust_start = x * start_increment
        adjust_end = x * end_increment
        update_date = "1970-01-{}".format(update_start + adjust_start)
        periods = (update_end + adjust_end) - update_start
        if periods <= 0:
            continue

        idx2 = pd.date_range(update_date, periods=periods, freq="D")
        df2 = pd.DataFrame({"a": range(1000 + x, 1000 + x + len(idx2))}, index=idx2)
        lmdb_version_store.update(symbol, df2)

        vit = lmdb_version_store.read(symbol)
        df.update(df2)
        assert_frame_equal(vit.data.astype("float"), df)


@pytest.mark.parametrize(
    "col_per_group, start_increment, end_increment, update_start, iterations, start_dist", gen_params()
)
def test_update_repeatedly_dynamic_schema_hashed(
    arcticdb_test_lmdb_config,
    col_per_group,
    start_increment,
    end_increment,
    update_start,
    iterations,
    start_dist,
    lib_name,
):
    row_per_segment = 2
    local_lib_cfg = arcticdb_test_lmdb_config(lib_name)
    lib = local_lib_cfg.env_by_id[Defaults.ENV].lib_by_path[lib_name]
    lib.version.write_options.column_group_size = col_per_group
    lib.version.write_options.segment_row_size = row_per_segment
    lib.version.write_options.dynamic_schema = True
    # lib.version.write_options.bucketize_dynamic = True
    lmdb_version_store = ArcticMemoryConfig(local_lib_cfg, Defaults.ENV)[lib_name]

    symbol = "update_dynamic_schema"

    idx = pd.date_range("1970-01-01", periods=100, freq="D")
    l = len(idx)
    df = pd.DataFrame(
        {"a": range(l), "b": range(1, l + 1), "c": range(2, l + 2), "d": range(3, l + 3), "e": range(4, l + 4)},
        index=idx,
    )

    lmdb_version_store.write(symbol, df)
    update_end = update_start + start_dist

    for x in range(iterations):
        adjust_start = x * start_increment
        adjust_end = x * end_increment
        update_date = "1970-01-{}".format(update_start + adjust_start)
        periods = (update_end + adjust_end) - update_start
        if periods <= 0:
            continue

        idx2 = pd.date_range(update_date, periods=periods, freq="D")
        l = len(idx2)
        df2 = pd.DataFrame(
            {
                "a": range(x, l + x),
                "b": range(1 + x, l + 1 + x),
                "c": range(2 + x, l + 2 + x),
                "d": range(3 + x, l + 3 + x),
                "e": range(4 + x, l + 4 + x),
            },
            index=idx2,
        )

        lmdb_version_store.update(symbol, df2)

        vit = lmdb_version_store.read(symbol)
        df.update(df2)
        assert_frame_equal(vit.data.astype("float"), df)


@pytest.mark.parametrize(
    "col_per_group, start_increment, end_increment, update_start, iterations, start_dist", gen_params()
)
def test_update_repeatedly(
    arcticdb_test_lmdb_config,
    col_per_group,
    start_increment,
    end_increment,
    update_start,
    iterations,
    start_dist,
    lib_name,
):
    row_per_segment = 2
    local_lib_cfg = arcticdb_test_lmdb_config(lib_name)
    lib = local_lib_cfg.env_by_id[Defaults.ENV].lib_by_path[lib_name]
    lib.version.write_options.column_group_size = col_per_group
    lib.version.write_options.segment_row_size = row_per_segment
    lmdb_version_store = ArcticMemoryConfig(local_lib_cfg, Defaults.ENV)[lib_name]

    symbol = "update_no_daterange"

    idx = pd.date_range("1970-01-01", periods=100, freq="D")
    df = pd.DataFrame({"a": range(len(idx))}, index=idx)
    lmdb_version_store.write(symbol, df)
    update_end = update_start + start_dist

    for x in range(iterations):
        adjust_start = x * start_increment
        adjust_end = x * end_increment
        update_date = "1970-01-{}".format(update_start + adjust_start)
        periods = (update_end + adjust_end) - update_start
        if periods <= 0:
            continue

        idx2 = pd.date_range(update_date, periods=periods, freq="D")
        df2 = pd.DataFrame({"a": range(1000 + x, 1000 + x + len(idx2))}, index=idx2)
        lmdb_version_store.update(symbol, df2)

        vit = lmdb_version_store.read(symbol)
        df.update(df2)
        assert_frame_equal(vit.data.astype("float"), df)


@pytest.mark.parametrize(
    "col_per_group, start_increment, end_increment, update_start, iterations, start_dist", gen_params()
)
def test_update_repeatedly_with_strings(
    arcticdb_test_lmdb_config,
    col_per_group,
    start_increment,
    end_increment,
    update_start,
    iterations,
    start_dist,
    lib_name,
):
    row_per_segment = 2
    local_lib_cfg = arcticdb_test_lmdb_config(lib_name)
    lib = local_lib_cfg.env_by_id[Defaults.ENV].lib_by_path[lib_name]
    lib.version.write_options.column_group_size = col_per_group
    lib.version.write_options.segment_row_size = row_per_segment
    lmdb_version_store = ArcticMemoryConfig(local_lib_cfg, Defaults.ENV)[lib_name]

    symbol = "update_no_daterange"

    idx = pd.date_range("1970-01-01", periods=100, freq="D")
    df = pd.DataFrame({"a": [tm.rands(10) for _ in range(len(idx))]}, index=idx)
    lmdb_version_store.write(symbol, df)
    update_end = update_start + start_dist

    for x in range(iterations):
        adjust_start = x * start_increment
        adjust_end = x * end_increment
        update_date = "1970-01-{}".format(update_start + adjust_start)
        periods = (update_end + adjust_end) - update_start
        if periods <= 0:
            continue

        idx2 = pd.date_range(update_date, periods=periods, freq="D")
        df2 = pd.DataFrame({"a": [tm.rands(10) for _ in range(len(idx2))]}, index=idx2)
        lmdb_version_store.update(symbol, df2)

        vit = lmdb_version_store.read(symbol)
        df.update(df2)
        assert_frame_equal(vit.data, df)


def test_update_with_snapshot(arcticdb_test_lmdb_config, lib_name):
    col_per_group = 2
    row_per_segment = 2
    local_lib_cfg = arcticdb_test_lmdb_config(lib_name)
    lib = local_lib_cfg.env_by_id[Defaults.ENV].lib_by_path[lib_name]
    lib.version.write_options.column_group_size = col_per_group
    lib.version.write_options.segment_row_size = row_per_segment
    lmdb_version_store = ArcticMemoryConfig(local_lib_cfg, Defaults.ENV)[lib_name]

    symbol = "update_no_daterange"

    idx = pd.date_range("1970-01-01", periods=100, freq="D")
    df = pd.DataFrame({"a": range(len(idx))}, index=idx)
    original_df = df.copy(deep=True)
    lmdb_version_store.write(symbol, df)

    lmdb_version_store.snapshot("my_snap")

    idx2 = pd.date_range("1970-01-12", periods=10, freq="D")
    df2 = pd.DataFrame({"a": range(1000, 1000 + len(idx2))}, index=idx2)
    lmdb_version_store.update(symbol, df2)

    assert_frame_equal(lmdb_version_store.read(symbol, as_of=0).data.astype("int"), original_df)
    assert_frame_equal(lmdb_version_store.read(symbol, as_of="my_snap").data.astype("int"), original_df)

    df.update(df2)

    vit = lmdb_version_store.read(symbol)
    assert_frame_equal(vit.data.astype("float"), df)
    assert_frame_equal(lmdb_version_store.read(symbol, as_of=1).data.astype("float"), df)
    assert_frame_equal(lmdb_version_store.read(symbol, as_of="my_snap").data.astype("int"), original_df)

    lmdb_version_store.delete(symbol)
    assert lmdb_version_store.list_versions() == []

    assert_frame_equal(lmdb_version_store.read(symbol, as_of="my_snap").data.astype("int"), original_df)


def generate_dataframe(columns, dt, num_days, num_rows_per_day):
    df = pd.DataFrame()
    for _ in range(num_days):
        index = pd.Index([dt + datetime.timedelta(seconds=s) for s in range(num_rows_per_day)])
        vals = {c: random_floats(num_rows_per_day) for c in columns}
        new_df = pd.DataFrame(data=vals, index=index)
        df = df.append(new_df)
        dt = dt + datetime.timedelta(days=1)

    return df


def test_update_with_daterange(lmdb_version_store):
    lib = lmdb_version_store

    def get_frame_for_date_range(start, end):
        df = pd.DataFrame(index=pd.date_range(start, end, freq="D"))
        df["value"] = df.index.day
        return df

    df1 = get_frame_for_date_range("2020-01-01", "2021-01-01")
    lib.write("test", df1)

    df2 = get_frame_for_date_range("2020-06-01", "2021-06-01")
    date_range = DateRange("2020-01-01", "2022-01-01")
    lib.update("test", df2, date_range=date_range)
    stored_df = lib.read("test").data
    assert stored_df.index.min() == df2.index.min()
    assert stored_df.index.max() == df2.index.max()


def test_update_schema_change(lmdb_version_store_dynamic_schema):
    dt = datetime.datetime(2019, 4, 8, 0, 0, 0)
    column_length = 6
    num_days = 10
    num_rows_per_day = 2
    num_columns = 8
    columns = random_strings_of_length(num_columns, column_length, True)
    symbol = "test_update_schema_change"

    df1 = generate_dataframe(random.sample(columns, 6), dt, num_days, num_rows_per_day)
    lmdb_version_store_dynamic_schema.write(symbol, df1)
    df2 = generate_dataframe(columns, dt, num_days, num_rows_per_day)

    block_size = 6
    # offsets =  np.random.randint(len(df2), size=num_samples)
    offsets = [7]
    samples = [df2.iloc[x : x + block_size] for x in offsets]

    for sample in samples:
        missing = df1.columns.symmetric_difference(sample.columns)
        for col in missing:
            df1[col] = np.nan

        df1.update(sample)
        lmdb_version_store_dynamic_schema.update(symbol, sample)

    vit = lmdb_version_store_dynamic_schema.read(symbol)
    df1.sort_index(axis=1, inplace=True)
    result = vit.data
    result.sort_index(axis=1, inplace=True)
    assert_frame_equal(df1, vit.data)


def test_update_schema_change_with_params(lmdb_version_store_dynamic_schema):
    lib = lmdb_version_store_dynamic_schema

    dt = datetime.datetime(2019, 4, 8, 0, 0, 0)
    column_length = 6
    num_days = 10
    num_rows_per_day = 2
    num_columns = 8
    columns = random_strings_of_length(num_columns, column_length, True)
    symbol = "test_update_schema_change"

    df1 = generate_dataframe(random.sample(columns, 6), dt, num_days, num_rows_per_day)
    lib.write(symbol, df1)
    df2 = generate_dataframe(columns, dt, num_days, num_rows_per_day)

    block_size = 6
    num_samples = 1
    # offsets =  np.random.randint(len(df2), size=num_samples)
    offsets = [7]
    samples = [df2.iloc[x : x + block_size] for x in offsets]

    for sample in samples:
        missing = df1.columns.symmetric_difference(sample.columns)
        for col in missing:
            df1[col] = np.nan

        df1.update(sample)
        lib.update(symbol, sample, dynamic_schema=True)

    vit = lib.read(symbol, dynamic_schema=True)
    df1.sort_index(axis=1, inplace=True)
    result = vit.data
    result.sort_index(axis=1, inplace=True)
    assert_frame_equal(df1, vit.data)


def test_update_single_line(lmdb_version_store_dynamic_schema):
    dt = datetime.datetime(2019, 4, 8, 0, 0, 0)
    column_length = 6
    num_days = 1000
    num_rows_per_day = 1
    num_columns = 8
    columns = random_strings_of_length(num_columns, column_length, True)
    symbol = "test_update_schema_change"

    df = generate_dataframe(columns, dt, num_days, num_rows_per_day)
    x = [False] * len(df)

    first = True

    while not all(x):
        start = random.randrange(0, len(df))
        try:
            idx = x.index(False, start)
        except ValueError:
            pass

        x[idx] = True
        if first:
            lmdb_version_store_dynamic_schema.write(symbol, df[idx:])
            first = False
        else:
            lmdb_version_store_dynamic_schema.update(symbol, df[idx:])

    vit = lmdb_version_store_dynamic_schema.read(symbol)
    df.sort_index(axis=1, inplace=True)
    result = vit.data
    result.sort_index(axis=1, inplace=True)
    assert_frame_equal(df, vit.data)


def test_update_pickled_data(lmdb_version_store):
    symbol = "test_update_pickled_data"
    idx = pd.date_range("2000-01-01", periods=3)
    df = pd.DataFrame({"a": [[1, 2], [3, 4], [5, 6]]}, index=idx)
    lmdb_version_store.write(symbol, df, pickle_on_failure=True)
    assert lmdb_version_store.is_symbol_pickled(symbol)
    df2 = pd.DataFrame({"a": [1000]}, index=idx[1:2])
    with pytest.raises(ArcticNativeCxxException) as e_info:
        lmdb_version_store.update(symbol, df2)
