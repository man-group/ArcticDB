"""
Copyright 2024 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

from math import log10
import time

import numpy as np
import pandas as pd
import pytest

from arcticdb import QueryBuilder, Arctic
from arcticdb.util.test import assert_frame_equal, random_strings_of_length

rows_per_segment = 100_000
rng = np.random.default_rng()

ac = Arctic("lmdb:///tmp/arcticdb")
lib = ac.get_library("resample_profiling", create_if_missing=True)
lib = lib._nvs


@pytest.mark.parametrize("num_rows", [100_000, 1_000_000, 10_000_000, 100_000_000, 1_000_000_000])
@pytest.mark.parametrize(
    "col_type",
    [
        "bool",
        "int",
        "float",
        "float_with_nans",
        "datetime",
        "datetime_with_nats",
        "str10",
        "str100",
        "str1000",
        "str10000",
        "str100000",
        "str_with_nones10",
    ],
)
def test_write_data(num_rows, col_type):
    power_of_ten = round(log10(num_rows))
    sym = f"10^{power_of_ten}_{col_type}"
    lib.delete(sym)

    num_segments = num_rows // rows_per_segment
    for idx in range(num_segments):
        index = pd.date_range(pd.Timestamp(idx * rows_per_segment, unit="us"), freq="us", periods=rows_per_segment)
        if col_type == "int":
            col_data = rng.integers(0, 100_000, rows_per_segment)
        elif col_type == "bool":
            col_data = rng.integers(0, 2, rows_per_segment)
            col_data = col_data.astype(np.bool)
        elif col_type.startswith("float"):
            col_data = 100_000 * rng.random(rows_per_segment)
            if col_type == "float_with_nans":
                col_data[: rows_per_segment // 2] = np.nan
                rng.shuffle(col_data)
        elif col_type.startswith("datetime"):
            col_data = rng.integers(0, 100_000, rows_per_segment)
            col_data = col_data.astype("datetime64[s]")
            if col_type == "datetime_with_nats":
                col_data[: rows_per_segment // 2] = np.datetime64("NaT")
                rng.shuffle(col_data)
        elif col_type.startswith("str"):
            num_unique_strings = int(col_type.lstrip("str_with_nones"))
            unique_strings = random_strings_of_length(num_unique_strings, 10, True)
            col_data = np.random.choice(unique_strings, rows_per_segment)
            if col_type.startswith("str_with_nones"):
                col_data[: rows_per_segment // 2] = None
                rng.shuffle(col_data)
        df = pd.DataFrame({"col": col_data}, index=index)
        lib.append(sym, df, write_if_missing=True)


@pytest.mark.parametrize("num_rows", [100_000, 1_000_000, 10_000_000, 100_000_000, 1_000_000_000])
@pytest.mark.parametrize(
    "col_type",
    [
        "bool",
        "int",
        "float",
        "float_with_nans",
        "datetime",
        "datetime_with_nats",
        "str10",
        "str100",
        "str1000",
        "str10000",
        "str100000",
        "str_with_nones10",
    ],
)
@pytest.mark.parametrize("freq", ["1us", "10us", "100us", "1ms", "10ms", "100ms", "1s", "10s", "100s", "1000s"])
@pytest.mark.parametrize("agg", ["sum", "mean", "min", "max", "first", "last", "count"])
def test_resample_data(num_rows, col_type, freq, agg):
    if col_type in ["datetime", "datetime_with_nats"] and agg == "sum":
        pytest.skip()
    if col_type.startswith("str") and agg in ["sum", "mean", "min", "max"]:
        pytest.skip()
    if num_rows == 100_000 and "freq" in ["1s", "10s", "100s", "1000s"]:
        pytest.skip()
    if num_rows == 1_000_000 and "freq" in ["10s", "100s", "1000s"]:
        pytest.skip()
    if num_rows == 10_000_000 and "freq" in ["100s", "1000s"]:
        pytest.skip()
    if num_rows == 100_000_000 and "freq" in ["1000s"]:
        pytest.skip()

    input_power_of_ten = round(log10(num_rows))
    sym = f"10^{input_power_of_ten}_{col_type}"

    start = time.time()
    df = lib.read(sym).data
    end = time.time()
    read_time = end - start

    q = QueryBuilder()
    q = q.resample(freq).agg({"col": agg})
    start = time.time()
    df = lib.read(sym, query_builder=q).data
    end = time.time()
    arcticdb_resample_time = end - start

    output_power_of_ten = round(log10(len(df)))

    results_sym = f"results_1_core_10^{input_power_of_ten}_to_10^{output_power_of_ten}_{col_type}_{agg}"
    results_df = pd.DataFrame(
        {
            "Input rows": [num_rows],
            "Output rows": [len(df)],
            "Column type": [col_type],
            "Aggregation": [agg],
            "Read time": [read_time],
            "Resample time": [arcticdb_resample_time],
        }
    )
    lib.write(results_sym, results_df)
    print(
        f"Downsampling ({agg}) 10^{input_power_of_ten}->10^{output_power_of_ten} rows of {col_type} took {end - start}"
    )


@pytest.mark.parametrize("num_rows", [100_000])
@pytest.mark.parametrize("col_type", ["int"])
@pytest.mark.parametrize("freq", ["10ns"])
@pytest.mark.parametrize("agg", ["sum"])
def test_resample_mostly_missing_buckets(num_rows, col_type, freq, agg):
    input_power_of_ten = round(log10(num_rows))
    sym = f"10^{input_power_of_ten}_{col_type}"

    q = QueryBuilder()
    q = q.resample(freq).agg({"col": agg})
    start = time.time()
    df = lib.read(sym, query_builder=q).data
    end = time.time()

    output_power_of_ten = round(log10(len(df)))

    print(
        f"Downsampling ({agg}) 10^{input_power_of_ten}->10^{output_power_of_ten} rows of {col_type} took {end - start}"
    )


@pytest.mark.parametrize("num_rows", [100_000_000])
@pytest.mark.parametrize("col_type", ["int"])
@pytest.mark.parametrize("freq", ["10us"])
def test_resample_all_aggs_one_column(num_rows, col_type, freq):
    input_power_of_ten = round(log10(num_rows))
    sym = f"10^{input_power_of_ten}_{col_type}"

    q = QueryBuilder()
    q = q.resample(freq).agg(
        {
            "sum": ("col", "sum"),
            "mean": ("col", "mean"),
            "min": ("col", "min"),
            "max": ("col", "max"),
            "first": ("col", "first"),
            "last": ("col", "last"),
            "count": ("col", "count"),
        }
    )
    start = time.time()
    df = lib.read(sym, query_builder=q).data
    end = time.time()

    output_power_of_ten = round(log10(len(df)))

    print(
        f"Downsampling (all aggregators) 10^{input_power_of_ten}->10^{output_power_of_ten} rows of {col_type} took {end - start}"
    )


@pytest.mark.parametrize("num_rows", [10_000_000, 100_000_000])
def test_write_ohlcvt(num_rows):
    power_of_ten = round(log10(num_rows))
    sym = f"10^{power_of_ten}_ohlcvt"
    lib.delete(sym)

    num_segments = num_rows // rows_per_segment
    for idx in range(num_segments):
        index = pd.date_range(pd.Timestamp(idx * rows_per_segment, unit="m"), freq="min", periods=rows_per_segment)
        df = pd.DataFrame(
            {
                "open": 100 * rng.random(rows_per_segment),
                "high": 100 * rng.random(rows_per_segment),
                "low": 100 * rng.random(rows_per_segment),
                "close": 100 * rng.random(rows_per_segment),
                "volume": rng.integers(0, 100_000, rows_per_segment),
                "trades": rng.integers(0, 1_000, rows_per_segment),
            },
            index=index,
        )
        lib.append(sym, df, write_if_missing=True)


@pytest.mark.parametrize("num_rows", [10_000_000, 100_000_000])
@pytest.mark.parametrize("freq", ["5min", "15min", "H", "D"])
def test_resample_ohlcvt(num_rows, freq):
    power_of_ten = round(log10(num_rows))
    sym = f"10^{power_of_ten}_ohlcvt"

    q = QueryBuilder()
    q = q.resample(freq).agg(
        {
            "open": "first",
            "high": "max",
            "low": "min",
            "close": "last",
            "volume": "sum",
            "trades": "sum",
        }
    )
    start = time.time()
    df = lib.read(sym, query_builder=q).data
    end = time.time()

    print(f"Downsampling OHLCVT {num_rows}->{len(df)} rows took {end - start}")


def test_write_wide_data():
    sym = f"wide_data"
    lib.delete(sym)

    num_rows = 2_600
    num_cols = 27_000

    index = pd.date_range("2000-01-01", periods=num_rows)
    cols = [f"col_{idx}" for idx in range(num_cols)]
    data = dict()
    for col in cols:
        data[col] = 100 * rng.random(num_rows, dtype=np.float64)
    df = pd.DataFrame(data, index=index)
    lib.write(sym, df)


def test_resample_wide_data():
    sym = f"wide_data"
    num_cols = 27_000
    cols = [f"col_{idx}" for idx in range(num_cols)]
    aggs = dict()
    for col in cols:
        aggs[col] = "last"
    q = QueryBuilder().resample("30D").agg(aggs)

    start = time.time()
    df = lib.read(sym, query_builder=q).data
    end = time.time()
    print(f"Downsampling wide df ({num_cols} columns) 2600->{len(df)} rows took {end - start}")
