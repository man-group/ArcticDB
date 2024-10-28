"""
Copyright 2023 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

import datetime
import random
import pytest

import time
import numpy as np
import pandas as pd

from tests.util.mark import SLOW_TESTS_MARK

from arcticdb.util.test import (
    assert_frame_equal,
    dataframe_for_date,
    random_strings_of_length,
    random_floats,
    random_strings_of_length_with_nan,
)


def create_large_df(num_rows, start_time):
    index = pd.date_range(start=start_time, periods=num_rows, freq="ns")
    df = pd.DataFrame(data={"col": np.arange(num_rows, dtype=np.int8)}, index=index)
    return df


# We use map_size of at least 2**35 to allow storing 2**32 rows.
# This test is similar to the one below but it uses a time index so we can test that update also works.
# The problem with that is that the time index takes up too much storage and it fails on the github runners.
# TODO: If we get more storage on github runners we can reenable this test and remove the below
@pytest.mark.skip(reason="Uses too much storage and fails on GitHub runners")
def test_write_and_update_large_df_in_chunks(lmdb_version_store_very_big_map):
    symbol = "symbol"
    lib = lmdb_version_store_very_big_map
    chunk_size = 2**22
    num_chunks = 2**10
    start_time = pd.Timestamp(0)

    chunk = create_large_df(chunk_size, start_time)
    lib.write(symbol, chunk)
    del chunk

    for i in range(num_chunks - 1):
        start_time = start_time + pd.Timedelta(chunk_size)
        chunk = create_large_df(chunk_size, start_time)
        lib.append(symbol, chunk)
        del chunk

    # Verify that number of rows is correct
    assert lib.get_num_rows(symbol) == chunk_size * num_chunks

    # Test that head works correctly
    expected_head = create_large_df(5, datetime.datetime.utcfromtimestamp(0))
    assert_frame_equal(lib.head(symbol).data, expected_head)

    # Test that update works correctly
    expected_head["col"] = np.array([7, 8, 9, 10, 11], dtype=np.int8)
    lib.update(symbol, expected_head)
    assert_frame_equal(lib.head(symbol).data, expected_head)


@SLOW_TESTS_MARK
def test_write_large_df_in_chunks(lmdb_version_store_big_map):
    symbol = "symbol"
    lib = lmdb_version_store_big_map
    chunk_size = 2**22
    num_chunks = 2**10

    all_zeros = pd.DataFrame({"col": np.zeros(chunk_size)})
    lib.write(symbol, all_zeros)

    for i in range(num_chunks - 1):
        lib.append(symbol, all_zeros)

    # Verify that number of rows is correct
    assert lib.get_num_rows(symbol) == chunk_size * num_chunks

    assert_frame_equal(lib.head(symbol).data, all_zeros.head(5))


def test_write_read_time_large_df(lmdb_version_store_big_map):
    symbol = "timeseries_data"
    lib = lmdb_version_store_big_map
    #total_rows = 1000000000
    total_rows = 100000
    start_dt = pd.Timestamp("2020-01-01")
    df = pd.DataFrame({
        "bitpacked": np.random.randint(0, 5, total_rows),
        "ffor": np.random.randint(100, 105, total_rows)
    }, index=pd.date_range(start=start_dt, periods=total_rows, freq='S'))

    write_start = time.time()
    lib.write(symbol, df)
    write_time = time.time() - write_start

    assert lib.get_num_rows(symbol) == total_rows

    read_start = time.time()
    df_read = lib.read(symbol)
    read_time = time.time() - read_start
    rows_per_second = total_rows / read_time

   # assert_frame_equal(df_read.head(5), df.head(5))
    #assert df_read.index.is_monotonic_increasing

    print(f"Write time: {write_time:.2f} seconds, Read time: {read_time:.2f} seconds, Rows/Second read: {rows_per_second:.2f}")



