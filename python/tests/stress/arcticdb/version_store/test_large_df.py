"""
Copyright 2023 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""
import datetime
import random

import numpy as np
import pandas as pd

from arcticdb.util.test import (
    assert_frame_equal,
    dataframe_for_date,
    random_strings_of_length,
    random_floats,
    random_strings_of_length_with_nan,
)

def create_large_df(num_rows, start_time):
    index = pd.date_range(start=start_time, periods=num_rows, freq='ns')
    df = pd.DataFrame(data={"col": np.arange(num_rows, dtype=np.int8)}, index=index)
    return df

# We use map_size of at least 2**35 to allow storing 2**32 rows
def test_write_large_df_in_chunks(lmdb_version_store_very_big_map):
    symbol = "symbol"
    lib = lmdb_version_store_very_big_map
    chunk_size = 2**22
    num_chunks = 2**10
    start_time = pd.Timestamp(0)

    lib.delete(symbol)
    chunk = create_large_df(chunk_size, start_time)
    lib.write(symbol, chunk)
    del chunk

    for i in range(num_chunks-1):
        start_time = start_time + pd.Timedelta(chunk_size)
        chunk = create_large_df(chunk_size, start_time)
        lib.append(symbol, chunk)
        del chunk

    # Verify that number of rows is correct
    assert(lib.get_num_rows(symbol) == chunk_size*num_chunks)

    # Test that head works correctly
    expected_head = create_large_df(5, datetime.datetime.utcfromtimestamp(0))
    assert_frame_equal(lib.head(symbol).data, expected_head)

    # Test that update works correctly
    expected_head["col"] = np.array([7, 8, 9, 10, 11], dtype=np.int8)
    lib.update(symbol, expected_head)
    assert_frame_equal(lib.head(symbol).data, expected_head)

