"""
Copyright 2025 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

from arcticdb.util.utils import DFGenerator, generate_random_sparse_numpy_array
from arcticdb.util.test import assert_frame_equal, assert_series_equal
from arcticdb.version_store._store import NativeVersionStore
import numpy as np
import pandas as pd

def test_write_sparse_data(basic_store):
    nvs: NativeVersionStore = basic_store
    sym = "__qwerty124"

    arr_float = generate_random_sparse_numpy_array(1000, np.float64)
    arr_str = generate_random_sparse_numpy_array(1000, str)
    arr_datetime = generate_random_sparse_numpy_array(1000, np.datetime64)

    nvs.write(sym, arr_float)
    np.testing.assert_array_equal(arr_float, nvs.read(sym).data)
    nvs.write(sym, arr_str)
    np.testing.assert_array_equal(arr_str, nvs.read(sym).data)
    nvs.write(sym, arr_datetime)
    np.testing.assert_array_equal(arr_datetime, nvs.read(sym).data)

    ser_float = pd.Series(arr_float, name="float")
    nvs.write(sym, ser_float)
    assert_series_equal(ser_float, nvs.read(sym).data)

    ser_str = pd.Series(arr_str, name="str")
    nvs.write(sym, ser_str)
    assert_series_equal(ser_str, nvs.read(sym).data)

    ser_date = pd.Series(arr_datetime, name="date")
    nvs.write(sym, ser_date)
    assert_series_equal(ser_date, nvs.read(sym).data)

    df = (DFGenerator(size=4000, density=0.10)
          .add_float_col("float")
          .add_int_col("int")
          .add_bool_col("bool")
          .add_timestamp_col("ts")
          .add_string_col("str", str_size=18)
          .add_string_col("str2", num_unique_values=44, str_size=6)
          .generate_dataframe())
    
    nvs.write(sym, df)
    assert_frame_equal(df, nvs.read(sym).data)

