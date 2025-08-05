"""
Copyright 2023 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""


import datetime
import pandas as pd
import numpy as np
import pytest

from arcticdb.util.arctic_simulator import ArcticSymbolSimulator, calculate_different_and_common_parts
from arcticdb.util.test import assert_frame_equal
from arcticdb.util.utils import verify_dynamically_added_columns


def test_simulator_append_basic_test_range_index():
    
    df1 = pd.DataFrame({
    'A': [1, 2],
    'B': [3, 4],
    'C': [5, 6]
    })

    df2 = pd.DataFrame({
        'B': [7, 8],
        'C': [9, 0],
        'D': [11, 12]
    })


    df_expected = pd.DataFrame({
        'A': [1, 2, 0, 0],
        'B': [3, 4, 7, 8],
        'C': [5, 6, 9, 0],
        'D': [0, 0, 11, 12],
    })

    asym = ArcticSymbolSimulator(keep_versions=True)
    asym.write(df1)
    asym.append(df2)
    df_result = asym.read()

    assert_frame_equal(df_expected, df_result)
    assert_frame_equal(df1, asym.read(as_of=0))


def test_simulator_append_basic_test_timestamp_index():
    # Create timestamp index starting from now
    start_time = datetime.datetime.now()
    df1_index = pd.date_range(start=start_time, periods=2, freq='D')
    df2_index = pd.date_range(start=start_time + datetime.timedelta(days=2), periods=2, freq='D')

    df1 = pd.DataFrame({
        'A': [1, 2],
        'B': [3, 4],
        'C': [5, 6]
    }, index=df1_index)

    df2 = pd.DataFrame({
        'B': [7, 8],
        'C': [9, 0],
        'D': [11, 12]
    }, index=df2_index)

    all_index = df1_index.append(df2_index)
    df_expected = pd.DataFrame({
        'A': [1, 2, 0, 0],
        'B': [3, 4, 7, 8],
        'C': [5, 6, 9, 0],
        'D': [0, 0, 11, 12]
    }, index=all_index)

    asym = ArcticSymbolSimulator()
    df_result = asym.simulate_arctic_append(df1, df2)
    assert_frame_equal(df_expected, df_result)
    

def test_simulator_update_all_types_check_simulator_versions_store():
    index_dates = pd.date_range(start=datetime.datetime(2025, 8, 1), periods=5, freq="D")

    df = pd.DataFrame({
        "int_col": [10, 20, 30, 40, 50],
        "float_col": [1.5, 2.5, 3.5, 4.5, 5.5],
        "bool_col": [True, False, True, False, True],
        "str_col": ["a", "b", "c", "d", "e"],
        "timestamp_col": index_dates + pd.to_timedelta(2, unit="h")
    }, index=index_dates)

    index_dates = pd.date_range(start=datetime.datetime(2025, 7, 18), periods=1, freq="D")
    df1 = pd.DataFrame({
        "int_col": [111],
        "float_col": [111.0],
        "bool_col": [False],
        "str_col": ["Z"],
        "timestamp_col": index_dates + pd.to_timedelta(2, unit="h")
    }, index=index_dates)

    index_dates = pd.date_range(start=datetime.datetime(2025, 6, 18), periods=1, freq="D")
    df2 = pd.DataFrame({
        "int_col": [111],
        "float_col": [111.0],
        "bool_col": [False],
        "str_col": ["Z"],
        "timestamp_col": index_dates + pd.to_timedelta(2, unit="h"),
        "int_col1": [111],
        "float_col1": [111.0],
        "bool_col1": [False],
        "str_col1": ["Z"],
        "timestamp_col1": index_dates + pd.to_timedelta(2, unit="h")
    }, index=index_dates)

    # Now df2 is one row and earliest timestamp
    # Now df1 is one row and second earliest timestamp
    # Now df is several rows and latest timestamp
    # so from timeline perspective it is this timeline df2, df1, df

    asym = ArcticSymbolSimulator(keep_versions=True, dynamic_schema=True)
    asym.write(df)
    asym.update(df1)
    
    assert df.shape[0] + df1.shape[0] == asym.read().shape[0] # Result dataframe is combination of both
    assert_frame_equal(df1, asym.read().iloc[[0]]) # First row is updated
    assert_frame_equal(df, asym.read(as_of=1).iloc[1:]) # df starts from 2nd row
    assert_frame_equal(df, asym.read(as_of=0)) # First version is df

    asym.update(df2)
    assert df.shape[0] + df1.shape[0] + df1.shape[0] == asym.read().shape[0] 
    assert_frame_equal(df2, asym.read().iloc[[0]]) # First row is updated
    # Verify new columns added to first line from previous version are correct
    new_cols = calculate_different_and_common_parts(df2, df1)[0].columns.to_list()
    verify_dynamically_added_columns(asym.read(), df1.index[0], new_cols)
    # Verify new columns added to second line from previous version are correct
    new_cols = calculate_different_and_common_parts(df2, df)[0].columns.to_list()
    verify_dynamically_added_columns(asym.read(), df.index[1], new_cols)

    asym = ArcticSymbolSimulator(keep_versions=True, dynamic_schema=False)
    asym.write(df)
    asym.update(df1)
    # Update with static schema will not pass
    with pytest.raises(AssertionError):
        asym.update(df2)


def test_simulator_append_series():
    s1 = pd.Series([10, 20, 30], name="name", index=pd.to_datetime(['2023-01-01', '2023-01-02', '2023-01-03']))
    s2 = pd.Series([40, 50], name="name", index=pd.to_datetime(['2023-01-04', '2023-01-05']))
    asym = ArcticSymbolSimulator()
    asym.write(s1)
    asym.append(s2)
    assert 5 == len(asym.read())
    assert 10 == asym.read()["name"].iloc[0]
    assert 50 == asym.read()["name"].iloc[-1]

    s1 = pd.Series([10, 20, 30], name="name")
    s2 = pd.Series([40, 50], name="name")
    asym.write(s1)
    asym.append(s2)
    assert 5 == len(asym.read())
    assert 10 == asym.read()["name"].iloc[0]
    assert 50 == asym.read()["name"].iloc[-1]


def test_simulator_append_series_and_dataframe(lmdb_library_dynamic_schema):
    lib = lmdb_library_dynamic_schema
    asym = ArcticSymbolSimulator().associate_arctic_lib(lib)
    s1 = pd.Series([10, 20, 30], name="name")
    s2 = pd.Series([100, 200, 300], name="name")
    df1 = pd.DataFrame({
        'A': [1, 2],
        'B': [3, 4],
        'C': [5, 6]
    })
    asym.write(s1).arctic_lib().write("s", s1)
    asym.append(df1).arctic_lib().append("s", df1)
    asym.assert_equal_to_associated_lib("s")
    asym.append(s2).arctic_lib().append("s", s2)
    
