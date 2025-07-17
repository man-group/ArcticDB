"""
Copyright 2023 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

import re
from typing import Set
import pytest
import pandas as pd
import numpy as np

from arcticdb.util.test import (
    assert_series_equal,
    dataframe_simulate_arcticdb_update_static,
)
from arcticdb.util.utils import ArcticTypes, generate_random_series, set_seed, supported_types_list, verify_dynamically_added_columns
from arcticdb.version_store._store import NativeVersionStore, VersionedItem
from datetime import timedelta, timezone

from arcticdb.exceptions import (
    ArcticNativeException,
)
from arcticdb_ext.exceptions import (
    InternalException,
    NormalizationException,
)

from benchmarks.bi_benchmarks import assert_frame_equal
from tests.util.mark import SLOW_TESTS_MARK


def add_index(df: pd.DataFrame, start_time: pd.Timestamp):
    df.index = pd.date_range(start_time, periods=df.shape[0], freq='s')


def make_df_appendable(original_df: pd.DataFrame, append_df:pd.DataFrame):
    """ Creates such index for `append_df` so that it can be appended to `original_df`"""
    last_time = original_df.index[-1]
    add_index(append_df, last_time + timedelta(seconds=1))


def create_all_arcticdb_types_df(length: int, column_prefix: str = ""):
    """ Creates a dataframe with columns of all supported arcticdb data types."""
    arr = []
    for dtype in supported_types_list:
        name = f"{column_prefix}_{np.dtype(dtype).name}"
        arr.append(generate_random_series(dtype, length, name, seed=None))
    return pd.concat(arr, axis=1)


def wrap_df_add_new_columns(df: pd.DataFrame, prefix: str = ""):
    """ Adds columns at the beginning and at the end of dataframe 
    Columns added are of all supported arcticdb types and have
    specified prefix
    """
    length = df.shape[0]
    df1 = create_all_arcticdb_types_df(length, column_prefix=f"{prefix}_pre_")
    df1.index = df.index
    df2 = create_all_arcticdb_types_df(length, column_prefix=f"{prefix}_post_")
    df2.index = df.index
    df = pd.concat([df1, df, df2], axis=1)
    return df


def get_metadata():
    '''Returns weird and complex metadata'''
    metadata = {
        "experiment": {
            "id": 42,
            "params": {
                "learning_rate": 0.01,
                "batch_size": 32
            }
        },
        "source": "integration_test",
        "tags": ["v1.2", "regression"]
    }
    return [metadata, {}, [metadata, {}, [metadata, {}], 1], "", None]


@SLOW_TESTS_MARK
@pytest.mark.parametrize("dtype", supported_types_list)
@pytest.mark.parametrize("schema", [True, False])
def test_write_append_update_read_scenario_with_different_series_combinations(version_store_factory, dtype, schema):
    """This test covers series with timestamp index of all supported arcticdb types.
    Write, append and read combinations of different boundary length sizes of series

    Verifies:
     - append/update operations over symbol containing timestamped series
     - append/update operations with different types of series - empty, one element, many elements
     - tests repeated over each supported arcticdb type - ints, floats, str, bool, datetime
     - tests work as expected over static and dynamic schema with small segment row size 
    """
    segment_row_size = 3
    lib: NativeVersionStore = version_store_factory(dynamic_schema=schema, segment_row_size=segment_row_size)
    set_seed(3484356)
    max_length = segment_row_size * 29
    symbol = f"symbol-{re.sub(r'[^A-Za-z0-9]', '_', str(dtype))}"
    name = "some_name!"
    timestamp = pd.Timestamp(4839275892348)
    series_length = [ 1, 0, 2, max_length]
    meta = get_metadata()

    for length in series_length:
        total_length = 0
        series = generate_random_series(dtype, length, name, start_time=timestamp, seed=None)
        total_length += length
        lib.write(symbol, series)
        result_series = series
        assert_series_equal(lib.read(symbol).data, series, check_index_type=(len(series) > 0))
        
        for append_series_length in series_length:
            append_series = generate_random_series(dtype, append_series_length, name, 
                                                   start_time=timestamp + timedelta(seconds=total_length), seed=None)
            lib.append(symbol, append_series, metadata=meta)
            result_series = pd.concat([result_series, append_series])
            ver = lib.read(symbol)
            assert_series_equal(result_series, ver.data, check_index_type=(len(result_series) > 0))
            assert meta == ver.metadata
            # Note update is of same length but starts in previous period

            update_series = generate_random_series(dtype, append_series_length, name, 
                                                   start_time=timestamp + timedelta(seconds=total_length - 1), seed=None)
            total_length += append_series_length
            lib.update(symbol, update_series, metadata=meta)
            result_series = dataframe_simulate_arcticdb_update_static(result_series, update_series)
            ver = lib.read(symbol)
            assert_series_equal(result_series, ver.data, check_index_type=(len(result_series) > 0))
            assert meta == ver.metadata


@pytest.mark.storage
def test_append_update_dynamic_schema_add_columns_all_types(version_store_and_real_s3_basic_store_factory):
    """
    The test does series of append operations with new columns of all supported column types.
    The resulting symbol will have additional columns each time added, with predefined default 
    values for different data types

    Verifies:
     - continuous appends/updates with new columns works as expected across dynamic and static schema
     - updates/appends with new columns and combinations incomplete, metadata and prune previous
       always deliver desired result
    """

    set_seed(32432)
    counter = 1
    lib: NativeVersionStore = version_store_and_real_s3_basic_store_factory(
        dynamic_schema=True, dynamic_strings=True, segment_row_size=3)
    symbol = "232_43213dfmkd_!"
    col_name = 'MIDDLE'
    start_time = pd.Timestamp(32513454)

    def get_df() -> pd.DataFrame:
        """ Creates new dataframe with one constant one column and one row where the value is 
        auto incremented on each new dataframe created"""
        nonlocal counter
        df = pd.DataFrame({col_name: [counter]})
        counter += 1
        return df

    initial_df = get_df()
    add_index(initial_df, start_time)
    lib.write(symbol, initial_df)
    read_data = lib.read(symbol).data

    # Will do append operation times to confirm growth is not a problem
    for i in range(10):
        num_versions_before = len(lib.list_versions(symbol))
        middle_df = get_df()
        add_index(middle_df, start_time)
        append_df = wrap_df_add_new_columns(middle_df, i)
        make_df_appendable(read_data, append_df) # timestamps of append_df to be after the initial

        # Get new columns that will be added to appended rows
        new_columns_to_appended_df = set(read_data.columns.to_list())
        # Remove the common column
        new_columns_to_appended_df.remove(col_name)
        # Get new columns that are added with this append
        new_columns_to_symbol = set(append_df.columns.to_list())
        # Remove the common column
        new_columns_to_symbol.remove(col_name)

        # Do append with different mixes of parameters
        if i % 2 == 0:
            meta = get_metadata()
            lib.append(symbol, append_df, prune_previous_version=True, validate_index=True, metadata=meta)
            assert num_versions_before, lib.list_versions(symbol) # previous version is pruned
        else:
            meta = None
            # NOTE: metadata is not stored then incomplete=True
            lib.append(symbol, append_df, incomplete=True, prune_previous_version=False, validate_index=True, metadata=meta)
            lib.compact_incomplete(symbol, True, False)
            assert num_versions_before, lib.list_versions(symbol) + 1 # previous version is not pruned

        # Verify  metadata and dynamically added columns
        ver = lib.read(symbol)
        read_data:pd.DataFrame = ver.data
        assert meta == ver.metadata
        verify_dynamically_added_columns(read_data, -2, new_columns_to_symbol)
        verify_dynamically_added_columns(read_data, -1, new_columns_to_appended_df)

    # Will only update last row with 1 row dataframe
    update_timestamp = read_data.index[-1]
    # Will take previous to the update row timestamp
    previous_timestamp = read_data.index[-2]

    # repeat update operation several times, to confirm growth is not a problem
    for i in range(5):
        num_versions_before = len(lib.list_versions(symbol))
        middle_df = get_df()
        add_index(middle_df, update_timestamp)
        update_df = wrap_df_add_new_columns(middle_df, f"upd_{i}")

        # Get new columns that will be added to updated rows
        new_columns_to_update_df = set(read_data.columns.to_list())
        # Remove the common column
        new_columns_to_update_df.remove(col_name)
        # Get new columns that are added with this update
        new_columns_to_symbol = set(update_df.columns.to_list())
        # Remove the common column
        new_columns_to_symbol.remove(col_name)

        if i % 2 == 0:
            meta = get_metadata()
            lib.update(symbol, update_df, prune_previous_version=False, metadata=meta)
            assert num_versions_before, lib.list_versions(symbol) + 1 # previous version is not pruned
        else:
            meta = "just a message"
            lib.update(symbol, update_df, prune_previous_version=True, metadata=meta)
            assert num_versions_before, lib.list_versions(symbol) # previous version is pruned

        # Verify  metadata and dynamically added columns
        ver = lib.read(symbol)
        read_data:pd.DataFrame = ver.data
        assert meta == ver.metadata
        verify_dynamically_added_columns(read_data, previous_timestamp, new_columns_to_symbol)
        verify_dynamically_added_columns(read_data, update_timestamp, new_columns_to_update_df)


@pytest.mark.parametrize("schema", [True, False])
@pytest.mark.storage
def test_append_scenario_error_messages_and_exceptions(version_store_and_real_s3_basic_store_factory, schema):
    """
    Test error messages and exception types for various failure scenarios.
    
    Verifies:
    - Appropriate exception types are raised
    - Edge cases are handled gracefully
    - Exceptions are same across all storage types
    """
    lib: NativeVersionStore = version_store_and_real_s3_basic_store_factory(
            dynamic_schema=True, dynamic_strings=schema, segment_row_size=1)
    symbol = "test_append_errors"
    
    df = pd.DataFrame({'value': [1, 2, 3]}, index=pd.date_range('2023-01-01', periods=3, freq='s'))
    df_2 = pd.DataFrame({'value': [3, 4, 5]}, index=pd.date_range('2023-01-02', periods=3, freq='s'))
    df_empty = df.iloc[0:0]
    df_same_index = pd.DataFrame({'value': [10, 982]}, index=pd.date_range('2023-01-01', periods=2, freq='s'))
    df_different_index = pd.DataFrame(
        {'value': [4, 5, 6]}, 
        index=['a', 'b', 'c']  # String index instead of datetime
    )
    df_no_index = pd.DataFrame(
        {'value': [4, 5, 6]}
    )

    # Test append to non-existent symbol
    for sym in [symbol, None, ""]:
        try:
            lib.append(symbol)
            assert False, "Expected exception was not raised"
        except Exception as e:
            # Verify exception type and message content
            assert isinstance(e, TypeError)
    assert len(lib.list_symbols()) == 0

    # Empty dataframe will create symbol with one version
    version = lib.append(symbol, df_empty)
    assert len(lib.list_symbols()) == 1
    assert len(lib.list_versions()) == 1
    assert_frame_equal(df_empty, lib.read(symbol).data)

    # Create symbol for further tests
    lib.write(symbol, df)
    assert len(lib.list_symbols()) == 1
    assert len(lib.list_versions()) == 2
    assert_frame_equal(df, lib.read(symbol).data)
    
    # Test append with invalid data type
    with pytest.raises(ArcticNativeException):
        lib.append(symbol, "invalid_data_type")
    assert len(lib.list_symbols()) == 1
    assert len(lib.list_versions()) == 2
    
    # Test append with mismatched index type
    for frame in [df_different_index, df_no_index]:
        with pytest.raises((NormalizationException)):
            lib.append(symbol, frame)
    assert len(lib.list_symbols()) == 1
    assert len(lib.list_versions()) == 2

    # Test append overlapping indexes
    for frame in [df_same_index]:
        with pytest.raises((InternalException)):
            lib.append(symbol, frame)
    assert len(lib.list_symbols()) == 1
    assert len(lib.list_versions()) == 2

    before_append = pd.Timestamp.now(tz=timezone.utc).value 
    result = lib.append(symbol, df_2)
    after_append = pd.Timestamp.now(tz=timezone.utc).value  

    # Verify VersionedItem structure
    assert isinstance(result, VersionedItem)
    assert result.symbol == symbol
    assert result.version == 2
    assert result.metadata == None
    assert result.data is None  
    assert result.library == lib._library.library_path
    assert result.host == lib.env
    # Verify timestamp is reasonable (within the append operation timeframe)
    assert before_append <= result.timestamp <= after_append
    assert len(lib.list_symbols()) == 1
    assert len(lib.list_versions()) == 3

    # Append empty dataframe to symbol with content does not increase version
    # but as we saw previously it will create symbol 
    result = lib.append(symbol, df_empty)
    assert len(lib.list_symbols()) == 1
    assert len(lib.list_versions()) == 3
