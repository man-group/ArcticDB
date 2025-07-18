"""
Copyright 2023 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

import re
import sys
from typing import Set
import pytest
import pandas as pd
import numpy as np
import pkg_resources

from arcticdb.util.test import (
    assert_series_equal_pandas_1,
    dataframe_simulate_arcticdb_update_static,
)
from arcticdb.util.utils import generate_random_series, list_installed_packages, set_seed, supported_types_list, verify_dynamically_added_columns
from arcticdb.version_store._store import NativeVersionStore, VersionedItem
from datetime import timedelta, timezone

from arcticdb.exceptions import (
    ArcticNativeException,
    SortingException
)
from arcticdb_ext.version_store import StreamDescriptorMismatch

from arcticdb_ext.exceptions import (
    InternalException,
    NormalizationException,
)

from benchmarks.bi_benchmarks import assert_frame_equal
from tests.util.mark import LINUX, SLOW_TESTS_MARK



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
@pytest.mark.parametrize("append_type", ["append", "stage"])
def test_write_append_update_read_scenario_with_different_series_combinations(version_store_factory, dtype, schema, append_type):
    """This test covers series with timestamp index of all supported arcticdb types.
    Write, append and read combinations of different boundary length sizes of series

    Verifies:
     - append/update operations over symbol containing timestamped series
     - append/update operations with different types of series - empty, one element, many elements
     - tests repeated over each supported arcticdb type - ints, floats, str, bool, datetime
     - tests work as expected over static and dynamic schema with small segment row size 
    """
    if LINUX and (sys.version_info[:2] == (3, 8)) and dtype == np.float64 and schema == False:
        """ https://github.com/man-group/ArcticDB/actions/runs/16363364782/job/46235614310?pr=2470
        E                   arcticdb_ext.version_store.StreamDescriptorMismatch: The columns (names and types) in the argument are not identical to that of the existing version: APPEND
        E                   stream_id="symbol-_class__numpy_float64__"
        E                   (Showing only the mismatch. Full col list saved in the `last_mismatch_msg` attribute of the lib instance.
        E                   '-' marks columns missing from the argument, '+' for unexpected.)
        E                   -FD<name=some_name!, type=TD<type=UTF_DYNAMIC64, dim=0>, idx=1>"
        E                   +FD<name=some_name!, type=TD<type=FLOAT64, dim=0>, idx=1>"
        """
        pytest.xfail("Test fails due to issue (9589648728)")
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
        assert_series_equal_pandas_1(series, lib.read(symbol).data, check_index_type=(len(series) > 0))
        
        for append_series_length in series_length:
            append_series = generate_random_series(dtype, append_series_length, name, 
                                                   start_time=timestamp + timedelta(seconds=total_length), seed=None)
            if append_type == "append":
                lib.append(symbol, append_series, metadata=meta)
            else:
                meta = None # Metadata is not added to version with stage method
                lib.stage(symbol, append_series, validate_index=False, sort_on_index=False) 
                lib.compact_incomplete(symbol, append=True, convert_int_to_float=False)
            result_series = pd.concat([result_series, append_series])
            ver = lib.read(symbol)
            assert_series_equal_pandas_1(result_series, ver.data, check_index_type=(len(result_series) > 0))
            assert meta == ver.metadata
            # Note update is of same length but starts in previous period

            update_series = generate_random_series(dtype, append_series_length, name, 
                                                   start_time=timestamp + timedelta(seconds=total_length - 1), seed=None)
            total_length += append_series_length
            lib.update(symbol, update_series, metadata=meta)
            result_series = dataframe_simulate_arcticdb_update_static(result_series, update_series)
            ver = lib.read(symbol)
            assert_series_equal_pandas_1(result_series, ver.data, check_index_type=(len(result_series) > 0))
            assert meta == ver.metadata


def test_for_9589648728(version_store_factory):
    lib: NativeVersionStore = version_store_factory(dynamic_schema=False, segment_row_size=3)
    symbol = "32"
    set_seed(3484356)
    timestamp = pd.Timestamp(4839275892348)
    series_length = [ 1, 0, 2, 10]
    dtype = np.float64
    name = "dsf"
    for length in series_length:
        total_length = 0
        series = generate_random_series(dtype, 4, name, start_time=timestamp, seed=None)
        total_length += length
        print("Series to write:", series.info(verbose=True))
        lib.write(symbol, series)
        print("Series read:", lib.read(symbol).data.info(verbose=True))
        append_series = generate_random_series(dtype, 0, name, 
                                                    start_time=timestamp + timedelta(seconds=total_length), seed=None)
        print("Series to append:", append_series.info(verbose=True))
        try:
            lib.append(symbol, append_series)
            lib.update(symbol, append_series)
        except Exception as e:
            for package in list_installed_packages():
                print(package)
            raise



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
def test_append_scenario_with_errors_and_success(version_store_and_real_s3_basic_store_factory, schema):
    """
    Test error messages and exception types for various failure scenarios mixed with 
    
    Verifies:
    - Appropriate exception types are raised
    - Edge cases are handled gracefully
    - Exceptions are same across all storage types
    """
    lib: NativeVersionStore = version_store_and_real_s3_basic_store_factory(
            dynamic_schema=schema, dynamic_strings=schema, segment_row_size=1)
    symbol = "test_append_errors"
    
    df = pd.DataFrame({'value': [1, 2, 3]}, index=pd.date_range('2023-01-01', periods=3, freq='s'))
    df_2 = pd.DataFrame({'value': [3, 4, 5]}, index=pd.date_range('2023-01-02', periods=3, freq='s'))
    df_not_sorted = pd.DataFrame({"value": [11, 24, 1]}, index=[pd.Timestamp("2024-01-04"), 
                                                                pd.Timestamp("2024-01-03"), 
                                                                pd.Timestamp("2024-01-05")])
    df_different_schema = pd.DataFrame({'value': [3.1, 44, 5.324]}, 
                                       index=pd.date_range('2023-01-03', periods=3, freq='s'))
    df_empty = df.iloc[0:0]
    df_same_index = pd.DataFrame({'value': [10, 982]}, index=pd.date_range('2023-01-01', periods=2, freq='s'))
    pickled_data = [34243, 3253, 53425]
    pickled_data_2 = get_metadata()
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
            lib.append(sym)
            assert False, "Expected exception was not raised"
        except Exception as e:
            # Verify exception type and message content
            assert isinstance(e, TypeError)
    assert len(lib.list_symbols()) == 0

    # Empty dataframe will create symbol with one version
    result = lib.append(symbol, df_empty)
    assert len(lib.list_symbols()) == 1
    assert len(lib.list_versions()) == 1
    assert 0 == result.version
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
    assert lib.list_symbols() == [symbol]
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
    assert lib.list_symbols_with_incomplete_data() == []          
    assert_frame_equal( pd.concat([df, df_2], sort=True), lib.read(symbol).data)

    # Test append overlapping indexes
    for frame in [df_same_index]:
        with pytest.raises((InternalException)):
            lib.append(symbol, frame)
    # Append can happen only as incomplete
    lib.append(symbol, frame, validate_index=False, incomplete=True) 
    assert lib.list_symbols_with_incomplete_data() == [symbol]          
    assert len(lib.list_versions()) == 3

    # Cannot append pickle data to dataframe
    for pick in [pickled_data, pickled_data_2]:
        with pytest.raises(NormalizationException):
            lib.append(symbol, pick)

    # Append with different schema works on dynamic schema only
    if schema:
        lib.append(symbol, df_different_schema)    
        assert len(lib.list_versions()) == 4
        assert_frame_equal( pd.concat([df, df_2, df_different_schema], sort=True), lib.read(symbol).data)
    else:
        # Should raise StreamDescriptorMismatch
        with pytest.raises(StreamDescriptorMismatch):
            lib.append(symbol, df_different_schema)    

    # Append empty dataframe to symbol with content does not increase version
    # but as we saw previously it will create symbol 
    result = lib.append(symbol, df_empty)
    assert len(lib.list_symbols()) == 1
    assert len(lib.list_versions()) == 4 if schema else 3

    # Validate that validate_index works as expected
    with pytest.raises(SortingException):
        lib.append(symbol, df_not_sorted, validate_index=True)
    with pytest.raises(SortingException):
        lib.append(symbol, df_not_sorted, validate_index=True, incomplete=True)
    result2 = lib.append(symbol, df_not_sorted, validate_index=False)
    assert result2.version == result.version + 1


def test_update_date_range_exhaustive(lmdb_version_store):
    """Test update with open-ended and closed date ranges,
    and verifications over the result dataframe what remains"""

    lib = lmdb_version_store
    symbol = "test_date_range_open"
    start_date_original_df = "2023-01-01"
    start_date_update_df = "2023-01-05"

    update_data = pd.DataFrame(
        {"value": [999]},
        index=pd.date_range(start_date_update_df, periods=1, freq="D")
    )

    def run_test(lib, initial_data, start, end, update_expected_at_index, length_of_result_df, scenario):
        nonlocal update_data
        lib.write(symbol, initial_data) # always reset symbol
        
        lib.update(symbol, update_data, date_range=(start, end))
        
        result =  lib.read(symbol).data
        
        assert result.iloc[update_expected_at_index]["value"] == 999, f"Failed for {scenario} Scenario"
        assert len(result) == length_of_result_df 
        return result      

    initial_data = pd.DataFrame(
        {"value": range(10)},
        index=pd.date_range(start_date_original_df, periods=10, freq="D")
    )
    lib.write(symbol, initial_data)
    
    # Test an open end scenario - where the start date is the start date of the update
    # all data from original dataframe from its start date until  the update start date is removed
    # and what is left is from update start date until end fate of original dataframe
    run_test(lib, initial_data, 
                start=pd.Timestamp(start_date_update_df), end=None, 
                update_expected_at_index=4, length_of_result_df=5, 
                scenario="open end")
    
    # Test an open end scenario, where data is way before initial timestamp
    # of original dataframe - in this case only the update will be present in the symbol
    # and no parts of the dataframe that was updated    
    result_df = run_test(lib, initial_data, 
                start=update_data.index[0] - timedelta(days=300), end=None, 
                update_expected_at_index=0, length_of_result_df=1, 
                scenario="open end - way before initial dataframe")
    assert_frame_equal(update_data.head(1), result_df)

    # Test an open start scenario where end date overlaps with the start date
    # of the update. In that case all original start until the update start will be preserved 
    # in the result
    run_test(lib, initial_data, 
                start=None, end=pd.Timestamp(start_date_update_df), 
                update_expected_at_index=0, length_of_result_df=6, 
                scenario="open start")

    # Both start and end are open - in this case only the update will be the result
    result_df = run_test(lib, initial_data, 
                start=None, end=None, 
                update_expected_at_index=0, length_of_result_df=update_data.shape[0], 
                scenario="both open")
    assert_frame_equal(update_data, result_df)

    lib.list_symbols_with_incomplete_data

    