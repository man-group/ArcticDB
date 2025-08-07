from datetime import timedelta
import datetime
import re
import sys
import numpy as np
import pytest

from arcticdb.util.arctic_simulator import ArcticSymbolSimulator, append_na_row
from arcticdb.util.test import assert_frame_equal_rebuild_index_first, assert_series_equal_pandas_1
from arcticdb.util.utils import generate_random_series, set_seed
from arcticdb.version_store._store import NativeVersionStore


import pandas as pd

from tests.util.mark import LINUX, SLOW_TESTS_MARK

def calculate_different_and_common_parts(df1, df2):
    """ Create 4 dataframes of different and common parts of both dataframes

    Returns 4 dataframes:
     - first dataframe contain columns of 1st dataframe not part of 2nd
     - second dataframe is composed of common columns from first one
     - third dataframe is composed of common columns from second one
     - fourth dataframe is composed of columns of 2nd one not part of 1st
    """
    # Extract column sets
    cols_df1 = set(df1.columns)
    cols_df2 = set(df2.columns)

    # Determine column groups
    only_in_df1 = list(cols_df1 - cols_df2)
    only_in_df2 = list(cols_df2 - cols_df1)
    common_cols = list(cols_df1 & cols_df2)

    # Check type consistency for common columns
    for col in common_cols:
        dtype1 = df1[col].dtype
        dtype2 = df2[col].dtype
        if dtype1 != dtype2:
            raise TypeError(
                f"Column '{col}' has different types: df1={dtype1}, df2={dtype2}. Type mismatch not supported."
            )

    # Create the resulting DataFrames
    df_only_df1 = df1[only_in_df1].copy(deep=True)
    df_only_df2 = df2[only_in_df2].copy(deep=True)
    df1_common_only = df1[common_cols].copy(deep=True)
    df2_common_only = df2[common_cols].copy(deep=True)

    return df_only_df1, df1_common_only, df2_common_only, df_only_df2


def create_arctic_added_columns_from_dataframe(df):
    """Given a dataframe will return a new one with None default values for the type"""
    result = df.copy(deep=True)

    for col in result.columns:
        col_type = result[col].dtype

        if pd.api.types.is_float_dtype(col_type):
            result[col] = np.nan

        elif pd.api.types.is_integer_dtype(col_type):
            result[col] = np.zeros(len(result), dtype=col_type)

        elif pd.api.types.is_bool_dtype(col_type):
            result[col] = False

        elif pd.api.types.is_string_dtype(col_type) or pd.api.types.is_object_dtype(col_type):
            result[col] = [None] * len(result)

        elif pd.api.types.is_datetime64_any_dtype(col_type):
            result[col] = pd.NaT

        else:
            raise TypeError(f"Unsupported column type in '{col}': {col_type}")

    return result


def simulate_arctic_append(df1: pd.DataFrame, df2: pd.DataFrame) -> pd.DataFrame:
    df_left, df_common_first, df_common_second, df_right = calculate_different_and_common_parts(df1, df2)
    df_left_zeroed = create_arctic_added_columns_from_dataframe(df_left)
    df_right_zeroed = create_arctic_added_columns_from_dataframe(df_right)

    df1_prepared_with_all_cols = pd.concat([df1, df_right_zeroed], axis=1)
    df2_prepared_with_all_cols = pd.concat([df2, df_left_zeroed], axis=1)

    append_df_to_df1 = pd.concat([df1_prepared_with_all_cols, df2_prepared_with_all_cols])
    return append_df_to_df1


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

df_left, df_common_first, df_common_second, df_right = calculate_different_and_common_parts(df1, df2)

print("Only in df1:")
print(df_left)

print("\nCommon first df:")
print(df_common_first)

print("\nCommon second df:")
print(df_common_second)

print("\nOnly in df2:")
print(df_right)

df = pd.DataFrame({
    'float_col': [1.1, 2.2],
    'int_col': [1, 2],
    'bool_col': [True, False],
    'str_col': ['yes', 'no'],
    'date_col': pd.to_datetime(['2022-01-01', '2022-01-02'])
})

print(create_arctic_added_columns_from_dataframe(df))

df_left_zeroed = create_arctic_added_columns_from_dataframe(df_left)
df_right_zeroed = create_arctic_added_columns_from_dataframe(df_right)

df1_prepared_with_all_cols = pd.concat([df1, df_right_zeroed], axis=1)
df2_prepared_with_all_cols = pd.concat([df2, df_left_zeroed], axis=1)

append_df_to_df1 = pd.concat([df1_prepared_with_all_cols, df2_prepared_with_all_cols])

print(simulate_arctic_append(df1, df2))






#########################

from arcticdb.util.utils import DFGenerator, generate_random_series, generate_random_timestamp_array, list_installed_packages, set_seed, supported_types_list, verify_dynamically_added_columns

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

@pytest.mark.storage
def test_ZZZ(version_store_and_real_s3_basic_store_factory):
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
    asym = ArcticSymbolSimulator(keep_versions=True)
    asym.write(initial_df)
    append_df = wrap_df_add_new_columns(initial_df, 2)
    make_df_appendable(lib.read(symbol).data, append_df)
    asym.append(append_df)
    lib.append(symbol, append_df)
    pd.set_option('display.max_colwidth', None)
    pd.set_option('display.max_columns', None)
    pd.set_option('display.max_rows', None)

    df1 = asym.read()
    df2 = lib.read(symbol).data
    df2 = df2[df1.columns]

    print(df1)
    print(df2)
    assert_frame_equal_rebuild_index_first(df1, df2)

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

@pytest.mark.storage
def test_ZZZ2(version_store_and_real_s3_basic_store_factory):
    """
    The test does series of append operations with new columns of all supported column types.
    The resulting symbol will have additional columns each time added, with predefined default 
    values for different data types

    Verifies:
     - continuous appends/updates with new columns works as expected across dynamic schema
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
    asym = ArcticSymbolSimulator(keep_versions=True).associate_arctic_lib(lib)


    def get_df() -> pd.DataFrame:
        """ Creates new dataframe with one constant one column and one row where the value is 
        auto incremented on each new dataframe created"""
        nonlocal counter
        df = pd.DataFrame({col_name: [counter]})
        counter += 1
        return df

    initial_df = get_df()
    add_index(initial_df, start_time)
    asym.write(initial_df).arctic_lib().write(symbol, initial_df)
    read_data = asym.arctic_lib().read(symbol).data

    # Will do append operation times to confirm growth is not a problem
    for i in range(10):
        num_versions_before = len(asym.arctic_lib().list_versions(symbol))
        middle_df = get_df()
        add_index(middle_df, start_time)
        append_df = wrap_df_add_new_columns(middle_df, i)
        make_df_appendable(read_data, append_df) # timestamps of append_df to be after the initial

        # Do append with different mixes of parameters
        if i % 2 == 0:
            meta = get_metadata()
            asym.arctic_lib().append(symbol, append_df, prune_previous_version=True, validate_index=True, metadata=meta)
            assert num_versions_before, lib.list_versions(symbol) # previous version is pruned
        else:
            meta = None
            # NOTE: metadata is not stored then incomplete=True
            asym.arctic_lib().append(symbol, append_df, incomplete=True, prune_previous_version=False, validate_index=True, metadata=meta)
            asym.arctic_lib().compact_incomplete(symbol, True, False)
            assert num_versions_before, asym.arctic_lib().list_versions(symbol) + 1 # previous version is not pruned

        asym.append(append_df)

        # Verify  metadata and dynamically added columns
        ver = asym.arctic_lib().read(symbol)
        read_data:pd.DataFrame = ver.data
        assert meta == ver.metadata
        asym.assert_equal_to_associated_lib(symbol)   

    # Will only update last row with 1 row dataframe
    update_timestamp = read_data.index[-1]

    # repeat update operation several times, to confirm growth is not a problem
    for i in range(5):
        num_versions_before = len(asym.arctic_lib().list_versions(symbol))
        middle_df = get_df()
        add_index(middle_df, update_timestamp)
        update_df = wrap_df_add_new_columns(middle_df, f"upd_{i}")

        if i % 2 == 0:
            meta = get_metadata()
            asym.arctic_lib().update(symbol, update_df, prune_previous_version=False, metadata=meta)
            assert num_versions_before, asym.arctic_lib().list_versions(symbol) + 1 # previous version is not pruned
        else:
            meta = "just a message"
            asym.arctic_lib().update(symbol, update_df, prune_previous_version=True, metadata=meta)
            assert num_versions_before, asym.arctic_lib().list_versions(symbol) # previous version is pruned
        
        asym.update(update_df)

        # Verify  metadata and dynamically added columns
        ver = lib.read(symbol)
        read_data:pd.DataFrame = ver.data
        assert meta == ver.metadata
        asym.assert_equal_to_associated_lib(symbol)        


def test_ZZZ3():
    # Create timestamp index different from timestamp columnpyt
    index_dates = pd.date_range(start=datetime.datetime(2025, 8, 1), periods=5, freq="D")

    # Build the DataFrame
    df = pd.DataFrame({
        "int_col": [10, 20, 30, 40, 50],
        "float_col": [1.5, 2.5, 3.5, 4.5, 5.5],
        "bool_col": [True, False, True, False, True],
        "str_col": ["a", "b", "c", "d", "e"],
        "timestamp_col": index_dates + pd.to_timedelta(2, unit="h")
    }, index=index_dates)

    index_dates = pd.date_range(start=datetime.datetime(2025, 8, 18), periods=1, freq="D")
    df1 = pd.DataFrame({
        "int_col": [111],
        "float_col": [111.0],
        "bool_col": [False],
        "str_col": ["Z"],
        "timestamp_col": index_dates + pd.to_timedelta(2, unit="h")
    }, index=index_dates)


    asym = ArcticSymbolSimulator(keep_versions=True)
    asym.write(df)
    print(asym.read())
    asym.update(df1)
    print(asym.read())


def test_ZZZ4():

    df = pd.DataFrame({
    "int_col": [1, 2],
    "float_col": [1.1, 2.2],
    "bool_col": [True, False],
    "str_col": ["foo", "bar"],
    "time_col": pd.to_datetime(["2023-01-01", "2023-01-02"])
})

df_new = append_na_row(df)
print(df_new)

df = df.iloc[0:0]
df_new = append_na_row(df)
print(df_new)

