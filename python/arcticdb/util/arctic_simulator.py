"""
Copyright 2023 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

from datetime import datetime, timedelta
from typing import List, Optional, Union
import pandas as pd
import numpy as np
from arcticdb.util.logger import get_logger
from arcticdb.util.test import (
    assert_frame_equal_rebuild_index_first,
    assert_series_equal_pandas_1,
)
from arcticdb.util.utils import ARCTICDB_NA_VALUE_BOOL, ARCTICDB_NA_VALUE_FLOAT, ARCTICDB_NA_VALUE_INT, ARCTICDB_NA_VALUE_STRING, ARCTICDB_NA_VALUE_TIMESTAMP
from arcticdb.version_store.library import Library


logger = get_logger()


def calculate_different_and_common_parts(df1: pd.DataFrame, df2: pd.DataFrame, 
                                         dynamic_schema: bool = False) -> List[pd.DataFrame]:
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
    if not dynamic_schema:
        for col in common_cols:
            dtype1 = df1[col].dtype
            dtype2 = df2[col].dtype
            if dtype1 != dtype2:
                raise TypeError(
                    f"Column '{col}' has different types: df1={dtype1}, df2={dtype2}. Type mismatch not supported."
                )
        # for static schema it does not allow adding new columns.
        assert len(only_in_df1) == 0
        assert len(only_in_df2) == 0

    # Create the resulting DataFrames
    df_only_df1 = df1[only_in_df1].copy(deep=True)
    df_only_df2 = df2[only_in_df2].copy(deep=True)
    df1_common_only = df1[common_cols].copy(deep=True)
    df2_common_only = df2[common_cols].copy(deep=True)

    return df_only_df1, df1_common_only, df2_common_only, df_only_df2


def append_na_row(df: pd.DataFrame) -> pd.DataFrame:
    """ Adds a not a value row to a dataframe"""
    defaults = {}

    for col in df.columns:
        dtype = df[col].dtype
        if pd.api.types.is_integer_dtype(dtype):
            defaults[col] = ARCTICDB_NA_VALUE_INT
        elif pd.api.types.is_float_dtype(dtype):
            defaults[col] = ARCTICDB_NA_VALUE_FLOAT  
        elif pd.api.types.is_bool_dtype(dtype):
            defaults[col] = ARCTICDB_NA_VALUE_BOOL
        elif pd.api.types.is_string_dtype(dtype) or pd.api.types.is_object_dtype(dtype):
            defaults[col] = ARCTICDB_NA_VALUE_STRING
        elif pd.api.types.is_datetime64_any_dtype(dtype):
            defaults[col] = ARCTICDB_NA_VALUE_TIMESTAMP
        else:
            raise TypeError(f"Unhandled dtype in column '{col}': {dtype}")

    one_row_df = pd.DataFrame([defaults], columns=df.columns)

    if df.shape[0] < 1:
        return one_row_df
    else: 
        return pd.concat([df, pd.DataFrame([defaults], columns=df.columns)], ignore_index=True)


def create_dataframe_with_na(df: pd.DataFrame):
    """Given a dataframe will return a new one with None default values of type 
    not a value for the type (as per arcticdb)"""
    result = df.copy(deep=True)

    for col in result.columns:
        col_type = result[col].dtype
        if pd.api.types.is_float_dtype(col_type):
            result[col] = ARCTICDB_NA_VALUE_FLOAT
        elif pd.api.types.is_integer_dtype(col_type):
            result[col] = np.full(len(result), ARCTICDB_NA_VALUE_INT, dtype=col_type)
        elif pd.api.types.is_bool_dtype(col_type):
            result[col] = ARCTICDB_NA_VALUE_BOOL
        elif pd.api.types.is_string_dtype(col_type) or pd.api.types.is_object_dtype(col_type):
            result[col] = [ARCTICDB_NA_VALUE_STRING] * len(result) # string is None
        elif pd.api.types.is_datetime64_any_dtype(col_type):
            result[col] = ARCTICDB_NA_VALUE_TIMESTAMP
        else:
            raise TypeError(f"Unsupported column type in '{col}': {col_type}")

    return result


class ArcticSymbolSimulator:
    """This class is intended to be test Oracle for Arctic operations.
    Test oracles serve to predict result of an operation performed by actual product.
    As this is work in progress this is not intended to be full oracle 
    from the very beginning, but slowly grow with the actual needs
    """

    def __init__(self, keep_versions: bool = False, dynamic_schema: bool = True):
        self._versions: List[pd.DataFrame] = list()
        self._keep_versions: bool = keep_versions
        self._dynamic_schema: bool = dynamic_schema

    def write(self, df: pd.DataFrame) -> 'ArcticSymbolSimulator':
        if (len(self._versions) == 0) or self._keep_versions: 
            self._versions.append(df.copy(deep=True))
        else:
            self._versions[len(self._versions) - 1] = df
        return self

    def append(self, df: pd.DataFrame) -> 'ArcticSymbolSimulator':
        self.write(self.simulate_arctic_append(self.read(), df, self._dynamic_schema))
        return self

    def update(self, df: pd.DataFrame) -> 'ArcticSymbolSimulator':
        self.write(self.simulate_arctic_update(self.read(), df, self._dynamic_schema))
        return self

    def read(self, as_of: Optional[int] = None) -> pd.DataFrame:
        as_of = as_of if as_of is not None else len(self._versions) - 1
        assert as_of < len(self._versions)
        df = self._versions[as_of]
        return df.copy(deep=True) if df is not None else None
    
    def assert_equal_to(self, other_df_or_series: Union[pd.DataFrame, pd.Series]):
        self.assert_frame_equal_rebuild_index_first(self.read(), other_df_or_series)

    @staticmethod
    def assert_frame_equal_rebuild_index_first(expected: Union[pd.DataFrame, pd.Series], actual: Union[pd.DataFrame, pd.Series]):
        if isinstance(expected, pd.Series) and isinstance(actual, pd.Series):
            assert_series_equal_pandas_1(expected, actual)
        else:
            actual_df_same_col_sequence = actual[expected.columns]
            assert_frame_equal_rebuild_index_first(expected, actual_df_same_col_sequence)

    @staticmethod
    def simulate_arctic_append(df1: Union[pd.DataFrame, pd.Series], 
                               df2: Union[pd.DataFrame, pd.Series], 
                               dynamic_schema: bool = True) -> pd.DataFrame:
        """Simulates arctic append operation
        
        Result will be dataframe where df2 is appended to df1.
        Limitation: The order of the returned columns may differ from those from arctic"""

        def validate_index(df: pd.DataFrame):
            if not isinstance(df.index, (pd.RangeIndex, pd.DatetimeIndex)):
                raise TypeError(f"Unsupported index type: {type(df.index).__name__}." +
                                "Only RangeIndex or DatetimeIndex are supported.")
            
        def match_row_count(df1: pd.DataFrame, df2: pd.DataFrame) -> pd.DataFrame:
            """Makes df1 have same number of rows as df2, truncating if it has more or multiplying
            the last rows as many times as needed if has less
            """
            target_len = len(df1)
            current_len = len(df2)

            if target_len == current_len:
                return df2.copy()

            elif target_len < current_len:
                # Truncate df2
                return df2.iloc[:target_len].copy()

            else:
                # Extend df2 with last row repeated
                last_row = df2.iloc[[-1]].copy()
                extra_rows = pd.concat([last_row] * (target_len - current_len), ignore_index=True)

                # Reset df2 index to avoid duplication, concat and restore it
                df2_reset = df2.reset_index(drop=True)
                result = pd.concat([df2_reset, extra_rows], ignore_index=True)
                result.index = df1.index  

                return result

        # Check and validation section
        validate_index(df1)
        validate_index(df2)
        assert type(df1.index) == type(df2.index), "Dataframes with same types of indexes can be appended"
        assert df1.index.dtype == df2.index.dtype, "Dataframe indexes must have same type"
        if isinstance(df1.index, pd.RangeIndex):
            assert df1.index.step == df1.index.step, "Ranged indexes dataframes have different steps, cannot be appended"
        if df1.empty: return df2 
        if df2.empty: return df1

        if isinstance(df1, pd.Series) and isinstance(df2, pd.Series):
            # When we have series only case
            if df1.name != df2.name:
                assert dynamic_schema, "Can append Series with different schema with dynamic schema only"
            result_series = pd.concat([df1, df2])
            # With range index we might have a glitch
            if isinstance(df1.index, pd.RangeIndex):
                result_series.index = pd.RangeIndex(start=0, stop=len(result_series), step=df1.index.step)
            return result_series

        else: 
            if isinstance(df1, pd.Series):
                df1 = pd.DataFrame(df1)
                
            if isinstance(df2, pd.Series):
                df2 = pd.DataFrame(df2)

            # Lets split the dataframes in several parts - differences and common parts
            df_left, df_common_first, df_common_second, df_right = calculate_different_and_common_parts(df1, df2, dynamic_schema)
            df_left_zeroed = create_dataframe_with_na(df_left)
            df_right_zeroed = create_dataframe_with_na(df_right)

            # Collect all float32 columns which will be added
            # Those float32 may be forced to float64 if a float column is all None
            df_left_float32_cols = [col for col in df_left.columns if df_left[col].dtype == np.float32]
            df_right_float32_cols = [col for col in df_right.columns if df_right[col].dtype == np.float32]
            source_float32_cols = list()
            source_float32_cols.extend(df_left_float32_cols)
            source_float32_cols.extend(df_right_float32_cols)

            # We will extend df1 and df2 with dataframes with NA values that have 
            # columns that that the dataframe does not have but the other has
            # match_row_count will adapt the number of rows of both dataframes
            df_right_zeroed = match_row_count(df1, df_right_zeroed)
            # This way we synchronize the index of NA dataframe with the 
            # dataframe to which it will be added
            df_right_zeroed.index = df1.index
            # Stitching both dataframes horizontally will will create
            # a dataframe that has all columns but new added columns are NA
            df1_prepared_with_all_cols = pd.concat([df1, df_right_zeroed], axis=1)
            # Same procedure for the other dataframe
            df_left_zeroed = match_row_count(df2, df_left_zeroed)
            df_left_zeroed.index = df2.index
            df2_prepared_with_all_cols = pd.concat([df2, df_left_zeroed], axis=1)

            append_df_to_df1 = pd.concat([df1_prepared_with_all_cols, df2_prepared_with_all_cols])
            # With range index we might have a glitch
            if isinstance(df1.index, pd.RangeIndex):
                append_df_to_df1.index = pd.RangeIndex(start=0, stop=len(append_df_to_df1), step=df1.index.step)

            # Convert back float64 columns that had to be float 32
            for col in source_float32_cols:
                if append_df_to_df1[col].dtype == np.float64:
                    append_df_to_df1[col] = append_df_to_df1[col].astype(np.float32)        

            return append_df_to_df1

    @staticmethod
    def simulate_arctic_update(existing_df: Union[pd.DataFrame, pd.Series], 
                               update_df: Union[pd.DataFrame, pd.Series],
                               dynamic_schema: bool = True) -> Union[pd.DataFrame, pd.Series]:
        """
        Does implement arctic logic of update() method functionality over pandas dataframes/series.
        In other words the result, new data frame will have the content of 'existing_df' dataframe/series
        updated with the content of "update_df" dataframe the same way that arctic is supposed to work.
        Useful for prediction of result content of arctic database after update operation
        NOTE: you have to pass indexed dataframe
        """

        if isinstance(existing_df, pd.Series) and isinstance(update_df, pd.Series):
            if len(update_df) < 1:
                return existing_df # Nothing to update
            if not dynamic_schema:
                assert existing_df.dtype == update_df.dtype, f"Series must have same type {existing_df.dtype} == {update_df.dtype}"
                assert existing_df.name == update_df.name, "Series name must be same"
        elif isinstance(existing_df, pd.DataFrame) and isinstance(update_df, pd.DataFrame):
            if not dynamic_schema:
                assert existing_df.dtypes.to_list() == update_df.dtypes.to_list(), (
                    f"Dataframe must have identical columns types in same order.\n"
                    + f"{existing_df.dtypes.to_list()} == {update_df.dtypes.to_list()}."
                )
                assert existing_df.columns.to_list() == update_df.columns.to_list(), "Columns names also need to be in same order"
        else:
            raise(f"Expected existing_df and update_df to have the same type. Types: {type(existing_df)} and {type(update_df)}")

        start2 = update_df.first_valid_index()
        end2 = update_df.last_valid_index()

        chunks = []
        df1 = existing_df[existing_df.index < start2]
        chunks.append(df1)
        chunks.append(update_df)
        df2 = existing_df[existing_df.index > end2]
        chunks.append(df2)
        if dynamic_schema:
            result_df = chunks[0]
            for i in range(1, len(chunks), 1):
                if len(chunks[i]) > 0:
                    result_df = ArcticSymbolSimulator.simulate_arctic_append(result_df, chunks[i])
            return result_df
        else:
            return pd.concat(chunks)

