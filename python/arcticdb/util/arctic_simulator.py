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
from arcticdb.util.utils import (
    ARCTICDB_NA_VALUE_BOOL,
    ARCTICDB_NA_VALUE_FLOAT,
    ARCTICDB_NA_VALUE_INT,
    ARCTICDB_NA_VALUE_STRING,
    ARCTICDB_NA_VALUE_TIMESTAMP,
)
from arcticdb.version_store.library import Library


logger = get_logger()


def apply_dynamic_schema_changes(to_df: pd.DataFrame, from_df: pd.DataFrame):
    """
    Modifies `to_df` adding all missing columns from `from_df` at the end.
    Also modifies `from_df` to have the same schema by adding missing columns in appropriate positions.
    This is how arcticdb treats column combining with dynamic_schema=True on append/update
    """

    def empty_column_of_type(num_rows, dtype):
        if pd.api.types.is_integer_dtype(dtype):
            default_value = ARCTICDB_NA_VALUE_INT
        elif pd.api.types.is_float_dtype(dtype):
            default_value = ARCTICDB_NA_VALUE_FLOAT
        elif pd.api.types.is_bool_dtype(dtype):
            default_value = ARCTICDB_NA_VALUE_BOOL
        elif pd.api.types.is_string_dtype(dtype) or pd.api.types.is_object_dtype(dtype):
            default_value = ARCTICDB_NA_VALUE_STRING
        elif pd.api.types.is_datetime64_any_dtype(dtype):
            default_value = ARCTICDB_NA_VALUE_TIMESTAMP
        else:
            raise TypeError(f"Unhandled dtype default for: {dtype}")
        return np.full(num_rows, default_value, dtype=dtype)

    def add_missing_columns_at_end(to_df, from_df):
        cols_to_df = set(to_df.columns)
        for col in from_df.columns:
            if col in cols_to_df:
                # TODO: Can do schema checks here if we want friendly error messages.
                # Not strictly needed as pandas.concat will raise errors if types are incompatible
                continue
            dtype = from_df[col].dtype
            to_df[col] = empty_column_of_type(len(to_df), dtype)
        return to_df

    to_df = add_missing_columns_at_end(to_df, from_df)
    from_df = add_missing_columns_at_end(from_df, to_df)
    # Reorder from_df to have the same column order as to_df
    from_df = from_df[to_df.columns]
    return to_df, from_df


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

    def write(self, df: pd.DataFrame) -> "ArcticSymbolSimulator":
        if (len(self._versions) == 0) or self._keep_versions:
            self._versions.append(df.copy(deep=True))
        else:
            self._versions[len(self._versions) - 1] = df
        return self

    def append(self, df: pd.DataFrame) -> "ArcticSymbolSimulator":
        self.write(self.simulate_arctic_append(self.read(), df, self._dynamic_schema))
        return self

    def update(self, df: pd.DataFrame) -> "ArcticSymbolSimulator":
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
    def assert_frame_equal_rebuild_index_first(
        expected: Union[pd.DataFrame, pd.Series], actual: Union[pd.DataFrame, pd.Series]
    ):
        if isinstance(expected, pd.Series) and isinstance(actual, pd.Series):
            assert_series_equal_pandas_1(expected, actual)
        else:
            actual_df_same_col_sequence = actual[expected.columns]
            assert_frame_equal_rebuild_index_first(expected, actual_df_same_col_sequence)

    @staticmethod
    def simulate_arctic_append(
        df1: Union[pd.DataFrame, pd.Series], df2: Union[pd.DataFrame, pd.Series], dynamic_schema: bool = True
    ) -> pd.DataFrame:
        """Simulates arctic append operation

        Result will be dataframe where df2 is appended to df1.
        Limitation: The order of the returned columns may differ from those from arctic"""

        def validate_index(df: pd.DataFrame):
            if not isinstance(df.index, (pd.RangeIndex, pd.DatetimeIndex)):
                raise TypeError(
                    f"Unsupported index type: {type(df.index).__name__}."
                    + "Only RangeIndex or DatetimeIndex are supported."
                )

        # Check and validation section
        validate_index(df1)
        validate_index(df2)

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

            if dynamic_schema:
                df1, df2 = apply_dynamic_schema_changes(df1, df2)

            result_df = pd.concat([df1, df2])
            if isinstance(df1.index, pd.RangeIndex):
                # TODO: Can abstract index fixing to a common function
                result_df = result_df.reset_index(drop=True)
            return result_df

    @staticmethod
    def simulate_arctic_update(
        existing_df: Union[pd.DataFrame, pd.Series],
        update_df: Union[pd.DataFrame, pd.Series],
        dynamic_schema: bool = True,
    ) -> Union[pd.DataFrame, pd.Series]:
        """
        Does implement arctic logic of update() method functionality over pandas dataframes/series.
        In other words the result, new data frame will have the content of 'existing_df' dataframe/series
        updated with the content of "update_df" dataframe the same way that arctic is supposed to work.
        Useful for prediction of result content of arctic database after update operation
        NOTE: you have to pass indexed dataframe
        """

        if isinstance(existing_df, pd.Series) and isinstance(update_df, pd.Series):
            if len(update_df) < 1:
                return existing_df  # Nothing to update
            if not dynamic_schema:
                assert (
                    existing_df.dtype == update_df.dtype
                ), f"Series must have same type {existing_df.dtype} == {update_df.dtype}"
                assert existing_df.name == update_df.name, "Series name must be same"
        elif isinstance(existing_df, pd.DataFrame) and isinstance(update_df, pd.DataFrame):
            if not dynamic_schema:
                assert existing_df.dtypes.to_list() == update_df.dtypes.to_list(), (
                    f"Dataframe must have identical columns types in same order.\n"
                    + f"{existing_df.dtypes.to_list()} == {update_df.dtypes.to_list()}."
                )
                assert (
                    existing_df.columns.to_list() == update_df.columns.to_list()
                ), "Columns names also need to be in same order"
        else:
            raise (
                f"Expected existing_df and update_df to have the same type. Types: {type(existing_df)} and {type(update_df)}"
            )

        if dynamic_schema:
            existing_df, update_df = apply_dynamic_schema_changes(existing_df, update_df)

        start2 = update_df.first_valid_index()
        end2 = update_df.last_valid_index()
        chunks = []
        df1 = existing_df[existing_df.index < start2]
        chunks.append(df1)
        chunks.append(update_df)
        df2 = existing_df[existing_df.index > end2]
        chunks.append(df2)
        result_df = pd.concat(chunks)
        return result_df
