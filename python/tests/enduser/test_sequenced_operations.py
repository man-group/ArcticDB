
"""
Copyright 2025 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""
import inspect
import random
import time
import pandas as pd
import numpy as np
import pytest
import arcticdb_ext

from arcticdb.options import LibraryOptions
from arcticdb.util import test
from arcticdb.util.test import dataframe_dump_to_log, dataframe_simulate_arcticdb_update_static
from arcticdb.util.utils import ArcticTypes, DFGenerator, TimestampNumber, get_logger
from arcticdb.version_store.library import Library
from dataclasses import dataclass
from enum import Enum
from typing import Any, List, Tuple


logger = get_logger()
NUMBER_ROW_SEGMENTS = 5
NUMBER_COL_SEGMENTS = 5
DEFAULT_FREQ = 's'


class DataFrameType(Enum):
    EMPTY = 0
    ONE_ROW = 1
    SEGMENT_SIZE_MINUS_ONE = 2
    SEGMENT_SIZE = 3
    SEGMENT_SIZE_PLUS_ONE = 4
    LARGE = 5
    XTRA_LARGE = 6
    NORMAL = 7


class UpdatePositionType(Enum):
    BEFORE = 0
    RIGHT_BEFORE = 1
    INSIDE_OVERLAP_BEGINNING = 3
    TOTAL_OVERLAP = 4
    INSIDE = 5
    INSIDE_OVERLAP_END = 6
    RIGHT_AFTER = 7
    AFTER = 8


@dataclass
class UtilizationProfile:
    """
    Utilization profile stores details about specific library configurations
    profiles over different storages. The goal is to provide abstraction
    over the way the dataframes would be generated for this specific case
    """
    segment_size: int = 5
    timestamp_default_frequency: str = DEFAULT_FREQ
    large_dataframe_size: Tuple[int, int] = (25 * segment_size, 50 * segment_size)
    extra_large_dataframe_size: Tuple[int, int] = (250 * segment_size, 500 * segment_size)
    normal_dataframe_size: Tuple[int, int] = (1 * segment_size, 24 * segment_size)

    def get_random_large_df_size(self):
        return random.randint(*self.large_dataframe_size)
    
    def get_random_extra_large_df_size(self):
        return random.randint(*self.extra_large_dataframe_size)

    def get_random_normal_df_size(self):
        return random.randint(*self.normal_dataframe_size)


def prepare_base_dataframe(number_rows: int, start_timestamp_number: TimestampNumber, profile: UtilizationProfile, 
                           auto_inc_initial_value: int, type_value: int) -> DFGenerator:
    return (DFGenerator(number_rows, seed=None)
              .add_bool_col("bool")
              .add_float_col(name="float64", dtype=np.float64)
              .add_float_col(name="float64", dtype=np.float64, min=0.0, max=100_000, round_at=4)
              .add_int_col(name="int32", dtype=np.int32)
              .add_int_col(name="int16", dtype=np.int16, min=-10_000, max=1_000)
              .add_int_col(name="uint64", dtype=np.uint64)
              .add_string_col("string", 10)
              .add_string_col("string_enum", 5, 250)
              .add_timestamp_index("time", profile.timestamp_default_frequency, start_timestamp_number.to_timestamp())
              .add_auto_inc_col("auto", auto_inc_initial_value)
              .add_const_col("type", type_value)
              )


def append_dataframes(df_list : List[pd.DataFrame]) -> pd.DataFrame:
    # filter out empty dataframes and Nones, due to concat bug with type
    # changes in pandas when doing concat empty df
    filtered_list = [ df for df in df_list if df is not None and df.shape[0] > 0]
    return pd.concat(filtered_list)


class AppendGenerator:

    def __init__(self, start_timestamp_number: TimestampNumber = 
                 TimestampNumber.from_timestamp(pd.Timestamp("3-9-1974"), DEFAULT_FREQ)):
        self.rows_generated = 0
        self.utilization_profile = UtilizationProfile()
        self.append_constant = 1
        self.start_timestamp_number = start_timestamp_number
        self.current_timestamp_number = start_timestamp_number
        self.__max_generated = 100000
        self.__options_list = [random.choice(list(DataFrameType)) for _ in range(self.__max_generated)]
        self.__rows_between_updates = [random.choice(range(25)) for _ in range(self.__max_generated)]

    def set_profile(self, profile: UtilizationProfile):
        self.utilization_profile = profile
        return self

    def _generate_dataframe(self, number_elements: int) -> pd.DataFrame:
        df = (prepare_base_dataframe(number_elements, self.current_timestamp_number, self.utilization_profile,
                                     self.rows_generated, self.append_constant))
        self.rows_generated += number_elements
        self.current_timestamp_number = self.current_timestamp_number + number_elements
        return df.generate_dataframe()
    
    def generate_sequence(self, num_dataframes:int) -> List[pd.DataFrame]:
        sequence: List[pd.DataFrame] = list()

        for count, option in enumerate(self.__options_list):
            logger.debug(f"Generating append type: {option}")
            df = self.generate_append(option)
            # Caclculate next dataframe start
            self.current_timestamp_number.inc(self.__rows_between_updates[count])
            sequence.append(df)
            if count >= num_dataframes:
                break

        # remove number of options used from long options random list
        self.__options_list = self.__options_list[num_dataframes:] 
        return sequence
    
    def generate_append(self, df_type: DataFrameType) -> pd.DataFrame:
        if df_type == DataFrameType.EMPTY:
            append_df = self._generate_dataframe(0)
        elif df_type == DataFrameType.ONE_ROW:
            append_df = self._generate_dataframe(1)
        elif df_type == DataFrameType.SEGMENT_SIZE_MINUS_ONE:
            append_df = self._generate_dataframe(self.utilization_profile.segment_size - 1)
        elif df_type == DataFrameType.SEGMENT_SIZE:
            append_df = self._generate_dataframe(self.utilization_profile.segment_size)
        elif df_type == DataFrameType.SEGMENT_SIZE_PLUS_ONE:
            append_df = self._generate_dataframe(self.utilization_profile.segment_size + 1)
        elif df_type == DataFrameType.LARGE:
            append_df = self._generate_dataframe(self.utilization_profile.get_random_large_df_size())
        elif df_type == DataFrameType.XTRA_LARGE:
            append_df = self._generate_dataframe(self.utilization_profile.get_random_extra_large_df_size())
        elif df_type == DataFrameType.NORMAL:
            append_df = self._generate_dataframe(self.utilization_profile.get_random_normal_df_size())
        else:
            raise ValueError(f"Invalid data frame type: {df_type}")
        return append_df

    
    @classmethod
    def move_leading_emptiy_dataframes_to_the_back(cls, df_sequence: List[pd.DataFrame]) -> pd.DataFrame:
        """
        Writing initial empty dataframe in arcticdb will lead to problems with next appends due to bug
        this function is patching this behavior, moving the empty dataframe last
        """
        
        def move_to_back_if_empty(df_sequence: List[pd.DataFrame]):
            if df_sequence and df_sequence[0].shape[0] < 1:
                df_sequence.append(df_sequence.pop(0))
                return True
            return False
        
        while move_to_back_if_empty(df_sequence):
            pass
    
        return df_sequence


class UpdateGenerator:
    
    def __init__(self):
        self.rows_updated = 0
        self.update_constant = 2
        self.utilization_profile = UtilizationProfile()

    def set_profile(self, profile: UtilizationProfile):
        self.utilization_profile = profile
        return self

    def _generate_dataframe(self, number_elements: int, start_timestamp_number: TimestampNumber) -> pd.DataFrame:
        df = (prepare_base_dataframe(number_elements, start_timestamp_number, self.utilization_profile,
                                     self.rows_updated, self.update_constant))
        self.rows_updated += number_elements
        return df.generate_dataframe()

    def generate_sequence(self, original_dataframe: pd.DataFrame,  number_elements:int) -> List[pd.DataFrame]:
        sequence: List[pd.DataFrame] = list()

        for count, option in enumerate(UpdatePositionType):
            df = self.generate_update(original_dataframe, option, number_elements)
            sequence.append(df)

        self._generate_dataframe(0, TimestampNumber.from_timestamp(original_dataframe.index[0], 
                                                                   self.utilization_profile.timestamp_default_frequency))
        self._generate_dataframe(0, TimestampNumber.from_timestamp(original_dataframe.index[-1], 
                                                                   self.utilization_profile.timestamp_default_frequency))

        return sequence
    
    def generate_update(self, original_dataframe: pd.DataFrame, position_type: UpdatePositionType, number_elements: int):
        start = TimestampNumber.from_timestamp(original_dataframe.index[0], DEFAULT_FREQ)
        end = TimestampNumber.from_timestamp(original_dataframe.index[-1], DEFAULT_FREQ)
        rows = original_dataframe.shape[0]
        if position_type == UpdatePositionType.BEFORE:
            update_df = self._generate_dataframe(number_elements, start.dec(number_elements+1))
        elif position_type == UpdatePositionType.RIGHT_BEFORE:
            update_df = self._generate_dataframe(number_elements, start.dec(number_elements))
        elif position_type == UpdatePositionType.INSIDE_OVERLAP_BEGINNING:
            update_df = self._generate_dataframe(number_elements, start)
        elif position_type == UpdatePositionType.INSIDE:
            to_update = number_elements if (number_elements + 2) <= rows else rows - 2
            to_update = max(to_update, 1) # if the dataframe is tiny
            update_df = self._generate_dataframe(to_update, start.inc(1))
        elif position_type == UpdatePositionType.TOTAL_OVERLAP:
            # In this case we generate total overlap
            update_df = self._generate_dataframe(rows, start) 
        elif position_type == UpdatePositionType.INSIDE_OVERLAP_END:
            update_df = self._generate_dataframe(number_elements, end.dec(number_elements-1))
        elif position_type == UpdatePositionType.RIGHT_AFTER:
            update_df = self._generate_dataframe(number_elements, end.inc(1))
        elif position_type == UpdatePositionType.AFTER:
            update_df = self._generate_dataframe(number_elements, end.inc(2))
        else:
            raise ValueError(f"Invalid update position type: {position_type}")
        return update_df


class ErrorDataGenerator:

    def __init__(self):
        self.rows_generated = 0
        self.errors_constant = -3
        self.utilization_profile = UtilizationProfile()

    def set_profile(self, profile: UtilizationProfile):
        self.utilization_profile = profile
        return self

    def _generate_dataframe(self, number_elements: int, start_timestamp_number: TimestampNumber) -> pd.DataFrame:
        df = (prepare_base_dataframe(number_elements, start_timestamp_number, self.utilization_profile,
                                     self.rows_generated, self.errors_constant))
        self.rows_generated += number_elements
        return df.generate_dataframe()
    
    def generate_error_values_for_type(self, dtype: ArcticTypes) -> List[Tuple[Any, ArcticTypes]]:
        values_list: List[Tuple[Any, ArcticTypes]] = list()
        if dtype == np.int32:
            values_list.append((np.int64(np.iinfo(np.int32).max) + 10, np.int64))
            values_list.append((np.iinfo(np.uint32).max, np.uint32))
            values_list.append((0.1, np.float32))
            values_list.append((0.1, np.float64))
            values_list.append(("error", str))
        return values_list
        

    def generate_error_data_sequence(self, original_dataframe: pd.DataFrame) -> List[pd.DataFrame]:
        errors_list: List[pd.DataFrame] = list()

        start = TimestampNumber.from_timestamp(original_dataframe.index[0], DEFAULT_FREQ)
        number_rows = 2

        _type = np.int32
        err_val_list = self.generate_error_values_for_type(_type)
        columns = original_dataframe.select_dtypes(include=[_type]).columns
        if columns is not None:
            for err_val in err_val_list:
                df = self._generate_dataframe(number_elements=number_rows,
                                            start_timestamp_number=start+1) 
                df[columns[0]] = df[columns[0]].astype(err_val[1])
                errors_list.append(df)

        return errors_list


def update_symbol_protected(lib: Library, symbol_name:str, update: pd.DataFrame) -> pd.DataFrame:
    """
    If an error happens we would like to save the dataframe which caused it 
    """
    try:
        lib.update(symbol_name, update)
    except arcticdb_ext.exceptions.StorageException as se: 
        if update.shape[0] < 100:
            dataframe_dump_to_log("update df that stuck", update)
        update.to_parquet(path="update_failed.parquet", index=True)
        logger.error("An error occurred", exc_info=True)
        exit(5)


def synchronous_update_and_check(lib: Library, symbol_name:str, original_df: pd.DataFrame, update_df: pd.DataFrame) -> pd.DataFrame:
    # first update storage symbol
    update_symbol_protected(lib, symbol_name, update_df)
    # now update over current memory representation of the symbol with same update dataframe
    update_over_original_df = dataframe_simulate_arcticdb_update_static(original_df, update_df) 
    arctic_df = lib.read(symbol_name).data
    # check if after the update we have same dataframes in memory and on storage
    test.assert_frame_equal(update_over_original_df, arctic_df)
    return update_over_original_df   


def set_seed(seed_value: int = None):
    if seed_value is None:
        seed_value = int(time.time())
    caller = inspect.stack()[1].function
    with open(f"seed_{caller}.txt","w") as f:
        f.write(f"SEED={seed_value}")
    logger.info(f"caller={caller}  SEED={seed_value}")    
    np.random.seed(seed_value)
    random.seed(seed_value)

symbol="s"

#@pytest.mark.only_fixture_params(["real_s3", "real_gcp"] )
@pytest.mark.only_fixture_params(["lmdb"] )
def test_errors_prevention(arctic_client_v1, lib_name):
    set_seed()
    default_profile = UtilizationProfile()
    seg_size=default_profile.segment_size
    lib = arctic_client_v1.create_library(name=lib_name, 
                                          library_options=LibraryOptions(rows_per_segment=seg_size,
                                                                         columns_per_segment=seg_size))
    try:
        error_missed = False
        gen = ErrorDataGenerator().set_profile(default_profile)
        df = gen._generate_dataframe(10, TimestampNumber.from_timestamp(pd.Timestamp("1-1-1990"), DEFAULT_FREQ))
        lib.write(symbol, df)
        err_list = gen.generate_error_data_sequence(df)
        for err_df in err_list:
            try:
                lib.update(symbol=symbol,data=err_df)
                logger.error(f"Dataframe did not produced error")
                logger.error(f"{err_df}")
                error_missed = True
            except Exception as e:
                logger.info(f"Expected error appeared")
        assert not error_missed, "At least one error was accepted"
    except Exception as e:
        arctic_client_v1.delete_library(lib_name)
        raise


@pytest.mark.only_fixture_params(["real_s3", "real_gcp"] )
def test_sequenced_operations_updates_over_updates(arctic_client_v1, lib_name):

    set_seed()
    default_profile = UtilizationProfile()
    seg_size=default_profile.segment_size
    lib = arctic_client_v1.create_library(name=lib_name, 
                                          library_options=LibraryOptions(rows_per_segment=seg_size,
                                                                         columns_per_segment=seg_size))

    # Add to profile list to examine different parameters
    try:
        for profile in [default_profile]:
            gen = AppendGenerator().set_profile(profile)

            def single_frame_update_tests(original_df_size_rows: int):
                    original_df = gen._generate_dataframe(original_df_size_rows)
                    logger.info("Generation complete")
                    updates = UpdateGenerator().generate_sequence(original_df, 1) # Minimal  
                    updates.extend(UpdateGenerator().generate_sequence(original_df, 
                                                                    profile.segment_size // 2)) # <= segment
                    updates.extend(UpdateGenerator().generate_sequence(original_df, 
                                                                    profile.segment_size + 1)) # > segment
                    updated_df = original_df.copy(deep=True)
                    lib.write(symbol, original_df)
                    for count, update in enumerate(updates):
                        logger.info(f"Update {count}/{len(updates)}")
                        updated_df = synchronous_update_and_check(lib, symbol, updated_df, update)
                    logger.info(lib.read(symbol=symbol).data)

            for rows in [1, 2, profile.segment_size, 
                        profile.segment_size+1,
                        profile.get_random_normal_df_size(),
                        profile.get_random_large_df_size(),
                        profile.get_random_extra_large_df_size()]:
                single_frame_update_tests(rows)
    except Exception as e:
        arctic_client_v1.delete_library(lib_name)
        raise

@pytest.mark.only_fixture_params(["real_s3", "real_gcp"] )
def test_appends_and_updates(arctic_client_v1, lib_name):

    default_profile = UtilizationProfile()
    set_seed()
    seg_size=default_profile.segment_size
    lib = arctic_client_v1.create_library(name=lib_name, 
                                          library_options=LibraryOptions(rows_per_segment=seg_size,
                                                                         columns_per_segment=seg_size))

    
    number_of_sequenced_appends_followed_by_updates = 4
    number_sequenced_appends = 8
    gen = AppendGenerator().set_profile(default_profile)
    updt = UpdateGenerator()

    try:
        for i in  range(number_of_sequenced_appends_followed_by_updates):
            logger.info(f"Append iteration {i}/{number_of_sequenced_appends_followed_by_updates} started")
            list_dfs = gen.generate_sequence(number_sequenced_appends) 
            list_dfs = AppendGenerator.move_leading_emptiy_dataframes_to_the_back(list_dfs)
            lib.write(symbol, list_dfs[0])
            for count, current_df in enumerate(list_dfs[1:]):
                logger.info(f"Doing Append {count}/{len(list_dfs)}")
                lib.append(symbol, current_df)

            df_appends = append_dataframes(list_dfs) # loading appends in memory
            test.assert_frame_equal(df_appends, lib.read(symbol=symbol).data)

            updated_df = df_appends.copy(deep=True) # At the end should be same as content of symbol
            for count, current_df in enumerate(list_dfs):
                logger.info(f"PROCESSING UPDATE {count}/{len(list_dfs)}")
                if current_df.shape[0] > 0:
                    logger.info(f"Updating {count}/{len(list_dfs)} dataframe" )
                    update_sequence = updt.generate_sequence(original_dataframe=current_df, number_elements=1)
                    update_sequence.extend(updt.generate_sequence(original_dataframe=current_df, number_elements=2))
                    update_sequence.extend(updt.generate_sequence(original_dataframe=current_df, number_elements=6))

                    for ind, update in enumerate(update_sequence):
                        logger.info(f"Apply update {ind}/{len(update_sequence)}" )
                        updated_df = synchronous_update_and_check(lib, symbol, updated_df, update)

                    updated_df = synchronous_update_and_check(lib, symbol, updated_df, current_df)

                    for ind, update in enumerate(update_sequence):
                        logger.info(f"Apply update {ind}/{len(update_sequence)}" )
                        updated_df = synchronous_update_and_check(lib, symbol, updated_df, update)

    except Exception as e:
        arctic_client_v1.delete_library(lib_name)
        raise
