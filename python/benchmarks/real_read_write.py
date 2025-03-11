"""
Copyright 2025 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

import os
import time
from typing import List
import numpy as np
import pandas as pd

from arcticdb.util.utils import DFGenerator, TimestampNumber
# from arcticdb.util.environment_setup import SetupSingleLibrary, Storage
from arcticdb.util.environment_setup_refactored import (
    DataFrameGenerator,
    LibraryPopulationPolicy,
    Storage,
    LibraryType,
    get_arctic_client,
    get_library,
    populate_library,
    populate_persistent_library_if_missing,
    get_logger_for_asv,
)


#region Setup classes
class AllColumnTypesGenerator(DataFrameGenerator):
    def get_dataframe(self, num_rows):
        df = (DFGenerator(num_rows)
              .add_int_col("int8", np.int8)
              .add_int_col("int16", np.int16)
              .add_int_col("int32", np.int32)
              .add_int_col("int64", min=-26, max=31)
              .add_int_col("uint64", np.uint64, min=100, max=199)
              .add_float_col("float16",np.float32)
              .add_float_col("float2",min=-100.0, max=200.0, round_at=4)
              .add_string_col("string10", str_size=10)
              .add_string_col("string20", str_size=20, num_unique_values=20000)
              .add_bool_col("bool")
              .add_timestamp_index("time", ReadWriteBenchmarkSettings.INDEX_FREQ, ReadWriteBenchmarkSettings.START_DATE_INDEX)
              ).generate_dataframe()
        return df

class PerRowsPopulationPolicy(LibraryPopulationPolicy):
    def __init__(self, num_rows: List[int]):
        self.num_rows = num_rows
        self.num_symbols = len(num_rows)
        self.df_generator = AllColumnTypesGenerator()

    def get_symbol_from_rows(self, num_rows):
        return f"sym_{num_rows}"

    def get_symbol_name(self, i):
        return self.get_symbol_from_rows(self.num_rows[i])

    def get_generator_kwargs(self, ind: int) -> pd.DataFrame:
        return {"num_rows": self.num_rows[ind]}

#endregion

class LMDBReadWrite:
    """
    This class is for general read write tests on LMDB

    Uses 1 persistent library for read tests
    Uses 1 modifiable library for write tests
    """

    rounds = 1
    number = 3 # invokes 3 times the test runs between each setup-teardown 
    repeat = 1 # defines the number of times the measurements will invoke setup-teardown
    min_run_count = 1
    warmup_time = 0

    timeout = 1200

    param_names = ["num_rows"]
    params = [2_500_000, 5_000_000]

    storage = Storage.LMDB

    def setup_cache(self):
        '''
        In setup_cache we only populate the persistent libraries if they are missing.
        '''
        ac = get_arctic_client(LMDBReadWrite.storage)
        population_policy = PerRowsPopulationPolicy(LMDBReadWrite.params)
        populate_persistent_library_if_missing(ac, LibraryType.PERSISTENT, LMDBReadWrite, "READ_LIB", population_policy)

    def setup(self, num_rows):
        self.logger = get_logger_for_asv(LMDBReadWrite)
        self.ac = get_arctic_client(LMDBReadWrite.storage)
        self.population_policy = PerRowsPopulationPolicy(LMDBReadWrite.params)
        self.sym = self.population_policy.get_symbol_from_rows(num_rows)
        # We use the same generator as the policy
        self.to_write_df = self.population_policy.df_generator.get_dataframe(num_rows)
        # Functions operating on differetent date ranges to be moved in some shared utils
        self.last_20 = utils.get_last_x_percent_date_range(num_rows, 20)

        self.read_lib = get_library(ac, LibraryType.PERSISTENT, LMDBReadWrite, "READ_LIB")
        self.write_lib = get_library(ac, LibraryType.MODIFIABLE, LMDBReadWrite, "WRITE_LIB")
        # We could also populate the library like so (we don't need )
        # populate_library(self.write_lib, )

    def teardown(self, num_rows):
        # We could clear the modifiable libraries we used
        # self.write_lib.clear()
        pass

    def time_read(self, num_rows):
        self.read_lib.read(self.sym)

    def peakmem_read(self, num_rows):
        self.read_lib.read(self.sym)

    def time_write(self, num_rows):
        self.write_lib.write(self.sym, self.to_write_df)

    def peakmem_write(self, num_rows):
        self.write_lib.write(self.sym, self.to_write_df)

    def time_read_with_column_float(self, num_rows):
        COLS = ["float2"]
        self.read_lib.read(symbol=self.sym, columns=COLS).data

    def peakmem_read_with_column_float(self, num_rows):
        COLS = ["float2"]
        self.read_lib.read(symbol=self.sym, columns=COLS).data

    def time_read_with_columns_all_types(self, num_rows):
        COLS = ["float2","string10","bool", "int64","uint64"]
        self.read_lib.read(symbol=self.sym, columns=COLS).data

    def peakmem_read_with_columns_all_types(self, num_rows):
        COLS = ["float2","string10","bool", "int64","uint64"]
        self.read_lib.read(symbol=self.sym, columns=COLS).data

    def time_write_staged(self, num_rows):
        lib = self.write_library
        lib.write(self.sym, self.to_write_df, staged=True)
        lib._nvs.compact_incomplete(self.sym, False, False)

    def peakmem_write_staged(self, num_rows):
        lib = self.write_library
        lib.write(self.sym, self.to_write_df, staged=True)
        lib._nvs.compact_incomplete(self.sym, False, False)

    def time_read_with_date_ranges_last20_percent_rows(self, num_rows):
        self.read_lib.read(symbol=self.sym, date_range=self.last_20).data

    def peakmem_read_with_date_ranges_last20_percent_rows(self, num_rows):
        self.read_lib.read(symbol=self.sym, date_range=self.last_20).data

class AWSReadWrite(LMDBReadWrite):
    """
    This class is for general read write tests on AWS. It inherits its all tests from from
    the LMDB class but makes sure it does its own setup for the environment 
    """

    rounds = 1
    number = 3 # invoke X times the test runs between each setup-teardown 
    repeat = 1 # defines the number of times the measurements will invoke setup-teardown
    min_run_count = 1
    warmup_time = 0

    timeout = 1200

    SETUP_CLASS = ReadWriteBenchmarkSettings(Storage.AMAZON, 
                                             # Define UNIQUE STRING for persistent libraries names 
                                             # as well as name of unique storage prefix
                                             prefix="READ_WRITE").set_params([1_000_000, 2_000_000])

    params = SETUP_CLASS.get_parameter_list()
    param_names = LMDBReadWrite.param_names

    def setup_cache(self):
        '''
        Always provide implementation of setup_cache in
        the child class

        And always return storage info which should 
        be first parameter for setup, tests and teardowns
        '''
        aws_setup = AWSReadWrite.SETUP_CLASS.setup_environment() 
        return aws_setup.get_storage_info()

