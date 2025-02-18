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
from arcticdb.util.environment_setup import GeneralSetupLibraryWithSymbols, Storage


#region Setup classes

class ReadWriteBenchmarkSettings(GeneralSetupLibraryWithSymbols):
    """
    Setup Read Tests Library for different storages.
    Its aim is to have at one place the responsibility for setting up any supported storage
    with proper symbols.

    It is also responsible for providing 2 libraries: 
    - one that will hold persistent data across runs
    - one that will will hold transient data for operations which data will be wiped out
    """

    START_DATE_INDEX = pd.Timestamp("2000-1-1")
    INDEX_FREQ = 's'

    def get_last_x_percent_date_range(self, row_num, percents):
        """
        Returns a date range selecting last X% of rows of dataframe
        pass percents as 0.0-1.0
        """
        start = TimestampNumber.from_timestamp(
            ReadWriteBenchmarkSettings.START_DATE_INDEX, ReadWriteBenchmarkSettings.INDEX_FREQ)
        percent_5 = int(row_num * percents)
        end_range = start + row_num
        start_range = end_range - percent_5
        range = pd.date_range(start=start_range.to_timestamp(), end=end_range.to_timestamp(), freq="s")
        return range
    
    def generate_dataframe(self, row_num:int, col_num: int) -> pd.DataFrame:
        """
        Dataframe that will be used in read and write tests
        """
        st = time.time()
        # NOTE: Use only setup environment logger!
        self.logger().info("Dataframe generation started.")
        df = (DFGenerator(row_num)
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
        self.logger().info(f"Dataframe {row_num} rows generated for {time.time() - st} sec")
        return df

#endregion

class LMDBReadWrite:
    """
    This class is for general read write tests on LMDB

        IMPORTANT: 
        - When we inherit from another test we inherit test, setup and teardown methods
        - setup_cache() method we inherit it AS IS, thus it will be executed only ONCE for
          all classes that inherit from the base class. Therefore it is perhaps best to ALWAYS
          provide implementation in the child class, no matter that it might look like code repeat
    """

    rounds = 1
    number = 3 # invokes 3 times the test runs between each setup-teardown 
    repeat = 1 # defines the number of times the measurements will invoke setup-teardown
    min_run_count = 1
    warmup_time = 0

    timeout = 1200

    SETUP_CLASS = ReadWriteBenchmarkSettings(Storage.LMDB).set_params([2_500_000, 5_000_000])

    params = SETUP_CLASS.get_parameter_list()
    param_names = ["num_rows"]

    def setup_cache(self):
        '''
        Always provide implementation of setup_cache in
        the child class

        And always return storage info which should 
        be first parameter for setup, tests and teardowns
        '''
        lmdb_setup = LMDBReadWrite.SETUP_CLASS.setup_environment() 
        info = lmdb_setup.get_storage_info()
        # NOTE: use only logger defined by setup class
        lmdb_setup.logger().info(f"storage info object: {info}")
        return info

    def setup(self, storage_info, num_rows):
        '''
        This setup method for read and writes can be executed only once
        No need to be executed before each test. That is why we define 
        `repeat` as 1
        '''
        ## Construct back from arctic url the object
        self.setup_env: ReadWriteBenchmarkSettings = ReadWriteBenchmarkSettings.from_storage_info(storage_info)
        sym = self.setup_env.get_symbol_name(num_rows, None)
        self.to_write_df = self.setup_env.get_library().read(symbol=sym).data
        self.last_20 = self.setup_env.get_last_x_percent_date_range(num_rows, 20)
        ##
        ## Writing into library that has suffix same as process
        ## will protect ASV processes from writing on one and same symbol
        ## this way each one is going to have its unique library
        self.write_library = self.setup_env.get_modifiable_library(os.getpid())

    def time_read(self, storage_info, num_rows):
        sym = self.setup_env.get_symbol_name(num_rows, None)
        self.setup_env.get_library().read(symbol=sym)

    def peakmem_read(self, storage_info, num_rows):
        sym = self.setup_env.get_symbol_name(num_rows, None)
        self.setup_env.get_library().read(symbol=sym)

    def time_write(self, storage_info, num_rows):
        sym = self.setup_env.get_symbol_name(num_rows, None)
        self.write_library.write(symbol=sym, data=self.to_write_df)

    def peakmem_write(self, storage_info, num_rows):
        sym = self.setup_env.get_symbol_name(num_rows, None)
        self.write_library.write(symbol=sym, data=self.to_write_df)

    def time_read_with_column_float(self, storage_info, num_rows):
        COLS = ["float2"]
        sym = self.setup_env.get_symbol_name(num_rows, None)
        self.setup_env.get_library().read(symbol=sym, columns=COLS).data

    def peakmem_read_with_column_float(self, storage_info, num_rows):
        COLS = ["float2"]
        sym = self.setup_env.get_symbol_name(num_rows, None)
        self.setup_env.get_library().read(symbol=sym, columns=COLS).data           

    def time_read_with_columns_all_types(self, storage_info, num_rows):
        COLS = ["float2","string10","bool", "int64","uint64"]
        sym = self.setup_env.get_symbol_name(num_rows, None)
        self.setup_env.get_library().read(symbol=sym, columns=COLS).data

    def peakmem_read_with_columns_all_types(self, storage_info, num_rows):
        COLS = ["float2","string10","bool", "int64","uint64"]
        sym = self.setup_env.get_symbol_name(num_rows, None)
        self.setup_env.get_library().read(symbol=sym, columns=COLS).data           

    def time_write_staged(self, storage_info, num_rows):
        lib = self.write_library
        lib.write(f"sym", self.to_write_df, staged=True)
        lib._nvs.compact_incomplete(f"sym", False, False)

    def peakmem_write_staged(self, storage_info, num_rows):
        lib = self.write_library
        lib.write(f"sym", self.to_write_df, staged=True)
        lib._nvs.compact_incomplete(f"sym", False, False)

    def time_read_with_date_ranges_last20_percent_rows(self, storage_info, num_rows):
        sym = self.setup_env.get_symbol_name(num_rows, None)
        self.setup_env.get_library().read(symbol=sym, date_range=self.last_20).data

    def peakmem_read_with_date_ranges_last20_percent_rows(self, storage_info, num_rows):
        sym = self.setup_env.get_symbol_name(num_rows, None)
        self.setup_env.get_library().read(symbol=sym, date_range=self.last_20).data

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

