"""
Copyright 2025 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

from logging import Logger
import numpy as np
import pandas as pd

from arcticdb.options import LibraryOptions
from arcticdb.util.environment_setup import DataFrameGenerator, TestLibraryManager, LibraryPopulationPolicy, LibraryType, Storage, get_console_logger, populate_library_if_missing
from arcticdb.util.utils import DFGenerator, DataRangeUtils, TimestampNumber
import arcticdb.toolbox.query_stats as qs
from benchmarks.common import AsvBase


#region Setup classes
class AllColumnTypesGenerator(DataFrameGenerator):


    def get_dataframe(self, number_rows, number_columns):
        df = (DFGenerator(number_rows)
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
              .add_timestamp_index("time", self.freq, self.initial_timestamp)
              ).generate_dataframe()
        return df
    

#endregion

class AWSReadWrite(AsvBase):
    """
    This class is for general read write tests 

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
    # NOTE: If you plan to make changes to parameters, consider that a library with previous definition 
    #       may already exist. This means that symbols there will be having having different number
    #       of rows than what you defined in the test. To resolve this problem check with documentation:
    #           https://github.com/man-group/ArcticDB/wiki/ASV-Benchmarks:-Real-storage-tests
    params = [1_000_000, 2_000_000]

    library_manager = TestLibraryManager(storage=Storage.AMAZON, name_benchmark="READ_WRITE")

    def get_logger(self) -> Logger:
        return get_console_logger(self)

    def get_library_manager(self) -> TestLibraryManager:
        return AWSReadWrite.library_manager
    
    def get_population_policy(self) -> LibraryPopulationPolicy:
        lpp = LibraryPopulationPolicy(self.get_logger(), AllColumnTypesGenerator()).set_parameters(AWSReadWrite.params)
        return lpp

    def setup_cache(self):
        '''
        In setup_cache we only populate the persistent libraries if they are missing.
        '''
        manager = self.get_library_manager()
        policy = self.get_population_policy()
        populate_library_if_missing(manager, policy, LibraryType.PERSISTENT)
        manager.log_info() # Logs info about ArcticURI - do always use last

    def setup(self, num_rows):
        self.population_policy = self.get_population_policy()
        self.symbol = self.population_policy.get_symbol_name(num_rows)
        # We use the same generator as the policy
        self.to_write_df = self.population_policy.df_generator.get_dataframe(num_rows, 0)
        
        # Functions operating on differetent date ranges to be moved in some shared utils
        self.last_20 = self.get_last_x_percent_date_range(num_rows, 20)

        self.read_lib = self.get_library_manager().get_library(LibraryType.PERSISTENT)
        self.write_lib = self.get_library_manager().get_library(LibraryType.MODIFIABLE)
        # We could also populate the library like so (we don't need )
        # populate_library(self.write_lib, )

    def teardown(self, num_rows):
        # We could clear the modifiable libraries we used
        self.get_library_manager().clear_all_modifiable_libs_from_this_process()

    def get_last_x_percent_date_range(self, num_rows, percents):
        """
        Returns a date range tuple selecting last X% of rows of dataframe
        pass percents as 0.0-1.0
        """
        df_generator = self.population_policy.df_generator
        freq = df_generator.freq
        return DataRangeUtils.get_last_x_percent_date_range(initial_timestamp=df_generator.initial_timestamp,
                                                            freq=freq, num_rows=num_rows, percents=percents)
    
    def time_read(self, num_rows):
        self.read_lib.read(self.symbol)

    def peakmem_read(self, num_rows):
        self.read_lib.read(self.symbol)

    def time_write(self, num_rows):
        self.write_lib.write(self.symbol, self.to_write_df)

    def peakmem_write(self, num_rows):
        self.write_lib.write(self.symbol, self.to_write_df)

    def time_read_with_column_float(self, num_rows):
        COLS = ["float2"]
        self.read_lib.read(symbol=self.symbol, columns=COLS).data

    def peakmem_read_with_column_float(self, num_rows):
        COLS = ["float2"]
        self.read_lib.read(symbol=self.symbol, columns=COLS).data

    def time_read_with_columns_all_types(self, num_rows):
        COLS = ["float2","string10","bool", "int64","uint64"]
        self.read_lib.read(symbol=self.symbol, columns=COLS).data

    def peakmem_read_with_columns_all_types(self, num_rows):
        COLS = ["float2","string10","bool", "int64","uint64"]
        self.read_lib.read(symbol=self.symbol, columns=COLS).data

    def time_write_staged(self, num_rows):
        lib = self.write_lib
        lib.write(self.symbol, self.to_write_df, staged=True)
        lib._nvs.compact_incomplete(self.symbol, False, False)

    def peakmem_write_staged(self, num_rows):
        lib = self.write_lib
        lib.write(self.symbol, self.to_write_df, staged=True)
        lib._nvs.compact_incomplete(self.symbol, False, False)

    def time_read_with_date_ranges_last20_percent_rows(self, num_rows):
        self.read_lib.read(symbol=self.symbol, date_range=self.last_20).data

    def peakmem_read_with_date_ranges_last20_percent_rows(self, num_rows):
        self.read_lib.read(symbol=self.symbol, date_range=self.last_20).data


class AWSWideDataFrameTests(AWSReadWrite):
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

    library_manager = TestLibraryManager(storage=Storage.AMAZON, name_benchmark="READ_WRITE_WIDE",
                                     library_options=LibraryOptions(rows_per_segment=1000, columns_per_segment=1000))

    param_names = ["num_cols"]
    # NOTE: If you plan to make changes to parameters, consider that a library with previous definition 
    #       may already exist. This means that symbols there will be having having different number
    #       of rows than what you defined in the test. To resolve this problem check with documentation:
    #           https://github.com/man-group/ArcticDB/wiki/ASV-Benchmarks:-Real-storage-tests
    params = [15000, 30000]

    number_rows= 3000

    def get_library_manager(self) -> TestLibraryManager:
        return AWSWideDataFrameTests.library_manager
    
    def get_population_policy(self) -> LibraryPopulationPolicy:
        lpp = LibraryPopulationPolicy(self.get_logger())
        lpp.set_parameters(AWSWideDataFrameTests.number_rows, AWSWideDataFrameTests.params)
        return lpp
    
    def setup_cache(self):
        # Each class that has specific setup and inherits from another class,
        # must implement setup_cache
        super().setup_cache()


class AWSReadWriteWithQueryStats(AWSReadWrite):
    """
    This class inherits from AWSReadWrite and always runs with query_stats enabled
    """
    rounds = 1
    number = 3 # invokes 3 times the test runs between each setup-teardown 
    repeat = 1 # defines the number of times the measurements will invoke setup-teardown
    min_run_count = 1
    warmup_time = 0
    timeout = 1200
    param_names = ["num_rows"]
    params = [1_000_000, 2_000_000]

    library_manager = TestLibraryManager(storage=Storage.AMAZON, name_benchmark="READ_WRITE_QUERY_STATS")

    def get_library_manager(self) -> TestLibraryManager:
        return AWSReadWriteWithQueryStats.library_manager
    
    def get_population_policy(self) -> LibraryPopulationPolicy:
        lpp = LibraryPopulationPolicy(self.get_logger(), AllColumnTypesGenerator()).set_parameters(AWSReadWriteWithQueryStats.params)
        return lpp
        
    def setup_cache(self):
       super().setup_cache()

    def setup(self, num_rows):
        super().setup(num_rows)
        qs.enable()

    def teardown(self, num_rows):
        qs.disable()
        qs.reset_stats()
        super().teardown(num_rows)
