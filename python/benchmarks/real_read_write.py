"""
Copyright 2025 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

from logging import Logger
import os
import time
from typing import List
import numpy as np
import pandas as pd

from arcticdb.util.environment_setup import DataFrameGenerator, LibraryManager, LibraryPopulationPolicy, LibraryType, Storage, get_console_logger
from arcticdb.util.utils import DFGenerator, TimestampNumber
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
              .add_timestamp_index("time", "s", pd.Timestamp("1-1-2000"))
              ).generate_dataframe()
        return df
    

#endregion

class AWSReadWrite(AsvBase):
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
    params = [250, 500]

    library_manager = LibraryManager(Storage.AMAZON, "READ_WRITE")

    def get_logger(self) -> Logger:
        return get_console_logger(self)

    def get_library_manager(self) -> LibraryManager:
        return AWSReadWrite.library_manager
    
    def get_population_policy(self) -> LibraryPopulationPolicy:
        lpp = LibraryPopulationPolicy(AWSReadWrite.params, self.get_logger(), AllColumnTypesGenerator())
        lpp.set_manager(self.get_library_manager())
        return lpp

    def setup_cache(self):
        '''
        In setup_cache we only populate the persistent libraries if they are missing.
        '''
        lgp = self.get_population_policy()
        lgp.populate_persistent_library_if_missing(LibraryType.PERSISTENT)
        lgp.manager.log_info()

    def setup(self, num_rows):
        self.population_policy = self.get_population_policy()
        self.symbol = self.population_policy.get_symbol_name(num_rows)
        # We use the same generator as the policy
        self.to_write_df = self.population_policy.df_generator.get_dataframe(num_rows, 0)
        
        # Functions operating on differetent date ranges to be moved in some shared utils
        #self.last_20 = utils.get_last_x_percent_date_range(num_rows, 20)

        self.read_lib = self.get_library_manager().get_library(LibraryType.PERSISTENT)
        self.write_lib = self.get_library_manager().get_library(LibraryType.MODIFIABLE)
        # We could also populate the library like so (we don't need )
        # populate_library(self.write_lib, )

    def teardown(self, num_rows):
        # We could clear the modifiable libraries we used
        # self.write_lib.clear()
        self.get_library_manager().clear_all_modifiable_libs_from_this_process()

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

    """
    def time_read_with_date_ranges_last20_percent_rows(self, num_rows):
        self.read_lib.read(symbol=self.sym, date_range=self.last_20).data

    def peakmem_read_with_date_ranges_last20_percent_rows(self, num_rows):
        self.read_lib.read(symbol=self.sym, date_range=self.last_20).data
    """

class AWSReadWriteWide(AWSReadWrite):
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

    library_manager = LibraryManager(Storage.AMAZON, "READ_WRITE_WIDE")

    param_names = ["num_cols"]
    params = [400, 1000]

    number_rows= 100

    def get_library_manager(self) -> LibraryManager:
        return AWSReadWriteWide.library_manager
    
    def get_population_policy(self) -> LibraryPopulationPolicy:
        lpp = LibraryPopulationPolicy(AWSReadWriteWide.params, self.get_logger())
        lpp.use_parameters_are_columns().set_rows(AWSReadWriteWide.number_rows)
        lpp.set_manager(self.get_library_manager())
        return lpp
    
    def setup_cache(self):
        # Each class that has specific setup and inherits from another class,
        # must implement setup_cache
        super().setup_cache()


class AWSListSymbols(AsvBase):

    rounds = 1
    number = 1 # invoke X times the test runs between each setup-teardown 
    repeat = 3 # defines the number of times the measurements will invoke setup-teardown
    min_run_count = 1
    warmup_time = 0

    timeout = 1200
    
    library_manager = LibraryManager(storage=Storage.AMAZON, name_benchmark="LIST_SYMBOLS")

    params = [6,8]
    param_names = ["num_syms"]

    number_columns = 2
    number_rows = 2

    def get_library_manager(self) -> LibraryManager:
        return AWSListSymbols.library_manager
    
    def get_population_policy(self) -> LibraryPopulationPolicy:
        lpp = LibraryPopulationPolicy(None, self.get_logger())
        lpp.set_columns(AWSListSymbols.number_columns)
        lpp.use_auto_increment_index()
        lpp.set_manager(self.get_library_manager())
        return lpp

    def setup_cache(self):
        assert AWSListSymbols.number == 1, "There must be always one test between setup and tear down"
        lgp = self.get_population_policy()
        for number_symbols in AWSListSymbols.params:
            lgp.set_parameters([AWSListSymbols.number_rows] * number_symbols)
            lgp.populate_persistent_library_if_missing(LibraryType.PERSISTENT, number_symbols)
        lgp.manager.log_info() # Always log the ArcticURIs 
    
    def setup(self, num_syms):
        self.population_policy = self.get_population_policy()
        self.lib = self.get_library_manager().get_library(LibraryType.PERSISTENT, num_syms)
        self.test_counter = 1
        assert num_syms == len(self.lib.list_symbols()), "The library contains expected number of symbols"
        self.lib._nvs.version_store._clear_symbol_list_keys() # clear cache

    def time_list_symbols(self, num_syms):
        assert self.test_counter == 1, "Test executed only once in setup-teardown cycle" 
        self.lib.list_symbols()
        self.test_counter += 1

    def time_has_symbol_nonexisting(self, num_syms):
        assert self.test_counter == 1, "Test executed only once in setup-teardown cycle" 
        self.lib.has_symbol("250_sym")        
        self.test_counter += 1

    def peakmem_list_symbols(self, num_syms):
        assert self.test_counter == 1, "Test executed only once in setup-teardown cycle" 
        self.lib.list_symbols()
        self.test_counter += 1


class AWSVersionSymbols(AsvBase):

    rounds = 1
    number = 3 # invoke X times the test runs between each setup-teardown 
    repeat = 1 # defines the number of times the measurements will invoke setup-teardown
    min_run_count = 1
    warmup_time = 0

    timeout = 1200

    library_manager = LibraryManager(storage=Storage.AMAZON, name_benchmark="LIST_SYMBOLS")

    params = [5,7]
    param_names = ["num_syms"]

    number_columns = 2
    number_rows = 2

    mean_number_versions_per_symbol = 5

    def get_library_manager(self) -> LibraryManager:
        return AWSVersionSymbols.library_manager
    
    def get_population_policy(self) -> LibraryPopulationPolicy:
        lpp = LibraryPopulationPolicy(None, self.get_logger())
        lpp.set_columns(AWSVersionSymbols.number_columns)
        lpp.use_auto_increment_index()
        lpp.generate_versions(versions_max=int(1.5 * AWSVersionSymbols.mean_number_versions_per_symbol), 
                              mean=AWSVersionSymbols.mean_number_versions_per_symbol)
        lpp.generate_metadata().generate_snapshots()
        lpp.set_manager(self.get_library_manager())
        return lpp

    def setup_cache(self):
        lgp = self.get_population_policy()
        last_snapshot_names_dict = {}
        for number_symbols in AWSVersionSymbols.params:
            lgp.set_parameters([AWSVersionSymbols.number_rows] * number_symbols)
            lgp.populate_persistent_library_if_missing(LibraryType.PERSISTENT, number_symbols)
            lib = self.get_library_manager().get_library(LibraryType.PERSISTENT, number_symbols)
            snaps = lib.list_snapshots(load_metadata=False)
            snap = snaps[-1]
            last_snapshot_names_dict[number_symbols] = snap
        lgp.manager.log_info() # Always log the ArcticURIs 
        return last_snapshot_names_dict
    
    def setup(self, last_snapshot_names_dict, num_syms):
        self.population_policy = self.get_population_policy()
        self.lib = self.get_library_manager().get_library(LibraryType.PERSISTENT, num_syms)
        self.test_counter = 1
        expected_num_versions = AWSVersionSymbols.mean_number_versions_per_symbol * num_syms
        assert num_syms == len(self.lib.list_symbols()), "The library contains expected number of symbols"
        assert expected_num_versions - 1 <= len(self.lib.list_versions()), "There are sufficient versions"
        assert expected_num_versions - 1 <= len(self.lib.list_snapshots()), "There are sufficient snapshots"
        assert last_snapshot_names_dict[num_syms] is not None

    def time_list_versions(self, last_snapshot_names_dict, num_syms):
        self.lib.list_versions()

    def time_list_versions_latest_only(self, last_snapshot_names_dict, num_syms):
        self.lib.list_versions(latest_only=True)        

    def time_list_versions_skip_snapshots(self, last_snapshot_names_dict, num_syms):
        self.lib.list_versions(skip_snapshots=True)        

    def time_list_versions_latest_only_and_skip_snapshots(self, last_snapshot_names_dict, num_syms):
        self.lib.list_versions(latest_only=True, skip_snapshots=True)        

    def time_list_versions_snapshot(self, last_snapshot_names_dict, num_syms):
        self.lib.list_versions(snapshot=last_snapshot_names_dict[num_syms])        

    def peakmem_list_versions(self, last_snapshot_names_dict, num_syms):
        self.lib.list_versions()

    def time_list_snapshots(self, last_snapshot_names_dict, num_syms):
        self.lib.list_snapshots()
    
    def time_list_snapshots_without_metadata(self, last_snapshot_names_dict, num_syms):
        self.lib.list_snapshots(load_metadata=False)

    def peakmem_list_snapshots(self, last_snapshot_names_dict, num_syms):
        self.lib.list_snapshots()
    
    def peakmem_list_snapshots_without_metadata(self, last_snapshot_names_dict, num_syms):
        self.lib.list_snapshots(load_metadata=False)
