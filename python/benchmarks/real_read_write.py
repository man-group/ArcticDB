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

from arcticdb.util.environment_setup import DataFrameGenerator, LibraryManager, LibraryPopulationPolicy, LibraryType, Storage, get_console_logger, populate_library, populate_library_if_missing
from arcticdb.util.utils import DFGenerator, TimestampNumber
from arcticdb.version_store.library import Library, ReadRequest, WritePayload
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

    library_manager = LibraryManager(Storage.LMDB, "READ_WRITE")

    def get_logger(self) -> Logger:
        return get_console_logger(self)

    def get_library_manager(self) -> LibraryManager:
        return AWSReadWrite.library_manager
    
    def get_population_policy(self) -> LibraryPopulationPolicy:
        lpp = LibraryPopulationPolicy(AWSReadWrite.params, self.get_logger(), AllColumnTypesGenerator())
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
        start = TimestampNumber.from_timestamp(df_generator.initial_timestamp, freq)
        percent_5 = int(num_rows * percents)
        end_range: TimestampNumber = start + num_rows
        start_range: TimestampNumber = end_range - percent_5
        range = (start_range.to_timestamp(), end_range.to_timestamp())
        return range
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

    library_manager = LibraryManager(Storage.LMDB, "READ_WRITE_WIDE")

    param_names = ["num_cols"]
    params = [400, 1000]

    number_rows= 100

    def get_library_manager(self) -> LibraryManager:
        return AWSReadWriteWide.library_manager
    
    def get_population_policy(self) -> LibraryPopulationPolicy:
        lpp = LibraryPopulationPolicy(AWSReadWriteWide.params, self.get_logger())
        lpp.use_parameters_are_columns().set_rows(AWSReadWriteWide.number_rows)
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
    
    library_manager = LibraryManager(storage=Storage.LMDB, name_benchmark="LIST_SYMBOLS")

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
        return lpp

    def setup_cache(self):
        manager = self.get_library_manager()
        assert AWSListSymbols.number == 1, "There must be always one test between setup and tear down"
        policy = self.get_population_policy()
        for number_symbols in AWSListSymbols.params:
            policy.set_parameters([AWSListSymbols.number_rows] * number_symbols)
            populate_library_if_missing(manager, policy, LibraryType.PERSISTENT, number_symbols)

        manager.log_info() # Always log the ArcticURIs 
    
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

    library_manager = LibraryManager(storage=Storage.LMDB, name_benchmark="LIST_SYMBOLS")

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
        return lpp

    def setup_cache(self):
        manager = self.get_library_manager()
        policy = self.get_population_policy()
        last_snapshot_names_dict = {}
        for number_symbols in AWSVersionSymbols.params:
            policy.set_parameters([AWSVersionSymbols.number_rows] * number_symbols)
            populate_library_if_missing(manager, policy, LibraryType.PERSISTENT, number_symbols)
            lib = self.get_library_manager().get_library(LibraryType.PERSISTENT, number_symbols)
            snapshot_name = lib.list_snapshots(load_metadata=False)[-1]
            last_snapshot_names_dict[number_symbols] = snapshot_name
        manager.log_info() # Always log the ArcticURIs 
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


class AWSBatchBasicFunctions(AsvBase):
    """
    This is similar test to :class:`BatchBasicFunctions`
    Note that because batch functions are silent we do check if they work correctly along with 
    peakmem test where this will not influence result in any meaningful way
    """

    rounds = 1
    number = 3 # invokes 3 times the test runs between each setup-teardown 
    repeat = 1 # defines the number of times the measurements will invoke setup-teardown
    min_run_count = 1
    warmup_time = 0

    timeout = 1200

    params = [[5, 10], [250, 500]]
    param_names = ["num_symbols", "num_rows"]

    library_manager = LibraryManager(Storage.LMDB, "BASIC_BATCH")

    number_columns = 10

    def get_library_manager(self) -> LibraryManager:
        return AWSBatchBasicFunctions.library_manager
    
    def get_population_policy(self) -> LibraryPopulationPolicy:
        lpp = LibraryPopulationPolicy(AWSBatchBasicFunctions.params, self.get_logger())
        lpp.set_columns(AWSBatchBasicFunctions.number_columns)
        lpp.use_auto_increment_index()
        return lpp
    
    def setup_cache(self):
        manager = self.get_library_manager()
        policy = self.get_population_policy()
        number_symbols_list, number_rows_list = AWSBatchBasicFunctions.params
        for number_symbols in number_symbols_list:
            lib_suffix = number_symbols
            if not manager.has_library(LibraryType.PERSISTENT, lib_suffix):
                for number_rows in number_rows_list:
                    policy.set_parameters([number_rows] * lib_suffix)
                    # the name of symbols during generation will have now 2 parameters:
                    # the index of symbol + number of rows
                    # that allows generating more than one symbol in a library
                    policy.set_symbol_fixed_str(number_rows) 
                    populate_library(manager, policy, LibraryType.PERSISTENT, lib_suffix)
        manager.log_info() # Always log the ArcticURIs 

    def teardown(self, num_symbols, num_rows):
        # We could clear the modifiable libraries we used
        self.get_library_manager().clear_all_modifiable_libs_from_this_process()

    def setup(self, num_symbols, num_rows):
        self.manager = self.get_library_manager()
        self.population_policy = self.get_population_policy()
        # We use the same generator as the policy

        self.lib: Library = self.manager.get_library(LibraryType.PERSISTENT, num_symbols)
        self.write_lib: Library = self.manager.get_library(LibraryType.MODIFIABLE, num_symbols)
        self.get_logger().info(f"Library {self.lib}") 
        self.get_logger().debug(f"Symbols {self.lib.list_symbols()}") 
        
        # Get generated symbol names
        self.symbols = []
        for num_symb_idx in range(num_symbols):
            # the name is constructed of 2 parts index + number of rows
            sym_name = self.population_policy.get_symbol_name(num_symb_idx, num_rows)
            if not self.lib.has_symbol(sym_name):
                self.get_logger().error(f"symbol not found {sym_name}") 
            self.symbols.append(sym_name)

        #Construct read requests (will equal to number of symbols)
        self.read_reqs = [ReadRequest(symbol) for symbol in self.symbols]

        #Construct dataframe that will be used for write requests, not whole DF (will equal to number of symbols)
        self.df = self.population_policy.df_generator.get_dataframe(num_rows, AWSBatchBasicFunctions.number_columns)

        #Construct read requests based on 2 colmns, not whole DF (will equal to number of symbols)
        COLS = self.df.columns[2:4]
        self.read_reqs_with_cols = [ReadRequest(symbol, columns=COLS) for symbol in self.symbols]

        #Construct read request with date_range
        self.date_range = self.get_last_x_percent_date_range(num_rows, 0.05)
        self.read_reqs_date_range = [ReadRequest(symbol, date_range=self.date_range) for symbol in self.symbols]

    def get_last_x_percent_date_range(self, num_rows, percents):
        """
        Returns a date range tuple selecting last X% of rows of dataframe
        pass percents as 0.0-1.0
        """
        df_generator = self.population_policy.df_generator
        freq = df_generator.freq
        start = TimestampNumber.from_timestamp(df_generator.initial_timestamp, freq)
        percent_5 = int(num_rows * percents)
        end_range: TimestampNumber = start + num_rows
        start_range: TimestampNumber = end_range - percent_5
        range = (start_range.to_timestamp(), end_range.to_timestamp())
        return range
    
    def peakmem_read_batch(self, num_symbols, num_rows):
        read_batch_result = self.lib.read_batch(self.read_reqs)
        # Quick check all is ok (will not affect bemchmarks)
        assert read_batch_result[0].data.shape[0] == num_rows
        assert read_batch_result[-1].data.shape[0] == num_rows

    def time_read_batch(self, num_symbols, num_rows):
        read_batch_result = self.lib.read_batch(self.read_reqs) 

    def time_write_batch(self, num_symbols, num_rows):
        payloads = [WritePayload(symbol, self.df) for symbol in self.symbols]
        write_batch_result = self.write_lib.write_batch(payloads)

    def time_read_batch_with_columns(self, num_symbols, num_rows):
        read_batch_result = self.lib.read_batch(self.read_reqs_with_cols)

    def peakmem_write_batch(self, num_symbols, num_rows):
        payloads = [WritePayload(symbol, self.df) for symbol in self.symbols]
        write_batch_result = self.write_lib.write_batch(payloads)
        # Quick check all is ok (will not affect bemchmarks)
        assert write_batch_result[0].symbol in self.symbols
        assert write_batch_result[-1].symbol in self.symbols

    def peakmem_read_batch_with_columns(self, num_symbols, num_rows):
        read_batch_result = self.lib.read_batch(self.read_reqs_with_cols)
        # Quick check all is ok (will not affect bemchmarks)
        assert read_batch_result[0].data.shape[0] == num_rows
        assert read_batch_result[-1].data.shape[0] == num_rows

    def time_read_batch_with_date_ranges(self, num_symbols, num_rows):
        self.lib.read_batch(self.read_reqs_date_range)

    def peakmem_read_batch_with_date_ranges(self, num_symbols, num_rows):
        read_batch_result = self.lib.read_batch(self.read_reqs_date_range)
        # Quick check all is ok (will not affect bemchmarks)
        assert read_batch_result[0].data.shape[0] > 2
        assert read_batch_result[-1].data.shape[0] > 2
