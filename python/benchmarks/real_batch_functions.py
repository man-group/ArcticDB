"""
Copyright 2025 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

import time
import pandas as pd
from arcticdb.util.environment_setup import TestLibraryManager, LibraryPopulationPolicy, LibraryType, Storage, populate_library
from arcticdb.util.utils import DataRangeUtils, TimestampNumber
from arcticdb.version_store.library import Library, ReadRequest, WritePayload
from benchmarks.common import AsvBase


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

    # NOTE: If you plan to make changes to parameters, consider that a library with previous definition 
    #       may already exist. This means that symbols there will be having having different number
    #       of rows than what you defined in the test. To resolve this problem check with documentation:
    #           https://github.com/man-group/ArcticDB/wiki/ASV-Benchmarks:-Real-storage-tests
    params = [[500, 1000], [25_000, 50_000]] #[[5, 10], [250, 500]]
    param_names = ["num_symbols", "num_rows"]

    library_manager = TestLibraryManager(storage=Storage.AMAZON, name_benchmark="BASIC_BATCH")

    number_columns = 10
    initial_timestamp = pd.Timestamp("10-11-1978")
    freq = 's'

    def get_library_manager(self) -> TestLibraryManager:
        return AWSBatchBasicFunctions.library_manager
    
    def get_population_policy(self) -> LibraryPopulationPolicy:
        lpp = LibraryPopulationPolicy(None) # Silence logger when too noisy
        lpp.use_auto_increment_index()
        return lpp
    
    def setup_cache(self):
        manager = self.get_library_manager()
        policy = self.get_population_policy()
        logger = self.get_logger()
        number_symbols_list, number_rows_list = AWSBatchBasicFunctions.params
        for number_symbols in number_symbols_list:
            lib_suffix = number_symbols
            if not manager.has_library(LibraryType.PERSISTENT, lib_suffix):
                start = time.time()
                for number_rows in number_rows_list:
                    policy.set_parameters([number_rows] * lib_suffix, AWSBatchBasicFunctions.number_columns)
                    # the name of symbols during generation will have now 2 parameters:
                    # the index of symbol + number of rows
                    # that allows generating more than one symbol in a library
                    policy.set_symbol_fixed_str(number_rows) 
                    populate_library(manager, policy, LibraryType.PERSISTENT, lib_suffix)
                    logger.info(f"Generated {number_symbols} with {number_rows} each for {time.time()- start}")
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
        return DataRangeUtils.get_last_x_percent_date_range(initial_timestamp=df_generator.initial_timestamp,
                                                            freq=freq, num_rows=num_rows, percents=percents)
    
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
