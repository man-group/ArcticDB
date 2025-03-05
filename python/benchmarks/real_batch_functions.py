"""
Copyright 2025 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

import os
import numpy as np
import pandas as pd
from arcticdb.util.environment_setup import Storage, SetupMultipleLibraries
from arcticdb.util.utils import TimestampNumber
from arcticdb.version_store.library import Library, ReadRequest, WritePayload


class AWSBatchBasicFunctions:
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

    SETUP_CLASS = (SetupMultipleLibraries(storage=Storage.AMAZON, 
                                                      prefix="BASIC_BATCH")
                   .set_params([[500, 1000], [25_000, 50_000]]) # For test purposes
                   .set_number_symbols_parameter_index(0)
                   .set_number_rows_parameter_index(1)
                   .set_number_columns_parameter_index(None))
    params = SETUP_CLASS.get_parameter_list()
    param_names = ["num_symbols", "num_rows"]

    def setup_cache(self):
        set_env = AWSBatchBasicFunctions.SETUP_CLASS
        set_env.setup_environment()
        info = set_env.get_storage_info()
        # NOTE: use only logger defined by setup class
        set_env.logger().info(f"storage info object: {info}")
        return info

    def teardown(self, storage_info, num_symbols, num_rows):
        ## Delete own modifyable library
        self.setup_env.delete_modifiable_library(os.getpid())

    def setup(self, storage_info, num_symbols, num_rows):
        self.setup_env = SetupMultipleLibraries.from_storage_info(storage_info)
        
        self.lib: Library = self.setup_env.get_library(num_symbols)
        
        # Get generated symbol names
        self.symbols = []
        for num_symb_idx in range(num_symbols):
            sym_name = self.setup_env.get_symbol_name(num_symb_idx, num_rows, self.setup_env.default_number_cols)
            self.symbols.append(sym_name)

        #Construct read requests (will equal to number of symbols)
        self.read_reqs = [ReadRequest(symbol) for symbol in self.symbols]

        #Construct read requests based on 2 colmns, not whole DF (will equal to number of symbols)
        COLS = self.setup_env.generate_dataframe(0, self.setup_env.default_number_cols).columns[2:4]
        self.read_reqs_with_cols = [ReadRequest(symbol, columns=COLS) for symbol in self.symbols]

        #Construct dataframe that will be used for write requests, not whole DF (will equal to number of symbols)
        self.df: pd.DataFrame = self.setup_env.generate_dataframe(num_rows, self.setup_env.default_number_cols)

        #Construct read request with date_range
        self.date_range = self.get_last_x_percent_date_range(num_rows, 0.05)
        self.read_reqs_date_range = [ReadRequest(symbol, date_range=self.date_range) for symbol in self.symbols]
        
        ## Make sure each process has its own write library
        self.fresh_lib: Library = self.setup_env.get_modifiable_library(os.getpid())

    def get_last_x_percent_date_range(self, num_rows, percents):
        """
        Returns a date range selecting last X% of rows of dataframe
        pass percents as 0.0-1.0
        """
        freq = self.setup_env.index_freq
        start = TimestampNumber.from_timestamp(self.setup_env.start_timestamp, freq)
        percent_5 = int(num_rows * percents)
        end_range: TimestampNumber = start + num_rows
        start_range: TimestampNumber = end_range - percent_5
        range = pd.date_range(start=start_range.to_timestamp(), end=end_range.to_timestamp(), freq=freq)
        return range

    def time_read_batch(self, storage_info, num_symbols, num_rows):
        read_batch_result = self.lib.read_batch(self.read_reqs) 

    def time_write_batch(self, storage_info, num_symbols, num_rows):
        payloads = [WritePayload(symbol, self.df) for symbol in self.symbols]
        write_batch_result = self.fresh_lib.write_batch(payloads)

    def time_read_batch_with_columns(self, storage_info, num_symbols, num_rows):
        read_batch_result = self.lib.read_batch(self.read_reqs_with_cols)

    def peakmem_write_batch(self, storage_info, num_symbols, num_rows):
        payloads = [WritePayload(symbol, self.df) for symbol in self.symbols]
        write_batch_result = self.fresh_lib.write_batch(payloads)
        # Quick check all is ok (will not affect bemchmarks)
        assert write_batch_result[0].symbol in self.symbols
        assert write_batch_result[-1].symbol in self.symbols

    def peakmem_read_batch(self, storage_info, num_symbols, num_rows):
        read_batch_result = self.lib.read_batch(self.read_reqs)
        # Quick check all is ok (will not affect bemchmarks)
        assert read_batch_result[0].data.shape[0] == num_rows
        assert read_batch_result[-1].data.shape[0] == num_rows

    def peakmem_read_batch_with_columns(self, storage_info, num_symbols, num_rows):
        read_batch_result = self.lib.read_batch(self.read_reqs_with_cols)
        # Quick check all is ok (will not affect bemchmarks)
        assert read_batch_result[0].data.shape[0] == num_rows
        assert read_batch_result[-1].data.shape[0] == num_rows

    def time_read_batch_with_date_ranges(self, storage_info, num_symbols, num_rows):
        self.lib.read_batch(self.read_reqs_date_range)

    def peakmem_read_batch_with_date_ranges(self, storage_info, num_symbols, num_rows):
        read_batch_result = self.lib.read_batch(self.read_reqs_date_range)
        # Quick check all is ok (will not affect bemchmarks)
        assert read_batch_result[0].data.shape[0] > 2
        assert read_batch_result[-1].data.shape[0] > 2
