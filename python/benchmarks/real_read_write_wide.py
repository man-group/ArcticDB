"""
Copyright 2025 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

import os
import time
from typing import List, Union
import numpy as np
import pandas as pd
from arcticdb.options import LibraryOptions
from arcticdb.util.environment_setup import GeneralSetupLibraryWithSymbols, Storage


class AWSWideDataFrameTests:
    """
    This is ASV class for testing arcticdb with wide dataframe


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

    SETUP_CLASS = (GeneralSetupLibraryWithSymbols(storage=Storage.AMAZON,
                                                  # Define UNIQUE STRING for persistent libraries names 
                                                  # as well as name of unique storage prefix
                                                  prefix="WIDE_TESTS", 
                                                  library_options=LibraryOptions(rows_per_segment=1000, columns_per_segment=1000))
                    .set_params([
                        [2500, 3000],
                        [15000, 30000]]))

    params = SETUP_CLASS.get_parameter_list()
    param_names = ["num_rows", "num_cols"]

    def setup_cache(self):
        '''
        Always provide implementation of setup_cache in
        the child class

        And always return storage info which should 
        be first parameter for setup, tests and teardown
        '''
        setup_env = AWSWideDataFrameTests.SETUP_CLASS.setup_environment() 
        info = setup_env.get_storage_info()
        # NOTE: use only logger defined by setup class
        setup_env.logger().info(f"storage info object: {info}")
        return info

    def setup(self, storage_info, num_rows, num_cols):
        '''
        This setup method for read and writes can be executed only once
        No need to be executed before each test. That is why we define 
        `repeat` as 1
        '''
        ## Construct back from arctic url the object
        self.storage = GeneralSetupLibraryWithSymbols.from_storage_info(storage_info)
        sym = self.storage.get_symbol_name(num_rows, num_cols)
        self.to_write_df = self.storage.get_library().read(symbol=sym).data
        ##
        ## Writing into library that has suffix same as process
        ## will protect ASV processes from writing on one and same symbol
        ## this way each one is going to have its unique library
        self.write_library = self.storage.get_modifiable_library(os.getpid())

    def time_read_wide(self, storage_info, num_rows, num_cols):
        sym = self.storage.get_symbol_name(num_rows, num_cols)
        self.storage.get_library().read(symbol=sym)

    def peakmem_read_wide(self, storage_info, num_rows, num_cols):
        sym = self.storage.get_symbol_name(num_rows, num_cols)
        self.storage.get_library().read(symbol=sym)

    def time_write_wide(self, storage_info, num_rows, num_cols):
        sym = self.storage.get_symbol_name(num_rows, num_cols)
        self.write_library.write(symbol=sym, data=self.to_write_df)

    def peakmem_write_wide(self, storage_info, num_rows, num_cols):
        sym = self.storage.get_symbol_name(num_rows, num_cols)
        self.write_library.write(symbol=sym, data=self.to_write_df)        
 