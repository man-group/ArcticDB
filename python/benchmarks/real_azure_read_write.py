"""
Copyright 2024 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

import time
from arcticdb.util.utils import DFGenerator
from arcticdb.util.environment_setup import TestLibraryManager, LibraryPopulationPolicy, LibraryType, Storage, populate_library
from arcticdb.util.utils import DataRangeUtils
from benchmarks.common import AsvBase


class AzureReadWrite(AsvBase):
    """
    This class is for general read write tests on Azure Blob Storage
    """
    rounds = 1
    number = 3  # invokes 3 times the test runs between each setup-teardown 
    repeat = 1  # defines the number of times the measurements will invoke setup-teardown
    min_run_count = 1
    warmup_time = 0

    timeout = 1200

    library_manager = TestLibraryManager(storage=Storage.AZURE, name_benchmark="READ_WRITE")
    library_type = LibraryType.PERSISTENT

    param_names = ["num_rows"]
    params = [10_000_000]  # 10M rows

    number_columns = 100  # 100 columns

    def get_library_manager(self) -> TestLibraryManager:
        return AzureReadWrite.library_manager
    
    def get_population_policy(self) -> LibraryPopulationPolicy:
        lpp = LibraryPopulationPolicy(self.get_logger())
        lpp.set_parameters(AzureReadWrite.params, [AzureReadWrite.number_columns])
        return lpp
    
    def setup_cache(self):
        self.setup_library()
        self.symbol = "test_symbol"
        self.to_write_df = None
        self.last_20 = None

    def setup(self, num_rows):
        self.setup_library()
        self.lib = self.get_library_manager().get_library(AzureReadWrite.library_type, 1)
        
        # Generate test data with mixed types including strings
        df_generator = DFGenerator(num_rows, [AzureReadWrite.number_columns])
        self.to_write_df = df_generator.generate_dataframe()
        
        # Add some string columns
        string_cols = [f"string_{i}" for i in range(10)]  # 10 string columns
        for col in string_cols:
            self.to_write_df[col] = [f"string_value_{i}" for i in range(num_rows)]
        
        # Write the data
        self.lib.write(self.symbol, self.to_write_df)
        
        # Calculate date range for last 20% of rows
        self.last_20 = self.get_last_x_percent_date_range(num_rows, 20)

    def time_read(self, num_rows):
        self.lib.read(self.symbol)

    def peakmem_read(self, num_rows):
        self.lib.read(self.symbol)

    def time_write(self, num_rows):
        self.lib.write(self.symbol, self.to_write_df)

    def peakmem_write(self, num_rows):
        self.lib.write(self.symbol, self.to_write_df)

    def time_read_with_column_float(self, num_rows):
        COLS = ["float2"]
        self.lib.read(symbol=self.symbol, columns=COLS).data

    def peakmem_read_with_column_float(self, num_rows):
        COLS = ["float2"]
        self.lib.read(symbol=self.symbol, columns=COLS).data

    def time_read_with_columns_all_types(self, num_rows):
        COLS = ["float2", "string_0", "bool", "int64", "uint64"]
        self.lib.read(symbol=self.symbol, columns=COLS).data

    def peakmem_read_with_columns_all_types(self, num_rows):
        COLS = ["float2", "string_0", "bool", "int64", "uint64"]
        self.lib.read(symbol=self.symbol, columns=COLS).data

    def time_write_staged(self, num_rows):
        lib = self.lib
        lib.write(self.symbol, self.to_write_df, staged=True)
        lib._nvs.compact_incomplete(self.symbol, False, False)

    def peakmem_write_staged(self, num_rows):
        lib = self.lib
        lib.write(self.symbol, self.to_write_df, staged=True)
        lib._nvs.compact_incomplete(self.symbol, False, False)

    def time_read_with_date_ranges_last20_percent_rows(self, num_rows):
        self.lib.read(symbol=self.symbol, date_range=self.last_20).data

    def peakmem_read_with_date_ranges_last20_percent_rows(self, num_rows):
        self.lib.read(symbol=self.symbol, date_range=self.last_20).data

    def get_last_x_percent_date_range(self, num_rows, percents):
        df_generator = self.population_policy.df_generator
        freq = df_generator.freq
        return DataRangeUtils.get_last_x_percent_date_range(
            initial_timestamp=df_generator.initial_timestamp,
            freq=freq, 
            num_rows=num_rows, 
            percents=percents
        )


class AzureListVersions(AsvBase):
    """
    This class is for testing list_versions performance on Azure Blob Storage
    """
    rounds = 1
    number = 3
    repeat = 1
    min_run_count = 1
    warmup_time = 0

    timeout = 1200

    library_manager = TestLibraryManager(storage=Storage.AZURE, name_benchmark="LIST_VERSIONS")
    library_type = LibraryType.PERSISTENT

    param_names = ["num_symbols"]
    params = [10_000]  # 10k symbols

    def get_library_manager(self) -> TestLibraryManager:
        return AzureListVersions.library_manager
    
    def get_population_policy(self) -> LibraryPopulationPolicy:
        lpp = LibraryPopulationPolicy(self.get_logger())
        lpp.set_parameters([1000] * AzureListVersions.params[0], [10])  # 1000 rows per symbol, 10 columns
        return lpp
    
    def setup_cache(self):
        self.setup_library()
        self.test_counter = 1

    def setup(self, num_symbols):
        self.setup_library()
        self.lib = self.get_library_manager().get_library(AzureListVersions.library_type, num_symbols)
        
        # Generate and write test data
        start = time.time()
        policy = self.get_population_policy()
        policy.set_parameters([1000] * num_symbols, [10])
        if not self.library_manager.has_library(AzureListVersions.library_type, num_symbols):
            populate_library(self.library_manager, policy, AzureListVersions.library_type, num_symbols)
            self.get_logger().info(f"Generated {num_symbols} symbols with 1000 rows each in {time.time() - start:.2f}s")
        else:
            self.get_logger().info("Library already exists, population skipped")
        
        # Clear cache to ensure we're testing actual storage performance
        self.lib._nvs.version_store._clear_symbol_list_keys()

    def time_list_versions(self, num_symbols):
        assert self.test_counter == 1, "Test executed only once in setup-teardown cycle"
        self.lib.list_versions()
        self.test_counter += 1

    def peakmem_list_versions(self, num_symbols):
        assert self.test_counter == 1, "Test executed only once in setup-teardown cycle"
        self.lib.list_versions()
        self.test_counter += 1 