"""
Copyright 2025 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

import os
from typing import List
import pandas as pd
from arcticdb.options import LibraryOptions
from arcticdb.util.environment_setup import Storage, StorageInfo, GeneralAppendSetup
from arcticdb.version_store.library import Library


#region Setup classes

WIDE_DATAFRAME_NUM_COLS = 30_000

class LargeAppendDataModifyCache:
    """
    Stores pre-generated information for `AWSLargeAppendDataModify` test
    dictionary with keys that are for parameter values
    """

    def __init__(self, ):
        self.storage_info: StorageInfo = None
        self.write_and_append_dict = {}
        self.update_full_dict = {}
        self.update_full_dict = {}
        self.update_half_dict = {}
        self.update_upsert_dict = {}
        self.update_single_dict = {}
        self.append_single_dict = {}

#endregion


class AWSWide30K:

    rounds = 1
    number = 3 # invokes 3 times the test runs between each setup-teardown 
    repeat = 1 # defines the number of times the measurements will invoke setup-teardown
    min_run_count = 1
    warmup_time = 0

    timeout = 1200

    SETUP_CLASS = (GeneralAppendSetup(storage=Storage.AMAZON, 
                                    prefix="BUG_WIDE_APPEND",
                                    library_options=LibraryOptions(rows_per_segment=1000,columns_per_segment=1000)
                                    )
                                    .set_default_columns(WIDE_DATAFRAME_NUM_COLS))
        
    params = [2_500] # [1000, 1500] # for test purposes
    param_names = ["num_rows"]

    def setup_cache(self):
        return self.initialize_cache(AWSWide30K.SETUP_CLASS,
                                     AWSWide30K.warmup_time,
                                     AWSWide30K.params)
    
    def initialize_cache(self, setup_obj: GeneralAppendSetup, warmup_time, params):
        # warmup will execute tests additional time and we do not want that at all for write
        # update and append tests. We want exact specified `number` of times to be executed between
        assert warmup_time == 0, "warm up must be 0"

        set_env = setup_obj
        num_sequenced_dataframes = AWSWide30K.number + 1
        cache = LargeAppendDataModifyCache()

        for num_rows in params:
            cache.write_and_append_dict[num_rows] = set_env.generate_chained_writes(num_rows, num_sequenced_dataframes)
            self.initialize_update_dataframes(num_rows=num_rows, cached_results=cache, set_env=set_env)
    
        #only create the library
        set_env.remove_all_modifiable_libraries(True)
        lib = set_env.get_modifiable_library()

        set_env.logger().info(f"Storage info: {set_env.get_storage_info()}")
        set_env.logger().info(f"Library: {lib}")
        # With modifiable tests we do not prepare libraries here,
        # but we do still return storage info as it has to be unique across processes
        # We also leave each process to setup its initial library in setup
        cache.storage_info = set_env.get_storage_info()
        return cache
    
    def initialize_update_dataframes(self, num_rows, cached_results: LargeAppendDataModifyCache, 
                                     set_env: GeneralAppendSetup):
        # calculate update dataframes
        # update same size same date range start-end
        timestamp_number = set_env.get_next_timestamp_number(cached_results.write_and_append_dict[num_rows],
                                                           set_env.FREQ )
        print(timestamp_number)
        cached_results.update_full_dict[num_rows] = set_env.generate_dataframe(num_rows, timestamp_number)

        # update 2nd half of initial date range
        half = (num_rows // 2) 
        timestamp_number.inc(half - 3) 
        cached_results.update_half_dict[num_rows] = set_env.generate_dataframe(half, timestamp_number)

        # update from the half with same size dataframe (end period is outside initial bounds)
        cached_results.update_upsert_dict[num_rows] = set_env.generate_dataframe(num_rows, timestamp_number)
        timestamp_number.inc(half) 

        # update one line at the end
        cached_results.update_single_dict[num_rows] = set_env.generate_dataframe(1, timestamp_number)

        next_timestamp = timestamp_number.to_initial_value()
        cached_results.append_single_dict[num_rows] = set_env.generate_dataframe(1, 
                                                                               next_timestamp)
    
    def setup(self, cache: LargeAppendDataModifyCache, num_rows):
        self.set_env = GeneralAppendSetup.from_storage_info(cache.storage_info)
        self.cache = cache
        writes_list = self.cache.write_and_append_dict[num_rows]
        
        self.pid = os.getpid()
        self.set_env.remove_all_modifiable_libraries(True)
        self.set_env.delete_modifiable_library(self.pid)
        self.lib = self.set_env.get_modifiable_library(self.pid)

        self.symbol = self.set_env.get_symbol_name_template(f"_pid-{self.pid}")
        self.lib.write(self.symbol, writes_list[0])

        self.appends_list = writes_list[1:]

    def teardown(self, cache, num_rows):
        self.set_env.delete_modifiable_library( self.pid )

    def time_update_single(self, cache, num_rows):
        self.lib.update(self.symbol, self.cache.update_single_dict[num_rows])

    def time_update_half(self, cache, num_rows):
        self.lib.update(self.symbol, self.cache.update_half_dict[num_rows])

    def time_update_full(self, cache, num_rows):
        #self.lib.update(self.symbol, self.cache.update_full)
        self.lib.update(self.symbol, self.cache.update_full_dict[num_rows])

    def time_update_upsert(self, cache, num_rows):
        self.lib.update(self.symbol, self.cache.update_upsert_dict[num_rows], upsert=True)

    def time_append_large(self, cache, num_rows):
        large: pd.DataFrame = self.appends_list.pop(0)
        self.lib.append(self.symbol, large)

    def time_append_single(self, cache, num_rows):
        self.lib.append(self.symbol, self.cache.append_single_dict[num_rows])
    
class AWSWide30KNoSegments(AWSWide30K):

    rounds = 1
    number = 3 # invokes 3 times the test runs between each setup-teardown 
    repeat = 1 # defines the number of times the measurements will invoke setup-teardown
    min_run_count = 1
    warmup_time = 0

    timeout = 1200

    SETUP_CLASS = (GeneralAppendSetup(storage=Storage.AMAZON, 
                                    prefix="BUG_WIDE_APPEND",
                                    )
                                    .set_default_columns(WIDE_DATAFRAME_NUM_COLS))
        
    params = [1_000, 2_500, 5_000] # [1000, 1500] # for test purposes
    param_names = ["num_rows"]

    def setup_cache(self):
        return self.initialize_cache(AWSWide30KNoSegments.SETUP_CLASS,
                                     AWSWide30KNoSegments.warmup_time,
                                     AWSWide30KNoSegments.params)
    

class AWSWide30KNewInitializeFoUpdates(AWSWide30K):

    rounds = 1
    number = 3 # invokes 3 times the test runs between each setup-teardown 
    repeat = 1 # defines the number of times the measurements will invoke setup-teardown
    min_run_count = 1
    warmup_time = 0

    timeout = 1200

    SETUP_CLASS = (GeneralAppendSetup(storage=Storage.AMAZON, 
                                    prefix="BUG_WIDE_APPEND",
                                    library_options=LibraryOptions(rows_per_segment=1000,columns_per_segment=1000)
                                    )
                                    .set_default_columns(WIDE_DATAFRAME_NUM_COLS))
        
    params = [2_500, 5_000] # [1000, 1500] # for test purposes
    param_names = ["num_rows"]

    def setup_cache(self):
        return self.initialize_cache(AWSWide30KNewInitializeFoUpdates.SETUP_CLASS,
                                     AWSWide30KNewInitializeFoUpdates.warmup_time,
                                     AWSWide30KNewInitializeFoUpdates.params)
    
    def initialize_update_dataframes(self, num_rows, cached_results: LargeAppendDataModifyCache, 
                                     set_env: GeneralAppendSetup):
        timestamp_number = set_env.get_initial_time_number()
        end_timestamp_number = timestamp_number + num_rows
        set_env.logger().info(f"Frame START-LAST Timestamps {timestamp_number} == {end_timestamp_number}")

        # calculate update dataframes
        # update same size same date range start-end
        cached_results.update_full_dict[num_rows] = set_env.generate_dataframe(num_rows, timestamp_number)
        time_range = set_env.get_first_and_last_timestamp([cached_results.update_full_dict[num_rows]])
        set_env.logger().info(f"Time range FULL update { time_range }")

        # update 2nd half of initial date range
        half = (num_rows // 2) 
        timestamp_number.inc(half - 3) 
        cached_results.update_half_dict[num_rows] = set_env.generate_dataframe(half, timestamp_number)
        time_range = set_env.get_first_and_last_timestamp([cached_results.update_half_dict[num_rows]])
        set_env.logger().info(f"Time range HALF update { time_range }")

        # update from the half with same size dataframe (end period is outside initial bounds)
        cached_results.update_upsert_dict[num_rows] = set_env.generate_dataframe(num_rows, timestamp_number)
        time_range = set_env.get_first_and_last_timestamp([cached_results.update_upsert_dict[num_rows]])
        set_env.logger().info(f"Time range UPSERT update { time_range }")

        # update one line at the end
        timestamp_number.inc(half) 
        cached_results.update_single_dict[num_rows] = set_env.generate_dataframe(1, timestamp_number)
        time_range = set_env.get_first_and_last_timestamp([cached_results.update_single_dict[num_rows]])
        set_env.logger().info(f"Time range SINGLE update { time_range }")

        next_timestamp =  set_env.get_next_timestamp_number(cached_results.write_and_append_dict[num_rows],
                                                           set_env.FREQ )
        cached_results.append_single_dict[num_rows] = set_env.generate_dataframe(1, 
                                                                                next_timestamp)
        time_range = set_env.get_first_and_last_timestamp([cached_results.append_single_dict[num_rows]])
        set_env.logger().info(f"Time range SINGLE append { time_range }")
        