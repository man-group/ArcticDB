"""
Copyright 2025 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

import os
from typing import List
import pandas as pd
from arcticdb.options import LibraryOptions
from arcticdb.util.environment_setup import TestLibraryManager, LibraryType, SequentialDataframesGenerator, Storage
from arcticdb.util.utils import TimestampNumber
from arcticdb.version_store.library import Library
from benchmarks.common import AsvBase


#region Setup classes

WIDE_DATAFRAME_NUM_COLS = 30_000

class LargeAppendDataModifyCache:
    """
    Stores pre-generated information for `AWSLargeAppendDataModify` test
    dictionary with keys that are for parameter values
    """

    def __init__(self, ):
        self.write_and_append_dict = {}
        self.update_full_dict = {}
        self.update_full_dict = {}
        self.update_half_dict = {}
        self.update_upsert_dict = {}
        self.update_single_dict = {}
        self.append_single_dict = {}

#endregion


class AWSLargeAppendTests(AsvBase):

    rounds = 1
    number = 3 # invokes 3 times the test runs between each setup-teardown 
    repeat = 1 # defines the number of times the measurements will invoke setup-teardown
    min_run_count = 1
    warmup_time = 0

    timeout = 1200

    params = [500_000, 1_000_000] # [1000, 1500] # for test purposes
    param_names = ["num_rows"]

    library_manager = TestLibraryManager(storage=Storage.AMAZON, name_benchmark="APPEND_LARGE")

    number_columns = 30

    def get_library_manager(self) -> TestLibraryManager:
        return AWSLargeAppendTests.library_manager
    
    def get_population_policy(self):
        pass
    
    def get_index_info(self):
        """
        Returns initial timestamp and index frequency
        """
        return (pd.Timestamp("2-2-1986"), 's')

    def setup_cache(self):
        return self.initialize_cache(AWSLargeAppendTests.warmup_time,
                                     AWSLargeAppendTests.params,
                                     AWSLargeAppendTests.number_columns,
                                     AWSLargeAppendTests.number)
    
    def initialize_cache(self, warmup_time, params, num_cols, num_sequential_dataframes):
        # warmup will execute tests additional time and we do not want that at all for write
        # update and append tests. We want exact specified `number` of times to be executed between
        assert warmup_time == 0, "warm up must be 0"

        num_sequential_dataframes = num_sequential_dataframes + 1
        cache = LargeAppendDataModifyCache()
        generator = SequentialDataframesGenerator()

        initial_timestamp, freq = self.get_index_info()

        for num_rows in params:
            df_list = generator.generate_sequential_dataframes(number_data_frames=num_sequential_dataframes,
                                                               number_rows=num_rows, number_columns=num_cols, 
                                                               start_timestamp=initial_timestamp, freq=freq)
            cache.write_and_append_dict[num_rows] = df_list
            self.initialize_update_dataframes(num_rows=num_rows, num_cols=num_cols, cached_results=cache, 
                                              generator=generator)
    
        self.get_library_manager().clear_all_benchmark_libs()

        self.get_library_manager().log_info()

        return cache
    
    def initialize_update_dataframes(self, num_rows: int, num_cols: int, cached_results: LargeAppendDataModifyCache, 
                                     generator: SequentialDataframesGenerator):
        
        logger = self.get_logger()
        initial_timestamp, freq = self.get_index_info()
        timestamp_number = TimestampNumber.from_timestamp(initial_timestamp, freq)
        end_timestamp_number = timestamp_number + num_rows
        logger.info(f"Frame START-LAST Timestamps {timestamp_number} == {end_timestamp_number}")

        # calculate update dataframes
        # update same size same date range start-end
        cached_results.update_full_dict[num_rows] = generator.df_generator.get_dataframe(number_rows=num_rows, 
                        number_columns=num_cols, start_timestamp=initial_timestamp, freq=freq)
        time_range = generator.get_first_and_last_timestamp([cached_results.update_full_dict[num_rows]])
        logger.info(f"Time range FULL update { time_range }")

        # update 2nd half of initial date range
        half = (num_rows // 2) 
        timestamp_number.inc(half - 3) 
        cached_results.update_half_dict[num_rows] = generator.df_generator.get_dataframe(number_rows=half, 
                        number_columns=num_cols, start_timestamp=timestamp_number.to_timestamp(), freq=freq)
        time_range = generator.get_first_and_last_timestamp([cached_results.update_half_dict[num_rows]])
        logger.info(f"Time range HALF update { time_range }")

        # update from the half with same size dataframe (end period is outside initial bounds)
        cached_results.update_upsert_dict[num_rows] = generator.df_generator.get_dataframe(number_rows=num_rows, 
                        number_columns=num_cols, start_timestamp=timestamp_number.to_timestamp(), freq=freq)
        time_range = generator.get_first_and_last_timestamp([cached_results.update_upsert_dict[num_rows]])
        logger.info(f"Time range UPSERT update { time_range }")

        # update one line at the end
        timestamp_number.inc(half) 
        cached_results.update_single_dict[num_rows] = generator.df_generator.get_dataframe(number_rows=1, 
                        number_columns=num_cols, start_timestamp=timestamp_number.to_timestamp(), freq=freq)
        time_range = generator.get_first_and_last_timestamp([cached_results.update_single_dict[num_rows]])
        logger.info(f"Time range SINGLE update { time_range }")

        next_timestamp =  generator.get_next_timestamp_number(cached_results.write_and_append_dict[num_rows], freq)
        cached_results.append_single_dict[num_rows] = generator.df_generator.get_dataframe(number_rows=1, 
                        number_columns=num_cols, start_timestamp=next_timestamp.to_timestamp(), freq=freq)
        time_range = generator.get_first_and_last_timestamp([cached_results.append_single_dict[num_rows]])
        logger.info(f"Time range SINGLE append { time_range }")
            
    def setup(self, cache: LargeAppendDataModifyCache, num_rows):
        manager = self.get_library_manager()
        self.cache = cache
        writes_list = self.cache.write_and_append_dict[num_rows]
        
        self.lib = manager.get_library(LibraryType.MODIFIABLE)

        self.symbol = f"symbol-{os.getpid()}"
        self.lib.write(self.symbol, writes_list[0])

        self.appends_list = writes_list[1:]

    def teardown(self, cache, num_rows):
        self.get_library_manager().clear_all_modifiable_libs_from_this_process()

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


class AWS30kColsWideDFLargeAppendTests(AWSLargeAppendTests):
    """
    Inherits from previous test all common functionalities.
    Defines specific such that the test targets operations with very wide dataframe
    """

    rounds = 1
    number = 3 # invokes 3 times the test runs between each setup-teardown 
    repeat = 1 # defines the number of times the measurements will invoke setup-teardown
    min_run_count = 1
    warmup_time = 0

    timeout = 1200

    params = [2_500, 5_000] #[100, 150] for test purposes
    param_names = ["num_rows"]

    library_manager = TestLibraryManager(storage=Storage.AMAZON, name_benchmark="APPEND_LARGE_WIDE")

    number_columns = 3_000

    def setup_cache(self):
        return self.initialize_cache(AWS30kColsWideDFLargeAppendTests.warmup_time,
                                     AWS30kColsWideDFLargeAppendTests.params,
                                     AWS30kColsWideDFLargeAppendTests.number_columns,
                                     AWS30kColsWideDFLargeAppendTests.number)    


class AWSDeleteTestsFewLarge(AsvBase):
    """
    Delete is a special case and must not be with other tests.
    Main reason is because it is easy to create false results as
    delete will be not do anything if there is nothing to delete

    Therefore this test is designed to make sure delete has what is needed
    to delete and check that it is deleted
    """
    
    rounds = 1
    number = 1 # invokes 1 times the test runs between each setup-teardown 
    repeat = 3 # defines the number of times the measurements will invoke setup-teardown
    min_run_count = 1
    warmup_time = 0

    timeout = 1200
    
    params = [500_000, 1_000_000] # [100, 150] # for test purposes
    param_names = ["num_rows"]

    library_manager = TestLibraryManager(storage=Storage.AMAZON, name_benchmark="BASIC_DELETE_NEW", 
                                     library_options=LibraryOptions(rows_per_segment=1000,columns_per_segment=1000))

    number_columns = 10

    number_appends_to_symbol = 3 

    def get_library_manager(self) -> TestLibraryManager:
        return AWSLargeAppendTests.library_manager
    
    def get_population_policy(self):
        pass

    def setup_cache(self):
        # warmup will execute tests additional time and we do not want that at all for write
        # update and append tests. We want exact specified `number` of times to be executed between
        assert AWSDeleteTestsFewLarge.warmup_time == 0, "warm up must be 0"
        assert AWSDeleteTestsFewLarge.number == 1, "delete works only once per setup=teardown"

        num_sequential_dataframes = AWSDeleteTestsFewLarge.number_appends_to_symbol + 1 # for initial dataframe
        cache = LargeAppendDataModifyCache()

        generator = SequentialDataframesGenerator()

        for num_rows in AWSDeleteTestsFewLarge.params:
            num_cols = AWSDeleteTestsFewLarge.number_columns
            df_list = generator.generate_sequential_dataframes(number_data_frames=num_sequential_dataframes,
                                                               number_rows=num_rows, number_columns=num_cols, 
                                                               start_timestamp=pd.Timestamp("1-1-1980"), freq='s')
            cache.write_and_append_dict[num_rows] = df_list
    
        manager = self.get_library_manager()
        manager.clear_all_benchmark_libs()
        manager.log_info()

        return cache
    
    def setup(self, cache: LargeAppendDataModifyCache, num_rows):
        manager = self.get_library_manager()
        writes_list = cache.write_and_append_dict[num_rows]

        self.lib = manager.get_library(LibraryType.MODIFIABLE)

        self.setup_symbol(self.lib, writes_list)
        self.get_logger().info(f"Library {self.lib}")
        assert self.lib.has_symbol(self.symbol)

    def setup_symbol(self, lib: Library, writes_list: List[pd.DataFrame]):
        logger = self.get_logger()
        self.symbol = f"_pid-{os.getpid()}"
        lib.write(self.symbol, writes_list[0])
        logger.info(f"Written first version {writes_list[0].shape[0]}")


        for frame in writes_list[1:]:
            lib.append(self.symbol, frame)
            logger.info(f"Appended frame {frame.shape[0]}")

    def teardown(self, cache, num_rows):
        assert not self.lib.has_symbol(self.symbol)
        self.get_library_manager().clear_all_modifiable_libs_from_this_process()

    def time_delete(self, cache, num_rows):
        self.lib.delete(self.symbol)    