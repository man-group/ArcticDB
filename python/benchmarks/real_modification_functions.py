"""
Copyright 2025 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

import os
from typing import Dict, List
import pandas as pd
from arcticdb.options import LibraryOptions
from arcticdb.util.environment_setup import TestLibraryManager, LibraryType, SequentialDataframesGenerator, Storage
from arcticdb.util.utils import TimestampNumber
from arcticdb.version_store.library import Library
from benchmarks.common import AsvBase
from arcticdb.util.test import config_context


# region Setup classes

WIDE_DATAFRAME_NUM_COLS = 30_000


class CacheForModifiableTests:
    """
    Stores pre-generated information for `AWSLargeAppendDataModify` test
    dictionary with keys that are for parameter values
    """

    def __init__(
        self,
    ):
        self.write_and_append_dict = {}
        self.update_full_dict = {}
        self.update_half_dict = {}
        self.update_upsert_dict = {}
        self.update_single_dict = {}
        self.append_single_dict = {}


# endregion


class AWSLargeAppendTests(AsvBase):
    rounds = 1
    number = 3  # invokes 3 times the test runs between each setup-teardown
    repeat = 1  # defines the number of times the measurements will invoke setup-teardown
    min_run_count = 1
    warmup_time = 0

    timeout = 1200

    params = [500_000, 1_000_000]  # [1000, 1500] # for test purposes
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
        return (pd.Timestamp("2-2-1986"), "s")

    def setup_cache(self):
        return self.initialize_cache(
            AWSLargeAppendTests.warmup_time,
            AWSLargeAppendTests.params,
            AWSLargeAppendTests.number_columns,
            AWSLargeAppendTests.number,
        )

    def initialize_cache(self, warmup_time, params, num_cols, num_sequential_dataframes):
        # warmup will execute tests additional time and we do not want that at all for write
        # update and append tests. We want exact specified `number` of times to be executed between
        assert warmup_time == 0, "warm is expected to be 0. If not 0 all tests will be invalid or not working"

        num_sequential_dataframes += 1
        cache = CacheForModifiableTests()
        generator = SequentialDataframesGenerator()

        initial_timestamp, freq = self.get_index_info()

        for num_rows in params:
            df_list = generator.generate_sequential_dataframes(
                number_data_frames=num_sequential_dataframes,
                number_rows=num_rows,
                number_columns=num_cols,
                start_timestamp=initial_timestamp,
                freq=freq,
            )
            cache.write_and_append_dict[num_rows] = df_list
            self.initialize_update_dataframes(
                num_rows=num_rows, num_cols=num_cols, cached_results=cache, generator=generator
            )

        self.get_library_manager().clear_all_benchmark_libs()

        self.get_library_manager().log_info()

        return cache

    def initialize_update_dataframes(
        self,
        num_rows: int,
        num_cols: int,
        cached_results: CacheForModifiableTests,
        generator: SequentialDataframesGenerator,
    ):
        logger = self.get_logger()
        initial_timestamp, freq = self.get_index_info()
        timestamp_number = TimestampNumber.from_timestamp(initial_timestamp, freq)

        def log_time_range(update_dict: Dict, update_type: str):
            time_range = generator.get_first_and_last_timestamp([update_dict[num_rows]])
            logger.info(f"Time range {update_type.upper()} update {time_range}")

        def generate_and_log(update_dict: Dict, update_type: str, number_rows: int, start_ts: pd.Timestamp):
            df = generator.df_generator.get_dataframe(
                number_rows=number_rows, number_columns=num_cols, start_timestamp=start_ts, freq=freq
            )
            update_dict[num_rows] = df
            log_time_range(update_dict, update_type)

        logger.info(f"Frame START-LAST Timestamps {timestamp_number} == {timestamp_number + num_rows}")

        # Full update
        generate_and_log(cached_results.update_full_dict, "update_full_dict", num_rows, initial_timestamp)

        # Half update
        half = num_rows // 2
        timestamp_number.inc(half - 3)
        generate_and_log(cached_results.update_half_dict, "update_half_dict", half, timestamp_number.to_timestamp())

        # Upsert update
        generate_and_log(
            cached_results.update_upsert_dict, "update_upsert_dict", num_rows, timestamp_number.to_timestamp()
        )

        # Single update
        timestamp_number.inc(half)
        generate_and_log(cached_results.update_single_dict, "update_single_dict", 1, timestamp_number.to_timestamp())

        # Single append
        next_timestamp = generator.get_next_timestamp_number(cached_results.write_and_append_dict[num_rows], freq)
        generate_and_log(cached_results.append_single_dict, "append_single_dict", 1, next_timestamp.to_timestamp())

    def setup(self, cache: CacheForModifiableTests, num_rows):
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
    number = 3  # invokes 3 times the test runs between each setup-teardown
    repeat = 1  # defines the number of times the measurements will invoke setup-teardown
    min_run_count = 1
    warmup_time = 0

    timeout = 1200

    params = [2_500, 5_000]  # [100, 150] for test purposes
    param_names = ["num_rows"]

    library_manager = TestLibraryManager(storage=Storage.AMAZON, name_benchmark="APPEND_LARGE_WIDE")

    number_columns = 3_000

    def get_library_manager(self) -> TestLibraryManager:
        return AWS30kColsWideDFLargeAppendTests.library_manager

    def setup_cache(self):
        return self.initialize_cache(
            AWS30kColsWideDFLargeAppendTests.warmup_time,
            AWS30kColsWideDFLargeAppendTests.params,
            AWS30kColsWideDFLargeAppendTests.number_columns,
            AWS30kColsWideDFLargeAppendTests.number,
        )


class AWSDeleteTestsFewLarge(AsvBase):
    """
    Delete is a special case and must not be with other tests.
    Main reason is because it is easy to create false results as
    delete will be not do anything if there is nothing to delete

    Therefore this test is designed to make sure delete has what is needed
    to delete and check that it is deleted
    """

    rounds = 1
    number = 1  # invokes 1 times the test runs between each setup-teardown
    repeat = 3  # defines the number of times the measurements will invoke setup-teardown
    min_run_count = 1
    warmup_time = 0

    timeout = 1200

    params = [500_000, 1_000_000]  # [100, 150] # for test purposes
    param_names = ["num_rows"]

    library_manager = TestLibraryManager(
        storage=Storage.AMAZON,
        name_benchmark="BASIC_DELETE_NEW",
        library_options=LibraryOptions(rows_per_segment=1000, columns_per_segment=1000),
    )

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

        num_sequential_dataframes = AWSDeleteTestsFewLarge.number_appends_to_symbol + 1  # for initial dataframe
        cache = CacheForModifiableTests()

        generator = SequentialDataframesGenerator()

        for num_rows in AWSDeleteTestsFewLarge.params:
            num_cols = AWSDeleteTestsFewLarge.number_columns
            df_list = generator.generate_sequential_dataframes(
                number_data_frames=num_sequential_dataframes,
                number_rows=num_rows,
                number_columns=num_cols,
                start_timestamp=pd.Timestamp("1-1-1980"),
                freq="s",
            )
            cache.write_and_append_dict[num_rows] = df_list

        manager = self.get_library_manager()
        manager.clear_all_benchmark_libs()
        manager.log_info()

        return cache

    def setup(self, cache: CacheForModifiableTests, num_rows):
        manager = self.get_library_manager()
        writes_list = cache.write_and_append_dict[num_rows]

        self.lib = manager.get_library(LibraryType.MODIFIABLE)

        self.setup_symbol(self.lib, writes_list)
        self.get_logger().info(f"Library {self.lib}")
        assert self.lib.has_symbol(self.symbol)
        self.symbol_deleted = False

    def setup_symbol(self, lib: Library, writes_list: List[pd.DataFrame]):
        logger = self.get_logger()
        self.symbol = f"_pid-{os.getpid()}"
        lib.write(self.symbol, writes_list[0])
        logger.info(f"Written first version {writes_list[0].shape[0]}")

        for frame in writes_list[1:]:
            lib.append(self.symbol, frame)
            logger.info(f"Appended frame {frame.shape[0]}")

    def teardown(self, cache, num_rows):
        ## This is check only for standard delete operation
        if self.symbol_deleted:
            assert not self.lib.has_symbol(self.symbol)
        self.get_library_manager().clear_all_modifiable_libs_from_this_process()

    def time_delete(self, cache, num_rows):
        self.lib.delete(self.symbol)
        self.symbol_deleted = True

    def time_delete_over_time(self, cache, num_rows):
        with config_context("VersionMap.ReloadInterval", 0):
            for i in range(25):
                self.lib.write("delete_over_time", pd.DataFrame())
                self.lib.delete("delete_over_time")
