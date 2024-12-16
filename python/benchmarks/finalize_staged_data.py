"""
Copyright 2024 Man Group Operations Limited
Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
from arcticdb import Arctic
from tests.stress.arcticdb.version_store.test_stress_finalize_stage_data import CachedDFGenerator, stage_chunks
from arcticdb.util.utils import TimestampNumber
"""

import sys
from arcticdb.arctic import Arctic
from arcticdb.util.utils import CachedDFGenerator, TimestampNumber, stage_chunks
from arcticdb.version_store.library import Library, StagedDataFinalizeMethod
from .common import *


class FinalizeStagedData:
    """
    Check and benchmark performance of finalize_staged_data().
    Due to specifics of this procedure we tune asv to make single measurement
    which would be over a relatively big staged data.
    """

    number = 1
    rounds = 1
    repeat = 1
    min_run_count = 1

    timeout = 600
    LIB_NAME = "Finalize_Staged_Data_LIB"

    # Define the number of chunks
    params = [1000, 2000]

    def __init__(self):
        self.lib_name = FinalizeStagedData.LIB_NAME
        self.symbol = "symbol"
        self.time_runs = {}

    def setup_cache(self):
        # Generating dataframe with all kind of supported data types
        cachedDF = CachedDFGenerator(350000, [5])
        return cachedDF

    def setup(self, cache: CachedDFGenerator, param: int):
        cachedDF = cache

        # Unfortunately there is no way to tell asv to run single time
        # each of finalize_stage_data() tests if we do the large setup in the
        # setup_cache() method. We can only force it to work with single execution
        # if the symbol setup with stage data is in the setup() method

        self.ac = Arctic(f"lmdb://{self.lib_name}{param}?map_size=40GB")
        self.ac.delete_library(self.lib_name)
        self.lib = self.ac.create_library(self.lib_name)

        INITIAL_TIMESTAMP: TimestampNumber = TimestampNumber(
            0, cachedDF.TIME_UNIT
        )  # Synchronize index frequency

        df = cachedDF.generate_dataframe_timestamp_indexed(200, 0, cachedDF.TIME_UNIT)
        list_of_chunks = [10000] * param
        self.symbol

        self.lib.write(self.symbol, data=df, prune_previous_versions=True)
        stage_chunks(self.lib, self.symbol, cachedDF, INITIAL_TIMESTAMP, list_of_chunks)

    def time_finalize_staged_data(self, cache: CachedDFGenerator, param: int):
        print(">>> Library:", self.lib)
        print(">>> Symbol:", self.symbol)
        self.lib.finalize_staged_data(self.symbol, mode=StagedDataFinalizeMethod.WRITE)

    def peakmem_finalize_staged_data(self, cache: CachedDFGenerator, param: int):
        print(">>> Library:", self.lib)
        print(">>> Symbol:", self.symbol)
        self.lib.finalize_staged_data(self.symbol, mode=StagedDataFinalizeMethod.WRITE)

    def teardown(self, cache: CachedDFGenerator, param: int):
        self.ac.delete_library(self.lib_name)


from asv_runner.benchmarks.mark import SkipNotImplemented


class FinalizeStagedDataWiderDataframeX3(FinalizeStagedData):
    """
    The test is meant to be executed with 3 times wider dataframe than the base test
    """

    def setup_cache(self):
        # Generating dataframe with all kind of supported data type
        cachedDF = CachedDFGenerator(
            350000, [5, 25, 50]
        )  # 3 times wider DF with bigger string columns
        return cachedDF

    def setup(self, cache: CachedDFGenerator, param: int):
        if not SLOW_TESTS:
            raise SkipNotImplemented("Slow tests are skipped")
        super().setup(cache, param)

    def time_finalize_staged_data(self, cache: CachedDFGenerator, param: int):
        if not SLOW_TESTS:
            raise SkipNotImplemented("Slow tests are skipped")
        super().time_finalize_staged_data(cache, param)

    def peakmem_finalize_staged_data(self, cache: CachedDFGenerator, param: int):
        if not SLOW_TESTS:
            raise SkipNotImplemented("Slow tests are skipped")
        super().peakmem_finalize_staged_data(cache, param)

    def teardown(self, cache: CachedDFGenerator, param: int):
        if SLOW_TESTS:
            # Run only on slow tests
            self.ac.delete_library(self.lib_name)
