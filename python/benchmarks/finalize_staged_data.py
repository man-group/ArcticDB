"""
Copyright 2024 Man Group Operations Limited
Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
from arcticdb import Arctic
from tests.stress.arcticdb.version_store.test_stress_finalize_stage_data import CachedDFGenerator, stage_chunks
from arcticdb.util.utils import TimestampNumber
"""

import time
from arcticdb.arctic import Arctic
from arcticdb.util.utils import CachedDFGenerator, TimestampNumber, stage_chunks
from arcticdb.version_store.library import Library, StagedDataFinalizeMethod
from .common import *
from shutil import copytree, rmtree


class FinalizeStagedData:
    """
    Check and benchmark performance of finalize_staged_data().
    Due to specifics of this procedure we tune asv to make single measurement
    which would be over a relatively big staged data.
    """

    number = 1
    rounds = 1
    repeat = 5
    min_run_count = 1
    warmup_time = 0
    timeout = 600

    # ASV creates temp directory for each run and then sets current working directory to it
    # After end of test will remove all files and subfolders created there.
    # No need for special tempfile handling
    ARCTIC_DIR = "staged_data"
    ARCTIC_DIR_ORIGINAL = "staged_data_original"
    CONNECTION_STRING = f"lmdb://{ARCTIC_DIR}?map_size=40GB"
    LIB_NAME = "Finalize_Staged_Data_LIB"

    # Define the number of chunks
    params = [1000]

    def __init__(self):
        self.lib_name = FinalizeStagedData.LIB_NAME
        self.logger = get_logger()

    def setup_cache(self):
        start = time.time()
        self._setup_cache(CachedDFGenerator(350000, [5]))
        self.logger.info(f"SETUP_CACHE TIME: {time.time() - start}")

    def _setup_cache(self, cachedDF):
        ac = Arctic(f"lmdb://{FinalizeStagedData.ARCTIC_DIR}?map_size=40GB")
        self.logger.info(f"{ac}")
        ac.delete_library(self.lib_name)
        lib = ac.create_library(self.lib_name)
        for param in FinalizeStagedData.params:
            symbol = f"symbol{param}"
            INITIAL_TIMESTAMP: TimestampNumber = TimestampNumber(0, cachedDF.TIME_UNIT)  # Synchronize index frequency

            df = cachedDF.generate_dataframe_timestamp_indexed(200, 0, cachedDF.TIME_UNIT)
            list_of_chunks = [10000] * param

            lib.write(symbol, data=df, prune_previous_versions=True)
            self.logger.info(f"LIBRARY: {lib}")
            self.logger.info(f"Created Symbol: {symbol}")
            stage_chunks(lib, symbol, cachedDF, INITIAL_TIMESTAMP, list_of_chunks)
        copytree(FinalizeStagedData.ARCTIC_DIR, FinalizeStagedData.ARCTIC_DIR_ORIGINAL)

    def setup(self, param: int):
        self.ac = Arctic(FinalizeStagedData.CONNECTION_STRING)
        self.lib = self.ac.get_library(self.lib_name)
        self.symbol = f"symbol{param}"

    def time_finalize_staged_data(self, param: int):
        self.logger.info(f"LIBRARY: {self.lib}")
        self.logger.info(f"Created Symbol: {self.symbol}")
        self.lib.finalize_staged_data(self.symbol, mode=StagedDataFinalizeMethod.WRITE)

    def peakmem_finalize_staged_data(self, param: int):
        self.logger.info(f"LIBRARY: {self.lib}")
        self.logger.info(f"Created Symbol: {self.symbol}")
        self.lib.finalize_staged_data(self.symbol, mode=StagedDataFinalizeMethod.WRITE)

    def teardown(self, param: int):
        rmtree(FinalizeStagedData.ARCTIC_DIR)
        copytree(FinalizeStagedData.ARCTIC_DIR_ORIGINAL, FinalizeStagedData.ARCTIC_DIR, dirs_exist_ok=True)
        del self.ac


from asv_runner.benchmarks.mark import SkipNotImplemented


class FinalizeStagedDataWiderDataframeX3(FinalizeStagedData):
    warmup_time = 0
    """
    The test is meant to be executed with 3 times wider dataframe than the base test
    """

    def setup_cache(self):
        # Generating dataframe with all kind of supported data type
        if not SLOW_TESTS:
            return  # Avoid setup when skipping
        cachedDF = CachedDFGenerator(350000, [5, 25, 50])  # 3 times wider DF with bigger string columns
        start = time.time()
        self._setup_cache(cachedDF)
        self.logger.info(f"SETUP_CACHE TIME: {time.time() - start}")

    def setup(self, param: int):
        if not SLOW_TESTS:
            raise SkipNotImplemented("Slow tests are skipped")
        super().setup(param)

    def time_finalize_staged_data(self, param: int):
        if not SLOW_TESTS:
            raise SkipNotImplemented("Slow tests are skipped")
        super().time_finalize_staged_data(param)

    def peakmem_finalize_staged_data(self, param: int):
        if not SLOW_TESTS:
            raise SkipNotImplemented("Slow tests are skipped")
        super().peakmem_finalize_staged_data(param)

    def teardown(self, param: int):
        if SLOW_TESTS:
            # Run only on slow tests
            self.ac.delete_library(self.lib_name)
