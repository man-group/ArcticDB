"""
Copyright 2025 Man Group Operations Limited
Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""
import time

from arcticdb.arctic import Arctic
from arcticdb.storage_fixtures.s3 import real_s3_from_environment_variables
from arcticdb.util.test_utils import CachedDFGenerator, TimestampNumber, stage_chunks
from arcticdb.version_store.library import Library, StagedDataFinalizeMethod
from .environment_setup import storages_for_asv, create_library
from .common import get_logger


def _symbol_name(num_chunks):
    return f"symbol{num_chunks}"


class FinalizeStagedData:
    number = 1
    rounds = 1
    repeat = 1
    min_run_count = 1
    warmup_time = 0
    timeout = 100

    storages = storages_for_asv()
    num_chunks = [10, 100]

    params = [
        num_chunks,
        storages
    ]

    param_names = ["num_chunks", "storage"]

    def __init__(self):
        self.logger = get_logger()
        self.lib = None
        self.symbol = None

    def setup_cache(self):
        start = time.time()

        # FUTURE rip out the CachedDFGenerator
        cachedDF = CachedDFGenerator(350000, [5])
        lib_for_storage = dict()
        for storage in self.storages:
            lib = create_library(storage)
            self._prepopulate_library(lib, cachedDF)
            lib_for_storage[storage] = lib

        self.logger.info(f"SETUP_CACHE TIME: {time.time() - start}")
        return lib_for_storage

    def _prepopulate_library(self, lib, df_generator):
        for param in self.num_chunks:
            initial_timestamp = TimestampNumber(0, df_generator.TIME_UNIT)

            list_of_chunks = [10_000] * param

            for suffix in ("-time", "-mem"):
                symbol = _symbol_name(param) + suffix
                stage_chunks(lib, symbol, df_generator, initial_timestamp, list_of_chunks)
                self.logger.info(f"Created Symbol: {symbol}")

    def setup(self, lib_for_storage, num_chunks, storage):
        self.lib = lib_for_storage[storage]
        self.symbol = _symbol_name(num_chunks)

    def time_finalize_staged_data(self, *args):
        staged_symbols = self.lib.get_staged_symbols()
        assert self.symbol + "-time" in staged_symbols
        self.lib.finalize_staged_data(self.symbol + "-time", mode=StagedDataFinalizeMethod.WRITE)

    def peakmem_finalize_staged_data(self, *args):
        staged_symbols = self.lib.get_staged_symbols()
        assert self.symbol + "-mem" in staged_symbols
        self.lib.finalize_staged_data(self.symbol + "-mem", mode=StagedDataFinalizeMethod.WRITE)
