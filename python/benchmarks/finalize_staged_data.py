"""
Copyright 2026 Man Group Operations Limited
Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

from arcticdb.util.test_utils import CachedDFGenerator, TimestampNumber, stage_chunks
from arcticdb.version_store.library import StagedDataFinalizeMethod
from asv_runner.benchmarks.mark import SkipNotImplemented

from .environment_setup import is_storage_enabled, create_library, Storage
from .common import get_logger


def _symbol_name(num_chunks):
    return f"symbol{num_chunks}"


class FinalizeStagedData:
    rounds = 1
    repeat = 3  # Safe to increase if needed, each `setup` creates the staged data
    min_run_count = 1

    # We need to run each benchmark just once after setup to make sure there is staged data
    # so it is not safe to change these.
    number = 1
    warmup_time = 0

    timeout = 100

    storages = [Storage.LMDB, Storage.AMAZON]
    num_chunks = [10, 100]

    params = [num_chunks, storages]

    param_names = ["num_chunks", "storage"]

    def __init__(self):
        self.logger = get_logger()
        self.lib = None
        self.symbol = None
        # FUTURE rip out the CachedDFGenerator
        self.df_generator = CachedDFGenerator(350000, [5])

    def setup_cache(self):
        lib_for_storage = dict()
        for storage in self.storages:
            lib = create_library(storage) if is_storage_enabled(storage) else None
            lib_for_storage[storage] = lib

        return lib_for_storage

    def setup(self, lib_for_storage, num_chunks, storage):
        self.lib = lib_for_storage[storage]
        if self.lib is None:
            raise SkipNotImplemented

        assert len(self.lib.list_symbols()) == 0  # check we are in a clean state
        initial_timestamp = TimestampNumber(0, self.df_generator.TIME_UNIT)

        list_of_chunks = [10_000] * num_chunks

        for suffix in ("-time", "-mem"):
            symbol = _symbol_name(num_chunks) + suffix
            stage_chunks(self.lib, symbol, self.df_generator, initial_timestamp, list_of_chunks)
            self.logger.info(f"Created Symbol: {symbol}")
        self.symbol = _symbol_name(num_chunks)

    def teardown(self, lib_for_storage, num_chunks, storage):
        if self.lib is None:
            return
        assert len(self.lib.list_symbols()) == 1  # check that the benchmark actually did something
        self.lib._nvs.version_store.clear()

    def time_finalize_staged_data(self, *args):
        staged_symbols = self.lib.get_staged_symbols()
        assert self.symbol + "-time" in staged_symbols
        self.lib.finalize_staged_data(self.symbol + "-time", mode=StagedDataFinalizeMethod.WRITE)

    def peakmem_finalize_staged_data(self, *args):
        staged_symbols = self.lib.get_staged_symbols()
        assert self.symbol + "-mem" in staged_symbols
        self.lib.finalize_staged_data(self.symbol + "-mem", mode=StagedDataFinalizeMethod.WRITE)
