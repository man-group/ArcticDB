"""
Copyright 2025 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""


import os
from arcticdb.util.environment_setup import Storage, GeneralUseCaseNoSetup
from arcticdb.util.utils import CachedDFGenerator, TimestampNumber, stage_chunks
from arcticdb.version_store.library import StagedDataFinalizeMethod


class AWSFinalizeStagedData:
    """
    Checks finalizing staged data. Note, that staged symbols can be finalized only twice,
    therefore certain design decisions must be taken in advance so each process sets up
    environment for exactly one test
    """

    rounds = 1
    number = 1 
    repeat = 1 
    min_run_count = 1
    warmup_time = 0

    timeout = 1200

    SETUP_CLASS = (GeneralUseCaseNoSetup(storage=Storage.LMDB, 
                                                      prefix="FINALIZE"))
                   
    params = [500, 1000] # Test data [10, 20]
    param_names = ["num_chunks"]

    def setup_cache(self):
        # Preconditions for this test
        assert AWSFinalizeStagedData.number == 1
        assert AWSFinalizeStagedData.repeat == 1
        assert AWSFinalizeStagedData.rounds == 1
        assert AWSFinalizeStagedData.warmup_time == 0

        set_env = AWSFinalizeStagedData.SETUP_CLASS
        info = set_env.get_storage_info()
        set_env.logger().info(f"storage info object: {info}")
        df_cache = CachedDFGenerator(500000, [5])
        cache = {
            "storage_info" : info,
            "df_cache" : df_cache
        }
        return cache
    
    def setup(self, cache, num_chunks: int):
        self.df_cache: CachedDFGenerator = cache["df_cache"]
        self.set_env = GeneralUseCaseNoSetup.from_storage_info(cache["storage_info"])

        self.pid = os.getpid()
        self.lib = self.set_env.get_modifiable_library(self.pid)

        INITIAL_TIMESTAMP: TimestampNumber = TimestampNumber(
            0, self.df_cache.TIME_UNIT
        )  # Synchronize index frequency

        df = self.df_cache.generate_dataframe_timestamp_indexed(200, 0, self.df_cache.TIME_UNIT)
        list_of_chunks = [10000] * num_chunks
        self.symbol = self.set_env.get_symbol_name_template(self.pid)

        self.lib.write(self.symbol, data=df, prune_previous_versions=True)
        stage_chunks(self.lib, self.symbol, self.df_cache, INITIAL_TIMESTAMP, list_of_chunks)

    def teardown(self, cache: CachedDFGenerator, param: int):
        self.set_env.delete_modifiable_library(self.pid)    

    def time_finalize_staged_data(self, cache: CachedDFGenerator, param: int):
        self.set_env.logger().info(f"Library: {self.lib}")
        self.set_env.logger().info(f"Symbol: {self.symbol}")
        assert self.symbol in self.lib.get_staged_symbols()
        self.lib.finalize_staged_data(self.symbol, mode=StagedDataFinalizeMethod.WRITE)

    def peakmem_finalize_staged_data(self, cache: CachedDFGenerator, param: int):
        self.set_env.logger().info(f"Library: {self.lib}")
        self.set_env.logger().info(f"Symbol: {self.symbol}")
        assert self.symbol in self.lib.get_staged_symbols()
        self.lib.finalize_staged_data(self.symbol, mode=StagedDataFinalizeMethod.WRITE)        