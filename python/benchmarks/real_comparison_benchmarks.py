"""
Copyright 2025 Man Group Operations Limited
Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

from logging import Logger
import os
import tempfile
import time
from typing import Any, Dict
from arcticdb import Arctic
import pandas as pd
import numpy as np

from arcticdb.util.environment_setup import LibraryPopulationPolicy, LibraryType, Storage, TestLibraryManager, get_console_logger
from arcticdb.util.test import random_string, random_integers, random_dates


BASE_MEMORY = "no-operation-load"
CREATE_DATAFRAME = "create-df-pandas-from_dict"
PANDAS_PARQUET = "pandas-parquet"
ARCTICDB_LMDB = "arcticdb-lmdb"
ARCTICDB_AMAZON_S3 = "arcticdb-amazon-s3"


class RealComparisonBenchmarks:
    """
    The test aims to compare efficiency of read and write operations
    in terms of memory on one graph
        - creation of dataframe
        - read/write dataframe
        - read/write dataframe to arcticdb LMDB
        - read/write dataframe to arcticdb Amazon S3
    """

    rounds = 1
    ## Note in most of our cases setup() is expensive therefore we play with number only and fix repeat to 1
    number = 2 # invoke X times the test runs between each setup-teardown 
    repeat = 1 # defines the number of times the measurements will invoke setup-teardown
    min_run_count = 1
    warmup_time = 0

    timeout = 60000

    LIB_NAME = "COMPARISON"
    URL = "lmdb://compare"
    SYMBOL = "dataframe"
    # NOTE: If you plan to make changes to parameters, consider that a library with previous definition 
    #       may already exist. This means that symbols there will be having having different number
    #       of rows than what you defined in the test. To resolve this problem check with documentation:
    #           https://github.com/man-group/ArcticDB/wiki/ASV-Benchmarks:-Real-storage-tests
    NUMBER_ROWS = 2_000_000 #100_000

    # BASE_MEMORY measures class memory allocation. This is the actual memory that
    # is used by the tools and code that does the measurement. Thus any other measurement
    # number should be deducted with BASE_MEMORY number to receive actual number.
    # The whole discussion is available at: 
    # https://github.com/man-group/ArcticDB/wiki/ASV-Benchmarks:-Running,-designing-and-implementing#understanding-and-implementing-peakmem-benchmarks
    params = [BASE_MEMORY, CREATE_DATAFRAME, PANDAS_PARQUET, ARCTICDB_LMDB, ARCTICDB_AMAZON_S3]
    param_names = ["backend_type"]

    library_manager = TestLibraryManager(storage=Storage.AMAZON, name_benchmark="COMPARISON")
    
    def get_logger(self) -> Logger:
        return get_console_logger(self)

    def get_library_manager(self) -> TestLibraryManager:
        return RealComparisonBenchmarks.library_manager
    
    def get_population_policy(self) -> LibraryPopulationPolicy:
        lpp = LibraryPopulationPolicy(RealComparisonBenchmarks.params, self.get_logger())
        return lpp

    def setup_cache(self):
        logger = self.get_logger()
        logger.info(f"Setup CACHE start")
        manager = self.get_library_manager()
        symbol = RealComparisonBenchmarks.SYMBOL
        num_rows = RealComparisonBenchmarks.NUMBER_ROWS
        
        st = time.time()
        dict = self.create_dict(num_rows)
        df = pd.DataFrame(dict)
        logger.info(f"DF with {num_rows} rows generated for {time.time() - st}")

        # Prepare local LMDB lib
        ac = Arctic(RealComparisonBenchmarks.URL)
        ac.delete_library(RealComparisonBenchmarks.LIB_NAME)
        lib = ac.create_library(RealComparisonBenchmarks.LIB_NAME)
        lib.write(symbol=symbol, data=df)
        
        # Prepare persistent library if does not exist
        manager.clear_all_benchmark_libs()
        if not manager.has_library(LibraryType.PERSISTENT):
            s3_lib = manager.get_library(LibraryType.PERSISTENT)
            s3_lib.write(symbol, df) 
        return (df, dict)

    def teardown(self, tpl, btype):
        self.delete_if_exists(self.parquet_to_write)
        self.delete_if_exists(self.parquet_to_read )
        self.manager.clear_all_modifiable_libs_from_this_process()
        self.logger.info(f"Teardown completed")

    def setup(self, tpl, btype):
        df : pd.DataFrame
        dict: Dict[str, Any]
        df, dict = tpl
        self.manager = self.get_library_manager()
        self.logger = self.get_logger()
        self.logger.info(f"Setup started")
        # LMDB Setup
        self.ac = Arctic(RealComparisonBenchmarks.URL)
        self.lib = self.ac[RealComparisonBenchmarks.LIB_NAME]
        self.parquet_to_write = f"{tempfile.gettempdir()}/df.parquet"
        self.parquet_to_read = f"{tempfile.gettempdir()}/df_to_read.parquet"
        self.delete_if_exists(self.parquet_to_write)
        df.to_parquet(self.parquet_to_read , index=True)

        # With shared storage we create different libs for each process
        self.s3_lib_write = self.manager.get_library(LibraryType.MODIFIABLE)
        self.s3_lib_read = self.manager.get_library(LibraryType.PERSISTENT)
        self.s3_symbol = RealComparisonBenchmarks.SYMBOL
        self.logger.info(f"Setup ended")

    def delete_if_exists(self, path):
        if os.path.exists(self.parquet_to_write):
            os.remove(self.parquet_to_write)

    def create_dict(self, size):

        def str_col(num, size):
            return [random_string(num) for _ in range(size)]

        return {
            "element_name object" : str_col(20, size),        
            "element_value" : np.arange(size, dtype=np.float64), 
            "element_unit" : str_col(10, size),
            "period_year" :  random_integers(size, np.int64), 
            "region" : str_col(10, size),
            "last_published_date" : random_dates(size),
            "model_snapshot_id" :  random_integers(size, np.int64), 
            "period" : str_col(20, size), 
            "observation_type" : str_col(10, size),
            "ric" : str_col(10, size),
            "dtype" : str_col(10, size),
        }       

    def peakmem_read_dataframe(self, tpl, btype):
        df, dict = tpl
        if btype == BASE_MEMORY:
            # measures base memory which need to be deducted from 
            # any measurements with actual operations
            # see discussion above 
            return
        if btype == CREATE_DATAFRAME:
            df = pd.DataFrame(dict)
        elif btype == PANDAS_PARQUET:
            pd.read_parquet(self.parquet_to_read )
        elif btype == ARCTICDB_LMDB:
            self.lib.read(self.SYMBOL)
        elif btype == ARCTICDB_AMAZON_S3:
            self.s3_lib_read.read(self.s3_symbol) 
        else: 
            raise Exception(f"Unsupported type: {btype}")

    def peakmem_write_dataframe(self, tpl, btype):
        df, dict = tpl
        if btype == BASE_MEMORY:
            # What is the tool mem load?
            return
        if btype == CREATE_DATAFRAME:
            df = pd.DataFrame(dict)
        elif btype == PANDAS_PARQUET:
            df.to_parquet(self.parquet_to_write, index=True)
        elif btype == ARCTICDB_LMDB:
            self.lib.write("symbol", df)
        elif btype == ARCTICDB_AMAZON_S3:
            self.s3_lib_write.write(self.s3_symbol, df)
        else: 
            raise Exception(f"Unsupported type: {btype}")
        
    def create_then_write_dataframe(self, tpl, btype):
        """
        This scenario includes creation of dataframe and then its serialization to storage
        """
        df, dict = tpl
        if btype == BASE_MEMORY:
            # What is the tool mem load?
            return
        df = pd.DataFrame(dict) # always create dataframe in this scenario
        if btype == CREATE_DATAFRAME:
            pass
        elif btype == PANDAS_PARQUET:
            df.to_parquet(self.parquet_to_write, index=True)
        elif btype == ARCTICDB_LMDB:
            self.lib.write("symbol", df)
        elif btype == ARCTICDB_AMAZON_S3:
            self.s3_lib_write.write(self.s3_symbol, df)
            pass
        else: 
            raise Exception(f"Unsupported type: {btype}")        

    def peakmem_create_then_write_dataframe(self, tpl, btype):
        self.create_then_write_dataframe(tpl, btype)
        
    def time_create_then_write_dataframe(self, tpl, btype):      
        self.create_then_write_dataframe(tpl, btype)
