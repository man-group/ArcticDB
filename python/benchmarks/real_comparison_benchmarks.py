"""
Copyright 2025 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

import tempfile
import time
from arcticdb import Arctic
import pandas as pd

from arcticdb.util.environment_setup import GeneralUseCaseNoSetup, Storage
from arcticdb.util.test import random_string, random_integers, random_dates
from benchmarks.common import *

#ASV captures console output thus we create console handler
logger = create_asv_logger()


def str_col(num, size):
    return [random_string(num) for _ in range(size)]

NO_OPERATION = "no-operation-load"
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

    LIB_NAME = "compare"
    URL = "lmdb://compare"
    SYMBOL = "dataframe"
    NUMBER_ROWS = 2_000_000 #100_000

    REAL_STORAGE_SETUP = GeneralUseCaseNoSetup(Storage.AMAZON, "COMPARISON_OPS")

    params = [NO_OPERATION, CREATE_DATAFRAME, PANDAS_PARQUET, ARCTICDB_LMDB, ARCTICDB_AMAZON_S3]
    param_names = ["backend_type"]

    def storage(self):
        return RealComparisonBenchmarks.REAL_STORAGE_SETUP
    
    def setup_cache(self):
        logger.info(f"Setup CACHE start")
        st = time.time()
        dict = self.create_dict(RealComparisonBenchmarks.NUMBER_ROWS)
        df = pd.DataFrame(dict)
        logger.info(f"DF with {RealComparisonBenchmarks.NUMBER_ROWS} rows generated for {time.time() - st}")
        ac = Arctic(RealComparisonBenchmarks.URL)
        ac.delete_library(RealComparisonBenchmarks.LIB_NAME)
        lib = ac.create_library(RealComparisonBenchmarks.LIB_NAME)
        lib.write(symbol=RealComparisonBenchmarks.SYMBOL, data=df)
        self.storage().remove_all_modifiable_libraries(confirm=True)
        return (df, dict)
    
    def teardown(self, tpl, btype):
        self.delete_if_exists(self.parquet_to_write)
        self.delete_if_exists(self.parquet_to_read )
        RealComparisonBenchmarks.REAL_STORAGE_SETUP.delete_modifiable_library(self.pid)
        logger.info(f"Teardown completed")

    def setup(self, tpl, btype):
        df, dict = tpl
        logger.info(f"Setup started")
        # LMDB Setup
        self.ac = Arctic(RealComparisonBenchmarks.URL)
        self.lib = self.ac[RealComparisonBenchmarks.LIB_NAME]
        self.parquet_to_write = f"{tempfile.gettempdir()}/df.parquet"
        self.parquet_to_read = f"{tempfile.gettempdir()}/df_to_read.parquet"
        self.delete_if_exists(self.parquet_to_write)
        df.to_parquet(self.parquet_to_read , index=True)

        # With shared storage we create different libs for each process
        # therefore we initialize the symbol here also not in setup_cache
        self.pid = os.getpid()
        self.s3_lib = self.storage().get_modifiable_library(self.pid)
        self.s3_symbol = f"symbol_{self.pid}"
        self.s3_lib.write(self.s3_symbol, df) 
        logger.info(f"Setup ended")

    def delete_if_exists(self, path):
        if os.path.exists(self.parquet_to_write):
            os.remove(self.parquet_to_write)

    def create_dict(self, size):
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
        if btype == NO_OPERATION:
            # What is the tool mem load?
            return
        if btype == CREATE_DATAFRAME:
            df = pd.DataFrame(dict)
        elif btype == PANDAS_PARQUET:
            pd.read_parquet(self.parquet_to_read )
        elif btype == ARCTICDB_LMDB:
            self.lib.read(self.SYMBOL)
        elif btype == ARCTICDB_AMAZON_S3:
            self.s3_lib.read(self.s3_symbol) 
            pass
        else: 
            raise Exception(f"Unsupported type: {btype}")

    def peakmem_write_dataframe(self, tpl, btype):
        df, dict = tpl
        if btype == NO_OPERATION:
            # What is the tool mem load?
            return
        if btype == CREATE_DATAFRAME:
            df = pd.DataFrame(dict)
        elif btype == PANDAS_PARQUET:
            df.to_parquet(self.parquet_to_write, index=True)
        elif btype == ARCTICDB_LMDB:
            self.lib.write("symbol", df)
        elif btype == ARCTICDB_AMAZON_S3:
            self.s3_lib.write(self.s3_symbol, df)
            pass
        else: 
            raise Exception(f"Unsupported type: {btype}")
        
    def peakmem_create_write_dataframe(self, tpl, btype):
        """
        This scenario includes creation of dataframe and then its serialization to storage
        """
        df, dict = tpl
        if btype == NO_OPERATION:
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
            self.s3_lib.write(self.s3_symbol, df)
            pass
        else: 
            raise Exception(f"Unsupported type: {btype}")

