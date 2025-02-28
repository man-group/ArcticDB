"""
Copyright 2025 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

import gc
import tempfile
import time
from arcticdb import Arctic
import pandas as pd

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

class ComparisonBenchmarks:
    """
    The test aims to compare efficiency of read and write operations
    in terms of memory on one graph
        - creation of dataframe
        - read/write dataframe
        - read/write dataframe to arcticdb LMDB 
    """

    rounds = 1
    ## Note in most of our cases setup() is expensive therefore we play with number only and fix repeat to 1
    number = 3 # invoke X times the test runs between each setup-teardown 
    repeat = 1 # defines the number of times the measurements will invoke setup-teardown
    min_run_count = 1
    warmup_time = 0

    timeout = 60000

    LIB_NAME = "compare"
    URL = "lmdb://compare"
    SYMBOL = "dataframe"
    NUMBER_ROWS = 3_000_00

    params = [NO_OPERATION, CREATE_DATAFRAME, PANDAS_PARQUET, ARCTICDB_LMDB]
    param_names = ["backend_type"]

    def setup_cache(self):
        logger.info(f"Setup CACHE start")
        st = time.time()
        dict = self.create_dict(ComparisonBenchmarks.NUMBER_ROWS)
        df = pd.DataFrame(dict)
        logger.info(f"DF with {ComparisonBenchmarks.NUMBER_ROWS} rows generated for {time.time() - st}")
        ac = Arctic(ComparisonBenchmarks.URL)
        ac.delete_library(ComparisonBenchmarks.LIB_NAME)
        lib = ac.create_library(ComparisonBenchmarks.LIB_NAME)
        lib.write(symbol=ComparisonBenchmarks.SYMBOL, data=df)
        return (df, dict)

    def teardown(self, tpl, btype):
        self.delete_if_exists(self.path)
        self.delete_if_exists(self.path_to_read)
        logger.info(f"Teardown completed")

    def setup(self, tpl, btype):
        logger.info(f"Setup started")
        df, dict = tpl
        self.ac = Arctic(ComparisonBenchmarks.URL)
        self.lib = self.ac[ComparisonBenchmarks.LIB_NAME]
        self.path = f"{tempfile.gettempdir()}/df.parquet"
        self.path_to_read = f"{tempfile.gettempdir()}/df_to_read.parquet"
        self.delete_if_exists(self.path)
        df.to_parquet(self.path_to_read, index=True)
        del df, dict, tpl
        gc.collect()
        logger.info(f"Setup ended")

    def delete_if_exists(self, path):
        if os.path.exists(self.path):
            os.remove(self.path)

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
            pd.read_parquet(self.path_to_read)
        elif btype == ARCTICDB_LMDB:
            self.lib.read(self.SYMBOL)
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
            df.to_parquet(self.path, index=True)
        elif btype == ARCTICDB_LMDB:
            self.lib.write("symbol", df)
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
            df.to_parquet(self.path, index=True)
        elif btype == ARCTICDB_LMDB:
            self.lib.write("symbol", df)
        else: 
            raise Exception(f"Unsupported type: {btype}")

