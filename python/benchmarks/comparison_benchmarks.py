"""
Copyright 2023 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

import tempfile
import time
from arcticdb import Arctic
import pandas as pd

from arcticdb.util.test import random_string, random_integers, random_dates
from benchmarks.common import *


def str_col(num, size):
    return [random_string(num) for _ in range(size)]

class ComparisonBenchmarks:
    """
    The test aims to compare efficiency of read and write operations
    in terms of memory on one graph
        - creation of dataframe
        - read/write dataframe
        - read/write dataframe to arcticdb
    """
    number = 5
    timeout = 60000

    LIB_NAME = "compare"
    URL = "lmdb://compare"
    SYMBOL = "dataframe"
    NUMBER_ROWS = 3000000

    def setup_cache(self):
        st = time.time()
        dict = self.create_dict(ComparisonBenchmarks.NUMBER_ROWS)
        df = pd.DataFrame(dict)
        print(f"DF generated {time.time() - st}")
        ac = Arctic(ComparisonBenchmarks.URL)
        ac.delete_library(ComparisonBenchmarks.LIB_NAME)
        lib = ac.create_library(ComparisonBenchmarks.LIB_NAME)
        lib.write(symbol=ComparisonBenchmarks.SYMBOL, data=df)
        return (df, dict)

    def teardown(self, df: pd.DataFrame):
        self.delete_if_exists(self.path)
        self.delete_if_exists(self.path_to_read)

    def setup(self, tpl):
        df, dict = tpl
        self.ac = Arctic(ComparisonBenchmarks.URL)
        self.lib = self.ac[ComparisonBenchmarks.LIB_NAME]
        self.path = f"{tempfile.gettempdir()}/df.parquet"
        self.path_to_read = f"{tempfile.gettempdir()}/df_to_read.parquet"
        self.delete_if_exists(self.path)
        df.to_parquet(self.path_to_read, index=True)

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
    
    def peakmem_create_dataframe(self, tpl):
        df, dict = tpl
        df = pd.DataFrame(dict)

    def peakmem_write_dataframe_arctic(self, tpl):
        df, dict = tpl
        self.lib.write("symbol", df)

    def peakmem_read_dataframe_arctic(self, tpl):
        self.lib.read(ComparisonBenchmarks.SYMBOL)

    def peakmem_write_dataframe_parquet(self, tpl):
        df, dict = tpl
        df.to_parquet(self.path, index=True)

    def peakmem_read_dataframe_parquet(self, tpl):
        pd.read_parquet(self.path_to_read)

