"""
Copyright 2023 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

import random
import tempfile
import time
from arcticdb import Arctic
import pandas as pd

from arcticdb.util.test import random_ascii_string, random_integers, random_dates
from benchmarks.common import *


class ComparisonBenchmarks:
    """
    The test aims to compare memory use of Arctic operations compared to Parquet, which we use as a baseline for comparison.
    """

    rounds = 1
    number = 5
    warmup_time = 0

    LIB_NAME = "compare"
    URL = "lmdb://compare"
    SYMBOL = "dataframe"
    NUMBER_ROWS = 3_000_000

    def __init__(self):
        self.logger = get_logger()

    def setup_cache(self):
        start = time.time()
        df = self._setup_cache()
        self.logger.info(f"SETUP_CACHE TIME: {time.time() - start}")
        return df

    def _setup_cache(self):
        st = time.time()
        dict = self.create_dict(ComparisonBenchmarks.NUMBER_ROWS)
        df = pd.DataFrame(dict)
        print(f"DF generated {time.time() - st}")
        ac = Arctic(ComparisonBenchmarks.URL)
        ac.delete_library(ComparisonBenchmarks.LIB_NAME)
        lib = ac.create_library(ComparisonBenchmarks.LIB_NAME)
        lib.write(symbol=ComparisonBenchmarks.SYMBOL, data=df)
        return df

    def teardown(self, df: pd.DataFrame):
        self.delete_if_exists(self.path)
        self.delete_if_exists(self.path_to_read)

    def setup(self, df):
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
        np.random.seed(1)
        random.seed(1)
        ten_char_strings = [random_ascii_string(10) for _ in range(1000)]
        twenty_char_strings = [random_ascii_string(20) for _ in range(1000)]
        return {
            "element_name object": np.random.choice(twenty_char_strings, size),
            "element_value": np.arange(size, dtype=np.float64),
            "element_unit": np.random.choice(ten_char_strings, size),
            "period_year": random_integers(size, np.int64),
            "region": np.random.choice(ten_char_strings, size),
            "last_published_date": random_dates(size),
            "model_snapshot_id": random_integers(size, np.int64),
            "period": np.random.choice(twenty_char_strings, size),
            "observation_type": np.random.choice(ten_char_strings, size),
            "ric": np.random.choice(ten_char_strings, size),
            "dtype": np.random.choice(ten_char_strings, size),
        }

    def peakmem_create_dataframe(self, df):
        # Just measure how much memory the df takes up
        pass

    def peakmem_write_dataframe_arctic(self, df):
        self.lib.write("symbol", df)

    def peakmem_read_dataframe_arctic(self, df):
        self.lib.read(ComparisonBenchmarks.SYMBOL)

    def peakmem_write_dataframe_parquet(self, df):
        df.to_parquet(self.path, index=True)

    def peakmem_read_dataframe_parquet(self, df):
        pd.read_parquet(self.path_to_read)
