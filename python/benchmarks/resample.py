"""
Copyright 2026 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

# Use this import when upgraded to ASV 0.6.0 or later
# from asv_runner.benchmarks.mark import SkipNotImplemented
import time
import numpy as np
import pandas as pd
import itertools
import random

from arcticdb import Arctic
from arcticdb import QueryBuilder
from arcticdb.util.logger import get_logger
from arcticdb.util.test import random_strings_of_length
from asv_runner.benchmarks.mark import skip_for_params


class Resample:
    number = 5

    LIB_NAME = "resample"
    CONNECTION_STRING = "lmdb://resample"
    ROWS_PER_SEGMENT = 100_000

    param_names = [
        "num_rows",
        "downsampling_factor",
        "col_type",
        "aggregation",
    ]
    params = [
        [3_000_000, 10_000_000],  # num_rows
        [10, 100, 100_000],  # downsampling factor
        ["bool", "int", "float", "datetime", "str"],  # col_type
        ["sum", "mean", "min", "max", "first", "last", "count"],  # aggregation
    ]

    # Peakmem params are tuned for the machine that runs the tests. It has 16 CPU threads and 24 IO threads. Having too
    # much segments on leads to variability of ~20% in the processing pipeline because of the scheduling. 3_000_000 rows
    # are 30 segments, a bit more than the number of IO threads but still not enough to cause large variability
    PEAKMEM_PARAMS = list(filter(lambda x: x[0] == 3_000_000, itertools.product(*params)))
    TIME_PARAMS = list(filter(lambda x: x[0] != 3_000_000, itertools.product(*params)))

    def __init__(self):
        self.logger = get_logger()

    def setup_cache(self):
        start = time.time()
        self._setup_cache()
        self.logger.info(f"SETUP_CACHE TIME: {time.time() - start}")

    def _setup_cache(self):
        random.seed(42)
        np.random.seed(42)
        ac = Arctic(self.CONNECTION_STRING)
        ac.delete_library(self.LIB_NAME)
        lib = ac.create_library(self.LIB_NAME)
        rng = np.random.default_rng()
        col_types = self.params[2]
        rows = max(self.params[0])
        for col_type in col_types:
            if col_type == "str":
                num_unique_strings = 100
                unique_strings = random_strings_of_length(num_unique_strings, 10, True)
            sym = col_type
            num_segments = rows // self.ROWS_PER_SEGMENT
            for idx in range(num_segments):
                index = pd.date_range(
                    pd.Timestamp(idx * self.ROWS_PER_SEGMENT, unit="us"), freq="us", periods=self.ROWS_PER_SEGMENT
                )
                if col_type == "int":
                    col_data = rng.integers(0, 100_000, self.ROWS_PER_SEGMENT)
                elif col_type == "bool":
                    col_data = rng.integers(0, 2, self.ROWS_PER_SEGMENT)
                    col_data = col_data.astype(bool)
                elif col_type == "float":
                    col_data = 100_000 * rng.random(self.ROWS_PER_SEGMENT)
                elif col_type == "datetime":
                    col_data = rng.integers(0, 100_000, self.ROWS_PER_SEGMENT)
                    col_data = col_data.astype("datetime64[s]")
                elif col_type == "str":
                    col_data = np.random.choice(unique_strings, self.ROWS_PER_SEGMENT)
                df = pd.DataFrame({"col": col_data}, index=index)
                lib.append(sym, df)

    def teardown(self, num_rows, downsampling_factor, col_type, aggregation):
        if not self.skipped:
            del self.lib
            del self.ac

    def setup(self, num_rows, downsampling_factor, col_type, aggregation):
        if (
            col_type == "datetime"
            and aggregation == "sum"
            or col_type == "str"
            and aggregation in ["sum", "mean", "min", "max"]
        ):
            self.skipped = True
            raise NotImplementedError(f"{aggregation} not supported on columns of type {col_type}")

        self.skipped = False
        self.ac = Arctic(self.CONNECTION_STRING)
        self.lib = self.ac[self.LIB_NAME]
        self.date_range = (pd.Timestamp(0), pd.Timestamp(num_rows, unit="us"))
        self.query_builder = QueryBuilder().resample(f"{downsampling_factor}us").agg({"col": aggregation})

    @skip_for_params(PEAKMEM_PARAMS)
    def time_resample(self, num_rows, downsampling_factor, col_type, aggregation):
        self.lib.read(col_type, date_range=self.date_range, query_builder=self.query_builder)

    @skip_for_params(TIME_PARAMS)
    def peakmem_resample(self, num_rows, downsampling_factor, col_type, aggregation):
        self.lib.read(col_type, date_range=self.date_range, query_builder=self.query_builder)


class ResampleWide:
    LIB_NAME = "resample_wide"
    CONNECTION_STRING = "lmdb://resample_wide"
    SYM = "resample_wide"
    NUM_COLS = 30_000
    COLS = [f"col_{idx}" for idx in range(NUM_COLS)]

    def setup_cache(self):
        ac = Arctic(self.CONNECTION_STRING)
        ac.delete_library(self.LIB_NAME)
        lib = ac.create_library(self.LIB_NAME)
        rng = np.random.default_rng()
        num_rows = 3000
        index = pd.date_range(pd.Timestamp(0, unit="us"), freq="us", periods=num_rows)
        data = dict()
        for col in self.COLS:
            data[col] = 100 * rng.random(num_rows, dtype=np.float64)
        df = pd.DataFrame(data, index=index)
        lib.write(self.SYM, df)

    def teardown(self):
        del self.lib
        del self.ac

    def setup(self):
        self.ac = Arctic(self.CONNECTION_STRING)
        self.lib = self.ac[self.LIB_NAME]
        aggs = dict()
        for col in self.COLS:
            aggs[col] = "last"
        self.query_builder = QueryBuilder().resample("30us").agg(aggs)

    def time_resample_wide(self):
        self.lib.read(self.SYM, query_builder=self.query_builder)

    def peakmem_resample_wide(self):
        self.lib.read(self.SYM, query_builder=self.query_builder)
