"""
Copyright 2024 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""
# Use this import when upgraded to ASV 0.6.0 or later
# from asv_runner.benchmarks.mark import SkipNotImplemented
import numpy as np
import pandas as pd

from arcticdb import Arctic
from arcticdb import QueryBuilder
from arcticdb.util.test import random_strings_of_length

class Resample:
    number = 5

    LIB_NAME = "resample"
    CONNECTION_STRING = "lmdb://resample?map_size=5GB"
    ROWS_PER_SEGMENT = 100_000

    param_names = [
        "num_rows",
        "downsampling_factor",
        "col_type",
        "aggregation",
    ]
    params = [
        [1_000_000, 10_000_000],
        [10, 100, 100_000],
        ["bool", "int", "float", "datetime", "str"],
        ["sum", "mean", "min", "max", "first", "last", "count"],
    ]

    def setup_cache(self):
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
                index = pd.date_range(pd.Timestamp(idx * self.ROWS_PER_SEGMENT, unit="us"), freq="us", periods=self.ROWS_PER_SEGMENT)
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
        del self.lib
        del self.ac

    def setup(self, num_rows, downsampling_factor, col_type, aggregation):
        self.ac = Arctic(self.CONNECTION_STRING)
        self.lib = self.ac[self.LIB_NAME]
        self.date_range = (pd.Timestamp(0), pd.Timestamp(num_rows, unit="us"))
        self.query_builder = QueryBuilder().resample(f"{downsampling_factor}us").agg({"col": aggregation})

    def time_resample(self, num_rows, downsampling_factor, col_type, aggregation):
        if col_type == "datetime" and aggregation == "sum" or col_type == "str" and aggregation in ["sum", "mean", "min", "max"]:
            pass
            # Use this when upgrading to ASV 0.6.0 or later
            # raise SkipNotImplemented(f"{aggregation} not supported on columns of type {col_type}")
        else:
            self.lib.read(col_type, date_range=self.date_range, query_builder=self.query_builder)

    def peakmem_resample(self, num_rows, downsampling_factor, col_type, aggregation):
        if col_type == "datetime" and aggregation == "sum" or col_type == "str" and aggregation in ["sum", "mean", "min", "max"]:
            pass
            # Use this when upgrading to ASV 0.6.0 or later
            # raise SkipNotImplemented(f"{aggregation} not supported on columns of type {col_type}")
        else:
            self.lib.read(col_type, date_range=self.date_range, query_builder=self.query_builder)


class ResampleWide:
    number = 5

    LIB_NAME = "resample_wide"
    CONNECTION_STRING = "lmdb://resample_wide?map_size=5GB"
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
