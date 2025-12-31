"""
Copyright 2023 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

import time

from arcticdb import Arctic, QueryBuilder
from arcticdb.version_store.library import UpdatePayload, WritePayload, ReadRequest
from asv_runner.benchmarks.mark import skip_benchmark

import pandas as pd

from benchmarks.common import *

# We use larger dataframes for non-batch methods
PARAMS = [1_000_000, 10_000_000]
PARAM_NAMES = ["rows"]
BATCH_PARAMS = ([25_000, 50_000], [100, 200])
BATCH_PARAM_NAMES = ["rows", "num_symbols"]
DATE_RANGE = (pd.Timestamp("2022-12-31"), pd.Timestamp("2023-01-01"))


class BasicFunctions:
    sample_time = 0.1
    rounds = 1

    params = PARAMS
    param_names = PARAM_NAMES

    CONNECTION_STRING = "lmdb://basic_functions"

    def __init__(self):
        self.logger = get_logger()

    def setup_cache(self):
        start = time.time()
        self._setup_cache()
        self.logger.info(f"SETUP_CACHE TIME: {time.time() - start}")

    def _setup_cache(self):
        self.ac = Arctic(BasicFunctions.CONNECTION_STRING)
        rows_values = BasicFunctions.params

        self.dfs = {rows: generate_pseudo_random_dataframe(rows) for rows in rows_values}
        for rows in rows_values:
            lib = get_prewritten_lib_name(rows)
            self.ac.delete_library(lib)
            self.ac.create_library(lib)
            lib = self.ac[lib]
            lib.write(f"sym", self.dfs[rows])

    def teardown(self, rows):
        for lib in self.ac.list_libraries():
            if "prewritten" in lib:
                continue
            self.ac.delete_library(lib)

        del self.ac

    def setup(self, rows):
        self.ac = Arctic(BasicFunctions.CONNECTION_STRING)

        self.df = generate_pseudo_random_dataframe(rows)

        self.lib = self.ac[get_prewritten_lib_name(rows)]
        self.fresh_lib = self.get_fresh_lib()

    def get_fresh_lib(self):
        self.ac.delete_library("fresh_lib")
        return self.ac.create_library("fresh_lib")

    def time_write(self, rows):
        self.fresh_lib.write(f"sym", self.df)

    def peakmem_write(self, rows):
        self.fresh_lib.write(f"sym", self.df)

    def time_read(self, rows):
        self.lib.read(f"sym").data

    def peakmem_read(self, rows):
        self.lib.read(f"sym").data

    def time_read_with_columns(self, rows):
        COLS = ["value"]
        self.lib.read(f"sym", columns=COLS).data

    def peakmem_read_with_columns(self, rows):
        COLS = ["value"]
        self.lib.read(f"sym", columns=COLS).data

    def time_read_with_date_ranges(self, rows):
        self.lib.read(f"sym", date_range=DATE_RANGE).data

    def peakmem_read_with_date_ranges(self, rows):
        self.lib.read(f"sym", date_range=DATE_RANGE).data

    def time_read_with_date_ranges_query_builder(self, rows):
        q = QueryBuilder().date_range(DATE_RANGE)
        self.lib.read(f"sym", query_builder=q).data

    def peakmem_read_with_date_ranges_query_builder(self, rows):
        q = QueryBuilder().date_range(DATE_RANGE)
        self.lib.read(f"sym", query_builder=q).data

    def time_write_staged(self, rows):
        self.fresh_lib.write(f"sym", self.df, staged=True)
        self.fresh_lib._nvs.compact_incomplete(f"sym", False, False)

    def peakmem_write_staged(self, rows):
        self.fresh_lib.write(f"sym", self.df, staged=True)
        self.fresh_lib._nvs.compact_incomplete(f"sym", False, False)


class ShortWideWrite:
    CONNECTION_STRING = "lmdb://short_wide_write"
    COLS = 30_000

    params = [1, 5_000]
    param_names = ["rows"]

    def __init__(self):
        self.logger = get_logger()

    def setup(self, rows):
        self.ac = Arctic(ShortWideWrite.CONNECTION_STRING)
        self.lib = self.ac.create_library("lib")
        self.df = generate_random_floats_dataframe(rows, ShortWideWrite.COLS)

    def teardown(self, *args):
        self.ac.delete_library("lib")

    def time_write(self, *args):
        self.lib.write("sym", self.df)

    def peakmem_write(self, *args):
        self.lib.write("sym", self.df)


class ShortWideRead:
    CONNECTION_STRING = "lmdb://short_wide_read"
    ROWS = [1, 5_000]
    COLS = 30_000

    params = ROWS
    param_names = ["rows"]

    def __init__(self):
        self.logger = get_logger()

    def setup_cache(self):
        ac = Arctic(ShortWideRead.CONNECTION_STRING)
        lib = ac.create_library("lib")
        for r in ShortWideRead.ROWS:
            df = generate_random_floats_dataframe(r, ShortWideRead.COLS)
            lib.write(f"sym_{r}", df)

    def setup(self, rows):
        ac = Arctic(ShortWideRead.CONNECTION_STRING)
        self.lib = ac["lib"]

    def time_read(self, rows):
        self.lib.read(f"sym_{rows}")

    def peakmem_read(self, rows):
        self.lib.read(f"sym_{rows}")


class BatchBasicFunctions:
    rounds = 1
    sample_time = 0.1

    CONNECTION_STRING = "lmdb://batch_basic_functions"
    DATE_RANGE = DATE_RANGE
    params = BATCH_PARAMS
    param_names = BATCH_PARAM_NAMES

    def __init__(self):
        self.logger = get_logger()

    def setup_cache(self):
        start = time.time()
        self._setup_cache()
        self.logger.info(f"SETUP_CACHE TIME: {time.time() - start}")

    def _setup_cache(self):
        self.ac = Arctic(BatchBasicFunctions.CONNECTION_STRING)
        rows_values, num_symbols_values = BatchBasicFunctions.params

        self.dfs = {rows: generate_pseudo_random_dataframe(rows) for rows in rows_values}
        for rows in rows_values:
            lib = get_prewritten_lib_name(rows)
            self.ac.delete_library(lib)
            self.ac.create_library(lib)
            lib = self.ac[lib]
            for sym in range(num_symbols_values[-1]):
                lib.write(f"{sym}_sym", self.dfs[rows])

    def teardown(self, rows, num_symbols):
        for lib in self.ac.list_libraries():
            if "prewritten" in lib:
                continue
            self.ac.delete_library(lib)

        del self.ac

    def setup(self, rows, num_symbols):
        self.ac = Arctic(BatchBasicFunctions.CONNECTION_STRING)
        self.read_reqs = [ReadRequest(f"{sym}_sym") for sym in range(num_symbols)]

        self.df = generate_pseudo_random_dataframe(rows)
        self.update_df = generate_pseudo_random_dataframe(rows // 2)
        self.lib = self.ac[get_prewritten_lib_name(rows)]
        self.fresh_lib = self.get_fresh_lib()

    def get_fresh_lib(self):
        self.ac.delete_library("fresh_lib")
        lib = self.ac.create_library("fresh_lib")
        return lib

    def time_write_batch(self, rows, num_symbols):
        payloads = [WritePayload(f"{sym}_sym", self.df) for sym in range(num_symbols)]
        self.fresh_lib.write_batch(payloads)

    def peakmem_write_batch(self, rows, num_symbols):
        payloads = [WritePayload(f"{sym}_sym", self.df) for sym in range(num_symbols)]
        self.fresh_lib.write_batch(payloads)

    def time_update_batch(self, rows, num_symbols):
        payloads = [UpdatePayload(f"{sym}_sym", self.update_df) for sym in range(num_symbols)]
        results = self.lib.update_batch(payloads)
        assert results[0].version >= 1
        assert results[-1].version >= 1

    @skip_benchmark
    def peakmem_update_batch(self, rows, num_symbols):
        payloads = [UpdatePayload(f"{sym}_sym", self.update_df) for sym in range(num_symbols)]
        results = self.lib.update_batch(payloads)
        assert results[0].version >= 1

    def time_read_batch(self, rows, num_symbols):
        read_reqs = [ReadRequest(f"{sym}_sym") for sym in range(num_symbols)]
        self.lib.read_batch(read_reqs)

    def peakmem_read_batch(self, rows, num_symbols):
        read_reqs = [ReadRequest(f"{sym}_sym") for sym in range(num_symbols)]
        self.lib.read_batch(read_reqs)

    def time_read_batch_with_columns(self, rows, num_symbols):
        COLS = ["value"]
        read_reqs = [ReadRequest(f"{sym}_sym", columns=COLS) for sym in range(num_symbols)]
        self.lib.read_batch(read_reqs)

    def peakmem_read_batch_with_columns(self, rows, num_symbols):
        COLS = ["value"]
        read_reqs = [ReadRequest(f"{sym}_sym", columns=COLS) for sym in range(num_symbols)]
        self.lib.read_batch(read_reqs)

    def time_read_batch_with_date_ranges(self, rows, num_symbols):
        read_reqs = [ReadRequest(f"{sym}_sym", date_range=BatchBasicFunctions.DATE_RANGE) for sym in range(num_symbols)]
        self.lib.read_batch(read_reqs)

    def peakmem_read_batch_with_date_ranges(self, rows, num_symbols):
        read_reqs = [ReadRequest(f"{sym}_sym", date_range=BatchBasicFunctions.DATE_RANGE) for sym in range(num_symbols)]
        self.lib.read_batch(read_reqs)
