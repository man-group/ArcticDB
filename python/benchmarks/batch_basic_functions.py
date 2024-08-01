"""
Copyright 2023 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

from arcticdb import Arctic
from arcticdb.version_store.library import WritePayload, ReadRequest

from .common import *


class BatchBasicFunctions:
    number = 5
    timeout = 6000
    CONNECTION_STRING = "lmdb://batch_basic_functions?map_size=20GB"
    DATE_RANGE = DATE_RANGE
    params = BATCH_PARAMS
    param_names = BATCH_PARAM_NAMES

    def setup_cache(self):
        self.ac = Arctic(BatchBasicFunctions.CONNECTION_STRING)
        rows_values, num_symbols_values = BatchBasicFunctions.params

        self.dfs = {
            rows: generate_pseudo_random_dataframe(rows) for rows in rows_values
        }
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
        self.lib = self.ac[get_prewritten_lib_name(rows)]
        self.fresh_lib = self.get_fresh_lib()

    def get_fresh_lib(self):
        self.ac.delete_library("fresh_lib")
        return self.ac.create_library("fresh_lib")

    def time_write_batch(self, rows, num_symbols):
        payloads = [WritePayload(f"{sym}_sym", self.df) for sym in range(num_symbols)]
        self.fresh_lib.write_batch(payloads)

    def peakmem_write_batch(self, rows, num_symbols):
        payloads = [WritePayload(f"{sym}_sym", self.df) for sym in range(num_symbols)]
        self.fresh_lib.write_batch(payloads)

    def time_read_batch(self, rows, num_symbols):
        read_reqs = [ReadRequest(f"{sym}_sym") for sym in range(num_symbols)]
        self.lib.read_batch(read_reqs)

    def time_read_batch_pure(self, rows, num_symbols):
        self.lib.read_batch(self.read_reqs)

    def peakmem_read_batch(self, rows, num_symbols):
        read_reqs = [ReadRequest(f"{sym}_sym") for sym in range(num_symbols)]
        self.lib.read_batch(read_reqs)

    def time_read_batch_with_columns(self, rows, num_symbols):
        COLS = ["value"]
        read_reqs = [
            ReadRequest(f"{sym}_sym", columns=COLS) for sym in range(num_symbols)
        ]
        self.lib.read_batch(read_reqs)

    def peakmem_read_batch_with_columns(self, rows, num_symbols):
        COLS = ["value"]
        read_reqs = [
            ReadRequest(f"{sym}_sym", columns=COLS) for sym in range(num_symbols)
        ]
        self.lib.read_batch(read_reqs)

    def time_read_batch_with_date_ranges(self, rows, num_symbols):
        read_reqs = [
            ReadRequest(f"{sym}_sym", date_range=BatchBasicFunctions.DATE_RANGE)
            for sym in range(num_symbols)
        ]
        self.lib.read_batch(read_reqs)

    def peakmem_read_batch_with_date_ranges(self, rows, num_symbols):
        read_reqs = [
            ReadRequest(f"{sym}_sym", date_range=BatchBasicFunctions.DATE_RANGE)
            for sym in range(num_symbols)
        ]
        self.lib.read_batch(read_reqs)
