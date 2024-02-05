"""
Copyright 2023 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""
from arcticdb import Arctic
from arcticdb.version_store.library import WritePayload, ReadRequest
import pandas as pd

from .common import *


class BasicFunctions:
    number = 5
    timeout = 6000

    params = ([100_000, 150_000], [500, 1000])
    # params = ([1000, 1500], [500, 1000])
    param_names = ["rows", "num_symbols"]

    def setup_cache(self):
        self.ac = Arctic("lmdb://basic_functions?map_size=10GB")
        num_rows, num_symbols = BasicFunctions.params

        self.dfs = {rows: generate_pseudo_random_dataframe(rows) for rows in num_rows}
        for rows in num_rows:
            lib = get_prewritten_lib_name(rows)
            self.ac.delete_library(lib)
            self.ac.create_library(lib)
            lib = self.ac[lib]
            for sym in range(num_symbols[-1]):
                lib.write(f"{sym}_sym", self.dfs[rows])
        lib.write("short_wide_sym", generate_random_floats_dataframe(5_000, 30_000))

    def teardown(self, rows, num_symbols):
        for lib in self.ac.list_libraries():
            if "prewritten" in lib:
                continue
            self.ac.delete_library(lib)

    def setup(self, rows, num_symbols):
        self.ac = Arctic("lmdb://basic_functions?map_size=10GB")
        self.read_reqs = [ReadRequest(f"{sym}_sym") for sym in range(num_symbols)]

        self.df = generate_pseudo_random_dataframe(rows)
        self.df_short_wide = generate_random_floats_dataframe(5_000, 30_000)

    def get_fresh_lib(self):
        self.ac.delete_library("fresh_lib")
        self.ac.create_library("fresh_lib")
        return self.ac["fresh_lib"]

    def time_write(self, rows, num_symbols):
        lib = self.get_fresh_lib()
        for sym in range(num_symbols):
            lib.write(f"{sym}_sym", self.df)

    def peakmem_write(self, rows, num_symbols):
        lib = self.get_fresh_lib()
        for sym in range(num_symbols):
            lib.write(f"{sym}_sym", self.df)

    def time_write_short_wide(self, rows, num_symbols):
        lib = self.get_fresh_lib()
        lib.write("short_wide_sym", self.df_short_wide)

    def peakmem_write_short_wide(self, rows, num_symbols):
        lib = self.get_fresh_lib()
        lib.write("short_wide_sym", self.df_short_wide)

    def time_write_staged(self, rows, num_symbols):
        lib = self.get_fresh_lib()
        for sym in range(num_symbols):
            lib.write(f"{sym}_sym", self.df, staged=True)

        for sym in range(num_symbols):
            lib._nvs.compact_incomplete(f"{sym}_sym", False, False)

    def peakmem_write_staged(self, rows, num_symbols):
        lib = self.get_fresh_lib()
        for sym in range(num_symbols):
            lib.write(f"{sym}_sym", self.df, staged=True)

        for sym in range(num_symbols):
            lib._nvs.compact_incomplete(f"{sym}_sym", False, False)

    def time_write_batch(self, rows, num_symbols):
        lib = self.get_fresh_lib()
        df = self.df
        payloads = [WritePayload(f"{sym}_sym", df) for sym in range(num_symbols)]
        lib.write_batch(payloads)

    def peakmem_write_batch(self, rows, num_symbols):
        lib = self.get_fresh_lib()
        df = self.df
        payloads = [WritePayload(f"{sym}_sym", df) for sym in range(num_symbols)]
        lib.write_batch(payloads)

    def time_read(self, rows, num_symbols):
        lib = self.ac[get_prewritten_lib_name(rows)]
        [lib.read(f"{sym}_sym").data for sym in range(num_symbols)]

    def peakmem_read(self, rows, num_symbols):
        lib = self.ac[get_prewritten_lib_name(rows)]
        [lib.read(f"{sym}_sym").data for sym in range(num_symbols)]

    def time_read_short_wide(self, rows, num_symbols):
        lib = self.ac[get_prewritten_lib_name(rows)]
        lib.read("short_wide_sym").data

    def peakmem_read_short_wide(self, rows, num_symbols):
        lib = self.ac[get_prewritten_lib_name(rows)]
        lib.read("short_wide_sym").data

    def time_read_batch(self, rows, num_symbols):
        lib = self.ac[get_prewritten_lib_name(rows)]
        read_reqs = [ReadRequest(f"{sym}_sym") for sym in range(num_symbols)]
        lib.read_batch(read_reqs)

    def time_read_batch_pure(self, rows, num_symbols):
        lib = self.ac[get_prewritten_lib_name(rows)]
        lib.read_batch(self.read_reqs)

    def peakmem_read_batch(self, rows, num_symbols):
        lib = self.ac[get_prewritten_lib_name(rows)]
        read_reqs = [ReadRequest(f"{sym}_sym") for sym in range(num_symbols)]
        lib.read_batch(read_reqs)

    def time_read_with_columns(self, rows, num_symbols):
        lib = self.ac[get_prewritten_lib_name(rows)]
        COLS = ["value"]
        [lib.read(f"{sym}_sym", columns=COLS).data for sym in range(num_symbols)]

    def peakmem_read_with_columns(self, rows, num_symbols):
        lib = self.ac[get_prewritten_lib_name(rows)]
        COLS = ["value"]
        [lib.read(f"{sym}_sym", columns=COLS).data for sym in range(num_symbols)]

    def time_read_batch_with_columns(self, rows, num_symbols):
        lib = self.ac[get_prewritten_lib_name(rows)]
        COLS = ["value"]
        read_reqs = [ReadRequest(f"{sym}_sym", columns=COLS) for sym in range(num_symbols)]
        lib.read_batch(read_reqs)

    def peakmem_read_batch_with_columns(self, rows, num_symbols):
        lib = self.ac[get_prewritten_lib_name(rows)]
        COLS = ["value"]
        read_reqs = [ReadRequest(f"{sym}_sym", columns=COLS) for sym in range(num_symbols)]
        lib.read_batch(read_reqs)

    def time_read_with_date_ranges(self, rows, num_symbols):
        lib = self.ac[get_prewritten_lib_name(rows)]
        dr = pd.date_range("2023-01-01", "2023-01-01")
        [lib.read(f"{sym}_sym", date_range=dr).data for sym in range(num_symbols)]

    def peakmem_read_with_date_ranges(self, rows, num_symbols):
        lib = self.ac[get_prewritten_lib_name(rows)]
        dr = pd.date_range("2023-01-01", "2023-01-01")
        [lib.read(f"{sym}_sym", date_range=dr).data for sym in range(num_symbols)]

    def time_read_batch_with_date_ranges(self, rows, num_symbols):
        lib = self.ac[get_prewritten_lib_name(rows)]
        dr = pd.date_range("2023-01-01", "2023-01-01")
        read_reqs = [ReadRequest(f"{sym}_sym", date_range=dr) for sym in range(num_symbols)]
        lib.read_batch(read_reqs)

    def peakmem_read_batch_with_date_ranges(self, rows, num_symbols):
        lib = self.ac[get_prewritten_lib_name(rows)]
        dr = pd.date_range("2023-01-01", "2023-01-01")
        read_reqs = [ReadRequest(f"{sym}_sym", date_range=dr) for sym in range(num_symbols)]
        lib.read_batch(read_reqs)
