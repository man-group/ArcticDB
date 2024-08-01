"""
Copyright 2023 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

from arcticdb import Arctic

from .common import *


class BasicFunctions:
    number = 5
    timeout = 6000
    CONNECTION_STRING = "lmdb://basic_functions?map_size=20GB"
    WIDE_DF_ROWS = WIDE_DF_ROWS
    WIDE_DF_COLS = WIDE_DF_COLS
    DATE_RANGE = DATE_RANGE
    params = PARAMS
    param_names = PARAM_NAMES

    def setup_cache(self):
        self.ac = Arctic(BasicFunctions.CONNECTION_STRING)
        rows_values = BasicFunctions.params

        self.dfs = {
            rows: generate_pseudo_random_dataframe(rows) for rows in rows_values
        }
        for rows in rows_values:
            lib = get_prewritten_lib_name(rows)
            self.ac.delete_library(lib)
            self.ac.create_library(lib)
            lib = self.ac[lib]
            lib.write(f"sym", self.dfs[rows])

        lib_name = get_prewritten_lib_name(BasicFunctions.WIDE_DF_ROWS)
        self.ac.delete_library(lib_name)
        lib = self.ac.create_library(lib_name)
        lib.write(
            "short_wide_sym",
            generate_random_floats_dataframe(
                BasicFunctions.WIDE_DF_ROWS, BasicFunctions.WIDE_DF_COLS
            ),
        )

    def teardown(self, rows):
        for lib in self.ac.list_libraries():
            if "prewritten" in lib:
                continue
            self.ac.delete_library(lib)

        del self.ac

    def setup(self, rows):
        self.ac = Arctic(BasicFunctions.CONNECTION_STRING)

        self.df = generate_pseudo_random_dataframe(rows)
        self.df_short_wide = generate_random_floats_dataframe(
            BasicFunctions.WIDE_DF_ROWS, BasicFunctions.WIDE_DF_COLS
        )

        self.lib = self.ac[get_prewritten_lib_name(rows)]
        self.fresh_lib = self.get_fresh_lib()

    def get_fresh_lib(self):
        self.ac.delete_library("fresh_lib")
        return self.ac.create_library("fresh_lib")

    def time_write(self, rows):
        self.fresh_lib.write(f"sym", self.df)

    def peakmem_write(self, rows):
        self.fresh_lib.write(f"sym", self.df)

    def time_write_short_wide(self, rows):
        self.fresh_lib.write("short_wide_sym", self.df_short_wide)

    def peakmem_write_short_wide(self, rows):
        self.fresh_lib.write("short_wide_sym", self.df_short_wide)

    def time_read(self, rows):
        self.lib.read(f"sym").data

    def peakmem_read(self, rows):
        self.lib.read(f"sym").data

    def time_read_short_wide(self, rows):
        lib = self.ac[get_prewritten_lib_name(BasicFunctions.WIDE_DF_ROWS)]
        lib.read("short_wide_sym").data

    def peakmem_read_short_wide(self, rows):
        lib = self.ac[get_prewritten_lib_name(BasicFunctions.WIDE_DF_ROWS)]
        lib.read("short_wide_sym").data

    def time_read_with_columns(self, rows):
        COLS = ["value"]
        self.lib.read(f"sym", columns=COLS).data

    def peakmem_read_with_columns(self, rows):
        COLS = ["value"]
        self.lib.read(f"sym", columns=COLS).data

    def time_read_with_date_ranges(self, rows):
        self.lib.read(f"sym", date_range=BasicFunctions.DATE_RANGE).data

    def peakmem_read_with_date_ranges(self, rows):
        self.lib.read(f"sym", date_range=BasicFunctions.DATE_RANGE).data

    def time_write_staged(self, rows):
        self.fresh_lib.write(f"sym", self.df, staged=True)
        self.fresh_lib._nvs.compact_incomplete(f"sym", False, False)

    def peakmem_write_staged(self, rows):
        self.fresh_lib.write(f"sym", self.df, staged=True)
        self.fresh_lib._nvs.compact_incomplete(f"sym", False, False)
