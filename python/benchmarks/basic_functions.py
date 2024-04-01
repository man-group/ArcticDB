"""
Copyright 2023 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""
from arcticdb import Arctic
from arcticdb.version_store.library import WritePayload, ReadRequest
import pandas as pd

from .common import *


# Common parameters between BasicFunctions and ModificationFunctions
WIDE_DF_ROWS = 5_000
WIDE_DF_COLS = 30_000
# PARAMS = ([1_000, 1_500], [50, 100])
PARAMS = ([100_000, 150_000], [500, 1000])
PARAM_NAMES = ["rows", "num_symbols"]


class BasicFunctions:
    number = 5
    timeout = 6000
    CONNECTION_STRING = "lmdb://basic_functions?map_size=20GB"
    WIDE_DF_ROWS = WIDE_DF_ROWS
    WIDE_DF_COLS = WIDE_DF_COLS
    DATE_RANGE = pd.date_range("2023-01-01", "2023-01-01")

    params = PARAMS
    param_names = PARAM_NAMES

    def setup_cache(self):
        self.ac = Arctic(BasicFunctions.CONNECTION_STRING)
        num_rows, num_symbols = BasicFunctions.params

        self.dfs = {rows: generate_pseudo_random_dataframe(rows) for rows in num_rows}
        for rows in num_rows:
            lib = get_prewritten_lib_name(rows)
            self.ac.delete_library(lib)
            self.ac.create_library(lib)
            lib = self.ac[lib]
            for sym in range(num_symbols[-1]):
                lib.write(f"{sym}_sym", self.dfs[rows])

        lib_name = get_prewritten_lib_name(BasicFunctions.WIDE_DF_ROWS)
        self.ac.delete_library(lib_name)
        lib = self.ac.create_library(lib_name)
        lib.write(
            "short_wide_sym",
            generate_random_floats_dataframe(
                BasicFunctions.WIDE_DF_ROWS, BasicFunctions.WIDE_DF_COLS
            ),
        )

    def teardown(self, rows, num_symbols):
        for lib in self.ac.list_libraries():
            if "prewritten" in lib:
                continue
            self.ac.delete_library(lib)

        del self.ac

    def setup(self, rows, num_symbols):
        self.ac = Arctic(BasicFunctions.CONNECTION_STRING)
        self.read_reqs = [ReadRequest(f"{sym}_sym") for sym in range(num_symbols)]

        self.df = generate_pseudo_random_dataframe(rows)
        self.df_short_wide = generate_random_floats_dataframe(
            BasicFunctions.WIDE_DF_ROWS, BasicFunctions.WIDE_DF_COLS
        )

        self.lib = self.ac[get_prewritten_lib_name(rows)]
        self.fresh_lib = self.get_fresh_lib()

    def get_fresh_lib(self):
        self.ac.delete_library("fresh_lib")
        return self.ac.create_library("fresh_lib")

    def time_write(self, rows, num_symbols):
        for sym in range(num_symbols):
            self.fresh_lib.write(f"{sym}_sym", self.df)

    def peakmem_write(self, rows, num_symbols):
        for sym in range(num_symbols):
            self.fresh_lib.write(f"{sym}_sym", self.df)

    def time_write_short_wide(self, rows, num_symbols):
        self.fresh_lib.write("short_wide_sym", self.df_short_wide)

    def peakmem_write_short_wide(self, rows, num_symbols):
        self.fresh_lib.write("short_wide_sym", self.df_short_wide)

    def time_write_staged(self, rows, num_symbols):
        for sym in range(num_symbols):
            self.fresh_lib.write(f"{sym}_sym", self.df, staged=True)

        for sym in range(num_symbols):
            self.fresh_lib._nvs.compact_incomplete(f"{sym}_sym", False, False)

    def peakmem_write_staged(self, rows, num_symbols):
        for sym in range(num_symbols):
            self.fresh_lib.write(f"{sym}_sym", self.df, staged=True)

        for sym in range(num_symbols):
            self.fresh_lib._nvs.compact_incomplete(f"{sym}_sym", False, False)

    def time_write_batch(self, rows, num_symbols):
        payloads = [WritePayload(f"{sym}_sym", self.df) for sym in range(num_symbols)]
        self.fresh_lib.write_batch(payloads)

    def peakmem_write_batch(self, rows, num_symbols):
        payloads = [WritePayload(f"{sym}_sym", self.df) for sym in range(num_symbols)]
        self.fresh_lib.write_batch(payloads)

    def time_read(self, rows, num_symbols):
        [self.lib.read(f"{sym}_sym").data for sym in range(num_symbols)]

    def peakmem_read(self, rows, num_symbols):
        [self.lib.read(f"{sym}_sym").data for sym in range(num_symbols)]

    def time_read_short_wide(self, rows, num_symbols):
        lib = self.ac[get_prewritten_lib_name(BasicFunctions.WIDE_DF_ROWS)]
        lib.read("short_wide_sym").data

    def peakmem_read_short_wide(self, rows, num_symbols):
        lib = self.ac[get_prewritten_lib_name(BasicFunctions.WIDE_DF_ROWS)]
        lib.read("short_wide_sym").data

    def time_read_batch(self, rows, num_symbols):
        read_reqs = [ReadRequest(f"{sym}_sym") for sym in range(num_symbols)]
        self.lib.read_batch(read_reqs)

    def time_read_batch_pure(self, rows, num_symbols):
        self.lib.read_batch(self.read_reqs)

    def peakmem_read_batch(self, rows, num_symbols):
        read_reqs = [ReadRequest(f"{sym}_sym") for sym in range(num_symbols)]
        self.lib.read_batch(read_reqs)

    def time_read_with_columns(self, rows, num_symbols):
        COLS = ["value"]
        [self.lib.read(f"{sym}_sym", columns=COLS).data for sym in range(num_symbols)]

    def peakmem_read_with_columns(self, rows, num_symbols):
        COLS = ["value"]
        [self.lib.read(f"{sym}_sym", columns=COLS).data for sym in range(num_symbols)]

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

    def time_read_with_date_ranges(self, rows, num_symbols):
        [
            self.lib.read(f"{sym}_sym", date_range=BasicFunctions.DATE_RANGE).data
            for sym in range(num_symbols)
        ]

    def peakmem_read_with_date_ranges(self, rows, num_symbols):
        [
            self.lib.read(f"{sym}_sym", date_range=BasicFunctions.DATE_RANGE).data
            for sym in range(num_symbols)
        ]

    def time_read_batch_with_date_ranges(self, rows, num_symbols):
        read_reqs = [
            ReadRequest(f"{sym}_sym", date_range=BasicFunctions.DATE_RANGE)
            for sym in range(num_symbols)
        ]
        self.lib.read_batch(read_reqs)

    def peakmem_read_batch_with_date_ranges(self, rows, num_symbols):
        read_reqs = [
            ReadRequest(f"{sym}_sym", date_range=BasicFunctions.DATE_RANGE)
            for sym in range(num_symbols)
        ]
        self.lib.read_batch(read_reqs)


from shutil import copytree, rmtree
class ModificationFunctions:
    """
    Modification functions (update, append, delete) need a different setup/teardown process, thus we place them in a
    separate group.
    """
    number = 1 # We do a single run between setup and teardown because we e.g. can't delete a symbol twice
    timeout = 6000
    ARCTIC_DIR = "modification_functions"
    ARCTIC_DIR_ORIGINAL = "modification_functions_original"
    CONNECTION_STRING = f"lmdb://{ARCTIC_DIR}?map_size=20GB"
    WIDE_DF_ROWS = WIDE_DF_ROWS
    WIDE_DF_COLS = WIDE_DF_COLS

    params = PARAMS
    param_names = PARAM_NAMES

    def setup_cache(self):
        self.ac = Arctic(ModificationFunctions.CONNECTION_STRING)
        num_rows, num_symbols = ModificationFunctions.params

        self.init_dfs = {rows: generate_pseudo_random_dataframe(rows) for rows in num_rows}
        for rows in num_rows:
            lib_name = get_prewritten_lib_name(rows)
            self.ac.delete_library(lib_name)
            lib = self.ac.create_library(lib_name)
            payloads = [WritePayload(f"{sym}_sym", self.init_dfs[rows]) for sym in range(num_symbols[-1])]
            lib.write_batch(payloads)

        lib_name = get_prewritten_lib_name(ModificationFunctions.WIDE_DF_ROWS)
        self.ac.delete_library(lib_name)
        lib = self.ac.create_library(lib_name)
        lib.write(
            "short_wide_sym",
            generate_random_floats_dataframe_with_index(
                ModificationFunctions.WIDE_DF_ROWS, ModificationFunctions.WIDE_DF_COLS
            ),
        )

        # We use the fact that we're running on LMDB to store a copy of the initial arctic directory.
        # Then on each teardown we restore the initial state by overwriting the modified with the original.
        copytree(ModificationFunctions.ARCTIC_DIR, ModificationFunctions.ARCTIC_DIR_ORIGINAL)


    def setup(self, rows, num_symbols):
        def get_time_at_fraction_of_df(fraction, rows=rows):
            end_time = pd.Timestamp("1/1/2023")
            time_delta = pd.tseries.offsets.DateOffset(seconds=round(rows * (fraction-1)))
            return end_time + time_delta

        self.df_update_single = generate_pseudo_random_dataframe(1, "s", get_time_at_fraction_of_df(0.5))
        self.df_update_half = generate_pseudo_random_dataframe(rows//2, "s", get_time_at_fraction_of_df(0.75))
        self.df_update_upsert = generate_pseudo_random_dataframe(rows, "s", get_time_at_fraction_of_df(1.5))
        self.df_append_single = generate_pseudo_random_dataframe(1, "s", get_time_at_fraction_of_df(1.1))
        self.df_append_large = generate_pseudo_random_dataframe(rows, "s", get_time_at_fraction_of_df(2))

        self.df_update_short_wide = generate_random_floats_dataframe_with_index(
            ModificationFunctions.WIDE_DF_ROWS, ModificationFunctions.WIDE_DF_COLS
        )
        self.df_append_short_wide = generate_random_floats_dataframe_with_index(
            ModificationFunctions.WIDE_DF_ROWS, ModificationFunctions.WIDE_DF_COLS, "s", get_time_at_fraction_of_df(2, rows=ModificationFunctions.WIDE_DF_ROWS)
        )

        self.ac = Arctic(ModificationFunctions.CONNECTION_STRING)
        self.lib = self.ac[get_prewritten_lib_name(rows)]
        self.lib_short_wide = self.ac[get_prewritten_lib_name(ModificationFunctions.WIDE_DF_ROWS)]


    def teardown(self, rows, num_symbols):
        # After the modification functions clean up the changes by replacing the modified ARCTIC_DIR with the original ARCTIC_DIR_ORIGINAL
        # TODO: We can use dirs_exist_ok=True on copytree instead of removing first if we run with python version >=3.8
        rmtree(ModificationFunctions.ARCTIC_DIR)
        copytree(ModificationFunctions.ARCTIC_DIR_ORIGINAL, ModificationFunctions.ARCTIC_DIR)
        del self.ac


    def time_update_single(self, rows, num_symbols):
        [self.lib.update(f"{sym}_sym", self.df_update_single) for sym in range(num_symbols)]

    def time_update_half(self, rows, num_symbols):
        [self.lib.update(f"{sym}_sym", self.df_update_half) for sym in range(num_symbols)]

    def time_update_upsert(self, rows, num_symbols):
        [self.lib.update(f"{sym}_sym", self.df_update_upsert, upsert=True) for sym in range(num_symbols)]

    def time_update_short_wide(self, rows, num_symbols):
        self.lib_short_wide.update("short_wide_sym", self.df_update_short_wide)

    def time_append_single(self, rows, num_symbols):
        [self.lib.append(f"{sym}_sym", self.df_append_single) for sym in range(num_symbols)]

    def time_append_large(self, rows, num_symbols):
        [self.lib.append(f"{sym}_sym", self.df_append_large) for sym in range(num_symbols)]

    def time_append_short_wide(self, rows, num_symbols):
        self.lib_short_wide.append("short_wide_sym", self.df_append_short_wide)

    def time_delete(self, rows, num_symbols):
        [self.lib.delete(f"{sym}_sym") for sym in range(num_symbols)]

    def time_delete_short_wide(self, rows, num_symbols):
        self.lib_short_wide.delete("short_wide_sym")
