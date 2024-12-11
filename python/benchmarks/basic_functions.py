"""
Copyright 2023 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""
import time
from typing import List
from arcticdb import Arctic
from arcticdb.version_store.library import WritePayload, ReadRequest
import pandas as pd

from .common import *


# Common parameters between BasicFunctions and ModificationFunctions
WIDE_DF_ROWS = 5_000
WIDE_DF_COLS = 30_000
# We use larger dataframes for non-batch methods
PARAMS = ([1_000_000, 1_500_000])
PARAM_NAMES = ["rows"]
BATCH_PARAMS = ([25_000, 50_000], [500, 1000])
BATCH_PARAM_NAMES = ["rows", "num_symbols"]
DATE_RANGE = pd.date_range("2022-12-31", "2023-01-01")


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

        self.dfs = {rows: generate_pseudo_random_dataframe(rows) for rows in rows_values}
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

def get_time_at_fraction_of_df(fraction, rows):
    end_time = pd.Timestamp("1/1/2023")
    time_delta = pd.tseries.offsets.DateOffset(seconds=round(rows * (fraction-1)))
    return end_time + time_delta

from shutil import copytree, rmtree
class ModificationFunctions:
    """
    Modification functions (update, append, delete) need a different setup/teardown process, thus we place them in a
    separate group.
    """
    rounds = 1
    number = 1 # We do a single run between setup and teardown because we e.g. can't delete a symbol twice
    repeat = 3
    timeout = 6000
    ARCTIC_DIR = "modification_functions"
    ARCTIC_DIR_ORIGINAL = "modification_functions_original"
    CONNECTION_STRING = f"lmdb://{ARCTIC_DIR}?map_size=20GB"
    WIDE_DF_ROWS = WIDE_DF_ROWS
    WIDE_DF_COLS = WIDE_DF_COLS

    params = PARAMS
    param_names = PARAM_NAMES

    class LargeAppendDataModify:
        """
            This class will hold a cache of append large dataframes
            The purpose of this cache is to create dataframes
            which timestamps are sequenced over time so that 
            overlap does not occur
        """

        def __init__(self, num_rows_list:List[int], number_elements:int):
            self.df_append_large = {}
            self.df_append_short_wide = {}
            start_time = time.time()
            for rows in num_rows_list:
                print("Generating dataframe with rows: ", rows)
                lst = list()
                lst_saw = list()
                for n in range(number_elements+1):
                    print("Generating dataframe no: ", n)
                    
                    df = generate_pseudo_random_dataframe(rows, "s", get_time_at_fraction_of_df(2*(n+1), rows))
                    df_saw = generate_random_floats_dataframe_with_index(
                        ModificationFunctions.WIDE_DF_ROWS, ModificationFunctions.WIDE_DF_COLS, "s", 
                        get_time_at_fraction_of_df(2*(n+1), rows=ModificationFunctions.WIDE_DF_ROWS)
                    )

                    lst.append(df)
                    lst_saw.append(df_saw)
                    print(f"STANDARD     Index {df.iloc[0].name} - {df.iloc[df.shape[0] - 1].name}")
                    print(f"SHORT_n_WIDE Index {df_saw.iloc[0].name} - {df_saw.iloc[df_saw.shape[0] - 1].name}")
                print("Add dataframes: ", len(lst))
                self.df_append_large[rows] = lst
                self.df_append_short_wide[rows] = lst_saw
            print("APPEND LARGE cache generation took (s) :", time.time() - start_time)

    def setup_cache(self):

        self.ac = Arctic(ModificationFunctions.CONNECTION_STRING)
        rows_values = ModificationFunctions.params

        self.init_dfs = {rows: generate_pseudo_random_dataframe(rows) for rows in rows_values}
        for rows in rows_values:
            lib_name = get_prewritten_lib_name(rows)
            self.ac.delete_library(lib_name)
            lib = self.ac.create_library(lib_name)
            df = self.init_dfs[rows]
            lib.write("sym", df)
            print(f"INITIAL DATAFRAME {rows} rows has Index {df.iloc[0].name} - {df.iloc[df.shape[0] - 1].name}")

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

        number_iteration = ModificationFunctions.repeat * ModificationFunctions.number * ModificationFunctions.rounds

        lad = ModificationFunctions.LargeAppendDataModify(ModificationFunctions.params, number_iteration)

        return lad

    def setup(self, lad: LargeAppendDataModify, rows):

        self.df_update_single = generate_pseudo_random_dataframe(1, "s", get_time_at_fraction_of_df(0.5, rows))
        self.df_update_half = generate_pseudo_random_dataframe(rows//2, "s", get_time_at_fraction_of_df(0.75, rows))
        self.df_update_upsert = generate_pseudo_random_dataframe(rows, "s", get_time_at_fraction_of_df(1.5, rows))
        self.df_append_single = generate_pseudo_random_dataframe(1, "s", get_time_at_fraction_of_df(1.1, rows))

        self.df_update_short_wide = generate_random_floats_dataframe_with_index(
            ModificationFunctions.WIDE_DF_ROWS, ModificationFunctions.WIDE_DF_COLS
        )

        self.ac = Arctic(ModificationFunctions.CONNECTION_STRING)
        self.lib = self.ac[get_prewritten_lib_name(rows)]
        self.lib_short_wide = self.ac[get_prewritten_lib_name(ModificationFunctions.WIDE_DF_ROWS)]


    def teardown(self, lad: LargeAppendDataModify, rows):
        # After the modification functions clean up the changes by replacing the modified ARCTIC_DIR with the original ARCTIC_DIR_ORIGINAL
        # TODO: We can use dirs_exist_ok=True on copytree instead of removing first if we run with python version >=3.8
        rmtree(ModificationFunctions.ARCTIC_DIR)
        copytree(ModificationFunctions.ARCTIC_DIR_ORIGINAL, ModificationFunctions.ARCTIC_DIR)

        del self.ac

    def time_update_single(self, lad: LargeAppendDataModify, rows):
        self.lib.update(f"sym", self.df_update_single)

    def time_update_half(self, lad: LargeAppendDataModify, rows):
        self.lib.update(f"sym", self.df_update_half)

    def time_update_upsert(self, lad: LargeAppendDataModify, rows):
        self.lib.update(f"sym", self.df_update_upsert, upsert=True)

    def time_update_short_wide(self, lad: LargeAppendDataModify, rows):
        self.lib_short_wide.update("short_wide_sym", self.df_update_short_wide)

    def time_append_single(self, lad: LargeAppendDataModify, rows):
        self.lib.append(f"sym", self.df_append_single)

    def time_append_large(self, lad: LargeAppendDataModify, rows):
        large: pd.DataFrame = lad.df_append_large[rows].pop()
        self.lib.append(f"sym", large)

    def time_append_short_wide(self, lad: LargeAppendDataModify, rows):
        large: pd.DataFrame = lad.df_append_short_wide[rows].pop()
        self.lib_short_wide.append("short_wide_sym", large)

    def time_delete(self, lad: LargeAppendDataModify, rows):
        self.lib.delete(f"sym")

    def time_delete_short_wide(self, lad: LargeAppendDataModify, rows):
        self.lib_short_wide.delete("short_wide_sym")
