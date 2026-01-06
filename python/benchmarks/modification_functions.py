"""
Copyright 2026 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""
import sys
import time
from typing import List
from shutil import copytree, rmtree

from arcticdb import Arctic
from arcticdb.util.test import config_context

import pandas as pd

from benchmarks.common import *


def get_time_at_fraction_of_df(fraction, rows):
    end_time = pd.Timestamp("1/1/2023")
    time_delta = pd.tseries.offsets.DateOffset(seconds=round(rows * (fraction - 1)))
    return end_time + time_delta


class ModificationFunctions:
    rounds = 1
    number = 1  # We do a single run between setup and teardown because we e.g. can't delete a symbol twice
    repeat = 3
    warmup_time = 0

    ARCTIC_DIR = "modification_functions"
    ARCTIC_DIR_ORIGINAL = "modification_functions_original"
    CONNECTION_STRING = f"lmdb://{ARCTIC_DIR}"
    WIDE_DF_ROWS = 5_000
    WIDE_DF_COLS = 30_000

    params = [1_000_000, 10_000_000]
    param_names = ["rows"]

    class LargeAppendDataModify:
        """
        This class will hold a cache of append large dataframes.

        This cache is to create dataframes with timestamps sequenced over time so that overlap does not occur.
        """

        def __init__(self, num_rows_list: List[int], number_elements: int):
            self.df_append_large = {}
            self.df_append_short_wide = {}
            start_time = time.time()
            for rows in num_rows_list:
                print("Generating dataframe with rows: ", rows)
                lst = list()
                lst_saw = list()
                for n in range(number_elements + 1):
                    print("Generating dataframe no: ", n)

                    df = generate_pseudo_random_dataframe(rows, "s", get_time_at_fraction_of_df(2 * (n + 1), rows))
                    df_saw = generate_random_floats_dataframe_with_index(
                        ModificationFunctions.WIDE_DF_ROWS,
                        ModificationFunctions.WIDE_DF_COLS,
                        "s",
                        get_time_at_fraction_of_df(2 * (n + 1), rows=ModificationFunctions.WIDE_DF_ROWS),
                    )

                    lst.append(df)
                    lst_saw.append(df_saw)
                    print(f"STANDARD     Index {df.iloc[0].name} - {df.iloc[df.shape[0] - 1].name}")
                    print(f"SHORT_n_WIDE Index {df_saw.iloc[0].name} - {df_saw.iloc[df_saw.shape[0] - 1].name}")
                print("Add dataframes: ", len(lst))
                self.df_append_large[rows] = lst
                self.df_append_short_wide[rows] = lst_saw
            print("APPEND LARGE cache generation took (s) :", time.time() - start_time)

    def __init__(self):
        self.logger = get_logger()

    def setup_cache(self):
        start = time.time()
        lad = self._setup_cache()
        self.logger.info(f"SETUP_CACHE TIME: {time.time() - start}")
        return lad

    def _setup_cache(self):
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
        self.df_update_half = generate_pseudo_random_dataframe(rows // 2, "s", get_time_at_fraction_of_df(0.75, rows))
        self.df_update_upsert = generate_pseudo_random_dataframe(rows, "s", get_time_at_fraction_of_df(1.5, rows))
        self.df_append_single = generate_pseudo_random_dataframe(1, "s", get_time_at_fraction_of_df(1.1, rows))

        self.df_update_short_wide = generate_random_floats_dataframe_with_index(
            ModificationFunctions.WIDE_DF_ROWS, ModificationFunctions.WIDE_DF_COLS
        )

        self.ac = Arctic(ModificationFunctions.CONNECTION_STRING)
        self.lib = self.ac[get_prewritten_lib_name(rows)]
        self.lib_short_wide = self.ac[get_prewritten_lib_name(ModificationFunctions.WIDE_DF_ROWS)]

        # Used by time_delete_multiple_versions
        for i in range(100):
            self.lib.write("sym_delete_multiple", pd.DataFrame({"a": [0]}), prune_previous_versions=False)

    def teardown(self, lad: LargeAppendDataModify, rows):
        rmtree(ModificationFunctions.ARCTIC_DIR)
        copytree(ModificationFunctions.ARCTIC_DIR_ORIGINAL, ModificationFunctions.ARCTIC_DIR, dirs_exist_ok=True)

        del self.ac

    def time_update_single(self, lad: LargeAppendDataModify, rows):
        self.lib.update("sym", self.df_update_single)

    def time_update_half(self, lad: LargeAppendDataModify, rows):
        self.lib.update("sym", self.df_update_half)

    def time_update_upsert(self, lad: LargeAppendDataModify, rows):
        self.lib.update("sym", self.df_update_upsert, upsert=True)

    def time_update_short_wide(self, lad: LargeAppendDataModify, rows):
        self.lib_short_wide.update("short_wide_sym", self.df_update_short_wide)

    def time_append_single(self, lad: LargeAppendDataModify, rows):
        self.lib.append("sym", self.df_append_single)

    def time_append_large(self, lad: LargeAppendDataModify, rows):
        large: pd.DataFrame = lad.df_append_large[rows].pop(0)
        self.lib.append("sym", large)

    def time_append_short_wide(self, lad: LargeAppendDataModify, rows):
        large: pd.DataFrame = lad.df_append_short_wide[rows].pop(0)
        self.lib_short_wide.append("short_wide_sym", large)

    def time_delete(self, lad: LargeAppendDataModify, rows):
        self.lib.delete("sym")

    def time_delete_multiple_versions(self, lad: LargeAppendDataModify, rows):
        self.lib.delete("sym_delete_multiple", list(range(99)))

    def time_delete_short_wide(self, lad: LargeAppendDataModify, rows):
        self.lib_short_wide.delete("short_wide_sym")

    def time_delete_over_time(self, lad: LargeAppendDataModify, rows):
        with config_context("VersionMap.ReloadInterval", 0):
            for i in range(100):
                self.lib.write("delete_over_time", pd.DataFrame())
                self.lib.delete("delete_over_time")
