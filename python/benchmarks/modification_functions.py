"""
Copyright 2023 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

from arcticdb import Arctic
import pandas as pd
from .common import *
from shutil import copytree, rmtree


class ModificationFunctions:
    """
    Modification functions (update, append, delete) need a different setup/teardown process, thus we place them in a
    separate group.
    """

    number = 1  # We do a single run between setup and teardown because we e.g. can't delete a symbol twice
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
        rows_values = ModificationFunctions.params

        self.init_dfs = {
            rows: generate_pseudo_random_dataframe(rows) for rows in rows_values
        }
        for rows in rows_values:
            lib_name = get_prewritten_lib_name(rows)
            self.ac.delete_library(lib_name)
            lib = self.ac.create_library(lib_name)
            lib.write("sym", self.init_dfs[rows])

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
        copytree(
            ModificationFunctions.ARCTIC_DIR, ModificationFunctions.ARCTIC_DIR_ORIGINAL
        )

    def setup(self, rows):
        def get_time_at_fraction_of_df(fraction, rows=rows):
            end_time = pd.Timestamp("1/1/2023")
            time_delta = pd.tseries.offsets.DateOffset(
                seconds=round(rows * (fraction - 1))
            )
            return end_time + time_delta

        self.df_update_single = generate_pseudo_random_dataframe(
            1, "s", get_time_at_fraction_of_df(0.5)
        )
        self.df_update_half = generate_pseudo_random_dataframe(
            rows // 2, "s", get_time_at_fraction_of_df(0.75)
        )
        self.df_update_upsert = generate_pseudo_random_dataframe(
            rows, "s", get_time_at_fraction_of_df(1.5)
        )
        self.df_append_single = generate_pseudo_random_dataframe(
            1, "s", get_time_at_fraction_of_df(1.1)
        )
        self.df_append_large = generate_pseudo_random_dataframe(
            rows, "s", get_time_at_fraction_of_df(2)
        )

        self.df_update_short_wide = generate_random_floats_dataframe_with_index(
            ModificationFunctions.WIDE_DF_ROWS, ModificationFunctions.WIDE_DF_COLS
        )
        self.df_append_short_wide = generate_random_floats_dataframe_with_index(
            ModificationFunctions.WIDE_DF_ROWS,
            ModificationFunctions.WIDE_DF_COLS,
            "s",
            get_time_at_fraction_of_df(2, rows=ModificationFunctions.WIDE_DF_ROWS),
        )

        self.ac = Arctic(ModificationFunctions.CONNECTION_STRING)
        self.lib = self.ac[get_prewritten_lib_name(rows)]
        self.lib_short_wide = self.ac[
            get_prewritten_lib_name(ModificationFunctions.WIDE_DF_ROWS)
        ]

    def teardown(self, rows):
        # After the modification functions clean up the changes by replacing the modified ARCTIC_DIR with the original ARCTIC_DIR_ORIGINAL
        # TODO: We can use dirs_exist_ok=True on copytree instead of removing first if we run with python version >=3.8
        rmtree(ModificationFunctions.ARCTIC_DIR)
        copytree(
            ModificationFunctions.ARCTIC_DIR_ORIGINAL, ModificationFunctions.ARCTIC_DIR
        )
        del self.ac

    def time_update_single(self, rows):
        self.lib.update(f"sym", self.df_update_single)

    def time_update_half(self, rows):
        self.lib.update(f"sym", self.df_update_half)

    def time_update_upsert(self, rows):
        self.lib.update(f"sym", self.df_update_upsert, upsert=True)

    def time_update_short_wide(self, rows):
        self.lib_short_wide.update("short_wide_sym", self.df_update_short_wide)

    def time_append_single(self, rows):
        self.lib.append(f"sym", self.df_append_single)

    def time_append_large(self, rows):
        self.lib.append(f"sym", self.df_append_large)

    def time_append_short_wide(self, rows):
        self.lib_short_wide.append("short_wide_sym", self.df_append_short_wide)

    def time_delete(self, rows):
        self.lib.delete(f"sym")

    def time_delete_short_wide(self, rows):
        self.lib_short_wide.delete("short_wide_sym")
