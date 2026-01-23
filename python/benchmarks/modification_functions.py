"""
Copyright 2026 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

import random
import time
from shutil import copytree, rmtree

from arcticdb import Arctic
from arcticdb.util.test import config_context

import pandas as pd
from asv_runner.benchmarks.mark import SkipNotImplemented

from benchmarks.common import *
from arcticdb.util.logger import get_logger

from benchmarks.environment_setup import Storage, create_libraries_across_storages


def get_time_at_fraction_of_df(fraction, rows):
    end_time = pd.Timestamp("1/1/2023")
    time_delta = pd.tseries.offsets.DateOffset(seconds=round(rows * (fraction - 1)))
    return end_time + time_delta


def _sym_name(rows, cols):
    return f"sym_{rows}_{cols}"


class ModificationFunctions:
    number = 1  # We do a single run between setup and teardown so we always start in the same state
    warmup_time = 0
    timeout = 600

    rows_and_cols = [(1_000_000, 2), (10_000_000, 2), (5_000, 30_000)]
    storages = [Storage.LMDB, Storage.AMAZON]
    param_names = ["rows_and_cols", "storage"]

    params = [rows_and_cols, storages]

    def __init__(self):
        self.logger = get_logger()
        self.lib = None
        self.sym = None

    def setup_cache(self):
        start = time.time()
        libs_for_storage = self._setup_cache()
        self.logger.info(f"SETUP_CACHE TIME: {time.time() - start}")
        return libs_for_storage

    def _setup_cache(self):
        lib_for_storage = create_libraries_across_storages(ModificationFunctions.storages)

        dfs = {
            (r, c): generate_random_floats_dataframe_with_index(num_rows=r, num_cols=c, end_timestamp="1/1/2023")
            for r, c in ModificationFunctions.rows_and_cols
        }
        for storage in ModificationFunctions.storages:
            lib = lib_for_storage[storage]
            if lib is None:
                continue
            for rows, cols in ModificationFunctions.rows_and_cols:
                df = dfs[(rows, cols)]
                lib.write(_sym_name(rows, cols), df)

        return lib_for_storage

    def setup(self, libs_for_storage, rows_and_cols, storage):
        self.lib = libs_for_storage[storage]
        if self.lib is None:
            raise SkipNotImplemented

        rows, cols = rows_and_cols

        self.sym = _sym_name(rows, cols)
        assert self.lib.has_symbol(self.sym)
        self.lib._nvs.restore_version(self.sym, 0)
        assert self.lib.get_description(self.sym).row_count == rows

        self.df_update_single = generate_random_floats_dataframe_with_index(
            1, cols, end_timestamp=get_time_at_fraction_of_df(0.5, rows)
        )
        self.df_update_half = generate_random_floats_dataframe_with_index(
            rows // 2, cols, end_timestamp=get_time_at_fraction_of_df(0.75, rows)
        )
        self.df_update_outside_date_range = generate_random_floats_dataframe_with_index(
            rows, cols, end_timestamp=get_time_at_fraction_of_df(1.5, rows)
        )
        self.df_append_single = generate_random_floats_dataframe_with_index(
            1, cols, end_timestamp=get_time_at_fraction_of_df(1.1, rows)
        )
        self.df_append_large = generate_random_floats_dataframe_with_index(rows, cols)
        append_index = pd.date_range(start="1/2/2023", periods=rows, freq="ms")
        self.df_append_large.index = append_index

    def time_update_single(self, *args):
        self.lib.update(self.sym, self.df_update_single)

    def time_update_half(self, *args):
        self.lib.update(self.sym, self.df_update_half)

    def time_update_outside_date_range(self, *args):
        self.lib.update(self.sym, self.df_update_outside_date_range, upsert=True)

    def time_append_single(self, *args):
        self.lib.append(self.sym, self.df_append_single)

    def time_append_large(self, *args):
        self.lib.append(self.sym, self.df_append_large)


class Deletion:
    number = 1  # We do a single run between setup and teardown because we can't delete a symbol twice
    warmup_time = 0
    timeout = 600

    rows_and_cols = [(1_000_000, 2), (10_000_000, 2), (5_000, 30_000)]
    storages = [Storage.AMAZON, Storage.LMDB]
    params = [rows_and_cols, storages]
    param_names = ["rows_and_cols", "storage"]

    def setup_cache(self):
        lib_for_storage = create_libraries_across_storages(Deletion.storages)
        return lib_for_storage

    def setup(self, lib_for_storage, rows_and_cols, storage):
        lib = lib_for_storage[storage]
        if lib is None:
            raise SkipNotImplemented
        self.lib = lib
        self.lib._nvs.version_store.clear()

        rows, cols = rows_and_cols
        df = generate_random_floats_dataframe_with_index(num_rows=rows, num_cols=cols)
        self.lib.write("sym", df)

    def time_delete(self, *args):
        assert self.lib.has_symbol("sym")
        self.lib.delete("sym")


class DeleteOverTimeModificationFunctions:

    ARCTIC_DIR = "modification_functions_delete"
    CONNECTION_STRING = f"lmdb://{ARCTIC_DIR}"

    def setup(self):
        self.ac = Arctic(self.CONNECTION_STRING)
        self.ac.delete_library("test_lib")
        self.lib = self.ac.create_library("test_lib")

    def teardown(self):
        self.ac.delete_library("test_lib")
        del self.ac
        rmtree(self.ARCTIC_DIR, ignore_errors=True)

    def time_delete_over_time(self):
        sym = f"sym_{random.randint(0, 1_000_000)}"
        with config_context("VersionMap.ReloadInterval", 0):
            for i in range(100):
                self.lib.write(sym, pd.DataFrame({"a": [1]}))
                self.lib.delete(sym)


class DeleteMultipleVersions:
    number = 1  # need to prepare the symbol in setup so can only run a single time per setup
    warmup_time = 0

    ARCTIC_DIR = "modification_functions_delete_versions"
    CONNECTION_STRING = f"lmdb://{ARCTIC_DIR}"

    def setup(self):
        self.ac = Arctic(self.CONNECTION_STRING)
        self.ac.delete_library("test_lib")
        self.lib = self.ac.create_library("test_lib")
        self.sym = f"sym_{random.randint(0, 1_000_000)}"

        for i in range(100):
            self.lib.write(self.sym, pd.DataFrame({"a": [0]}), prune_previous_versions=False)

    def teardown(self):
        self.ac.delete_library("test_lib")
        del self.ac
        rmtree(self.ARCTIC_DIR, ignore_errors=True)

    def time_delete_multiple_versions(self):
        self.lib.delete(self.sym, list(range(99)))
