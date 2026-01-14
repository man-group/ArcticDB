"""
Copyright 2026 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

import time

from arcticdb import Arctic, QueryBuilder
from arcticdb.version_store.library import UpdatePayload, WritePayload, ReadRequest
from asv_runner.benchmarks.mark import skip_benchmark, SkipNotImplemented

import pandas as pd

from benchmarks.common import *

from arcticdb.util.logger import get_logger
from benchmarks.common import generate_pseudo_random_dataframe, get_prewritten_lib_name
from benchmarks.environment_setup import Storage, create_libraries_across_storages, is_storage_enabled, \
    create_libraries

# We use larger dataframes for non-batch methods
PARAMS = [1_000_000, 10_000_000]
PARAM_NAMES = ["rows"]
DATE_RANGE = (pd.Timestamp("2022-12-31"), pd.Timestamp("2023-01-01"))


class BasicFunctions:
    sample_time = 2
    rounds = 2
    repeat = (1, 10, 20.0)
    warmup_time = 0.2

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


class BatchWrite:
    rounds = 1
    sample_time = 0.1

    num_rows = [25_000, 50_000]
    num_symbols = [100, 200]
    storages = [Storage.LMDB, Storage.AMAZON]

    params = [num_rows, num_symbols, storages]
    param_names = ["num_rows", "num_symbols", "storage"]

    def __init__(self):
        self.logger = get_logger()
        self.lib = None
        self.df = None

    def setup_cache(self):
        start = time.time()
        lib_for_storage = self._setup_cache()
        self.logger.info(f"SETUP_CACHE TIME: {time.time() - start}")
        return lib_for_storage

    def _lib_name(self, rows):
        return f"lib_{rows}"

    def _setup_cache(self):
        lib_for_storage = create_libraries_across_storages(BatchWrite.storages)
        return lib_for_storage

    def setup(self, lib_for_storage, rows, num_symbols, storage):
        self.lib = lib_for_storage[storage]
        if self.lib is None:
            raise SkipNotImplemented

        self.df = generate_pseudo_random_dataframe(rows)

    def time_write_batch(self, lib_for_storage, rows, num_symbols, storage):
        payloads = [WritePayload(f"{sym}_sym", self.df) for sym in range(num_symbols)]
        self.lib.write_batch(payloads)

    def peakmem_write_batch(self, lib_for_storage, rows, num_symbols, storage):
        payloads = [WritePayload(f"{sym}_sym", self.df) for sym in range(num_symbols)]
        self.lib.write_batch(payloads)


class BatchFunctions:
    rounds = 1
    sample_time = 0.1

    num_rows = [25_000, 50_000]
    num_symbols = [100, 200]
    storages = [Storage.LMDB, Storage.AMAZON]

    params = [num_rows, num_symbols, storages]
    param_names = ["num_rows", "num_symbols", "storage"]

    def __init__(self):
        self.logger = get_logger()

    def setup_cache(self):
        start = time.time()
        lib_for_storage = self._setup_cache()
        self.logger.info(f"SETUP_CACHE TIME: {time.time() - start}")
        return lib_for_storage

    def _lib_name(self, rows):
        return f"lib_{rows}"

    def _setup_cache(self):
        libs_for_storage = dict()
        library_names = [self._lib_name(rows) for rows in BatchFunctions.num_rows]
        self.dfs = {rows: generate_pseudo_random_dataframe(rows) for rows in BatchFunctions.num_rows}

        for storage in BatchFunctions.storages:
            libraries = create_libraries(storage, library_names)
            libs_for_storage[storage] = dict(zip(library_names, libraries))

        for rows in BatchFunctions.num_rows:
            num_syms = BatchFunctions.num_symbols[-1]
            df = self.dfs[rows]
            lib_name = self._lib_name(rows)
            write_payloads = [WritePayload(f"{i}_sym", df) for i in range(num_syms)]
            for storage in BatchFunctions.storages:
                lib = libs_for_storage[storage][lib_name]
                if not lib:
                    continue
                lib.write_batch(write_payloads)

        return libs_for_storage

    def setup(self, libs_for_storage, rows, num_symbols, storage):
        self.lib = libs_for_storage[storage][self._lib_name(rows)]
        if self.lib is None:
            raise SkipNotImplemented

        self.df = generate_pseudo_random_dataframe(rows)
        self.update_df = generate_pseudo_random_dataframe(rows // 2)

    def time_update_batch(self, libs_for_storage, rows, num_symbols, storage):
        payloads = [UpdatePayload(f"{sym}_sym", self.update_df) for sym in range(num_symbols)]
        results = self.lib.update_batch(payloads)
        assert results[0].version >= 1
        assert results[-1].version >= 1

    @skip_benchmark
    def peakmem_update_batch(self, libs_for_storage, rows, num_symbols, storage):
        payloads = [UpdatePayload(f"{sym}_sym", self.update_df) for sym in range(num_symbols)]
        results = self.lib.update_batch(payloads)
        assert results[0].version >= 1
        assert results[-1].version >= 1

    def time_read_batch(self, libs_for_storage, rows, num_symbols, storage):
        read_reqs = [ReadRequest(f"{sym}_sym") for sym in range(num_symbols)]
        self.lib.read_batch(read_reqs)

    def peakmem_read_batch(self, libs_for_storage, rows, num_symbols, storage):
        read_reqs = [ReadRequest(f"{sym}_sym") for sym in range(num_symbols)]
        self.lib.read_batch(read_reqs)

    def time_read_batch_with_columns(self, libs_for_storage, rows, num_symbols, storage):
        COLS = ["value"]
        read_reqs = [ReadRequest(f"{sym}_sym", columns=COLS) for sym in range(num_symbols)]
        self.lib.read_batch(read_reqs)

    def peakmem_read_batch_with_columns(self, libs_for_storage, rows, num_symbols, storage):
        COLS = ["value"]
        read_reqs = [ReadRequest(f"{sym}_sym", columns=COLS) for sym in range(num_symbols)]
        self.lib.read_batch(read_reqs)

    def time_read_batch_with_date_ranges(self, libs_for_storage, rows, num_symbols, storage):
        read_reqs = [ReadRequest(f"{sym}_sym", date_range=DATE_RANGE) for sym in range(num_symbols)]
        self.lib.read_batch(read_reqs)

    def peakmem_read_batch_with_date_ranges(self, libs_for_storage, rows, num_symbols, storage):
        read_reqs = [ReadRequest(f"{sym}_sym", date_range=DATE_RANGE) for sym in range(num_symbols)]
        self.lib.read_batch(read_reqs)
