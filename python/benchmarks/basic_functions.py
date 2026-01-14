"""
Copyright 2026 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

import time

from arcticdb import QueryBuilder
from arcticdb.version_store.library import UpdatePayload, WritePayload, ReadRequest
from asv_runner.benchmarks.mark import skip_benchmark, SkipNotImplemented

import pandas as pd

from benchmarks.common import *

from arcticdb.util.logger import get_logger
from benchmarks.common import generate_pseudo_random_dataframe
from benchmarks.environment_setup import Storage, create_libraries_across_storages, is_storage_enabled, create_libraries
import arcticdb.toolbox.query_stats as qs

DATE_RANGE = (pd.Timestamp("2022-12-31"), pd.Timestamp("2023-01-01"))
STORAGES = [Storage.LMDB, Storage.AMAZON]


def _lib_name(rows):
    return f"lib_{rows}"


class BasicFunctions:
    sample_time = 2
    rounds = 2
    repeat = (1, 10, 20.0)
    warmup_time = 0.2
    timeout = 600

    num_rows = [1_000_000, 10_000_000]
    use_query_stats = [True, False]

    params = [num_rows, use_query_stats, STORAGES]
    param_names = ["num_rows", "use_query_stats", "storage"]

    def __init__(self):
        self.logger = get_logger()
        self.lib = None
        self.df = None

    def setup_cache(self):
        start = time.time()
        libs_for_storage = self._setup_cache()
        self.logger.info(f"SETUP_CACHE TIME: {time.time() - start}")
        return libs_for_storage

    def _setup_cache(self):
        libs_for_storage = dict()
        library_names = [_lib_name(rows) for rows in BasicFunctions.num_rows]
        self.dfs = {rows: generate_pseudo_random_dataframe(rows) for rows in BasicFunctions.num_rows}

        for storage in STORAGES:
            libraries = create_libraries(storage, library_names)
            libs_for_storage[storage] = dict(zip(library_names, libraries))

        for rows in BasicFunctions.num_rows:
            df = self.dfs[rows]
            lib_name = _lib_name(rows)
            for storage in STORAGES:
                lib = libs_for_storage[storage][lib_name]
                if not lib:
                    continue
                lib.write("sym", df)

        return libs_for_storage

    def setup(self, libs_for_storage, rows, use_query_stats, storage):
        self.lib = libs_for_storage[storage][_lib_name(rows)]
        if self.lib is None:
            raise SkipNotImplemented

        if use_query_stats:
            if storage != Storage.AMAZON:
                raise SkipNotImplemented("Query stats only supported against S3")
            qs.enable()

        self.df = generate_pseudo_random_dataframe(rows)

    def teardown(self, *args):
        qs.disable()
        qs.reset_stats()

    def time_write(self, *args):
        self.lib.write(f"time_write_sym", self.df)

    def peakmem_write(self, *args):
        self.lib.write(f"peakmem_write_sym", self.df)

    def time_read(self, *args):
        self.lib.read(f"sym").data

    def peakmem_read(self, *args):
        self.lib.read(f"sym").data

    def time_read_with_columns(self, *args):
        COLS = ["value"]
        self.lib.read(f"sym", columns=COLS).data

    def peakmem_read_with_columns(self, *args):
        COLS = ["value"]
        self.lib.read(f"sym", columns=COLS).data

    def time_read_with_date_ranges(self, *args):
        self.lib.read(f"sym", date_range=DATE_RANGE).data

    def peakmem_read_with_date_ranges(self, *args):
        self.lib.read(f"sym", date_range=DATE_RANGE).data

    def time_read_with_date_ranges_query_builder(self, *args):
        q = QueryBuilder().date_range(DATE_RANGE)
        self.lib.read(f"sym", query_builder=q).data

    def peakmem_read_with_date_ranges_query_builder(self, *args):
        q = QueryBuilder().date_range(DATE_RANGE)
        self.lib.read(f"sym", query_builder=q).data

    def time_write_staged(self, *args):
        self.lib.write(f"time_write_staged", self.df, staged=True)

    def peakmem_write_staged(self, *args):
        self.lib.write(f"peakmem_write_staged", self.df, staged=True)


class ShortWideWrite:
    rows = [1, 5_000]
    cols = [30_000]

    params = [rows, cols, STORAGES]
    param_names = ["rows", "cols", "storages"]

    def __init__(self):
        self.logger = get_logger()
        self.lib = None
        self.df = None

    def setup_cache(self):
        lib_for_storage = create_libraries_across_storages(STORAGES)
        return lib_for_storage

    def setup(self, lib_for_storage, rows, cols, storage):
        self.lib = lib_for_storage[storage]
        if self.lib is None:
            raise SkipNotImplemented
        self.df = generate_random_floats_dataframe(rows, cols)

    def time_write(self, *args):
        self.lib.write("sym", self.df)

    def peakmem_write(self, *args):
        self.lib.write("sym", self.df)

    def time_write_staged(self, *args):
        self.lib.write(f"time_write_staged", self.df, staged=True)

    def peakmem_write_staged(self, *args):
        self.lib.write(f"peakmem_write_staged", self.df, staged=True)


class ShortWideRead:
    rows = [1, 5_000]
    COLS = 30_000

    params = [rows, STORAGES]
    param_names = ["num_rows", "storage"]

    def __init__(self):
        self.logger = get_logger()
        self.lib = None
        self.dfs = None

    def setup_cache(self):
        libs_for_storage = dict()
        library_names = [_lib_name(rows) for rows in ShortWideRead.rows]
        self.dfs = {rows: generate_pseudo_random_dataframe(rows) for rows in ShortWideRead.rows}

        for storage in STORAGES:
            libraries = create_libraries(storage, library_names)
            libs_for_storage[storage] = dict(zip(library_names, libraries))

        for rows in ShortWideRead.rows:
            df = self.dfs[rows]
            lib_name = _lib_name(rows)
            for storage in STORAGES:
                lib = libs_for_storage[storage][lib_name]
                if not lib:
                    continue
                lib.write("sym", df)

        return libs_for_storage

    def setup(self, libs_for_storage, rows, storage):
        self.lib = libs_for_storage[storage][_lib_name(rows)]
        if self.lib is None:
            raise SkipNotImplemented

    def time_read(self, *args):
        self.lib.read(f"sym")

    def peakmem_read(self, *args):
        self.lib.read(f"sym")


class BatchWrite:
    rounds = 1
    sample_time = 0.1

    num_rows = [25_000, 50_000]
    num_symbols = [100, 200]

    params = [num_rows, num_symbols, STORAGES]
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
        lib_for_storage = create_libraries_across_storages(STORAGES)
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

    params = [num_rows, num_symbols, STORAGES]
    param_names = ["num_rows", "num_symbols", "storage"]

    def __init__(self):
        self.logger = get_logger()

    def setup_cache(self):
        start = time.time()
        lib_for_storage = self._setup_cache()
        self.logger.info(f"SETUP_CACHE TIME: {time.time() - start}")
        return lib_for_storage

    def _setup_cache(self):
        libs_for_storage = dict()
        library_names = [_lib_name(rows) for rows in BatchFunctions.num_rows]
        self.dfs = {rows: generate_pseudo_random_dataframe(rows) for rows in BatchFunctions.num_rows}

        for storage in STORAGES:
            libraries = create_libraries(storage, library_names)
            libs_for_storage[storage] = dict(zip(library_names, libraries))

        for rows in BatchFunctions.num_rows:
            num_syms = BatchFunctions.num_symbols[-1]
            df = self.dfs[rows]
            lib_name = _lib_name(rows)
            write_payloads = [WritePayload(f"{i}_sym", df) for i in range(num_syms)]
            for storage in STORAGES:
                lib = libs_for_storage[storage][lib_name]
                if not lib:
                    continue
                lib.write_batch(write_payloads)

        return libs_for_storage

    def setup(self, libs_for_storage, rows, num_symbols, storage):
        self.lib = libs_for_storage[storage][_lib_name(rows)]
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
