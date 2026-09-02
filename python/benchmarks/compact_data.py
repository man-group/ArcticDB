"""
Copyright 2026 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

import time

import numpy as np
import pandas as pd
import random

from arcticdb import Arctic, LibraryOptions, WritePayload
from arcticdb.util.logger import get_logger
from arcticdb.util.test import random_strings_of_length

from benchmarks.common import lib_name
from benchmarks.seaweed_utils import SeaweedClient

random.seed(42)
rng = np.random.default_rng(42)

CACHE_BUCKET = "arcticdb-compact-cache"
WORK_BUCKET = "arcticdb-compact-work"


class CompactDataBase:
    def __init__(self):
        self.logger = get_logger()
        self.SYM = "sym"
        # Do not interleave benchmarks as they all share the same work bucket while actually running the benchmarks
        self.rounds = 1
        # These two parameters are important, because compaction is a destructive process, we must call setup before
        # each measurement
        self.number = 1
        self.warmup_time = 0
        # Each derived benchmark class takes less than 2 minutes total against the local SeaweedFS server
        self.repeat = 15
        self.timeout = 600
        self.ac = None
        self.lib = None
        self.seaweed = SeaweedClient()
        self.base_param_names = [
            "(num_rows, initial_rows_per_segment, target_rows_per_segment)",
            "num_columns",
            "column_slicing",
        ]

    def _reset_cache_storage(self):
        self.seaweed.reset_bucket(CACHE_BUCKET)

    def _cache_arctic(self, lib_name):
        return Arctic(self.seaweed.arctic_uri(CACHE_BUCKET, lib_name))

    def _setup_cache_base(self, lib_name, rows_per_segment, columns_per_segment, dfs):
        lib = self._cache_arctic(lib_name).create_library(
            lib_name,
            LibraryOptions(
                dynamic_schema=self.DYNAMIC_SCHEMA,
                rows_per_segment=rows_per_segment,
                columns_per_segment=columns_per_segment,
            ),
        )
        for df in dfs:
            lib.append(self.SYM, df)

    def _storage_setup(self, lib_name):
        # Create a new Arctic instance, otherwise we will be holding a reference to the previous iteration's
        # bucket and the deletion and recreation won't be noticed by Arctic
        del self.ac
        self.seaweed.reset_bucket(WORK_BUCKET)
        # Copy the config library and the relevant data library for these benchmark parameters to the work
        # bucket where compaction will happen; both live under the lib_name prefix
        self.seaweed.copy_bucket(CACHE_BUCKET, WORK_BUCKET, prefixes=[f"{lib_name}/"])
        self.ac = Arctic(self.seaweed.arctic_uri(WORK_BUCKET, lib_name))
        self.lib = self.ac.get_library(lib_name)

    def _setup(self, lib_name, target_rows_per_segment):
        self._storage_setup(lib_name)
        # Check the compaction will actually do something!
        assert self.lib.compact_data_explain_plan(self.SYM, rows_per_segment=target_rows_per_segment).will_do_work
        # read the symbol to warm up the cache
        self.lib.read(self.SYM)

    def _teardown(self):
        self.seaweed.delete_bucket(WORK_BUCKET)

    def compact_data(self, target_rows_per_segment):
        # Prune previous disabled so we are not also measuring memory/time for the deletion step
        self.lib.compact_data(self.SYM, rows_per_segment=target_rows_per_segment, prune_previous_versions=False)


class CompactDataNumericStaticSchema(CompactDataBase):
    def __init__(self):
        super().__init__()
        self.DYNAMIC_SCHEMA = False
        self.param_names = self.base_param_names
        self.params = [
            [
                (1_000_000, 10_000, 100_000),
                (100_000, 100_000, 10_000),
            ],  # (num_rows, initial_rows_per_segment, target_rows_per_segment)
            [2, 10, 100],  # num_columns
            [False, True],  # column_slicing
        ]

    def setup_cache(self):
        start = time.time()
        self._setup_cache()
        self.logger.info(f"SETUP_CACHE TIME: {time.time() - start}")

    def _setup_cache(self):
        # Populate the cache bucket only once
        self._reset_cache_storage()
        for row_params in self.params[0]:
            num_rows, initial_rows_per_segment, _ = row_params
            for num_columns in self.params[1]:
                for column_slicing in self.params[2]:
                    # Create one library per combination of benchmark parameters, as they don't all use the same slicing
                    df = pd.DataFrame(
                        {f"col_{i}": np.arange(i * num_rows, (i + 1) * num_rows) for i in range(num_columns)}
                    )
                    self._setup_cache_base(
                        lib_name(*row_params, num_columns, column_slicing),
                        initial_rows_per_segment,
                        num_columns // 2 if column_slicing else num_columns * 2,
                        [df],
                    )

    def setup(self, row_params, num_columns, column_slicing):
        self._setup(lib_name(*row_params, num_columns, column_slicing), row_params[2])

    def teardown(self, row_params, num_columns, column_slicing):
        self._teardown()

    def time_compact_data(self, row_params, num_columns, column_slicing):
        self.compact_data(row_params[2])

    def peakmem_compact_data(self, row_params, num_columns, column_slicing):
        self.compact_data(row_params[2])


class CompactDataStringsStaticSchema(CompactDataBase):
    def __init__(self):
        super().__init__()
        self.DYNAMIC_SCHEMA = False
        self.param_names = self.base_param_names + ["num_unique_strings"]
        self.params = [
            [
                (1_000_000, 10_000, 100_000),
                (100_000, 100_000, 10_000),
            ],  # (num_rows, initial_rows_per_segment, target_rows_per_segment)
            [2, 10],  # num_columns
            [False, True],  # column_slicing
            [2, 10, 100_000],  # num_unique_strings
        ]
        self.unique_strings = random_strings_of_length(max(self.params[3]), length=10, unique=True, kind="ascii")

    def setup_cache(self):
        start = time.time()
        self._setup_cache()
        self.logger.info(f"SETUP_CACHE TIME: {time.time() - start}")

    def _setup_cache(self):
        # Populate the cache bucket only once
        self._reset_cache_storage()
        for row_params in self.params[0]:
            num_rows, initial_rows_per_segment, _ = row_params
            for num_columns in self.params[1]:
                for column_slicing in self.params[2]:
                    for num_unique_strings in self.params[3]:
                        # Create one library per combination of benchmark parameters, as they don't all use the same
                        # slicing
                        strings = self.unique_strings[:num_unique_strings]
                        df = pd.DataFrame({f"col_{i}": rng.choice(strings, num_rows) for i in range(num_columns)})
                        self._setup_cache_base(
                            lib_name(*row_params, num_columns, column_slicing, num_unique_strings),
                            initial_rows_per_segment,
                            num_columns // 2 if column_slicing else num_columns * 2,
                            [df],
                        )

    def setup(self, row_params, num_columns, column_slicing, num_unique_strings):
        self._setup(lib_name(*row_params, num_columns, column_slicing, num_unique_strings), row_params[2])

    def teardown(self, row_params, num_columns, column_slicing, num_unique_strings):
        self._teardown()

    def time_compact_data(self, row_params, num_columns, column_slicing, num_unique_strings):
        self.compact_data(row_params[2])

    def peakmem_compact_data(self, row_params, num_columns, column_slicing, num_unique_strings):
        self.compact_data(row_params[2])


class CompactDataNumericDynamicSchema(CompactDataBase):
    def __init__(self):
        super().__init__()
        self.DYNAMIC_SCHEMA = True
        self.param_names = self.base_param_names[:2]
        self.params = [
            [
                (1_000, 10, 1_000),
            ],  # (num_rows, initial_rows_per_segment, target_rows_per_segment)
            [100, 1_000, 10_000],  # num_columns
        ]

    def setup_cache(self):
        start = time.time()
        self._setup_cache()
        self.logger.info(f"SETUP_CACHE TIME: {time.time() - start}")

    def _setup_cache(self):
        # Populate the cache bucket only once
        self._reset_cache_storage()
        for row_params in self.params[0]:
            num_rows, initial_rows_per_segment, _ = row_params
            for num_columns in self.params[1]:
                # Create one library per combination of benchmark parameters, as they don't all use the same slicing
                num_row_slices = num_rows // initial_rows_per_segment
                column_names = [f"col_{idx}" for idx in range(num_columns)]
                dfs = []
                for _ in range(num_row_slices):
                    columns = rng.choice(column_names, num_columns // 2, replace=False)
                    dfs.append(pd.DataFrame({column: np.arange(initial_rows_per_segment) for column in columns}))
                self._setup_cache_base(
                    lib_name(*row_params, num_columns),
                    initial_rows_per_segment,
                    0,  # Column slicing doesn't apply to dynamic schema
                    dfs,
                )

    def setup(self, row_params, num_columns):
        self._setup(lib_name(*row_params, num_columns), row_params[2])

    def teardown(self, row_params, num_columns):
        self._teardown()

    def time_compact_data(self, row_params, num_columns):
        self.compact_data(row_params[2])

    def peakmem_compact_data(self, row_params, num_columns):
        self.compact_data(row_params[2])


class AppendCompactDataBase:
    def __init__(self):
        self.logger = get_logger()
        # Do not interleave benchmarks as they all share the same work bucket while actually running the benchmarks
        self.rounds = 1
        # These two parameters are important, because appending with compact_data=True is a destructive process, we must
        # call setup before each measurement
        self.number = 1
        self.warmup_time = 0
        # Total runtime of the 3 derived benchmark classes is ~15m against the local SeaweedFS server
        self.repeat = 15
        self.timeout = 600
        self.ac = None
        self.lib = None
        self.seaweed = SeaweedClient()
        self.base_param_names = ["num_symbols", "existing_data_fragmented", "append_rows"]

    def finish_init(self):
        self.SYMS = [f"sym_{i}" for i in range(self.params[0][-1])]

    def _reset_cache_storage(self):
        self.seaweed.reset_bucket(CACHE_BUCKET)

    def _cache_arctic(self, lib_name):
        return Arctic(self.seaweed.arctic_uri(CACHE_BUCKET, lib_name))

    def _setup_cache_base(self, lib_name, dfs):
        lib = self._cache_arctic(lib_name).create_library(lib_name, LibraryOptions(dynamic_schema=self.DYNAMIC_SCHEMA))
        for df in dfs:
            lib.append_batch([WritePayload(sym, df) for sym in self.SYMS])

    def _setup(self, lib_name):
        # Create a new Arctic instance, otherwise we will be holding a reference to the previous iteration's
        # bucket and the deletion and recreation won't be noticed by Arctic
        del self.ac
        self.seaweed.reset_bucket(WORK_BUCKET)
        self.seaweed.copy_bucket(CACHE_BUCKET, WORK_BUCKET, prefixes=[f"{lib_name}/"])
        self.ac = Arctic(self.seaweed.arctic_uri(WORK_BUCKET, lib_name))
        self.lib = self.ac.get_library(lib_name)

    def _teardown(self):
        self.seaweed.delete_bucket(WORK_BUCKET)

    def append(self, num_symbols):
        # Prune previous disabled so we are not also measuring memory/time for the deletion step
        if num_symbols == 1:
            self.lib.append(self.SYMS[0], self.append_df, prune_previous_versions=False, compact_data=True)
        else:
            self.lib.append_batch(
                [WritePayload(sym, self.append_df) for sym in self.SYMS[:num_symbols]],
                prune_previous_versions=False,
                compact_data=True,
            )


class AppendCompactDataNumericStaticSchema(AppendCompactDataBase):
    def __init__(self):
        super().__init__()
        self.DYNAMIC_SCHEMA = False
        self.NUM_COLUMNS = 10
        self.param_names = self.base_param_names
        self.params = [
            [1, 10],  # num_symbols
            [False, True],  # existing_data_fragmented
            [1, 50_000, 1_000_000],  # append_rows
        ]
        super().finish_init()

    def setup_cache(self):
        start = time.time()
        self._setup_cache()
        self.logger.info(f"SETUP_CACHE TIME: {time.time() - start}")

    def _setup_cache(self):
        # Populate the cache bucket only once
        self._reset_cache_storage()
        num_rows = 1_000_000
        df = pd.DataFrame({f"col_{i}": np.arange(i * num_rows, (i + 1) * num_rows) for i in range(self.NUM_COLUMNS)})
        for existing_data_fragmented in self.params[1]:
            if existing_data_fragmented:
                dfs = [df[i * 10_000 : (i + 1) * 10_000] for i in range(num_rows // 10_000)]
            else:
                dfs = [df]
            self._setup_cache_base(lib_name(existing_data_fragmented), dfs)

    def setup(self, num_symbols, existing_data_fragmented, append_rows):
        self.append_df = pd.DataFrame(
            {f"col_{i}": np.arange(i * append_rows, (i + 1) * append_rows) for i in range(self.NUM_COLUMNS)}
        )
        self._setup(lib_name(existing_data_fragmented))

    def teardown(self, num_symbols, existing_data_fragmented, append_rows):
        self._teardown()

    def time_append_compact_data(self, num_symbols, existing_data_fragmented, append_rows):
        self.append(num_symbols)

    def peakmem_append_compact_data(self, num_symbols, existing_data_fragmented, append_rows):
        self.append(num_symbols)


class AppendCompactDataStringsStaticSchema(AppendCompactDataBase):
    def __init__(self):
        super().__init__()
        self.DYNAMIC_SCHEMA = False
        self.NUM_COLUMNS = 10
        self.param_names = self.base_param_names
        self.params = [
            [1, 10],  # num_symbols
            [False, True],  # existing_data_fragmented
            [1, 50_000, 1_000_000],  # append_rows
        ]
        super().finish_init()
        self.unique_strings = random_strings_of_length(10, length=10, unique=True, kind="ascii")

    def setup_cache(self):
        start = time.time()
        self._setup_cache()
        self.logger.info(f"SETUP_CACHE TIME: {time.time() - start}")

    def _setup_cache(self):
        # Populate the cache bucket only once
        self._reset_cache_storage()
        num_rows = 1_000_000
        df = pd.DataFrame({f"col_{i}": rng.choice(self.unique_strings, num_rows) for i in range(self.NUM_COLUMNS)})
        for existing_data_fragmented in self.params[1]:
            if existing_data_fragmented:
                dfs = [df[i * 10_000 : (i + 1) * 10_000] for i in range(num_rows // 10_000)]
            else:
                dfs = [df]
            self._setup_cache_base(lib_name(existing_data_fragmented), dfs)

    def setup(self, num_symbols, existing_data_fragmented, append_rows):
        self.append_df = pd.DataFrame(
            {f"col_{i}": rng.choice(self.unique_strings, append_rows) for i in range(self.NUM_COLUMNS)}
        )
        self._setup(lib_name(existing_data_fragmented))

    def teardown(self, num_symbols, existing_data_fragmented, append_rows):
        self._teardown()

    def time_append_compact_data(self, num_symbols, existing_data_fragmented, append_rows):
        self.append(num_symbols)

    def peakmem_append_compact_data(self, num_symbols, existing_data_fragmented, append_rows):
        self.append(num_symbols)


class AppendCompactDataNumericDynamicSchema(AppendCompactDataBase):
    def __init__(self):
        super().__init__()
        self.DYNAMIC_SCHEMA = True
        self.NUM_COLUMNS = 10_000
        self.COLUMN_NAMES = [f"col_{idx}" for idx in range(self.NUM_COLUMNS)]
        self.param_names = self.base_param_names
        self.params = [
            [1, 10],  # num_symbols
            [False, True],  # existing_data_fragmented
            [1, 1_000],  # append_rows
        ]
        super().finish_init()

    def setup_cache(self):
        start = time.time()
        self._setup_cache()
        self.logger.info(f"SETUP_CACHE TIME: {time.time() - start}")

    def _setup_cache(self):
        # Populate the cache bucket only once
        self._reset_cache_storage()
        num_rows = 1_000
        for existing_data_fragmented in self.params[1]:
            num_row_slices = 10 if existing_data_fragmented else 1

            dfs = []
            for _ in range(num_row_slices):
                columns = rng.choice(self.COLUMN_NAMES, self.NUM_COLUMNS // 2, replace=False)
                dfs.append(pd.DataFrame({column: np.arange(num_rows // num_row_slices) for column in columns}))
            self._setup_cache_base(lib_name(existing_data_fragmented), dfs)

    def setup(self, num_symbols, existing_data_fragmented, append_rows):
        columns = rng.choice(self.COLUMN_NAMES, self.NUM_COLUMNS // 2, replace=False)
        self.append_df = pd.DataFrame({column: np.arange(append_rows) for column in columns})
        self._setup(lib_name(existing_data_fragmented))

    def teardown(self, num_symbols, existing_data_fragmented, append_rows):
        self._teardown()

    def time_append_compact_data(self, num_symbols, existing_data_fragmented, append_rows):
        self.append(num_symbols)

    def peakmem_append_compact_data(self, num_symbols, existing_data_fragmented, append_rows):
        self.append(num_symbols)
