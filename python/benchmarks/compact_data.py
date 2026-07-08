"""
Copyright 2026 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

import os
import shutil
import time

import numpy as np
import pandas as pd
import random

from arcticdb import Arctic, LibraryOptions
from arcticdb.options import ModifiableLibraryOption
from arcticdb.util.logger import get_logger
from arcticdb.util.test import random_strings_of_length

random.seed(42)
rng = np.random.default_rng(42)


class CompactDataBase:
    def __init__(self):
        self.logger = get_logger()
        self.SYM = "sym"
        # Do not interleave benchmarks as they are using the same LMDB directory for actually running the benchmarks
        self.rounds = 1
        # These two parameters are important, because compaction is a destructive process, we must call setup before
        # each measurement
        self.number = 1
        self.warmup_time = 0
        # Each derived benchmark class takes less than 2 minutes total locally on an SSD
        self.repeat = 15
        self.ac = None
        self.lib = None
        self.base_param_names = [
            "(num_rows, initial_rows_per_segment, target_rows_per_segment)",
            "num_columns",
            "column_slicing",
        ]

    def finish_init(self):
        # Base LMDB instance that will be populated by setup_cache. Relevant libraries will then be copied from here
        # to another directory for actual compaction
        self.LMDB_BASE_DIR = f"{self.LMDB_DIR}_base"
        self.CONNECTION_STRING_BASE = f"lmdb://{self.LMDB_BASE_DIR}"
        self.CONNECTION_STRING = f"lmdb://{self.LMDB_DIR}"

    def lib_name(self, *args):
        return "_".join(f"{arg}" for arg in args)

    def _setup_cache_base(self, ac, lib_name, rows_per_segment, columns_per_segment, dfs):
        ac.delete_library(lib_name)
        lib = ac.create_library(
            lib_name,
            LibraryOptions(
                dynamic_schema=self.DYNAMIC_SCHEMA,
                rows_per_segment=rows_per_segment,
                columns_per_segment=columns_per_segment,
            ),
        )
        for df in dfs:
            lib.append(self.SYM, df)

    def _setup(self, lib_name, target_rows_per_segment):
        os.mkdir(self.LMDB_DIR)
        # Copy the config database and the relevant library database for these benchmark parameters to the actual
        # LMDB directory where compaction will happen
        shutil.copytree(os.path.join(self.LMDB_BASE_DIR, "_arctic_cfg"), os.path.join(self.LMDB_DIR, "_arctic_cfg"))
        shutil.copytree(os.path.join(self.LMDB_BASE_DIR, lib_name), os.path.join(self.LMDB_DIR, lib_name))
        # Create a new Arctic instance, otherwise we will be holding a reference to the previous iteration's .mdb files
        # and the deletion and recreation won't be noticed by Arctic
        del self.ac
        self.ac = Arctic(self.CONNECTION_STRING)
        self.lib = self.ac.get_library(lib_name)
        # Check the compaction will actually do something!
        assert self.lib.compact_data_explain_plan(self.SYM, rows_per_segment=target_rows_per_segment).will_do_work
        # read the symbol to warm up the cache
        self.lib.read(self.SYM)

    def _teardown(self):
        shutil.rmtree(self.LMDB_DIR)

    def compact_data(self, target_rows_per_segment):
        # Prune previous disabled so we are not also measuring memory/time for the deletion step
        self.lib.compact_data(self.SYM, rows_per_segment=target_rows_per_segment, prune_previous_versions=False)


class CompactDataNumericStaticSchema(CompactDataBase):
    def __init__(self):
        super().__init__()
        self.DYNAMIC_SCHEMA = False
        # Directory that will contain libraries that are actually compacted
        self.LMDB_DIR = "compact_data_numeric_static_schema"
        super().finish_init()
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
        # Populate the base libraries only once
        ac = Arctic(self.CONNECTION_STRING_BASE)
        for row_params in self.params[0]:
            num_rows, initial_rows_per_segment, _ = row_params
            for num_columns in self.params[1]:
                for column_slicing in self.params[2]:
                    # Create one library per combination of benchmark parameters, as they don't all use the same slicing
                    df = pd.DataFrame(
                        {f"col_{i}": np.arange(i * num_rows, (i + 1) * num_rows) for i in range(num_columns)}
                    )
                    self._setup_cache_base(
                        ac,
                        self.lib_name(row_params, num_columns, column_slicing),
                        initial_rows_per_segment,
                        num_columns // 2 if column_slicing else num_columns * 2,
                        [df],
                    )

    def setup(self, row_params, num_columns, column_slicing):
        self._setup(self.lib_name(row_params, num_columns, column_slicing), row_params[2])

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
        # Directory that will contain libraries that are actually compacted
        self.LMDB_DIR = "compact_data_strings_static_schema"
        super().finish_init()
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
        # Populate the base libraries only once
        ac = Arctic(self.CONNECTION_STRING_BASE)
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
                            ac,
                            self.lib_name(row_params, num_columns, column_slicing, num_unique_strings),
                            initial_rows_per_segment,
                            num_columns // 2 if column_slicing else num_columns * 2,
                            [df],
                        )

    def setup(self, row_params, num_columns, column_slicing, num_unique_strings):
        self._setup(self.lib_name(row_params, num_columns, column_slicing, num_unique_strings), row_params[2])

    def teardown(self, row_params, num_columns, column_slicing, num_unique_strings):
        self._teardown()

    def time_compact_data(self, row_params, num_columns, column_slicing, num_unique_strings):
        self.compact_data(row_params[2])

    def peakmem_compact_data(self, row_params, num_columns, column_slicing, num_unique_strings):
        target_rows_per_segment = row_params[2]
        self.compact_data(row_params[2])


class CompactDataNumericDynamicSchema(CompactDataBase):
    def __init__(self):
        super().__init__()
        self.DYNAMIC_SCHEMA = True
        # Directory that will contain libraries that are actually compacted
        self.LMDB_DIR = "compact_data_numeric_dynamic_schema"
        super().finish_init()
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
        # Populate the base libraries only once
        ac = Arctic(self.CONNECTION_STRING_BASE)
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
                    ac,
                    self.lib_name(row_params, num_columns),
                    initial_rows_per_segment,
                    0,  # Column slicing doesn't apply to dynamic schema
                    dfs,
                )

    def setup(self, row_params, num_columns):
        self._setup(self.lib_name(row_params, num_columns), row_params[2])

    def teardown(self, row_params, num_columns):
        self._teardown()

    def time_compact_data(self, row_params, num_columns):
        self.compact_data(row_params[2])

    def peakmem_compact_data(self, row_params, num_columns):
        self.compact_data(row_params[2])
