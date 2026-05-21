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

from arcticdb import Arctic, LibraryOptions
from arcticdb.options import ModifiableLibraryOption
from arcticdb.util.logger import get_logger
from arcticdb.util.test import random_strings_of_length

rng = np.random.default_rng()


class CompactDataNumericStaticSchema:
    def __init__(self):
        self.logger = get_logger()
        self.SYM = "sym"
        # Do not interleave benchmarks as they are using the same LMDB directory for actually running the benchmarks
        self.rounds = 1
        # Takes around 2 minutes total locally on an SSD
        self.repeat = 15
        # These two parameters are important, because compaction is a destructive process, we must call setup before
        # each measurement
        self.number = 1
        self.warmup_time = 0
        # Directory that will contain libraries that are actually compacted
        self.LMDB_DIR = "compact_data_numeric_static_schema"
        # Base LMDB instance that will be populated by setup_cache. Relevant libraries will then be copied from here
        # to another directory for actual compaction
        self.LMDB_BASE_DIR = f"{self.LMDB_DIR}_base"
        self.CONNECTION_STRING_BASE = f"lmdb://{self.LMDB_BASE_DIR}"
        self.CONNECTION_STRING = f"lmdb://{self.LMDB_DIR}"
        self.param_names = [
            "(num_rows, initial_rows_per_segment, target_rows_per_segment)",
            "num_columns",
            "column_slicing",
        ]
        self.params = [
            [
                (1_000_000, 10_000, 100_000),
                (100_000, 100_000, 10_000),
            ],  # (num_rows, initial_rows_per_segment, target_rows_per_segment)
            [2, 10, 100],  # num_columns
            [False, True],  # column_slicing
        ]
        self.ac = None
        self.lib = None

    def setup_cache(self):
        start = time.time()
        self._setup_cache()
        self.logger.info(f"SETUP_CACHE TIME: {time.time() - start}")

    def _setup_cache(self):
        # Populate the base libraries only once
        ac = Arctic(self.CONNECTION_STRING_BASE)
        for row_params in self.params[0]:
            num_rows, initial_rows_per_segment, target_rows_per_segment = row_params
            for num_columns in self.params[1]:
                for column_slicing in self.params[2]:
                    lib_name = self.lib_name(row_params, num_columns, column_slicing)
                    ac.delete_library(lib_name)
                    # Create one library per combination of benchmark parameters, as they don't all use the same slicing
                    lib = ac.create_library(
                        lib_name,
                        LibraryOptions(
                            rows_per_segment=initial_rows_per_segment,
                            columns_per_segment=num_columns // 2 if column_slicing else num_columns * 2,
                        ),
                    )
                    df = pd.DataFrame(
                        {f"col_{i}": np.arange(i * num_rows, (i + 1) * num_rows) for i in range(num_columns)}
                    )
                    lib.write(self.SYM, df)

    def lib_name(self, row_params, num_columns, column_slicing):
        return f"{row_params}_{num_columns}_{column_slicing}"

    def setup(self, row_params, num_columns, column_slicing):
        os.mkdir(self.LMDB_DIR)
        # Copy the config database and the relevant library database for these benchmark parameters to the actual
        # LMDB directory where compaction will happen
        shutil.copytree(os.path.join(self.LMDB_BASE_DIR, "_arctic_cfg"), os.path.join(self.LMDB_DIR, "_arctic_cfg"))
        lib_name = self.lib_name(row_params, num_columns, column_slicing)
        shutil.copytree(os.path.join(self.LMDB_BASE_DIR, lib_name), os.path.join(self.LMDB_DIR, lib_name))
        # Create a new Arctic instance, otherwise we will be holding a reference to the previous iteration's .mdb files
        # and the deletion and recreation won't be noticed by Arctic
        del self.ac
        self.ac = Arctic(self.CONNECTION_STRING)
        self.lib = self.ac.get_library(lib_name)
        # Check the compaction will actually do something!
        target_rows_per_segment = row_params[2]
        assert self.lib.compact_data_explain_plan(self.SYM, rows_per_segment=target_rows_per_segment).will_do_work

    def teardown(self, row_params, num_columns, column_slicing):
        shutil.rmtree(self.LMDB_DIR)

    def time_compact_data(self, row_params, num_columns, column_slicing):
        target_rows_per_segment = row_params[2]
        self.lib.compact_data(self.SYM, rows_per_segment=target_rows_per_segment, prune_previous_versions=False)

    def peakmem_compact_data(self, row_params, num_columns, column_slicing):
        target_rows_per_segment = row_params[2]
        self.lib.compact_data(self.SYM, rows_per_segment=target_rows_per_segment, prune_previous_versions=False)


class CompactDataStringsStaticSchema:
    def __init__(self):
        self.logger = get_logger()
        self.SYM = "sym"
        # Do not interleave benchmarks as they are using the same LMDB directory for actually running the benchmarks
        self.rounds = 1
        # Takes around 2 minutes total locally on an SSD
        self.repeat = 15
        # These two parameters are important, because compaction is a destructive process, we must call setup before
        # each measurement
        self.number = 1
        self.warmup_time = 0
        # Directory that will contain libraries that are actually compacted
        self.LMDB_DIR = "compact_data_strings_static_schema"
        # Base LMDB instance that will be populated by setup_cache. Relevant libraries will then be copied from here
        # to another directory for actual compaction
        self.LMDB_BASE_DIR = f"{self.LMDB_DIR}_base"
        self.CONNECTION_STRING_BASE = f"lmdb://{self.LMDB_BASE_DIR}"
        self.CONNECTION_STRING = f"lmdb://{self.LMDB_DIR}"
        self.param_names = [
            "(num_rows, initial_rows_per_segment, target_rows_per_segment)",
            "num_columns",
            "column_slicing",
            "num_unique_strings",
        ]
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
        self.ac = None
        self.lib = None

    def setup_cache(self):
        start = time.time()
        self._setup_cache()
        self.logger.info(f"SETUP_CACHE TIME: {time.time() - start}")

    def _setup_cache(self):
        # Populate the base libraries only once
        ac = Arctic(self.CONNECTION_STRING_BASE)
        for row_params in self.params[0]:
            num_rows, initial_rows_per_segment, target_rows_per_segment = row_params
            for num_columns in self.params[1]:
                for column_slicing in self.params[2]:
                    for num_unique_strings in self.params[3]:
                        lib_name = self.lib_name(row_params, num_columns, column_slicing, num_unique_strings)
                        ac.delete_library(lib_name)
                        # Create one library per combination of benchmark parameters, as they don't all use the same
                        # slicing
                        lib = ac.create_library(
                            lib_name,
                            LibraryOptions(
                                rows_per_segment=initial_rows_per_segment,
                                columns_per_segment=num_columns // 2 if column_slicing else num_columns * 2,
                            ),
                        )
                        df = pd.DataFrame(
                            {f"col_{i}": rng.choice(num_unique_strings, num_rows) for i in range(num_columns)}
                        )
                        lib.write(self.SYM, df)

    def lib_name(self, row_params, num_columns, column_slicing, num_unique_strings):
        return f"{row_params}_{num_columns}_{column_slicing}_{num_unique_strings}"

    def setup(self, row_params, num_columns, column_slicing, num_unique_strings):
        os.mkdir(self.LMDB_DIR)
        # Copy the config database and the relevant library database for these benchmark parameters to the actual
        # LMDB directory where compaction will happen
        shutil.copytree(os.path.join(self.LMDB_BASE_DIR, "_arctic_cfg"), os.path.join(self.LMDB_DIR, "_arctic_cfg"))
        lib_name = self.lib_name(row_params, num_columns, column_slicing, num_unique_strings)
        shutil.copytree(os.path.join(self.LMDB_BASE_DIR, lib_name), os.path.join(self.LMDB_DIR, lib_name))
        # Create a new Arctic instance, otherwise we will be holding a reference to the previous iteration's .mdb files
        # and the deletion and recreation won't be noticed by Arctic
        del self.ac
        self.ac = Arctic(self.CONNECTION_STRING)
        self.lib = self.ac.get_library(lib_name)
        # Check the compaction will actually do something!
        target_rows_per_segment = row_params[2]
        assert self.lib.compact_data_explain_plan(self.SYM, rows_per_segment=target_rows_per_segment).will_do_work

    def teardown(self, row_params, num_columns, column_slicing, num_unique_strings):
        shutil.rmtree(self.LMDB_DIR)

    def time_compact_data(self, row_params, num_columns, column_slicing, num_unique_strings):
        target_rows_per_segment = row_params[2]
        self.lib.compact_data(self.SYM, rows_per_segment=target_rows_per_segment, prune_previous_versions=False)

    def peakmem_compact_data(self, row_params, num_columns, column_slicing, num_unique_strings):
        target_rows_per_segment = row_params[2]
        self.lib.compact_data(self.SYM, rows_per_segment=target_rows_per_segment, prune_previous_versions=False)


class CompactDataNumericDynamicSchema:
    def __init__(self):
        self.logger = get_logger()
        self.SYM = "sym"
        # Do not interleave benchmarks as they are using the same LMDB directory for actually running the benchmarks
        self.rounds = 1
        # Takes around 75 seconds total locally on an SSD
        self.repeat = 15
        # These two parameters are important, because compaction is a destructive process, we must call setup before
        # each measurement
        self.number = 1
        self.warmup_time = 0
        # Directory that will contain libraries that are actually compacted
        self.LMDB_DIR = "compact_data_numeric_dynamic_schema"
        # Base LMDB instance that will be populated by setup_cache. Relevant libraries will then be copied from here
        # to another directory for actual compaction
        self.LMDB_BASE_DIR = f"{self.LMDB_DIR}_base"
        self.CONNECTION_STRING_BASE = f"lmdb://{self.LMDB_BASE_DIR}"
        self.CONNECTION_STRING = f"lmdb://{self.LMDB_DIR}"
        self.param_names = [
            "(num_rows, initial_rows_per_segment, target_rows_per_segment)",
            "num_columns",
        ]
        self.params = [
            [
                (1_000, 10, 1_000),
            ],  # (num_rows, initial_rows_per_segment, target_rows_per_segment)
            [100, 1_000, 10_000],  # num_columns
        ]
        self.ac = None
        self.lib = None

    def setup_cache(self):
        start = time.time()
        self._setup_cache()
        self.logger.info(f"SETUP_CACHE TIME: {time.time() - start}")

    def _setup_cache(self):
        # Populate the base libraries only once
        ac = Arctic(self.CONNECTION_STRING_BASE)
        for row_params in self.params[0]:
            num_rows, initial_rows_per_segment, target_rows_per_segment = row_params
            for num_columns in self.params[1]:
                lib_name = self.lib_name(row_params, num_columns)
                ac.delete_library(lib_name)
                # Create one library per combination of benchmark parameters, as they don't all use the same slicing
                lib = ac.create_library(
                    lib_name,
                    LibraryOptions(
                        dynamic_schema=True,
                        rows_per_segment=initial_rows_per_segment,
                    ),
                )
                num_row_slices = num_rows // initial_rows_per_segment
                column_names = [f"col_{idx}" for idx in range(num_columns)]
                for _ in range(num_row_slices):
                    columns = rng.choice(column_names, num_columns // 2, replace=False)
                    df = pd.DataFrame({column: np.arange(initial_rows_per_segment) for column in columns})
                    lib.append(self.SYM, df)

    def lib_name(self, row_params, num_columns):
        return f"{row_params}_{num_columns}"

    def setup(self, row_params, num_columns):
        os.mkdir(self.LMDB_DIR)
        # Copy the config database and the relevant library database for these benchmark parameters to the actual
        # LMDB directory where compaction will happen
        shutil.copytree(os.path.join(self.LMDB_BASE_DIR, "_arctic_cfg"), os.path.join(self.LMDB_DIR, "_arctic_cfg"))
        lib_name = self.lib_name(row_params, num_columns)
        shutil.copytree(os.path.join(self.LMDB_BASE_DIR, lib_name), os.path.join(self.LMDB_DIR, lib_name))
        # Create a new Arctic instance, otherwise we will be holding a reference to the previous iteration's .mdb files
        # and the deletion and recreation won't be noticed by Arctic
        del self.ac
        self.ac = Arctic(self.CONNECTION_STRING)
        self.lib = self.ac.get_library(lib_name)
        # Check the compaction will actually do something!
        target_rows_per_segment = row_params[2]
        assert self.lib.compact_data_explain_plan(self.SYM, rows_per_segment=target_rows_per_segment).will_do_work

    def teardown(self, row_params, num_columns):
        shutil.rmtree(self.LMDB_DIR)

    def time_compact_data(self, row_params, num_columns):
        target_rows_per_segment = row_params[2]
        self.lib.compact_data(self.SYM, rows_per_segment=target_rows_per_segment, prune_previous_versions=False)

    def peakmem_compact_data(self, row_params, num_columns):
        target_rows_per_segment = row_params[2]
        self.lib.compact_data(self.SYM, rows_per_segment=target_rows_per_segment, prune_previous_versions=False)
