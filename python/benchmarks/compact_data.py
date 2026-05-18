"""
Copyright 2026 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

import time

import numpy as np
import pandas as pd

from arcticdb import Arctic, LibraryOptions
from arcticdb.options import ModifiableLibraryOption
from arcticdb.util.logger import get_logger
from arcticdb.util.test import random_strings_of_length

rng = np.random.default_rng()


class CompactDataBase:
    def __init__(self):
        self.logger = get_logger()
        self.SYM = "sym"
        # This is important, because compaction is a destructive process, we must call setup before each measurement
        self.number = 1
        self.warmup_time = 0

        self.base_param_names = [
            "(num_rows, initial_rows_per_segment, target_rows_per_segment)",
            "num_columns",
            "column_slicing",
        ]

    def _setup_cache(self):
        ac = Arctic(self.CONNECTION_STRING)
        ac.delete_library(self.LIB_NAME)
        ac.create_library(self.LIB_NAME, LibraryOptions(dynamic_schema=self.DYNAMIC_SCHEMA))

    def teardown(self, *args):
        self.lib.delete(self.SYM)
        self.ac.modify_library_option(self.lib, ModifiableLibraryOption.ROWS_PER_SEGMENT, 100_000)
        self.ac.modify_library_option(self.lib, ModifiableLibraryOption.COLUMNS_PER_SEGMENT, 127)
        del self.lib
        del self.ac

    def _setup(self, row_params, num_columns, column_slicing, dfs):
        _, initial_rows_per_segment, target_rows_per_segment = row_params
        self.ac = Arctic(self.CONNECTION_STRING)
        self.lib = self.ac[self.LIB_NAME]
        self.ac.modify_library_option(self.lib, ModifiableLibraryOption.ROWS_PER_SEGMENT, initial_rows_per_segment)
        if column_slicing:
            self.ac.modify_library_option(self.lib, ModifiableLibraryOption.COLUMNS_PER_SEGMENT, num_columns // 2)
        self.lib.write(self.SYM, dfs[0])
        for df in dfs[1:]:
            self.lib.append(self.SYM, df)
        assert self.lib.compact_data_explain_plan_experimental(
            self.SYM, rows_per_segment=target_rows_per_segment
        ).will_do_work

    def _time_compact_data(self, target_rows_per_segment):
        self.lib.compact_data_experimental(
            self.SYM, rows_per_segment=target_rows_per_segment, prune_previous_versions=False
        )

    def _peakmem_compact_data(self, target_rows_per_segment):
        self.lib.compact_data_experimental(
            self.SYM, rows_per_segment=target_rows_per_segment, prune_previous_versions=False
        )


class CompactDataNumericStaticSchema(CompactDataBase):
    def __init__(self):
        super().__init__()
        # 50 iterations takes around 10s
        self.repeat = (50, 100, 30)
        self.LIB_NAME = "compact_data_numeric_static_schema"
        self.CONNECTION_STRING = "lmdb://compact_data_numeric_static_schema"
        self.DYNAMIC_SCHEMA = False
        self.param_names = self.base_param_names
        # Note that ColumnDataBase._setup will need modifying if num_columns>127 added for the non-column-sliced case
        self.params = [
            [
                (1_000_000, 10_000, 100_000),
                (100_000, 100_000, 10_000),
            ],  # (num_rows, initial_rows_per_segment, target_rows_per_segment)
            [2, 10, 100],  # num_columns
            [False, True],  # column_slicing
        ]

    # ASV's setup_cache discovery logic will only run the method once if it is in the base class, even if that base
    # class has multiple classes inheriting from it
    def setup_cache(self):
        start = time.time()
        self._setup_cache()
        self.logger.info(f"SETUP_CACHE TIME: {time.time() - start}")

    def setup(self, row_params, num_columns, column_slicing):
        num_rows = row_params[0]
        df = pd.DataFrame({f"col_{i}": np.arange(i * num_rows, (i + 1) * num_rows) for i in range(num_columns)})
        self._setup(row_params, num_columns, column_slicing, [df])

    def time_compact_data(self, row_params, num_columns, column_slicing):
        self._time_compact_data(row_params[2])

    def peakmem_compact_data(self, row_params, num_columns, column_slicing):
        self._peakmem_compact_data(row_params[2])


class CompactDataStringsStaticSchema(CompactDataBase):
    def __init__(self):
        super().__init__()
        # 5 iterations takes around 10s
        self.repeat = (5, 10, 30)
        self.LIB_NAME = "compact_data_strings_static_schema"
        self.CONNECTION_STRING = "lmdb://compact_data_strings_static_schema"
        self.DYNAMIC_SCHEMA = False
        self.param_names = self.base_param_names + ["num_unique_strings"]
        # Note that ColumnDataBase._setup will need modifying if num_columns>127 added for the non-column-sliced case
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

    # ASV's setup_cache discovery logic will only run the method once if it is in the base class, even if that base
    # class has multiple classes inheriting from it
    def setup_cache(self):
        start = time.time()
        self._setup_cache()
        self.logger.info(f"SETUP_CACHE TIME: {time.time() - start}")

    def setup(self, row_params, num_columns, column_slicing, num_unique_strings):
        num_rows = row_params[0]
        unique_strings = self.unique_strings[:num_unique_strings]
        df = pd.DataFrame({f"col_{i}": rng.choice(unique_strings, num_rows) for i in range(num_columns)})
        self._setup(row_params, num_columns, column_slicing, [df])

    def time_compact_data(self, row_params, num_columns, column_slicing, num_unique_strings):
        self._time_compact_data(row_params[2])

    def peakmem_compact_data(self, row_params, num_columns, column_slicing, num_unique_strings):
        self._peakmem_compact_data(row_params[2])


class CompactDataNumericDynamicSchema(CompactDataBase):
    def __init__(self):
        super().__init__()
        # 5 iterations takes around 340s
        self.repeat = (5, 10, 500)
        self.LIB_NAME = "compact_data_numeric_dynamic_schema"
        self.CONNECTION_STRING = "lmdb://compact_data_numeric_dynamic_schema"
        self.DYNAMIC_SCHEMA = True
        self.param_names = self.base_param_names[:2]
        self.params = [
            [
                (1_000, 10, 1_000),
            ],  # (num_rows, initial_rows_per_segment, target_rows_per_segment)
            [100, 1_000, 10_000],  # num_columns
        ]
        # The setup for this is much slower than static schema as we must call append repeatedly to produce row-slices
        # with missing columns
        self.timeout = 500

    # ASV's setup_cache discovery logic will only run the method once if it is in the base class, even if that base
    # class has multiple classes inheriting from it
    def setup_cache(self):
        start = time.time()
        self._setup_cache()
        self.logger.info(f"SETUP_CACHE TIME: {time.time() - start}")

    def setup(self, row_params, num_columns):
        num_rows, initial_rows_per_segment, _ = row_params
        num_row_slices = num_rows // initial_rows_per_segment
        column_names = [f"col_{idx}" for idx in range(num_columns)]
        dfs = []
        for _ in range(num_row_slices):
            columns = rng.choice(column_names, num_columns // 2, replace=False)
            dfs.append(pd.DataFrame({column: np.arange(initial_rows_per_segment) for column in columns}))
        self._setup(row_params, num_columns, False, dfs)

    def time_compact_data(self, row_params, num_columns):
        self._time_compact_data(row_params[2])

    def peakmem_compact_data(self, row_params, num_columns):
        self._peakmem_compact_data(row_params[2])
