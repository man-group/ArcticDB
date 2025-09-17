"""
Copyright 2025 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

import time
import numpy as np
import pandas as pd

from arcticdb import Arctic, OutputFormat
from arcticdb.util.logger import get_logger
from arcticdb.util.test import random_strings_of_length


class ArrowReadNumeric:
    number = 5
    warmup_time = 0
    timeout = 6000
    rounds = 1
    connection_string = "lmdb://arrow_read_numeric?map_size=20GB"
    lib_name = "arrow_read_numeric"
    params = ([100_000, 100_000_000], [None, "middle"])
    param_names = ["rows", "date_range"]

    def symbol_name(self, num_rows: int):
        return f"numeric_{num_rows}_rows"

    def __init__(self):
        self.logger = get_logger()

    def setup_cache(self):
        start = time.time()
        self._setup_cache()
        self.logger.info(f"SETUP_CACHE TIME: {time.time() - start}")

    def _setup_cache(self):
        self.ac = Arctic(self.connection_string, output_format=OutputFormat.EXPERIMENTAL_ARROW)
        num_rows, date_ranges = self.params
        num_cols = 9  # 10 including the index column
        self.ac.delete_library(self.lib_name)
        self.ac.create_library(self.lib_name)
        lib = self.ac.get_library(self.lib_name)
        for rows in num_rows:
            df = pd.DataFrame(
                {f"col{idx}": np.arange(idx * rows, (idx + 1) * rows, dtype=np.int64) for idx in range(num_cols)},
                index=pd.date_range("1970-01-01", freq="ns", periods=rows),
            )
            lib.write(self.symbol_name(rows), df)

    def teardown(self, rows, date_range):
        del self.ac

    def setup(self, rows, date_range):
        self.ac = Arctic(self.connection_string, output_format=OutputFormat.EXPERIMENTAL_ARROW)
        self.lib = self.ac.get_library(self.lib_name)
        if date_range is None:
            self.date_range = None
        else:
            # Create a date range that excludes the first and last 10 rows of the data only
            self.date_range = (pd.Timestamp(10), pd.Timestamp(rows - 10))

    def time_read(self, rows, date_range):
        self.lib.read(self.symbol_name(rows), date_range=self.date_range)

    def peakmem_read(self, rows, date_range):
        self.lib.read(self.symbol_name(rows), date_range=self.date_range)


class ArrowReadStrings:
    number = 5
    warmup_time = 0
    timeout = 6000
    rounds = 1
    connection_string = "lmdb://arrow_read_strings?map_size=20GB"
    lib_name = "arrow_read_strings"
    params = ([10_000, 1_000_000], [None, "middle"], [1, 100, 100_000])
    param_names = ["rows", "date_range", "unique_string_count"]

    def symbol_name(self, num_rows: int, unique_strings: int):
        return f"string_{num_rows}_rows_{unique_strings}_unique_strings"

    def __init__(self):
        self.logger = get_logger()

    def setup_cache(self):
        start = time.time()
        self._setup_cache()
        self.logger.info(f"SETUP_CACHE TIME: {time.time() - start}")

    def _setup_cache(self):
        rng = np.random.default_rng()
        self.ac = Arctic(self.connection_string, output_format=OutputFormat.EXPERIMENTAL_ARROW)
        num_rows, date_ranges, unique_string_counts = self.params
        num_cols = 10
        self.ac.delete_library(self.lib_name)
        self.ac.create_library(self.lib_name)
        lib = self.ac.get_library(self.lib_name)
        for unique_string_count in unique_string_counts:
            strings = np.array(random_strings_of_length(unique_string_count, 10, unique=True))
            for rows in num_rows:
                df = pd.DataFrame(
                    {f"col{idx}": rng.choice(strings, rows) for idx in range(num_cols)},
                    index=pd.date_range("1970-01-01", freq="ns", periods=rows),
                )
                lib.write(self.symbol_name(rows, unique_string_count), df)

    def teardown(self, rows, date_range, unique_string_count):
        del self.ac

    def setup(self, rows, date_range, unique_string_count):
        self.ac = Arctic(self.connection_string, output_format=OutputFormat.EXPERIMENTAL_ARROW)
        self.lib = self.ac.get_library(self.lib_name)
        if date_range is None:
            self.date_range = None
        else:
            # Create a date range that excludes the first and last 10 rows of the data only
            self.date_range = (pd.Timestamp(10), pd.Timestamp(rows - 10))

    def time_read(self, rows, date_range, unique_string_count):
        self.lib.read(self.symbol_name(rows, unique_string_count), date_range=self.date_range)

    def peakmem_read(self, rows, date_range, unique_string_count):
        self.lib.read(self.symbol_name(rows, unique_string_count), date_range=self.date_range)
