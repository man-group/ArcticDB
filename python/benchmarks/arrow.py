"""
Copyright 2026 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

import gc
import random
import time
import numpy as np
import pandas as pd

from arcticdb import Arctic, OutputFormat, ArrowOutputStringFormat
from arcticdb.dependencies import pyarrow as pa
from arcticdb.util.logger import get_logger
from arcticdb.util.test import random_strings_of_length
from asv_runner.benchmarks.mark import SkipNotImplemented

from benchmarks.common import generate_pseudo_random_dataframe


def _get_anon_rss_kb():
    """Get anonymous RSS in kB from /proc/self/status (Linux only).

    RssAnon excludes file-backed pages (e.g. LMDB mmap), isolating heap/stack memory.
    """
    try:
        with open("/proc/self/status") as f:
            for line in f:
                if line.startswith("RssAnon:"):
                    return int(line.split()[1])
    except FileNotFoundError:
        pass
    return 0


class ArrowNumeric:
    timeout = 600
    connection_string = "lmdb://arrow_numeric"
    lib_name_prewritten = "arrow_numeric_prewritten"
    lib_name_fresh = "arrow_numeric_fresh"
    params = ([1_000_000, 10_000_000], [None, "middle"])
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
        self.ac = Arctic(self.connection_string, output_format=OutputFormat.PYARROW)
        num_rows, date_ranges = self.params
        num_cols = 9  # 10 including the index column
        self.ac.delete_library(self.lib_name_prewritten)
        self.ac.create_library(self.lib_name_prewritten)
        lib = self.ac.get_library(self.lib_name_prewritten)
        lib._nvs._set_allow_arrow_input()
        for rows in num_rows:
            df = pd.DataFrame(
                {f"col{idx}": np.arange(idx * rows, (idx + 1) * rows, dtype=np.int64) for idx in range(num_cols)},
                index=pd.date_range("1970-01-01", freq="ns", periods=rows),
            )
            lib.write(self.symbol_name(rows), df)

    def teardown(self, rows, date_range):
        for lib in self.ac.list_libraries():
            if "prewritten" in lib:
                continue
            self.ac.delete_library(lib)
        del self.ac

    def setup(self, rows, date_range):
        np.random.seed(42)
        random.seed(42)
        self.ac = Arctic(self.connection_string, output_format=OutputFormat.PYARROW)
        self.lib = self.ac.get_library(self.lib_name_prewritten)
        self.lib._nvs._set_allow_arrow_input()
        if date_range is None:
            self.date_range = None
        else:
            # Create a date range that excludes the first and last 10 rows of the data only
            self.date_range = (pd.Timestamp(10), pd.Timestamp(rows - 10))
        self.fresh_lib = self.get_fresh_lib()
        self.fresh_lib._nvs._set_allow_arrow_input()
        self.table = pa.Table.from_pandas(generate_pseudo_random_dataframe(rows))

    def get_fresh_lib(self):
        self.ac.delete_library(self.lib_name_fresh)
        return self.ac.create_library(self.lib_name_fresh)

    def time_write(self, rows, date_range):
        self.fresh_lib.write(f"sym_{rows}", self.table, index_column="ts")

    def peakmem_write(self, rows, date_range):
        self.fresh_lib.write(f"sym_{rows}", self.table, index_column="ts")

    def time_read(self, rows, date_range):
        self.lib.read(self.symbol_name(rows), date_range=self.date_range)

    def peakmem_read(self, rows, date_range):
        self.lib.read(self.symbol_name(rows), date_range=self.date_range)


class ArrowStrings:
    timeout = 600
    connection_string = "lmdb://arrow_strings"
    lib_name_prewritten = "arrow_strings_prewritten"
    lib_name_fresh = "arrow_strings_fresh"
    params = ([100_000, 1_000_000], [None, "middle"], [100, 10_000], list(ArrowOutputStringFormat))
    param_names = ["rows", "date_range", "unique_string_count", "arrow_string_format"]
    num_cols = 10

    def symbol_name(self, num_rows: int, unique_strings: int):
        return f"string_{num_rows}_rows_{unique_strings}_unique_strings"

    def __init__(self):
        self.logger = get_logger()

    def setup_cache(self):
        start = time.time()
        self._setup_cache()
        self.logger.info(f"SETUP_CACHE TIME: {time.time() - start}")

    def _generate_table(self, num_rows, num_cols, unique_string_count):
        np.random.seed(42)
        random.seed(42)
        strings = np.array(random_strings_of_length(unique_string_count, 10, unique=True, kind="ascii"))
        names = ["ts"] + [f"col{idx}" for idx in range(num_cols)]
        index = pd.date_range("1970-01-01", freq="ns", periods=num_rows)
        return pa.Table.from_arrays(
            [index] + [np.random.choice(strings, num_rows) for _ in range(num_cols)], names=names
        )

    def _setup_cache(self):
        self.ac = Arctic(self.connection_string, output_format=OutputFormat.PYARROW)
        num_rows, date_ranges, unique_string_counts, arrow_string_format = self.params
        self.ac.delete_library(self.lib_name_prewritten)
        self.ac.create_library(self.lib_name_prewritten)
        lib = self.ac.get_library(self.lib_name_prewritten)
        lib._nvs._set_allow_arrow_input()
        for rows in num_rows:
            for unique_string_count in unique_string_counts:
                table = self._generate_table(rows, self.num_cols, unique_string_count)
                lib.write(self.symbol_name(rows, unique_string_count), table, index_column="ts")

    def teardown(self, rows, date_range, unique_string_count, arrow_string_format):
        for lib in self.ac.list_libraries():
            if "prewritten" in lib:
                continue
            self.ac.delete_library(lib)
        del self.ac

    def setup(self, rows, date_range, unique_string_count, arrow_string_format):
        self.ac = Arctic(self.connection_string, output_format=OutputFormat.PYARROW)
        self.lib = self.ac.get_library(self.lib_name_prewritten)
        self.lib._nvs._set_allow_arrow_input()
        if date_range is None:
            self.date_range = None
        else:
            # Create a date range that excludes the first and last 10 rows of the data only
            self.date_range = (pd.Timestamp(10), pd.Timestamp(rows - 10))
        self.fresh_lib = self.get_fresh_lib()
        self.fresh_lib._nvs._set_allow_arrow_input()
        self.table = self._generate_table(rows, self.num_cols, unique_string_count)

    def get_fresh_lib(self):
        self.ac.delete_library(self.lib_name_fresh)
        return self.ac.create_library(self.lib_name_fresh)

    def time_write(self, rows, date_range, unique_string_count, arrow_string_format):
        # No point in running with all read time options
        if date_range is None and arrow_string_format == ArrowOutputStringFormat.CATEGORICAL:
            self.fresh_lib.write(self.symbol_name(rows, unique_string_count), self.table, index_column="ts")
        else:
            raise SkipNotImplemented

    def peakmem_write(self, rows, date_range, unique_string_count, arrow_string_format):
        # No point in running with all read time options
        if date_range is None and arrow_string_format == ArrowOutputStringFormat.CATEGORICAL:
            self.fresh_lib.write(self.symbol_name(rows, unique_string_count), self.table, index_column="ts")
        else:
            raise SkipNotImplemented

    def _check_should_run_benchmark(self, *, date_range, arrow_string_format):
        # No need to run against all arrow_string_format to check the date_range filtering
        if date_range and arrow_string_format != ArrowOutputStringFormat.LARGE_STRING:
            raise SkipNotImplemented

    def time_read(self, rows, date_range, unique_string_count, arrow_string_format):
        self._check_should_run_benchmark(date_range=date_range, arrow_string_format=arrow_string_format)
        self.lib.read(
            self.symbol_name(rows, unique_string_count),
            date_range=self.date_range,
            arrow_string_format_default=arrow_string_format,
        )

    def peakmem_read(self, rows, date_range, unique_string_count, arrow_string_format):
        self._check_should_run_benchmark(date_range=date_range, arrow_string_format=arrow_string_format)
        self.lib.read(
            self.symbol_name(rows, unique_string_count),
            date_range=self.date_range,
            arrow_string_format_default=arrow_string_format,
        )


class ArrowMemoryStability:
    """Detect memory leaks in the Arrow read/write path.

    Writes and reads the same ~1MB PyArrow table 500 times with prune_previous_versions=True,
    and tracks anonymous RSS growth (excludes LMDB file-backed mmap pages).
    A healthy implementation should show near-zero growth.
    """

    timeout = 600
    connection_string = "lmdb://arrow_memory_stability"
    lib_name = "arrow_memory_stability"
    sym = "mem_test"
    num_iterations = 500
    warmup_iterations = 10
    # ~1MB: 10 int64 columns × 13,000 rows = 10 × 13000 × 8 bytes ≈ 1.04MB
    num_rows = 13_000
    num_cols = 10

    def setup(self):
        self.ac = Arctic(self.connection_string, output_format=OutputFormat.PYARROW)
        self.ac.delete_library(self.lib_name)
        self.ac.create_library(self.lib_name)
        self.lib = self.ac.get_library(self.lib_name)
        self.lib._nvs._set_allow_arrow_input()
        self.table = pa.table(
            {
                f"col{i}": np.arange(i * self.num_rows, (i + 1) * self.num_rows, dtype=np.int64)
                for i in range(self.num_cols)
            },
        )

    def teardown(self):
        self.ac.delete_library(self.lib_name)
        del self.ac

    def track_rss_growth_write_read(self):
        """Anonymous RSS growth (kB) after 500 write+read cycles with prune_previous_versions=True.

        Near zero for a healthy implementation. Large positive values indicate a memory leak
        (e.g. Arrow C Data Interface release callbacks not being called).
        """
        # Warmup: let internal caches, pools, and allocators stabilize
        for _ in range(self.warmup_iterations):
            self.lib.write(self.sym, self.table, prune_previous_versions=True)
            self.lib.read(self.sym)

        gc.collect()
        rss_before = _get_anon_rss_kb()

        for _ in range(self.num_iterations):
            self.lib.write(self.sym, self.table, prune_previous_versions=True)
            self.lib.read(self.sym)

        gc.collect()
        rss_after = _get_anon_rss_kb()

        return rss_after - rss_before

    track_rss_growth_write_read.unit = "kB"
