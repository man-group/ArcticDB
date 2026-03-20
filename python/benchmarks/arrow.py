"""
Copyright 2026 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

import random
import time
import numpy as np
import pandas as pd

from arcticdb import Arctic, OutputFormat, ArrowOutputStringFormat
from arcticdb.options import LibraryOptions
from arcticdb.dependencies import pyarrow as pa
from arcticdb.util.logger import get_logger
from arcticdb.util.test import random_strings_of_length
from asv_runner.benchmarks.mark import SkipNotImplemented

from benchmarks.common import generate_pseudo_random_dataframe


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


class ArrowSparseNumeric:
    timeout = 600
    connection_string = "lmdb://arrow_sparse_numeric"
    lib_name_prewritten = "arrow_sparse_numeric_prewritten"
    lib_name_fresh = "arrow_sparse_numeric_fresh"
    type_promotion_suffix = "_with_type_promotion"
    num_cols = 9
    params = ([1_000_000, 10_000_000], [0.1, 0.5, 0.9])
    param_names = ["rows", "sparsity"]

    def symbol_name(self, num_rows: int, sparsity: float, suffix=""):
        return f"sparse_{num_rows}_{sparsity}{suffix}"

    def _generate_table(self, num_rows, sparsity, pa_type=pa.int64()):
        np.random.seed(42)
        mask = np.random.random(num_rows) < sparsity
        columns = {}
        for i in range(self.num_cols):
            vals = np.arange(i * num_rows, (i + 1) * num_rows, dtype=np.int64)
            columns[f"col{i}"] = pa.array(np.where(mask, None, vals), pa_type)
        return pa.table(columns)

    def setup_cache(self):
        ac = Arctic(self.connection_string, output_format=OutputFormat.PYARROW)
        num_rows_list, sparsity_list = self.params
        ac.delete_library(self.lib_name_prewritten)
        lib = ac.create_library(self.lib_name_prewritten, library_options=LibraryOptions(dynamic_schema=True))
        lib._nvs._set_allow_arrow_input()
        for rows in num_rows_list:
            for sparsity in sparsity_list:
                lib.write(self.symbol_name(rows, sparsity), self._generate_table(rows, sparsity))
                lib.write(
                    self.symbol_name(rows, sparsity, self.type_promotion_suffix),
                    self._generate_table(rows // 2, sparsity, pa.int32()),
                )
                lib.append(
                    self.symbol_name(rows, sparsity, self.type_promotion_suffix),
                    self._generate_table(rows // 2, sparsity, pa.int64()),
                )

    def teardown(self, rows, sparsity):
        for lib in self.ac.list_libraries():
            if "prewritten" in lib:
                continue
            self.ac.delete_library(lib)
        del self.ac

    def setup(self, rows, sparsity):
        self.ac = Arctic(self.connection_string, output_format=OutputFormat.PYARROW)
        self.lib = self.ac.get_library(self.lib_name_prewritten)
        self.lib._nvs._set_allow_arrow_input()
        self.fresh_lib = self.get_fresh_lib()
        self.fresh_lib._nvs._set_allow_arrow_input()
        self.table = self._generate_table(rows, sparsity)
        self.sym = self.symbol_name(rows, sparsity)
        self.sym_with_type_promotion = self.symbol_name(rows, sparsity, self.type_promotion_suffix)

    def get_fresh_lib(self):
        self.ac.delete_library(self.lib_name_fresh)
        return self.ac.create_library(self.lib_name_fresh)

    def time_write(self, rows, sparsity):
        self.fresh_lib.write(self.sym, self.table)

    def peakmem_write(self, rows, sparsity):
        self.fresh_lib.write(self.sym, self.table)

    def time_read(self, rows, sparsity):
        self.lib.read(self.sym)

    def peakmem_read(self, rows, sparsity):
        self.lib.read(self.sym)

    def time_read_pandas(self, rows, sparsity):
        self.lib.read(self.sym, output_format="PANDAS")

    def peakmem_read_pandas(self, rows, sparsity):
        self.lib.read(self.sym, output_format="PANDAS")

    def time_read_with_type_promotion(self, rows, sparsity):
        self.lib.read(self.sym_with_type_promotion)

    def peakmem_read_with_type_promotion(self, rows, sparsity):
        self.lib.read(self.sym_with_type_promotion)


class ArrowBools:
    timeout = 600
    connection_string = "lmdb://arrow_bools"
    lib_name_prewritten = "arrow_bools_prewritten"
    lib_name_fresh = "arrow_bools_fresh"
    num_cols = 9
    params = ([1_000_000, 10_000_000], [0.0, 0.5])
    param_names = ["rows", "sparsity"]

    def symbol_name(self, num_rows: int, sparsity: float):
        return f"bools_{num_rows}_{sparsity}"

    def _generate_table(self, num_rows, sparsity):
        np.random.seed(42)
        vals = np.random.random(num_rows) < 0.5
        if sparsity > 0.0:
            null_mask = np.random.random(num_rows) < sparsity
            columns = {f"col{i}": pa.array(np.where(null_mask, None, vals), pa.bool_()) for i in range(self.num_cols)}
        else:
            columns = {f"col{i}": pa.array(vals, pa.bool_()) for i in range(self.num_cols)}
        return pa.table(columns)

    def setup_cache(self):
        ac = Arctic(self.connection_string, output_format=OutputFormat.PYARROW)
        num_rows_list, sparsity_list = self.params
        ac.delete_library(self.lib_name_prewritten)
        lib = ac.create_library(self.lib_name_prewritten)
        lib._nvs._set_allow_arrow_input()
        for rows in num_rows_list:
            for sparsity in sparsity_list:
                lib.write(self.symbol_name(rows, sparsity), self._generate_table(rows, sparsity))

    def teardown(self, rows, sparsity):
        for lib in self.ac.list_libraries():
            if "prewritten" in lib:
                continue
            self.ac.delete_library(lib)
        del self.ac

    def setup(self, rows, sparsity):
        self.ac = Arctic(self.connection_string, output_format=OutputFormat.PYARROW)
        self.lib = self.ac.get_library(self.lib_name_prewritten)
        self.lib._nvs._set_allow_arrow_input()
        self.fresh_lib = self.get_fresh_lib()
        self.fresh_lib._nvs._set_allow_arrow_input()
        self.table = self._generate_table(rows, sparsity)
        self.sym = self.symbol_name(rows, sparsity)

    def get_fresh_lib(self):
        self.ac.delete_library(self.lib_name_fresh)
        return self.ac.create_library(self.lib_name_fresh)

    def time_write(self, rows, sparsity):
        self.fresh_lib.write(self.sym, self.table)

    def peakmem_write(self, rows, sparsity):
        self.fresh_lib.write(self.sym, self.table)

    def time_read(self, rows, sparsity):
        self.lib.read(self.sym)

    def peakmem_read(self, rows, sparsity):
        self.lib.read(self.sym)


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
