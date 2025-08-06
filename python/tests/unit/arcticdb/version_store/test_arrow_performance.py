import time

import numpy as np
import pandas as pd
import pyarrow as pa
import pytest

from arcticdb import Arctic, LibraryOptions
from arcticdb_ext.version_store import OutputFormat
from arcticdb.version_store.processing import QueryBuilder
from arcticdb.util.test import config_context, random_strings_of_length


ac = Arctic("lmdb:///tmp/arrow-profiling")
lib = ac.get_library("arrow_profiling", create_if_missing=True)
num_numeric_cols = 9 # +1 for index
numeric_num_rows = [1, 1_000, 100_000, 1_000_000, 100_000_000]

num_string_cols = 10
unique_string_counts = [1, 100, 100_000]
string_num_rows = [1, 100, 10_000, 1_000_000]


def numeric_symbol(num_rows: int):
    return f"numeric_{num_rows}_rows"


def string_symbol(num_rows: int, unique_strings: int):
    return f"string_{num_rows}_rows_{unique_strings}_unique_strings"


def test_write_test_data():
    lib._nvs.version_store.clear()
    for num_rows in numeric_num_rows:
        sym = numeric_symbol(num_rows)
        df = pd.DataFrame(
            {
                f"col{idx}": np.arange(idx * num_rows, (idx + 1) * num_rows, dtype=np.int64) for idx in range(num_numeric_cols)
            },
            index = pd.date_range("2025-01-01", freq="s", periods=num_rows)
        )
        lib.write(sym, df)

    rng = np.random.default_rng()
    for unique_string_count in unique_string_counts:
        strings = np.array(random_strings_of_length(unique_string_count, 10, unique=True))
        for num_rows in string_num_rows:
            sym = string_symbol(num_rows, unique_string_count)
            df = pd.DataFrame(
                {
                    f"col{idx}": rng.choice(strings, num_rows) for idx in range(num_string_cols)
                },
                index = pd.date_range("2025-01-01", freq="s", periods=num_rows)
            )
            lib.write(sym, df)



@pytest.mark.parametrize("num_rows", numeric_num_rows)
def test_basic_numeric_read_perf(num_rows):
    # Remove version map caching from timings
    with config_context("VersionMap.ReloadInterval", 0):
        sym = numeric_symbol(num_rows)
        repeats = 10
        num_values = num_rows * (num_numeric_cols + 1) # +1 for index

        lib._nvs._normalizer.df._skip_df_consolidation = False
        start = time.time()
        for _ in range(repeats):
            lib.read(sym)
        end = time.time()
        print(f"Pandas with consolidation read {num_values} integers in {(end - start) / repeats}s")

        lib._nvs._normalizer.df._skip_df_consolidation = True
        start = time.time()
        for _ in range(repeats):
            lib.read(sym)
        end = time.time()
        print(f"Pandas without consolidation read {num_values} integers in {(end - start) / repeats}s")

        start = time.time()
        for _ in range(repeats):
            lib._nvs.read(sym, _output_format=OutputFormat.ARROW)
        end = time.time()
        print(f"Arrow read {num_values} integers in {(end - start) / repeats}s")


def test_basic_numeric_read_perf_individual():
    num_rows = 100_000_000
    # Remove version map caching from timings
    with config_context("VersionMap.ReloadInterval", 0):
        sym = numeric_symbol(num_rows)
        repeats = 10
        num_values = num_rows * (num_numeric_cols + 1) # +1 for index

        # lib._nvs._normalizer.df._skip_df_consolidation = False
        # start = time.time()
        # for _ in range(repeats):
        #     lib.read(sym)
        # end = time.time()
        # print(f"Pandas with consolidation read {num_values} integers in {(end - start) / repeats}s")

        lib._nvs._normalizer.df._skip_df_consolidation = True
        start = time.time()
        for _ in range(repeats):
            lib.read(sym)
        end = time.time()
        print(f"Pandas without consolidation read {num_values} integers in {(end - start) / repeats}s")

        start = time.time()
        for _ in range(repeats):
            lib._nvs.read(sym, _output_format=OutputFormat.ARROW)
        end = time.time()
        print(f"Arrow read {num_values} integers in {(end - start) / repeats}s")


@pytest.mark.parametrize("num_unique_strings", unique_string_counts)
@pytest.mark.parametrize("num_rows", string_num_rows)
def test_basic_string_read_perf(num_rows, num_unique_strings):
    # Remove version map caching from timings
    with config_context("VersionMap.ReloadInterval", 0):
        sym = string_symbol(num_rows, num_unique_strings)
        repeats = 1
        num_values = num_rows * num_string_cols

        lib._nvs._normalizer.df._skip_df_consolidation = False
        start = time.time()
        for _ in range(repeats):
            lib.read(sym)
        end = time.time()
        print(f"Pandas with consolidation read {num_values} strings ({num_unique_strings} unique) in {(end - start) / repeats}s")

        lib._nvs._normalizer.df._skip_df_consolidation = True
        start = time.time()
        for _ in range(repeats):
            lib.read(sym)
        end = time.time()
        print(f"Pandas without consolidation read {num_values} strings ({num_unique_strings} unique) in {(end - start) / repeats}s")

        start = time.time()
        for _ in range(repeats):
            lib._nvs.read(sym, _output_format=OutputFormat.ARROW)
        end = time.time()
        print(f"Arrow read {num_values} strings ({num_unique_strings} unique) in {(end - start) / repeats}s")


def test_basic_string_read_perf_individual():
    num_rows = 1_000_000
    num_unique_strings = 1
    # Remove version map caching from timings
    with config_context("VersionMap.ReloadInterval", 0):
        sym = string_symbol(num_rows, num_unique_strings)
        repeats = 1
        num_values = num_rows * num_string_cols

        # lib._nvs._normalizer.df._skip_df_consolidation = False
        # start = time.time()
        # for _ in range(repeats):
        #     lib.read(sym)
        # end = time.time()
        # print(f"Pandas with consolidation read {num_values} strings ({num_unique_strings} unique) in {(end - start) / repeats}s")
        #
        # lib._nvs._normalizer.df._skip_df_consolidation = True
        # start = time.time()
        # for _ in range(repeats):
        #     lib.read(sym)
        # end = time.time()
        # print(f"Pandas without consolidation read {num_values} strings ({num_unique_strings} unique) in {(end - start) / repeats}s")

        start = time.time()
        for _ in range(repeats):
            lib._nvs.read(sym, _output_format=OutputFormat.ARROW)
        end = time.time()
        print(f"Arrow read {num_values} strings ({num_unique_strings} unique) in {(end - start) / repeats}s")
