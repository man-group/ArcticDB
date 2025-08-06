import time

import numpy as np
import pandas as pd
import pyarrow as pa
import pytest

from arcticdb_ext.version_store import OutputFormat
from arcticdb.version_store.processing import QueryBuilder
from arcticdb.util.test import config_context, random_strings_of_length


@pytest.mark.parametrize("num_rows", [1, 100, 10_000, 1_000_000, 100_000_000])
def test_basic_numeric_read_perf(lmdb_version_store_v1, num_rows):
    # Remove version map caching from timings
    with config_context("VersionMap.ReloadInterval", 0):
        lib = lmdb_version_store_v1
        sym = "test_basic_numeric_read_perf"
        num_cols = 10
        num_values = num_rows * num_cols
        df = pd.DataFrame({f"col{idx}": np.arange(idx * num_rows, (idx + 1) + num_rows, dtype=np.int64) for idx in range(num_cols)})
        lib.write(sym, df)

        pd_start = time.time()
        lib.read(sym, _output_format=OutputFormat.PANDAS)
        pd_end = time.time()

        pa_start = time.time()
        lib.read(sym, _output_format=OutputFormat.ARROW)
        pa_end = time.time()

        print(f"Pandas read {num_values} integers in {pd_end - pd_start}s")
        print(f"Arrow read {num_values} integers in {pa_end - pa_start}s")


@pytest.mark.parametrize("num_unique_strings", [1, 100, 100_000])
@pytest.mark.parametrize("num_rows", [1, 100, 10_000, 1_000_000])
def test_basic_string_read_perf(lmdb_version_store_v1, num_rows, num_unique_strings):
    # Remove version map caching from timings
    with config_context("VersionMap.ReloadInterval", 0):
        lib = lmdb_version_store_v1
        sym = "test_basic_string_read_perf"
        strings = np.array(random_strings_of_length(num_unique_strings, 10, unique=True))
        rng = np.random.default_rng()
        num_cols = 10
        num_values = num_rows * num_cols
        df = pd.DataFrame({f"col{idx}": rng.choice(strings, num_rows) for idx in range(num_cols)})
        lib.write(sym, df)

        pd_start = time.time()
        lib.read(sym, _output_format=OutputFormat.PANDAS)
        pd_end = time.time()

        pa_start = time.time()
        lib.read(sym, _output_format=OutputFormat.ARROW)
        pa_end = time.time()

        print(f"Pandas read {num_values} strings in {pd_end - pd_start}s")
        print(f"Arrow read {num_values} strings in {pa_end - pa_start}s")
