"""
Copyright 2026 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

import time

import numpy as np
import pandas as pd
from arcticdb import QueryBuilder
from arcticdb_ext import set_config_int, unset_config_int
from asv_runner.benchmarks.mark import SkipNotImplemented

from benchmarks.common import *
from arcticdb.util.logger import get_logger
from benchmarks.environment_setup import (
    Storage,
    create_libraries_across_storages,
    is_storage_enabled,
)

STORAGES = [Storage.LMDB, Storage.AMAZON]

COLUMN_STATS = {
    "uint64_col": {"MINMAX"},
    "float_col": {"MINMAX"},
    "bool_col": {"MINMAX"},
    "datetime_col": {"MINMAX"},
}


def _symbol_name(rows, ordered):
    suffix = "ordered" if ordered else "random"
    return f"col_stats_{rows}_{suffix}"


def _generate_column_stats_dataframe(n):
    """Generate a DataFrame with ordered data suitable for column stats benchmarking.

    Data is monotonically ordered so that each storage segment has a tight min/max range,
    allowing column stats to effectively prune segments during filtered reads.
    """
    timestamps = pd.date_range(end="1/1/2023", periods=n, freq="s")
    # Monotonically increasing datetime column spanning a separate range from the index
    datetime_start = pd.Timestamp("2000-01-01")
    datetime_col = pd.date_range(start=datetime_start, periods=n, freq="s")
    df = pd.DataFrame(
        {
            "uint64_col": np.arange(n, dtype=np.uint64),
            "float_col": np.linspace(0.0, 1000.0, n),
            "bool_col": np.array([i >= n // 2 for i in range(n)], dtype=bool),
            "datetime_col": datetime_col,
        }
    )
    df.index = timestamps
    df.index.name = "ts"
    return df


def _generate_random_dataframe(n):
    """Generate a DataFrame with random data that will not benefit from column stats.

    Every segment will span the full value range for each column, so MINMAX stats
    cannot prune any segments. Uses the same value ranges as the ordered generator
    so the same filter thresholds select a similar fraction of rows.
    """
    rng = np.random.default_rng(42)
    timestamps = pd.date_range(end="1/1/2023", periods=n, freq="s")
    datetime_start = pd.Timestamp("2000-01-01")
    datetime_offsets = rng.integers(0, n, size=n)
    datetime_col = datetime_start + pd.to_timedelta(datetime_offsets, unit="s")
    df = pd.DataFrame(
        {
            "uint64_col": rng.integers(0, n, size=n).astype(np.uint64),
            "float_col": rng.uniform(0.0, 1000.0, size=n),
            "bool_col": rng.choice([True, False], size=n),
            "datetime_col": datetime_col,
        }
    )
    df.index = timestamps
    df.index.name = "ts"
    return df


class ColumnStatsQueryPerformance:
    """Benchmark query filtering performance with and without column stats enabled.

    Runs against two data distributions:
    - ordered: monotonically sorted so segment-level min/max stats allow effective pruning.
    - random: uniformly distributed so every segment spans the full range and stats cannot prune.

    Filters target ~10% of the data range (or 50% for bool). With ordered data ~90% of segments can be skipped.
    With random data no segments can be skipped, measuring the overhead of stats when they cannot help.
    """

    sample_time = 0.5
    rounds = 1
    repeat = (2, 3, 5.0)
    warmup_time = 0.5
    timeout = 600

    num_rows = [1_000_000, 10_000_000]
    column_stats_enabled = [True, False]
    data_ordered = [True, False]
    storages = STORAGES

    params = [num_rows, column_stats_enabled, data_ordered, storages]
    param_names = ["num_rows", "column_stats_enabled", "data_ordered", "storage"]

    def __init__(self):
        self.logger = get_logger()
        self.lib = None
        self.symbol = None

    def setup_cache(self):
        start = time.time()
        lib_for_storage = create_libraries_across_storages(self.storages)
        for rows in ColumnStatsQueryPerformance.num_rows:
            for ordered in ColumnStatsQueryPerformance.data_ordered:
                df = _generate_column_stats_dataframe(rows) if ordered else _generate_random_dataframe(rows)
                sym = _symbol_name(rows, ordered)
                for storage in ColumnStatsQueryPerformance.storages:
                    if not is_storage_enabled(storage):
                        continue
                    lib = lib_for_storage[storage]
                    self.logger.info(f"Writing {sym} with {rows} rows")
                    lib.write(sym, df)
                    lib._nvs.create_column_stats(sym, COLUMN_STATS)
        self.logger.info(f"setup_cache time: {time.time() - start}")
        return lib_for_storage

    def setup(self, lib_for_storage, num_rows, column_stats_enabled, data_ordered, storage):
        self.lib = lib_for_storage[storage]
        if self.lib is None:
            raise SkipNotImplemented
        self.symbol = _symbol_name(num_rows, data_ordered)
        self.data_ordered = data_ordered

        # Compute filter thresholds targeting ~10% of the value range
        self.uint64_low = num_rows // 10
        self.uint64_high = num_rows * 9 // 10
        # ~100 values from the first 10% of the uint64 range
        self.isin_values = list(range(0, num_rows // 10, max(1, num_rows // 1000)))
        # Datetime threshold: last 10% of the datetime_col range
        datetime_start = pd.Timestamp("2000-01-01")
        self.datetime_high = datetime_start + pd.Timedelta(seconds=num_rows * 9 // 10)
        # Zero-match threshold: max uint64 value is num_rows - 1, so > num_rows matches nothing
        self.zero_match_threshold = num_rows
        # Date range covering the last 50% of the index, for combined date_range + filter benchmarks
        index_end = pd.Timestamp("2023-01-01")
        self.date_range_half = (index_end - pd.Timedelta(seconds=num_rows // 2), index_end)

        if column_stats_enabled:
            set_config_int("ColumnStats.UseForQueries", 1)
        else:
            unset_config_int("ColumnStats.UseForQueries")

    def teardown(self, *args):
        unset_config_int("ColumnStats.UseForQueries")

    def time_filter_uint64(self, *args):
        q = QueryBuilder()
        q = q[q["uint64_col"] < self.uint64_low]
        self.lib.read(self.symbol, columns=["uint64_col"], query_builder=q)

    def time_filter_float(self, *args):
        q = QueryBuilder()
        q = q[q["float_col"] > 900.0]
        self.lib.read(self.symbol, columns=["float_col"], query_builder=q)

    def time_filter_bool(self, *args):
        q = QueryBuilder()
        q = q[q["bool_col"] == True]
        self.lib.read(self.symbol, columns=["bool_col"], query_builder=q)

    def time_filter_combined_and(self, *args):
        q = QueryBuilder()
        q = q[(q["uint64_col"] > self.uint64_high) & (q["float_col"] > 900.0)]
        self.lib.read(self.symbol, columns=["uint64_col", "float_col"], query_builder=q)

    def time_filter_combined_or(self, *args):
        q = QueryBuilder()
        q = q[(q["uint64_col"] < self.uint64_low) | (q["float_col"] > 900.0)]
        self.lib.read(self.symbol, columns=["uint64_col", "float_col"], query_builder=q)

    def time_filter_datetime(self, *args):
        q = QueryBuilder()
        q = q[q["datetime_col"] > self.datetime_high]
        self.lib.read(self.symbol, columns=["datetime_col"], query_builder=q)

    def time_isin_uint64(self, *args):
        q = QueryBuilder()
        q = q[q["uint64_col"].isin(self.isin_values)]
        self.lib.read(self.symbol, columns=["uint64_col"], query_builder=q)

    def time_filter_zero_match(self, *args):
        """Filter that matches no rows, best case for column stats."""
        if not self.data_ordered:
            raise SkipNotImplemented
        q = QueryBuilder()
        q = q[q["uint64_col"] > self.zero_match_threshold]
        self.lib.read(self.symbol, columns=["uint64_col"], query_builder=q)

    def time_filter_with_date_range(self, *args):
        """date_range narrows to 50% of rows, then filter selects the top 10% of uint64 values.

        Tests whether stats provide additional pruning on top of index-based date range filtering.
        """
        if not self.data_ordered:
            raise SkipNotImplemented
        q = QueryBuilder()
        q = q[q["uint64_col"] > self.uint64_high]
        self.lib.read(self.symbol, columns=["uint64_col"], date_range=self.date_range_half, query_builder=q)

    def time_isnotin_uint64(self, *args):
        if not self.data_ordered:
            # Not that interesting - we already run isin in the unordered case
            raise SkipNotImplemented
        q = QueryBuilder()
        q = q[q["uint64_col"].isnotin(self.isin_values)]
        self.lib.read(self.symbol, columns=["uint64_col"], query_builder=q)


class ColumnStatsCreate:
    """Benchmark column stats creation. Setup drops stats so time_ measures pure creation."""

    number = 1
    warmup_time = 0
    timeout = 600

    num_rows = [1_000_000, 10_000_000]
    storages = STORAGES

    params = [num_rows, storages]
    param_names = ["num_rows", "storage"]

    def __init__(self):
        self.logger = get_logger()

    def setup_cache(self):
        start = time.time()
        lib_for_storage = create_libraries_across_storages(self.storages)
        for rows in ColumnStatsCreate.num_rows:
            df = _generate_column_stats_dataframe(rows)
            sym = _symbol_name(rows, ordered=True)
            for storage in ColumnStatsCreate.storages:
                if not is_storage_enabled(storage):
                    continue
                lib = lib_for_storage[storage]
                self.logger.info(f"Writing {sym} with {rows} rows")
                lib.write(sym, df)
        self.logger.info(f"setup_cache time: {time.time() - start}")
        return lib_for_storage

    def setup(self, lib_for_storage, num_rows, storage):
        self.lib = lib_for_storage[storage]
        if self.lib is None:
            raise SkipNotImplemented
        self.nvs = self.lib._nvs
        self.symbol = _symbol_name(num_rows, ordered=True)
        # Drop stats so time_ measures creation from scratch
        self.nvs.drop_column_stats(self.symbol)

    def time_create_column_stats(self, *args):
        self.nvs.create_column_stats(self.symbol, COLUMN_STATS)


class ColumnStatsManagement:
    """Benchmark column stats operations: drop, get_info, read."""

    number = 1
    warmup_time = 0
    timeout = 600

    num_rows = [1_000_000, 10_000_000]
    storages = STORAGES

    params = [num_rows, storages]
    param_names = ["num_rows", "storage"]

    def __init__(self):
        self.logger = get_logger()

    def setup_cache(self):
        start = time.time()
        lib_for_storage = create_libraries_across_storages(self.storages)
        for rows in ColumnStatsManagement.num_rows:
            df = _generate_column_stats_dataframe(rows)
            sym = _symbol_name(rows, ordered=True)
            for storage in ColumnStatsManagement.storages:
                if not is_storage_enabled(storage):
                    continue
                lib = lib_for_storage[storage]
                self.logger.info(f"Writing {sym} with {rows} rows")
                lib.write(sym, df)
                lib._nvs.create_column_stats(sym, COLUMN_STATS)
        self.logger.info(f"setup_cache time: {time.time() - start}")
        return lib_for_storage

    def setup(self, lib_for_storage, num_rows, storage):
        self.lib = lib_for_storage[storage]
        if self.lib is None:
            raise SkipNotImplemented
        self.nvs = self.lib._nvs
        self.symbol = _symbol_name(num_rows, ordered=True)
        self.nvs.create_column_stats(self.symbol, COLUMN_STATS)

    def time_drop_column_stats(self, *args):
        self.nvs.drop_column_stats(self.symbol)

    def time_get_column_stats_info(self, *args):
        self.nvs.get_column_stats_info(self.symbol)

    def time_read_column_stats(self, *args):
        self.nvs.read_column_stats(self.symbol)
