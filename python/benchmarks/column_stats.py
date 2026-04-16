"""
Copyright 2026 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

import time

import numpy as np
import pandas as pd
from arcticdb import QueryBuilder
from arcticdb.options import LibraryOptions
from arcticdb_ext import set_config_int, unset_config_int
from asv_runner.benchmarks.mark import SkipNotImplemented

from benchmarks.common import *
from arcticdb.util.logger import get_logger
from benchmarks.environment_setup import (
    Storage,
    create_library,
    create_libraries_across_storages,
    is_storage_enabled,
)

STORAGES = [Storage.LMDB, Storage.AMAZON]

BENCHMARK_COLUMNS = ["uint64_col", "float_col", "bool_col", "datetime_col"]

COLUMN_STATS = {
    "uint64_col": {"MINMAX"},
    "float_col": {"MINMAX"},
    "bool_col": {"MINMAX"},
    "datetime_col": {"MINMAX"},
}


def _symbol_name(rows, ordered):
    suffix = "ordered" if ordered else "random"
    return f"col_stats_{rows}_{suffix}"


def _add_extra_columns(df, n, num_cols=100, ordered=True):
    """Add extra filler columns: half int64, half float64, all positive values."""
    n_int = num_cols // 2
    n_float = num_cols - n_int
    if ordered:
        base_int = np.arange(1, n + 1, dtype=np.int64)
        base_float = np.linspace(1.0, 1000.0, n)
        for i in range(n_int):
            df[f"extra_int64_{i}"] = base_int + i
        for i in range(n_float):
            df[f"extra_float_{i}"] = base_float + i * 0.1
    else:
        rng = np.random.default_rng(123)
        for i in range(n_int):
            df[f"extra_int64_{i}"] = rng.integers(1, n, size=n, dtype=np.int64)
        for i in range(n_float):
            df[f"extra_float_{i}"] = rng.uniform(1.0, 1000.0, size=n)


def _generate_column_stats_dataframe(n):
    """Generate a DataFrame with ordered data suitable for column stats benchmarking."""
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
    _add_extra_columns(df, n, ordered=True)
    df.index = timestamps
    df.index.name = "ts"
    return df


def _generate_random_dataframe(n):
    """Generate a DataFrame with random data that will not benefit from column stats."""
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
    _add_extra_columns(df, n, ordered=False)
    df.index = timestamps
    df.index.name = "ts"
    return df


class ColumnStatsQueryPerformance:
    """Benchmark query filtering performance with and without column stats enabled.

    Runs against two data distributions:
    - ordered: sorted so segment-level min/max stats allow pruning
    - random: uniformly distributed so every segment spans the full range and stats cannot prune

    Filters target ~10% of the data range (or 50% for bool). With ordered data ~90% of segments
    can be skipped. With random data no segments can be skipped, measuring the overhead of stats
    when they cannot help.

    Data includes 100 extra filler columns; reads use columns= to restrict to the 4 benchmark columns.
    """

    sample_time = 0.5
    rounds = 1
    repeat = (2, 3, 5.0)
    warmup_time = 0.5
    timeout = 600

    num_rows = [10_000_000]
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
        dfs = {}
        for storage in ColumnStatsQueryPerformance.storages:
            if not is_storage_enabled(storage):
                continue
            lib = lib_for_storage[storage]
            for rows in ColumnStatsQueryPerformance.num_rows:
                for ordered in ColumnStatsQueryPerformance.data_ordered:
                    key = (rows, ordered)
                    if key not in dfs:
                        dfs[key] = (
                            _generate_column_stats_dataframe(rows) if ordered else _generate_random_dataframe(rows)
                        )
                    df = dfs[key]
                    sym = _symbol_name(rows, ordered)
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
        self.lib.read(self.symbol, columns=BENCHMARK_COLUMNS, query_builder=q)

    def time_filter_float(self, *args):
        q = QueryBuilder()
        q = q[q["float_col"] > 900.0]
        self.lib.read(self.symbol, columns=BENCHMARK_COLUMNS, query_builder=q)

    def time_filter_bool(self, *args):
        q = QueryBuilder()
        q = q[q["bool_col"]]
        self.lib.read(self.symbol, columns=BENCHMARK_COLUMNS, query_builder=q)

    def time_filter_combined_and(self, *args):
        q = QueryBuilder()
        q = q[(q["uint64_col"] > self.uint64_high) & (q["float_col"] > 900.0)]
        self.lib.read(self.symbol, columns=BENCHMARK_COLUMNS, query_builder=q)

    def time_filter_combined_or(self, *args):
        q = QueryBuilder()
        q = q[(q["uint64_col"] < self.uint64_low) | (q["float_col"] > 900.0)]
        self.lib.read(self.symbol, columns=BENCHMARK_COLUMNS, query_builder=q)

    def time_filter_datetime(self, *args):
        q = QueryBuilder()
        q = q[q["datetime_col"] > self.datetime_high]
        self.lib.read(self.symbol, columns=BENCHMARK_COLUMNS, query_builder=q)

    def time_isin_uint64(self, *args):
        q = QueryBuilder()
        q = q[q["uint64_col"].isin(self.isin_values)]
        self.lib.read(self.symbol, columns=BENCHMARK_COLUMNS, query_builder=q)

    def time_filter_zero_match(self, *args):
        """Filter that matches no rows, best case for column stats."""
        if not self.data_ordered:
            raise SkipNotImplemented
        q = QueryBuilder()
        q = q[q["uint64_col"] > self.zero_match_threshold]
        self.lib.read(self.symbol, columns=BENCHMARK_COLUMNS, query_builder=q)

    def time_filter_with_date_range(self, *args):
        """date_range narrows to 50% of rows, then filter selects the top 10% of uint64 values.

        Tests whether stats provide additional pruning on top of index-based date range filtering.
        """
        if not self.data_ordered:
            raise SkipNotImplemented
        q = QueryBuilder()
        q = q[q["uint64_col"] > self.uint64_high]
        self.lib.read(self.symbol, columns=BENCHMARK_COLUMNS, date_range=self.date_range_half, query_builder=q)

    def time_isnotin_uint64(self, *args):
        q = QueryBuilder()
        q = q[q["uint64_col"].isnotin(self.isin_values)]
        self.lib.read(self.symbol, columns=BENCHMARK_COLUMNS, query_builder=q)


class ColumnStatsDynamicSchema:
    """Benchmark column stats filtering when the filter column exists in some segments but not others.

    Uses dynamic schema to write data in alternating chunks: odd-numbered chunks include
    sometimes_missing_col (uint64), even-numbered chunks do not.

    Data includes 100 extra filler columns; reads use columns= to restrict to the benchmark
    columns plus sometimes_missing_col.
    """

    sample_time = 0.5
    rounds = 1
    repeat = (2, 3, 5.0)
    warmup_time = 0.5
    timeout = 600

    column_stats_enabled = [True, False]
    storages = STORAGES

    params = [column_stats_enabled, storages]
    param_names = ["column_stats_enabled", "storage"]

    NUM_ROWS = 10_000_000
    NUM_CHUNKS = 10
    ROWS_PER_CHUNK = NUM_ROWS // NUM_CHUNKS

    def __init__(self):
        self.logger = get_logger()

    def setup_cache(self):
        start = time.time()
        lib_for_storage = {}
        for storage in self.storages:
            if not is_storage_enabled(storage):
                lib_for_storage[storage] = None
                continue
            lib = create_library(storage, LibraryOptions(dynamic_schema=True))
            lib_for_storage[storage] = lib

            sym = "dynamic_schema"
            rows_per_chunk = self.ROWS_PER_CHUNK
            sometimes_missing_counter = 0

            for chunk_idx in range(self.NUM_CHUNKS):
                offset = chunk_idx * rows_per_chunk
                timestamps = pd.date_range(
                    start=pd.Timestamp("2020-01-01") + pd.Timedelta(seconds=offset),
                    periods=rows_per_chunk,
                    freq="s",
                )
                datetime_start = pd.Timestamp("2000-01-01")
                data = {
                    "uint64_col": np.arange(offset, offset + rows_per_chunk, dtype=np.uint64),
                    "float_col": np.linspace(chunk_idx * 100.0, (chunk_idx + 1) * 100.0, rows_per_chunk),
                    "bool_col": np.zeros(rows_per_chunk, dtype=bool),
                    "datetime_col": pd.date_range(
                        start=datetime_start + pd.Timedelta(seconds=offset),
                        periods=rows_per_chunk,
                        freq="s",
                    ),
                }
                # Odd-numbered chunks include sometimes_missing_col with monotonically increasing values
                if chunk_idx % 2 == 1:
                    data["sometimes_missing_col"] = np.arange(
                        sometimes_missing_counter, sometimes_missing_counter + rows_per_chunk, dtype=np.uint64
                    )
                    sometimes_missing_counter += rows_per_chunk

                df = pd.DataFrame(data)
                _add_extra_columns(df, rows_per_chunk, ordered=True)
                df.index = timestamps
                df.index.name = "ts"

                lib.append(sym, df)
                self.logger.info(f"Wrote chunk {chunk_idx} for {sym}")

            lib._nvs.create_column_stats(sym, {"sometimes_missing_col": {"MINMAX"}, **COLUMN_STATS})
            self.logger.info(f"Created column stats for {sym}")

        self.logger.info(f"setup_cache time: {time.time() - start}")
        return lib_for_storage

    def setup(self, lib_for_storage, column_stats_enabled, storage):
        self.lib = lib_for_storage[storage]
        if self.lib is None:
            raise SkipNotImplemented
        self.symbol = "dynamic_schema"

        # sometimes_missing_col has values 0..(5*ROWS_PER_CHUNK - 1) across the 5 odd chunks
        total_sometimes_missing_rows = self.NUM_ROWS // 2
        self.sometimes_missing_low = total_sometimes_missing_rows // 10

        if column_stats_enabled:
            set_config_int("ColumnStats.UseForQueries", 1)
        else:
            unset_config_int("ColumnStats.UseForQueries")

    def teardown(self, *args):
        unset_config_int("ColumnStats.UseForQueries")

    def time_filter_sometimes_missing_column(self, *args):
        q = QueryBuilder()
        q = q[q["sometimes_missing_col"] < self.sometimes_missing_low]
        self.lib.read(self.symbol, columns=BENCHMARK_COLUMNS + ["sometimes_missing_col"], query_builder=q)


class ColumnStatsCreate:
    number = 1
    warmup_time = 0
    timeout = 600

    num_rows = [10_000_000]
    storages = STORAGES

    params = [num_rows, storages]
    param_names = ["num_rows", "storage"]

    def __init__(self):
        self.logger = get_logger()

    def setup_cache(self):
        start = time.time()
        lib_for_storage = create_libraries_across_storages(self.storages)
        for storage in ColumnStatsCreate.storages:
            if not is_storage_enabled(storage):
                continue
            lib = lib_for_storage[storage]
            for rows in ColumnStatsCreate.num_rows:
                df = _generate_column_stats_dataframe(rows)
                sym = _symbol_name(rows, ordered=True)
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

    num_rows = [10_000_000]
    storages = STORAGES

    params = [num_rows, storages]
    param_names = ["num_rows", "storage"]

    def __init__(self):
        self.logger = get_logger()

    def setup_cache(self):
        start = time.time()
        lib_for_storage = create_libraries_across_storages(self.storages)
        for storage in ColumnStatsManagement.storages:
            if not is_storage_enabled(storage):
                continue
            lib = lib_for_storage[storage]
            for rows in ColumnStatsManagement.num_rows:
                df = _generate_column_stats_dataframe(rows)
                sym = _symbol_name(rows, ordered=True)
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


class ColumnStatsWideFilter:
    """Benchmark the overhead of query builder filters that force decoding many extra columns.

    Writes 10M rows with 204 columns (4 benchmark + 100 int64 + 100 float) and create column stats on everything.
    The query builder ORs the main uint64 filter with a ``< 0`` condition on every extra column
    (all values are positive, so these never match). Without column stats, this forces a full table scan
    through all these columns.
    """

    sample_time = 0.5
    rounds = 1
    repeat = (2, 3, 5.0)
    warmup_time = 0.5
    timeout = 600

    column_stats_enabled = [True, False]
    storages = STORAGES

    params = [column_stats_enabled, storages]
    param_names = ["column_stats_enabled", "storage"]

    NUM_ROWS = 10_000_000
    NUM_EXTRA = 200
    ROWS_PER_CHUNK = 1_000_000

    def __init__(self):
        self.logger = get_logger()

    def setup_cache(self):
        start = time.time()
        lib_for_storage = create_libraries_across_storages(self.storages)
        for storage in self.storages:
            if not is_storage_enabled(storage):
                continue
            lib = lib_for_storage[storage]
            sym = "wide_filter"
            n_int = self.NUM_EXTRA // 2
            n_float = self.NUM_EXTRA - n_int

            for chunk_idx in range(self.NUM_ROWS // self.ROWS_PER_CHUNK):
                n = self.ROWS_PER_CHUNK
                offset = chunk_idx * n
                timestamps = pd.date_range(
                    start=pd.Timestamp("2020-01-01") + pd.Timedelta(seconds=offset),
                    periods=n,
                    freq="s",
                )
                datetime_start = pd.Timestamp("2000-01-01")
                data = {
                    "uint64_col": np.arange(offset, offset + n, dtype=np.uint64),
                    "float_col": np.linspace(
                        offset * 1000.0 / self.NUM_ROWS,
                        (offset + n) * 1000.0 / self.NUM_ROWS,
                        n,
                    ),
                    "bool_col": np.array([i + offset >= self.NUM_ROWS // 2 for i in range(n)], dtype=bool),
                    "datetime_col": pd.date_range(
                        start=datetime_start + pd.Timedelta(seconds=offset),
                        periods=n,
                        freq="s",
                    ),
                }
                base_int = np.arange(offset + 1, offset + n + 1, dtype=np.int64)
                base_float = np.linspace(1.0, 1000.0, n)
                for i in range(n_int):
                    data[f"extra_int64_{i}"] = base_int + i
                for i in range(n_float):
                    data[f"extra_float_{i}"] = base_float + i * 0.1

                df = pd.DataFrame(data)
                df.index = timestamps
                df.index.name = "ts"

                lib.append(sym, df)
                self.logger.info(f"Wrote chunk {chunk_idx} for {sym}")

            # Create stats on all columns (benchmark + extra) so column stats can prune
            all_stats = dict(COLUMN_STATS)
            for i in range(n_int):
                all_stats[f"extra_int64_{i}"] = {"MINMAX"}
            for i in range(n_float):
                all_stats[f"extra_float_{i}"] = {"MINMAX"}
            lib._nvs.create_column_stats(sym, all_stats)
            self.logger.info(f"Created column stats for {sym}")

        self.logger.info(f"setup_cache time: {time.time() - start}")
        return lib_for_storage

    def setup(self, lib_for_storage, column_stats_enabled, storage):
        self.lib = lib_for_storage[storage]
        if self.lib is None:
            raise SkipNotImplemented
        self.symbol = "wide_filter"

        n_int = self.NUM_EXTRA // 2
        n_float = self.NUM_EXTRA - n_int

        # Build the QueryBuilder with 201 OR'd conditions as a balanced tree.
        # A left-leaning chain would exceed Python's recursion limit in the
        # recursive expression visitor in processing.py.
        q = QueryBuilder()
        uint64_low = self.NUM_ROWS // 10
        conditions = [q["uint64_col"] < uint64_low]
        for i in range(n_int):
            conditions.append(q[f"extra_int64_{i}"] < np.int64(0))
        for i in range(n_float):
            conditions.append(q[f"extra_float_{i}"] < 0.0)
        while len(conditions) > 1:
            next_level = []
            for j in range(0, len(conditions), 2):
                if j + 1 < len(conditions):
                    next_level.append(conditions[j] | conditions[j + 1])
                else:
                    next_level.append(conditions[j])
            conditions = next_level
        q = q[conditions[0]]
        self.query = q

        if column_stats_enabled:
            set_config_int("ColumnStats.UseForQueries", 1)
        else:
            unset_config_int("ColumnStats.UseForQueries")

    def teardown(self, *args):
        unset_config_int("ColumnStats.UseForQueries")

    def time_filter_wide(self, *args):
        """Filter with 201 OR'd conditions forcing decode of 200 extra columns."""
        self.lib.read(self.symbol, columns=BENCHMARK_COLUMNS, query_builder=self.query)
