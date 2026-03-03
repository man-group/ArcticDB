"""
Copyright 2026 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

import itertools
import time

import numpy as np
import pandas as pd
from asv_runner.benchmarks.mark import SkipNotImplemented, skip_for_params

from arcticdb import Arctic
from arcticdb.util.logger import get_logger

from .common import generate_benchmark_df, generate_pseudo_random_dataframe


def _sym(rows):
    return f"sym_{rows}"


class SQLQueries:
    """
    Benchmark SQL query execution via lib.sql().

    Tests simple SELECT, column projection, WHERE filtering, GROUP BY
    aggregation, and JOIN performance across different data sizes.
    Uses generate_benchmark_df which provides string/int/float columns
    suitable for diverse query patterns (same data as QueryBuilder benchmarks).
    """

    sample_time = 2
    rounds = 2
    repeat = (1, 10, 20.0)
    warmup_time = 0.2
    timeout = 600

    num_rows = [1_000_000, 10_000_000]

    params = [num_rows]
    param_names = ["num_rows"]

    CONNECTION_STRING = "lmdb://sql_queries"
    LIB_NAME = "sql_queries"
    # Second symbol for JOIN benchmarks (10% of the main symbol size)
    JOIN_SYMBOL = "join_lookup"

    def __init__(self):
        self.logger = get_logger()

    def setup_cache(self):
        start = time.time()
        self._setup_cache()
        self.logger.info(f"SETUP_CACHE TIME: {time.time() - start}")

    def _setup_cache(self):
        np.random.seed(42)
        ac = Arctic(self.CONNECTION_STRING)
        ac.delete_library(self.LIB_NAME)
        lib = ac.create_library(self.LIB_NAME)
        for rows in self.num_rows:
            df = generate_benchmark_df(rows)
            lib.write(_sym(rows), df)

        # Write a small lookup table for JOINs — unique id1 values with a label
        # Use the largest dataset's id1 column as the basis
        max_rows = max(self.num_rows)
        k = max_rows // 10
        lookup = pd.DataFrame(
            {
                "id1": [f"id{str(i).zfill(3)}" for i in range(1, k + 1)],
                "category": np.random.choice(["A", "B", "C", "D"], k),
                "weight": np.random.uniform(0.5, 2.0, k),
            }
        )
        lib.write(self.JOIN_SYMBOL, lookup)

    def setup(self, rows):
        self.ac = Arctic(self.CONNECTION_STRING)
        self.lib = self.ac.get_library(self.LIB_NAME)
        self.symbol = _sym(rows)

    def teardown(self, *args):
        del self.lib
        del self.ac

    # --- Simple SELECT ---

    def time_select_all(self, rows):
        self.lib.sql(f"SELECT * FROM {self.symbol}")

    def peakmem_select_all(self, rows):
        self.lib.sql(f"SELECT * FROM {self.symbol}")

    # --- Column projection (pushdown) ---

    def time_select_columns(self, rows):
        self.lib.sql(f"SELECT v1, v2, v3 FROM {self.symbol}")

    def peakmem_select_columns(self, rows):
        self.lib.sql(f"SELECT v1, v2, v3 FROM {self.symbol}")

    # --- WHERE filtering (pushdown) ---

    def time_filter_numeric(self, rows):
        """Filter on float column — ~1% selectivity."""
        self.lib.sql(f"SELECT v3 FROM {self.symbol} WHERE v3 < 1.0")

    def peakmem_filter_numeric(self, rows):
        self.lib.sql(f"SELECT v3 FROM {self.symbol} WHERE v3 < 1.0")

    def time_filter_string_equality(self, rows):
        """Filter on string column — single value."""
        self.lib.sql(f"SELECT v1, v3 FROM {self.symbol} WHERE id1 = 'id001'")

    def peakmem_filter_string_equality(self, rows):
        self.lib.sql(f"SELECT v1, v3 FROM {self.symbol} WHERE id1 = 'id001'")

    # --- GROUP BY aggregation ---

    def time_groupby_sum(self, rows):
        """Low-cardinality groupby (id1 has ~N/10 distinct values)."""
        self.lib.sql(f"SELECT id1, SUM(v1) as total FROM {self.symbol} GROUP BY id1")

    def peakmem_groupby_sum(self, rows):
        self.lib.sql(f"SELECT id1, SUM(v1) as total FROM {self.symbol} GROUP BY id1")

    def time_groupby_multi_agg(self, rows):
        """Multiple aggregations in a single GROUP BY."""
        self.lib.sql(
            f"SELECT id1, SUM(v1) as s, AVG(v3) as a, MIN(v2) as mn, MAX(v2) as mx " f"FROM {self.symbol} GROUP BY id1"
        )

    def peakmem_groupby_multi_agg(self, rows):
        self.lib.sql(
            f"SELECT id1, SUM(v1) as s, AVG(v3) as a, MIN(v2) as mn, MAX(v2) as mx " f"FROM {self.symbol} GROUP BY id1"
        )

    def time_groupby_high_cardinality(self, rows):
        """High-cardinality groupby (id6 has ~N/k distinct values)."""
        self.lib.sql(f"SELECT id6, SUM(v1), SUM(v2) FROM {self.symbol} GROUP BY id6")

    def peakmem_groupby_high_cardinality(self, rows):
        self.lib.sql(f"SELECT id6, SUM(v1), SUM(v2) FROM {self.symbol} GROUP BY id6")

    # --- JOIN ---

    def time_join(self, rows):
        """JOIN main symbol with small lookup table."""
        self.lib.sql(
            f"SELECT t.id1, t.v1, t.v3, j.category, j.weight "
            f"FROM {self.symbol} t JOIN {self.JOIN_SYMBOL} j ON t.id1 = j.id1"
        )

    def peakmem_join(self, rows):
        self.lib.sql(
            f"SELECT t.id1, t.v1, t.v3, j.category, j.weight "
            f"FROM {self.symbol} t JOIN {self.JOIN_SYMBOL} j ON t.id1 = j.id1"
        )

    # --- Filtered aggregation (filter + groupby) ---

    def time_filter_then_groupby(self, rows):
        """WHERE filter reducing data ~10x, then GROUP BY."""
        self.lib.sql(f"SELECT id1, SUM(v3) as total " f"FROM {self.symbol} WHERE v3 < 10.0 GROUP BY id1")

    def peakmem_filter_then_groupby(self, rows):
        self.lib.sql(f"SELECT id1, SUM(v3) as total " f"FROM {self.symbol} WHERE v3 < 10.0 GROUP BY id1")

    # --- LIMIT pushdown ---

    def time_limit(self, rows):
        """LIMIT pushdown — should read minimal data."""
        self.lib.sql(f"SELECT * FROM {self.symbol} LIMIT 100")

    def peakmem_limit(self, rows):
        self.lib.sql(f"SELECT * FROM {self.symbol} LIMIT 100")

    # --- Output format: pyarrow ---

    def time_select_all_arrow(self, rows):
        """Same as select_all but returning Arrow table (no pandas conversion)."""
        self.lib.sql(f"SELECT * FROM {self.symbol}", output_format="pyarrow")

    def peakmem_select_all_arrow(self, rows):
        self.lib.sql(f"SELECT * FROM {self.symbol}", output_format="pyarrow")


class SQLStreamingMemory:
    """
    Peak memory benchmarks for streaming SQL queries.

    Compares memory usage when DuckDB processes data via the Arrow
    RecordBatchReader (streaming) versus materialized reads.  The streaming
    path should NOT materialize the full table — peak memory should be
    proportional to the result set, not the source data.

    Key scenarios:
    - Aggregation via SQL (result is tiny, source is large)
    - Filtered query (result is small subset of source)
    - Full scan (result ≈ source — baseline)

    We use the DuckDB context manager (lib.duckdb()) with explicit
    register_symbol() to control the registration path, and compare
    against lib.read() which fully materializes.
    """

    timeout = 600

    CONNECTION_STRING = "lmdb://sql_streaming"
    LIB_NAME = "sql_streaming"

    # Use 10M rows to make memory differences meaningful
    NUM_ROWS = 10_000_000
    SYMBOL = "timeseries"

    query_types = ["aggregation", "filtered_1pct", "full_scan"]

    params = [query_types]
    param_names = ["query_type"]

    def __init__(self):
        self.logger = get_logger()

    def setup_cache(self):
        start = time.time()
        self._setup_cache()
        self.logger.info(f"SETUP_CACHE TIME: {time.time() - start}")

    def _setup_cache(self):
        np.random.seed(42)
        ac = Arctic(self.CONNECTION_STRING)
        ac.delete_library(self.LIB_NAME)
        lib = ac.create_library(self.LIB_NAME)
        # Multi-column dataframe to increase memory footprint per row
        df = generate_benchmark_df(self.NUM_ROWS)
        lib.write(self.SYMBOL, df)

    def setup(self, query_type):
        self.ac = Arctic(self.CONNECTION_STRING)
        self.lib = self.ac.get_library(self.LIB_NAME)
        if query_type == "aggregation":
            self.query = f"SELECT id1, SUM(v1) as total, AVG(v3) as avg_v3 FROM {self.SYMBOL} GROUP BY id1"
        elif query_type == "filtered_1pct":
            self.query = f"SELECT v1, v2, v3 FROM {self.SYMBOL} WHERE v3 < 1.0"
        elif query_type == "full_scan":
            self.query = f"SELECT * FROM {self.SYMBOL}"

    def teardown(self, *args):
        del self.lib
        del self.ac

    def peakmem_sql_query(self, query_type):
        """Peak memory of lib.sql() — uses streaming under the hood."""
        self.lib.sql(self.query)

    def peakmem_sql_query_arrow(self, query_type):
        """Peak memory of lib.sql() returning Arrow (avoids pandas conversion overhead)."""
        self.lib.sql(self.query, output_format="pyarrow")

    def peakmem_read_baseline(self, query_type):
        """
        Peak memory of lib.read() — materializes the full table.

        This is the baseline: the streaming SQL path should use less memory
        than this for aggregation and filtered queries.
        """
        self.lib.read(self.SYMBOL)

    def time_sql_query(self, query_type):
        """Execution time of lib.sql()."""
        self.lib.sql(self.query)

    def time_sql_query_arrow(self, query_type):
        """Execution time returning Arrow."""
        self.lib.sql(self.query, output_format="pyarrow")


class SQLLargeGroupBy:
    """
    Benchmark SQL GROUP BY on large data where the aggregation result
    fits comfortably in memory even though the source data is large.

    Uses 10M rows with various group cardinalities and aggregation types.
    The result sizes range from ~10 rows (low cardinality) to ~1M rows
    (high cardinality), all fitting in memory.
    """

    timeout = 600
    number = 5

    CONNECTION_STRING = "lmdb://sql_large_groupby"
    LIB_NAME = "sql_large_groupby"
    NUM_ROWS = 10_000_000
    SYMBOL = "benchmark_data"

    # (group_column, description)
    # id1: ~N/10 distinct values (high cardinality)
    # id6: ~N/(N/10) = ~10 distinct values (low cardinality)
    group_columns = ["id1", "id6"]
    aggregations = ["sum", "mean", "count"]

    params = [group_columns, aggregations]
    param_names = ["group_column", "aggregation"]

    def __init__(self):
        self.logger = get_logger()

    def setup_cache(self):
        start = time.time()
        self._setup_cache()
        self.logger.info(f"SETUP_CACHE TIME: {time.time() - start}")

    def _setup_cache(self):
        np.random.seed(42)
        ac = Arctic(self.CONNECTION_STRING)
        ac.delete_library(self.LIB_NAME)
        lib = ac.create_library(self.LIB_NAME)
        df = generate_benchmark_df(self.NUM_ROWS)
        lib.write(self.SYMBOL, df)

    def setup(self, group_column, aggregation):
        self.ac = Arctic(self.CONNECTION_STRING)
        self.lib = self.ac.get_library(self.LIB_NAME)
        agg_func = aggregation.upper()
        if aggregation == "count":
            agg_expr = "COUNT(*) as cnt"
        elif aggregation == "mean":
            agg_expr = f"AVG(v3) as avg_v3"
        else:
            agg_expr = f"{agg_func}(v3) as agg_v3"
        self.query = f"SELECT {group_column}, {agg_expr} FROM {self.SYMBOL} GROUP BY {group_column}"

    def teardown(self, *args):
        del self.lib
        del self.ac

    def time_groupby(self, group_column, aggregation):
        self.lib.sql(self.query)

    def peakmem_groupby(self, group_column, aggregation):
        self.lib.sql(self.query)

    def time_groupby_arrow(self, group_column, aggregation):
        """GROUP BY returning Arrow — avoids pandas conversion."""
        self.lib.sql(self.query, output_format="pyarrow")

    def peakmem_groupby_arrow(self, group_column, aggregation):
        self.lib.sql(self.query, output_format="pyarrow")


class SQLFilteringMemory:
    """
    Benchmark SQL WHERE filtering on large data, measuring both time
    and peak memory to verify that filtering pushdown keeps memory low.

    Uses increasing selectivity: 0.1%, 1%, 10%, 50% of rows pass the filter.
    Peak memory should scale roughly with the result size, not the source size,
    when pushdown is effective.
    """

    timeout = 600
    number = 5

    CONNECTION_STRING = "lmdb://sql_filtering"
    LIB_NAME = "sql_filtering"
    NUM_ROWS = 10_000_000
    SYMBOL = "benchmark_data"

    # v3 is uniform(0, 100), so v3 < X selects X% of rows
    selectivities = [0.1, 1.0, 10.0, 50.0]

    params = [selectivities]
    param_names = ["threshold_pct"]

    def __init__(self):
        self.logger = get_logger()

    def setup_cache(self):
        start = time.time()
        self._setup_cache()
        self.logger.info(f"SETUP_CACHE TIME: {time.time() - start}")

    def _setup_cache(self):
        np.random.seed(42)
        ac = Arctic(self.CONNECTION_STRING)
        ac.delete_library(self.LIB_NAME)
        lib = ac.create_library(self.LIB_NAME)
        df = generate_benchmark_df(self.NUM_ROWS)
        lib.write(self.SYMBOL, df)

    def setup(self, threshold_pct):
        self.ac = Arctic(self.CONNECTION_STRING)
        self.lib = self.ac.get_library(self.LIB_NAME)
        self.query = f"SELECT v1, v2, v3 FROM {self.SYMBOL} WHERE v3 < {threshold_pct}"

    def teardown(self, *args):
        del self.lib
        del self.ac

    def time_filter(self, threshold_pct):
        self.lib.sql(self.query)

    def peakmem_filter(self, threshold_pct):
        self.lib.sql(self.query)

    def time_filter_arrow(self, threshold_pct):
        self.lib.sql(self.query, output_format="pyarrow")

    def peakmem_filter_arrow(self, threshold_pct):
        self.lib.sql(self.query, output_format="pyarrow")


class SQLWideTableDateRange:
    """
    Benchmark SQL on wide tables with named DatetimeIndex and date_range filters.

    This represents real-world workloads like the CTA dataset (407 columns, ~1M rows)
    where SQL filters on a named DatetimeIndex must be pushed down as date_range
    to avoid reading all segments.

    Compares lib.sql() against lib.read() with date_range and QueryBuilder
    to track the overhead of the SQL/Arrow/DuckDB path.
    """

    timeout = 600
    number = 3

    CONNECTION_STRING = "lmdb://sql_wide_date_range"
    LIB_NAME = "sql_wide_date_range"
    SYMBOL = "wide_ts"
    NUM_ROWS = 1_000_000
    NUM_FLOAT_COLS = 350
    NUM_STRING_COLS = 57  # Total 407 columns, matching CTA
    DATE_LO = "2024-11-01"
    DATE_HI = "2024-12-01"
    FILTER_COL = "s0"
    FILTER_VALUE = "A"
    GROUP_COL = "s1"
    AGG_COL = "f0"

    # Benchmark full-width, projected, filter, and filter+agg queries
    query_types = ["select_star", "projection_3col", "filter", "filter_agg"]

    params = [query_types]
    param_names = ["query_type"]

    def __init__(self):
        self.logger = get_logger()

    def setup_cache(self):
        start = time.time()
        self._setup_cache()
        self.logger.info(f"SETUP_CACHE TIME: {time.time() - start}")

    def _setup_cache(self):
        np.random.seed(42)
        ac = Arctic(self.CONNECTION_STRING)
        ac.delete_library(self.LIB_NAME)
        lib = ac.create_library(self.LIB_NAME)

        rng = np.random.default_rng(42)
        dates = pd.date_range("2024-01-01", periods=self.NUM_ROWS, freq="min")
        data = {}
        for i in range(self.NUM_FLOAT_COLS):
            data[f"f{i}"] = rng.standard_normal(self.NUM_ROWS).astype(np.float64)
        cats = ["A", "B", "C", "D", "E"]
        for i in range(self.NUM_STRING_COLS):
            data[f"s{i}"] = rng.choice(cats, self.NUM_ROWS)

        df = pd.DataFrame(data, index=pd.DatetimeIndex(dates, name="Date"))
        lib.write(self.SYMBOL, df)

    def setup(self, query_type):
        from arcticdb.version_store.processing import QueryBuilder

        self.ac = Arctic(self.CONNECTION_STRING)
        self.lib = self.ac.get_library(self.LIB_NAME)
        self.date_range = (pd.Timestamp(self.DATE_LO), pd.Timestamp(self.DATE_HI))

        if query_type == "select_star":
            self.sql_query = f"SELECT * FROM {self.SYMBOL} WHERE Date >= '{self.DATE_LO}' AND Date <= '{self.DATE_HI}'"
            self.read_columns = None
            self.qb = None
        elif query_type == "projection_3col":
            self.sql_query = (
                f"SELECT f0, f1, s0 FROM {self.SYMBOL} WHERE Date >= '{self.DATE_LO}' AND Date <= '{self.DATE_HI}'"
            )
            self.read_columns = ["f0", "f1", "s0"]
            self.qb = None
        elif query_type == "filter":
            self.sql_query = (
                f"SELECT * FROM {self.SYMBOL} "
                f"WHERE Date >= '{self.DATE_LO}' AND Date <= '{self.DATE_HI}' "
                f"AND \"{self.FILTER_COL}\" = '{self.FILTER_VALUE}'"
            )
            self.read_columns = None
            q = QueryBuilder()
            self.qb = q[q[self.FILTER_COL] == self.FILTER_VALUE]
        elif query_type == "filter_agg":
            self.sql_query = (
                f'SELECT "{self.GROUP_COL}", SUM("{self.AGG_COL}") AS total '
                f"FROM {self.SYMBOL} "
                f"WHERE Date >= '{self.DATE_LO}' AND Date <= '{self.DATE_HI}' "
                f"AND \"{self.FILTER_COL}\" = '{self.FILTER_VALUE}' "
                f'GROUP BY "{self.GROUP_COL}"'
            )
            self.read_columns = None
            q = QueryBuilder()
            q = q[q[self.FILTER_COL] == self.FILTER_VALUE]
            self.qb = q.groupby(self.GROUP_COL).agg({self.AGG_COL: "sum"})

        # Warmup — ensure LMDB pages are cached
        self.lib.read(self.SYMBOL, columns=self.read_columns, date_range=self.date_range)

    def teardown(self, *args):
        del self.lib
        del self.ac

    def time_sql(self, query_type):
        """SQL query via lib.sql()."""
        self.lib.sql(self.sql_query)

    def time_read_date_range(self, query_type):
        """lib.read() with date_range — the storage-optimal path."""
        self.lib.read(self.SYMBOL, columns=self.read_columns, date_range=self.date_range, query_builder=self.qb)

    def peakmem_sql(self, query_type):
        self.lib.sql(self.sql_query)

    def peakmem_read_date_range(self, query_type):
        self.lib.read(self.SYMBOL, columns=self.read_columns, date_range=self.date_range, query_builder=self.qb)
