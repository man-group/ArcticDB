"""
Copyright 2026 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, version 2.0.
"""

import numpy as np
import pandas as pd
import pytest

duckdb = pytest.importorskip("duckdb")


def _write_dynamic_schema_symbol(lib, symbol="sym"):
    """Write a symbol where different segments have different column subsets.

    Segment 1: columns a, b
    Segment 2: columns b, c
    Result should have columns a, b, c with nulls where columns are absent.
    """
    idx1 = pd.date_range("2024-01-01", periods=5, freq="D")
    df1 = pd.DataFrame({"a": np.arange(5, dtype=np.float64), "b": np.arange(10, 15, dtype=np.float64)}, index=idx1)
    lib.write(symbol, df1)

    idx2 = pd.date_range("2024-01-06", periods=5, freq="D")
    df2 = pd.DataFrame({"b": np.arange(20, 25, dtype=np.float64), "c": np.arange(30, 35, dtype=np.float64)}, index=idx2)
    lib.append(symbol, df2)

    return df1, df2


class TestSqlDynamicSchema:
    """Tests for SQL queries on symbols with dynamic schema (different columns per segment)."""

    def test_sql_select_all(self, lmdb_library_dynamic_schema):
        """SELECT * returns all columns across all segments, with nulls for missing columns."""
        lib = lmdb_library_dynamic_schema
        df1, df2 = _write_dynamic_schema_symbol(lib)

        result = lib.sql("SELECT * FROM sym ORDER BY index")

        assert len(result) == 10
        assert set(result.columns) >= {"a", "b", "c"}
        # First 5 rows: a and b have values, c is null
        assert not result["a"].iloc[:5].isna().any()
        assert not result["b"].iloc[:5].isna().any()
        assert result["c"].iloc[:5].isna().all()
        # Last 5 rows: b and c have values, a is null
        assert result["a"].iloc[5:].isna().all()
        assert not result["b"].iloc[5:].isna().any()
        assert not result["c"].iloc[5:].isna().any()

    def test_sql_select_shared_column(self, lmdb_library_dynamic_schema):
        """Selecting a column present in all segments works correctly."""
        lib = lmdb_library_dynamic_schema
        _write_dynamic_schema_symbol(lib)

        result = lib.sql("SELECT b FROM sym")

        assert len(result) == 10
        assert list(result.columns) == ["b"]
        assert not result["b"].isna().any()

    def test_sql_select_sparse_column(self, lmdb_library_dynamic_schema):
        """Selecting a column present in only some segments returns values and nulls."""
        lib = lmdb_library_dynamic_schema
        _write_dynamic_schema_symbol(lib)

        result = lib.sql("SELECT index, a FROM sym ORDER BY index")

        assert len(result) == 10
        assert not result["a"].iloc[:5].isna().any()
        assert result["a"].iloc[5:].isna().all()

    def test_sql_filter_on_shared_column(self, lmdb_library_dynamic_schema):
        """WHERE filter on a column present in all segments works."""
        lib = lmdb_library_dynamic_schema
        _write_dynamic_schema_symbol(lib)

        result = lib.sql("SELECT * FROM sym WHERE b > 15 ORDER BY index")

        assert len(result) > 0
        assert (result["b"] > 15).all()

    def test_sql_filter_on_sparse_column(self, lmdb_library_dynamic_schema):
        """WHERE filter on a column present in only some segments doesn't crash."""
        lib = lmdb_library_dynamic_schema
        _write_dynamic_schema_symbol(lib)

        result = lib.sql("SELECT * FROM sym WHERE c > 31 ORDER BY index")

        assert len(result) > 0
        assert (result["c"] > 31).all()

    def test_sql_aggregation(self, lmdb_library_dynamic_schema):
        """GROUP BY aggregation works across dynamic schema segments."""
        lib = lmdb_library_dynamic_schema
        _write_dynamic_schema_symbol(lib)

        result = lib.sql("SELECT SUM(b) as total_b FROM sym")

        expected_b = sum(range(10, 15)) + sum(range(20, 25))
        assert result["total_b"].iloc[0] == expected_b

    def test_sql_aggregation_sparse_column(self, lmdb_library_dynamic_schema):
        """SUM on a sparse column ignores null segments."""
        lib = lmdb_library_dynamic_schema
        _write_dynamic_schema_symbol(lib)

        result = lib.sql("SELECT SUM(a) as total_a FROM sym")

        expected_a = sum(range(5))
        assert result["total_a"].iloc[0] == expected_a


class TestDuckDBContextDynamicSchema:
    """Tests for lib.duckdb() context manager with dynamic schema symbols."""

    def test_context_select_all(self, lmdb_library_dynamic_schema):
        """DuckDB context manager works with dynamic schema symbols."""
        lib = lmdb_library_dynamic_schema
        _write_dynamic_schema_symbol(lib)

        with lib.duckdb() as ctx:
            ctx.register_symbol("sym")
            result = ctx.sql("SELECT * FROM sym ORDER BY index")

        assert len(result) == 10
        assert set(result.columns) >= {"a", "b", "c"}

    def test_context_with_date_range(self, lmdb_library_dynamic_schema):
        """Date range filtering works with dynamic schema in DuckDB context."""
        lib = lmdb_library_dynamic_schema
        _write_dynamic_schema_symbol(lib)

        with lib.duckdb() as ctx:
            ctx.register_symbol("sym", date_range=(pd.Timestamp("2024-01-03"), pd.Timestamp("2024-01-08")))
            result = ctx.sql("SELECT * FROM sym ORDER BY index")

        assert len(result) > 0
        assert len(result) < 10


class TestDynamicSchemaWithStrings:
    """Tests for dynamic schema with string columns."""

    def test_sql_string_columns(self, lmdb_library_dynamic_schema):
        """Dynamic schema works with string columns."""
        lib = lmdb_library_dynamic_schema
        idx1 = pd.date_range("2024-01-01", periods=3, freq="D")
        df1 = pd.DataFrame({"name": ["alice", "bob", "carol"], "val": [1.0, 2.0, 3.0]}, index=idx1)
        lib.write("sym", df1)

        idx2 = pd.date_range("2024-01-04", periods=3, freq="D")
        df2 = pd.DataFrame({"val": [4.0, 5.0, 6.0], "tag": ["x", "y", "z"]}, index=idx2)
        lib.append("sym", df2)

        result = lib.sql("SELECT * FROM sym ORDER BY index")

        assert len(result) == 6
        assert set(result.columns) >= {"name", "val", "tag"}
        # name: values in first 3 rows, null in last 3
        assert not result["name"].iloc[:3].isna().any()
        assert result["name"].iloc[3:].isna().all()
        # tag: null in first 3 rows, values in last 3
        assert result["tag"].iloc[:3].isna().all()
        assert not result["tag"].iloc[3:].isna().any()
