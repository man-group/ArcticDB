"""
Copyright 2026 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, version 2.0.
"""

"""
Unit tests for sql_pushdown.py - SQL filter parsing and QueryBuilder generation.

These tests verify the correct transformation of SQL filter expressions to ArcticDB
QueryBuilder expressions, using pure AST parsing without requiring actual tables.
"""

import json
import pandas as pd
import pytest

duckdb = pytest.importorskip("duckdb")

from arcticdb.version_store.sql_pushdown import (
    PushdownInfo,
    _ast_to_filters,
    _build_query_builder,
    _extract_date_range,
    _extract_constant_value,
    _extract_limit_from_ast,
    extract_pushdown_from_sql,
)


def _json_serialize_sql(query: str):
    """Helper to call json_serialize_sql via a connection (works in DuckDB 1.x)."""
    conn = duckdb.connect(":memory:")
    try:
        result = conn.execute("SELECT json_serialize_sql(?)", [query]).fetchone()
        return result[0] if result else None
    finally:
        conn.close()


def _parse_where_clause(filter_expr: str):
    """Helper to parse a filter expression into AST filters.

    Wraps the filter in a dummy SELECT and extracts the WHERE clause AST.
    """
    dummy_sql = f"SELECT * FROM __t__ WHERE {filter_expr}"
    ast_json = _json_serialize_sql(dummy_sql)
    ast = json.loads(ast_json)
    where_clause = ast["statements"][0]["node"].get("where_clause")
    if where_clause:
        return _ast_to_filters(where_clause)
    return []


class TestAstToFilters:
    """Tests for _ast_to_filters - parsing filter expressions via DuckDB AST."""

    def test_simple_equality(self):
        """Test parsing simple equality filter."""
        result = _parse_where_clause("x = 10")
        assert len(result) == 1
        assert result[0]["column"] == "x"
        assert result[0]["op"] == "="
        assert result[0]["value"] == 10
        assert result[0]["type"] == "comparison"

    def test_string_equality(self):
        """Test parsing string equality filter."""
        result = _parse_where_clause("name = 'Alice'")
        assert len(result) == 1
        assert result[0]["column"] == "name"
        assert result[0]["op"] == "="
        assert result[0]["value"] == "Alice"

    def test_comparison_operators(self):
        """Test all comparison operators."""
        ops = [
            ("x > 5", ">"),
            ("x < 5", "<"),
            ("x >= 5", ">="),
            ("x <= 5", "<="),
            ("x != 5", "!="),
        ]
        for expr, expected_op in ops:
            result = _parse_where_clause(expr)
            assert len(result) == 1, f"Failed for {expr}"
            assert result[0]["op"] == expected_op, f"Failed for {expr}"

    def test_and_conjunction(self):
        """Test AND conjunction flattens to multiple filters."""
        result = _parse_where_clause("x > 5 AND y < 10")
        assert len(result) == 2
        columns = {f["column"] for f in result}
        assert columns == {"x", "y"}

    def test_or_conjunction_returns_empty(self):
        """Test OR conjunction cannot be pushed down."""
        result = _parse_where_clause("x > 5 OR y < 10")
        assert result == []

    def test_in_clause(self):
        """Test IN clause parsing."""
        result = _parse_where_clause("x IN (1, 2, 3)")
        assert len(result) == 1
        assert result[0]["column"] == "x"
        assert result[0]["op"] == "IN"
        assert result[0]["value"] == [1, 2, 3]
        assert result[0]["type"] == "membership"

    def test_not_in_clause(self):
        """Test NOT IN clause parsing."""
        result = _parse_where_clause("x NOT IN (1, 2)")
        assert len(result) == 1
        assert result[0]["op"] == "NOT IN"
        assert result[0]["value"] == [1, 2]

    def test_is_null(self):
        """Test IS NULL parsing."""
        result = _parse_where_clause("x IS NULL")
        assert len(result) == 1
        assert result[0]["column"] == "x"
        assert result[0]["op"] == "IS NULL"
        assert result[0]["type"] == "null_check"

    def test_is_not_null(self):
        """Test IS NOT NULL parsing."""
        result = _parse_where_clause("x IS NOT NULL")
        assert len(result) == 1
        assert result[0]["op"] == "IS NOT NULL"

    def test_between(self):
        """Test BETWEEN clause parsing."""
        result = _parse_where_clause("x BETWEEN 1 AND 10")
        assert len(result) == 1
        assert result[0]["column"] == "x"
        assert result[0]["op"] == "BETWEEN"
        assert result[0]["value"] == (1, 10)
        assert result[0]["type"] == "range"

    def test_timestamp_with_cast(self):
        """Test timestamp values with explicit CAST."""
        result = _parse_where_clause("ts > '2024-01-01 00:00:00'::TIMESTAMP")
        assert len(result) == 1
        assert result[0]["column"] == "ts"
        assert isinstance(result[0]["value"], pd.Timestamp)
        assert result[0]["value"] == pd.Timestamp("2024-01-01")

    def test_float_value(self):
        """Test float value parsing."""
        result = _parse_where_clause("price > 99.99")
        assert len(result) == 1
        assert result[0]["value"] == 99.99
        assert isinstance(result[0]["value"], float)

    def test_function_not_pushable(self):
        """Test that function expressions return empty (not pushable)."""
        result = _parse_where_clause("UPPER(name) = 'ALICE'")
        assert result == []

    def test_like_not_pushable(self):
        """Test that LIKE expressions return empty (not pushable)."""
        result = _parse_where_clause("name LIKE 'A%'")
        assert result == []

    def test_complex_and_chain(self):
        """Test parsing a complex AND chain."""
        result = _parse_where_clause("a > 1 AND b < 2 AND c = 3 AND d != 4")
        assert len(result) == 4
        columns = {f["column"] for f in result}
        assert columns == {"a", "b", "c", "d"}


class TestBuildQueryBuilder:
    """Tests for _build_query_builder - converting parsed filters to QueryBuilder."""

    def test_empty_filters_returns_none(self):
        """Test that empty filter list returns None."""
        result = _build_query_builder([])
        assert result is None

    def test_single_equality(self):
        """Test building QueryBuilder for single equality."""
        filters = [{"column": "x", "op": "=", "value": 10, "type": "comparison"}]
        qb = _build_query_builder(filters)
        assert qb is not None

    def test_all_comparison_ops(self):
        """Test all comparison operators build correctly."""
        ops = ["=", "!=", "<", ">", "<=", ">="]
        for op in ops:
            filters = [{"column": "x", "op": op, "value": 5, "type": "comparison"}]
            qb = _build_query_builder(filters)
            assert qb is not None, f"Failed for op {op}"

    def test_in_membership(self):
        """Test IN membership builds correctly."""
        filters = [{"column": "x", "op": "IN", "value": [1, 2, 3], "type": "membership"}]
        qb = _build_query_builder(filters)
        assert qb is not None

    def test_not_in_membership(self):
        """Test NOT IN membership builds correctly."""
        filters = [{"column": "x", "op": "NOT IN", "value": [1, 2], "type": "membership"}]
        qb = _build_query_builder(filters)
        assert qb is not None

    def test_is_null(self):
        """Test IS NULL builds correctly."""
        filters = [{"column": "x", "op": "IS NULL", "value": None, "type": "null_check"}]
        qb = _build_query_builder(filters)
        assert qb is not None

    def test_is_not_null(self):
        """Test IS NOT NULL builds correctly."""
        filters = [{"column": "x", "op": "IS NOT NULL", "value": None, "type": "null_check"}]
        qb = _build_query_builder(filters)
        assert qb is not None

    def test_range_between(self):
        """Test BETWEEN range builds correctly."""
        filters = [{"column": "x", "op": "BETWEEN", "value": (1, 10), "type": "range"}]
        qb = _build_query_builder(filters)
        assert qb is not None

    def test_multiple_filters_combined_with_and(self):
        """Test multiple filters are combined with AND."""
        filters = [
            {"column": "x", "op": ">", "value": 5, "type": "comparison"},
            {"column": "y", "op": "<", "value": 10, "type": "comparison"},
        ]
        qb = _build_query_builder(filters)
        assert qb is not None

    def test_unknown_type_skipped(self):
        """Test unknown filter types are skipped."""
        filters = [
            {"column": "x", "op": "UNKNOWN", "value": 5, "type": "unknown"},
            {"column": "y", "op": "=", "value": 10, "type": "comparison"},
        ]
        qb = _build_query_builder(filters)
        assert qb is not None  # Should still build with the valid filter


class TestExtractDateRange:
    """Tests for _extract_date_range - extracting date ranges from index filters."""

    def test_no_index_filters(self):
        """Test with no index column filters."""
        filters = [{"column": "x", "op": ">", "value": 5, "type": "comparison"}]
        date_range, remaining = _extract_date_range(filters)
        assert date_range is None
        assert remaining == filters

    def test_index_between(self):
        """Test BETWEEN on index column extracts date range."""
        start = pd.Timestamp("2024-01-01")
        end = pd.Timestamp("2024-01-31")
        filters = [{"column": "index", "op": "BETWEEN", "value": (start, end), "type": "range"}]
        date_range, remaining = _extract_date_range(filters)
        assert date_range == (start, end)
        assert remaining == []

    def test_index_gte(self):
        """Test >= on index column extracts start of date range."""
        start = pd.Timestamp("2024-01-01")
        filters = [{"column": "index", "op": ">=", "value": start, "type": "comparison"}]
        date_range, remaining = _extract_date_range(filters)
        assert date_range == (start, None)
        assert remaining == []

    def test_index_lte(self):
        """Test <= on index column extracts end of date range."""
        end = pd.Timestamp("2024-01-31")
        filters = [{"column": "index", "op": "<=", "value": end, "type": "comparison"}]
        date_range, remaining = _extract_date_range(filters)
        assert date_range == (None, end)
        assert remaining == []

    def test_index_range_from_two_comparisons(self):
        """Test combining >= and <= on index column."""
        start = pd.Timestamp("2024-01-01")
        end = pd.Timestamp("2024-01-31")
        filters = [
            {"column": "index", "op": ">=", "value": start, "type": "comparison"},
            {"column": "index", "op": "<=", "value": end, "type": "comparison"},
        ]
        date_range, remaining = _extract_date_range(filters)
        assert date_range == (start, end)
        assert remaining == []

    def test_mixed_index_and_other_filters(self):
        """Test index filters extracted while others remain."""
        start = pd.Timestamp("2024-01-01")
        filters = [
            {"column": "index", "op": ">=", "value": start, "type": "comparison"},
            {"column": "x", "op": ">", "value": 5, "type": "comparison"},
        ]
        date_range, remaining = _extract_date_range(filters)
        assert date_range == (start, None)
        assert len(remaining) == 1
        assert remaining[0]["column"] == "x"

    def test_index_case_insensitive(self):
        """Test that 'INDEX' column name is case-insensitive."""
        start = pd.Timestamp("2024-01-01")
        filters = [{"column": "INDEX", "op": ">=", "value": start, "type": "comparison"}]
        date_range, remaining = _extract_date_range(filters)
        assert date_range == (start, None)


class TestExtractLimitFromAst:
    """Tests for _extract_limit_from_ast - extracting LIMIT via AST."""

    def test_simple_limit(self):
        """Test extracting simple LIMIT."""
        ast = json.loads(_json_serialize_sql("SELECT * FROM t LIMIT 10"))
        result = _extract_limit_from_ast(ast)
        assert result == 10

    def test_limit_with_offset(self):
        """Test extracting LIMIT when OFFSET is present."""
        ast = json.loads(_json_serialize_sql("SELECT * FROM t LIMIT 10 OFFSET 5"))
        result = _extract_limit_from_ast(ast)
        assert result == 10

    def test_no_limit(self):
        """Test query without LIMIT returns None."""
        ast = json.loads(_json_serialize_sql("SELECT * FROM t"))
        result = _extract_limit_from_ast(ast)
        assert result is None

    def test_limit_in_string_not_extracted(self):
        """Test that LIMIT in string literal is not incorrectly extracted."""
        ast = json.loads(_json_serialize_sql(
            "SELECT * FROM t WHERE name = 'LIMIT 999' LIMIT 5"
        ))
        result = _extract_limit_from_ast(ast)
        assert result == 5


class TestExtractConstantValue:
    """Tests for _extract_constant_value - extracting values from AST nodes."""

    def test_integer_constant(self):
        """Test extracting integer constant."""
        node = {
            "class": "CONSTANT",
            "type": "VALUE_CONSTANT",
            "value": {"type": {"id": "INTEGER"}, "is_null": False, "value": 42},
        }
        assert _extract_constant_value(node) == 42

    def test_float_constant(self):
        """Test extracting float constant."""
        node = {
            "class": "CONSTANT",
            "type": "VALUE_CONSTANT",
            "value": {"type": {"id": "DOUBLE"}, "is_null": False, "value": 3.14},
        }
        assert _extract_constant_value(node) == 3.14

    def test_string_constant(self):
        """Test extracting string constant."""
        node = {
            "class": "CONSTANT",
            "type": "VALUE_CONSTANT",
            "value": {"type": {"id": "VARCHAR"}, "is_null": False, "value": "hello"},
        }
        assert _extract_constant_value(node) == "hello"

    def test_boolean_constant(self):
        """Test extracting boolean constant."""
        node = {
            "class": "CONSTANT",
            "type": "VALUE_CONSTANT",
            "value": {"type": {"id": "BOOLEAN"}, "is_null": False, "value": True},
        }
        assert _extract_constant_value(node) is True

    def test_null_constant(self):
        """Test extracting null constant."""
        node = {
            "class": "CONSTANT",
            "type": "VALUE_CONSTANT",
            "value": {"type": {"id": "INTEGER"}, "is_null": True, "value": None},
        }
        assert _extract_constant_value(node) is None

    def test_timestamp_constant(self):
        """Test extracting timestamp constant."""
        node = {
            "class": "CONSTANT",
            "type": "VALUE_CONSTANT",
            "value": {
                "type": {"id": "TIMESTAMP"},
                "is_null": False,
                "value": "2024-01-01 00:00:00",
            },
        }
        result = _extract_constant_value(node)
        assert isinstance(result, pd.Timestamp)
        assert result == pd.Timestamp("2024-01-01")

    def test_cast_to_timestamp(self):
        """Test extracting value with CAST to timestamp."""
        node = {
            "class": "CAST",
            "type": "OPERATOR_CAST",
            "child": {
                "class": "CONSTANT",
                "type": "VALUE_CONSTANT",
                "value": {"type": {"id": "VARCHAR"}, "is_null": False, "value": "2024-01-01"},
            },
            "cast_type": {"id": "TIMESTAMP_NS"},
        }
        result = _extract_constant_value(node)
        assert isinstance(result, pd.Timestamp)

    def test_cast_to_integer(self):
        """Test extracting value with CAST to integer."""
        node = {
            "class": "CAST",
            "type": "OPERATOR_CAST",
            "child": {
                "class": "CONSTANT",
                "type": "VALUE_CONSTANT",
                "value": {"type": {"id": "VARCHAR"}, "is_null": False, "value": "42"},
            },
            "cast_type": {"id": "INTEGER"},
        }
        assert _extract_constant_value(node) == 42

    def test_non_constant_returns_none(self):
        """Test non-constant node returns None."""
        node = {"class": "COLUMN_REF", "type": "COLUMN_REF"}
        assert _extract_constant_value(node) is None


class TestExtractPushdownFromSql:
    """Tests for extract_pushdown_from_sql - pure AST-based pushdown extraction."""

    def test_returns_info_for_requested_tables(self):
        """Test that PushdownInfo is returned for each requested table."""
        result = extract_pushdown_from_sql(
            "SELECT * FROM test_table", ["test_table"]
        )
        assert "test_table" in result
        assert isinstance(result["test_table"], PushdownInfo)

    def test_limit_extracted_from_query(self):
        """Test LIMIT is extracted from query."""
        result = extract_pushdown_from_sql(
            "SELECT x FROM test_table LIMIT 10", ["test_table"]
        )
        info = result["test_table"]
        assert info.limit == 10
        assert info.limit_pushed_down == 10

    def test_unknown_table_returns_default(self):
        """Test unknown table returns default PushdownInfo with LIMIT still applied."""
        result = extract_pushdown_from_sql(
            "SELECT * FROM test_table LIMIT 5", ["unknown_table"]
        )
        assert "unknown_table" in result
        info = result["unknown_table"]
        # LIMIT still applies to all requested tables
        assert info.limit == 5

    def test_multiple_tables(self):
        """Test extracting pushdown for multiple tables."""
        result = extract_pushdown_from_sql(
            "SELECT * FROM test_table, other_table LIMIT 5",
            ["test_table", "other_table"]
        )
        assert "test_table" in result
        assert "other_table" in result
        # LIMIT applies to both
        assert result["test_table"].limit == 5
        assert result["other_table"].limit == 5

    def test_where_filter_pushdown(self):
        """Test WHERE clause filter is pushed down."""
        result = extract_pushdown_from_sql(
            "SELECT * FROM test_table WHERE x > 100", ["test_table"]
        )
        info = result["test_table"]
        assert info.filter_pushed_down is True
        assert info.query_builder is not None

    def test_date_range_pushdown(self):
        """Test date range from index filter is pushed down."""
        result = extract_pushdown_from_sql(
            "SELECT * FROM test_table WHERE index BETWEEN '2024-01-01' AND '2024-12-31'",
            ["test_table"]
        )
        info = result["test_table"]
        assert info.date_range_pushed_down is True
        assert info.date_range is not None
        assert info.date_range[0] == pd.Timestamp("2024-01-01")
        assert info.date_range[1] == pd.Timestamp("2024-12-31")

    def test_column_projection_pushdown(self):
        """Test column projection is pushed down."""
        result = extract_pushdown_from_sql(
            "SELECT x, y FROM test_table", ["test_table"]
        )
        info = result["test_table"]
        assert info.columns_pushed_down is not None
        assert set(info.columns_pushed_down) == {"x", "y"}

    def test_select_star_no_column_pushdown(self):
        """Test SELECT * doesn't push down column projection."""
        result = extract_pushdown_from_sql(
            "SELECT * FROM test_table", ["test_table"]
        )
        info = result["test_table"]
        # SELECT * means no specific column projection
        assert info.columns is None

    def test_join_query(self):
        """Test pushdown extraction for JOIN query."""
        result = extract_pushdown_from_sql(
            "SELECT a.x, b.y FROM table_a a JOIN table_b b ON a.id = b.id LIMIT 10",
            ["table_a", "table_b"]
        )
        assert "table_a" in result
        assert "table_b" in result
        assert result["table_a"].limit == 10
        assert result["table_b"].limit == 10


class TestPushdownInfoDataclass:
    """Tests for PushdownInfo dataclass."""

    def test_default_values(self):
        """Test PushdownInfo default values."""
        info = PushdownInfo()
        assert info.columns is None
        assert info.query_builder is None
        assert info.limit is None
        assert info.date_range is None
        assert info.filter_pushed_down is False
        assert info.columns_pushed_down is None
        assert info.limit_pushed_down is None
        assert info.date_range_pushed_down is False
        assert info.unpushed_filters == []

    def test_with_values(self):
        """Test PushdownInfo with values."""
        info = PushdownInfo(
            columns=["x", "y"],
            limit=10,
            date_range=(pd.Timestamp("2024-01-01"), pd.Timestamp("2024-12-31")),
        )
        assert info.columns == ["x", "y"]
        assert info.limit == 10
        assert info.date_range[0] == pd.Timestamp("2024-01-01")
