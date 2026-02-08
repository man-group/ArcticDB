"""
Copyright 2026 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, version 2.0.
"""

"""
Unit tests for duckdb/pushdown.py - SQL filter parsing and QueryBuilder generation.

These tests verify the correct transformation of SQL filter expressions to ArcticDB
QueryBuilder expressions, using pure AST parsing without requiring actual tables.
"""

import json
import numpy as np
import pandas as pd
import pytest

duckdb = pytest.importorskip("duckdb")

from arcticdb.version_store.duckdb.pushdown import (
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

    def test_iso_date_string_auto_converts_to_timestamp(self):
        """Test that ISO date strings are automatically converted to Timestamp.

        Users expect `WHERE ts < '2024-01-03'` to work the same as
        `WHERE ts < TIMESTAMP '2024-01-03'`. The pushdown code should
        detect ISO date patterns (YYYY-MM-DD) and auto-convert.
        """
        result = _parse_where_clause("ts < '2024-01-03'")
        assert len(result) == 1
        assert isinstance(result[0]["value"], pd.Timestamp)
        assert result[0]["value"] == pd.Timestamp("2024-01-03")

    def test_iso_datetime_string_auto_converts_to_timestamp(self):
        """Test that ISO datetime strings with time component auto-convert."""
        result = _parse_where_clause("ts >= '2024-01-02 09:30:00'")
        assert len(result) == 1
        assert isinstance(result[0]["value"], pd.Timestamp)
        assert result[0]["value"] == pd.Timestamp("2024-01-02 09:30:00")

    def test_non_date_string_stays_as_string(self):
        """Test that regular string values are NOT auto-converted."""
        result = _parse_where_clause("type = 'call'")
        assert len(result) == 1
        assert isinstance(result[0]["value"], str)
        assert result[0]["value"] == "call"

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
        ast = json.loads(_json_serialize_sql("SELECT * FROM t WHERE name = 'LIMIT 999' LIMIT 5"))
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
        result, symbols = extract_pushdown_from_sql("SELECT * FROM test_table", ["test_table"])
        assert "test_table" in result
        assert isinstance(result["test_table"], PushdownInfo)
        assert "test_table" in symbols

    def test_limit_extracted_from_query(self):
        """Test LIMIT is extracted from query."""
        result, _ = extract_pushdown_from_sql("SELECT x FROM test_table LIMIT 10", ["test_table"])
        info = result["test_table"]
        assert info.limit == 10
        assert info.limit_pushed_down == 10

    def test_limit_not_pushed_with_order_by(self):
        """LIMIT + ORDER BY: DuckDB needs all rows to sort, so LIMIT cannot be pushed to storage."""
        result, _ = extract_pushdown_from_sql("SELECT x FROM test_table ORDER BY x LIMIT 10", ["test_table"])
        assert result["test_table"].limit is None
        assert result["test_table"].limit_pushed_down is None

    def test_limit_not_pushed_with_group_by(self):
        """LIMIT + GROUP BY: LIMIT applies to aggregated result, not source rows."""
        result, _ = extract_pushdown_from_sql("SELECT x, COUNT(*) FROM test_table GROUP BY x LIMIT 10", ["test_table"])
        assert result["test_table"].limit is None
        assert result["test_table"].limit_pushed_down is None

    def test_limit_not_pushed_with_distinct(self):
        """LIMIT + DISTINCT: LIMIT applies to deduplicated result, not source rows."""
        result, _ = extract_pushdown_from_sql("SELECT DISTINCT x FROM test_table LIMIT 10", ["test_table"])
        assert result["test_table"].limit is None
        assert result["test_table"].limit_pushed_down is None

    def test_unknown_table_returns_default(self):
        """Test unknown table returns default PushdownInfo with LIMIT still applied."""
        result, _ = extract_pushdown_from_sql("SELECT * FROM test_table LIMIT 5", ["unknown_table"])
        assert "unknown_table" in result
        info = result["unknown_table"]
        # LIMIT still applies to all requested tables
        assert info.limit == 5

    def test_multiple_tables(self):
        """Test extracting pushdown for multiple tables."""
        result, _ = extract_pushdown_from_sql(
            "SELECT * FROM test_table, other_table LIMIT 5", ["test_table", "other_table"]
        )
        assert "test_table" in result
        assert "other_table" in result
        # LIMIT is NOT pushed down for multi-table queries — it applies to
        # the joined result, not individual tables
        assert result["test_table"].limit is None
        assert result["other_table"].limit is None

    def test_where_filter_pushdown(self):
        """Test WHERE clause filter is pushed down."""
        result, _ = extract_pushdown_from_sql("SELECT * FROM test_table WHERE x > 100", ["test_table"])
        info = result["test_table"]
        assert info.filter_pushed_down is True
        assert info.query_builder is not None

    def test_date_range_pushdown(self):
        """Test date range from index filter is pushed down."""
        result, _ = extract_pushdown_from_sql(
            "SELECT * FROM test_table WHERE index BETWEEN '2024-01-01' AND '2024-12-31'", ["test_table"]
        )
        info = result["test_table"]
        assert info.date_range_pushed_down is True
        assert info.date_range is not None
        assert info.date_range[0] == pd.Timestamp("2024-01-01")
        assert info.date_range[1] == pd.Timestamp("2024-12-31")

    def test_column_projection_pushdown(self):
        """Test column projection is pushed down."""
        result, _ = extract_pushdown_from_sql("SELECT x, y FROM test_table", ["test_table"])
        info = result["test_table"]
        assert info.columns_pushed_down is not None
        assert set(info.columns_pushed_down) == {"x", "y"}

    def test_select_star_no_column_pushdown(self):
        """Test SELECT * doesn't push down column projection."""
        result, _ = extract_pushdown_from_sql("SELECT * FROM test_table", ["test_table"])
        info = result["test_table"]
        # SELECT * means no specific column projection
        assert info.columns is None

    def test_join_query(self):
        """Test pushdown extraction for JOIN query."""
        result, symbols = extract_pushdown_from_sql(
            "SELECT a.x, b.y FROM table_a a JOIN table_b b ON a.id = b.id LIMIT 10", ["table_a", "table_b"]
        )
        assert "table_a" in result
        assert "table_b" in result
        # LIMIT is NOT pushed down for multi-table (JOIN) queries — it applies
        # to the joined result, not individual tables
        assert result["table_a"].limit is None
        assert result["table_b"].limit is None
        assert "table_a" in symbols
        assert "table_b" in symbols

    def test_extracts_symbols_when_none_provided(self):
        """Test that symbols are extracted from query when table_names is None."""
        result, symbols = extract_pushdown_from_sql("SELECT * FROM my_symbol")
        assert symbols == ["my_symbol"]
        assert "my_symbol" in result

    def test_raises_on_empty_sql(self):
        """Test that ValueError is raised for empty SQL (no tables)."""
        # Empty string is parseable but has no tables
        with pytest.raises(ValueError, match="Could not extract symbol names"):
            extract_pushdown_from_sql("")

    def test_raises_on_invalid_sql(self):
        """Test that ValueError is raised for invalid SQL syntax."""
        with pytest.raises(ValueError, match="Could not parse SQL query"):
            extract_pushdown_from_sql("SELECT * FORM invalid_syntax")

    def test_raises_on_no_tables(self):
        """Test that ValueError is raised when no tables in query."""
        with pytest.raises(ValueError, match="Could not extract symbol names"):
            extract_pushdown_from_sql("SELECT 1 + 1")

    def test_cte_extracts_real_tables_not_cte_names(self):
        """Test that CTE aliases are excluded from extracted symbols."""
        _, symbols = extract_pushdown_from_sql(
            "WITH filtered AS (SELECT * FROM trades WHERE price > 100) " "SELECT ticker FROM filtered GROUP BY ticker"
        )
        assert "trades" in symbols
        assert "filtered" not in symbols

    def test_cte_with_multiple_real_tables(self):
        """Test CTE referencing multiple real tables."""
        _, symbols = extract_pushdown_from_sql(
            "WITH t AS (SELECT * FROM trades), p AS (SELECT * FROM prices) "
            "SELECT * FROM t JOIN p ON t.ticker = p.ticker"
        )
        assert "trades" in symbols
        assert "prices" in symbols
        assert "t" not in symbols
        assert "p" not in symbols

    def test_nested_cte(self):
        """Test nested CTEs don't leak alias names as symbols."""
        _, symbols = extract_pushdown_from_sql(
            "WITH step1 AS (SELECT * FROM raw_data), "
            "step2 AS (SELECT * FROM step1 WHERE x > 0) "
            "SELECT * FROM step2"
        )
        assert "raw_data" in symbols
        assert "step1" not in symbols
        assert "step2" not in symbols


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


# =============================================================================
# Integration tests for SQL predicate pushdown with actual Library operations
# =============================================================================


class TestSQLPredicatePushdown:
    """Tests for SQL predicate pushdown to ArcticDB."""

    def test_column_projection_pushdown(self, lmdb_library):
        """Test that SELECT columns are pushed down to ArcticDB."""
        lib = lmdb_library
        df = pd.DataFrame(
            {
                "a": np.arange(100),
                "b": np.arange(100, 200),
                "c": np.arange(200, 300),
                "d": np.arange(300, 400),
            }
        )
        lib.write("test_symbol", df)

        # Query only columns a and b - should only read those from storage
        data = lib.sql("SELECT a, b FROM test_symbol")

        assert len(data) == 100
        assert list(data.columns) == ["a", "b"]
        # Verify pushdown happened by checking metadata
        info = lib.explain("SELECT a, b FROM test_symbol")
        assert "columns_pushed_down" in info
        assert set(info["columns_pushed_down"]) == {"a", "b"}

    def test_where_comparison_pushdown(self, lmdb_library):
        """Test that simple WHERE comparisons are pushed down."""
        lib = lmdb_library
        df = pd.DataFrame({"x": np.arange(1000), "y": np.arange(1000, 2000)})
        lib.write("test_symbol", df)

        # WHERE x > 900 should be pushed down
        data = lib.sql("SELECT x, y FROM test_symbol WHERE x > 900")

        assert len(data) == 99  # 901-999
        assert data["x"].min() > 900
        info = lib.explain("SELECT x, y FROM test_symbol WHERE x > 900")
        assert "filter_pushed_down" in info
        assert info["filter_pushed_down"] is True

    def test_where_multiple_conditions_pushdown(self, lmdb_library):
        """Test that AND/OR conditions are pushed down."""
        lib = lmdb_library
        df = pd.DataFrame(
            {
                "x": np.arange(100),
                "y": np.arange(100, 200),
            }
        )
        lib.write("test_symbol", df)

        # Multiple conditions with AND
        result = lib.sql("SELECT x, y FROM test_symbol WHERE x > 50 AND y < 180")

        assert len(result) == 29  # x: 51-79, y: 151-179
        assert result["x"].min() > 50
        assert result["y"].max() < 180

    def test_where_in_clause_pushdown(self, lmdb_library):
        """Test that IN clause is pushed down."""
        lib = lmdb_library
        df = pd.DataFrame(
            {
                "category": ["A", "B", "C", "D", "E"] * 20,
                "value": np.arange(100),
            }
        )
        lib.write("test_symbol", df)

        result = lib.sql("SELECT category, value FROM test_symbol WHERE category IN ('A', 'C')")

        assert len(result) == 40  # 20 A's + 20 C's
        assert set(result["category"].unique()) == {"A", "C"}

    def test_where_is_null_pushdown(self, lmdb_library):
        """Test that IS NULL is pushed down."""
        lib = lmdb_library
        df = pd.DataFrame(
            {
                "x": [1, 2, None, 4, None, 6, 7, None, 9, 10],
                "y": np.arange(10),
            }
        )
        lib.write("test_symbol", df)

        result = lib.sql("SELECT x, y FROM test_symbol WHERE x IS NOT NULL")

        assert len(result) == 7  # 10 - 3 nulls

    def test_limit_pushdown(self, lmdb_library):
        """Test that LIMIT is pushed down as head()."""
        lib = lmdb_library
        df = pd.DataFrame({"x": np.arange(1000)})
        lib.write("test_symbol", df)

        data = lib.sql("SELECT x FROM test_symbol LIMIT 10")

        assert len(data) == 10
        # Verify first 10 rows returned (storage order preserved)
        assert list(data["x"]) == list(range(10))
        info = lib.explain("SELECT x FROM test_symbol LIMIT 10")
        assert "limit_pushed_down" in info
        assert info["limit_pushed_down"] == 10

    def test_limit_with_order_by_not_pushed(self, lmdb_library):
        """LIMIT + ORDER BY: LIMIT is not pushed to storage but result is still correct."""
        lib = lmdb_library
        df = pd.DataFrame({"x": np.arange(1000)})
        lib.write("test_symbol", df)

        data = lib.sql("SELECT x FROM test_symbol ORDER BY x DESC LIMIT 5")
        assert len(data) == 5
        assert list(data["x"]) == [999, 998, 997, 996, 995]

        info = lib.explain("SELECT x FROM test_symbol ORDER BY x DESC LIMIT 5")
        # LIMIT is NOT pushed when ORDER BY is present
        assert info.get("limit_pushed_down") is None

    def test_limit_with_group_by_not_pushed(self, lmdb_library):
        """LIMIT + GROUP BY: LIMIT is not pushed to storage but result is still correct."""
        lib = lmdb_library
        df = pd.DataFrame({"category": ["A", "B", "C"] * 100, "value": np.arange(300)})
        lib.write("test_symbol", df)

        data = lib.sql("SELECT category, SUM(value) as total FROM test_symbol GROUP BY category LIMIT 2")
        assert len(data) == 2

        info = lib.explain("SELECT category, SUM(value) as total FROM test_symbol GROUP BY category LIMIT 2")
        assert info.get("limit_pushed_down") is None

    def test_date_range_pushdown_between(self, lmdb_library):
        """Test that BETWEEN on timestamp index is pushed down as date_range."""
        lib = lmdb_library
        dates = pd.date_range("2024-01-01", periods=365, freq="D")
        df = pd.DataFrame({"value": np.arange(365)}, index=dates)
        lib.write("test_symbol", df)

        # Query for January only using BETWEEN on index
        data = lib.sql("""
            SELECT value FROM test_symbol
            WHERE index BETWEEN '2024-01-01' AND '2024-01-31'
        """)

        assert len(data) == 31
        info = lib.explain("""
            SELECT value FROM test_symbol
            WHERE index BETWEEN '2024-01-01' AND '2024-01-31'
        """)
        assert "date_range_pushed_down" in info

    def test_combined_pushdown(self, lmdb_library):
        """Test combining column projection, WHERE, and LIMIT pushdown."""
        lib = lmdb_library
        df = pd.DataFrame(
            {
                "a": np.arange(1000),
                "b": np.arange(1000, 2000),
                "c": np.arange(2000, 3000),
            }
        )
        lib.write("test_symbol", df)

        result = lib.sql("SELECT a, b FROM test_symbol WHERE a > 500 LIMIT 50")

        assert len(result) == 50
        assert list(result.columns) == ["a", "b"]
        assert result["a"].min() > 500

    def test_pushdown_with_aggregation(self, lmdb_library):
        """Test that filters are pushed down even with aggregation."""
        lib = lmdb_library
        df = pd.DataFrame(
            {
                "category": ["A", "B", "C"] * 100,
                "value": np.arange(300),
            }
        )
        lib.write("test_symbol", df)

        # Filter should be pushed to ArcticDB, aggregation done by DuckDB
        result = lib.sql("""
            SELECT category, SUM(value) as total
            FROM test_symbol
            WHERE value > 100
            GROUP BY category
        """)

        assert len(result) == 3  # Still 3 categories

    def test_pushdown_preserves_correctness(self, lmdb_library):
        """Test that pushdown produces same results as non-pushdown."""
        lib = lmdb_library
        df = pd.DataFrame(
            {
                "x": np.arange(500),
                "y": np.random.randn(500),
            }
        )
        lib.write("test_symbol", df)

        # Get result with pushdown
        result_pushdown = lib.sql("SELECT x, y FROM test_symbol WHERE x > 200 AND x < 300")

        # Get result without pushdown (full read + DuckDB filter)
        full_data = lib.read("test_symbol").data
        expected = full_data[(full_data["x"] > 200) & (full_data["x"] < 300)][["x", "y"]]

        pd.testing.assert_frame_equal(
            result_pushdown.reset_index(drop=True),
            expected.reset_index(drop=True),
        )

    def test_unsupported_predicate_not_pushed(self, lmdb_library):
        """Test that unsupported predicates fall back to DuckDB filtering."""
        lib = lmdb_library
        df = pd.DataFrame(
            {
                "name": ["alice", "bob", "charlie", "david"],
                "value": [1, 2, 3, 4],
            }
        )
        lib.write("test_symbol", df)

        # LIKE is not directly supported by ArcticDB QueryBuilder
        # Should still work via DuckDB
        result = lib.sql("SELECT name, value FROM test_symbol WHERE name LIKE 'a%'")

        assert len(result) == 1
        assert result["name"].iloc[0] == "alice"

    def test_date_range_pushdown_extreme_dates(self, lmdb_library):
        """Test that date range pushdown works for dates across the full pandas range.

        Pandas Timestamp supports dates from 1677 to 2262. This test verifies
        pushdown works for historical and futuristic dates outside typical ranges.
        """
        lib = lmdb_library

        # Test historical data (1850)
        dates = pd.date_range("1850-01-01", periods=365, freq="D")
        df = pd.DataFrame({"value": np.arange(365)}, index=dates)
        lib.write("historical", df)

        data = lib.sql("""
            SELECT value FROM historical
            WHERE index BETWEEN '1850-01-01' AND '1850-01-31'
        """)
        assert len(data) == 31
        info = lib.explain("""
            SELECT value FROM historical
            WHERE index BETWEEN '1850-01-01' AND '1850-01-31'
        """)
        assert "date_range_pushed_down" in info

        # Test futuristic data (2150)
        dates = pd.date_range("2150-01-01", periods=365, freq="D")
        df = pd.DataFrame({"value": np.arange(365)}, index=dates)
        lib.write("futuristic", df)

        data = lib.sql("""
            SELECT value FROM futuristic
            WHERE index BETWEEN '2150-01-01' AND '2150-01-31'
        """)
        assert len(data) == 31
        info = lib.explain("""
            SELECT value FROM futuristic
            WHERE index BETWEEN '2150-01-01' AND '2150-01-31'
        """)
        assert "date_range_pushed_down" in info


class TestSQLPushdownEdgeCases:
    """Tests for edge cases and limitations of SQL pushdown."""

    def test_unsigned_integer_types(self, lmdb_library):
        """Test that unsigned integer columns work correctly with SQL queries."""
        lib = lmdb_library
        df = pd.DataFrame(
            {
                "u8": np.array([1, 2, 3], dtype=np.uint8),
                "u16": np.array([100, 200, 300], dtype=np.uint16),
                "u32": np.array([1000, 2000, 3000], dtype=np.uint32),
                "u64": np.array([10000, 20000, 30000], dtype=np.uint64),
            }
        )
        lib.write("uint_test", df)

        # Should not crash and return correct results
        result = lib.sql("SELECT u8, u16, u32, u64 FROM uint_test WHERE u32 > 1500")
        assert len(result) == 2
        assert result["u32"].min() > 1500

    def test_small_integer_types(self, lmdb_library):
        """Test that small integer types (int8, int16) work correctly."""
        lib = lmdb_library
        df = pd.DataFrame(
            {
                "i8": np.array([-50, 0, 50], dtype=np.int8),
                "i16": np.array([-1000, 0, 1000], dtype=np.int16),
            }
        )
        lib.write("small_int_test", df)

        result = lib.sql("SELECT i8, i16 FROM small_int_test WHERE i8 > 0")
        assert len(result) == 1
        assert result["i8"].iloc[0] == 50

    def test_filter_outside_pushdown_range_still_works(self, lmdb_library):
        """Test that filters outside dummy data range still return correct results.

        When filter values are outside the dummy data range used for plan analysis,
        pushdown may not occur, but the query should still return correct results
        via DuckDB filtering.
        """
        lib = lmdb_library
        # Values far outside the typical dummy range
        df = pd.DataFrame({"x": [5_000_000_000, 6_000_000_000, 7_000_000_000]})
        lib.write("big_values", df)

        result = lib.sql("SELECT x FROM big_values WHERE x > 5500000000")
        assert len(result) == 2  # Correct result even if not pushed down

    def test_or_predicate_works_via_duckdb(self, lmdb_library):
        """Test that OR predicates work correctly (handled by DuckDB, not pushed)."""
        lib = lmdb_library
        df = pd.DataFrame({"x": [1, 2, 3, 4, 5]})
        lib.write("or_test", df)

        data = lib.sql("SELECT x FROM or_test WHERE x = 1 OR x = 5")
        assert len(data) == 2
        assert set(data["x"]) == {1, 5}
        # OR predicates are not pushed down to ArcticDB
        info = lib.explain("SELECT x FROM or_test WHERE x = 1 OR x = 5")
        assert "filter_pushed_down" not in info

    def test_like_predicate_works_via_duckdb(self, lmdb_library):
        """Test that LIKE predicates work correctly (handled by DuckDB, not pushed)."""
        lib = lmdb_library
        df = pd.DataFrame({"name": ["apple", "banana", "apricot", "cherry"]})
        lib.write("like_test", df)

        result = lib.sql("SELECT name FROM like_test WHERE name LIKE 'ap%'")
        assert len(result) == 2
        assert set(result["name"]) == {"apple", "apricot"}

    def test_function_in_predicate_works_via_duckdb(self, lmdb_library):
        """Test that function predicates work correctly (handled by DuckDB, not pushed)."""
        lib = lmdb_library
        df = pd.DataFrame({"name": ["Apple", "Banana", "APPLE"]})
        lib.write("func_test", df)

        result = lib.sql("SELECT name FROM func_test WHERE UPPER(name) = 'APPLE'")
        assert len(result) == 2
        assert set(result["name"]) == {"Apple", "APPLE"}

    def test_limit_in_string_literal_not_confused(self, lmdb_library):
        """Test that LIMIT in a string literal doesn't confuse the LIMIT extraction.

        Regex-based parsing would incorrectly extract 999 from the string literal.
        Using DuckDB's AST parser ensures only the actual LIMIT clause is extracted.
        """
        lib = lmdb_library
        df = pd.DataFrame(
            {
                "description": ["LIMIT 999 items", "Normal text", "Another row"] * 10,
                "value": np.arange(30),
            }
        )
        lib.write("limit_string_test", df)

        data = lib.sql("""
            SELECT description, value FROM limit_string_test
            WHERE description LIKE '%LIMIT%'
            LIMIT 5
        """)

        assert len(data) == 5
        info = lib.explain("""
            SELECT description, value FROM limit_string_test
            WHERE description LIKE '%LIMIT%'
            LIMIT 5
        """)
        # LIMIT is not pushed when WHERE clause is present (WHERE may reduce
        # rows below LIMIT, so storage can't know how many rows to read).
        # The key test here is that the string literal "LIMIT 999" doesn't
        # confuse the parser — the query still works correctly.
        assert info.get("limit_pushed_down") is None


class TestReadOnlyValidation:
    """Tests that non-SELECT statements are rejected via DuckDB's AST parser.

    DuckDB's json_serialize_sql() only accepts SELECT-like statements. Non-SELECT
    statements produce an error that _get_sql_ast_or_raise translates into a clear
    ValueError. This is tested through extract_pushdown_from_sql which calls it.
    """

    @pytest.mark.parametrize(
        "query",
        [
            # Data modification
            "INSERT INTO t VALUES (1, 2)",
            "UPDATE t SET x = 1",
            "DELETE FROM t WHERE x = 1",
            # DDL
            "CREATE TABLE t (x INT)",
            "DROP TABLE t",
            "ALTER TABLE t ADD COLUMN y INT",
            # Other non-SELECT
            "COPY t TO 'file.csv'",
            "BEGIN TRANSACTION",
            "EXPLAIN SELECT * FROM t",
        ],
    )
    def test_non_select_statements_rejected(self, query):
        """Non-SELECT SQL statements should raise ValueError."""
        with pytest.raises(ValueError, match="Unsupported SQL statement|read-only"):
            extract_pushdown_from_sql(query)

    def test_error_message_mentions_alternatives(self):
        """Error message should mention lib.write() and lib.update() as alternatives."""
        with pytest.raises(ValueError, match="lib.write\\(\\) or lib.update\\(\\)"):
            extract_pushdown_from_sql("INSERT INTO t VALUES (1)")
