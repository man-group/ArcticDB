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

    def test_unknown_filter_type_returns_none(self):
        """Test that a single unknown filter type returns None (no valid filters)."""
        filters = [{"column": "x", "op": "WEIRD", "value": 1, "type": "unknown_type"}]
        result = _build_query_builder(filters)
        assert result is None


class TestExtractDateRange:
    """Tests for _extract_date_range - extracting date ranges from index filters."""

    def test_no_index_filters(self):
        """Test with no index column filters."""
        filters = [{"column": "x", "op": ">", "value": 5, "type": "comparison"}]
        date_range, remaining, _ = _extract_date_range(filters)
        assert date_range is None
        assert remaining == filters

    def test_index_between(self):
        """Test BETWEEN on index column extracts date range."""
        start = pd.Timestamp("2024-01-01")
        end = pd.Timestamp("2024-01-31")
        filters = [{"column": "index", "op": "BETWEEN", "value": (start, end), "type": "range"}]
        date_range, remaining, _ = _extract_date_range(filters)
        assert date_range == (start, end)
        assert remaining == []

    def test_index_gte(self):
        """Test >= on index column extracts start of date range."""
        start = pd.Timestamp("2024-01-01")
        filters = [{"column": "index", "op": ">=", "value": start, "type": "comparison"}]
        date_range, remaining, _ = _extract_date_range(filters)
        assert date_range == (start, None)
        assert remaining == []

    def test_index_lte(self):
        """Test <= on index column extracts end of date range."""
        end = pd.Timestamp("2024-01-31")
        filters = [{"column": "index", "op": "<=", "value": end, "type": "comparison"}]
        date_range, remaining, _ = _extract_date_range(filters)
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
        date_range, remaining, _ = _extract_date_range(filters)
        assert date_range == (start, end)
        assert remaining == []

    def test_mixed_index_and_other_filters(self):
        """Test index filters extracted while others remain."""
        start = pd.Timestamp("2024-01-01")
        filters = [
            {"column": "index", "op": ">=", "value": start, "type": "comparison"},
            {"column": "x", "op": ">", "value": 5, "type": "comparison"},
        ]
        date_range, remaining, _ = _extract_date_range(filters)
        assert date_range == (start, None)
        assert len(remaining) == 1
        assert remaining[0]["column"] == "x"

    def test_index_case_insensitive(self):
        """Test that 'INDEX' column name is case-insensitive."""
        start = pd.Timestamp("2024-01-01")
        filters = [{"column": "INDEX", "op": ">=", "value": start, "type": "comparison"}]
        date_range, remaining, _ = _extract_date_range(filters)
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

    def test_cast_to_float(self):
        """Test extracting value with CAST to DOUBLE."""
        node = {
            "class": "CAST",
            "type": "OPERATOR_CAST",
            "child": {
                "class": "CONSTANT",
                "type": "VALUE_CONSTANT",
                "value": {"type": {"id": "VARCHAR"}, "is_null": False, "value": "3.14"},
            },
            "cast_type": {"id": "DOUBLE"},
        }
        result = _extract_constant_value(node)
        assert result == 3.14
        assert isinstance(result, float)

    def test_cast_to_integer_invalid_value(self):
        """Test CAST to BIGINT with non-numeric string returns None."""
        node = {
            "class": "CAST",
            "type": "OPERATOR_CAST",
            "child": {
                "class": "CONSTANT",
                "type": "VALUE_CONSTANT",
                "value": {"type": {"id": "VARCHAR"}, "is_null": False, "value": "abc"},
            },
            "cast_type": {"id": "BIGINT"},
        }
        assert _extract_constant_value(node) is None

    def test_decimal_constant(self):
        """Test extracting DECIMAL VALUE_CONSTANT with scale."""
        node = {
            "class": "CONSTANT",
            "type": "VALUE_CONSTANT",
            "value": {
                "type": {"id": "DECIMAL", "type_info": {"scale": 2}},
                "is_null": False,
                "value": 12345,
            },
        }
        result = _extract_constant_value(node)
        assert result == 123.45

    def test_raw_value_none(self):
        """Test VALUE_CONSTANT with value=None returns None."""
        node = {
            "class": "CONSTANT",
            "type": "VALUE_CONSTANT",
            "value": {"type": {"id": "INTEGER"}, "is_null": False, "value": None},
        }
        assert _extract_constant_value(node) is None

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

    def test_date_range_pushdown_named_index(self):
        """Test date range pushdown when the index column has a name (e.g. 'Date')."""
        result, _ = extract_pushdown_from_sql(
            "SELECT * FROM test_table WHERE Date >= '2025-01-01' AND Date <= '2025-02-01'",
            ["test_table"],
            index_columns=["Date"],
        )
        info = result["test_table"]
        assert info.date_range_pushed_down is True
        assert info.date_range is not None
        assert info.date_range[0] == pd.Timestamp("2025-01-01")
        assert info.date_range[1] == pd.Timestamp("2025-02-01")
        # Date filters should NOT remain in the query_builder
        assert info.query_builder is None

    def test_date_range_pushdown_named_index_with_value_filter(self):
        """Test date range + value filter on named index separates correctly."""
        result, _ = extract_pushdown_from_sql(
            """SELECT * FROM test_table WHERE Date >= '2025-01-01' AND Date <= '2025-02-01' AND "status" = 'active'""",
            ["test_table"],
            index_columns=["Date"],
        )
        info = result["test_table"]
        assert info.date_range_pushed_down is True
        assert info.date_range[0] == pd.Timestamp("2025-01-01")
        assert info.date_range[1] == pd.Timestamp("2025-02-01")
        # Only the status filter should remain
        assert info.filter_pushed_down is True
        qb_str = str(info.query_builder)
        assert "status" in qb_str
        assert "Date" not in qb_str

    def test_date_range_pushdown_named_index_case_insensitive(self):
        """Test that named index matching is case-insensitive."""
        result, _ = extract_pushdown_from_sql(
            "SELECT * FROM test_table WHERE date >= '2025-01-01' AND date <= '2025-02-01'",
            ["test_table"],
            index_columns=["Date"],
        )
        info = result["test_table"]
        assert info.date_range_pushed_down is True
        assert info.date_range is not None

    def test_date_range_no_pushdown_without_index_columns(self):
        """Test that non-'index' column names are NOT treated as date range without index_columns."""
        result, _ = extract_pushdown_from_sql(
            "SELECT * FROM test_table WHERE Date >= '2025-01-01' AND Date <= '2025-02-01'",
            ["test_table"],
        )
        info = result["test_table"]
        # Without index_columns, Date is not recognized as the index
        assert info.date_range_pushed_down is False
        assert info.date_range is None
        # Instead, it's pushed as a value filter
        assert info.filter_pushed_down is True

    def test_numeric_index_not_pushed_as_date_range(self):
        """Test that numeric index columns are NOT incorrectly pushed as date_range.

        When index_columns contains a numeric column name, the filter should be
        treated as a value filter, not a date_range. pd.Timestamp(100) silently
        produces a nonsensical timestamp (1970-01-01 00:00:00.000000100) so
        numeric values must never enter the date_range path.
        """
        result, _ = extract_pushdown_from_sql(
            "SELECT * FROM test_table WHERE id >= 100 AND id <= 200",
            ["test_table"],
            index_columns=["id"],
        )
        info = result["test_table"]
        # Numeric values on an index column are NOT pushed as date_range because
        # pd.Timestamp(int) produces a nonsensical nanosecond-epoch timestamp.
        # _extract_date_range now skips int/float values and keeps them as
        # remaining filters, which are pushed as value filters via QueryBuilder.
        assert info.date_range is None
        assert info.filter_pushed_down is True

    def test_numeric_filter_without_index_columns_stays_value_filter(self):
        """Test that numeric filters without index_columns are value filters."""
        result, _ = extract_pushdown_from_sql(
            "SELECT * FROM test_table WHERE id >= 100 AND id <= 200",
            ["test_table"],
        )
        info = result["test_table"]
        # Without index_columns, id is NOT the index — pushed as value filter
        assert info.date_range_pushed_down is False
        assert info.date_range is None
        assert info.filter_pushed_down is True

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
        """Test that IS NULL / IS NOT NULL works via DuckDB.

        Note: Pandas stores None in a float column as NaN, which DuckDB treats
        as NOT NULL (SQL standard — NaN is a valid float, not NULL). Use a
        string column for proper IS NULL / IS NOT NULL semantics.
        """
        lib = lmdb_library
        df = pd.DataFrame(
            {
                "x": np.arange(10, dtype=float),
                "y": ["a", "b", None, "d", None, "f", "g", None, "i", "j"],
            }
        )
        lib.write("test_symbol", df)

        result = lib.sql("SELECT x, y FROM test_symbol WHERE y IS NOT NULL")

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


class TestNamedIndexDateRangePushdown:
    """Tests for date_range pushdown on symbols with named DatetimeIndex columns."""

    def test_named_index_date_range_pushdown(self, lmdb_library):
        """Test that date filters on named index columns are pushed down as date_range."""
        lib = lmdb_library
        dates = pd.date_range("2024-01-01", periods=365, freq="D")
        df = pd.DataFrame({"value": np.arange(365)}, index=pd.DatetimeIndex(dates, name="Date"))
        lib.write("test_symbol", df)

        # Query using the named index column
        result = lib.sql("""
            SELECT value FROM test_symbol
            WHERE Date >= '2024-01-01' AND Date <= '2024-01-31'
        """)
        assert len(result) == 31

        # Verify via explain that date_range was pushed down
        info = lib.explain("""
            SELECT value FROM test_symbol
            WHERE Date >= '2024-01-01' AND Date <= '2024-01-31'
        """)
        assert "date_range_pushed_down" in info

    def test_named_index_with_value_filter(self, lmdb_library):
        """Test named index date filter combined with a value filter."""
        lib = lmdb_library
        dates = pd.date_range("2024-01-01", periods=365, freq="D")
        df = pd.DataFrame(
            {"value": np.arange(365), "category": ["A", "B", "C"] * 121 + ["A", "B"]},
            index=pd.DatetimeIndex(dates, name="Date"),
        )
        lib.write("test_symbol", df)

        # Combine date range on named index with value filter
        result = lib.sql("""
            SELECT value, category FROM test_symbol
            WHERE Date >= '2024-01-01' AND Date <= '2024-01-31'
              AND category = 'A'
        """)
        # January has 31 days, ~1/3 are category A
        assert len(result) > 0
        assert (result["category"] == "A").all()

    def test_named_index_correctness(self, lmdb_library):
        """Test that named index pushdown produces same results as QB with date_range."""
        lib = lmdb_library
        dates = pd.date_range("2024-01-01", periods=365, freq="D")
        df = pd.DataFrame({"value": np.arange(365)}, index=pd.DatetimeIndex(dates, name="Date"))
        lib.write("test_symbol", df)

        # SQL path with named index date filter
        sql_result = lib.sql("""
            SELECT value FROM test_symbol
            WHERE Date >= '2024-03-01' AND Date <= '2024-03-31'
        """)

        # QueryBuilder path with native date_range
        qb_result = lib.read(
            "test_symbol",
            columns=["value"],
            date_range=(pd.Timestamp("2024-03-01"), pd.Timestamp("2024-03-31")),
        ).data

        pd.testing.assert_frame_equal(
            sql_result.reset_index(drop=True),
            qb_result.reset_index(drop=True),
        )


class TestNumericIndexSQL:
    """Tests for SQL queries on symbols with numeric (non-datetime) indexes.

    Numeric indexes must NOT be pushed as date_range — pd.Timestamp(int)
    silently produces nonsensical results.  The filter should instead be
    handled as a value filter by DuckDB.
    """

    def test_numeric_index_sql_returns_correct_results(self, lmdb_library):
        """SQL filter on a numeric index produces correct results."""
        lib = lmdb_library
        df = pd.DataFrame(
            {"value": np.arange(1000, dtype=np.float64)},
            index=pd.Index(np.arange(1000, dtype=np.int64), name="id"),
        )
        lib.write("num_idx", df)

        result = lib.sql("SELECT value FROM num_idx WHERE id >= 100 AND id <= 200")
        assert len(result) == 101
        # Values should correspond to the filtered index range
        assert result["value"].min() == 100.0
        assert result["value"].max() == 200.0

    def test_numeric_index_not_pushed_as_date_range(self, lmdb_library):
        """Explain should show no date_range pushdown for numeric index filters."""
        lib = lmdb_library
        df = pd.DataFrame(
            {"value": [1, 2, 3]},
            index=pd.Index([100, 200, 300], name="id", dtype="int64"),
        )
        lib.write("num_idx_explain", df)

        info = lib.explain("SELECT value FROM num_idx_explain WHERE id >= 100 AND id <= 200")
        # Numeric index must NOT be pushed as date_range
        assert info.get("date_range_pushed_down") is None or info["date_range_pushed_down"] is False

    def test_numeric_index_correctness_vs_pandas(self, lmdb_library):
        """SQL on numeric index matches pandas filtering."""
        lib = lmdb_library
        df = pd.DataFrame(
            {"x": np.random.default_rng(42).standard_normal(500), "cat": ["A", "B"] * 250},
            index=pd.Index(np.arange(500, dtype=np.int64), name="row_id"),
        )
        lib.write("num_idx_vs_pd", df)

        sql_result = lib.sql("SELECT x FROM num_idx_vs_pd WHERE row_id >= 100 AND row_id <= 199 AND cat = 'A'")
        pd_result = df.loc[(df.index >= 100) & (df.index <= 199) & (df["cat"] == "A"), ["x"]]

        # Compare values (ignore index details — SQL may not reconstruct numeric index)
        np.testing.assert_array_almost_equal(
            sorted(sql_result["x"].values),
            sorted(pd_result["x"].values),
        )

    def test_float_index_sql(self, lmdb_library):
        """SQL filter on a float64 index works correctly."""
        lib = lmdb_library
        df = pd.DataFrame(
            {"value": [10, 20, 30, 40, 50]},
            index=pd.Index([1.0, 2.5, 3.0, 4.5, 5.0], name="price", dtype="float64"),
        )
        lib.write("float_idx", df)

        result = lib.sql("SELECT value FROM float_idx WHERE price >= 2.0 AND price <= 4.0")
        assert len(result) == 2
        assert set(result["value"]) == {20, 30}


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


class TestFastPathColumnProjection:
    """Tests for the SQL fast-path with column projection.

    When a query is fully pushable (single table, no GROUP BY/ORDER BY/DISTINCT/LIMIT)
    and only projects specific columns, it should use the fast-path that skips DuckDB.
    """

    def test_select_columns_uses_fast_path(self, lmdb_library):
        """SELECT a, b FROM sym (fully pushable with column projection) should use fast path."""
        lib = lmdb_library
        df = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6], "c": [7, 8, 9]})
        lib.write("sym", df)

        result = lib.sql("SELECT a, b FROM sym", output_format="pyarrow")

        import pyarrow as pa

        assert isinstance(result, pa.Table)
        assert set(result.column_names) == {"a", "b"}
        assert result.column("a").to_pylist() == [1, 2, 3]
        assert result.column("b").to_pylist() == [4, 5, 6]

    def test_select_columns_with_filter_uses_fast_path(self, lmdb_library):
        """SELECT a FROM sym WHERE a > 1 should use fast path (filter + column projection)."""
        lib = lmdb_library
        df = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
        lib.write("sym", df)

        result = lib.sql("SELECT a FROM sym WHERE a > 1", output_format="pyarrow")

        import pyarrow as pa

        assert isinstance(result, pa.Table)
        assert result.column("a").to_pylist() == [2, 3]

    def test_select_columns_with_date_range_uses_fast_path(self, lmdb_library):
        """SELECT a FROM sym WHERE index >= ... should combine column proj + date_range."""
        lib = lmdb_library
        dates = pd.date_range("2024-01-01", periods=10, freq="D")
        df = pd.DataFrame({"a": range(10), "b": range(10, 20)}, index=dates)
        lib.write("sym", df)

        result = lib.sql("SELECT a FROM sym WHERE index >= '2024-01-05'", output_format="pyarrow")

        import pyarrow as pa

        assert isinstance(result, pa.Table)
        assert len(result) == 6  # Jan 5 through Jan 10
        assert "b" not in result.column_names

    def test_select_star_still_works(self, lmdb_library):
        """SELECT * should still use fast path (as before, no column restriction)."""
        lib = lmdb_library
        df = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
        lib.write("sym", df)

        result = lib.sql("SELECT * FROM sym", output_format="pyarrow")

        import pyarrow as pa

        assert isinstance(result, pa.Table)
        assert len(result) == 3

    def test_select_columns_result_matches_read(self, lmdb_library):
        """Fast-path SELECT a, b should produce same result as lib.read(columns=[...])."""
        lib = lmdb_library
        df = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6], "c": [7, 8, 9]})
        lib.write("sym", df)

        sql_result = lib.sql("SELECT a, b FROM sym", output_format="pyarrow")
        read_result = lib.read("sym", columns=["a", "b"], output_format="pyarrow").data

        # Both should produce same data
        assert sql_result.column("a").to_pylist() == read_result.column("a").to_pylist()
        assert sql_result.column("b").to_pylist() == read_result.column("b").to_pylist()

    def test_select_columns_where_extra_columns_projected_away(self, lmdb_library):
        """SELECT a FROM sym WHERE b > 1 — WHERE column b must not appear in result."""
        lib = lmdb_library
        df = pd.DataFrame({"a": [10, 20, 30], "b": [1, 2, 3]})
        lib.write("sym", df)

        result = lib.sql("SELECT a FROM sym WHERE b > 1", output_format="pyarrow")

        import pyarrow as pa

        assert isinstance(result, pa.Table)
        assert list(result.column_names) == ["a"]
        assert result.column("a").to_pylist() == [20, 30]


class TestColumnSliceAwareFilterPushdown:
    """Tests for Phase 1: column-slice-aware filter pushdown.

    Row-sliced data (narrow tables): filter pushed per-segment in C++ (parallel).
    Column-sliced data (wide tables): filter skipped in C++, DuckDB applies WHERE post-merge.
    """

    def test_row_sliced_filter_with_column_projection(self, lmdb_library):
        """Row-sliced + filter + columns: filter applied per-segment in C++."""
        lib = lmdb_library
        df = pd.DataFrame({"a": range(100), "b": range(100, 200), "c": range(200, 300)})
        lib.write("sym", df)

        result = lib.sql("SELECT a, b FROM sym WHERE c > 250", output_format="pyarrow")

        import pyarrow as pa

        assert isinstance(result, pa.Table)
        expected = df[df["c"] > 250][["a", "b"]].reset_index(drop=True)
        assert result.column("a").to_pylist() == expected["a"].tolist()
        assert result.column("b").to_pylist() == expected["b"].tolist()
        assert "c" not in result.column_names

    def test_column_sliced_filter_with_column_projection(self, lmdb_library_factory):
        """Column-sliced + filter + columns: DuckDB applies WHERE post-merge.

        Column layout with columns_per_segment=2:
          chunk 0: [a, b]   chunk 1: [c, d]   chunk 2: [e]

        cols_to_decode = {a, b} (SELECT) + {d} (WHERE) = {a, b, d}
          chunk 0: decodes a, b   chunk 1: decodes d   chunk 2: zero columns (skipped)

        This exercises the empty-column-chunk path in the lazy iterator where
        segment_to_arrow_data returns an empty vector for a chunk with no needed
        columns, and the column-slice merging loop skips it via next_batches.empty().
        """
        from arcticdb.options import LibraryOptions

        lib = lmdb_library_factory(LibraryOptions(columns_per_segment=2))
        # 5 columns with columns_per_segment=2 => 3 column chunks: [a,b], [c,d], [e]
        df = pd.DataFrame(
            {"a": range(50), "b": range(50, 100), "c": range(100, 150), "d": range(150, 200), "e": range(200, 250)}
        )
        lib.write("sym", df)

        # chunk [e] has zero needed columns — tests that the lazy iterator skips it cleanly
        result = lib.sql("SELECT a, b FROM sym WHERE d > 180", output_format="pyarrow")

        import pyarrow as pa

        assert isinstance(result, pa.Table)
        expected = df[df["d"] > 180][["a", "b"]].reset_index(drop=True)
        assert result.column("a").to_pylist() == expected["a"].tolist()
        assert result.column("b").to_pylist() == expected["b"].tolist()

    def test_column_sliced_filter_column_in_different_slice(self, lmdb_library_factory):
        """Filter column in a different column slice than projected columns."""
        from arcticdb.options import LibraryOptions

        lib = lmdb_library_factory(LibraryOptions(columns_per_segment=2))
        df = pd.DataFrame({"a": [1, 2, 3, 4, 5], "b": [10, 20, 30, 40, 50], "c": [100, 200, 300, 400, 500]})
        lib.write("sym", df)

        # Filter on 'c' (different slice from 'a'), project only 'a'
        result = lib.sql("SELECT a FROM sym WHERE c > 200", output_format="pyarrow")

        import pyarrow as pa

        assert isinstance(result, pa.Table)
        assert list(result.column_names) == ["a"]
        assert result.column("a").to_pylist() == [3, 4, 5]

    def test_row_sliced_filter_column_not_in_projection(self, lmdb_library):
        """Row-sliced: filter on column not in SELECT still works (filter col decoded but not returned)."""
        lib = lmdb_library
        df = pd.DataFrame({"a": range(100), "b": range(100, 200), "c": range(200, 300)})
        lib.write("sym", df)

        # Filter on 'c', select only 'a' — 'c' must be decoded for filter but not in result
        result = lib.sql("SELECT a FROM sym WHERE c >= 290", output_format="pyarrow")

        import pyarrow as pa

        assert isinstance(result, pa.Table)
        assert list(result.column_names) == ["a"]
        expected = df[df["c"] >= 290]["a"].tolist()
        assert result.column("a").to_pylist() == expected

    def test_row_sliced_filter_no_column_projection(self, lmdb_library):
        """Row-sliced + filter, no column projection: verify no regression."""
        lib = lmdb_library
        df = pd.DataFrame({"x": range(100), "y": range(100, 200)})
        lib.write("sym", df)

        result = lib.sql("SELECT * FROM sym WHERE x > 90 ORDER BY x", output_format="pyarrow")

        import pyarrow as pa

        assert isinstance(result, pa.Table)
        assert len(result) == 9
        assert result.column("x").to_pylist() == list(range(91, 100))

    def test_dynamic_schema_filter_with_projection(self, lmdb_library_factory):
        """Dynamic schema + filter: single column-slice, filter works normally."""
        from arcticdb.options import LibraryOptions

        lib = lmdb_library_factory(LibraryOptions(dynamic_schema=True))
        df = pd.DataFrame({"a": [1, 2, 3, 4, 5], "b": [10, 20, 30, 40, 50]})
        lib.write("sym", df)

        result = lib.sql("SELECT a FROM sym WHERE b > 20", output_format="pyarrow")

        import pyarrow as pa

        assert isinstance(result, pa.Table)
        assert list(result.column_names) == ["a"]
        assert result.column("a").to_pylist() == [3, 4, 5]

    def test_dynamic_schema_missing_filter_column(self, lmdb_library_factory):
        """Dynamic schema: filter on column missing from some appends returns consistent results."""
        from arcticdb.options import LibraryOptions

        lib = lmdb_library_factory(LibraryOptions(dynamic_schema=True))
        # First append has column 'a' and 'b'
        df1 = pd.DataFrame({"a": [1, 2, 3], "b": [10, 20, 30]})
        lib.write("sym", df1)
        # Second append only has column 'a' (no 'b')
        df2 = pd.DataFrame({"a": [4, 5, 6]})
        lib.append("sym", df2)

        # Filter on 'b' which is missing in the second append
        result = lib.sql("SELECT a FROM sym WHERE b > 15", output_format="pyarrow")

        import pyarrow as pa

        assert isinstance(result, pa.Table)
        # Only rows from df1 where b > 15 should match
        assert result.column("a").to_pylist() == [2, 3]

    def test_column_sliced_multi_row_group_filter(self, lmdb_library_factory):
        """Column-sliced + multiple row groups + filter: all row groups handled correctly.

        Storage layout with rows_per_segment=50, columns_per_segment=3, 200 rows, 6 columns:
          Row group 0 (rows 0-49):   chunk [a,b,c] + chunk [d,e,f]
          Row group 1 (rows 50-99):  chunk [a,b,c] + chunk [d,e,f]
          Row group 2 (rows 100-149): chunk [a,b,c] + chunk [d,e,f]
          Row group 3 (rows 150-199): chunk [a,b,c] + chunk [d,e,f]

        cols_to_decode = {a, b} (SELECT) + {e} (WHERE) = {a, b, e}
        Both column chunks have needed columns (no empty-column-chunk case here).

        Filter e > 950 with e = 800+row_index:
          Row groups 0-2: e = 800-949 → all filtered out (3 entirely empty row groups)
          Row group 3:    e = 950-999 → rows 151-199 pass (49 rows)

        This exercises the empty-row-group skip path in the lazy iterator where
        batches.empty() is true after DuckDB filtering and remaining same-row-group
        column slices are consumed and discarded.
        """
        from arcticdb.options import LibraryOptions

        lib = lmdb_library_factory(LibraryOptions(rows_per_segment=50, columns_per_segment=3))
        # 6 columns, rows_per_segment=50 => 2 column chunks per row group, 4 row groups
        df = pd.DataFrame(
            {
                "a": range(200),
                "b": range(200, 400),
                "c": range(400, 600),
                "d": range(600, 800),
                "e": range(800, 1000),
                "f": range(1000, 1200),
            }
        )
        lib.write("sym", df)

        # Row groups 0-2 are entirely empty after filter; only row group 3 has 49 matching rows
        result = lib.sql("SELECT a, b FROM sym WHERE e > 950", output_format="pyarrow")

        import pyarrow as pa

        assert isinstance(result, pa.Table)
        expected = df[df["e"] > 950][["a", "b"]].reset_index(drop=True)
        assert result.column("a").to_pylist() == expected["a"].tolist()
        assert result.column("b").to_pylist() == expected["b"].tolist()


# =============================================================================
# Coverage gap tests for pushdown.py
# =============================================================================


class TestExtractConstantValueCoverageGaps:
    """Additional coverage for _extract_constant_value edge cases."""

    def test_decimal_scale_zero(self):
        """DECIMAL with scale=0 should produce an integer-like float."""
        node = {
            "class": "CONSTANT",
            "type": "VALUE_CONSTANT",
            "value": {
                "type": {"id": "DECIMAL", "type_info": {"scale": 0}},
                "is_null": False,
                "value": 42,
            },
        }
        result = _extract_constant_value(node)
        assert result == 42.0

    def test_decimal_negative_value(self):
        """Negative DECIMAL value with scale."""
        node = {
            "class": "CONSTANT",
            "type": "VALUE_CONSTANT",
            "value": {
                "type": {"id": "DECIMAL", "type_info": {"scale": 2}},
                "is_null": False,
                "value": -12345,
            },
        }
        result = _extract_constant_value(node)
        assert result == -123.45

    def test_hugeint_constant(self):
        """HUGEINT type is handled as integer."""
        node = {
            "class": "CONSTANT",
            "type": "VALUE_CONSTANT",
            "value": {
                "type": {"id": "HUGEINT"},
                "is_null": False,
                "value": 99999999999999,
            },
        }
        result = _extract_constant_value(node)
        assert result == 99999999999999
        assert isinstance(result, int)

    def test_cast_to_float_invalid_value(self):
        """CAST to DOUBLE with non-numeric string returns None."""
        node = {
            "class": "CAST",
            "type": "OPERATOR_CAST",
            "child": {
                "class": "CONSTANT",
                "type": "VALUE_CONSTANT",
                "value": {"type": {"id": "VARCHAR"}, "is_null": False, "value": "not_a_number"},
            },
            "cast_type": {"id": "DOUBLE"},
        }
        assert _extract_constant_value(node) is None

    def test_cast_unknown_type_passthrough(self):
        """CAST to an unknown type returns the child value unchanged."""
        node = {
            "class": "CAST",
            "type": "OPERATOR_CAST",
            "child": {
                "class": "CONSTANT",
                "type": "VALUE_CONSTANT",
                "value": {"type": {"id": "VARCHAR"}, "is_null": False, "value": "some_value"},
            },
            "cast_type": {"id": "BLOB"},
        }
        result = _extract_constant_value(node)
        assert result == "some_value"

    def test_cast_with_null_child(self):
        """CAST with null child value returns None."""
        node = {
            "class": "CAST",
            "type": "OPERATOR_CAST",
            "child": {
                "class": "CONSTANT",
                "type": "VALUE_CONSTANT",
                "value": {"type": {"id": "INTEGER"}, "is_null": True, "value": None},
            },
            "cast_type": {"id": "TIMESTAMP"},
        }
        assert _extract_constant_value(node) is None

    def test_all_timestamp_cast_types(self):
        """All timestamp-family CAST types are handled."""
        for cast_type in [
            "TIMESTAMP",
            "TIMESTAMP WITH TIME ZONE",
            "TIMESTAMP_NS",
            "TIMESTAMP_MS",
            "TIMESTAMP_S",
            "DATE",
        ]:
            node = {
                "class": "CAST",
                "type": "OPERATOR_CAST",
                "child": {
                    "class": "CONSTANT",
                    "type": "VALUE_CONSTANT",
                    "value": {"type": {"id": "VARCHAR"}, "is_null": False, "value": "2024-06-15"},
                },
                "cast_type": {"id": cast_type},
            }
            result = _extract_constant_value(node)
            assert isinstance(result, pd.Timestamp), f"Failed for cast_type={cast_type}"

    def test_all_integer_cast_types(self):
        """All integer-family CAST types are handled."""
        for cast_type in [
            "INTEGER",
            "BIGINT",
            "SMALLINT",
            "TINYINT",
            "UINTEGER",
            "UBIGINT",
            "USMALLINT",
            "UTINYINT",
        ]:
            node = {
                "class": "CAST",
                "type": "OPERATOR_CAST",
                "child": {
                    "class": "CONSTANT",
                    "type": "VALUE_CONSTANT",
                    "value": {"type": {"id": "VARCHAR"}, "is_null": False, "value": "42"},
                },
                "cast_type": {"id": cast_type},
            }
            result = _extract_constant_value(node)
            assert result == 42, f"Failed for cast_type={cast_type}"
            assert isinstance(result, int), f"Not int for cast_type={cast_type}"


class TestAstToFiltersCoverageGaps:
    """Additional coverage for _ast_to_filters edge cases."""

    def test_deeply_nested_and_chain(self):
        """Deeply nested AND conjunction (10 levels) flattens correctly."""
        cols = [chr(ord("a") + i) for i in range(10)]
        expr = " AND ".join(f"{c} > {i + 1}" for i, c in enumerate(cols))
        result = _parse_where_clause(expr)
        assert len(result) == 10
        parsed_cols = {f["column"] for f in result}
        assert parsed_cols == set(cols)

    def test_and_with_or_subexpression_drops_or(self):
        """AND chain containing an OR subexpression: OR part is dropped."""
        result = _parse_where_clause("a > 1 AND (b > 2 OR c > 3)")
        assert len(result) == 1
        assert result[0]["column"] == "a"

    def test_between_with_string_values(self):
        """BETWEEN with string values (auto-converted to timestamps)."""
        result = _parse_where_clause("ts BETWEEN '2024-01-01' AND '2024-12-31'")
        assert len(result) == 1
        assert result[0]["op"] == "BETWEEN"
        low, high = result[0]["value"]
        assert isinstance(low, pd.Timestamp)
        assert isinstance(high, pd.Timestamp)

    def test_in_with_string_values(self):
        """IN clause with string values preserves string type."""
        result = _parse_where_clause("category IN ('A', 'B', 'C')")
        assert len(result) == 1
        assert result[0]["op"] == "IN"
        assert result[0]["value"] == ["A", "B", "C"]

    def test_comparison_with_column_on_right_not_pushed(self):
        """Comparison with column ref on both sides cannot be pushed."""
        result = _parse_where_clause("a > b")
        assert result == []

    def test_not_in_with_mixed_types(self):
        """NOT IN with mixed integer and float values."""
        result = _parse_where_clause("x NOT IN (1, 2.5, 3)")
        assert len(result) == 1
        assert result[0]["op"] == "NOT IN"
        assert len(result[0]["value"]) == 3


class TestExtractDateRangeCoverageGaps:
    """Additional coverage for _extract_date_range edge cases."""

    def test_strict_greater_than_sets_flag(self):
        """Strict > on index sets has_strict_date_op flag."""
        start = pd.Timestamp("2024-01-01")
        filters = [{"column": "index", "op": ">", "value": start, "type": "comparison"}]
        date_range, remaining, has_strict = _extract_date_range(filters)
        assert date_range == (start, None)
        assert has_strict is True
        assert remaining == []

    def test_strict_less_than_sets_flag(self):
        """Strict < on index sets has_strict_date_op flag."""
        end = pd.Timestamp("2024-12-31")
        filters = [{"column": "index", "op": "<", "value": end, "type": "comparison"}]
        date_range, remaining, has_strict = _extract_date_range(filters)
        assert date_range == (None, end)
        assert has_strict is True
        assert remaining == []

    def test_mixed_strict_and_inclusive(self):
        """Combining > (strict) and <= (inclusive) on index."""
        start = pd.Timestamp("2024-01-01")
        end = pd.Timestamp("2024-12-31")
        filters = [
            {"column": "index", "op": ">", "value": start, "type": "comparison"},
            {"column": "index", "op": "<=", "value": end, "type": "comparison"},
        ]
        date_range, remaining, has_strict = _extract_date_range(filters)
        assert date_range == (start, end)
        assert has_strict is True
        assert remaining == []

    def test_equality_on_index_stays_as_remaining(self):
        """= on index is not a range filter — stays in remaining."""
        ts = pd.Timestamp("2024-01-15")
        filters = [{"column": "index", "op": "=", "value": ts, "type": "comparison"}]
        date_range, remaining, _ = _extract_date_range(filters)
        assert date_range is None
        assert len(remaining) == 1

    def test_named_index_multiple_names(self):
        """Multiple index_columns all work as date range targets."""
        start = pd.Timestamp("2024-01-01")
        filters = [
            {"column": "Date", "op": ">=", "value": start, "type": "comparison"},
        ]
        date_range, remaining, _ = _extract_date_range(filters, index_columns=["Date", "Timestamp"])
        assert date_range == (start, None)
        assert remaining == []


class TestExtractPushdownFromSqlCoverageGaps:
    """Additional coverage for extract_pushdown_from_sql edge cases."""

    def test_fully_pushed_disabled_by_or(self):
        """Query with OR in WHERE disables fast-path (all_where_conditions_parsed=False)."""
        result, _ = extract_pushdown_from_sql("SELECT * FROM sym WHERE x = 1 OR x = 2", ["sym"])
        info = result["sym"]
        assert info.fully_pushed is False

    def test_fully_pushed_disabled_by_null_check(self):
        """Query with IS NULL filter disables fast-path (null-check semantics differ)."""
        result, _ = extract_pushdown_from_sql("SELECT * FROM sym WHERE x IS NULL", ["sym"])
        info = result["sym"]
        assert info.fully_pushed is False

    def test_fully_pushed_enabled_simple_filter(self):
        """Simple WHERE with AND filters is fully pushable."""
        result, _ = extract_pushdown_from_sql("SELECT x, y FROM sym WHERE x > 5 AND y < 10", ["sym"])
        info = result["sym"]
        assert info.fully_pushed is True

    def test_fully_pushed_disabled_by_limit(self):
        """LIMIT disables fast-path."""
        result, _ = extract_pushdown_from_sql("SELECT * FROM sym LIMIT 10", ["sym"])
        info = result["sym"]
        assert info.fully_pushed is False

    def test_fully_pushed_disabled_by_distinct(self):
        """DISTINCT disables fast-path."""
        result, _ = extract_pushdown_from_sql("SELECT DISTINCT x FROM sym", ["sym"])
        info = result["sym"]
        assert info.fully_pushed is False

    def test_fully_pushed_disabled_by_aggregation(self):
        """Aggregation (non-simple SELECT) disables fast-path."""
        result, _ = extract_pushdown_from_sql("SELECT SUM(x) FROM sym", ["sym"])
        info = result["sym"]
        assert info.fully_pushed is False

    def test_limit_not_pushed_with_where(self):
        """LIMIT + WHERE: LIMIT not pushed because filter may reduce row count."""
        result, _ = extract_pushdown_from_sql("SELECT x FROM sym WHERE x > 5 LIMIT 10", ["sym"])
        info = result["sym"]
        assert info.limit is None
        assert info.limit_pushed_down is None

    def test_select_columns_tracked_separately_from_where_columns(self):
        """select_columns only contains columns from SELECT, not WHERE."""
        result, _ = extract_pushdown_from_sql("SELECT a FROM sym WHERE b > 5", ["sym"])
        info = result["sym"]
        assert info.select_columns == ["a"]
        assert set(info.columns) == {"a", "b"}

    def test_subquery_tables_not_extracted(self):
        """Subquery table references are ignored (not treated as ArcticDB symbols)."""
        _, symbols = extract_pushdown_from_sql("SELECT * FROM sym WHERE x IN (SELECT y FROM sym WHERE y > 0)")
        assert symbols == ["sym"]
