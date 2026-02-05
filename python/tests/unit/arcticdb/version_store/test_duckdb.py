"""
Copyright 2026 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, version 2.0.
"""

import numpy as np
import pandas as pd
import pytest

from arcticdb.options import OutputFormat
from arcticdb.version_store.duckdb.integration import (
    _extract_symbols_from_query,
)

# Skip all tests if duckdb is not installed
duckdb = pytest.importorskip("duckdb")


class TestExtractSymbolsFromQuery:
    """Tests for _extract_symbols_from_query function."""

    def test_simple_from(self):
        symbols = _extract_symbols_from_query("SELECT * FROM my_symbol")
        assert symbols == ["my_symbol"]

    def test_from_with_alias(self):
        symbols = _extract_symbols_from_query("SELECT * FROM my_symbol AS s")
        assert symbols == ["my_symbol"]

    def test_join(self):
        symbols = _extract_symbols_from_query("SELECT * FROM a JOIN b ON a.x = b.x")
        assert symbols == ["a", "b"]

    def test_left_join(self):
        symbols = _extract_symbols_from_query("SELECT * FROM trades LEFT JOIN prices ON trades.x = prices.x")
        assert symbols == ["trades", "prices"]

    def test_multiple_joins(self):
        symbols = _extract_symbols_from_query("SELECT * FROM a JOIN b ON a.x = b.x JOIN c ON b.y = c.y")
        assert symbols == ["a", "b", "c"]

    def test_case_insensitive(self):
        symbols = _extract_symbols_from_query("select * from MY_SYMBOL")
        assert symbols == ["MY_SYMBOL"]

    def test_duplicate_symbol_only_appears_once(self):
        symbols = _extract_symbols_from_query("SELECT * FROM sym JOIN sym ON 1=1")
        assert symbols == ["sym"]

    def test_no_from_raises(self):
        with pytest.raises(ValueError, match="Could not extract symbol names"):
            _extract_symbols_from_query("SELECT 1 + 1")

    def test_empty_query_raises(self):
        with pytest.raises(ValueError, match="Could not extract symbol names"):
            _extract_symbols_from_query("")


class TestRecordBatchReader:
    """Tests for the ArcticRecordBatchReader class."""

    def test_basic_iteration(self, lmdb_library):
        """Test that we can iterate over record batches."""
        lib = lmdb_library
        df = pd.DataFrame({"x": np.arange(100), "y": np.arange(100, 200)})
        lib.write("test_symbol", df)

        reader = lib._read_as_record_batch_reader("test_symbol")

        # Should be able to iterate
        batches = list(reader)
        assert len(batches) >= 1

        # Total rows should match
        total_rows = sum(len(batch) for batch in batches)
        assert total_rows == 100

    def test_read_all(self, lmdb_library):
        """Test read_all() materializes to Arrow table."""
        lib = lmdb_library
        df = pd.DataFrame({"x": np.arange(50)})
        lib.write("test_symbol", df)

        reader = lib._read_as_record_batch_reader("test_symbol")
        table = reader.read_all()

        import pyarrow as pa

        assert isinstance(table, pa.Table)
        assert len(table) == 50

    def test_schema_property(self, lmdb_library):
        """Test that schema is correctly extracted."""
        lib = lmdb_library
        df = pd.DataFrame({"col_int": [1, 2, 3], "col_float": [1.0, 2.0, 3.0]})
        lib.write("test_symbol", df)

        reader = lib._read_as_record_batch_reader("test_symbol")

        # Schema should have our columns
        schema = reader.schema
        field_names = [field.name for field in schema]
        assert "col_int" in field_names
        assert "col_float" in field_names

    def test_with_date_range(self, lmdb_library):
        """Test record batch reader with date range filter."""
        lib = lmdb_library
        dates = pd.date_range("2024-01-01", periods=100, freq="D")
        df = pd.DataFrame({"value": np.arange(100)}, index=dates)
        lib.write("test_symbol", df)

        # Read only January data
        reader = lib._read_as_record_batch_reader(
            "test_symbol",
            date_range=(pd.Timestamp("2024-01-01"), pd.Timestamp("2024-01-31")),
        )

        table = reader.read_all()
        assert len(table) == 31  # 31 days in January

    def test_with_columns(self, lmdb_library):
        """Test record batch reader with column subset."""
        lib = lmdb_library
        df = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6], "c": [7, 8, 9]})
        lib.write("test_symbol", df)

        reader = lib._read_as_record_batch_reader("test_symbol", columns=["a", "c"])

        table = reader.read_all()
        assert "a" in table.column_names
        assert "c" in table.column_names
        assert "b" not in table.column_names


class TestDuckDBSimpleSQL:
    """Tests for the Library.sql() method."""

    def test_simple_select(self, lmdb_library):
        """Test basic SELECT query."""
        lib = lmdb_library
        df = pd.DataFrame({"x": np.arange(100), "y": np.arange(100, 200)})
        lib.write("test_symbol", df)

        result = lib.sql("SELECT x, y FROM test_symbol WHERE x > 50")

        assert len(result.data) == 49  # x values 51-99
        assert list(result.data.columns) == ["x", "y"]
        assert result.data["x"].min() > 50

    def test_aggregation(self, lmdb_library):
        """Test aggregation query."""
        lib = lmdb_library
        df = pd.DataFrame({"category": ["A", "B", "A", "B", "A"], "value": [10, 20, 30, 40, 50]})
        lib.write("test_symbol", df)

        result = lib.sql("SELECT category, SUM(value) as total FROM test_symbol GROUP BY category ORDER BY category")

        assert len(result.data) == 2
        assert list(result.data["category"]) == ["A", "B"]
        assert list(result.data["total"]) == [90, 60]

    def test_output_format_arrow(self, lmdb_library):
        """Test SQL with Arrow output format."""
        import pyarrow as pa

        lib = lmdb_library
        df = pd.DataFrame({"x": [1, 2, 3]})
        lib.write("test_symbol", df)

        result = lib.sql("SELECT * FROM test_symbol", output_format=OutputFormat.PYARROW)

        assert isinstance(result.data, pa.Table)

    def test_output_format_polars(self, lmdb_library):
        """Test SQL with Polars output format."""
        pl = pytest.importorskip("polars")

        lib = lmdb_library
        df = pd.DataFrame({"x": [1, 2, 3]})
        lib.write("test_symbol", df)

        result = lib.sql("SELECT * FROM test_symbol", output_format=OutputFormat.POLARS)

        assert isinstance(result.data, pl.DataFrame)

    def test_metadata_contains_query(self, lmdb_library):
        """Test that result metadata contains the query."""
        lib = lmdb_library
        df = pd.DataFrame({"x": [1, 2, 3]})
        lib.write("test_symbol", df)

        query = "SELECT * FROM test_symbol"
        result = lib.sql(query)

        assert result.metadata["query"] == query

    def test_symbol_field_contains_queried_symbols(self, lmdb_library):
        """Test that result symbol field contains queried symbols."""
        lib = lmdb_library
        df = pd.DataFrame({"x": [1, 2, 3]})
        lib.write("test_symbol", df)

        result = lib.sql("SELECT * FROM test_symbol")

        assert result.symbol == "test_symbol"

    def test_invalid_query_no_symbol(self, lmdb_library):
        """Test that query without FROM clause raises error."""
        lib = lmdb_library

        with pytest.raises(ValueError, match="Could not extract symbol names"):
            lib.sql("SELECT 1")


class TestDuckDBContext:
    """Tests for the DuckDBContext class."""

    def test_basic_context(self, lmdb_library):
        """Test basic context manager usage."""
        lib = lmdb_library
        df = pd.DataFrame({"x": [1, 2, 3]})
        lib.write("test_symbol", df)

        with lib.duckdb() as ddb:
            ddb.register_symbol("test_symbol")
            result = ddb.query("SELECT * FROM test_symbol")

        assert len(result) == 3

    def test_join_two_symbols(self, lmdb_library):
        """Test JOIN query across two symbols."""
        lib = lmdb_library

        trades = pd.DataFrame({"ticker": ["AAPL", "GOOG", "AAPL"], "quantity": [100, 200, 150]})

        prices = pd.DataFrame({"ticker": ["AAPL", "GOOG", "MSFT"], "price": [150.0, 2800.0, 300.0]})

        lib.write("trades", trades)
        lib.write("prices", prices)

        with lib.duckdb() as ddb:
            ddb.register_symbol("trades")
            ddb.register_symbol("prices")

            result = ddb.query(
                """
                SELECT t.ticker, t.quantity, p.price, t.quantity * p.price as notional
                FROM trades t
                JOIN prices p ON t.ticker = p.ticker
            """
            )

        assert len(result) == 3  # AAPL (2 rows) + GOOG (1 row)
        assert "notional" in result.columns

    def test_symbol_alias(self, lmdb_library):
        """Test registering symbol with alias."""
        lib = lmdb_library
        df = pd.DataFrame({"x": [1, 2, 3]})
        lib.write("test_symbol", df)

        with lib.duckdb() as ddb:
            ddb.register_symbol("test_symbol", alias="my_table")
            result = ddb.query("SELECT * FROM my_table")

        assert len(result) == 3

    def test_register_same_symbol_twice_with_different_filters(self, lmdb_library):
        """Test registering same symbol with different filters."""
        lib = lmdb_library
        dates = pd.date_range("2024-01-01", periods=100, freq="D")
        df = pd.DataFrame({"value": np.arange(100)}, index=dates)
        lib.write("test_symbol", df)

        with lib.duckdb() as ddb:
            ddb.register_symbol(
                "test_symbol",
                alias="jan_data",
                date_range=(pd.Timestamp("2024-01-01"), pd.Timestamp("2024-01-31")),
            )
            ddb.register_symbol(
                "test_symbol",
                alias="feb_data",
                date_range=(pd.Timestamp("2024-02-01"), pd.Timestamp("2024-02-29")),
            )

            jan_count = ddb.query("SELECT COUNT(*) as cnt FROM jan_data")["cnt"].iloc[0]
            feb_count = ddb.query("SELECT COUNT(*) as cnt FROM feb_data")["cnt"].iloc[0]

        assert jan_count == 31
        assert feb_count == 29

    def test_output_format_arrow(self, lmdb_library):
        """Test context query with Arrow output."""
        import pyarrow as pa

        lib = lmdb_library
        df = pd.DataFrame({"x": [1, 2, 3]})
        lib.write("test_symbol", df)

        with lib.duckdb() as ddb:
            ddb.register_symbol("test_symbol")
            result = ddb.query("SELECT * FROM test_symbol", output_format="arrow")

        assert isinstance(result, pa.Table)

    def test_method_chaining(self, lmdb_library):
        """Test method chaining with register_symbol."""
        lib = lmdb_library
        lib.write("sym1", pd.DataFrame({"x": [1, 2]}))
        lib.write("sym2", pd.DataFrame({"y": [3, 4]}))

        with lib.duckdb() as ddb:
            result = ddb.register_symbol("sym1").register_symbol("sym2").query("SELECT * FROM sym1, sym2")

        # Cross join should give 4 rows
        assert len(result) == 4

    def test_execute_method(self, lmdb_library):
        """Test execute method for DDL statements."""
        lib = lmdb_library
        df = pd.DataFrame({"x": [1, 2, 3]})
        lib.write("test_symbol", df)

        with lib.duckdb() as ddb:
            ddb.register_symbol("test_symbol")
            # Create a view using execute
            ddb.execute("CREATE VIEW filtered AS SELECT * FROM test_symbol WHERE x > 1")
            result = ddb.query("SELECT * FROM filtered")

        assert len(result) == 2

    def test_registered_symbols_property(self, lmdb_library):
        """Test registered_symbols property."""
        lib = lmdb_library
        lib.write("sym1", pd.DataFrame({"x": [1]}))
        lib.write("sym2", pd.DataFrame({"y": [2]}))

        with lib.duckdb() as ddb:
            ddb.register_symbol("sym1")
            ddb.register_symbol("sym2", alias="alias2", as_of=-1)

            registered = ddb.registered_symbols

        assert "sym1" in registered
        assert "alias2" in registered
        assert registered["alias2"]["symbol"] == "sym2"
        assert registered["alias2"]["as_of"] == -1

    def test_context_outside_with_raises(self, lmdb_library):
        """Test that using context outside 'with' raises error."""
        lib = lmdb_library

        ddb = lib.duckdb()

        with pytest.raises(RuntimeError, match="must be used within"):
            ddb.register_symbol("test")


class TestSQLPredicatePushdown:
    """Tests for SQL predicate pushdown to ArcticDB."""

    def test_column_projection_pushdown(self, lmdb_library):
        """Test that SELECT columns are pushed down to ArcticDB."""
        lib = lmdb_library
        df = pd.DataFrame({
            "a": np.arange(100),
            "b": np.arange(100, 200),
            "c": np.arange(200, 300),
            "d": np.arange(300, 400),
        })
        lib.write("test_symbol", df)

        # Query only columns a and b - should only read those from storage
        result = lib.sql("SELECT a, b FROM test_symbol")

        assert len(result.data) == 100
        assert list(result.data.columns) == ["a", "b"]
        # Verify pushdown happened by checking metadata
        assert "columns_pushed_down" in result.metadata
        assert set(result.metadata["columns_pushed_down"]) == {"a", "b"}

    def test_where_comparison_pushdown(self, lmdb_library):
        """Test that simple WHERE comparisons are pushed down."""
        lib = lmdb_library
        df = pd.DataFrame({"x": np.arange(1000), "y": np.arange(1000, 2000)})
        lib.write("test_symbol", df)

        # WHERE x > 900 should be pushed down
        result = lib.sql("SELECT x, y FROM test_symbol WHERE x > 900")

        assert len(result.data) == 99  # 901-999
        assert result.data["x"].min() > 900
        assert "filter_pushed_down" in result.metadata
        assert result.metadata["filter_pushed_down"] is True

    def test_where_multiple_conditions_pushdown(self, lmdb_library):
        """Test that AND/OR conditions are pushed down."""
        lib = lmdb_library
        df = pd.DataFrame({
            "x": np.arange(100),
            "y": np.arange(100, 200),
        })
        lib.write("test_symbol", df)

        # Multiple conditions with AND
        result = lib.sql("SELECT x, y FROM test_symbol WHERE x > 50 AND y < 180")

        assert len(result.data) == 29  # x: 51-79, y: 151-179
        assert result.data["x"].min() > 50
        assert result.data["y"].max() < 180

    def test_where_in_clause_pushdown(self, lmdb_library):
        """Test that IN clause is pushed down."""
        lib = lmdb_library
        df = pd.DataFrame({
            "category": ["A", "B", "C", "D", "E"] * 20,
            "value": np.arange(100),
        })
        lib.write("test_symbol", df)

        result = lib.sql("SELECT category, value FROM test_symbol WHERE category IN ('A', 'C')")

        assert len(result.data) == 40  # 20 A's + 20 C's
        assert set(result.data["category"].unique()) == {"A", "C"}

    def test_where_is_null_pushdown(self, lmdb_library):
        """Test that IS NULL is pushed down."""
        lib = lmdb_library
        df = pd.DataFrame({
            "x": [1, 2, None, 4, None, 6, 7, None, 9, 10],
            "y": np.arange(10),
        })
        lib.write("test_symbol", df)

        result = lib.sql("SELECT x, y FROM test_symbol WHERE x IS NOT NULL")

        assert len(result.data) == 7  # 10 - 3 nulls

    def test_limit_pushdown(self, lmdb_library):
        """Test that LIMIT is pushed down as head()."""
        lib = lmdb_library
        df = pd.DataFrame({"x": np.arange(1000)})
        lib.write("test_symbol", df)

        result = lib.sql("SELECT x FROM test_symbol LIMIT 10")

        assert len(result.data) == 10
        assert "limit_pushed_down" in result.metadata
        assert result.metadata["limit_pushed_down"] == 10

    def test_date_range_pushdown_between(self, lmdb_library):
        """Test that BETWEEN on timestamp index is pushed down as date_range."""
        lib = lmdb_library
        dates = pd.date_range("2024-01-01", periods=365, freq="D")
        df = pd.DataFrame({"value": np.arange(365)}, index=dates)
        lib.write("test_symbol", df)

        # Query for January only using BETWEEN on index
        result = lib.sql("""
            SELECT value FROM test_symbol
            WHERE index BETWEEN '2024-01-01' AND '2024-01-31'
        """)

        assert len(result.data) == 31
        assert "date_range_pushed_down" in result.metadata

    def test_combined_pushdown(self, lmdb_library):
        """Test combining column projection, WHERE, and LIMIT pushdown."""
        lib = lmdb_library
        df = pd.DataFrame({
            "a": np.arange(1000),
            "b": np.arange(1000, 2000),
            "c": np.arange(2000, 3000),
        })
        lib.write("test_symbol", df)

        result = lib.sql("SELECT a, b FROM test_symbol WHERE a > 500 LIMIT 50")

        assert len(result.data) == 50
        assert list(result.data.columns) == ["a", "b"]
        assert result.data["a"].min() > 500

    def test_pushdown_with_aggregation(self, lmdb_library):
        """Test that filters are pushed down even with aggregation."""
        lib = lmdb_library
        df = pd.DataFrame({
            "category": ["A", "B", "C"] * 100,
            "value": np.arange(300),
        })
        lib.write("test_symbol", df)

        # Filter should be pushed to ArcticDB, aggregation done by DuckDB
        result = lib.sql("""
            SELECT category, SUM(value) as total
            FROM test_symbol
            WHERE value > 100
            GROUP BY category
        """)

        assert len(result.data) == 3  # Still 3 categories

    def test_pushdown_preserves_correctness(self, lmdb_library):
        """Test that pushdown produces same results as non-pushdown."""
        lib = lmdb_library
        df = pd.DataFrame({
            "x": np.arange(500),
            "y": np.random.randn(500),
        })
        lib.write("test_symbol", df)

        # Get result with pushdown
        result_pushdown = lib.sql("SELECT x, y FROM test_symbol WHERE x > 200 AND x < 300")

        # Get result without pushdown (full read + DuckDB filter)
        full_data = lib.read("test_symbol").data
        expected = full_data[(full_data["x"] > 200) & (full_data["x"] < 300)][["x", "y"]]

        pd.testing.assert_frame_equal(
            result_pushdown.data.reset_index(drop=True),
            expected.reset_index(drop=True),
        )

    def test_unsupported_predicate_not_pushed(self, lmdb_library):
        """Test that unsupported predicates fall back to DuckDB filtering."""
        lib = lmdb_library
        df = pd.DataFrame({
            "name": ["alice", "bob", "charlie", "david"],
            "value": [1, 2, 3, 4],
        })
        lib.write("test_symbol", df)

        # LIKE is not directly supported by ArcticDB QueryBuilder
        # Should still work via DuckDB
        result = lib.sql("SELECT name, value FROM test_symbol WHERE name LIKE 'a%'")

        assert len(result.data) == 1
        assert result.data["name"].iloc[0] == "alice"

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

        result = lib.sql("""
            SELECT value FROM historical
            WHERE index BETWEEN '1850-01-01' AND '1850-01-31'
        """)
        assert len(result.data) == 31
        assert "date_range_pushed_down" in result.metadata

        # Test futuristic data (2150)
        dates = pd.date_range("2150-01-01", periods=365, freq="D")
        df = pd.DataFrame({"value": np.arange(365)}, index=dates)
        lib.write("futuristic", df)

        result = lib.sql("""
            SELECT value FROM futuristic
            WHERE index BETWEEN '2150-01-01' AND '2150-01-31'
        """)
        assert len(result.data) == 31
        assert "date_range_pushed_down" in result.metadata


class TestDuckDBIntegrationWithArrow:
    """Tests verifying the DuckDB integration uses Arrow correctly."""

    def test_duckdb_from_arrow_reader(self, lmdb_library):
        """Test that DuckDB can directly consume our reader."""
        lib = lmdb_library
        df = pd.DataFrame({"x": np.arange(100), "y": np.arange(100, 200)})
        lib.write("test_symbol", df)

        reader = lib._read_as_record_batch_reader("test_symbol")

        # Convert to PyArrow reader for DuckDB compatibility
        pa_reader = reader.to_pyarrow_reader()

        # DuckDB should be able to query the reader directly
        result = duckdb.from_arrow(pa_reader).filter("x > 50").arrow()

        assert len(result) == 49

    def test_batches_streamed(self, lmdb_library):
        """Test that data is correctly streamed through batches."""
        lib = lmdb_library

        # Write data
        df = pd.DataFrame({"x": np.arange(1000)})
        lib.write("test_symbol", df)

        reader = lib._read_as_record_batch_reader("test_symbol")

        batch_count = 0
        total_rows = 0
        for batch in reader:
            batch_count += 1
            total_rows += len(batch)

        # With standard segment size, all data may fit in one batch
        # The important thing is that streaming works correctly
        assert batch_count >= 1, "Expected at least one batch"
        assert total_rows == 1000


class TestSQLPushdownEdgeCases:
    """Tests for edge cases and limitations of SQL pushdown."""

    def test_unsigned_integer_types(self, lmdb_library):
        """Test that unsigned integer columns work correctly with SQL queries."""
        lib = lmdb_library
        df = pd.DataFrame({
            "u8": np.array([1, 2, 3], dtype=np.uint8),
            "u16": np.array([100, 200, 300], dtype=np.uint16),
            "u32": np.array([1000, 2000, 3000], dtype=np.uint32),
            "u64": np.array([10000, 20000, 30000], dtype=np.uint64),
        })
        lib.write("uint_test", df)

        # Should not crash and return correct results
        result = lib.sql("SELECT u8, u16, u32, u64 FROM uint_test WHERE u32 > 1500")
        assert len(result.data) == 2
        assert result.data["u32"].min() > 1500

    def test_small_integer_types(self, lmdb_library):
        """Test that small integer types (int8, int16) work correctly."""
        lib = lmdb_library
        df = pd.DataFrame({
            "i8": np.array([-50, 0, 50], dtype=np.int8),
            "i16": np.array([-1000, 0, 1000], dtype=np.int16),
        })
        lib.write("small_int_test", df)

        result = lib.sql("SELECT i8, i16 FROM small_int_test WHERE i8 > 0")
        assert len(result.data) == 1
        assert result.data["i8"].iloc[0] == 50

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
        assert len(result.data) == 2  # Correct result even if not pushed down

    def test_or_predicate_works_via_duckdb(self, lmdb_library):
        """Test that OR predicates work correctly (handled by DuckDB, not pushed)."""
        lib = lmdb_library
        df = pd.DataFrame({"x": [1, 2, 3, 4, 5]})
        lib.write("or_test", df)

        result = lib.sql("SELECT x FROM or_test WHERE x = 1 OR x = 5")
        assert len(result.data) == 2
        assert set(result.data["x"]) == {1, 5}
        # OR predicates are not pushed down to ArcticDB
        assert "filter_pushed_down" not in result.metadata

    def test_like_predicate_works_via_duckdb(self, lmdb_library):
        """Test that LIKE predicates work correctly (handled by DuckDB, not pushed)."""
        lib = lmdb_library
        df = pd.DataFrame({"name": ["apple", "banana", "apricot", "cherry"]})
        lib.write("like_test", df)

        result = lib.sql("SELECT name FROM like_test WHERE name LIKE 'ap%'")
        assert len(result.data) == 2
        assert set(result.data["name"]) == {"apple", "apricot"}

    def test_function_in_predicate_works_via_duckdb(self, lmdb_library):
        """Test that function predicates work correctly (handled by DuckDB, not pushed)."""
        lib = lmdb_library
        df = pd.DataFrame({"name": ["Apple", "Banana", "APPLE"]})
        lib.write("func_test", df)

        result = lib.sql("SELECT name FROM func_test WHERE UPPER(name) = 'APPLE'")
        assert len(result.data) == 2
        assert set(result.data["name"]) == {"Apple", "APPLE"}

    def test_limit_in_string_literal_not_confused(self, lmdb_library):
        """Test that LIMIT in a string literal doesn't confuse the LIMIT extraction.

        Regex-based parsing would incorrectly extract 999 from the string literal.
        Using DuckDB's AST parser ensures only the actual LIMIT clause is extracted.
        """
        lib = lmdb_library
        df = pd.DataFrame({
            "description": ["LIMIT 999 items", "Normal text", "Another row"] * 10,
            "value": np.arange(30),
        })
        lib.write("limit_string_test", df)

        result = lib.sql("""
            SELECT description, value FROM limit_string_test
            WHERE description LIKE '%LIMIT%'
            LIMIT 5
        """)

        assert len(result.data) == 5
        assert "limit_pushed_down" in result.metadata
        assert result.metadata["limit_pushed_down"] == 5  # Not 999!
