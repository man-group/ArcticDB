"""
Copyright 2026 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, version 2.0.
"""

"""
Unit tests for duckdb/duckdb.py - DuckDBContext and Library.sql() integration.

Tests verify the high-level SQL interface for querying ArcticDB data with DuckDB.
"""

import numpy as np
import pandas as pd
import pytest

from arcticdb.options import OutputFormat
from arcticdb.version_store.duckdb.duckdb import (
    _extract_symbols_from_query,
    _parse_library_name,
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


class TestDuckDBSimpleSQL:
    """Tests for the Library.sql() method."""

    def test_simple_select(self, lmdb_library):
        """Test basic SELECT query."""
        lib = lmdb_library
        df = pd.DataFrame({"x": np.arange(100), "y": np.arange(100, 200)})
        lib.write("test_symbol", df)

        result = lib.sql("SELECT x, y FROM test_symbol WHERE x > 50")

        assert len(result) == 49  # x values 51-99
        assert list(result.columns) == ["x", "y"]
        assert result["x"].min() > 50

    def test_aggregation(self, lmdb_library):
        """Test aggregation query."""
        lib = lmdb_library
        df = pd.DataFrame({"category": ["A", "B", "A", "B", "A"], "value": [10, 20, 30, 40, 50]})
        lib.write("test_symbol", df)

        result = lib.sql("SELECT category, SUM(value) as total FROM test_symbol GROUP BY category ORDER BY category")

        assert len(result) == 2
        assert list(result["category"]) == ["A", "B"]
        assert list(result["total"]) == [90, 60]

    def test_output_format_arrow(self, lmdb_library):
        """Test SQL with Arrow output format."""
        import pyarrow as pa

        lib = lmdb_library
        df = pd.DataFrame({"x": [1, 2, 3]})
        lib.write("test_symbol", df)

        result = lib.sql("SELECT * FROM test_symbol", output_format=OutputFormat.PYARROW)

        assert isinstance(result, pa.Table)

    def test_output_format_polars(self, lmdb_library):
        """Test SQL with Polars output format."""
        pl = pytest.importorskip("polars")

        lib = lmdb_library
        df = pd.DataFrame({"x": [1, 2, 3]})
        lib.write("test_symbol", df)

        result = lib.sql("SELECT * FROM test_symbol", output_format=OutputFormat.POLARS)

        assert isinstance(result, pl.DataFrame)

    def test_output_format_pandas(self, lmdb_library):
        """Test SQL with explicit Pandas output format."""
        lib = lmdb_library
        df = pd.DataFrame({"x": [1, 2, 3]})
        lib.write("test_symbol", df)

        result = lib.sql("SELECT * FROM test_symbol", output_format=OutputFormat.PANDAS)

        assert isinstance(result, pd.DataFrame)
        assert list(result["x"]) == [1, 2, 3]

    def test_metadata_contains_query(self, lmdb_library):
        """Test that result metadata contains the query."""
        lib = lmdb_library
        df = pd.DataFrame({"x": [1, 2, 3]})
        lib.write("test_symbol", df)

        query = "SELECT * FROM test_symbol"
        info = lib.explain(query)

        assert info["query"] == query

    def test_join_two_symbols(self, lmdb_library):
        """Test JOIN query across two symbols using lib.sql() directly."""
        lib = lmdb_library

        trades = pd.DataFrame({"ticker": ["AAPL", "GOOG", "AAPL"], "quantity": [100, 200, 150]})
        prices = pd.DataFrame({"ticker": ["AAPL", "GOOG", "MSFT"], "price": [150.0, 2800.0, 300.0]})

        lib.write("trades", trades)
        lib.write("prices", prices)

        result = lib.sql("""
            SELECT t.ticker, t.quantity, p.price, t.quantity * p.price as notional
            FROM trades t
            JOIN prices p ON t.ticker = p.ticker
            ORDER BY t.ticker, t.quantity
        """)

        assert len(result) == 3  # AAPL (2 rows) + GOOG (1 row)
        assert "notional" in result.columns
        assert set(result["ticker"]) == {"AAPL", "GOOG"}

    def test_join_with_aggregation(self, lmdb_library):
        """Test JOIN with GROUP BY using lib.sql() directly."""
        lib = lmdb_library

        orders = pd.DataFrame({"product_id": [1, 1, 2, 2, 3], "quantity": [10, 20, 5, 15, 8]})
        products = pd.DataFrame(
            {"product_id": [1, 2, 3], "name": ["Widget", "Gadget", "Gizmo"], "price": [10.0, 25.0, 15.0]}
        )

        lib.write("orders", orders)
        lib.write("products", products)

        result = lib.sql("""
            SELECT p.name, SUM(o.quantity) as total_qty, SUM(o.quantity * p.price) as revenue
            FROM orders o
            JOIN products p ON o.product_id = p.product_id
            GROUP BY p.name
            ORDER BY p.name
        """)

        assert len(result) == 3
        assert list(result["name"]) == ["Gadget", "Gizmo", "Widget"]
        assert list(result["total_qty"]) == [20, 8, 30]
        assert list(result["revenue"]) == [500.0, 120.0, 300.0]

    def test_cte_query(self, lmdb_library):
        """Test that WITH (CTE) queries work through lib.sql()."""
        lib = lmdb_library
        df = pd.DataFrame({"x": [1, 2, 3, 4, 5], "y": [10, 20, 30, 40, 50]})
        lib.write("test_symbol", df)

        result = lib.sql(
            "WITH filtered AS (SELECT * FROM test_symbol WHERE x > 2) " "SELECT SUM(y) as total FROM filtered"
        )
        assert result["total"].iloc[0] == 120  # y values for x=3,4,5: 30+40+50

    def test_invalid_query_no_symbol(self, lmdb_library):
        """Test that query without FROM clause raises error."""
        lib = lmdb_library

        with pytest.raises(ValueError, match="Could not extract symbol names"):
            lib.sql("SELECT 1")

    @pytest.mark.parametrize(
        "query",
        [
            "INSERT INTO my_symbol VALUES (1, 2)",
            "UPDATE my_symbol SET x = 1",
            "DELETE FROM my_symbol WHERE x = 1",
            "CREATE TABLE my_symbol (x INT)",
            "DROP TABLE my_symbol",
            "ALTER TABLE my_symbol ADD COLUMN y INT",
        ],
    )
    def test_rejects_mutating_sql(self, lmdb_library, query):
        """Test that lib.sql() rejects INSERT, UPDATE, DELETE and DDL statements."""
        lib = lmdb_library
        lib.write("my_symbol", pd.DataFrame({"x": [1, 2, 3]}))

        with pytest.raises(ValueError, match="Unsupported SQL statement|read-only"):
            lib.sql(query)


class TestDuckDBCaseSensitivity:
    """Tests for case-insensitive symbol resolution in Library.sql()."""

    def test_lowercase_symbol_uppercase_sql(self, lmdb_library):
        """SQL identifiers are case-insensitive — uppercase SQL should find lowercase symbol."""
        lib = lmdb_library
        df = pd.DataFrame({"x": [1, 2, 3]})
        lib.write("trades", df)

        result = lib.sql("SELECT * FROM TRADES")
        assert len(result) == 3

    def test_lowercase_symbol_mixed_case_sql(self, lmdb_library):
        """Mixed case SQL identifier should find lowercase symbol."""
        lib = lmdb_library
        df = pd.DataFrame({"x": [1, 2, 3]})
        lib.write("trades", df)

        result = lib.sql("SELECT * FROM Trades")
        assert len(result) == 3

    def test_mixed_case_symbol_lowercase_sql(self, lmdb_library):
        """Lowercase SQL identifier should find mixed-case symbol."""
        lib = lmdb_library
        df = pd.DataFrame({"x": [1, 2, 3]})
        lib.write("My_Symbol", df)

        result = lib.sql("SELECT * FROM my_symbol")
        assert len(result) == 3

    def test_exact_case_match_preferred(self, lmdb_library):
        """When both 'trades' and 'TRADES' exist, exact match takes priority."""
        lib = lmdb_library
        lib.write("trades", pd.DataFrame({"x": [1, 2, 3]}))
        lib.write("TRADES", pd.DataFrame({"x": [10, 20, 30]}))

        result_lower = lib.sql("SELECT * FROM trades")
        result_upper = lib.sql("SELECT * FROM TRADES")

        assert list(result_lower["x"]) == [1, 2, 3]
        assert list(result_upper["x"]) == [10, 20, 30]

    def test_case_insensitive_with_where(self, lmdb_library):
        """Case-insensitive resolution works with WHERE pushdown."""
        lib = lmdb_library
        df = pd.DataFrame({"x": np.arange(10), "y": np.arange(10, 20)})
        lib.write("prices", df)

        result = lib.sql("SELECT x, y FROM PRICES WHERE x > 5")
        assert len(result) == 4  # x values 6, 7, 8, 9

    def test_case_insensitive_nonexistent_symbol(self, lmdb_library):
        """Non-existent symbol (even case-insensitively) still raises error."""
        lib = lmdb_library
        lib.write("trades", pd.DataFrame({"x": [1]}))

        with pytest.raises(Exception):
            lib.sql("SELECT * FROM nonexistent")


class TestDuckDBTimestampFilters:
    """Tests for implicit string-to-timestamp conversion in WHERE filters."""

    def test_string_date_literal_in_where(self, lmdb_library):
        """WHERE ts < '2024-01-04' should work without explicit TIMESTAMP keyword."""
        lib = lmdb_library
        dates = pd.date_range("2024-01-01", periods=10, freq="D", name="ts")
        df = pd.DataFrame({"value": range(10)}, index=dates)
        lib.write("ts_data", df)

        result = lib.sql("SELECT * FROM ts_data WHERE ts < '2024-01-04'")
        assert len(result) == 3  # Jan 1, 2, 3

    def test_string_datetime_literal_in_where(self, lmdb_library):
        """WHERE ts >= '2024-01-05 00:00:00' should auto-convert to timestamp."""
        lib = lmdb_library
        dates = pd.date_range("2024-01-01", periods=10, freq="D", name="ts")
        df = pd.DataFrame({"value": range(10)}, index=dates)
        lib.write("ts_data", df)

        result = lib.sql("SELECT * FROM ts_data WHERE ts >= '2024-01-05 00:00:00'")
        assert len(result) == 6  # Jan 5 through Jan 10

    def test_explicit_timestamp_keyword_still_works(self, lmdb_library):
        """Explicit TIMESTAMP '...' syntax should still work."""
        lib = lmdb_library
        dates = pd.date_range("2024-01-01", periods=10, freq="D", name="ts")
        df = pd.DataFrame({"value": range(10)}, index=dates)
        lib.write("ts_data", df)

        result = lib.sql("SELECT * FROM ts_data WHERE ts < TIMESTAMP '2024-01-04'")
        assert len(result) == 3

    def test_string_filter_not_affected(self, lmdb_library):
        """Regular string filters should not be affected by timestamp auto-conversion."""
        lib = lmdb_library
        df = pd.DataFrame({"category": ["call", "put", "call"], "value": [1, 2, 3]})
        lib.write("opts", df)

        result = lib.sql("SELECT * FROM opts WHERE category = 'call'")
        assert len(result) == 2


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

            result = ddb.query("""
                SELECT t.ticker, t.quantity, p.price, t.quantity * p.price as notional
                FROM trades t
                JOIN prices p ON t.ticker = p.ticker
            """)

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

    def test_output_format_polars(self, lmdb_library):
        """Test context query with Polars output."""
        pl = pytest.importorskip("polars")

        lib = lmdb_library
        df = pd.DataFrame({"x": [1, 2, 3]})
        lib.write("test_symbol", df)

        with lib.duckdb() as ddb:
            ddb.register_symbol("test_symbol")
            result = ddb.query("SELECT * FROM test_symbol", output_format="polars")

        assert isinstance(result, pl.DataFrame)

    def test_output_format_pandas(self, lmdb_library):
        """Test context query with explicit Pandas output."""
        lib = lmdb_library
        df = pd.DataFrame({"x": [1, 2, 3]})
        lib.write("test_symbol", df)

        with lib.duckdb() as ddb:
            ddb.register_symbol("test_symbol")
            result = ddb.query("SELECT * FROM test_symbol", output_format="pandas")

        assert isinstance(result, pd.DataFrame)
        assert list(result["x"]) == [1, 2, 3]

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

    def test_query_without_registration_raises(self, lmdb_library):
        """Test that querying without registering symbols raises helpful error."""
        lib = lmdb_library
        lib.write("test_symbol", pd.DataFrame({"x": [1, 2, 3]}))

        with lib.duckdb() as ddb:
            with pytest.raises(RuntimeError, match="No symbols have been registered"):
                ddb.query("SELECT * FROM test_symbol")

    def test_with_as_of_version(self, lmdb_library):
        """Test register_symbol with as_of parameter."""
        lib = lmdb_library
        df1 = pd.DataFrame({"x": [1, 2, 3]})
        df2 = pd.DataFrame({"x": [10, 20, 30]})

        lib.write("test_symbol", df1)  # version 0
        lib.write("test_symbol", df2)  # version 1

        with lib.duckdb() as ddb:
            # Read version 0
            ddb.register_symbol("test_symbol", alias="v0", as_of=0)
            result = ddb.query("SELECT SUM(x) as total FROM v0")

        assert result["total"].iloc[0] == 6  # 1 + 2 + 3

    def test_with_row_range(self, lmdb_library):
        """Test register_symbol with row_range parameter."""
        lib = lmdb_library
        df = pd.DataFrame({"x": np.arange(100)})
        lib.write("test_symbol", df)

        with lib.duckdb() as ddb:
            # Read only rows 10-20
            ddb.register_symbol("test_symbol", row_range=(10, 20))
            result = ddb.query("SELECT COUNT(*) as cnt FROM test_symbol")

        assert result["cnt"].iloc[0] == 10


class TestDuckDBEdgeCases:
    """Tests for edge cases and special scenarios."""

    def test_empty_dataframe(self, lmdb_library):
        """Test SQL on empty DataFrame."""
        lib = lmdb_library
        df = pd.DataFrame({"x": pd.Series([], dtype=np.int64), "y": pd.Series([], dtype=np.float64)})
        lib.write("empty_symbol", df)

        result = lib.sql("SELECT * FROM empty_symbol")

        assert len(result) == 0

    def test_dataframe_with_nulls(self, lmdb_library):
        """Test SQL on DataFrame with null values."""
        lib = lmdb_library
        df = pd.DataFrame({"x": [1, None, 3], "y": [None, "b", None]})
        lib.write("test_symbol", df)

        result = lib.sql("SELECT * FROM test_symbol WHERE x IS NOT NULL")

        assert len(result) == 2  # Two non-null x values

    def test_special_characters_in_values(self, lmdb_library):
        """Test SQL on DataFrame with special characters in string values."""
        lib = lmdb_library
        df = pd.DataFrame({"text": ["hello", "world's", '"quoted"', "new\nline"]})
        lib.write("test_symbol", df)

        result = lib.sql("SELECT * FROM test_symbol")

        assert len(result) == 4
        assert "world's" in list(result["text"])

    def test_large_string_values(self, lmdb_library):
        """Test SQL on DataFrame with large string values."""
        lib = lmdb_library
        large_string = "x" * 10000
        df = pd.DataFrame({"text": [large_string, "small"]})
        lib.write("test_symbol", df)

        result = lib.sql("SELECT LENGTH(text) as len FROM test_symbol")

        assert result["len"].max() == 10000

    def test_float_special_values(self, lmdb_library):
        """Test SQL on DataFrame with special float values."""
        lib = lmdb_library
        df = pd.DataFrame({"x": [1.0, float("inf"), float("-inf"), float("nan")]})
        lib.write("test_symbol", df)

        result = lib.sql("SELECT * FROM test_symbol WHERE x = 1.0")

        assert len(result) == 1

    def test_mixed_numeric_types(self, lmdb_library):
        """Test SQL on DataFrame with mixed numeric types."""
        lib = lmdb_library
        df = pd.DataFrame(
            {
                "int8": np.array([1, 2, 3], dtype=np.int8),
                "int64": np.array([1, 2, 3], dtype=np.int64),
                "float32": np.array([1.0, 2.0, 3.0], dtype=np.float32),
                "float64": np.array([1.0, 2.0, 3.0], dtype=np.float64),
            }
        )
        lib.write("test_symbol", df)

        result = lib.sql("SELECT int8 + int64 + float32 + float64 as total FROM test_symbol")

        assert len(result) == 3

    def test_boolean_columns(self, lmdb_library):
        """Test SQL on DataFrame with boolean columns."""
        lib = lmdb_library
        df = pd.DataFrame({"flag": [True, False, True], "value": [1, 2, 3]})
        lib.write("test_symbol", df)

        result = lib.sql("SELECT SUM(value) as total FROM test_symbol WHERE flag")

        assert result["total"].iloc[0] == 4  # 1 + 3


class TestExternalDuckDBConnection:
    """Tests for using external DuckDB connections with ArcticDB."""

    def test_external_connection_not_closed(self, lmdb_library):
        """Test that external connections are NOT closed when context exits."""
        import duckdb

        lib = lmdb_library
        df = pd.DataFrame({"x": [1, 2, 3]})
        lib.write("test_symbol", df)

        # Create external connection
        conn = duckdb.connect(":memory:")
        conn.execute("CREATE TABLE external_data AS SELECT 10 as y")

        # Use with ArcticDB
        with lib.duckdb(connection=conn) as ddb:
            ddb.register_symbol("test_symbol")
            result = ddb.query("SELECT * FROM test_symbol")
            assert len(result) == 3

        # Connection should still be usable after context exits
        result = conn.execute("SELECT * FROM external_data").fetchall()
        assert result == [(10,)]

        # Clean up
        conn.close()

    def test_internal_connection_closed(self, lmdb_library):
        """Test that internal connections ARE closed when context exits."""
        import duckdb

        lib = lmdb_library
        df = pd.DataFrame({"x": [1, 2, 3]})
        lib.write("test_symbol", df)

        # Get reference to internal connection
        internal_conn = None
        with lib.duckdb() as ddb:
            ddb.register_symbol("test_symbol")
            internal_conn = ddb.connection

        # Connection should be closed (attempting to use it should fail)
        with pytest.raises(duckdb.ConnectionException):
            internal_conn.execute("SELECT 1")

    def test_join_arcticdb_with_external_table(self, lmdb_library):
        """Test joining ArcticDB data with external DuckDB tables."""
        import duckdb

        lib = lmdb_library

        # Write ArcticDB data
        trades = pd.DataFrame({"ticker": ["AAPL", "GOOG", "MSFT"], "quantity": [100, 200, 150]})
        lib.write("trades", trades)

        # Create external connection with reference data
        conn = duckdb.connect(":memory:")
        conn.execute("""
            CREATE TABLE sectors AS
            SELECT * FROM (VALUES
                ('AAPL', 'Technology'),
                ('GOOG', 'Technology'),
                ('MSFT', 'Technology'),
                ('JPM', 'Finance')
            ) AS t(ticker, sector)
        """)

        # Join ArcticDB data with external table
        with lib.duckdb(connection=conn) as ddb:
            ddb.register_symbol("trades")
            result = ddb.query("""
                SELECT t.ticker, t.quantity, s.sector
                FROM trades t
                JOIN sectors s ON t.ticker = s.ticker
                ORDER BY t.ticker
            """)

        assert len(result) == 3
        assert list(result["sector"]) == ["Technology", "Technology", "Technology"]

        # Verify connection still works
        assert conn.execute("SELECT COUNT(*) FROM sectors").fetchone()[0] == 4
        conn.close()

    def test_external_connection_with_multiple_symbols(self, lmdb_library):
        """Test joining multiple ArcticDB symbols with external data."""
        import duckdb

        lib = lmdb_library

        # Write multiple symbols
        trades = pd.DataFrame({"ticker": ["AAPL", "GOOG"], "qty": [100, 200]})
        prices = pd.DataFrame({"ticker": ["AAPL", "GOOG"], "price": [150.0, 2800.0]})
        lib.write("trades", trades)
        lib.write("prices", prices)

        # External multiplier data
        conn = duckdb.connect(":memory:")
        conn.execute("""
            CREATE TABLE multipliers AS
            SELECT * FROM (VALUES ('AAPL', 1.1), ('GOOG', 1.2)) AS t(ticker, mult)
        """)

        # Three-way join
        with lib.duckdb(connection=conn) as ddb:
            ddb.register_symbol("trades")
            ddb.register_symbol("prices")
            result = ddb.query("""
                SELECT t.ticker, t.qty * p.price * m.mult as adjusted_value
                FROM trades t
                JOIN prices p ON t.ticker = p.ticker
                JOIN multipliers m ON t.ticker = m.ticker
                ORDER BY t.ticker
            """)

        assert len(result) == 2
        # AAPL: 100 * 150 * 1.1 = 16500
        # GOOG: 200 * 2800 * 1.2 = 672000
        assert result["adjusted_value"].iloc[0] == pytest.approx(16500.0)
        assert result["adjusted_value"].iloc[1] == pytest.approx(672000.0)

        conn.close()

    def test_external_connection_preserves_existing_tables(self, lmdb_library):
        """Test that registering ArcticDB symbols doesn't affect existing tables."""
        import duckdb

        lib = lmdb_library
        df = pd.DataFrame({"x": [1, 2, 3]})
        lib.write("arcticdb_data", df)

        # Create connection with existing tables
        conn = duckdb.connect(":memory:")
        conn.execute("CREATE TABLE existing1 AS SELECT 'a' as col")
        conn.execute("CREATE TABLE existing2 AS SELECT 'b' as col")

        with lib.duckdb(connection=conn) as ddb:
            ddb.register_symbol("arcticdb_data")
            # Query should work on ArcticDB data
            result = ddb.query("SELECT * FROM arcticdb_data")
            assert len(result) == 3

        # Existing tables should still be intact
        assert conn.execute("SELECT col FROM existing1").fetchone()[0] == "a"
        assert conn.execute("SELECT col FROM existing2").fetchone()[0] == "b"
        conn.close()


class TestDocumentationExamples:
    """Tests for examples from the SQL queries documentation (docs/mkdocs/docs/tutorials/sql_queries.md)."""

    def test_quick_start_aggregation(self, lmdb_library):
        """Test the Quick Start example with GROUP BY aggregation."""
        lib = lmdb_library

        trades = pd.DataFrame(
            {
                "ticker": ["AAPL", "GOOG", "AAPL", "MSFT"],
                "price": [150.0, 2800.0, 151.0, 300.0],
                "quantity": [100, 50, 200, 75],
            }
        )
        lib.write("trades", trades)

        result = lib.sql("""
            SELECT ticker, AVG(price) as avg_price, SUM(quantity) as total_qty
            FROM trades
            GROUP BY ticker
            ORDER BY total_qty DESC
        """)

        assert len(result) == 3
        # AAPL has total_qty 300, should be first
        assert result.iloc[0]["ticker"] == "AAPL"
        assert result.iloc[0]["total_qty"] == 300
        assert result.iloc[0]["avg_price"] == pytest.approx(150.5)

    def test_join_with_market_value(self, lmdb_library):
        """Test JOIN example calculating market value."""
        lib = lmdb_library

        trades = pd.DataFrame(
            {
                "ticker": ["AAPL", "GOOG", "AAPL", "MSFT"],
                "price": [150.0, 2800.0, 151.0, 300.0],
                "quantity": [100, 50, 200, 75],
            }
        )
        prices = pd.DataFrame({"ticker": ["AAPL", "GOOG", "MSFT"], "current_price": [155.0, 2850.0, 310.0]})
        lib.write("trades", trades)
        lib.write("prices", prices)

        result = lib.sql("""
            SELECT t.ticker, t.quantity, p.current_price,
                   t.quantity * p.current_price as market_value
            FROM trades t
            JOIN prices p ON t.ticker = p.ticker
        """)

        assert len(result) == 4  # All trades have matching prices
        assert "market_value" in result.columns
        # Check one calculation: AAPL 100 * 155 = 15500
        aapl_rows = result[result["ticker"] == "AAPL"]
        assert 15500.0 in list(aapl_rows["market_value"])

    def test_window_function_lag_daily_returns(self, lmdb_library):
        """Test Financial Analytics example: daily returns with LAG window function."""
        lib = lmdb_library

        # Create price data with dates
        prices = pd.DataFrame(
            {
                "ticker": ["AAPL", "AAPL", "AAPL", "GOOG", "GOOG", "GOOG"],
                "date": pd.to_datetime(
                    ["2024-01-01", "2024-01-02", "2024-01-03", "2024-01-01", "2024-01-02", "2024-01-03"]
                ),
                "close": [150.0, 152.0, 151.0, 2800.0, 2850.0, 2820.0],
            }
        )
        lib.write("prices", prices)

        result = lib.sql("""
            SELECT
                ticker,
                date,
                close,
                (close - LAG(close) OVER (PARTITION BY ticker ORDER BY date)) /
                    LAG(close) OVER (PARTITION BY ticker ORDER BY date) as daily_return
            FROM prices
            ORDER BY ticker, date
        """)

        assert len(result) == 6
        assert "daily_return" in result.columns
        # First day of each ticker should have NULL return
        aapl_returns = result[result["ticker"] == "AAPL"]["daily_return"].tolist()
        assert pd.isna(aapl_returns[0])  # First day has no previous
        # Second day: (152 - 150) / 150 = 0.0133...
        assert aapl_returns[1] == pytest.approx(2.0 / 150.0)

    def test_portfolio_value_calculation(self, lmdb_library):
        """Test Financial Analytics example: portfolio value with positions and prices."""
        lib = lmdb_library

        positions = pd.DataFrame({"ticker": ["AAPL", "GOOG", "MSFT"], "shares": [100, 50, 75]})
        prices = pd.DataFrame({"ticker": ["AAPL", "GOOG", "MSFT"], "price": [155.0, 2850.0, 310.0]})
        lib.write("positions", positions)
        lib.write("prices", prices)

        with lib.duckdb() as ddb:
            ddb.register_symbol("positions")
            ddb.register_symbol("prices")

            result = ddb.query("""
                SELECT
                    pos.ticker,
                    pos.shares,
                    p.price,
                    pos.shares * p.price as market_value
                FROM positions pos
                JOIN prices p ON pos.ticker = p.ticker
            """)

        assert len(result) == 3
        # AAPL: 100 * 155 = 15500
        aapl_row = result[result["ticker"] == "AAPL"].iloc[0]
        assert aapl_row["market_value"] == pytest.approx(15500.0)
        # Total portfolio value
        total_value = result["market_value"].sum()
        # 100*155 + 50*2850 + 75*310 = 15500 + 142500 + 23250 = 181250
        assert total_value == pytest.approx(181250.0)

    def test_time_series_ohlc_resampling(self, lmdb_library):
        """Test Time Series Analysis example: resample to daily OHLC."""
        lib = lmdb_library

        # Create tick data with timestamps
        ticks = pd.DataFrame(
            {
                "price": [100.0, 102.0, 99.0, 101.0, 105.0, 103.0, 102.0, 108.0],
                "volume": [1000, 500, 800, 1200, 600, 900, 700, 1100],
            },
            index=pd.to_datetime(
                [
                    "2024-01-01 09:30:00",
                    "2024-01-01 10:00:00",
                    "2024-01-01 11:00:00",
                    "2024-01-01 16:00:00",
                    "2024-01-02 09:30:00",
                    "2024-01-02 10:00:00",
                    "2024-01-02 11:00:00",
                    "2024-01-02 16:00:00",
                ]
            ),
        )
        lib.write("ticks", ticks)

        result = lib.sql("""
            SELECT
                DATE_TRUNC('day', index) as date,
                FIRST(price) as open,
                MAX(price) as high,
                MIN(price) as low,
                LAST(price) as close,
                SUM(volume) as volume
            FROM ticks
            GROUP BY DATE_TRUNC('day', index)
            ORDER BY date
        """)

        assert len(result) == 2  # Two days
        day1 = result.iloc[0]
        day2 = result.iloc[1]

        # Day 1: open=100, high=102, low=99, close=101, volume=3500
        assert day1["open"] == pytest.approx(100.0)
        assert day1["high"] == pytest.approx(102.0)
        assert day1["low"] == pytest.approx(99.0)
        assert day1["close"] == pytest.approx(101.0)
        assert day1["volume"] == 3500

        # Day 2: open=105, high=108, low=102, close=108, volume=3300
        assert day2["open"] == pytest.approx(105.0)
        assert day2["high"] == pytest.approx(108.0)
        assert day2["low"] == pytest.approx(102.0)
        assert day2["close"] == pytest.approx(108.0)
        assert day2["volume"] == 3300

    def test_data_quality_find_gaps(self, lmdb_library):
        """Test Data Quality example: find gaps in time series using window functions."""
        lib = lmdb_library

        # Create data with a gap (missing Jan 3)
        prices = pd.DataFrame(
            {"price": [100.0, 101.0, 103.0, 104.0]},
            index=pd.to_datetime(["2024-01-01", "2024-01-02", "2024-01-04", "2024-01-05"]),  # Note: Jan 3 is missing
        )
        lib.write("prices", prices)

        # Use duckdb() context to avoid CTE name being treated as a symbol
        with lib.duckdb() as ddb:
            ddb.register_symbol("prices")
            result = ddb.query("""
                WITH date_series AS (
                    SELECT DISTINCT DATE_TRUNC('day', index) as date FROM prices
                )
                SELECT
                    date,
                    LEAD(date) OVER (ORDER BY date) as next_date,
                    LEAD(date) OVER (ORDER BY date) - date as gap
                FROM date_series
                ORDER BY date
            """)

        assert len(result) == 4
        # Check that we can detect the gap between Jan 2 and Jan 4
        gaps = result.dropna(subset=["gap"])
        # The gap column might be returned as interval or integer days
        # Find the row with the 2-day gap (gap > 1 day)
        gap_values = gaps["gap"]
        if hasattr(gap_values.iloc[0], "days"):
            # Timedelta/interval type
            large_gaps = gaps[gap_values.apply(lambda x: x.days > 1)]
        else:
            # Integer days
            large_gaps = gaps[gap_values > 1]
        assert len(large_gaps) == 1
        assert pd.Timestamp(large_gaps.iloc[0]["date"]).date() == pd.Timestamp("2024-01-02").date()

    def test_version_selection_as_of(self, lmdb_library):
        """Test Version Selection example: query specific version."""
        lib = lmdb_library

        # Write multiple versions
        trades_v0 = pd.DataFrame({"ticker": ["AAPL"], "price": [150.0]})
        trades_v1 = pd.DataFrame({"ticker": ["AAPL", "GOOG"], "price": [155.0, 2800.0]})

        lib.write("trades", trades_v0)  # version 0
        lib.write("trades", trades_v1)  # version 1

        # Query version 0
        result_v0 = lib.sql("SELECT * FROM trades", as_of=0)
        assert len(result_v0) == 1

        # Query latest (version 1)
        result_v1 = lib.sql("SELECT * FROM trades")
        assert len(result_v1) == 2


class TestSchemaDDLQueries:
    """Tests for schema introspection via DuckDB DDL queries (DESCRIBE, SHOW COLUMNS)."""

    def test_describe_basic_types(self, lmdb_library):
        """Test DESCRIBE query returns correct types for basic columns."""
        lib = lmdb_library

        df = pd.DataFrame(
            {
                "int64_col": np.array([1, 2, 3], dtype=np.int64),
                "float64_col": np.array([1.5, 2.5, 3.5], dtype=np.float64),
                "string_col": ["a", "b", "c"],
                "bool_col": [True, False, True],
            }
        )
        lib.write("test_symbol", df)

        with lib.duckdb() as ddb:
            ddb.register_symbol("test_symbol")
            result = ddb.query("DESCRIBE test_symbol")

        # Check we get the expected columns in the DESCRIBE output
        assert "column_name" in result.columns
        assert "column_type" in result.columns

        # Build a mapping of column name to type
        type_map = dict(zip(result["column_name"], result["column_type"]))

        assert type_map["int64_col"] == "BIGINT"
        assert type_map["float64_col"] == "DOUBLE"
        assert type_map["string_col"] == "VARCHAR"
        assert type_map["bool_col"] == "BOOLEAN"

    def test_describe_integer_types(self, lmdb_library):
        """Test DESCRIBE returns correct types for various integer sizes."""
        lib = lmdb_library

        df = pd.DataFrame(
            {
                "int8_col": np.array([1, 2, 3], dtype=np.int8),
                "int16_col": np.array([1, 2, 3], dtype=np.int16),
                "int32_col": np.array([1, 2, 3], dtype=np.int32),
                "int64_col": np.array([1, 2, 3], dtype=np.int64),
                "uint8_col": np.array([1, 2, 3], dtype=np.uint8),
                "uint16_col": np.array([1, 2, 3], dtype=np.uint16),
                "uint32_col": np.array([1, 2, 3], dtype=np.uint32),
                "uint64_col": np.array([1, 2, 3], dtype=np.uint64),
            }
        )
        lib.write("test_symbol", df)

        with lib.duckdb() as ddb:
            ddb.register_symbol("test_symbol")
            result = ddb.query("DESCRIBE test_symbol")

        type_map = dict(zip(result["column_name"], result["column_type"]))

        # Signed integers
        assert type_map["int8_col"] == "TINYINT"
        assert type_map["int16_col"] == "SMALLINT"
        assert type_map["int32_col"] == "INTEGER"
        assert type_map["int64_col"] == "BIGINT"

        # Unsigned integers
        assert type_map["uint8_col"] == "UTINYINT"
        assert type_map["uint16_col"] == "USMALLINT"
        assert type_map["uint32_col"] == "UINTEGER"
        assert type_map["uint64_col"] == "UBIGINT"

    def test_describe_float_types(self, lmdb_library):
        """Test DESCRIBE returns correct types for float columns."""
        lib = lmdb_library

        df = pd.DataFrame(
            {
                "float32_col": np.array([1.5, 2.5, 3.5], dtype=np.float32),
                "float64_col": np.array([1.5, 2.5, 3.5], dtype=np.float64),
            }
        )
        lib.write("test_symbol", df)

        with lib.duckdb() as ddb:
            ddb.register_symbol("test_symbol")
            result = ddb.query("DESCRIBE test_symbol")

        type_map = dict(zip(result["column_name"], result["column_type"]))

        assert type_map["float32_col"] == "FLOAT"
        assert type_map["float64_col"] == "DOUBLE"

    def test_describe_timestamp_index(self, lmdb_library):
        """Test DESCRIBE returns correct type for timestamp index."""
        lib = lmdb_library

        df = pd.DataFrame({"value": [1.0, 2.0, 3.0]}, index=pd.to_datetime(["2024-01-01", "2024-01-02", "2024-01-03"]))
        lib.write("test_symbol", df)

        with lib.duckdb() as ddb:
            ddb.register_symbol("test_symbol")
            result = ddb.query("DESCRIBE test_symbol")

        type_map = dict(zip(result["column_name"], result["column_type"]))

        # Index should be exposed as a timestamp column
        assert "index" in type_map
        assert "TIMESTAMP" in type_map["index"]

    def test_show_columns_equivalent(self, lmdb_library):
        """Test SHOW COLUMNS returns same info as DESCRIBE."""
        lib = lmdb_library

        df = pd.DataFrame(
            {
                "x": [1, 2, 3],
                "y": [1.0, 2.0, 3.0],
            }
        )
        lib.write("test_symbol", df)

        with lib.duckdb() as ddb:
            ddb.register_symbol("test_symbol")
            describe_result = ddb.query("DESCRIBE test_symbol")
            # SHOW is an alias for DESCRIBE in DuckDB
            show_result = ddb.query("SHOW test_symbol")

        # Both should return the same column information
        assert list(describe_result["column_name"]) == list(show_result["column_name"])
        assert list(describe_result["column_type"]) == list(show_result["column_type"])

    def test_describe_multiple_symbols(self, lmdb_library):
        """Test DESCRIBE works on multiple registered symbols."""
        lib = lmdb_library

        df1 = pd.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "z"]})
        df2 = pd.DataFrame({"c": [1.0, 2.0], "d": [True, False]})
        lib.write("symbol1", df1)
        lib.write("symbol2", df2)

        with lib.duckdb() as ddb:
            ddb.register_symbol("symbol1")
            ddb.register_symbol("symbol2")

            result1 = ddb.query("DESCRIBE symbol1")
            result2 = ddb.query("DESCRIBE symbol2")

        type_map1 = dict(zip(result1["column_name"], result1["column_type"]))
        type_map2 = dict(zip(result2["column_name"], result2["column_type"]))

        assert "a" in type_map1
        assert "b" in type_map1
        assert "c" in type_map2
        assert "d" in type_map2

    def test_describe_with_alias(self, lmdb_library):
        """Test DESCRIBE works with aliased symbol registration."""
        lib = lmdb_library

        df = pd.DataFrame({"x": [1, 2, 3]})
        lib.write("original_name", df)

        with lib.duckdb() as ddb:
            ddb.register_symbol("original_name", alias="aliased_name")
            result = ddb.query("DESCRIBE aliased_name")

        assert "x" in list(result["column_name"])

    def test_show_tables_enumerates_all_symbols(self, lmdb_library):
        """Test SHOW TABLES returns all registered symbols for data discovery."""
        lib = lmdb_library

        # Write multiple symbols
        lib.write("prices", pd.DataFrame({"price": [100.0, 101.0]}))
        lib.write("trades", pd.DataFrame({"qty": [10, 20]}))
        lib.write("positions", pd.DataFrame({"shares": [100, 200]}))

        with lib.duckdb() as ddb:
            ddb.register_symbol("prices")
            ddb.register_symbol("trades")
            ddb.register_symbol("positions")

            # SHOW TABLES should list all registered symbols
            result = ddb.query("SHOW TABLES")

        table_names = set(result["name"])
        assert "prices" in table_names
        assert "trades" in table_names
        assert "positions" in table_names
        assert len(table_names) == 3

    def test_show_all_tables_with_metadata(self, lmdb_library):
        """Test SHOW ALL TABLES returns symbols with column metadata."""
        lib = lmdb_library

        lib.write("symbol1", pd.DataFrame({"a": [1, 2], "b": [3, 4]}))
        lib.write("symbol2", pd.DataFrame({"x": [1.0], "y": [2.0], "z": [3.0]}))

        with lib.duckdb() as ddb:
            ddb.register_symbol("symbol1")
            ddb.register_symbol("symbol2")

            result = ddb.query("SHOW ALL TABLES")

        # Should have both tables
        table_names = set(result["name"])
        assert "symbol1" in table_names
        assert "symbol2" in table_names

        # Check column_names are included for discovery
        assert "column_names" in result.columns
        symbol1_row = result[result["name"] == "symbol1"].iloc[0]
        symbol2_row = result[result["name"] == "symbol2"].iloc[0]

        # symbol1 has columns a, b
        assert "a" in symbol1_row["column_names"]
        assert "b" in symbol1_row["column_names"]

        # symbol2 has columns x, y, z
        assert "x" in symbol2_row["column_names"]
        assert "y" in symbol2_row["column_names"]
        assert "z" in symbol2_row["column_names"]

    def test_show_tables_with_aliases(self, lmdb_library):
        """Test SHOW TABLES shows aliased names, not original symbol names."""
        lib = lmdb_library

        lib.write("original_symbol", pd.DataFrame({"x": [1, 2, 3]}))

        with lib.duckdb() as ddb:
            ddb.register_symbol("original_symbol", alias="my_alias")
            result = ddb.query("SHOW TABLES")

        table_names = set(result["name"])
        # Should see the alias, not the original name
        assert "my_alias" in table_names
        assert "original_symbol" not in table_names

    def test_register_all_symbols_discovers_library(self, lmdb_library):
        """Test register_all_symbols() discovers all symbols in the library."""
        lib = lmdb_library

        # Write multiple symbols to the library
        lib.write("market_data", pd.DataFrame({"price": [100.0, 101.0, 102.0]}))
        lib.write("trades", pd.DataFrame({"qty": [10, 20, 30], "side": ["buy", "sell", "buy"]}))
        lib.write("positions", pd.DataFrame({"shares": [100, 200], "symbol": ["AAPL", "GOOG"]}))
        lib.write("metadata", pd.DataFrame({"key": ["a", "b"], "value": ["x", "y"]}))

        # Use register_all_symbols() to auto-discover
        with lib.duckdb() as ddb:
            ddb.register_all_symbols()
            result = ddb.query("SHOW TABLES")

        table_names = set(result["name"])

        # All symbols should be discoverable
        assert "market_data" in table_names
        assert "trades" in table_names
        assert "positions" in table_names
        assert "metadata" in table_names
        assert len(table_names) == 4

    def test_show_all_tables_discovers_library_with_columns(self, lmdb_library):
        """Test SHOW ALL TABLES discovers all library symbols with column metadata."""
        lib = lmdb_library

        lib.write("prices", pd.DataFrame({"ticker": ["AAPL"], "price": [150.0], "volume": [1000]}))
        lib.write("orders", pd.DataFrame({"order_id": [1], "quantity": [100]}))

        with lib.duckdb() as ddb:
            ddb.register_all_symbols()
            result = ddb.query("SHOW ALL TABLES")

        # Check all symbols are discovered
        table_names = set(result["name"])
        assert "prices" in table_names
        assert "orders" in table_names

        # Check column metadata is available
        assert "column_names" in result.columns

        prices_row = result[result["name"] == "prices"].iloc[0]
        assert "ticker" in prices_row["column_names"]
        assert "price" in prices_row["column_names"]
        assert "volume" in prices_row["column_names"]

        orders_row = result[result["name"] == "orders"].iloc[0]
        assert "order_id" in orders_row["column_names"]
        assert "quantity" in orders_row["column_names"]

    def test_sql_describe_basic_types(self, lmdb_library):
        """Test lib.sql() with DESCRIBE query returns correct types."""
        lib = lmdb_library

        df = pd.DataFrame(
            {
                "int_col": np.array([1, 2, 3], dtype=np.int64),
                "float_col": np.array([1.5, 2.5, 3.5], dtype=np.float64),
                "str_col": ["a", "b", "c"],
            }
        )
        lib.write("test_symbol", df)

        # Use lib.sql() directly with DESCRIBE
        result = lib.sql("DESCRIBE test_symbol")

        type_map = dict(zip(result["column_name"], result["column_type"]))
        assert type_map["int_col"] == "BIGINT"
        assert type_map["float_col"] == "DOUBLE"
        assert type_map["str_col"] == "VARCHAR"

    def test_sql_describe_integer_types(self, lmdb_library):
        """Test lib.sql() DESCRIBE with various integer types."""
        lib = lmdb_library

        df = pd.DataFrame(
            {
                "int8_col": np.array([1], dtype=np.int8),
                "int16_col": np.array([1], dtype=np.int16),
                "int32_col": np.array([1], dtype=np.int32),
                "int64_col": np.array([1], dtype=np.int64),
                "uint8_col": np.array([1], dtype=np.uint8),
                "uint16_col": np.array([1], dtype=np.uint16),
                "uint32_col": np.array([1], dtype=np.uint32),
                "uint64_col": np.array([1], dtype=np.uint64),
            }
        )
        lib.write("test_symbol", df)

        result = lib.sql("DESCRIBE test_symbol")
        type_map = dict(zip(result["column_name"], result["column_type"]))

        assert type_map["int8_col"] == "TINYINT"
        assert type_map["int16_col"] == "SMALLINT"
        assert type_map["int32_col"] == "INTEGER"
        assert type_map["int64_col"] == "BIGINT"
        assert type_map["uint8_col"] == "UTINYINT"
        assert type_map["uint16_col"] == "USMALLINT"
        assert type_map["uint32_col"] == "UINTEGER"
        assert type_map["uint64_col"] == "UBIGINT"

    def test_sql_describe_timestamp_index(self, lmdb_library):
        """Test lib.sql() DESCRIBE with timestamp index."""
        lib = lmdb_library

        df = pd.DataFrame({"value": [1.0, 2.0, 3.0]}, index=pd.to_datetime(["2024-01-01", "2024-01-02", "2024-01-03"]))
        lib.write("test_symbol", df)

        result = lib.sql("DESCRIBE test_symbol")
        type_map = dict(zip(result["column_name"], result["column_type"]))

        assert "index" in type_map
        assert "TIMESTAMP" in type_map["index"]

    def test_sql_show_equivalent_to_describe(self, lmdb_library):
        """Test lib.sql() SHOW returns same as DESCRIBE."""
        lib = lmdb_library

        df = pd.DataFrame({"x": [1, 2], "y": [3.0, 4.0]})
        lib.write("test_symbol", df)

        describe_result = lib.sql("DESCRIBE test_symbol")
        show_result = lib.sql("SHOW test_symbol")

        assert list(describe_result["column_name"]) == list(show_result["column_name"])
        assert list(describe_result["column_type"]) == list(show_result["column_type"])

    def test_sql_show_all_tables_discovers_library(self, lmdb_library):
        """Test lib.sql() with SHOW ALL TABLES discovers all symbols in library."""
        lib = lmdb_library

        # Write multiple symbols
        lib.write("prices", pd.DataFrame({"price": [100.0, 101.0]}))
        lib.write("trades", pd.DataFrame({"qty": [10, 20]}))
        lib.write("positions", pd.DataFrame({"shares": [100, 200]}))

        # SHOW ALL TABLES should discover all library symbols
        result = lib.sql("SHOW ALL TABLES")

        table_names = set(result["name"])
        assert "prices" in table_names
        assert "trades" in table_names
        assert "positions" in table_names
        assert len(table_names) == 3

    def test_sql_show_tables_discovers_library(self, lmdb_library):
        """Test lib.sql() with SHOW TABLES discovers all symbols in library."""
        lib = lmdb_library

        lib.write("symbol_a", pd.DataFrame({"a": [1, 2]}))
        lib.write("symbol_b", pd.DataFrame({"b": [3, 4]}))

        result = lib.sql("SHOW TABLES")

        table_names = set(result["name"])
        assert "symbol_a" in table_names
        assert "symbol_b" in table_names


class TestArcticDuckDBShowDatabases:
    """Tests for SHOW DATABASES functionality at the Arctic level."""

    def test_arctic_sql_show_databases_empty(self, lmdb_storage):
        """Test arctic.sql('SHOW DATABASES') with no libraries returns empty result."""
        arctic = lmdb_storage.create_arctic()

        result = arctic.sql("SHOW DATABASES")

        assert "database_name" in result.columns
        assert len(result) == 0

    def test_arctic_sql_show_databases_single_library(self, lmdb_storage):
        """Test arctic.sql('SHOW DATABASES') with a single library."""
        arctic = lmdb_storage.create_arctic()
        arctic.create_library("testuser.market_data")

        result = arctic.sql("SHOW DATABASES")

        assert "database_name" in result.columns
        assert "library_count" in result.columns
        assert len(result) == 1
        assert "testuser" in list(result["database_name"])
        assert result["library_count"].iloc[0] == 1

    def test_arctic_sql_show_databases_multiple_libraries(self, lmdb_storage):
        """Test arctic.sql('SHOW DATABASES') with multiple libraries in different databases."""
        arctic = lmdb_storage.create_arctic()
        arctic.create_library("testuser.market_data")
        arctic.create_library("testuser.reference_data")
        arctic.create_library("otheruser.portfolios")

        result = arctic.sql("SHOW DATABASES")

        assert "database_name" in result.columns
        assert "library_count" in result.columns
        assert len(result) == 2  # Two databases: testuser, otheruser
        db_counts = dict(zip(result["database_name"], result["library_count"]))
        assert db_counts["testuser"] == 2
        assert db_counts["otheruser"] == 1

    def test_arctic_sql_show_databases_output_format_arrow(self, lmdb_storage):
        """Test arctic.sql('SHOW DATABASES') with Arrow output format."""
        import pyarrow as pa

        arctic = lmdb_storage.create_arctic()
        arctic.create_library("testuser.test_lib")

        result = arctic.sql("SHOW DATABASES", output_format="pyarrow")

        assert isinstance(result, pa.Table)
        assert "database_name" in result.column_names
        assert "library_count" in result.column_names
        assert result.num_rows == 1

    def test_arctic_sql_show_databases_invalid_query_raises(self, lmdb_storage):
        """Test arctic.sql() raises error for non-database queries."""
        arctic = lmdb_storage.create_arctic()
        arctic.create_library("test_lib")
        lib = arctic["test_lib"]
        lib.write("test_symbol", pd.DataFrame({"x": [1, 2, 3]}))

        with pytest.raises(ValueError, match="only supports SHOW DATABASES"):
            arctic.sql("SELECT * FROM test_symbol")

    def test_arctic_duckdb_context_show_databases(self, lmdb_storage):
        """Test arctic.duckdb() context manager with SHOW DATABASES."""
        arctic = lmdb_storage.create_arctic()
        arctic.create_library("testuser.lib_a")
        arctic.create_library("testuser.lib_b")

        with arctic.duckdb() as ddb:
            ddb.register_all_libraries()
            result = ddb.query("SHOW DATABASES")

        assert "database_name" in result.columns
        assert "library_count" in result.columns
        db_counts = dict(zip(result["database_name"], result["library_count"]))
        assert db_counts["testuser"] == 2

    def test_arctic_duckdb_context_register_library(self, lmdb_storage):
        """Test arctic.duckdb() with explicit register_library()."""
        arctic = lmdb_storage.create_arctic()
        arctic.create_library("user1.lib_a")
        arctic.create_library("user1.lib_b")
        arctic.create_library("user2.lib_c")

        with arctic.duckdb() as ddb:
            # Only register two of three libraries (from different databases)
            ddb.register_library("user1.lib_a")
            ddb.register_library("user2.lib_c")
            result = ddb.query("SHOW DATABASES")

        db_counts = dict(zip(result["database_name"], result["library_count"]))
        assert db_counts["user1"] == 1  # Only lib_a registered, not lib_b
        assert db_counts["user2"] == 1

    def test_arctic_duckdb_context_register_nonexistent_library_raises(self, lmdb_storage):
        """Test arctic.duckdb() register_library() raises for non-existent library."""
        arctic = lmdb_storage.create_arctic()

        with arctic.duckdb() as ddb:
            with pytest.raises(ValueError, match="does not exist"):
                ddb.register_library("nonexistent")

    def test_arctic_duckdb_context_register_symbol(self, lmdb_storage):
        """Test arctic.duckdb() context manager with register_symbol() for cross-library queries."""
        arctic = lmdb_storage.create_arctic()
        lib_a = arctic.create_library("user1.lib_a")
        lib_b = arctic.create_library("user1.lib_b")

        lib_a.write("prices", pd.DataFrame({"ticker": ["AAPL", "GOOG"], "price": [150.0, 180.0]}))
        lib_b.write("info", pd.DataFrame({"ticker": ["AAPL", "GOOG"], "name": ["Apple", "Google"]}))

        with arctic.duckdb() as ddb:
            ddb.register_symbol("user1.lib_a", "prices")
            ddb.register_symbol("user1.lib_b", "info")
            result = ddb.query("""
                SELECT p.ticker, p.price, i.name
                FROM prices p
                JOIN info i ON p.ticker = i.ticker
                ORDER BY p.ticker
            """)

        assert len(result) == 2
        assert list(result["ticker"]) == ["AAPL", "GOOG"]
        assert list(result["name"]) == ["Apple", "Google"]

    def test_arctic_duckdb_context_register_symbol_with_alias(self, lmdb_storage):
        """Test arctic.duckdb() register_symbol() with alias."""
        arctic = lmdb_storage.create_arctic()
        lib = arctic.create_library("testuser.mylib")
        lib.write("original_name", pd.DataFrame({"x": [1, 2, 3]}))

        with arctic.duckdb() as ddb:
            ddb.register_symbol("testuser.mylib", "original_name", alias="aliased")
            result = ddb.query("SELECT * FROM aliased")

        assert len(result) == 3
        assert list(result["x"]) == [1, 2, 3]

    def test_arctic_duckdb_context_show_databases_with_registered_symbols(self, lmdb_storage):
        """Test SHOW DATABASES includes libraries implicitly from registered symbols."""
        arctic = lmdb_storage.create_arctic()
        lib = arctic.create_library("testuser.implicit_lib")
        lib.write("symbol", pd.DataFrame({"x": [1]}))

        with arctic.duckdb() as ddb:
            # Just register a symbol (don't call register_library explicitly)
            ddb.register_symbol("testuser.implicit_lib", "symbol")
            result = ddb.query("SHOW DATABASES")

        # Library's database should be in SHOW DATABASES even without explicit registration
        assert "testuser" in list(result["database_name"])

    def test_arctic_duckdb_context_registered_libraries_property(self, lmdb_storage):
        """Test registered_libraries property on ArcticDuckDBContext."""
        arctic = lmdb_storage.create_arctic()
        arctic.create_library("testuser.lib1")
        arctic.create_library("testuser.lib2")

        with arctic.duckdb() as ddb:
            ddb.register_library("testuser.lib1")
            ddb.register_library("testuser.lib2")

            libs = ddb.registered_libraries
            assert "testuser.lib1" in libs
            assert "testuser.lib2" in libs

    def test_arctic_duckdb_context_registered_symbols_property(self, lmdb_storage):
        """Test registered_symbols property on ArcticDuckDBContext."""
        arctic = lmdb_storage.create_arctic()
        lib = arctic.create_library("testuser.mylib")
        lib.write("sym1", pd.DataFrame({"a": [1]}))
        lib.write("sym2", pd.DataFrame({"b": [2]}))

        with arctic.duckdb() as ddb:
            ddb.register_symbol("testuser.mylib", "sym1")
            ddb.register_symbol("testuser.mylib", "sym2", alias="alias2")

            syms = ddb.registered_symbols
            assert "sym1" in syms
            assert "alias2" in syms
            assert syms["sym1"]["library"] == "testuser.mylib"
            assert syms["sym1"]["symbol"] == "sym1"
            assert syms["alias2"]["library"] == "testuser.mylib"
            assert syms["alias2"]["symbol"] == "sym2"

    def test_arctic_duckdb_context_query_without_symbols_raises(self, lmdb_storage):
        """Test arctic.duckdb() query() raises if no symbols registered (non-SHOW DATABASES)."""
        arctic = lmdb_storage.create_arctic()
        arctic.create_library("testuser.lib")

        with arctic.duckdb() as ddb:
            ddb.register_library("testuser.lib")
            # SHOW DATABASES should work without symbol registration
            ddb.query("SHOW DATABASES")

            # But a data query should fail
            with pytest.raises(RuntimeError, match="No symbols have been registered"):
                ddb.query("SELECT * FROM some_table")

    def test_arctic_duckdb_context_arrow_output_format(self, lmdb_storage):
        """Test arctic.duckdb() with arrow output format."""
        import pyarrow as pa

        arctic = lmdb_storage.create_arctic()
        lib = arctic.create_library("testuser.lib")
        lib.write("data", pd.DataFrame({"x": [1, 2, 3]}))

        with arctic.duckdb() as ddb:
            ddb.register_symbol("testuser.lib", "data")
            result = ddb.query("SELECT * FROM data", output_format="arrow")

        assert isinstance(result, pa.Table)
        assert result.num_rows == 3

    def test_arctic_duckdb_context_external_connection(self, lmdb_storage):
        """Test arctic.duckdb() with external DuckDB connection."""
        arctic = lmdb_storage.create_arctic()
        lib = arctic.create_library("testuser.lib")
        lib.write("arctic_data", pd.DataFrame({"key": ["a", "b"], "value": [1, 2]}))

        # Create external connection with other data
        conn = duckdb.connect(":memory:")
        conn.execute("CREATE TABLE external_data AS SELECT 'a' as key, 100 as extra UNION SELECT 'b', 200")

        with arctic.duckdb(connection=conn) as ddb:
            ddb.register_symbol("testuser.lib", "arctic_data")
            result = ddb.query("""
                SELECT a.key, a.value, e.extra
                FROM arctic_data a
                JOIN external_data e ON a.key = e.key
                ORDER BY a.key
            """)

        assert len(result) == 2
        assert list(result["key"]) == ["a", "b"]
        assert list(result["extra"]) == [100, 200]

        # Connection should still be open
        assert conn.execute("SELECT count(*) FROM external_data").fetchone()[0] == 2
        conn.close()

    def test_arctic_duckdb_context_chaining(self, lmdb_storage):
        """Test method chaining on ArcticDuckDBContext."""
        arctic = lmdb_storage.create_arctic()
        lib = arctic.create_library("testuser.lib")
        lib.write("data", pd.DataFrame({"x": [1, 2, 3]}))

        with arctic.duckdb() as ddb:
            result = (
                ddb.register_library("testuser.lib")
                .register_symbol("testuser.lib", "data")
                .query("SELECT SUM(x) as total FROM data")
            )

        assert result["total"].iloc[0] == 6


class TestDatabaseLibraryNamespace:
    """Tests for database.library namespace hierarchy handling."""

    # Tests for _parse_library_name function

    def test_parse_library_name_with_database(self):
        """Test parsing jblackburn.test_lib format."""
        database, library = _parse_library_name("jblackburn.test_lib")
        assert database == "jblackburn"
        assert library == "test_lib"

    def test_parse_library_name_multi_dot(self):
        """Test parsing jblackburn.test.lib - split on first dot only."""
        database, library = _parse_library_name("jblackburn.test.lib")
        assert database == "jblackburn"
        assert library == "test.lib"

    def test_parse_library_name_top_level(self):
        """Test top-level library without dot goes to __default__."""
        database, library = _parse_library_name("global_data")
        assert database == "__default__"
        assert library == "global_data"

    def test_parse_library_name_leading_dot(self):
        """Test library name starting with dot."""
        database, library = _parse_library_name(".hidden_lib")
        assert database == ""
        assert library == "hidden_lib"

    def test_parse_library_name_trailing_dot(self):
        """Test library name ending with dot."""
        database, library = _parse_library_name("user.")
        assert database == "user"
        assert library == ""

    # Tests for SHOW DATABASES with database hierarchy

    def test_show_databases_groups_by_database(self, lmdb_storage):
        """Test SHOW DATABASES returns database_name and library_count columns."""
        arctic = lmdb_storage.create_arctic()
        # Create libraries with database.library format
        arctic.create_library("jblackburn.lib1")
        arctic.create_library("jblackburn.lib2")
        arctic.create_library("other_user.lib1")

        result = arctic.sql("SHOW DATABASES")

        assert "database_name" in result.columns
        assert "library_count" in result.columns

        # Convert to dict for easier assertions
        db_counts = dict(zip(result["database_name"], result["library_count"]))
        assert db_counts["jblackburn"] == 2
        assert db_counts["other_user"] == 1

    def test_show_databases_default_namespace(self, lmdb_storage):
        """Test top-level libraries grouped under __default__."""
        arctic = lmdb_storage.create_arctic()
        # Mix of namespaced and top-level libraries
        arctic.create_library("jblackburn.lib1")
        arctic.create_library("global_config")
        arctic.create_library("shared_data")

        result = arctic.sql("SHOW DATABASES")

        db_counts = dict(zip(result["database_name"], result["library_count"]))
        assert db_counts["jblackburn"] == 1
        assert db_counts["__default__"] == 2

    def test_show_databases_empty(self, lmdb_storage):
        """Test SHOW DATABASES with no libraries returns empty result."""
        arctic = lmdb_storage.create_arctic()

        result = arctic.sql("SHOW DATABASES")

        assert "database_name" in result.columns
        assert "library_count" in result.columns
        assert len(result) == 0

    # Tests for ArcticDuckDBContext with database hierarchy

    def test_context_show_databases_with_hierarchy(self, lmdb_storage):
        """Test arctic.duckdb() SHOW DATABASES with hierarchy grouping."""
        arctic = lmdb_storage.create_arctic()
        arctic.create_library("jblackburn.market_data")
        arctic.create_library("jblackburn.reference_data")
        arctic.create_library("shared.global_config")

        with arctic.duckdb() as ddb:
            ddb.register_all_libraries()
            result = ddb.query("SHOW DATABASES")

        assert "database_name" in result.columns
        assert "library_count" in result.columns

        db_counts = dict(zip(result["database_name"], result["library_count"]))
        assert db_counts["jblackburn"] == 2
        assert db_counts["shared"] == 1

    # Tests for cross-library queries with database.library naming

    def test_cross_database_query(self, lmdb_storage):
        """Test queries across symbols from different databases."""
        arctic = lmdb_storage.create_arctic()

        # Create libraries in different databases
        lib1 = arctic.create_library("user1.market_data")
        lib2 = arctic.create_library("user2.reference_data")

        lib1.write("prices", pd.DataFrame({"ticker": ["AAPL", "GOOG"], "price": [150.0, 180.0]}))
        lib2.write("info", pd.DataFrame({"ticker": ["AAPL", "GOOG"], "name": ["Apple", "Google"]}))

        with arctic.duckdb() as ddb:
            ddb.register_symbol("user1.market_data", "prices")
            ddb.register_symbol("user2.reference_data", "info")
            result = ddb.query("""
                SELECT p.ticker, p.price, i.name
                FROM prices p
                JOIN info i ON p.ticker = i.ticker
                ORDER BY p.ticker
            """)

        assert len(result) == 2
        assert list(result["ticker"]) == ["AAPL", "GOOG"]
        assert list(result["name"]) == ["Apple", "Google"]

    def test_registered_symbols_shows_library_info(self, lmdb_storage):
        """Test registered_symbols property includes library with database.library format."""
        arctic = lmdb_storage.create_arctic()
        lib = arctic.create_library("jblackburn.market_data")
        lib.write("prices", pd.DataFrame({"x": [1, 2, 3]}))

        with arctic.duckdb() as ddb:
            ddb.register_symbol("jblackburn.market_data", "prices")
            symbols = ddb.registered_symbols

        assert "prices" in symbols
        assert symbols["prices"]["library"] == "jblackburn.market_data"
        assert symbols["prices"]["symbol"] == "prices"


class TestLibraryRegister:
    """Tests for lib.duckdb_register() method that materializes symbols as DuckDB tables."""

    def test_register_all_symbols(self, lmdb_library):
        """Test registering all symbols in a library."""
        lib = lmdb_library

        # Write multiple symbols
        lib.write("prices", pd.DataFrame({"ticker": ["AAPL", "GOOG"], "price": [150.0, 2800.0]}))
        lib.write("trades", pd.DataFrame({"ticker": ["AAPL"], "quantity": [100]}))
        lib.write("positions", pd.DataFrame({"ticker": ["GOOG"], "shares": [50]}))

        # Create external DuckDB connection
        conn = duckdb.connect(":memory:")

        # Register all symbols
        symbols = lib.duckdb_register(conn)

        # Verify SHOW TABLES lists all symbols
        tables_result = conn.execute("SHOW TABLES").fetchall()
        table_names = [row[0] for row in tables_result]
        assert "prices" in table_names
        assert "trades" in table_names
        assert "positions" in table_names

        # Verify data can be queried
        result = conn.execute("SELECT * FROM prices ORDER BY ticker").fetchdf()
        assert len(result) == 2
        assert list(result["ticker"]) == ["AAPL", "GOOG"]

        # Verify return value
        assert set(symbols) == {"prices", "trades", "positions"}

        conn.close()

    def test_register_specific_symbols(self, lmdb_library):
        """Test registering only specific symbols."""
        lib = lmdb_library

        lib.write("sym1", pd.DataFrame({"a": [1, 2]}))
        lib.write("sym2", pd.DataFrame({"b": [3, 4]}))
        lib.write("sym3", pd.DataFrame({"c": [5, 6]}))

        conn = duckdb.connect(":memory:")

        # Register only 2 of 3 symbols
        symbols = lib.duckdb_register(conn, symbols=["sym1", "sym3"])

        tables_result = conn.execute("SHOW TABLES").fetchall()
        table_names = [row[0] for row in tables_result]
        assert "sym1" in table_names
        assert "sym3" in table_names
        assert "sym2" not in table_names

        assert set(symbols) == {"sym1", "sym3"}

        conn.close()

    def test_register_multiple_queries(self, lmdb_library):
        """Test that registered data can be queried multiple times (materialized, not streaming)."""
        lib = lmdb_library

        lib.write("data", pd.DataFrame({"x": [1, 2, 3, 4, 5]}))

        conn = duckdb.connect(":memory:")
        lib.duckdb_register(conn)

        # Query multiple times - should work if data is materialized
        result1 = conn.execute("SELECT SUM(x) as total FROM data").fetchone()[0]
        result2 = conn.execute("SELECT COUNT(*) as cnt FROM data").fetchone()[0]
        result3 = conn.execute("SELECT AVG(x) as avg FROM data").fetchone()[0]

        assert result1 == 15
        assert result2 == 5
        assert result3 == pytest.approx(3.0)

        conn.close()

    def test_register_returns_symbol_list(self, lmdb_library):
        """Test that duckdb_register() returns list of registered symbols."""
        lib = lmdb_library

        lib.write("sym_a", pd.DataFrame({"x": [1]}))
        lib.write("sym_b", pd.DataFrame({"y": [2]}))

        conn = duckdb.connect(":memory:")
        symbols = lib.duckdb_register(conn)

        assert isinstance(symbols, list)
        assert set(symbols) == {"sym_a", "sym_b"}

        conn.close()

    def test_register_with_as_of(self, lmdb_library):
        """Test registering a specific version using as_of parameter."""
        lib = lmdb_library

        # Write multiple versions
        lib.write("data", pd.DataFrame({"x": [1, 2, 3]}))  # version 0
        lib.write("data", pd.DataFrame({"x": [10, 20, 30]}))  # version 1

        conn = duckdb.connect(":memory:")

        # Register version 0
        lib.duckdb_register(conn, as_of=0)

        result = conn.execute("SELECT SUM(x) as total FROM data").fetchone()[0]
        assert result == 6  # 1 + 2 + 3 from version 0

        conn.close()

    def test_register_invalid_connection(self, lmdb_library):
        """Test that passing invalid connection raises error."""
        lib = lmdb_library

        lib.write("data", pd.DataFrame({"x": [1, 2, 3]}))

        # Pass None
        with pytest.raises((TypeError, AttributeError)):
            lib.duckdb_register(None)

        # Pass invalid object
        with pytest.raises((TypeError, AttributeError)):
            lib.duckdb_register("not a connection")


class TestArcticRegister:
    """Tests for arctic.duckdb_register() method that registers symbols from multiple libraries."""

    def test_register_all_libraries(self, lmdb_storage):
        """Test registering symbols from all libraries with library__symbol prefix."""
        arctic = lmdb_storage.create_arctic()

        # Create multiple libraries with symbols
        lib1 = arctic.create_library("lib1")
        lib2 = arctic.create_library("lib2")

        lib1.write("prices", pd.DataFrame({"ticker": ["AAPL"], "price": [150.0]}))
        lib1.write("trades", pd.DataFrame({"ticker": ["AAPL"], "qty": [100]}))
        lib2.write("positions", pd.DataFrame({"ticker": ["GOOG"], "shares": [50]}))

        conn = duckdb.connect(":memory:")

        # Register all libraries
        tables = arctic.duckdb_register(conn)

        # Verify table names have library__symbol prefix
        tables_result = conn.execute("SHOW TABLES").fetchall()
        table_names = [row[0] for row in tables_result]
        assert "lib1__prices" in table_names
        assert "lib1__trades" in table_names
        assert "lib2__positions" in table_names

        # Verify data can be queried
        result = conn.execute("SELECT * FROM lib1__prices").fetchdf()
        assert len(result) == 1
        assert result["ticker"].iloc[0] == "AAPL"

        # Verify return value
        assert set(tables) == {"lib1__prices", "lib1__trades", "lib2__positions"}

        conn.close()

    def test_register_specific_libraries(self, lmdb_storage):
        """Test registering symbols from only specific libraries."""
        arctic = lmdb_storage.create_arctic()

        lib1 = arctic.create_library("lib1")
        lib2 = arctic.create_library("lib2")
        lib3 = arctic.create_library("lib3")

        lib1.write("data1", pd.DataFrame({"a": [1]}))
        lib2.write("data2", pd.DataFrame({"b": [2]}))
        lib3.write("data3", pd.DataFrame({"c": [3]}))

        conn = duckdb.connect(":memory:")

        # Register only lib1 and lib3
        tables = arctic.duckdb_register(conn, libraries=["lib1", "lib3"])

        tables_result = conn.execute("SHOW TABLES").fetchall()
        table_names = [row[0] for row in tables_result]
        assert "lib1__data1" in table_names
        assert "lib3__data3" in table_names
        assert "lib2__data2" not in table_names

        assert set(tables) == {"lib1__data1", "lib3__data3"}

        conn.close()

    def test_register_returns_table_names(self, lmdb_storage):
        """Test that duckdb_register() returns list of table names with library prefix."""
        arctic = lmdb_storage.create_arctic()

        lib1 = arctic.create_library("testlib")
        lib1.write("sym1", pd.DataFrame({"x": [1]}))
        lib1.write("sym2", pd.DataFrame({"y": [2]}))

        conn = duckdb.connect(":memory:")
        tables = arctic.duckdb_register(conn)

        assert isinstance(tables, list)
        assert set(tables) == {"testlib__sym1", "testlib__sym2"}

        conn.close()
