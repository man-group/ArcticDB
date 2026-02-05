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

    def test_output_format_pandas(self, lmdb_library):
        """Test SQL with explicit Pandas output format."""
        lib = lmdb_library
        df = pd.DataFrame({"x": [1, 2, 3]})
        lib.write("test_symbol", df)

        result = lib.sql("SELECT * FROM test_symbol", output_format=OutputFormat.PANDAS)

        assert isinstance(result.data, pd.DataFrame)
        assert list(result.data["x"]) == [1, 2, 3]

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

    def test_join_two_symbols(self, lmdb_library):
        """Test JOIN query across two symbols using lib.sql() directly."""
        lib = lmdb_library

        trades = pd.DataFrame({
            "ticker": ["AAPL", "GOOG", "AAPL"],
            "quantity": [100, 200, 150]
        })
        prices = pd.DataFrame({
            "ticker": ["AAPL", "GOOG", "MSFT"],
            "price": [150.0, 2800.0, 300.0]
        })

        lib.write("trades", trades)
        lib.write("prices", prices)

        result = lib.sql("""
            SELECT t.ticker, t.quantity, p.price, t.quantity * p.price as notional
            FROM trades t
            JOIN prices p ON t.ticker = p.ticker
            ORDER BY t.ticker, t.quantity
        """)

        assert len(result.data) == 3  # AAPL (2 rows) + GOOG (1 row)
        assert "notional" in result.data.columns
        assert set(result.data["ticker"]) == {"AAPL", "GOOG"}
        # Verify symbol field contains both symbols
        assert "trades" in result.symbol
        assert "prices" in result.symbol

    def test_join_with_aggregation(self, lmdb_library):
        """Test JOIN with GROUP BY using lib.sql() directly."""
        lib = lmdb_library

        orders = pd.DataFrame({
            "product_id": [1, 1, 2, 2, 3],
            "quantity": [10, 20, 5, 15, 8]
        })
        products = pd.DataFrame({
            "product_id": [1, 2, 3],
            "name": ["Widget", "Gadget", "Gizmo"],
            "price": [10.0, 25.0, 15.0]
        })

        lib.write("orders", orders)
        lib.write("products", products)

        result = lib.sql("""
            SELECT p.name, SUM(o.quantity) as total_qty, SUM(o.quantity * p.price) as revenue
            FROM orders o
            JOIN products p ON o.product_id = p.product_id
            GROUP BY p.name
            ORDER BY p.name
        """)

        assert len(result.data) == 3
        assert list(result.data["name"]) == ["Gadget", "Gizmo", "Widget"]
        assert list(result.data["total_qty"]) == [20, 8, 30]
        assert list(result.data["revenue"]) == [500.0, 120.0, 300.0]

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

        assert len(result.data) == 0

    def test_dataframe_with_nulls(self, lmdb_library):
        """Test SQL on DataFrame with null values."""
        lib = lmdb_library
        df = pd.DataFrame({"x": [1, None, 3], "y": [None, "b", None]})
        lib.write("test_symbol", df)

        result = lib.sql("SELECT * FROM test_symbol WHERE x IS NOT NULL")

        assert len(result.data) == 2  # Two non-null x values

    def test_special_characters_in_values(self, lmdb_library):
        """Test SQL on DataFrame with special characters in string values."""
        lib = lmdb_library
        df = pd.DataFrame({"text": ["hello", "world's", '"quoted"', "new\nline"]})
        lib.write("test_symbol", df)

        result = lib.sql("SELECT * FROM test_symbol")

        assert len(result.data) == 4
        assert "world's" in list(result.data["text"])

    def test_large_string_values(self, lmdb_library):
        """Test SQL on DataFrame with large string values."""
        lib = lmdb_library
        large_string = "x" * 10000
        df = pd.DataFrame({"text": [large_string, "small"]})
        lib.write("test_symbol", df)

        result = lib.sql("SELECT LENGTH(text) as len FROM test_symbol")

        assert result.data["len"].max() == 10000

    def test_float_special_values(self, lmdb_library):
        """Test SQL on DataFrame with special float values."""
        lib = lmdb_library
        df = pd.DataFrame({"x": [1.0, float("inf"), float("-inf"), float("nan")]})
        lib.write("test_symbol", df)

        result = lib.sql("SELECT * FROM test_symbol WHERE x = 1.0")

        assert len(result.data) == 1

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

        assert len(result.data) == 3

    def test_boolean_columns(self, lmdb_library):
        """Test SQL on DataFrame with boolean columns."""
        lib = lmdb_library
        df = pd.DataFrame({"flag": [True, False, True], "value": [1, 2, 3]})
        lib.write("test_symbol", df)

        result = lib.sql("SELECT SUM(value) as total FROM test_symbol WHERE flag")

        assert result.data["total"].iloc[0] == 4  # 1 + 3


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
        trades = pd.DataFrame({
            "ticker": ["AAPL", "GOOG", "MSFT"],
            "quantity": [100, 200, 150]
        })
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

        trades = pd.DataFrame({
            "ticker": ["AAPL", "GOOG", "AAPL", "MSFT"],
            "price": [150.0, 2800.0, 151.0, 300.0],
            "quantity": [100, 50, 200, 75]
        })
        lib.write("trades", trades)

        result = lib.sql("""
            SELECT ticker, AVG(price) as avg_price, SUM(quantity) as total_qty
            FROM trades
            GROUP BY ticker
            ORDER BY total_qty DESC
        """)

        assert len(result.data) == 3
        # AAPL has total_qty 300, should be first
        assert result.data.iloc[0]["ticker"] == "AAPL"
        assert result.data.iloc[0]["total_qty"] == 300
        assert result.data.iloc[0]["avg_price"] == pytest.approx(150.5)

    def test_join_with_market_value(self, lmdb_library):
        """Test JOIN example calculating market value."""
        lib = lmdb_library

        trades = pd.DataFrame({
            "ticker": ["AAPL", "GOOG", "AAPL", "MSFT"],
            "price": [150.0, 2800.0, 151.0, 300.0],
            "quantity": [100, 50, 200, 75]
        })
        prices = pd.DataFrame({
            "ticker": ["AAPL", "GOOG", "MSFT"],
            "current_price": [155.0, 2850.0, 310.0]
        })
        lib.write("trades", trades)
        lib.write("prices", prices)

        result = lib.sql("""
            SELECT t.ticker, t.quantity, p.current_price,
                   t.quantity * p.current_price as market_value
            FROM trades t
            JOIN prices p ON t.ticker = p.ticker
        """)

        assert len(result.data) == 4  # All trades have matching prices
        assert "market_value" in result.data.columns
        # Check one calculation: AAPL 100 * 155 = 15500
        aapl_rows = result.data[result.data["ticker"] == "AAPL"]
        assert 15500.0 in list(aapl_rows["market_value"])

    def test_window_function_lag_daily_returns(self, lmdb_library):
        """Test Financial Analytics example: daily returns with LAG window function."""
        lib = lmdb_library

        # Create price data with dates
        prices = pd.DataFrame({
            "ticker": ["AAPL", "AAPL", "AAPL", "GOOG", "GOOG", "GOOG"],
            "date": pd.to_datetime([
                "2024-01-01", "2024-01-02", "2024-01-03",
                "2024-01-01", "2024-01-02", "2024-01-03"
            ]),
            "close": [150.0, 152.0, 151.0, 2800.0, 2850.0, 2820.0]
        })
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

        assert len(result.data) == 6
        assert "daily_return" in result.data.columns
        # First day of each ticker should have NULL return
        aapl_returns = result.data[result.data["ticker"] == "AAPL"]["daily_return"].tolist()
        assert pd.isna(aapl_returns[0])  # First day has no previous
        # Second day: (152 - 150) / 150 = 0.0133...
        assert aapl_returns[1] == pytest.approx(2.0 / 150.0)

    def test_portfolio_value_calculation(self, lmdb_library):
        """Test Financial Analytics example: portfolio value with positions and prices."""
        lib = lmdb_library

        positions = pd.DataFrame({
            "ticker": ["AAPL", "GOOG", "MSFT"],
            "shares": [100, 50, 75]
        })
        prices = pd.DataFrame({
            "ticker": ["AAPL", "GOOG", "MSFT"],
            "price": [155.0, 2850.0, 310.0]
        })
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
        ticks = pd.DataFrame({
            "price": [100.0, 102.0, 99.0, 101.0, 105.0, 103.0, 102.0, 108.0],
            "volume": [1000, 500, 800, 1200, 600, 900, 700, 1100]
        }, index=pd.to_datetime([
            "2024-01-01 09:30:00", "2024-01-01 10:00:00",
            "2024-01-01 11:00:00", "2024-01-01 16:00:00",
            "2024-01-02 09:30:00", "2024-01-02 10:00:00",
            "2024-01-02 11:00:00", "2024-01-02 16:00:00",
        ]))
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

        assert len(result.data) == 2  # Two days
        day1 = result.data.iloc[0]
        day2 = result.data.iloc[1]

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
        prices = pd.DataFrame({
            "price": [100.0, 101.0, 103.0, 104.0]
        }, index=pd.to_datetime([
            "2024-01-01", "2024-01-02", "2024-01-04", "2024-01-05"  # Note: Jan 3 is missing
        ]))
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
        assert len(result_v0.data) == 1

        # Query latest (version 1)
        result_v1 = lib.sql("SELECT * FROM trades")
        assert len(result_v1.data) == 2


class TestSchemaDDLQueries:
    """Tests for schema introspection via DuckDB DDL queries (DESCRIBE, SHOW COLUMNS)."""

    def test_describe_basic_types(self, lmdb_library):
        """Test DESCRIBE query returns correct types for basic columns."""
        lib = lmdb_library

        df = pd.DataFrame({
            "int64_col": np.array([1, 2, 3], dtype=np.int64),
            "float64_col": np.array([1.5, 2.5, 3.5], dtype=np.float64),
            "string_col": ["a", "b", "c"],
            "bool_col": [True, False, True],
        })
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

        df = pd.DataFrame({
            "int8_col": np.array([1, 2, 3], dtype=np.int8),
            "int16_col": np.array([1, 2, 3], dtype=np.int16),
            "int32_col": np.array([1, 2, 3], dtype=np.int32),
            "int64_col": np.array([1, 2, 3], dtype=np.int64),
            "uint8_col": np.array([1, 2, 3], dtype=np.uint8),
            "uint16_col": np.array([1, 2, 3], dtype=np.uint16),
            "uint32_col": np.array([1, 2, 3], dtype=np.uint32),
            "uint64_col": np.array([1, 2, 3], dtype=np.uint64),
        })
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

        df = pd.DataFrame({
            "float32_col": np.array([1.5, 2.5, 3.5], dtype=np.float32),
            "float64_col": np.array([1.5, 2.5, 3.5], dtype=np.float64),
        })
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

        df = pd.DataFrame({
            "value": [1.0, 2.0, 3.0]
        }, index=pd.to_datetime(["2024-01-01", "2024-01-02", "2024-01-03"]))
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

        df = pd.DataFrame({
            "x": [1, 2, 3],
            "y": [1.0, 2.0, 3.0],
        })
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

        df = pd.DataFrame({
            "int_col": np.array([1, 2, 3], dtype=np.int64),
            "float_col": np.array([1.5, 2.5, 3.5], dtype=np.float64),
            "str_col": ["a", "b", "c"],
        })
        lib.write("test_symbol", df)

        # Use lib.sql() directly with DESCRIBE
        result = lib.sql("DESCRIBE test_symbol")

        type_map = dict(zip(result.data["column_name"], result.data["column_type"]))
        assert type_map["int_col"] == "BIGINT"
        assert type_map["float_col"] == "DOUBLE"
        assert type_map["str_col"] == "VARCHAR"

    def test_sql_describe_integer_types(self, lmdb_library):
        """Test lib.sql() DESCRIBE with various integer types."""
        lib = lmdb_library

        df = pd.DataFrame({
            "int8_col": np.array([1], dtype=np.int8),
            "int16_col": np.array([1], dtype=np.int16),
            "int32_col": np.array([1], dtype=np.int32),
            "int64_col": np.array([1], dtype=np.int64),
            "uint8_col": np.array([1], dtype=np.uint8),
            "uint16_col": np.array([1], dtype=np.uint16),
            "uint32_col": np.array([1], dtype=np.uint32),
            "uint64_col": np.array([1], dtype=np.uint64),
        })
        lib.write("test_symbol", df)

        result = lib.sql("DESCRIBE test_symbol")
        type_map = dict(zip(result.data["column_name"], result.data["column_type"]))

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

        df = pd.DataFrame({
            "value": [1.0, 2.0, 3.0]
        }, index=pd.to_datetime(["2024-01-01", "2024-01-02", "2024-01-03"]))
        lib.write("test_symbol", df)

        result = lib.sql("DESCRIBE test_symbol")
        type_map = dict(zip(result.data["column_name"], result.data["column_type"]))

        assert "index" in type_map
        assert "TIMESTAMP" in type_map["index"]

    def test_sql_show_equivalent_to_describe(self, lmdb_library):
        """Test lib.sql() SHOW returns same as DESCRIBE."""
        lib = lmdb_library

        df = pd.DataFrame({"x": [1, 2], "y": [3.0, 4.0]})
        lib.write("test_symbol", df)

        describe_result = lib.sql("DESCRIBE test_symbol")
        show_result = lib.sql("SHOW test_symbol")

        assert list(describe_result.data["column_name"]) == list(show_result.data["column_name"])
        assert list(describe_result.data["column_type"]) == list(show_result.data["column_type"])

    def test_sql_show_all_tables_discovers_library(self, lmdb_library):
        """Test lib.sql() with SHOW ALL TABLES discovers all symbols in library."""
        lib = lmdb_library

        # Write multiple symbols
        lib.write("prices", pd.DataFrame({"price": [100.0, 101.0]}))
        lib.write("trades", pd.DataFrame({"qty": [10, 20]}))
        lib.write("positions", pd.DataFrame({"shares": [100, 200]}))

        # SHOW ALL TABLES should discover all library symbols
        result = lib.sql("SHOW ALL TABLES")

        table_names = set(result.data["name"])
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

        table_names = set(result.data["name"])
        assert "symbol_a" in table_names
        assert "symbol_b" in table_names
