"""
Copyright 2026 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, version 2.0.
"""

"""
Unit tests for duckdb/integration.py - DuckDBContext and Library.sql() integration.

Tests verify the high-level SQL interface for querying ArcticDB data with DuckDB.
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
