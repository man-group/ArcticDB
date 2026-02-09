"""
Copyright 2026 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, version 2.0.
"""

"""
Tests for DuckDBContext and Library.sql() / Library.duckdb() integration.

Covers: simple queries, case sensitivity, timestamps, context manager lifecycle,
edge cases, external connections, query_builder, error handling, and output formats.

See also:
- test_arctic_duckdb.py — ArcticDuckDBContext, SHOW DATABASES, cross-library joins
- test_schema_ddl.py — DESCRIBE, SHOW TABLES, SHOW ALL TABLES
- test_doc_examples.py — tutorial examples and explain() introspection
"""

import numpy as np
import pandas as pd
import pytest

from arcticdb.options import OutputFormat
from arcticdb.version_store.duckdb.duckdb import _extract_symbols_from_query

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
            result = ddb.sql("SELECT * FROM test_symbol")

        assert len(result) == 3

    def test_auto_register_single_symbol(self, lmdb_library):
        """Test that sql() auto-registers symbols without explicit register_symbol()."""
        lib = lmdb_library
        lib.write("trades", pd.DataFrame({"ticker": ["AAPL", "GOOG"], "price": [150.0, 2800.0]}))

        with lib.duckdb() as ddb:
            result = ddb.sql("SELECT * FROM trades WHERE price > 200")

        assert len(result) == 1
        assert result.iloc[0]["ticker"] == "GOOG"

    def test_auto_register_join(self, lmdb_library):
        """Test that sql() auto-registers multiple symbols for a JOIN."""
        lib = lmdb_library
        lib.write("trades", pd.DataFrame({"ticker": ["AAPL", "GOOG"], "quantity": [100, 200]}))
        lib.write("prices", pd.DataFrame({"ticker": ["AAPL", "GOOG"], "price": [150.0, 2800.0]}))

        with lib.duckdb() as ddb:
            result = ddb.sql("""
                SELECT t.ticker, t.quantity * p.price as notional
                FROM trades t JOIN prices p ON t.ticker = p.ticker
                ORDER BY notional DESC
            """)

        assert len(result) == 2
        assert result.iloc[0]["notional"] == pytest.approx(560000.0)

    def test_auto_register_case_insensitive(self, lmdb_library):
        """Test that auto-registration resolves case-insensitive symbol names."""
        lib = lmdb_library
        lib.write("MyData", pd.DataFrame({"x": [1, 2, 3]}))

        with lib.duckdb() as ddb:
            # SQL uses lowercase, symbol is mixed-case
            result = ddb.sql("SELECT SUM(x) as total FROM mydata")

        assert result.iloc[0]["total"] == 6

    def test_auto_register_skips_already_registered(self, lmdb_library):
        """Test that auto-registration skips symbols that were explicitly registered."""
        lib = lmdb_library
        lib.write("trades", pd.DataFrame({"x": [1, 2, 3]}))

        with lib.duckdb() as ddb:
            # Explicitly register with a filter
            ddb.register_symbol("trades", columns=["x"])
            # sql() should use the already-registered version, not re-register
            result = ddb.sql("SELECT SUM(x) as total FROM trades")

        assert result.iloc[0]["total"] == 6

    def test_auto_register_mixed(self, lmdb_library):
        """Test mix of explicitly registered and auto-registered symbols."""
        lib = lmdb_library
        lib.write("trades", pd.DataFrame({"ticker": ["AAPL", "GOOG"], "quantity": [100, 200]}))
        lib.write("prices", pd.DataFrame({"ticker": ["AAPL", "GOOG"], "price": [150.0, 2800.0]}))

        with lib.duckdb() as ddb:
            # Only register one symbol explicitly
            ddb.register_symbol("trades")
            # prices should be auto-registered
            result = ddb.sql("""
                SELECT t.ticker, p.price
                FROM trades t JOIN prices p ON t.ticker = p.ticker
                ORDER BY t.ticker
            """)

        assert len(result) == 2
        assert result.iloc[0]["ticker"] == "AAPL"

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

            result = ddb.sql("""
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
            result = ddb.sql("SELECT * FROM my_table")

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

            jan_count = ddb.sql("SELECT COUNT(*) as cnt FROM jan_data")["cnt"].iloc[0]
            feb_count = ddb.sql("SELECT COUNT(*) as cnt FROM feb_data")["cnt"].iloc[0]

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
            result = ddb.sql("SELECT * FROM test_symbol", output_format=OutputFormat.PYARROW)

        assert isinstance(result, pa.Table)

    def test_output_format_polars(self, lmdb_library):
        """Test context query with Polars output."""
        pl = pytest.importorskip("polars")

        lib = lmdb_library
        df = pd.DataFrame({"x": [1, 2, 3]})
        lib.write("test_symbol", df)

        with lib.duckdb() as ddb:
            ddb.register_symbol("test_symbol")
            result = ddb.sql("SELECT * FROM test_symbol", output_format=OutputFormat.POLARS)

        assert isinstance(result, pl.DataFrame)

    def test_output_format_pandas(self, lmdb_library):
        """Test context query with explicit Pandas output."""
        lib = lmdb_library
        df = pd.DataFrame({"x": [1, 2, 3]})
        lib.write("test_symbol", df)

        with lib.duckdb() as ddb:
            ddb.register_symbol("test_symbol")
            result = ddb.sql("SELECT * FROM test_symbol", output_format=OutputFormat.PANDAS)

        assert isinstance(result, pd.DataFrame)
        assert list(result["x"]) == [1, 2, 3]

    def test_method_chaining(self, lmdb_library):
        """Test method chaining with register_symbol."""
        lib = lmdb_library
        lib.write("sym1", pd.DataFrame({"x": [1, 2]}))
        lib.write("sym2", pd.DataFrame({"y": [3, 4]}))

        with lib.duckdb() as ddb:
            result = ddb.register_symbol("sym1").register_symbol("sym2").sql("SELECT * FROM sym1, sym2")

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
            result = ddb.sql("SELECT * FROM filtered")

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

    def test_query_without_registration_auto_registers(self, lmdb_library):
        """Test that querying without explicit registration auto-registers from the library."""
        lib = lmdb_library
        lib.write("test_symbol", pd.DataFrame({"x": [1, 2, 3]}))

        with lib.duckdb() as ddb:
            result = ddb.sql("SELECT * FROM test_symbol")

        assert len(result) == 3
        assert list(result["x"]) == [1, 2, 3]

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
            result = ddb.sql("SELECT SUM(x) as total FROM v0")

        assert result["total"].iloc[0] == 6  # 1 + 2 + 3

    def test_with_row_range(self, lmdb_library):
        """Test register_symbol with row_range parameter."""
        lib = lmdb_library
        df = pd.DataFrame({"x": np.arange(100)})
        lib.write("test_symbol", df)

        with lib.duckdb() as ddb:
            # Read only rows 10-20
            ddb.register_symbol("test_symbol", row_range=(10, 20))
            result = ddb.sql("SELECT COUNT(*) as cnt FROM test_symbol")

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
            result = ddb.sql("SELECT * FROM test_symbol")
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
            result = ddb.sql("""
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
            result = ddb.sql("""
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
            result = ddb.sql("SELECT * FROM arcticdb_data")
            assert len(result) == 3

        # Existing tables should still be intact
        assert conn.execute("SELECT col FROM existing1").fetchone()[0] == "a"
        assert conn.execute("SELECT col FROM existing2").fetchone()[0] == "b"
        conn.close()


class TestQueryBuilderParameter:
    """Tests for the query_builder parameter on register_symbol()."""

    def test_query_builder_filters_before_sql(self, lmdb_library):
        """Test that query_builder pre-filters data before DuckDB sees it."""
        from arcticdb import QueryBuilder

        lib = lmdb_library
        df = pd.DataFrame({"category": ["A", "B", "A", "B"], "value": [10, 20, 30, 40]})
        lib.write("data", df)

        q = QueryBuilder()
        q = q[q["category"] == "A"]

        with lib.duckdb() as ddb:
            ddb.register_symbol("data", query_builder=q)
            result = ddb.sql("SELECT SUM(value) as total FROM data")

        assert result["total"].iloc[0] == 40  # 10 + 30 (only category A)

    def test_query_builder_with_columns(self, lmdb_library):
        """Test query_builder combined with columns parameter."""
        from arcticdb import QueryBuilder

        lib = lmdb_library
        df = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6], "c": [7, 8, 9]})
        lib.write("data", df)

        q = QueryBuilder()
        q = q[q["a"] > 1]

        with lib.duckdb() as ddb:
            ddb.register_symbol("data", columns=["a", "b"], query_builder=q)
            result = ddb.sql("SELECT * FROM data ORDER BY a")

        assert list(result.columns) == ["a", "b"]
        assert list(result["a"]) == [2, 3]


class TestRegisterSymbolErrors:
    """Tests for error handling in register_symbol()."""

    def test_register_nonexistent_symbol_raises(self, lmdb_library):
        """Test that registering a non-existent symbol raises a clear error."""
        lib = lmdb_library

        with lib.duckdb() as ddb:
            with pytest.raises(Exception):
                ddb.register_symbol("does_not_exist")

    def test_auto_register_nonexistent_symbol_raises(self, lmdb_library):
        """Test that auto-registration of a non-existent symbol raises."""
        lib = lmdb_library

        with lib.duckdb() as ddb:
            with pytest.raises(Exception):
                ddb.sql("SELECT * FROM does_not_exist")


class TestContextManagerCleanup:
    """Tests for context manager cleanup and error handling."""

    def test_cleanup_on_exception(self, lmdb_library):
        """Test that symbols are unregistered even when user code throws."""
        import duckdb as duckdb_mod

        lib = lmdb_library
        lib.write("test_symbol", pd.DataFrame({"x": [1, 2, 3]}))

        conn = duckdb_mod.connect(":memory:")
        try:
            with lib.duckdb(connection=conn) as ddb:
                ddb.register_symbol("test_symbol")
                # Verify it's registered
                assert conn.execute("SELECT COUNT(*) FROM test_symbol").fetchone()[0] == 3
                raise ValueError("simulated error")
        except ValueError:
            pass

        # After context exit, the symbol should be unregistered from the shared connection
        with pytest.raises(duckdb_mod.CatalogException):
            conn.execute("SELECT * FROM test_symbol")
        conn.close()

    def test_cleanup_on_exception_internal_conn(self, lmdb_library):
        """Test that internal connections are closed even when user code throws."""
        lib = lmdb_library
        lib.write("test_symbol", pd.DataFrame({"x": [1, 2, 3]}))

        ctx = lib.duckdb()
        try:
            with ctx as ddb:
                ddb.register_symbol("test_symbol")
                raise RuntimeError("simulated error")
        except RuntimeError:
            pass

        # Connection should be cleaned up - accessing it should fail
        assert ctx._conn is None


class TestOutputFormatErrors:
    """Tests for invalid output_format handling."""

    def test_invalid_output_format_raises(self, lmdb_library):
        """Test that an invalid output_format string raises ValueError."""
        lib = lmdb_library
        lib.write("test_symbol", pd.DataFrame({"x": [1, 2, 3]}))

        with pytest.raises(ValueError, match="Unknown OutputFormat"):
            lib.sql("SELECT * FROM test_symbol", output_format="xml")

    def test_invalid_output_format_context_manager(self, lmdb_library):
        """Test invalid output_format in context manager sql()."""
        lib = lmdb_library
        lib.write("test_symbol", pd.DataFrame({"x": [1, 2, 3]}))

        with lib.duckdb() as ddb:
            ddb.register_symbol("test_symbol")
            with pytest.raises(ValueError, match="Unknown OutputFormat"):
                ddb.sql("SELECT * FROM test_symbol", output_format="csv")

    def test_output_format_case_insensitive(self, lmdb_library):
        """Test that output_format strings are case-insensitive."""
        import pyarrow as pa

        lib = lmdb_library
        lib.write("test_symbol", pd.DataFrame({"x": [1, 2, 3]}))

        result = lib.sql("SELECT * FROM test_symbol", output_format="PyArrow")
        assert isinstance(result, pa.Table)

        result = lib.sql("SELECT * FROM test_symbol", output_format="PANDAS")
        assert isinstance(result, pd.DataFrame)


class TestTimestampPrecisions:
    """Tests for non-nanosecond timestamp data with DuckDB queries."""

    def test_microsecond_timestamps_queryable(self, lmdb_library):
        """Test that data written with microsecond timestamps can be queried via SQL."""
        lib = lmdb_library

        # Write data with microsecond precision timestamps
        index_us = pd.to_datetime(["2024-01-01", "2024-01-02", "2024-01-03"]).astype("datetime64[us]")
        df = pd.DataFrame({"value": [1.0, 2.0, 3.0]}, index=index_us)
        lib.write("us_data", df)

        # ArcticDB converts to ns on write; verify SQL queries work
        result = lib.sql("SELECT * FROM us_data WHERE value > 1.0 ORDER BY index")

        assert len(result) == 2
        assert list(result["value"]) == [2.0, 3.0]

    def test_millisecond_timestamps_queryable(self, lmdb_library):
        """Test that data written with millisecond timestamps can be queried via SQL."""
        lib = lmdb_library

        # Write data with millisecond precision timestamps
        index_ms = pd.to_datetime(["2024-06-01", "2024-06-02", "2024-06-03"]).astype("datetime64[ms]")
        df = pd.DataFrame({"price": [10, 20, 30]}, index=index_ms)
        lib.write("ms_data", df)

        result = lib.sql("SELECT SUM(price) as total FROM ms_data")

        assert result["total"].iloc[0] == 60

    def test_mixed_precision_join(self, lmdb_library):
        """Test joining symbols originally written with different timestamp precisions."""
        lib = lmdb_library

        dates = pd.to_datetime(["2024-01-01", "2024-01-02"])

        # Write one symbol with microsecond timestamps
        df_us = pd.DataFrame({"price": [100.0, 200.0]}, index=dates.astype("datetime64[us]"))
        lib.write("prices_us", df_us)

        # Write another with millisecond timestamps
        df_ms = pd.DataFrame({"volume": [1000, 2000]}, index=dates.astype("datetime64[ms]"))
        lib.write("volumes_ms", df_ms)

        # Both are stored as ns, so JOIN on index should work
        result = lib.sql("""
            SELECT p.price, v.volume, p.price * v.volume as notional
            FROM prices_us p
            JOIN volumes_ms v ON p.index = v.index
            ORDER BY p.index
        """)

        assert len(result) == 2
        assert result["notional"].iloc[0] == pytest.approx(100000.0)
        assert result["notional"].iloc[1] == pytest.approx(400000.0)

    def test_timestamp_date_range_filter(self, lmdb_library):
        """Test date range filtering on data originally written with non-ns timestamps."""
        lib = lmdb_library

        index_us = pd.to_datetime(["2024-01-01", "2024-01-15", "2024-02-01", "2024-02-15", "2024-03-01"]).astype(
            "datetime64[us]"
        )
        df = pd.DataFrame({"value": [1, 2, 3, 4, 5]}, index=index_us)
        lib.write("ts_data", df)

        # Date range filter using SQL WHERE on index
        result = lib.sql("""
            SELECT value FROM ts_data
            WHERE index >= '2024-02-01' AND index < '2024-03-01'
            ORDER BY index
        """)

        assert len(result) == 2
        assert list(result["value"]) == [3, 4]


class TestMultiIndexJoins:
    """Tests for SQL JOINs on pandas MultiIndex DataFrames.

    ArcticDB flattens MultiIndex levels into columns. The first level keeps its
    original name; subsequent levels are prefixed with ``__idx__``.  These tests
    verify that joins on those flattened index columns work correctly.
    """

    # -- helpers ---------------------------------------------------------------

    @staticmethod
    def _momentum_df():
        """(date, security_id) -> momentum — mimics a risk-factor panel."""
        dates = pd.to_datetime(["2025-01-02", "2025-01-02", "2025-01-03", "2025-01-03", "2025-01-06", "2025-01-06"])
        sids = [100, 200, 100, 200, 100, 200]
        return pd.DataFrame(
            {"momentum": [-2.7, 0.19, -0.25, 0.27, 0.06, -1.75]},
            index=pd.MultiIndex.from_arrays([dates, sids], names=["date", "security_id"]),
        )

    @staticmethod
    def _inflow_df():
        """(date, security_id) -> inflow — mimics a fund-flow panel."""
        dates = pd.to_datetime(["2025-01-02", "2025-01-02", "2025-01-03", "2025-01-03", "2025-01-06", "2025-01-06"])
        sids = [100, 200, 100, 300, 100, 200]  # sid 300 only in inflow
        return pd.DataFrame(
            {"inflow": [0.5, 0.6, 0.7, 0.8, 0.9, 1.0]},
            index=pd.MultiIndex.from_arrays([dates, sids], names=["date", "security_id"]),
        )

    @staticmethod
    def _analyst_df():
        """Single DatetimeIndex -> analyst_mom — mimics a market-level signal."""
        return pd.DataFrame(
            {"analyst_mom": [0.019, 0.020, 0.021]},
            index=pd.DatetimeIndex(pd.to_datetime(["2025-01-02", "2025-01-03", "2025-01-06"]), name="date"),
        )

    # -- tests -----------------------------------------------------------------

    def test_inner_join_two_multiindex_symbols(self, lmdb_library):
        """INNER JOIN two (date, security_id) MultiIndex symbols on both index levels."""
        lib = lmdb_library
        lib.write("momentum", self._momentum_df())
        lib.write("inflow", self._inflow_df())

        result = lib.sql("""
            SELECT m.date, m.security_id,
                   m.momentum, i.inflow
            FROM momentum m
            JOIN inflow i
              ON m.date = i.date
             AND m.security_id = i.security_id
            ORDER BY m.date, m.security_id
        """)

        # Index reconstructed from (date, security_id)
        assert isinstance(result.index, pd.MultiIndex)
        assert result.index.names == ["date", "security_id"]
        # sid 300 is only in inflow, so inner join should exclude it
        assert len(result) == 5
        assert set(result.index.get_level_values("security_id")) == {100, 200}
        # Check a specific row: 2025-01-02, sid=100
        row = result.loc[(pd.Timestamp("2025-01-02"), 100)]
        assert row["momentum"] == pytest.approx(-2.7)
        assert row["inflow"] == pytest.approx(0.5)

    def test_left_join_two_multiindex_symbols(self, lmdb_library):
        """LEFT JOIN preserves all rows from the left table even when right has no match."""
        lib = lmdb_library
        lib.write("momentum", self._momentum_df())
        lib.write("inflow", self._inflow_df())

        result = lib.sql("""
            SELECT m.date, m.security_id,
                   m.momentum, i.inflow
            FROM momentum m
            LEFT JOIN inflow i
              ON m.date = i.date
             AND m.security_id = i.security_id
            ORDER BY m.date, m.security_id
        """)

        # All 6 momentum rows should appear; sid 200 on 2025-01-03 has no match
        assert isinstance(result.index, pd.MultiIndex)
        assert len(result) == 6
        no_match = result.loc[(pd.Timestamp("2025-01-03"), 200)]
        assert pd.isna(no_match["inflow"])

    def test_join_multiindex_with_single_index(self, lmdb_library):
        """JOIN a (date, security_id) MultiIndex symbol with a date-only single-index symbol.

        This broadcasts the single-index value across all securities for the
        matching date — a common pattern when enriching a security-level panel
        with a market-level signal.

        The most specific index (date, security_id) is reconstructed.
        """
        lib = lmdb_library
        lib.write("momentum", self._momentum_df())
        lib.write("analyst", self._analyst_df())

        result = lib.sql("""
            SELECT m.date, m.security_id,
                   m.momentum, a.analyst_mom
            FROM momentum m
            JOIN analyst a ON m.date = a.date
            ORDER BY m.date, m.security_id
        """)

        # Most specific index (date, security_id) is reconstructed
        assert isinstance(result.index, pd.MultiIndex)
        assert result.index.names == ["date", "security_id"]
        # Every momentum row should match since all 3 dates exist in analyst
        assert len(result) == 6
        # analyst_mom should be the same for all securities on the same date
        flat = result.reset_index()
        for date_val in flat["date"].unique():
            subset = flat[flat["date"] == date_val]
            assert subset["analyst_mom"].nunique() == 1

        row = result.loc[(pd.Timestamp("2025-01-02"), 100)]
        assert row["analyst_mom"] == pytest.approx(0.019)
        assert row["momentum"] == pytest.approx(-2.7)

    def test_multiindex_join_with_aggregation(self, lmdb_library):
        """JOIN two MultiIndex symbols and aggregate by date.

        Only ``date`` is in the result (not ``security_id``), so the best
        matching index is the single ``date`` index from either symbol.
        """
        lib = lmdb_library
        lib.write("momentum", self._momentum_df())
        lib.write("inflow", self._inflow_df())

        result = lib.sql("""
            SELECT m.date,
                   AVG(m.momentum) AS avg_momentum,
                   SUM(i.inflow) AS total_inflow
            FROM momentum m
            JOIN inflow i
              ON m.date = i.date
             AND m.security_id = i.security_id
            GROUP BY m.date
            ORDER BY m.date
        """)

        assert len(result) == 3
        # 2025-01-02: sids 100,200 match — avg(-2.7, 0.19) = -1.255
        assert result["avg_momentum"].iloc[0] == pytest.approx(-1.255)
        assert result["total_inflow"].iloc[0] == pytest.approx(1.1)

    def test_multiindex_join_with_date_filter(self, lmdb_library):
        """JOIN two MultiIndex symbols with a WHERE clause filtering on the date index."""
        lib = lmdb_library
        lib.write("momentum", self._momentum_df())
        lib.write("inflow", self._inflow_df())

        result = lib.sql("""
            SELECT m.date, m.security_id,
                   m.momentum, i.inflow
            FROM momentum m
            JOIN inflow i
              ON m.date = i.date
             AND m.security_id = i.security_id
            WHERE m.date = '2025-01-06'
            ORDER BY m.security_id
        """)

        assert isinstance(result.index, pd.MultiIndex)
        assert len(result) == 2
        assert list(result.index.get_level_values("security_id")) == [100, 200]

    def test_select_star_shows_clean_column_names(self, lmdb_library):
        """SELECT * on a MultiIndex symbol shows clean column names without __idx__ prefix.

        For a single-symbol query, the original MultiIndex is reconstructed so
        ``date`` and ``security_id`` appear as index levels rather than columns.
        """
        lib = lmdb_library
        lib.write("momentum", self._momentum_df())

        result = lib.sql("SELECT * FROM momentum LIMIT 1")

        # Index reconstructed — date and security_id are now index levels
        assert isinstance(result.index, pd.MultiIndex)
        assert result.index.names == ["date", "security_id"]
        assert "momentum" in result.columns
        # No __idx__ prefix anywhere
        assert "__idx__security_id" not in result.columns
        assert "__idx__security_id" not in result.index.names

    def test_describe_shows_clean_column_names(self, lmdb_library):
        """DESCRIBE on a MultiIndex symbol shows clean column names."""
        lib = lmdb_library
        lib.write("momentum", self._momentum_df())

        schema = lib.sql("DESCRIBE momentum")

        col_names = list(schema["column_name"])
        assert "security_id" in col_names
        assert "__idx__security_id" not in col_names

    def test_multiindex_filter_on_index_column(self, lmdb_library):
        """Single-table WHERE filter on a MultiIndex level uses clean column name."""
        lib = lmdb_library
        lib.write("momentum", self._momentum_df())

        result = lib.sql("""
            SELECT date, security_id, momentum
            FROM momentum
            WHERE security_id = 100
            ORDER BY date
        """)

        assert len(result) == 3
        # Index reconstructed — security_id is in the index
        assert isinstance(result.index, pd.MultiIndex)
        assert all(result.index.get_level_values("security_id") == 100)


class TestIndexReconstruction:
    """Tests for index round-trip: SQL output should reconstruct the original pandas index
    for single-symbol queries when all index columns are present in the result."""

    @staticmethod
    def _multiindex_df():
        """MultiIndex (date, security_id) -> momentum"""
        dates = pd.to_datetime(["2025-01-02", "2025-01-02", "2025-01-03", "2025-01-03"])
        idx = pd.MultiIndex.from_arrays([dates, [100, 200, 100, 200]], names=["date", "security_id"])
        return pd.DataFrame({"momentum": [1.1, 2.2, 3.3, 4.4]}, index=idx)

    @staticmethod
    def _single_index_df():
        """Single DatetimeIndex named 'date' -> value"""
        dates = pd.to_datetime(["2025-01-02", "2025-01-03", "2025-01-04"])
        return pd.DataFrame({"value": [10.0, 20.0, 30.0]}, index=pd.DatetimeIndex(dates, name="date"))

    @staticmethod
    def _rangeindex_df():
        """Default RangeIndex -> a, b"""
        return pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})

    def test_multiindex_roundtrip_via_sql(self, lmdb_library):
        """MultiIndex is reconstructed for single-symbol SELECT *."""
        lib = lmdb_library
        original = self._multiindex_df()
        lib.write("sym", original)

        result = lib.sql("SELECT * FROM sym ORDER BY date, security_id")

        assert isinstance(result.index, pd.MultiIndex)
        assert result.index.names == ["date", "security_id"]
        assert list(result.columns) == ["momentum"]
        pd.testing.assert_frame_equal(result, original)

    def test_single_named_index_roundtrip_via_sql(self, lmdb_library):
        """Single named DatetimeIndex is reconstructed for single-symbol query."""
        lib = lmdb_library
        original = self._single_index_df()
        lib.write("sym", original)

        result = lib.sql("SELECT * FROM sym ORDER BY date")

        assert result.index.name == "date"
        assert list(result.columns) == ["value"]
        pd.testing.assert_frame_equal(result, original)

    def test_rangeindex_stays_flat(self, lmdb_library):
        """RangeIndex symbols stay as RangeIndex (no reconstruction needed)."""
        lib = lmdb_library
        original = self._rangeindex_df()
        lib.write("sym", original)

        result = lib.sql("SELECT * FROM sym")

        assert isinstance(result.index, pd.RangeIndex)
        assert list(result.columns) == ["a", "b"]
        pd.testing.assert_frame_equal(result, original)

    def test_aggregation_drops_index(self, lmdb_library):
        """Aggregation that doesn't include all index columns → no reconstruction."""
        lib = lmdb_library
        lib.write("sym", self._multiindex_df())

        result = lib.sql("SELECT AVG(momentum) AS avg_mom FROM sym")

        # Only one row with aggregation, no index columns present
        assert isinstance(result.index, pd.RangeIndex)
        assert "avg_mom" in result.columns

    def test_partial_index_columns_no_reconstruction(self, lmdb_library):
        """When only some index columns are selected, index is NOT reconstructed."""
        lib = lmdb_library
        lib.write("sym", self._multiindex_df())

        # Select only security_id (missing date) — can't reconstruct full MultiIndex
        result = lib.sql("SELECT security_id, momentum FROM sym")

        assert isinstance(result.index, pd.RangeIndex)
        assert "security_id" in result.columns

    def test_join_reconstructs_best_index(self, lmdb_library):
        """JOINs reconstruct the most specific matching index."""
        lib = lmdb_library
        lib.write("left_sym", self._multiindex_df())
        lib.write("right_sym", self._single_index_df())

        result = lib.sql("""
            SELECT l.date, l.security_id, l.momentum, r.value
            FROM left_sym l
            JOIN right_sym r ON l.date = r.date
            ORDER BY l.date, l.security_id
        """)

        # Most specific index (date, security_id) from left_sym is reconstructed
        assert isinstance(result.index, pd.MultiIndex)
        assert result.index.names == ["date", "security_id"]
        assert list(result.columns) == ["momentum", "value"]

    def test_arrow_output_no_reconstruction(self, lmdb_library):
        """Arrow output format should not attempt index reconstruction."""
        lib = lmdb_library
        lib.write("sym", self._multiindex_df())

        import pyarrow as pa

        result = lib.sql("SELECT * FROM sym", output_format="pyarrow")

        assert isinstance(result, pa.Table)
        assert "date" in result.column_names
        assert "security_id" in result.column_names
        assert "momentum" in result.column_names

    def test_duckdb_context_single_symbol_reconstruction(self, lmdb_library):
        """DuckDBContext.sql() also reconstructs the index for single-symbol queries."""
        lib = lmdb_library
        original = self._multiindex_df()
        lib.write("sym", original)

        with lib.duckdb() as ddb:
            result = ddb.sql("SELECT * FROM sym ORDER BY date, security_id")

        assert isinstance(result.index, pd.MultiIndex)
        assert result.index.names == ["date", "security_id"]
        pd.testing.assert_frame_equal(result, original)

    def test_duckdb_context_multi_symbol_reconstruction(self, lmdb_library):
        """DuckDBContext.sql() reconstructs the best matching index even for JOINs."""
        lib = lmdb_library
        lib.write("sym1", self._multiindex_df())
        lib.write("sym2", self._single_index_df())

        with lib.duckdb() as ddb:
            ddb.register_symbol("sym1")
            ddb.register_symbol("sym2")
            result = ddb.sql("""
                SELECT s1.date, s1.security_id, s1.momentum, s2.value
                FROM sym1 s1
                JOIN sym2 s2 ON s1.date = s2.date
            """)

        # Most specific index (date, security_id) from sym1 is reconstructed
        assert isinstance(result.index, pd.MultiIndex)
        assert result.index.names == ["date", "security_id"]
        assert list(result.columns) == ["momentum", "value"]
