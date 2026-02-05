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

# Skip all tests if duckdb is not installed
duckdb = pytest.importorskip("duckdb")


class TestRecordBatchReader:
    """Tests for the ArcticRecordBatchReader class."""

    def test_basic_iteration(self, lmdb_library):
        """Test that we can iterate over record batches."""
        lib = lmdb_library
        df = pd.DataFrame({"x": np.arange(100), "y": np.arange(100, 200)})
        lib.write("test_symbol", df)

        reader = lib.read_as_record_batch_reader("test_symbol")

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

        reader = lib.read_as_record_batch_reader("test_symbol")
        table = reader.read_all()

        import pyarrow as pa

        assert isinstance(table, pa.Table)
        assert len(table) == 50

    def test_schema_property(self, lmdb_library):
        """Test that schema is correctly extracted."""
        lib = lmdb_library
        df = pd.DataFrame({"col_int": [1, 2, 3], "col_float": [1.0, 2.0, 3.0]})
        lib.write("test_symbol", df)

        reader = lib.read_as_record_batch_reader("test_symbol")

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
        reader = lib.read_as_record_batch_reader(
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

        reader = lib.read_as_record_batch_reader("test_symbol", columns=["a", "c"])

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

        with lib.duckdb_context() as ddb:
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

        with lib.duckdb_context() as ddb:
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

        with lib.duckdb_context() as ddb:
            ddb.register_symbol("test_symbol", alias="my_table")
            result = ddb.query("SELECT * FROM my_table")

        assert len(result) == 3

    def test_register_same_symbol_twice_with_different_filters(self, lmdb_library):
        """Test registering same symbol with different filters."""
        lib = lmdb_library
        dates = pd.date_range("2024-01-01", periods=100, freq="D")
        df = pd.DataFrame({"value": np.arange(100)}, index=dates)
        lib.write("test_symbol", df)

        with lib.duckdb_context() as ddb:
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

        with lib.duckdb_context() as ddb:
            ddb.register_symbol("test_symbol")
            result = ddb.query("SELECT * FROM test_symbol", output_format="arrow")

        assert isinstance(result, pa.Table)

    def test_method_chaining(self, lmdb_library):
        """Test method chaining with register_symbol."""
        lib = lmdb_library
        lib.write("sym1", pd.DataFrame({"x": [1, 2]}))
        lib.write("sym2", pd.DataFrame({"y": [3, 4]}))

        with lib.duckdb_context() as ddb:
            result = ddb.register_symbol("sym1").register_symbol("sym2").query("SELECT * FROM sym1, sym2")

        # Cross join should give 4 rows
        assert len(result) == 4

    def test_execute_method(self, lmdb_library):
        """Test execute method for DDL statements."""
        lib = lmdb_library
        df = pd.DataFrame({"x": [1, 2, 3]})
        lib.write("test_symbol", df)

        with lib.duckdb_context() as ddb:
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

        with lib.duckdb_context() as ddb:
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

        ddb = lib.duckdb_context()

        with pytest.raises(RuntimeError, match="must be used within"):
            ddb.register_symbol("test")


class TestDuckDBIntegrationWithArrow:
    """Tests verifying the DuckDB integration uses Arrow correctly."""

    def test_duckdb_from_arrow_reader(self, lmdb_library):
        """Test that DuckDB can directly consume our reader."""
        lib = lmdb_library
        df = pd.DataFrame({"x": np.arange(100), "y": np.arange(100, 200)})
        lib.write("test_symbol", df)

        reader = lib.read_as_record_batch_reader("test_symbol")

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

        reader = lib.read_as_record_batch_reader("test_symbol")

        batch_count = 0
        total_rows = 0
        for batch in reader:
            batch_count += 1
            total_rows += len(batch)

        # With standard segment size, all data may fit in one batch
        # The important thing is that streaming works correctly
        assert batch_count >= 1, "Expected at least one batch"
        assert total_rows == 1000
