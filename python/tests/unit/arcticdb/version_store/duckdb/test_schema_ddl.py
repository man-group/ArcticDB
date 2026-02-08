"""
Copyright 2026 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, version 2.0.
"""

"""
Tests for schema introspection via DuckDB DDL queries: DESCRIBE, SHOW COLUMNS,
SHOW TABLES, SHOW ALL TABLES, and register_all_symbols discovery.
"""

import numpy as np
import pandas as pd
import pytest

# Skip all tests if duckdb is not installed
duckdb = pytest.importorskip("duckdb")


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
            result = ddb.sql("DESCRIBE test_symbol")

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
            result = ddb.sql("DESCRIBE test_symbol")

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
            result = ddb.sql("DESCRIBE test_symbol")

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
            result = ddb.sql("DESCRIBE test_symbol")

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
            describe_result = ddb.sql("DESCRIBE test_symbol")
            # SHOW is an alias for DESCRIBE in DuckDB
            show_result = ddb.sql("SHOW test_symbol")

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

            result1 = ddb.sql("DESCRIBE symbol1")
            result2 = ddb.sql("DESCRIBE symbol2")

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
            result = ddb.sql("DESCRIBE aliased_name")

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
            result = ddb.sql("SHOW TABLES")

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

            result = ddb.sql("SHOW ALL TABLES")

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
            result = ddb.sql("SHOW TABLES")

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
            result = ddb.sql("SHOW TABLES")

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
            result = ddb.sql("SHOW ALL TABLES")

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
