"""
Copyright 2026 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, version 2.0.
"""

"""
Tests for Arctic-level DuckDB integration: ArcticDuckDBContext, SHOW DATABASES,
database.library namespace hierarchy, and cross-library joins.
"""

import pandas as pd
import pytest

from arcticdb.options import OutputFormat
from arcticdb.version_store.duckdb.duckdb import _parse_library_name

# Skip all tests if duckdb is not installed
duckdb = pytest.importorskip("duckdb")


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
        assert "library_name" in result.columns
        assert len(result) == 1
        assert result["database_name"].iloc[0] == "testuser"
        assert result["library_name"].iloc[0] == "market_data"

    def test_arctic_sql_show_databases_multiple_libraries(self, lmdb_storage):
        """Test arctic.sql('SHOW DATABASES') with multiple libraries in different databases."""
        arctic = lmdb_storage.create_arctic()
        arctic.create_library("testuser.market_data")
        arctic.create_library("testuser.reference_data")
        arctic.create_library("otheruser.portfolios")

        result = arctic.sql("SHOW DATABASES")

        assert "database_name" in result.columns
        assert "library_name" in result.columns
        assert len(result) == 3  # Three libraries total
        testuser_libs = sorted(result[result["database_name"] == "testuser"]["library_name"].tolist())
        assert testuser_libs == ["market_data", "reference_data"]
        otheruser_libs = result[result["database_name"] == "otheruser"]["library_name"].tolist()
        assert otheruser_libs == ["portfolios"]

    def test_arctic_sql_show_databases_output_format_arrow(self, lmdb_storage):
        """Test arctic.sql('SHOW DATABASES') with Arrow output format."""
        import pyarrow as pa

        arctic = lmdb_storage.create_arctic()
        arctic.create_library("testuser.test_lib")

        result = arctic.sql("SHOW DATABASES", output_format="pyarrow")

        assert isinstance(result, pa.Table)
        assert "database_name" in result.column_names
        assert "library_name" in result.column_names
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
            result = ddb.sql("SHOW DATABASES")

        assert "database_name" in result.columns
        assert "library_name" in result.columns
        testuser_libs = sorted(result[result["database_name"] == "testuser"]["library_name"].tolist())
        assert testuser_libs == ["lib_a", "lib_b"]

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
            result = ddb.sql("SHOW DATABASES")

        assert len(result) == 2  # Only lib_a and lib_c registered, not lib_b
        user1_libs = result[result["database_name"] == "user1"]["library_name"].tolist()
        assert user1_libs == ["lib_a"]
        user2_libs = result[result["database_name"] == "user2"]["library_name"].tolist()
        assert user2_libs == ["lib_c"]

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
            result = ddb.sql("""
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
            result = ddb.sql("SELECT * FROM aliased")

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
            result = ddb.sql("SHOW DATABASES")

        # Library's database should be in SHOW DATABASES even without explicit registration
        assert "testuser" in list(result["database_name"])
        assert "implicit_lib" in list(result["library_name"])

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
            ddb.sql("SHOW DATABASES")

            # But a data query should fail
            with pytest.raises(RuntimeError, match="No symbols have been registered"):
                ddb.sql("SELECT * FROM some_table")

    def test_arctic_duckdb_context_arrow_output_format(self, lmdb_storage):
        """Test arctic.duckdb() with arrow output format."""
        import pyarrow as pa

        arctic = lmdb_storage.create_arctic()
        lib = arctic.create_library("testuser.lib")
        lib.write("data", pd.DataFrame({"x": [1, 2, 3]}))

        with arctic.duckdb() as ddb:
            ddb.register_symbol("testuser.lib", "data")
            result = ddb.sql("SELECT * FROM data", output_format=OutputFormat.PYARROW)

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
            result = ddb.sql("""
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
                .sql("SELECT SUM(x) as total FROM data")
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
        """Test SHOW DATABASES returns database_name and library_name columns."""
        arctic = lmdb_storage.create_arctic()
        # Create libraries with database.library format
        arctic.create_library("jblackburn.lib1")
        arctic.create_library("jblackburn.lib2")
        arctic.create_library("other_user.lib1")

        result = arctic.sql("SHOW DATABASES")

        assert "database_name" in result.columns
        assert "library_name" in result.columns
        assert len(result) == 3

        jb_libs = sorted(result[result["database_name"] == "jblackburn"]["library_name"].tolist())
        assert jb_libs == ["lib1", "lib2"]
        other_libs = result[result["database_name"] == "other_user"]["library_name"].tolist()
        assert other_libs == ["lib1"]

    def test_show_databases_default_namespace(self, lmdb_storage):
        """Test top-level libraries grouped under __default__."""
        arctic = lmdb_storage.create_arctic()
        # Mix of namespaced and top-level libraries
        arctic.create_library("jblackburn.lib1")
        arctic.create_library("global_config")
        arctic.create_library("shared_data")

        result = arctic.sql("SHOW DATABASES")

        assert len(result) == 3
        jb_libs = result[result["database_name"] == "jblackburn"]["library_name"].tolist()
        assert jb_libs == ["lib1"]
        default_libs = sorted(result[result["database_name"] == "__default__"]["library_name"].tolist())
        assert default_libs == ["global_config", "shared_data"]

    def test_show_databases_empty(self, lmdb_storage):
        """Test SHOW DATABASES with no libraries returns empty result."""
        arctic = lmdb_storage.create_arctic()

        result = arctic.sql("SHOW DATABASES")

        assert "database_name" in result.columns
        assert "library_name" in result.columns
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
            result = ddb.sql("SHOW DATABASES")

        assert "database_name" in result.columns
        assert "library_name" in result.columns
        assert len(result) == 3

        jb_libs = sorted(result[result["database_name"] == "jblackburn"]["library_name"].tolist())
        assert jb_libs == ["market_data", "reference_data"]
        shared_libs = result[result["database_name"] == "shared"]["library_name"].tolist()
        assert shared_libs == ["global_config"]

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
            result = ddb.sql("""
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


class TestCrossLibraryJoins:
    """Tests for joining data across multiple ArcticDB library instances via nested context managers."""

    def test_join_across_libraries_nested(self, lmdb_storage):
        """Nested Library.duckdb() context managers for cross-library JOIN."""
        arctic = lmdb_storage.create_arctic()
        lib_a = arctic.create_library("team_a.positions")
        lib_b = arctic.create_library("team_b.prices")

        lib_a.write("portfolio", pd.DataFrame({"ticker": ["AAPL", "GOOG"], "shares": [1000, 500]}))
        lib_b.write("marks", pd.DataFrame({"ticker": ["AAPL", "GOOG"], "mark": [195.0, 175.0]}))

        with lib_a.duckdb() as ddb_a:
            ddb_a.register_symbol("portfolio")

            with lib_b.duckdb(connection=ddb_a.connection) as ddb_b:
                ddb_b.register_symbol("marks")
                result = ddb_b.sql("""
                    SELECT p.ticker, p.shares, m.mark, p.shares * m.mark AS market_value
                    FROM portfolio p
                    JOIN marks m ON p.ticker = m.ticker
                    ORDER BY market_value DESC
                """)

        assert len(result) == 2
        assert result.iloc[0]["ticker"] == "AAPL"
        assert result.iloc[0]["market_value"] == pytest.approx(195000.0)
        assert result.iloc[1]["ticker"] == "GOOG"
        assert result.iloc[1]["market_value"] == pytest.approx(87500.0)

    def test_join_across_separate_lmdb_instances_nested(self, tmp_path):
        """Nested context managers from two separate LMDB Arctic instances."""
        from arcticdb import Arctic

        arctic_a = Arctic(f"lmdb://{tmp_path}/db_alpha")
        arctic_b = Arctic(f"lmdb://{tmp_path}/db_beta")

        lib_a = arctic_a.create_library("data")
        lib_b = arctic_b.create_library("data")

        lib_a.write("trades", pd.DataFrame({"ticker": ["AAPL", "GOOG"], "notional": [15000.0, 140000.0]}))
        lib_b.write("fx_rates", pd.DataFrame({"ticker": ["AAPL", "GOOG"], "fx_rate": [0.79, 0.79]}))

        with lib_a.duckdb() as ddb_a:
            ddb_a.register_symbol("trades")

            with lib_b.duckdb(connection=ddb_a.connection) as ddb_b:
                ddb_b.register_symbol("fx_rates")
                result = ddb_b.sql("""
                    SELECT t.ticker, t.notional, f.fx_rate,
                           ROUND(t.notional * f.fx_rate, 2) AS notional_gbp
                    FROM trades t
                    JOIN fx_rates f ON t.ticker = f.ticker
                    ORDER BY t.ticker
                """)

        assert len(result) == 2
        assert result.iloc[0]["notional_gbp"] == pytest.approx(11850.0)
        assert result.iloc[1]["notional_gbp"] == pytest.approx(110600.0)

    def test_cleanup_on_exit(self, lmdb_storage):
        """Verify that registered symbols are unregistered when the context exits."""
        arctic = lmdb_storage.create_arctic()
        lib = arctic.create_library("test_lib")
        lib.write("data", pd.DataFrame({"x": [1, 2, 3]}))

        import duckdb

        conn = duckdb.connect(":memory:")

        with lib.duckdb(connection=conn) as ddb:
            ddb.register_symbol("data")
            # Table is visible inside context
            assert conn.execute("SELECT COUNT(*) FROM data").fetchone()[0] == 3

        # After exit, the table should be gone
        tables = [row[0] for row in conn.execute("SHOW TABLES").fetchall()]
        assert "data" not in tables
        conn.close()

    def test_join_across_libraries_via_arctic_duckdb(self, lmdb_storage):
        """Arctic.duckdb() context manager registers symbols from different libraries."""
        arctic = lmdb_storage.create_arctic()
        lib_a = arctic.create_library("fund.nav")
        lib_b = arctic.create_library("fund.benchmarks")

        lib_a.write("daily_nav", pd.DataFrame({"date": ["2025-01-01", "2025-01-02"], "nav": [100.0, 102.5]}))
        lib_b.write("index_level", pd.DataFrame({"date": ["2025-01-01", "2025-01-02"], "level": [5000.0, 5050.0]}))

        with arctic.duckdb() as ddb:
            ddb.register_symbol("fund.nav", "daily_nav")
            ddb.register_symbol("fund.benchmarks", "index_level")
            result = ddb.sql("""
                SELECT n.date, n.nav, i.level,
                       ROUND(n.nav / i.level * 100, 4) AS nav_pct_of_index
                FROM daily_nav n
                JOIN index_level i ON n.date = i.date
                ORDER BY n.date
            """)

        assert len(result) == 2
        assert result.iloc[0]["nav_pct_of_index"] == pytest.approx(2.0, abs=0.01)
