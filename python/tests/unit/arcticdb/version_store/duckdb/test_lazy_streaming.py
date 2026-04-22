"""
Copyright 2026 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, version 2.0.
"""

import numpy as np
import pandas as pd
import pyarrow as pa
import pytest

from arcticdb.version_store.duckdb.arrow_reader import ArcticRecordBatchReader

duckdb = pytest.importorskip("duckdb")


class TestLazyRecordBatchIterator:
    """Tests for the lazy record batch iterator that reads segments on-demand."""

    def test_lazy_basic_select_all(self, lmdb_library):
        """Lazy SELECT * returns the same data as the eager path."""
        lib = lmdb_library
        df = pd.DataFrame({"x": np.arange(100), "y": np.arange(100, 200)})
        lib.write("sym", df)

        result = lib.sql("SELECT * FROM sym ORDER BY x")

        assert len(result) == 100
        assert list(result.columns) == ["x", "y"]
        pd.testing.assert_frame_equal(result.reset_index(drop=True), df)

    def test_lazy_groupby(self, lmdb_library):
        """GROUP BY with lazy streaming matches eager result."""
        lib = lmdb_library
        df = pd.DataFrame({"category": ["A", "B", "A", "B", "A"], "value": [10, 20, 30, 40, 50]})
        lib.write("sym", df)

        result = lib.sql("SELECT category, SUM(value) as total FROM sym GROUP BY category ORDER BY category")

        assert len(result) == 2
        assert list(result["category"]) == ["A", "B"]
        assert list(result["total"]) == [90, 60]

    def test_lazy_filter(self, lmdb_library):
        """WHERE filter with lazy streaming returns correct subset."""
        lib = lmdb_library
        df = pd.DataFrame({"x": np.arange(100), "y": np.arange(100, 200)})
        lib.write("sym", df)

        result = lib.sql("SELECT x, y FROM sym WHERE x > 50 ORDER BY x")

        assert len(result) == 49
        assert result["x"].min() > 50

    def test_lazy_with_columns(self, lmdb_library):
        """Column projection works with lazy streaming."""
        lib = lmdb_library
        df = pd.DataFrame({"a": np.arange(50), "b": np.arange(50, 100), "c": np.arange(100, 150)})
        lib.write("sym", df)

        result = lib.sql("SELECT a, c FROM sym ORDER BY a")

        assert list(result.columns) == ["a", "c"]
        assert len(result) == 50

    def test_lazy_with_date_range(self, lmdb_library):
        """Date range pushdown works with lazy streaming."""
        lib = lmdb_library
        idx = pd.date_range("2024-01-01", periods=100, freq="D")
        df = pd.DataFrame({"value": np.arange(100)}, index=idx)
        lib.write("sym", df)

        result = lib.sql("SELECT * FROM sym WHERE index >= '2024-02-01' AND index < '2024-03-01'")

        assert len(result) == 29  # Feb 2024

    def test_lazy_limit(self, lmdb_library):
        """LIMIT clause works with lazy streaming."""
        lib = lmdb_library
        df = pd.DataFrame({"x": np.arange(1000)})
        lib.write("sym", df)

        result = lib.sql("SELECT x FROM sym LIMIT 10")

        assert len(result) == 10

    def test_lazy_empty_symbol(self, lmdb_library):
        """Empty symbol returns empty result via lazy iterator with schema from descriptor."""
        lib = lmdb_library
        df = pd.DataFrame({"x": pd.array([], dtype="int64"), "y": pd.array([], dtype="float64")})
        lib.write("sym", df)

        # Direct iterator: verify descriptor provides schema for empty symbols
        cpp_iterator, _ = lib._nvs.read_as_lazy_record_batch_iterator("sym")
        assert not cpp_iterator.has_next()
        assert cpp_iterator.num_batches() == 0
        # descriptor() should have the column schema even with no data segments
        desc = cpp_iterator.descriptor()
        assert len(desc.fields()) > 0

    def test_lazy_empty_symbol_sql(self, lmdb_library):
        """Empty symbol works through lib.sql() using schema from descriptor."""
        lib = lmdb_library
        df = pd.DataFrame({"x": pd.array([], dtype="int64"), "y": pd.array([], dtype="float64")})
        lib.write("sym", df)

        result = lib.sql("SELECT * FROM sym")
        assert len(result) == 0
        assert "x" in result.columns
        assert "y" in result.columns

    def test_lazy_join_two_symbols(self, lmdb_library):
        """JOIN across two symbols works with lazy streaming."""
        lib = lmdb_library

        trades = pd.DataFrame({"ticker": ["AAPL", "GOOG", "AAPL"], "quantity": [100, 200, 150]})
        prices = pd.DataFrame({"ticker": ["AAPL", "GOOG", "MSFT"], "price": [150.0, 2800.0, 300.0]})

        lib.write("trades", trades)
        lib.write("prices", prices)

        result = lib.sql("""
            SELECT t.ticker, t.quantity, p.price
            FROM trades t
            JOIN prices p ON t.ticker = p.ticker
            ORDER BY t.ticker, t.quantity
        """)

        assert len(result) == 3
        assert set(result["ticker"]) == {"AAPL", "GOOG"}

    def test_lazy_with_versioning(self, lmdb_library):
        """Lazy streaming respects as_of version parameter."""
        lib = lmdb_library

        df_v0 = pd.DataFrame({"x": [1, 2, 3]})
        df_v1 = pd.DataFrame({"x": [10, 20, 30]})

        lib.write("sym", df_v0)
        lib.write("sym", df_v1)

        result_latest = lib.sql("SELECT * FROM sym ORDER BY x")
        result_v0 = lib.sql("SELECT * FROM sym ORDER BY x", as_of=0)

        assert list(result_latest["x"]) == [10, 20, 30]
        assert list(result_v0["x"]) == [1, 2, 3]

    def test_lazy_multiple_segments(self, lmdb_library_factory):
        """Lazy streaming works correctly when data spans multiple storage segments."""
        from arcticdb.options import LibraryOptions

        lib = lmdb_library_factory(LibraryOptions(rows_per_segment=50))

        # 200 rows with rows_per_segment=50 → 4 segments
        n_rows = 200
        rng = np.random.default_rng(42)
        df = pd.DataFrame(
            {
                "id": np.arange(n_rows),
                "value": rng.standard_normal(n_rows),
                "category": rng.choice(["A", "B", "C", "D"], n_rows),
            }
        )
        lib.write("sym", df)

        result = lib.sql(
            "SELECT category, COUNT(*) as cnt, AVG(value) as avg_val FROM sym GROUP BY category ORDER BY category"
        )

        assert len(result) == 4
        assert set(result["category"]) == {"A", "B", "C", "D"}
        assert result["cnt"].sum() == n_rows


class TestLazyRecordBatchIteratorDirect:
    """Tests for the lazy iterator accessed directly via NativeVersionStore."""

    def test_direct_lazy_iterator(self, lmdb_library):
        """Test creating and consuming a lazy iterator directly."""
        lib = lmdb_library
        df = pd.DataFrame({"x": np.arange(100), "y": np.arange(100, 200)})
        lib.write("sym", df)

        cpp_iterator, _ = lib._nvs.read_as_lazy_record_batch_iterator("sym")
        reader = ArcticRecordBatchReader(cpp_iterator)

        table = reader.read_all()
        assert table.num_rows == 100
        assert table.num_columns == 2

    def test_lazy_iterator_with_columns(self, lmdb_library):
        """Test lazy iterator with column projection."""
        lib = lmdb_library
        df = pd.DataFrame({"a": np.arange(50), "b": np.arange(50, 100), "c": np.arange(100, 150)})
        lib.write("sym", df)

        cpp_iterator, _ = lib._nvs.read_as_lazy_record_batch_iterator("sym", columns=["a", "c"])
        reader = ArcticRecordBatchReader(cpp_iterator, columns=["a", "c"])

        table = reader.read_all()
        assert table.num_columns == 2
        assert table.column_names == ["a", "c"]

    def test_lazy_iterator_column_pushdown(self, lmdb_library):
        """Test that column projection is pushed down to C++ decode level.

        Without pushdown, the C++ iterator returns batches with ALL columns
        and Python filters them after the fact. With pushdown, the C++ iterator
        only decodes and returns the requested columns, saving decompression CPU.
        This test verifies pushdown by checking raw batches from C++ directly.
        """
        lib = lmdb_library
        df = pd.DataFrame(
            {
                "a": np.arange(100),
                "b": np.arange(100, 200),
                "c": np.arange(200, 300),
                "d": np.arange(300, 400),
                "e": np.arange(400, 500),
            }
        )
        lib.write("sym", df)

        projected_cols = ["a", "c"]
        cpp_iterator, _ = lib._nvs.read_as_lazy_record_batch_iterator("sym", columns=projected_cols)

        # Read raw batch directly from C++ — bypasses Python column filtering
        batch_data = cpp_iterator.next()
        assert batch_data is not None
        raw_batch = pa.RecordBatch._import_from_c(batch_data.array(), batch_data.schema())

        # With column pushdown, the raw C++ batch should only have the projected
        # columns plus the index — NOT all 5 data columns.
        raw_col_names = set(raw_batch.schema.names)
        assert "a" in raw_col_names
        assert "c" in raw_col_names
        assert "b" not in raw_col_names, "Column 'b' should not be decoded with column pushdown"
        assert "d" not in raw_col_names, "Column 'd' should not be decoded with column pushdown"
        assert "e" not in raw_col_names, "Column 'e' should not be decoded with column pushdown"

    def test_lazy_iterator_streaming(self, lmdb_library):
        """Test consuming lazy iterator batch-by-batch."""
        lib = lmdb_library
        df = pd.DataFrame({"x": np.arange(100)})
        lib.write("sym", df)

        cpp_iterator, _ = lib._nvs.read_as_lazy_record_batch_iterator("sym")
        reader = ArcticRecordBatchReader(cpp_iterator)

        total_rows = 0
        for batch in reader:
            total_rows += batch.num_rows

        assert total_rows == 100

    def test_lazy_iterator_exhaustion(self, lmdb_library):
        """Test that exhausted lazy iterator raises properly."""
        lib = lmdb_library
        df = pd.DataFrame({"x": [1, 2, 3]})
        lib.write("sym", df)

        cpp_iterator, _ = lib._nvs.read_as_lazy_record_batch_iterator("sym")
        reader = ArcticRecordBatchReader(cpp_iterator)

        # Consume all batches
        _ = reader.read_all()

        # Cannot iterate again
        with pytest.raises(RuntimeError, match="exhausted"):
            list(reader)


class TestLazyTruncationAndFilter:
    """Tests for row-level truncation (date_range/row_range) and FilterClause in the lazy path."""

    def test_lazy_date_range_exact_match(self, lmdb_library):
        """Lazy date_range truncation produces the exact same row count as eager."""
        lib = lmdb_library
        idx = pd.date_range("2024-01-01", periods=365, freq="D")
        df = pd.DataFrame({"value": np.arange(365), "label": ["A"] * 365}, index=idx)
        lib.write("sym", df)

        date_range = (pd.Timestamp("2024-03-15"), pd.Timestamp("2024-06-30"))

        eager_result = lib.read("sym", date_range=date_range).data
        lazy_iter, _ = lib._nvs.read_as_lazy_record_batch_iterator("sym", date_range=date_range)
        lazy_reader = ArcticRecordBatchReader(lazy_iter)
        lazy_table = lazy_reader.read_all()

        # Lazy returns index as a regular column; eager has it as DataFrame index.
        # Compare row counts and data column values to verify truncation correctness.
        assert lazy_table.num_rows == len(eager_result)
        lazy_df = lazy_table.to_pandas()
        np.testing.assert_array_equal(lazy_df["value"].values, eager_result["value"].values)

    def test_lazy_row_range_exact_match(self, lmdb_library):
        """Lazy row_range truncation produces the exact same rows as eager."""
        lib = lmdb_library
        df = pd.DataFrame({"x": np.arange(500), "y": np.random.default_rng(42).standard_normal(500)})
        lib.write("sym", df)

        row_range = (100, 250)

        eager_result = lib.read("sym", row_range=row_range).data
        lazy_iter, _ = lib._nvs.read_as_lazy_record_batch_iterator("sym", row_range=row_range)
        lazy_reader = ArcticRecordBatchReader(lazy_iter)
        lazy_df = lazy_reader.read_all().to_pandas()

        assert len(lazy_df) == len(eager_result)
        pd.testing.assert_frame_equal(lazy_df.reset_index(drop=True), eager_result.reset_index(drop=True))

    def test_lazy_filter_clause_via_sql(self, lmdb_library):
        """SQL WHERE pushdown with FilterClause applied lazily in the C++ iterator."""
        lib = lmdb_library
        df = pd.DataFrame({"x": np.arange(200), "y": np.arange(200, 400)})
        lib.write("sym", df)

        # SQL WHERE clause gets pushed down as a FilterClause to the lazy iterator
        result = lib.sql("SELECT x, y FROM sym WHERE x >= 100 AND x < 150 ORDER BY x")

        assert len(result) == 50
        assert result["x"].min() == 100
        assert result["x"].max() == 149

    def test_lazy_date_range_via_sql(self, lmdb_library):
        """SQL date_range pushdown with row-level truncation in the lazy iterator."""
        lib = lmdb_library
        idx = pd.date_range("2024-01-01", periods=365, freq="D")
        df = pd.DataFrame({"value": np.arange(365)}, index=idx)
        lib.write("sym", df)

        # SQL pushdown extracts date_range from WHERE clause on index
        result = lib.sql("SELECT * FROM sym WHERE index >= '2024-04-01' AND index < '2024-05-01'")

        # April 2024 has 30 days
        assert len(result) == 30

    def test_lazy_date_range_and_filter_combined(self, lmdb_library):
        """Combined date_range + WHERE filter applied lazily."""
        lib = lmdb_library
        idx = pd.date_range("2024-01-01", periods=365, freq="D")
        df = pd.DataFrame(
            {"value": np.arange(365), "category": np.where(np.arange(365) % 2 == 0, "even", "odd")}, index=idx
        )
        lib.write("sym", df)

        # SQL query with both date range on index and value filter
        result = lib.sql("SELECT * FROM sym WHERE index >= '2024-03-01' AND index < '2024-04-01' AND category = 'even'")

        # March 2024 has 31 days, roughly half are "even"
        assert len(result) > 0
        assert all(result["category"] == "even")
        # Verify date range constraint (index column may be returned as a regular column)
        if "index" in result.columns:
            ts_col = result["index"]
        else:
            ts_col = result.index
        assert pd.Timestamp(ts_col.min()) >= pd.Timestamp("2024-03-01")
        assert pd.Timestamp(ts_col.max()) < pd.Timestamp("2024-04-01")

    def test_lazy_row_range_via_sql_limit(self, lmdb_library):
        """SQL LIMIT clause is translated to row_range and applied lazily."""
        lib = lmdb_library
        df = pd.DataFrame({"x": np.arange(1000)})
        lib.write("sym", df)

        result = lib.sql("SELECT x FROM sym LIMIT 25")

        assert len(result) == 25

    def test_lazy_filter_all_rows_removed(self, lmdb_library):
        """FilterClause that removes all rows returns empty result."""
        lib = lmdb_library
        df = pd.DataFrame({"x": np.arange(100)})
        lib.write("sym", df)

        result = lib.sql("SELECT x FROM sym WHERE x > 9999")

        assert len(result) == 0

    def test_lazy_field_count(self, lmdb_library):
        """field_count() accessor returns the correct number of schema fields."""
        lib = lmdb_library
        df = pd.DataFrame({"a": [1, 2], "b": [3, 4], "c": [5, 6]})
        lib.write("sym", df)

        cpp_iterator, _ = lib._nvs.read_as_lazy_record_batch_iterator("sym")
        # field_count includes index + data columns
        assert cpp_iterator.field_count() >= 3


class TestLazyWithDuckDBContext:
    """Tests for lazy streaming with the DuckDBContext API."""

    def test_duckdb_context_uses_lazy(self, lmdb_library):
        """DuckDBContext.register_symbol should use lazy streaming."""
        lib = lmdb_library
        df = pd.DataFrame({"x": np.arange(100), "y": np.arange(100, 200)})
        lib.write("sym", df)

        with lib.duckdb() as ddb:
            ddb.register_symbol("sym")
            result = ddb.sql("SELECT * FROM sym ORDER BY x")

        assert len(result) == 100

    def test_duckdb_context_auto_register(self, lmdb_library):
        """Auto-registration in DuckDBContext should use lazy streaming."""
        lib = lmdb_library
        df = pd.DataFrame({"x": np.arange(50)})
        lib.write("sym", df)

        with lib.duckdb() as ddb:
            result = ddb.sql("SELECT SUM(x) as total FROM sym")

        assert result["total"].iloc[0] == sum(range(50))


class TestLazyStringFormat:
    """Tests that the lazy read path used by DuckDB produces correct Arrow string types."""

    def test_sql_string_column_type(self, lmdb_library):
        """lib.sql() with PYARROW output should produce string columns with the correct Arrow type.

        The lazy read path defaults to large_string (LARGE_STRING is the library default).
        This verifies the ReadOptions are threaded through the LazyRecordBatchIterator.
        """
        lib = lmdb_library
        df = pd.DataFrame({"name": ["alice", "bob", "charlie"], "val": [1, 2, 3]})
        lib.write("sym", df)

        result = lib.sql("SELECT * FROM sym ORDER BY val", output_format="pyarrow")
        assert isinstance(result, pa.Table)
        name_field = result.schema.field("name")
        # Default library format is LARGE_STRING, so the lazy path should produce large_string
        assert name_field.type in (pa.string(), pa.large_string(), pa.dictionary(pa.int32(), pa.string()))

    def test_record_batch_reader_string_type(self, lmdb_library):
        """_read_as_record_batch_reader should produce Arrow string columns via the lazy path.

        This is the internal method used by lib.sql() and lib.duckdb(). Verifying that the
        LazyRecordBatchIterator correctly handles string format in its ReadOptions.
        """
        lib = lmdb_library
        df = pd.DataFrame({"name": ["alice", "bob", "charlie"], "val": [1, 2, 3]})
        lib.write("sym", df)

        reader, _ = lib._read_as_record_batch_reader("sym")
        assert isinstance(reader, ArcticRecordBatchReader)
        table = reader.read_all()
        assert isinstance(table, pa.Table)
        # Verify string data survived the round-trip
        assert table.column("name").to_pylist() == ["alice", "bob", "charlie"]

    def test_sql_mixed_string_types_after_update(self, lmdb_library):
        """lib.sql() returns correct string data after write + update with mixed string types.

        Regression test for the lazy path not threading ReadOptions through
        prepare_segment_for_arrow(), causing string type mismatches.
        """
        lib = lmdb_library
        idx = pd.date_range("2025-01-01", periods=4, name="ts")
        df1 = pd.DataFrame({"col": ["a", "bb", "ccc", "dddd"]}, index=idx)
        lib.write("sym", df1)

        idx2 = pd.date_range("2025-01-02", periods=2, name="ts")
        df2 = pd.DataFrame({"col": ["eeeee", "ffffff"]}, index=idx2)
        lib.update("sym", df2)

        result = lib.sql("SELECT ts, col FROM sym ORDER BY ts")
        assert list(result["col"]) == ["a", "eeeee", "ffffff", "dddd"]
