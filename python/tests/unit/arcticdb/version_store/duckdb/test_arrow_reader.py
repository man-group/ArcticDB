"""
Copyright 2026 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, version 2.0.
"""

"""
Unit tests for duckdb/arrow_reader.py - ArcticRecordBatchReader class.

Tests verify the streaming Arrow RecordBatch interface for DuckDB integration.
"""

import numpy as np
import pandas as pd
import pytest

# Skip all tests if duckdb is not installed
duckdb = pytest.importorskip("duckdb")


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
