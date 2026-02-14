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


class TestRecordBatchReaderEdgeCases:
    """Tests for edge cases and error handling in ArcticRecordBatchReader."""

    def test_is_exhausted_property(self, lmdb_library):
        """Test is_exhausted property tracks reader state."""
        lib = lmdb_library
        df = pd.DataFrame({"x": [1, 2, 3]})
        lib.write("test_symbol", df)

        reader = lib._read_as_record_batch_reader("test_symbol")

        assert not reader.is_exhausted

        # Exhaust the reader
        list(reader)

        assert reader.is_exhausted

    def test_num_batches_property(self, lmdb_library):
        """Test num_batches property returns batch count."""
        lib = lmdb_library
        df = pd.DataFrame({"x": np.arange(100)})
        lib.write("test_symbol", df)

        reader = lib._read_as_record_batch_reader("test_symbol")

        assert reader.num_batches >= 1

    def test_current_index_property(self, lmdb_library):
        """Test current_index tracks iteration progress."""
        lib = lmdb_library
        df = pd.DataFrame({"x": np.arange(100)})
        lib.write("test_symbol", df)

        reader = lib._read_as_record_batch_reader("test_symbol")

        initial_index = reader.current_index

        # Read one batch
        next(reader)

        assert reader.current_index > initial_index

    def test_len_returns_batch_count(self, lmdb_library):
        """Test __len__ returns the number of batches."""
        lib = lmdb_library
        df = pd.DataFrame({"x": np.arange(100)})
        lib.write("test_symbol", df)

        reader = lib._read_as_record_batch_reader("test_symbol")

        assert len(reader) == reader.num_batches

    def test_iterate_exhausted_reader_raises(self, lmdb_library):
        """Test that iterating over exhausted reader raises helpful error."""
        lib = lmdb_library
        df = pd.DataFrame({"x": [1, 2, 3]})
        lib.write("test_symbol", df)

        reader = lib._read_as_record_batch_reader("test_symbol")

        # Exhaust the reader
        list(reader)

        with pytest.raises(RuntimeError, match="Cannot iterate over exhausted reader"):
            list(reader)

    def test_read_all_after_iteration_raises(self, lmdb_library):
        """Test that read_all() after partial iteration raises error."""
        lib = lmdb_library
        df = pd.DataFrame({"x": [1, 2, 3]})
        lib.write("test_symbol", df)

        reader = lib._read_as_record_batch_reader("test_symbol")

        # Start iteration
        next(reader)

        with pytest.raises(RuntimeError, match="Cannot call read_all"):
            reader.read_all()

    def test_read_next_batch_returns_none_when_exhausted(self, lmdb_library):
        """Test that read_next_batch returns None when exhausted."""
        lib = lmdb_library
        df = pd.DataFrame({"x": [1, 2, 3]})
        lib.write("test_symbol", df)

        reader = lib._read_as_record_batch_reader("test_symbol")

        # Exhaust the reader
        while reader.read_next_batch() is not None:
            pass

        # Should return None consistently
        assert reader.read_next_batch() is None
        assert reader.read_next_batch() is None

    def test_with_row_range(self, lmdb_library):
        """Test record batch reader with row_range filter."""
        lib = lmdb_library
        df = pd.DataFrame({"x": np.arange(100)})
        lib.write("test_symbol", df)

        # Read only rows 10-30 (exclusive end)
        reader = lib._read_as_record_batch_reader("test_symbol", row_range=(10, 30))

        table = reader.read_all()
        assert len(table) == 20

    def test_with_as_of_version(self, lmdb_library):
        """Test record batch reader with as_of parameter."""
        lib = lmdb_library
        df1 = pd.DataFrame({"x": [1, 2, 3]})
        df2 = pd.DataFrame({"x": [10, 20, 30]})

        lib.write("test_symbol", df1)  # version 0
        lib.write("test_symbol", df2)  # version 1

        # Read version 0
        reader = lib._read_as_record_batch_reader("test_symbol", as_of=0)
        table = reader.read_all()

        # Should get first version data
        assert table.column("x").to_pylist() == [1, 2, 3]


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
        result = duckdb.from_arrow(pa_reader).filter("x > 50").fetch_arrow_table()

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
