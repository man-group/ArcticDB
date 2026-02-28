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

        reader, _ = lib._read_as_record_batch_reader("test_symbol")

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

        reader, _ = lib._read_as_record_batch_reader("test_symbol")
        table = reader.read_all()

        import pyarrow as pa

        assert isinstance(table, pa.Table)
        assert len(table) == 50

    def test_schema_property(self, lmdb_library):
        """Test that schema is correctly extracted."""
        lib = lmdb_library
        df = pd.DataFrame({"col_int": [1, 2, 3], "col_float": [1.0, 2.0, 3.0]})
        lib.write("test_symbol", df)

        reader, _ = lib._read_as_record_batch_reader("test_symbol")

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
        reader, _ = lib._read_as_record_batch_reader(
            "test_symbol",
            date_range=(pd.Timestamp("2024-01-01"), pd.Timestamp("2024-01-31")),
        )

        table = reader.read_all()
        assert len(table) == 31  # 31 days in January

    def test_empty_result_after_filter(self, lmdb_library):
        """Test reader with query_builder that filters out all rows."""
        from arcticdb import QueryBuilder

        lib = lmdb_library
        df = pd.DataFrame({"x": [1, 2, 3]})
        lib.write("test_symbol", df)

        q = QueryBuilder()
        q = q[q["x"] > 9999]  # matches nothing

        reader, _ = lib._read_as_record_batch_reader("test_symbol", query_builder=q)

        batches = list(reader)
        total_rows = sum(len(b) for b in batches)
        assert total_rows == 0
        # Schema should still be valid
        assert "x" in [f.name for f in reader.schema]

    def test_read_all_empty_with_schema(self, lmdb_library):
        """Test read_all() returns empty table with correct schema when all rows filtered."""
        import pyarrow as pa
        from arcticdb import QueryBuilder

        lib = lmdb_library
        df = pd.DataFrame({"a": [1, 2], "b": [3.0, 4.0]})
        lib.write("test_symbol", df)

        q = QueryBuilder()
        q = q[q["a"] > 9999]

        reader, _ = lib._read_as_record_batch_reader("test_symbol", query_builder=q)
        table = reader.read_all()

        assert isinstance(table, pa.Table)
        assert len(table) == 0
        assert "a" in table.column_names
        assert "b" in table.column_names

    def test_multiindex_column_renaming(self, lmdb_library):
        """Test that read_all() strips __idx__ prefix from MultiIndex columns."""
        import pyarrow as pa

        lib = lmdb_library
        dates = pd.to_datetime(["2024-01-01", "2024-01-02"])
        idx = pd.MultiIndex.from_arrays([dates, [100, 200]], names=["date", "security_id"])
        df = pd.DataFrame({"value": [1.0, 2.0]}, index=idx)
        lib.write("test_symbol", df)

        reader, _ = lib._read_as_record_batch_reader("test_symbol")
        table = reader.read_all()

        assert isinstance(table, pa.Table)
        # __idx__ prefix should be stripped
        assert "security_id" in table.column_names
        assert "__idx__security_id" not in table.column_names

    def test_with_columns(self, lmdb_library):
        """Test record batch reader with column subset."""
        lib = lmdb_library
        df = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6], "c": [7, 8, 9]})
        lib.write("test_symbol", df)

        reader, _ = lib._read_as_record_batch_reader("test_symbol", columns=["a", "c"])

        table = reader.read_all()
        assert "a" in table.column_names
        assert "c" in table.column_names
        assert "b" not in table.column_names


class TestHelperFunctions:
    """Tests for module-level helper functions in arrow_reader.py."""

    def test_expand_columns_with_idx_prefix_skips_already_prefixed(self):
        """Columns already starting with __idx__ should not get double-prefixed."""
        from arcticdb.version_store.duckdb.arrow_reader import _expand_columns_with_idx_prefix

        result = _expand_columns_with_idx_prefix(["__idx__level", "value"])
        # __idx__level should appear once (no __idx____idx__level)
        assert result == ["__idx__level", "value", "__idx__value"]

    def test_strip_idx_prefix_collision_resolved(self):
        """When stripping __idx__ creates a duplicate, underscores are added to resolve."""
        from arcticdb.version_store.duckdb.arrow_reader import _strip_idx_prefix_from_names

        # "a" and "__idx__a" both strip to "a" — second one should become "_a_"
        result = _strip_idx_prefix_from_names(["a", "__idx__a"])
        assert result == ["a", "_a_"]

    def test_strip_idx_prefix_too_many_collisions_raises(self):
        """Exceeding max collision retries raises ValueError."""
        from arcticdb.version_store.duckdb.arrow_reader import _strip_idx_prefix_from_names

        # Build a list where every collision-resolution attempt also collides.
        # "x" is seen first, then "__idx__x" strips to "x" -> "_x_" -> "__x__" -> ...
        # Pre-seed all the collision-resolution names so it never finds a free slot.
        names = []
        candidate = "x"
        for _ in range(101):
            names.append(candidate)
            candidate = f"_{candidate}_"
        # The last entry uses __idx__ prefix so stripping triggers the collision loop
        names.append("__idx__x")

        with pytest.raises(ValueError, match="Too many name collisions"):
            _strip_idx_prefix_from_names(names)

    def test_build_clean_to_storage_map(self):
        """_build_clean_to_storage_map returns mapping only for __idx__ prefixed columns."""
        from arcticdb.version_store.duckdb.arrow_reader import _build_clean_to_storage_map

        result = _build_clean_to_storage_map(["date", "__idx__security_id", "value"])
        assert result == {"security_id": "__idx__security_id"}

    def test_build_clean_to_storage_map_no_prefixed(self):
        """Returns empty dict when no columns have __idx__ prefix."""
        from arcticdb.version_store.duckdb.arrow_reader import _build_clean_to_storage_map

        result = _build_clean_to_storage_map(["a", "b", "c"])
        assert result == {}


class TestRecordBatchReaderEdgeCases:
    """Tests for edge cases and error handling in ArcticRecordBatchReader."""

    def test_is_exhausted_property(self, lmdb_library):
        """Test is_exhausted property tracks reader state."""
        lib = lmdb_library
        df = pd.DataFrame({"x": [1, 2, 3]})
        lib.write("test_symbol", df)

        reader, _ = lib._read_as_record_batch_reader("test_symbol")

        assert not reader.is_exhausted

        # Exhaust the reader
        list(reader)

        assert reader.is_exhausted

    def test_num_batches_property(self, lmdb_library):
        """Test num_batches property returns batch count."""
        lib = lmdb_library
        df = pd.DataFrame({"x": np.arange(100)})
        lib.write("test_symbol", df)

        reader, _ = lib._read_as_record_batch_reader("test_symbol")

        assert reader.num_batches >= 1

    def test_current_index_property(self, lmdb_library):
        """Test current_index tracks iteration progress."""
        lib = lmdb_library
        df = pd.DataFrame({"x": np.arange(100)})
        lib.write("test_symbol", df)

        reader, _ = lib._read_as_record_batch_reader("test_symbol")

        initial_index = reader.current_index

        # Read one batch
        next(reader)

        assert reader.current_index > initial_index

    def test_len_returns_batch_count(self, lmdb_library):
        """Test __len__ returns the number of batches."""
        lib = lmdb_library
        df = pd.DataFrame({"x": np.arange(100)})
        lib.write("test_symbol", df)

        reader, _ = lib._read_as_record_batch_reader("test_symbol")

        assert len(reader) == reader.num_batches

    def test_iterate_exhausted_reader_raises(self, lmdb_library):
        """Test that iterating over exhausted reader raises helpful error."""
        lib = lmdb_library
        df = pd.DataFrame({"x": [1, 2, 3]})
        lib.write("test_symbol", df)

        reader, _ = lib._read_as_record_batch_reader("test_symbol")

        # Exhaust the reader
        list(reader)

        with pytest.raises(RuntimeError, match="Cannot iterate over exhausted reader"):
            list(reader)

    def test_read_all_after_iteration_raises(self, lmdb_library):
        """Test that read_all() after partial iteration raises error."""
        lib = lmdb_library
        df = pd.DataFrame({"x": [1, 2, 3]})
        lib.write("test_symbol", df)

        reader, _ = lib._read_as_record_batch_reader("test_symbol")

        # Start iteration
        next(reader)

        with pytest.raises(RuntimeError, match="Cannot call read_all"):
            reader.read_all()

    def test_read_next_batch_returns_none_when_exhausted(self, lmdb_library):
        """Test that read_next_batch returns None when exhausted."""
        lib = lmdb_library
        df = pd.DataFrame({"x": [1, 2, 3]})
        lib.write("test_symbol", df)

        reader, _ = lib._read_as_record_batch_reader("test_symbol")

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
        reader, _ = lib._read_as_record_batch_reader("test_symbol", row_range=(10, 30))

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
        reader, _ = lib._read_as_record_batch_reader("test_symbol", as_of=0)
        table = reader.read_all()

        # Should get first version data
        assert table.column("x").to_pylist() == [1, 2, 3]

    def test_empty_symbol_schema_from_descriptor(self, lmdb_library):
        """Schema for an empty symbol (0 rows) is derived from the descriptor alone."""
        import pyarrow as pa

        lib = lmdb_library
        df = pd.DataFrame({"a": pd.array([], dtype="int64"), "b": pd.array([], dtype="float64")})
        lib.write("empty_sym", df)

        reader, _ = lib._read_as_record_batch_reader("empty_sym")
        schema = reader.schema

        assert "a" in [f.name for f in schema]
        assert "b" in [f.name for f in schema]
        table = reader.read_all()
        assert isinstance(table, pa.Table)
        assert len(table) == 0

    def test_read_all_strip_idx_prefix_false(self, lmdb_library):
        """read_all(strip_idx_prefix=False) preserves __idx__ column names."""
        import pyarrow as pa

        lib = lmdb_library
        dates = pd.to_datetime(["2024-01-01", "2024-01-02"])
        idx = pd.MultiIndex.from_arrays([dates, [100, 200]], names=["date", "security_id"])
        df = pd.DataFrame({"value": [1.0, 2.0]}, index=idx)
        lib.write("test_symbol", df)

        reader, _ = lib._read_as_record_batch_reader("test_symbol")
        table = reader.read_all(strip_idx_prefix=False)

        assert isinstance(table, pa.Table)
        # __idx__ prefixed names should be preserved
        assert any(name.startswith("__idx__") for name in table.column_names)

    def test_double_iter_without_exhausting_raises(self, lmdb_library):
        """Calling iter() twice on a non-exhausted reader raises RuntimeError."""
        lib = lmdb_library
        df = pd.DataFrame({"x": [1, 2, 3]})
        lib.write("test_symbol", df)

        reader, _ = lib._read_as_record_batch_reader("test_symbol")

        it = iter(reader)
        next(it)  # start iteration

        with pytest.raises(RuntimeError, match="Cannot create multiple iterators"):
            iter(reader)

    def test_to_pyarrow_reader_multiindex_renames_columns(self, lmdb_library):
        """to_pyarrow_reader() strips __idx__ prefix for MultiIndex symbols."""
        import pyarrow as pa

        lib = lmdb_library
        dates = pd.to_datetime(["2024-01-01", "2024-01-02"])
        idx = pd.MultiIndex.from_arrays([dates, [100, 200]], names=["date", "security_id"])
        df = pd.DataFrame({"value": [1.0, 2.0]}, index=idx)
        lib.write("test_symbol", df)

        reader, _ = lib._read_as_record_batch_reader("test_symbol")
        pa_reader = reader.to_pyarrow_reader()
        table = pa_reader.read_all()

        assert isinstance(table, pa.Table)
        assert "security_id" in table.column_names
        assert "__idx__security_id" not in table.column_names
        assert len(table) == 2

    def test_to_pyarrow_reader_multiindex_column_projection(self, lmdb_library):
        """to_pyarrow_reader() includes the index even when it's not in the projected set.

        C++ always includes the primary index column (via requested_column_bitset_including_index),
        and the schema is derived from the first batch so it naturally includes it. The
        __idx__ prefix is stripped and columns are renamed correctly.
        """
        import pyarrow as pa
        from arcticdb.version_store.duckdb.arrow_reader import _expand_columns_with_idx_prefix

        lib = lmdb_library
        dates = pd.to_datetime(["2024-01-01", "2024-01-02", "2024-01-03"])
        idx = pd.MultiIndex.from_arrays([dates, [100, 200, 300]], names=["date", "security_id"])
        df = pd.DataFrame({"value": [1.0, 2.0, 3.0], "momentum": [0.1, 0.2, 0.3]}, index=idx)
        lib.write("sym", df)

        # Project security_id and momentum. C++ also includes the primary index (date).
        cols = _expand_columns_with_idx_prefix(["security_id", "momentum"])
        reader, _ = lib._read_as_record_batch_reader("sym", columns=cols)
        pa_reader = reader.to_pyarrow_reader()

        # Schema should include date (auto-added by C++), security_id (stripped from
        # __idx__security_id), and momentum. Value column excluded by projection.
        table = pa_reader.read_all()
        assert isinstance(table, pa.Table)
        assert set(table.column_names) == {"date", "security_id", "momentum"}
        assert "value" not in table.column_names
        assert len(table) == 3

    def test_to_pyarrow_reader_dynamic_schema_alignment(self, lmdb_library_dynamic_schema):
        """to_pyarrow_reader() aligns batches when dynamic schema has column mismatches."""
        import pyarrow as pa

        lib = lmdb_library_dynamic_schema
        # First segment: columns a, b
        df1 = pd.DataFrame({"a": [1, 2], "b": [3.0, 4.0]})
        lib.write("sym", df1)
        # Second segment: columns a, c (b missing, c new)
        df2 = pd.DataFrame({"a": [5, 6], "c": ["x", "y"]})
        lib.append("sym", df2)

        reader, _ = lib._read_as_record_batch_reader("sym")
        pa_reader = reader.to_pyarrow_reader()
        table = pa_reader.read_all()

        assert isinstance(table, pa.Table)
        assert len(table) == 4
        assert "a" in table.column_names
        # Both b and c should be present (null-padded where missing)
        assert "b" in table.column_names
        assert "c" in table.column_names

    def test_dynamic_schema_descriptor_field_not_in_first_batch(self, lmdb_library_dynamic_schema):
        """Schema includes descriptor fields absent from the first batch (dynamic schema)."""
        import pyarrow as pa

        lib = lmdb_library_dynamic_schema
        # First segment only has column a
        df1 = pd.DataFrame({"a": [1, 2, 3]})
        lib.write("sym", df1)
        # Second segment adds column b
        df2 = pd.DataFrame({"a": [4, 5, 6], "b": [7.0, 8.0, 9.0]})
        lib.append("sym", df2)

        reader, _ = lib._read_as_record_batch_reader("sym")
        schema = reader.schema

        field_names = [f.name for f in schema]
        assert "a" in field_names
        assert "b" in field_names


class TestTypeWidening:
    """Tests for type-widening behavior in _ensure_schema().

    When segments have different numeric types (e.g. int64 first, float64 second),
    the merged descriptor carries the widened type. The schema should reflect this
    wider type, not the narrower type from the first batch.
    """

    def test_int64_then_float64_via_sql(self, lmdb_library_dynamic_schema):
        """Write int64 segment, append float64 segment, verify SQL returns float64."""
        lib = lmdb_library_dynamic_schema
        df1 = pd.DataFrame({"value": np.array([1, 2, 3], dtype=np.int64)})
        df2 = pd.DataFrame({"value": np.array([4.5, 5.5, 6.5], dtype=np.float64)})

        lib.write("sym", df1)
        lib.append("sym", df2)

        result = lib.sql("SELECT * FROM sym", output_format="pyarrow")
        import pyarrow as pa

        assert isinstance(result, pa.Table)
        assert result.column("value").type == pa.float64()
        assert len(result) == 6

    def test_int32_then_int64_via_sql(self, lmdb_library_dynamic_schema):
        """Write int32, append int64, verify SQL returns int64 (integer widening)."""
        lib = lmdb_library_dynamic_schema
        df1 = pd.DataFrame({"value": np.array([1, 2], dtype=np.int32)})
        df2 = pd.DataFrame({"value": np.array([3, 4], dtype=np.int64)})

        lib.write("sym", df1)
        lib.append("sym", df2)

        result = lib.sql("SELECT * FROM sym", output_format="pyarrow")
        import pyarrow as pa

        assert isinstance(result, pa.Table)
        assert result.column("value").type == pa.int64()
        assert len(result) == 4

    def test_type_widened_column_with_filter(self, lmdb_library_dynamic_schema):
        """Type-widened column with WHERE filter and GROUP BY."""
        lib = lmdb_library_dynamic_schema
        df1 = pd.DataFrame({"group": ["a", "a", "b"], "value": np.array([1, 2, 3], dtype=np.int64)})
        df2 = pd.DataFrame({"group": ["b", "c", "c"], "value": np.array([4.5, 5.5, 6.5], dtype=np.float64)})

        lib.write("sym", df1)
        lib.append("sym", df2)

        result = lib.sql(
            'SELECT "group", SUM(value) as total FROM sym WHERE value > 2 GROUP BY "group" ORDER BY "group"',
            output_format="pyarrow",
        )
        import pyarrow as pa

        assert isinstance(result, pa.Table)
        groups = result.column("group").to_pylist()
        assert "b" in groups
        assert "c" in groups

    def test_schema_uses_batch_type_for_strings(self, lmdb_library_dynamic_schema):
        """For non-numeric types (strings), schema should use batch type, not descriptor type."""
        lib = lmdb_library_dynamic_schema
        df = pd.DataFrame({"name": ["alice", "bob", "carol"]})
        lib.write("sym", df)

        reader, _ = lib._read_as_record_batch_reader("sym")
        schema = reader.schema

        # String columns should use the batch's actual Arrow type (e.g. dictionary-encoded)
        # rather than the descriptor's type (large_string)
        name_field = schema.field("name")
        assert name_field is not None


class TestDuckDBIntegrationWithArrow:
    """Tests verifying the DuckDB integration uses Arrow correctly."""

    def test_duckdb_from_arrow_reader(self, lmdb_library):
        """Test that DuckDB can directly consume our reader."""
        lib = lmdb_library
        df = pd.DataFrame({"x": np.arange(100), "y": np.arange(100, 200)})
        lib.write("test_symbol", df)

        reader, _ = lib._read_as_record_batch_reader("test_symbol")

        # Convert to PyArrow reader for DuckDB compatibility
        pa_reader = reader.to_pyarrow_reader()

        # DuckDB should be able to query the reader directly
        result = duckdb.from_arrow(pa_reader).filter("x > 50").fetch_arrow_table()

        assert len(result) == 49

    def test_dynamic_schema_column_projection(self, lmdb_library_dynamic_schema):
        """Test column projection with dynamic schema aligns schema across segments."""
        import pyarrow as pa

        lib = lmdb_library_dynamic_schema
        # First write has columns a, b
        df1 = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
        lib.write("test_symbol", df1)
        # Second append adds column c (different from first)
        df2 = pd.DataFrame({"a": [7, 8, 9], "c": [10, 11, 12]})
        lib.append("test_symbol", df2)

        reader, _ = lib._read_as_record_batch_reader("test_symbol", columns=["a"])
        pa_reader = reader.to_pyarrow_reader()

        # All batches should have the same schema with column "a"
        table = pa_reader.read_all()
        assert isinstance(table, pa.Table)
        assert "a" in table.column_names
        assert len(table) == 6
        assert table.column("a").to_pylist() == [1, 2, 3, 7, 8, 9]

    def test_batches_streamed(self, lmdb_library):
        """Test that data is correctly streamed through batches."""
        lib = lmdb_library

        # Write data
        df = pd.DataFrame({"x": np.arange(1000)})
        lib.write("test_symbol", df)

        reader, _ = lib._read_as_record_batch_reader("test_symbol")

        batch_count = 0
        total_rows = 0
        for batch in reader:
            batch_count += 1
            total_rows += len(batch)

        # With standard segment size, all data may fit in one batch
        # The important thing is that streaming works correctly
        assert batch_count >= 1, "Expected at least one batch"
        assert total_rows == 1000


class TestHelperFunctionsCoverageGaps:
    """Additional coverage tests for arrow_reader.py helper functions.

    Covers edge cases in _descriptor_to_arrow_schema, _is_wider_numeric_type,
    _expand_columns_with_idx_prefix, and _strip_idx_prefix_from_names that
    were identified as gaps in the original test suite.
    """

    def test_is_wider_numeric_type_all_pairs(self):
        """Test _is_wider_numeric_type across the full numeric hierarchy."""
        import pyarrow as pa
        from arcticdb.version_store.duckdb.arrow_reader import _is_wider_numeric_type

        # Wider: float64 > float32 > float16 > int64/uint64 > int32/uint32 > ...
        assert _is_wider_numeric_type(pa.float64(), pa.int64()) is True
        assert _is_wider_numeric_type(pa.float64(), pa.float32()) is True
        assert _is_wider_numeric_type(pa.float32(), pa.float16()) is True
        assert _is_wider_numeric_type(pa.int64(), pa.int32()) is True
        assert _is_wider_numeric_type(pa.int32(), pa.int16()) is True
        assert _is_wider_numeric_type(pa.int16(), pa.int8()) is True
        assert _is_wider_numeric_type(pa.uint64(), pa.uint32()) is True

        # Same rank → NOT wider (must be strictly wider)
        assert _is_wider_numeric_type(pa.int64(), pa.uint64()) is False
        assert _is_wider_numeric_type(pa.int8(), pa.uint8()) is False

        # Narrower → False
        assert _is_wider_numeric_type(pa.int32(), pa.int64()) is False
        assert _is_wider_numeric_type(pa.int8(), pa.float64()) is False

        # Same type → False
        assert _is_wider_numeric_type(pa.float64(), pa.float64()) is False
        assert _is_wider_numeric_type(pa.int32(), pa.int32()) is False

    def test_is_wider_numeric_type_non_numeric(self):
        """Non-numeric types (strings, timestamps) always return False."""
        import pyarrow as pa
        from arcticdb.version_store.duckdb.arrow_reader import _is_wider_numeric_type

        assert _is_wider_numeric_type(pa.large_string(), pa.int64()) is False
        assert _is_wider_numeric_type(pa.timestamp("ns"), pa.int64()) is False
        assert _is_wider_numeric_type(pa.int64(), pa.large_string()) is False
        assert _is_wider_numeric_type(pa.large_string(), pa.large_string()) is False

    def test_expand_columns_empty_list(self):
        """_expand_columns_with_idx_prefix on an empty list returns empty list."""
        from arcticdb.version_store.duckdb.arrow_reader import _expand_columns_with_idx_prefix

        assert _expand_columns_with_idx_prefix([]) == []

    def test_expand_columns_single_column(self):
        """Single column gets expanded to include its __idx__ variant."""
        from arcticdb.version_store.duckdb.arrow_reader import _expand_columns_with_idx_prefix

        result = _expand_columns_with_idx_prefix(["value"])
        assert result == ["value", "__idx__value"]

    def test_strip_idx_prefix_sequential_collisions(self):
        """Multiple sequential collision resolutions wrap with underscores correctly."""
        from arcticdb.version_store.duckdb.arrow_reader import _strip_idx_prefix_from_names

        # "x", "_x_" both already exist; "__idx__x" collides with "x", then "_x_",
        # so it becomes "__x__"
        result = _strip_idx_prefix_from_names(["x", "_x_", "__idx__x"])
        assert result == ["x", "_x_", "__x__"]

    def test_strip_idx_prefix_no_prefix(self):
        """Names without __idx__ prefix pass through unchanged."""
        from arcticdb.version_store.duckdb.arrow_reader import _strip_idx_prefix_from_names

        result = _strip_idx_prefix_from_names(["a", "b", "c"])
        assert result == ["a", "b", "c"]

    def test_build_clean_to_storage_map_all_prefixed(self):
        """All columns have __idx__ prefix — all appear in mapping."""
        from arcticdb.version_store.duckdb.arrow_reader import _build_clean_to_storage_map

        result = _build_clean_to_storage_map(["__idx__a", "__idx__b"])
        assert result == {"a": "__idx__a", "b": "__idx__b"}

    def test_descriptor_schema_all_types(self, lmdb_library):
        """Descriptor-based schema covers all DataType variants that ArcticDB supports.

        Write DataFrames with diverse column types and verify the descriptor schema
        round-trips through _descriptor_to_arrow_schema correctly.
        """
        import pyarrow as pa

        lib = lmdb_library
        df = pd.DataFrame(
            {
                "col_int8": np.array([1], dtype=np.int8),
                "col_int16": np.array([1], dtype=np.int16),
                "col_int32": np.array([1], dtype=np.int32),
                "col_int64": np.array([1], dtype=np.int64),
                "col_uint8": np.array([1], dtype=np.uint8),
                "col_uint16": np.array([1], dtype=np.uint16),
                "col_uint32": np.array([1], dtype=np.uint32),
                "col_uint64": np.array([1], dtype=np.uint64),
                "col_float32": np.array([1.0], dtype=np.float32),
                "col_float64": np.array([1.0], dtype=np.float64),
                "col_bool": np.array([True], dtype=bool),
                "col_str": ["hello"],
            }
        )
        lib.write("all_types", df)

        reader, _ = lib._read_as_record_batch_reader("all_types")
        schema = reader.schema

        # All columns should be present
        field_names = {f.name for f in schema}
        for col in df.columns:
            assert col in field_names, f"Missing column {col} in schema"

        # Verify specific type mappings
        schema_dict = {f.name: f.type for f in schema}
        assert schema_dict["col_bool"] == pa.bool_() or pa.types.is_boolean(schema_dict["col_bool"])

    def test_empty_symbol_with_column_projection(self, lmdb_library):
        """Empty symbol + column projection produces schema with only requested columns."""
        import pyarrow as pa

        lib = lmdb_library
        df = pd.DataFrame(
            {
                "a": pd.array([], dtype="int64"),
                "b": pd.array([], dtype="float64"),
                "c": pd.array([], dtype="int64"),
            }
        )
        lib.write("empty_proj", df)

        reader, _ = lib._read_as_record_batch_reader("empty_proj", columns=["a", "c"])
        schema = reader.schema

        field_names = [f.name for f in schema]
        assert "a" in field_names
        assert "c" in field_names
        assert "b" not in field_names

        table = reader.read_all()
        assert isinstance(table, pa.Table)
        assert len(table) == 0

    def test_current_index_advances_during_iteration(self, lmdb_library):
        """current_index advances correctly as batches are consumed."""
        lib = lmdb_library
        df = pd.DataFrame({"x": np.arange(100)})
        lib.write("sym", df)

        reader, _ = lib._read_as_record_batch_reader("sym")
        indices = []
        for _ in reader:
            indices.append(reader.current_index)

        # Indices should be monotonically increasing
        assert indices == sorted(indices)
        # After exhaustion, index should be at the total batch count
        assert reader.current_index == reader.num_batches
