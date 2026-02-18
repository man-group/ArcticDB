"""
Copyright 2026 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

import numpy as np
import pandas as pd
import pyarrow as pa
import polars as pl
import pytest

from arcticdb_ext.exceptions import KeyNotFoundException
from arcticdb.exceptions import SchemaException
from arcticdb.options import ArrowOutputStringFormat, OutputFormat
import arcticdb.toolbox.query_stats as qs
from arcticdb.util.test import assert_frame_equal_with_arrow, config_context


def test_collect_schema_basic(lmdb_library):
    lib = lmdb_library
    lib._nvs.set_output_format(OutputFormat.POLARS)
    lib._nvs._set_allow_arrow_input()
    sym = "test_collect_schema_basic"
    table = pa.table(
        {
            "uint8": pa.array([0], pa.uint8()),
            "uint16": pa.array([1], pa.uint16()),
            "uint32": pa.array([2], pa.uint32()),
            "uint64": pa.array([3], pa.uint64()),
            "int8": pa.array([4], pa.int8()),
            "int16": pa.array([5], pa.int16()),
            "int32": pa.array([6], pa.int32()),
            "int64": pa.array([7], pa.int64()),
            "float32": pa.array([8], pa.float32()),
            "float64": pa.array([9], pa.float64()),
            "timestamp": pa.array([pa.scalar(10, type=pa.timestamp("ns"))]),
        }
    )
    lib.write(sym, table)

    lazy_df = lib.read(sym, lazy=True)
    schema = lazy_df._collect_schema()
    assert schema == pl.from_arrow(table).schema


def test_collect_schema_as_of(lmdb_library):
    lib = lmdb_library
    lib._nvs.set_output_format(OutputFormat.POLARS)
    lib._nvs._set_allow_arrow_input()
    sym = "test_collect_schema_as_of"
    table_0 = pa.table({"uint8": pa.array([0], pa.uint8())})
    lib.write(sym, table_0)
    lib.snapshot("snapshot")
    table_1 = pa.table({"uint16": pa.array([0], pa.uint16())})
    lib.write(sym, table_1)

    assert lib.read(sym, as_of=0, lazy=True)._collect_schema() == pl.from_arrow(table_0).schema
    assert lib.read(sym, as_of=1, lazy=True)._collect_schema() == pl.from_arrow(table_1).schema
    assert lib.read(sym, as_of="snapshot", lazy=True)._collect_schema() == pl.from_arrow(table_0).schema


def test_collect_schema_while_versions_being_added(lmdb_library):
    lib = lmdb_library
    lib._nvs.set_output_format(OutputFormat.POLARS)
    lib._nvs._set_allow_arrow_input()
    sym = "test_collect_schema_while_versions_being_added"
    table_0 = pa.table({"uint8": pa.array([0], pa.uint8())})
    lib.write(sym, table_0)
    lazy_df = lib.read(sym, lazy=True)
    schema = lazy_df._collect_schema()
    table_1 = pa.table({"uint16": pa.array([0], pa.uint16())})
    lib.write(sym, table_1)
    # Schema should still match table_0
    assert schema == pl.from_arrow(table_0).schema
    # Calling collect will fetch table_0
    assert_frame_equal_with_arrow(table_0, lazy_df.collect().data)
    # If we call collect_schema again we will go back to storage, so now schema and df will match table_1
    lazy_df = lib.read(sym, lazy=True)
    assert lazy_df._collect_schema() == pl.from_arrow(table_1).schema
    assert_frame_equal_with_arrow(table_1, lazy_df.collect().data)


def test_collect_schema_index_with_timezone(lmdb_library):
    lib = lmdb_library
    lib._nvs.set_output_format(OutputFormat.POLARS)
    sym = "test_collect_schema_index_with_timezone"
    # Unnamed, no column selection
    idx = pd.date_range("2026-01-01", periods=10, tz="Europe/Amsterdam")
    df = pd.DataFrame(
        {"col1": np.arange(len(idx), dtype=np.int8), "col2": np.arange(len(idx), dtype=np.uint16)}, index=idx
    )
    lib.write(sym, df)
    assert lib.read(sym, lazy=True)._collect_schema() == lib.read(sym).data.schema
    # Unnamed with column selection
    assert lib.read(sym, columns=["col2"], lazy=True)._collect_schema() == lib.read(sym, columns=["col2"]).data.schema
    # Named, no column selection
    df.index.name = "ts"
    lib.write(sym, df)
    assert lib.read(sym, lazy=True)._collect_schema() == lib.read(sym).data.schema
    # Named with column selection
    assert lib.read(sym, columns=["col2"], lazy=True)._collect_schema() == lib.read(sym, columns=["col2"]).data.schema


def test_collect_schema_multiindex(lmdb_library):
    lib = lmdb_library
    lib._nvs.set_output_format(OutputFormat.POLARS)
    sym = "test_collect_schema_multiindex"
    # Unnamed, no column selection
    idx = pd.MultiIndex.from_product([pd.date_range("2025-01-01", periods=2), np.arange(2, dtype=np.int32)])
    df = pd.DataFrame(
        {"col1": np.arange(len(idx), dtype=np.int8), "col2": np.arange(len(idx), dtype=np.uint16)}, index=idx
    )
    lib.write(sym, df)
    assert lib.read(sym, lazy=True)._collect_schema() == lib.read(sym).data.schema
    # Unnamed with column selection
    assert lib.read(sym, columns=["col2"], lazy=True)._collect_schema() == lib.read(sym, columns=["col2"]).data.schema
    # Named, no column selection
    idx = pd.MultiIndex.from_product(
        [pd.date_range("2025-01-01", periods=2), np.arange(2, dtype=np.int32)], names=["ts", "ints"]
    )
    df = pd.DataFrame(
        {"col1": np.arange(len(idx), dtype=np.int8), "col2": np.arange(len(idx), dtype=np.uint16)}, index=idx
    )
    lib.write(sym, df)
    assert lib.read(sym, lazy=True)._collect_schema() == lib.read(sym).data.schema
    # Named with column selection
    assert lib.read(sym, columns=["col2"], lazy=True)._collect_schema() == lib.read(sym, columns=["col2"]).data.schema


def test_collect_schema_string_types(lmdb_library):
    lib = lmdb_library
    lib._nvs.set_output_format(OutputFormat.POLARS)
    lib._nvs._set_allow_arrow_input()
    sym = "test_collect_schema_string_types"
    table = pa.table({"col1": pa.array(["a"], pa.string()), "col2": pa.array(["b"], pa.string())})
    lib.write(sym, table)

    df = pl.from_arrow(table)
    # No overrides
    lazy_df = lib.read(sym, lazy=True)
    schema = lazy_df._collect_schema()
    assert schema == df.schema
    # Default override
    lazy_df = lib.read(sym, arrow_string_format_default=ArrowOutputStringFormat.CATEGORICAL, lazy=True)
    schema = lazy_df._collect_schema()
    expected_schema = df.select(pl.col("col1").cast(pl.Categorical), pl.col("col2").cast(pl.Categorical)).schema
    assert schema == expected_schema
    # Specific override
    lazy_df = lib.read(sym, arrow_string_format_per_column={"col1": ArrowOutputStringFormat.CATEGORICAL}, lazy=True)
    schema = lazy_df._collect_schema()
    expected_schema = df.select(pl.col("col1").cast(pl.Categorical), pl.col("col2")).schema
    assert schema == expected_schema
    # Default and specific override
    lazy_df = lib.read(
        sym,
        arrow_string_format_default=ArrowOutputStringFormat.CATEGORICAL,
        arrow_string_format_per_column={"col1": ArrowOutputStringFormat.LARGE_STRING},
        lazy=True,
    )
    schema = lazy_df._collect_schema()
    expected_schema = df.select(pl.col("col1"), pl.col("col2").cast(pl.Categorical)).schema
    assert schema == expected_schema


def test_collect_schema_column_filtering(lmdb_library):
    lib = lmdb_library
    lib._nvs.set_output_format(OutputFormat.POLARS)
    lib._nvs._set_allow_arrow_input()
    sym = "test_collect_schema_basic"
    table = pa.table(
        {
            "col1": pa.array([0], pa.int64()),
            "col2": pa.array([1], pa.float32()),
            "col3": pa.array([2], pa.int8()),
            "col4": pa.array([3], pa.uint16()),
            "col5": pa.array([4], pa.float64()),
        }
    )
    lib.write(sym, table)

    lazy_df = lib.read(sym, columns=["col2", "col4", "col5"], lazy=True)
    schema = lazy_df._collect_schema()
    expected_schema = pl.from_arrow(table).select(["col2", "col4", "col5"]).schema
    assert schema == expected_schema


def test_collect_schema_timeseries(lmdb_library):
    lib = lmdb_library
    lib._nvs.set_output_format(OutputFormat.POLARS)
    sym = "test_collect_schema_timeseries"
    df = pd.DataFrame(
        {"col1": np.arange(10, dtype=np.int64), "col2": np.arange(100, 110, dtype=np.float32)},
        index=pd.date_range("2025-01-01", periods=10),
    )
    # Unnamed, no column selection
    lib.write(sym, df)
    schema = lib.read(sym, lazy=True)._collect_schema()
    assert schema == pl.Schema([("__index__", pl.Datetime("ns")), ("col1", pl.Int64), ("col2", pl.Float32)])
    # Named, no column selection
    df.index.name = "ts"
    lib.write(sym, df)
    schema = lib.read(sym, lazy=True)._collect_schema()
    assert schema == pl.Schema([("ts", pl.Datetime("ns")), ("col1", pl.Int64), ("col2", pl.Float32)])
    # With column selection
    schema = lib.read(sym, columns=["col2"], lazy=True)._collect_schema()
    assert schema == pl.Schema([("ts", pl.Datetime("ns")), ("col2", pl.Float32)])


def test_collect_schema_with_query(lmdb_library):
    lib = lmdb_library
    lib._nvs.set_output_format(OutputFormat.POLARS)
    lib._nvs._set_allow_arrow_input()

    sym = "test_collect_schema_basic"
    table = pa.table({"col1": pa.array([0], pa.int64()), "col2": pa.array([1], pa.float32())})
    lib.write(sym, table)

    lazy_df = lib.read(sym, lazy=True)
    lazy_df["new_col"] = 2 * lazy_df["col1"]
    schema = lazy_df._collect_schema()
    assert schema == pl.Schema([("col1", pl.Int64), ("col2", pl.Float32), ("new_col", pl.Int64)])


def test_collect_schema_and_collect_multiple_times(mem_library):
    with config_context("VersionMap.ReloadInterval", 0):
        lib = mem_library
        lib._nvs.set_output_format(OutputFormat.POLARS)
        sym = "test_collect_schema_multiple_times"
        df = pd.DataFrame(
            {"col1": np.arange(10, dtype=np.int64), "col2": np.arange(100, 110, dtype=np.float32)},
        )
        lib.write(sym, df)

        with qs.query_stats():
            lazy_df = lib.read(sym, lazy=True)
            stats = qs.get_query_stats()
        qs.reset_stats()
        # No IO yet
        assert stats == dict()
        with qs.query_stats():
            lazy_df._collect_schema()
            stats = qs.get_query_stats()
        qs.reset_stats()
        # Read the vref and index key to get the schema, but no data keys yet
        assert stats["storage_operations"]["Memory_GetObject"]["VERSION_REF"]["count"] == 1
        assert stats["storage_operations"]["Memory_GetObject"]["TABLE_INDEX"]["count"] == 1
        assert "TABLE_DATA" not in stats["storage_operations"]["Memory_GetObject"]
        with qs.query_stats():
            lazy_df._collect_schema()
            stats = qs.get_query_stats()
        qs.reset_stats()
        # Read the same keys again, see comment in _collect_schema() impl for explanation of why
        assert stats["storage_operations"]["Memory_GetObject"]["VERSION_REF"]["count"] == 1
        assert stats["storage_operations"]["Memory_GetObject"]["TABLE_INDEX"]["count"] == 1
        assert "TABLE_DATA" not in stats["storage_operations"]["Memory_GetObject"]
        with qs.query_stats():
            received_df = lazy_df.collect().data
            stats = qs.get_query_stats()
        qs.reset_stats()
        # Read the data key, but no vref, version, or index keys
        assert stats["storage_operations"]["Memory_GetObject"]["TABLE_DATA"]["count"] == 1
        assert "VERSION_REF" not in stats["storage_operations"]["Memory_GetObject"]
        assert "VERSION" not in stats["storage_operations"]["Memory_GetObject"]
        assert "TABLE_INDEX" not in stats["storage_operations"]["Memory_GetObject"]
        assert_frame_equal_with_arrow(df, received_df)

        # Change the query
        lazy_df["new_col"] = lazy_df["col1"] + lazy_df["col2"]
        with qs.query_stats():
            lazy_df._collect_schema()
            stats = qs.get_query_stats()
        qs.reset_stats()
        # Read the same keys again, see comment in _collect_schema() impl for explanation of why
        assert stats["storage_operations"]["Memory_GetObject"]["VERSION_REF"]["count"] == 1
        assert stats["storage_operations"]["Memory_GetObject"]["TABLE_INDEX"]["count"] == 1
        assert "TABLE_DATA" not in stats["storage_operations"]["Memory_GetObject"]
        with qs.query_stats():
            received_df = lazy_df.collect().data
            stats = qs.get_query_stats()
        qs.reset_stats()
        # Read the data key, but no vref, version, or index keys
        assert stats["storage_operations"]["Memory_GetObject"]["TABLE_DATA"]["count"] == 1
        assert "VERSION_REF" not in stats["storage_operations"]["Memory_GetObject"]
        assert "VERSION" not in stats["storage_operations"]["Memory_GetObject"]
        assert "TABLE_INDEX" not in stats["storage_operations"]["Memory_GetObject"]
        df["new_col"] = df["col1"] + df["col2"]
        assert_frame_equal_with_arrow(df, received_df)


def test_collect_schema_and_collect_version_deleted(mem_library):
    with config_context("VersionMap.ReloadInterval", 0):
        lib = mem_library
        lib._nvs.set_output_format(OutputFormat.POLARS)
        sym = "test_collect_schema_and_collect_version_deleted"
        df = pd.DataFrame(
            {"col1": np.arange(10, dtype=np.int64), "col2": np.arange(100, 110, dtype=np.float32)},
        )
        lib.write(sym, df)

        lazy_df = lib.read(sym, lazy=True)
        lazy_df._collect_schema()

        lib.delete(sym)

        with qs.query_stats():
            # This is the same behaviour as if the version being read is deleted after the index key is read but before
            # all of the data keys have been read
            with pytest.raises(KeyNotFoundException):
                lazy_df.collect()
            stats = qs.get_query_stats()
        qs.reset_stats()
        # Attempted to read the data key but failed (hence 0 bytes)
        assert stats["storage_operations"]["Memory_GetObject"]["TABLE_DATA"]["count"] == 1
        assert stats["storage_operations"]["Memory_GetObject"]["TABLE_DATA"]["size_bytes"] == 0
        # Did not attempt to read from the version chain again
        assert "VERSION_REF" not in stats["storage_operations"]["Memory_GetObject"]
        assert "VERSION" not in stats["storage_operations"]["Memory_GetObject"]
        assert "TABLE_INDEX" not in stats["storage_operations"]["Memory_GetObject"]


def test_collect_schema_recursive_normalizers(lmdb_library):
    lib = lmdb_library
    lib._nvs.set_output_format(OutputFormat.POLARS)
    sym = "test_collect_schema_recursive_normalizers"
    df = pd.DataFrame(
        {"col1": np.arange(10, dtype=np.int64), "col2": np.arange(100, 110, dtype=np.float32)},
    )
    lib.write(sym, {"a": df, "b": df}, recursive_normalizers=True)

    lazy_df = lib.read(sym, lazy=True)
    with pytest.raises(SchemaException):
        lazy_df._collect_schema()


def test_collect_schema_pickled_symbol(lmdb_library):
    lib = lmdb_library
    lib._nvs.set_output_format(OutputFormat.POLARS)
    sym = "test_collect_schema_pickled_symbol"
    lib.write_pickle(sym, "blah")
    with pytest.raises(SchemaException):
        lib.read(sym, lazy=True)._collect_schema()
