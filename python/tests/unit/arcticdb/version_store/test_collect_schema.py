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
    schema = lazy_df.collect_schema()
    assert schema == pl.from_arrow(table).schema


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
    schema = lazy_df.collect_schema()
    assert schema == df.schema
    # Default override
    lazy_df = lib.read(sym, arrow_string_format_default=ArrowOutputStringFormat.CATEGORICAL, lazy=True)
    schema = lazy_df.collect_schema()
    expected_schema = df.select(pl.col("col1").cast(pl.Categorical), pl.col("col2").cast(pl.Categorical)).schema
    assert schema == expected_schema
    # Specific override
    lazy_df = lib.read(sym, arrow_string_format_per_column={"col1": ArrowOutputStringFormat.CATEGORICAL}, lazy=True)
    schema = lazy_df.collect_schema()
    expected_schema = df.select(pl.col("col1").cast(pl.Categorical), pl.col("col2")).schema
    assert schema == expected_schema
    # Default and specific override
    lazy_df = lib.read(
        sym,
        arrow_string_format_default=ArrowOutputStringFormat.CATEGORICAL,
        arrow_string_format_per_column={"col1": ArrowOutputStringFormat.LARGE_STRING},
        lazy=True,
    )
    schema = lazy_df.collect_schema()
    expected_schema = df.select(pl.col("col1"), pl.col("col2").cast(pl.Categorical)).schema
    assert schema == expected_schema


def test_collect_schema_column_filtering(lmdb_library):
    lib = lmdb_library
    lib._nvs.set_output_format(OutputFormat.POLARS)
    lib._nvs._set_allow_arrow_input()
    sym = "test_collect_schema_basic"
    table = pa.table({"col1": pa.array([0], pa.int64()), "col2": pa.array([1], pa.float32())})
    lib.write(sym, table)

    lazy_df = lib.read(sym, columns=["col2"], lazy=True)
    schema = lazy_df.collect_schema()
    expected_schema = pl.from_arrow(table).select(pl.col("col2")).schema
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
    schema = lib.read(sym, lazy=True).collect_schema()
    assert schema == pl.Schema([("index", pl.Datetime("ns")), ("col1", pl.Int64), ("col2", pl.Float32)])
    # Named, no column selection
    df.index.name = "ts"
    lib.write(sym, df)
    schema = lib.read(sym, lazy=True).collect_schema()
    assert schema == pl.Schema([("ts", pl.Datetime("ns")), ("col1", pl.Int64), ("col2", pl.Float32)])
    # With column selection. Note the index column is dropped, as this will be the behaviour Polars expects
    schema = lib.read(sym, columns=["col2"], lazy=True).collect_schema()
    assert schema == pl.Schema([("col2", pl.Float32)])


def test_collect_schema_with_query(lmdb_library):
    lib = lmdb_library
    lib._nvs.set_output_format(OutputFormat.POLARS)
    lib._nvs._set_allow_arrow_input()

    sym = "test_collect_schema_basic"
    table = pa.table({"col1": pa.array([0], pa.int64()), "col2": pa.array([1], pa.float32())})
    lib.write(sym, table)

    lazy_df = lib.read(sym, lazy=True)
    lazy_df["new_col"] = 2 * lazy_df["col1"]
    schema = lazy_df.collect_schema()
    assert schema == pl.Schema([("col1", pl.Int64), ("col2", pl.Float32), ("new_col", pl.Int64)])


def test_collect_schema_and_collect_multiple_times(s3_library):
    with config_context("VersionMap.ReloadInterval", 0):
        lib = s3_library
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
            lazy_df.collect_schema()
            stats = qs.get_query_stats()
        qs.reset_stats()
        # Read the vref and index key to get the schema, but no data keys yet
        assert stats["storage_operations"]["S3_GetObject"]["VERSION_REF"]["count"] == 1
        assert stats["storage_operations"]["S3_GetObject"]["TABLE_INDEX"]["count"] == 1
        assert "TABLE_DATA" not in stats["storage_operations"]["S3_GetObject"]
        with qs.query_stats():
            lazy_df.collect_schema()
            stats = qs.get_query_stats()
        qs.reset_stats()
        # Collect again is a no-op as it would produce the same result
        assert stats == dict()
        with qs.query_stats():
            received_df = lazy_df.collect().data
            stats = qs.get_query_stats()
        qs.reset_stats()
        # Read the data key, but no vref, version, or index keys
        assert stats["storage_operations"]["S3_GetObject"]["TABLE_DATA"]["count"] == 1
        assert "VERSION_REF" not in stats["storage_operations"]["S3_GetObject"]
        assert "VERSION" not in stats["storage_operations"]["S3_GetObject"]
        assert "TABLE_INDEX" not in stats["storage_operations"]["S3_GetObject"]
        assert_frame_equal_with_arrow(df, received_df)

        # Change the query
        lazy_df["new_col"] = lazy_df["col1"] + lazy_df["col2"]
        with qs.query_stats():
            lazy_df.collect_schema()
            stats = qs.get_query_stats()
        qs.reset_stats()
        # The query has changed, so we go back to storage
        assert stats["storage_operations"]["S3_GetObject"]["VERSION_REF"]["count"] == 1
        assert stats["storage_operations"]["S3_GetObject"]["TABLE_INDEX"]["count"] == 1
        assert "TABLE_DATA" not in stats["storage_operations"]["S3_GetObject"]
        with qs.query_stats():
            received_df = lazy_df.collect().data
            stats = qs.get_query_stats()
        qs.reset_stats()
        # Read the data key, but no vref, version, or index keys
        assert stats["storage_operations"]["S3_GetObject"]["TABLE_DATA"]["count"] == 1
        assert "VERSION_REF" not in stats["storage_operations"]["S3_GetObject"]
        assert "VERSION" not in stats["storage_operations"]["S3_GetObject"]
        assert "TABLE_INDEX" not in stats["storage_operations"]["S3_GetObject"]
        df["new_col"] = df["col1"] + df["col2"]
        assert_frame_equal_with_arrow(df, received_df)


def test_collect_schema_multiindex(lmdb_library):
    lib = lmdb_library
    lib._nvs.set_output_format(OutputFormat.POLARS)
    sym = "test_collect_schema_multiindex"
    df = pd.DataFrame(
        {"col1": np.arange(10, dtype=np.int64), "col2": np.arange(100, 110, dtype=np.float32)},
    )
    lib.write(sym, {"a": df, "b": df}, recursive_normalizers=True)

    lazy_df = lib.read(sym, lazy=True)
    with pytest.raises(SchemaException) as e:
        lazy_df.collect_schema()
    print(e)
