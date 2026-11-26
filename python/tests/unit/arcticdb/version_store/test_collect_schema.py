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

from arcticdb.exceptions import SchemaException, KeyNotFoundException
from arcticdb.options import ArrowOutputStringFormat, OutputFormat
import arcticdb.toolbox.query_stats as qs
from arcticdb.util.test import assert_frame_equal_with_arrow, config_context, query_stats_operation_count
from arcticdb import LazyDataFrameCollection, concat
from tests.util.arrow import create_1d_arrow_structure
from pandas.testing import assert_frame_equal


@pytest.mark.parametrize("num_rows", [0, 1])
def test_collect_schema_basic(lmdb_library, num_rows):
    lib = lmdb_library
    lib._nvs.set_output_format(OutputFormat.POLARS)
    sym = "test_collect_schema_basic"
    table = pa.table(
        {
            "uint8": pa.array(num_rows * [0], pa.uint8()),
            "uint16": pa.array(num_rows * [1], pa.uint16()),
            "uint32": pa.array(num_rows * [2], pa.uint32()),
            "uint64": pa.array(num_rows * [3], pa.uint64()),
            "int8": pa.array(num_rows * [4], pa.int8()),
            "int16": pa.array(num_rows * [5], pa.int16()),
            "int32": pa.array(num_rows * [6], pa.int32()),
            "int64": pa.array(num_rows * [7], pa.int64()),
            "float32": pa.array(num_rows * [8], pa.float32()),
            "float64": pa.array(num_rows * [9], pa.float64()),
            "timestamp": pa.array(num_rows * [pa.scalar(10, type=pa.timestamp("ns"))], pa.timestamp("ns")),
        }
    )
    lib.write(sym, table)

    lazy_df = lib.read(sym, lazy=True)
    schema = lazy_df._collect_schema()
    assert schema == pl.from_arrow(table).schema


def test_collect_schema_as_of(lmdb_library):
    lib = lmdb_library
    lib._nvs.set_output_format(OutputFormat.POLARS)
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


def test_collect_schema_with_timezones(lmdb_library):
    lib = lmdb_library
    lib._nvs.set_output_format(OutputFormat.POLARS)
    sym = "test_collect_schema_with_timezones"
    # No column selection
    table = pa.table(
        {
            "ts": pa.Array.from_pandas(
                pd.date_range("2025-01-01", periods=2, tz="America/New_York"),
                type=pa.timestamp("ns", tz="America/New_York"),
            ),
            "col_utc": pa.Array.from_pandas(
                pd.date_range("2025-01-03", periods=2, tz="Etc/UTC"),
                type=pa.timestamp("ns", tz="Etc/UTC"),
            ),
            "col_with_tz": pa.Array.from_pandas(
                pd.date_range("2025-01-03", periods=2, tz="Europe/Brussels"),
                type=pa.timestamp("ns", tz="Europe/Brussels"),
            ),
            "col_without_tz": pa.Array.from_pandas(
                pd.date_range("2025-01-03", periods=2),
                type=pa.timestamp("ns"),
            ),
        }
    )
    lib.write(sym, table, index_column=True)
    assert lib.read(sym, lazy=True)._collect_schema() == lib.read(sym).data.schema
    # With column selection
    assert (
        lib.read(sym, columns=["col_with_tz", "col_without_tz"], lazy=True)._collect_schema()
        == lib.read(sym, columns=["col_with_tz", "col_without_tz"]).data.schema
    )


def test_collect_schema_pandas_with_timezone(lmdb_library):
    """Pandas-written data has to be described in Arrow terms here exactly as the read path describes it,
    so that _collect_schema and a read cannot disagree."""
    lib = lmdb_library
    lib._nvs.set_output_format(OutputFormat.POLARS)
    sym = "test_collect_schema_pandas_with_timezone"
    df = pd.DataFrame(
        {"col": np.arange(2, dtype=np.int64)}, index=pd.date_range("2025-01-01", periods=2, tz="America/New_York")
    )
    df.index.name = "ts"
    lib.write(sym, df)

    schema = lib.read(sym, lazy=True)._collect_schema()
    assert schema == lib.read(sym).data.schema
    assert schema["ts"].time_zone == "America/New_York"


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


@pytest.mark.parametrize("columns", [[], ["other"]])
def test_collect_schema_series_column_selection_matches_read(lmdb_library, columns):
    lib = lmdb_library
    lib._nvs.set_output_format(OutputFormat.POLARS)
    sym = "test_collect_schema_series_column_selection_matches_read"
    lib.write(sym, pd.Series([1, 2], index=pd.date_range("2026-01-01", periods=2), name="v"))
    schema = lib.read(sym, columns=columns, lazy=True)._collect_schema()
    assert schema.names() == ["__index__"]
    assert schema == lib.read(sym, columns=columns).data.schema


@pytest.mark.parametrize(
    "columns,expected_names",
    [
        (None, ["___index___", "a", "_a_", "__index__"]),
        (["__index__"], ["___index___", "__index__"]),
        # Selection matches on-disk names, which "a" is not, so only the index is returned
        (["a"], ["__index__"]),
    ],
)
def test_collect_schema_pandas_renamed_columns(lmdb_library, columns, expected_names):
    lib = lmdb_library
    lib._nvs.set_output_format(OutputFormat.POLARS)
    sym = "test_collect_schema_pandas_renamed_columns"
    idx = pd.date_range("2026-01-01", periods=2)
    lib.write(sym, pd.DataFrame([["x", "y", 0], ["z", "w", 1]], columns=["a", "a", "__index__"], index=idx))
    schema = lib.read(sym, columns=columns, lazy=True)._collect_schema()
    assert schema.names() == expected_names
    assert schema == lib.read(sym, columns=columns).data.schema


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

    sym = "test_collect_schema_basic"
    table = pa.table({"col1": pa.array([0], pa.int64()), "col2": pa.array([1], pa.float32())})
    lib.write(sym, table)

    lazy_df = lib.read(sym, lazy=True)
    lazy_df["new_col"] = 2 * lazy_df["col1"]
    schema = lazy_df._collect_schema()
    assert schema == pl.Schema([("col1", pl.Int64), ("col2", pl.Float32), ("new_col", pl.Int64)])


@pytest.mark.parametrize("create_column_stats", [True, False])
def test_collect_schema_and_collect_multiple_times(mem_library, create_column_stats, clear_query_stats):
    with (
        config_context("VersionMap.ReloadInterval", 0),
        config_context("ColumnStats.UseForQueries", int(create_column_stats)),
    ):
        lib = mem_library
        lib._nvs.set_output_format(OutputFormat.POLARS)
        sym = "test_collect_schema_multiple_times"

        df0 = pd.DataFrame(
            {"col1": np.arange(5, dtype=np.int64), "col2": np.arange(100, 105, dtype=np.float32)},
            index=pd.date_range("2000-01-01", periods=5),
        )
        df1 = pd.DataFrame(
            {"col1": np.arange(5, 10, dtype=np.int64), "col2": np.arange(105, 110, dtype=np.float32)},
            index=pd.date_range("2000-01-06", periods=5),
        )
        df0.index.name = "ts"
        df1.index.name = "ts"
        lib.write(sym, df0)
        lib.append(sym, df1)
        df = pd.concat([df0, df1])

        if create_column_stats:
            lib._nvs.create_column_stats_experimental(sym)

        with qs.query_stats():
            lazy_df = lib.read(sym, lazy=True)
            stats = qs.get_query_stats()
        qs.reset_stats()
        # No IO yet
        assert stats == dict()

        lazy_df = lazy_df[lazy_df["col1"] > 4]

        with qs.query_stats():
            lazy_df._collect_schema()
            stats = qs.get_query_stats()
        qs.reset_stats()
        # Read the vref and index key to get the schema, but no data keys yet
        assert query_stats_operation_count(stats, "Memory_GetObject", "VERSION_REF") == 1
        assert query_stats_operation_count(stats, "Memory_GetObject", "TABLE_INDEX") == 1
        assert query_stats_operation_count(stats, "Memory_GetObject", "TABLE_DATA") == 0
        if create_column_stats:
            assert query_stats_operation_count(stats, "Memory_GetObject", "COLUMN_STATS") == 1
        else:
            assert query_stats_operation_count(stats, "Memory_GetObject", "COLUMN_STATS") == 0
        with qs.query_stats():
            lazy_df._collect_schema()
            stats = qs.get_query_stats()
        qs.reset_stats()
        # Read the same keys again, see comment in _collect_schema() impl for explanation of why
        assert query_stats_operation_count(stats, "Memory_GetObject", "VERSION_REF") == 1
        assert query_stats_operation_count(stats, "Memory_GetObject", "TABLE_INDEX") == 1
        assert query_stats_operation_count(stats, "Memory_GetObject", "TABLE_DATA") == 0
        if create_column_stats:
            assert query_stats_operation_count(stats, "Memory_GetObject", "COLUMN_STATS") == 1
        with qs.query_stats():
            received_df = lazy_df.collect().data
            stats = qs.get_query_stats()
        qs.reset_stats()
        if create_column_stats:
            # Read the data key, but no vref, version, or index keys
            assert query_stats_operation_count(stats, "Memory_GetObject", "TABLE_DATA") == 1
        else:
            # Read the data key, but no vref, version, or index keys
            assert query_stats_operation_count(stats, "Memory_GetObject", "TABLE_DATA") == 2
        assert query_stats_operation_count(stats, "Memory_GetObject", "COLUMN_STATS") == 0
        assert query_stats_operation_count(stats, "Memory_GetObject", "VERSION_REF") == 0
        assert query_stats_operation_count(stats, "Memory_GetObject", "VERSION") == 0
        assert query_stats_operation_count(stats, "Memory_GetObject", "TABLE_INDEX") == 0
        expected = df[df["col1"] > 4].reset_index()
        assert_frame_equal_with_arrow(expected, received_df)

        # Change the query by adding a projection
        lazy_df["new_col"] = lazy_df["col1"] + lazy_df["col2"]
        with qs.query_stats():
            lazy_df._collect_schema()
            stats = qs.get_query_stats()
        qs.reset_stats()
        # Read the same keys again, see comment in _collect_schema() impl for explanation of why
        assert query_stats_operation_count(stats, "Memory_GetObject", "VERSION_REF") == 1
        assert query_stats_operation_count(stats, "Memory_GetObject", "TABLE_INDEX") == 1
        if create_column_stats:
            assert query_stats_operation_count(stats, "Memory_GetObject", "COLUMN_STATS") == 1
        assert "TABLE_DATA" not in stats["storage_operations"]["Memory_GetObject"]
        with qs.query_stats():
            received_df = lazy_df.collect().data
            stats = qs.get_query_stats()
        qs.reset_stats()
        # Column stats were preloaded during _collect_schema, clone preserves them
        assert query_stats_operation_count(stats, "Memory_GetObject", "COLUMN_STATS") == 0
        if create_column_stats:
            assert query_stats_operation_count(stats, "Memory_GetObject", "TABLE_DATA") == 1
        else:
            # Read the data key, but no vref, version, or index keys
            assert query_stats_operation_count(stats, "Memory_GetObject", "TABLE_DATA") == 2
        assert "VERSION_REF" not in stats["storage_operations"]["Memory_GetObject"]
        assert "VERSION" not in stats["storage_operations"]["Memory_GetObject"]
        assert "TABLE_INDEX" not in stats["storage_operations"]["Memory_GetObject"]
        expected["new_col"] = expected["col1"] + expected["col2"]
        assert_frame_equal_with_arrow(expected, received_df)


def test_collect_schema_and_collect_version_deleted(mem_library, clear_query_stats):
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
        assert query_stats_operation_count(stats, "Memory_GetObject", "TABLE_DATA") == 1
        assert stats["storage_operations"]["Memory_GetObject"]["TABLE_DATA"]["size_bytes"] == 0
        # Did not attempt to read from the version chain again
        assert query_stats_operation_count(stats, "Memory_GetObject", "VERSION_REF") == 0
        assert query_stats_operation_count(stats, "Memory_GetObject", "VERSION") == 0
        assert query_stats_operation_count(stats, "Memory_GetObject", "TABLE_INDEX") == 0


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


@pytest.mark.parametrize("input_type", ["Array", "ChunkedArray", "UnnamedSeries", "NamedSeries"])
def test_collect_schema_1d(mem_library, input_type):
    lib = mem_library
    sym = "test_collect_schema_1d"
    data = pa.array([0], pa.int64())
    input = create_1d_arrow_structure(input_type, data)
    lib.write(sym, input)
    schema = lib.read(sym, lazy=True)._collect_schema()
    expected_name = "series_name" if input_type == "NamedSeries" else ""
    assert schema == pl.Schema({expected_name: pl.Int64})


@pytest.mark.parametrize(
    "as_of",
    [
        pytest.param(None, id="latest"),
        pytest.param(0, id="version-zero"),
        pytest.param("saved", id="snapshot"),
    ],
)
def test_read_request_without_preloaded_index(mem_library, as_of):
    """Keep the requested version selector before schema discovery."""
    lib = mem_library
    lib.write("data", pd.DataFrame({"a": np.arange(6)}))
    lib.snapshot("saved")

    source = lib.read("data", as_of=as_of, lazy=True)
    request = source._to_read_request()

    # No index has been loaded yet
    assert source._preloaded_index is None

    # Building the request must not change the selected version
    assert request.as_of == as_of
    assert source.read_request.as_of == as_of


@pytest.mark.parametrize(
    "as_of",
    [
        pytest.param(None, id="latest"),
        pytest.param(0, id="version-zero"),
        pytest.param("saved", id="snapshot"),
    ],
)
def test_read_request_reuses_preloaded_index(mem_library, clear_query_stats, as_of):
    """Reuse the loaded index and preserve the existing filter."""
    lib = mem_library
    original = pd.DataFrame({"a": np.arange(6, dtype=np.int64)})

    lib.write("data", original)
    lib.snapshot("saved")

    source = lib.read("data", as_of=as_of, lazy=True)
    source = source[source["a"] > 1]

    with config_context("VersionMap.ReloadInterval", 0):
        # Load version 0's index while discovering its schema
        source._collect_schema()
        loaded_index = source._preloaded_index
        assert loaded_index is not None

        # Publish a newer version with different values
        lib.write("data", original.assign(a=original["a"] + 100))

        request = source._to_read_request()

        # The request must use the index already loaded for the schema
        assert request.as_of is loaded_index

        # Keep the original selector available for later schema discovery
        assert source.read_request.as_of == as_of

        qs.reset_stats()
        with qs.query_stats():
            # Read through the generated request, without using collect()
            result = lib.read(**request._asdict())
            stats = qs.get_query_stats()

        # The loaded index must keep this read on version 0
        assert result.version == 0

        # Keep the matching values from the original table.
        # ArcticDB rebuilds the row-count index after filtering, so reset
        # pandas' retained row labels to match that representation
        expected = original[original["a"] > 1].reset_index(drop=True)
        assert_frame_equal(result.data, expected)

        # This small table is stored as one data segment so reading its values should fetch it once
        assert query_stats_operation_count(stats, "Memory_GetObject", "TABLE_DATA") == 1

        # Do not fetch the index or resolve the version again
        for key_type in ("TABLE_INDEX", "VERSION_REF", "VERSION"):
            assert query_stats_operation_count(stats, "Memory_GetObject", key_type) == 0

        # A second request must still use the same loaded index
        assert source._to_read_request().as_of is loaded_index


def test_read_request_uses_latest_schema_index(mem_library):
    """Use the newly loaded index after discovering the schema again."""
    lib = mem_library
    original = pd.DataFrame({"a": np.arange(6, dtype=np.int64)})
    lib.write("data", original)

    source = lib.read("data", lazy=True)

    with config_context("VersionMap.ReloadInterval", 0):
        # The first schema call resolves version 0
        source._collect_schema()
        first_index = source._preloaded_index
        assert source._to_read_request().as_of is first_index

        # Publish version 1, then deliberately discover the schema again
        newer = original.assign(a=original["a"] + 100)
        lib.write("data", newer)
        source._collect_schema()

        # New requests must use the index from the second schema call
        second_index = source._preloaded_index
        assert second_index is not first_index

        # Build a new request using the index loaded by the second schema call
        request = source._to_read_request()
        assert request.as_of is second_index

        # Schema discovery must not overwrite the original "latest" instruction.
        # A future schema call should still be able to choose a newer version
        assert source.read_request.as_of is None

        # Execute the request directly and check that it reads version 1
        result = lib.read(**request._asdict())
        assert result.version == 1

        assert_frame_equal(result.data, newer)


@pytest.mark.parametrize("operation", ["batch", "concat"])
@pytest.mark.parametrize(
    "preload_first",
    [False, True],
    ids=["first-unresolved", "first-preloaded"],
)
@pytest.mark.parametrize(
    "preload_second",
    [False, True],
    ids=["second-unresolved", "second-preloaded"],
)
def test_multiple_reads_reuse_preloaded_indexes(
    mem_library,
    clear_query_stats,
    operation,
    preload_first,
    preload_second,
):
    """Reuse loaded versions in batch and concat reads."""
    lib = mem_library

    # Separate timestamp ranges make concat's expected row order clear
    first_data = pd.DataFrame(
        {"a": np.arange(3, dtype=np.int64)},
        index=pd.date_range("2020-01-01", periods=3),
    )
    second_data = pd.DataFrame(
        {"a": np.arange(3, 6, dtype=np.int64)},
        index=pd.date_range("2020-01-04", periods=3),
    )
    first_data.index.name = "ts"
    second_data.index.name = "ts"

    lib.write("first", first_data)
    lib.write("second", second_data)

    first = lib.read("first", lazy=True)
    second = lib.read("second", lazy=True)

    # These individual filters must survive batch/concat processing
    first = first[first["a"] != 0]
    second = second[second["a"] != 5]

    if preload_first:
        first._collect_schema()

    if preload_second:
        second._collect_schema()

    first_index = first._preloaded_index
    second_index = second._preloaded_index

    # Publish newer values after optional index loading
    newer_first = first_data.assign(a=first_data["a"] + 100)
    newer_second = second_data.assign(a=second_data["a"] + 100)
    lib.write("first", newer_first)
    lib.write("second", newer_second)

    # Loaded sources keep version 0; unresolved sources select version 1
    selected_first = first_data if preload_first else newer_first
    selected_second = second_data if preload_second else newer_second

    expected_first = selected_first[selected_first["a"] != 0]
    expected_second = selected_second[selected_second["a"] != 5]

    if operation == "batch":
        query = LazyDataFrameCollection([first, second])
    else:
        query = concat([first, second])

    # For batch, this filter applies to each source
    # For concat, it applies after combining the sources
    query = query[query["a"] < 105]

    expected_first = expected_first[expected_first["a"] < 105]
    expected_second = expected_second[expected_second["a"] < 105]

    with config_context("VersionMap.ReloadInterval", 0):
        # Repeat the read to check that loaded indexes remain reusable
        for _ in range(2):
            qs.reset_stats()

            with qs.query_stats():
                result = query.collect()
                stats = qs.get_query_stats()

            if operation == "batch":
                # Batch results must stay in input order
                assert len(result) == 2
                assert result[0].version == (0 if preload_first else 1)
                assert result[1].version == (0 if preload_second else 1)

                # Check values, timestamps and types.
                # ArcticDB does not preserve pandas' index-frequency annotation
                assert_frame_equal(result[0].data, expected_first, check_freq=False)
                assert_frame_equal(result[1].data, expected_second, check_freq=False)

            else:
                # Concat returns one combined frame
                expected = pd.concat([expected_first, expected_second])
                # Keep exact timestamp/value checks, but ignore index-frequency metadata
                assert_frame_equal(result.data, expected, check_freq=False)

            # Each input table occupies one data segment
            assert query_stats_operation_count(stats, "Memory_GetObject", "TABLE_DATA") == 2

            # Only unresolved inputs should fetch an index
            expected_index_reads = int(not preload_first) + int(not preload_second)
            assert query_stats_operation_count(stats, "Memory_GetObject", "TABLE_INDEX") == expected_index_reads

    # Collection must not replace the sources' saved indexes
    assert first._preloaded_index is first_index
    assert second._preloaded_index is second_index

    # Keep the original latest instructions for future schema discovery
    assert first.read_request.as_of is None
    assert second.read_request.as_of is None


def test_split_preserves_selector_and_preloaded_index(mem_library):
    """Keep the original version setting, loaded index and combined query."""
    lib = mem_library

    # Write version 0
    original = pd.DataFrame({"a": np.arange(6, dtype=np.int64)})
    lib.write("data", original)

    # Schema discovery now resolves latest to version 0 and loads its index
    source = lib.read("data", lazy=True)
    source._collect_schema()
    loaded_index = source._preloaded_index
    assert loaded_index is not None

    # Put the source in a collection and add a shared filter
    collection = LazyDataFrameCollection([source])
    collection = collection[collection["a"] > 1]

    # split() creates a new lazy source containing that filter
    split_source = collection.split()[0]

    # Keep "latest" as its original instruction.
    # Do not store the preloaded-index object in read_request.as_of
    assert split_source.read_request.as_of is None

    # Carry the loaded index separately so data reads can reuse it
    assert split_source._preloaded_index is loaded_index

    # Publish version 1
    newer = original.assign(a=original["a"] + 100)
    lib.write("data", newer)

    with config_context("VersionMap.ReloadInterval", 0):
        # The split source must still read version 0 using its saved index
        result = split_source.collect()
        assert result.version == 0

        # The collection's filter must also survive split()
        expected = original[original["a"] > 1].reset_index(drop=True)
        assert_frame_equal(result.data, expected)

        # A new schema call must still be able to resolve latest normally
        split_source._collect_schema()
        result = split_source.collect()

        assert result.version == 1
        assert_frame_equal(result.data, newer)

    # Refreshing the split source must not replace the original source's index
    assert source._preloaded_index is loaded_index
    assert source.read_request.as_of is None
