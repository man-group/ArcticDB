"""
Copyright 2026 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

"""
Interop between pandas and arrow, structured in three groups:

1. Write pandas, read arrow   -- pandas is written normally, then read with output_format=pyarrow.
2. Write arrow, read pandas   -- an arrow object is written, then read back as pandas.
3. Combine pandas and arrow     -- combine the two formats via append, update, or concat.
"""

from typing import Union

import numpy as np
import pandas as pd
import polars as pl
import pyarrow as pa
import pytest

from arcticdb import concat
from arcticdb.exceptions import ArcticException, InternalException, NormalizationException, SchemaException
from arcticdb.util.test import assert_frame_equal, assert_series_equal, assert_frame_equal_with_arrow


def _dt(periods, tz=None, start="2025-01-01"):
    return pd.date_range(start, periods=periods, tz=tz)


def _ts_array(dt_index):
    tz = str(dt_index.tz) if dt_index.tz is not None else None
    return pa.Array.from_pandas(dt_index, type=pa.timestamp("ns", tz))


def _indexed_arrow_table(index_name, values, tz=None, start="2025-01-01"):
    """Arrow table whose first column is a (optionally tz-aware) timestamp index."""
    index = _dt(len(values), tz=tz, start=start)
    return pa.table({index_name: _ts_array(index), "col": pa.array(values, pa.int64())})


def _pandas_ts(values, start="2025-01-01", tz=None, name="ts"):
    """pandas DataFrame with a named (optionally tz-aware) DatetimeIndex and a single int column."""
    df = pd.DataFrame({"col": np.array(values, dtype=np.int64)}, index=_dt(len(values), tz=tz, start=start))
    df.index.name = name
    return df


# ===========================================================================
# Section 1: Write pandas, read arrow
# ===========================================================================


def test_write_pandas_rangeindex_read_arrow(in_memory_version_store_arrow):
    """A default RangeIndex is not materialised as an arrow column; it is carried in the arrow
    schema's pandas_metadata, so round-tripping through to_pandas restores the RangeIndex."""
    lib = in_memory_version_store_arrow
    sym = "test_write_pandas_rangeindex_read_arrow"
    df = pd.DataFrame({"col0": np.arange(3, dtype=np.int64), "col1": ["a", "bb", "ccc"]})
    lib.write(sym, df)

    received = lib.read(sym).data
    assert isinstance(received, pa.Table)
    assert received.column_names == ["col0", "col1"]
    assert_frame_equal_with_arrow(received, df)


def test_write_pandas_named_datetime_index_read_arrow(in_memory_version_store_arrow):
    """A named DatetimeIndex is materialised as the first arrow column under its name."""
    lib = in_memory_version_store_arrow
    sym = "test_write_pandas_named_datetime_index_read_arrow"
    df = pd.DataFrame({"col": np.arange(3, dtype=np.int64)}, index=_dt(3))
    df.index.name = "ts"
    lib.write(sym, df)

    received = lib.read(sym).data
    assert received.column_names == ["ts", "col"]
    assert_frame_equal_with_arrow(received, df)


def test_write_pandas_unnamed_index_read_arrow(in_memory_version_store_arrow):
    """An unnamed physically stored index is written under the synthetic arrow column name
    ``__index__``; the pandas_metadata records that the index was unnamed, so to_pandas restores
    a None name."""
    lib = in_memory_version_store_arrow
    sym = "test_write_pandas_unnamed_index_read_arrow"
    df = pd.DataFrame({"col": np.arange(3, dtype=np.int64)}, index=_dt(3))
    assert df.index.name is None
    lib.write(sym, df)

    received = lib.read(sym).data
    assert received.column_names == ["__index__", "col"]
    assert_frame_equal_with_arrow(received, df)


def test_write_pandas_int_columns_read_arrow(in_memory_version_store_arrow):
    """Integer column names become string arrow column names; pandas_metadata records the int-ness
    so to_pandas restores integer labels."""
    lib = in_memory_version_store_arrow
    sym = "test_write_pandas_int_columns_read_arrow"
    df = pd.DataFrame({0: np.arange(2, dtype=np.int64), 1: np.arange(2, dtype=np.int64)})
    lib.write(sym, df)

    received = lib.read(sym).data
    assert received.column_names == ["0", "1"]
    assert_frame_equal_with_arrow(received, df)


def test_write_pandas_synthetic_columns_read_arrow(in_memory_version_store_arrow):
    """A frame with no column labels (RangeIndex columns) is materialised as arrow columns "0".."n"."""
    lib = in_memory_version_store_arrow
    sym = "test_write_pandas_synthetic_columns_read_arrow"
    df = pd.DataFrame([[1, 2], [3, 4]]).astype(np.int64)
    assert isinstance(df.columns, pd.RangeIndex)
    lib.write(sym, df)

    received = lib.read(sym).data
    assert received.column_names == ["0", "1"]
    # arrow.to_pandas does not convert to int column names correctly when `has_synthetic_columns=True`.
    assert_frame_equal_with_arrow(received, df.rename(columns=str))


def test_write_pandas_index_timezone_read_arrow(in_memory_version_store_arrow):
    """A tz-aware DatetimeIndex produces a tz-aware arrow timestamp index column."""
    lib = in_memory_version_store_arrow
    sym = "test_write_pandas_index_timezone_read_arrow"
    df = pd.DataFrame({"col": np.arange(3, dtype=np.int64)}, index=_dt(3, tz="America/New_York"))
    df.index.name = "ts"
    lib.write(sym, df)

    received = lib.read(sym).data
    assert pa.types.is_timestamp(received.schema.field("ts").type)
    assert received.schema.field("ts").type.tz == "America/New_York"
    assert_frame_equal_with_arrow(received, df)


def test_write_pandas_series_datetime_index_read_arrow(in_memory_version_store_arrow):
    """A Series with a physically stored index reads back as a pa.Table (index + value column)."""
    lib = in_memory_version_store_arrow
    sym = "test_write_pandas_series_datetime_index_read_arrow"
    series = pd.Series(np.arange(3, dtype=np.int64), index=_dt(3), name="values")
    series.index.name = "ts"
    lib.write(sym, series)

    received = lib.read(sym).data
    assert isinstance(received, pa.Table)
    assert received.column_names == ["ts", "values"]


@pytest.mark.xfail(reason="Series with RangeIndex returns a single-column Table, not a ChunkedArray yet", strict=True)
def test_write_pandas_series_rangeindex_read_arrow(in_memory_version_store_arrow):
    """A Series with a RangeIndex has no physical index column, so it reads back as a
    pa.ChunkedArray or pl.Series."""
    lib = in_memory_version_store_arrow
    sym = "test_write_pandas_series_rangeindex_read_arrow"
    series = pd.Series(np.arange(3, dtype=np.int64), name="values")
    lib.write(sym, series)

    received = lib.read(sym).data
    assert isinstance(received, pa.ChunkedArray)
    assert received.to_pylist() == [0, 1, 2]

    received = lib.read(sym, output_format="polars").data
    assert isinstance(received, pl.Series)
    assert received.to_list() == [0, 1, 2]


def test_write_pandas_ignores_index_column_kwarg(in_memory_version_store_arrow):
    """``index_column`` is an arrow-write concept; when the input is pandas it is ignored"""
    lib = in_memory_version_store_arrow
    sym = "test_write_pandas_ignores_index_column_kwarg"
    df = pd.DataFrame({"col": np.arange(2, dtype=np.int64)})
    lib.write(sym, df, index_column=True)
    assert_frame_equal(df, lib.read(sym, output_format="pandas").data)

    df_indexed = pd.DataFrame({"col": np.arange(2, dtype=np.int64)}, index=_dt(2))
    df_indexed.index.name = "ts"
    lib.write(sym, df_indexed, index_column=False)
    assert_frame_equal(df_indexed, lib.read(sym, output_format="pandas").data)


def test_write_pandas_multiindex_read_arrow(in_memory_version_store_arrow):
    """A MultiIndex frame materialises each index level as an arrow column."""
    lib = in_memory_version_store_arrow
    sym = "test_write_pandas_multiindex_read_arrow"
    index = pd.MultiIndex.from_arrays([_dt(3), ["a", "b", "c"]], names=["ts", "grp"])
    df = pd.DataFrame({"col": np.arange(3, dtype=np.int64)}, index=index)
    lib.write(sym, df)

    received = lib.read(sym).data
    assert received.column_names[:2] == ["ts", "grp"]
    assert_frame_equal_with_arrow(received, df)


def test_write_pandas_unnamed_multiindex_read_arrow(in_memory_version_store_arrow):
    """Unnamed MultiIndex levels are materialised under synthetic ``__index_level_N__`` names."""
    lib = in_memory_version_store_arrow
    sym = "test_write_pandas_unnamed_multiindex_read_arrow"
    index = pd.MultiIndex.from_arrays([_dt(3), ["a", "b", "c"]])
    assert index.names == [None, None]
    df = pd.DataFrame({"col": np.arange(3, dtype=np.int64)}, index=index)
    lib.write(sym, df)

    received = lib.read(sym).data
    assert received.column_names == ["__index_level_0__", "__index_level_1__", "col"]
    assert_frame_equal_with_arrow(received, df)


# Tests dealing with column name duplication and invalid arrow names
# Such pandas dataframes initially won't support combining with arrow (but still must be readable)


def test_write_pandas_duplicate_columns_read_arrow(in_memory_version_store_arrow):
    """Duplicate pandas column names are deduplicated with `_` when read as arrow,
    original columnn names are restored on `arrow.to_pandas`."""
    lib = in_memory_version_store_arrow
    sym = "test_write_pandas_duplicate_columns_read_arrow"
    df = pd.DataFrame([[1, 2]], columns=["a", "a"], index=_dt(1))
    df.index.name = "ts"
    lib.write(sym, df)

    received = lib.read(sym).data
    assert received.column_names == ["ts", "a", "_a_"]
    assert_frame_equal_with_arrow(received, df)


def test_write_pandas_index_name_clashes_with_column_read_arrow(in_memory_version_store_arrow):
    """An index whose name equals a column name is deduplcated when read as arrow"""
    lib = in_memory_version_store_arrow
    sym = "test_write_pandas_index_name_clashes_with_column_read_arrow"
    df = pd.DataFrame({"a": np.arange(2, dtype=np.int64)}, index=_dt(2))
    df.index.name = "a"
    lib.write(sym, df)

    received = lib.read(sym).data
    assert received.column_names == ["a", "_a_"]
    assert_frame_equal_with_arrow(received, df)


def test_write_pandas_none_column_read_arrow(in_memory_version_store_arrow):
    """A ``None`` column name is stored under the arrow name "None"."""
    lib = in_memory_version_store_arrow
    sym = "test_write_pandas_none_column_read_arrow"
    df = pd.DataFrame([[1, 2]], columns=[None, "b"], index=_dt(1))
    df.index.name = "ts"
    lib.write(sym, df)

    received = lib.read(sym).data
    assert received.column_names == ["ts", "None", "b"]
    assert_frame_equal_with_arrow(received, df)


def test_write_pandas_empty_column_read_arrow(in_memory_version_store_arrow):
    """An empty-string column name is stored under the empty arrow name"""
    lib = in_memory_version_store_arrow
    sym = "test_write_pandas_empty_column_read_arrow"
    df = pd.DataFrame([[1, 2]], columns=["", "b"], index=_dt(1))
    df.index.name = "ts"
    lib.write(sym, df)

    received = lib.read(sym).data
    assert received.column_names == ["ts", "", "b"]
    assert_frame_equal_with_arrow(received, df)


def test_write_pandas_unnamed_index_clashes_with_column_read_arrow(in_memory_version_store_arrow):
    """When an unnamed index would collide with an existing column called ``__index__``, the index
    is given a further-wrapped ``___index___`` name so it does not clash."""
    lib = in_memory_version_store_arrow
    sym = "test_write_pandas_unnamed_index_clashes_with_column_read_arrow"
    df = pd.DataFrame({"__index__": np.arange(2, dtype=np.int64)}, index=_dt(2))
    assert df.index.name is None
    lib.write(sym, df)

    received = lib.read(sym).data
    assert received.column_names == ["___index___", "__index__"]
    assert_frame_equal_with_arrow(received, df)


def test_write_pandas_unnamed_index_clashes_with_multiple_columns_read_arrow(in_memory_version_store_arrow):
    """The unnamed-index name is wrapped in underscores until it no longer clashes with any column,
    so with both ``__index__`` and ``___index___`` taken the index becomes ``____index____``."""
    lib = in_memory_version_store_arrow
    sym = "test_write_pandas_unnamed_index_clashes_with_multiple_columns_read_arrow"
    df = pd.DataFrame(
        {"__index__": np.arange(2, dtype=np.int64), "___index___": np.arange(2, dtype=np.int64)}, index=_dt(2)
    )
    assert df.index.name is None
    lib.write(sym, df)

    received = lib.read(sym).data
    assert received.column_names == ["____index____", "__index__", "___index___"]
    assert_frame_equal_with_arrow(received, df)


def test_write_pandas_multiindex_duplicates(in_memory_version_store_arrow):
    """Unnamed MultiIndex levels are materialised under synthetic ``__index_level_N__`` names and deduplicated."""
    lib = in_memory_version_store_arrow
    sym = "test_write_pandas_multiindex_duplicates"
    index = pd.MultiIndex.from_arrays([_dt(3), ["a", "b", "c"]])
    index.names = [None, "col"]
    df = pd.DataFrame(
        [[1, 2, 3, 4, 5]],
        columns=["__index_level_0__", "__index_level_0__", "___index_level_0___", "col", "col"],
        index=index,
    )
    lib.write(sym, df)

    received = lib.read(sym).data
    assert received.column_names == [
        "____index_level_0____",
        "col",
        "__index_level_0__",
        "___index_level_0___",
        "_____index_level_0_____",
        "_col_",
        "__col__",
    ]
    assert_frame_equal_with_arrow(received, df)


# ===========================================================================
# Section 2: Write arrow, read pandas
# ===========================================================================


def test_write_arrow_no_index_read_pandas(in_memory_version_store_arrow):
    lib = in_memory_version_store_arrow
    sym = "test_write_arrow_no_index_read_pandas"
    table = pa.table({"col0": pa.array([0, 1], pa.int64()), "col1": pa.array(["a", "bb"], pa.string())})
    lib.write(sym, table)

    received = lib.read(sym, output_format="pandas").data
    assert isinstance(received, pd.DataFrame)
    expected = pd.DataFrame({"col0": np.arange(2, dtype=np.int64), "col1": ["a", "bb"]})
    assert_frame_equal(expected, received)


def test_write_arrow_with_index_read_pandas(in_memory_version_store_arrow):
    lib = in_memory_version_store_arrow
    sym = "test_write_arrow_with_index_read_pandas"
    table = pa.table({"ts": _ts_array(_dt(2)), "col": pa.array([0, 1], pa.int64())})
    lib.write(sym, table, index_column=True)

    received = lib.read(sym, output_format="pandas").data
    expected = pd.DataFrame({"col": np.arange(2, dtype=np.int64)}, index=_dt(2))
    expected.index.name = "ts"
    assert_frame_equal(expected, received)


@pytest.mark.xfail(reason="Arrow index timezone not propagated to pandas norm metadata", strict=True)
def test_write_arrow_index_timezone_read_pandas(in_memory_version_store_arrow):
    """A tz-aware arrow timestamp index column reads back as a tz-aware pandas index."""
    lib = in_memory_version_store_arrow
    sym = "test_write_arrow_index_timezone_read_pandas"
    table = pa.table({"ts": _ts_array(_dt(2, tz="America/New_York")), "col": pa.array([0, 1], pa.int64())})
    lib.write(sym, table, index_column=True)

    received = lib.read(sym, output_format="pandas").data
    expected = pd.DataFrame({"col": np.arange(2, dtype=np.int64)}, index=_dt(2, tz="America/New_York"))
    expected.index.name = "ts"
    assert_frame_equal(expected, received)


@pytest.mark.xfail(reason="Arrow column timezone not propagated to pandas norm metadata", strict=True)
def test_write_arrow_column_timezone_read_pandas(in_memory_version_store_arrow):
    """A tz-aware arrow timestamp *column* (not the index) reads back as a tz-aware pandas column."""
    lib = in_memory_version_store_arrow
    sym = "test_write_arrow_column_timezone_read_pandas"
    table = pa.table(
        {
            "ts": _ts_array(_dt(2)),
            "col": _ts_array(_dt(2, tz="Europe/London", start="2024-06-01")),
        }
    )
    lib.write(sym, table, index_column=True)

    received = lib.read(sym, output_format="pandas").data
    expected = pd.DataFrame({"col": pd.date_range("2024-06-01", periods=2, tz="Europe/London")}, index=_dt(2))
    expected.index.name = "ts"
    assert_frame_equal(expected, received)


def test_write_arrow_strings_read_pandas(in_memory_version_store_arrow):
    """Arrow string, large_string, and dictionary-encoded (categorical) large_string columns all
    read back as pandas object columns."""
    lib = in_memory_version_store_arrow
    sym = "test_write_arrow_strings_read_pandas"
    table = pa.table(
        {
            "small": pa.array(["a", "bb"], pa.string()),
            "large": pa.array(["ccc", "dddd"], pa.large_string()),
            "cat": pa.array(["x", "y"], pa.large_string()).dictionary_encode(),
        }
    )
    lib.write(sym, table)

    received = lib.read(sym, output_format="pandas").data
    expected = pd.DataFrame({"small": ["a", "bb"], "large": ["ccc", "dddd"], "cat": ["x", "y"]})
    assert_frame_equal(expected, received)


# --- writing pyarrow / polars input primitives other than a plain Table -----


@pytest.mark.xfail(reason="pyarrow Array input should read back as a pandas Series, not be pickled", strict=True)
def test_write_arrow_array_read_pandas(in_memory_version_store_arrow):
    lib = in_memory_version_store_arrow
    sym = "test_write_arrow_array_read_pandas"
    lib.write(sym, pa.array([1, 2, 3], pa.int64()))
    received = lib.read(sym, output_format="pandas").data
    assert isinstance(received, pd.Series)
    assert_series_equal(pd.Series(np.arange(1, 4, dtype=np.int64)), received)


@pytest.mark.xfail(reason="pyarrow ChunkedArray input should read back as a pandas Series, not be pickled", strict=True)
def test_write_arrow_chunked_array_read_pandas(in_memory_version_store_arrow):
    lib = in_memory_version_store_arrow
    sym = "test_write_arrow_chunked_array_read_pandas"
    lib.write(sym, pa.chunked_array([[1, 2], [3]], pa.int64()))
    received = lib.read(sym, output_format="pandas").data
    assert isinstance(received, pd.Series)
    assert_series_equal(pd.Series(np.arange(1, 4, dtype=np.int64)), received)


@pytest.mark.xfail(
    reason="pyarrow RecordBatch input should read back as a pandas DataFrame, not be pickled", strict=True
)
def test_write_arrow_record_batch_read_pandas(in_memory_version_store_arrow):
    lib = in_memory_version_store_arrow
    sym = "test_write_arrow_record_batch_read_pandas"
    batch = pa.RecordBatch.from_arrays([pa.array([1, 2], pa.int64())], names=["col"])
    lib.write(sym, batch)
    received = lib.read(sym, output_format="pandas").data
    assert isinstance(received, pd.DataFrame)
    assert_frame_equal(pd.DataFrame({"col": np.array([1, 2], dtype=np.int64)}), received)


@pytest.mark.xfail(reason="polars Series input should read back as a range-indexed pandas Series", strict=True)
def test_write_polars_series_read_pandas(in_memory_version_store_arrow):
    lib = in_memory_version_store_arrow
    sym = "test_write_polars_series_read_pandas"
    lib.write(sym, pl.Series("col", [1, 2, 3]))
    received = lib.read(sym, output_format="pandas").data
    assert isinstance(received, pd.Series)
    assert_series_equal(pd.Series(np.arange(1, 4, dtype=np.int64), name="col"), received)


# ===========================================================================
# Section 3: Combine pandas and arrow (append / update / concat)
# ===========================================================================

ArrowOrPandas = Union[pd.DataFrame, pd.Series, pa.Table, pa.ChunkedArray]


def _maybe_arrow(df: pd.DataFrame, fmt: str) -> ArrowOrPandas:
    """Return ``df`` unchanged for "pandas", or a pyarrow Table for "arrow".

    Any non-range index is moved into leading column(s) so it can be written back with
    ``index_column=True``.
    """
    if fmt == "pandas":
        return df
    if isinstance(df.index, pd.RangeIndex):
        return pa.Table.from_pandas(df, preserve_index=False)
    return pa.Table.from_pandas(df.reset_index(), preserve_index=False)


def _combine(lib, op: str, first: ArrowOrPandas, second: ArrowOrPandas, index_column: bool = False) -> ArrowOrPandas:
    """Write ``first``, combine ``second`` via ``op`` ("append"/"update"/"concat"), return read-back
    data. ``index_column`` applied to all operations (but affects only arrow inputs)
    """
    if op == "concat":
        lib.write("sym0", first, index_column=index_column)
        lib.write("sym1", second, index_column=index_column)
        return concat(lib.read_batch(["sym0", "sym1"], lazy=True)).collect().data
    lib.write("sym", first, index_column=index_column)
    getattr(lib, op)("sym", second, index_column=index_column)
    return lib.read("sym").data


def _index_tz(received: ArrowOrPandas):
    """Return the timezone (as a string) of the index column of a combine result, or None."""
    if isinstance(received, pa.Table):
        typ = received.schema.field(0).type
        return str(typ.tz) if pa.types.is_timestamp(typ) and typ.tz is not None else None
    tz = getattr(received.index, "tz", None)
    return str(tz) if tz is not None else None


FORMATS_ORDER = [("arrow", "pandas"), ("pandas", "arrow")]
_APPEND = pytest.param(
    "append", marks=pytest.mark.xfail(reason="append between arrow and pandas not yet supported", strict=True)
)
_UPDATE = pytest.param(
    "update", marks=pytest.mark.xfail(reason="update between arrow and pandas not yet supported", strict=True)
)
# append/update/concat all produce a row-wise union for disjoint, contiguous timeseries chunks.
INDEXED_OPS = [_APPEND, _UPDATE, "concat"]
# operations that apply without a timeseries index.
UNINDEXED_OPS = [_APPEND, "concat"]


# --- matching schema (row-wise union), both directions --------------------


@pytest.mark.parametrize("first_fmt, second_fmt", FORMATS_ORDER)
@pytest.mark.parametrize("op", INDEXED_OPS)
def test_combine_matching_schema_indexed(arrow_library, op, first_fmt, second_fmt):
    first = _maybe_arrow(_pandas_ts([0, 1]), first_fmt)
    second = _maybe_arrow(_pandas_ts([2, 3], start="2025-01-03"), second_fmt)
    received = _combine(arrow_library, op, first, second, index_column=True)
    assert_frame_equal_with_arrow(received, _pandas_ts([0, 1, 2, 3]))


@pytest.mark.parametrize("first_fmt, second_fmt", FORMATS_ORDER)
@pytest.mark.parametrize("op", UNINDEXED_OPS)
def test_combine_matching_schema_rowcount(arrow_library, op, first_fmt, second_fmt):
    first = _maybe_arrow(pd.DataFrame({"col": [0, 1]}), first_fmt)
    second = _maybe_arrow(pd.DataFrame({"col": [2, 3]}), second_fmt)
    received = _combine(arrow_library, op, first, second)
    assert_frame_equal_with_arrow(received, pd.DataFrame({"col": [0, 1, 2, 3]}))


# --- unnamed pandas index / multiindex inherit the arrow name(s) ----------


@pytest.mark.parametrize("first_fmt, second_fmt", FORMATS_ORDER)
@pytest.mark.parametrize("op", INDEXED_OPS)
@pytest.mark.xfail(reason="unnamed pandas index name should inherit arrow column name on combine", strict=True)
def test_combine_pandas_unnamed_index_inherits_arrow_name(arrow_library_any_schema, op, first_fmt, second_fmt):
    """The arrow side carries the index name "ts"; the pandas side has an unnamed index and should
    inherit it. Expected for both directions and both schemas."""

    def build(fmt, values, start):
        df = _pandas_ts(values, start=start)
        if fmt == "arrow":
            return _maybe_arrow(df, "arrow")
        df.index.name = None
        return df

    first = build(first_fmt, [0, 1], "2025-01-01")
    second = build(second_fmt, [2, 3], "2025-01-03")
    received = _combine(arrow_library_any_schema, op, first, second, index_column=True)
    assert received.column_names == ["ts", "col"]
    assert_frame_equal_with_arrow(received, _pandas_ts([0, 1, 2, 3]))


@pytest.mark.parametrize("first_fmt, second_fmt", FORMATS_ORDER)
@pytest.mark.xfail(reason="unnamed pandas multiindex levels should inherit arrow column names on combine", strict=True)
def test_combine_unnamed_multiindex_inherits_arrow_names(arrow_library_any_schema, first_fmt, second_fmt):
    """A pandas frame with an unnamed MultiIndex inherits both level names from the arrow table."""

    def build(fmt, values, start):
        index = pd.MultiIndex.from_arrays([_dt(len(values), start=start), ["a"] * len(values)], names=["ts", "grp"])
        df = pd.DataFrame({"col": np.array(values, dtype=np.int64)}, index=index)
        if fmt == "arrow":
            return _maybe_arrow(df, "arrow")
        df.index.names = [None, None]
        return df

    first = build(first_fmt, [0, 1], "2025-01-01")
    second = build(second_fmt, [2, 3], "2025-01-03")
    received = _combine(arrow_library_any_schema, "concat", first, second, index_column=True)
    assert received.column_names[:2] == ["ts", "grp"]
    assert received.equals(build("arrow", [0, 1, 2, 3], "2025-01-01"))


# --- integer / synthetic columns ------------------------------------------


@pytest.mark.parametrize("first_fmt, second_fmt", FORMATS_ORDER)
@pytest.mark.parametrize("op", UNINDEXED_OPS)
@pytest.mark.xfail(reason="has_synthetic_columns should be preserved on append/concat", strict=True)
def test_combine_synthetic_columns(arrow_library, op, first_fmt, second_fmt):
    first = _maybe_arrow(pd.DataFrame([[1, 2], [3, 4]]).astype(np.int64), first_fmt)
    second = _maybe_arrow(pd.DataFrame([[5, 6], [7, 8]]).astype(np.int64), second_fmt)
    received = _combine(arrow_library, op, first, second)
    expected = pd.DataFrame([[1, 2], [3, 4], [5, 6], [7, 8]]).astype(np.int64)
    expected.index = pd.RangeIndex(4)
    assert_frame_equal_with_arrow(received, expected)


@pytest.mark.parametrize("first_fmt, second_fmt", FORMATS_ORDER)
@pytest.mark.parametrize("op", UNINDEXED_OPS)
def test_combine_int_columns(arrow_library, op, first_fmt, second_fmt):
    first = _maybe_arrow(pd.DataFrame({0: [1, 2], 1: [3, 4]}).astype(np.int64), first_fmt)
    second = _maybe_arrow(pd.DataFrame({0: [5, 7], 1: [6, 8]}).astype(np.int64), second_fmt)
    received = _combine(arrow_library, op, first, second)
    expected = pd.DataFrame({0: [1, 2, 5, 7], 1: [3, 4, 6, 8]}).astype(np.int64)
    expected.index = pd.RangeIndex(4)
    assert_frame_equal_with_arrow(received, expected)


# --- index timezone handling ----------------------------------------------


@pytest.mark.parametrize("first_fmt, second_fmt", FORMATS_ORDER)
@pytest.mark.parametrize("op", INDEXED_OPS)
def test_combine_index_tz_match(arrow_library, op, first_fmt, second_fmt):
    """Matching timezones are preserved (values and tz) when merging."""
    first = _maybe_arrow(_pandas_ts([0, 1], tz="America/New_York"), first_fmt)
    second = _maybe_arrow(_pandas_ts([2, 3], start="2025-01-03", tz="America/New_York"), second_fmt)
    received = _combine(arrow_library, op, first, second, index_column=True)
    assert_frame_equal_with_arrow(received, _pandas_ts([0, 1, 2, 3], tz="America/New_York"))


@pytest.mark.parametrize("first_fmt, second_fmt", FORMATS_ORDER)
@pytest.mark.parametrize("op", ["append", "update"])
@pytest.mark.xfail(
    reason="tz-mismatch under static schema should raise SchemaException once cross-format combine lands", strict=True
)
def test_combine_index_tz_mismatch_static_raises(arrow_library, op, first_fmt, second_fmt):
    """Static schema: a timezone mismatch on the index must raise SchemaException."""
    first = _maybe_arrow(_pandas_ts([0, 1, 2, 3], tz="America/New_York"), first_fmt)
    second = _maybe_arrow(_pandas_ts([20, 30], start="2025-01-05", tz="Europe/London"), second_fmt)
    with pytest.raises(SchemaException):
        _combine(arrow_library, op, first, second, index_column=True)


@pytest.mark.parametrize("first_fmt, second_fmt", FORMATS_ORDER)
@pytest.mark.parametrize("op", INDEXED_OPS)
@pytest.mark.xfail(reason="tz-mismatch under dynamic schema should return timezone naive results", strict=True)
def test_combine_index_tz_mismatch_clears_tz(arrow_library_dynamic, op, first_fmt, second_fmt):
    """Dynamic schema (append/update) and concat (any schema) are permissive about a timezone
    mismatch and yield a timezone-naive index."""
    first = _maybe_arrow(_pandas_ts([0, 1], tz="America/New_York"), first_fmt)
    second = _maybe_arrow(_pandas_ts([2, 3], start="2025-01-03", tz="Europe/London"), second_fmt)
    received = _combine(arrow_library_dynamic, op, first, second, index_column=True)
    assert _index_tz(received) is None


@pytest.mark.parametrize("first_fmt, second_fmt", FORMATS_ORDER)
@pytest.mark.parametrize("op", ["append", "update"])
@pytest.mark.xfail(
    reason="non-index column tz mismatch handling for pandas<->arrow combine is not implemented yet", strict=True
)
def test_combine_column_tz_mismatch_static_raises(arrow_library, op, first_fmt, second_fmt):
    """Static schema: a non-index column that is tz-aware in arrow but tz-naive in pandas must
    raise."""

    def build(fmt, values, start):
        index = _dt(len(values), start=start)
        if fmt == "arrow":
            return pa.table(
                {"ts": _ts_array(index), "col": _ts_array(_dt(len(values), tz="Europe/London", start="2024-06-01"))}
            )
        df = pd.DataFrame({"col": pd.date_range("2024-06-01", periods=len(values))}, index=index)
        df.index.name = "ts"
        return df

    first = build(first_fmt, [0, 1, 2, 3], "2025-01-01")
    second = build(second_fmt, [20, 30], "2025-01-05")
    with pytest.raises(SchemaException):
        _combine(arrow_library, op, first, second, index_column=True)


# --- series / array combinations -------------------------------------


@pytest.mark.parametrize("op", UNINDEXED_OPS)
@pytest.mark.xfail(reason="merging a Series with a pyarrow ChunkedArray is not supported yet", strict=True)
def test_combine_series_with_chunked_array(arrow_library, op):
    received = _combine(
        arrow_library, op, pd.Series(np.array([0, 1], dtype=np.int64)), pa.chunked_array([[2, 3]], pa.int64())
    )
    assert received.to_pylist() == [0, 1, 2, 3]


@pytest.mark.parametrize("op", UNINDEXED_OPS)
@pytest.mark.xfail(reason="merging a Series with a polars Series is not supported yet", strict=True)
def test_combine_series_with_polars_series(arrow_library, op):
    received = _combine(arrow_library, op, pd.Series(np.array([0, 1], dtype=np.int64)), pl.Series("col", [2, 3]))
    assert received.to_pylist() == [0, 1, 2, 3]


@pytest.mark.parametrize("op", INDEXED_OPS)
def test_combine_series_with_index_and_table(arrow_library, op):
    """An indexed Series (stored as a one-column frame) combines with a matching arrow table."""
    series = pd.Series(np.array([0, 1], dtype=np.int64), index=_dt(2), name="col")
    series.index.name = "ts"
    received = _combine(
        arrow_library, op, series, _indexed_arrow_table("ts", [2, 3], start="2025-01-03"), index_column=True
    )
    assert_frame_equal_with_arrow(received, _pandas_ts([0, 1, 2, 3]))


# --- failure conditions ----------------------------------------------------


def test_combine_pandas_duplicate_columns_raises(arrow_library):
    """Merging arrow with a pandas frame that has duplicate column names must raise."""
    lib = arrow_library
    arrow_tbl = pa.table(
        {"ts": _ts_array(_dt(2)), "col": pa.array([0, 1], pa.int64()), "_col_": pa.array([2, 3], pa.int64())}
    )
    lib.write("sym", arrow_tbl, index_column=True)
    df = pd.DataFrame([[4, 5]], columns=["col", "col"], index=_dt(1, start="2025-01-03"))
    df.index.name = "ts"
    with pytest.raises(ArcticException):
        lib.append("sym", df)


def test_combine_pandas_empty_column_raises(arrow_library):
    """Merging arrow with a pandas frame that has an empty column name must raise"""
    lib = arrow_library
    arrow_tbl = pa.table(
        {"ts": _ts_array(_dt(2)), "empty": pa.array([0, 1], pa.int64()), "col": pa.array([2, 3], pa.int64())}
    )
    lib.write("sym", arrow_tbl, index_column=True)
    df = pd.DataFrame([[4, 5]], columns=["", "col"], index=_dt(1, start="2025-01-03"))
    df.index.name = "ts"
    with pytest.raises(ArcticException):
        lib.append("sym", df)


def test_append_single_index_pandas_with_unindexed_arrow_raises(arrow_library_any_schema):
    """A single-index (timeseries) pandas symbol cannot be appended with an unindexed arrow table."""
    lib = arrow_library_any_schema
    lib.write("sym", _pandas_ts([0, 1]))
    with pytest.raises(ArcticException):
        lib.append("sym", pa.table({"col": pa.array([2, 3], pa.int64())}))


def test_append_multiindex_pandas_with_unindexed_arrow_raises(arrow_library_any_schema):
    """A MultiIndex pandas symbol cannot be appended with an unindexed arrow table."""
    lib = arrow_library_any_schema
    index = pd.MultiIndex.from_arrays([_dt(2), ["a", "b"]], names=["ts", "grp"])
    lib.write("sym", pd.DataFrame({"col": np.array([0, 1], dtype=np.int64)}, index=index))
    with pytest.raises(ArcticException):
        lib.append("sym", pa.table({"col": pa.array([2, 3], pa.int64())}))


def test_append_rowcount_pandas_with_indexed_arrow_raises(arrow_library_any_schema):
    """A rowcount (RangeIndex) pandas symbol cannot be appended with an indexed arrow table."""
    lib = arrow_library_any_schema
    lib.write("sym", pd.DataFrame({"col": np.array([0, 1], dtype=np.int64)}))
    with pytest.raises(ArcticException):
        lib.append("sym", _indexed_arrow_table("ts", [2, 3], start="2025-01-03"), index_column=True)
