"""
Copyright 2026 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

import numpy as np
import pandas as pd
import polars as pl
import pytest

from arcticdb import QueryBuilder
from arcticdb.exceptions import ArcticNativeException, UserInputException
import arcticdb.toolbox.query_stats as qs
from arcticdb.util.test import config_context, query_stats_operation_count


@pytest.mark.parametrize("prefetch_rows", [None, 0, 20])
def test_arrow_streaming_reads_basic(in_memory_store_factory, prefetch_rows):
    lib = in_memory_store_factory(segment_row_size=10)
    sym = "test_arrow_streaming_reads_basic"
    df = pl.from_pandas(pd.DataFrame({"col": np.arange(100)}))
    metadata = {"hello": "world"}
    lib.write(sym, df, metadata=metadata)
    gen = lib.read_streaming(sym, prefetch_rows=prefetch_rows)
    for idx, df_slice in enumerate(gen):
        assert df_slice.equals(df.slice(idx * 10, 10))
    vit = lib.read(sym)
    vit.data = None
    assert vit == gen.versioned_item()


def test_arrow_streaming_reads_col_slicing(in_memory_store_factory):
    lib = in_memory_store_factory(segment_row_size=10, column_group_size=2)
    sym = "test_arrow_streaming_reads_col_slicing"
    df = pl.from_pandas(pd.DataFrame({f"col_{i}": np.arange(i * 20, (i + 1) * 20) for i in range(5)}))
    lib.write(sym, df)
    gen = lib.read_streaming(sym)
    for idx, df_slice in enumerate(gen):
        assert df_slice.equals(df.slice(idx * 10, 10))
    vit = lib.read(sym)
    vit.data = None
    assert vit == gen.versioned_item()


def test_arrow_streaming_reads_with_row_range(in_memory_store_factory):
    lib = in_memory_store_factory(segment_row_size=10)
    sym = "test_arrow_streaming_reads_with_row_range"
    df = pl.from_pandas(pd.DataFrame({"col": np.arange(100)}))
    lib.write(sym, df)
    # Omits the first and last chunk, and truncates the second and penultimate chunks
    gen = lib.read_streaming(sym, row_range=(15, 85))
    for idx, df_slice in enumerate(gen):
        if idx == 0:
            assert df_slice.equals(df.slice(15, 5))
        elif idx == 7:
            assert df_slice.equals(df.slice(80, 5))
        else:
            assert df_slice.equals(df.slice((idx + 1) * 10, 10))


def test_arrow_streaming_reads_with_row_range_exact_boundaries(in_memory_store_factory):
    lib = in_memory_store_factory(segment_row_size=10)
    sym = "test_arrow_streaming_reads_with_row_range"
    df = pl.from_pandas(pd.DataFrame({"col": np.arange(100)}))
    lib.write(sym, df)
    # Omits the first and last chunk exactly
    gen = lib.read_streaming(sym, row_range=(10, 90))
    for idx, df_slice in enumerate(gen):
        assert df_slice.equals(df.slice((idx + 1) * 10, 10))


def test_arrow_streaming_reads_with_open_ended_row_range(in_memory_store_factory):
    lib = in_memory_store_factory(segment_row_size=10)
    sym = "test_arrow_streaming_reads_with_open_ended_row_range"
    df = pl.from_pandas(pd.DataFrame({"col": np.arange(100)}))
    lib.write(sym, df)
    gen = lib.read_streaming(sym, row_range=(None, None))
    for idx, df_slice in enumerate(gen):
        assert df_slice.equals(df.slice(idx * 10, 10))


@pytest.mark.parametrize("row_range", [(None, 15), (None, -85), (85, None), (-15, None)])
def test_arrow_streaming_reads_with_funky_row_ranges(in_memory_store_factory, row_range):
    lib = in_memory_store_factory(segment_row_size=10)
    sym = "test_arrow_streaming_reads_with_funky_row_ranges"
    df = pl.from_pandas(pd.DataFrame({"col": np.arange(100)}))
    lib.write(sym, df)
    gen = lib.read_streaming(sym, row_range=row_range)
    for idx, df_slice in enumerate(gen):
        if idx == 0:
            assert df_slice.equals(df.slice(0, 10) if row_range[0] is None else df.slice(85, 5))
        else:  # idx == 1
            assert df_slice.equals(df.slice(10, 5) if row_range[0] is None else df.slice(90, 10))


def test_arrow_streaming_reads_with_row_range_and_filter(in_memory_store_factory):
    lib = in_memory_store_factory(segment_row_size=10)
    sym = "test_arrow_streaming_reads_with_row_range_and_filter"
    df = pl.from_pandas(pd.DataFrame({"col": np.arange(100)}))
    lib.write(sym, df)
    # row_range the first and last chunk, and truncates the second and penultimate chunks
    # query then filters down further
    q = QueryBuilder()
    q = q[q["col"] < 28]
    gen = lib.read_streaming(sym, row_range=(15, 85), query_builder=q)
    for idx, df_slice in enumerate(gen):
        if idx == 0:
            assert df_slice.equals(df.slice(15, 5))
        elif idx == 1:
            assert df_slice.equals(df.slice(20, 8))
        else:
            assert not len(df_slice)


def test_arrow_streaming_reads_with_date_range(in_memory_store_factory):
    lib = in_memory_store_factory(segment_row_size=10)
    sym = "test_arrow_streaming_reads_with_date_range"
    # Index last value is 2026-04-10
    df = pl.from_pandas(
        pd.DataFrame({"col": np.arange(100)}, index=pd.date_range("2026-01-01", periods=100, name="ts")),
        include_index=True,
    )
    lib.write(sym, df, index_column=True)
    # Omits the first and last chunk, and truncates the second and penultimate chunks
    gen = lib.read_streaming(sym, date_range=(pd.Timestamp("2026-01-16"), pd.Timestamp("2026-03-26")))
    for idx, df_slice in enumerate(gen):
        if idx == 0:
            assert df_slice.equals(df.slice(15, 5))
        elif idx == 7:
            assert df_slice.equals(df.slice(80, 5))
        else:
            assert df_slice.equals(df.slice((idx + 1) * 10, 10))


def test_arrow_streaming_reads_with_filter(in_memory_store_factory):
    lib = in_memory_store_factory(segment_row_size=10)
    sym = "test_arrow_streaming_reads_with_filter"
    df = pl.from_pandas(pd.DataFrame({"col": np.arange(100)}))
    lib.write(sym, df)
    q = QueryBuilder()
    q = q[q["col"] < 25]
    gen = lib.read_streaming(sym, query_builder=q)
    for idx, df_slice in enumerate(gen):
        if idx < 2:
            assert df_slice.equals(df.slice(idx * 10, 10))
        elif idx == 2:
            assert df_slice.equals(df.slice(20, 5))
        else:
            assert not len(df_slice)
    vit = lib.read(sym)
    vit.data = None
    assert vit == gen.versioned_item()


def test_arrow_streaming_reads_with_projection(in_memory_store_factory):
    lib = in_memory_store_factory(segment_row_size=10)
    sym = "test_arrow_streaming_reads_with_projection"
    df = pl.from_pandas(pd.DataFrame({"col": np.arange(100)}))
    lib.write(sym, df)
    q = QueryBuilder()
    q = q.apply("new_col", q["col"] * 2)
    gen = lib.read_streaming(sym, query_builder=q)
    expected_df = pl.from_pandas(pd.DataFrame({"col": np.arange(100), "new_col": np.arange(0, 200, 2)}))
    for idx, df_slice in enumerate(gen):
        assert df_slice.equals(expected_df.slice(idx * 10, 10))


@pytest.mark.parametrize("call_versioned_item_before_loop", [True, False])
@pytest.mark.parametrize("prefetch_rows", [None, 0, 7, 13, 20])
def test_arrow_streaming_reads_io_counts(
    in_memory_store_factory, clear_query_stats, call_versioned_item_before_loop, prefetch_rows
):
    # Forces version map reload when reading, otherwise cached info is used
    with config_context("VersionMap.ReloadInterval", 0):
        lib = in_memory_store_factory(segment_row_size=10)
        sym = "test_arrow_streaming_reads_io_counts"
        df = pl.from_pandas(pd.DataFrame({"col": np.arange(100)}))
        lib.write(sym, df)
        with qs.query_stats():
            gen = lib.read_streaming(sym, prefetch_rows=prefetch_rows)
            stats = qs.get_query_stats()
        # No IO yet
        assert not len(stats)
        qs.reset_stats()
        if call_versioned_item_before_loop:
            with qs.query_stats():
                gen.versioned_item()
                stats = qs.get_query_stats()
            assert query_stats_operation_count(stats, "Memory_GetObject", "VERSION_REF") == 1
            # Index key is in version ref so no version chain traversal
            assert query_stats_operation_count(stats, "Memory_GetObject", "VERSION") == 0
            assert query_stats_operation_count(stats, "Memory_GetObject", "TABLE_INDEX") == 1
            # No data keys read yet
            assert query_stats_operation_count(stats, "Memory_GetObject", "TABLE_DATA") == 0
            qs.reset_stats()
        with qs.query_stats():
            for idx, df_slice in enumerate(gen):
                assert df_slice.equals(df.slice(idx * 10, 10))
            stats = qs.get_query_stats()
        # Version ref and index only read if we have not already called versioned_item
        assert (
            query_stats_operation_count(stats, "Memory_GetObject", "VERSION_REF") == 0
            if call_versioned_item_before_loop
            else 1
        )
        assert query_stats_operation_count(stats, "Memory_GetObject", "VERSION") == 0
        assert (
            query_stats_operation_count(stats, "Memory_GetObject", "TABLE_INDEX") == 0
            if call_versioned_item_before_loop
            else 1
        )
        # Each data key read exactly once, regardless of the prefetch_rows parameter
        assert query_stats_operation_count(stats, "Memory_GetObject", "TABLE_DATA") == 10
        qs.reset_stats()
        # Index key already loaded, so calling versioned_item() now does no IO
        with qs.query_stats():
            gen.versioned_item()
            stats = qs.get_query_stats()
        assert query_stats_operation_count(stats, "Memory_GetObject", "VERSION_REF") == 0
        assert query_stats_operation_count(stats, "Memory_GetObject", "VERSION") == 0
        assert query_stats_operation_count(stats, "Memory_GetObject", "TABLE_INDEX") == 0
        assert query_stats_operation_count(stats, "Memory_GetObject", "TABLE_DATA") == 0


@pytest.mark.parametrize("prefetch_rows", [-5, "not a number"])
def test_arrow_streaming_reads_invalid_prefetch_rows(in_memory_version_store, prefetch_rows):
    lib = in_memory_version_store
    with pytest.raises(ArcticNativeException if isinstance(prefetch_rows, int) else TypeError):
        lib.read_streaming("sym", prefetch_rows=prefetch_rows)


def test_arrow_streaming_reads_with_invalid_clauses(in_memory_store_factory):
    lib = in_memory_store_factory(segment_row_size=10)
    q = QueryBuilder().groupby("group").agg({"agg": "sum"})
    with pytest.raises(UserInputException):
        lib.read_streaming("sym", query_builder=q)
    q = QueryBuilder().resample("1s").agg({"agg": "sum"})
    with pytest.raises(UserInputException):
        lib.read_streaming("sym", query_builder=q)
