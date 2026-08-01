"""
Copyright 2026 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

"""Regression coverage for unexpected memory retention on ranged reads (#2348).

``lib.read(..., date_range=...)`` used to return numpy views onto full intersecting
segment buffers. Repeated ranged reads in a loop therefore retained entire segments
and could OOM. Ranges now go through the processing pipeline (C++ truncate) by
default, and any remaining Python post-filter path copies sliced columns.
"""

import gc

import numpy as np
import pandas as pd
import pytest

from arcticdb import QueryBuilder
from arcticdb.util.test import assert_frame_equal


def _wide_frame(n_rows: int, n_cols: int = 50) -> pd.DataFrame:
    index = pd.date_range("2020-01-01", periods=n_rows, freq="h")
    data = {f"c{i}": np.arange(n_rows, dtype=np.float64) + i for i in range(n_cols)}
    return pd.DataFrame(data, index=index)


def test_get_read_query_routes_date_range_through_processing_pipeline(lmdb_version_store):
    lib = lmdb_version_store
    read_query = lib._get_read_query(
        date_range=(pd.Timestamp("2020-01-01"), pd.Timestamp("2020-01-02")),
        row_range=None,
        columns=None,
        query_builder=None,
    )
    # Processing clauses disable Python post-filtering; C++ truncates segments.
    assert read_query.needs_post_processing is False
    assert read_query.row_filter is not None


def test_get_read_query_routes_row_range_through_processing_pipeline(lmdb_version_store):
    lib = lmdb_version_store
    read_query = lib._get_read_query(
        date_range=None,
        row_range=(10, 20),
        columns=None,
        query_builder=None,
    )
    assert read_query.needs_post_processing is False


def test_legacy_force_ranges_false_still_needs_post_processing(lmdb_version_store):
    lib = lmdb_version_store
    read_query = lib._get_read_query(
        date_range=(pd.Timestamp("2020-01-01"), pd.Timestamp("2020-01-02")),
        row_range=None,
        columns=None,
        query_builder=None,
        force_ranges_to_queries=False,
    )
    assert read_query.needs_post_processing is True
    assert read_query.row_filter is not None


def test_date_range_read_matches_query_builder(lmdb_version_store):
    lib = lmdb_version_store
    sym = "date_range_memory_correctness"
    df = _wide_frame(200)
    lib.write(sym, df)

    start, end = pd.Timestamp("2020-01-03"), pd.Timestamp("2020-01-05")
    via_arg = lib.read(sym, date_range=(start, end)).data
    via_qb = lib.read(sym, query_builder=QueryBuilder().date_range((start, end))).data
    expected = df.loc[start:end]

    assert_frame_equal(via_arg, expected)
    assert_frame_equal(via_qb, expected)
    assert_frame_equal(via_arg, via_qb)


def test_row_range_read_matches_iloc(lmdb_version_store):
    lib = lmdb_version_store
    sym = "row_range_memory_correctness"
    df = _wide_frame(150)
    lib.write(sym, df)

    via_arg = lib.read(sym, row_range=(25, 40)).data
    assert_frame_equal(via_arg, df.iloc[25:40])


def test_batch_read_date_ranges_match_single_read(lmdb_version_store):
    lib = lmdb_version_store
    symbols = ["batch_mem_a", "batch_mem_b"]
    frames = [_wide_frame(120), _wide_frame(120)]
    for sym, df in zip(symbols, frames):
        lib.write(sym, df)

    start, end = pd.Timestamp("2020-01-02"), pd.Timestamp("2020-01-04")
    date_ranges = [(start, end), (start, end)]
    batch = lib.read_batch(symbols, date_ranges=date_ranges)
    for i, sym in enumerate(symbols):
        assert_frame_equal(batch[i].data, lib.read(sym, date_range=(start, end)).data)
        assert_frame_equal(batch[i].data, frames[i].loc[start:end])


def test_repeated_date_range_reads_do_not_retain_segment_views(lmdb_version_store_tiny_segment):
    """Repeated small date_range reads must not keep growing retained array bases.

    With tiny segments, a short date_range still intersects whole segments. The
    returned columns must be owned copies (or truncated buffers), not views onto
    a growing set of parent segment allocations.
    """
    lib = lmdb_version_store_tiny_segment
    sym = "date_range_memory_loop"
    # 40 rows with segment_row_size=2 => many segments; request 2 hours at a time.
    df = _wide_frame(40, n_cols=20)
    lib.write(sym, df)

    retained = []
    for i in range(15):
        start = pd.Timestamp("2020-01-01") + pd.Timedelta(hours=i)
        end = start + pd.Timedelta(hours=1)
        out = lib.read(sym, date_range=(start, end)).data
        retained.append(out)
        # Column values should be contiguous owned arrays, not views of a larger buffer.
        for col in out.columns:
            values = out[col].to_numpy()
            # Either no base (owned) or base is not dramatically larger than the slice.
            if values.base is not None and isinstance(values.base, np.ndarray):
                assert values.base.size <= values.size * 4

    gc.collect()
    # Sanity: we actually retained the small frames.
    assert sum(len(frame) for frame in retained) > 0


def test_legacy_python_post_filter_copies_column_buffers(lmdb_version_store):
    """Safety net: if the legacy post-filter path is used, sliced columns are copied."""
    lib = lmdb_version_store
    sym = "legacy_post_filter_copy"
    df = _wide_frame(100, n_cols=5)
    lib.write(sym, df)

    version_query, read_options, read_query, output_format = lib._get_queries(
        as_of=None,
        date_range=(pd.Timestamp("2020-01-02"), pd.Timestamp("2020-01-03")),
        row_range=None,
        columns=None,
        query_builder=None,
    )
    # Rebuild with legacy zero-copy routing disabled so post-processing runs.
    read_query = lib._get_read_query(
        date_range=(pd.Timestamp("2020-01-02"), pd.Timestamp("2020-01-03")),
        row_range=None,
        columns=None,
        query_builder=None,
        force_ranges_to_queries=False,
    )
    assert read_query.needs_post_processing is True

    read_result = lib._read_dataframe(sym, version_query, read_query, read_options)
    # Before post-process the frame still holds full intersecting segments.
    loaded_rows = read_result.frame_data.row_count
    assert loaded_rows > 0

    vitem = lib._post_process_dataframe(read_result, read_query, read_options, output_format)
    for col in vitem.data.columns:
        values = vitem.data[col].to_numpy()
        # Copied slices must not share storage with a larger parent segment buffer.
        assert values.base is None or (
            isinstance(values.base, np.ndarray) and values.base.size <= values.size * 2
        )
