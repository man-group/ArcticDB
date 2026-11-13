"""
Copyright 2026 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

import numpy as np
import pandas as pd
import pytest

from arcticdb import LibraryOptions, QueryBuilder
from arcticdb.util.test import assert_frame_equal

# A segment's end_index is stored as its last index value + 1, which wraps to INT64_MIN when the last value is INT64_MAX
MAX = np.iinfo(np.int64).max


def _index(values):
    return pd.DatetimeIndex(np.array(values, dtype=np.int64).view("datetime64[ns]"))


def _near_max_df(num_rows):
    return pd.DataFrame(
        {"x": np.arange(num_rows, dtype=np.int64)}, index=_index(MAX - np.arange(num_rows - 1, -1, -1, dtype=np.int64))
    )


@pytest.fixture(params=[False, True], ids=["static", "dynamic"])
def dynamic_schema(request):
    return request.param


@pytest.fixture(params=[5, 100], ids=["many_segments", "one_segment"])
def rows_per_segment(request):
    return request.param


@pytest.fixture
def lib(lmdb_library_factory, dynamic_schema, rows_per_segment):
    return lmdb_library_factory(LibraryOptions(dynamic_schema=dynamic_schema, rows_per_segment=rows_per_segment))


def test_date_range_read_of_index_all_at_max(lib):
    df = pd.DataFrame({"x": np.arange(10, dtype=np.int64)}, index=_index([MAX] * 10))
    lib.write("sym", df)
    result = lib.read("sym", date_range=(pd.Timestamp(MAX), pd.Timestamp(MAX))).data
    assert_frame_equal(result, df)


@pytest.mark.parametrize(
    "date_range",
    [
        (MAX, MAX),
        (MAX - 1, MAX),
        (MAX - 3, None),
        (MAX - 11, MAX),
        (MAX - 6, MAX - 1),
        (None, MAX),
        (None, MAX - 2),
    ],
)
def test_date_range_read_near_max(lib, date_range):
    df = _near_max_df(12)
    lib.write("sym", df)
    start, end = (None if v is None else pd.Timestamp(v) for v in date_range)
    expected = df.loc[start:end]

    assert_frame_equal(lib.read("sym", date_range=(start, end)).data, expected)

    q = QueryBuilder().date_range((start, end))
    assert_frame_equal(lib.read("sym", query_builder=q).data, expected)


def test_get_description_date_range_ending_at_max(lib):
    lib.write("sym", _near_max_df(12))
    start, end = lib.get_description("sym").date_range
    assert start.value == MAX - 11
    assert end.value == MAX


@pytest.mark.parametrize("update_index", [[MAX], [MAX - 1, MAX], [MAX - 7, MAX - 2]])
def test_update_near_max(lib, update_index):
    df = _near_max_df(12)
    lib.write("sym", df)
    update = pd.DataFrame({"x": np.full(len(update_index), 100, dtype=np.int64)}, index=_index(update_index))
    lib.update("sym", update)
    ts = df.index.asi8
    expected = pd.concat([df[ts < update_index[0]], update, df[ts > update_index[-1]]])
    assert_frame_equal(lib.read("sym").data, expected)


def test_append_after_segment_ending_at_max(lib):
    df = _near_max_df(12)
    lib.write("sym", df.iloc[:6])
    lib.append("sym", df.iloc[6:])
    lib.append("sym", df.iloc[-1:])
    expected = pd.concat([df, df.iloc[-1:]])
    assert_frame_equal(lib.read("sym").data, expected)
    assert_frame_equal(lib.read("sym", date_range=(pd.Timestamp(MAX - 1), None)).data, expected.iloc[-3:])
