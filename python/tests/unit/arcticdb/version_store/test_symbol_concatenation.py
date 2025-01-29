"""
Copyright 2025 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""
import numpy as np
import pandas as pd
import pytest

from arcticdb import col, concat, LazyDataFrame, LazyDataFrameCollection, QueryBuilder, ReadRequest
from arcticdb.exceptions import SchemaException
from arcticdb.options import LibraryOptions
from arcticdb.util.test import assert_frame_equal

pytestmark = pytest.mark.pipeline


@pytest.mark.parametrize("rows_per_segment", [2, 100_000])
@pytest.mark.parametrize("columns_per_segment", [2, 100_000])
@pytest.mark.parametrize("index", [None, pd.date_range("2025-01-01", periods=12)])
def test_symbol_concat_basic(lmdb_library_factory, rows_per_segment, columns_per_segment, index):
    lib = lmdb_library_factory(LibraryOptions(rows_per_segment=rows_per_segment, columns_per_segment=columns_per_segment))
    df_1 = pd.DataFrame(
        {
            "col1": np.arange(3, dtype=np.int64),
            "col2": np.arange(100, 103, dtype=np.int64),
            "col3": np.arange(1000, 1003, dtype=np.int64),
        },
        index=index[:3] if index is not None else None,
    )
    df_2 = pd.DataFrame(
        {
            "col1": np.arange(4, dtype=np.int64),
            "col2": np.arange(200, 204, dtype=np.int64),
            "col3": np.arange(2000, 2004, dtype=np.int64),
        },
        index=index[3:7] if index is not None else None,
    )
    df_3 = pd.DataFrame(
        {
            "col1": np.arange(5, dtype=np.int64),
            "col2": np.arange(300, 305, dtype=np.int64),
            "col3": np.arange(3000, 3005, dtype=np.int64),
        },
        index=index[7:] if index is not None else None,
    )
    lib.write("sym1", df_1)
    lib.write("sym2", df_2)
    lib.write("sym3", df_3)

    received = concat(lib.read_batch(["sym1", "sym2", "sym3"], lazy=True)).collect().data
    expected = pd.concat([df_1, df_2, df_3])
    if index is None:
        expected.index = pd.RangeIndex(len(expected))
    assert_frame_equal(expected, received)


@pytest.mark.parametrize("rows_per_segment", [2, 100_000])
@pytest.mark.parametrize("columns_per_segment", [2, 100_000])
def test_symbol_concat_multiindex(lmdb_library_factory, rows_per_segment, columns_per_segment):
    lib = lmdb_library_factory(LibraryOptions(rows_per_segment=rows_per_segment, columns_per_segment=columns_per_segment))
    df = pd.DataFrame(
        {
            "col1": np.arange(12, dtype=np.int64),
            "col2": np.arange(100, 112, dtype=np.int64),
            "col3": np.arange(1000, 1012, dtype=np.int64),
        },
        index=pd.MultiIndex.from_product([pd.date_range("2025-01-01", periods=4), [0, 1, 2]], names=["datetime", "level"]),
    )
    lib.write("sym1", df[:3])
    lib.write("sym2", df[3:7])
    lib.write("sym3", df[7:])

    received = concat(lib.read_batch(["sym1", "sym2", "sym3"], lazy=True)).collect().data
    assert_frame_equal(df, received)


def test_symbol_concat_with_date_range(lmdb_library):
    lib = lmdb_library
    df_1 = pd.DataFrame(
        {
            "col1": np.arange(3, dtype=np.int64),
        },
        index=pd.date_range(pd.Timestamp(0), freq="1000ns", periods=3),
    )
    df_2 = pd.DataFrame(
        {
            "col1": np.arange(4, dtype=np.int64),
        },
        index=pd.date_range(pd.Timestamp(1000), freq="1000ns", periods=4),
    )
    lib.write("sym1", df_1)
    lib.write("sym2", df_2)

    # Use date_range arg to trim last row from sym1
    lazy_df_1 = lib.read("sym1", date_range=(None, pd.Timestamp(1000)), lazy=True)
    # Use date_range clause to trim first row from sym2
    lazy_df_2 = lib.read("sym2", lazy=True)
    lazy_df_2 = lazy_df_2.date_range((pd.Timestamp(2000), None))

    received = concat([lazy_df_1, lazy_df_2]).collect().data
    expected = pd.concat([df_1[:2], df_2[1:]])
    assert_frame_equal(expected, received)


@pytest.mark.parametrize("rows_per_segment", [2, 100_000])
@pytest.mark.parametrize("columns_per_segment", [2, 100_000])
def test_symbol_concat_complex(lmdb_library_factory, rows_per_segment, columns_per_segment):
    lib = lmdb_library_factory(LibraryOptions(rows_per_segment=rows_per_segment, columns_per_segment=columns_per_segment))
    df_1 = pd.DataFrame(
        {
        "col1": np.arange(3, dtype=np.int64),
        "col2": np.arange(100, 103, dtype=np.int64),
        "col3": np.arange(1000, 1003, dtype=np.int64),
        },
        index=pd.date_range(pd.Timestamp(0), freq="1000ns", periods=3),
    )
    df_2 = pd.DataFrame(
        {
        "col1": np.arange(4, dtype=np.int64),
        "col2": np.arange(200, 204, dtype=np.int64),
        "col3": np.arange(2000, 2004, dtype=np.int64),
        },
        index=pd.date_range(pd.Timestamp(2000), freq="1000ns", periods=4),
    )
    df_3 = pd.DataFrame(
        {
        "col1": np.arange(5, dtype=np.int64),
        "col2": np.arange(300, 305, dtype=np.int64),
        "col3": np.arange(3000, 3005, dtype=np.int64),
        },
        index=pd.date_range(pd.Timestamp(6000), freq="1000ns", periods=5),
    )
    lib.write("sym1", df_1)
    lib.write("sym2", df_2)
    lib.write("sym3", df_3)

    lazy_df_1 = lib.read("sym1", lazy=True)
    lazy_df_2 = lib.read("sym2", lazy=True)
    lazy_df_2 = lazy_df_2.date_range((pd.Timestamp(pd.Timestamp(3000)), None))
    lazy_df_3 = lib.read("sym3", date_range=(None, pd.Timestamp(9000)), lazy=True)

    lazy_df = concat([lazy_df_1, lazy_df_2, lazy_df_3])

    lazy_df.resample("2000ns").agg({"col1": "sum", "col2": "mean", "col3": "min"})

    received = lazy_df.collect().data
    received = received.reindex(columns=sorted(received.columns))
    expected = pd.concat([df_1, df_2[1:], df_3[:4]]).resample("2000ns").agg({"col1": "sum", "col2": "mean", "col3": "min"})
    assert_frame_equal(expected, received)


@pytest.mark.parametrize("index", [None, [pd.Timestamp(0)]])
def test_symbol_concat_symbols_with_different_columns(lmdb_library_factory, index):
    lib = lmdb_library_factory(LibraryOptions(columns_per_segment=2))
    df_1 = pd.DataFrame({"col1": [0], "col3": [0]}, index=index)
    df_2 = pd.DataFrame({"col2": [0], "col3": [0]}, index=index)
    df_3 = pd.DataFrame({"col1": [0], "col4": [0]}, index=index)
    df_4 = pd.DataFrame({"col1": [0], "col3": [0], "col5": [0], "col6": [0]}, index=index)
    df_5 = pd.DataFrame({"col1": [0], "col3": [0], "col5": [0], "col7": [0]}, index=index)
    lib.write("sym1", df_1)
    lib.write("sym2", df_2)
    lib.write("sym3", df_3)
    lib.write("sym4", df_4)
    lib.write("sym5", df_5)

    # First column different
    with pytest.raises(SchemaException):
        concat(lib.read_batch(["sym1", "sym2"], lazy=True)).collect()
    # Second column different
    with pytest.raises(SchemaException):
        concat(lib.read_batch(["sym1", "sym3"], lazy=True)).collect()
    # First row slice with extra column slice
    with pytest.raises(SchemaException):
        concat(lib.read_batch(["sym4", "sym1"], lazy=True)).collect()
    # Second row slice with extra column slice
    with pytest.raises(SchemaException):
        concat(lib.read_batch(["sym1", "sym4"], lazy=True)).collect()
    # Row slices differ only in second column slice
    with pytest.raises(SchemaException):
        concat(lib.read_batch(["sym4", "sym5"], lazy=True)).collect()


def test_symbol_concat_symbols_with_different_indexes(lmdb_library):
    lib = lmdb_library
    df_1 = pd.DataFrame({"col": [0]}, index=pd.RangeIndex(1))
    df_2 = pd.DataFrame({"col": [0]}, index=[pd.Timestamp(0)])
    dt1 = pd.Timestamp(0)
    dt2 = pd.Timestamp(1)
    arr1 = [dt1, dt1, dt2, dt2]
    arr2 = [0, 1, 0, 1]
    df_3 = pd.DataFrame({"col": [0]}, index=pd.MultiIndex.from_arrays([arr1, arr2], names=["datetime", "level"]))

    lib.write("range_index_sym", df_1)
    lib.write("timestamp_index_sym", df_2)
    lib.write("multiindex_sym", df_3)

    with pytest.raises(SchemaException):
        concat(lib.read_batch(["range_index_sym", "timestamp_index_sym"], lazy=True)).collect()

    with pytest.raises(SchemaException):
        concat(lib.read_batch(["timestamp_index_sym", "range_index_sym"], lazy=True)).collect()

    with pytest.raises(SchemaException):
        concat(lib.read_batch(["range_index_sym", "multiindex_sym"], lazy=True)).collect()

    with pytest.raises(SchemaException):
        concat(lib.read_batch(["multiindex_sym", "range_index_sym"], lazy=True)).collect()

    with pytest.raises(SchemaException):
        concat(lib.read_batch(["timestamp_index_sym", "multiindex_sym"], lazy=True)).collect()

    with pytest.raises(SchemaException):
        concat(lib.read_batch(["timestamp_index_sym", "multiindex_sym"], lazy=True)).collect()


def test_symbol_concat_pickled_data(lmdb_library):
    lib = lmdb_library
    df = pd.DataFrame({"bytes": np.arange(10, dtype=np.uint64)})
    pickled_data = {"hi", "there"}
    lib.write("sym1", df)
    lib.write_pickle("sym2", pickled_data)

    with pytest.raises(SchemaException):
        concat(lib.read_batch(["sym1", "sym2"], lazy=True)).collect()
