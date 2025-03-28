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


@pytest.mark.parametrize("dynamic_schema", [True, False])
@pytest.mark.parametrize("rows_per_segment", [2, 100_000])
@pytest.mark.parametrize("columns_per_segment", [2, 100_000])
@pytest.mark.parametrize("index", [None, pd.date_range("2025-01-01", periods=12)])
@pytest.mark.parametrize("join", ["inner", "outer"])
def test_symbol_concat_basic(lmdb_library_factory, dynamic_schema, rows_per_segment, columns_per_segment, index, join):
    lib = lmdb_library_factory(LibraryOptions(dynamic_schema=dynamic_schema, rows_per_segment=rows_per_segment, columns_per_segment=columns_per_segment))
    df_0 = pd.DataFrame(
        {
            "col1": np.arange(3, dtype=np.int64),
            "col2": np.arange(100, 103, dtype=np.int64),
            "col3": np.arange(1000, 1003, dtype=np.int64),
        },
        index=index[:3] if index is not None else None,
    )
    df_1 = pd.DataFrame(
        {
            "col1": np.arange(4, dtype=np.int64),
            "col2": np.arange(200, 204, dtype=np.int64),
            "col3": np.arange(2000, 2004, dtype=np.int64),
        },
        index=index[3:7] if index is not None else None,
    )
    df_2 = pd.DataFrame(
        {
            "col1": np.arange(5, dtype=np.int64),
            "col2": np.arange(300, 305, dtype=np.int64),
            "col3": np.arange(3000, 3005, dtype=np.int64),
        },
        index=index[7:] if index is not None else None,
    )
    lib.write("sym0", df_0, metadata=0)
    lib.write("sym1", df_1)
    lib.write("sym2", df_2, metadata=2)

    received = concat(lib.read_batch(["sym0", "sym1", "sym2"], lazy=True), join).collect()
    expected = pd.concat([df_0, df_1, df_2])
    if index is None:
        expected.index = pd.RangeIndex(len(expected))
    assert_frame_equal(expected, received.data)
    for idx, version in enumerate(received.versions):
        assert version.symbol == f"sym{idx}"
        assert version.version == 0
        assert version.data is None
        assert version.metadata == (None if idx == 1 else idx)


@pytest.mark.parametrize("dynamic_schema", [True, False])
@pytest.mark.parametrize("columns_per_segment", [2, 100_000])
@pytest.mark.parametrize("join", ["inner", "outer"])
def test_symbol_concat_different_column_sets(lmdb_library_factory, dynamic_schema, columns_per_segment, join):
    lib = lmdb_library_factory(LibraryOptions(dynamic_schema=dynamic_schema, columns_per_segment=columns_per_segment))
    # Use floats and strings so that our backfilling and Pandas' match
    df_0 = pd.DataFrame(
        {
            "col1": np.arange(5, dtype=np.float64),
            "col2": np.arange(5, dtype=np.float64),
            "col3": ["a", "b", "c", "d", "e"],
            "col4": ["1", "2", "3", "4", "5"],
            "col5": np.arange(5, dtype=np.float64),
        }
    )
    df_1 = pd.DataFrame(
        {
            "col7": np.arange(5, dtype=np.float64),
            "col5": np.arange(5, dtype=np.float64),
            "col3": ["f", "g", "h", "i", "j"],
            "col1": np.arange(5, dtype=np.float64),
            "col6": np.arange(5, dtype=np.float64),
        }
    )
    lib.write("sym0", df_0)
    lib.write("sym1", df_1)

    received = concat(lib.read_batch(["sym0", "sym1"], lazy=True), join=join).collect().data
    expected = pd.concat([df_0, df_1], join=join)
    expected.index = pd.RangeIndex(len(expected))
    assert_frame_equal(expected, received)


@pytest.mark.parametrize("dynamic_schema", [True, False])
@pytest.mark.parametrize("rows_per_segment", [2, 100_000])
@pytest.mark.parametrize("columns_per_segment", [2, 100_000])
@pytest.mark.parametrize("columns", [["col1"], ["col2"], ["col3"], ["col1", "col2"], ["col1", "col3"], ["col2", "col3"]])
@pytest.mark.parametrize("join", ["inner", "outer"])
def test_symbol_concat_column_slicing(lmdb_library_factory, dynamic_schema, rows_per_segment, columns_per_segment, columns, join):
    lib = lmdb_library_factory(LibraryOptions(dynamic_schema=dynamic_schema, rows_per_segment=rows_per_segment, columns_per_segment=columns_per_segment))
    df_0 = pd.DataFrame(
        {
            "col1": np.arange(3, dtype=np.int64),
            "col2": np.arange(100, 103, dtype=np.int64),
            "col3": np.arange(1000, 1003, dtype=np.int64),
        },
    )
    df_1 = pd.DataFrame(
        {
            "col0": np.arange(10, 14, dtype=np.int64),
            "col1": np.arange(4, dtype=np.int64),
            "col2": np.arange(200, 204, dtype=np.int64),
            "col3": np.arange(2000, 2004, dtype=np.int64),
        },
    )
    lib.write("sym0", df_0)
    lib.write("sym1", df_1)

    lazy_df_0 = lib.read("sym0", columns=columns, lazy=True)
    lazy_df_1 = lib.read("sym1", columns=columns, lazy=True)

    received = concat([lazy_df_0, lazy_df_1], join).collect().data
    expected = pd.concat([df_0.loc[:, columns], df_1.loc[:, columns]])
    expected.index = pd.RangeIndex(len(expected))
    assert_frame_equal(expected, received)


@pytest.mark.parametrize("dynamic_schema", [True, False])
@pytest.mark.parametrize("rows_per_segment", [2, 100_000])
@pytest.mark.parametrize("columns_per_segment", [2, 100_000])
@pytest.mark.parametrize("join", ["inner", "outer"])
def test_symbol_concat_multiindex(lmdb_library_factory, dynamic_schema, rows_per_segment, columns_per_segment, join):
    lib = lmdb_library_factory(LibraryOptions(dynamic_schema=dynamic_schema, rows_per_segment=rows_per_segment, columns_per_segment=columns_per_segment))
    df = pd.DataFrame(
        {
            "col1": np.arange(12, dtype=np.int64),
            "col2": np.arange(100, 112, dtype=np.int64),
            "col3": np.arange(1000, 1012, dtype=np.int64),
        },
        index=pd.MultiIndex.from_product([pd.date_range("2025-01-01", periods=4), [0, 1, 2]], names=["datetime", "level"]),
    )
    lib.write("sym0", df[:3])
    lib.write("sym1", df[3:7])
    lib.write("sym2", df[7:])

    received = concat(lib.read_batch(["sym0", "sym1", "sym2"], lazy=True), join).collect().data
    assert_frame_equal(df, received)


@pytest.mark.parametrize("join", ["inner", "outer"])
def test_symbol_concat_with_date_range(lmdb_library, join):
    lib = lmdb_library
    df_0 = pd.DataFrame(
        {
            "col1": np.arange(3, dtype=np.int64),
        },
        index=pd.date_range(pd.Timestamp(0), freq="1000ns", periods=3),
    )
    df_1 = pd.DataFrame(
        {
            "col1": np.arange(4, dtype=np.int64),
        },
        index=pd.date_range(pd.Timestamp(1000), freq="1000ns", periods=4),
    )
    lib.write("sym0", df_0)
    lib.write("sym1", df_1)

    # Use date_range arg to trim last row from sym0
    lazy_df_0 = lib.read("sym0", date_range=(None, pd.Timestamp(1000)), lazy=True)
    # Use date_range clause to trim first row from sym1
    lazy_df_1 = lib.read("sym1", lazy=True)
    lazy_df_1 = lazy_df_1.date_range((pd.Timestamp(2000), None))

    received = concat([lazy_df_0, lazy_df_1], join).collect().data
    expected = pd.concat([df_0[:2], df_1[1:]])
    assert_frame_equal(expected, received)


@pytest.mark.parametrize("dynamic_schema", [True, False])
@pytest.mark.parametrize("rows_per_segment", [2, 100_000])
@pytest.mark.parametrize("columns_per_segment", [2, 100_000])
@pytest.mark.parametrize("join", ["inner", "outer"])
def test_symbol_concat_complex(lmdb_library_factory, dynamic_schema, rows_per_segment, columns_per_segment, join):
    lib = lmdb_library_factory(LibraryOptions(dynamic_schema=dynamic_schema, rows_per_segment=rows_per_segment, columns_per_segment=columns_per_segment))
    df_0 = pd.DataFrame(
        {
        "col1": np.arange(3, dtype=np.int64),
        "col2": np.arange(100, 103, dtype=np.int64),
        "col3": np.arange(1000, 1003, dtype=np.int64),
        },
        index=pd.date_range(pd.Timestamp(0), freq="1000ns", periods=3),
    )
    df_1 = pd.DataFrame(
        {
        "col1": np.arange(4, dtype=np.int64),
        "col2": np.arange(200, 204, dtype=np.int64),
        "col3": np.arange(2000, 2004, dtype=np.int64),
        },
        index=pd.date_range(pd.Timestamp(2000), freq="1000ns", periods=4),
    )
    df_2 = pd.DataFrame(
        {
        "col1": np.arange(5, dtype=np.int64),
        "col2": np.arange(300, 305, dtype=np.int64),
        "col3": np.arange(3000, 3005, dtype=np.int64),
        },
        index=pd.date_range(pd.Timestamp(6000), freq="1000ns", periods=5),
    )
    lib.write("sym0", df_0)
    lib.write("sym1", df_1)
    lib.write("sym2", df_2)

    lazy_df_0 = lib.read("sym0", lazy=True)
    lazy_df_1 = lib.read("sym1", lazy=True)
    lazy_df_1 = lazy_df_1.date_range((pd.Timestamp(pd.Timestamp(3000)), None))
    lazy_df_2 = lib.read("sym2", date_range=(None, pd.Timestamp(9000)), lazy=True)

    lazy_df = concat([lazy_df_0, lazy_df_1, lazy_df_2], join)

    lazy_df.resample("2000ns").agg({"col1": "sum", "col2": "mean", "col3": "min"})

    received = lazy_df.collect().data
    received = received.reindex(columns=sorted(received.columns))
    expected = pd.concat([df_0, df_1[1:], df_2[:4]]).resample("2000ns").agg({"col1": "sum", "col2": "mean", "col3": "min"})
    assert_frame_equal(expected, received)


def test_symbol_concat_querybuilder_syntax(lmdb_library):
    lib = lmdb_library
    df_0 = pd.DataFrame(
        {
            "col1": np.arange(3, dtype=np.int64),
            "col2": np.arange(100, 103, dtype=np.int64),
            "col3": np.arange(1000, 1003, dtype=np.int64),
        },
        index=pd.date_range(pd.Timestamp(0), freq="1000ns", periods=3),
    )
    df_1 = pd.DataFrame(
        {
            "col1": np.arange(4, dtype=np.int64),
            "col2": np.arange(200, 204, dtype=np.int64),
            "col3": np.arange(2000, 2004, dtype=np.int64),
        },
        index=pd.date_range(pd.Timestamp(2000), freq="1000ns", periods=4),
    )
    df_2 = pd.DataFrame(
        {
            "col1": np.arange(5, dtype=np.int64),
            "col2": np.arange(300, 305, dtype=np.int64),
            "col3": np.arange(3000, 3005, dtype=np.int64),
        },
        index=pd.date_range(pd.Timestamp(6000), freq="1000ns", periods=5),
    )
    lib.write("sym0", df_0)
    lib.write("sym1", df_1)
    lib.write("sym2", df_2)

    read_request_0 = ReadRequest("sym0")
    qb1 = QueryBuilder().date_range((pd.Timestamp(pd.Timestamp(3000)), None))
    read_request_1 = ReadRequest("sym1", query_builder=qb1)
    read_request_2 = ReadRequest("sym2", date_range=(None, pd.Timestamp(9000)))

    q = QueryBuilder().concat().resample("2000ns").agg({"col1": "sum", "col2": "mean", "col3": "min"})
    received = lib.read_batch_with_join([read_request_0, read_request_1, read_request_2], query_builder=q).data

    received = received.reindex(columns=sorted(received.columns))
    expected = pd.concat([df_0, df_1[1:], df_2[:4]]).resample("2000ns").agg({"col1": "sum", "col2": "mean", "col3": "min"})
    assert_frame_equal(expected, received)

@pytest.mark.parametrize("index_name_0", [None, "ts1", "ts2"])
@pytest.mark.parametrize("index_name_1", [None, "ts1", "ts2"])
@pytest.mark.parametrize("join", ["inner", "outer"])
def test_symbol_concat_differently_named_timeseries(lmdb_library, index_name_0, index_name_1, join):
    lib = lmdb_library
    df_0 = pd.DataFrame({"col": [0]}, index=[pd.Timestamp(0)])
    df_1 = pd.DataFrame({"col": [1]}, index=[pd.Timestamp(1)])
    df_0.index.name = index_name_0
    df_1.index.name = index_name_1
    lib.write("sym0", df_0)
    lib.write("sym1", df_1)
    received = concat(lib.read_batch(["sym0", "sym1"], lazy=True)).collect().data
    expected = pd.concat([df_0, df_1])
    expected.index.name = index_name_0 if index_name_0 == index_name_1 else None
    assert_frame_equal(expected, received)


@pytest.mark.parametrize("join", ["inner", "outer"])
def test_symbol_concat_symbols_with_different_indexes(lmdb_library, join):
    lib = lmdb_library
    df_0 = pd.DataFrame({"col": [0]}, index=pd.RangeIndex(1))
    df_1 = pd.DataFrame({"col": [0]}, index=[pd.Timestamp(0)])
    dt1 = pd.Timestamp(0)
    dt2 = pd.Timestamp(1)
    arr1 = [dt1, dt1, dt2, dt2]
    arr2 = [0, 1, 0, 1]
    df_2 = pd.DataFrame({"col": [0]}, index=pd.MultiIndex.from_arrays([arr1, arr2], names=["datetime", "level"]))

    lib.write("range_index_sym", df_0)
    lib.write("timestamp_index_sym", df_1)
    lib.write("multiindex_sym", df_2)

    with pytest.raises(SchemaException):
        concat(lib.read_batch(["range_index_sym", "timestamp_index_sym"], lazy=True), join).collect()

    with pytest.raises(SchemaException):
        concat(lib.read_batch(["timestamp_index_sym", "range_index_sym"], lazy=True), join).collect()

    with pytest.raises(SchemaException):
        concat(lib.read_batch(["range_index_sym", "multiindex_sym"], lazy=True), join).collect()

    with pytest.raises(SchemaException):
        concat(lib.read_batch(["multiindex_sym", "range_index_sym"], lazy=True), join).collect()

    with pytest.raises(SchemaException):
        concat(lib.read_batch(["timestamp_index_sym", "multiindex_sym"], lazy=True), join).collect()

    with pytest.raises(SchemaException):
        concat(lib.read_batch(["timestamp_index_sym", "multiindex_sym"], lazy=True), join).collect()


def test_symbol_concat_pickled_data(lmdb_library):
    lib = lmdb_library
    df = pd.DataFrame({"bytes": np.arange(10, dtype=np.uint64)})
    pickled_data = {"hi", "there"}
    lib.write("sym0", df)
    lib.write_pickle("sym1", pickled_data)

    with pytest.raises(SchemaException):
        concat(lib.read_batch(["sym0", "sym1"], lazy=True)).collect()
