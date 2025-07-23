"""
Copyright 2025 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""
import numpy as np
import pandas as pd
import pytest

from arcticdb import col, concat, LazyDataFrame, LazyDataFrameCollection, QueryBuilder, ReadRequest
from arcticdb.exceptions import NoSuchVersionException, SchemaException
from arcticdb.options import LibraryOptions
from arcticdb.util.test import assert_frame_equal, assert_series_equal

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


@pytest.mark.parametrize("first_type", ["uint8", "uint16", "uint32", "uint64", "int8", "int16", "int32", "int64", "float32", "float64"])
@pytest.mark.parametrize("second_type", ["uint8", "uint16", "uint32", "uint64", "int8", "int16", "int32", "int64", "float32", "float64"])
def test_symbol_concat_type_promotion(lmdb_library, first_type, second_type):
    lib = lmdb_library
    df0 = pd.DataFrame({"col": np.arange(1, dtype=np.dtype(first_type))})
    df1 = pd.DataFrame({"col": np.arange(1, dtype=np.dtype(second_type))})
    lib.write("sym0", df0)
    lib.write("sym1", df1)
    received = concat(lib.read_batch(["sym0", "sym1"], lazy=True)).collect().data
    expected = pd.concat([df0, df1])
    expected.index = pd.RangeIndex(len(expected))
    assert_frame_equal(expected, received)


@pytest.mark.parametrize(
    "index",
    [
        None,
        pd.date_range("2025-01-01", periods=8),
        pd.MultiIndex.from_product([pd.date_range("2025-01-01", periods=2), [0, 1], ["hello", "goodbye"]]),
    ]
)
@pytest.mark.parametrize("name_0", [None, "", "s1", "s2"])
@pytest.mark.parametrize("name_1", [None, "", "s1", "s2"])
@pytest.mark.parametrize("join", ["inner", "outer"])
def test_symbol_concat_with_series(lmdb_library_factory, index, name_0, name_1, join):
    lib = lmdb_library_factory(LibraryOptions(columns_per_segment=2))
    s_0 = pd.Series(np.arange(8, dtype=np.float64), index=index, name=name_0)
    s_1 = pd.Series(np.arange(10, 18, dtype=np.float64), index=index, name=name_1)
    lib.write("sym0", s_0)
    lib.write("sym1", s_1)

    received = concat(lib.read_batch(["sym0", "sym1"], lazy=True), join=join).collect().data
    expected = pd.concat([s_0, s_1], join=join)
    if index is None:
        expected.index = pd.RangeIndex(len(expected))
    expected.name = name_0 if name_0 == name_1 else None
    assert_series_equal(expected, received)


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
@pytest.mark.parametrize("columns_per_segment", [2, 100_000])
def test_symbol_concat_integer_columns_outer_join(lmdb_library_factory, dynamic_schema, columns_per_segment):
    lib = lmdb_library_factory(LibraryOptions(dynamic_schema=dynamic_schema, columns_per_segment=columns_per_segment))
    df_0 = pd.DataFrame(
        {
            "col1": np.arange(5, dtype=np.int64),
            "col2": np.arange(5, 10, dtype=np.int64),
            "col3": np.arange(10, 15, dtype=np.int64),
            "col4": np.arange(15, 20, dtype=np.int64),
            "col5": np.arange(20, 25, dtype=np.int64),
        }
    )
    df_1 = pd.DataFrame(
        {
            "col7": np.arange(25, 30, dtype=np.int64),
            "col5": np.arange(30, 35, dtype=np.int64),
            "col3": np.arange(35, 40, dtype=np.int64),
            "col1": np.arange(40, 45, dtype=np.int64),
            "col6": np.arange(45, 50, dtype=np.int64),
        }
    )
    lib.write("sym0", df_0)
    lib.write("sym1", df_1)

    received = concat(lib.read_batch(["sym0", "sym1"], lazy=True), join="outer").collect().data
    expected = pd.concat([df_0, df_1], join="outer")
    expected.index = pd.RangeIndex(len(expected))
    expected.fillna(0, inplace=True)
    expected = expected.astype(np.int64)
    assert_frame_equal(expected, received)


@pytest.mark.parametrize("join", ["inner", "outer"])
def test_symbol_concat_dynamic_schema_missing_columns(lmdb_library_factory, join):
    lib = lmdb_library_factory(LibraryOptions(dynamic_schema=True))
    df_0 = pd.DataFrame(
        {
            "col1": np.arange(5, dtype=np.float64),
            "col2": np.arange(5, 10, dtype=np.float64),
            "col3": np.arange(10, 15, dtype=np.float64),
        }
    )
    df_1 = pd.DataFrame(
        {
            "col2": np.arange(15, 20, dtype=np.float64),
            "col3": np.arange(15, 20, dtype=np.float64),
            "col4": np.arange(20, 25, dtype=np.float64),
        }
    )
    df_2 = pd.DataFrame(
        {
            "col1": np.arange(25, 30, dtype=np.float64),
            "col4": np.arange(30, 35, dtype=np.float64),
            "col5": np.arange(35, 40, dtype=np.float64),
        }
    )
    df_3 = pd.DataFrame(
        {
            "col4": np.arange(40, 45, dtype=np.float64),
            "col5": np.arange(45, 50, dtype=np.float64),
            "col6": np.arange(50, 55, dtype=np.float64),
        }
    )
    lib.write("sym0", df_0)
    lib.append("sym0", df_1)
    lib.write("sym1", df_2)
    lib.append("sym1", df_3)

    received = concat(lib.read_batch(["sym0", "sym1"], lazy=True), join=join).collect().data
    expected = pd.concat([pd.concat([df_0, df_1], join="outer"), pd.concat([df_2, df_3], join="outer")], join=join)
    expected.index = pd.RangeIndex(len(expected))
    assert_frame_equal(expected, received)


@pytest.mark.parametrize("dynamic_schema", [True, False])
@pytest.mark.parametrize("columns_per_segment", [2, 100_000])
@pytest.mark.parametrize("index", [None, pd.date_range("2025-01-01", periods=5)])
@pytest.mark.parametrize("join", ["inner", "outer"])
def test_symbol_concat_empty_column_intersection(lmdb_library_factory, dynamic_schema, columns_per_segment, index, join):
    lib = lmdb_library_factory(LibraryOptions(dynamic_schema=dynamic_schema, columns_per_segment=columns_per_segment))
    df_0 = pd.DataFrame(
        {
            "col1": np.arange(5, dtype=np.float64),
            "col2": np.arange(5, dtype=np.float64),
            "col3": np.arange(5, dtype=np.float64),
        },
        index=index,
    )
    df_1 = pd.DataFrame(
        {
            "col4": np.arange(5, dtype=np.float64),
            "col5": np.arange(5, dtype=np.float64),
            "col6": np.arange(5, dtype=np.float64),
        },
        index=index,
    )
    lib.write("sym0", df_0)
    lib.write("sym1", df_1)

    received = concat(lib.read_batch(["sym0", "sym1"], lazy=True), join=join).collect().data
    if join == "inner":
        if index is None:
            assert not len(received)
        assert not len(received.columns)
    else:
        expected = pd.concat([df_0, df_1], join=join)
        if index is None:
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
@pytest.mark.parametrize("join", ["inner", "outer"])
def test_symbol_concat_filtering_with_column_selection(lmdb_library_factory, dynamic_schema, join):
    lib = lmdb_library_factory(LibraryOptions(dynamic_schema=dynamic_schema))
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
    columns=["col1"]
    lazy_df_0 = lib.read("sym0", columns=columns, lazy=True)
    lazy_df_0 = lazy_df_0[lazy_df_0["col3"] > 0]
    lazy_df_1 = lib.read("sym1", columns=columns, lazy=True)
    lazy_df_1 = lazy_df_1[lazy_df_1["col3"] > 0]

    received = concat([lazy_df_0, lazy_df_1], join).collect().data
    print(received)
    expected = pd.concat([df_0.loc[:, columns], df_1.loc[:, columns]])
    expected.index = pd.RangeIndex(len(expected))
    assert_frame_equal(expected, received)


@pytest.mark.parametrize("only_incompletes", [True, False])
@pytest.mark.parametrize("join", ["inner", "outer"])
def test_symbol_concat_with_streaming_incompletes(lmdb_library, only_incompletes, join):
    lib = lmdb_library
    if not only_incompletes:
        df_0 = pd.DataFrame({"col1": np.arange(3, dtype=np.float64), "col2": np.arange(3, 6, dtype=np.float64)}, index=pd.date_range("2025-01-01", periods=3))
        lib.write("sym0", df_0)
    df_1 = pd.DataFrame({"col1": np.arange(6, 9, dtype=np.float64), "col2": np.arange(9, 12, dtype=np.float64)}, index=pd.date_range("2025-01-04", periods=3))
    lib._dev_tools.library_tool().append_incomplete("sym0", df_1)
    df_2 = pd.DataFrame({"col2": np.arange(12, 15, dtype=np.float64), "col3": np.arange(15, 18, dtype=np.float64)}, index=pd.date_range("2025-01-07", periods=3))
    lib.write("sym1", df_2)
    # incomplete kwarg Not part of the V2 API
    received = lib._nvs.batch_read_and_join(
        ["sym0", "sym1"],
        QueryBuilder().concat(join),
        [None, None],
        [(None, None), (None, None)],
        [None, None],
        [None, None],
        [None, None],
        incomplete=True
    )
    if only_incompletes:
        expected = pd.concat([df_1, df_2], join=join)
    else:
        expected = pd.concat([df_0, df_1, df_2], join=join)
    assert_frame_equal(expected, received.data)
    for idx, version in enumerate(received.versions):
        assert version.symbol == f"sym{idx}"
        assert version.data is None
        assert version.metadata is None
    assert received.versions[0].version == (np.iinfo(np.uint64).max if only_incompletes else 0)
    assert received.versions[1].version == 0


@pytest.mark.parametrize("dynamic_schema", [True, False])
@pytest.mark.parametrize("rows_per_segment", [2, 100_000])
@pytest.mark.parametrize("columns_per_segment", [2, 100_000])
@pytest.mark.parametrize("join", ["inner", "outer"])
def test_symbol_concat_multiindex_basic(lmdb_library_factory, dynamic_schema, rows_per_segment, columns_per_segment, join):
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

    sym_0 = "sym0"
    qb1 = QueryBuilder().date_range((pd.Timestamp(pd.Timestamp(3000)), None))
    read_request_1 = ReadRequest("sym1", query_builder=qb1)
    read_request_2 = ReadRequest("sym2", date_range=(None, pd.Timestamp(9000)))

    q = QueryBuilder().concat().resample("2000ns").agg({"col1": "sum", "col2": "mean", "col3": "min"})
    received = lib.read_batch_and_join([sym_0, read_request_1, read_request_2], query_builder=q).data

    received = received.reindex(columns=sorted(received.columns))
    expected = pd.concat([df_0, df_1[1:], df_2[:4]]).resample("2000ns").agg({"col1": "sum", "col2": "mean", "col3": "min"})
    assert_frame_equal(expected, received)

@pytest.mark.parametrize("index_name_0", [None, "ts1", "ts2"])
@pytest.mark.parametrize("index_name_1", [None, "ts1", "ts2"])
@pytest.mark.parametrize("join", ["inner", "outer"])
def test_symbol_concat_differently_named_timeseries(lmdb_library, index_name_0, index_name_1, join):
    lib = lmdb_library
    df_0 = pd.DataFrame({"col1": np.arange(1, dtype=np.float64), "col2": np.arange(1, 2, dtype=np.float64)}, index=[pd.Timestamp(0)])
    df_1 = pd.DataFrame({"col1": np.arange(2, 3, dtype=np.float64), "col3": np.arange(3, 4, dtype=np.float64)}, index=[pd.Timestamp(1)])
    df_0.index.name = index_name_0
    df_1.index.name = index_name_1
    lib.write("sym0", df_0)
    lib.write("sym1", df_1)
    received = concat(lib.read_batch(["sym0", "sym1"], lazy=True), join).collect().data
    expected = pd.concat([df_0, df_1], join=join)
    expected.index.name = index_name_0 if index_name_0 == index_name_1 else None
    assert_frame_equal(expected, received)


@pytest.mark.parametrize("index_name_0_level_0", [None, "ts1", "ts2"])
@pytest.mark.parametrize("index_name_0_level_1", [None, "hello", "goodbye"])
@pytest.mark.parametrize("index_name_1_level_0", [None, "ts1", "ts2"])
@pytest.mark.parametrize("index_name_1_level_1", [None, "hello", "goodbye"])
@pytest.mark.parametrize("join", ["inner", "outer"])
def test_symbol_concat_differently_named_multiindexes(
        lmdb_library,
        index_name_0_level_0,
        index_name_0_level_1,
        index_name_1_level_0,
        index_name_1_level_1,
        join
):
    lib = lmdb_library
    df_0 = pd.DataFrame(
        {
            "col1": np.arange(1, dtype=np.float64),
            "col2": np.arange(1, 2, dtype=np.float64),
         },
        index=pd.MultiIndex.from_product([pd.date_range("2025-01-01", periods=4), ["hello", None, "goodbye"]], names=[index_name_0_level_0, index_name_0_level_1])
    )
    df_1 = pd.DataFrame(
        {
            "col1": np.arange(2, 3, dtype=np.float64),
            "col2": np.arange(3, 4, dtype=np.float64),
         },
        index=pd.MultiIndex.from_product([pd.date_range("2025-01-01", periods=4), ["bonjour", "au revoir", None]], names=[index_name_1_level_0, index_name_1_level_1])
    )
    lib.write("sym0", df_0)
    lib.write("sym1", df_1)
    received = concat(lib.read_batch(["sym0", "sym1"], lazy=True), join).collect().data
    expected = pd.concat([df_0, df_1], join=join)
    expected_level_0_name = index_name_0_level_0 if index_name_0_level_0 == index_name_1_level_0 else None
    expected_level_1_name = index_name_0_level_1 if index_name_0_level_1 == index_name_1_level_1 else None
    expected.index.names = [expected_level_0_name, expected_level_1_name]
    assert_frame_equal(expected, received)


@pytest.mark.parametrize("tz_0", [None, "Europe/Amsterdam", "US/Eastern"])
@pytest.mark.parametrize("tz_1", [None, "Europe/Amsterdam", "US/Eastern"])
@pytest.mark.parametrize("join", ["inner", "outer"])
def test_symbol_concat_timezone_handling(
        lmdb_library,
        tz_0,
        tz_1,
        join
):
    lib = lmdb_library
    df_0 = pd.DataFrame(
        {
            "col1": np.arange(1, dtype=np.float64),
            "col2": np.arange(1, 2, dtype=np.float64),
        },
        index=pd.date_range("2025-01-01", periods=1, tz=tz_0),
    )
    df_1 = pd.DataFrame(
        {
            "col1": np.arange(2, 3, dtype=np.float64),
            "col2": np.arange(3, 4, dtype=np.float64),
        },
        index=pd.date_range("2025-01-01", periods=1, tz=tz_1),
    )
    lib.write("sym0", df_0)
    lib.write("sym1", df_1)
    received = concat(lib.read_batch(["sym0", "sym1"], lazy=True), join).collect().data

    expected_tz = f"datetime64[ns, {tz_0}]" if (tz_0 == tz_1 and tz_0 is not None) else "datetime64[ns]"
    assert str(received.index.dtype) == expected_tz


@pytest.mark.parametrize("tz_0_level_0", [None, "Europe/Amsterdam", "US/Eastern"])
@pytest.mark.parametrize("tz_0_level_1", [None, "Europe/Amsterdam", "Australia/Sydney"])
@pytest.mark.parametrize("tz_1_level_0", [None, "Europe/Amsterdam", "US/Eastern"])
@pytest.mark.parametrize("tz_1_level_1", [None, "Europe/Amsterdam", "Australia/Sydney"])
@pytest.mark.parametrize("join", ["inner", "outer"])
def test_symbol_concat_multiindex_timezone_handling(
        lmdb_library,
        tz_0_level_0,
        tz_0_level_1,
        tz_1_level_0,
        tz_1_level_1,
        join
):
    lib = lmdb_library
    df_0 = pd.DataFrame(
        {
            "col1": np.arange(1, dtype=np.float64),
            "col2": np.arange(1, 2, dtype=np.float64),
        },
        index=pd.MultiIndex.from_product([pd.date_range("2025-01-01", periods=4, tz=tz_0_level_0), pd.date_range("2025-01-01", periods=3, tz=tz_0_level_1)])
    )
    df_1 = pd.DataFrame(
        {
            "col1": np.arange(2, 3, dtype=np.float64),
            "col2": np.arange(3, 4, dtype=np.float64),
        },
        index=pd.MultiIndex.from_product([pd.date_range("2025-01-01", periods=4, tz=tz_1_level_0), pd.date_range("2025-01-01", periods=3, tz=tz_1_level_1)])
    )
    lib.write("sym0", df_0)
    lib.write("sym1", df_1)
    received = concat(lib.read_batch(["sym0", "sym1"], lazy=True), join).collect().data

    expected_level_0_tz = f"datetime64[ns, {tz_0_level_0}]" if (tz_0_level_0 == tz_1_level_0 and tz_0_level_0 is not None) else "datetime64[ns]"
    expected_level_1_tz = f"datetime64[ns, {tz_0_level_1}]" if (tz_0_level_1 == tz_1_level_1 and tz_0_level_1 is not None) else "datetime64[ns]"
    assert str(received.index.dtypes[0]) == expected_level_0_tz
    assert str(received.index.dtypes[1]) == expected_level_1_tz


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


def test_symbol_concat_non_existent_symbol(lmdb_library):
    lib = lmdb_library
    sym = "test_symbol_concat_non_existent_symbol"
    lib.write(sym, pd.DataFrame({"col": [0]}))
    with pytest.raises(NoSuchVersionException):
        concat(lib.read_batch([sym, "non-existent symbol"], lazy=True)).collect()


def test_symbol_concat_pickled_data(lmdb_library):
    lib = lmdb_library
    df = pd.DataFrame({"bytes": np.arange(10, dtype=np.uint64)})
    pickled_data = {"hi", "there"}
    lib.write("sym0", df)
    lib.write_pickle("sym1", pickled_data)

    with pytest.raises(SchemaException):
        concat(lib.read_batch(["sym0", "sym1"], lazy=True)).collect()


def test_symbol_concat_docstring_example(lmdb_library):
    lib = lmdb_library
    df0 = pd.DataFrame(
        {
            "col": [0, 1, 2, 3, 4],
        },
        index=pd.date_range("2025-01-01", freq="min", periods=5),
    )
    df1 = pd.DataFrame(
        {
            "col": [5, 6, 7, 8, 9],
        },
        index=pd.date_range("2025-01-01T00:05:00", freq="min", periods=5),
    )
    lib.write("symbol0", df0)
    lib.write("symbol1", df1)
    lazy_df0, lazy_df1 = lib.read_batch(["symbol0", "symbol1"], lazy=True).split()
    lazy_df0 = lazy_df0[lazy_df0["col"] <= 2]
    lazy_df1 = lazy_df1[lazy_df1["col"] <= 6]
    lazy_df = concat([lazy_df0, lazy_df1])
    lazy_df = lazy_df.resample("10min").agg({"col": "sum"})
    received = lazy_df.collect().data
    assert_frame_equal(pd.DataFrame({"col": [14]}, index=[pd.Timestamp("2025-01-01")]), received)