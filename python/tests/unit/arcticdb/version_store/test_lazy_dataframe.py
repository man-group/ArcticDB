"""
Copyright 2024 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""
import numpy as np
import pandas as pd
import pickle
import pytest

from arcticdb import col, LazyDataFrame, LazyDataFrameCollection, QueryBuilder, ReadRequest
from arcticdb.util.test import assert_frame_equal
from arcticdb_ext.types import FieldDescriptor, TypeDescriptor, DataType, Dimension

pytestmark = pytest.mark.pipeline


def test_lazy_read(lmdb_library):
    lib = lmdb_library
    sym = "test_lazy_read"
    df = pd.DataFrame(
        {"col1": np.arange(10, dtype=np.int64), "col2": np.arange(100, 110, dtype=np.int64)}, index=pd.date_range("2000-01-01", periods=10)
    )
    lib.write(sym, df)
    lib.write_pickle(sym, 1)

    lazy_df = lib.read(sym, as_of=0, date_range=(pd.Timestamp("2000-01-03"), pd.Timestamp("2000-01-07")), columns=["col2"], lazy=True)
    assert isinstance(lazy_df, LazyDataFrame)
    received = lazy_df.collect().data
    expected = lib.read(sym, as_of=0, date_range=(pd.Timestamp("2000-01-03"), pd.Timestamp("2000-01-07")), columns=["col2"]).data

    assert_frame_equal(expected, received)


def test_lazy_date_range(lmdb_library):
    lib = lmdb_library
    sym = "test_lazy_date_range"
    df = pd.DataFrame(
        {"col1": np.arange(10, dtype=np.int64), "col2": np.arange(100, 110, dtype=np.int64)}, index=pd.date_range("2000-01-01", periods=10)
    )
    lib.write(sym, df)

    lazy_df = lib.read(sym, lazy=True)
    lazy_df = lazy_df.date_range((pd.Timestamp("2000-01-02"), pd.Timestamp("2000-01-09")))
    received = lazy_df.collect().data
    expected = df.iloc[1:9]

    assert_frame_equal(expected, received)


def test_lazy_filter(lmdb_library):
    lib = lmdb_library
    sym = "test_lazy_filter"
    df = pd.DataFrame(
        {"col1": np.arange(10, dtype=np.int64), "col2": np.arange(100, 110, dtype=np.int64)}, index=pd.date_range("2000-01-01", periods=10)
    )
    lib.write(sym, df)

    lazy_df = lib.read(sym, lazy=True)
    lazy_df = lazy_df[lazy_df["col1"].isin(0, 3, 6, 9)]
    received = lazy_df.collect().data
    expected = df.query("col1 in [0, 3, 6, 9]")

    assert_frame_equal(expected, received)


def test_lazy_head(lmdb_library):
    lib = lmdb_library
    sym = "test_lazy_head"
    df = pd.DataFrame(
        {"col1": np.arange(10, dtype=np.int64), "col2": np.arange(100, 110, dtype=np.int64)}, index=pd.date_range("2000-01-01", periods=10)
    )
    lib.write(sym, df)

    lazy_df = lib.head(sym, 4, lazy=True)
    lazy_df = lazy_df[lazy_df["col1"] >= 2]
    received = lazy_df.collect().data
    expected = df.iloc[2:4]

    assert_frame_equal(expected, received)


def test_lazy_tail(lmdb_library):
    lib = lmdb_library
    sym = "test_lazy_tail"
    df = pd.DataFrame(
        {"col1": np.arange(10, dtype=np.int64), "col2": np.arange(100, 110, dtype=np.int64)}, index=pd.date_range("2000-01-01", periods=10)
    )
    lib.write(sym, df)

    lazy_df = lib.tail(sym, 4, lazy=True)
    lazy_df = lazy_df[lazy_df["col1"] <= 7]
    received = lazy_df.collect().data
    expected = df.iloc[6:8]

    assert_frame_equal(expected, received)


def test_lazy_apply(lmdb_library):
    lib = lmdb_library
    sym = "test_lazy_apply"
    df = pd.DataFrame(
        {"col1": np.arange(10, dtype=np.int64), "col2": np.arange(100, 110, dtype=np.int64)}, index=pd.date_range("2000-01-01", periods=10)
    )
    lib.write(sym, df)

    lazy_df = lib.read(sym, lazy=True)
    lazy_df = lazy_df.apply("new_col", lazy_df["col1"] + lazy_df["col2"])
    received = lazy_df.collect().data
    expected = df
    expected["new_col"] = expected["col1"] + expected["col2"]

    assert_frame_equal(expected, received)


def test_lazy_apply_inline_col(lmdb_library):
    lib = lmdb_library
    sym = "test_lazy_apply_inline_col"
    df = pd.DataFrame(
        {"col1": np.arange(10, dtype=np.int64), "col2": np.arange(100, 110, dtype=np.int64)}, index=pd.date_range("2000-01-01", periods=10)
    )
    lib.write(sym, df)

    lazy_df = lib.read(sym, lazy=True).apply("new_col", col("col1") + col("col2"))
    received = lazy_df.collect().data
    expected = df
    expected["new_col"] = expected["col1"] + expected["col2"]

    assert_frame_equal(expected, received)


def test_lazy_project(lmdb_library):
    lib = lmdb_library
    sym = "test_lazy_project"
    df = pd.DataFrame(
        {"col1": np.arange(10, dtype=np.int64), "col2": np.arange(100, 110, dtype=np.int64)}, index=pd.date_range("2000-01-01", periods=10)
    )
    lib.write(sym, df)

    lazy_df = lib.read(sym, lazy=True)
    lazy_df["new_col"] = lazy_df["col1"] + lazy_df["col2"]
    received = lazy_df.collect().data
    expected = df
    expected["new_col"] = expected["col1"] + expected["col2"]

    assert_frame_equal(expected, received)


def test_lazy_groupby(lmdb_library):
    lib = lmdb_library
    sym = "test_lazy_groupby"
    df = pd.DataFrame({"col1": [0, 1, 0, 1, 2, 2], "col2": np.arange(6, dtype=np.int64)})
    lib.write(sym, df)

    lazy_df = lib.read(sym, lazy=True)
    lazy_df = lazy_df.groupby("col1").agg({"col2": "sum"})
    received = lazy_df.collect().data
    received.sort_index(inplace=True)
    expected = df.groupby("col1").agg({"col2": "sum"})

    assert_frame_equal(expected, received)


def test_lazy_resample(lmdb_library):
    lib = lmdb_library
    sym = "test_lazy_resample"
    df = pd.DataFrame(
        {"col1": np.arange(10, dtype=np.int64), "col2": np.arange(100, 110, dtype=np.int64)}, index=pd.date_range("2000-01-01", periods=10)
    )
    lib.write(sym, df)

    lazy_df = lib.read(sym, lazy=True)
    lazy_df = lazy_df.resample("D").agg({"col1": "sum", "col2": "first"})
    received = lazy_df.collect().data
    expected = df.resample("D").agg({"col1": "sum", "col2": "first"})

    assert_frame_equal(expected, received)


def test_lazy_with_initial_query_builder(lmdb_library):
    lib = lmdb_library
    sym = "test_lazy_chaining"
    idx = [0, 1, 2, 3, 1000, 1001]
    idx = np.array(idx, dtype="datetime64[ns]")
    df = pd.DataFrame({"col": np.arange(6, dtype=np.int64)}, index=idx)
    lib.write(sym, df)

    q = QueryBuilder().resample("us").agg({"col": "sum"})

    lazy_df = lib.read(sym, query_builder=q, lazy=True)
    lazy_df["new_col"] = lazy_df["col"] * 3
    received = lazy_df.collect().data

    expected = df.resample("us").agg({"col": "sum"})
    expected["new_col"] = expected["col"] * 3
    assert_frame_equal(expected, received, check_dtype=False)


def test_lazy_chaining(lmdb_library):
    lib = lmdb_library
    sym = "test_lazy_chaining"
    idx = [0, 1, 2, 3, 1000, 1001]
    idx = np.array(idx, dtype="datetime64[ns]")
    df = pd.DataFrame({"col": np.arange(6, dtype=np.int64)}, index=idx)
    lib.write(sym, df)

    lazy_df = lib.read(sym, lazy=True).resample("us").agg({"col": "sum"})
    lazy_df["new_col"] = lazy_df["col"] * 3
    received = lazy_df.collect().data

    expected = df.resample("us").agg({"col": "sum"})
    expected["new_col"] = expected["col"] * 3
    assert_frame_equal(expected, received, check_dtype=False)


def test_lazy_batch_read(lmdb_library):
    lib = lmdb_library
    sym_0 = "test_lazy_batch_read_0"
    sym_1 = "test_lazy_batch_read_1"
    df = pd.DataFrame(
        {"col1": np.arange(10, dtype=np.int64), "col2": np.arange(100, 110, dtype=np.int64)}, index=pd.date_range("2000-01-01", periods=10)
    )
    lib.write(sym_0, df)
    lib.write_pickle(sym_0, 1)
    lib.write(sym_1, df)

    read_request_0 = ReadRequest(
        symbol=sym_0,
        as_of=0,
        date_range=(pd.Timestamp("2000-01-03"), pd.Timestamp("2000-01-07")),
        columns=["col2"],
    )

    lazy_dfs = lib.read_batch([read_request_0, sym_1], lazy=True)
    assert isinstance(lazy_dfs, LazyDataFrameCollection)
    received = lazy_dfs.collect()
    expected_0 = lib.read(sym_0, as_of=0, date_range=(pd.Timestamp("2000-01-03"), pd.Timestamp("2000-01-07")), columns=["col2"]).data
    expected_1 = lib.read(sym_1).data
    assert_frame_equal(expected_0, received[0].data)
    assert_frame_equal(expected_1, received[1].data)


def test_lazy_batch_one_query(lmdb_library):
    lib = lmdb_library
    syms = [f"test_lazy_batch_one_query_{idx}" for idx in range(3)]
    df = pd.DataFrame(
        {"col1": np.arange(10, dtype=np.int64), "col2": np.arange(100, 110, dtype=np.int64)}, index=pd.date_range("2000-01-01", periods=10)
    )
    for sym in syms:
        lib.write(sym, df)
    lazy_dfs = lib.read_batch(syms, lazy=True)
    lazy_dfs = lazy_dfs[lazy_dfs["col1"].isin(0, 3, 6, 9)]
    received = lazy_dfs.collect()
    expected = df.query("col1 in [0, 3, 6, 9]")
    for vit in received:
        assert_frame_equal(expected, vit.data)


def test_lazy_batch_collect_separately(lmdb_library):
    lib = lmdb_library
    syms = [f"test_lazy_batch_collect_separately_{idx}" for idx in range(3)]
    df = pd.DataFrame(
        {"col1": np.arange(10, dtype=np.int64), "col2": np.arange(100, 110, dtype=np.int64)}, index=pd.date_range("2000-01-01", periods=10)
    )
    for sym in syms:
        lib.write(sym, df)
    lazy_dfs = lib.read_batch(syms, lazy=True)
    lazy_df_0, lazy_df_1, lazy_df_2 = lazy_dfs.split()
    lazy_df_0 = lazy_df_0[lazy_df_0["col1"].isin(0, 3, 6, 9)]
    lazy_df_2 = lazy_df_2[lazy_df_2["col1"].isin(2, 4, 8)]
    expected_0 = df.query("col1 in [0, 3, 6, 9]")
    expected_1 = df
    expected_2 = df.query("col1 in [2, 4, 8]")
    received_0 = lazy_df_0.collect().data
    received_1 = lazy_df_1.collect().data
    received_2 = lazy_df_2.collect().data
    assert_frame_equal(expected_0, received_0)
    assert_frame_equal(expected_1, received_1)
    assert_frame_equal(expected_2, received_2)


def test_lazy_batch_separate_queries_collect_together(lmdb_library):
    lib = lmdb_library
    syms = [f"test_lazy_batch_separate_queries_collect_together_{idx}" for idx in range(3)]
    df = pd.DataFrame(
        {"col1": np.arange(10, dtype=np.int64), "col2": np.arange(100, 110, dtype=np.int64)}, index=pd.date_range("2000-01-01", periods=10)
    )
    for sym in syms:
        lib.write(sym, df)
    lazy_dfs = lib.read_batch(syms, lazy=True).split()
    lazy_dfs[0] = lazy_dfs[0][lazy_dfs[0]["col1"].isin(0, 3, 6, 9)]
    lazy_dfs[2] = lazy_dfs[2][lazy_dfs[2]["col1"].isin(2, 4, 8)]
    expected_0 = df.query("col1 in [0, 3, 6, 9]")
    expected_1 = df
    expected_2 = df.query("col1 in [2, 4, 8]")

    received = LazyDataFrameCollection(lazy_dfs).collect()
    assert_frame_equal(expected_0, received[0].data)
    assert_frame_equal(expected_1, received[1].data)
    assert_frame_equal(expected_2, received[2].data)


def test_lazy_batch_complex(lmdb_library):
    lib = lmdb_library
    syms = [f"test_lazy_batch_complex_{idx}" for idx in range(3)]
    df = pd.DataFrame(
        {"col1": np.arange(10, dtype=np.int64), "col2": np.arange(100, 110, dtype=np.int64)}, index=pd.date_range("2000-01-01", periods=10)
    )
    for sym in syms:
        lib.write(sym, df)
    # Start with one query for all syms
    q = QueryBuilder()
    q = q[q["col1"] > 0]
    lazy_dfs = lib.read_batch(syms, query_builder=q, lazy=True)
    # Apply the same projection to all syms
    lazy_dfs["shared_new_col_1"] = lazy_dfs["col2"] * 2
    lazy_dfs = lazy_dfs.split()
    # Apply a different projection to each sym
    for idx, lazy_df in enumerate(lazy_dfs):
        lazy_df.apply(f"new_col", col("col1") * idx)
    # Collapse back together and apply another projection to all syms
    lazy_dfs = LazyDataFrameCollection(lazy_dfs)
    lazy_dfs["shared_new_col_2"] = lazy_dfs["new_col"] + 10
    received = lazy_dfs.collect()
    expected_0 = df.iloc[1:]
    expected_0["shared_new_col_1"] = expected_0["col2"] * 2
    expected_0["new_col"] = expected_0["col1"] * 0
    expected_0["shared_new_col_2"] = expected_0["new_col"] + 10
    expected_1 = df.iloc[1:]
    expected_1["shared_new_col_1"] = expected_1["col2"] * 2
    expected_1["new_col"] = expected_1["col1"] * 1
    expected_1["shared_new_col_2"] = expected_1["new_col"] + 10
    expected_2 = df.iloc[1:]
    expected_2["shared_new_col_1"] = expected_2["col2"] * 2
    expected_2["new_col"] = expected_2["col1"] * 2
    expected_2["shared_new_col_2"] = expected_2["new_col"] + 10
    assert_frame_equal(expected_0, received[0].data)
    assert_frame_equal(expected_1, received[1].data)
    assert_frame_equal(expected_2, received[2].data)


def test_lazy_collect_multiple_times(lmdb_library):
    lib = lmdb_library
    sym = "test_lazy_collect_multiple_times"
    idx = [0, 1, 2, 3, 1000, 1001]
    idx = np.array(idx, dtype="datetime64[ns]")
    df = pd.DataFrame({"col": np.arange(6, dtype=np.int64)}, index=idx)
    lib.write(sym, df)

    lazy_df = lib.read(sym, lazy=True).resample("us").agg({"col": "sum"})
    expected = df.resample("us").agg({"col": "sum"})
    received_0 = lazy_df.collect().data
    assert_frame_equal(expected, received_0, check_dtype=False)
    received_1 = lazy_df.collect().data
    assert_frame_equal(expected, received_1, check_dtype=False)

    lazy_df["new_col"] = lazy_df["col"] * 3
    received_2 = lazy_df.collect().data

    expected["new_col"] = expected["col"] * 3
    assert_frame_equal(expected, received_2, check_dtype=False)


def test_lazy_batch_collect_multiple_times(lmdb_library):
    lib = lmdb_library
    syms = [f"test_lazy_batch_collect_multiple_times_{idx}" for idx in range(3)]
    df = pd.DataFrame(
        {"col1": np.arange(10, dtype=np.int64), "col2": np.arange(100, 110, dtype=np.int64)}, index=pd.date_range("2000-01-01", periods=10)
    )
    for sym in syms:
        lib.write(sym, df)
    lazy_dfs = lib.read_batch(syms, lazy=True)
    lazy_dfs = lazy_dfs[lazy_dfs["col1"].isin(0, 3, 6, 9)]
    received_0 = lazy_dfs.collect()
    expected = df.query("col1 in [0, 3, 6, 9]")
    for vit in received_0:
        assert_frame_equal(expected, vit.data)

    received_1 = lazy_dfs.collect()
    for vit in received_1:
        assert_frame_equal(expected, vit.data)

    lazy_dfs = lazy_dfs[lazy_dfs["col1"].isin(0, 6)]
    received_2 = lazy_dfs.collect()
    expected = df.query("col1 in [0, 6]")
    for vit in received_2:
        assert_frame_equal(expected, vit.data)


def test_lazy_collect_twice_with_date_range(lmdb_library):
    lib = lmdb_library
    sym = "test_lazy_collect_twice_with_date_range"
    df = pd.DataFrame(
        {
            "col1": np.arange(10, dtype=np.int64),
            "col2": np.arange(100, 110, dtype=np.int64),
        },
        index=pd.date_range("2000-01-01", periods=10),
    )
    lib.write(sym, df)
    lazy_df = lib.read(sym, date_range=(pd.Timestamp("2000-01-03"), pd.Timestamp("2000-01-07")), lazy=True)
    expected = lib.read(sym, date_range=(pd.Timestamp("2000-01-03"), pd.Timestamp("2000-01-07"))).data
    received_0 = lazy_df.collect().data
    assert_frame_equal(expected, received_0, check_dtype=False)
    received_1 = lazy_df.collect().data
    assert_frame_equal(expected, received_1, check_dtype=False)


def test_lazy_pickling(lmdb_library):
    lib = lmdb_library
    sym = "test_lazy_pickling"
    idx = [0, 1, 2, 3, 1000, 1001]
    idx = np.array(idx, dtype="datetime64[ns]")
    df = pd.DataFrame({"col": np.arange(6, dtype=np.int64)}, index=idx)
    lib.write(sym, df)

    lazy_df = lib.read(sym, lazy=True).resample("us").agg({"col": "sum"})
    lazy_df["new_col"] = lazy_df["col"] * 3

    expected = df.resample("us").agg({"col": "sum"})
    expected["new_col"] = expected["col"] * 3

    roundtripped = pickle.loads(pickle.dumps(lazy_df))
    assert roundtripped == lazy_df
    received_initial = lazy_df.collect().data
    assert_frame_equal(expected, received_initial, check_dtype=False)

    received_roundtripped = roundtripped.collect().data
    assert_frame_equal(expected, received_roundtripped, check_dtype=False)


def test_lazy_batch_pickling(lmdb_library):
    lib = lmdb_library
    syms = [f"test_lazy_batch_pickling_{idx}" for idx in range(3)]
    idx = [0, 1, 2, 3, 1000, 1001]
    idx = np.array(idx, dtype="datetime64[ns]")
    df = pd.DataFrame(
        {"col1": np.arange(10, dtype=np.int64), "col2": np.arange(100, 110, dtype=np.int64)}, index=pd.date_range("2000-01-01", periods=10)
    )
    for sym in syms:
        lib.write(sym, df)
    lazy_dfs = lib.read_batch(syms, lazy=True)
    lazy_dfs = lazy_dfs[lazy_dfs["col1"].isin(0, 3, 6, 9)]

    expected = df.query("col1 in [0, 3, 6, 9]")

    roundtripped = pickle.loads(pickle.dumps(lazy_dfs))
    assert roundtripped == lazy_dfs
    received_initial = lazy_dfs.collect()
    for vit in received_initial:
        assert_frame_equal(expected, vit.data)

    received_roundtripped = roundtripped.collect()
    for vit in received_roundtripped:
        assert_frame_equal(expected, vit.data)


def to_pd_dtype(type_descriptor):
    # TODO: Extend with more types
    typ = type_descriptor.data_type()
    if typ == DataType.INT64:
        return np.int64
    if typ == DataType.FLOAT64:
        return np.float64
    if typ == DataType.FLOAT32:
        return np.float32
    if typ == DataType.NANOSECONDS_UTC64:
        return np.dtype("datetime64[ns]")
    if typ == DataType.UTF_DYNAMIC64:
        return np.dtype("object")
    raise ValueError("Unexpected data type")


def assert_lazy_frame_schema_equal(lazy_df, expected_types_and_names, expect_equal_column_order=True, check_against_collected=True):
    tsd = lazy_df.collect_schema().timeseries_descriptor
    lazy_fields = tsd.fields

    if not expect_equal_column_order:
        lazy_fields[1:] = sorted(lazy_fields[1:], key=lambda x: x.name)
        expected_types_and_names[1:] = sorted(expected_types_and_names[1:], key=lambda x: x[1])

    # Assert lazy_fields are same as expected
    assert len(lazy_fields) == len(expected_types_and_names)
    for lazy_field, (expected_type, expected_name) in zip(lazy_fields, expected_types_and_names):
        assert lazy_field.type == TypeDescriptor(expected_type, Dimension.Dim0)
        assert lazy_field.name == expected_name

    # Assert lazy_fields are same as collected
    if check_against_collected:
        collected_df = lazy_df.collect().data
        if not expect_equal_column_order:
            collected_df = collected_df.reindex(sorted(collected_df.columns), axis=1)

        field_names = [field.name for field in lazy_fields]
        field_dtypes = [to_pd_dtype(field.type) for field in lazy_fields]
        # TODO: The index name assertion should probably be better. Also add tests for dataframes with index name and other index types
        assert field_names[0] == collected_df.index.name or (field_names[0] == "index" and collected_df.index.name is None)
        assert field_names[1:] == list(collected_df.columns)
        assert field_dtypes[0] == collected_df.index.dtype
        print(type(collected_df.dtypes[0]), type(collected_df.index.dtype))
        assert field_dtypes[1:] == list(collected_df.dtypes)


def test_lazy_collect_schema_basic(lmdb_library):
    lib = lmdb_library
    sym = "sym"
    num_rows = 10
    df = pd.DataFrame({
        "col_int": np.arange(num_rows, dtype=np.int64),
        "col_float": np.arange(num_rows, dtype=np.float64),
        "col_float_2": np.arange(num_rows, dtype=np.float32),
        "col_str": [f"str_{i//5}" for i in range(num_rows)],
        # TODO: Add more types
    }, index=pd.date_range(pd.Timestamp(2025, 1, 1), periods=num_rows, freq="s"))
    lib.write(sym, df)

    # No query_builder
    lazy_df = lib.read(sym, lazy=True)
    expected_fields = [
        (DataType.NANOSECONDS_UTC64, "index"),
        (DataType.INT64, "col_int"),
        (DataType.FLOAT64, "col_float"),
        (DataType.FLOAT32, "col_float_2"),
        (DataType.UTF_DYNAMIC64, "col_str"),
    ]
    assert_lazy_frame_schema_equal(lazy_df, expected_fields)

    # With a filter we don't change the expected schema
    lazy_df = lazy_df[(lazy_df["col_int"] < 20) & (lazy_df["col_float"] >= 0.4) & (lazy_df["col_str"].isin(["str_0", "str_1"]))]
    assert_lazy_frame_schema_equal(lazy_df, expected_fields)

    # With a projection
    lazy_df.apply("col_sum", lazy_df["col_int"] + lazy_df["col_float"] / lazy_df["col_float_2"])
    expected_fields = [
        (DataType.NANOSECONDS_UTC64, "index"),
        (DataType.INT64, "col_int"),
        (DataType.FLOAT64, "col_float"),
        (DataType.FLOAT32, "col_float_2"),
        (DataType.UTF_DYNAMIC64, "col_str"),
        (DataType.FLOAT64, "col_sum"),
    ]
    assert_lazy_frame_schema_equal(lazy_df, expected_fields)

    # With a columns filter
    lazy_df.select_columns(["col_int", "col_float", "col_sum"])
    expected_fields = [
        (DataType.NANOSECONDS_UTC64, "index"),
        (DataType.INT64, "col_int"),
        (DataType.FLOAT64, "col_float"),
        (DataType.FLOAT64, "col_sum"),
    ]
    # TODO: We should check against collected. Problem will be fixed by 8774208809
    assert_lazy_frame_schema_equal(lazy_df, expected_fields, check_against_collected=False)

    # With resampling
    lazy_df.resample("3s").agg({"col_int": "max", "col_float_2": "sum", "col_sum": "mean"})
    lazy_df.select_columns(None) # Remove column filter
    expected_fields = [
        (DataType.NANOSECONDS_UTC64, "index"),
        (DataType.INT64, "col_int"),
        (DataType.FLOAT64, "col_float_2"),
        (DataType.FLOAT64, "col_sum"),
    ]
    assert_lazy_frame_schema_equal(lazy_df, expected_fields, expect_equal_column_order=False)

    # With group by
    lazy_df = lib.read(sym, lazy=True)
    lazy_df.groupby("col_str").agg({"col_int": "sum", "col_float": "mean", "col_float_2": "mean"})
    expected_fields = [
        (DataType.UTF_DYNAMIC64, "col_str"),
        (DataType.INT64, "col_int"),
        (DataType.FLOAT64, "col_float"),
        (DataType.FLOAT64, "col_float_2"),
    ]
    assert_lazy_frame_schema_equal(lazy_df, expected_fields, expect_equal_column_order=False)


def test_lazy_index_caching(lmdb_library_dynamic_schema):
    lib = lmdb_library_dynamic_schema
    sym = "sym"
    dfs = [
        pd.DataFrame({"col_1": np.arange(3, dtype=np.int32)}, index=pd.date_range(pd.Timestamp(2025, 1, 1), periods=3)),
        pd.DataFrame({"col_1": np.arange(3, dtype=np.int64), "col_2": np.arange(3, dtype=np.int64)}, index=pd.date_range(pd.Timestamp(2025, 1, 4), periods=3)),
        pd.DataFrame({"col_2": np.arange(3, dtype=np.int32), "col_3": np.arange(3, dtype=np.float32)}, index=pd.date_range(pd.Timestamp(2025, 1, 7), periods=3)),
    ]

    lib.write(sym, dfs[0])

    lazy_df = lib.read(sym, lazy=True)
    expected_fields = [
        (DataType.NANOSECONDS_UTC64, "index"),
        (DataType.INT32, "col_1"),
    ]
    assert_lazy_frame_schema_equal(lazy_df, expected_fields, check_against_collected=False) # We don't call collect

    lib.append(sym, dfs[1])
    # The lazy_df should have cached the index and still return the old schema and dataframe
    assert_lazy_frame_schema_equal(lazy_df, expected_fields, check_against_collected=False) # We don't call collect
    vit = lazy_df.collect()
    assert vit.version == 0
    assert_frame_equal(vit.data, dfs[0])

    # But if we decide to use
    vit = lazy_df.collect(use_latest_version=True)
    assert vit.version == 1
    assert_frame_equal(vit.data, pd.concat(dfs[:2]))

