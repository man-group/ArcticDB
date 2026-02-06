"""
Copyright 2026 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

import numpy as np
import pandas as pd
import pickle
import pytest

from arcticdb import col, LazyDataFrame, LazyDataFrameCollection, QueryBuilder, ReadRequest, where
from arcticdb.util.test import assert_frame_equal

pytestmark = pytest.mark.pipeline


@pytest.mark.parametrize("collect_schema_first", [True, False])
class TestLazyDataFrame:
    def test_lazy_read(self, lmdb_library, any_output_format, collect_schema_first):
        lib = lmdb_library
        lib._nvs._set_output_format_for_pipeline_tests(any_output_format)
        sym = "test_lazy_read"
        df = pd.DataFrame(
            {"col1": np.arange(10, dtype=np.int64), "col2": np.arange(100, 110, dtype=np.int64)},
            index=pd.date_range("2000-01-01", periods=10),
        )
        lib.write(sym, df)
        lib.write_pickle(sym, 1)

        lazy_df = lib.read(
            sym,
            as_of=0,
            date_range=(pd.Timestamp("2000-01-03"), pd.Timestamp("2000-01-07")),
            columns=["col2"],
            lazy=True,
        )
        assert isinstance(lazy_df, LazyDataFrame)
        if collect_schema_first:
            lazy_df.collect_schema()
        received = lazy_df.collect().data
        expected = lib.read(
            sym, as_of=0, date_range=(pd.Timestamp("2000-01-03"), pd.Timestamp("2000-01-07")), columns=["col2"]
        ).data

        assert_frame_equal(expected, received)

    def test_lazy_date_range(self, lmdb_library, any_output_format, collect_schema_first):
        lib = lmdb_library
        lib._nvs._set_output_format_for_pipeline_tests(any_output_format)
        sym = "test_lazy_date_range"
        df = pd.DataFrame(
            {"col1": np.arange(10, dtype=np.int64), "col2": np.arange(100, 110, dtype=np.int64)},
            index=pd.date_range("2000-01-01", periods=10),
        )
        lib.write(sym, df)

        lazy_df = lib.read(sym, lazy=True)
        lazy_df = lazy_df.date_range((pd.Timestamp("2000-01-02"), pd.Timestamp("2000-01-09")))
        if collect_schema_first:
            lazy_df.collect_schema()
        received = lazy_df.collect().data
        expected = df.iloc[1:9]

        assert_frame_equal(expected, received)

    def test_lazy_filter(self, lmdb_library, any_output_format, collect_schema_first):
        lib = lmdb_library
        lib._nvs._set_output_format_for_pipeline_tests(any_output_format)
        sym = "test_lazy_filter"
        df = pd.DataFrame(
            {"col1": np.arange(10, dtype=np.int64), "col2": np.arange(100, 110, dtype=np.int64)},
            index=pd.date_range("2000-01-01", periods=10),
        )
        lib.write(sym, df)

        lazy_df = lib.read(sym, lazy=True)
        lazy_df = lazy_df[lazy_df["col1"].isin(0, 3, 6, 9)]
        if collect_schema_first:
            lazy_df.collect_schema()
        received = lazy_df.collect().data
        expected = df.query("col1 in [0, 3, 6, 9]")

        assert_frame_equal(expected, received)

    def test_lazy_head(self, lmdb_library, any_output_format, collect_schema_first):
        lib = lmdb_library
        lib._nvs._set_output_format_for_pipeline_tests(any_output_format)
        sym = "test_lazy_head"
        df = pd.DataFrame(
            {"col1": np.arange(10, dtype=np.int64), "col2": np.arange(100, 110, dtype=np.int64)},
            index=pd.date_range("2000-01-01", periods=10),
        )
        lib.write(sym, df)

        lazy_df = lib.head(sym, 4, lazy=True)
        lazy_df = lazy_df[lazy_df["col1"] >= 2]
        if collect_schema_first:
            lazy_df.collect_schema()
        received = lazy_df.collect().data
        expected = df.iloc[2:4]

        assert_frame_equal(expected, received)

    def test_lazy_tail(self, lmdb_library, any_output_format, collect_schema_first):
        lib = lmdb_library
        lib._nvs._set_output_format_for_pipeline_tests(any_output_format)
        sym = "test_lazy_tail"
        df = pd.DataFrame(
            {"col1": np.arange(10, dtype=np.int64), "col2": np.arange(100, 110, dtype=np.int64)},
            index=pd.date_range("2000-01-01", periods=10),
        )
        lib.write(sym, df)

        lazy_df = lib.tail(sym, 4, lazy=True)
        lazy_df = lazy_df[lazy_df["col1"] <= 7]
        if collect_schema_first:
            lazy_df.collect_schema()
        received = lazy_df.collect().data
        expected = df.iloc[6:8]

        assert_frame_equal(expected, received)

    def test_lazy_apply(self, lmdb_library, any_output_format, collect_schema_first):
        lib = lmdb_library
        lib._nvs._set_output_format_for_pipeline_tests(any_output_format)
        sym = "test_lazy_apply"
        df = pd.DataFrame(
            {"col1": np.arange(10, dtype=np.int64), "col2": np.arange(100, 110, dtype=np.int64)},
            index=pd.date_range("2000-01-01", periods=10),
        )
        lib.write(sym, df)

        lazy_df = lib.read(sym, lazy=True)
        lazy_df = lazy_df.apply("new_col", lazy_df["col1"] + lazy_df["col2"])
        if collect_schema_first:
            lazy_df.collect_schema()
        received = lazy_df.collect().data
        expected = df
        expected["new_col"] = expected["col1"] + expected["col2"]

        assert_frame_equal(expected, received)

    def test_lazy_apply_inline_col(self, lmdb_library, any_output_format, collect_schema_first):
        lib = lmdb_library
        lib._nvs._set_output_format_for_pipeline_tests(any_output_format)
        sym = "test_lazy_apply_inline_col"
        df = pd.DataFrame(
            {"col1": np.arange(10, dtype=np.int64), "col2": np.arange(100, 110, dtype=np.int64)},
            index=pd.date_range("2000-01-01", periods=10),
        )
        lib.write(sym, df)

        lazy_df = lib.read(sym, lazy=True).apply("new_col", col("col1") + col("col2"))
        if collect_schema_first:
            lazy_df.collect_schema()
        received = lazy_df.collect().data
        expected = df
        expected["new_col"] = expected["col1"] + expected["col2"]

        assert_frame_equal(expected, received)

    def test_lazy_project(self, lmdb_library, any_output_format, collect_schema_first):
        lib = lmdb_library
        lib._nvs._set_output_format_for_pipeline_tests(any_output_format)
        sym = "test_lazy_project"
        df = pd.DataFrame(
            {"col1": np.arange(10, dtype=np.int64), "col2": np.arange(100, 110, dtype=np.int64)},
            index=pd.date_range("2000-01-01", periods=10),
        )
        lib.write(sym, df)

        lazy_df = lib.read(sym, lazy=True)
        lazy_df["new_col"] = lazy_df["col1"] + lazy_df["col2"]
        if collect_schema_first:
            lazy_df.collect_schema()
        received = lazy_df.collect().data
        expected = df
        expected["new_col"] = expected["col1"] + expected["col2"]

        assert_frame_equal(expected, received)

    def test_lazy_project_constant_value(self, lmdb_library, any_output_format, collect_schema_first):
        lib = lmdb_library
        lib._nvs._set_output_format_for_pipeline_tests(any_output_format)
        sym = "test_lazy_project"
        df = pd.DataFrame(
            {"col1": np.arange(10, dtype=np.int64), "col2": np.arange(100, 110, dtype=np.int64)},
            index=pd.date_range("2000-01-01", periods=10),
        )
        lib.write(sym, df)

        lazy_df = lib.read(sym, lazy=True)
        lazy_df["new_col"] = 5
        if collect_schema_first:
            lazy_df.collect_schema()
        received = lazy_df.collect().data
        expected = df
        expected["new_col"] = 5

        assert_frame_equal(expected, received, check_dtype=False)

    def test_lazy_ternary(self, lmdb_library, any_output_format, collect_schema_first):
        lib = lmdb_library
        lib._nvs._set_output_format_for_pipeline_tests(any_output_format)
        sym = "test_lazy_ternary"
        df = pd.DataFrame(
            {
                "conditional": [True, False, True, True, False] * 2,
                "col1": np.arange(10, dtype=np.int64),
                "col2": np.arange(100, 110, dtype=np.int64),
            },
            index=pd.date_range("2000-01-01", periods=10),
        )
        lib.write(sym, df)

        lazy_df = lib.read(sym, lazy=True)
        lazy_df["new_col"] = where(lazy_df["conditional"], lazy_df["col1"], lazy_df["col2"])
        if collect_schema_first:
            lazy_df.collect_schema()
        received = lazy_df.collect().data
        expected = df
        expected["new_col"] = np.where(df["conditional"].to_numpy(), df["col1"].to_numpy(), df["col2"].to_numpy())

        assert_frame_equal(expected, received)

    def test_lazy_groupby(self, lmdb_library, any_output_format, collect_schema_first):
        lib = lmdb_library
        lib._nvs._set_output_format_for_pipeline_tests(any_output_format)
        sym = "test_lazy_groupby"
        df = pd.DataFrame({"col1": [0, 1, 0, 1, 2, 2], "col2": np.arange(6, dtype=np.int64)})
        lib.write(sym, df)

        lazy_df = lib.read(sym, lazy=True)
        lazy_df = lazy_df.groupby("col1").agg({"col2": "sum"})
        if collect_schema_first:
            lazy_df.collect_schema()
        received = lazy_df.collect().data
        received.sort_index(inplace=True)
        expected = df.groupby("col1").agg({"col2": "sum"})

        assert_frame_equal(expected, received)

    def test_lazy_resample(self, lmdb_library, any_output_format, collect_schema_first):
        lib = lmdb_library
        lib._nvs._set_output_format_for_pipeline_tests(any_output_format)
        sym = "test_lazy_resample"
        df = pd.DataFrame(
            {"col1": np.arange(10, dtype=np.int64), "col2": np.arange(100, 110, dtype=np.int64)},
            index=pd.date_range("2000-01-01", periods=10),
        )
        lib.write(sym, df)

        lazy_df = lib.read(sym, lazy=True)
        lazy_df = lazy_df.resample("D").agg({"col1": "sum", "col2": "first"})
        if collect_schema_first:
            lazy_df.collect_schema()
        received = lazy_df.collect().data
        expected = df.resample("D").agg({"col1": "sum", "col2": "first"})

        expected.sort_index(inplace=True, axis=1)
        received.sort_index(inplace=True, axis=1)
        assert_frame_equal(expected, received)

    def test_lazy_regex_match(self, lmdb_library, sym, any_output_format, collect_schema_first):
        lib = lmdb_library
        lib._nvs._set_output_format_for_pipeline_tests(any_output_format)
        df = pd.DataFrame(
            index=pd.date_range(pd.Timestamp(0), periods=3), data={"a": ["abc", "abcd", "aabc"], "b": [1, 2, 3]}
        )
        lib.write(sym, df)

        lazy_df = lib.read(sym, lazy=True)
        pattern = "^abc"
        lazy_df = lazy_df[lazy_df["a"].regex_match(pattern)]
        if collect_schema_first:
            lazy_df.collect_schema()
        received = lazy_df.collect().data
        expected = df[df.a.str.contains(pattern)]

        assert_frame_equal(expected, received)

    def test_lazy_with_initial_query_builder(self, lmdb_library, any_output_format, collect_schema_first):
        lib = lmdb_library
        lib._nvs._set_output_format_for_pipeline_tests(any_output_format)
        sym = "test_lazy_chaining"
        idx = [0, 1, 2, 3, 1000, 1001]
        idx = np.array(idx, dtype="datetime64[ns]")
        df = pd.DataFrame({"col": np.arange(6, dtype=np.int64)}, index=idx)
        lib.write(sym, df)

        q = QueryBuilder().resample("us").agg({"col": "sum"})

        lazy_df = lib.read(sym, query_builder=q, lazy=True)
        lazy_df["new_col"] = lazy_df["col"] * 3
        if collect_schema_first:
            lazy_df.collect_schema()
        received = lazy_df.collect().data

        expected = df.resample("us").agg({"col": "sum"})
        expected["new_col"] = expected["col"] * 3
        assert_frame_equal(expected, received, check_dtype=False)

    def test_lazy_chaining(self, lmdb_library, any_output_format, collect_schema_first):
        lib = lmdb_library
        lib._nvs._set_output_format_for_pipeline_tests(any_output_format)
        sym = "test_lazy_chaining"
        idx = [0, 1, 2, 3, 1000, 1001]
        idx = np.array(idx, dtype="datetime64[ns]")
        df = pd.DataFrame({"col": np.arange(6, dtype=np.int64)}, index=idx)
        lib.write(sym, df)

        lazy_df = lib.read(sym, lazy=True).resample("us").agg({"col": "sum"})
        lazy_df["new_col"] = lazy_df["col"] * 3
        if collect_schema_first:
            lazy_df.collect_schema()
        received = lazy_df.collect().data

        expected = df.resample("us").agg({"col": "sum"})
        expected["new_col"] = expected["col"] * 3
        assert_frame_equal(expected, received, check_dtype=False)

    def test_lazy_batch_collect_separately(self, lmdb_library, any_output_format, collect_schema_first):
        lib = lmdb_library
        lib._nvs._set_output_format_for_pipeline_tests(any_output_format)
        syms = [f"test_lazy_batch_collect_separately_{idx}" for idx in range(3)]
        df = pd.DataFrame(
            {"col1": np.arange(10, dtype=np.int64), "col2": np.arange(100, 110, dtype=np.int64)},
            index=pd.date_range("2000-01-01", periods=10),
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
        if collect_schema_first:
            lazy_df_0.collect_schema()
            lazy_df_1.collect_schema()
            lazy_df_2.collect_schema()
        received_0 = lazy_df_0.collect().data
        received_1 = lazy_df_1.collect().data
        received_2 = lazy_df_2.collect().data
        assert_frame_equal(expected_0, received_0)
        assert_frame_equal(expected_1, received_1)
        assert_frame_equal(expected_2, received_2)

    def test_lazy_batch_separate_queries_collect_together(self, lmdb_library, any_output_format, collect_schema_first):
        lib = lmdb_library
        lib._nvs._set_output_format_for_pipeline_tests(any_output_format)
        syms = [f"test_lazy_batch_separate_queries_collect_together_{idx}" for idx in range(3)]
        df = pd.DataFrame(
            {"col1": np.arange(10, dtype=np.int64), "col2": np.arange(100, 110, dtype=np.int64)},
            index=pd.date_range("2000-01-01", periods=10),
        )
        for sym in syms:
            lib.write(sym, df)
        lazy_dfs = lib.read_batch(syms, lazy=True).split()
        lazy_dfs[0] = lazy_dfs[0][lazy_dfs[0]["col1"].isin(0, 3, 6, 9)]
        lazy_dfs[2] = lazy_dfs[2][lazy_dfs[2]["col1"].isin(2, 4, 8)]
        expected_0 = df.query("col1 in [0, 3, 6, 9]")
        expected_1 = df
        expected_2 = df.query("col1 in [2, 4, 8]")
        if collect_schema_first:
            lazy_dfs[0].collect_schema()
            lazy_dfs[1].collect_schema()
            lazy_dfs[2].collect_schema()
        received = LazyDataFrameCollection(lazy_dfs).collect()
        assert_frame_equal(expected_0, received[0].data)
        assert_frame_equal(expected_1, received[1].data)
        assert_frame_equal(expected_2, received[2].data)

    def test_lazy_collect_multiple_times(self, lmdb_library, any_output_format, collect_schema_first):
        lib = lmdb_library
        lib._nvs._set_output_format_for_pipeline_tests(any_output_format)
        sym = "test_lazy_collect_multiple_times"
        idx = [0, 1, 2, 3, 1000, 1001]
        idx = np.array(idx, dtype="datetime64[ns]")
        df = pd.DataFrame({"col": np.arange(6, dtype=np.int64)}, index=idx)
        lib.write(sym, df)

        lazy_df = lib.read(sym, lazy=True).resample("us").agg({"col": "sum"})
        expected = df.resample("us").agg({"col": "sum"})
        if collect_schema_first:
            lazy_df.collect_schema()
        received_0 = lazy_df.collect().data
        assert_frame_equal(expected, received_0, check_dtype=False)
        received_1 = lazy_df.collect().data
        assert_frame_equal(expected, received_1, check_dtype=False)

        lazy_df["new_col"] = lazy_df["col"] * 3
        received_2 = lazy_df.collect().data

        expected["new_col"] = expected["col"] * 3
        assert_frame_equal(expected, received_2, check_dtype=False)

    def test_lazy_collect_twice_with_date_range(self, lmdb_library, any_output_format, collect_schema_first):
        lib = lmdb_library
        lib._nvs._set_output_format_for_pipeline_tests(any_output_format)
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
        if collect_schema_first:
            lazy_df.collect_schema()
        received_0 = lazy_df.collect().data
        assert_frame_equal(expected, received_0, check_dtype=False)
        received_1 = lazy_df.collect().data
        assert_frame_equal(expected, received_1, check_dtype=False)


def test_lazy_batch_read(lmdb_library, any_output_format):
    lib = lmdb_library
    lib._nvs._set_output_format_for_pipeline_tests(any_output_format)
    sym_0 = "test_lazy_batch_read_0"
    sym_1 = "test_lazy_batch_read_1"
    df = pd.DataFrame(
        {"col1": np.arange(10, dtype=np.int64), "col2": np.arange(100, 110, dtype=np.int64)},
        index=pd.date_range("2000-01-01", periods=10),
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
    expected_0 = lib.read(
        sym_0, as_of=0, date_range=(pd.Timestamp("2000-01-03"), pd.Timestamp("2000-01-07")), columns=["col2"]
    ).data
    expected_1 = lib.read(sym_1).data
    assert_frame_equal(expected_0, received[0].data)
    assert_frame_equal(expected_1, received[1].data)


def test_lazy_batch_one_query(lmdb_library, any_output_format):
    lib = lmdb_library
    lib._nvs._set_output_format_for_pipeline_tests(any_output_format)
    syms = [f"test_lazy_batch_one_query_{idx}" for idx in range(3)]
    df = pd.DataFrame(
        {"col1": np.arange(10, dtype=np.int64), "col2": np.arange(100, 110, dtype=np.int64)},
        index=pd.date_range("2000-01-01", periods=10),
    )
    for sym in syms:
        lib.write(sym, df)
    lazy_dfs = lib.read_batch(syms, lazy=True)
    lazy_dfs = lazy_dfs[lazy_dfs["col1"].isin(0, 3, 6, 9)]
    received = lazy_dfs.collect()
    expected = df.query("col1 in [0, 3, 6, 9]")
    for vit in received:
        assert_frame_equal(expected, vit.data)


def test_lazy_batch_complex(lmdb_library, any_output_format):
    lib = lmdb_library
    lib._nvs._set_output_format_for_pipeline_tests(any_output_format)
    syms = [f"test_lazy_batch_complex_{idx}" for idx in range(3)]
    df = pd.DataFrame(
        {"col1": np.arange(10, dtype=np.int64), "col2": np.arange(100, 110, dtype=np.int64)},
        index=pd.date_range("2000-01-01", periods=10),
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


def test_lazy_batch_collect_multiple_times(lmdb_library, any_output_format):
    lib = lmdb_library
    lib._nvs._set_output_format_for_pipeline_tests(any_output_format)
    syms = [f"test_lazy_batch_collect_multiple_times_{idx}" for idx in range(3)]
    df = pd.DataFrame(
        {"col1": np.arange(10, dtype=np.int64), "col2": np.arange(100, 110, dtype=np.int64)},
        index=pd.date_range("2000-01-01", periods=10),
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


def test_lazy_pickling(lmdb_library, any_output_format):
    lib = lmdb_library
    lib._nvs._set_output_format_for_pipeline_tests(any_output_format)
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


def test_lazy_batch_pickling(lmdb_library, any_output_format):
    lib = lmdb_library
    lib._nvs._set_output_format_for_pipeline_tests(any_output_format)
    syms = [f"test_lazy_batch_pickling_{idx}" for idx in range(3)]
    idx = [0, 1, 2, 3, 1000, 1001]
    idx = np.array(idx, dtype="datetime64[ns]")
    df = pd.DataFrame(
        {"col1": np.arange(10, dtype=np.int64), "col2": np.arange(100, 110, dtype=np.int64)},
        index=pd.date_range("2000-01-01", periods=10),
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
