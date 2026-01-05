"""
Copyright 2024 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

from hypothesis import assume, given, settings, strategies
from hypothesis.extra.pandas import columns, data_frames
import numpy as np
import pandas as pd
import pytest

from arcticdb.version_store.processing import QueryBuilder
from arcticdb.util.test import assert_frame_equal
from arcticdb.util.hypothesis import use_of_function_scoped_fixtures_in_hypothesis_checked

pytestmark = pytest.mark.pipeline


class TestQueryBuilderSparse:
    sym = "TestQueryBuilderSparse"
    df = None

    @pytest.fixture(autouse=True)
    def write_test_data(self, lmdb_version_store, any_output_format):
        lib = lmdb_version_store
        lib._set_output_format_for_pipeline_tests(any_output_format)
        df_0 = pd.DataFrame(
            {
                "sparse1": [1.0, np.nan, 2.0, np.nan],
                "sparse2": [np.nan, 1.0, 2.0, np.nan],
            },
            index=pd.date_range("2024-01-01", periods=4, tz="UTC"),
        )
        df_1 = pd.DataFrame(
            {
                "sparse1": [1.0, np.nan, 2.0, np.nan],
                "sparse2": [np.nan, 1.0, 2.0, np.nan],
            },
            index=pd.date_range("2024-01-05", periods=4, tz="UTC"),
        )
        # Use parallel write to generate 2 segments as append does not have the sparsify_floats kwarg
        lib.write(self.sym, df_0, parallel=True, sparsify_floats=True)
        lib.write(self.sym, df_1, parallel=True, sparsify_floats=True)
        lib.compact_incomplete(self.sym, False, False, sparsify=True)
        self.df = pd.concat([df_0, df_1])

    def test_filter_isnull(self, lmdb_version_store):
        expected = self.df[self.df["sparse1"].isnull()]
        q = QueryBuilder()
        q = q[q["sparse1"].isnull()]
        received = lmdb_version_store.read(self.sym, query_builder=q).data
        assert_frame_equal(expected, received)

    def test_filter_notnull(self, lmdb_version_store):
        expected = self.df[self.df["sparse1"].notnull()]
        q = QueryBuilder()
        q = q[q["sparse1"].notnull()]
        received = lmdb_version_store.read(self.sym, query_builder=q).data
        assert_frame_equal(expected, received)

    def test_filter_col_equals_val(self, lmdb_version_store):
        expected = self.df.query("sparse1 == 1")
        q = QueryBuilder()
        q = q[q["sparse1"] == 1]
        received = lmdb_version_store.read(self.sym, query_builder=q).data
        assert_frame_equal(expected, received)

    def test_filter_col_not_equals_val(self, lmdb_version_store):
        expected = self.df.query("sparse1 != 2")
        q = QueryBuilder()
        q = q[q["sparse1"] != 2]
        received = lmdb_version_store.read(self.sym, query_builder=q).data
        assert_frame_equal(expected, received)

    def test_filter_col_isin_value_set(self, lmdb_version_store):
        expected = self.df.query("sparse1 in [1]")
        q = QueryBuilder()
        q = q[q["sparse1"].isin([1])]
        received = lmdb_version_store.read(self.sym, query_builder=q).data
        assert_frame_equal(expected, received)

    def test_filter_col_isnotin_value_set(self, lmdb_version_store):
        expected = self.df.query("sparse1 not in [1]")
        q = QueryBuilder()
        q = q[q["sparse1"].isnotin([1])]
        received = lmdb_version_store.read(self.sym, query_builder=q).data
        assert_frame_equal(expected, received)

    def test_filter_col_equals_col(self, lmdb_version_store):
        expected = self.df.query("sparse1 == sparse2")
        q = QueryBuilder()
        q = q[q["sparse1"] == q["sparse2"]]
        received = lmdb_version_store.read(self.sym, query_builder=q).data
        assert_frame_equal(expected, received)

    def test_filter_col_not_equals_col(self, lmdb_version_store):
        expected = self.df.query("sparse1 != sparse2")
        q = QueryBuilder()
        q = q[q["sparse1"] != q["sparse2"]]
        received = lmdb_version_store.read(self.sym, query_builder=q).data
        assert_frame_equal(expected, received)

    def test_project_minus_col(self, lmdb_version_store):
        expected = self.df
        expected["projected"] = -expected["sparse1"]
        q = QueryBuilder()
        q = q.apply("projected", -q["sparse1"])
        received = lmdb_version_store.read(self.sym, query_builder=q).data
        assert_frame_equal(expected, received)

    def test_project_col_plus_val(self, lmdb_version_store):
        expected = self.df
        expected["projected"] = expected["sparse1"] + 1
        q = QueryBuilder()
        q = q.apply("projected", q["sparse1"] + 1)
        received = lmdb_version_store.read(self.sym, query_builder=q).data
        assert_frame_equal(expected, received)

    def test_project_col_divided_by_col(self, lmdb_version_store):
        expected = self.df
        expected["projected"] = expected["sparse1"] / expected["sparse2"]
        q = QueryBuilder()
        q = q.apply("projected", q["sparse1"] / q["sparse2"])
        received = lmdb_version_store.read(self.sym, query_builder=q).data
        assert_frame_equal(expected, received)

    def test_groupby(self, lmdb_version_store):
        aggs = {
            "sum": ("sparse2", "sum"),
            "min": ("sparse2", "min"),
            "max": ("sparse2", "max"),
            "mean": ("sparse2", "mean"),
            "count": ("sparse2", "count"),
        }
        expected = self.df.groupby("sparse1").agg(None, **aggs)
        expected = expected.reindex(columns=sorted(expected.columns))
        q = QueryBuilder()
        q = q.groupby("sparse1").agg(aggs)
        received = lmdb_version_store.read(self.sym, query_builder=q).data
        received = received.reindex(columns=sorted(received.columns))
        received.sort_index(inplace=True)
        assert_frame_equal(expected, received, check_dtype=False)


def test_query_builder_sparse_dynamic_schema_type_change(lmdb_version_store_dynamic_schema, any_output_format):
    lib = lmdb_version_store_dynamic_schema
    lib._set_output_format_for_pipeline_tests(any_output_format)
    sym = "test_query_builder_sparse_dynamic_schema_type_change"
    df_0 = pd.DataFrame(
        {
            "sparse1": [1.0, np.nan, 2.0, np.nan],
            "sparse2": [np.nan, 1.0, 2.0, np.nan],
        },
        dtype=np.float64,
        index=pd.date_range("2024-01-01", periods=4, tz="UTC"),
    )
    df_1 = pd.DataFrame(
        {
            "sparse1": [1.0, np.nan, 2.0, np.nan],
            "sparse2": [np.nan, 1.0, 2.0, np.nan],
        },
        dtype=np.float32,
        index=pd.date_range("2024-01-05", periods=4, tz="UTC"),
    )
    lib.write(sym, df_0, parallel=True, sparsify_floats=True)
    lib.write(sym, df_1, parallel=True, sparsify_floats=True)
    lib.compact_incomplete(sym, False, False, sparsify=True)

    expected = pd.concat([df_0, df_1])
    expected = expected[expected["sparse1"].isnull()]
    q = QueryBuilder()
    q = q[q["sparse1"].isnull()]
    received = lib.read(sym, query_builder=q).data
    assert_frame_equal(expected, received)


@use_of_function_scoped_fixtures_in_hypothesis_checked
@settings(deadline=None)
@given(
    df=data_frames(
        columns(
            ["sparse1", "sparse2"],
            elements=strategies.floats(min_value=0, max_value=1000, allow_nan=False, allow_subnormal=False),
            fill=strategies.just(np.nan),
        ),
    ),
)
def test_query_builder_sparse_hypothesis(lmdb_version_store_v1, df):
    assume(not df.empty and not df["sparse1"].isnull().all() and not df["sparse2"].isnull().all())
    lib = lmdb_version_store_v1
    sym = "test_query_builder_sparse_hypothesis"

    df.index = pd.date_range("2024-01-01", periods=len(df))

    lib.write(sym, df, sparsify_floats=True)

    # Filter
    expected = df[df["sparse1"].isnull()]
    q = QueryBuilder()
    q = q[q["sparse1"].isnull()]
    received = lib.read(sym, query_builder=q).data
    assert_frame_equal(expected, received)

    # Projection
    expected = df
    expected["projected"] = expected["sparse1"] + expected["sparse2"]
    q = QueryBuilder()
    q = q.apply("projected", q["sparse1"] + q["sparse2"])
    received = lib.read(sym, query_builder=q).data
    assert_frame_equal(expected, received)

    # Groupby + aggregation
    expected = df.groupby("sparse1").agg({"sparse2": "sum"})
    q = QueryBuilder().groupby("sparse1").agg({"sparse2": "sum"})
    received = lib.read(sym, query_builder=q).data
    received.sort_index(inplace=True)
    assert_frame_equal(expected, received)
