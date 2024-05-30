import numpy as np
import pandas as pd

from arcticdb.version_store.processing import QueryBuilder
from arcticdb.util.test import assert_frame_equal


def generate_sparse_test_data():
    # Generates a dataframe with sparse columns suitable for testing all of the interesting combinations for filters,
    # projections, groupbys, and aggregations

    return pd.DataFrame(
        {
            "sparse1": [1.0, np.nan, 2.0, np.nan],
            "sparse2": [np.nan, 1.0, 2.0, np.nan],
            # "dense": [1.0, 2.0, 1.0, 3.0]
        },
        index=pd.date_range("2024-01-01", periods=4)
    )


def test_filter_sparse(lmdb_version_store):
    lib = lmdb_version_store
    sym = "test_filter_sparse"
    df = generate_sparse_test_data()
    lib.write(sym, df, sparsify_floats=True)

    # Col isnull
    expected = df[df["sparse1"].isnull()]
    q = QueryBuilder()
    q = q[q["sparse1"].isnull()]
    received = lib.read(sym, query_builder=q).data
    assert_frame_equal(expected, received)

    # Col notnull
    expected = df[df["sparse1"].notnull()]
    q = QueryBuilder()
    q = q[q["sparse1"].notnull()]
    received = lib.read(sym, query_builder=q).data
    assert_frame_equal(expected, received)

    # Col == Val
    expected = df.query("sparse1 == 1")
    q = QueryBuilder()
    q = q[q["sparse1"] == 1]
    received = lib.read(sym, query_builder=q).data
    assert_frame_equal(expected, received)

    # Col != Val
    expected = df.query("sparse1 != 2")
    q = QueryBuilder()
    q = q[q["sparse1"] != 2]
    received = lib.read(sym, query_builder=q).data
    assert_frame_equal(expected, received)

    # Col isin ValueSet
    expected = df.query("sparse1 in [1]")
    q = QueryBuilder()
    q = q[q["sparse1"].isin([1])]
    received = lib.read(sym, query_builder=q).data
    assert_frame_equal(expected, received)

    # Col isnotin ValueSet
    expected = df.query("sparse1 not in [1]")
    q = QueryBuilder()
    q = q[q["sparse1"].isnotin([1])]
    received = lib.read(sym, query_builder=q).data
    assert_frame_equal(expected, received)

    # Col == Col
    expected = df.query("sparse1 == sparse2")
    q = QueryBuilder()
    q = q[q["sparse1"] == q["sparse2"]]
    received = lib.read(sym, query_builder=q).data
    assert_frame_equal(expected, received)

    # Col != Col
    expected = df.query("sparse1 != sparse2")
    q = QueryBuilder()
    q = q[q["sparse1"] != q["sparse2"]]
    received = lib.read(sym, query_builder=q).data
    assert_frame_equal(expected, received)


def test_project_sparse(lmdb_version_store):
    lib = lmdb_version_store
    sym = "test_project_sparse"
    df = generate_sparse_test_data()
    lib.write(sym, df, sparsify_floats=True)

    # -Col
    expected = df
    expected["projected"] = -expected["sparse1"]
    q = QueryBuilder()
    q = q.apply("projected", -q["sparse1"])
    received = lib.read(sym, query_builder=q).data
    assert_frame_equal(expected, received)

    # Col + Val
    expected = df
    expected["projected"] = expected["sparse1"] + 1
    q = QueryBuilder()
    q = q.apply("projected", q["sparse1"] + 1)
    received = lib.read(sym, query_builder=q).data
    assert_frame_equal(expected, received)

    # Col / Col
    expected = df
    expected["projected"] = expected["sparse1"] / expected["sparse2"]
    q = QueryBuilder()
    q = q.apply("projected", q["sparse1"] / q["sparse2"])
    received = lib.read(sym, query_builder=q).data
    assert_frame_equal(expected, received)


def test_groupby_sparse(lmdb_version_store_v1):
    lib = lmdb_version_store_v1
    sym = "test_groupby_sparse"
    df = generate_sparse_test_data()
    lib.write(sym, df, sparsify_floats=True)

    aggs = {
        "sum": ("sparse2", "sum"),
        "min": ("sparse2", "min"),
        "max": ("sparse2", "max"),
        "mean": ("sparse2", "mean"),
        "count": ("sparse2", "count"),
    }
    expected = df.groupby("sparse1").agg(None, **aggs)
    expected = expected.reindex(columns=sorted(expected.columns))
    q = QueryBuilder()
    q = q.groupby("sparse1").agg(aggs)
    received = lib.read(sym, query_builder=q).data
    received = received.reindex(columns=sorted(received.columns))
    assert_frame_equal(expected, received, check_dtype=False)