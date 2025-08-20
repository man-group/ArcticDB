"""
Copyright 2024 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""
import numpy as np
import pandas as pd
import pytest

from arcticdb.exceptions import ArcticNativeException
from arcticdb_ext.storage import KeyType, NoDataFoundException
from arcticdb.version_store.processing import QueryBuilder
from arcticdb_ext.exceptions import InternalException, StorageException, UserInputException
from arcticdb.util.test import assert_frame_equal_with_arrow


pytestmark = pytest.mark.pipeline


def test_filter_batch_one_query(lmdb_version_store_v1, any_output_format):
    lib = lmdb_version_store_v1
    lib.set_output_format(any_output_format)
    sym1 = "sym1"
    sym2 = "sym2"
    df1 = pd.DataFrame({"a": [1, 2]}, index=np.arange(2))
    df2 = pd.DataFrame({"a": [2, 3]}, index=np.arange(2))
    lib.write(sym1, df1)
    lib.write(sym2, df2)

    q = QueryBuilder()
    q = q[q["a"] == 2]
    pandas_query = "a == 2"
    batch_res = lib.batch_read([sym1, sym2], query_builder=q)
    res1 = batch_res[sym1].data
    res2 = batch_res[sym2].data
    assert_frame_equal_with_arrow(df1.query(pandas_query), res1)
    assert_frame_equal_with_arrow(df2.query(pandas_query), res2)


def test_filter_batch_multiple_queries(lmdb_version_store_v1, any_output_format):
    lib = lmdb_version_store_v1
    lib.set_output_format(any_output_format)
    sym1 = "sym1"
    sym2 = "sym2"
    df1 = pd.DataFrame({"a": [1, 2]}, index=np.arange(2))
    df2 = pd.DataFrame({"a": [2, 3]}, index=np.arange(2))
    lib.write(sym1, df1)
    lib.write(sym2, df2)

    q1 = QueryBuilder()
    q1 = q1[q1["a"] == 1]
    pandas_query1 = "a == 1"
    q2 = QueryBuilder()
    q2 = q2[q2["a"] == 3]
    pandas_query2 = "a == 3"
    batch_res = lib.batch_read([sym1, sym2], query_builder=[q1, q2])
    res1 = batch_res[sym1].data
    res2 = batch_res[sym2].data
    assert_frame_equal_with_arrow(df1.query(pandas_query1), res1)
    assert_frame_equal_with_arrow(df2.query(pandas_query2), res2)


def test_filter_batch_multiple_queries_with_none(lmdb_version_store_v1, any_output_format):
    lib = lmdb_version_store_v1
    lib.set_output_format(any_output_format)
    sym1 = "sym1"
    sym2 = "sym2"
    df1 = pd.DataFrame({"a": [1, 2]}, index=np.arange(2))
    df2 = pd.DataFrame({"a": [2, 3]}, index=np.arange(2))
    lib.write(sym1, df1)
    lib.write(sym2, df2)

    q2 = QueryBuilder()
    q2 = q2[q2["a"] == 3]
    pandas_query2 = "a == 3"
    batch_res = lib.batch_read([sym1, sym2], query_builder=[None, q2])
    res1 = batch_res[sym1].data
    res2 = batch_res[sym2].data
    assert_frame_equal_with_arrow(df1, res1)
    assert_frame_equal_with_arrow(df2.query(pandas_query2), res2)


def test_filter_batch_incorrect_query_count(lmdb_version_store_v1):
    lib = lmdb_version_store_v1
    sym1 = "sym1"
    sym2 = "sym2"
    df1 = pd.DataFrame({"a": [1, 2]}, index=np.arange(2))
    df2 = pd.DataFrame({"a": [2, 3]}, index=np.arange(2))
    lib.write(sym1, df1)
    lib.write(sym2, df2)

    q = QueryBuilder()
    q = q[q["a"] == 3]
    with pytest.raises(ArcticNativeException):
        lib.batch_read([sym1, sym2], query_builder=[q])
    with pytest.raises(ArcticNativeException):
        lib.batch_read([sym1, sym2], query_builder=[q, q, q])


def test_filter_batch_symbol_doesnt_exist(lmdb_version_store_v1):
    lib = lmdb_version_store_v1
    sym1 = "sym1"
    sym2 = "sym2"
    df1 = pd.DataFrame({"a": [1, 2]}, index=np.arange(2))
    lib.write(sym1, df1)
    q = QueryBuilder()
    q = q[q["a"] == 2]
    with pytest.raises(NoDataFoundException):
        lib.batch_read([sym1, sym2], query_builder=q)


def test_filter_batch_version_doesnt_exist(lmdb_version_store_v1):
    lib = lmdb_version_store_v1
    sym1 = "sym1"
    sym2 = "sym2"
    df1 = pd.DataFrame({"a": [1, 2]}, index=np.arange(2))
    df2 = pd.DataFrame({"a": [2, 3]}, index=np.arange(2))
    lib.write(sym1, df1)
    lib.write(sym2, df2)

    q = QueryBuilder()
    q = q[q["a"] == 2]
    with pytest.raises(NoDataFoundException):
        lib.batch_read([sym1, sym2], as_ofs=[0, 1], query_builder=q)


def test_filter_batch_missing_keys(lmdb_version_store_v1):
    lib = lmdb_version_store_v1

    df1 = pd.DataFrame({"a": [3, 5, 7]})
    df2 = pd.DataFrame({"a": [4, 6, 8]})
    df3 = pd.DataFrame({"a": [5, 7, 9]})
    lib.write("s1", df1)
    lib.write("s2", df2)
    # Need two versions for this symbol as we're going to delete a version key, and the optimisation of storing the
    # latest index key in the version ref key means it will still work if we just write one version key and then delete
    # it
    lib.write("s3", df3)
    lib.write("s3", df3)
    lib_tool = lib.library_tool()
    s1_index_key = lib_tool.find_keys_for_id(KeyType.TABLE_INDEX, "s1")[0]
    s2_data_key = lib_tool.find_keys_for_id(KeyType.TABLE_DATA, "s2")[0]
    s3_version_keys = lib_tool.find_keys_for_id(KeyType.VERSION, "s3")
    s3_key_to_delete = [key for key in s3_version_keys if key.version_id == 0][0]
    lib_tool.remove(s1_index_key)
    lib_tool.remove(s2_data_key)
    lib_tool.remove(s3_key_to_delete)

    q = QueryBuilder()
    q = q[q["a"] == 2]

    # The exception thrown is different for missing version keys to everything else, and so depends on which symbol is
    # processed first
    with pytest.raises((NoDataFoundException, StorageException)):
        lib.batch_read(["s1", "s2", "s3"], [None, None, 0], query_builder=q)
