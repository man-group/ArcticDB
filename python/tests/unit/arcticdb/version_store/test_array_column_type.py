"""
Copyright 2023 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

from arcticdb.arctic import Arctic
import pandas as pd
from pandas.testing import assert_frame_equal
import numpy as np
import pytest


@pytest.fixture(params=("float", "int"))
def array_type(request):
    yield request.param


def assert_db_in_out_match(version_store, df_in, symbol):
    version_store.write(symbol, df_in)
    df_out = version_store.read(symbol)
    assert_frame_equal(df_in, df_out.data)


class TestEmptyArrays:
    def test_single_empty_array(self, lmdb_version_store):
        df_in = pd.DataFrame({"col1": [np.array([])]})
        assert_db_in_out_match(lmdb_version_store, df_in, "test_single_empty_array")

    def test_multiple_empty_arrays(self, lmdb_version_store):
        df_in = pd.DataFrame({"col1": [np.array([]), np.array([]), np.array([])]})
        assert_db_in_out_match(lmdb_version_store, df_in, "test_multiple_empty_arrays")

    def test_empty_array_can_coexist_with_nonempty_arrays(self, lmdb_version_store, array_type):
        df_in = pd.DataFrame({"col1": [np.array([]), np.array([1, 2, 3]).astype(array_type), np.array([])]})
        assert_db_in_out_match(lmdb_version_store, df_in, "test_empty_array_can_coexist_with_nonempty_arrays")


class TestNonEmptyArrays:
    def test_single_array(self, lmdb_version_store, array_type):
        df_in = pd.DataFrame({"col1": [np.array([1, 2, 3]).astype(array_type)]})
        assert_db_in_out_match(lmdb_version_store, df_in, "test_single_array")

    def test_differently_shaped_rows(self, lmdb_version_store, array_type):
        df_in = pd.DataFrame(
            {
                "col1": [
                    np.array([1, 2, 3]).astype(array_type),
                    np.array([1]).astype(array_type),
                    np.array([1, 2, 3, 4]).astype(array_type),
                    np.array([2, 4]).astype(array_type),
                ]
            }
        )
        assert_db_in_out_match(lmdb_version_store, df_in, "test_differently_shaped_rows")

    def test_rows_can_be_none(self, lmdb_version_store, array_type):
        df_in = pd.DataFrame({"col1": [None, np.array([1, 2]).astype(array_type), None]})
        assert_db_in_out_match(lmdb_version_store, df_in, "test_rows_can_be_none")

    def test_can_contain_nan(self, lmdb_version_store):
        # Note integers don't have a NaN value
        df_in = pd.DataFrame({"col1": [np.array([np.NAN, float("NaN")])]})
        assert_db_in_out_match(lmdb_version_store, df_in, "test_can_contain_nan")
