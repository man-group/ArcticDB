"""
Copyright 2023 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""
from math import nan
import pandas as pd
from pandas.testing import assert_frame_equal
import numpy as np
import pytest

@pytest.fixture(params=[t + s for s in ["8", "16", "32", "64"] for t in ["int", "uint"]])
def int_dtype(request):
    yield request.param


@pytest.fixture(params=["float" + s for s in ["32", "64"]])
def float_dtype(request):
    yield request.param


# object is for nullable boolean
@pytest.fixture(params=["bool", "object"])
def boolean_dtype(request):
    yield request.param


class TestCanAppendToEmptyColumn:

    @pytest.fixture(autouse=True)
    def create_empty_column(self, lmdb_version_store):
        df_empty = pd.DataFrame({"col1": [None, None]})
        lmdb_version_store.write("sym", df_empty)
        yield

    def test_integer(self, lmdb_version_store, int_dtype):
        df_non_empty = pd.DataFrame({"col1": np.array([1,2,3], dtype=int_dtype)})
        lmdb_version_store.append("sym", df_non_empty)
        expected_result = pd.DataFrame({"col1": np.array([0,0,1,2,3], dtype=int_dtype)})
        assert_frame_equal(lmdb_version_store.read("sym").data, expected_result)

    def test_float(self, lmdb_version_store, float_dtype):
        df_non_empty = pd.DataFrame({"col1": np.array([1,2,3], dtype=float_dtype)})
        lmdb_version_store.append("sym", df_non_empty)
        expected_result = pd.DataFrame({"col1": np.array([float("NaN"),float("NaN"),1,2,3], dtype=float_dtype)})
        assert_frame_equal(lmdb_version_store.read("sym").data, expected_result)

    def test_bool(self, lmdb_version_store, boolean_dtype):
        df_non_empty = pd.DataFrame({"col1": np.array([True, False, True], dtype=boolean_dtype)})
        lmdb_version_store.append("sym", df_non_empty)
        expected_result = pd.DataFrame({"col1": np.array([None, None, True, False, True], dtype=boolean_dtype)})
        assert_frame_equal(lmdb_version_store.read("sym").data, expected_result)

    def test_empty(self, lmdb_version_store):
        df_non_empty = pd.DataFrame({"col1": np.array([None, None, None])})
        lmdb_version_store.append("sym", df_non_empty)
        expected_result = pd.DataFrame({"col1": np.array([None, None, None, None, None])})
        assert_frame_equal(lmdb_version_store.read("sym").data, expected_result)

    def test_string(self, lmdb_version_store):
        df_non_empty = pd.DataFrame({"col1": np.array(["some_string", "long_string"*100])})
        lmdb_version_store.append("sym", df_non_empty)
        expected_result = pd.DataFrame({"col1": np.array([None, None, "some_string", "long_string"*100])})
        assert_frame_equal(lmdb_version_store.read("sym").data, expected_result)

    def test_date(self, lmdb_version_store):
        df_non_empty = pd.DataFrame({"col1": np.array([np.datetime64('2005-02'), np.datetime64('2005-03'), np.datetime64('2005-03')])})
        lmdb_version_store.append("sym", df_non_empty)
        expected_result = pd.DataFrame({"col1": np.array([np.datetime64('NaT'), np.datetime64('NaT'), np.datetime64('2005-02'), np.datetime64('2005-03'), np.datetime64('2005-03')], dtype="datetime64[ns]")})
        assert_frame_equal(lmdb_version_store.read("sym").data, expected_result)

