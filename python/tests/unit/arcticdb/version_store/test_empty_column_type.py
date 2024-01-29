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
@pytest.fixture(params=["object", "bool"])
def boolean_dtype(request):
    yield request.param


class TestCanAppendToEmptyColumn:

    @pytest.fixture(autouse=True)
    def create_empty_column(self, lmdb_version_store_static_and_dynamic):
        lmdb_version_store_static_and_dynamic.write("sym", pd.DataFrame({"col1": [None, None]}))
        yield

    def test_integer(self, lmdb_version_store_static_and_dynamic, int_dtype):
        df_non_empty = pd.DataFrame({"col1": np.array([1,2,3], dtype=int_dtype)})
        lmdb_version_store_static_and_dynamic.append("sym", df_non_empty)
        expected_result = pd.DataFrame({"col1": np.array([0,0,1,2,3], dtype=int_dtype)})
        assert_frame_equal(lmdb_version_store_static_and_dynamic.read("sym").data, expected_result)
        assert_frame_equal(
            lmdb_version_store_static_and_dynamic.read("sym", row_range=[0,2]).data,
            pd.DataFrame({"col1": np.array([0,0], dtype=int_dtype)})
        )
        assert_frame_equal(
            lmdb_version_store_static_and_dynamic.read("sym", row_range=[2,5]).data,
            df_non_empty
        )

    def test_float(self, lmdb_version_store_static_and_dynamic, float_dtype):
        df_non_empty = pd.DataFrame({"col1": np.array([1,2,3], dtype=float_dtype)})
        lmdb_version_store_static_and_dynamic.append("sym", df_non_empty)
        expected_result = pd.DataFrame({"col1": np.array([float("NaN"),float("NaN"),1,2,3], dtype=float_dtype)})
        assert_frame_equal(lmdb_version_store_static_and_dynamic.read("sym").data, expected_result)
        assert_frame_equal(
            lmdb_version_store_static_and_dynamic.read("sym", row_range=[0,2]).data,
            pd.DataFrame({"col1": np.array([float("NaN"),float("NaN")], dtype=float_dtype)})
        )
        assert_frame_equal(
            lmdb_version_store_static_and_dynamic.read("sym", row_range=[2,5]).data,
            df_non_empty
        )

    def test_bool(self, lmdb_version_store_static_and_dynamic, boolean_dtype):
        df_non_empty = pd.DataFrame({"col1": np.array([True, False, True], dtype=boolean_dtype)})
        lmdb_version_store_static_and_dynamic.append("sym", df_non_empty)
        expected_result = pd.DataFrame({"col1": np.array([None, None, True, False, True], dtype=boolean_dtype)})
        assert_frame_equal(lmdb_version_store_static_and_dynamic.read("sym").data, expected_result)
        assert_frame_equal(
            lmdb_version_store_static_and_dynamic.read("sym", row_range=[0,2]).data,
            pd.DataFrame({"col1": np.array([None, None], dtype=boolean_dtype)})
        )
        assert_frame_equal(
            lmdb_version_store_static_and_dynamic.read("sym", row_range=[2,5]).data,
            df_non_empty
        )

    def test_empty(self, lmdb_version_store_static_and_dynamic):
        df_non_empty = pd.DataFrame({"col1": np.array([None, None, None])})
        lmdb_version_store_static_and_dynamic.append("sym", df_non_empty)
        expected_result = pd.DataFrame({"col1": np.array([None, None, None, None, None])})
        assert_frame_equal(lmdb_version_store_static_and_dynamic.read("sym").data, expected_result)
        assert_frame_equal(
            lmdb_version_store_static_and_dynamic.read("sym", row_range=[0,2]).data,
            pd.DataFrame({"col1": np.array([None, None])})
        )
        assert_frame_equal(
            lmdb_version_store_static_and_dynamic.read("sym", row_range=[2,5]).data,
            df_non_empty
        )

    def test_string(self, lmdb_version_store_static_and_dynamic):
        df_non_empty = pd.DataFrame({"col1": np.array(["some_string", "long_string"*100])})
        lmdb_version_store_static_and_dynamic.append("sym", df_non_empty)
        expected_result = pd.DataFrame({"col1": np.array([None, None, "some_string", "long_string"*100])})
        assert_frame_equal(lmdb_version_store_static_and_dynamic.read("sym").data, expected_result)
        assert_frame_equal(
            lmdb_version_store_static_and_dynamic.read("sym", row_range=[0,2]).data,
            pd.DataFrame({"col1": np.array([None, None])})
        )
        assert_frame_equal(
            lmdb_version_store_static_and_dynamic.read("sym", row_range=[2,5]).data,
            df_non_empty
        )

    def test_date(self, lmdb_version_store_static_and_dynamic):
        df_non_empty = pd.DataFrame({"col1": np.array([np.datetime64('2005-02'), np.datetime64('2005-03'), np.datetime64('2005-03')])}, dtype="datetime64[ns]")
        lmdb_version_store_static_and_dynamic.append("sym", df_non_empty)
        expected_result = pd.DataFrame({"col1": np.array([np.datetime64('NaT'), np.datetime64('NaT'), np.datetime64('2005-02'), np.datetime64('2005-03'), np.datetime64('2005-03')], dtype="datetime64[ns]")})
        assert_frame_equal(lmdb_version_store_static_and_dynamic.read("sym").data, expected_result)
        assert_frame_equal(
            lmdb_version_store_static_and_dynamic.read("sym", row_range=[0,2]).data,
            pd.DataFrame({"col1": np.array([np.datetime64('NaT'), np.datetime64('NaT')], dtype="datetime64[ns]")})
        )
        assert_frame_equal(
            lmdb_version_store_static_and_dynamic.read("sym", row_range=[2,5]).data,
            df_non_empty
        )


class TestCanAppendEmptyToColumn:

    def test_integer(self, lmdb_version_store_static_and_dynamic, int_dtype):
        df_initial = pd.DataFrame({"col1": np.array([1,2,3], dtype=int_dtype)})
        lmdb_version_store_static_and_dynamic.write("sym", df_initial)
        lmdb_version_store_static_and_dynamic.append("sym", pd.DataFrame({"col1": [None, None]}))
        assert_frame_equal(
            lmdb_version_store_static_and_dynamic.read("sym").data,
            pd.DataFrame({"col1": np.array([1,2,3,0,0], dtype=int_dtype)})
        )
        assert_frame_equal(
            lmdb_version_store_static_and_dynamic.read("sym", row_range=[0,3]).data,
            df_initial
        )
        assert_frame_equal(
            lmdb_version_store_static_and_dynamic.read("sym", row_range=[3,5]).data,
            pd.DataFrame({"col1": np.array([0,0], dtype=int_dtype)})
        )

    def test_float(self, lmdb_version_store_static_and_dynamic, float_dtype):
        df_initial = pd.DataFrame({"col1": np.array([1,2,3], dtype=float_dtype)})
        lmdb_version_store_static_and_dynamic.write("sym", df_initial)
        lmdb_version_store_static_and_dynamic.append("sym", pd.DataFrame({"col1": [None, None]}))
        assert_frame_equal(
            lmdb_version_store_static_and_dynamic.read("sym").data,
            pd.DataFrame({"col1": np.array([1,2,3,float("NaN"),float("NaN")], dtype=float_dtype)})
        )
        assert_frame_equal(
            lmdb_version_store_static_and_dynamic.read("sym", row_range=[0,3]).data,
            df_initial
        )
        assert_frame_equal(
            lmdb_version_store_static_and_dynamic.read("sym", row_range=[3,5]).data,
            pd.DataFrame({"col1": np.array([float("NaN"), float("NaN")], dtype=float_dtype)})
        )

    def test_bool(self, lmdb_version_store_static_and_dynamic, boolean_dtype):
        df_initial = pd.DataFrame({"col1": np.array([True, False, True], dtype=boolean_dtype)})
        df_with_none = pd.DataFrame({"col1": np.array([None, None])})
        lmdb_version_store_static_and_dynamic.write("sym", df_initial)
        lmdb_version_store_static_and_dynamic.append("sym", df_with_none)
        assert_frame_equal(
            lmdb_version_store_static_and_dynamic.read("sym").data,
            pd.DataFrame({"col1": np.array([True, False, True, None, None], dtype=boolean_dtype)})
        )
        assert_frame_equal(
            lmdb_version_store_static_and_dynamic.read("sym", row_range=[0,3]).data,
            df_initial
        )
        assert_frame_equal(
            lmdb_version_store_static_and_dynamic.read("sym", row_range=[3,5]).data,
            pd.DataFrame(df_with_none, dtype=boolean_dtype)
        )

    def test_string(self, lmdb_version_store_static_and_dynamic):
        df_initial = pd.DataFrame({"col1": np.array(["some_string", "long_string"*100, ""])})
        df_with_none = pd.DataFrame({"col1": np.array([None, None])})
        lmdb_version_store_static_and_dynamic.write("sym", df_initial)
        lmdb_version_store_static_and_dynamic.append("sym", df_with_none)
        assert_frame_equal(
            lmdb_version_store_static_and_dynamic.read("sym").data,
            pd.DataFrame({"col1": np.array(["some_string", "long_string"*100, "", None, None])})
        )
        assert_frame_equal(
            lmdb_version_store_static_and_dynamic.read("sym", row_range=[0,3]).data,
            df_initial
        )
        assert_frame_equal(
            lmdb_version_store_static_and_dynamic.read("sym", row_range=[3,5]).data,
            df_with_none
        )

    def test_date(self, lmdb_version_store_static_and_dynamic):
        df_initial = pd.DataFrame(
            {
                "col1": np.array(
                    [
                        np.datetime64('2005-02'),
                        np.datetime64('2005-03'),
                        np.datetime64('2005-03')
                    ]
                )
            },
            dtype="datetime64[ns]"
        )
        lmdb_version_store_static_and_dynamic.write("sym", df_initial)
        lmdb_version_store_static_and_dynamic.append("sym", pd.DataFrame({"col1": np.array([None, None])}))
        expected_result = pd.DataFrame(
            {
                "col1": np.array(
                    [
                        np.datetime64('2005-02'),
                        np.datetime64('2005-03'),
                        np.datetime64('2005-03'),
                        np.datetime64('NaT'),
                        np.datetime64('NaT')
                    ],
                    dtype="datetime64[ns]")
            }
        )
        assert_frame_equal(lmdb_version_store_static_and_dynamic.read("sym").data, expected_result)
        assert_frame_equal(
            lmdb_version_store_static_and_dynamic.read("sym", row_range=[0,3]).data,
            df_initial
        )
        assert_frame_equal(
            lmdb_version_store_static_and_dynamic.read("sym", row_range=[3,5]).data,
            pd.DataFrame({"col1": np.array([np.datetime64('NaT'), np.datetime64('NaT')], dtype="datetime64[ns]")})
        )


class TestCanUpdateWithEmpty:
    def test_bool(self, lmdb_version_store_static_and_dynamic, boolean_dtype):
        index = list(pd.date_range(start="1/1/2024", end="1/4/2024"))
        lmdb_version_store_static_and_dynamic.write(
            "sym",
            pd.DataFrame({"col": [True, True, True, True]}, dtype=boolean_dtype, index=index)
        )
        lmdb_version_store_static_and_dynamic.update(
            "sym",
            pd.DataFrame({"col": [None, None]}, dtype=boolean_dtype, index=list(pd.date_range(start="1/2/2024", end="1/3/2024")))
        )
        assert_frame_equal(
            lmdb_version_store_static_and_dynamic.read("sym").data,
            pd.DataFrame({"col": [True, None, None, True]}, index=index, dtype=boolean_dtype)
        )
