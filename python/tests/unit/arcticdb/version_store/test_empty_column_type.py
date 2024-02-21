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
    if request.param == "object":
        pytest.skip("Nullable booleans are temporary disabled")
    yield request.param


@pytest.fixture(params=["datetime64[ns]"])
def date_dtype(request):
    yield request.param


class TestCanAppendToColunWithNones:
    """
    Tests that it is possible to write a column containing None values and latter
    append to it. Initially the type of the column must be empty type after the
    append the column must be of the same type as the appended data
    """

    @pytest.fixture(autouse=True)
    def create_empty_column(self, lmdb_version_store_static_and_dynamic):
        lmdb_version_store_static_and_dynamic.write("sym", pd.DataFrame({"col1": 2 * [None]}))
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
        # Note: if dtype is bool pandas will convert None to False
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

    def test_date(self, lmdb_version_store_static_and_dynamic, date_dtype):
        df_non_empty = pd.DataFrame(
            {
                "col1": np.array(
                    [
                        np.datetime64('2005-02'),
                        np.datetime64('2005-03'),
                        np.datetime64('2005-03')
                    ]
                )
            },
            dtype=date_dtype
        )
        lmdb_version_store_static_and_dynamic.append("sym", df_non_empty)
        expected_result = pd.DataFrame(
            {
                "col1": np.array(
                    [
                        np.datetime64('NaT'),
                        np.datetime64('NaT'),
                        np.datetime64('2005-02'),
                        np.datetime64('2005-03'),
                        np.datetime64('2005-03')
                    ],
                    dtype=date_dtype
                )
            }
        )
        assert_frame_equal(lmdb_version_store_static_and_dynamic.read("sym").data, expected_result)
        assert_frame_equal(
            lmdb_version_store_static_and_dynamic.read("sym", row_range=[0,2]).data,
            pd.DataFrame({"col1": np.array([np.datetime64('NaT'), np.datetime64('NaT')], dtype=date_dtype)})
        )
        assert_frame_equal(
            lmdb_version_store_static_and_dynamic.read("sym", row_range=[2,5]).data,
            df_non_empty
        )


class TestCanAppendToEmptyColumn:
    """
    Tests that it's possible to append to a column which contains no rows.
    The type of the columns (including the index column) is decided after
    the first append.
    """

    @pytest.fixture(params=[[0,1,2], list(pd.date_range(start="1/1/2024", end="1/3/2024"))])
    def append_index(self, request):
        yield request.param

    @pytest.fixture(params=[pd.RangeIndex(0,0), pd.DatetimeIndex([])])
    def empty_index(self, request):
        yield request.param

    @pytest.fixture(autouse=True)
    def create_empty_column(self, lmdb_version_store_static_and_dynamic, int_dtype, empty_index):
        lmdb_version_store_static_and_dynamic.write("sym", pd.DataFrame({"col1": []}, dtype=int_dtype, index=empty_index))
        yield

    def test_integer(self, lmdb_version_store_static_and_dynamic, int_dtype, empty_index, append_index):
        df_to_append = pd.DataFrame({"col1": [1,2,3]}, dtype=int_dtype, index=append_index)
        lmdb_version_store_static_and_dynamic.append("sym", df_to_append)
        assert_frame_equal(lmdb_version_store_static_and_dynamic.read("sym").data, df_to_append)


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
        # Note: if dtype is bool pandas will convert None to False
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

    def test_date(self, lmdb_version_store_static_and_dynamic, date_dtype):
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
            dtype=date_dtype
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
                    dtype=date_dtype)
            }
        )
        assert_frame_equal(lmdb_version_store_static_and_dynamic.read("sym").data, expected_result)
        assert_frame_equal(
            lmdb_version_store_static_and_dynamic.read("sym", row_range=[0,3]).data,
            df_initial
        )
        assert_frame_equal(
            lmdb_version_store_static_and_dynamic.read("sym", row_range=[3,5]).data,
            pd.DataFrame({"col1": np.array([np.datetime64('NaT'), np.datetime64('NaT')], dtype=date_dtype)})
        )


class TestCanUpdateWithEmpty:
    def index(self):
        return list(pd.date_range(start="1/1/2024", end="1/4/2024"))

    def update_index(self):
        return list(pd.date_range(start="1/2/2024", end="1/3/2024"))

    def test_int(self, lmdb_version_store_static_and_dynamic, int_dtype):
        lmdb_version_store_static_and_dynamic.write(
            "sym",
            pd.DataFrame({"col": [1, 2, 3, 4]}, dtype=int_dtype, index=self.index())
        )
        lmdb_version_store_static_and_dynamic.update(
            "sym",
            pd.DataFrame({"col": [None, None]}, index=self.update_index())
        )
        assert_frame_equal(
            lmdb_version_store_static_and_dynamic.read("sym").data,
            pd.DataFrame({"col": [1, 0, 0, 4]}, index=self.index(), dtype=int_dtype)
        )

    def test_float(self, lmdb_version_store_static_and_dynamic, float_dtype):
        lmdb_version_store_static_and_dynamic.write(
            "sym",
            pd.DataFrame({"col": [1, 2, 3, 4]}, dtype=float_dtype, index=self.index())
        )
        lmdb_version_store_static_and_dynamic.update(
            "sym",
            pd.DataFrame({"col": [None, None]}, index=self.update_index())
        )
        assert_frame_equal(
            lmdb_version_store_static_and_dynamic.read("sym").data,
            pd.DataFrame({"col": [1, float("NaN"), float("NaN"), 4]}, index=self.index(), dtype=float_dtype)
        )

    def test_bool(self, lmdb_version_store_static_and_dynamic, boolean_dtype):
        # Note: if dtype is bool pandas will convert None to False
        lmdb_version_store_static_and_dynamic.write(
            "sym",
            pd.DataFrame({"col": [True, True, True, True]}, dtype=boolean_dtype, index=self.index())
        )
        lmdb_version_store_static_and_dynamic.update(
            "sym",
            pd.DataFrame({"col": [None, None]}, dtype=boolean_dtype, index=self.update_index())
        )
        assert_frame_equal(
            lmdb_version_store_static_and_dynamic.read("sym").data,
            pd.DataFrame({"col": [True, None, None, True]}, index=self.index(), dtype=boolean_dtype)
        )

    def test_string(self, lmdb_version_store_static_and_dynamic):
        lmdb_version_store_static_and_dynamic.write(
            "sym",
            pd.DataFrame({"col": ["a", "longstr"*20, "b", "longstr"*20]}, index=self.index())
        )
        lmdb_version_store_static_and_dynamic.update(
            "sym",
            pd.DataFrame({"col": [None, None]}, index=self.update_index())
        )
        assert_frame_equal(
            lmdb_version_store_static_and_dynamic.read("sym").data,
            pd.DataFrame({"col": ["a", None, None, "longstr"*20]}, index=self.index())
        )

    def test_date(self, lmdb_version_store_static_and_dynamic, date_dtype):
        lmdb_version_store_static_and_dynamic.write(
            "sym",
            pd.DataFrame(
                {
                    "col": [
                        np.datetime64('2005-02'),
                        np.datetime64('2005-03'),
                        np.datetime64('2005-04'),
                        np.datetime64('2005-05')
                    ]
                },
                dtype=date_dtype,
                index=self.index()
            )
        )
        lmdb_version_store_static_and_dynamic.update(
            "sym",
            pd.DataFrame({"col": [None, None]}, index=self.update_index())
        )
        assert_frame_equal(
            lmdb_version_store_static_and_dynamic.read("sym").data,
            pd.DataFrame(
                {
                    "col": [
                        np.datetime64('2005-02'),
                        np.datetime64('NaT'),
                        np.datetime64('NaT'),
                        np.datetime64('2005-05')
                    ]
                },
                index=self.index()
            )
        )


class TestCanUpdateEmpty:
    """
    Check that a column containing only empty type and date index can be updated and that
    the type of the column will be changed to the type of the new data. The update range
    is right in the middle of the initial date range, so there will be values unaffected
    by the update. Check that on read the type of the unaffected entries will be changed
    from empty to the new type of the column.
    """


    def index(self):
        return list(pd.date_range(start="1/1/2024", end="1/4/2024"))

    def update_index(self):
        return list(pd.date_range(start="1/2/2024", end="1/3/2024"))

    @pytest.fixture(autouse=True)
    def create_empty_column(self, lmdb_version_store_static_and_dynamic):
        lmdb_version_store_static_and_dynamic.write("sym", pd.DataFrame({"col": 4 * [None]}, index=self.index()))
        yield

    def test_integer(self, lmdb_version_store_static_and_dynamic, int_dtype):
        lmdb_version_store_static_and_dynamic.update(
            "sym",
            pd.DataFrame({"col": [1, 2]}, dtype=int_dtype, index=self.update_index())
        )
        assert_frame_equal(
            lmdb_version_store_static_and_dynamic.read("sym").data,
            pd.DataFrame({"col": [0,1,2,0]}, dtype=int_dtype, index=self.index())
        )
        assert_frame_equal(
            lmdb_version_store_static_and_dynamic.read(
                "sym",
                date_range=(pd.to_datetime("1/1/2024"), pd.to_datetime("1/1/2024"))
            ).data,
            pd.DataFrame({"col": [0]}, dtype=int_dtype, index=[pd.to_datetime("1/1/2024")])
        )

    def test_float(self, lmdb_version_store_static_and_dynamic, float_dtype):
        lmdb_version_store_static_and_dynamic.update(
            "sym",
            pd.DataFrame({"col": [1, 2]}, dtype=float_dtype, index=self.update_index())
        )
        assert_frame_equal(
            lmdb_version_store_static_and_dynamic.read("sym").data,
            pd.DataFrame(
                {"col": [float("NaN"), 1, 2, float("NaN")]},
                dtype=float_dtype,
                index=self.index()
            )
        )
        assert_frame_equal(
            lmdb_version_store_static_and_dynamic.read(
                "sym",
                date_range=(pd.to_datetime("1/1/2024"), pd.to_datetime("1/1/2024"))
            ).data,
            pd.DataFrame({"col": [float("NaN")]}, dtype=float_dtype, index=[pd.to_datetime("1/1/2024")])
        )

    def test_bool(self, lmdb_version_store_static_and_dynamic, boolean_dtype):
        # Note: if dtype is bool pandas will convert None to False
        lmdb_version_store_static_and_dynamic.update(
            "sym",
            pd.DataFrame({"col": [True, False]}, dtype=boolean_dtype, index=self.update_index())
        )
        assert_frame_equal(
            lmdb_version_store_static_and_dynamic.read("sym").data,
            pd.DataFrame(
                {"col": [None, True, False, None]},
                dtype=boolean_dtype,
                index=self.index()
            )
        )
        assert_frame_equal(
            lmdb_version_store_static_and_dynamic.read(
                "sym",
                date_range=(pd.to_datetime("1/1/2024"), pd.to_datetime("1/1/2024"))
            ).data,
            pd.DataFrame({"col": [None]}, dtype=boolean_dtype, index=[pd.to_datetime("1/1/2024")])
        )

    def test_string(self, lmdb_version_store_static_and_dynamic):
        lmdb_version_store_static_and_dynamic.update(
            "sym",
            pd.DataFrame({"col": ["a", 20*"long_string"]}, index=self.update_index())
        )
        assert_frame_equal(
            lmdb_version_store_static_and_dynamic.read("sym").data,
            pd.DataFrame(
                {"col": [None, "a", 20*"long_string", None]},
                index=self.index()
            )
        )
        assert_frame_equal(
            lmdb_version_store_static_and_dynamic.read(
                "sym",
                date_range=(pd.to_datetime("1/1/2024"), pd.to_datetime("1/1/2024"))
            ).data,
            pd.DataFrame({"col": [None]}, index=[pd.to_datetime("1/1/2024")])
        )

    def test_empty(self, lmdb_version_store_static_and_dynamic):
        lmdb_version_store_static_and_dynamic.update(
            "sym",
            pd.DataFrame({"col": 2*[None]}, index=self.update_index())
        )
        assert_frame_equal(
            lmdb_version_store_static_and_dynamic.read("sym").data,
            pd.DataFrame({"col": 4*[None]}, index=self.index())
        )

    def test_date(self, lmdb_version_store_static_and_dynamic, date_dtype):
        lmdb_version_store_static_and_dynamic.update(
            "sym",
            pd.DataFrame(
                {"col": [np.datetime64('2005-02'), np.datetime64('2005-03')]},
                dtype=date_dtype,
                index=self.update_index()
            )
        )
        assert_frame_equal(
            lmdb_version_store_static_and_dynamic.read("sym").data,
            pd.DataFrame(
                {
                    "col": [
                        np.datetime64('NaT'),
                        np.datetime64('2005-02'),
                        np.datetime64('2005-03'),
                        np.datetime64('NaT')
                    ]
                },
                index=self.index(),
                dtype=date_dtype
            )
        )
        assert_frame_equal(
            lmdb_version_store_static_and_dynamic.read(
                "sym",
                date_range=(pd.to_datetime("1/1/2024"), pd.to_datetime("1/1/2024"))
            ).data,
            pd.DataFrame(
                {
                    "col": [np.datetime64('NaT', 'ns')]
                },
                index=[pd.to_datetime("1/1/2024")],
                dtype=date_dtype
            )
        )


class TestEmptyTypesIsOverriden:
    def test_cannot_append_different_type_after_first_not_none(self, lmdb_version_store_static_and_dynamic):
        lmdb_version_store_static_and_dynamic.write("sym", pd.DataFrame({"col": [None, None]}))
        lmdb_version_store_static_and_dynamic.append("sym", pd.DataFrame({"col": [1, 2, 3]}))
        lmdb_version_store_static_and_dynamic.append("sym", pd.DataFrame({"col": [None, None]}))
        with pytest.raises(Exception):
            lmdb_version_store_static_and_dynamic.append("sym", pd.DataFrame({"col": ["some", "string"]}))