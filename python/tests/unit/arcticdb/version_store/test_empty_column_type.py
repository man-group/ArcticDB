"""
Copyright 2023 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""
import sys
from math import nan
import pandas as pd
from pandas.testing import assert_frame_equal
import numpy as np
import pytest

class DtypeGenerator:
    """
    Class which can generate all dtypes which ArcticDB supports. It can generate them by type category e.g. int, float,
    etc. It can also generate a list of all available dtypes
    """


    @staticmethod
    def int_dtype():
        return [t + s for s in ["8", "16", "32", "64"] for t in ["int", "uint"]]

    @staticmethod
    def float_dtype():
        return ["float" + s for s in ["32", "64"]]
    
    @staticmethod
    def bool_dtype():
        # object is for nullable boolean
        return ["object", "bool"]
    
    @staticmethod
    def date_dtype():
        return ["datetime64[ns]"]
    
    @classmethod
    def dtype(cls):
        # There are no overlaps in dtypes at the moment but since having additional dtype might increase the test count
        # by a lot we err on the safe side and filter the dtypes so that they are unique. Note dtypes are sorted so that
        # the fixture name is deterministic, otherwise the CI might fail to run the tests in parallel.
        return sorted(list(set([*cls.int_dtype(), *cls.float_dtype(), *cls.bool_dtype(), *cls.date_dtype()])))

@pytest.fixture(params=DtypeGenerator.int_dtype())
def int_dtype(request):
    yield request.param

@pytest.fixture(params=DtypeGenerator.float_dtype())
def float_dtype(request):
    yield request.param


@pytest.fixture(params=DtypeGenerator.bool_dtype())
def boolean_dtype(request):
    if request.param == "object":
        pytest.skip("Nullable booleans are temporary disabled")
    yield request.param


@pytest.fixture(params=DtypeGenerator.date_dtype())
def date_dtype(request):
    yield request.param


@pytest.fixture(params=DtypeGenerator.dtype())
def dtype(request):
    yield request.param


@pytest.fixture(params=[pd.RangeIndex(0,0), pd.DatetimeIndex([])])
def empty_index(request):
    yield request.param


class TestCanAppendToColumnWithNones:
    """
    Tests that it is possible to write a column containing None values and latter append to it. Initially the type of
    the column must be empty type after the append the column must be of the same type as the appended data.
    """


    @pytest.fixture(autouse=True)
    def create_empty_column(self, lmdb_version_store_static_and_dynamic):
        lmdb_version_store_static_and_dynamic.write("sym", pd.DataFrame({"col": 2 * [None]}))
        yield

    def test_integer(self, lmdb_version_store_static_and_dynamic, int_dtype):
        df_non_empty = pd.DataFrame({"col": np.array([1,2,3], dtype=int_dtype)})
        lmdb_version_store_static_and_dynamic.append("sym", df_non_empty)
        expected_result = pd.DataFrame({"col": np.array([0,0,1,2,3], dtype=int_dtype)})
        assert_frame_equal(lmdb_version_store_static_and_dynamic.read("sym").data, expected_result)
        assert_frame_equal(
            lmdb_version_store_static_and_dynamic.read("sym", row_range=[0,2]).data,
            pd.DataFrame({"col": np.array([0,0], dtype=int_dtype)})
        )
        assert_frame_equal(
            lmdb_version_store_static_and_dynamic.read("sym", row_range=[2,5]).data,
            df_non_empty
        )

    def test_float(self, lmdb_version_store_static_and_dynamic, float_dtype):
        df_non_empty = pd.DataFrame({"col": np.array([1,2,3], dtype=float_dtype)})
        lmdb_version_store_static_and_dynamic.append("sym", df_non_empty)
        expected_result = pd.DataFrame({"col": np.array([float("NaN"),float("NaN"),1,2,3], dtype=float_dtype)})
        assert_frame_equal(lmdb_version_store_static_and_dynamic.read("sym").data, expected_result)
        assert_frame_equal(
            lmdb_version_store_static_and_dynamic.read("sym", row_range=[0,2]).data,
            pd.DataFrame({"col": np.array([float("NaN"),float("NaN")], dtype=float_dtype)})
        )
        assert_frame_equal(
            lmdb_version_store_static_and_dynamic.read("sym", row_range=[2,5]).data,
            df_non_empty
        )

    def test_bool(self, lmdb_version_store_static_and_dynamic, boolean_dtype):
        # Note: if dtype is bool pandas will convert None to False
        df_non_empty = pd.DataFrame({"col": np.array([True, False, True], dtype=boolean_dtype)})
        lmdb_version_store_static_and_dynamic.append("sym", df_non_empty)
        expected_result = pd.DataFrame({"col": np.array([None, None, True, False, True], dtype=boolean_dtype)})
        assert_frame_equal(lmdb_version_store_static_and_dynamic.read("sym").data, expected_result)
        assert_frame_equal(
            lmdb_version_store_static_and_dynamic.read("sym", row_range=[0,2]).data,
            pd.DataFrame({"col": np.array([None, None], dtype=boolean_dtype)})
        )
        assert_frame_equal(
            lmdb_version_store_static_and_dynamic.read("sym", row_range=[2,5]).data,
            df_non_empty
        )

    def test_empty(self, lmdb_version_store_static_and_dynamic):
        df_non_empty = pd.DataFrame({"col": np.array([None, None, None])})
        lmdb_version_store_static_and_dynamic.append("sym", df_non_empty)
        expected_result = pd.DataFrame({"col": np.array([None, None, None, None, None])})
        assert_frame_equal(lmdb_version_store_static_and_dynamic.read("sym").data, expected_result)
        assert_frame_equal(
            lmdb_version_store_static_and_dynamic.read("sym", row_range=[0,2]).data,
            pd.DataFrame({"col": np.array([None, None])})
        )
        assert_frame_equal(
            lmdb_version_store_static_and_dynamic.read("sym", row_range=[2,5]).data,
            df_non_empty
        )

    def test_string(self, lmdb_version_store_static_and_dynamic):
        df_non_empty = pd.DataFrame({"col": np.array(["some_string", "long_string"*100])})
        lmdb_version_store_static_and_dynamic.append("sym", df_non_empty)
        expected_result = pd.DataFrame({"col": np.array([None, None, "some_string", "long_string"*100])})
        assert_frame_equal(lmdb_version_store_static_and_dynamic.read("sym").data, expected_result)
        assert_frame_equal(
            lmdb_version_store_static_and_dynamic.read("sym", row_range=[0,2]).data,
            pd.DataFrame({"col": np.array([None, None])})
        )
        assert_frame_equal(
            lmdb_version_store_static_and_dynamic.read("sym", row_range=[2,5]).data,
            df_non_empty
        )

    def test_date(self, lmdb_version_store_static_and_dynamic, date_dtype):
        df_non_empty = pd.DataFrame(
            {
                "col": np.array(
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
                "col": np.array(
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
            pd.DataFrame({"col": np.array([np.datetime64('NaT'), np.datetime64('NaT')], dtype=date_dtype)})
        )
        assert_frame_equal(
            lmdb_version_store_static_and_dynamic.read("sym", row_range=[2,5]).data,
            df_non_empty
        )


class TestCanAppendColumnWithNonesToColumn:
    """
    Tests if it's possible to append column consisted only of None values to another column. The type of the column
    containing only None values should be empty type, thus we should be able to append it to any other column
    regardless of its type. Appending column containing only None values should not change the type of a column. On
    read the None values will be backfilled depending on the overall type of the column.
    """


    def test_integer(self, lmdb_version_store_static_and_dynamic, int_dtype):
        df_initial = pd.DataFrame({"col": np.array([1,2,3], dtype=int_dtype)})
        lmdb_version_store_static_and_dynamic.write("sym", df_initial)
        lmdb_version_store_static_and_dynamic.append("sym", pd.DataFrame({"col": [None, None]}))
        assert_frame_equal(
            lmdb_version_store_static_and_dynamic.read("sym").data,
            pd.DataFrame({"col": np.array([1,2,3,0,0], dtype=int_dtype)})
        )
        assert_frame_equal(
            lmdb_version_store_static_and_dynamic.read("sym", row_range=[0,3]).data,
            df_initial
        )
        assert_frame_equal(
            lmdb_version_store_static_and_dynamic.read("sym", row_range=[3,5]).data,
            pd.DataFrame({"col": np.array([0,0], dtype=int_dtype)})
        )

    def test_float(self, lmdb_version_store_static_and_dynamic, float_dtype):
        df_initial = pd.DataFrame({"col": np.array([1,2,3], dtype=float_dtype)})
        lmdb_version_store_static_and_dynamic.write("sym", df_initial)
        lmdb_version_store_static_and_dynamic.append("sym", pd.DataFrame({"col": [None, None]}))
        assert_frame_equal(
            lmdb_version_store_static_and_dynamic.read("sym").data,
            pd.DataFrame({"col": np.array([1,2,3,float("NaN"),float("NaN")], dtype=float_dtype)})
        )
        assert_frame_equal(
            lmdb_version_store_static_and_dynamic.read("sym", row_range=[0,3]).data,
            df_initial
        )
        assert_frame_equal(
            lmdb_version_store_static_and_dynamic.read("sym", row_range=[3,5]).data,
            pd.DataFrame({"col": np.array([float("NaN"), float("NaN")], dtype=float_dtype)})
        )

    def test_bool(self, lmdb_version_store_static_and_dynamic, boolean_dtype):
        # Note: if dtype is bool pandas will convert None to False
        df_initial = pd.DataFrame({"col": np.array([True, False, True], dtype=boolean_dtype)})
        df_with_none = pd.DataFrame({"col": np.array([None, None])})
        lmdb_version_store_static_and_dynamic.write("sym", df_initial)
        lmdb_version_store_static_and_dynamic.append("sym", df_with_none)
        assert_frame_equal(
            lmdb_version_store_static_and_dynamic.read("sym").data,
            pd.DataFrame({"col": np.array([True, False, True, None, None], dtype=boolean_dtype)})
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
        df_initial = pd.DataFrame({"col": np.array(["some_string", "long_string"*100, ""])})
        df_with_none = pd.DataFrame({"col": np.array([None, None])})
        lmdb_version_store_static_and_dynamic.write("sym", df_initial)
        lmdb_version_store_static_and_dynamic.append("sym", df_with_none)
        assert_frame_equal(
            lmdb_version_store_static_and_dynamic.read("sym").data,
            pd.DataFrame({"col": np.array(["some_string", "long_string"*100, "", None, None])})
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
                "col": np.array(
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
        lmdb_version_store_static_and_dynamic.append("sym", pd.DataFrame({"col": np.array([None, None])}))
        expected_result = pd.DataFrame(
            {
                "col": np.array(
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
            pd.DataFrame({"col": np.array([np.datetime64('NaT'), np.datetime64('NaT')], dtype=date_dtype)})
        )


class TestCanUpdateNones:
    """
    Check that a column containing only empty type and date index can be updated and that the type of the column will
    be changed to the type of the new data. The update range is right in the middle of the initial date range, so
    there will be values unaffected by the update. Check that on read the type of the unaffected entries will be
    changed from empty to the new type of the column.
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


class TestCanUpdateWithNone:
    """
    Tests if a subrange of column of any type can be updated with a subrange of None values. The overall type of the
    column should not change. The None values will be backfilled depending on the type (NaN/NaT/None/0).
    """


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
            pd.DataFrame({"col": [None, np.nan]}, index=self.update_index(), dtype=float_dtype)
        )
        assert_frame_equal(
            lmdb_version_store_static_and_dynamic.read("sym").data,
            pd.DataFrame({"col": [1, np.nan, np.nan, 4]}, index=self.index(), dtype=float_dtype)
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

class TestCanAppendToEmptyColumn:
    """
    Tests that it's possible to append to a column which contains no rows. The type of the columns, including the index
    column, is decided after the first append.
    """


    @pytest.fixture(params=
            [
                pytest.param(
                    pd.RangeIndex(0,3),
                    marks=pytest.mark.xfail(
                        reason="Appending row ranged columns to empty columns is not supported yet."
                        "The index of empty df is of type datetime and it clashes with the row-range type."
                        "The expected behavior is to allow this as long as the initial df is of 0 rows.")
                    ),
                list(pd.date_range(start="1/1/2024", end="1/3/2024"))
            ]
    )
    def append_index(self, request):
        yield request.param

    @pytest.fixture(autouse=True)
    def create_empty_column(self, lmdb_version_store_static_and_dynamic, dtype, empty_index):
        if isinstance(empty_index, pd.RangeIndex) and sys.version_info[1] < 9:
            pytest.xfail("""compat-36 and compat-38 tests are failing because this would assign a row-range index to
                         the empty df. This will be fixed when the pandas-agnostic empty index type is added""")
        lmdb_version_store_static_and_dynamic.write("sym", pd.DataFrame({"col": []}, dtype=dtype, index=empty_index))
        yield

    def test_integer(self, lmdb_version_store_static_and_dynamic, int_dtype, empty_index, dtype, append_index):
        df_to_append = pd.DataFrame({"col": [1,2,3]}, dtype=int_dtype, index=append_index)
        lmdb_version_store_static_and_dynamic.append("sym", df_to_append)
        assert_frame_equal(lmdb_version_store_static_and_dynamic.read("sym").data, df_to_append)

    def test_float(self, lmdb_version_store_static_and_dynamic, float_dtype, empty_index, append_index):
        df_to_append = pd.DataFrame({"col": [1.0,2.0,3.0]}, dtype=float_dtype, index=append_index)
        lmdb_version_store_static_and_dynamic.append("sym", df_to_append)
        assert_frame_equal(lmdb_version_store_static_and_dynamic.read("sym").data, df_to_append)

    def test_bool(self, lmdb_version_store_static_and_dynamic, boolean_dtype, empty_index, append_index):
        df_to_append = pd.DataFrame({"col": [True, False, None]}, dtype=boolean_dtype, index=append_index)
        lmdb_version_store_static_and_dynamic.append("sym", df_to_append)
        assert_frame_equal(lmdb_version_store_static_and_dynamic.read("sym").data, df_to_append)

    def test_empty(self, lmdb_version_store_static_and_dynamic, empty_index, append_index):
        df_to_append = pd.DataFrame({"col": [None, None, None]}, index=append_index)
        lmdb_version_store_static_and_dynamic.append("sym", df_to_append)
        assert_frame_equal(lmdb_version_store_static_and_dynamic.read("sym").data, df_to_append)

    def test_string(self, lmdb_version_store_static_and_dynamic, empty_index, append_index):
        df_to_append = pd.DataFrame({"col": ["short_string", None, 20 * "long_string"]}, index=append_index)
        lmdb_version_store_static_and_dynamic.append("sym", df_to_append)
        assert_frame_equal(lmdb_version_store_static_and_dynamic.read("sym").data, df_to_append)

    def test_date(self, lmdb_version_store_static_and_dynamic, date_dtype, empty_index, append_index):
        df_to_append = pd.DataFrame(
            {
                "col": np.array(
                    [
                        np.datetime64('2005-02'),
                        np.datetime64('2005-03'),
                        np.datetime64('2005-03')
                    ]
                )
            },
            dtype=date_dtype,
            index=append_index
        )
        lmdb_version_store_static_and_dynamic.append("sym", df_to_append)
        assert_frame_equal(lmdb_version_store_static_and_dynamic.read("sym").data, df_to_append)


class TestAppendingEmptyToColumnDoesNothing:
    """
    Test if it is possible to append empty column to an already existing column. The append should not change anything.
    If dynamic schema is used the update should not create new columns. The update should not even reach the C++ layer.
    """


    @pytest.fixture(params=[pd.RangeIndex(0,3), list(pd.date_range(start="1/1/2024", end="1/3/2024"))])
    def index(self, request):
        yield request.param

    def test_integer(self, lmdb_version_store_static_and_dynamic, index, int_dtype, empty_index, dtype):
        df = pd.DataFrame({"col": [1,2,3]}, dtype=int_dtype, index=index)
        lmdb_version_store_static_and_dynamic.write("sym", df)
        lmdb_version_store_static_and_dynamic.append("sym", pd.DataFrame({"col": []}, dtype=dtype, index=empty_index))
        read_result = lmdb_version_store_static_and_dynamic.read("sym")
        assert_frame_equal(read_result.data, df)
        assert read_result.version == 0

    def test_float(self, lmdb_version_store_static_and_dynamic, index, float_dtype, empty_index, dtype):
        df = pd.DataFrame({"col": [1,2,3]}, dtype=float_dtype, index=index)
        lmdb_version_store_static_and_dynamic.write("sym", df)
        lmdb_version_store_static_and_dynamic.append("sym", pd.DataFrame({"col": []}, dtype=dtype, index=empty_index))
        read_result = lmdb_version_store_static_and_dynamic.read("sym")
        assert_frame_equal(read_result.data, df)
        assert read_result.version == 0

    def test_bool(self, lmdb_version_store_static_and_dynamic, index, boolean_dtype, empty_index, dtype):
        df = pd.DataFrame({"col": [False, True, None]}, dtype=boolean_dtype, index=index)
        lmdb_version_store_static_and_dynamic.write("sym", df)
        lmdb_version_store_static_and_dynamic.append("sym", pd.DataFrame({"col": []}, dtype=dtype, index=empty_index))
        read_result = lmdb_version_store_static_and_dynamic.read("sym")
        assert_frame_equal(read_result.data, df)
        assert read_result.version == 0

    def test_empty(self, lmdb_version_store_static_and_dynamic, index, empty_index, dtype):
        df = pd.DataFrame({"col": [None, None, None]}, index=index)
        lmdb_version_store_static_and_dynamic.write("sym", df)
        lmdb_version_store_static_and_dynamic.append("sym", pd.DataFrame({"col": []}, dtype=dtype, index=empty_index))
        read_result = lmdb_version_store_static_and_dynamic.read("sym")
        assert_frame_equal(read_result.data, df)
        assert read_result.version == 0

    def test_string(self, lmdb_version_store_static_and_dynamic, index, empty_index, dtype):
        df = pd.DataFrame({"col": ["shord", 20*"long", None]}, index=index)
        lmdb_version_store_static_and_dynamic.write("sym", df)
        lmdb_version_store_static_and_dynamic.append("sym", pd.DataFrame({"col": []}, dtype=dtype, index=empty_index))
        read_result = lmdb_version_store_static_and_dynamic.read("sym")
        assert_frame_equal(read_result.data, df)
        assert read_result.version == 0

    def test_date(self, lmdb_version_store_static_and_dynamic, date_dtype, index, empty_index, dtype):
        df = pd.DataFrame(
            {
                "col": np.array(
                    [
                        np.datetime64('2005-02'),
                        np.datetime64('2005-03'),
                        np.datetime64('2005-03')
                    ]
                )
            }, dtype=date_dtype, index=index)
        lmdb_version_store_static_and_dynamic.write("sym", df)
        lmdb_version_store_static_and_dynamic.append("sym", pd.DataFrame({"col": []}, dtype=dtype, index=empty_index))
        read_result = lmdb_version_store_static_and_dynamic.read("sym")
        assert_frame_equal(read_result.data, df)
        assert read_result.version == 0

    def test_empty_df_does_not_create_new_columns_in_dynamic_schema(self, lmdb_version_store_dynamic_schema, index):
        df = pd.DataFrame({"col": [1,2,3]}, dtype="int32", index=index)
        lmdb_version_store_dynamic_schema.write("sym", df)
        to_append = pd.DataFrame(
            {
                "col_1": np.array([], dtype="int"),
                "col_2": np.array([], dtype="float"),
                "col_3": np.array([], dtype="object"),
                "col_4": np.array([], dtype="str")
            }
        )
        lmdb_version_store_dynamic_schema.append("sym", to_append)
        read_result = lmdb_version_store_dynamic_schema.read("sym")
        assert_frame_equal(read_result.data, df)
        assert read_result.version == 0


class TestCanUpdateEmptyColumn:
    """
    Test if it's possible to update a completely empty dataframe. The index type and the column type will be set after
    the update. Before that the column type must be empty type and the index type is undecided (empty index type).
    """

    @pytest.fixture(autouse=True)
    def create_empty_column(self, lmdb_version_store_static_and_dynamic, dtype, empty_index):
        if isinstance(empty_index, pd.RangeIndex) and sys.version_info[1] < 9:
            pytest.xfail("""compat-36 and compat-38 tests are failing because this would assign a row-range index to
                         the empty df. This will be fixed when the pandas-agnostic empty index type is added""")
        lmdb_version_store_static_and_dynamic.write("sym", pd.DataFrame({"col": []}, dtype=dtype, index=empty_index))
        yield

    def update_index(self):
        return list(pd.date_range(start="1/2/2024", end="1/4/2024"))

    def test_integer(self, lmdb_version_store_static_and_dynamic, int_dtype):
        df = pd.DataFrame({"col": [1,2,3]}, index=self.update_index(), dtype=int_dtype)
        lmdb_version_store_static_and_dynamic.update("sym", df)
        assert_frame_equal(lmdb_version_store_static_and_dynamic.read("sym").data, df)

    def test_float(self, lmdb_version_store_static_and_dynamic, float_dtype):
        df = pd.DataFrame({"col": [1,2,3]}, index=self.update_index(), dtype=float_dtype)
        lmdb_version_store_static_and_dynamic.update("sym", df)
        assert_frame_equal(lmdb_version_store_static_and_dynamic.read("sym").data, df)

    def test_bool(self, lmdb_version_store_static_and_dynamic, boolean_dtype):
        df = pd.DataFrame({"col": [True,False,None]}, index=self.update_index(), dtype=boolean_dtype)
        lmdb_version_store_static_and_dynamic.update("sym", df)
        assert_frame_equal(lmdb_version_store_static_and_dynamic.read("sym").data, df)

    def test_bool(self, lmdb_version_store_static_and_dynamic):
        df = pd.DataFrame({"col": [None,None,None]}, index=self.update_index())
        lmdb_version_store_static_and_dynamic.update("sym", df)
        assert_frame_equal(lmdb_version_store_static_and_dynamic.read("sym").data, df)

    def test_string(self, lmdb_version_store_static_and_dynamic):
        df = pd.DataFrame({"col": ["short",20*"long",None]}, index=self.update_index())
        lmdb_version_store_static_and_dynamic.update("sym", df)
        assert_frame_equal(lmdb_version_store_static_and_dynamic.read("sym").data, df)

    def test_date(self, lmdb_version_store_static_and_dynamic, date_dtype):
        df = pd.DataFrame(
            {
                "col": np.array(
                    [
                        np.datetime64('2005-02'),
                        np.datetime64('2005-03'),
                        np.datetime64('2005-03')
                    ]
                )
            }, dtype=date_dtype, index=self.update_index()
        )
        lmdb_version_store_static_and_dynamic.update("sym", df)
        assert_frame_equal(lmdb_version_store_static_and_dynamic.read("sym").data, df)

class TestEmptyTypesIsOverriden:
    def test_cannot_append_different_type_after_first_not_none(self, lmdb_version_store_static_and_dynamic):
        lmdb_version_store_static_and_dynamic.write("sym", pd.DataFrame({"col": [None, None]}))
        lmdb_version_store_static_and_dynamic.append("sym", pd.DataFrame({"col": [1, 2, 3]}))
        lmdb_version_store_static_and_dynamic.append("sym", pd.DataFrame({"col": [None, None]}))
        with pytest.raises(Exception):
            lmdb_version_store_static_and_dynamic.append("sym", pd.DataFrame({"col": ["some", "string"]}))