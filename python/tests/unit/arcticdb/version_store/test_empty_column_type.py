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
from packaging.version import Version
from arcticdb.util._versions import PANDAS_VERSION
from arcticdb_ext.exceptions import NormalizationException


class DtypeGenerator:
    """
    Can generate representative subset of all supported dtypes. Can generate by category (e.g. int, float, etc...) or
    all. Generating the full set of dtypes leads to combinatoric explosion in the number of test cases.
    """

    @staticmethod
    def int_dtype():
        return ["int32", "uint64"]

    @staticmethod
    def float_dtype():
        return ["float64"]

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


@pytest.fixture(
    params=[pd.RangeIndex(0, 0), pd.DatetimeIndex([]), pd.MultiIndex.from_arrays([[], []], names=["a", "b"])]
)
def empty_index(request):
    yield request.param


def test_simple_empty_column(lmdb_version_store_empty_types_v1):
    lib = lmdb_version_store_empty_types_v1
    df = pd.DataFrame({"col": 2 * [None]})
    lib.write("sym", df)
    vit = lib.read("sym")
    assert_frame_equal(vit.data, df)


def test_integer_simple(lmdb_version_store_empty_types_v1):
    lib = lmdb_version_store_empty_types_v1
    lib.write("sym", pd.DataFrame({"col": 2 * [None]}))
    int_dtype = "int16"
    df_non_empty = pd.DataFrame({"col": np.array([1, 2, 3], dtype=int_dtype)})
    lib.append("sym", df_non_empty)
    expected_result = pd.DataFrame({"col": np.array([0, 0, 1, 2, 3], dtype=int_dtype)})
    assert_frame_equal(lib.read("sym").data, expected_result)
    assert_frame_equal(lib.read("sym", row_range=[0, 2]).data, pd.DataFrame({"col": np.array([0, 0], dtype=int_dtype)}))
    assert_frame_equal(lib.read("sym", row_range=[2, 5]).data, df_non_empty)


def test_integer_simple_dynamic(lmdb_version_store_empty_types_dynamic_schema_v1):
    lib = lmdb_version_store_empty_types_dynamic_schema_v1
    lib.write("sym", pd.DataFrame({"col": 2 * [None]}))
    int_dtype = "int16"
    df_non_empty = pd.DataFrame({"col": np.array([1, 2, 3], dtype=int_dtype)})
    lib.append("sym", df_non_empty)
    expected_result = pd.DataFrame({"col": np.array([0, 0, 1, 2, 3], dtype=int_dtype)})
    assert_frame_equal(lib.read("sym").data, expected_result)
    assert_frame_equal(lib.read("sym", row_range=[0, 2]).data, pd.DataFrame({"col": np.array([0, 0], dtype=int_dtype)}))
    assert_frame_equal(lib.read("sym", row_range=[2, 5]).data, df_non_empty)


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
        df_non_empty = pd.DataFrame({"col": np.array([1, 2, 3], dtype=int_dtype)})
        lmdb_version_store_static_and_dynamic.append("sym", df_non_empty)
        expected_result = pd.DataFrame({"col": np.array([0, 0, 1, 2, 3], dtype=int_dtype)})
        assert_frame_equal(lmdb_version_store_static_and_dynamic.read("sym").data, expected_result)
        assert_frame_equal(
            lmdb_version_store_static_and_dynamic.read("sym", row_range=[0, 2]).data,
            pd.DataFrame({"col": np.array([0, 0], dtype=int_dtype)}),
        )
        assert_frame_equal(lmdb_version_store_static_and_dynamic.read("sym", row_range=[2, 5]).data, df_non_empty)

    def test_float(self, lmdb_version_store_static_and_dynamic, float_dtype):
        df_non_empty = pd.DataFrame({"col": np.array([1, 2, 3], dtype=float_dtype)})
        lmdb_version_store_static_and_dynamic.append("sym", df_non_empty)
        expected_result = pd.DataFrame({"col": np.array([float("NaN"), float("NaN"), 1, 2, 3], dtype=float_dtype)})
        assert_frame_equal(lmdb_version_store_static_and_dynamic.read("sym").data, expected_result)
        assert_frame_equal(
            lmdb_version_store_static_and_dynamic.read("sym", row_range=[0, 2]).data,
            pd.DataFrame({"col": np.array([float("NaN"), float("NaN")], dtype=float_dtype)}),
        )
        assert_frame_equal(lmdb_version_store_static_and_dynamic.read("sym", row_range=[2, 5]).data, df_non_empty)

    def test_bool(self, lmdb_version_store_static_and_dynamic, boolean_dtype):
        # Note: if dtype is bool pandas will convert None to False
        df_non_empty = pd.DataFrame({"col": np.array([True, False, True], dtype=boolean_dtype)})
        lmdb_version_store_static_and_dynamic.append("sym", df_non_empty)
        expected_result = pd.DataFrame({"col": np.array([None, None, True, False, True], dtype=boolean_dtype)})
        assert_frame_equal(lmdb_version_store_static_and_dynamic.read("sym").data, expected_result)
        assert_frame_equal(
            lmdb_version_store_static_and_dynamic.read("sym", row_range=[0, 2]).data,
            pd.DataFrame({"col": np.array([None, None], dtype=boolean_dtype)}),
        )
        assert_frame_equal(lmdb_version_store_static_and_dynamic.read("sym", row_range=[2, 5]).data, df_non_empty)

    def test_empty(self, lmdb_version_store_static_and_dynamic):
        df_non_empty = pd.DataFrame({"col": np.array([None, None, None])})
        lmdb_version_store_static_and_dynamic.append("sym", df_non_empty)
        expected_result = pd.DataFrame({"col": np.array([None, None, None, None, None])})
        assert_frame_equal(lmdb_version_store_static_and_dynamic.read("sym").data, expected_result)
        assert_frame_equal(
            lmdb_version_store_static_and_dynamic.read("sym", row_range=[0, 2]).data,
            pd.DataFrame({"col": np.array([None, None])}),
        )
        assert_frame_equal(lmdb_version_store_static_and_dynamic.read("sym", row_range=[2, 5]).data, df_non_empty)

    def test_string(self, lmdb_version_store_static_and_dynamic):
        df_non_empty = pd.DataFrame({"col": np.array(["some_string", "long_string" * 100])})
        lmdb_version_store_static_and_dynamic.append("sym", df_non_empty)
        expected_result = pd.DataFrame({"col": np.array([None, None, "some_string", "long_string" * 100])})
        assert_frame_equal(lmdb_version_store_static_and_dynamic.read("sym").data, expected_result)
        assert_frame_equal(
            lmdb_version_store_static_and_dynamic.read("sym", row_range=[0, 2]).data,
            pd.DataFrame({"col": np.array([None, None])}),
        )
        assert_frame_equal(lmdb_version_store_static_and_dynamic.read("sym", row_range=[2, 5]).data, df_non_empty)

    def test_date(self, lmdb_version_store_static_and_dynamic, date_dtype):
        df_non_empty = pd.DataFrame(
            {"col": np.array([np.datetime64("2005-02"), np.datetime64("2005-03"), np.datetime64("2005-03")])},
            dtype=date_dtype,
        )
        lmdb_version_store_static_and_dynamic.append("sym", df_non_empty)
        expected_result = pd.DataFrame(
            {
                "col": np.array(
                    [
                        np.datetime64("NaT"),
                        np.datetime64("NaT"),
                        np.datetime64("2005-02"),
                        np.datetime64("2005-03"),
                        np.datetime64("2005-03"),
                    ],
                    dtype=date_dtype,
                )
            }
        )
        assert_frame_equal(lmdb_version_store_static_and_dynamic.read("sym").data, expected_result)
        assert_frame_equal(
            lmdb_version_store_static_and_dynamic.read("sym", row_range=[0, 2]).data,
            pd.DataFrame({"col": np.array([np.datetime64("NaT"), np.datetime64("NaT")], dtype=date_dtype)}),
        )
        assert_frame_equal(lmdb_version_store_static_and_dynamic.read("sym", row_range=[2, 5]).data, df_non_empty)


class TestCanAppendColumnWithNonesToColumn:
    """
    Tests if it's possible to append column consisted only of None values to another column. The type of the column
    containing only None values should be empty type, thus we should be able to append it to any other column
    regardless of its type. Appending column containing only None values should not change the type of a column. On
    read the None values will be backfilled depending on the overall type of the column.
    """

    def test_integer(self, lmdb_version_store_static_and_dynamic, int_dtype):
        df_initial = pd.DataFrame({"col": np.array([1, 2, 3], dtype=int_dtype), "other": [1, 2, 3]})
        lmdb_version_store_static_and_dynamic.write("sym", df_initial)
        lmdb_version_store_static_and_dynamic.append("sym", pd.DataFrame({"col": [None, None], "other": [4, 5]}))
        expected_df = pd.DataFrame({"col": np.array([1, 2, 3, 0, 0], dtype=int_dtype), "other": [1, 2, 3, 4, 5]})
        assert_frame_equal(lmdb_version_store_static_and_dynamic.read("sym").data, expected_df)
        assert_frame_equal(lmdb_version_store_static_and_dynamic.read("sym", row_range=[0, 3]).data, df_initial)
        assert_frame_equal(
            lmdb_version_store_static_and_dynamic.read("sym", row_range=[3, 5]).data,
            pd.DataFrame({"col": np.array([0, 0], dtype=int_dtype), "other": [4, 5]}),
        )
        # Cannot compare with expected_df.tail(n=1) due to issue #1537: https://github.com/man-group/ArcticDB/issues/1537
        # TODO: Move in a separate test suite testing the processing pipeline
        expected_tail = pd.DataFrame({"col": np.array([0], dtype=int_dtype), "other": [5]})
        assert_frame_equal(lmdb_version_store_static_and_dynamic.tail("sym", n=1).data, expected_tail)
        assert_frame_equal(lmdb_version_store_static_and_dynamic.head("sym", n=1).data, expected_df.head(n=1))

    def test_float(self, lmdb_version_store_static_and_dynamic, float_dtype):
        df_initial = pd.DataFrame({"col": np.array([1, 2, 3], dtype=float_dtype), "other": [1, 2, 3]})
        lmdb_version_store_static_and_dynamic.write("sym", df_initial)
        lmdb_version_store_static_and_dynamic.append("sym", pd.DataFrame({"col": [None, None], "other": [4, 5]}))
        expected_df = pd.DataFrame(
            {"col": np.array([1, 2, 3, float("NaN"), np.nan], dtype=float_dtype), "other": [1, 2, 3, 4, 5]}
        )
        assert_frame_equal(lmdb_version_store_static_and_dynamic.read("sym").data, expected_df)
        assert_frame_equal(lmdb_version_store_static_and_dynamic.read("sym", row_range=[0, 3]).data, df_initial)
        assert_frame_equal(
            lmdb_version_store_static_and_dynamic.read("sym", row_range=[3, 5]).data,
            pd.DataFrame({"col": np.array([np.nan, float("NaN")], dtype=float_dtype), "other": [4, 5]}),
        )
        # Cannot compare with expected_df.tail(n=1) due to issue #1537: https://github.com/man-group/ArcticDB/issues/1537
        # TODO: Move in a separate test suite testing the processing pipeline
        expected_tail = pd.DataFrame({"col": np.array([np.nan], dtype=float_dtype), "other": [5]})
        assert_frame_equal(lmdb_version_store_static_and_dynamic.tail("sym", n=1).data, expected_tail)
        assert_frame_equal(lmdb_version_store_static_and_dynamic.head("sym", n=1).data, expected_df.head(n=1))
        assert_frame_equal(lmdb_version_store_static_and_dynamic.head("sym", n=1).data, expected_df.head(n=1))

    def test_bool(self, lmdb_version_store_static_and_dynamic, boolean_dtype):
        # Note: if dtype is bool pandas will convert None to False
        df_initial = pd.DataFrame({"col": np.array([True, False, True], dtype=boolean_dtype), "other": [1, 2, 3]})
        lmdb_version_store_static_and_dynamic.write("sym", df_initial)
        lmdb_version_store_static_and_dynamic.append(
            "sym", pd.DataFrame({"col": np.array([None, None]), "other": [4, 5]})
        )
        expected_df = pd.DataFrame(
            {"col": np.array([True, False, True, None, None], dtype=boolean_dtype), "other": [1, 2, 3, 4, 5]}
        )
        assert_frame_equal(lmdb_version_store_static_and_dynamic.read("sym").data, expected_df)
        assert_frame_equal(lmdb_version_store_static_and_dynamic.read("sym", row_range=[0, 3]).data, df_initial)
        assert_frame_equal(
            lmdb_version_store_static_and_dynamic.read("sym", row_range=[3, 5]).data,
            pd.DataFrame({"col": np.array([None, None], dtype=boolean_dtype), "other": [4, 5]}),
        )
        # Cannot compare with expected_df.tail(n=1) due to issue #1537: https://github.com/man-group/ArcticDB/issues/1537
        # TODO: Move in a separate test suite testing the processing pipeline
        expected_tail = pd.DataFrame({"col": np.array(None, dtype=boolean_dtype), "other": [5]})
        assert_frame_equal(lmdb_version_store_static_and_dynamic.tail("sym", n=1).data, expected_tail)
        assert_frame_equal(lmdb_version_store_static_and_dynamic.head("sym", n=1).data, expected_df.head(n=1))

    def test_string(self, lmdb_version_store_static_and_dynamic):
        df_initial = pd.DataFrame({"col": np.array(["some_string", "long_string" * 100, ""]), "other": [1, 2, 3]})
        df_with_none = pd.DataFrame({"col": np.array([None, None]), "other": [4, 5]})
        lmdb_version_store_static_and_dynamic.write("sym", df_initial)
        lmdb_version_store_static_and_dynamic.append("sym", df_with_none)
        expected_df = pd.DataFrame(
            {"col": np.array(["some_string", "long_string" * 100, "", None, None]), "other": [1, 2, 3, 4, 5]}
        )
        assert_frame_equal(lmdb_version_store_static_and_dynamic.read("sym").data, expected_df)
        assert_frame_equal(lmdb_version_store_static_and_dynamic.read("sym", row_range=[0, 3]).data, df_initial)
        assert_frame_equal(lmdb_version_store_static_and_dynamic.read("sym", row_range=[3, 5]).data, df_with_none)
        # Cannot compare with expected_df.tail(n=1) due to issue #1537: https://github.com/man-group/ArcticDB/issues/1537
        # TODO: Move in a separate test suite testing the processing pipeline
        expected_tail = pd.DataFrame({"col": np.array([None]), "other": [5]})
        assert_frame_equal(lmdb_version_store_static_and_dynamic.tail("sym", n=1).data, expected_tail)
        assert_frame_equal(lmdb_version_store_static_and_dynamic.head("sym", n=1).data, expected_df.head(n=1))

    def test_date(self, lmdb_version_store_static_and_dynamic, date_dtype):
        df_initial = pd.DataFrame(
            {
                "col": np.array(
                    [np.datetime64("2005-02"), np.datetime64("2005-03"), np.datetime64("2005-03")], dtype=date_dtype
                ),
                "other": [1, 2, 3],
            }
        )
        lmdb_version_store_static_and_dynamic.write("sym", df_initial)
        lmdb_version_store_static_and_dynamic.append(
            "sym", pd.DataFrame({"col": np.array([None, None]), "other": [4, 5]})
        )
        expected_df = pd.DataFrame(
            {
                "col": np.array(
                    [
                        np.datetime64("2005-02"),
                        np.datetime64("2005-03"),
                        np.datetime64("2005-03"),
                        np.datetime64("NaT"),
                        np.datetime64("NaT"),
                    ],
                    dtype=date_dtype,
                ),
                "other": [1, 2, 3, 4, 5],
            }
        )
        assert_frame_equal(lmdb_version_store_static_and_dynamic.read("sym").data, expected_df)
        assert_frame_equal(lmdb_version_store_static_and_dynamic.read("sym", row_range=[0, 3]).data, df_initial)
        assert_frame_equal(
            lmdb_version_store_static_and_dynamic.read("sym", row_range=[3, 5]).data,
            pd.DataFrame(
                {"col": np.array([np.datetime64("NaT"), np.datetime64("NaT")], dtype=date_dtype), "other": [4, 5]}
            ),
        )
        # Cannot compare with expected_df.tail(n=1) due to issue #1537: https://github.com/man-group/ArcticDB/issues/1537
        # TODO: Move in a separate test suite testing the processing pipeline
        expected_tail = pd.DataFrame({"col": np.array([np.datetime64("NaT")], dtype=date_dtype), "other": [5]})
        assert_frame_equal(lmdb_version_store_static_and_dynamic.tail("sym", n=1).data, expected_tail)
        assert_frame_equal(lmdb_version_store_static_and_dynamic.head("sym", n=1).data, expected_df.head(n=1))


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
            "sym", pd.DataFrame({"col": [1, 2]}, dtype=int_dtype, index=self.update_index())
        )
        assert_frame_equal(
            lmdb_version_store_static_and_dynamic.read("sym").data,
            pd.DataFrame({"col": [0, 1, 2, 0]}, dtype=int_dtype, index=self.index()),
        )
        assert_frame_equal(
            lmdb_version_store_static_and_dynamic.read(
                "sym", date_range=(pd.to_datetime("1/1/2024"), pd.to_datetime("1/1/2024"))
            ).data,
            pd.DataFrame({"col": [0]}, dtype=int_dtype, index=[pd.to_datetime("1/1/2024")]),
        )

    def test_float(self, lmdb_version_store_static_and_dynamic, float_dtype):
        lmdb_version_store_static_and_dynamic.update(
            "sym", pd.DataFrame({"col": [1, 2]}, dtype=float_dtype, index=self.update_index())
        )
        assert_frame_equal(
            lmdb_version_store_static_and_dynamic.read("sym").data,
            pd.DataFrame({"col": [float("NaN"), 1, 2, float("NaN")]}, dtype=float_dtype, index=self.index()),
        )
        assert_frame_equal(
            lmdb_version_store_static_and_dynamic.read(
                "sym", date_range=(pd.to_datetime("1/1/2024"), pd.to_datetime("1/1/2024"))
            ).data,
            pd.DataFrame({"col": [float("NaN")]}, dtype=float_dtype, index=[pd.to_datetime("1/1/2024")]),
        )

    def test_bool(self, lmdb_version_store_static_and_dynamic, boolean_dtype):
        # Note: if dtype is bool pandas will convert None to False
        lmdb_version_store_static_and_dynamic.update(
            "sym", pd.DataFrame({"col": [True, False]}, dtype=boolean_dtype, index=self.update_index())
        )
        assert_frame_equal(
            lmdb_version_store_static_and_dynamic.read("sym").data,
            pd.DataFrame({"col": [None, True, False, None]}, dtype=boolean_dtype, index=self.index()),
        )
        assert_frame_equal(
            lmdb_version_store_static_and_dynamic.read(
                "sym", date_range=(pd.to_datetime("1/1/2024"), pd.to_datetime("1/1/2024"))
            ).data,
            pd.DataFrame({"col": [None]}, dtype=boolean_dtype, index=[pd.to_datetime("1/1/2024")]),
        )

    def test_string(self, lmdb_version_store_static_and_dynamic):
        lmdb_version_store_static_and_dynamic.update(
            "sym", pd.DataFrame({"col": ["a", 20 * "long_string"]}, index=self.update_index())
        )
        assert_frame_equal(
            lmdb_version_store_static_and_dynamic.read("sym").data,
            pd.DataFrame({"col": [None, "a", 20 * "long_string", None]}, index=self.index()),
        )
        assert_frame_equal(
            lmdb_version_store_static_and_dynamic.read(
                "sym", date_range=(pd.to_datetime("1/1/2024"), pd.to_datetime("1/1/2024"))
            ).data,
            pd.DataFrame({"col": [None]}, index=[pd.to_datetime("1/1/2024")]),
        )

    def test_empty(self, lmdb_version_store_static_and_dynamic):
        lmdb_version_store_static_and_dynamic.update(
            "sym", pd.DataFrame({"col": 2 * [None]}, index=self.update_index())
        )
        assert_frame_equal(
            lmdb_version_store_static_and_dynamic.read("sym").data,
            pd.DataFrame({"col": 4 * [None]}, index=self.index()),
        )

    def test_date(self, lmdb_version_store_static_and_dynamic, date_dtype):
        lmdb_version_store_static_and_dynamic.update(
            "sym",
            pd.DataFrame(
                {"col": [np.datetime64("2005-02"), np.datetime64("2005-03")]},
                dtype=date_dtype,
                index=self.update_index(),
            ),
        )
        assert_frame_equal(
            lmdb_version_store_static_and_dynamic.read("sym").data,
            pd.DataFrame(
                {
                    "col": [
                        np.datetime64("NaT"),
                        np.datetime64("2005-02"),
                        np.datetime64("2005-03"),
                        np.datetime64("NaT"),
                    ]
                },
                index=self.index(),
                dtype=date_dtype,
            ),
        )
        assert_frame_equal(
            lmdb_version_store_static_and_dynamic.read(
                "sym", date_range=(pd.to_datetime("1/1/2024"), pd.to_datetime("1/1/2024"))
            ).data,
            pd.DataFrame({"col": [np.datetime64("NaT", "ns")]}, index=[pd.to_datetime("1/1/2024")], dtype=date_dtype),
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
            "sym", pd.DataFrame({"col": [1, 2, 3, 4]}, dtype=int_dtype, index=self.index())
        )
        lmdb_version_store_static_and_dynamic.update(
            "sym", pd.DataFrame({"col": [None, None]}, index=self.update_index())
        )
        assert_frame_equal(
            lmdb_version_store_static_and_dynamic.read("sym").data,
            pd.DataFrame({"col": [1, 0, 0, 4]}, index=self.index(), dtype=int_dtype),
        )

    def test_float(self, lmdb_version_store_static_and_dynamic, float_dtype):
        lmdb_version_store_static_and_dynamic.write(
            "sym", pd.DataFrame({"col": [1, 2, 3, 4]}, dtype=float_dtype, index=self.index())
        )
        lmdb_version_store_static_and_dynamic.update(
            "sym", pd.DataFrame({"col": [None, np.nan]}, index=self.update_index())
        )
        assert_frame_equal(
            lmdb_version_store_static_and_dynamic.read("sym").data,
            pd.DataFrame({"col": [1, float("NaN"), np.nan, 4]}, index=self.index(), dtype=float_dtype),
        )

    def test_bool(self, lmdb_version_store_static_and_dynamic, boolean_dtype):
        # Note: if dtype is bool pandas will convert None to False
        lmdb_version_store_static_and_dynamic.write(
            "sym", pd.DataFrame({"col": [True, True, True, True]}, dtype=boolean_dtype, index=self.index())
        )
        lmdb_version_store_static_and_dynamic.update(
            "sym", pd.DataFrame({"col": [None, None]}, dtype=boolean_dtype, index=self.update_index())
        )
        assert_frame_equal(
            lmdb_version_store_static_and_dynamic.read("sym").data,
            pd.DataFrame({"col": [True, None, None, True]}, index=self.index(), dtype=boolean_dtype),
        )

    def test_string(self, lmdb_version_store_static_and_dynamic):
        lmdb_version_store_static_and_dynamic.write(
            "sym", pd.DataFrame({"col": ["a", "longstr" * 20, "b", "longstr" * 20]}, index=self.index())
        )
        lmdb_version_store_static_and_dynamic.update(
            "sym", pd.DataFrame({"col": [None, None]}, index=self.update_index())
        )
        assert_frame_equal(
            lmdb_version_store_static_and_dynamic.read("sym").data,
            pd.DataFrame({"col": ["a", None, None, "longstr" * 20]}, index=self.index()),
        )

    def test_date(self, lmdb_version_store_static_and_dynamic, date_dtype):
        lmdb_version_store_static_and_dynamic.write(
            "sym",
            pd.DataFrame(
                {
                    "col": [
                        np.datetime64("2005-02"),
                        np.datetime64("2005-03"),
                        np.datetime64("2005-04"),
                        np.datetime64("2005-05"),
                    ]
                },
                dtype=date_dtype,
                index=self.index(),
            ),
        )
        lmdb_version_store_static_and_dynamic.update(
            "sym", pd.DataFrame({"col": [None, None]}, index=self.update_index())
        )
        assert_frame_equal(
            lmdb_version_store_static_and_dynamic.read("sym").data,
            pd.DataFrame(
                {
                    "col": [
                        np.datetime64("2005-02"),
                        np.datetime64("NaT"),
                        np.datetime64("NaT"),
                        np.datetime64("2005-05"),
                    ]
                },
                index=self.index(),
            ),
        )


class TestCanAppendToEmptyColumn:
    """
    Tests that it's possible to append to a column which contains no rows. The type of the columns, including the index
    column, is decided after the first append.
    """

    @pytest.fixture(params=[pd.RangeIndex(0, 3), list(pd.date_range(start="1/1/2024", end="1/3/2024"))])
    def append_index(self, request):
        yield request.param

    @pytest.fixture(autouse=True)
    def create_empty_column(self, lmdb_version_store_static_and_dynamic, dtype, empty_index):
        lmdb_version_store_static_and_dynamic.write("sym", pd.DataFrame({"col": []}, dtype=dtype, index=empty_index))
        assert lmdb_version_store_static_and_dynamic.read("sym").data.index.equals(pd.DatetimeIndex([]))
        yield

    def test_integer(self, lmdb_version_store_static_and_dynamic, int_dtype, dtype, append_index):
        df_to_append = pd.DataFrame({"col": [1, 2, 3]}, dtype=int_dtype, index=append_index)
        lmdb_version_store_static_and_dynamic.append("sym", df_to_append)
        assert_frame_equal(lmdb_version_store_static_and_dynamic.read("sym").data, df_to_append)

    def test_float(self, lmdb_version_store_static_and_dynamic, float_dtype, append_index):
        df_to_append = pd.DataFrame({"col": [1.0, 2.0, 3.0]}, dtype=float_dtype, index=append_index)
        lmdb_version_store_static_and_dynamic.append("sym", df_to_append)
        assert_frame_equal(lmdb_version_store_static_and_dynamic.read("sym").data, df_to_append)

    def test_bool(self, lmdb_version_store_static_and_dynamic, boolean_dtype, append_index):
        df_to_append = pd.DataFrame({"col": [True, False, None]}, dtype=boolean_dtype, index=append_index)
        lmdb_version_store_static_and_dynamic.append("sym", df_to_append)
        assert_frame_equal(lmdb_version_store_static_and_dynamic.read("sym").data, df_to_append)

    def test_nones(self, lmdb_version_store_static_and_dynamic, append_index):
        df_to_append = pd.DataFrame({"col": [None, None, None]}, index=append_index)
        lmdb_version_store_static_and_dynamic.append("sym", df_to_append)
        assert_frame_equal(lmdb_version_store_static_and_dynamic.read("sym").data, df_to_append)

    def test_string(self, lmdb_version_store_static_and_dynamic, append_index):
        df_to_append = pd.DataFrame({"col": ["short_string", None, 20 * "long_string"]}, index=append_index)
        lmdb_version_store_static_and_dynamic.append("sym", df_to_append)
        assert_frame_equal(lmdb_version_store_static_and_dynamic.read("sym").data, df_to_append)

    def test_date(self, lmdb_version_store_static_and_dynamic, date_dtype, append_index):
        df_to_append = pd.DataFrame(
            {"col": np.array([np.datetime64("2005-02"), np.datetime64("2005-03"), np.datetime64("2005-03")])},
            dtype=date_dtype,
            index=append_index,
        )
        lmdb_version_store_static_and_dynamic.append("sym", df_to_append)
        assert_frame_equal(lmdb_version_store_static_and_dynamic.read("sym").data, df_to_append)


class TestAppendAndUpdateWithEmptyToColumnDoesNothing:
    """
    Test if it is possible to append/update empty column to an already existing column. The append/update should not
    change anything. If dynamic schema is used the append/update should not create new columns. The append/update
    should not even reach the C++ layer and the version should not be changed.
    """

    @pytest.fixture(params=[pd.RangeIndex(0, 3), list(pd.date_range(start="1/1/2024", end="1/3/2024"))])
    def index(self, request):
        yield request.param

    @pytest.fixture()
    def empty_dataframe(self, empty_index, dtype):
        yield pd.DataFrame({"col": []}, dtype=dtype, index=empty_index)

    @staticmethod
    def assert_append_empty_does_nothing(initial_df, store, empty):
        store.append("sym", empty)
        read_result = store.read("sym")
        assert_frame_equal(read_result.data, initial_df)
        assert read_result.version == 0

    @staticmethod
    def assert_update_empty_does_nothing(initial_df, store, empty):
        store.update("sym", empty)
        read_result = store.read("sym")
        assert_frame_equal(read_result.data, initial_df)
        assert read_result.version == 0

    def test_integer(self, lmdb_version_store_static_and_dynamic, index, int_dtype, empty_dataframe):
        df = pd.DataFrame({"col": [1, 2, 3]}, dtype=int_dtype, index=index)
        lmdb_version_store_static_and_dynamic.write("sym", df)
        self.assert_append_empty_does_nothing(df, lmdb_version_store_static_and_dynamic, empty_dataframe)
        self.assert_update_empty_does_nothing(df, lmdb_version_store_static_and_dynamic, empty_dataframe)

    def test_float(self, lmdb_version_store_static_and_dynamic, index, float_dtype, empty_dataframe):
        df = pd.DataFrame({"col": [1, 2, 3]}, dtype=float_dtype, index=index)
        lmdb_version_store_static_and_dynamic.write("sym", df)
        self.assert_append_empty_does_nothing(df, lmdb_version_store_static_and_dynamic, empty_dataframe)
        self.assert_update_empty_does_nothing(df, lmdb_version_store_static_and_dynamic, empty_dataframe)

    def test_bool(self, lmdb_version_store_static_and_dynamic, index, boolean_dtype, empty_dataframe):
        df = pd.DataFrame({"col": [False, True, None]}, dtype=boolean_dtype, index=index)
        lmdb_version_store_static_and_dynamic.write("sym", df)
        self.assert_append_empty_does_nothing(df, lmdb_version_store_static_and_dynamic, empty_dataframe)
        self.assert_update_empty_does_nothing(df, lmdb_version_store_static_and_dynamic, empty_dataframe)

    def test_nones(self, lmdb_version_store_static_and_dynamic, index, empty_dataframe):
        df = pd.DataFrame({"col": [None, None, None]}, index=index)
        lmdb_version_store_static_and_dynamic.write("sym", df)
        self.assert_append_empty_does_nothing(df, lmdb_version_store_static_and_dynamic, empty_dataframe)
        self.assert_update_empty_does_nothing(df, lmdb_version_store_static_and_dynamic, empty_dataframe)

    @pytest.mark.parametrize("initial_empty_index", [pd.RangeIndex(0, 0), pd.DatetimeIndex([])])
    def test_empty(self, lmdb_version_store_static_and_dynamic, initial_empty_index, empty_dataframe):
        df = pd.DataFrame({"col": []}, index=initial_empty_index)
        lmdb_version_store_static_and_dynamic.write("sym", df)
        self.assert_append_empty_does_nothing(
            lmdb_version_store_static_and_dynamic.read("sym").data,
            lmdb_version_store_static_and_dynamic,
            empty_dataframe,
        )
        self.assert_update_empty_does_nothing(
            lmdb_version_store_static_and_dynamic.read("sym").data,
            lmdb_version_store_static_and_dynamic,
            empty_dataframe,
        )

    def test_string(self, lmdb_version_store_static_and_dynamic, index, empty_dataframe):
        df = pd.DataFrame({"col": ["short", 20 * "long", None]}, index=index)
        lmdb_version_store_static_and_dynamic.write("sym", df)
        self.assert_append_empty_does_nothing(df, lmdb_version_store_static_and_dynamic, empty_dataframe)
        self.assert_update_empty_does_nothing(df, lmdb_version_store_static_and_dynamic, empty_dataframe)

    def test_date(self, lmdb_version_store_static_and_dynamic, date_dtype, index, empty_dataframe):
        df = pd.DataFrame(
            {"col": np.array([np.datetime64("2005-02"), np.datetime64("2005-03"), np.datetime64("2005-03")])},
            dtype=date_dtype,
            index=index,
        )
        lmdb_version_store_static_and_dynamic.write("sym", df)
        self.assert_append_empty_does_nothing(df, lmdb_version_store_static_and_dynamic, empty_dataframe)
        self.assert_update_empty_does_nothing(df, lmdb_version_store_static_and_dynamic, empty_dataframe)

    def test_empty_df_does_not_create_new_columns_in_dynamic_schema(self, lmdb_version_store_dynamic_schema, index):
        df = pd.DataFrame({"col": [1, 2, 3]}, dtype="int32", index=index)
        lmdb_version_store_dynamic_schema.write("sym", df)
        to_append = pd.DataFrame(
            {
                "col_1": np.array([], dtype="int"),
                "col_2": np.array([], dtype="float"),
                "col_3": np.array([], dtype="object"),
                "col_4": np.array([], dtype="str"),
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
        lmdb_version_store_static_and_dynamic.write("sym", pd.DataFrame({"col": []}, dtype=dtype, index=empty_index))
        assert lmdb_version_store_static_and_dynamic.read("sym").data.index.equals(pd.DatetimeIndex([]))
        yield

    def update_index(self):
        return list(pd.date_range(start="1/2/2024", end="1/4/2024"))

    def test_integer(self, lmdb_version_store_static_and_dynamic, int_dtype):
        df = pd.DataFrame({"col": [1, 2, 3]}, index=self.update_index(), dtype=int_dtype)
        lmdb_version_store_static_and_dynamic.update("sym", df)
        assert_frame_equal(lmdb_version_store_static_and_dynamic.read("sym").data, df)

    def test_float(self, lmdb_version_store_static_and_dynamic, float_dtype):
        df = pd.DataFrame({"col": [1, 2, 3]}, index=self.update_index(), dtype=float_dtype)
        lmdb_version_store_static_and_dynamic.update("sym", df)
        assert_frame_equal(lmdb_version_store_static_and_dynamic.read("sym").data, df)

    def test_bool(self, lmdb_version_store_static_and_dynamic, boolean_dtype):
        df = pd.DataFrame({"col": [True, False, None]}, index=self.update_index(), dtype=boolean_dtype)
        lmdb_version_store_static_and_dynamic.update("sym", df)
        assert_frame_equal(lmdb_version_store_static_and_dynamic.read("sym").data, df)

    def test_bool(self, lmdb_version_store_static_and_dynamic):
        df = pd.DataFrame({"col": [None, None, None]}, index=self.update_index())
        lmdb_version_store_static_and_dynamic.update("sym", df)
        assert_frame_equal(lmdb_version_store_static_and_dynamic.read("sym").data, df)

    def test_string(self, lmdb_version_store_static_and_dynamic):
        df = pd.DataFrame({"col": ["short", 20 * "long", None]}, index=self.update_index())
        lmdb_version_store_static_and_dynamic.update("sym", df)
        assert_frame_equal(lmdb_version_store_static_and_dynamic.read("sym").data, df)

    def test_date(self, lmdb_version_store_static_and_dynamic, date_dtype):
        df = pd.DataFrame(
            {"col": np.array([np.datetime64("2005-02"), np.datetime64("2005-03"), np.datetime64("2005-03")])},
            dtype=date_dtype,
            index=self.update_index(),
        )
        lmdb_version_store_static_and_dynamic.update("sym", df)
        assert_frame_equal(lmdb_version_store_static_and_dynamic.read("sym").data, df)


class TestEmptyTypeIsOverriden:
    """
    When an empty column (or a column containing only None) values is initially written it is assigned the empty type.
    The first write/update to change the type determines the actual type of the column. Test that the first non-empty
    append determines the actual type and subsequent appends with different types fail.
    """

    def test_cannot_append_different_type_after_first_not_none(self, lmdb_version_store_static_and_dynamic):
        lmdb_version_store_static_and_dynamic.write("sym", pd.DataFrame({"col": [None, None]}))
        lmdb_version_store_static_and_dynamic.append("sym", pd.DataFrame({"col": [1, 2, 3]}))
        lmdb_version_store_static_and_dynamic.append("sym", pd.DataFrame({"col": [None, None]}))
        with pytest.raises(Exception):
            lmdb_version_store_static_and_dynamic.append("sym", pd.DataFrame({"col": ["some", "string"]}))

    @pytest.mark.parametrize(
        "index, incompatible_index",
        [
            (pd.RangeIndex(0, 3), list(pd.date_range(start="1/1/2024", end="1/3/2024"))),
            (list(pd.date_range(start="1/1/2024", end="1/3/2024")), pd.RangeIndex(0, 3)),
        ],
    )
    def test_cannot_append_different_index_type_after_first_non_empty(
        self, lmdb_version_store_static_and_dynamic, index, incompatible_index
    ):
        lmdb_version_store_static_and_dynamic.write("sym", pd.DataFrame({"col": []}))
        assert lmdb_version_store_static_and_dynamic.read("sym").data.index.equals(pd.DatetimeIndex([]))
        df_to_append_successfuly = pd.DataFrame({"col": [1, 2, 3]}, index=index)
        lmdb_version_store_static_and_dynamic.append("sym", df_to_append_successfuly, validate_index=False)
        assert_frame_equal(lmdb_version_store_static_and_dynamic.read("sym").data, df_to_append_successfuly)
        with pytest.raises(Exception):
            lmdb_version_store_static_and_dynamic.append(
                "sym", pd.DataFrame({"col": [4, 5, 6]}, index=incompatible_index)
            )


class DisabledEmptyIndexBase:
    @classmethod
    def sym(cls):
        return "sym"

    @classmethod
    def roundtrip(cls, dataframe, storage):
        storage.write(cls.sym(), dataframe)
        return storage.read(cls.sym()).data

    @classmethod
    def is_dynamic_schema(cls, storage):
        return storage.lib_cfg().lib_desc.version.write_options.dynamic_schema

    @pytest.fixture(
        scope="function",
        params=(
            "lmdb_version_store_v1",
            "lmdb_version_store_v2",
            "lmdb_version_store_dynamic_schema_v1",
            "lmdb_version_store_dynamic_schema_v2",
        ),
    )
    def lmdb_version_store_static_and_dynamic(self, request):
        yield request.getfixturevalue(request.param)


@pytest.mark.skipif(PANDAS_VERSION < Version("2.0.0"), reason="This tests behavior of Pandas 2 and grater.")
class TestIndexTypeWithEmptyTypeDisabledPands2AndLater(DisabledEmptyIndexBase):

    def test_no_cols(self, lmdb_version_store_static_and_dynamic):
        result = self.roundtrip(pd.DataFrame([]), lmdb_version_store_static_and_dynamic)
        assert result.index.equals(pd.DatetimeIndex([]))

    def test_has_a_column(self, lmdb_version_store_static_and_dynamic):
        result = self.roundtrip(pd.DataFrame({"a": []}), lmdb_version_store_static_and_dynamic)
        assert result.index.equals(pd.DatetimeIndex([]))
        with pytest.raises(NormalizationException):
            lmdb_version_store_static_and_dynamic.append(self.sym(), pd.DataFrame({"a": [1.0]}))
        to_append_successfuly = pd.DataFrame({"a": [1.0]}, index=pd.DatetimeIndex(["01/01/2024"]))
        lmdb_version_store_static_and_dynamic.append(self.sym(), to_append_successfuly)
        assert_frame_equal(lmdb_version_store_static_and_dynamic.read(self.sym()).data, to_append_successfuly)

    def test_explicit_row_range_no_columns(self, lmdb_version_store_static_and_dynamic):
        result = self.roundtrip(
            pd.DataFrame([], index=pd.RangeIndex(start=5, stop=5, step=100)), lmdb_version_store_static_and_dynamic
        )
        assert result.index.equals(pd.DatetimeIndex([]))

    def test_explicit_row_range_with_columns(self, lmdb_version_store_static_and_dynamic):
        result = self.roundtrip(
            pd.DataFrame({"a": []}, index=pd.RangeIndex(start=5, stop=5, step=100)),
            lmdb_version_store_static_and_dynamic,
        )
        assert result.index.equals(pd.DatetimeIndex([]))
        with pytest.raises(Exception):
            lmdb_version_store_static_and_dynamic.append(
                self.sym(), pd.DataFrame({"a": [1.0]}, pd.RangeIndex(start=0, stop=1, step=1))
            )
        to_append_successfuly = pd.DataFrame({"a": [1.0]}, index=pd.DatetimeIndex(["01/01/2024"]))
        lmdb_version_store_static_and_dynamic.append(self.sym(), to_append_successfuly)
        assert_frame_equal(lmdb_version_store_static_and_dynamic.read(self.sym()).data, to_append_successfuly)

    def test_explicit_rowrange_default_step(self, lmdb_version_store_static_and_dynamic):
        result = self.roundtrip(
            pd.DataFrame({"a": []}, index=pd.RangeIndex(start=0, stop=0)), lmdb_version_store_static_and_dynamic
        )
        assert result.index.equals(pd.DatetimeIndex([]))

    def test_explicit_datetime(self, lmdb_version_store_static_and_dynamic):
        result = self.roundtrip(
            pd.DataFrame({"a": []}, index=pd.DatetimeIndex([])), lmdb_version_store_static_and_dynamic
        )
        assert result.index.equals(pd.DatetimeIndex([]))

    @pytest.mark.parametrize(
        "arrays, expected_arrays",
        [
            ([[], []], ([np.array([], dtype="datetime64[ns]"), np.array([], dtype="object")])),
            (
                [np.array([], dtype="float"), np.array([], dtype="int")],
                ([np.array([], dtype="float"), np.array([], dtype="int")]),
            ),
            (
                [np.array([], dtype="int"), np.array([], dtype="float")],
                ([np.array([], dtype="int"), np.array([], "float")]),
            ),
            (
                [np.array([], dtype="datetime64[ns]"), np.array([], dtype="float64")],
                ([np.array([], dtype="datetime64[ns]"), np.array([], dtype="float")]),
            ),
        ],
    )
    def test_multiindex(self, lmdb_version_store_static_and_dynamic, arrays, expected_arrays):
        # When multiindex is used the dtypes are preserved. In case default empty numpy arrays are used like so:
        # pd.MultiIndex.from_arrays([np.array([]), np.array([])], names=["p", "s"]) the result varies depending on
        # numpy's defaults
        input_index = pd.MultiIndex.from_arrays(arrays, names=["p", "s"])
        result = self.roundtrip(pd.DataFrame({"a": []}, index=input_index), lmdb_version_store_static_and_dynamic)
        expected_multiindex = pd.MultiIndex.from_arrays(expected_arrays, names=["p", "s"])
        assert result.index.equals(expected_multiindex)


@pytest.mark.skipif(PANDAS_VERSION >= Version("2.0.0"), reason="This tests only the behavior with Pandas <= 2")
class TestIndexTypeWithEmptyTypeDisabledPands0AndPands1(DisabledEmptyIndexBase):
    @classmethod
    def multiindex_dtypes(cls, index):
        """
        The MultiIndex class in Pandas < 2 does not have dtypes method. This emulates that
        """
        return pd.Series({name: level.dtype for name, level in zip(index.names, index.levels)})

    def test_no_cols(self, lmdb_version_store_static_and_dynamic):
        result = self.roundtrip(pd.DataFrame([]), lmdb_version_store_static_and_dynamic)
        assert result.index.equals(pd.DatetimeIndex([]))

    def test_has_a_column(self, lmdb_version_store_static_and_dynamic):
        result = self.roundtrip(pd.DataFrame({"a": []}), lmdb_version_store_static_and_dynamic)
        assert result.index.equals(pd.RangeIndex(start=0, stop=0, step=1))
        with pytest.raises(NormalizationException):
            lmdb_version_store_static_and_dynamic.append(
                self.sym(), pd.DataFrame({"a": ["a"]}, index=pd.DatetimeIndex(["01/01/2024"]))
            )
        to_append_successfuly = pd.DataFrame({"a": ["a"]})
        lmdb_version_store_static_and_dynamic.append(self.sym(), to_append_successfuly)
        assert_frame_equal(lmdb_version_store_static_and_dynamic.read(self.sym()).data, to_append_successfuly)

    def test_explicit_row_range_no_columns(self, lmdb_version_store_static_and_dynamic):
        result = self.roundtrip(
            pd.DataFrame([], index=pd.RangeIndex(start=5, stop=5, step=100)), lmdb_version_store_static_and_dynamic
        )
        assert result.index.equals(pd.RangeIndex(start=0, stop=0, step=1))

    def test_explicit_row_range_with_columns(self, lmdb_version_store_static_and_dynamic):
        result = self.roundtrip(
            pd.DataFrame({"a": []}, index=pd.RangeIndex(start=5, stop=5, step=100)),
            lmdb_version_store_static_and_dynamic,
        )
        assert result.index.equals(pd.RangeIndex(start=5, stop=5, step=100))
        # Cannot append datetime indexed df to empty rowrange index
        with pytest.raises(NormalizationException):
            lmdb_version_store_static_and_dynamic.append(
                self.sym(), pd.DataFrame({"a": ["a"]}, index=pd.DatetimeIndex(["01/01/2024"]))
            )
        # Cannot append rowrange indexed df if the start of the appended is not matching the stop of the empty
        with pytest.raises(NormalizationException):
            lmdb_version_store_static_and_dynamic.append(
                self.sym(), pd.DataFrame({"a": ["a"]}, index=pd.RangeIndex(start=9, stop=109, step=100))
            )
            lmdb_version_store_static_and_dynamic.append(
                self.sym(), pd.DataFrame({"a": ["a"]}, index=pd.RangeIndex(start=10, stop=110, step=100))
            )
        # Cannot append rowrange indexed df if the step is different
        with pytest.raises(NormalizationException):
            lmdb_version_store_static_and_dynamic.append(
                self.sym(), pd.DataFrame({"a": ["a"]}, index=pd.RangeIndex(start=5, stop=6, step=1))
            )
        to_append_successfuly = pd.DataFrame({"a": ["a"]}, index=pd.RangeIndex(start=5, stop=105, step=100))
        lmdb_version_store_static_and_dynamic.append(self.sym(), to_append_successfuly)
        assert_frame_equal(lmdb_version_store_static_and_dynamic.read(self.sym()).data, to_append_successfuly)

    def test_explicit_rowrange_default_step(self, lmdb_version_store_static_and_dynamic):
        result = self.roundtrip(
            pd.DataFrame({"a": []}, index=pd.RangeIndex(start=0, stop=0)), lmdb_version_store_static_and_dynamic
        )
        assert result.index.equals(pd.RangeIndex(start=0, stop=0, step=1))

    def test_explicit_datetime(self, lmdb_version_store_static_and_dynamic):
        result = self.roundtrip(
            pd.DataFrame({"a": []}, index=pd.DatetimeIndex([])), lmdb_version_store_static_and_dynamic
        )
        assert result.index.equals(pd.DatetimeIndex([]))

    @pytest.mark.parametrize(
        "arrays, expected_arrays",
        [
            ([[], []], ([np.array([], dtype="datetime64[ns]"), np.array([], dtype="object")])),
            (
                [np.array([], dtype="float"), np.array([], dtype="int")],
                ([np.array([], dtype="object"), np.array([], dtype="int")]),
            ),
            (
                [np.array([], dtype="int"), np.array([], dtype="float")],
                ([np.array([], dtype="int"), np.array([], "object")]),
            ),
            (
                [np.array([], dtype="datetime64[ns]"), np.array([], dtype="float64")],
                ([np.array([], dtype="datetime64[ns]"), np.array([], dtype="object")]),
            ),
        ],
    )
    def test_multiindex(self, lmdb_version_store_static_and_dynamic, arrays, expected_arrays):
        # When multiindex is used the dtypes are preserved. In case default empty numpy arrays are used like so:
        # pd.MultiIndex.from_arrays([np.array([]), np.array([])], names=["p", "s"]) the result varies depending on
        # numpy's defaults
        input_index = pd.MultiIndex.from_arrays(arrays, names=["p", "s"])
        result = self.roundtrip(pd.DataFrame({"a": []}, index=input_index), lmdb_version_store_static_and_dynamic)
        expected_multiindex = pd.MultiIndex.from_arrays(expected_arrays, names=["p", "s"])
        assert result.index.equals(expected_multiindex)
