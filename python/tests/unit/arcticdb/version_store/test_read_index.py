"""
Copyright 2024 Man Group Operations Limited
Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt. As of the
Change Date specified in that file, in accordance with the Business Source License, use of this software will be
governed by the Apache License, version 2.0.
"""

import pandas as pd
import numpy as np
import arcticdb
import pytest
from datetime import datetime
from functools import reduce
from packaging.version import Version
from arcticdb.encoding_version import EncodingVersion
from arcticdb.util._versions import PANDAS_VERSION
from arcticdb_ext.exceptions import UserInputException
from arcticdb.util.test import CustomThing, TestCustomNormalizer
from arcticdb.version_store._custom_normalizers import register_normalizer, clear_registered_normalizers
from arcticdb.options import LibraryOptions
from arcticdb import ReadRequest
from arcticdb.util.test import assert_frame_equal


@pytest.fixture(
    scope="function",
    params=(
        pd.RangeIndex(start=0, stop=10),
        pd.RangeIndex(start=0, stop=10, step=2),
        pd.RangeIndex(start=5, stop=25, step=5),
        pd.date_range(start="01/01/2024", end="01/10/2024"),
        pd.MultiIndex.from_arrays(
            [pd.date_range(start="01/01/2024", end="01/10/2024"), pd.RangeIndex(start=0, stop=10)],
            names=["datetime", "level"]
        )
    )
)
def index(request):
    yield request.param


class TestBasicReadIndex:

    @pytest.mark.parametrize("dynamic_schema", [False, True])
    def test_read_index_columns(self, lmdb_storage, index, lib_name, dynamic_schema):
        ac = lmdb_storage.create_arctic()
        lib = ac.create_library(lib_name, LibraryOptions(dynamic_schema=dynamic_schema))
        df = pd.DataFrame({"col": range(0, len(index))}, index=index)
        lib.write("sym", df)
        result = lib.read("sym", columns=[])
        assert result.data.index.equals(index)
        assert result.data.empty

    @pytest.mark.parametrize("dynamic_schema", [False, True])
    def test_read_index_column_and_row_slice(self, lmdb_storage, index, lib_name, dynamic_schema):
        col1 = list(range(0, len(index)))
        col2 = [2 * i for i in range(0, len(index))]
        df = pd.DataFrame({"col": col1, "col2": col2, "col3": col1}, index=index)
        ac = lmdb_storage.create_arctic()
        lib = ac.create_library(lib_name, LibraryOptions(dynamic_schema=dynamic_schema, rows_per_segment=5,
                                                         columns_per_segment=2))
        lib.write("sym", df)
        result = lib.read("sym", columns=[])
        assert result.data.index.equals(index)
        assert result.data.empty

    @pytest.mark.parametrize("dynamic_schema", [False, True])
    @pytest.mark.parametrize("n", [3, -3])
    def test_read_index_columns_head(self, lmdb_storage, index, lib_name, dynamic_schema, n):
        ac = lmdb_storage.create_arctic()
        lib = ac.create_library(lib_name, LibraryOptions(dynamic_schema=dynamic_schema))
        lib.write("sym", pd.DataFrame({"col": range(0, len(index))}, index=index))
        result = lib.head("sym", columns=[], n=n)
        assert result.data.index.equals(index[:n])
        assert result.data.empty

    @pytest.mark.parametrize("dynamic_schema", [False, True])
    @pytest.mark.parametrize("n", [3, -3])
    def test_read_index_columns_tail(self, lmdb_storage, index, lib_name, dynamic_schema, n):
        ac = lmdb_storage.create_arctic()
        lib = ac.create_library(lib_name, LibraryOptions(dynamic_schema=dynamic_schema))
        lib.write("sym", pd.DataFrame({"col": range(0, len(index))}, index=index))
        result = lib.tail("sym", columns=[], n=n)
        assert result.data.index.equals(index[-n:])
        assert result.data.empty


class TestReadEmptyIndex:
    @pytest.mark.parametrize("dynamic_schema", [False, True])
    def test_empty_range_index(self, lmdb_storage, lib_name, dynamic_schema):
        ac = lmdb_storage.create_arctic()
        lib = ac.create_library(lib_name, LibraryOptions(dynamic_schema=dynamic_schema))
        lib.write("sym", pd.DataFrame({"col": []}, index=pd.RangeIndex(start=5, stop=5)))
        result = lib.read("sym", columns=[])
        if PANDAS_VERSION < Version("2.0.0"):
            assert result.data.index.equals(pd.RangeIndex(start=0, stop=0, step=1))
        else:
            assert result.data.index.equals(pd.DatetimeIndex([]))
        assert result.data.empty
        assert result.data.index.equals(lib.read("sym").data.index)

    @pytest.mark.parametrize("dynamic_schema", [False, True])
    def test_empty_datetime_index(self, lmdb_storage, lib_name, dynamic_schema):
        ac = lmdb_storage.create_arctic()
        lib = ac.create_library(lib_name, LibraryOptions(dynamic_schema=dynamic_schema))
        lib.write("sym", pd.DataFrame({"col": []}, index=pd.DatetimeIndex([])))
        result = lib.read("sym", columns=[])
        assert result.data.index.equals(pd.DatetimeIndex([]))
        assert result.data.empty
        assert result.data.index.equals(lib.read("sym").data.index)

    @pytest.mark.parametrize(
        "input_index,expected_index",
        [
            pytest.param(
                pd.MultiIndex.from_arrays([[], np.array([], dtype="int"), np.array([], dtype="float"), []]),
                pd.MultiIndex.from_arrays([
                    np.array([], dtype="datetime64[ns]"),
                    np.array([], dtype="int"),
                    np.array([], dtype="float"),
                    np.array([], dtype="object")
                ]),
                marks=pytest.mark.skipif(PANDAS_VERSION < Version("2.0.0"),
                                         reason="This tests behavior of Pandas 2 and grater.")
            ),
            pytest.param(
                pd.MultiIndex.from_arrays([[], np.array([], dtype="int"), np.array([], dtype="float"), []]),
                pd.MultiIndex.from_arrays([
                    np.array([], dtype="datetime64[ns]"),
                    np.array([], dtype="int"),
                    np.array([], dtype="float"),
                    np.array([], dtype="float")
                ]),
                marks=pytest.mark.skipif(PANDAS_VERSION >= Version("2.0.0"),
                                         reason="This tests only the behavior with Pandas <= 2")
            )
        ]
    )
    @pytest.mark.parametrize("dynamic_schema", [False, True])
    def test_empty_multiindex(self, lmdb_storage, lib_name, dynamic_schema, input_index, expected_index):
        ac = lmdb_storage.create_arctic()
        lib = ac.create_library(lib_name, LibraryOptions(dynamic_schema=dynamic_schema))
        lib.write("sym", pd.DataFrame({"col_0": [], "col_1": []}, index=input_index))
        result = lib.read("sym", columns=[])
        assert result.data.index.equals(expected_index)
        assert result.data.empty


class TestReadIndexAsOf:
    @pytest.mark.parametrize("indexes", [
        [
            pd.date_range(start="01/01/2024", end="01/10/2024"),
            pd.date_range(start="01/11/2024", end="01/15/2024"),
            pd.date_range(start="01/22/2024", end="01/30/2024")
        ],
        [
            pd.RangeIndex(start=0, stop=10),
            pd.RangeIndex(start=10, stop=15),
            pd.RangeIndex(start=15, stop=22)
        ],
        [
            pd.MultiIndex.from_arrays(
                [pd.date_range(start="01/01/2024", end="01/10/2024"), pd.RangeIndex(start=0, stop=10)],
                names=["datetime", "level"]
            ),
            pd.MultiIndex.from_arrays(
                [pd.date_range(start="01/11/2024", end="01/21/2024"), pd.RangeIndex(start=10, stop=21)],
                names=["datetime", "level"]
            )
        ]
    ])
    @pytest.mark.parametrize("dynamic_schema", [False, True])
    def test_as_of_version(self, lmdb_storage, lib_name, dynamic_schema, indexes):
        data = [list(range(0, len(index))) for index in indexes]
        ac = lmdb_storage.create_arctic()
        lib = ac.create_library(lib_name, LibraryOptions(dynamic_schema=dynamic_schema))
        lib.write("sym", pd.DataFrame({"col": data[0]}, index=indexes[0]))
        for i in range(1, len(indexes)):
            lib.append("sym", pd.DataFrame({"col": data[i]}, index=indexes[i]))
        for i in range(0, len(indexes)):
            read_index_result = lib.read("sym", columns=[], as_of=i)
            assert read_index_result.data.index.equals(reduce(lambda current, new: current.append(new), indexes[:i+1]))
            assert read_index_result.data.empty

    @pytest.mark.parametrize("index", [
        pd.RangeIndex(start=0, stop=5),
        pd.date_range(start="01/01/2024", end="01/5/2024"),
        pd.MultiIndex.from_arrays(
            [pd.date_range(start="01/11/2024", end="01/21/2024"), pd.RangeIndex(start=10, stop=21)],
            names=["datetime", "level"]
        )
    ])
    @pytest.mark.parametrize("dynamic_schema", [False, True])
    def test_as_of_snapshot(self, lmdb_storage, lib_name, dynamic_schema, index):
        data = list(range(0, len(index)))
        ac = lmdb_storage.create_arctic()
        lib = ac.create_library(lib_name, LibraryOptions(dynamic_schema=dynamic_schema))
        lib.write("sym", pd.DataFrame({"col": data}, index=index))
        lib.snapshot("snap")
        lib.write("sym", pd.DataFrame({"col": [1]}, index=pd.RangeIndex(start=100, stop=101)))
        result = lib.read("sym", as_of="snap", columns=[])
        assert result.data.index.equals(index)
        assert result.data.empty


class TestReadIndexRange:
    @pytest.mark.parametrize("dynamic_schema", [False, True])
    def test_row_range(self, lmdb_storage, lib_name, dynamic_schema, index):
        row_range = (1, 3)
        ac = lmdb_storage.create_arctic()
        lib = ac.create_library(lib_name, LibraryOptions(dynamic_schema=dynamic_schema))
        lib.write("sym", pd.DataFrame({"col": list(range(0, len(index)))}, index=index))
        result = lib.read("sym", row_range=row_range, columns=[])
        assert result.data.index.equals(index[row_range[0]:row_range[1]])
        assert result.data.empty

    @pytest.mark.parametrize("dynamic_schema", [False, True])
    def test_date_range(self, lmdb_storage, lib_name, dynamic_schema):
        index = pd.date_range(start="01/01/2024", end="01/10/2024")
        ac = lmdb_storage.create_arctic()
        lib = ac.create_library(lib_name, LibraryOptions(dynamic_schema=dynamic_schema))
        lib.write("sym", pd.DataFrame({"col": list(range(0, len(index)))}, index=index))
        result = lib.read("sym", date_range=(datetime(2024, 1, 4), datetime(2024, 1, 8)), columns=[])
        assert result.data.index.equals(pd.date_range(start="01/04/2024", end="01/08/2024"))
        assert result.data.empty

    @pytest.mark.parametrize("dynamic_schema", [False, True])
    def test_date_range_left_open(self, lmdb_storage, lib_name, dynamic_schema):
        ac = lmdb_storage.create_arctic()
        lib = ac.create_library(lib_name, LibraryOptions(dynamic_schema=dynamic_schema))
        index = pd.date_range(start="01/01/2024", end="01/10/2024")
        lib.write("sym", pd.DataFrame({"col": list(range(0, len(index)))}, index=index))
        result = lib.read("sym", date_range=(None, datetime(2024, 1, 8)), columns=[])
        assert result.data.index.equals(pd.date_range(start="01/01/2024", end="01/08/2024"))
        assert result.data.empty

    @pytest.mark.parametrize("dynamic_schema", [False, True])
    def test_date_range_right_open(self, lmdb_storage, lib_name, dynamic_schema):
        ac = lmdb_storage.create_arctic()
        lib = ac.create_library(lib_name, LibraryOptions(dynamic_schema=dynamic_schema))
        index = pd.date_range(start="01/01/2024", end="01/10/2024")
        lib.write("sym", pd.DataFrame({"col": list(range(0, len(index)))}, index=index))
        result = lib.read("sym", date_range=(datetime(2024, 1, 4), None), columns=[])
        assert result.data.index.equals(pd.date_range(start="01/04/2024", end="01/10/2024"))
        assert result.data.empty

    @pytest.mark.parametrize("dynamic_schema", [False, True])
    def test_row_range_across_row_slices(self, lmdb_storage, lib_name, dynamic_schema, index):
        ac = lmdb_storage.create_arctic()
        lib = ac.create_library(lib_name, LibraryOptions(dynamic_schema=dynamic_schema, rows_per_segment=5,
                                                         columns_per_segment=2))
        row_range = (3, 8)
        lib.write("sym", pd.DataFrame({"col": range(0, len(index))}, index=index))
        result = lib.read("sym", row_range=row_range, columns=[])
        assert result.data.index.equals(index[row_range[0]:row_range[1]])
        assert result.data.empty

    @pytest.mark.parametrize("non_datetime_index", [
        pd.RangeIndex(start=0, stop=5),
        pd.MultiIndex.from_arrays(
            [pd.RangeIndex(start=10, stop=21), pd.date_range(start="01/11/2024", end="01/21/2024")],
            names=["range", "date"]
        )
    ])
    @pytest.mark.parametrize("dynamic_schema", [False, True])
    def test_date_range_throws(self, lmdb_storage, lib_name, dynamic_schema, non_datetime_index):
        ac = lmdb_storage.create_arctic()
        lib = ac.create_library(lib_name, LibraryOptions(dynamic_schema=dynamic_schema))
        lib.write("sym", pd.DataFrame({"col": list(range(0, len(non_datetime_index)))}, index=non_datetime_index))
        with pytest.raises(Exception):
            lib.read("sym", date_range=(datetime(2024, 1, 4), datetime(2024, 1, 10)), columns=[])


class TestWithNormalizers:

    @pytest.mark.parametrize("dynamic_schema", [False, True])
    def test_recursive_throws(self, lmdb_storage, lib_name, dynamic_schema):
        data = {"a": np.arange(5), "b": np.arange(8)}
        ac = lmdb_storage.create_arctic()
        lib = ac.create_library(lib_name, LibraryOptions(dynamic_schema=dynamic_schema))
        lib._nvs.write("sym_recursive", data, recursive_normalizers=True)
        with pytest.raises(UserInputException) as exception_info:
            lib.read("sym_recursive", columns=[])
        assert "normalizers" in str(exception_info.value)

    @pytest.mark.parametrize("dynamic_schema", [False, True])
    def test_custom_throws(self, lmdb_storage, lib_name, dynamic_schema):
        register_normalizer(TestCustomNormalizer())
        ac = lmdb_storage.create_arctic()
        lib = ac.create_library(lib_name, LibraryOptions(dynamic_schema=dynamic_schema))
        data = CustomThing(custom_columns=["a", "b"], custom_index=[12, 13], custom_values=[[2.0, 4.0], [3.0, 5.0]])
        lib._nvs.write("sym_custom", data)

        with pytest.raises(UserInputException) as exception_info:
            lib.read("sym_custom", columns=[])
        assert "normalizers" in str(exception_info.value)


class TestReadBatch:
    @pytest.mark.parametrize("dynamic_schema", [False, True])
    def test_read_batch(self, lmdb_storage, lib_name, dynamic_schema, index):
        ac = lmdb_storage.create_arctic()
        lib = ac.create_library(lib_name, LibraryOptions(dynamic_schema=dynamic_schema))
        df1 = pd.DataFrame({"a": range(0, len(index))}, index=index)
        df2 = pd.DataFrame({"b": range(0, len(index))})
        df3 = pd.DataFrame({"c": range(0, len(index))}, index=index)
        lib.write("a", df1)
        lib.write("b", df2)
        lib.write("c", df3)
        res = lib.read_batch([ReadRequest("a", columns=[]), ReadRequest("b", columns=[]), ReadRequest("c")])
        assert res[0].data.index.equals(df1.index)
        assert res[0].data.empty
        assert res[1].data.index.equals(df2.index)
        assert res[1].data.empty
        assert_frame_equal(res[2].data, df3)

    @pytest.mark.parametrize("dynamic_schema", [False, True])
    def test_read_batch_row_range(self, lmdb_storage, lib_name, dynamic_schema, index):
        ac = lmdb_storage.create_arctic()
        lib = ac.create_library(lib_name, LibraryOptions(dynamic_schema=dynamic_schema))
        df1 = pd.DataFrame({"a": range(0, len(index))}, index=index)
        df2 = pd.DataFrame({"b": range(0, len(index))})
        df3 = pd.DataFrame({"c": range(0, len(index))}, index=index)
        lib.write("a", df1)
        lib.write("b", df2)
        lib.write("c", df3)
        res = lib.read_batch([ReadRequest("a", columns=[], row_range=(1, 3)), ReadRequest("b", columns=[],
                                                                                          row_range=(4, 5))])
        assert res[0].data.index.equals(df1.index[1:3])
        assert res[0].data.empty
        assert res[1].data.index.equals(df2.index[4:5])
        assert res[1].data.empty


class Dummy:
    pass


class TestPickled:
    @pytest.mark.parametrize("dynamic_schema", [False, True])
    def test_throws(self, lmdb_storage, lib_name, dynamic_schema):
        ac = lmdb_storage.create_arctic()
        lib = ac.create_library(lib_name, LibraryOptions(dynamic_schema=dynamic_schema))
        lib.write_pickle("sym_recursive", pd.DataFrame({"col": [Dummy(), Dummy()]}))
        with pytest.raises(UserInputException) as exception_info:
            lib.read("sym_recursive", columns=[])
        assert "pickled" in str(exception_info.value)


class TestReadIndexV1LibraryNonReg:

    @pytest.mark.parametrize("dynamic_schema", [False, True])
    def test_read(self, version_store_factory, index, dynamic_schema):
        v1_lib = version_store_factory(dynamic_schema=dynamic_schema)
        df = pd.DataFrame({"col": range(0, len(index)), "another": range(0, len(index))}, index=index)
        v1_lib.write("sym", df)
        assert v1_lib.read("sym").data.columns.equals(df.columns)
        assert v1_lib.read("sym", columns=None).data.columns.equals(df.columns)
        assert v1_lib.read("sym", columns=[]).data.columns.equals(df.columns)

    @pytest.mark.parametrize("dynamic_schema", [False, True])
    def test_head(self, version_store_factory, index, dynamic_schema):
        v1_lib = version_store_factory(dynamic_schema=dynamic_schema)
        df = pd.DataFrame({"col": range(0, len(index)), "another": range(0, len(index))}, index=index)
        v1_lib.write("sym", df)
        assert v1_lib.head("sym").data.columns.equals(df.columns)
        assert v1_lib.head("sym", columns=None).data.columns.equals(df.columns)
        assert v1_lib.head("sym", columns=[]).data.columns.equals(df.columns)

    @pytest.mark.parametrize("dynamic_schema", [False, True])
    def test_tail(self, version_store_factory, index, dynamic_schema):
        v1_lib = version_store_factory(dynamic_schema=dynamic_schema)
        df = pd.DataFrame({"col": range(0, len(index)), "another": range(0, len(index))}, index=index)
        v1_lib.write("sym", df)
        assert v1_lib.tail("sym").data.columns.equals(df.columns)
        assert v1_lib.tail("sym", columns=None).data.columns.equals(df.columns)
        assert v1_lib.tail("sym", columns=[]).data.columns.equals(df.columns)

    @pytest.mark.parametrize("dynamic_schema", [False, True])
    def test_read_batch(self, version_store_factory, dynamic_schema, index):
        v1_lib = version_store_factory(dynamic_schema=dynamic_schema)
        df1 = pd.DataFrame({"a": range(0, len(index))}, index=index)
        df2 = pd.DataFrame({"b": range(0, len(index))})
        df3 = pd.DataFrame({"c": range(0, len(index))}, index=index)
        v1_lib.write("a", df1)
        v1_lib.write("b", df2)
        v1_lib.write("c", df3)
        res = v1_lib.batch_read(["a", "b", "c"], columns=[[], None, []])
        assert_frame_equal(res['a'].data, df1)
        assert_frame_equal(res['b'].data, df2)
        assert_frame_equal(res['c'].data, df3)
