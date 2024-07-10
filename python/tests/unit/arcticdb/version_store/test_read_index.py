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
from arcticdb_ext.exceptions import InternalException
from arcticdb.util.test import  CustomThing, TestCustomNormalizer
from arcticdb.version_store._custom_normalizers import register_normalizer, clear_registered_normalizers

@pytest.fixture(
    scope="function",
    params=[
        # (encoding_version, dynamic_schema)
        (EncodingVersion.V1, False),
        (EncodingVersion.V1, True),
        (EncodingVersion.V2, False),
        (EncodingVersion.V2, True),
    ]
)
def lmdb_version_store_row_slice(request, version_store_factory):
    return version_store_factory(
        dynamic_strings=True,
        encoding_version=int(request.param[0]),
        dynamic_schema=request.param[1],
        column_group_size=1,
        segment_row_size=5
    )

@pytest.fixture(
    scope="function",
    params=(
        "lmdb_version_store_v1",
        "lmdb_version_store_v2",
        "lmdb_version_store_dynamic_schema_v1",
        "lmdb_version_store_dynamic_schema_v2",
    ),
)
def lmdb_version_store_static_and_dynamic(request):
    """
    Designed to test all combinations between schema and encoding version for LMDB
    """
    yield request.getfixturevalue(request.param)


@pytest.fixture(
    scope="function",
    params=(
        pd.RangeIndex(start=0, stop=10),
        pd.date_range(start="01/01/2024",end="01/10/2024"),
        pd.MultiIndex.from_arrays(
            [
                pd.date_range(start="01/01/2024", end="01/10/2024"),
                pd.RangeIndex(start=0, stop=10)
            ],
            names=["datetime", "level"]
        )
    )
)
def index(request):
    yield request.param

class TestBasicReadIndex:
    @pytest.mark.parametrize("index", [
        pd.RangeIndex(start=0, stop=10),
        pd.RangeIndex(start=0, stop=10, step=2),
        pd.RangeIndex(start=5, stop=25, step=5),
        pd.date_range(start="01/01/2024",end="01/10/2024"),
        pd.MultiIndex.from_arrays(
            [pd.date_range(start="01/01/2024", end="01/10/2024"), pd.RangeIndex(start=0, stop=10)],
            names=["datetime", "level"]
        )
    ])
    def test_read_index_columns(self, lmdb_version_store_static_and_dynamic, index):
        lmdb_version_store_static_and_dynamic.write("sym", pd.DataFrame({"col": range(0, len(index))}, index=index))
        result = lmdb_version_store_static_and_dynamic.read("sym", columns=[], implement_read_index=True)
        assert isinstance(result, arcticdb.VersionedItem)
        assert result.symbol == "sym"
        assert result.version == 0
        assert result.data.index.equals(index)
        assert result.data.empty

    @pytest.mark.parametrize("staged", [True, False])
    def test_read_index_columns_column_slice(self, lmdb_version_store_row_slice, index, staged):
        col1 = list(range(0, len(index)))
        col2 = [2 * i for i in range(0, len(index))]
        df = pd.DataFrame({"col": col1, "col2": col2}, index=index)
        lmdb_version_store_row_slice.write("sym", df, staged=staged)
        result = lmdb_version_store_row_slice.read("sym", columns=[], implement_read_index=True)
        assert isinstance(result, arcticdb.VersionedItem)
        assert result.symbol == "sym"
        assert result.version == 0
        assert result.data.index.equals(index)
        assert result.data.empty


class TestReadEmptyIndex:
    def test_empty_range_index(self, lmdb_version_store_static_and_dynamic):
        lmdb_version_store_static_and_dynamic.write("sym", pd.DataFrame({"col": []}, index=pd.RangeIndex(start=5,stop=5)))
        result = lmdb_version_store_static_and_dynamic.read("sym", columns=[], implement_read_index=True)
        assert isinstance(result, arcticdb.VersionedItem)
        assert result.symbol == "sym"
        assert result.version == 0
        if PANDAS_VERSION < Version("2.0.0"):
            assert result.data.index.equals(pd.RangeIndex(start=5,stop=5))
        else:
            assert result.data.index.equals(pd.DatetimeIndex([]))
        assert result.data.empty

    def test_empty_datetime_index(self, lmdb_version_store_static_and_dynamic):
        lmdb_version_store_static_and_dynamic.write("sym", pd.DataFrame({"col": []}, index=pd.DatetimeIndex([])))
        result = lmdb_version_store_static_and_dynamic.read("sym", columns=[], implement_read_index=True)
        assert isinstance(result, arcticdb.VersionedItem)
        assert result.symbol == "sym"
        assert result.version == 0
        assert result.data.index.equals(pd.DatetimeIndex([]))
        assert result.data.empty


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
                marks=pytest.mark.skipif(PANDAS_VERSION < Version("2.0.0"), reason="This tests behavior of Pandas 2 and grater.")
            ),
            pytest.param(
                pd.MultiIndex.from_arrays([[], np.array([], dtype="int"), np.array([], dtype="float"), []]),
                pd.MultiIndex.from_arrays([
                    np.array([], dtype="datetime64[ns]"),
                    np.array([], dtype="int"),
                    np.array([], dtype="float"),
                    np.array([], dtype="float")
                ]),
                marks=pytest.mark.skipif(PANDAS_VERSION >= Version("2.0.0"), reason="This tests only the behavior with Pandas <= 2")
            )
        ]
    )
    def test_empty_multiindex(self, lmdb_version_store_static_and_dynamic, input_index, expected_index):
        index = pd.MultiIndex.from_arrays([[], np.array([], dtype="int"), np.array([], dtype="float"), []])
        lmdb_version_store_static_and_dynamic.write("sym", pd.DataFrame({"col_0": [],"col_1": []}, index=input_index))
        result = lmdb_version_store_static_and_dynamic.read("sym", columns=[], implement_read_index=True)
        assert isinstance(result, arcticdb.VersionedItem)
        assert result.symbol == "sym"
        assert result.version == 0
        assert result.data.index.equals(expected_index)
        assert result.data.empty




class TestReadIndexAsOf:
    @pytest.mark.parametrize("indexes", [
        [
            pd.date_range(start="01/01/2024",end="01/10/2024"),
            pd.date_range(start="01/11/2024",end="01/15/2024"),
            pd.date_range(start="01/22/2024",end="01/30/2024")
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
    def test_as_of_version(self, lmdb_version_store_static_and_dynamic, indexes):
        data = [list(range(0, len(index))) for index in indexes]
        lmdb_version_store_static_and_dynamic.write("sym", pd.DataFrame({"col": data[0]}, index=indexes[0]))
        for i in range(1, len(indexes)):
            lmdb_version_store_static_and_dynamic.append("sym", pd.DataFrame({"col": data[i]}, index=indexes[i]))
        for i in range(0, len(indexes)):
            read_index_result = lmdb_version_store_static_and_dynamic.read("sym", columns=[], as_of=i, implement_read_index=True)
            assert isinstance(read_index_result, arcticdb.VersionedItem)
            assert read_index_result.symbol == "sym"
            assert read_index_result.version == i
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
    def test_as_of_snapshot(self, lmdb_version_store_static_and_dynamic, index):
        data = list(range(0, len(index)))
        lmdb_version_store_static_and_dynamic.write("sym", pd.DataFrame({"col": data}, index=index))
        lmdb_version_store_static_and_dynamic.snapshot("snap")
        lmdb_version_store_static_and_dynamic.write("sym", pd.DataFrame({"col": [1]}, index=pd.RangeIndex(start=100, stop=101)))
        result = lmdb_version_store_static_and_dynamic.read("sym", as_of="snap", columns=[], implement_read_index=True)
        assert isinstance(result, arcticdb.VersionedItem)
        assert result.symbol == "sym"
        assert result.version == 0
        assert result.data.index.equals(index)
        assert result.data.empty


class TestReadIndexRange:
    @pytest.mark.parametrize("staged", [True, False])
    def test_row_range(self, lmdb_version_store_static_and_dynamic, index, staged):
        row_range = (1, 3)
        lmdb_version_store_static_and_dynamic.write("sym", pd.DataFrame({"col": list(range(0, len(index)))}, index=index), staged=staged)
        result = lmdb_version_store_static_and_dynamic.read("sym", row_range=row_range, columns=[], implement_read_index=True)
        assert isinstance(result, arcticdb.VersionedItem)
        assert result.symbol == "sym"
        assert result.version == 0
        assert result.data.index.equals(index[row_range[0]:row_range[1]])
        assert result.data.empty

    @pytest.mark.parametrize("staged", [True, False])
    def test_date_range(self, lmdb_version_store_static_and_dynamic, staged):
        index = pd.date_range(start="01/01/2024", end="01/10/2024")
        lmdb_version_store_static_and_dynamic.write("sym", pd.DataFrame({"col": list(range(0, len(index)))}, index=index), staged=staged)
        result = lmdb_version_store_static_and_dynamic.read("sym", date_range=(datetime(2024,1,4), datetime(2024,1,8)), columns=[], implement_read_index=True)
        assert isinstance(result, arcticdb.VersionedItem)
        assert result.symbol == "sym"
        assert result.version == 0
        assert result.data.index.equals(pd.date_range(start="01/04/2024", end="01/08/2024"))
        assert result.data.empty

    def test_date_range_left_open(self, lmdb_version_store_static_and_dynamic):
        index = pd.date_range(start="01/01/2024", end="01/10/2024")
        lmdb_version_store_static_and_dynamic.write("sym", pd.DataFrame({"col": list(range(0, len(index)))}, index=index))
        result = lmdb_version_store_static_and_dynamic.read("sym", date_range=(None, datetime(2024,1,8)), columns=[], implement_read_index=True)
        assert isinstance(result, arcticdb.VersionedItem)
        assert result.symbol == "sym"
        assert result.version == 0
        assert result.data.index.equals(pd.date_range(start="01/01/2024", end="01/08/2024"))
        assert result.data.empty

    def test_date_range_right_open(self, lmdb_version_store_static_and_dynamic):
        index = pd.date_range(start="01/01/2024", end="01/10/2024")
        lmdb_version_store_static_and_dynamic.write("sym", pd.DataFrame({"col": list(range(0, len(index)))}, index=index))
        result = lmdb_version_store_static_and_dynamic.read("sym", date_range=(datetime(2024,1,4), None), columns=[], implement_read_index=True)
        assert isinstance(result, arcticdb.VersionedItem)
        assert result.symbol == "sym"
        assert result.version == 0
        assert result.data.index.equals(pd.date_range(start="01/04/2024", end="01/10/2024"))
        assert result.data.empty

    def test_row_range_across_row_slices(self, lmdb_version_store_row_slice, index):
        assert lmdb_version_store_row_slice._lib_cfg.lib_desc.version.write_options.segment_row_size == 5
        row_range = (3, 8)
        lmdb_version_store_row_slice.write("sym", pd.DataFrame({"col": range(0, len(index))}, index=index))
        result = lmdb_version_store_row_slice.read("sym", row_range=row_range, columns=[], implement_read_index=True)
        assert isinstance(result, arcticdb.VersionedItem)
        assert result.symbol == "sym"
        assert result.version == 0
        assert result.data.index.equals(index[row_range[0]:row_range[1]])
        assert result.data.empty


    @pytest.mark.parametrize("non_datetime_index", [
        pd.RangeIndex(start=0, stop=5),
        pd.MultiIndex.from_arrays(
            [pd.RangeIndex(start=10, stop=21), pd.date_range(start="01/11/2024", end="01/21/2024")],
            names=["range", "date"]
        )
    ])
    def test_date_range_throws(self, lmdb_version_store_static_and_dynamic, non_datetime_index):
        lmdb_version_store_static_and_dynamic.write("sym", pd.DataFrame({"col": list(range(0, len(non_datetime_index)))}, index=non_datetime_index))
        with pytest.raises(Exception):
            lmdb_version_store_static_and_dynamic.read("sym", date_range=(datetime(2024,1,4), datetime(2024,1,10)), columns=[], implement_read_index=True)


class TestWithNormalizers:
    @pytest.fixture(autouse=True)
    def custom_normalizer(self):
        register_normalizer(TestCustomNormalizer())

    def test_recursive_throws(self, lmdb_version_store_static_and_dynamic):
        data = {"a": np.arange(5), "b": np.arange(8)}
        lmdb_version_store_static_and_dynamic.write("sym_recursive", data, recursive_normalizers=True, pickle_on_failure=False)
        with pytest.raises(InternalException) as exception_info:
            lmdb_version_store_static_and_dynamic.read("sym_recursive", columns=[], implement_read_index=True)
        assert "Reading the index column is not supported when recursive or custom normalizers are used." in str(exception_info.value)
            
    def test_custom_throws(self, custom_normalizer, lmdb_version_store_static_and_dynamic):
        data = CustomThing(custom_columns=["a", "b"], custom_index=[12, 13], custom_values=[[2.0, 4.0], [3.0, 5.0]])
        lmdb_version_store_static_and_dynamic.write("sym_custom", data, pickle_on_failure=False)

        with pytest.raises(InternalException) as exception_info:
            lmdb_version_store_static_and_dynamic.read("sym_custom", columns=[], implement_read_index=True)
        assert "Reading the index column is not supported when recursive or custom normalizers are used." in str(exception_info.value)


class TestPickled:
    def test_throws(self, lmdb_version_store_static_and_dynamic):
        data = {"a": np.arange(5), "b": np.arange(8)}
        lmdb_version_store_static_and_dynamic.write("sym_recursive", data, recursive_normalizers=False, pickle_on_failure=True)
        with pytest.raises(InternalException) as exception_info:
            lmdb_version_store_static_and_dynamic.read("sym_recursive", columns=[], implement_read_index=True)
        assert "Reading index columns is not supported with pickled data." in str(exception_info.value)