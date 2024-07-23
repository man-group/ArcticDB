import pandas as pd
import numpy as np
from pandas.testing import assert_frame_equal
import pytest
from arcticdb.version_store.library import StagedDataFinalizeMethod

def test_merge_single_column(arctic_library):
    lib = arctic_library

    dates1 = [np.datetime64('2023-01-01'), np.datetime64('2023-01-03'), np.datetime64('2023-01-05')]
    dates2 = [np.datetime64('2023-01-02'), np.datetime64('2023-01-04'), np.datetime64('2023-01-06')]

    data1 = {"x": [1, 3, 5]}
    data2 = {"x": [2, 4, 6]}

    df1 = pd.DataFrame(data1, index=dates1)
    df2 = pd.DataFrame(data2, index=dates2)

    sym1 = "symbol_1"
    lib.write(sym1, df1, staged=True)
    lib.write(sym1, df2, staged=True)
    lib.sort_and_finalize_staged_data(sym1)

    expected_dates = [np.datetime64('2023-01-01'), np.datetime64('2023-01-02'), np.datetime64('2023-01-03'),
                      np.datetime64('2023-01-04'), np.datetime64('2023-01-05'), np.datetime64('2023-01-06')]

    expected_values = {"x": [1, 2, 3, 4, 5, 6]}
    expected_df = pd.DataFrame(expected_values, index=expected_dates)
    assert_frame_equal(lib.read(sym1).data, expected_df)


def test_merge_two_column(arctic_library):
    lib = arctic_library

    dates1 = [np.datetime64('2023-01-01'), np.datetime64('2023-01-03'), np.datetime64('2023-01-05')]
    dates2 = [np.datetime64('2023-01-02'), np.datetime64('2023-01-04'), np.datetime64('2023-01-06')]

    data1 = {"x": [1, 3, 5], "y": [10, 12, 14]}
    data2 = {"x": [2, 4, 6], "y": [11, 13, 15]}

    df1 = pd.DataFrame(data1, index=dates1)
    df2 = pd.DataFrame(data2, index=dates2)

    sym1 = "symbol_1"
    lib.write(sym1, df1, staged=True)
    lib.write(sym1, df2, staged=True)
    lib.sort_and_finalize_staged_data(sym1)

    expected_dates = [np.datetime64('2023-01-01'), np.datetime64('2023-01-02'), np.datetime64('2023-01-03'),
                      np.datetime64('2023-01-04'), np.datetime64('2023-01-05'), np.datetime64('2023-01-06')]

    expected_values = {"x": [1, 2, 3, 4, 5, 6], "y": [10, 11, 12, 13, 14, 15]}
    expected_df = pd.DataFrame(expected_values, index=expected_dates)
    assert_frame_equal(lib.read(sym1).data, expected_df)


def test_merge_dynamic(arctic_library):
    lib = arctic_library

    dates1 = [np.datetime64('2023-01-01'), np.datetime64('2023-01-03'), np.datetime64('2023-01-05')]
    dates2 = [np.datetime64('2023-01-02'), np.datetime64('2023-01-04'), np.datetime64('2023-01-06')]

    data1 = {"x": [1, 3, 5]}
    data2 = {"y": [2, 4, 6]}

    df1 = pd.DataFrame(data1, index=dates1)
    df2 = pd.DataFrame(data2, index=dates2)

    sym1 = "symbol_1"
    lib.write(sym1, df1, staged=True)
    lib.write(sym1, df2, staged=True)
    lib.sort_and_finalize_staged_data(sym1)

    expected_dates = [np.datetime64('2023-01-01'), np.datetime64('2023-01-02'), np.datetime64('2023-01-03'),
                      np.datetime64('2023-01-04'), np.datetime64('2023-01-05'), np.datetime64('2023-01-06')]

    expected_values = {"x": [1, 0, 3, 0, 5, 0], "y": [0, 2, 0, 4, 0, 6]}
    expected_df = pd.DataFrame(expected_values, index=expected_dates)
    assert_frame_equal(lib.read(sym1).data, expected_df)


def test_merge_strings(arctic_library):
    lib = arctic_library

    dates1 = [np.datetime64('2023-01-01'), np.datetime64('2023-01-03'), np.datetime64('2023-01-05')]
    dates2 = [np.datetime64('2023-01-02'), np.datetime64('2023-01-04'), np.datetime64('2023-01-06')]

    data1 = {"x": [1, 3, 5], "y": ["one","three", "five"]}
    data2 = {"x": [2, 4, 6], "y": ["two", "four", "six"]}

    df1 = pd.DataFrame(data1, index=dates1)
    df2 = pd.DataFrame(data2, index=dates2)

    sym1 = "symbol_1"
    lib.write(sym1, df1, staged=True)
    lib.write(sym1, df2, staged=True)
    lib.sort_and_finalize_staged_data(sym1)

    expected_dates = [np.datetime64('2023-01-01'), np.datetime64('2023-01-02'), np.datetime64('2023-01-03'),
                      np.datetime64('2023-01-04'), np.datetime64('2023-01-05'), np.datetime64('2023-01-06')]

    expected_values = {"x": [1, 2, 3, 4, 5, 6], "y": ["one", "two", "three", "four", "five", "six"]}
    expected_df = pd.DataFrame(expected_values, index=expected_dates)
    assert_frame_equal(lib.read(sym1).data, expected_df)


def test_merge_strings_dynamic(arctic_library):
    lib = arctic_library

    dates1 = [np.datetime64('2023-01-01'), np.datetime64('2023-01-03'), np.datetime64('2023-01-05')]
    dates2 = [np.datetime64('2023-01-02'), np.datetime64('2023-01-04'), np.datetime64('2023-01-06')]

    data1 = {"x": ["one","three", "five"]}
    data2 = {"y": ["two", "four", "six"]}

    df1 = pd.DataFrame(data1, index=dates1)
    df2 = pd.DataFrame(data2, index=dates2)

    sym1 = "symbol_1"
    lib.write(sym1, df1, staged=True)
    lib.write(sym1, df2, staged=True)
    lib.sort_and_finalize_staged_data(sym1)

    expected_dates = [np.datetime64('2023-01-01'), np.datetime64('2023-01-02'), np.datetime64('2023-01-03'),
                      np.datetime64('2023-01-04'), np.datetime64('2023-01-05'), np.datetime64('2023-01-06')]

    expected_values = {"x": ["one", None, "three", None, "five", None], "y": [None, "two", None, "four", None, "six"]}
    expected_df = pd.DataFrame(expected_values, index=expected_dates)
    assert_frame_equal(lib.read(sym1).data, expected_df)


@pytest.mark.xfail(reason="Unsorted segments are not implemented")
def test_unordered_segment(lmdb_storage, lib_name):
    ac = lmdb_storage.create_arctic()
    lib = ac.create_library(lib_name)
    dates = [np.datetime64('2023-01-03'), np.datetime64('2023-01-01'), np.datetime64('2023-01-05')]
    df = pd.DataFrame({"col": [2, 1, 3]}, index=dates)
    lib.write("sym", df, staged=True)
    lib.sort_and_finalize_staged_data("sym")
    assert_frame_equal(lib.read('sym'), pd.DataFrame({"col": [1, 2, 3]}, index=[np.datetime64('2023-01-01'), np.datetime64('2023-01-03'), np.datetime64('2023-01-05')]))

def test_repeating_index_values(lmdb_storage, lib_name):
    ac = lmdb_storage.create_arctic()
    lib = ac.create_library(lib_name)
    dates = [np.datetime64('2023-01-01'), np.datetime64('2023-01-03'), np.datetime64('2023-01-05')]
    df1 = pd.DataFrame({"col": [1,2,3]}, index=dates)
    df2 = pd.DataFrame({"col": [4,5,6]}, index=dates)
    lib.write("sym", df1, staged=True)
    lib.write("sym", df2, staged=True)
    lib.sort_and_finalize_staged_data("sym")
    expected_index = pd.DatetimeIndex([np.datetime64('2023-01-01'), np.datetime64('2023-01-01'), np.datetime64('2023-01-03'),
                                       np.datetime64('2023-01-03'), np.datetime64('2023-01-05'), np.datetime64('2023-01-05')])
    data = lib.read("sym").data
    assert data.index.equals(expected_index)
    for i in range(0, len(data["col"])):
        assert data["col"][i] == df1["col"][i // 2] or data["col"][i] == df2["col"][i // 2]

class TestMergeSortAppend:
    def test_appended_values_are_after(self, lmdb_storage, lib_name):
        ac = lmdb_storage.create_arctic()
        lib = ac.create_library(lib_name)
        initial_df = pd.DataFrame({"col": [1, 2, 3]}, index=pd.DatetimeIndex([np.datetime64('2023-01-01'), np.datetime64('2023-01-02'), np.datetime64('2023-01-03')]))
        lib.write("sym", initial_df)
        df1 = pd.DataFrame({"col": [4, 7, 8]}, index=pd.DatetimeIndex([np.datetime64('2023-01-05'), np.datetime64('2023-01-09'), np.datetime64('2023-01-10')]))
        df2 = pd.DataFrame({"col": [5, 6]}, index=pd.DatetimeIndex([np.datetime64('2023-01-06'), np.datetime64('2023-01-08')]))
        lib.write("sym", df1, staged=True)
        lib.write("sym", df2, staged=True)
        lib.sort_and_finalize_staged_data("sym", mode=StagedDataFinalizeMethod.APPEND)
        expected_index = pd.DatetimeIndex([np.datetime64('2023-01-01'), np.datetime64('2023-01-02'), np.datetime64('2023-01-03'), np.datetime64('2023-01-05'),
                                           np.datetime64('2023-01-06'), np.datetime64('2023-01-08'), np.datetime64('2023-01-09'), np.datetime64('2023-01-10')])
        expected_df = pd.DataFrame({"col": range(1, 9)}, index=expected_index)
        assert_frame_equal(lib.read("sym").data, expected_df)

    @pytest.mark.xfail(reason="Does not throw and creates dataframe with unordered index.")
    def test_appended_df_interleaves_with_storage(self, lmdb_storage, lib_name):
        ac = lmdb_storage.create_arctic()
        lib = ac.create_library(lib_name)
        initial_df = pd.DataFrame({"col": [1, 3]}, index=pd.DatetimeIndex([np.datetime64('2023-01-01'), np.datetime64('2023-01-03')]))
        lib.write("sym", initial_df)
        df1 = pd.DataFrame({"col": [2]}, index=pd.DatetimeIndex([np.datetime64('2023-01-02')]))
        lib.write("sym", df1, staged=True)
        with pytest.raises(Exception) as exception_info:
            lib.sort_and_finalize_staged_data("sym", mode=StagedDataFinalizeMethod.APPEND)
        assert "append" in str(exception_info.value)


def test_prune_previous(lmdb_storage, lib_name):
    ac = lmdb_storage.create_arctic()
    lib = ac.create_library(lib_name)
    df = pd.DataFrame({"col": [1, 3]}, index=pd.DatetimeIndex([np.datetime64('2023-01-01'), np.datetime64('2023-01-03')]))
    lib.write("sym", df)
    lib.write("sym", df)
    lib.write("sym", df, staged=True)
    lib.sort_and_finalize_staged_data("sym", prune_previous_versions=True)
    assert_frame_equal(df, lib.read("sym").data)
    assert len(lib.list_versions("sym")) == 1

@pytest.mark.xfail(reason="Bug")
def test_empty_df_in_staged_segment(lmdb_storage, lib_name):
    ac = lmdb_storage.create_arctic()
    lib = ac.create_library(lib_name)
    lib.write("sym", pd.DataFrame([]), staged=True)
    lib.sort_and_finalize_staged_data("sym")
    assert_frame_equal(lib.read("sym").data, pd.DataFrame([]))

@pytest.mark.xfail(reason="Throws: E_ASSERTION_FAILURE Stream descriptor not found in pipeline context")
def test_finalize_without_adding_segments(lmdb_storage, lib_name):
    ac = lmdb_storage.create_arctic()
    lib = ac.create_library(lib_name)
    lib.write("sym", pd.DataFrame({"col": [1]}, index=pd.DatetimeIndex([np.datetime64('2023-01-01')])))
    lib.sort_and_finalize_staged_data("sym")