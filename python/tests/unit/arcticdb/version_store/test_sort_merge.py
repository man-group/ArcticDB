import pandas as pd
import numpy as np
from arcticdb.util.test import assert_frame_equal
import pytest
from arcticdb.version_store.library import StagedDataFinalizeMethod
from arcticdb.exceptions import UserInputException, SortingException, StreamDescriptorMismatch, InternalException, SchemaException
from arcticdb.util._versions import IS_PANDAS_TWO

def test_merge_single_column(lmdb_library_static_dynamic):
    lib = lmdb_library_static_dynamic

    dates1 = [np.datetime64('2023-01-01'), np.datetime64('2023-01-03'), np.datetime64('2023-01-05')]
    dates2 = [np.datetime64('2023-01-02'), np.datetime64('2023-01-04'), np.datetime64('2023-01-06')]

    data1 = {"x": [1, 3, 5]}
    data2 = {"x": [2, 4, 6]}

    df1 = pd.DataFrame(data1, index=dates1)
    df2 = pd.DataFrame(data2, index=dates2)

    sym1 = "symbol_1"
    lib.write(sym1, df1, staged=True)
    lib.write(sym1, df2, staged=True)
    metadata = {"meta": ["data"]}
    sort_and_finalize_res = lib.sort_and_finalize_staged_data(sym1, metadata=metadata)

    expected_dates = [np.datetime64('2023-01-01'), np.datetime64('2023-01-02'), np.datetime64('2023-01-03'),
                      np.datetime64('2023-01-04'), np.datetime64('2023-01-05'), np.datetime64('2023-01-06')]

    expected_values = {"x": [1, 2, 3, 4, 5, 6]}
    expected_df = pd.DataFrame(expected_values, index=expected_dates)
    assert_frame_equal(lib.read(sym1).data, expected_df)
    assert sort_and_finalize_res.metadata == {"meta": ["data"]}
    assert sort_and_finalize_res.symbol == sym1
    assert sort_and_finalize_res.library == lib.name
    assert sort_and_finalize_res.version == 0
    assert lib.read(sym1).metadata == metadata

def test_merge_two_column(lmdb_library_static_dynamic):
    lib = lmdb_library_static_dynamic

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


def test_merge_dynamic(lmdb_library_dynamic_schema):
    lib = lmdb_library_dynamic_schema

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



def test_merge_strings(lmdb_library_static_dynamic):
    lib = lmdb_library_static_dynamic

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


def test_merge_strings_dynamic(lmdb_library_dynamic_schema):
    lib = lmdb_library_dynamic_schema

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

def test_unordered_segment(lmdb_library_static_dynamic):
    lib = lmdb_library_static_dynamic
    dates = [np.datetime64('2023-01-03'), np.datetime64('2023-01-01'), np.datetime64('2023-01-05')]
    df = pd.DataFrame({"col": [2, 1, 3]}, index=dates)
    lib.write("sym", df, staged=True)
    lib.sort_and_finalize_staged_data("sym")
    assert_frame_equal(lib.read('sym').data, pd.DataFrame({"col": [1, 2, 3]}, index=[np.datetime64('2023-01-01'), np.datetime64('2023-01-03'), np.datetime64('2023-01-05')]))

def test_repeating_index_values(lmdb_library_static_dynamic):
    lib = lmdb_library_static_dynamic
    dates = [np.datetime64('2023-01-01'), np.datetime64('2023-01-03'), np.datetime64('2023-01-05')]
    df1 = pd.DataFrame({"col": [1,2,3]}, index=dates)
    df2 = pd.DataFrame({"col": [4,5,6]}, index=dates)
    lib.write("sym", df1, staged=True)
    lib.write("sym", df2, staged=True)
    lib.sort_and_finalize_staged_data("sym")
    data = lib.read("sym").data
    expected = pd.concat([df1, df2]).sort_index()
    assert data.index.equals(expected.index)

class TestMergeSortAppend:
    def test_appended_values_are_after(self, lmdb_library_static_dynamic):
        lib = lmdb_library_static_dynamic
        initial_df = pd.DataFrame(
            {"col": [1, 2, 3]},
            index=pd.DatetimeIndex([np.datetime64('2023-01-01'), np.datetime64('2023-01-02'), np.datetime64('2023-01-03')], dtype="datetime64[ns]")
        )
        lib.write("sym", initial_df)
        df1 = pd.DataFrame(
            {"col": [4, 7, 8]},
            index=pd.DatetimeIndex([np.datetime64('2023-01-05'), np.datetime64('2023-01-09'), np.datetime64('2023-01-10')], dtype="datetime64[ns]")
        )
        df2 = pd.DataFrame({"col": [5, 6]}, index=pd.DatetimeIndex([np.datetime64('2023-01-06'), np.datetime64('2023-01-08')], dtype="datetime64[ns]"))
        lib.write("sym", df1, staged=True)
        lib.write("sym", df2, staged=True)
        lib.sort_and_finalize_staged_data("sym", mode=StagedDataFinalizeMethod.APPEND)
        expected_index = pd.DatetimeIndex([np.datetime64('2023-01-01'), np.datetime64('2023-01-02'), np.datetime64('2023-01-03'), np.datetime64('2023-01-05'),
                                           np.datetime64('2023-01-06'), np.datetime64('2023-01-08'), np.datetime64('2023-01-09'), np.datetime64('2023-01-10')],
                                          dtype="datetime64[ns]")
        expected_df = pd.DataFrame({"col": range(1, 9)}, index=expected_index)
        assert_frame_equal(lib.read("sym").data, expected_df)

    def test_appended_df_interleaves_with_storage(self, lmdb_library_static_dynamic):
        lib = lmdb_library_static_dynamic
        initial_df = pd.DataFrame({"col": [1, 3]}, index=pd.DatetimeIndex([np.datetime64('2023-01-01'), np.datetime64('2023-01-03')], dtype="datetime64[ns]"))
        lib.write("sym", initial_df)
        df1 = pd.DataFrame({"col": [2]}, index=pd.DatetimeIndex([np.datetime64('2023-01-02')], dtype="datetime64[ns]"))
        lib.write("sym", df1, staged=True)
        with pytest.raises(SortingException) as exception_info:
            lib.sort_and_finalize_staged_data("sym", mode=StagedDataFinalizeMethod.APPEND)
        assert "append" in str(exception_info.value)

    def test_appended_df_start_same_as_df_end(self, lmdb_library_static_dynamic):
        lib = lmdb_library_static_dynamic
        df = pd.DataFrame(
            {"col": [1, 2, 3]},
            index=pd.DatetimeIndex([np.datetime64('2023-01-01'), np.datetime64('2023-01-02'), np.datetime64('2023-01-03')], dtype="datetime64[ns]")
        )
        lib.write("sym", df)
        df_to_append = pd.DataFrame(
            {"col": [4, 5, 6]},
            index=pd.DatetimeIndex([np.datetime64('2023-01-03'), np.datetime64('2023-01-04'), np.datetime64('2023-01-05')], dtype="datetime64[ns]")
        )
        lib.write("sym", df_to_append, staged=True)
        lib.sort_and_finalize_staged_data("sym", mode=StagedDataFinalizeMethod.APPEND)
        res = lib.read("sym").data
        expected_df = pd.concat([df, df_to_append])
        assert_frame_equal(lib.read("sym").data, expected_df)


def test_prune_previous(lmdb_library_static_dynamic):
    lib = lmdb_library_static_dynamic
    idx = pd.DatetimeIndex([np.datetime64('2023-01-01'), np.datetime64('2023-01-03')], dtype="datetime64[ns]")
    df = pd.DataFrame({"col": [1, 3]}, index=idx)
    lib.write("sym", df)
    lib.write("sym", df)
    lib.write("sym", df, staged=True)
    lib.sort_and_finalize_staged_data("sym", prune_previous_versions=True)
    assert_frame_equal(df, lib.read("sym").data)
    assert len(lib.list_versions("sym")) == 1

@pytest.mark.parametrize("mode", [StagedDataFinalizeMethod.APPEND, StagedDataFinalizeMethod.WRITE])
class TestEmptySegments:
    def test_staged_segment_is_only_empty_dfs(self, lmdb_library_static_dynamic, mode):
        lib = lmdb_library_static_dynamic
        lib.write("sym", pd.DataFrame([]), staged=True)
        lib.write("sym", pd.DataFrame([]), staged=True)
        lib.sort_and_finalize_staged_data("sym", mode=mode)
        assert_frame_equal(lib.read("sym").data, pd.DataFrame([], index=pd.DatetimeIndex([])))

    def test_staged_segment_has_empty_df(self, lmdb_library_dynamic_schema, mode):
        lib = lmdb_library_dynamic_schema
        index = pd.DatetimeIndex([pd.Timestamp(2024, 1, 1), pd.Timestamp(2024, 1, 3), pd.Timestamp(2024, 1, 4)])
        df1 = pd.DataFrame({"col": [1, 2, 3]}, index=index)
        df2 = pd.DataFrame({})
        df3 = pd.DataFrame({"col": [5]}, index=pd.DatetimeIndex([pd.Timestamp(2024, 1, 2)]))
        lib.write("sym", df1, staged=True)
        lib.write("sym", df2, staged=True)
        lib.write("sym", df3, staged=True)
        lib.sort_and_finalize_staged_data("sym", mode=mode)
        assert_frame_equal(lib.read("sym").data, pd.concat([df1, df2, df3]).sort_index())

    def test_df_without_rows(self, lmdb_library_static_dynamic, mode):
        lib = lmdb_library_static_dynamic
        df = pd.DataFrame({"col": []}, index=pd.DatetimeIndex([]))
        lib.write("sym", df, staged=True)
        lib.sort_and_finalize_staged_data("sym", mode=mode)
        assert_frame_equal(lib.read("sym").data, df)

    def test_finalize_without_adding_segments(self, lmdb_library_static_dynamic, mode):
        lib = lmdb_library_static_dynamic
        with pytest.raises(UserInputException) as exception_info:
            lib.sort_and_finalize_staged_data("sym", mode=mode)
        assert "E_NO_STAGED_SEGMENTS" in str(exception_info.value)

    def test_mixing_empty_and_non_empty_columns(self, lmdb_library_dynamic_schema, mode):
        lib = lmdb_library_dynamic_schema

        df = pd.DataFrame({"a": [1]},index=pd.DatetimeIndex([pd.Timestamp("1970-01-01")]))
        df2 = pd.DataFrame({"b": np.array([], dtype="float"), "c": np.array([], dtype="int64"), "d": np.array([], dtype="object")},index=pd.DatetimeIndex([]))
        lib.write("sym", df, staged=True)
        lib.write("sym", df2, staged=True)
        lib.sort_and_finalize_staged_data("sym", mode=mode)
        if IS_PANDAS_TWO:
            expected = pd.DataFrame({"a": [1], "b": np.array([np.nan], dtype="float"), "c": np.array([0], dtype="int64"), "d": np.array([None], dtype="object")}, index=[pd.Timestamp("1970-01-01")])
        else:
            expected = pd.DataFrame({"a": [1], "b": np.array([np.nan], dtype="object"), "c": np.array([0], dtype="int64"), "d": np.array([None], dtype="object")}, index=[pd.Timestamp("1970-01-01")])
        assert_frame_equal(lib.read("sym").data, expected)

def test_append_to_missing_symbol(lmdb_library):
    lib = lmdb_library
    df = pd.DataFrame({"col": [1]}, index=pd.DatetimeIndex([np.datetime64('2023-01-01')], dtype="datetime64[ns]"))
    lib.write("sym", df, staged=True)
    lib.sort_and_finalize_staged_data("sym", mode=StagedDataFinalizeMethod.APPEND)
    assert_frame_equal(lib.read("sym").data, df)

def test_pre_epoch(lmdb_library):
    lib = lmdb_library

    df = pd.DataFrame({"col": [1]}, pd.DatetimeIndex([pd.Timestamp(1969, 12, 31)]))
    lib.write("sym", df, staged=True)
    lib.sort_and_finalize_staged_data("sym")

    assert_frame_equal(lib.read("sym").data, df)


class TestDescriptorMismatchBetweenStagedSegments:
    def test_append_throws_with_missmatched_column_set(self, lmdb_library):
        lib = lmdb_library

        initial_df = pd.DataFrame({"col_0": [1]}, index=pd.DatetimeIndex([pd.Timestamp(2024, 1, 1)]))
        lib.write("sym", initial_df, staged=True)

        appended_df = pd.DataFrame({"col_1": [1]}, index=pd.DatetimeIndex([pd.Timestamp(2024, 1, 2)]))
        lib.write("sym", appended_df, staged=True)

        with pytest.raises(SchemaException) as exception_info:
            lib.sort_and_finalize_staged_data("sym", mode=StagedDataFinalizeMethod.APPEND)
        assert "col_1" in str(exception_info.value)

    def test_append_throws_column_subset(self, lmdb_library):
        lib = lmdb_library

        df1 = pd.DataFrame(
            {
                "col_0": np.array([1.1], dtype="float"),
                "col_1": np.array([2], dtype="int64")
            },
            index=pd.DatetimeIndex([pd.Timestamp("2024-01-01")])
        )
        lib.write("sym", df1, staged=True)

        df2 = pd.DataFrame({"col_1": [1]}, index=pd.DatetimeIndex([pd.Timestamp("2024-01-02")]))
        lib.write("sym", df2, staged=True)

        with pytest.raises(SchemaException) as exception_info: 
            lib.sort_and_finalize_staged_data("sym", StagedDataFinalizeMethod.APPEND)
        assert "col_0" in str(exception_info.value)
        assert "col_1" in str(exception_info.value)

    def test_appending_reordered_column_set_throws(self, lmdb_library):
        lib = lmdb_library

        df1 = pd.DataFrame({"col_0": [1], "col_1": ["test"], "col_2": [1.2]}, index=pd.DatetimeIndex([pd.Timestamp(2024, 1, 1)]))
        lib.write("sym", df1, staged=True)

        df2 = pd.DataFrame({"col_1": ["asd"], "col_2": [2.5], "col_1": [2]}, index=pd.DatetimeIndex([pd.Timestamp(2024, 1, 2)]))
        lib.write("sym", df2, staged=True)

        with pytest.raises(SchemaException) as exception_info:
            lib.sort_and_finalize_staged_data("sym", mode=StagedDataFinalizeMethod.APPEND)
        assert "col_0" in str(exception_info.value)
        assert "col_1" in str(exception_info.value)
        assert "col_2" in str(exception_info.value)

    def test_append_throws_on_incompatible_dtype(self, lmdb_library):
        lib = lmdb_library

        df1 = pd.DataFrame({"col_0": np.array([1], dtype="int64")}, index=pd.DatetimeIndex([pd.Timestamp(2024, 1, 1)]))
        lib.write("sym", df1, staged=True)

        df2 = pd.DataFrame({"col_0": ["asd"]}, index=pd.DatetimeIndex([pd.Timestamp(2024, 1, 2)]))
        lib.write("sym", df2, staged=True)

        with pytest.raises(SchemaException) as exception_info:
            lib.sort_and_finalize_staged_data("sym", mode=StagedDataFinalizeMethod.APPEND)
        assert "col_0" in str(exception_info.value)
        assert "INT64" in str(exception_info.value)

    @pytest.mark.parametrize("mode", [StagedDataFinalizeMethod.APPEND, StagedDataFinalizeMethod.WRITE])
    def test_types_cant_be_promoted(self, lmdb_library, mode):
        lib = lmdb_library

        df1 = pd.DataFrame({"col_0": np.array([1], dtype="float")}, index=pd.DatetimeIndex([np.datetime64('2023-01-01')]))
        lib.write("sym", df1, staged=True)
        
        df2 = pd.DataFrame({"col_0": np.array([1], dtype="int")}, index=pd.DatetimeIndex([np.datetime64('2023-01-02')]))
        lib.write("sym", df2, staged=True)

        with pytest.raises(SchemaException) as exception_info:
            lib.sort_and_finalize_staged_data("sym", mode=mode)
        assert "col_0" in str(exception_info.value)
        assert "FLOAT" in str(exception_info.value)
        assert "INT" in str(exception_info.value)

    @pytest.mark.parametrize("mode", [StagedDataFinalizeMethod.APPEND, StagedDataFinalizeMethod.WRITE])
    def test_type_mismatch_in_staged_segments_throws_with_promotoable_types(self, lmdb_library, mode):
        lib = lmdb_library

        df1 = pd.DataFrame({"col": np.array([1], dtype="int64")}, index=pd.DatetimeIndex([np.datetime64('2023-01-01')]))
        lib.write("sym", df1, staged=True)

        df2 = pd.DataFrame({"col": ["test"]}, index=pd.DatetimeIndex([np.datetime64('2023-01-02')]))
        lib.write("sym", df2, staged=True)

        with pytest.raises(SchemaException) as exception_info:
            lib.sort_and_finalize_staged_data("sym", mode=mode)
        assert "INT" in str(exception_info.value)
        assert "UTF_DYNAMIC" in str(exception_info.value)

    @pytest.mark.parametrize("mode", [StagedDataFinalizeMethod.APPEND, StagedDataFinalizeMethod.WRITE])
    def test_staged_segments_cant_be_reordered(self, lmdb_library, mode):
        lib = lmdb_library

        df1 = pd.DataFrame({"col_0": [1], "col_1": ["test"], "col_2": [1.2]}, index=pd.DatetimeIndex([pd.Timestamp(2024, 1, 1)]))
        lib.write("sym", df1, staged=True)
        
        df2 = pd.DataFrame({"col_1": ["asd"], "col_2": [2.5], "col_1": [2]}, index=pd.DatetimeIndex([pd.Timestamp(2024, 1, 2)]))
        lib.write("sym", df2, staged=True)

        with pytest.raises(SchemaException) as exception_info:
            lib.sort_and_finalize_staged_data("sym", mode=mode)
        assert "col_0" in str(exception_info.value)
        assert "col_1" in str(exception_info.value)
        assert "col_2" in str(exception_info.value)


class TestStreamDescriptorMismatchOnFinalizeAppend:
    def init_symbol(self, lib, sym):
        df = pd.DataFrame({"col_0": np.array([1], "int32"), "col_1": [0.5], "col_2": ["val"]}, index=pd.DatetimeIndex([pd.Timestamp(2024, 1, 1)]))
        lib.write(sym, df)

    def test_cannot_append_column_subset(self, lmdb_library):
        lib = lmdb_library
        self.init_symbol(lib, "sym")
        
        df = pd.DataFrame({"col_0": [1]}, index=pd.DatetimeIndex([pd.Timestamp(2024, 1, 1)]))
        lib.write("sym", df, staged=True)
        with pytest.raises(SchemaException) as exception_info:
            lib.sort_and_finalize_staged_data("sym", mode=StagedDataFinalizeMethod.APPEND)
        assert "col_1" in str(exception_info.value)
        assert "col_2" in str(exception_info.value)

    def test_cannot_append_reordered_columns(self, lmdb_library):
        lib = lmdb_library
        self.init_symbol(lib, "sym")
        
        df = pd.DataFrame({"col_1": [1.4], "col_0": [5], "col_2": ["val"]}, index=pd.DatetimeIndex([pd.Timestamp(2024, 1, 1)]))
        lib.write("sym", df, staged=True)
        with pytest.raises(SchemaException) as exception_info:
            lib.sort_and_finalize_staged_data("sym", mode=StagedDataFinalizeMethod.APPEND)
        assert "col_0" in str(exception_info.value)
        assert "col_1" in str(exception_info.value)
        assert "col_2" in str(exception_info.value)

    def test_cannot_promote_type(self, lmdb_library):
        lib = lmdb_library
        self.init_symbol(lib, "sym")
        
        df = pd.DataFrame({"col_0": np.array([1], dtype="int64"), "col_1": [5], "col_2": ["val"]}, index=pd.DatetimeIndex([pd.Timestamp(2024, 1, 1)]))
        lib.write("sym", df, staged=True)
        with pytest.raises(SchemaException) as exception_info:
            lib.sort_and_finalize_staged_data("sym", mode=StagedDataFinalizeMethod.APPEND)
        assert "col_0" in str(exception_info.value)
        assert "INT32" in str(exception_info.value)
        assert "INT64" in str(exception_info.value)

        df = pd.DataFrame({"col_0": np.array([1], dtype="int16"), "col_1": [5], "col_2": ["val"]}, index=pd.DatetimeIndex([pd.Timestamp(2024, 1, 1)]))
        lib.write("sym", df, staged=True)
        with pytest.raises(SchemaException) as exception_info:
            lib.sort_and_finalize_staged_data("sym", mode=StagedDataFinalizeMethod.APPEND)
        assert "col_0" in str(exception_info.value)
        assert "INT32" in str(exception_info.value)
        assert "INT16" in str(exception_info.value)

    def test_cannot_add_new_columns(self, lmdb_library):
        lib = lmdb_library
        self.init_symbol(lib, "sym")
        
        df = pd.DataFrame({"col_0": np.array([1], dtype="int32"), "col_1": [5], "col_2": ["val"], "col_3": [1]}, index=pd.DatetimeIndex([pd.Timestamp(2024, 1, 1)]))
        lib.write("sym", df, staged=True)
        with pytest.raises(SchemaException) as exception_info:
            lib.sort_and_finalize_staged_data("sym", mode=StagedDataFinalizeMethod.APPEND)
        assert "col_3" in str(exception_info.value)
        
        
# This was a added as a bug repro for GH issue #1795.
def test_two_columns_with_different_dtypes(lmdb_library_dynamic_schema):
    lib = lmdb_library_dynamic_schema

    idx1 = pd.DatetimeIndex([
        pd.Timestamp("2024-01-02")
    ])
    df1 = pd.DataFrame({
         "a": np.array([1], dtype="float"),
         "b": np.array([22250], dtype="int64")
    }, index=idx1)
    
    b = np.array([-53979, -53973], dtype="int64")

    idx = pd.DatetimeIndex([
        pd.Timestamp("2024-01-03"),
        pd.Timestamp("2024-01-01")
    ])

    df2 = pd.DataFrame({"b": b}, index=idx)
    
    lib.write("sym", df1, staged=True)
    lib.write("sym", df2, staged=True)
    lib.sort_and_finalize_staged_data("sym")
    lib.read("sym")

@pytest.mark.parametrize("mode", [StagedDataFinalizeMethod.APPEND, StagedDataFinalizeMethod.WRITE])
def test_nat_is_not_allowed_in_index(lmdb_library_static_dynamic, mode):
    lib = lmdb_library_static_dynamic

    df1 = pd.DataFrame({"a": [1]}, index=pd.DatetimeIndex([pd.NaT]))
    lib.write("sym", df1, staged=True)
    with pytest.raises(SortingException) as exception_info:
        lib.sort_and_finalize_staged_data("sym", mode=mode)
    assert "NaT" in str(exception_info.value)

class TestSortMergeDynamicSchema:

    def test_appended_columns_are_subset(self, lmdb_library_dynamic_schema):
        lib = lmdb_library_dynamic_schema

        lib.write("sym", pd.DataFrame({"a": [1], "b": [1.2]}, index=pd.DatetimeIndex([pd.Timestamp(2024, 1, 1)])))

        lib.write("sym", pd.DataFrame({"a": [2]}, index=pd.DatetimeIndex([pd.Timestamp(2024, 1, 2)])), staged=True)
        lib.write("sym", pd.DataFrame({"b": [5.3]}, index=pd.DatetimeIndex([pd.Timestamp(2024, 1, 3)])), staged=True)
        lib.sort_and_finalize_staged_data("sym", mode=StagedDataFinalizeMethod.APPEND)

        expected = pd.DataFrame({"a": [1, 2, 0], "b": [1.2, np.nan, 5.3]}, index=pd.date_range("2024-01-01", "2024-01-03"))
        stored = lib.read("sym").data

        assert_frame_equal(expected, stored)

    def test_can_append_new_columns(self, lmdb_library_dynamic_schema):
        lib = lmdb_library_dynamic_schema

        lib.write("sym", pd.DataFrame({"a": [1]}, index=pd.DatetimeIndex([pd.Timestamp(2024, 1, 1)])))

        lib.write("sym", pd.DataFrame({"b": [1.5]}, index=pd.DatetimeIndex([pd.Timestamp(2024, 1, 2)])), staged=True)
        lib.write("sym", pd.DataFrame({"c": ["c"]}, index=pd.DatetimeIndex([pd.Timestamp(2024, 1, 3)])), staged=True)
        lib.sort_and_finalize_staged_data("sym", mode=StagedDataFinalizeMethod.APPEND)

        expected = pd.DataFrame({"a": [1, 0, 0], "b": [np.nan, 1.5, np.nan], "c": [None, None, "c"]}, index=pd.date_range("2024-01-01", "2024-01-03"))
        stored = lib.read("sym").data

        assert_frame_equal(expected, stored, check_like=True)