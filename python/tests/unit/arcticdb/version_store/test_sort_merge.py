import pandas as pd
import numpy as np
from arcticdb.util.test import assert_frame_equal, config_context
import pytest
from arcticdb_ext.storage import KeyType
from arcticdb.version_store.library import StagedDataFinalizeMethod
from arcticdb.exceptions import (
    UserInputException,
    SortingException,
    StreamDescriptorMismatch,
    InternalException,
    SchemaException,
)
from arcticdb.util._versions import IS_PANDAS_TWO
from arcticdb_ext import set_config_int
from arcticdb.options import LibraryOptions
import arcticdb.toolbox.library_tool


def get_append_keys(lib, sym):
    lib_tool = lib._nvs.library_tool()
    keys = lib_tool.find_keys_for_symbol(arcticdb.toolbox.library_tool.KeyType.APPEND_DATA, sym)
    return keys


def assert_delete_staged_data_clears_append_keys(lib, sym):
    """
    Clear APPEND_DATA keys for a symbol and assert there are no APPEND_DATA keys after that.
    Note this has a side effect of actually deleting the APPEND_DATA keys.
    """
    lib.delete_staged_data(sym)
    assert len(get_append_keys(lib, sym)) == 0


def test_merge_single_column(lmdb_library_static_dynamic):
    lib = lmdb_library_static_dynamic

    dates1 = [np.datetime64("2023-01-01"), np.datetime64("2023-01-03"), np.datetime64("2023-01-05")]
    dates2 = [np.datetime64("2023-01-02"), np.datetime64("2023-01-04"), np.datetime64("2023-01-06")]

    data1 = {"x": [1, 3, 5]}
    data2 = {"x": [2, 4, 6]}

    df1 = pd.DataFrame(data1, index=dates1)
    df2 = pd.DataFrame(data2, index=dates2)

    sym1 = "symbol_1"
    lib.write(sym1, df1, staged=True)
    lib.write(sym1, df2, staged=True)
    metadata = {"meta": ["data"]}
    sort_and_finalize_res = lib.sort_and_finalize_staged_data(sym1, metadata=metadata)

    expected_dates = [
        np.datetime64("2023-01-01"),
        np.datetime64("2023-01-02"),
        np.datetime64("2023-01-03"),
        np.datetime64("2023-01-04"),
        np.datetime64("2023-01-05"),
        np.datetime64("2023-01-06"),
    ]

    expected_values = {"x": [1, 2, 3, 4, 5, 6]}
    expected_df = pd.DataFrame(expected_values, index=expected_dates)
    assert_frame_equal(lib.read(sym1).data, expected_df)
    assert sort_and_finalize_res.metadata == {"meta": ["data"]}
    assert sort_and_finalize_res.symbol == sym1
    assert sort_and_finalize_res.library == lib.name
    assert sort_and_finalize_res.version == 0
    assert lib.read(sym1).metadata == metadata
    assert len(get_append_keys(lib, sym1)) == 0


def test_merge_two_column(lmdb_library_static_dynamic):
    lib = lmdb_library_static_dynamic

    dates1 = [np.datetime64("2023-01-01"), np.datetime64("2023-01-03"), np.datetime64("2023-01-05")]
    dates2 = [np.datetime64("2023-01-02"), np.datetime64("2023-01-04"), np.datetime64("2023-01-06")]

    data1 = {"x": [1, 3, 5], "y": [10, 12, 14]}
    data2 = {"x": [2, 4, 6], "y": [11, 13, 15]}

    df1 = pd.DataFrame(data1, index=dates1)
    df2 = pd.DataFrame(data2, index=dates2)

    sym1 = "symbol_1"
    lib.write(sym1, df1, staged=True)
    lib.write(sym1, df2, staged=True)
    lib.sort_and_finalize_staged_data(sym1)

    expected_dates = [
        np.datetime64("2023-01-01"),
        np.datetime64("2023-01-02"),
        np.datetime64("2023-01-03"),
        np.datetime64("2023-01-04"),
        np.datetime64("2023-01-05"),
        np.datetime64("2023-01-06"),
    ]

    expected_values = {"x": [1, 2, 3, 4, 5, 6], "y": [10, 11, 12, 13, 14, 15]}
    expected_df = pd.DataFrame(expected_values, index=expected_dates)
    assert_frame_equal(lib.read(sym1).data, expected_df)


def test_merge_dynamic(lmdb_library_dynamic_schema):
    lib = lmdb_library_dynamic_schema

    dates1 = [np.datetime64("2023-01-01"), np.datetime64("2023-01-03"), np.datetime64("2023-01-05")]
    dates2 = [np.datetime64("2023-01-02"), np.datetime64("2023-01-04"), np.datetime64("2023-01-06")]

    data1 = {"x": [1, 3, 5]}
    data2 = {"y": [2, 4, 6]}

    df1 = pd.DataFrame(data1, index=dates1)
    df2 = pd.DataFrame(data2, index=dates2)

    sym1 = "symbol_1"
    lib.write(sym1, df1, staged=True)
    lib.write(sym1, df2, staged=True)
    lib.sort_and_finalize_staged_data(sym1)

    expected_dates = [
        np.datetime64("2023-01-01"),
        np.datetime64("2023-01-02"),
        np.datetime64("2023-01-03"),
        np.datetime64("2023-01-04"),
        np.datetime64("2023-01-05"),
        np.datetime64("2023-01-06"),
    ]

    expected_values = {"x": [1, 0, 3, 0, 5, 0], "y": [0, 2, 0, 4, 0, 6]}
    expected_df = pd.DataFrame(expected_values, index=expected_dates)
    assert_frame_equal(lib.read(sym1).data, expected_df)


def test_merge_strings(lmdb_library_static_dynamic):
    lib = lmdb_library_static_dynamic

    dates1 = [np.datetime64("2023-01-01"), np.datetime64("2023-01-03"), np.datetime64("2023-01-05")]
    dates2 = [np.datetime64("2023-01-02"), np.datetime64("2023-01-04"), np.datetime64("2023-01-06")]

    data1 = {"x": [1, 3, 5], "y": ["one", "three", "five"]}
    data2 = {"x": [2, 4, 6], "y": ["two", "four", "six"]}

    df1 = pd.DataFrame(data1, index=dates1)
    df2 = pd.DataFrame(data2, index=dates2)

    sym1 = "symbol_1"
    lib.write(sym1, df1, staged=True)
    lib.write(sym1, df2, staged=True)
    lib.sort_and_finalize_staged_data(sym1)

    expected_dates = [
        np.datetime64("2023-01-01"),
        np.datetime64("2023-01-02"),
        np.datetime64("2023-01-03"),
        np.datetime64("2023-01-04"),
        np.datetime64("2023-01-05"),
        np.datetime64("2023-01-06"),
    ]

    expected_values = {"x": [1, 2, 3, 4, 5, 6], "y": ["one", "two", "three", "four", "five", "six"]}
    expected_df = pd.DataFrame(expected_values, index=expected_dates)
    assert_frame_equal(lib.read(sym1).data, expected_df)


def test_merge_strings_dynamic(lmdb_library_dynamic_schema):
    lib = lmdb_library_dynamic_schema

    dates1 = [np.datetime64("2023-01-01"), np.datetime64("2023-01-03"), np.datetime64("2023-01-05")]
    dates2 = [np.datetime64("2023-01-02"), np.datetime64("2023-01-04"), np.datetime64("2023-01-06")]

    data1 = {"x": ["one", "three", "five"]}
    data2 = {"y": ["two", "four", "six"]}

    df1 = pd.DataFrame(data1, index=dates1)
    df2 = pd.DataFrame(data2, index=dates2)

    sym1 = "symbol_1"
    lib.write(sym1, df1, staged=True)
    lib.write(sym1, df2, staged=True)
    lib.sort_and_finalize_staged_data(sym1)

    expected_dates = [
        np.datetime64("2023-01-01"),
        np.datetime64("2023-01-02"),
        np.datetime64("2023-01-03"),
        np.datetime64("2023-01-04"),
        np.datetime64("2023-01-05"),
        np.datetime64("2023-01-06"),
    ]

    expected_values = {"x": ["one", None, "three", None, "five", None], "y": [None, "two", None, "four", None, "six"]}
    expected_df = pd.DataFrame(expected_values, index=expected_dates)
    assert_frame_equal(lib.read(sym1).data, expected_df)


def test_unordered_segment(lmdb_library_static_dynamic):
    lib = lmdb_library_static_dynamic
    dates = [np.datetime64("2023-01-03"), np.datetime64("2023-01-01"), np.datetime64("2023-01-05")]
    df = pd.DataFrame({"col": [2, 1, 3]}, index=dates)
    lib.write("sym", df, staged=True, validate_index=False)
    lib.sort_and_finalize_staged_data("sym")
    assert_frame_equal(
        lib.read("sym").data,
        pd.DataFrame(
            {"col": [1, 2, 3]},
            index=[np.datetime64("2023-01-01"), np.datetime64("2023-01-03"), np.datetime64("2023-01-05")],
        ),
    )


def test_repeating_index_values(lmdb_library_static_dynamic):
    lib = lmdb_library_static_dynamic
    dates = [np.datetime64("2023-01-01"), np.datetime64("2023-01-03"), np.datetime64("2023-01-05")]
    df1 = pd.DataFrame({"col": [1, 2, 3]}, index=dates)
    df2 = pd.DataFrame({"col": [4, 5, 6]}, index=dates)
    lib.write("sym", df1, staged=True)
    lib.write("sym", df2, staged=True)
    lib.sort_and_finalize_staged_data("sym")
    data = lib.read("sym").data
    expected = pd.concat([df1, df2]).sort_index()
    assert data.index.equals(expected.index)
    for i in range(0, len(df1)):
        row = 2 * i
        assert data["col"][row] == df1["col"][i] or data["col"][row] == df2["col"][i]
        assert (data["col"][row + 1] == df1["col"][i] or data["col"][row + 1] == df2["col"][i]) and data["col"][
            row
        ] != data["col"][row + 1]


class TestMergeSortAppend:
    def test_appended_values_are_after(self, lmdb_library_static_dynamic):
        lib = lmdb_library_static_dynamic
        initial_df = pd.DataFrame(
            {"col": [1, 2, 3]},
            index=pd.DatetimeIndex(
                [np.datetime64("2023-01-01"), np.datetime64("2023-01-02"), np.datetime64("2023-01-03")],
                dtype="datetime64[ns]",
            ),
        )
        lib.write("sym", initial_df)
        df1 = pd.DataFrame(
            {"col": [4, 7, 8]},
            index=pd.DatetimeIndex(
                [np.datetime64("2023-01-05"), np.datetime64("2023-01-09"), np.datetime64("2023-01-10")],
                dtype="datetime64[ns]",
            ),
        )
        df2 = pd.DataFrame(
            {"col": [5, 6]},
            index=pd.DatetimeIndex([np.datetime64("2023-01-06"), np.datetime64("2023-01-08")], dtype="datetime64[ns]"),
        )
        lib.write("sym", df1, staged=True)
        lib.write("sym", df2, staged=True)
        lib.sort_and_finalize_staged_data("sym", mode=StagedDataFinalizeMethod.APPEND)
        expected_index = pd.DatetimeIndex(
            [
                np.datetime64("2023-01-01"),
                np.datetime64("2023-01-02"),
                np.datetime64("2023-01-03"),
                np.datetime64("2023-01-05"),
                np.datetime64("2023-01-06"),
                np.datetime64("2023-01-08"),
                np.datetime64("2023-01-09"),
                np.datetime64("2023-01-10"),
            ],
            dtype="datetime64[ns]",
        )
        expected_df = pd.DataFrame({"col": range(1, 9)}, index=expected_index)
        assert_frame_equal(lib.read("sym").data, expected_df)

    @pytest.mark.parametrize("delete_staged_data_on_failure", [True, False])
    def test_appended_df_interleaves_with_storage(self, lmdb_library_static_dynamic, delete_staged_data_on_failure):
        lib = lmdb_library_static_dynamic
        initial_df = pd.DataFrame(
            {"col": [1, 3]},
            index=pd.DatetimeIndex([np.datetime64("2023-01-01"), np.datetime64("2023-01-03")], dtype="datetime64[ns]"),
        )
        lib.write("sym", initial_df)
        df1 = pd.DataFrame({"col": [2]}, index=pd.DatetimeIndex([np.datetime64("2023-01-02")], dtype="datetime64[ns]"))
        lib.write("sym", df1, staged=True)
        with pytest.raises(SortingException) as exception_info:
            lib.sort_and_finalize_staged_data(
                "sym", mode=StagedDataFinalizeMethod.APPEND, delete_staged_data_on_failure=delete_staged_data_on_failure
            )
        assert "append" in str(exception_info.value)
        expected_key_count = 0 if delete_staged_data_on_failure else 1
        assert len(get_append_keys(lib, "sym")) == expected_key_count
        assert_delete_staged_data_clears_append_keys(lib, "sym")

    def test_appended_df_start_same_as_df_end(self, lmdb_library_static_dynamic):
        lib = lmdb_library_static_dynamic
        df = pd.DataFrame(
            {"col": [1, 2, 3]},
            index=pd.DatetimeIndex(
                [np.datetime64("2023-01-01"), np.datetime64("2023-01-02"), np.datetime64("2023-01-03")],
                dtype="datetime64[ns]",
            ),
        )
        lib.write("sym", df)
        df_to_append = pd.DataFrame(
            {"col": [4, 5, 6]},
            index=pd.DatetimeIndex(
                [np.datetime64("2023-01-03"), np.datetime64("2023-01-04"), np.datetime64("2023-01-05")],
                dtype="datetime64[ns]",
            ),
        )
        lib.write("sym", df_to_append, staged=True)
        lib.sort_and_finalize_staged_data("sym", mode=StagedDataFinalizeMethod.APPEND)
        res = lib.read("sym").data
        expected_df = pd.concat([df, df_to_append])
        assert_frame_equal(lib.read("sym").data, expected_df)


def test_prune_previous(lmdb_library_static_dynamic):
    lib = lmdb_library_static_dynamic
    idx = pd.DatetimeIndex([np.datetime64("2023-01-01"), np.datetime64("2023-01-03")], dtype="datetime64[ns]")
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
        assert len(get_append_keys(lib, "sym")) == 0

    def test_mixing_empty_and_non_empty_columns(self, lmdb_library_dynamic_schema, mode):
        lib = lmdb_library_dynamic_schema

        df = pd.DataFrame({"a": [1]}, index=pd.DatetimeIndex([pd.Timestamp("1970-01-01")]))
        df2 = pd.DataFrame(
            {"b": np.array([], dtype="float"), "c": np.array([], dtype="int64"), "d": np.array([], dtype="object")},
            index=pd.DatetimeIndex([]),
        )
        lib.write("sym", df, staged=True)
        lib.write("sym", df2, staged=True)
        lib.sort_and_finalize_staged_data("sym", mode=mode)
        if IS_PANDAS_TWO:
            expected = pd.DataFrame(
                {
                    "a": [1],
                    "b": np.array([np.nan], dtype="float"),
                    "c": np.array([0], dtype="int64"),
                    "d": np.array([None], dtype="object"),
                },
                index=[pd.Timestamp("1970-01-01")],
            )
        else:
            expected = pd.DataFrame(
                {
                    "a": [1],
                    "b": np.array([np.nan], dtype="object"),
                    "c": np.array([0], dtype="int64"),
                    "d": np.array([None], dtype="object"),
                },
                index=[pd.Timestamp("1970-01-01")],
            )
        assert_frame_equal(lib.read("sym").data, expected)


def test_append_to_missing_symbol(lmdb_library):
    lib = lmdb_library
    df = pd.DataFrame({"col": [1]}, index=pd.DatetimeIndex([np.datetime64("2023-01-01")], dtype="datetime64[ns]"))
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
    @pytest.mark.parametrize("delete_staged_data_on_failure", [True, False])
    def test_append_throws_with_missmatched_column_set(self, lmdb_library, delete_staged_data_on_failure):
        lib = lmdb_library

        initial_df = pd.DataFrame({"col_0": [1]}, index=pd.DatetimeIndex([pd.Timestamp(2024, 1, 1)]))
        lib.write("sym", initial_df, staged=True)

        appended_df = pd.DataFrame({"col_1": [1]}, index=pd.DatetimeIndex([pd.Timestamp(2024, 1, 2)]))
        lib.write("sym", appended_df, staged=True)

        with pytest.raises(SchemaException) as exception_info:
            lib.sort_and_finalize_staged_data(
                "sym", mode=StagedDataFinalizeMethod.APPEND, delete_staged_data_on_failure=delete_staged_data_on_failure
            )
        assert "col_1" in str(exception_info.value)
        expected_key_count = 0 if delete_staged_data_on_failure else 2
        assert len(get_append_keys(lib, "sym")) == expected_key_count
        assert_delete_staged_data_clears_append_keys(lib, "sym")

    @pytest.mark.parametrize("delete_staged_data_on_failure", [True, False])
    def test_append_throws_column_subset(self, lmdb_library, delete_staged_data_on_failure):
        lib = lmdb_library

        df1 = pd.DataFrame(
            {"col_0": np.array([1.1], dtype="float"), "col_1": np.array([2], dtype="int64")},
            index=pd.DatetimeIndex([pd.Timestamp("2024-01-01")]),
        )
        lib.write("sym", df1, staged=True)

        df2 = pd.DataFrame({"col_1": [1]}, index=pd.DatetimeIndex([pd.Timestamp("2024-01-02")]))
        lib.write("sym", df2, staged=True)

        with pytest.raises(SchemaException) as exception_info:
            lib.sort_and_finalize_staged_data(
                "sym", StagedDataFinalizeMethod.APPEND, delete_staged_data_on_failure=delete_staged_data_on_failure
            )
        assert "col_0" in str(exception_info.value)
        assert "col_1" in str(exception_info.value)
        expected_key_count = 0 if delete_staged_data_on_failure else 2
        assert len(get_append_keys(lib, "sym")) == expected_key_count
        assert_delete_staged_data_clears_append_keys(lib, "sym")

    @pytest.mark.parametrize("delete_staged_data_on_failure", [True, False])
    def test_appending_reordered_column_set_throws(self, lmdb_library, delete_staged_data_on_failure):
        lib = lmdb_library

        df1 = pd.DataFrame(
            {"col_0": [1], "col_1": ["test"], "col_2": [1.2]}, index=pd.DatetimeIndex([pd.Timestamp(2024, 1, 1)])
        )
        lib.write("sym", df1, staged=True)

        df2 = pd.DataFrame(
            {"col_1": ["asd"], "col_2": [2.5], "col_0": [2]}, index=pd.DatetimeIndex([pd.Timestamp(2024, 1, 2)])
        )
        lib.write("sym", df2, staged=True)

        with pytest.raises(SchemaException) as exception_info:
            lib.sort_and_finalize_staged_data(
                "sym", mode=StagedDataFinalizeMethod.APPEND, delete_staged_data_on_failure=delete_staged_data_on_failure
            )
        assert "col_0" in str(exception_info.value)
        assert "col_1" in str(exception_info.value)
        assert "col_2" in str(exception_info.value)
        expected_key_count = 0 if delete_staged_data_on_failure else 2
        assert len(get_append_keys(lib, "sym")) == expected_key_count
        assert_delete_staged_data_clears_append_keys(lib, "sym")

    @pytest.mark.parametrize("delete_staged_data_on_failure", [True, False])
    def test_append_throws_on_incompatible_dtype(self, lmdb_library, delete_staged_data_on_failure):
        lib = lmdb_library

        df1 = pd.DataFrame({"col_0": np.array([1], dtype="int64")}, index=pd.DatetimeIndex([pd.Timestamp(2024, 1, 1)]))
        lib.write("sym", df1, staged=True)

        df2 = pd.DataFrame({"col_0": ["asd"]}, index=pd.DatetimeIndex([pd.Timestamp(2024, 1, 2)]))
        lib.write("sym", df2, staged=True)

        with pytest.raises(SchemaException) as exception_info:
            lib.sort_and_finalize_staged_data(
                "sym", mode=StagedDataFinalizeMethod.APPEND, delete_staged_data_on_failure=delete_staged_data_on_failure
            )
        assert "col_0" in str(exception_info.value)
        assert "INT64" in str(exception_info.value)
        expected_key_count = 0 if delete_staged_data_on_failure else 2
        assert len(get_append_keys(lib, "sym")) == expected_key_count
        assert_delete_staged_data_clears_append_keys(lib, "sym")

    @pytest.mark.parametrize("delete_staged_data_on_failure", [True, False])
    @pytest.mark.parametrize("mode", [StagedDataFinalizeMethod.APPEND, StagedDataFinalizeMethod.WRITE])
    def test_types_cant_be_promoted(self, lmdb_library, mode, delete_staged_data_on_failure):
        lib = lmdb_library

        df1 = pd.DataFrame(
            {"col_0": np.array([1], dtype="float")}, index=pd.DatetimeIndex([np.datetime64("2023-01-01")])
        )
        lib.write("sym", df1, staged=True)

        df2 = pd.DataFrame({"col_0": np.array([1], dtype="int")}, index=pd.DatetimeIndex([np.datetime64("2023-01-02")]))
        lib.write("sym", df2, staged=True)

        with pytest.raises(SchemaException) as exception_info:
            lib.sort_and_finalize_staged_data(
                "sym", mode=mode, delete_staged_data_on_failure=delete_staged_data_on_failure
            )
        assert "col_0" in str(exception_info.value)
        assert "FLOAT" in str(exception_info.value)
        assert "INT" in str(exception_info.value)
        expected_key_count = 0 if delete_staged_data_on_failure else 2
        assert len(get_append_keys(lib, "sym")) == expected_key_count
        assert_delete_staged_data_clears_append_keys(lib, "sym")

    @pytest.mark.parametrize("delete_staged_data_on_failure", [True, False])
    @pytest.mark.parametrize("mode", [StagedDataFinalizeMethod.APPEND, StagedDataFinalizeMethod.WRITE])
    def test_type_mismatch_in_staged_segments_throws_with_non_promotoable_types(
        self, lmdb_library, mode, delete_staged_data_on_failure
    ):
        lib = lmdb_library

        df1 = pd.DataFrame({"col": np.array([1], dtype="int64")}, index=pd.DatetimeIndex([np.datetime64("2023-01-01")]))
        lib.write("sym", df1, staged=True)

        df2 = pd.DataFrame({"col": ["test"]}, index=pd.DatetimeIndex([np.datetime64("2023-01-02")]))
        lib.write("sym", df2, staged=True)

        with pytest.raises(SchemaException) as exception_info:
            lib.sort_and_finalize_staged_data(
                "sym", mode=mode, delete_staged_data_on_failure=delete_staged_data_on_failure
            )
        assert "INT" in str(exception_info.value)
        assert "UTF_DYNAMIC" in str(exception_info.value)
        expected_key_count = 0 if delete_staged_data_on_failure else 2
        assert len(get_append_keys(lib, "sym")) == expected_key_count
        assert_delete_staged_data_clears_append_keys(lib, "sym")

    @pytest.mark.parametrize("delete_staged_data_on_failure", [True, False])
    @pytest.mark.parametrize("mode", [StagedDataFinalizeMethod.APPEND, StagedDataFinalizeMethod.WRITE])
    def test_staged_segments_cant_be_reordered(self, lmdb_library, mode, delete_staged_data_on_failure):
        lib = lmdb_library

        df1 = pd.DataFrame(
            {"col_0": [1], "col_1": ["test"], "col_2": [1.2]}, index=pd.DatetimeIndex([pd.Timestamp(2024, 1, 1)])
        )
        lib.write("sym", df1, staged=True)

        df2 = pd.DataFrame(
            {"col_1": ["asd"], "col_2": [2.5], "col_0": [2]}, index=pd.DatetimeIndex([pd.Timestamp(2024, 1, 2)])
        )
        lib.write("sym", df2, staged=True)

        with pytest.raises(SchemaException) as exception_info:
            lib.sort_and_finalize_staged_data(
                "sym", mode=mode, delete_staged_data_on_failure=delete_staged_data_on_failure
            )
        assert "col_0" in str(exception_info.value)
        assert "col_1" in str(exception_info.value)
        assert "col_2" in str(exception_info.value)
        expected_key_count = 0 if delete_staged_data_on_failure else 2
        assert len(get_append_keys(lib, "sym")) == expected_key_count
        assert_delete_staged_data_clears_append_keys(lib, "sym")


class TestStreamDescriptorMismatchOnFinalizeAppend:
    def init_symbol(self, lib, sym):
        df = pd.DataFrame(
            {
                "col_0": np.array([1], dtype="int32"),
                "col_1": np.array([0.5], dtype="float64"),
                "col_2": np.array(["val"], dtype="object"),
            },
            index=pd.DatetimeIndex([pd.Timestamp(2024, 1, 1)]),
        )
        lib.write(sym, df)

    @pytest.mark.parametrize("delete_staged_data_on_failure", [True, False])
    def test_cannot_append_column_subset(self, lmdb_library, delete_staged_data_on_failure):
        lib = lmdb_library
        self.init_symbol(lib, "sym")

        df = pd.DataFrame({"col_0": np.array([1], dtype="int32")}, index=pd.DatetimeIndex([pd.Timestamp(2024, 1, 1)]))
        lib.write("sym", df, staged=True)
        with pytest.raises(SchemaException) as exception_info:
            lib.sort_and_finalize_staged_data(
                "sym", mode=StagedDataFinalizeMethod.APPEND, delete_staged_data_on_failure=delete_staged_data_on_failure
            )
        assert "col_1" in str(exception_info.value)
        assert "col_2" in str(exception_info.value)
        expected_key_count = 0 if delete_staged_data_on_failure else 1
        assert len(get_append_keys(lib, "sym")) == expected_key_count
        assert_delete_staged_data_clears_append_keys(lib, "sym")

    @pytest.mark.parametrize("delete_staged_data_on_failure", [True, False])
    def test_cannot_append_reordered_columns(self, lmdb_library, delete_staged_data_on_failure):
        lib = lmdb_library
        self.init_symbol(lib, "sym")

        df = pd.DataFrame(
            {
                "col_1": np.array([1.4], dtype="float64"),
                "col_0": np.array([5], dtype="int32"),
                "col_2": np.array(["val"], dtype="object"),
            },
            index=pd.DatetimeIndex([pd.Timestamp(2024, 1, 1)]),
        )
        lib.write("sym", df, staged=True)
        with pytest.raises(SchemaException) as exception_info:
            lib.sort_and_finalize_staged_data(
                "sym", mode=StagedDataFinalizeMethod.APPEND, delete_staged_data_on_failure=delete_staged_data_on_failure
            )
        assert "col_0" in str(exception_info.value)
        assert "col_1" in str(exception_info.value)
        assert "col_2" in str(exception_info.value)
        expected_key_count = 0 if delete_staged_data_on_failure else 1
        assert len(get_append_keys(lib, "sym")) == expected_key_count
        assert_delete_staged_data_clears_append_keys(lib, "sym")

    @pytest.mark.parametrize("delete_staged_data_on_failure", [True, False])
    def test_cannot_promote_stored_type(self, lmdb_library, delete_staged_data_on_failure):
        lib = lmdb_library
        self.init_symbol(lib, "sym")

        df = pd.DataFrame(
            {
                "col_0": np.array([1], dtype="int64"),
                "col_1": np.array([5], dtype="float64"),
                "col_2": np.array(["val"], dtype="object"),
            },
            index=pd.DatetimeIndex([pd.Timestamp(2024, 1, 1)]),
        )
        lib.write("sym", df, staged=True)
        with pytest.raises(SchemaException) as exception_info:
            lib.sort_and_finalize_staged_data(
                "sym", mode=StagedDataFinalizeMethod.APPEND, delete_staged_data_on_failure=delete_staged_data_on_failure
            )
        assert "col_0" in str(exception_info.value)
        assert "INT32" in str(exception_info.value)
        assert "INT64" in str(exception_info.value)
        expected_key_count = 0 if delete_staged_data_on_failure else 1
        assert len(get_append_keys(lib, "sym")) == expected_key_count
        assert_delete_staged_data_clears_append_keys(lib, "sym")

    @pytest.mark.parametrize("delete_staged_data_on_failure", [True, False])
    def test_cannot_promote_input_type(self, lmdb_library, delete_staged_data_on_failure):
        lib = lmdb_library
        self.init_symbol(lib, "sym")

        df = pd.DataFrame(
            {
                "col_0": np.array([1], dtype="int16"),
                "col_1": np.array([5], dtype="float64"),
                "col_2": np.array(["val"], dtype="object"),
            },
            index=pd.DatetimeIndex([pd.Timestamp(2024, 1, 1)]),
        )
        lib.write("sym", df, staged=True)
        with pytest.raises(SchemaException) as exception_info:
            lib.sort_and_finalize_staged_data(
                "sym", mode=StagedDataFinalizeMethod.APPEND, delete_staged_data_on_failure=delete_staged_data_on_failure
            )
        assert "col_0" in str(exception_info.value)
        assert "INT32" in str(exception_info.value)
        assert "INT16" in str(exception_info.value)
        expected_key_count = 0 if delete_staged_data_on_failure else 1
        assert len(get_append_keys(lib, "sym")) == expected_key_count
        assert_delete_staged_data_clears_append_keys(lib, "sym")

    @pytest.mark.parametrize("delete_staged_data_on_failure", [True, False])
    def test_cannot_add_new_columns(self, lmdb_library, delete_staged_data_on_failure):
        lib = lmdb_library
        self.init_symbol(lib, "sym")

        df = pd.DataFrame(
            {
                "col_0": np.array([1], dtype="int32"),
                "col_1": np.array([5], dtype="float64"),
                "col_2": np.array(["val"], dtype="object"),
                "col_3": np.array([1], dtype="int32"),
            },
            index=pd.DatetimeIndex([pd.Timestamp(2024, 1, 1)]),
        )
        lib.write("sym", df, staged=True)
        with pytest.raises(SchemaException) as exception_info:
            lib.sort_and_finalize_staged_data(
                "sym", mode=StagedDataFinalizeMethod.APPEND, delete_staged_data_on_failure=delete_staged_data_on_failure
            )
        assert "col_3" in str(exception_info.value)
        expected_key_count = 0 if delete_staged_data_on_failure else 1
        assert len(get_append_keys(lib, "sym")) == expected_key_count
        assert_delete_staged_data_clears_append_keys(lib, "sym")


@pytest.mark.parametrize("mode", [StagedDataFinalizeMethod.APPEND, StagedDataFinalizeMethod.WRITE])
class TestNatInIndexNotAllowed:

    @classmethod
    def assert_nat_not_allowed(cls, lib, symbol, mode, delete_staged_data_on_failure):
        with pytest.raises(SortingException) as exception_info:
            lib.sort_and_finalize_staged_data(
                symbol, mode=mode, delete_staged_data_on_failure=delete_staged_data_on_failure
            )
        assert "NaT" in str(exception_info.value)

    @pytest.mark.parametrize("delete_staged_data_on_failure", [True, False])
    def test_index_only_nat(self, lmdb_library_static_dynamic, mode, delete_staged_data_on_failure):
        lib = lmdb_library_static_dynamic

        df1 = pd.DataFrame({"a": [1, 2]}, index=pd.DatetimeIndex([pd.NaT, pd.NaT]))
        lib.write("sym", df1, staged=True, validate_index=False)
        self.assert_nat_not_allowed(lib, "sym", mode, delete_staged_data_on_failure)
        expected_key_count = 0 if delete_staged_data_on_failure else 1
        assert len(get_append_keys(lib, "sym")) == expected_key_count
        assert_delete_staged_data_clears_append_keys(lib, "sym")

    @pytest.mark.parametrize("delete_staged_data_on_failure", [True, False])
    def test_nat_and_valid_date(self, lmdb_library_static_dynamic, mode, delete_staged_data_on_failure):
        lib = lmdb_library_static_dynamic

        df1 = pd.DataFrame({"a": [1, 2]}, index=pd.DatetimeIndex([pd.NaT, pd.Timestamp(2024, 1, 1)]))
        lib.write("sym", df1, staged=True, validate_index=False)
        self.assert_nat_not_allowed(lib, "sym", mode, delete_staged_data_on_failure)
        expected_key_count = 0 if delete_staged_data_on_failure else 1
        assert len(get_append_keys(lib, "sym")) == expected_key_count
        assert_delete_staged_data_clears_append_keys(lib, "sym")

        df1 = pd.DataFrame({"a": [1, 2]}, index=pd.DatetimeIndex([pd.Timestamp(2024, 1, 1), pd.NaT]))
        lib.write("sym", df1, staged=True, validate_index=False)
        self.assert_nat_not_allowed(lib, "sym", mode, delete_staged_data_on_failure)
        expected_key_count = 0 if delete_staged_data_on_failure else 1
        assert len(get_append_keys(lib, "sym")) == expected_key_count
        assert_delete_staged_data_clears_append_keys(lib, "sym")


class TestSortMergeDynamicSchema:

    def test_appended_columns_are_subset(self, lmdb_library_dynamic_schema):
        lib = lmdb_library_dynamic_schema

        lib.write("sym", pd.DataFrame({"a": [1], "b": [1.2]}, index=pd.DatetimeIndex([pd.Timestamp(2024, 1, 1)])))

        lib.write("sym", pd.DataFrame({"a": [2]}, index=pd.DatetimeIndex([pd.Timestamp(2024, 1, 2)])), staged=True)
        lib.write("sym", pd.DataFrame({"b": [5.3]}, index=pd.DatetimeIndex([pd.Timestamp(2024, 1, 3)])), staged=True)
        lib.sort_and_finalize_staged_data("sym", mode=StagedDataFinalizeMethod.APPEND)

        expected = pd.DataFrame(
            {"a": [1, 2, 0], "b": [1.2, np.nan, 5.3]}, index=pd.date_range("2024-01-01", "2024-01-03")
        )
        stored = lib.read("sym").data

        assert_frame_equal(expected, stored)

    def test_can_append_new_columns(self, lmdb_library_dynamic_schema):
        lib = lmdb_library_dynamic_schema

        lib.write("sym", pd.DataFrame({"a": [1]}, index=pd.DatetimeIndex([pd.Timestamp(2024, 1, 1)])))

        lib.write("sym", pd.DataFrame({"b": [1.5]}, index=pd.DatetimeIndex([pd.Timestamp(2024, 1, 2)])), staged=True)
        lib.write("sym", pd.DataFrame({"c": ["c"]}, index=pd.DatetimeIndex([pd.Timestamp(2024, 1, 3)])), staged=True)
        lib.sort_and_finalize_staged_data("sym", mode=StagedDataFinalizeMethod.APPEND)

        expected = pd.DataFrame(
            {"a": [1, 0, 0], "b": [np.nan, 1.5, np.nan], "c": [None, None, "c"]},
            index=pd.date_range("2024-01-01", "2024-01-03"),
        )
        stored = lib.read("sym").data

        assert_frame_equal(expected, stored, check_like=True)

    def test_staged_segments_are_promoted(self, lmdb_library_dynamic_schema):
        lib = lmdb_library_dynamic_schema
        df1 = pd.DataFrame(
            {
                "col_0": np.array([1], dtype="int16"),
                "col_1": np.array([2], dtype="int64"),
                "col_3": np.array([3], dtype="int32"),
            },
            index=pd.DatetimeIndex([pd.Timestamp(2024, 1, 1)]),
        )
        lib.write("sym", df1, staged=True)

        df2 = pd.DataFrame(
            {
                "col_0": np.array([10], dtype="int32"),
                "col_1": np.array([20], dtype="int16"),
                "col_3": np.array([30], dtype="float32"),
            },
            index=pd.DatetimeIndex([pd.Timestamp(2024, 1, 2)]),
        )
        lib.write("sym", df2, staged=True)

        lib.sort_and_finalize_staged_data("sym")

        expected = pd.DataFrame(
            {
                "col_0": np.array([1, 10], dtype="int32"),
                "col_1": np.array([2, 20], dtype="int64"),
                "col_3": np.array([3, 30], dtype="float64"),
            },
            index=pd.DatetimeIndex([pd.Timestamp(2024, 1, 1), pd.Timestamp(2024, 1, 2)]),
        )
        assert_frame_equal(lib.read("sym").data, expected, check_dtype=True)

    def test_finalize_append_promotes_types(self, lmdb_library_dynamic_schema):
        lib = lmdb_library_dynamic_schema
        df1 = pd.DataFrame(
            {
                "col_0": np.array([1], dtype="int16"),
                "col_1": np.array([2], dtype="int64"),
                "col_3": np.array([3], dtype="int32"),
            },
            index=pd.DatetimeIndex([pd.Timestamp(2024, 1, 1)]),
        )
        lib.write("sym", df1)

        df2 = pd.DataFrame(
            {
                "col_0": np.array([10], dtype="int32"),
                "col_1": np.array([20], dtype="int16"),
                "col_3": np.array([30], dtype="float32"),
            },
            index=pd.DatetimeIndex([pd.Timestamp(2024, 1, 2)]),
        )
        lib.write("sym", df2, staged=True)

        lib.sort_and_finalize_staged_data("sym", mode=StagedDataFinalizeMethod.APPEND)

        expected = pd.DataFrame(
            {
                "col_0": np.array([1, 10], dtype="int32"),
                "col_1": np.array([2, 20], dtype="int64"),
                "col_3": np.array([3, 30], dtype="float64"),
            },
            index=pd.DatetimeIndex([pd.Timestamp(2024, 1, 1), pd.Timestamp(2024, 1, 2)]),
        )
        assert_frame_equal(lib.read("sym").data, expected, check_dtype=True)


def test_update_symbol_list(lmdb_library):
    lib = lmdb_library
    lib_tool = lmdb_library._nvs.library_tool()
    sym = "sym"
    sym_2 = "sym_2"
    df = pd.DataFrame({"col": [1]}, index=pd.DatetimeIndex([np.datetime64("2023-01-01")], dtype="datetime64[ns]"))

    # We always add to the symbol list on write
    lib.write(sym, df, staged=True)
    lib.sort_and_finalize_staged_data(sym, mode=StagedDataFinalizeMethod.WRITE)
    assert lib_tool.count_keys(KeyType.SYMBOL_LIST) == 1
    assert lib.list_symbols() == [sym]

    # We don't add to the symbol on append when there is an existing version
    lib.write(sym, df, staged=True)
    lib.sort_and_finalize_staged_data(sym, mode=StagedDataFinalizeMethod.APPEND)
    assert lib_tool.count_keys(KeyType.SYMBOL_LIST) == 1
    assert lib.list_symbols() == [sym]

    # We always add to the symbol list on write, even when there is an existing version
    lib.write(sym, df, staged=True)
    lib.sort_and_finalize_staged_data(sym, mode=StagedDataFinalizeMethod.WRITE)
    assert lib_tool.count_keys(KeyType.SYMBOL_LIST) == 2
    assert lib.list_symbols() == [sym]

    # We add to the symbol list on append when there is no previous version
    lib.write(sym_2, df, staged=True)
    lib.sort_and_finalize_staged_data(sym_2, mode=StagedDataFinalizeMethod.APPEND)
    assert lib_tool.count_keys(KeyType.SYMBOL_LIST) == 3
    assert set(lib.list_symbols()) == {sym, sym_2}


def test_delete_staged_data(lmdb_library):
    lib = lmdb_library
    start_date = pd.Timestamp(2024, 1, 1)
    for i in range(0, 10):
        lmdb_library.write(
            "sym", pd.DataFrame({"col": [i]}, index=pd.DatetimeIndex([start_date + pd.Timedelta(days=1)])), staged=True
        )
    assert len(get_append_keys(lib, "sym")) == 10
    assert_delete_staged_data_clears_append_keys(lib, "sym")


@pytest.mark.parametrize("mode", [StagedDataFinalizeMethod.APPEND, StagedDataFinalizeMethod.WRITE])
def test_get_staged_symbols(lmdb_library, mode):
    lib = lmdb_library
    df = pd.DataFrame({"col": [1]}, index=pd.DatetimeIndex([pd.Timestamp(2024, 1, 1)]))

    for i in range(0, 3):
        lib.write(f"sym_{i}", df, staged=True)

    assert set(lib.get_staged_symbols()) == set([f"sym_{i}" for i in range(0, 3)])

    lib.sort_and_finalize_staged_data("sym_0", mode=mode)
    assert set(lib.get_staged_symbols()) == set([f"sym_{i}" for i in range(1, 3)])

    lib.delete_staged_data("sym_1")
    assert lib.get_staged_symbols() == ["sym_2"]

    lib.write(
        "sym_2", pd.DataFrame({"not_matching": [2]}, index=pd.DatetimeIndex([pd.Timestamp(2024, 1, 2)])), staged=True
    )
    with pytest.raises(SchemaException):
        lib.sort_and_finalize_staged_data("sym_2", delete_staged_data_on_failure=True)
    assert lib.get_staged_symbols() == []


class TestSlicing:
    def test_append_long_segment(self, lmdb_library):
        with config_context("Merge.SegmentSize", 5):
            lib = lmdb_library
            df_0 = pd.DataFrame({"col_0": [1, 2, 3]}, index=pd.date_range("2024-01-01", "2024-01-03"))
            lib.write("sym", df_0)

            index = pd.date_range("2024-01-05", "2024-01-15")
            df_1 = pd.DataFrame({"col_0": range(0, len(index))}, index=index)
            lib.write("sym", df_1, staged=True)
            lib.sort_and_finalize_staged_data("sym", mode=StagedDataFinalizeMethod.APPEND)

            assert_frame_equal(lib.read("sym").data, pd.concat([df_0, df_1]))

    def test_write_long_segment(self, lmdb_library):
        with config_context("Merge.SegmentSize", 5):
            lib = lmdb_library
            index = pd.date_range("2024-01-05", "2024-01-15")
            df = pd.DataFrame({"col_0": range(0, len(index))}, index=index)
            lib.write("sym", df, staged=True)
            lib.sort_and_finalize_staged_data("sym", mode=StagedDataFinalizeMethod.WRITE)
            assert_frame_equal(lib.read("sym").data, df)

    def test_write_several_segments_triggering_slicing(self, lmdb_library):
        with config_context("Merge.SegmentSize", 5):
            lib = lmdb_library
            combined_staged_index = pd.date_range(pd.Timestamp(2024, 1, 1), pd.Timestamp(2024, 1, 15))
            staged_values = range(0, len(combined_staged_index))
            for value, date in zip(staged_values, combined_staged_index):
                df = pd.DataFrame({"a": [value]}, index=pd.DatetimeIndex([date]))
                lib.write("sym", df, staged=True)
            lib.sort_and_finalize_staged_data("sym", mode=StagedDataFinalizeMethod.WRITE)
            expected = pd.DataFrame({"a": staged_values}, index=combined_staged_index)
            assert_frame_equal(lib.read("sym").data, expected)

    def test_append_several_segments_trigger_slicing(self, lmdb_library):
        with config_context("Merge.SegmentSize", 5):
            lib = lmdb_library
            df_0 = pd.DataFrame(
                {"a": [1, 2, 3]}, index=pd.date_range(pd.Timestamp(2024, 1, 1), pd.Timestamp(2024, 1, 3))
            )
            lib.write("sym", df_0)
            combined_staged_index = pd.date_range(pd.Timestamp(2024, 1, 5), pd.Timestamp(2024, 1, 20))
            staged_values = range(0, len(combined_staged_index))
            for value, date in zip(staged_values, combined_staged_index):
                df = pd.DataFrame({"a": [value]}, index=pd.DatetimeIndex([date]))
                lib.write("sym", df, staged=True)
            lib.sort_and_finalize_staged_data("sym", mode=StagedDataFinalizeMethod.APPEND)
            expected = pd.concat([df_0, pd.DataFrame({"a": staged_values}, index=combined_staged_index)])
            assert_frame_equal(lib.read("sym").data, expected)

    @pytest.mark.parametrize("delete_staged_data_on_failure", [True, False])
    @pytest.mark.parametrize("mode", [StagedDataFinalizeMethod.APPEND, StagedDataFinalizeMethod.WRITE])
    def test_wide_segment_with_no_prior_slicing(self, lmdb_storage, lib_name, mode, delete_staged_data_on_failure):
        columns_per_segment = 5
        dataframe_columns = 2 * columns_per_segment
        lib = lmdb_storage.create_arctic().create_library(
            lib_name, library_options=LibraryOptions(columns_per_segment=columns_per_segment)
        )
        df_0 = pd.DataFrame(
            {f"col_{i}": [i] for i in range(0, dataframe_columns)}, index=pd.DatetimeIndex([pd.Timestamp(2024, 1, 1)])
        )
        # Initial staged write of wide dataframe is allowed
        lib.write("sym", df_0, staged=True)
        lib.sort_and_finalize_staged_data("sym", mode=mode)
        assert_frame_equal(lib.read("sym").data, df_0)

        # Appending to unsliced dataframe is allowed
        df_1 = pd.DataFrame(
            {f"col_{i}": [i] for i in range(0, dataframe_columns)}, index=pd.DatetimeIndex([pd.Timestamp(2024, 1, 2)])
        )
        lib.append("sym", df_1)
        assert_frame_equal(lib.read("sym").data, pd.concat([df_0, df_1]))
        # Cannot perform another sort and finalize append when column sliced data has been written even though the first
        # write is done using sort and finalize
        lib.write(
            "sym",
            pd.DataFrame(
                {f"col_{i}": [i] for i in range(0, dataframe_columns)},
                index=pd.DatetimeIndex([pd.Timestamp(2024, 1, 3)]),
            ),
            staged=True,
        )
        with pytest.raises(UserInputException) as exception_info:
            lib.sort_and_finalize_staged_data(
                "sym", mode=StagedDataFinalizeMethod.APPEND, delete_staged_data_on_failure=delete_staged_data_on_failure
            )
        assert "append" in str(exception_info.value).lower()
        assert "column" in str(exception_info.value).lower()
        assert "sliced" in str(exception_info.value).lower()
        expected_key_count = 0 if delete_staged_data_on_failure else 1
        assert len(get_append_keys(lib, "sym")) == expected_key_count

    @pytest.mark.parametrize("delete_staged_data_on_failure", [True, False])
    @pytest.mark.parametrize("mode", [StagedDataFinalizeMethod.APPEND, StagedDataFinalizeMethod.WRITE])
    def test_update_wide_staged_segment(self, lmdb_storage, lib_name, mode, delete_staged_data_on_failure):
        columns_per_segment = 5
        dataframe_columns = 2 * columns_per_segment
        lib = lmdb_storage.create_arctic().create_library(
            lib_name, library_options=LibraryOptions(columns_per_segment=columns_per_segment)
        )
        df_0 = pd.DataFrame(
            {f"col_{i}": [1, 2, 3] for i in range(0, dataframe_columns)},
            index=pd.DatetimeIndex(pd.date_range(pd.Timestamp(2024, 1, 1), pd.Timestamp(2024, 1, 3))),
        )
        # Initial staged write of wide dataframe is allowed
        lib.write("sym", df_0, staged=True)
        lib.sort_and_finalize_staged_data("sym", mode=mode)
        assert_frame_equal(lib.read("sym").data, df_0)

        # Updating unsliced dataframe is allowed
        update_df = pd.DataFrame(
            {f"col_{i}": [4] for i in range(0, dataframe_columns)}, index=pd.DatetimeIndex([pd.Timestamp(2024, 1, 2)])
        )
        lib.update("sym", update_df)

        expected = pd.DataFrame(
            {f"col_{i}": [1, 4, 3] for i in range(0, dataframe_columns)},
            index=pd.DatetimeIndex(pd.date_range(pd.Timestamp(2024, 1, 1), pd.Timestamp(2024, 1, 3))),
        )
        assert_frame_equal(lib.read("sym").data, expected)

        df_1 = pd.DataFrame(
            {f"col_{i}": [5, 6, 7] for i in range(0, dataframe_columns)},
            index=pd.DatetimeIndex(pd.date_range(pd.Timestamp(2024, 1, 4), pd.Timestamp(2024, 1, 6))),
        )
        # Cannot append via sort and finalize because slicing has occurred
        with pytest.raises(UserInputException) as exception_info:
            lib.write("sym", df_1, staged=True)
            lib.sort_and_finalize_staged_data(
                "sym", mode=StagedDataFinalizeMethod.APPEND, delete_staged_data_on_failure=delete_staged_data_on_failure
            )
        assert "append" in str(exception_info.value).lower()
        assert "column" in str(exception_info.value).lower()
        assert "sliced" in str(exception_info.value).lower()
        expected_key_count = 0 if delete_staged_data_on_failure else 1
        assert len(get_append_keys(lib, "sym")) == expected_key_count

        # Can perform regular append
        lib.append("sym", df_1)

        expected = pd.DataFrame(
            {f"col_{i}": [1, 4, 3, 5, 6, 7] for i in range(0, dataframe_columns)},
            index=pd.DatetimeIndex(pd.date_range(pd.Timestamp(2024, 1, 1), pd.Timestamp(2024, 1, 6))),
        )
        assert_frame_equal(lib.read("sym").data, expected)

    @pytest.mark.parametrize("delete_staged_data_on_failure", [True, False])
    def test_appending_wide_segment_throws_with_prior_slicing(
        self, lmdb_storage, lib_name, delete_staged_data_on_failure
    ):
        columns_per_segment = 5
        lib = lmdb_storage.create_arctic().create_library(
            lib_name, library_options=LibraryOptions(columns_per_segment=columns_per_segment)
        )
        df_0 = pd.DataFrame({f"col_{i}": [i] for i in range(0, 10)}, index=pd.DatetimeIndex([pd.Timestamp(2024, 1, 1)]))
        lib.write("sym", df_0)

        df_1 = pd.DataFrame({f"col_{i}": [i] for i in range(0, 10)}, index=pd.DatetimeIndex([pd.Timestamp(2024, 1, 2)]))
        lib.write("sym", df_1, staged=True)

        with pytest.raises(UserInputException) as exception_info:
            lib.sort_and_finalize_staged_data(
                "sym", mode=StagedDataFinalizeMethod.APPEND, delete_staged_data_on_failure=delete_staged_data_on_failure
            )
        assert "append" in str(exception_info.value).lower()
        assert "column" in str(exception_info.value).lower()
        assert "sliced" in str(exception_info.value).lower()
        expected_key_count = 0 if delete_staged_data_on_failure else 1
        assert len(get_append_keys(lib, "sym")) == expected_key_count

    def test_writing_wide_segment_over_sliced_data(self, lmdb_storage, lib_name):
        columns_per_segment = 5
        lib = lmdb_storage.create_arctic().create_library(
            lib_name, library_options=LibraryOptions(columns_per_segment=columns_per_segment)
        )
        df_0 = pd.DataFrame({f"col_{i}": [i] for i in range(0, 10)}, index=pd.DatetimeIndex([pd.Timestamp(2024, 1, 1)]))
        lib.write("sym", df_0)

        df_1 = pd.DataFrame({f"col_{i}": [i] for i in range(0, 10)}, index=pd.DatetimeIndex([pd.Timestamp(2024, 1, 2)]))
        lib.write("sym", df_1, staged=True)

        lib.sort_and_finalize_staged_data("sym", mode=StagedDataFinalizeMethod.WRITE)

        assert_frame_equal(lib.read("sym").data, df_1)


@pytest.mark.parametrize("delete_staged_data_on_failure", [True, False])
@pytest.mark.parametrize("mode", [StagedDataFinalizeMethod.APPEND, StagedDataFinalizeMethod.WRITE])
def test_sort_and_finalize_staged_data_dynamic_schema_named_index(
    lmdb_library_static_dynamic, delete_staged_data_on_failure, mode
):
    lib = lmdb_library_static_dynamic
    sym = "test_sort_and_finalize_staged_data_append_dynamic_schema_named_index"
    df_0 = pd.DataFrame({"col_0": [0], "col_1": [0.5]}, index=pd.date_range("2024-01-01", periods=1))
    df_0.index.name = "date"
    df_1 = pd.DataFrame({"col_0": [1]}, index=pd.date_range("2024-01-02", periods=1))
    if mode == StagedDataFinalizeMethod.APPEND:
        lib.write(sym, df_0)
    else:
        lib.write(sym, df_0, staged=True)
    lib.write(sym, df_1, staged=True)

    with pytest.raises(SchemaException) as exception_info:
        lib.sort_and_finalize_staged_data(
            sym,
            mode=mode,
            delete_staged_data_on_failure=delete_staged_data_on_failure,
        )

    # Make sure that name of the problematic index column
    assert "date" in str(exception_info.value)
    staged_keys = 1 if mode == StagedDataFinalizeMethod.APPEND else 2
    expected_key_count = 0 if delete_staged_data_on_failure else staged_keys
    assert len(get_append_keys(lib, sym)) == expected_key_count


class TestEmptyDataFrames:
    """
    Tests the behavior of appending with compact incomplete when the dataframe on disk is an empty dataframe. It should
    behave the same way is if there is data. Static schema must check index and column names and types, dynamic schema
    should allow appending with differing names and types which are promotable. Index names must match regardless of
    schema type.

    Note with introduction of empty index and empty types (feature flagged at the moment) the tests might have to be
    changed. Refer to TestEmptyIndexPreservesIndexNames class comment in python/tests/unit/arcticdb/version_store/test_empty_writes.py
    """

    def test_append_to_empty(self, lmdb_library):
        lib = lmdb_library
        symbol = "symbol"
        lib.write(symbol, pd.DataFrame({"a": np.array([], np.int64)}, index=pd.DatetimeIndex([])))
        df = pd.DataFrame({"a": [1]}, index=pd.DatetimeIndex([pd.Timestamp(0)]))
        lib.write(symbol, df, staged=True)
        lib.sort_and_finalize_staged_data(symbol, mode=StagedDataFinalizeMethod.APPEND)
        assert_frame_equal(lib.read(symbol).data, df)

    def test_appending_to_empty_with_differing_index_name_fails(self, lmdb_library_static_dynamic, request):
        lib = lmdb_library_static_dynamic
        symbol = "symbol"
        empty = pd.DataFrame({"a": np.array([], np.int64)}, index=pd.DatetimeIndex([], name="my_initial_index"))
        lib.write(symbol, empty)
        df = pd.DataFrame({"a": [1]}, index=pd.DatetimeIndex([pd.Timestamp(0)], name="my_new_index"))
        lib.write(symbol, df, staged=True)
        with pytest.raises(SchemaException) as exception_info:
            lib.sort_and_finalize_staged_data(symbol, mode=StagedDataFinalizeMethod.APPEND)
        assert "index" in str(exception_info.value)
        assert "my_initial_index" in str(exception_info.value)
        assert "my_new_index" in str(exception_info.value)

    @pytest.mark.parametrize(
        "to_append",
        [
            pd.DataFrame({"wrong_col": [1]}, pd.DatetimeIndex([pd.Timestamp(0)])),
            pd.DataFrame({"a": [1], "wrong_col": [2]}, pd.DatetimeIndex([pd.Timestamp(0)])),
        ],
    )
    def test_appending_to_empty_with_differing_columns_fails(self, lmdb_library, to_append):
        lib = lmdb_library
        symbol = "symbol"
        empty = pd.DataFrame({"a": np.array([], np.int64)}, index=pd.DatetimeIndex([]))
        lib.write(symbol, empty)
        lib.write(symbol, to_append, staged=True)
        with pytest.raises(SchemaException, match="wrong_col"):
            lib.sort_and_finalize_staged_data(symbol, mode=StagedDataFinalizeMethod.APPEND)


class TestSegmentsWithNaNAndNone:
    @pytest.mark.parametrize("value", [np.nan, None])
    def test_staged_segment_has_only_nan_none(self, lmdb_library, value):
        symbol = "symbol"
        lib = lmdb_library
        df = pd.DataFrame(
            {"a": 3 * [value]}, index=pd.DatetimeIndex([pd.Timestamp(1), pd.Timestamp(2), pd.Timestamp(3)])
        )
        lib.write(symbol, df, staged=True)
        lib.sort_and_finalize_staged_data(symbol)
        assert_frame_equal(lib.read(symbol).data, df)

    def test_float_column_contains_only_nan_none(self, lmdb_library):
        symbol = "symbol"
        lib = lmdb_library
        df = pd.DataFrame(
            {"a": np.array(3 * [np.nan], dtype=np.float32)},
            index=pd.DatetimeIndex([pd.Timestamp(1), pd.Timestamp(2), pd.Timestamp(3)]),
        )
        lib.write(symbol, df, staged=True)
        lib.sort_and_finalize_staged_data(symbol)
        assert_frame_equal(lib.read(symbol).data, df)

    @pytest.mark.parametrize("rows_per_segment", [2, 100_000])
    def test_multiple_segments_with_nan_none(self, lmdb_library_factory, rows_per_segment):
        symbol = "symbol"
        lib = lmdb_library_factory(arcticdb.LibraryOptions(rows_per_segment=rows_per_segment))
        set_config_int("Merge.SegmentSize", rows_per_segment)
        df1 = pd.DataFrame(
            {"a": [None, np.nan, np.nan]}, index=pd.DatetimeIndex([pd.Timestamp(1), pd.Timestamp(3), pd.Timestamp(5)])
        )
        lib.write(symbol, df1, staged=True)
        df2 = pd.DataFrame(
            {"a": [None, np.nan, None]}, index=pd.DatetimeIndex([pd.Timestamp(2), pd.Timestamp(4), pd.Timestamp(6)])
        )
        lib.write(symbol, df2, staged=True)
        lib.sort_and_finalize_staged_data(symbol)
        expected = pd.DataFrame(
            {"a": [None, None, np.nan, np.nan, np.nan, None]},
            index=pd.DatetimeIndex([pd.Timestamp(i) for i in range(1, 7)]),
        )
        assert_frame_equal(lib.read(symbol).data, expected)

    @pytest.mark.parametrize("rows_per_segment", [2, 100_000])
    def test_input_contains_actual_values(self, lmdb_library_factory, rows_per_segment):
        symbol = "symbol"
        lib = lmdb_library_factory(arcticdb.LibraryOptions(rows_per_segment=rows_per_segment))
        set_config_int("Merge.SegmentSize", rows_per_segment)
        df1 = pd.DataFrame(
            {"a": [None, "a", None]}, index=pd.DatetimeIndex([pd.Timestamp(1), pd.Timestamp(3), pd.Timestamp(5)])
        )
        lib.write(symbol, df1, staged=True)
        df2 = pd.DataFrame(
            {"a": [None, None, "b"]}, index=pd.DatetimeIndex([pd.Timestamp(2), pd.Timestamp(4), pd.Timestamp(6)])
        )
        lib.write(symbol, df2, staged=True)
        lib.sort_and_finalize_staged_data(symbol)
        expected = pd.DataFrame(
            {"a": [None, None, "a", None, None, "b"]}, index=pd.DatetimeIndex([pd.Timestamp(i) for i in range(1, 7)])
        )
        assert_frame_equal(lib.read(symbol).data, expected)
