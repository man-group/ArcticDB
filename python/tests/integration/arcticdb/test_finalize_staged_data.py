"""
Copyright 2026 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

import time
import pandas as pd
import numpy as np

import pytest

from arcticdb.exceptions import UnsortedDataException, UserInputException
from arcticdb.util.test import dataframe_dump_to_log, assert_frame_equal
from arcticdb.util.test_utils import CachedDFGenerator, TimestampNumber, stage_chunks
from arcticdb.version_store.library import Library, StagedDataFinalizeMethod
from tests.stress.arcticdb.version_store.test_stress_finalize_staged_data import generate_chunk_sizes


class CacheParts:

    def __init__(self, model_df: pd.DataFrame):
        self.dataframe = copy_dataframe_structure(model_df)

    def cache_samples_from(self, data_frame: pd.DataFrame):
        self.dataframe = pd.concat([self.dataframe, data_frame.head(1), data_frame.tail(1)])

    def verify_finalized_data(self, lib: Library, symbol: str):
        size = self.dataframe.shape[0]
        print(f"We will inspect {size} rows from both dataframes")
        for indx in range(size):
            expected = self.dataframe.iloc[indx]

            timestamp = expected.name
            actual_df: pd.DataFrame = lib.read(symbol=symbol, date_range=(timestamp, timestamp)).data

            assert 1 == actual_df.shape[0], "There is always one row matching"
            actual_df.iloc[0]
            pd.testing.assert_series_equal(expected, actual_df.iloc[0])
            print(f"Iter[{indx}] Timestamp {timestamp} row in both datatframe matches")


def construct_sample_array(numpy_type: type):
    """
    Constructs sample array with min and max and mid value for given type
    """
    if "str" in str(numpy_type):
        return ["ABCDEFG", None, ""]
    if "bool" in str(numpy_type):
        return np.array([True, False, True], dtype=numpy_type)
    func = np.iinfo
    if "float" in str(numpy_type):
        func = np.finfo
    return np.array([func(numpy_type).min, func(numpy_type).max, func(numpy_type).max / 2], dtype=numpy_type)


def sample_dataframe(start_date, *arr) -> pd.DataFrame:
    """
    Creates a dataframe based on arrays that are passed.
    Arrays will be used as columns data of the dataframe.
    The returned dataframe will be indexed with timestamp
    starting from the given date
    Arrays must be numpy arrays of same size
    """
    date_range = pd.date_range(start=start_date, periods=len(arr[0]), freq="D")
    columns = {}
    cnt = 0
    for ar in arr:
        columns[f"NUMBER{cnt}"] = ar
        cnt = cnt + 1

    return pd.DataFrame(columns, index=date_range)


def verify_dataframe_column(df: pd.DataFrame, row_name, max_type, expected_array_of_column_values):
    """
    Verification of column by type. Especially when dynamic schema was
    used and new columns were added later to original dataseries
    """
    row = 0
    print("EXPECTED NUMBERS:", expected_array_of_column_values)
    print("ACTUAL   NUMBERS:", df[row_name].to_list())
    for number in expected_array_of_column_values:
        actual_value_from_df = df.iloc[row][row_name]

        if pd.isna(number) or (number is None):
            # None and nan handling for new columns
            if "int" in str(max_type):
                assert (
                    actual_value_from_df == 0
                ), f"When adding new integer column, previous missing values should be 0 (zero) for row {row}"
            elif "float" in str(max_type):
                assert pd.isna(
                    actual_value_from_df
                ), f"When adding new float column, previous missing values should be nan (not a number) for row {row}"
            elif "bool" in str(max_type):
                assert (
                    False == actual_value_from_df
                ), f"When adding new boolean column, previous missing values should be False for row {row}"
            else:
                assert (
                    actual_value_from_df is None
                ), f"When adding str/object column, previous missing values should be None for row {row}"
        else:
            if "str" in str(max_type):
                assert (
                    number == actual_value_from_df
                ), f"Number {number} is not same in arcticdb {actual_value_from_df} for row {row}"
            else:
                # When we upcast we first try to evaluate against original number
                # if that fails we try to upcast and evaluate
                assert (number == actual_value_from_df) or (
                    max_type(number) == max_type(actual_value_from_df)
                ), f"Number {number} is not same in arcticdb {actual_value_from_df} for row {row}"
        row += 1


def copy_dataframe_structure(model_df: pd.DataFrame) -> pd.DataFrame:
    dataframe: pd.DataFrame = pd.DataFrame(columns=model_df.columns).astype(model_df.dtypes)
    dataframe.index = pd.Index([], dtype=model_df.index.dtype)
    dataframe.index.name = model_df.index.name
    return dataframe


def concat_all_arrays(*arrays):
    """
    Shorthand for extending an array with 1 or more
    """
    res = []
    for arr in arrays:
        res.extend(arr)
    return res


@pytest.mark.skip(reason="Problem with named indexes Monday#7941575430")
@pytest.mark.parametrize("new_version", [True, False])
@pytest.mark.storage
def test_finalize_empty_dataframe(basic_arctic_library, new_version):
    """
    Primary goal of the test is to finalize with staged empty array that has
    exactly same schema as the one in symbol
    """

    def small_dataframe(start_date):
        date_range = pd.date_range(start=start_date, periods=5, freq="D")
        df = pd.DataFrame({"Column1": [10, 20, 30, 40, 50]}, index=date_range)
        df.index.name = "timestamp"
        return df

    lib = basic_arctic_library
    symbol = "symbol"

    df = small_dataframe("2023-01-01")
    dataframe_dump_to_log("Structure of df", df)
    empty_df = df.drop(df.index)
    dataframe_dump_to_log("empty", empty_df)
    lib.write(symbol, validate_index=True, data=df)
    if new_version:
        # There is no problem to append empty dataframe
        # However later this will result in problem in finalization
        # which will be detected. For some reason the empty dataframe name of the index is
        # accepted as 'index' although it is 'timstamp'
        lib.append(symbol, validate_index=True, data=empty_df)
    else:
        # The problem when we start with an empty dataframe the new version
        # and then we try to append is more severe.
        # Segmentation fault is the result
        lib.write(symbol, validate_index=True, data=empty_df)
    dataframe_dump_to_log("after write + append", lib.read(symbol).data)
    df = small_dataframe("2024-01-01")
    dataframe_dump_to_log("df to be staged", df)
    lib.write(symbol, data=df, validate_index=True, staged=True)
    lib.write(symbol, data=empty_df, validate_index=True, staged=True)
    lib.finalize_staged_data(symbol=symbol, mode=StagedDataFinalizeMethod.APPEND)


def test_finalize_with_upcast_type(lmdb_library_dynamic_schema):
    """
    The test starts with several columns in the dataseries
    which have predefined type. Then we do finalization with several additional
    staged chunks each one of which is redefining columns in higher order type
    For thus each time a chunk is finalized it is supposed to change the type
    of the column upcasting it.

    The test covers only int, uint and float types as we do not have upcast for
    boolean and fo string defined.
    """

    lib = lmdb_library_dynamic_schema
    symbol = "symbol"

    # Upcast np.uint8 -> np.uint16 -> int32 -> float32
    arr_a1 = construct_sample_array(np.uint8)
    arr_a2 = construct_sample_array(np.uint16)
    arr_a3 = construct_sample_array(np.int32)
    arr_a4 = construct_sample_array(np.float32)
    last_type_a = np.float32

    arr_b1 = construct_sample_array(np.int8)
    arr_b2 = construct_sample_array(np.int16)
    arr_b3 = construct_sample_array(np.float32)
    arr_b4 = construct_sample_array(np.float64)
    last_type_b = np.float64

    arr_c1 = construct_sample_array(np.int8)
    arr_c2 = construct_sample_array(np.int16)
    arr_c3 = construct_sample_array(np.int32)
    arr_c4 = construct_sample_array(np.int64)
    last_type_c = np.int64

    df = sample_dataframe("2020-1-1", arr_a1, arr_b1, arr_c1)
    df1 = sample_dataframe("2020-3-1", arr_a2, arr_b2, arr_c2)
    df2 = sample_dataframe("2020-4-1", arr_a3, arr_b3, arr_c3)
    df3 = sample_dataframe("2020-5-1", arr_a4, arr_b4, arr_c4)
    df_all = pd.concat([df, df1, df2, df3])
    dataframe_dump_to_log("DF TO WRITE:", df_all)

    arr_all_a = concat_all_arrays(arr_a1, arr_a2, arr_a3, arr_a4)
    arr_all_b = concat_all_arrays(arr_b1, arr_b2, arr_b3, arr_b4)
    arr_all_c = concat_all_arrays(arr_c1, arr_c2, arr_c3, arr_c4)

    lib.write(symbol, df)
    lib.write(symbol, df1, staged=True)
    lib.write(symbol, df2, staged=True)
    lib.write(symbol, df3, staged=True)
    lib.finalize_staged_data(symbol=symbol, mode=StagedDataFinalizeMethod.APPEND)

    result: pd.DataFrame = lib.read(symbol).data
    dataframe_dump_to_log("RESULT DF:", result)

    verify_dataframe_column(
        df=result, row_name="NUMBER0", max_type=last_type_a, expected_array_of_column_values=arr_all_a
    )
    verify_dataframe_column(
        df=result, row_name="NUMBER2", max_type=last_type_c, expected_array_of_column_values=arr_all_c
    )
    verify_dataframe_column(
        df=result, row_name="NUMBER1", max_type=last_type_b, expected_array_of_column_values=arr_all_b
    )


@pytest.mark.parametrize(
    "mode, validate_index",
    [
        (StagedDataFinalizeMethod.WRITE, True),
        (StagedDataFinalizeMethod.WRITE, False),
        (StagedDataFinalizeMethod.APPEND, True),
        (StagedDataFinalizeMethod.APPEND, False),
    ],
)
def test_finalize_with_unsorted_indexes(lmdb_library_dynamic_schema, mode, validate_index):
    """
    Verify that unsorted dataframes are not finalized
    """
    lib = lmdb_library_dynamic_schema
    symbol = "symbol"

    df = sample_dataframe("2020-1-1", [1, 2, 3])
    df1 = sample_dataframe("2020-2-2", [4])
    df2 = sample_dataframe("2010-1-2", [4])
    df3 = sample_dataframe("2026-1-2", [4])
    df4 = sample_dataframe("2021-1-2", [4])
    df_unsorted = pd.concat([df1, df2, df3, df4])

    lib.write(symbol=symbol, data=df)

    if validate_index:
        with pytest.raises(UnsortedDataException):
            lib.write(symbol=symbol, staged=True, validate_index=True, data=df_unsorted)
        with pytest.raises(UserInputException):
            lib.finalize_staged_data(symbol=symbol, mode=mode, validate_index=False)
    else:
        lib.write(symbol=symbol, staged=True, validate_index=False, data=df_unsorted)
        with pytest.raises(UnsortedDataException):
            lib.finalize_staged_data(symbol=symbol, mode=mode, validate_index=False)

    result: pd.DataFrame = lib.read(symbol).data
    assert_frame_equal(df, result)


def test_finalize_with_upcast_type_new_columns(lmdb_library_dynamic_schema):
    """
    Study the upcast behavior over staging, finalizng, deleting last version
    and staging and finalizing again.

    When upcasted columns that had empty rows would return either 0 or nan depending
    on upcast of the numeric type

    for boolean always False and for str -> None
    """

    lib = lmdb_library_dynamic_schema
    symbol = "symbol"

    # NUMBER0 column definition is:
    # Upcast starting [1] np.uint8 -> [2] np.uint16 -> [3] int32 -> [4] float32
    arr_a1 = construct_sample_array(np.uint8)
    arr_a2 = construct_sample_array(np.uint16)
    arr_a3 = construct_sample_array(np.int32)
    arr_a4 = construct_sample_array(np.float32)
    last_type_a = np.float32

    # NUMBER1 column definition is:
    # Upcast starting [1] <non-existing> -> [2] np.int16 -> [3] float32 -> [4] float64
    arr_b1 = [np.nan, np.nan, np.nan]
    arr_b2 = construct_sample_array(np.int16)
    arr_b3 = construct_sample_array(np.float32)
    arr_b4 = construct_sample_array(np.float64)
    last_type_b = np.float64

    # NUMBER2 column definition is:
    # Upcast starting [1] <non-existing> -> [2] <non-existing> -> [3] int32 -> [4] int64
    arr_c1 = [np.nan, np.nan, np.nan]
    arr_c2 = [np.nan, np.nan, np.nan]
    arr_c3 = construct_sample_array(np.int32)
    arr_c4 = construct_sample_array(np.int64)
    last_type_c = np.int64

    # NUMBER3 column definition is:
    # Upcast starting [1] <non-existing> -> [2] <non-existing> -> [3] str -> [4] str
    arr_str1 = [None, None, None]
    arr_str2 = [None, None, None]
    arr_str3 = ["A", "ABC", "11"]
    arr_str4 = construct_sample_array(str)
    last_type_str = str

    # NUMBER4 column definition is:
    # Upcast starting [1] <non-existing> -> [2] <non-existing> -> [3] <non-existing> -> [4] bool
    arr_bool1 = [np.nan, np.nan, np.nan]
    arr_bool2 = [np.nan, np.nan, np.nan]
    arr_bool3 = [np.nan, np.nan, np.nan]
    arr_bool4 = construct_sample_array(bool)
    last_type_bool = bool

    # NUMBER5 column definition is:
    # Upcast starting [1] <non-existing> -> [2] <non-existing> -> [3] <non-existing> -> [4] float32
    arr_d1 = [np.nan, np.nan, np.nan]
    arr_d2 = [np.nan, np.nan, np.nan]
    arr_d3 = [np.nan, np.nan, np.nan]
    arr_d4 = construct_sample_array(np.float32)
    last_type_d = np.float32

    arr_all_a = concat_all_arrays(arr_a1, arr_a2, arr_a3, arr_a4)
    arr_all_b = concat_all_arrays(arr_b1, arr_b2, arr_b3, arr_b4)
    arr_all_c = concat_all_arrays(arr_c1, arr_c2, arr_c3, arr_c4)
    arr_all_d = concat_all_arrays(arr_d1, arr_d2, arr_d3, arr_d4)
    arr_all_str = concat_all_arrays(arr_str1, arr_str2, arr_str3, arr_str4)
    arr_all_bool = concat_all_arrays(arr_bool1, arr_bool2, arr_bool3, arr_bool4)

    df = sample_dataframe("2020-1-1", arr_a1)
    df1 = sample_dataframe("2020-3-1", arr_a2, arr_b2)
    df2 = sample_dataframe("2020-4-1", arr_a3, arr_b3, arr_c3, arr_str3)
    df3 = sample_dataframe("2020-5-1", arr_a4, arr_b4, arr_c4, arr_str4, arr_bool4, arr_d4)
    df_all = pd.concat([df, df1, df2, df3])
    dataframe_dump_to_log("DF TO WRITE:", df_all)

    # We create 3 versions now
    lib.write(symbol, df)
    lib.write(symbol, df1, staged=True)
    lib.finalize_staged_data(symbol=symbol, mode=StagedDataFinalizeMethod.APPEND)
    lib.write(symbol, df2, staged=True)
    lib.write(symbol, df3, staged=True)
    lib.finalize_staged_data(symbol=symbol, mode=StagedDataFinalizeMethod.APPEND)

    result: pd.DataFrame = lib.read(symbol).data
    dataframe_dump_to_log("RESULT DF:", result)

    verify_dataframe_column(
        df=result, row_name="NUMBER0", max_type=last_type_a, expected_array_of_column_values=arr_all_a
    )
    verify_dataframe_column(
        df=result, row_name="NUMBER1", max_type=last_type_b, expected_array_of_column_values=arr_all_b
    )
    verify_dataframe_column(
        df=result, row_name="NUMBER2", max_type=last_type_c, expected_array_of_column_values=arr_all_c
    )
    verify_dataframe_column(
        df=result, row_name="NUMBER3", max_type=last_type_str, expected_array_of_column_values=arr_all_str
    )
    verify_dataframe_column(
        df=result, row_name="NUMBER5", max_type=last_type_d, expected_array_of_column_values=arr_all_d
    )
    verify_dataframe_column(
        df=result, row_name="NUMBER4", max_type=last_type_bool, expected_array_of_column_values=arr_all_bool
    )

    assert 3 == len(lib.list_versions(symbol=symbol))

    lib.delete(symbol=symbol, versions=2)

    assert 2 == len(lib.list_versions(symbol=symbol))

    result: pd.DataFrame = lib.read(symbol).data
    dataframe_dump_to_log("RESULT DF:", result)

    # We want to validate that after delete last version the empty cell are
    # filled with 0 not nans
    verify_dataframe_column(
        df=result,
        row_name="NUMBER1",
        max_type=np.int16,
        expected_array_of_column_values=concat_all_arrays(arr_b1, arr_b2),
    )

    # As final step we once again repeat staging op with same data
    # To arrive at final stage
    lib.write(symbol, df2, staged=True)
    lib.write(symbol, df3, staged=True)
    lib.finalize_staged_data(symbol=symbol, mode=StagedDataFinalizeMethod.APPEND)

    result: pd.DataFrame = lib.read(symbol).data
    dataframe_dump_to_log("RESULT DF:", result)
    assert 3 == len(lib.list_versions(symbol=symbol))

    # Some final confirmations all is ok
    verify_dataframe_column(
        df=result, row_name="NUMBER1", max_type=last_type_b, expected_array_of_column_values=arr_all_b
    )
    verify_dataframe_column(
        df=result, row_name="NUMBER2", max_type=last_type_c, expected_array_of_column_values=arr_all_c
    )


@pytest.mark.storage
def test_finalize_staged_data_long_scenario(basic_arctic_library):
    """
    The purpose of of the test is to assure all staged segments along with their data
    are correctly finalized and resulting
    """

    start_time = time.time()
    lib = basic_arctic_library
    symbol = "symbol"

    cachedDF = CachedDFGenerator(25000, [1])

    total_number_rows: TimestampNumber = TimestampNumber(0, cachedDF.TIME_UNIT)  # Synchronize index frequency

    num_rows_initially = 999
    print(f"Writing to symbol initially {num_rows_initially} rows")
    df = cachedDF.generate_dataframe_timestamp_indexed(num_rows_initially, total_number_rows, cachedDF.TIME_UNIT)
    dataframe_dump_to_log("df", df)
    empty_df = df.drop(df.index)
    dataframe_dump_to_log("empty", empty_df)
    total_number_rows = total_number_rows + num_rows_initially
    lib.write(symbol, data=df, prune_previous_versions=True)
    cachedParts = CacheParts(df)
    cachedParts.cache_samples_from(df)
    chunk_list = generate_chunk_sizes(200, 900, 1100)
    for chunk_size in chunk_list:
        df = cachedDF.generate_dataframe_timestamp_indexed(chunk_size, total_number_rows, cachedDF.TIME_UNIT)
        lib.write(symbol, data=df, validate_index=True, staged=True)
        ## Unfortunately there is a bug with empty dataframe with index
        ## Uncomment when this is solved
        # lib.write(symbol, data=df.drop(df.index), validate_index=True, staged=True)
        total_number_rows = total_number_rows + chunk_size
        cachedParts.cache_samples_from(df)
    lib.finalize_staged_data(symbol=symbol, mode=StagedDataFinalizeMethod.APPEND)
    cachedParts.verify_finalized_data(lib, symbol)


@pytest.mark.parametrize("mode", [StagedDataFinalizeMethod.WRITE, "write", None])
def test_finalize_staged_data_mode_write(basic_arctic_library, mode):
    lib = basic_arctic_library
    symbol = "symbol"
    df_initial = sample_dataframe("2020-1-1", [1, 2, 3], [4, 5, 6])
    df_staged = sample_dataframe("2020-1-4", [7, 8, 9])
    lib.write(symbol, df_initial)
    lib.write(symbol, df_staged, staged=True)
    assert_frame_equal(lib.read(symbol).data, df_initial)

    lib.finalize_staged_data(symbol="symbol", mode=mode)
    assert_frame_equal(lib.read(symbol).data, df_staged)


@pytest.mark.parametrize("mode", [StagedDataFinalizeMethod.APPEND, "append"])
def test_finalize_staged_data_mode_append(basic_arctic_library, mode):
    lib = basic_arctic_library
    symbol = "symbol"
    df_initial = sample_dataframe("2020-1-1", [1, 2, 3], [4, 5, 6])
    df_staged = sample_dataframe("2020-1-4", [7, 8, 9], [10, 11, 12])
    lib.write(symbol, df_initial)
    lib.write(symbol, df_staged, staged=True)
    assert_frame_equal(lib.read(symbol).data, df_initial)

    lib.finalize_staged_data(symbol="symbol", mode=mode)
    expected = pd.concat([df_initial, df_staged])
    assert_frame_equal(lib.read(symbol).data, expected)
