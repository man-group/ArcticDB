import pandas as pd
import numpy as np
from pandas.testing import assert_frame_equal


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