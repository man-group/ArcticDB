"""
Copyright 2026 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

import datetime
import pandas as pd
import numpy as np
import pytest

from arcticdb.util.arctic_simulator import ArcticSymbolSimulator
from arcticdb.util.test import assert_frame_equal
from arcticdb.util.test_utils import verify_dynamically_added_columns
from arcticdb.version_store.library import Library
from arcticdb_ext.exceptions import SchemaException, NormalizationException


def append_and_compare(asim: ArcticSymbolSimulator, df: pd.DataFrame, sym_name: str, lib: Library):
    asim.append(df)
    lib.append(sym_name, df)
    asim.assert_equal_to(lib.read(sym_name).data)


def write_and_compare(asim: ArcticSymbolSimulator, df: pd.DataFrame, sym_name: str, lib: Library):
    asim.write(df)
    lib.write(sym_name, df)
    asim.assert_equal_to(lib.read(sym_name).data)


def update_and_compare(asim: ArcticSymbolSimulator, df: pd.DataFrame, sym_name: str, lib: Library):
    asim.update(df)
    lib.update(sym_name, df)
    asim.assert_equal_to(lib.read(sym_name).data)


def test_simulator_append_basic_test_range_index():

    df1 = pd.DataFrame({"A": [1, 2], "B": [3, 4], "C": [5, 6]})

    df2 = pd.DataFrame({"B": [7, 8], "C": [9, 0], "D": [11, 12]})

    df_expected = pd.DataFrame(
        {
            "A": [1, 2, 0, 0],
            "B": [3, 4, 7, 8],
            "C": [5, 6, 9, 0],
            "D": [0, 0, 11, 12],
        }
    )

    asim = ArcticSymbolSimulator(keep_versions=True)
    asim.write(df1)
    asim.append(df2)
    df_result = asim.read()

    assert_frame_equal(df_expected, df_result)
    assert_frame_equal(df1, asim.read(as_of=0))


def test_simulator_append_basic_test_timestamp_index():
    # Create timestamp index starting from now
    start_time = datetime.datetime.now()
    df1_index = pd.date_range(start=start_time, periods=2, freq="D")
    df2_index = pd.date_range(start=start_time + datetime.timedelta(days=2), periods=2, freq="D")

    df1 = pd.DataFrame({"A": [1, 2], "B": [3, 4], "C": [5, 6]}, index=df1_index)

    df2 = pd.DataFrame({"B": [7, 8], "C": [9, 0], "D": [11, 12]}, index=df2_index)

    all_index = df1_index.append(df2_index)
    df_expected = pd.DataFrame(
        {"A": [1, 2, 0, 0], "B": [3, 4, 7, 8], "C": [5, 6, 9, 0], "D": [0, 0, 11, 12]}, index=all_index
    )

    asim = ArcticSymbolSimulator()
    df_result = asim.simulate_arctic_append(df1, df2)
    assert_frame_equal(df_expected, df_result)


def test_simulator_update_all_types_check_simulator_versions_store():
    index_dates = pd.date_range(start=datetime.datetime(2025, 8, 1), periods=5, freq="D")

    df = pd.DataFrame(
        {
            "int_col": [10, 20, 30, 40, 50],
            "float_col": [1.5, 2.5, 3.5, 4.5, 5.5],
            "bool_col": [True, False, True, False, True],
            "str_col": ["a", "b", "c", "d", "e"],
            "timestamp_col": index_dates + pd.to_timedelta(2, unit="h"),
        },
        index=index_dates,
    )

    index_dates = pd.date_range(start=datetime.datetime(2025, 7, 18), periods=1, freq="D")
    df1 = pd.DataFrame(
        {
            "int_col": [111],
            "float_col": [111.0],
            "bool_col": [False],
            "str_col": ["Z"],
            "timestamp_col": index_dates + pd.to_timedelta(2, unit="h"),
        },
        index=index_dates,
    )

    index_dates = pd.date_range(start=datetime.datetime(2025, 6, 18), periods=1, freq="D")
    df2 = pd.DataFrame(
        {
            "int_col": [111],
            "float_col": [111.0],
            "bool_col": [False],
            "str_col": ["Z"],
            "timestamp_col": index_dates + pd.to_timedelta(2, unit="h"),
            "int_col1": [111],
            "float_col1": [111.0],
            "bool_col1": [False],
            "str_col1": ["Z"],
            "timestamp_col1": index_dates + pd.to_timedelta(2, unit="h"),
        },
        index=index_dates,
    )

    # Now df2 is one row and earliest timestamp
    # Now df1 is one row and second earliest timestamp
    # Now df is several rows and latest timestamp
    # so from timeline perspective it is this timeline df2, df1, df

    asim = ArcticSymbolSimulator(keep_versions=True, dynamic_schema=True)
    asim.write(df)
    asim.update(df1)

    assert df.shape[0] + df1.shape[0] == asim.read().shape[0]  # Result dataframe is combination of both
    assert_frame_equal(df1, asim.read().iloc[[0]])  # First row is updated
    assert_frame_equal(df, asim.read(as_of=1).iloc[1:])  # df starts from 2nd row
    assert_frame_equal(df, asim.read(as_of=0))  # First version is df

    asim.update(df2)
    assert df.shape[0] + df1.shape[0] + df1.shape[0] == asim.read().shape[0]
    assert_frame_equal(df2, asim.read().iloc[[0]])  # First row is updated
    # Verify new columns added to first line from previous version are correct
    new_cols = set(df2.columns) - set(df1.columns)
    verify_dynamically_added_columns(asim.read(), df1.index[0], new_cols)
    # Verify new columns added to second line from previous version are correct
    new_cols = set(df2.columns) - set(df.columns)
    verify_dynamically_added_columns(asim.read(), df.index[1], new_cols)

    asim = ArcticSymbolSimulator(keep_versions=True, dynamic_schema=False)
    asim.write(df)
    asim.update(df1)
    # Update with static schema will not pass
    with pytest.raises(AssertionError):
        asim.update(df2)


def test_simulator_append_series():
    s1 = pd.Series([10, 20, 30], name="name", index=pd.to_datetime(["2023-01-01", "2023-01-02", "2023-01-03"]))
    s2 = pd.Series([40, 50], name="name", index=pd.to_datetime(["2023-01-04", "2023-01-05"]))
    asim = ArcticSymbolSimulator()
    asim.write(s1)
    asim.append(s2)
    assert 5 == len(asim.read())
    assert 10 == asim.read().iloc[0]
    assert 50 == asim.read().iloc[-1]

    s1 = pd.Series([10, 20, 30], name="name")
    s2 = pd.Series([40, 50], name="name")
    asim.write(s1)
    asim.append(s2)
    assert 5 == len(asim.read())
    assert 10 == asim.read().iloc[0]
    assert 50 == asim.read().iloc[-1]


def test_simulator_append_series_and_dataframe(lmdb_library_dynamic_schema):
    lib: Library = lmdb_library_dynamic_schema
    asim = ArcticSymbolSimulator()
    s1 = pd.Series([10, 20, 30], name="name")
    df1 = pd.DataFrame({"A": [1, 2], "B": [3, 4], "C": [5, 6]})

    # A scenario where we append DataFrame to symbol containing Series
    write_and_compare(asim, s1, "s_err", lib)
    asim.append(df1)
    with pytest.raises(NormalizationException):
        lib.append("s_err", df1)

    # A scenario where we append DataFrame to symbol containing Series first converted to dataframe
    s1 = pd.DataFrame(s1)
    write_and_compare(asim, s1, "s", lib)
    append_and_compare(asim, s1, "s", lib)


def test_simulator_append_series_and_dataframe_with_timestamp(lmdb_library_dynamic_schema):
    """We cannot have Series + Dataframes mix, so we convert to one of them"""
    lib: Library = lmdb_library_dynamic_schema
    asim0 = ArcticSymbolSimulator()
    index_dates = pd.date_range(start=datetime.datetime(2025, 6, 18), periods=3, freq="D")
    s1 = pd.DataFrame(pd.Series([10, 20, 30], name="name", index=index_dates))
    index_dates = pd.date_range(start=datetime.datetime(2025, 7, 18), periods=1, freq="D")
    df1 = pd.DataFrame(
        {
            "int_col": [111],
            "float_col": [111.0],
            "bool_col": [False],
            "str_col": ["Z"],
            "timestamp_col": index_dates + pd.to_timedelta(2, unit="h"),
        },
        index=index_dates,
    )

    write_and_compare(asim0, s1, "s", lib)
    append_and_compare(asim0, df1, "s", lib)


def test_simulator_append_series_and_dataframe_mix(lmdb_library_dynamic_schema):
    """This currently is supported only if start Series and any Series appended
    is converted to dataframe
    """

    def append_and_compare(asim: ArcticSymbolSimulator, df: pd.DataFrame, sym_name: str, lib: Library):
        asim.append(df)
        lib.append(sym_name, df)
        asim.assert_equal_to(lib.read(sym_name).data)

    def write_and_compare(asim: ArcticSymbolSimulator, df: pd.DataFrame, sym_name: str, lib: Library):
        asim.write(df)
        lib.write(sym_name, df)
        asim.assert_equal_to(lib.read(sym_name).data)

    lib = lmdb_library_dynamic_schema
    asim = ArcticSymbolSimulator()
    s1 = pd.DataFrame(pd.Series([10, 20, 30], name="name"))
    s2 = pd.DataFrame(pd.Series([100, 200, 300], name="ioop"))
    s3 = pd.DataFrame(pd.Series([1000, 2000, 3000], name="name"))
    df1 = pd.DataFrame({"A": [1, 2], "B": [3, 4], "C": [5, 6]})
    index_dates = pd.date_range(start=datetime.datetime(2025, 6, 18), periods=1, freq="D")
    df2 = pd.DataFrame(
        {
            "int_col": [111],
            "float_col": [111.0],
            "bool_col": [False],
            "str_col": ["Z"],
            "timestamp_col": index_dates + pd.to_timedelta(2, unit="h"),
        }
    )
    write_and_compare(asim, s1, "s", lib)
    append_and_compare(asim, df1, "s", lib)
    append_and_compare(asim, df1, "s", lib)
    append_and_compare(asim, df1, "s", lib)
    append_and_compare(asim, s2, "s", lib)
    append_and_compare(asim, s2, "s", lib)
    append_and_compare(asim, s3, "s", lib)
    append_and_compare(asim, df2, "s", lib)


def test_simulator_update_all_columns_promote_in_type(lmdb_library_dynamic_schema):
    lib = lmdb_library_dynamic_schema
    asym = ArcticSymbolSimulator()
    index_dates = pd.date_range(start=datetime.datetime(2025, 8, 1), periods=3, freq="D")
    df1 = pd.DataFrame(
        {
            "int_col": np.array([10, 20, 30], dtype=np.int16),
            "uint_col": np.array([10, 20, 30], dtype=np.uint16),
            "uint_col_to_int": np.array([10, 20, 30], dtype=np.uint16),
            "int_col_to_float": np.array([10, 20, 30], dtype=np.float64),
            "uint_col_to_float": np.array([10, 20, 30], dtype=np.uint16),
            "float_col": np.array([1.5, 2.5, 3.5], dtype=np.float32),
            "bool_col": [True, False, True],
            "str_col": ["a", "b", "c"],
            "timestamp_col": index_dates + pd.to_timedelta(2, unit="h"),
        },
        index=index_dates,
    )
    index_dates = pd.date_range(start=datetime.datetime(2025, 8, 3), periods=1, freq="D")
    df2 = pd.DataFrame(
        {
            "int_col": np.array([-100], dtype=np.int64),
            "int_col1": np.array([-100], dtype=np.int64),
            "uint_col": np.array([200], dtype=np.uint32),
            "uint_col2": np.array([200], dtype=np.uint32),
            "uint_col_to_int": np.array([-1243], dtype=np.int32),
            "uint_col_to_float": np.array([11.11], dtype=np.float32),
            "int_col_uint": np.array([100], dtype=np.uint64),
            "float_col": np.array([15.55], dtype=np.float64),
            "int_col_to_float": np.array([1234.567], dtype=np.float64),
            "bool_col": [False],
            "str_col": ["a"],
            "timestamp_col": index_dates + pd.to_timedelta(2, unit="h"),
        },
        index=index_dates,
    )
    write_and_compare(asym, df1, "s", lib)
    update_and_compare(asym, df1, "s", lib)
    assert 3 == len(asym.read())


def test_simulator_append_serries_converted_to_dataframes_supported_combos(lmdb_library_dynamic_schema):

    def test_append_serries_converted_to_dataframes(s1, s2):
        lib: Library = lmdb_library_dynamic_schema
        asim = ArcticSymbolSimulator()
        df1 = pd.DataFrame(s1)
        df2 = pd.DataFrame(s2)
        write_and_compare(asim, df1, "s", lib)
        append_and_compare(asim, df2, "s", lib)
        return asim.read()

    s1 = pd.Series([10, 20, 30], name="name")
    s2 = pd.Series([100, 200, 300], name="name")
    test_append_serries_converted_to_dataframes(s1, s2)

    s1 = pd.Series([10, 20, 30])
    s2 = pd.Series([100, 200, 300])
    test_append_serries_converted_to_dataframes(s1, s2)

    # Series and Dataframe - results in Dataframe
    s1 = pd.Series([10, 20, 30], name="name")
    s2 = pd.DataFrame(pd.Series([100, 200, 300], name="name"))
    df: pd.DataFrame = test_append_serries_converted_to_dataframes(s1, s2)
    assert 1 == df.shape[1]

    # Series and Dataframe - results in Dataframe
    # when column names different - we have 2 cols
    s1 = pd.Series([10, 20, 30], name="name")
    s2 = pd.DataFrame(pd.Series([100, 200, 300], name="name2"))
    df: pd.DataFrame = test_append_serries_converted_to_dataframes(s1, s2)
    assert 2 == df.shape[1]


def test_simulator_append_serries_supported_errors(lmdb_library_dynamic_schema):

    def test_append_serries_with_error(s1, s2):
        lib = lmdb_library_dynamic_schema
        asim = ArcticSymbolSimulator()
        asim.write(s1)
        lib.write("s", s1)
        asim.append(s2)
        with pytest.raises(SchemaException):
            lib.append("s", s2)

    s1 = pd.Series([10, 20, 30], name="name")
    s2 = pd.Series([100, 200, 300], name="name2")
    test_append_serries_with_error(s1, s2)
