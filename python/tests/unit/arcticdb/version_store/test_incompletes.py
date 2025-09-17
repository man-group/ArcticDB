"""
Copyright 2024 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

import numpy as np
import pandas as pd
import pytest
from arcticdb.util.test import assert_frame_equal
from arcticdb.exceptions import MissingDataException
from arcticdb_ext.storage import KeyType


@pytest.mark.parametrize("batch", (True, False))
def test_read_incompletes_with_indexed_data(lmdb_version_store_v1, batch):
    lib = lmdb_version_store_v1
    lib_tool = lib.library_tool()
    sym = "test_read_incompletes_with_indexed_data"
    num_rows = 10
    df = pd.DataFrame({"col": np.arange(num_rows)}, pd.date_range("2024-01-01", periods=num_rows))
    lib.write(sym, df.iloc[: num_rows // 2])
    for idx in range(num_rows // 2, num_rows):
        lib_tool.append_incomplete(sym, df.iloc[idx : idx + 1])
    assert lib.has_symbol(sym)
    if batch:
        received_vit = lib.batch_read([sym], date_ranges=[(df.index[1], df.index[-2])], incomplete=True)[sym]
    else:
        received_vit = lib.read(sym, date_range=(df.index[1], df.index[-2]), incomplete=True)
    assert received_vit.symbol == sym
    assert_frame_equal(df.iloc[1:-1], received_vit.data)


@pytest.mark.parametrize("batch", (True, False))
def test_read_incompletes_no_indexed_data(lmdb_version_store_v1, batch):
    lib = lmdb_version_store_v1
    lib_tool = lib.library_tool()
    sym = "test_read_incompletes_no_indexed_data"
    num_rows = 10
    df = pd.DataFrame({"col": np.arange(num_rows)}, pd.date_range("2024-01-01", periods=num_rows))
    for idx in range(num_rows):
        lib_tool.append_incomplete(sym, df.iloc[idx : idx + 1])
    assert not lib.has_symbol(sym)
    if batch:
        received_vit = lib.batch_read([sym], date_ranges=[(df.index[1], df.index[-2])], incomplete=True)[sym]
    else:
        received_vit = lib.read(sym, date_range=(df.index[1], df.index[-2]), incomplete=True)
    assert received_vit.symbol == sym
    assert_frame_equal(df.iloc[1:-1], received_vit.data)


@pytest.mark.parametrize("batch", (True, False))
def test_read_incompletes_non_existent_symbol(lmdb_version_store_v1, batch):
    lib = lmdb_version_store_v1
    sym = "test_read_incompletes_non_existent_symbol"
    # Incomplete reads require a date range
    date_range = (pd.Timestamp(0), pd.Timestamp(1))
    with pytest.raises(MissingDataException):
        if batch:
            lib.batch_read([sym], date_ranges=[date_range], incomplete=True)
        else:
            lib.read(sym, date_range=date_range, incomplete=True)


def test_read_incompletes_no_chunking(lmdb_version_store_tiny_segment):
    lib = lmdb_version_store_tiny_segment
    lib_tool = lib.library_tool()
    sym = "sym"
    df = pd.DataFrame({"col": np.arange(10)}, pd.date_range("2024-01-01", periods=10))
    lib_tool.append_incomplete(sym, df.iloc[:8])
    lib_tool.append_incomplete(sym, df.iloc[8:])
    received_vit = lib.read(sym, date_range=(None, None), incomplete=True)

    assert_frame_equal(df, received_vit.data)

    data_keys = lib_tool.find_keys_for_symbol(KeyType.APPEND_DATA, sym)
    # We don't apply row slicing within append_incomplete, callers must handle their own slicing
    assert len(data_keys) == 2

    ref_keys = lib_tool.find_keys_for_symbol(KeyType.APPEND_REF, sym)
    assert len(ref_keys) == 1


@pytest.mark.parametrize("dynamic_schema", [True, False])
def test_read_incompletes_columns_filter(version_store_factory, dynamic_schema):
    lib = version_store_factory(dynamic_schema=dynamic_schema)
    lib_tool = lib.library_tool()
    sym = "sym"
    df = pd.DataFrame(
        {
            "col": np.arange(20),
            "float_col": np.arange(20, dtype=np.float64),
            "str_col": [f"str_{i}" for i in range(20)],
        },
        pd.date_range("2024-01-01", periods=20),
    )
    lib_tool.append_incomplete(sym, df.iloc[:5])
    lib_tool.append_incomplete(sym, df.iloc[5:8])
    lib_tool.append_incomplete(sym, df.iloc[8:10])

    date_range = (None, None)
    col_df = lib.read(sym, date_range=date_range, incomplete=True, columns=["col"]).data
    assert_frame_equal(col_df, df[["col"]].iloc[:10])

    float_col_df = lib.read(sym, date_range=date_range, incomplete=True, columns=["float_col"]).data
    assert_frame_equal(float_col_df, df[["float_col"]].iloc[:10])

    float_and_str_col_df = lib.read(sym, date_range=date_range, incomplete=True, columns=["float_col", "str_col"]).data
    assert_frame_equal(float_and_str_col_df, df[["float_col", "str_col"]].iloc[:10])

    date_range = (pd.Timestamp(2024, 1, 3), pd.Timestamp(2024, 1, 8))
    float_and_str_col_df = lib.read(sym, date_range=date_range, incomplete=True, columns=["float_col", "str_col"]).data
    assert_frame_equal(float_and_str_col_df, df[["float_col", "str_col"]].iloc[2:8])

    # Compact and add the rest of the df
    lib.compact_incomplete(sym, append=True, convert_int_to_float=False, via_iteration=False)
    lib_tool.append_incomplete(sym, df.iloc[10:17])
    lib_tool.append_incomplete(sym, df.iloc[17:])

    date_range = (None, None)
    float_col_df = lib.read(sym, date_range=date_range, incomplete=True, columns=["float_col"]).data
    assert_frame_equal(float_col_df, df[["float_col"]])

    float_and_str_col_df = lib.read(sym, date_range=date_range, incomplete=True, columns=["float_col", "str_col"]).data
    assert_frame_equal(float_and_str_col_df, df[["float_col", "str_col"]])

    # Only incomplete range
    date_range = (pd.Timestamp(2024, 1, 12), pd.Timestamp(2024, 1, 18))
    float_and_str_col_df = lib.read(sym, date_range=date_range, incomplete=True, columns=["float_col", "str_col"]).data
    assert_frame_equal(float_and_str_col_df, df[["float_col", "str_col"]].iloc[11:18])


def test_read_incompletes_dynamic(lmdb_version_store_dynamic_schema_v1):
    lib = lmdb_version_store_dynamic_schema_v1
    lib_tool = lib.library_tool()
    sym = "sym"

    def get_date(days_after_epoch):
        return pd.Timestamp(0) + pd.DateOffset(days=days_after_epoch)

    def get_index(days_after_epoch, num_days):
        return pd.date_range(get_date(days_after_epoch), periods=num_days, freq="d")

    df_1 = pd.DataFrame({"col_1": [1.0, 2.0, 3.0], "col_2": [1.0, 2.0, 3.0]}, index=get_index(0, 3))
    df_2 = pd.DataFrame({"col_2": [4.0, 5.0], "col_3": [1.0, 2.0]}, index=get_index(3, 2))
    df_3 = pd.DataFrame({"col_3": [3.0, 4.0], "col_4": [1.0, 2.0]}, index=get_index(5, 2))

    lib_tool.append_incomplete(sym, df_1)
    lib_tool.append_incomplete(sym, df_2)

    df = lib.read(sym, date_range=(None, None), incomplete=True).data
    assert_frame_equal(df, pd.concat([df_1, df_2]))

    # If reading just a single incomplete we will get the result in its own schema
    df = lib.read(sym, date_range=(get_date(3), None), incomplete=True).data
    assert_frame_equal(df, df_2)

    lib.compact_incomplete(sym, append=True, convert_int_to_float=False, via_iteration=False)

    df = lib.read(sym, date_range=(None, None), incomplete=True).data
    assert_frame_equal(df, pd.concat([df_1, df_2]))

    lib_tool.append_incomplete(sym, df_3)
    df = lib.read(sym, date_range=(None, None), incomplete=True).data
    assert_frame_equal(df, pd.concat([df_1, df_2, df_3]))

    df_col_filter = lib.read(sym, date_range=(None, None), incomplete=True, columns=["col_2", "col_4"]).data
    assert_frame_equal(df_col_filter, pd.concat([df_1, df_2, df_3])[["col_2", "col_4"]])
