"""
Copyright 2023 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""
import numpy as np
import pytest

from arcticdb.version_store.processing import QueryBuilder
from arcticdb_ext.exceptions import InternalException


def generic_row_range_test(version_store, symbol, df, start_row, end_row):
    version_store.write(symbol, df)

    expected_array = df.iloc[start_row:end_row]
    received_array = version_store.read(symbol, row_range=(start_row, end_row)).data
    q = QueryBuilder()._row_range((start_row, end_row))
    received_array_via_querybuilder = version_store.read(symbol, query_builder=q).data

    np.testing.assert_array_equal(expected_array, received_array)
    np.testing.assert_array_equal(expected_array, received_array_via_querybuilder)

    expected_array = df.iloc[-end_row:-start_row]
    received_array = version_store.read(symbol, row_range=(-end_row, -start_row)).data
    q = QueryBuilder()._row_range((-end_row, -start_row))
    received_array_via_querybuilder = version_store.read(symbol, query_builder=q).data

    np.testing.assert_array_equal(expected_array, received_array)
    np.testing.assert_array_equal(expected_array, received_array_via_querybuilder)


def test_row_range_start_row_greater_than_end_row(lmdb_version_store, one_col_df):
    generic_row_range_test(lmdb_version_store, "test_row_range_start_row_greater_than_end_row", one_col_df(), 3, 2)


def test_row_range_zero_num_rows(lmdb_version_store, one_col_df):
    generic_row_range_test(lmdb_version_store, "test_row_range_zero_num_rows", one_col_df(), 2, 2)


def test_row_range_one_num_rows(lmdb_version_store_tiny_segment, one_col_df):
    generic_row_range_test(lmdb_version_store_tiny_segment, "test_row_range_one_num_rows", one_col_df(), 2, 3)


def test_row_range_segment_boundary_num_rows(lmdb_version_store_tiny_segment, one_col_df):
    # lmdb_version_store_tiny_segment has segment_row_size set to 2
    generic_row_range_test(
        lmdb_version_store_tiny_segment, "test_row_range_segment_boundary_num_rows", one_col_df(), 2, 4
    )


def test_row_range_multiple_segments(lmdb_version_store_tiny_segment, one_col_df):
    # lmdb_version_store_tiny_segment has segment_row_size set to 2
    generic_row_range_test(lmdb_version_store_tiny_segment, "test_row_range_multiple_segments", one_col_df(), 3, 7)


def test_row_range_all_rows(lmdb_version_store_tiny_segment, one_col_df):
    # one_col_df generates a dataframe with 10 rows
    generic_row_range_test(lmdb_version_store_tiny_segment, "test_row_range_all_rows", one_col_df(), 0, 10)


def test_row_range_past_end(lmdb_version_store_tiny_segment, one_col_df):
    # one_col_df generates a dataframe with 10 rows
    generic_row_range_test(lmdb_version_store_tiny_segment, "test_row_range_past_end", one_col_df(), 5, 15)


def test_row_range_with_column_filter(lmdb_version_store_tiny_segment, three_col_df):
    symbol = "test_row_range_with_column_filter"
    lmdb_version_store_tiny_segment.write(symbol, three_col_df())
    # lmdb_version_store_tiny_segment has column_group_size set to 2
    start_row = 5
    end_row = 8
    # three_col_df generates a dataframe with 10 rows and 3 columns labelled x, y, and z
    columns = ["x", "z"]
    assert np.array_equal(
        three_col_df().filter(items=columns).iloc[start_row:end_row],
        lmdb_version_store_tiny_segment.read(symbol, row_range=(start_row, end_row), columns=columns).data,
    )


def test_row_range_pickled_symbol(lmdb_version_store):
    symbol = "test_row_range_pickled_symbol"
    lmdb_version_store.write(symbol, np.arange(100).tolist())
    assert lmdb_version_store.is_symbol_pickled(symbol)
    with pytest.raises(InternalException):
        _ = lmdb_version_store.read(symbol, row_range=(1, 2))
