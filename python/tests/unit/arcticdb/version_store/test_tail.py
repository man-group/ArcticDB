"""
Copyright 2023 Man Group Operations Ltd.
NO WARRANTY, EXPRESSED OR IMPLIED.
"""
from inspect import signature

import numpy as np
from pandas import DataFrame
import pytest

from arcticdb_ext.exceptions import ArcticNativeCxxException
from arcticdb_ext.version_store import TailRange as _TailRange


def generic_tail_test(version_store, symbol, df, num_rows):
    version_store.write(symbol, df)
    expected = df.tail(num_rows)
    actual = version_store.tail(symbol, num_rows).data
    assert np.array_equal(expected, actual)


def test_tail_zero_num_rows(lmdb_version_store, one_col_df):
    generic_tail_test(lmdb_version_store, "test_tail_zero_num_rows", one_col_df(), 0)


def test_tail_one_num_rows(lmdb_version_store_tiny_segment, one_col_df):
    generic_tail_test(lmdb_version_store_tiny_segment, "test_tail_one_num_rows", one_col_df(), 1)


def test_tail_segment_boundary_num_rows(lmdb_version_store_tiny_segment, one_col_df):
    # lmdb_version_store_tiny_segment has segment_row_size set to 2
    generic_tail_test(lmdb_version_store_tiny_segment, "test_tail_segment_boundary_num_rows", one_col_df(), 2)


def test_tail_multiple_segments(lmdb_version_store_tiny_segment, one_col_df):
    # lmdb_version_store_tiny_segment has segment_row_size set to 2
    generic_tail_test(lmdb_version_store_tiny_segment, "test_tail_multiple_segments", one_col_df(), 7)


def test_tail_negative_num_rows(lmdb_version_store_tiny_segment, one_col_df):
    generic_tail_test(lmdb_version_store_tiny_segment, "test_tail_negative_num_rows", one_col_df(), -7)


def test_tail_num_rows_equals_table_length(lmdb_version_store_tiny_segment, one_col_df):
    # one_col_df generates a dataframe with 10 rows
    generic_tail_test(lmdb_version_store_tiny_segment, "test_tail_num_rows_greater_than_table_length", one_col_df(), 10)


def test_tail_negative_num_rows_equals_table_length(lmdb_version_store_tiny_segment, one_col_df):
    # one_col_df generates a dataframe with 10 rows
    generic_tail_test(
        lmdb_version_store_tiny_segment, "test_tail_negative_num_rows_equals_table_length", one_col_df(), -10
    )


def test_tail_num_rows_greater_than_table_length(lmdb_version_store_tiny_segment, one_col_df):
    # one_col_df generates a dataframe with 10 rows
    generic_tail_test(lmdb_version_store_tiny_segment, "test_tail_num_rows_greater_than_table_length", one_col_df(), 11)


def test_tail_negative_num_rows_greater_than_table_length(lmdb_version_store_tiny_segment, one_col_df):
    # one_col_df generates a dataframe with 10 rows
    generic_tail_test(
        lmdb_version_store_tiny_segment, "test_tail_negative_num_rows_greater_than_table_length", one_col_df(), -11
    )


def test_tail_default_num_rows(lmdb_version_store_tiny_segment, one_col_df):
    symbol = "test_tail_default_num_rows"
    lmdb_version_store_tiny_segment.write(symbol, one_col_df())
    num_rows = signature(lmdb_version_store_tiny_segment.tail).parameters["n"].default
    assert np.array_equal(one_col_df().tail(num_rows), lmdb_version_store_tiny_segment.tail(symbol).data)


def test_tail_with_column_filter(lmdb_version_store_tiny_segment, three_col_df):
    symbol = "test_tail_with_column_filter"
    lmdb_version_store_tiny_segment.write(symbol, three_col_df())
    # lmdb_version_store_tiny_segment has column_group_size set to 2
    num_rows = 5
    # three_col_df generates a dataframe with 10 rows and 3 columns labelled x, y, and z
    columns = ["x", "z"]
    assert np.array_equal(
        three_col_df().filter(items=columns).tail(num_rows),
        lmdb_version_store_tiny_segment.tail(symbol, num_rows, columns=columns).data,
    )


# Tests that the frame data returned to the python layer contains at most num_rows + lib segment_row_size
def test_tail_frame_data_size(lmdb_version_store_small_segment):
    symbol = "test_tail_frame_data_size"
    segment_row_size = lmdb_version_store_small_segment._cfg.write_options.segment_row_size
    if segment_row_size < 2:
        raise Exception("test_tail_frame_data_size needs segment_row_size to be at least 2")
    rows_in_table = (2 * segment_row_size) + 1
    rows_in_tail_request = segment_row_size + 1
    lmdb_version_store_small_segment.write(symbol, DataFrame({"x": np.arange(rows_in_table, dtype=np.int64)}))
    version_query, read_options, read_query = lmdb_version_store_small_segment._get_queries(
        None, None, _TailRange(rows_in_tail_request), None, None
    )
    read_result = lmdb_version_store_small_segment._read_dataframe(symbol, version_query, read_query, read_options)
    rows_in_raw_dataframe = read_result.frame_data.value.data[0].size
    assert rows_in_raw_dataframe <= (1 + rows_in_tail_request // segment_row_size) * segment_row_size


def test_tail_multiple_segments_odd_total_rows(lmdb_version_store_tiny_segment):
    generic_tail_test(
        lmdb_version_store_tiny_segment,
        "test_tail_multiple_segments_odd_total_rows",
        DataFrame({"x": np.arange(11, dtype=np.int64)}),
        7,
    )


def test_tail_pickled_symbol(lmdb_version_store):
    symbol = "test_tail_pickled_symbol"
    lmdb_version_store.write(symbol, np.arange(100).tolist())
    assert lmdb_version_store.is_symbol_pickled(symbol)
    with pytest.raises(ArcticNativeCxxException):
        _ = lmdb_version_store.tail(symbol)
