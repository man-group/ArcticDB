"""
Copyright 2024 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

import numpy as np
import pandas as pd
import pytest

from arcticdb.version_store.processing import QueryBuilder
from arcticdb_ext.exceptions import InternalException

from arcticdb.util.test import assert_frame_equal

pytestmark = pytest.mark.pipeline


def generic_row_range_test(version_store, symbol, df, start_row, end_row):
    version_store.write(symbol, df)

    expected_array = df.iloc[start_row:end_row]
    received_array = version_store.read(symbol, row_range=(start_row, end_row)).data
    q = QueryBuilder().row_range((start_row, end_row))
    received_array_via_querybuilder = version_store.read(symbol, query_builder=q).data

    np.testing.assert_array_equal(expected_array, received_array)
    np.testing.assert_array_equal(expected_array, received_array_via_querybuilder)

    expected_array = df.iloc[-end_row:-start_row]
    received_array = version_store.read(symbol, row_range=(-end_row, -start_row)).data
    q = QueryBuilder().row_range((-end_row, -start_row))
    received_array_via_querybuilder = version_store.read(symbol, query_builder=q).data

    np.testing.assert_array_equal(expected_array, received_array)
    np.testing.assert_array_equal(expected_array, received_array_via_querybuilder)


def test_row_range_start_row_greater_than_end_row(lmdb_version_store, one_col_df, any_output_format):
    lmdb_version_store._set_output_format_for_pipeline_tests(any_output_format)
    generic_row_range_test(lmdb_version_store, "test_row_range_start_row_greater_than_end_row", one_col_df(), 3, 2)


def test_row_range_zero_num_rows(lmdb_version_store, one_col_df, any_output_format):
    lmdb_version_store._set_output_format_for_pipeline_tests(any_output_format)
    generic_row_range_test(lmdb_version_store, "test_row_range_zero_num_rows", one_col_df(), 2, 2)


def test_row_range_one_num_rows(lmdb_version_store_tiny_segment, one_col_df, any_output_format):
    lmdb_version_store_tiny_segment._set_output_format_for_pipeline_tests(any_output_format)
    generic_row_range_test(lmdb_version_store_tiny_segment, "test_row_range_one_num_rows", one_col_df(), 2, 3)


def test_row_range_segment_boundary_num_rows(lmdb_version_store_tiny_segment, one_col_df, any_output_format):
    lmdb_version_store_tiny_segment._set_output_format_for_pipeline_tests(any_output_format)
    # lmdb_version_store_tiny_segment has segment_row_size set to 2
    generic_row_range_test(
        lmdb_version_store_tiny_segment, "test_row_range_segment_boundary_num_rows", one_col_df(), 2, 4
    )


def test_row_range_multiple_segments(lmdb_version_store_tiny_segment, one_col_df, any_output_format):
    lmdb_version_store_tiny_segment._set_output_format_for_pipeline_tests(any_output_format)
    # lmdb_version_store_tiny_segment has segment_row_size set to 2
    generic_row_range_test(lmdb_version_store_tiny_segment, "test_row_range_multiple_segments", one_col_df(), 3, 7)


def test_row_range_all_rows(lmdb_version_store_tiny_segment, one_col_df, any_output_format):
    lmdb_version_store_tiny_segment._set_output_format_for_pipeline_tests(any_output_format)
    # one_col_df generates a dataframe with 10 rows
    generic_row_range_test(lmdb_version_store_tiny_segment, "test_row_range_all_rows", one_col_df(), 0, 10)


def test_row_range_past_end(lmdb_version_store_tiny_segment, one_col_df, any_output_format):
    lmdb_version_store_tiny_segment._set_output_format_for_pipeline_tests(any_output_format)
    # one_col_df generates a dataframe with 10 rows
    generic_row_range_test(lmdb_version_store_tiny_segment, "test_row_range_past_end", one_col_df(), 5, 15)


def test_row_range_with_column_filter(lmdb_version_store_tiny_segment, three_col_df, any_output_format):
    lmdb_version_store_tiny_segment._set_output_format_for_pipeline_tests(any_output_format)
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


def test_row_range_pickled_symbol(lmdb_version_store, any_output_format):
    lmdb_version_store._set_output_format_for_pipeline_tests(any_output_format)
    symbol = "test_row_range_pickled_symbol"
    lmdb_version_store.write(symbol, np.arange(100).tolist())
    assert lmdb_version_store.is_symbol_pickled(symbol)
    with pytest.raises(InternalException):
        _ = lmdb_version_store.read(symbol, row_range=(1, 2))


@pytest.mark.parametrize(
    "row_range,expected",
    (
        ((-5, None), pd.DataFrame({"a": np.arange(95, 100)})),
        ((5, None), pd.DataFrame({"a": np.arange(5, 100)})),
        ((0, None), pd.DataFrame({"a": np.arange(0, 100)})),
        ((None, -5), pd.DataFrame({"a": np.arange(95)})),
        ((None, 5), pd.DataFrame({"a": np.arange(5)})),
        ((None, 0), pd.DataFrame({"a": []}, dtype=np.int64)),
        ((None, None), pd.DataFrame({"a": np.arange(100)})),
        ((5, 3), pd.DataFrame({"a": []}, dtype=np.int64)),
    ),
)
@pytest.mark.parametrize("api", ("query_builder", "read", "read_batch"))
def test_row_range_open_ended(lmdb_version_store_v1, api, row_range, expected, any_output_format):
    lmdb_version_store_v1._set_output_format_for_pipeline_tests(any_output_format)
    symbol = "test_row_range"
    df = pd.DataFrame({"a": np.arange(100)})
    lmdb_version_store_v1.write(symbol, df)

    if api == "query_builder":
        q = QueryBuilder().row_range(row_range)
        received = lmdb_version_store_v1.read(symbol, query_builder=q).data
    elif api == "read":
        received = lmdb_version_store_v1.read(symbol, row_range=row_range).data
    else:
        assert api == "read_batch"
        received = lmdb_version_store_v1.batch_read([symbol], row_ranges=[row_range])[symbol].data

    assert_frame_equal(received, expected, check_dtype=False)
