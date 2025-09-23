"""
Copyright 2023 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

import time

from pandas import Timestamp
from unittest.mock import MagicMock
import pytest

from arcticdb.exceptions import NoSuchVersionException, NoDataFoundException
from arcticdb.util.test import distinct_timestamps
from arcticdb.version_store.library import StagedDataFinalizeMethod, ArcticInvalidApiUsageException


def test_read_descriptor(lmdb_version_store, one_col_df):
    symbol = "test_read_descriptor"
    lmdb_version_store.write(symbol, one_col_df())
    column_names = lmdb_version_store.column_names(symbol)
    expected = ["x"]
    assert column_names == expected


def test_column_names_by_version(lmdb_version_store, one_col_df, two_col_df):
    symbol = "test_column_names_by_version"

    # Write a DF with a single column
    one_col_version = lmdb_version_store.write(symbol, one_col_df()).version

    # Write a DF with two columns
    lmdb_version_store.write(symbol, two_col_df())

    # Assert querying with the version of the first write only returns a single column
    assert lmdb_version_store.column_names(symbol, as_of=one_col_version) == ["x"]


def test_column_names_by_snapshot(lmdb_version_store, one_col_df, two_col_df):
    symbol = "test_column_names_by_snapshot"

    # Write a DF with a single column and snapshot
    lmdb_version_store.write(symbol, one_col_df())
    lmdb_version_store.snapshot("one_col_snap")

    # Write a DF with two columns
    lmdb_version_store.write(symbol, two_col_df())
    lmdb_version_store.snapshot("two_col_snap")

    # Assert querying with the snapshot after the first write only returns a single column
    assert lmdb_version_store.column_names(symbol, as_of="one_col_snap") == ["x"]


@pytest.mark.xfail(reason="Needs to be fixed by issue #496")
def test_column_names_by_timestamp(lmdb_version_store, one_col_df, two_col_df):
    symbol = "test_column_names_by_timestamp"

    # Write a DF with a single column
    with distinct_timestamps(lmdb_version_store) as first_write_timestamp:
        lmdb_version_store.write(symbol, one_col_df())

    # Ensure the timestamps differ
    time.sleep(0.1)

    with distinct_timestamps(lmdb_version_store) as second_write_timestamp:
        lmdb_version_store.write(symbol, two_col_df())

    # Assert querying with a time before the first write raises an exception
    with pytest.raises(NoDataFoundException) as excinfo:
        lmdb_version_store.column_names(symbol, as_of=Timestamp("1970-01-01", tz="UTC"))
    assert issubclass(excinfo.type, NoSuchVersionException)

    # Assert query with the timestamp after the one col write returns only a single column
    assert lmdb_version_store.column_names(symbol, as_of=first_write_timestamp.after) == ["x"]

    # Assert query with the timestamp after the two col write returns two columns
    assert lmdb_version_store.column_names(symbol, as_of=second_write_timestamp.after) == ["x", "y"]


def test_get_num_rows(lmdb_version_store, two_col_df):
    symbol = "test_get_num_rows"
    df = two_col_df()
    lmdb_version_store.write(symbol, df)
    rows = lmdb_version_store.get_num_rows(symbol)

    assert rows == df.shape[0]


def test_get_num_rows_pickled(lmdb_version_store):
    symbol = "test_get_num_rows_pickled"
    lmdb_version_store.write(symbol, 1)
    assert lmdb_version_store.get_num_rows(symbol) is None


@pytest.mark.parametrize(
    "input_mode, expected_append",
    [
        ("write", False),
        ("append", True),
        (None, False),
        (StagedDataFinalizeMethod.APPEND, True),
        (StagedDataFinalizeMethod.WRITE, False),
    ],
)
def test_finalize_staged_data(arctic_library_lmdb, input_mode, expected_append):
    symbol = "sym"
    arctic_library_lmdb._nvs = MagicMock()

    arctic_library_lmdb.finalize_staged_data(symbol, input_mode)

    default_args = {
        "convert_int_to_float": False,
        "metadata": None,
        "prune_previous_version": False,
        "validate_index": True,
        "delete_staged_data_on_failure": False,
        "stage_results": None,
    }

    arctic_library_lmdb._nvs.compact_incomplete.assert_called_once_with(symbol, append=expected_append, **default_args)


@pytest.mark.parametrize("input_mode", ["something", 3])
def test_finalize_staged_data_incorrect_args(arctic_library_lmdb, input_mode):
    symbol = "sym"
    arctic_library_lmdb._nvs = MagicMock()
    with pytest.raises(ArcticInvalidApiUsageException):
        arctic_library_lmdb.finalize_staged_data(symbol, input_mode)
