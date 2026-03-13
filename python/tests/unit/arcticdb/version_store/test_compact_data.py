"""
Copyright 2026 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

import numpy as np
import pandas as pd
import pytest

from arcticdb_ext.exceptions import StorageException
from arcticdb.exceptions import ArcticNativeException, UserInputException
from arcticdb.util.test import assert_frame_equal


def min_max_rows_per_segment(rows_per_segment):
    # Definitions taken from CompactDataClause constructor
    min_rows_per_segment = max((2 * rows_per_segment) // 3, 1)
    max_rows_per_segment = max((4 * rows_per_segment) // 3, rows_per_segment + 1)
    return min_rows_per_segment, max_rows_per_segment


def test_compact_data_symbol_doesnt_exist(lmdb_version_store_v1):
    lib = lmdb_version_store_v1
    sym = "test_compact_data_symbol_doesnt_exist"
    with pytest.raises(StorageException) as e:
        lib.compact_data_experimental(sym)
    assert sym in str(e.value)


def test_compact_data_negative_rows_per_segment(lmdb_version_store_v1):
    lib = lmdb_version_store_v1
    sym = "test_compact_data_negative_rows_per_segment"
    with pytest.raises(ArcticNativeException):
        lib.compact_data_experimental(sym, rows_per_segment=-1)


def test_compact_data_maintain_metadata(lmdb_version_store_v1):
    lib = lmdb_version_store_v1
    sym = "test_compact_data_maintain_metadata"
    df = pd.DataFrame({"col": np.arange(10)})
    lib.write(sym, df)
    metadata = {"hello": "world"}
    lib.append(sym, df, metadata=metadata)
    assert lib.read_metadata(sym).metadata == metadata
    lib.compact_data_experimental(sym)
    assert len(lib.read_index(sym)) == 1
    assert lib.read_metadata(sym).metadata == metadata


@pytest.mark.parametrize("lib_config_value", [1, 2, 3, 5, 7, 10])
@pytest.mark.parametrize("method_arg", [1, 2, 3, 5, 7, 10])
def test_compact_data_explicit_rows_per_segment(version_store_factory, lib_config_value, method_arg):
    lib = version_store_factory(segment_row_size=lib_config_value)
    sym = "test_compact_data_explicit_rows_per_segment"
    df = pd.DataFrame({"col": np.arange(30)})
    lib.write(sym, df)
    lib.compact_data_experimental(sym, rows_per_segment=method_arg)
    assert_frame_equal(df, lib.read(sym).data)
    index = lib.read_index(sym)
    row_counts = index["end_row"] - index["start_row"]
    min_rows_per_segment, max_rows_per_segment = min_max_rows_per_segment(method_arg)
    assert row_counts.min() >= min_rows_per_segment
    assert row_counts.max() <= max_rows_per_segment


@pytest.mark.parametrize("rows_per_segment", [1, 2, 3, 5, 7, 10])
@pytest.mark.parametrize("initial_rows", [20, 21, 22, 23, 24, 25, 26, 27, 28, 29])
@pytest.mark.parametrize("append_rows", [1, 2, 3, 4, 5, 6, 7, 8, 9, 10])
def test_compact_data_append(version_store_factory, rows_per_segment, initial_rows, append_rows):
    lib = version_store_factory(segment_row_size=rows_per_segment)
    sym = "test_compact_data_append"
    df = pd.DataFrame({"col": np.arange(initial_rows + append_rows)})
    lib.write(sym, df[:initial_rows])
    lib.append(sym, df[initial_rows:])
    lib.compact_data_experimental(sym)
    assert_frame_equal(df, lib.read(sym).data)
    index = lib.read_index(sym)
    row_counts = index["end_row"] - index["start_row"]
    min_rows_per_segment, max_rows_per_segment = min_max_rows_per_segment(rows_per_segment)
    assert row_counts.min() >= min_rows_per_segment
    assert row_counts.max() <= max_rows_per_segment


@pytest.mark.parametrize("rows_per_segment", [1, 2, 3, 5, 7, 10])
@pytest.mark.parametrize("initial_rows", [20, 21, 22, 23, 24, 25, 26, 27, 28, 29])
@pytest.mark.parametrize("update_rows", [1, 2, 3, 4, 5, 6, 7, 8, 9, 10])
def test_compact_data_update(version_store_factory, rows_per_segment, initial_rows, update_rows):
    lib = version_store_factory(segment_row_size=rows_per_segment)
    sym = "test_compact_data_update"
    write_df = pd.DataFrame({"col": np.arange(initial_rows)}, index=pd.date_range("2026-01-01", periods=initial_rows))
    lib.write(sym, write_df)
    update_df = pd.DataFrame(
        {"col": np.arange(initial_rows, initial_rows + update_rows)},
        index=pd.date_range("2026-01-15", periods=update_rows),
    )
    lib.update(sym, update_df)
    lib.compact_data_experimental(sym)
    received_df = lib.read(sym).data
    assert_frame_equal(write_df[:14], received_df[:14])
    assert_frame_equal(update_df, received_df[14 : 14 + update_rows])
    assert_frame_equal(write_df[14 + update_rows :], received_df[14 + update_rows :])
    index = lib.read_index(sym)
    row_counts = index["end_row"] - index["start_row"]
    min_rows_per_segment, max_rows_per_segment = min_max_rows_per_segment(rows_per_segment)
    assert row_counts.min() >= min_rows_per_segment
    assert row_counts.max() <= max_rows_per_segment
