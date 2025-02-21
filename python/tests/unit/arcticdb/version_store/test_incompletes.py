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
