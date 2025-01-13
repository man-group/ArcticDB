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


@pytest.mark.parametrize("batch", (True, False))
def test_read_incompletes_with_indexed_data(lmdb_version_store_v1, batch):
    # TODO aseaton broken due to the next_key handling used for tick collector - introduce totally new API?
    lib = lmdb_version_store_v1
    lib_tool = lib.library_tool()
    sym = "test_read_incompletes_with_indexed_data"
    num_rows = 10
    df = pd.DataFrame({"col": np.arange(num_rows)}, pd.date_range("2024-01-01", periods=num_rows))
    lib.write(sym, df.iloc[:num_rows // 2])
    for idx in range(num_rows // 2, num_rows):
        lib_tool.append_incomplete(sym, df.iloc[idx: idx+1])
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
        lib_tool.append_incomplete(sym, df.iloc[idx: idx+1])
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
