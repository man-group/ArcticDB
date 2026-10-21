"""
Copyright 2026 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

import numpy as np
import pandas as pd
import pytest

from arcticdb.exceptions import NoDataFoundException

# TODO: Parametrize over v1 and v2 APIs.

SEGMENT_SIZE = 10


def gen_df(size: int) -> pd.Series:
    return pd.Series(np.arange(size), index=pd.date_range("2020-01-01", periods=size, name="date"), name="data")


def test_basic(in_memory_store_factory):

    lib = in_memory_store_factory(segment_row_size=SEGMENT_SIZE, de_duplication=True)
    df1 = gen_df(SEGMENT_SIZE)
    df2 = gen_df(2 * SEGMENT_SIZE)
    symbol_name: str = "symbol"

    lib.write(symbol_name, df1)
    lib.write(symbol_name, df2)

    result = lib.batch_dedup_ratios(["symbol"])
    expected = {("symbol", 0): 0.0, ("symbol", 1): 0.5}
    assert result == pytest.approx(expected)

    df3 = gen_df(3 * SEGMENT_SIZE)
    lib.write(symbol_name, df3)
    lib.delete_version(symbol_name, 1)

    result = lib.batch_dedup_ratios(["symbol"])
    expected = {("symbol", 0): 0.0, ("symbol", 2): 1 / 3}
    assert result == pytest.approx(expected)

    lib.delete_version(symbol_name, 0)

    result = lib.batch_dedup_ratios(["symbol"])
    expected = {("symbol", 2): 0}
    assert result == pytest.approx(expected)


def test_deleted_versions_in_snapshots(in_memory_store_factory):
    lib = in_memory_store_factory(segment_row_size=SEGMENT_SIZE, de_duplication=True)
    df1 = gen_df(SEGMENT_SIZE)
    df2 = gen_df(2 * SEGMENT_SIZE)
    df3 = gen_df(3 * SEGMENT_SIZE)
    symbol_name: str = "symbol"

    lib.write(symbol_name, df1)
    lib.snapshot("snapshot")
    lib.write(symbol_name, df2)
    lib.snapshot("snapshot2")
    lib.write(symbol_name, df3)
    # This will be kept alive by the snapshot
    lib.delete_version(symbol_name, 0)

    result = lib.batch_dedup_ratios(["symbol"])
    expected = {("symbol", 0): 0.0, ("symbol", 1): 0.5, ("symbol", 2): 2 / 3}
    assert result == pytest.approx(expected)

    lib.delete_version(symbol_name, 1)
    result = lib.batch_dedup_ratios(["symbol"])
    expected = {("symbol", 0): 0.0, ("symbol", 1): 0.5, ("symbol", 2): 2 / 3}
    assert result == pytest.approx(expected)


def test_raise_on_missing(in_memory_store_factory):
    lib = in_memory_store_factory()
    with pytest.raises(NoDataFoundException, match="batch_dedup_ratios: version not found for symbol 'symbol'"):
        lib.batch_dedup_ratios(["symbol"])


def test_batch_operation(in_memory_store_factory):
    lib = in_memory_store_factory(segment_row_size=SEGMENT_SIZE, de_duplication=True)

    lib.write("symbol1", gen_df(SEGMENT_SIZE))
    lib.snapshot("snapshot")
    lib.write("symbol2", gen_df(2 * SEGMENT_SIZE))
    lib.snapshot("snapshot2")
    lib.write("symbol1", gen_df(3 * SEGMENT_SIZE))
    lib.write("symbol2", gen_df(4 * SEGMENT_SIZE))
    # This will be kept alive by the snapshot
    lib.delete_version("symbol1", 0)

    result = lib.batch_dedup_ratios(["symbol1", "symbol2"])
    expected = {("symbol1", 0): 0.0, ("symbol1", 1): 1 / 3, ("symbol2", 0): 0, ("symbol2", 1): 1 / 2}
    assert result == pytest.approx(expected)


def test_empty_writes(in_memory_store_factory):
    lib = in_memory_store_factory(segment_row_size=SEGMENT_SIZE, de_duplication=True)

    lib.write("symbol", gen_df(0))
    result = lib.batch_dedup_ratios(["symbol"])
    expected = {("symbol", 0): 0.0}
    assert result == pytest.approx(expected)
