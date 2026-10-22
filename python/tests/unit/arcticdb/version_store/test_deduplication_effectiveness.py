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
# TODO: Check works with deleted versions kept alive by snapshots.


def python_dedupe(lib, symbol):
    from more_itertools import pairwise

    versions = lib.list_versions(symbol)
    if not versions:
        raise NoDataFoundException(f"dedup_ratios: version not found for symbol {symbol}")

    assert all(new["version"] > old["version"] for new, old in pairwise(versions))

    res = {}
    new_atom_keys = set()
    for version in reversed(versions):
        as_of = version["version"] if not version["deleted"] else version["snapshots"][0]
        index = lib.read_index(symbol, as_of=as_of)
        newly_added_cells = 0
        for row in index.itertuples():
            atom_key = (
                row.Index,
                row.end_index,
                row.version_id,
                row.stream_id,
                row.creation_ts,
                row.content_hash,
                row.index_type,
            )

            if atom_key not in new_atom_keys:
                new_atom_keys.add(atom_key)
                newly_added_cells += (row.end_row - row.start_row) * (row.end_col - row.start_col)

        total_cells = int((index["end_row"] - index["start_row"]).mul(index["end_col"] - index["start_col"]).sum())
        res[version["version"]] = 1 - newly_added_cells / total_cells

    return res


SEGMENT_SIZE = 10


def test_basic(in_memory_store_factory):

    lib = in_memory_store_factory(segment_row_size=SEGMENT_SIZE, de_duplication=True)
    df1 = pd.Series(np.arange(SEGMENT_SIZE), index=pd.date_range("2020-01-01", periods=10, name="date"), name="data")
    df2 = pd.Series(np.arange(2 * SEGMENT_SIZE), index=pd.date_range("2020-01-01", periods=2 * SEGMENT_SIZE, name="date"), name="data")
    symbol_name: str = "symbol"

    lib.write(symbol_name, df1)
    lib.write(symbol_name, df2)

    result = lib.dedup_ratios("symbol")
    expected = {0: 0.0, 1: 0.5}
    assert result == pytest.approx(expected)

    df3 = pd.Series(np.arange(3 * SEGMENT_SIZE), index=pd.date_range("2020-01-01", periods=30, name="date"), name="data")
    lib.write(symbol_name, df3)
    lib.delete_version(symbol_name, 1)

    result = lib.dedup_ratios("symbol")
    expected = {0: 0.0, 2: 1 / 3}
    assert result == pytest.approx(expected)

    lib.delete_version(symbol_name, 0)

    result = lib.dedup_ratios("symbol")
    expected = {2: 0}
    assert result == pytest.approx(expected)


def test_deleted_versions_in_snapshots(in_memory_store_factory):
    lib = in_memory_store_factory(segment_row_size=SEGMENT_SIZE, de_duplication=True)
    df1 = pd.Series(np.arange(SEGMENT_SIZE), index=pd.date_range("2020-01-01", periods=10, name="date"), name="data")
    df2 = pd.Series(np.arange(2 * SEGMENT_SIZE), index=pd.date_range("2020-01-01", periods=2 * SEGMENT_SIZE, name="date"), name="data")
    df3 = pd.Series(np.arange(3 * SEGMENT_SIZE), index=pd.date_range("2020-01-01", periods=30, name="date"), name="data")
    symbol_name: str = "symbol"

    lib.write(symbol_name, df1)
    lib.snapshot("snapshot")
    lib.write(symbol_name, df2)
    lib.snapshot("snapshot2")
    lib.write(symbol_name, df3)
    # This will be kept alive by the snapshot
    lib.delete_version(symbol_name, 0)

    result = lib.dedup_ratios("symbol")
    # result = python_dedupe(lib, "symbol")
    expected = {0: 0.0, 1: 0.5, 2: 2 / 3}
    assert result == pytest.approx(expected)

    lib.delete_version(symbol_name, 1)
    result = lib.dedup_ratios("symbol")
    # result = python_dedupe(lib, "symbol")
    expected = {0: 0.0, 1: 0.5, 2: 2 / 3}
    assert result == pytest.approx(expected)


def test_raise_on_missing(in_memory_store_factory):
    lib = in_memory_store_factory()
    with pytest.raises(NoDataFoundException):
        lib.dedup_ratios("symbol")
