"""
Copyright 2023 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

import numpy as np
import pandas as pd
from pandas import DataFrame, Timestamp
import pytest

from arcticdb.version_store import NativeVersionStore, VersionedItem
from arcticdb.exceptions import ArcticDbNotYetImplemented
from arcticdb_ext.storage import NoDataFoundException
from arcticdb.util.test import assert_frame_equal, distinct_timestamps


# In the following lines, the naming convention is
# test_rt_df stands for roundtrip dataframe (implicitly pandas given file name)


def test_rt_df_with_small_meta(object_and_mem_and_lmdb_version_store):
    lib = object_and_mem_and_lmdb_version_store
    #  type: (NativeVersionStore)->None

    df = DataFrame(data=["A", "B", "C"])
    meta = {"abc": "def", "xxx": "yyy"}
    lib.write("pandas", df, metadata=meta)
    vit = lib.read("pandas")
    assert_frame_equal(df, vit.data)
    assert meta == vit.metadata


def test_rt_df_with_humonguous_meta(object_and_mem_and_lmdb_version_store):
    with pytest.raises(ArcticDbNotYetImplemented):
        from arcticdb.version_store._normalization import _MAX_USER_DEFINED_META as MAX

        df = DataFrame(data=["A", "B", "C"])
        meta = {"a": "x" * (MAX)}
        object_and_mem_and_lmdb_version_store.write("pandas", df, metadata=meta)

        vit = object_and_mem_and_lmdb_version_store.read("pandas")
        assert_frame_equal(df, vit)
        assert meta == vit.metadata


def test_read_metadata(object_and_mem_and_lmdb_version_store):
    lib = object_and_mem_and_lmdb_version_store
    original_data = [1, 2, 3]
    snap_name = "metadata_snap_1"
    symbol = "test_symbol"
    metadata = {"something": 1}
    lib.write(symbol, original_data, metadata={"something": 1})
    lib.snapshot(snap_name, metadata={"snap_meta": 1})
    assert lib.read_metadata("test_symbol").metadata == metadata


def test_read_metadata_by_version(object_and_mem_and_lmdb_version_store):
    lib = object_and_mem_and_lmdb_version_store
    data_v1 = [1, 2, 3]
    data_v2 = [10, 20, 30]
    symbol = "test_symbol"
    metadata_v0 = {"something": 1}
    metadata_v1 = {"something more": 2}
    lib.write(symbol, data_v1, metadata=metadata_v0)
    lib.write(symbol, data_v2, metadata=metadata_v1)

    assert lib.read_metadata(symbol).metadata == metadata_v1
    assert lib.read_metadata(symbol, 0).metadata == metadata_v0
    assert lib.read_metadata(symbol, 1).metadata == metadata_v1


def test_read_metadata_by_snapshot(basic_store):
    original_data = [1, 2, 3]
    snap_name = "metadata_snap_1"
    symbol = "test_symbol"
    metadata = {"something": 1}
    basic_store.write(symbol, original_data, metadata={"something": 1})
    basic_store.snapshot(snap_name, metadata={"snap_meta": 1})
    assert basic_store.read_metadata(symbol).metadata == metadata
    assert basic_store.read_metadata(symbol, snap_name).metadata == metadata


def test_read_metadata_by_timestamp(basic_store):
    symbol = "test_symbol"

    metadata_v0 = {"something": 1}
    with distinct_timestamps(basic_store) as first_write_timestamps:
        basic_store.write(symbol, 1, metadata=metadata_v0)  # v0

    with pytest.raises(NoDataFoundException):
        basic_store.read(symbol, as_of=Timestamp(0))

    assert basic_store.read_metadata(symbol, as_of=first_write_timestamps.after).metadata == metadata_v0

    metadata_v1 = {"something more": 2}
    with distinct_timestamps(basic_store):
        basic_store.write(symbol, 2, metadata=metadata_v1)  # v1

    metadata_v2 = {"something else": 3}
    with distinct_timestamps(basic_store):
        basic_store.write(symbol, 3, metadata=metadata_v2)  # v2

    metadata_v3 = {"nothing": 4}
    basic_store.write(symbol, 4, metadata=metadata_v3)  # v3

    versions = basic_store.list_versions()
    assert len(versions) == 4
    sorted_versions_for_a = sorted([v for v in versions if v["symbol"] == symbol], key=lambda x: x["version"])

    assert basic_store.read_metadata(symbol, as_of=first_write_timestamps.after).metadata == metadata_v0

    ts_for_v1 = sorted_versions_for_a[1]["date"]
    assert basic_store.read_metadata(symbol, as_of=ts_for_v1).metadata == metadata_v1

    ts_for_v2 = sorted_versions_for_a[2]["date"]
    assert basic_store.read_metadata(symbol, as_of=ts_for_v2).metadata == metadata_v2

    with pytest.raises(NoDataFoundException):
        basic_store.read(symbol, as_of=Timestamp(0))

    brexit_almost_over = Timestamp(np.iinfo(np.int64).max)  # Timestamp("2262-04-11 23:47:16.854775807")
    assert basic_store.read_metadata(symbol, as_of=brexit_almost_over).metadata == metadata_v3


def test_write_metadata_first_write(basic_store, sym):
    metadata_v0 = {"something": 1}
    # basic_store.write(symbol, 1, metadata=metadata_v0)  # v0
    basic_store.write_metadata(sym, metadata_v0)
    vitem = basic_store.read(sym)
    assert vitem.data is None
    assert vitem.metadata == metadata_v0
    assert len(basic_store.list_versions(sym)) == 1


def test_write_metadata_preexisting_symbol(basic_store, sym):
    lib = basic_store
    metadata_v0 = {"something": 1}
    metadata_v1 = {"something": 2}
    # basic_store.write(symbol, 1, metadata=metadata_v0)  # v0
    lib.write(sym, 1, metadata=metadata_v0)
    vi = lib.write_metadata(sym, metadata_v1)
    assert vi.version == 1
    assert isinstance(vi, VersionedItem)
    assert lib.read(sym).metadata == metadata_v1
    assert lib.read(sym).data == 1


def test_write_metadata_preexisting_symbol_no_pruning(basic_store, sym):
    lib = basic_store
    metadata_v0 = {"something": 1}
    metadata_v1 = {"something": 2}
    # basic_store.write(symbol, 1, metadata=metadata_v0)  # v0
    lib.write(sym, 1, metadata=metadata_v0)
    lib.write_metadata(sym, metadata_v1, prune_previous_version=False)
    assert lib.read(sym).metadata == metadata_v1
    assert lib.read(sym).data == 1
    assert lib.read(sym, as_of=0).metadata == metadata_v0
    assert lib.read(sym, as_of=0).data == 1


def timestamp_indexed_df():
    return pd.DataFrame({"col": [0]}, index=[pd.Timestamp("2024-01-01")])


def test_rv_contains_metadata_write(lmdb_version_store_v1):
    lib = lmdb_version_store_v1
    sym = "test_write_rv_contains_metadata"
    assert lib.write(sym, 1).metadata is None
    metadata = {"some": "metadata"}
    assert lib.write(sym, 1, metadata).metadata == metadata


def test_rv_contains_metadata_append(lmdb_version_store_v1):
    lib = lmdb_version_store_v1
    sym = "test_rv_contains_metadata_append"
    assert lib.append(sym, timestamp_indexed_df(), write_if_missing=True).metadata is None
    metadata = {"some": "metadata"}
    assert lib.append(sym, timestamp_indexed_df(), metadata, write_if_missing=True).metadata == metadata


def test_rv_contains_metadata_update(lmdb_version_store_v1):
    lib = lmdb_version_store_v1
    sym = "test_rv_contains_metadata_update"
    assert lib.update(sym, timestamp_indexed_df(), upsert=True).metadata is None
    metadata = {"some": "metadata"}
    assert lib.update(sym, timestamp_indexed_df(), metadata, upsert=True).metadata == metadata


def test_rv_contains_metadata_write_metadata(lmdb_version_store_v1):
    lib = lmdb_version_store_v1
    sym = "test_rv_contains_metadata_write_metadata"
    metadata = {"some": "metadata"}
    assert lib.write_metadata(sym, metadata).metadata == metadata


def test_rv_contains_metadata_batch_write(lmdb_version_store_v1):
    lib = lmdb_version_store_v1
    sym_0 = "test_rv_contains_metadata_batch_write_0"
    sym_1 = "test_rv_contains_metadata_batch_write_1"
    sym_2 = "test_rv_contains_metadata_batch_write_2"
    vits = lib.batch_write([sym_0, sym_1, sym_2], 3 * [1])
    assert all(vit.metadata is None for vit in vits)
    metadata_0 = {"some": "metadata_0"}
    metadata_2 = {"some": "metadata_2"}
    vits = lib.batch_write([sym_0, sym_1, sym_2], 3 * [1], [metadata_0, None, metadata_2])
    assert vits[0].metadata == metadata_0
    assert vits[1].metadata is None
    assert vits[2].metadata == metadata_2


def test_rv_contains_metadata_batch_append(lmdb_version_store_v1):
    lib = lmdb_version_store_v1
    sym_0 = "test_rv_contains_metadata_batch_append_0"
    sym_1 = "test_rv_contains_metadata_batch_append_1"
    sym_2 = "test_rv_contains_metadata_batch_append_2"
    vits = lib.batch_append([sym_0, sym_1, sym_2], 3 * [timestamp_indexed_df()], write_if_missing=True)
    assert all(vit.metadata is None for vit in vits)
    metadata_0 = {"some": "metadata_0"}
    metadata_2 = {"some": "metadata_2"}
    vits = lib.batch_append(
        [sym_0, sym_1, sym_2], 3 * [timestamp_indexed_df()], [metadata_0, None, metadata_2], write_if_missing=True
    )
    assert vits[0].metadata == metadata_0
    assert vits[1].metadata is None
    assert vits[2].metadata == metadata_2


def test_rv_contains_metadata_batch_write_metadata(lmdb_version_store_v1):
    lib = lmdb_version_store_v1
    sym_0 = "test_rv_contains_metadata_batch_write_metadata_0"
    sym_1 = "test_rv_contains_metadata_batch_write_metadata_1"
    metadata_0 = {"some": "metadata_0"}
    metadata_1 = {"some": "metadata_1"}
    vits = lib.batch_write_metadata([sym_0, sym_1], [metadata_0, metadata_1])
    assert vits[0].metadata == metadata_0
    assert vits[1].metadata == metadata_1
