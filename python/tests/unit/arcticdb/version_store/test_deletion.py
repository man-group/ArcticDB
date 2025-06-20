"""
Copyright 2023 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

import sys
import numpy as np
import pandas as pd
import pytest

from arcticdb.util.test import assert_frame_equal
from arcticdb.util.tasks import (
    append_small_df,
    snapshot_new_name,
    append_small_df_and_prune_previous,
    delete_snapshot,
)
from arcticdb_ext.exceptions import InternalException
from arcticdb_ext.storage import KeyType, NoDataFoundException
from arcticdb_ext.version_store import ManualClockVersionStore
from arcticdb.version_store._normalization import NPDDataFrame
from arcticdb.util.test import sample_dataframe
from arcticdb.version_store._store import resolve_defaults


def eprint(*args, **kwargs):
    print(*args, file=sys.stderr, **kwargs)


@pytest.mark.parametrize("pos", [0, 1])
def test_delete_version_with_update(version_store_factory, pos, sym):
    lmdb_version_store = version_store_factory()

    symbol = sym

    idx = pd.date_range("1970-01-01", periods=100, freq="D")
    df = pd.DataFrame({"a": np.arange(len(idx), dtype="float")}, index=idx)
    original_df = df.copy(deep=True)
    lmdb_version_store.write(symbol, df)

    idx2 = pd.date_range("1970-01-12", periods=10, freq="D")
    df2 = pd.DataFrame({"a": np.arange(1000, 1000 + len(idx2), dtype="float")}, index=idx2)
    lmdb_version_store.update(symbol, df2)

    assert_frame_equal(lmdb_version_store.read(symbol, 0).data, original_df)

    df.update(df2)

    vit = lmdb_version_store.read(symbol)
    assert_frame_equal(vit.data, df)
    assert_frame_equal(lmdb_version_store.read(symbol, 1).data, df)

    lmdb_version_store.delete_version(symbol, pos)
    assert len(lmdb_version_store.list_versions()) == 1

    if pos == 0:
        assert_frame_equal(lmdb_version_store.read(symbol).data, df)
        assert_frame_equal(lmdb_version_store.read(symbol, 1).data, df)
    else:
        assert_frame_equal(lmdb_version_store.read(symbol).data, original_df)
        assert_frame_equal(lmdb_version_store.read(symbol, 0).data, original_df)


def test_delete_by_timestamp(lmdb_version_store, sym):
    symbol = sym
    now = lmdb_version_store.version_store.get_store_current_timestamp_for_tests()
    lmdb_version_store.version_store = ManualClockVersionStore(lmdb_version_store._library)
    minute_in_ns = 60 * int(1e9)

    ManualClockVersionStore.time = now - 5 * minute_in_ns
    lmdb_version_store.write(symbol, 1)  # v0

    ManualClockVersionStore.time = now - 4 * minute_in_ns
    lmdb_version_store.write(symbol, 2)  # v1

    ManualClockVersionStore.time = now - 3 * minute_in_ns
    lmdb_version_store.write(symbol, 3)  # v2

    ManualClockVersionStore.time = now - 2 * minute_in_ns
    lmdb_version_store.write(symbol, 4)  # v3

    ManualClockVersionStore.time = now - 1 * minute_in_ns
    lmdb_version_store.write(symbol, 5)  # v4

    lmdb_version_store._prune_previous_versions(symbol, keep_mins=5.5)
    assert len(lmdb_version_store.list_versions(symbol)) == 5

    lmdb_version_store._prune_previous_versions(symbol, keep_mins=4.5)
    assert len(lmdb_version_store.list_versions(symbol)) == 4

    lmdb_version_store._prune_previous_versions(symbol, keep_mins=1.5, keep_version=2)
    assert len(lmdb_version_store.list_versions(symbol)) == 2

    lmdb_version_store._prune_previous_versions(symbol, keep_mins=1.5)
    assert len(lmdb_version_store.list_versions(symbol)) == 1


def test_clear_lmdb(lmdb_version_store, sym):
    symbol = sym
    lmdb_version_store.version_store.clear()
    df1 = pd.DataFrame({"x": np.arange(10, dtype=np.int64)})
    lmdb_version_store.write(symbol, df1)
    df2 = pd.DataFrame({"y": np.arange(10, dtype=np.int32)})
    lmdb_version_store.write(symbol, df2)
    df3 = pd.DataFrame({"z": np.arange(10, dtype=np.uint64)})
    lmdb_version_store.version_store.clear()
    df2 = pd.DataFrame({"y": np.arange(10, dtype=np.int32)})
    lmdb_version_store.write(symbol, df2)
    df3 = pd.DataFrame({"z": np.arange(10, dtype=np.uint64)})
    lmdb_version_store.write(symbol, df3)
    assert len(lmdb_version_store.list_versions(symbol)) == 2
    lmdb_version_store.version_store.clear()
    assert len(lmdb_version_store.list_symbols()) == 0


def test_delete_library_tool(version_store_factory, sym):
    ut_small_all_version_store = version_store_factory(col_per_group=5, row_per_segment=10)
    symbol = sym
    lt = ut_small_all_version_store.library_tool()
    ut_small_all_version_store.write(symbol, pd.DataFrame({"x": np.arange(10, dtype=np.int64)}))
    ut_small_all_version_store.write(symbol, pd.DataFrame({"y": np.arange(10, dtype=np.int32)}))
    df = sample_dataframe(1000)
    ut_small_all_version_store.write(symbol, df)

    assert len(ut_small_all_version_store.list_versions(symbol)) == 3
    ut_small_all_version_store.delete(symbol)
    for kt in KeyType.__members__.values():
        if kt == KeyType.VERSION or kt == KeyType.VERSION_REF:
            continue

        assert len(lt.find_keys_for_id(kt, symbol)) == 0


def test_delete_snapshot(version_store_factory):
    lmdb_version_store = version_store_factory(col_per_group=5, row_per_segment=10)
    lt = lmdb_version_store.library_tool()

    symbol = "test_delete_snapshot"
    snap = "test_delete_snapshot_snap"

    df1 = sample_dataframe(1000)
    lmdb_version_store.write(symbol, df1)
    lmdb_version_store.snapshot(snap)

    df2 = sample_dataframe(1000)
    lmdb_version_store.write(symbol, df2, prune_previous_version=True)

    # Should not raise as it exists in a snapshot
    lmdb_version_store.read(symbol, 0)

    assert_frame_equal(lmdb_version_store.read(symbol, as_of=snap).data, df1)

    lmdb_version_store.delete_snapshot(snap)
    with pytest.raises(NoDataFoundException):
        lmdb_version_store.read(symbol, as_of=snap)

    index_keys = lt.find_keys_for_id(KeyType.TABLE_INDEX, symbol)
    for k in index_keys:
        assert k.version_id != 0

    data_keys = lt.find_keys_for_id(KeyType.TABLE_DATA, symbol)
    for k in data_keys:
        assert k.version_id != 0


def test_tombstones_deleted_data_keys_prune(lmdb_version_store_prune_previous, sym):
    lib = lmdb_version_store_prune_previous
    assert lib._lib_cfg.lib_desc.version.write_options.prune_previous_version is True
    assert lib._lib_cfg.lib_desc.version.write_options.use_tombstones is True
    assert lib._lib_cfg.lib_desc.version.write_options.delayed_deletes is False
    lib.write(sym, 1)
    lib.write(sym, 2)
    lib.write(sym, 3)
    lib_tool = lib.library_tool()
    data_keys = lib_tool.find_keys_for_id(KeyType.TABLE_DATA, sym)
    assert len(data_keys) == 1

    lib.delete(sym)
    data_keys = lib_tool.find_keys_for_id(KeyType.TABLE_DATA, sym)
    assert len(data_keys) == 0


# this test also doubles up as a test for dfs with int index
def test_delete_snapshot_after_prune_previous(lmdb_version_store, sym):
    lib = lmdb_version_store
    assert lib._lib_cfg.lib_desc.version.write_options.delayed_deletes is False
    append_small_df(lib, sym)  # v0
    snapshot_new_name(lib, "snap")
    append_small_df(lib, sym)  # v1
    append_small_df_and_prune_previous(lib, sym)  # v2
    pre_delete_snapshot = lib.read(sym).data
    delete_snapshot(lib, "snap")
    post_delete_snapshot = lib.read(sym).data
    assert_frame_equal(pre_delete_snapshot, post_delete_snapshot)
    assert_frame_equal(pre_delete_snapshot, lib.read(sym, as_of=2).data)


# this test also doubles up as a test for dfs with datetime index
def test_delete_snapshot_after_update_and_prune_previous(lmdb_version_store, sym):
    lib = lmdb_version_store
    assert lib._lib_cfg.lib_desc.version.write_options.delayed_deletes is False
    idx = pd.date_range("1970-01-01", periods=100, freq="D")
    df = pd.DataFrame({"a": np.arange(len(idx), dtype="float")}, index=idx)
    lib.write(sym, df)  # v0
    snapshot_new_name(lib, "snap")
    idx2 = pd.date_range("1970-01-12", periods=10, freq="D")
    df2 = pd.DataFrame({"a": np.arange(1000, 1000 + len(idx2), dtype="float")}, index=idx2)
    lib.update(sym, df2)  # v1
    df.update(df2)
    assert_frame_equal(lib.read(sym).data, df)
    idx3 = pd.date_range("1970-06-22", periods=10, freq="D")
    df3 = pd.DataFrame({"a": np.arange(1000, 1000 + len(idx3), dtype="float")}, index=idx3)
    lib.append(sym, df3, prune_previous_version=True)  # v2
    final_df = pd.concat([df, df3])
    assert_frame_equal(lib.read(sym).data, final_df)

    pre_delete_snapshot = lib.read(sym).data
    delete_snapshot(lib, "snap")
    post_delete_snapshot = lib.read(sym).data
    assert_frame_equal(pre_delete_snapshot, post_delete_snapshot)
    assert_frame_equal(pre_delete_snapshot, lib.read(sym, as_of=2).data)
    assert_frame_equal(pre_delete_snapshot, final_df)


def test_delete_snapshot_update_with_full_overlap(lmdb_version_store, sym):
    idx = pd.date_range("1970-01-01", periods=100, freq="D")
    df = pd.DataFrame({"a": np.arange(len(idx), dtype="float")}, index=idx)
    original_df = df.copy(deep=True)
    lmdb_version_store.write(sym, df)

    lmdb_version_store.snapshot("snap")

    idx2 = pd.date_range("1970-01-12", periods=10, freq="D")
    df2 = pd.DataFrame({"a": np.arange(1000, 1000 + len(idx2), dtype="float")}, index=idx2)
    lmdb_version_store.update(sym, df2)

    assert_frame_equal(lmdb_version_store.read(sym, 0).data, original_df)

    df.update(df2)
    assert_frame_equal(lmdb_version_store.read(sym, 1).data, df)

    vit = lmdb_version_store.read(sym)
    assert_frame_equal(vit.data, df)

    lmdb_version_store.delete_version(sym, 0)
    assert len(lmdb_version_store.list_versions()) == 2

    assert_frame_equal(lmdb_version_store.read(sym).data, df)
    assert_frame_equal(lmdb_version_store.read(sym, 1).data, df)

    idx3 = pd.date_range("1970-01-12", periods=5, freq="D")
    df3 = pd.DataFrame({"a": np.arange(1000, 1000 + len(idx3), dtype="float")}, index=idx3)
    lmdb_version_store.update(sym, df3)

    df.update(df3)

    pre_delete_snapshot = lmdb_version_store.read(sym).data
    assert_frame_equal(pre_delete_snapshot, df)
    assert_frame_equal(pre_delete_snapshot, lmdb_version_store.read(sym, as_of=2).data)

    delete_snapshot(lmdb_version_store, "snap")
    post_delete_snapshot = lmdb_version_store.read(sym).data
    assert_frame_equal(pre_delete_snapshot, post_delete_snapshot)
    assert_frame_equal(pre_delete_snapshot, lmdb_version_store.read(sym, as_of=2).data)
    assert_frame_equal(pre_delete_snapshot, df)


def test_delete_snapshot_update_with_partial_overlap(lmdb_version_store, sym):
    idx = pd.date_range("1970-01-01", periods=100, freq="D")
    df = pd.DataFrame({"a": np.arange(len(idx), dtype="float")}, index=idx)
    original_df = df.copy(deep=True)
    lmdb_version_store.write(sym, df)

    lmdb_version_store.snapshot("snap")

    idx2 = pd.date_range("1970-02-12", periods=100, freq="D")
    df2 = pd.DataFrame({"a": np.arange(1000, 1000 + len(idx2), dtype="float")}, index=idx2)
    lmdb_version_store.update(sym, df2)

    assert_frame_equal(lmdb_version_store.read(sym, 0).data, original_df)

    # This will fix extend df to be the same as df + overlap + new data from df2
    # and then update df with df2
    # this way we can achieve the same result as an arcticdb update
    df = df.combine_first(df2)
    df.update(df2)
    assert_frame_equal(lmdb_version_store.read(sym, 1).data, df)

    res_df = lmdb_version_store.read(sym).data
    assert_frame_equal(res_df, df)

    lmdb_version_store.delete_version(sym, 0)
    assert len(lmdb_version_store.list_versions()) == 2

    assert_frame_equal(lmdb_version_store.read(sym).data, df)
    assert_frame_equal(lmdb_version_store.read(sym, 1).data, df)

    idx3 = pd.date_range("1970-03-12", periods=100, freq="D")
    df3 = pd.DataFrame({"a": np.arange(1000, 1000 + len(idx3), dtype="float")}, index=idx3)
    lmdb_version_store.update(sym, df3)

    df = df.combine_first(df3)
    df.update(df3)

    pre_delete_snapshot = lmdb_version_store.read(sym).data
    assert_frame_equal(pre_delete_snapshot, df)
    assert_frame_equal(pre_delete_snapshot, lmdb_version_store.read(sym, as_of=2).data)

    delete_snapshot(lmdb_version_store, "snap")
    post_delete_snapshot = lmdb_version_store.read(sym).data
    assert_frame_equal(pre_delete_snapshot, post_delete_snapshot)
    assert_frame_equal(pre_delete_snapshot, lmdb_version_store.read(sym, as_of=2).data)
    assert_frame_equal(pre_delete_snapshot, df)


@pytest.mark.parametrize("delete_order", [[0, 1, 2], [1, 0, 2], [0, 2, 1], [1, 2, 0], [2, 0, 1], [2, 1, 0]])
def test_tombstones_deleted_data_keys_version_delete(lmdb_version_store_prune_previous, sym, delete_order):
    lib = lmdb_version_store_prune_previous
    assert lib._lib_cfg.lib_desc.version.write_options.use_tombstones is True
    assert lib._lib_cfg.lib_desc.version.write_options.delayed_deletes is False

    lib.write(sym, 1)
    lib.write(sym, 2, prune_previous_version=False)
    lib.write(sym, 3, prune_previous_version=False)

    lib_tool = lib.library_tool()
    data_keys = lib_tool.find_keys_for_id(KeyType.TABLE_DATA, sym)
    assert len(data_keys) == 3

    for id in range(len(delete_order)):
        lib.delete_version(sym, delete_order[id])
        data_keys = lib_tool.find_keys_for_id(KeyType.TABLE_DATA, sym)
        assert len(data_keys) == 2 - id


@pytest.mark.parametrize("versions_to_delete", [[0, 1], [1, 0], [0, 2], [1, 2], [2, 0], [2, 1]])
def test_tombstones_deleted_data_keys_delete_versions(lmdb_version_store_prune_previous, sym, versions_to_delete):
    lib = lmdb_version_store_prune_previous
    assert lib._lib_cfg.lib_desc.version.write_options.use_tombstones is True
    assert lib._lib_cfg.lib_desc.version.write_options.delayed_deletes is False

    lib.write(sym, 1)
    lib.write(sym, 2, prune_previous_version=False)
    lib.write(sym, 3, prune_previous_version=False)

    lib_tool = lib.library_tool()
    data_keys = lib_tool.find_keys_for_id(KeyType.TABLE_DATA, sym)
    assert len(data_keys) == 3

    lib.delete_versions(sym, versions_to_delete)
    data_keys = lib_tool.find_keys_for_id(KeyType.TABLE_DATA, sym)
    assert len(data_keys) == 3 - len(versions_to_delete)


def test_tombstones_deleted_data_keys_snapshot(lmdb_version_store_prune_previous, sym):
    lib = lmdb_version_store_prune_previous
    assert lib._lib_cfg.lib_desc.version.write_options.prune_previous_version is True
    assert lib._lib_cfg.lib_desc.version.write_options.use_tombstones is True
    assert lib._lib_cfg.lib_desc.version.write_options.delayed_deletes is False

    lib.write(sym, 1)
    lib.snapshot("mysnap1")
    lib.write(sym, 2)
    lib.snapshot("mysnap2")
    lib.write(sym, 3)

    lib_tool = lib.library_tool()
    data_keys = lib_tool.find_keys_for_id(KeyType.TABLE_DATA, sym)
    assert len(data_keys) == 3

    lib.delete_version(sym, 2)

    data_keys = lib_tool.find_keys_for_id(KeyType.TABLE_DATA, sym)
    assert len(data_keys) == 2

    lib.delete_snapshot("mysnap1")

    data_keys = lib_tool.find_keys_for_id(KeyType.TABLE_DATA, sym)
    assert len(data_keys) == 1


def test_tombstones_multiple_deleted_data_keys_snapshot(lmdb_version_store, sym):
    lib = lmdb_version_store
    assert lib._lib_cfg.lib_desc.version.write_options.prune_previous_version is False

    lib.write(sym, 1)
    lib.snapshot("mysnap1")
    lib.write(sym, 2)
    lib.snapshot("mysnap2")
    lib.write(sym, 3)

    lib_tool = lib.library_tool()
    data_keys = lib_tool.find_keys_for_id(KeyType.TABLE_DATA, sym)
    assert len(data_keys) == 3

    lib.delete_versions(sym, [2, 1])

    data_keys = lib_tool.find_keys_for_id(KeyType.TABLE_DATA, sym)
    assert len(data_keys) == 2

    lib.delete_snapshot("mysnap2")

    data_keys = lib_tool.find_keys_for_id(KeyType.TABLE_DATA, sym)
    assert len(data_keys) == 1

    lib.delete_snapshot("mysnap1")

    data_keys = lib_tool.find_keys_for_id(KeyType.TABLE_DATA, sym)
    assert len(data_keys) == 1


def test_tombstone_of_non_existing_version(lmdb_version_store_tombstone, sym):
    def write_specific_version(data, version):
        proto_cfg = lib._lib_cfg.lib_desc.version.write_options
        dynamic_strings = resolve_defaults("dynamic_strings", proto_cfg, False)
        pickle_on_failure = resolve_defaults("pickle_on_failure", proto_cfg, False)
        udm, item, norm_meta = lib._try_normalize(sym, data, None, pickle_on_failure, dynamic_strings, None)
        if isinstance(item, NPDDataFrame):
            lib.version_store.write_dataframe_specific_version(sym, item, norm_meta, udm, version)

    lib = lmdb_version_store_tombstone
    assert lib._lib_cfg.lib_desc.version.write_options.prune_previous_version is False
    assert lib._lib_cfg.lib_desc.version.write_options.use_tombstones is True
    assert lib._lib_cfg.lib_desc.version.write_options.delayed_deletes is False

    write_specific_version(0, 0)
    write_specific_version(1, 1)
    write_specific_version(3, 3)
    write_specific_version(5, 5)

    # cant tombstone beyond latest
    with pytest.raises(Exception):
        lib.delete_version(sym, 6)

    with pytest.raises(Exception):
        lib.delete_version(sym, 10)

    lib.delete_version(sym, 2)
    lib.delete_version(sym, 1)
    lib.delete_version(sym, 4)
    lib.delete_version(sym, 5)

    assert lib.has_symbol(sym, 0) is True
    assert lib.has_symbol(sym, 1) is False
    assert lib.has_symbol(sym, 2) is False
    assert lib.has_symbol(sym, 3) is True
    assert lib.has_symbol(sym, 4) is False
    assert lib.has_symbol(sym, 5) is False

    with pytest.raises(Exception):
        lib.delete_version(sym, 6)


def test_tombstone_of_non_existing_version_multiple_deletes(lmdb_version_store_tombstone, sym):
    def write_specific_version(data, version):
        udm, item, norm_meta = lib._try_normalize(sym, data, None, False, False, None)
        if isinstance(item, NPDDataFrame):
            lib.version_store.write_dataframe_specific_version(sym, item, norm_meta, udm, version)

    lib = lmdb_version_store_tombstone
    assert lib._lib_cfg.lib_desc.version.write_options.prune_previous_version is False
    assert lib._lib_cfg.lib_desc.version.write_options.use_tombstones is True
    assert lib._lib_cfg.lib_desc.version.write_options.delayed_deletes is False

    write_specific_version(0, 0)
    write_specific_version(1, 1)
    write_specific_version(3, 3)
    write_specific_version(5, 5)

    # cant tombstone beyond latest
    with pytest.raises(Exception):
        lib.delete_versions(sym, [5, 6])

    with pytest.raises(Exception):
        lib.delete_versions(sym, [10, 11])

    lib.delete_versions(sym, [2, 1, 4, 5])

    assert lib.has_symbol(sym, 0) is True
    assert lib.has_symbol(sym, 1) is False
    assert lib.has_symbol(sym, 2) is False
    assert lib.has_symbol(sym, 3) is True
    assert lib.has_symbol(sym, 4) is False
    assert lib.has_symbol(sym, 5) is False

    with pytest.raises(Exception):
        lib.delete_versions(sym, [3, 5])

    with pytest.raises(Exception):
        lib.delete_versions(sym, [4, 5])

    with pytest.raises(Exception):
        lib.delete_versions(sym, [5, 6])

    with pytest.raises(Exception):
        lib.delete_versions(sym, [10, 11])


def test_delete_date_range_pickled_symbol(lmdb_version_store):
    symbol = "test_delete_date_range_pickled_symbol"
    idx = pd.date_range("2000-01-01", periods=4)
    df = pd.DataFrame({"a": [[1, 2], [3, 4], [5, 6], [7, 8]]}, index=idx)
    lmdb_version_store.write(symbol, df, pickle_on_failure=True)
    assert lmdb_version_store.is_symbol_pickled(symbol)
    with pytest.raises(InternalException) as e_info:
        lmdb_version_store.delete(symbol, (idx[1], idx[2]))
