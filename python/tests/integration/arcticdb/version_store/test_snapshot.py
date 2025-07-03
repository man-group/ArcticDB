"""
Copyright 2023 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

import pytest
import numpy as np
import pandas as pd
import datetime as dt
import re

from typing import Any
from arcticdb.arctic import Arctic
from arcticdb.arctic import Library
from arcticdb_ext.exceptions import InternalException
from arcticdb_ext.version_store import NoSuchVersionException
from arcticdb_ext.storage import NoDataFoundException
from arcticdb.util.test import distinct_timestamps
from arcticdb.version_store._store import NativeVersionStore
from arcticdb.util.test import (
    assert_frame_equal,
    create_df_index_rownum,
    create_df_index_datetime,
    dataframe_simulate_arcticdb_update_static,
)
from tests.util.storage_test import get_s3_storage_config
from arcticdb_ext.storage import KeyType
from arcticdb.version_store.library import DeleteRequest, WritePayload


@pytest.mark.storage
def test_basic_snapshot_flow(basic_store):
    original_data = [1, 2, 3]
    basic_store.write("a", original_data)
    basic_store.snapshot("snap_1")
    assert basic_store.read("a", as_of="snap_1").data == original_data
    snaps = [snap for snap, _meta in basic_store.list_snapshots().items()]  # snap: (snap_name, metadata)
    assert sorted(snaps) == sorted(["snap_1"])
    basic_store.delete_snapshot("snap_1")
    assert "snap_1" not in basic_store.list_snapshots()


@pytest.mark.storage
def test_re_snapshot_with_same_name(basic_store):
    original_data = [1, 2, 3]
    basic_store.write("a", original_data)
    basic_store.snapshot("snap_1")

    updated_data = [4, 5, 6]
    basic_store.write("a", updated_data, prune_previous_version=False)

    assert basic_store.read("a", as_of="snap_1").data == original_data

    with pytest.raises(InternalException):
        basic_store.snapshot("snap_1")


def test_read_old_snapshot_data(object_version_store):
    original_data = [1, 2, 3]
    modified_data = [1, 2, 3, 4]
    object_version_store.write("c", original_data)
    object_version_store.snapshot("snap_3")
    object_version_store.write("c", modified_data)
    object_version_store.snapshot("snap_4")
    assert object_version_store.read("c", as_of="snap_3").data == original_data
    assert object_version_store.read("c").data == modified_data


@pytest.mark.storage
def test_snapshot_metadata(object_version_store):
    original_data = [1, 2, 3]
    metadata = {"metadata": "Because why not?"}
    snap_name = "meta_snap"
    object_version_store.write(snap_name, original_data)
    object_version_store.snapshot(snap_name, metadata=metadata)

    assert object_version_store.read(snap_name).data == original_data
    all_snaps = object_version_store.list_snapshots()
    metadata_for_snap = [meta for snap, meta in all_snaps.items() if snap == snap_name][0]
    assert metadata_for_snap == metadata


@pytest.mark.storage
def test_snapshots_skip_symbol(object_version_store):
    original_data = [1, 2, 3]
    object_version_store.write("f", original_data)
    object_version_store.write("g", original_data)
    object_version_store.snapshot("snap_5", metadata=None, skip_symbols=["f"])
    assert object_version_store.read("g", as_of="snap_5").data == original_data
    with pytest.raises(Exception):
        object_version_store.read("f", as_of="snap_5")


@pytest.mark.storage
def test_snapshot_explicit_versions(basic_store):
    lib = basic_store
    original_data = [1, 2, 3]
    modified_data = [1, 2, 3, 4]

    lib.write("i", original_data)  # --> i, v0
    lib.write("i", modified_data)  # --> i, v1
    lib.write("j", original_data)  # --> j, v0
    lib.write("j", modified_data)  # --> j, v1

    lib.snapshot("snap_8", metadata=None, skip_symbols=[], versions={"i": 0, "j": 1})

    assert lib.read("i", as_of="snap_8").data == original_data
    assert lib.read("j", as_of="snap_8").data == modified_data


@pytest.mark.storage
def test_list_symbols_with_snaps(object_version_store):
    original_data = [1, 2, 3]

    object_version_store.write("s1", original_data)
    object_version_store.write("s2", original_data)

    object_version_store.snapshot("snap_9")
    object_version_store.write("s3", original_data)

    assert "s3" not in object_version_store.list_symbols(snapshot="snap_9")
    assert "s3" in object_version_store.list_symbols()


@pytest.mark.storage
def test_list_versions(object_version_store):
    lib = object_version_store
    original_data = [1, 2, 3]

    lib.write("t1", original_data)
    lib.write("t2", original_data)

    lib.snapshot("snap_versions")
    lib.write("t1", original_data)

    all_versions = lib.list_versions()
    print(all_versions)

    assert sorted([v["version"] for v in all_versions if v["symbol"] == "t1" and not v["deleted"]]) == sorted([0, 1])


@pytest.mark.storage
def test_snapshots_with_deletes(basic_store):
    original_data = [1, 2, 3]
    v1_data = [1, 2, 3, 4]

    basic_store.write("sym1", original_data)
    basic_store.write("sym1", v1_data)
    basic_store.write("sym2", original_data)

    # Delete without having anything in snapshots -> should delete that symbol completely
    basic_store.delete("sym1")

    assert basic_store.has_symbol("sym2")
    assert not basic_store.has_symbol("sym1")
    assert basic_store.list_symbols() == ["sym2"]
    # We should just have sym2
    assert not [ver for ver in basic_store.list_versions() if ver["symbol"] == "sym1"]

    basic_store.write("sym3", original_data)
    basic_store.snapshot("sym3_snap")

    # This version of sym3 is not in a snapshot
    basic_store.write("sym3", v1_data)

    # This should not delete the first version of sym3
    basic_store.delete("sym3")

    # Shouldn't be readable now without going through the snapshot.
    with pytest.raises(Exception):
        basic_store.read("sym3")

    assert basic_store.read("sym3", as_of="sym3_snap").data == original_data


@pytest.mark.storage
def test_snapshots_with_delete_batch(basic_store):
    original_data = [1, 2, 3]
    v1_data = [1, 2, 3, 4]

    basic_store.write_batch(
        [WritePayload("sym1", original_data), WritePayload("sym1", v1_data), WritePayload("sym2", original_data)]
    )

    # Delete without having anything in snapshots -> should delete:
    # - sym1, version 1
    # - sym2 completely
    basic_store.delete_batch([DeleteRequest("sym1", [1]), "sym2"])

    assert basic_store.has_symbol("sym1")
    assert not basic_store.has_symbol("sym2")
    assert basic_store.list_symbols() == ["sym1"]
    # We should just have sym1
    assert not [ver for ver in basic_store.list_versions() if ver["symbol"] == "sym2"]

    basic_store.write("sym3", original_data)
    basic_store.snapshot("sym3_snap")

    # This version of sym3 is not in a snapshot
    basic_store.write("sym3", v1_data)

    # This should NOT delete sym1 and shoud return an error
    # but should delete sym3 v1, which is not in a snapshot
    res = basic_store.delete_batch([DeleteRequest("sym1", [0]), "sym3"])
    assert len(res) == 1
    assert res[0].symbol == "sym1"

    assert basic_store.has_symbol("sym1")
    assert basic_store.read("sym1").data == original_data

    # sym3 shouldn't be readable now without going through the snapshot.
    with pytest.raises(Exception):
        basic_store.read("sym3")

    assert basic_store.read("sym3", as_of="sym3_snap").data == original_data
    assert basic_store.list_symbols() == ["sym1"]


@pytest.mark.storage
def test_delete_symbol_without_snapshot(basic_store):
    original_data = [1, 2, 3]
    v1_data = [1, 2, 3, 4]

    basic_store.write("sym1", original_data)
    basic_store.snapshot("sym1_snap")

    basic_store.write("sym1", v1_data)
    assert basic_store.read("sym1", as_of="sym1_snap").data == original_data

    basic_store.delete("sym1")

    with pytest.raises(Exception):
        basic_store.read("sym1")

    assert basic_store.read("sym1", as_of="sym1_snap").data == original_data
    assert basic_store.list_symbols() == []
    assert not basic_store.has_symbol("sym1")


@pytest.mark.storage
def test_write_to_symbol_in_snapshot_only(basic_store):
    original_data = [1, 2, 3]
    v1_data = [1, 2, 3, 4]

    basic_store.write("weird", original_data)
    basic_store.snapshot("store_sym_old")

    # The symbol should now only be in the snapshot
    basic_store.delete("weird")
    basic_store.write("weird", v1_data)

    assert basic_store.read("weird").data == v1_data
    assert basic_store.read("weird", as_of="store_sym_old").data == original_data


@pytest.mark.storage
def test_read_after_delete_with_snap(basic_store):
    data = np.random.randint(0, 10000, 1024 * 1024).reshape(1024, 1024)
    sym = "2003_australia1"
    basic_store.write(sym, data)
    basic_store.snapshot("world_cup_winners")

    basic_store.delete(sym)
    # import time; time.sleep(1)

    with pytest.raises(Exception):
        basic_store.read(sym)

    with pytest.raises(Exception):
        basic_store.read("random")


@pytest.mark.storage
def test_snapshot_with_versions_dict(basic_store):
    original_data = [1, 2, 3]
    basic_store.write("a", original_data)
    basic_store.write("b", original_data)
    basic_store.snapshot("snap_a", versions={"a": 0})
    basic_store.snapshot("snap_all")

    with pytest.raises(Exception):
        # Shouldn't be in the snapshots
        basic_store.read("b", as_of="snap_a")
    assert basic_store.read("b", as_of="snap_all").data == original_data


@pytest.mark.storage
def test_has_symbol_with_snapshot(basic_store):
    basic_store.write("a1", 1)
    basic_store.write("a3", 3)
    basic_store.snapshot("snap")
    assert basic_store.has_symbol("a2", "snap") is False
    with pytest.raises(Exception):
        basic_store.read("a2", "snap")

    assert basic_store.has_symbol("a1", "snap") is True
    assert basic_store.read("a1", as_of="snap").data == 1


@pytest.mark.storage
def test_pruned_symbol_in_symbol_read_version(basic_store_tombstone_and_pruning):
    lib = basic_store_tombstone_and_pruning
    lib.write("a", 1)
    lib.snapshot("snap")
    lib.write("a", 2)
    lib.write("a", 3)

    assert len([ver for ver in lib.list_versions() if not ver["deleted"]]) == 1
    assert lib.list_versions()[0]["version"] == 2

    assert lib.read("a").data == 3

    assert lib.read("a", as_of=0).data == 1  # Should be in snapshot
    assert lib.read("a", as_of="snap").data == 1


# TODO: Fix this with lazy-fixture
@pytest.mark.parametrize(
    "store", ["lmdb_version_store_v1", "lmdb_version_store_v2", "lmdb_version_store_tombstone_and_pruning"]
)
def test_read_symbol_with_ts_in_snapshot(store, request, sym):
    lib = request.getfixturevalue(store)
    lib.write(sym, 0)
    with distinct_timestamps(lib) as second_write_timestamps:
        lib.write(sym, 1)
    lib.snapshot("snap")
    # After this write only version 1 exists via the snapshot
    with distinct_timestamps(lib) as third_write_timestamps:
        lib.write(sym, 2, prune_previous_version=True)

    assert lib.read(sym).data == 2
    versions = lib.list_versions()
    assert len(versions) == 2  # deleted for version 1

    assert lib.read(sym, as_of=1).data == 1
    assert lib.read(sym, as_of=second_write_timestamps.after).version == 1
    assert lib.read(sym, as_of=second_write_timestamps.after).data == 1

    lib.snapshot("snap1")
    lib.delete_version(sym, 2)
    assert lib.read(sym, as_of=2).data == 2  # still in snapshot
    assert lib.read(sym, as_of=third_write_timestamps.after).version == 2


@pytest.mark.storage
def test_add_to_snapshot_simple(basic_store_tombstone_and_pruning):
    lib = basic_store_tombstone_and_pruning
    lib.write("s1", 1)
    lib.write("s2", 2)

    lib.snapshot("snap")
    lib.write("s3", 3)

    lib.add_to_snapshot("snap", ["s3"])
    lib.write("s3", 99)

    assert lib.read("s1", as_of="snap").data == 1
    assert lib.read("s2", as_of="snap").data == 2
    assert lib.read("s3", as_of="snap").data == 3


@pytest.mark.storage
def test_add_to_snapshot_missing_snap(basic_store_tombstone_and_pruning):
    lib = basic_store_tombstone_and_pruning
    lib.write("s1", 1)
    lib.write("s2", 2)

    lib.write("s3", 3)
    with pytest.raises(NoDataFoundException):
        lib.add_to_snapshot("snap", ["s3"])


@pytest.mark.storage
def test_add_to_snapshot_specific_version(basic_store_tombstone_and_pruning):
    lib = basic_store_tombstone_and_pruning
    lib.write("s1", 1)
    lib.write("s2", 2)

    lib.snapshot("snap")
    lib.write("s3", 1)
    lib.write("s3", 2)
    lib.write("s3", 3)

    lib.add_to_snapshot("snap", ["s3"], as_ofs=[2])
    lib.write("s3", 99)

    assert lib.read("s1", as_of="snap").data == 1
    assert lib.read("s2", as_of="snap").data == 2
    assert lib.read("s3", as_of="snap").data == 3


@pytest.mark.storage
def test_add_to_snapshot_replace(basic_store_tombstone_and_pruning):
    lib = basic_store_tombstone_and_pruning
    lib.write("s1", 1)
    lib.write("s2", 2)
    lib.write("s3", 2)

    lib.snapshot("snap")
    lib.write("s3", 3)

    lib.add_to_snapshot("snap", ["s3"])
    lib.write("s3", 99)

    assert lib.read("s1", as_of="snap").data == 1
    assert lib.read("s2", as_of="snap").data == 2
    assert lib.read("s3", as_of="snap").data == 3


@pytest.mark.storage
def test_add_to_snapshot_replace_specific(basic_store_tombstone_and_pruning):
    lib = basic_store_tombstone_and_pruning
    lib.write("s1", 1)
    lib.write("s2", 2)
    lib.write("s3", 1)

    lib.snapshot("snap")
    lib.snapshot("saved")
    lib.write("s3", 2)
    lib.write("s3", 3)

    lib.add_to_snapshot("snap", ["s3"], as_ofs=[2])
    lib.write("s3", 99)

    assert lib.read("s1", as_of="snap").data == 1
    assert lib.read("s2", as_of="snap").data == 2
    assert lib.read("s3", as_of="snap").data == 3

    # Make sure the key in the other snapshot is still readable
    assert lib.read("s3", as_of="saved").data == 1


@pytest.mark.storage
def test_add_to_snapshot_multiple(basic_store_tombstone_and_pruning):
    lib = basic_store_tombstone_and_pruning
    lib.write("s1", 1)
    lib.write("s2", 2)
    lib.write("s3", 2)

    lib.snapshot("snap")
    lib.write("s3", 2)
    lib.write("s3", 3)
    lib.write("s4", 3)
    lib.write("s4", 4)

    lib.add_to_snapshot("snap", ["s3", "s4"], as_ofs=[2, None])
    lib.write("s3", 99)
    lib.write("s4", 99)

    assert lib.read("s1", as_of="snap").data == 1
    assert lib.read("s2", as_of="snap").data == 2
    assert lib.read("s3", as_of="snap").data == 3
    assert lib.read("s4", as_of="snap").data == 4


@pytest.mark.storage
def test_remove_from_snapshot(basic_store_tombstone_and_pruning):
    lib = basic_store_tombstone_and_pruning
    lib.write("s1", 1)
    lib.write("s2", 2)
    lib.write("s3", 3)

    lib.snapshot("saved")
    lib.snapshot("snap")
    lib.write("s3", 4)

    assert lib.read("s3", as_of="snap").data == 3

    lib.remove_from_snapshot("snap", ["s3"], [0])
    versions = lib.list_versions(snapshot="snap")
    assert len(versions) == 2

    assert lib.read("s3", as_of="saved").data == 3


@pytest.mark.storage
def test_remove_from_snapshot_missing_snap(basic_store_tombstone_and_pruning):
    lib = basic_store_tombstone_and_pruning
    lib.write("s1", 1)
    lib.write("s2", 2)
    lib.write("s3", 3)

    with pytest.raises(NoDataFoundException):
        lib.remove_from_snapshot("snap", ["s3"], [0])


@pytest.mark.storage
def test_remove_from_snapshot_multiple(basic_store_tombstone_and_pruning):
    lib = basic_store_tombstone_and_pruning
    lib.write("s1", 1)
    lib.write("s2", 1)
    lib.write("s2", 2)
    lib.write("s3", 3)

    lib.snapshot("saved")
    lib.snapshot("snap")
    lib.write("s3", 4)

    assert lib.read("s3", as_of="snap").data == 3

    lib.remove_from_snapshot("snap", ["s2", "s3"], [1, 0])
    versions = lib.list_versions(snapshot="snap")
    assert len(versions) == 1
    assert lib.read("s3", as_of="saved").data == 3
    assert lib.read("s2", as_of="saved").data == 2


@pytest.mark.storage
def test_snapshot_not_accept_tombstoned_key(basic_store_delayed_deletes_v1, sym):
    lib = basic_store_delayed_deletes_v1
    ver = lib.write(sym, 1).version
    lib.write(sym, 2)
    with pytest.raises(NoSuchVersionException, match=re.escape(f"{sym}:{ver}")):  # sym contains square bracket...
        lib.snapshot("s", versions={sym: ver})


@pytest.mark.storage
def test_snapshot_partially_valid_version_map(basic_store_delayed_deletes_v1):
    lib = basic_store_delayed_deletes_v1
    symA = "A"
    symB = "B"

    symA_ver1 = lib.write(symA, 1).version
    lib.delete(symA)
    symB_ver1 = lib.write(symB, 3).version
    partial_valid_versions = {symA: symA_ver1, symB: symB_ver1}
    with pytest.raises(NoSuchVersionException):
        lib.snapshot("s", versions=partial_valid_versions)
    assert len(lib.list_snapshots()) == 0

    lib.snapshot("s", versions=partial_valid_versions, allow_partial_snapshot=True)
    with pytest.raises(NoSuchVersionException):
        lib.read(symA, as_of="s")
    assert lib.read(symB, as_of="s").data == 3


@pytest.mark.storage
def test_snapshot_tombstoned_but_referenced_in_other_snapshot_version(basic_store_delayed_deletes_v1):
    lib = basic_store_delayed_deletes_v1
    symA = "A"
    symB = "B"

    symA_ver = lib.write(symA, 1).version
    symB_ver = lib.write(symB, 1).version
    lib.snapshot("s")
    lib.delete(symA)
    lib.delete(symB)
    lib.snapshot("s2", versions={symA: symA_ver, symB: symB_ver})
    assert lib.read(symA, as_of="s2").data == 1
    assert lib.read(symB, as_of="s2").data == 1


def test_add_to_snapshot_atomicity(s3_bucket_versioning_storage, lib_name):
    storage = s3_bucket_versioning_storage
    lib = storage.create_version_store_factory(lib_name)()

    lib.write("s1", 1)
    lib.snapshot("snap")
    s2_ver = lib.write("s2", 2).version

    # Only keys being deleted will in the list of delete markers; Overwritten won't
    def assert_0_delete_marker(lib, storage):
        s3_admin = storage.factory._s3_admin
        bucket = storage.bucket
        # It's overly simplified; May not be bullet proof for other tests
        prefix = get_s3_storage_config(lib.lib_cfg()).prefix.replace(".", "/")
        response = s3_admin.list_object_versions(Bucket=bucket, Prefix=prefix)
        tref_delete_markers = [marker for marker in response.get("DeleteMarkers", []) if "/tref/" in marker["Key"]]
        assert len(tref_delete_markers) == 0

    lib.add_to_snapshot("snap", ["s2"])
    assert_0_delete_marker(lib, storage)

    lib.remove_from_snapshot("snap", ["s2"], [s2_ver])
    assert_0_delete_marker(lib, storage)


@pytest.mark.storage
def test_delete_snapshot_basic_flow(basic_store):
    lib = basic_store
    symbol = "sym1"
    snap1 = "snap1"
    snap2 = "snap2"

    df_0 = create_df_index_rownum(10, 0, 10)
    df_1 = create_df_index_rownum(10, 10, 20)
    df_combined = pd.concat([df_0, df_1])

    lib.write(symbol, df_0)
    lib.snapshot(snap1)
    lib.append(symbol, df_1)
    lib.snapshot(snap2)
    versions_initial = [v["version"] for v in lib.list_versions()]

    assert sorted(lib.list_snapshots()) == [snap1, snap2]
    assert_frame_equal(df_combined, lib.read(symbol).data)

    lib.delete_snapshot(snap1)
    versions_current = [v["version"] for v in lib.list_versions()]

    assert sorted(lib.list_snapshots()) == [snap2]
    assert sorted(versions_initial) == sorted(versions_current)
    assert_frame_equal(df_combined, lib.read(symbol).data)

    lib.delete_snapshot(snap2)
    versions_current = [v["version"] for v in lib.list_versions()]

    assert sorted(lib.list_snapshots()) == []
    assert sorted(versions_initial) == sorted(versions_current)
    assert_frame_equal(df_combined, lib.read(symbol).data)


@pytest.mark.storage
def test_delete_snapshot_basic_flow_with_delete_last_version(basic_store):
    lib = basic_store
    symbol = "sym1"
    snap1 = "snap1"
    snap2 = "snap2"

    df_0 = create_df_index_rownum(10, 0, 10)
    df_1 = create_df_index_rownum(10, 10, 20)

    lib.write(symbol, df_0)
    lib.snapshot(snap1)
    lib.write(symbol, df_1)
    lib.snapshot(snap2)
    versions_initial = sorted([v["version"] for v in lib.list_versions()])

    assert sorted(lib.list_snapshots()) == [snap1, snap2]
    assert_frame_equal(df_1, lib.read(symbol).data)
    assert versions_initial == [0, 1]
    assert len([ver for ver in lib.list_versions() if ver["deleted"]]) == 0

    lib.delete_version(symbol, 1)

    assert sorted(lib.list_snapshots()) == [snap1, snap2]
    # Version is still returned, but marked for deletion
    # Latest, or ver 1st is delete ver 0 is not
    assert [ver["deleted"] for ver in lib.list_versions()] == [True, False]
    assert_frame_equal(df_0, lib.read(symbol).data)
    # Althought the version is deleted it is not physically deleted
    # It can be read from the snapshot still
    assert_frame_equal(df_1, lib.read(symbol, as_of=snap2).data)

    lib.delete_snapshot(snap2)

    assert sorted(lib.list_snapshots()) == [snap1]
    assert sorted([v["version"] for v in lib.list_versions()]) == [0]
    assert len([ver for ver in lib.list_versions() if not ver["deleted"]]) == 1
    assert_frame_equal(df_0, lib.read(symbol).data)

    # Cannot read from deleted snapshot label
    with pytest.raises(Exception):
        data = lib.read(symbol, as_of=snap2).data


@pytest.mark.storage
def test_delete_snapshot_basic_flow_with_delete_prev_version(basic_store):
    lib = basic_store
    symbol = "sym1"
    snap1 = "snap1"
    snap2 = "snap2"

    df_0 = create_df_index_rownum(10, 0, 10)
    df_1 = create_df_index_rownum(10, 10, 20)
    df_combined = pd.concat([df_0, df_1])

    lib.write(symbol, df_0)
    lib.snapshot(snap1)
    lib.append(symbol, df_1)
    lib.snapshot(snap2)
    versions_initial = sorted([v["version"] for v in lib.list_versions()])

    assert sorted(lib.list_snapshots()) == [snap1, snap2]
    assert_frame_equal(df_combined, lib.read(symbol).data)
    assert versions_initial == [0, 1]

    lib.delete_version(symbol, 0)

    assert sorted(lib.list_snapshots()) == [snap1, snap2]
    # list_versions() return the newest versions first
    assert [ver["deleted"] for ver in lib.list_versions()] == [False, True]
    assert_frame_equal(df_combined, lib.read(symbol).data)
    # we can still read the version as it is pointed in snapshot
    assert_frame_equal(df_0, lib.read(symbol, as_of=snap1).data)

    lib.delete_snapshot(snap1)

    assert_frame_equal(df_combined, lib.read(symbol).data)
    assert sorted(lib.list_snapshots()) == [snap2]
    assert [ver["deleted"] for ver in lib.list_versions()] == [False]


@pytest.mark.xfail(
    reason="""ArcticDB#1863 or other bug. The fail is in the line lib.
                   read(symbol1).data after deleting snapshot 1, read operation throws exception"""
)
@pytest.mark.storage
def test_delete_snapshot_complex_flow_with_delete_multible_symbols(basic_store_tiny_segment_dynamic):
    lib = basic_store_tiny_segment_dynamic

    symbol1 = "sym1"
    df_1_0 = create_df_index_rownum(10, 0, 10)
    df_1_1 = create_df_index_rownum(10, 10, 200)
    df_1_2 = create_df_index_rownum(10, 1000, 1300)
    df_1_combined_0_1 = pd.concat([df_1_0, df_1_1])
    df_1_combined_0_2 = pd.concat([df_1_0, df_1_2])
    df_1_combined = pd.concat([df_1_0, df_1_1, df_1_2])
    lib.write(symbol1, df_1_0)
    lib.append(symbol1, df_1_1)
    lib.append(symbol1, df_1_2)

    symbol2 = "sym2"
    df_2_0 = create_df_index_rownum(10, -100, -90)
    df_2_1 = create_df_index_rownum(10, -80, -50)
    df_2_combined = pd.concat([df_2_0, df_2_1])
    lib.write(symbol2, df_2_0)
    lib.append(symbol2, df_2_1)

    symbol3 = "sym3"
    df_3_0 = create_df_index_rownum(10, 1000, 1010)
    df_3_1 = create_df_index_rownum(10, 2000, 2210)
    df_3_combined = pd.concat([df_3_0, df_3_1])
    lib.write(symbol3, df_3_0)
    lib.append(symbol3, df_3_1)

    snap1 = "snap1"
    snap2 = "snap2"
    snap1_vers = {symbol1: 1, symbol2: 1}
    lib.snapshot(snap1, versions=snap1_vers)

    # verify initial state
    assert sorted(lib.list_snapshots()) == [snap1]
    assert_frame_equal(df_1_combined, lib.read(symbol1).data)
    assert_frame_equal(df_2_combined, lib.read(symbol2).data)
    assert_frame_equal(df_3_combined, lib.read(symbol3).data)

    lib.delete_version(symbol1, 1)
    lib.delete_version(symbol2, 1)

    # confirm afer deletion of versions all is as expected
    assert sorted(lib.list_snapshots()) == [snap1]
    assert_frame_equal(df_1_combined, lib.read(symbol1).data)
    assert_frame_equal(df_2_0, lib.read(symbol2).data)
    assert_frame_equal(df_3_combined, lib.read(symbol3).data)
    # verify sumbol 1
    assert len(lib.list_versions(symbol1)) == 3
    assert [ver["deleted"] for ver in lib.list_versions(symbol1)] == [False, True, False]
    assert_frame_equal(df_1_combined_0_1, lib.read(symbol1, as_of=snap1).data)
    assert_frame_equal(df_1_combined_0_1, lib.read(symbol1, as_of=snap1_vers[symbol1]).data)
    # verify sumbol 2
    assert len(lib.list_versions(symbol2)) == 2
    assert [ver["deleted"] for ver in lib.list_versions(symbol2)] == [True, False]
    assert_frame_equal(df_2_combined, lib.read(symbol2, as_of=snap1).data)
    assert_frame_equal(df_2_combined, lib.read(symbol2, as_of=snap1_vers[symbol2]).data)
    # verify sumbol 3
    assert [ver["deleted"] for ver in lib.list_versions(symbol3)] == [False, False]

    lib.delete_snapshot(snap1)

    # confirm afer deletion of versions all is as expected
    # as well as deleting the snapshot wipes the versions effectivly
    assert sorted(lib.list_snapshots()) == []
    assert_frame_equal(df_1_combined, lib.read(symbol1).data)
    assert_frame_equal(df_2_0, lib.read(symbol2).data)
    assert_frame_equal(df_3_combined, lib.read(symbol3).data)
    # verify sumbol 1
    assert [ver["deleted"] for ver in lib.list_versions(symbol1)] == [False, False]
    # verify sumbol 2
    assert [ver["deleted"] for ver in lib.list_versions(symbol2)] == [False]
    # verify sumbol 3
    assert [ver["deleted"] for ver in lib.list_versions(symbol3)] == [False, False]


@pytest.mark.storage
def test_delete_snapshot_multiple_edge_case(basic_store):
    """
    Purpose of test is to examine snapshoting 3 symbols with minimum
    versions, where a version is in couple of snapshot, and then verifying that
    deletion of versions in both snapshots followed deletion of one of the snapshot
    will preserve the data abd versions as of the snapshots. After that we wipe out
    all versions and all snapshots to start from clean and verify snapshot cycle
    is infinite on symbol with same name
    """
    lib = basic_store

    symbol1 = "sym1"
    df_1 = create_df_index_rownum(10, 0, 10)
    df_2 = create_df_index_rownum(10, 20, 30)
    df_combined = pd.concat([df_1, df_2])

    lib.write(symbol1, df_1)

    symbol2 = "sym2"
    lib.write(symbol2, df_2)

    symbol3 = "sym3"
    lib.write(symbol3, df_1)

    snap1 = "snap1"
    lib.snapshot(snap1)

    lib.append(symbol3, df_2)

    snap2 = "snap2"
    lib.snapshot(snap2)

    # Now we have a 2 snapshots:
    # snap1 - sym1:0, sym2:0, sym3:0
    # snap2 - sym1:0, sym2:0, sym3:1
    # both snapshots are sharing versions

    assert sorted(lib.list_snapshots()) == [snap1, snap2]
    assert len(lib.list_versions(symbol1)) == 1
    assert len(lib.list_versions(symbol2)) == 1
    assert len(lib.list_versions(symbol3)) == 2
    assert_frame_equal(df_1, lib.read(symbol1).data)
    assert_frame_equal(df_2, lib.read(symbol2).data)
    assert_frame_equal(df_combined, lib.read(symbol3).data)

    lib.delete_version(symbol1, 0)
    lib.delete_version(symbol2, 0)
    lib.delete_version(symbol3, 0)

    assert sorted(lib.list_snapshots()) == [snap1, snap2]
    assert [ver["deleted"] for ver in lib.list_versions(symbol1)] == [True]
    assert [ver["deleted"] for ver in lib.list_versions(symbol2)] == [True]
    assert [ver["deleted"] for ver in lib.list_versions(symbol3)] == [False, True]
    assert_frame_equal(df_1, lib.read(symbol1, as_of=snap1).data)
    assert_frame_equal(df_2, lib.read(symbol2, as_of=snap2).data)
    assert_frame_equal(df_combined, lib.read(symbol3).data)
    with pytest.raises(NoSuchVersionException):
        lib.read(symbol1).data
    with pytest.raises(NoSuchVersionException):
        lib.read(symbol2).data

    lib.delete_snapshot(snap1)

    # version 0 is not deleted from sym1 and sym2
    # although that they do no have any data left in them
    assert sorted(lib.list_snapshots()) == [snap2]
    assert [ver["deleted"] for ver in lib.list_versions(symbol1)] == [True]
    assert [ver["deleted"] for ver in lib.list_versions(symbol2)] == [True]
    assert len(lib.list_versions(symbol3)) == 1
    assert_frame_equal(df_combined, lib.read(symbol3).data)
    with pytest.raises(NoSuchVersionException):
        lib.read(symbol1).data
    with pytest.raises(NoSuchVersionException):
        lib.read(symbol2).data
    with pytest.raises(Exception):
        lib.read(symbol1, as_of=snap1).data
    with pytest.raises(Exception):
        lib.read(symbol1, as_of=snap1).data

    lib.delete_version(symbol3, 1)
    lib.delete_snapshot(snap2)

    # After deleting all versions and last snapshot
    # there will be no data left in symbols
    assert sorted(lib.list_snapshots()) == []
    with pytest.raises(NoSuchVersionException):
        lib.read(symbol1).data
    with pytest.raises(NoSuchVersionException):
        lib.read(symbol2).data
    with pytest.raises(NoSuchVersionException):
        lib.read(symbol3).data

    ## Lets repeat same cycle with same symbols names and
    ## one snapshot to affirm everything can start all over again
    lib.write(symbol1, df_1)
    lib.write(symbol2, df_1)
    lib.write(symbol3, df_1)
    lib.snapshot(snap1)
    lib.delete_snapshot(snap1)

    assert_frame_equal(df_1, lib.read(symbol2).data)
    assert_frame_equal(df_1, lib.read(symbol3).data)
    assert_frame_equal(df_1, lib.read(symbol1).data)


@pytest.mark.storage
def test_delete_snapshot_on_updated_and_appended_dataframe(basic_store_tiny_segment):
    lib = basic_store_tiny_segment
    df_1 = create_df_index_datetime(10, 3, 10)
    # this dataframe has both values before the date and within the dates of df_1
    df_2 = create_df_index_datetime(10, 1, 5)
    df_3 = create_df_index_datetime(10, 20, 30)
    df_updated = dataframe_simulate_arcticdb_update_static(df_1, df_2)
    df_final = pd.concat([df_updated, df_3])

    symbol1 = "sym1"
    snap1 = "s1"
    snap2 = "s2"
    lib.write(symbol1, df_1)
    lib.update(symbol1, df_2)
    lib.snapshot(snap1)
    lib.append(symbol1, df_3)
    lib.snapshot(snap2)

    lib.delete_version(symbol1, 1)
    lib.delete_version(symbol1, 2)

    assert_frame_equal(df_1, lib.read(symbol1).data)
    assert_frame_equal(df_updated, lib.read(symbol1, as_of=snap1).data)
    assert_frame_equal(df_final, lib.read(symbol1, as_of=snap2).data)
    assert [ver["deleted"] for ver in lib.list_versions(symbol1)] == [True, True, False]

    lib.delete_snapshot(snap1)
    lib.delete_snapshot(snap2)

    assert_frame_equal(df_1, lib.read(symbol1).data)
    assert len(lib.list_versions(symbol1)) == 1
    with pytest.raises(NoDataFoundException):
        lib.read(symbol1, as_of=snap1).data
    with pytest.raises(NoDataFoundException):
        lib.read(symbol1, as_of=snap2).data


def test_snapshot_deletion_multiple_symbols(lmdb_version_store_v1):
    lib = lmdb_version_store_v1
    for symbol_idx in range(2):
        lib.write(f"sym_{symbol_idx}", pd.DataFrame({"col": [1, 2]}))
        lib.append(f"sym_{symbol_idx}", pd.DataFrame({"col": [3, 4]}))

    lib.snapshot("snap")
    lib.delete_version("sym_0", 1)
    lib.delete_version("sym_1", 1)

    lib_tool = lib.library_tool()

    for symbol_idx in range(2):
        assert len(lib_tool.find_keys_for_symbol(KeyType.TABLE_DATA, f"sym_{symbol_idx}")) == 2
        assert len(lib_tool.find_keys_for_symbol(KeyType.TABLE_INDEX, f"sym_{symbol_idx}")) == 2

    lib.delete_snapshot("snap")
    for symbol_idx in range(2):
        assert len(lib_tool.find_keys_for_symbol(KeyType.TABLE_DATA, f"sym_{symbol_idx}")) == 1
        assert len(lib_tool.find_keys_for_symbol(KeyType.TABLE_INDEX, f"sym_{symbol_idx}")) == 1
