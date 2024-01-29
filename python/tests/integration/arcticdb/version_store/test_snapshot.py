"""
Copyright 2023 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""
import pytest
import numpy as np
import re

from arcticdb_ext.exceptions import InternalException
from arcticdb_ext.version_store import NoSuchVersionException
from arcticdb.util.test import distinct_timestamps


def test_basic_snapshot_flow(lmdb_version_store):
    original_data = [1, 2, 3]
    lmdb_version_store.write("a", original_data)
    lmdb_version_store.snapshot("snap_1")
    assert lmdb_version_store.read("a", as_of="snap_1").data == original_data
    snaps = [snap for snap, _meta in lmdb_version_store.list_snapshots().items()]  # snap: (snap_name, metadata)
    assert sorted(snaps) == sorted(["snap_1"])
    lmdb_version_store.delete_snapshot("snap_1")
    assert "snap_1" not in lmdb_version_store.list_snapshots()


def test_re_snapshot_with_same_name(lmdb_version_store):
    original_data = [1, 2, 3]
    lmdb_version_store.write("a", original_data)
    lmdb_version_store.snapshot("snap_1")

    updated_data = [4, 5, 6]
    lmdb_version_store.write("a", updated_data, prune_previous_version=False)

    assert lmdb_version_store.read("a", as_of="snap_1").data == original_data

    with pytest.raises(InternalException):
        lmdb_version_store.snapshot("snap_1")


def test_read_old_snapshot_data(object_version_store):
    original_data = [1, 2, 3]
    modified_data = [1, 2, 3, 4]
    object_version_store.write("c", original_data)
    object_version_store.snapshot("snap_3")
    object_version_store.write("c", modified_data)
    object_version_store.snapshot("snap_4")
    assert object_version_store.read("c", as_of="snap_3").data == original_data
    assert object_version_store.read("c").data == modified_data


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


def test_snapshots_skip_symbol(object_version_store):
    original_data = [1, 2, 3]
    object_version_store.write("f", original_data)
    object_version_store.write("g", original_data)
    object_version_store.snapshot("snap_5", metadata=None, skip_symbols=["f"])
    assert object_version_store.read("g", as_of="snap_5").data == original_data
    with pytest.raises(Exception):
        object_version_store.read("f", as_of="snap_5")


def test_snapshot_explicit_versions(lmdb_version_store):
    lib = lmdb_version_store
    original_data = [1, 2, 3]
    modified_data = [1, 2, 3, 4]

    lib.write("i", original_data)  # --> i, v0
    lib.write("i", modified_data)  # --> i, v1
    lib.write("j", original_data)  # --> j, v0
    lib.write("j", modified_data)  # --> j, v1

    lib.snapshot("snap_8", metadata=None, skip_symbols=[], versions={"i": 0, "j": 1})

    assert lib.read("i", as_of="snap_8").data == original_data
    assert lib.read("j", as_of="snap_8").data == modified_data


def test_list_symbols_with_snaps(object_version_store):
    original_data = [1, 2, 3]

    object_version_store.write("s1", original_data)
    object_version_store.write("s2", original_data)

    object_version_store.snapshot("snap_9")
    object_version_store.write("s3", original_data)

    assert "s3" not in object_version_store.list_symbols(snapshot="snap_9")
    assert "s3" in object_version_store.list_symbols()


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


def test_snapshots_with_deletes(lmdb_version_store):
    original_data = [1, 2, 3]
    v1_data = [1, 2, 3, 4]

    lmdb_version_store.write("sym1", original_data)
    lmdb_version_store.write("sym1", v1_data)
    lmdb_version_store.write("sym2", original_data)

    # Delete without having anything in snapshots -> should delete that symbol completely
    lmdb_version_store.delete("sym1")

    assert lmdb_version_store.has_symbol("sym2")
    assert not lmdb_version_store.has_symbol("sym1")
    assert lmdb_version_store.list_symbols() == ["sym2"]
    # We should just have sym2
    assert not [ver for ver in lmdb_version_store.list_versions() if ver["symbol"] == "sym1"]

    lmdb_version_store.write("sym3", original_data)
    lmdb_version_store.snapshot("sym3_snap")

    # This version of sym3 is not in a snapshot
    lmdb_version_store.write("sym3", v1_data)

    # This should not delete the first version of sym3
    lmdb_version_store.delete("sym3")

    # Shouldn't be readable now without going through the snapshot.
    with pytest.raises(Exception):
        lmdb_version_store.read("sym3")

    assert lmdb_version_store.read("sym3", as_of="sym3_snap").data == original_data


def test_delete_symbol_without_snapshot(lmdb_version_store):
    original_data = [1, 2, 3]
    v1_data = [1, 2, 3, 4]

    lmdb_version_store.write("sym1", original_data)
    lmdb_version_store.snapshot("sym1_snap")

    lmdb_version_store.write("sym1", v1_data)
    assert lmdb_version_store.read("sym1", as_of="sym1_snap").data == original_data

    lmdb_version_store.delete("sym1")

    with pytest.raises(Exception):
        lmdb_version_store.read("sym1")

    assert lmdb_version_store.read("sym1", as_of="sym1_snap").data == original_data
    assert lmdb_version_store.list_symbols() == []
    assert not lmdb_version_store.has_symbol("sym1")


def test_write_to_symbol_in_snapshot_only(lmdb_version_store):
    original_data = [1, 2, 3]
    v1_data = [1, 2, 3, 4]

    lmdb_version_store.write("weird", original_data)
    lmdb_version_store.snapshot("store_sym_old")

    # The symbol should now only be in the snapshot
    lmdb_version_store.delete("weird")
    lmdb_version_store.write("weird", v1_data)

    assert lmdb_version_store.read("weird").data == v1_data
    assert lmdb_version_store.read("weird", as_of="store_sym_old").data == original_data


def test_read_after_delete_with_snap(lmdb_version_store):
    data = np.random.randint(0, 10000, 1024 * 1024).reshape(1024, 1024)
    sym = "2003_australia1"
    lmdb_version_store.write(sym, data)
    lmdb_version_store.snapshot("world_cup_winners")

    lmdb_version_store.delete(sym)
    # import time; time.sleep(1)

    with pytest.raises(Exception):
        lmdb_version_store.read(sym)

    with pytest.raises(Exception):
        lmdb_version_store.read("random")


def test_snapshot_with_versions_dict(lmdb_version_store):
    original_data = [1, 2, 3]
    lmdb_version_store.write("a", original_data)
    lmdb_version_store.write("b", original_data)
    lmdb_version_store.snapshot("snap_a", versions={"a": 0})
    lmdb_version_store.snapshot("snap_all")

    with pytest.raises(Exception):
        # Shouldn't be in the snapshots
        lmdb_version_store.read("b", as_of="snap_a")
    assert lmdb_version_store.read("b", as_of="snap_all").data == original_data


def test_has_symbol_with_snapshot(lmdb_version_store):
    lmdb_version_store.write("a1", 1)
    lmdb_version_store.write("a3", 3)
    lmdb_version_store.snapshot("snap")
    assert lmdb_version_store.has_symbol("a2", "snap") is False
    with pytest.raises(Exception):
        lmdb_version_store.read("a2", "snap")

    assert lmdb_version_store.has_symbol("a1", "snap") is True
    assert lmdb_version_store.read("a1", as_of="snap").data == 1


def test_pruned_symbol_in_symbol_read_version(lmdb_version_store_tombstone_and_pruning):
    lib = lmdb_version_store_tombstone_and_pruning
    lib.write("a", 1)
    lib.snapshot("snap")
    lib.write("a", 2)
    lib.write("a", 3)

    assert len([ver for ver in lib.list_versions() if not ver["deleted"]]) == 1
    assert lib.list_versions()[0]["version"] == 2

    assert lib.read("a").data == 3

    assert lib.read("a", as_of=0).data == 1  # Should be in snapshot
    assert lib.read("a", as_of="snap").data == 1


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


def test_add_to_snapshot_simple(lmdb_version_store_tombstone_and_pruning):
    lib = lmdb_version_store_tombstone_and_pruning
    lib.write("s1", 1)
    lib.write("s2", 2)

    lib.snapshot("snap")
    lib.write("s3", 3)

    lib.add_to_snapshot("snap", ["s3"])
    lib.write("s3", 99)

    assert lib.read("s1", as_of="snap").data == 1
    assert lib.read("s2", as_of="snap").data == 2
    assert lib.read("s3", as_of="snap").data == 3


def test_add_to_snapshot_specific_version(lmdb_version_store_tombstone_and_pruning):
    lib = lmdb_version_store_tombstone_and_pruning
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


def test_add_to_snapshot_replace(lmdb_version_store_tombstone_and_pruning):
    lib = lmdb_version_store_tombstone_and_pruning
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


def test_add_to_snapshot_replace_specific(lmdb_version_store_tombstone_and_pruning):
    lib = lmdb_version_store_tombstone_and_pruning
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


def test_add_to_snapshot_multiple(lmdb_version_store_tombstone_and_pruning):
    lib = lmdb_version_store_tombstone_and_pruning
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


def test_remove_from_snapshot(lmdb_version_store_tombstone_and_pruning):
    lib = lmdb_version_store_tombstone_and_pruning
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


def test_remove_from_snapshot_multiple(lmdb_version_store_tombstone_and_pruning):
    lib = lmdb_version_store_tombstone_and_pruning
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


def test_snapshot_not_accept_tombstoned_key(lmdb_version_store_tombstone_and_pruning, sym):
    lib = lmdb_version_store_tombstone_and_pruning
    ver = lib.write(sym, 1).version
    lib.write(sym, 2)
    with pytest.raises(NoSuchVersionException, match=re.escape(f"{sym}:{ver}")): # sym contains square bracket...
        lib.snapshot("s", versions={sym:ver})


def test_snapshot_partially_valid_version_map(lmdb_version_store_tombstone_and_pruning):
    lib = lmdb_version_store_tombstone_and_pruning
    symA = "A"
    symB = "B"

    symA_ver1 = lib.write(symA, 1).version
    lib.delete(symA)
    symB_ver1 = lib.write(symB, 3).version
    partial_valid_versions = {symA:symA_ver1, symB:symB_ver1}
    with pytest.raises(NoSuchVersionException):
        lib.snapshot("s", versions=partial_valid_versions)
    assert len(lib.list_snapshots()) == 0

    lib.snapshot("s", versions=partial_valid_versions, allow_partial_snapshot=True)
    with pytest.raises(NoSuchVersionException):
        lib.read(symA, as_of="s")
    assert lib.read(symB, as_of="s").data == 3


def test_snapshot_tombstoned_but_referenced_in_other_snapshot_version(lmdb_version_store_tombstone_and_pruning):
    lib = lmdb_version_store_tombstone_and_pruning
    symA = "A"
    symB = "B"

    symA_ver = lib.write(symA, 1).version
    symB_ver = lib.write(symB, 1).version
    lib.snapshot("s")
    lib.delete(symA)
    lib.delete(symB)
    lib.snapshot("s2", versions={symA:symA_ver, symB:symB_ver})
    assert lib.read(symA, as_of="s2").data == 1
    assert lib.read(symB, as_of="s2").data == 1

