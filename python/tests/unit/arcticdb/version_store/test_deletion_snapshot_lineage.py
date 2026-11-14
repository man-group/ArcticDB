"""
Copyright 2026 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, version 2.0.

Deletes must keep the data of every snapshotted version, whichever versions it was shared with.
"""

import numpy as np
import pandas as pd
import pytest

from arcticdb.util.test import assert_frame_equal
from arcticdb_ext.storage import KeyType


def _df(start, n=1, offset=0):
    return pd.DataFrame(
        {"col": np.arange(start, start + n, dtype=np.int64) + offset},
        index=pd.date_range(pd.Timestamp("2020-01-01") + pd.Timedelta(days=start), periods=n),
    )


class Expected:
    """What each snapshot and each live version must read back as."""

    def __init__(self, lib):
        self.lib = lib
        self.snapshots = {}
        self.live = {}

    def wrote(self, sym, version):
        self.live[(sym, version)] = self.lib.read(sym, as_of=version).data

    def snapshot(self, name, versions=None):
        self.lib.snapshot(name, versions=versions)
        content = {}
        for sym in self.lib.list_symbols():
            if versions is not None and sym not in versions:
                continue
            content[sym] = self.lib.read(sym).data if versions is None else self.lib.read(sym, as_of=versions[sym]).data
        self.snapshots[name] = content

    def deleted_versions(self, sym, versions):
        for v in versions:
            self.live.pop((sym, v), None)

    def deleted_symbol(self, sym):
        for key in [k for k in self.live if k[0] == sym]:
            del self.live[key]

    def check(self):
        assert set(self.lib.list_snapshots()) == set(self.snapshots)
        for name, content in self.snapshots.items():
            for sym, df in content.items():
                assert_frame_equal(self.lib.read(sym, as_of=name).data, df)
        for (sym, v), df in self.live.items():
            assert_frame_equal(self.lib.read(sym, as_of=v).data, df)


@pytest.fixture
def make_lib(in_memory_store_factory, version_store_factory):
    def make(storage="mem", **kwargs):
        factory = in_memory_store_factory if storage == "mem" else version_store_factory
        return factory(use_tombstones=True, delayed_deletes=False, **kwargs)

    return make


@pytest.mark.parametrize("storage", ["mem", "lmdb"])
@pytest.mark.parametrize(
    "delete_method", ["delete_version_then_delete", "delete", "batch_delete_versions", "batch_delete_symbols"]
)
def test_append_on_older_version_after_snapshotted_one_is_deleted(make_lib, storage, delete_method):
    lib = make_lib(storage)
    exp = Expected(lib)
    lib.write("sym", _df(0))  # v0: d0
    lib.write("sym", _df(1))  # v1: d1
    exp.snapshot("snap_v1")
    lib.delete_version("sym", 1)
    lib.append("sym", _df(2))  # v2: (d0, d2)
    exp.snapshot("snap_v2")

    # Must keep d0 for snap_v2, though v0's nearest snapshotted version is v1
    if delete_method == "delete_version_then_delete":
        lib.delete_version("sym", 0)
        exp.check()
        lib.delete("sym")
    elif delete_method == "delete":
        lib.delete("sym")
    elif delete_method == "batch_delete_versions":
        lib.batch_delete_versions(["sym"], [[0, 2]])
    else:
        lib.batch_delete_symbols(["sym"])
    exp.check()


def test_update_chain_with_snapshots(make_lib):
    lib = make_lib(segment_row_size=2)
    exp = Expected(lib)
    lib.write("sym", _df(0, 10))
    exp.snapshot("s0")
    for i in range(1, 6):
        lib.update("sym", _df(2 * i - 1, 2, offset=100 * i))
        if i % 2:
            exp.snapshot(f"s{i}")
        exp.wrote("sym", i)
    lib.delete_versions("sym", [1, 2, 4])
    exp.deleted_versions("sym", [1, 2, 4])
    exp.check()
    lib.delete_snapshot("s1")
    del exp.snapshots["s1"]
    exp.check()
    lib.delete("sym")
    exp.deleted_symbol("sym")
    exp.check()
    for name in list(exp.snapshots):
        lib.delete_snapshot(name)
        del exp.snapshots[name]
        exp.check()
    lib_tool = lib.library_tool()
    assert not lib_tool.find_keys_for_id(KeyType.TABLE_DATA, "sym")


def test_snapshot_dedup_chain(make_lib):
    # Snapshot dedup shares v0's data with v1 though v0 is deleted, so deleting snapshot A must read B's v1
    lib = make_lib(de_duplication=True, snapshot_dedup=True, segment_row_size=2)
    exp = Expected(lib)
    df = _df(0, 6)
    lib.write("sym", df)  # v0
    exp.snapshot("A")
    lib.delete_version("sym", 0)
    lib.write("sym", df)  # v1: dedups against snapshot A's v0
    exp.snapshot("B")
    lib.delete_version("sym", 1)
    lib.write("sym", _df(100, 6))  # v2: fresh
    exp.wrote("sym", 2)
    data_keys = lib.library_tool().find_keys_for_id(KeyType.TABLE_DATA, "sym")
    assert {k.version_id for k in data_keys} == {0, 2}

    lib.delete_snapshot("A")
    del exp.snapshots["A"]
    exp.check()


@pytest.mark.parametrize("delete_method", ["delete_versions_then_delete", "delete"])
def test_dedup_against_live_version(make_lib, delete_method):
    lib = make_lib(de_duplication=True, segment_row_size=2)
    exp = Expected(lib)
    df = _df(0, 6)
    lib.write("sym", df)  # v0
    lib.write("sym", _df(50, 6))  # v1
    exp.snapshot("s1")
    lib.delete_version("sym", 1)
    lib.write("sym", df)  # v2: dedups against v0, though v1 is the nearest snapshotted version
    exp.snapshot("s2")
    if delete_method == "delete_versions_then_delete":
        lib.write("sym", _df(100, 6))  # v3
        exp.wrote("sym", 3)
        lib.delete_versions("sym", [0, 2])
        exp.check()
    lib.delete("sym")
    exp.deleted_symbol("sym")
    exp.check()


def test_restore_version(make_lib):
    lib = make_lib()
    exp = Expected(lib)
    lib.write("sym", _df(0))  # v0
    exp.snapshot("s0")
    lib.append("sym", _df(1))  # v1
    exp.snapshot("s1")
    lib.restore_version("sym", 0)  # v2: copies v0's data
    exp.snapshot("s2")
    lib.append("sym", _df(5))  # v3: built on v2
    exp.wrote("sym", 3)
    lib.delete_versions("sym", [0, 1, 2])
    exp.check()
    for name in ["s0", "s2", "s1"]:
        lib.delete_snapshot(name)
        del exp.snapshots[name]
        exp.check()


def test_symbol_recreated_after_delete(make_lib):
    lib = make_lib(de_duplication=True, snapshot_dedup=True, segment_row_size=2)
    exp = Expected(lib)
    df = _df(0, 4)
    lib.write("sym", df)  # v0
    exp.snapshot("first")
    lib.delete("sym")
    lib.write("sym", df)  # v1: snapshot dedup against v0
    exp.snapshot("second")
    lib.append("sym", _df(10, 2))  # v2
    exp.wrote("sym", 2)
    lib.delete_snapshot("first")
    del exp.snapshots["first"]
    exp.check()
    lib.delete("sym")
    exp.deleted_symbol("sym")
    exp.check()


@pytest.mark.parametrize("versions", [[1, 2], [1, 2, 3], [0, 1, 3, 4], [2, 3, 5, 6]])
def test_delete_runs_of_adjacent_versions(make_lib, versions):
    lib = make_lib()
    exp = Expected(lib)
    lib.write("sym", _df(0))
    for i in range(1, 8):
        lib.append("sym", _df(i))
        if i % 3 == 0:
            exp.snapshot(f"s{i}")
    for v in range(8):
        exp.wrote("sym", v)
    lib.delete_versions("sym", versions)
    exp.deleted_versions("sym", versions)
    exp.check()
    lib.delete("sym")
    exp.deleted_symbol("sym")
    exp.check()


def test_recursively_normalized_symbol(make_lib):
    lib = make_lib(recursive_normalizers=True)
    lib.write("sym", {"a": _df(0), "b": _df(1)})  # v0
    lib.snapshot("s0")
    lib.write("sym", {"a": _df(2), "b": _df(3)})  # v1
    lib.snapshot("s1")
    lib.write("sym", {"a": _df(4), "b": _df(5)})  # v2
    lib.delete_versions("sym", [0, 1])
    lib.delete_snapshot("s0")
    assert set(lib.read("sym", as_of="s1").data) == {"a", "b"}
    assert_frame_equal(lib.read("sym", as_of="s1").data["a"], _df(2))
    lib.delete("sym")
    assert_frame_equal(lib.read("sym", as_of="s1").data["b"], _df(3))


def test_delete_snapshot_mixed_live_and_tombstoned_targets(make_lib):
    lib = make_lib(de_duplication=True, segment_row_size=2)
    exp = Expected(lib)
    syms = [f"sym{i}" for i in range(12)]
    for i, sym in enumerate(syms):
        lib.write(sym, _df(0, 4, offset=i))  # v0
        lib.append(sym, _df(4, 2, offset=i))  # v1: shares v0's data
    exp.snapshot("mid")
    for i, sym in enumerate(syms):
        lib.append(sym, _df(6, 2, offset=i))  # v2
    exp.snapshot("later")
    for i, sym in enumerate(syms):
        kind = i % 4
        if kind == 0:  # target v1 tombstoned, shares data with live v0 and snapshotted v2
            lib.delete_version(sym, 1)
        elif kind == 1:  # target live
            pass
        elif kind == 2:  # everything tombstoned
            lib.delete(sym)
        else:  # target tombstoned, later snapshot's v2 tombstoned too, v0 live
            lib.delete_versions(sym, [1, 2])
    exp.live = {}
    for sym in syms:
        for v in range(3):
            if _is_live(lib, sym, v):
                exp.wrote(sym, v)

    lib.delete_snapshot("mid")
    del exp.snapshots["mid"]
    exp.check()
    lib.delete_snapshot("later")
    del exp.snapshots["later"]
    exp.check()


def test_delete_version_keeps_data_shared_with_snapshot_below_nearest_snapshotted_version(make_lib):
    lib = make_lib("lmdb")
    sym = "sym"
    lib.write(sym, _df(0))  # v0: data d0
    lib.snapshot("snap_v0")
    lib.write(sym, _df(1))  # v1: data d1, shares nothing with v0
    lib.snapshot("snap_v1")
    lib.delete_version(sym, 1)
    lib.append(sym, _df(2))  # v2: built on v0, so (d0, d2)
    lib.delete_version(sym, 0)

    # Must keep d0 for snap_v0, though v2's nearest snapshotted version is v1
    lib.delete_version(sym, 2)

    assert_frame_equal(lib.read(sym, as_of="snap_v0").data, _df(0))
    assert_frame_equal(lib.read(sym, as_of="snap_v1").data, _df(1))


@pytest.mark.parametrize("snapshot_method", ["delete_snapshot", "remove_from_snapshot"])
def test_delete_snapshot_keeps_data_shared_with_snapshot_below_nearest_snapshotted_version(make_lib, snapshot_method):
    lib = make_lib("lmdb")
    sym = "sym"
    lib.write(sym, _df(0))  # v0: data d0
    lib.snapshot("snap_v0")
    lib.write(sym, _df(1))  # v1: data d1, shares nothing with v0
    lib.snapshot("snap_v1")
    lib.delete_version(sym, 1)
    lib.append(sym, _df(2))  # v2: built on v0, so (d0, d2)
    lib.snapshot("snap_v2")
    lib.delete_version(sym, 2)
    lib.delete_version(sym, 0)

    # Must keep d0 for snap_v0, though v2's nearest snapshotted version is v1
    if snapshot_method == "delete_snapshot":
        lib.delete_snapshot("snap_v2")
    else:
        lib.remove_from_snapshot("snap_v2", [sym], [2])

    assert_frame_equal(lib.read(sym, as_of="snap_v0").data, _df(0))
    assert_frame_equal(lib.read(sym, as_of="snap_v1").data, _df(1))


def _is_live(lib, sym, version):
    return any(v["version"] == version and not v["deleted"] for v in lib.list_versions(sym, skip_snapshots=False))
