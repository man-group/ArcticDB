"""
Copyright 2023 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

from collections import defaultdict
from typing import Dict, List, Set, Any, Optional, Tuple
from pandas import DataFrame
from enum import Enum, IntFlag
from itertools import zip_longest
import attr
import os
import pytest

from hypothesis import strategies as st, assume, stateful, settings, HealthCheck
from hypothesis.stateful import RuleBasedStateMachine, Bundle, rule, invariant, run_state_machine_as_test

from arcticdb.version_store import NativeVersionStore
from arcticdb.util.test import assert_frame_equal

from tests.util.mark import SLOW_TESTS_MARK

# Patches future hypothesis features
consumes = getattr(stateful, "consumes", lambda x: x)  # consumes() first introduced in hypothesis 3.85


class State(Enum):
    NORMAL = "N"
    TOMBSTONED = "T"
    DELETED = "D"


@attr.s(auto_attribs=True, hash=True)
class Snapshot:
    name: str
    serial: int  # In case snapshots of the same name were deleted & re-added
    metadata: Any = attr.ib(cmp=False)
    sym_vers: Dict[str, int] = attr.ib(cmp=False)  # None = deleted
    state: State = attr.ib(cmp=False, default=State.NORMAL)

    def __str__(self):
        return (
            f"{self.name}@{self.serial}={self.sym_vers if self.state == State.NORMAL else self.state.value}"
            f"{'[M]' if self.metadata else ''}"
        )


@attr.s(auto_attribs=True)
class Version:
    data: Optional[DataFrame]
    metadata: Any
    state: State = State.NORMAL
    snaps: Set[Snapshot] = attr.Factory(set)

    def __str__(self):
        return (
            f"{self.data and self.data.iloc[0, 0]}[{'M' if self.metadata else ''}{self.state.value}]{self.snaps or ''}"
        )

    def can_delete(self):
        return self.state == State.TOMBSTONED and not any(s.state == State.NORMAL for s in self.snaps)

    def active_snap_names(self):
        return {s.name for s in self.snaps if s.state == State.NORMAL}


class WriteMode(IntFlag):
    DATA = 1
    META = 2
    BOTH = 3

    def __repr__(self):
        if self._name_ is not None:
            return "WriteMode." + self._name_
        return super().__repr__()


class VersionStoreComparison(RuleBasedStateMachine):
    _lib: NativeVersionStore
    _visible_symbols: Set[str]
    _versions: Dict[str, List[Version]]  # Symbol -> List of Versions
    _visible_snapshots: Dict[str, Snapshot]
    _snapshot_tombstones: Set[Snapshot]
    _serial: int = 0

    symbols = Bundle("symbols")
    snaps = Bundle("snaps")

    def __init__(self):
        super().__init__()
        self._visible_symbols = set()
        self._versions = defaultdict(list)
        self._visible_snapshots = {}
        self._snapshot_tombstones = set()

        self._lib.version_store.clear()
        self._lib.version_store.flush_version_map()

    # ================================ Basic version ops ================================

    def _prune_previous_versions(self, sym):
        vers = self._versions[sym]
        for value in vers:
            if value.state == State.NORMAL:
                value.state = State.TOMBSTONED  # Delayed deletes

    def _get_latest_undeleted_version(self, sym) -> Tuple[Optional[int], Optional[Version]]:
        vers = self._versions[sym]
        for ver_num in reversed(range(len(vers))):
            ver = vers[ver_num]
            if ver.state == State.NORMAL:
                return ver_num, ver
        return None, None

    @rule(
        sym=symbols,
        prune=st.sampled_from([True, False]),  # booleans() shrinks towards False
        write_mode=st.sampled_from(WriteMode),
    )
    def write_new_version_to_symbol(self, sym: str, prune: bool, write_mode: WriteMode):
        assume(sym in self._visible_symbols)

        meta = {"meta for": self._serial} if WriteMode.META in write_mode else None
        if write_mode == WriteMode.META:
            last = self._get_latest_undeleted_version(sym)[1]
            data = last and last.data
            created = self._lib.write_metadata(sym, meta, prune_previous_version=prune)
        else:
            data = DataFrame({"x": [self._serial]}, index=[0])
            data.index.name = "has_name"
            created = self._lib.write(sym, data, meta, prune_previous_version=prune)

        self._serial += 1
        if prune:
            self._prune_previous_versions(sym)
        vers = self._versions[sym]
        assert created.symbol == sym
        assert created.version == len(vers)
        vers.append(Version(data, meta))

    @rule(
        target=symbols,
        sym=st.sampled_from(["a" * 5, "b" * 100, *(chr(i) for i in range(32, 127) if chr(i) not in "*&<>")]),
        write_mode=st.sampled_from(WriteMode),
    )
    def write_new_symbol(self, sym: str, write_mode: WriteMode):
        assume(sym not in self._visible_symbols)  # Don't add duplicates to the bundle
        self._visible_symbols.add(sym)
        self.write_new_version_to_symbol(sym, True, write_mode)
        return sym

    @rule(sym=consumes(symbols))
    def delete_symbol(self, sym):
        assume(sym in self._visible_symbols)  # Older hypothesis don't have `consume()`
        self._lib.delete(sym)
        self._prune_previous_versions(sym)
        self._visible_symbols.remove(sym)

    @invariant()
    def test_list_versions_all_and_read(self):
        """lib.list_versions() with args"""
        expected_vers = {}
        for sym, versions in self._versions.items():
            # AN implementation first calls list_streams() and then resolve the available versions on those, which is
            # different from Python Arctic:
            last = versions[-1]
            if last and last.state == State.NORMAL:
                for ver_num, ver in enumerate(versions):
                    if not (ver.state == State.DELETED or ver.can_delete()):
                        expected_vers[(sym, ver_num)] = ver

        actual_vers = self._lib.list_versions()
        for actual in actual_vers:
            sym_ver = actual["symbol"], actual["version"]
            expected = expected_vers.pop(sym_ver)
            assert actual["deleted"] is (expected.state == State.TOMBSTONED)
            assert set(actual["snapshots"]) == expected.active_snap_names()

            actual_meta = self._lib.read_metadata(*sym_ver)
            assert (actual_meta.symbol, actual_meta.version) == sym_ver
            assert actual_meta.metadata == expected.metadata

            if expected.data is not None:
                read = self._lib.read(*sym_ver, columns=["x"])
                assert_frame_equal(read.data, expected.data)
            else:
                read = self._lib.read(*sym_ver)
                assert read.data is None
            assert read.metadata == expected.metadata

        assert len(expected_vers) == 0, "list_versions() failed to find these"

    def _check_batch_read(self, syms: List[str], vers: Optional[List[int]]):
        meta = self._lib.batch_read_metadata(syms, as_ofs=vers)
        data = self._lib.batch_read(syms, as_ofs=vers)
        for sym, ver in zip_longest(syms, vers or (), fillvalue=-1):
            vers = self._versions[sym]
            expected = vers[ver]

            for vi_map in (meta, data):
                actual = vi_map[sym]
                assert actual.symbol == sym
                assert actual.version == ver if ver != -1 else len(vers)
                assert actual.metadata == expected.metadata

            if expected.data is None:
                assert data[sym].data is None
            else:
                assert_frame_equal(data[sym].data, expected.data)

    @invariant()
    def test_batch_read_latest(self):
        self._check_batch_read(list(self._visible_symbols), None)

    @invariant()
    def test_list_versions_latest_only(self):
        expected = {sym: self._get_latest_undeleted_version(sym)[0] for sym in self._visible_symbols}

        actual = self._lib.list_versions(latest_only=True)
        for info in actual:
            expected_ver = expected.pop(info["symbol"])
            assert info["version"] == expected_ver

        assert expected == {}

    @invariant()
    def test_list_symbols_with_cache(self):
        assert set(self._lib.list_symbols(use_symbol_list=True)) == self._visible_symbols

    @invariant()
    def test_list_symbols_without_cache(self):
        assert set(self._lib.list_symbols(use_symbol_list=False)) == self._visible_symbols

    # ================================ Snapshot ops ================================

    @rule(target=snaps, name=st.sampled_from([f"s{i}" for i in range(20)]), with_meta=st.booleans())
    def snapshot(self, name: str, with_meta: bool):
        meta = {"meta for snap": name} if with_meta else None
        if name in self._visible_snapshots:  # Current snapshot() impl just logs an error instead of throwing
            return None
        if len(self._visible_symbols) == 0:
            return None

        self._lib.snapshot(name, meta)
        snap = Snapshot(name, self._serial, meta, {})
        self._serial += 1
        for sym in self._visible_symbols:
            vers = self._versions[sym]
            snap.sym_vers[sym] = len(vers) - 1
            vers[-1].snaps.add(snap)

        self._visible_snapshots[name] = snap
        return name

    @rule(name=consumes(snaps))
    def delete_snapshot(self, name: str):
        snap = self._visible_snapshots.pop(name, None)
        assume(snap)
        snap.state = State.TOMBSTONED  # Delayed deletes
        self._snapshot_tombstones.add(snap)
        self._lib.delete_snapshot(name)

    @invariant()
    def test_list_snapshots(self):
        assert self._lib.list_snapshots() == {k: v.metadata for k, v in self._visible_snapshots.items()}

    @invariant()
    def test_list_symbols_snapshot(self):
        for name, snap in self._visible_snapshots.items():
            expected_symbols = snap.sym_vers.keys()
            assert set(self._lib.list_symbols(snapshot=name, use_symbol_list=False)) == expected_symbols

    @invariant()
    def test_list_versions_snapshot(self, latest_only=False):
        for name, snap in self._visible_snapshots.items():
            actual = self._lib.list_versions(snapshot=name, latest_only=latest_only)
            actual_map = {v["symbol"]: v["version"] for v in actual}
            assert actual_map == snap.sym_vers

    @invariant()
    def test_list_versions_latest_only_snapshot(self):
        self.test_list_versions_snapshot(latest_only=True)


@pytest.mark.parametrize("lib_type", ["lmdb_version_store_delayed_deletes_v1", "lmdb_version_store_delayed_deletes_v2"])
@SLOW_TESTS_MARK
def test_stateful(lib_type, request):
    VersionStoreComparison._lib = request.getfixturevalue(lib_type)
    run_state_machine_as_test(
        VersionStoreComparison,
        settings=settings(  # Note: timeout is a legacy parameter
            max_examples=int(os.getenv("HYPOTHESIS_EXAMPLES", 100)),
            deadline=None,
            stateful_step_count=100,
            suppress_health_check=[HealthCheck.filter_too_much],
        ),
    )


def test_single(lmdb_version_store_delayed_deletes_v1):
    VersionStoreComparison._lib = lmdb_version_store_delayed_deletes_v1
    state = VersionStoreComparison()
    # Copy and paste the reproduction script hypothesis generated below:
    # print("Press enter to continue"); import sys; sys.stdin.readline()
    state.write_new_symbol(sym="0", write_mode=WriteMode.DATA)
    state.write_new_version_to_symbol(prune=True, sym="0", write_mode=WriteMode.META)
    state.snapshot(name="0", with_meta=False)
    state.delete_snapshot(name="0")
    state.write_new_version_to_symbol(prune=True, sym="0", write_mode=WriteMode.DATA)

    state.test_list_versions_all_and_read()
