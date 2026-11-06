"""
Copyright 2026 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, version 2.0.

Deletes must keep the data of every snapshotted version, but should only read the snapshotted index keys whose version
could hold data being deleted (VersionStore.LineagePruneDeletes).
"""

import os
import random
from collections import Counter

import numpy as np
import pandas as pd
import pytest

import arcticdb.toolbox.query_stats as qs
from arcticdb.util.test import assert_frame_equal, config_context, query_stats_operation_count
from arcticdb_ext.storage import KeyType

LINEAGE_PRUNE = "VersionStore.LineagePruneDeletes"


@pytest.fixture(params=[True, False], ids=["pruned", "full"])
def lineage_prune(request):
    with config_context(LINEAGE_PRUNE, int(request.param)):
        yield request.param


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
def test_append_on_older_version_after_snapshotted_one_is_deleted(make_lib, lineage_prune, storage):
    lib = make_lib(storage)
    exp = Expected(lib)
    lib.write("sym", _df(0))  # v0: d0
    lib.write("sym", _df(1))  # v1: d1
    exp.snapshot("snap_v1")
    lib.delete_version("sym", 1)
    lib.append("sym", _df(2))  # v2: (d0, d2)
    exp.snapshot("snap_v2")
    for v in (0, 2):
        exp.wrote("sym", v)

    lib.delete_version("sym", 0)
    exp.deleted_versions("sym", [0])
    exp.check()
    lib.delete("sym")
    exp.deleted_symbol("sym")
    exp.check()


def test_update_chain_with_snapshots(make_lib, lineage_prune):
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


def test_snapshot_dedup_chain(make_lib, lineage_prune):
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


def test_dedup_against_live_version(make_lib, lineage_prune):
    lib = make_lib(de_duplication=True, segment_row_size=2)
    exp = Expected(lib)
    df = _df(0, 6)
    lib.write("sym", df)  # v0
    lib.write("sym", _df(50, 6))  # v1
    exp.snapshot("s1")
    lib.delete_version("sym", 1)
    lib.write("sym", df)  # v2: dedups against v0
    exp.snapshot("s2")
    lib.write("sym", _df(100, 6))  # v3
    exp.wrote("sym", 3)
    lib.delete_versions("sym", [0, 2])
    exp.check()
    lib.delete("sym")
    exp.deleted_symbol("sym")
    exp.check()


def test_restore_version(make_lib, lineage_prune):
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


def test_symbol_recreated_after_delete(make_lib, lineage_prune):
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
def test_delete_runs_of_adjacent_versions(make_lib, lineage_prune, versions):
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


def test_missing_neighbour_is_not_used_to_skip_snapshotted_versions(make_lib, lineage_prune):
    lib = make_lib()
    exp = Expected(lib)
    lib.write("sym", _df(0))  # v0: d0
    lib.append("sym", _df(1))  # v1: (d0, d1)
    lib.append("sym", _df(2))  # v2: (d0, d1, d2)
    exp.snapshot("s2")
    lib.delete_version("sym", 2)
    lib.append("sym", _df(3))  # v3: (d0, d1, d3)
    # v1's index segment is lost, so v1 can't show that d0 is not needed by v2
    lib_tool = lib.library_tool()
    [v1_index] = [k for k in lib_tool.find_keys_for_id(KeyType.TABLE_INDEX, "sym") if k.version_id == 1]
    lib_tool.remove(v1_index)

    lib.delete_version("sym", 0)

    exp.check()


def test_recursively_normalized_symbol(make_lib, lineage_prune):
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


def test_delete_snapshot_mixed_live_and_tombstoned_targets(make_lib, lineage_prune):
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


@pytest.mark.parametrize("storage", ["mem", "lmdb"])
@pytest.mark.parametrize("delete_method", ["delete_version", "batch_delete_versions"])
def test_stale_cached_version_chain_does_not_skip_snapshotted_versions(
    in_memory_store_factory, version_store_factory, lineage_prune, storage, delete_method
):
    factory = in_memory_store_factory if storage == "mem" else version_store_factory
    kwargs = dict(name="stale_chain", use_tombstones=True, delayed_deletes=False)
    with config_context("VersionMap.ReloadInterval", 3_600_000_000_000):
        lib_a = factory(**kwargs)
        lib_a.write("sym", _df(0))  # v0: d0
        lib_a.write("sym", _df(1))  # v1: d1
        assert len(lib_a.list_versions("sym")) == 2  # lib_a caches the whole chain: v0 and v1 live

        lib_b = factory(reuse_name=True, **kwargs)
        lib_b.snapshot("snap_v1")  # keeps v1's index readable once v1 is deleted
        lib_b.delete_version("sym", 1)
        lib_b.append("sym", _df(2))  # v2: (d0, d2)
        lib_b.snapshot("snap_v2")
        lib_b.delete_version("sym", 2)

        # lib_a's cache still shows v1 live, but v2, built on v0, was written since and is snapshotted
        if delete_method == "delete_version":
            lib_a.delete_version("sym", 0)
        else:
            lib_a.batch_delete_versions(["sym"], [[0]])

        for lib in (lib_a, lib_b):
            assert_frame_equal(lib.read("sym", as_of="snap_v2").data, pd.concat([_df(0), _df(2)]))
            assert_frame_equal(lib.read("sym", as_of="snap_v1").data, _df(1))


def _is_live(lib, sym, version):
    return any(v["version"] == version and not v["deleted"] for v in lib.list_versions(sym, skip_snapshots=False))


# Request counts. LMDB is not instrumented, so these use the in-memory store.

E = 4  # unsnapshotted versions after the snapshotted ones


def _setup_snapshotted_history(lib, syms, n):
    """Versions 0..n-1 each followed by a snapshot of every symbol, then E unsnapshotted versions. Odd snapshotted
    versions are tombstoned."""
    for i in range(n):
        for sym in syms:
            lib.write(sym, _df(i, offset=i))
        lib.snapshot(f"snap{i}")
    for i in range(E):
        for sym in syms:
            lib.write(sym, _df(n + i, offset=n + i))
    odd = [v for v in range(n) if v % 2 == 1]
    lib.batch_delete_versions(syms, [odd] * len(syms))


def _index_reads(fn):
    qs.reset_stats()
    qs.enable()
    try:
        fn()
    finally:
        qs.disable()
    stats = qs.get_query_stats()
    qs.reset_stats()
    return query_stats_operation_count(stats, "Memory_GetObject", "TABLE_INDEX")


def _tombstoned_target(n):
    t = n // 2
    return t if t % 2 else t + 1


@pytest.mark.parametrize(
    "op, max_index_reads",
    [
        # target, prev and next. Master also read the nearest snapshotted versions.
        ("delete_version", 3),
        ("delete_versions", 4),
        ("remove_from_snapshot", 3),
        ("add_to_snapshot", 3),
        ("delete_snapshot", 3),
    ],
)
def test_single_symbol_index_reads(make_lib, clear_query_stats, op, max_index_reads):
    n = 50
    lib = make_lib()
    _setup_snapshotted_history(lib, ["sym"], n)
    t = _tombstoned_target(n)
    ops = {
        "delete_version": lambda: lib.delete_version("sym", n + 1),
        "delete_versions": lambda: lib.delete_versions("sym", [n, n + 1]),
        "remove_from_snapshot": lambda: lib.remove_from_snapshot(f"snap{t}", ["sym"], [t]),
        "add_to_snapshot": lambda: lib.add_to_snapshot(f"snap{t}", ["sym"], [n + 1]),
        "delete_snapshot": lambda: lib.delete_snapshot(f"snap{t}"),
    }
    reads = _index_reads(ops[op])
    assert reads <= max_index_reads
    lib.read("sym", as_of=f"snap{t - 1}")


def test_kill_switch_reads_every_snapshotted_index(make_lib, clear_query_stats):
    n = 20
    lib = make_lib()
    _setup_snapshotted_history(lib, ["sym"], n)
    with config_context(LINEAGE_PRUNE, 0):
        reads = _index_reads(lambda: lib.delete_version("sym", n + 1))
    assert reads >= n


@pytest.mark.parametrize("op", ["delete_snapshot_tombstoned", "delete_snapshot_live", "delete_snapshot_mixed"])
def test_many_symbols_delete_snapshot_index_reads(make_lib, clear_query_stats, op):
    n, m = 20, 20
    syms = [f"sym{j}" for j in range(m)]
    lib = make_lib()
    _setup_snapshotted_history(lib, syms, n)
    if op == "delete_snapshot_tombstoned":
        t = _tombstoned_target(n)
    elif op == "delete_snapshot_live":
        t = _tombstoned_target(n) - 1
    else:
        t = _tombstoned_target(n) + 1  # live in every symbol, so tombstone it in half of them
        lib.batch_delete_versions(syms[::2], [[t]] * len(syms[::2]))
    expected = {sym: lib.read(sym, as_of=f"snap{t + 1}").data for sym in syms}

    reads = _index_reads(lambda: lib.delete_snapshot(f"snap{t}"))

    assert reads <= 3 * m
    for sym in syms:
        assert_frame_equal(lib.read(sym, as_of=f"snap{t + 1}").data, expected[sym])


def test_many_symbols_batch_delete_versions_index_reads(make_lib, clear_query_stats):
    n, m = 20, 20
    syms = [f"sym{j}" for j in range(m)]
    lib = make_lib()
    _setup_snapshotted_history(lib, syms, n)
    batch = syms[:10]
    reads = _index_reads(lambda: lib.batch_delete_versions(batch, [list(range(n, n + E - 1))] * len(batch)))
    # Each symbol: E - 1 targets, the previous live version and the next version
    assert reads <= (E + 1) * len(batch)


# Differential test: random histories, run in lockstep on two libraries, one pruning protected index reads and one
# reading them all. Both must delete exactly the same keys, and every snapshot and live version must read back.

FUZZ_HISTORIES = int(os.getenv("ARCTICDB_LINEAGE_FUZZ_HISTORIES", "12"))
FUZZ_STEPS = int(os.getenv("ARCTICDB_LINEAGE_FUZZ_STEPS", "40"))
FUZZ_SEED = int(os.getenv("ARCTICDB_LINEAGE_FUZZ_SEED", "20261105"))


def _key_counts(lib, syms):
    lib_tool = lib.library_tool()
    out = Counter()
    for sym in syms:
        for k in lib_tool.find_keys_for_id(KeyType.TABLE_DATA, sym):
            out[("data", sym, k.version_id, k.content_hash, k.start_index, k.end_index)] += 1
        for k in lib_tool.find_keys_for_id(KeyType.TABLE_INDEX, sym):
            out[("index", sym, k.version_id)] += 1
    return out


class _Lockstep:
    def __init__(self, pruned, full, rng):
        self.libs = (pruned, full)
        self.rng = rng
        self.exp = Expected(pruned)
        self.full_exp = Expected(full)
        self.live = {}
        self.syms = ["a", "b"]
        self.snap_count = 0
        self.append_day = 1000
        self.ops = []

    def both(self, fn):
        with config_context(LINEAGE_PRUNE, 1):
            res = fn(self.libs[0])
        with config_context(LINEAGE_PRUNE, 0):
            fn(self.libs[1])
        return res

    def wrote(self, sym, vit):
        self.live.setdefault(sym, set()).add(vit.version)
        self.exp.wrote(sym, vit.version)
        self.full_exp.wrote(sym, vit.version)

    def frame(self, sym):
        pool = self.rng.randrange(6)
        return _df(10 * pool, self.rng.randint(1, 5), offset=pool)

    def step(self):
        rng = self.rng
        sym = rng.choice(self.syms)
        live = sorted(self.live.get(sym, ()))
        op = rng.choice(
            [
                "write", "write", "append", "update", "restore", "prune_write",
                "snapshot", "snapshot", "delete_versions", "delete_versions", "batch_delete_versions",
                "delete", "delete_snapshot", "remove_from_snapshot", "add_to_snapshot",
            ]
        )  # fmt: skip
        self.ops.append((op, sym, live))
        if op == "write":
            df = self.frame(sym)
            self.wrote(sym, self.both(lambda lib: lib.write(sym, df)))
        elif op == "prune_write" and live:
            df = self.frame(sym)
            self.wrote(sym, self.both(lambda lib: lib.write(sym, df, prune_previous_version=True)))
            pruned = set(live)
            self.live[sym] -= pruned
            for e in (self.exp, self.full_exp):
                e.deleted_versions(sym, pruned)
        elif op == "append" and live:
            df = _df(self.append_day, rng.randint(1, 3))
            self.append_day += 10
            self.wrote(sym, self.both(lambda lib: lib.append(sym, df)))
        elif op == "update" and live:
            df = _df(10 * rng.randrange(6), rng.randint(1, 3), offset=500)
            self.wrote(sym, self.both(lambda lib: lib.update(sym, df)))
        elif op == "restore" and live and len(live) > 1:
            as_of = rng.choice(live[:-1])
            self.wrote(sym, self.both(lambda lib: lib.restore_version(sym, as_of)))
        elif op == "snapshot" and any(self.live.values()):
            name = f"snap{self.snap_count}"
            self.snap_count += 1
            self.exp.snapshot(name)
            self.full_exp.snapshot(name)
        elif op == "delete_versions" and live:
            vs = rng.sample(live, rng.randint(1, min(3, len(live))))
            self.both(lambda lib: lib.delete_versions(sym, vs))
            self._deleted(sym, vs)
        elif op == "batch_delete_versions":
            syms, vss = [], []
            for s in self.syms:
                lv = sorted(self.live.get(s, ()))
                if lv:
                    syms.append(s)
                    vss.append(rng.sample(lv, rng.randint(1, min(2, len(lv)))))
            if syms:
                self.both(lambda lib: lib.batch_delete_versions(syms, vss))
                for s, vs in zip(syms, vss):
                    self._deleted(s, vs)
        elif op == "delete" and live:
            self.both(lambda lib: lib.delete(sym))
            self._deleted(sym, live)
        elif op in ("delete_snapshot", "remove_from_snapshot", "add_to_snapshot") and self.exp.snapshots:
            name = rng.choice(sorted(self.exp.snapshots))
            if op == "delete_snapshot":
                self.both(lambda lib: lib.delete_snapshot(name))
                for e in (self.exp, self.full_exp):
                    del e.snapshots[name]
            elif op == "remove_from_snapshot":
                content = self.exp.snapshots[name]
                # Emptying a snapshot leaves it unrewritten, still naming the deleted version
                if sym in content and len(content) > 1:
                    v = self._snapshot_version(name, sym)
                    self.both(lambda lib: lib.remove_from_snapshot(name, [sym], [v]))
                    for e in (self.exp, self.full_exp):
                        del e.snapshots[name][sym]
            elif live:
                v = rng.choice(live)
                self.both(lambda lib: lib.add_to_snapshot(name, [sym], [v]))
                for e in (self.exp, self.full_exp):
                    e.snapshots[name][sym] = e.live[(sym, v)]

    def _snapshot_version(self, name, sym):
        [entry] = [v for v in self.libs[0].list_versions(sym, snapshot=name)]
        return entry["version"]

    def _deleted(self, sym, versions):
        self.live[sym] -= set(versions)
        for e in (self.exp, self.full_exp):
            e.deleted_versions(sym, versions)

    def check(self):
        self.exp.check()
        self.full_exp.check()
        assert _key_counts(self.libs[0], self.syms) == _key_counts(self.libs[1], self.syms)


@pytest.mark.parametrize("history", range(FUZZ_HISTORIES))
def test_differential_random_histories(in_memory_store_factory, history):
    kwargs = dict(use_tombstones=True, delayed_deletes=False, de_duplication=True, snapshot_dedup=True)
    kwargs["segment_row_size"] = 2
    pruned = in_memory_store_factory(name=f"pruned_{history}", **kwargs)
    full = in_memory_store_factory(name=f"full_{history}", **kwargs)
    runner = _Lockstep(pruned, full, random.Random(FUZZ_SEED + history))
    for _ in range(FUZZ_STEPS):
        try:
            runner.step()
            runner.check()
        except Exception:
            print("\n".join(map(str, runner.ops)))
            raise
