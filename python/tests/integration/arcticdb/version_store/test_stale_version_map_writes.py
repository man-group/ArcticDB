"""
Copyright 2026 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, version 2.0.

Sequential writes to one symbol from two Library instances. B has written or read the symbol, so its version map
caches the symbol for VersionMap.ReloadInterval; A then writes a new version; then B writes again. B must build on A's
version, not on the one it cached.
"""

import multiprocessing

import pandas as pd
import pytest

import arcticdb.toolbox.query_stats as qs
from arcticdb import Arctic, UpdatePayload, WritePayload
from arcticdb.util.test import config_context, query_stats_operation_count
from arcticdb.version_store.library import WriteMetadataPayload

# Longer than any test, so that B's cached entry is always younger than the reload interval
FRESH = 3_600_000_000_000
T = [pd.Timestamp("2024-01-01") + pd.Timedelta(i, "s") for i in range(4)]


def _row(i, x):
    return pd.DataFrame({"x": [float(x)]}, index=pd.DatetimeIndex([T[i]]))


def _run(lib, op, sym):
    if op == "write0":
        return lib.write(sym, _row(0, 0)).version
    if op == "append1":
        return lib.append(sym, _row(1, 1)).version
    if op == "write1":
        return lib.write(sym, _row(1, 1)).version
    if op == "read":
        return lib.read(sym).version
    if op == "write":
        return lib.write(sym, _row(2, 2)).version
    if op == "append":
        return lib.append(sym, _row(2, 2)).version
    if op == "update":
        return lib.update(sym, _row(0, 10)).version
    if op == "delete_data_in_range":
        lib.delete_data_in_range(sym, (T[0], T[0]))
        return None
    if op == "write_metadata":
        return lib.write_metadata(sym, {"m": 1}).version
    if op == "write_batch":
        return lib.write_batch([WritePayload(sym, _row(2, 2))])[0].version
    if op == "append_batch":
        return lib.append_batch([WritePayload(sym, _row(2, 2))])[0].version
    if op == "update_batch":
        return lib.update_batch([UpdatePayload(sym, _row(0, 10))])[0].version
    if op == "write_metadata_batch":
        return lib.write_metadata_batch([WriteMetadataPayload(sym, {"m": 1})])[0].version
    if op == "write_prune":
        return lib.write(sym, _row(2, 2), prune_previous_versions=True).version
    if op == "append_prune":
        return lib.append(sym, _row(2, 2), prune_previous_versions=True).version
    if op == "list_symbols":
        return lib.list_symbols()
    if op == "snapshot":
        lib.snapshot(f"snap_{sym}")
        return None
    if op == "delete":
        lib.delete(sym)
        return None
    if op == "delete_batch":
        lib.delete_batch([sym])
        return None
    if op == "delete_v0":
        lib.delete(sym, versions=[0])
        return None
    raise ValueError(op)


# name: (steps as (actor, op), expected versions, expected x of the latest version)
SCENARIOS = {
    "write": ([("B", "write0"), ("A", "append1"), ("B", "write")], [0, 1, 2], [2.0]),
    "append": ([("B", "write0"), ("A", "append1"), ("B", "append")], [0, 1, 2], [0.0, 1.0, 2.0]),
    "update": ([("B", "write0"), ("A", "append1"), ("B", "update")], [0, 1, 2], [10.0, 1.0]),
    "delete_data_in_range": ([("B", "write0"), ("A", "append1"), ("B", "delete_data_in_range")], [0, 1, 2], [1.0]),
    "write_metadata": ([("B", "write0"), ("A", "append1"), ("B", "write_metadata")], [0, 1, 2], [0.0, 1.0]),
    "write_batch": ([("B", "write0"), ("A", "append1"), ("B", "write_batch")], [0, 1, 2], [2.0]),
    "append_batch": ([("B", "write0"), ("A", "append1"), ("B", "append_batch")], [0, 1, 2], [0.0, 1.0, 2.0]),
    "update_batch": ([("B", "write0"), ("A", "append1"), ("B", "update_batch")], [0, 1, 2], [10.0, 1.0]),
    "write_metadata_batch": (
        [("B", "write0"), ("A", "append1"), ("B", "write_metadata_batch")],
        [0, 1, 2],
        [0.0, 1.0],
    ),
    "write_prune": ([("B", "write0"), ("A", "append1"), ("B", "write_prune")], [2], [2.0]),
    "append_prune": ([("B", "write0"), ("A", "append1"), ("B", "append_prune")], [2], [0.0, 1.0, 2.0]),
    "read_then_append": (
        [("A", "write0"), ("B", "read"), ("A", "append1"), ("B", "append")],
        [0, 1, 2],
        [0.0, 1.0, 2.0],
    ),
    "delete": ([("B", "write0"), ("A", "append1"), ("B", "delete")], [], None),
    "delete_batch": ([("B", "write0"), ("A", "append1"), ("B", "delete_batch")], [], None),
    "delete_v0": ([("B", "write0"), ("A", "append1"), ("B", "delete_v0")], [1], [0.0, 1.0]),
}


def _two_libraries(arctic, lib_name):
    arctic.create_library(lib_name)
    return arctic.get_library(lib_name), arctic.get_library(lib_name)


def _check(checker, sym, expected_versions, expected_x):
    checker._nvs.version_store.flush_version_map()
    assert sorted(v.version for v in checker.list_versions(sym)) == expected_versions
    if expected_x is not None:
        assert checker.read(sym).data["x"].tolist() == expected_x


@pytest.mark.parametrize("scenario", list(SCENARIOS))
@pytest.mark.parametrize("storage", ["lmdb", "mem"])
def test_write_with_fresh_cache_builds_on_another_librarys_version(request, lib_name, storage, scenario):
    lib_a, lib_b = _two_libraries(request.getfixturevalue(f"{storage}_storage").create_arctic(), lib_name)
    steps, expected_versions, expected_x = SCENARIOS[scenario]
    with config_context("VersionMap.ReloadInterval", FRESH):
        for actor, op in steps:
            _run(lib_a if actor == "A" else lib_b, op, "sym")
    _check(lib_a, "sym", expected_versions, expected_x)


@pytest.mark.parametrize("compact_symbol_list", [False, True])
def test_snapshot_with_fresh_cache_pins_another_librarys_version(mem_storage, lib_name, compact_symbol_list):
    lib_a, lib_b = _two_libraries(mem_storage.create_arctic(), lib_name)
    with config_context("VersionMap.ReloadInterval", FRESH):
        if compact_symbol_list:
            lib_a.list_symbols()
        lib_b.write("sym", _row(0, 0))
        lib_a.append("sym", _row(1, 1))
        lib_b.snapshot("snap")
    lib_a._nvs.version_store.flush_version_map()
    assert lib_a.read("sym", as_of="snap").version == 1


def test_snapshot_with_fresh_cache_sees_a_symbol_another_library_recreated(mem_storage, lib_name):
    lib_a, lib_b = _two_libraries(mem_storage.create_arctic(), lib_name)
    with config_context("VersionMap.ReloadInterval", FRESH):
        lib_a.list_symbols()
        lib_b.write("sym", _row(0, 0))
        lib_b.delete("sym")
        lib_a.write("sym", _row(1, 1))
        assert lib_b.list_symbols() == ["sym"]
        lib_b.snapshot("snap")
    lib_a._nvs.version_store.flush_version_map()
    assert lib_a.read("sym", as_of="snap").version == 1


def test_write_with_fresh_cache_and_unchanged_head_reads_only_the_ref(mem_library, clear_query_stats):
    with config_context("VersionMap.ReloadInterval", FRESH):
        for i in range(3):
            mem_library.append("sym", _row(i, i))
        qs.reset_stats()
        qs.enable()
        try:
            mem_library.append("sym", _row(3, 3))
        finally:
            qs.disable()
    stats = qs.get_query_stats()
    assert query_stats_operation_count(stats, "Memory_GetObject", "VERSION_REF") == 1
    assert query_stats_operation_count(stats, "Memory_GetObject", "VERSION") == 0


def test_write_with_fresh_cache_kill_switch(mem_storage, lib_name):
    lib_a, lib_b = _two_libraries(mem_storage.create_arctic(), lib_name)
    with config_context("VersionMap.ReloadInterval", FRESH), config_context("VersionMap.ConfirmHeadBeforeWrite", 0):
        lib_b.write("sym", _row(0, 0))
        lib_a.append("sym", _row(1, 1))
        assert lib_b.append("sym", _row(2, 2)).version == 1


def _second_process(uri, lib_name, conn):
    lib = Arctic(uri)[lib_name]
    conn.send(lib.write("sym", _row(0, 0)).version)
    conn.recv()
    conn.send(lib.append("sym", _row(2, 2)).version)


def test_sequential_writes_from_two_processes(lmdb_storage, lib_name):
    """The reproducer from the issue, with the default reload interval."""
    ac = lmdb_storage.create_arctic()
    lib = ac.create_library(lib_name)
    ctx = multiprocessing.get_context("spawn")
    here, there = ctx.Pipe()
    proc = ctx.Process(target=_second_process, args=(ac.get_uri(), lib_name, there))
    proc.start()
    try:
        assert here.recv() == 0
        assert lib.append("sym", _row(1, 1)).version == 1
        here.send("go")
        assert here.recv() == 2
    finally:
        proc.join(timeout=60)
    _check(lib, "sym", [0, 1, 2], [0.0, 1.0, 2.0])
