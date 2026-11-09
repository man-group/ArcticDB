"""
Copyright 2026 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, version 2.0.

A cached version chain older than VersionMap.ReloadInterval is revalidated with one VERSION_REF read when its head is
unchanged, instead of walking the whole chain again.
"""

import numpy as np
import pandas as pd
import pytest

import arcticdb.toolbox.query_stats as qs
from arcticdb.util.test import assert_frame_equal, config_context, query_stats_operation_count

N = 20


def _df(i):
    return pd.DataFrame({"col": np.arange(1, dtype=np.int64) + i}, index=pd.date_range("2020-01-01", periods=1))


def _version_reads(fn):
    qs.reset_stats()
    qs.enable()
    try:
        fn()
    finally:
        qs.disable()
    stats = qs.get_query_stats()
    return (
        query_stats_operation_count(stats, "Memory_GetObject", "VERSION"),
        query_stats_operation_count(stats, "Memory_GetObject", "VERSION_REF"),
    )


@pytest.mark.parametrize("op", ["delete_version", "delete_snapshot", "add_to_snapshot"])
def test_stale_cache_with_unchanged_head_is_not_reloaded(in_memory_store_factory, clear_query_stats, op):
    lib = in_memory_store_factory(use_tombstones=True, delayed_deletes=False)
    for i in range(N):
        lib.write("sym", _df(i))
        lib.snapshot(f"snap{i}")
    lib.write("sym", _df(N))
    lib.write("sym", _df(N + 1))
    lib.delete_version("sym", 5)  # tombstoned and only held by snap5
    lib.list_versions("sym")
    with config_context("VersionMap.ReloadInterval", 0):
        if op == "delete_version":
            version_reads, ref_reads = _version_reads(lambda: lib.delete_version("sym", N))
        elif op == "delete_snapshot":
            version_reads, ref_reads = _version_reads(lambda: lib.delete_snapshot("snap5"))
        else:
            version_reads, ref_reads = _version_reads(lambda: lib.add_to_snapshot("snap5", ["sym"], [N + 1]))
    assert version_reads == 0
    assert ref_reads >= 1
    assert_frame_equal(lib.read("sym").data, _df(N + 1))


def _two_clients(in_memory_store_factory, name):
    lib_a = in_memory_store_factory(name=name, use_tombstones=True, delayed_deletes=False)
    lib_b = in_memory_store_factory(name=name, reuse_name=True, use_tombstones=True, delayed_deletes=False)
    return lib_a, lib_b


def test_stale_cache_reads_only_version_keys_written_by_another_client(in_memory_store_factory, clear_query_stats):
    lib_a, lib_b = _two_clients(in_memory_store_factory, "delta_walk")
    for i in range(N):
        lib_a.write("sym", _df(i))
    lib_b.list_versions("sym")
    lib_a.write("sym", _df(N))
    lib_a.write("sym", _df(N + 1))
    with config_context("VersionMap.ReloadInterval", 0):
        version_reads, ref_reads = _version_reads(lambda: lib_b.delete_version("sym", N))
    assert version_reads == 2
    assert ref_reads >= 1
    assert sorted(v["version"] for v in lib_b.list_versions("sym")) == [v for v in range(N + 2) if v != N]
    assert_frame_equal(lib_a.read("sym").data, _df(N + 1))


@pytest.mark.parametrize("change", ["write", "delete_version", "delete", "delete_and_recreate"])
def test_stale_cache_sees_changes_by_another_client(in_memory_store_factory, change):
    lib_a, lib_b = _two_clients(in_memory_store_factory, f"changes_{change}")
    for i in range(3):
        lib_a.write("sym", _df(i))
    assert len(lib_b.list_versions("sym")) == 3
    assert_frame_equal(lib_b.read("sym").data, _df(2))
    if change == "write":
        lib_a.write("sym", _df(3))
        expected_versions, expected_latest = [0, 1, 2, 3], _df(3)
    elif change == "delete_version":
        lib_a.delete_version("sym", 2)
        expected_versions, expected_latest = [0, 1], _df(1)
    elif change == "delete":
        lib_a.delete("sym")
        expected_versions, expected_latest = [], None
    else:
        lib_a.delete("sym")
        lib_a.write("sym", _df(10))
        expected_versions, expected_latest = [3], _df(10)
    with config_context("VersionMap.ReloadInterval", 0):
        assert sorted(v["version"] for v in lib_b.list_versions("sym")) == expected_versions
        if expected_latest is None:
            assert not lib_b.has_symbol("sym")
        else:
            assert_frame_equal(lib_b.read("sym").data, expected_latest)
