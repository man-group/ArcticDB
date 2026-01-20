"""
Copyright 2026 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

import pytest

import arcticdb.toolbox.query_stats as qs
from arcticdb_ext.exceptions import KeyNotFoundException
from arcticdb_ext.storage import KeyType


def populate_library(lib):
    # Construct data such that:
    # - sym<n> has n + 1 versions
    # - snap<n> contains version n of each symbol, or the latest version if symbol doesn't have that version
    # - even numbered versions are deleted
    num_symbols = 5
    num_snapshots = num_symbols
    snapshots = dict()
    all_versions = []
    for sym_idx in range(num_symbols):
        sym = f"sym{sym_idx}"
        for version_idx in range(sym_idx + 1):
            lib.write(sym, 10 * sym_idx + version_idx)
            all_versions.append(
                {
                    "symbol": sym,
                    "version": version_idx,
                    "deleted": version_idx % 2 == 0,
                    "snapshots": [],
                }
            )
    for snap_idx in range(num_snapshots):
        snap = f"snap{snap_idx}"
        versions = {f"sym{sym_idx}": min(snap_idx, sym_idx) for sym_idx in range(num_symbols)}
        snapshots[snap] = versions
        lib.snapshot(snap, versions=versions)
        for version in all_versions:
            if version["symbol"] in versions.keys() and version["version"] == versions[version["symbol"]]:
                version["snapshots"].append(snap)
    for sym_idx in range(num_symbols):
        lib.delete_versions(f"sym{sym_idx}", list(range(0, sym_idx + 1, 2)))
    # Although not specified in our documentation, historically list_versions output has been sorted by symbol, then by
    # date, both in reverse order, and the snapshots field of each dictionary has been sorted alphabetically
    # We use version as a proxy for date here as they would be in the same order with how this is written (and should be
    # always anyway)
    all_versions = sorted(all_versions, key=lambda version: (version["symbol"], version["version"]), reverse=True)
    for version in all_versions:
        version["snapshots"].sort()
    return all_versions, snapshots


def filter_for_symbol(versions, symbol):
    res = []
    for version in versions:
        if version["symbol"] == symbol:
            res.append(version)
    return res


def filter_for_snapshot(versions, snapshot_versions):
    # snapshot_versions is a map from symbol to version number in a given snapshot
    res = []
    for version in versions:
        if snapshot_versions.get(version["symbol"], None) == version["version"]:
            res.append(version)
    return res


def filter_for_latest_only(versions):
    res = []
    for version in versions:
        sym_idx = int(version["symbol"][3:])
        if not version["deleted"] and version["version"] == (sym_idx - 1) + (sym_idx % 2):
            res.append(version)
    return res


def filter_for_skip_snapshots(versions):
    res = []
    for version in versions:
        version["snapshots"] = []
        if not version["deleted"]:
            res.append(version)
    return res


def assert_versions_equal(expected_versions, versions):
    for version in versions:
        version.pop("date")
    assert expected_versions == versions


# Zero arguments


def test_list_versions_default_args(lmdb_version_store_v1):
    lib = lmdb_version_store_v1
    all_versions, _ = populate_library(lib)
    expected_versions = all_versions
    # Bug 18279584183: list_versions does not include versions of symbols that have been deleted, but have versions
    # kept alive in snapshots. Remove following line once resolved
    expected_versions = expected_versions[:-1]
    # end remove
    assert_versions_equal(expected_versions, lib.list_versions())


# 1 argument


@pytest.mark.parametrize("symbol", ["sym0", "sym1", "sym2"])
def test_list_versions_symbol(lmdb_version_store_v1, symbol):
    lib = lmdb_version_store_v1
    all_versions, _ = populate_library(lib)
    expected_versions = filter_for_symbol(all_versions, symbol)
    assert_versions_equal(expected_versions, lib.list_versions(symbol=symbol))


@pytest.mark.parametrize("snapshot", ["snap0", "snap1", "snap2"])
def test_list_versions_snapshot(lmdb_version_store_v1, snapshot):
    lib = lmdb_version_store_v1
    all_versions, snapshots = populate_library(lib)
    expected_versions = filter_for_snapshot(all_versions, snapshots[snapshot])
    # Bug 18286248854: list_versions has deleted=False for all elements when snapshot is specified. Remove following
    # lines once resolved
    for version in expected_versions:
        version["deleted"] = False
    # end remove
    assert_versions_equal(expected_versions, lib.list_versions(snapshot=snapshot))


def test_list_versions_latest_only(lmdb_version_store_v1):
    lib = lmdb_version_store_v1
    all_versions, _ = populate_library(lib)
    expected_versions = filter_for_latest_only(all_versions)
    assert_versions_equal(expected_versions, lib.list_versions(latest_only=True))


def test_list_versions_skip_snapshots(lmdb_version_store_v1):
    lib = lmdb_version_store_v1
    all_versions, _ = populate_library(lib)
    expected_versions = filter_for_skip_snapshots(all_versions)
    assert_versions_equal(expected_versions, lib.list_versions(skip_snapshots=True))


# 2 arguments


@pytest.mark.parametrize("symbol", ["sym0", "sym1", "sym2"])
@pytest.mark.parametrize("snapshot", ["snap0", "snap1", "snap2"])
def test_list_versions_symbol_and_snapshot(lmdb_version_store_v1, symbol, snapshot):
    lib = lmdb_version_store_v1
    all_versions, snapshots = populate_library(lib)
    expected_versions = filter_for_symbol(all_versions, symbol)
    expected_versions = filter_for_snapshot(expected_versions, snapshots[snapshot])
    # Bug 18286248854: list_versions has deleted=False for all elements when snapshot is specified. Remove following
    # lines once resolved
    for version in expected_versions:
        version["deleted"] = False
    # end remove
    assert_versions_equal(expected_versions, lib.list_versions(symbol=symbol, snapshot=snapshot))


@pytest.mark.parametrize("symbol", ["sym0", "sym1", "sym2"])
def test_list_versions_symbol_and_latest_only(lmdb_version_store_v1, symbol):
    lib = lmdb_version_store_v1
    all_versions, _ = populate_library(lib)
    expected_versions = filter_for_symbol(all_versions, symbol)
    expected_versions = filter_for_latest_only(expected_versions)
    assert_versions_equal(expected_versions, lib.list_versions(symbol=symbol, latest_only=True))


@pytest.mark.parametrize("symbol", ["sym0", "sym1", "sym2"])
def test_list_versions_symbol_and_skip_snapshots(lmdb_version_store_v1, symbol):
    lib = lmdb_version_store_v1
    all_versions, _ = populate_library(lib)
    expected_versions = filter_for_symbol(all_versions, symbol)
    expected_versions = filter_for_skip_snapshots(expected_versions)
    assert_versions_equal(expected_versions, lib.list_versions(symbol=symbol, skip_snapshots=True))


# Same as test_list_versions_snapshot as latest_only has no effect when snapshot also specified
@pytest.mark.parametrize("snapshot", ["snap0", "snap1", "snap2"])
def test_list_versions_snapshot_and_latest_only(lmdb_version_store_v1, snapshot):
    lib = lmdb_version_store_v1
    all_versions, snapshots = populate_library(lib)
    expected_versions = filter_for_snapshot(all_versions, snapshots[snapshot])
    # Bug 18286248854: list_versions has deleted=False for all elements when snapshot is specified. Remove following
    # lines once resolved
    for version in expected_versions:
        version["deleted"] = False
    # end remove
    assert_versions_equal(expected_versions, lib.list_versions(snapshot=snapshot, latest_only=True))


@pytest.mark.parametrize("snapshot", ["snap0", "snap1", "snap2"])
def test_list_versions_snapshot_and_skip_snapshots(lmdb_version_store_v1, snapshot):
    lib = lmdb_version_store_v1
    all_versions, snapshots = populate_library(lib)
    expected_versions = filter_for_snapshot(all_versions, snapshots[snapshot])
    # Bug 18286248854: list_versions has deleted=False for all elements when snapshot is specified. Remove following
    # lines once resolved
    for version in expected_versions:
        version["deleted"] = False
    # end remove
    # Bug 18262322490: list_versions does not respect skip_snapshots argument when snapshot is specified. Add in
    # following line once resolved
    # expected_versions = filter_for_skip_snapshots(expected_versions)
    # end add
    assert_versions_equal(expected_versions, lib.list_versions(snapshot=snapshot, skip_snapshots=True))


def test_list_versions_latest_only_and_skip_snapshots(lmdb_version_store_v1):
    lib = lmdb_version_store_v1
    all_versions, _ = populate_library(lib)
    expected_versions = filter_for_latest_only(all_versions)
    expected_versions = filter_for_skip_snapshots(expected_versions)
    assert_versions_equal(expected_versions, lib.list_versions(latest_only=True, skip_snapshots=True))


# 3 arguments


# Same as test_list_versions_symbol_and_snapshot as latest_only has no effect when snapshot also specified
@pytest.mark.parametrize("symbol", ["sym0", "sym1", "sym2"])
@pytest.mark.parametrize("snapshot", ["snap0", "snap1", "snap2"])
def test_list_versions_symbol_and_snapshot_and_latest_only(lmdb_version_store_v1, symbol, snapshot):
    lib = lmdb_version_store_v1
    all_versions, snapshots = populate_library(lib)
    expected_versions = filter_for_symbol(all_versions, symbol)
    expected_versions = filter_for_snapshot(expected_versions, snapshots[snapshot])
    # Bug 18286248854: list_versions has deleted=False for all elements when snapshot is specified. Remove following
    # lines once resolved
    for version in expected_versions:
        version["deleted"] = False
    # end remove
    assert_versions_equal(expected_versions, lib.list_versions(symbol=symbol, snapshot=snapshot, latest_only=True))


@pytest.mark.parametrize("symbol", ["sym0", "sym1", "sym2"])
@pytest.mark.parametrize("snapshot", ["snap0", "snap1", "snap2"])
def test_list_versions_symbol_and_snapshot_and_skip_snapshots(lmdb_version_store_v1, symbol, snapshot):
    lib = lmdb_version_store_v1
    all_versions, snapshots = populate_library(lib)
    expected_versions = filter_for_symbol(all_versions, symbol)
    expected_versions = filter_for_snapshot(expected_versions, snapshots[snapshot])
    # Bug 18286248854: list_versions has deleted=False for all elements when snapshot is specified. Remove following
    # lines once resolved
    for version in expected_versions:
        version["deleted"] = False
    # end remove
    # Bug 18262322490: list_versions does not respect skip_snapshots argument when snapshot is specified. Add in
    # following line once resolved
    # expected_versions = filter_for_skip_snapshots(expected_versions)
    # end add
    assert_versions_equal(expected_versions, lib.list_versions(symbol=symbol, snapshot=snapshot, skip_snapshots=True))


@pytest.mark.parametrize("symbol", ["sym0", "sym1", "sym2"])
def test_list_versions_symbol_and_latest_only_and_skip_snapshots(lmdb_version_store_v1, symbol):
    lib = lmdb_version_store_v1
    all_versions, _ = populate_library(lib)
    expected_versions = filter_for_symbol(all_versions, symbol)
    expected_versions = filter_for_latest_only(expected_versions)
    for version in expected_versions:
        version["snapshots"] = []
    assert_versions_equal(expected_versions, lib.list_versions(symbol=symbol, latest_only=True, skip_snapshots=True))


# Same as test_list_versions_snapshot_and_skip_snapshots as latest_only has no effect when snapshot also specified
@pytest.mark.parametrize("snapshot", ["snap0", "snap1", "snap2"])
def test_list_versions_snapshot_and_latest_only_and_skip_snapshots(lmdb_version_store_v1, snapshot):
    lib = lmdb_version_store_v1
    all_versions, snapshots = populate_library(lib)
    expected_versions = filter_for_snapshot(all_versions, snapshots[snapshot])
    # Bug 18286248854: list_versions has deleted=False for all elements when snapshot is specified. Remove following
    # lines once resolved
    for version in expected_versions:
        version["deleted"] = False
    # end remove
    # Bug 18262322490: list_versions does not respect skip_snapshots argument when snapshot is specified. Add in
    # following line once resolved
    # expected_versions = filter_for_skip_snapshots(expected_versions)
    # end add
    assert_versions_equal(
        expected_versions, lib.list_versions(snapshot=snapshot, latest_only=True, skip_snapshots=True)
    )


# Miscellaneous


@pytest.mark.parametrize("latest_only", [True, False])
def test_tombstone_all(lmdb_version_store_v1, latest_only):
    lib = lmdb_version_store_v1
    sym = "test_tombstone_all"
    lib.write(sym, 0)
    lib.write(sym, 1)
    # Versions 0 and 1 are deleted via a tombstone all key, and so are never loaded by LoadObjective::UNDELETED_ONLY
    lib.delete(sym)
    versions = lib.list_versions(latest_only=latest_only)
    assert not len(versions)
    lib.write(sym, 2)
    lib.write(sym, 3)
    lib.write(sym, 4)
    lib.delete_version(sym, 4)
    expected_versions = (
        [{"symbol": sym, "version": 3, "deleted": False, "snapshots": []}]
        if latest_only
        else [
            {"symbol": sym, "version": 3, "deleted": False, "snapshots": []},
            {"symbol": sym, "version": 2, "deleted": False, "snapshots": []},
        ]
    )
    versions = lib.list_versions(latest_only=latest_only)
    assert_versions_equal(expected_versions, versions)


@pytest.mark.parametrize("latest_only", [True, False])
def test_broken_version_chain(lmdb_version_store_v1, latest_only):
    lib = lmdb_version_store_v1
    sym = "test_broken_version_chain"
    broken_sym = "test_broken_version_chain_broken"
    num_versions = 4
    for _ in range(num_versions):
        lib.write(sym, 1)
        lib.write(broken_sym, 1)
    # Latest version of broken_sym will be version 0, so that even latest_only=True requires version chain traversal
    lib.delete_version(broken_sym, 3)
    lib.delete_version(broken_sym, 2)
    lib.delete_version(broken_sym, 1)
    lib_tool = lmdb_version_store_v1.library_tool()
    version_keys = lib_tool.find_keys_for_id(KeyType.VERSION, broken_sym)
    # This is the entry for writing version 1
    key_to_delete = version_keys[1]
    lib_tool.remove(key_to_delete)
    with pytest.raises(KeyNotFoundException) as e:
        lib.list_versions(latest_only=latest_only)
    assert broken_sym in str(e.value)


# Query stats test for bug demonstration


def test_list_versions_specific_snapshot_reads_all_snapshots(s3_version_store_v1, clear_query_stats):
    """
    Bug 18262322490: Demonstrates that list_versions reads all snapshots even when a specific
    snapshot name is provided.

    The bug is in version_store_api.cpp:332:
        const bool do_snapshots = !skip_snapshots || snap_name;

    When snap_name is specified, do_snapshots is always true, causing get_snapshot_version_info()
    to read all snapshots via get_versions_from_snapshots() which iterates over all snapshot keys.
    """
    lib = s3_version_store_v1
    num_snapshots = 5

    # Create multiple snapshots
    for i in range(num_snapshots):
        lib.write(f"sym{i}", i)
        lib.snapshot(f"snap{i}")

    qs.enable()
    qs.reset_stats()

    # Query versions for a specific snapshot - should only need to read that one snapshot
    lib.list_versions(snapshot="snap0")

    stats = qs.get_query_stats()
    storage_ops = stats["storage_operations"]

    # S3_ListObjectsV2["SNAPSHOT"] counts how many times snapshot iteration occurs
    # When querying a specific snapshot, we expect this to be 1
    # Due to the bug, it will be equal to num_snapshots (iterates all snapshots)
    snapshot_list_count = storage_ops.get("S3_ListObjectsV2", {}).get("SNAPSHOT", {}).get("count", 0)

    # This assertion documents the bug:
    # Expected (after fix): snapshot_list_count == 1
    # Actual (with bug): snapshot_list_count >= 1 (reads all snapshots)
    assert snapshot_list_count == 1, (
        f"Bug 18262322490: Expected to list only 1 snapshot when querying specific snapshot 'snap0', "
        f"but listed snapshots {snapshot_list_count} times. "
        f"This indicates all {num_snapshots} snapshots were iterated instead of just 'snap0'."
    )
