"""
Copyright 2025 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

import pprint

import pytest


def populate_library(lib):
    # Construct data such that:
    # - sym<n> has n + 1 versions
    # - snap<n> contains version n of each symbol, or the latest version if symbol doesn't have that version
    # - even numbered versions are deleted
    num_symbols = 3
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


def strip_date(versions):
    for version in versions:
        version.pop("date")
    return versions


# Zero arguments


def test_list_versions_default_args(lmdb_version_store_v1):
    lib = lmdb_version_store_v1
    all_versions, _ = populate_library(lib)
    expected_versions = all_versions
    # Bug 18279584183: list_versions does not include versions of symbols that have been deleted, but have versions
    # kept alive in snapshots. Remove following line once resolved
    expected_versions = expected_versions[:-1]
    # end remove
    versions = lib.list_versions()
    versions = strip_date(versions)
    assert versions == expected_versions


# 1 argument


@pytest.mark.parametrize("symbol", ["sym0", "sym1", "sym2"])
def test_list_versions_symbol(lmdb_version_store_v1, symbol):
    lib = lmdb_version_store_v1
    all_versions, _ = populate_library(lib)
    expected_versions = []
    for version in all_versions:
        if version["symbol"] == symbol:
            expected_versions.append(version)
    versions = lib.list_versions(symbol=symbol)
    versions = strip_date(versions)
    assert versions == expected_versions


@pytest.mark.parametrize("snapshot", ["snap0", "snap1", "snap2"])
def test_list_versions_snapshot(lmdb_version_store_v1, snapshot):
    lib = lmdb_version_store_v1
    all_versions, snapshots = populate_library(lib)
    expected_versions = []
    snapshot_versions = snapshots[snapshot]
    for version in all_versions:
        if snapshot_versions.get(version["symbol"], None) == version["version"]:
            expected_versions.append(version)
    # Bug 18286248854: list_versions has deleted=False for all elements when snapshot is specified. Remove following
    # lines once resolved
    for version in expected_versions:
        version["deleted"] = False
    # end remove
    versions = lib.list_versions(snapshot=snapshot)
    versions = strip_date(versions)
    assert versions == expected_versions


def test_list_versions_latest_only(lmdb_version_store_v1):
    lib = lmdb_version_store_v1
    all_versions, _ = populate_library(lib)
    expected_versions = []
    for version in all_versions:
        sym_idx = int(version["symbol"][-1])
        if not version["deleted"] and version["version"] == (sym_idx - 1) + (sym_idx % 2):
            expected_versions.append(version)
    versions = lib.list_versions(latest_only=True)
    versions = strip_date(versions)
    assert versions == expected_versions


def test_list_versions_skip_snapshots(lmdb_version_store_v1):
    lib = lmdb_version_store_v1
    all_versions, _ = populate_library(lib)
    expected_versions = []
    for version in all_versions:
        version["snapshots"] = []
        if not version["deleted"]:
            expected_versions.append(version)
    versions = lib.list_versions(skip_snapshots=True)
    versions = strip_date(versions)
    assert versions == expected_versions


# 2 arguments


@pytest.mark.parametrize("symbol", ["sym0", "sym1", "sym2"])
@pytest.mark.parametrize("snapshot", ["snap0", "snap1", "snap2"])
def test_list_versions_symbol_and_snapshot(lmdb_version_store_v1, symbol, snapshot):
    lib = lmdb_version_store_v1
    expected_versions, snapshots = populate_library(lib)
    tmp = []
    for version in expected_versions:
        if version["symbol"] == symbol:
            tmp.append(version)
    expected_versions = tmp
    tmp = []
    snapshot_versions = snapshots[snapshot]
    for version in expected_versions:
        if snapshot_versions.get(version["symbol"], None) == version["version"]:
            tmp.append(version)
    expected_versions = tmp
    # Bug 18286248854: list_versions has deleted=False for all elements when snapshot is specified. Remove following
    # lines once resolved
    for version in expected_versions:
        version["deleted"] = False
    # end remove
    versions = lib.list_versions(symbol=symbol, snapshot=snapshot)
    versions = strip_date(versions)
    assert versions == expected_versions


@pytest.mark.parametrize("symbol", ["sym0", "sym1", "sym2"])
def test_list_versions_symbol_and_latest_only(lmdb_version_store_v1, symbol):
    lib = lmdb_version_store_v1
    all_versions, _ = populate_library(lib)
    expected_versions = []
    for version in all_versions:
        if version["symbol"] == symbol:
            expected_versions.append(version)
    tmp = []
    for version in expected_versions:
        sym_idx = int(version["symbol"][-1])
        if not version["deleted"] and version["version"] == (sym_idx - 1) + (sym_idx % 2):
            tmp.append(version)
    expected_versions = tmp
    versions = lib.list_versions(symbol=symbol, latest_only=True)
    versions = strip_date(versions)
    assert versions == expected_versions


@pytest.mark.parametrize("symbol", ["sym0", "sym1", "sym2"])
def test_list_versions_symbol_and_skip_snapshots(lmdb_version_store_v1, symbol):
    lib = lmdb_version_store_v1
    all_versions, _ = populate_library(lib)
    expected_versions = []
    for version in all_versions:
        if version["symbol"] == symbol:
            expected_versions.append(version)
    tmp = []
    for version in expected_versions:
        version["snapshots"] = []
        if not version["deleted"]:
            tmp.append(version)
    expected_versions = tmp
    versions = lib.list_versions(symbol=symbol, skip_snapshots=True)
    versions = strip_date(versions)
    assert versions == expected_versions


# Same as test_list_versions_snapshot as latest_only has no effect when snapshot also specified
@pytest.mark.parametrize("snapshot", ["snap0", "snap1", "snap2"])
def test_list_versions_snapshot_and_latest_only(lmdb_version_store_v1, snapshot):
    lib = lmdb_version_store_v1
    all_versions, snapshots = populate_library(lib)
    expected_versions = []
    snapshot_versions = snapshots[snapshot]
    for version in all_versions:
        if snapshot_versions.get(version["symbol"], None) == version["version"]:
            expected_versions.append(version)
    # Bug 18286248854: list_versions has deleted=False for all elements when snapshot is specified. Remove following
    # lines once resolved
    for version in expected_versions:
        version["deleted"] = False
    # end remove
    versions = lib.list_versions(snapshot=snapshot, latest_only=True)
    versions = strip_date(versions)
    assert versions == expected_versions


@pytest.mark.parametrize("snapshot", ["snap0", "snap1", "snap2"])
def test_list_versions_snapshot_and_skip_snapshots(lmdb_version_store_v1, snapshot):
    lib = lmdb_version_store_v1
    all_versions, snapshots = populate_library(lib)
    expected_versions = []
    snapshot_versions = snapshots[snapshot]
    for version in all_versions:
        if snapshot_versions.get(version["symbol"], None) == version["version"]:
            expected_versions.append(version)
    # Bug 18286248854: list_versions has deleted=False for all elements when snapshot is specified. Remove following
    # lines once resolved
    for version in expected_versions:
        version["deleted"] = False
    # end remove
    # Bug 18262322490: list_versions does not respect skip_snapshots argument when snapshot is specified. Add in
    # following lines once resolved
    # for version in expected_versions:
    #     version["snapshots"] = []
    # end add
    versions = lib.list_versions(snapshot=snapshot, skip_snapshots=True)
    versions = strip_date(versions)
    assert versions == expected_versions


def test_list_versions_latest_only_and_skip_snapshots(lmdb_version_store_v1):
    lib = lmdb_version_store_v1
    all_versions, _ = populate_library(lib)
    expected_versions = []
    for version in all_versions:
        sym_idx = int(version["symbol"][-1])
        if not version["deleted"] and version["version"] == (sym_idx - 1) + (sym_idx % 2):
            expected_versions.append(version)
    for version in expected_versions:
        version["snapshots"] = []
    versions = lib.list_versions(latest_only=True, skip_snapshots=True)
    versions = strip_date(versions)
    assert versions == expected_versions


# 3 arguments


# Same as test_list_versions_symbol_and_snapshot as latest_only has no effect when snapshot also specified
@pytest.mark.parametrize("symbol", ["sym0", "sym1", "sym2"])
@pytest.mark.parametrize("snapshot", ["snap0", "snap1", "snap2"])
def test_list_versions_symbol_and_snapshot_and_latest_only(lmdb_version_store_v1, symbol, snapshot):
    lib = lmdb_version_store_v1
    expected_versions, snapshots = populate_library(lib)
    tmp = []
    for version in expected_versions:
        if version["symbol"] == symbol:
            tmp.append(version)
    expected_versions = tmp
    tmp = []
    snapshot_versions = snapshots[snapshot]
    for version in expected_versions:
        if snapshot_versions.get(version["symbol"], None) == version["version"]:
            tmp.append(version)
    expected_versions = tmp
    # Bug 18286248854: list_versions has deleted=False for all elements when snapshot is specified. Remove following
    # lines once resolved
    for version in expected_versions:
        version["deleted"] = False
    # end remove
    versions = lib.list_versions(symbol=symbol, snapshot=snapshot, latest_only=True)
    versions = strip_date(versions)
    assert versions == expected_versions


@pytest.mark.parametrize("symbol", ["sym0", "sym1", "sym2"])
@pytest.mark.parametrize("snapshot", ["snap0", "snap1", "snap2"])
def test_list_versions_symbol_and_snapshot_and_skip_snapshots(lmdb_version_store_v1, symbol, snapshot):
    lib = lmdb_version_store_v1
    expected_versions, snapshots = populate_library(lib)
    tmp = []
    for version in expected_versions:
        if version["symbol"] == symbol:
            tmp.append(version)
    expected_versions = tmp
    tmp = []
    snapshot_versions = snapshots[snapshot]
    for version in expected_versions:
        if snapshot_versions.get(version["symbol"], None) == version["version"]:
            tmp.append(version)
    expected_versions = tmp
    # Bug 18286248854: list_versions has deleted=False for all elements when snapshot is specified. Remove following
    # lines once resolved
    for version in expected_versions:
        version["deleted"] = False
    # end remove
    # Bug 18262322490: list_versions does not respect skip_snapshots argument when snapshot is specified. Add in
    # following lines once resolved
    # for version in expected_versions:
    #     version["snapshots"] = []
    # end add
    versions = lib.list_versions(symbol=symbol, snapshot=snapshot, skip_snapshots=True)
    versions = strip_date(versions)
    assert versions == expected_versions


@pytest.mark.parametrize("symbol", ["sym0", "sym1", "sym2"])
def test_list_versions_symbol_and_latest_only_and_skip_snapshots(lmdb_version_store_v1, symbol):
    lib = lmdb_version_store_v1
    all_versions, _ = populate_library(lib)
    expected_versions = []
    for version in all_versions:
        if version["symbol"] == symbol:
            expected_versions.append(version)
    tmp = []
    for version in expected_versions:
        sym_idx = int(version["symbol"][-1])
        if not version["deleted"] and version["version"] == (sym_idx - 1) + (sym_idx % 2):
            tmp.append(version)
    expected_versions = tmp
    for version in expected_versions:
        version["snapshots"] = []
    versions = lib.list_versions(symbol=symbol, latest_only=True, skip_snapshots=True)
    versions = strip_date(versions)
    assert versions == expected_versions


# Same as test_list_versions_snapshot_and_skip_snapshots as latest_only has no effect when snapshot also specified
@pytest.mark.parametrize("snapshot", ["snap0", "snap1", "snap2"])
def test_list_versions_snapshot_and_latest_only_and_skip_snapshots(lmdb_version_store_v1, snapshot):
    lib = lmdb_version_store_v1
    all_versions, snapshots = populate_library(lib)
    expected_versions = []
    snapshot_versions = snapshots[snapshot]
    for version in all_versions:
        if snapshot_versions.get(version["symbol"], None) == version["version"]:
            expected_versions.append(version)
    # Bug 18286248854: list_versions has deleted=False for all elements when snapshot is specified. Remove following
    # lines once resolved
    for version in expected_versions:
        version["deleted"] = False
    # end remove
    # Bug 18262322490: list_versions does not respect skip_snapshots argument when snapshot is specified. Add in
    # following lines once resolved
    # for version in expected_versions:
    #     version["snapshots"] = []
    # end add
    versions = lib.list_versions(snapshot=snapshot, latest_only=True, skip_snapshots=True)
    versions = strip_date(versions)
    assert versions == expected_versions
