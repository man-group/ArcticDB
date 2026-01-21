"""
Copyright 2026 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

import pytest

import arcticdb.toolbox.query_stats as qs
from arcticdb_ext.exceptions import KeyNotFoundException
from arcticdb_ext.storage import KeyType, NoDataFoundException


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
    assert_versions_equal(all_versions, lib.list_versions())


def test_list_versions_deleted_symbols_alive_in_snapshot(lmdb_version_store_v1):
    """Repro for bug 18279584183: list_versions does not include versions of symbols that have been deleted,
    but have versions kept alive in snapshots"""
    lib = lmdb_version_store_v1
    lib.write("sym", 1)
    lib.write("sym", 1)
    lib.write("sym2", 2)
    lib.write("sym3", 3)

    lib.snapshot("snap")

    lib.delete("sym")
    lib.delete("sym2")
    lib.write("sym2", 4)
    lib.snapshot("other_snap")
    lib.write("sym4", 5)

    res = lib.list_versions()

    expected = [
        {"symbol": "sym4", "version": 0, "deleted": False, "snapshots": []},
        {"symbol": "sym3", "version": 0, "deleted": False, "snapshots": ["other_snap", "snap"]},
        {"symbol": "sym2", "version": 1, "deleted": False, "snapshots": ["other_snap"]},
        {"symbol": "sym2", "version": 0, "deleted": True, "snapshots": ["snap"]},
        {"symbol": "sym", "version": 1, "deleted": True, "snapshots": ["snap"]},
    ]

    assert_versions_equal(expected, res)


# 1 argument


def test_list_versions_deleted_symbols_alive_in_snapshot_skip_snapshots(s3_version_store_v1, clear_query_stats):
    """Should not look in snapshots at all, even if the symbol is deleted."""
    lib = s3_version_store_v1
    lib.write("sym", 1)
    lib.snapshot("snap")
    lib.delete("sym")

    qs.enable()
    qs.reset_stats()
    res = lib.list_versions(skip_snapshots=True)

    assert res == []

    stats = qs.get_query_stats()
    storage_ops = stats["storage_operations"]

    assert "SNAPSHOT_REF" not in storage_ops["S3_ListObjectsV2"]
    assert "SNAPSHOT_REF" not in storage_ops["S3_GetObject"]


@pytest.mark.parametrize("symbol", [None, "sym"])
def test_list_versions_deleted_symbol_alive_in_snapshot(lmdb_version_store_v1, symbol):
    """Minimal repro for bug 18279584183: list_versions does not include versions of symbols that have been deleted,
    but have versions kept alive in snapshots"""
    lib = lmdb_version_store_v1
    lib.write("sym", 1)
    lib.snapshot("snap")
    lib.delete("sym")
    res = lib.list_versions(symbol=symbol)
    assert len(res) == 1
    res = res[0]
    assert res["symbol"] == "sym"
    assert res["version"] == 0
    assert res["deleted"] == True
    assert res["snapshots"] == ["snap"]


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
    # Won't fix bug 18286248854: list_versions has deleted=False for all elements when snapshot is specified.
    # Remove following lines if ever fixed:
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
    # Won't fix bug 18286248854: list_versions has deleted=False for all elements when snapshot is specified.
    # Remove following lines if ever fixed:
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
    # Won't fix bug 18286248854: list_versions has deleted=False for all elements when snapshot is specified.
    # Remove following lines if ever fixed:
    for version in expected_versions:
        version["deleted"] = False
    # end remove
    assert_versions_equal(expected_versions, lib.list_versions(snapshot=snapshot, latest_only=True))


@pytest.mark.parametrize("snapshot", ["snap0", "snap1", "snap2"])
def test_list_versions_snapshot_and_skip_snapshots(lmdb_version_store_v1, snapshot):
    lib = lmdb_version_store_v1
    all_versions, snapshots = populate_library(lib)
    expected_versions = filter_for_snapshot(all_versions, snapshots[snapshot])
    # Won't fix bug 18286248854: list_versions has deleted=False for all elements when snapshot is specified.
    # Remove following lines if ever fixed:
    for version in expected_versions:
        version["deleted"] = False
    # end remove
    expected_versions = filter_for_skip_snapshots(expected_versions)
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
    # Won't fix bug 18286248854: list_versions has deleted=False for all elements when snapshot is specified.
    # Remove following lines if ever fixed:
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
    # Won't fix bug 18286248854: list_versions has deleted=False for all elements when snapshot is specified.
    # Remove following lines if ever fixed:
    for version in expected_versions:
        version["deleted"] = False
    # end remove
    expected_versions = filter_for_skip_snapshots(expected_versions)
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
    # Won't fix bug 18286248854: list_versions has deleted=False for all elements when snapshot is specified.
    # Remove following lines if ever fixed:
    for version in expected_versions:
        version["deleted"] = False
    # end remove
    expected_versions = filter_for_skip_snapshots(expected_versions)
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


def test_list_versions_specific_snapshot_should_not_list_snapshots(s3_version_store_v1, clear_query_stats):
    lib = s3_version_store_v1
    num_snapshots = 5

    for i in range(num_snapshots):
        lib.write(f"sym{i}", i)
        lib.snapshot(f"snap{i}")

    qs.enable()
    qs.reset_stats()

    # Query versions for a specific snapshot - should only need to read that one snapshot
    lib.list_versions(snapshot="snap0", skip_snapshots=True)

    stats = qs.get_query_stats()
    storage_ops = stats["storage_operations"]

    # Should not list snapshots when user specifies a particular snapshot
    assert "S3_ListObjectsV2" not in storage_ops
    snapshot_read_count = storage_ops["S3_GetObject"]["SNAPSHOT_REF"]["count"]
    assert snapshot_read_count == 1

    # Reasonableness check that "S3_ListObjectsV2" can be in the output
    lib.list_snapshots()
    stats = qs.get_query_stats()
    storage_ops = stats["storage_operations"]
    assert "S3_ListObjectsV2" in storage_ops


def test_list_versions_specific_snapshot_all_symbols(lmdb_version_store_v1):
    lib = lmdb_version_store_v1
    num_snapshots = 5

    for i in range(num_snapshots):
        lib.write(f"sym{i}", i)
        lib.snapshot(f"snap{i}")

    res = lib.list_versions(snapshot="snap0", skip_snapshots=True)
    assert len(res) == 1
    assert res[0]["symbol"] == "sym0"
    assert res[0]["snapshots"] == []

    res = lib.list_versions(snapshot="snap4", skip_snapshots=True)
    assert len(res) == 5

    for v in res:
        assert v["snapshots"] == []

    symbols = {r["symbol"] for r in res}
    for i in range(num_snapshots):
        assert f"sym{i}" in symbols


def test_list_versions_specific_snapshot_specific_symbols(lmdb_version_store_v1):
    lib = lmdb_version_store_v1
    num_snapshots = 5

    for i in range(num_snapshots):
        lib.write(f"sym{i}", i)
        lib.snapshot(f"snap{i}")

    res = lib.list_versions(snapshot="snap0", skip_snapshots=True, symbol="sym0")
    assert len(res) == 1
    assert res[0]["symbol"] == "sym0"
    assert res[0]["snapshots"] == []

    res = lib.list_versions(snapshot="snap4", skip_snapshots=True, symbol="sym0")
    assert len(res) == 1
    assert res[0]["symbol"] == "sym0"
    assert res[0]["snapshots"] == []


def test_list_versions_snapshot_not_found(lmdb_version_store_v1):
    lib = lmdb_version_store_v1
    lib.write("sym", 1)
    lib.snapshot("snap")

    expected_message = r".*non_existent_snap not found"
    with pytest.raises(NoDataFoundException, match=expected_message):
        lib.list_versions("sym", snapshot="non_existent_snap")

    with pytest.raises(NoDataFoundException, match=expected_message):
        lib.list_versions("sym", snapshot="non_existent_snap", skip_snapshots=True)

    with pytest.raises(NoDataFoundException, match=expected_message):
        lib.list_versions(snapshot="non_existent_snap")

    with pytest.raises(NoDataFoundException, match=expected_message):
        lib.list_versions(snapshot="non_existent_snap", skip_snapshots=True)


@pytest.mark.parametrize("symbol", ["sym", None])
@pytest.mark.parametrize("latest_only", [None, True, False])
@pytest.mark.parametrize("skip_snapshots", [None, True, False])
def test_list_versions_with_snapshot_deleted_always_false(lmdb_version_store_v1, symbol, latest_only, skip_snapshots):
    """See 18286248854, which is marked won't fix due to the performance impact a fix would have. This limitation
    is documented on our `list_versions` APIs. If you change this behaviour, be sure to update that documentation.
    """
    lib = lmdb_version_store_v1
    lib.write("sym", 1)
    lib.snapshot("snap")
    lib.delete("sym")

    res = lib.list_versions(snapshot="snap")
    assert res[0]["deleted"] == False
