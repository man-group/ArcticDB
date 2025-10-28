"""
Copyright 2025 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

import pprint

import pytest


@pytest.mark.parametrize("symbol", [None, "sym1"])
@pytest.mark.parametrize("snapshot", [None, "snap2"])
@pytest.mark.parametrize("latest_only", [False, True])
@pytest.mark.parametrize("skip_snapshots", [False, True])
def test_list_versions(lmdb_version_store_v1, symbol, snapshot, latest_only, skip_snapshots):
    lib = lmdb_version_store_v1
    # Construct data such that:
    # - sym<n> has n + 1 versions
    # - snap<n> contains version n of each symbol, or the latest version if symbol doesn't have that version
    # - even numbered versions are deleted
    num_symbols = 3
    num_snapshots = num_symbols
    snapshots = dict()
    expected_versions = []
    for sym_idx in range(num_symbols):
        sym = f"sym{sym_idx}"
        for version_idx in range(sym_idx + 1):
            lib.write(sym, 10 * sym_idx + version_idx)
            expected_versions.append(
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
        for version in expected_versions:
            if version["symbol"] in versions.keys() and version["version"] == versions[version["symbol"]]:
                version["snapshots"].append(snap)
    for sym_idx in range(num_symbols):
        lib.delete_versions(f"sym{sym_idx}", list(range(0, sym_idx + 1, 2)))
    if symbol is not None:
        tmp = []
        for version in expected_versions:
            if version["symbol"] == symbol:
                tmp.append(version)
        expected_versions = tmp
    if snapshot is not None:
        tmp = []
        snapshot_versions = snapshots[snapshot]
        for version in expected_versions:
            if snapshot_versions.get(version["symbol"], None) == version["version"]:
                tmp.append(version)
        expected_versions = tmp
    if latest_only and snapshot is None:
        tmp = []
        for version in expected_versions:
            sym_idx = int(version["symbol"][-1])
            if not version["deleted"] and version["version"] == (sym_idx - 1) + (sym_idx % 2):
                tmp.append(version)
        expected_versions = tmp
    if skip_snapshots:
        for version in expected_versions:
            version["snapshots"] = []
    print(f"\nsymbol         {symbol}")
    print(f"snapshot       {snapshot}")
    print(f"latest_only    {latest_only}")
    print(f"skip_snapshots {skip_snapshots}")
    pprint.pp(expected_versions, width=100)
    versions = lib.list_versions(
        symbol=symbol, snapshot=snapshot, latest_only=latest_only, skip_snapshots=skip_snapshots
    )
    for version in versions:
        version.pop("date")
    assert len(versions) == len(expected_versions)
    for version in expected_versions:
        assert version in versions
