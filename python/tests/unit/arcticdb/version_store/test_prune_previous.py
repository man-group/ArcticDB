"""
Copyright 2026 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
will be governed by the Apache License, version 2.0.
"""

import pandas as pd
import pytest

from arcticdb_ext.storage import KeyType
from arcticdb.util.test import assert_frame_equal, config_context


@pytest.fixture
def _with_protection_window():
    """Enable the 600-second protection window for a single test, then restore the prior value.

    The session-scoped conftest fixture sets PrunePreviousProtectionSecs=0 once at session start.
    This fixture temporarily overrides it to 600 s for tests that exercise window behaviour and
    restores the previous value in a finally block so subsequent tests are not affected even if
    the test raises.
    """
    with config_context("VersionStore.PrunePreviousProtectionSecs", 600):
        yield


def test_prune_previous_preserves_recent_data_in_storage(version_store_factory, _with_protection_window):
    """Writing with prune_previous_version=True must not physically delete data younger than the window.

    Regression test for a race condition: if two writers concurrently append to the same base version,
    Writer 1's prune must not delete the base version's data segments before Writer 2 has finished
    referencing them. Under the new policy the pre-existing version is tombstoned immediately, but
    its data remains in storage until it has aged past the protection window.
    """
    lib = version_store_factory()
    sym = "test_sym"

    df0 = pd.DataFrame({"x": range(5)})
    df1 = pd.DataFrame({"x": range(5, 10)})

    lib.write(sym, df0)
    # V0 is freshly written (< 10 minutes old). Writing V1 with prune must tombstone V0 but
    # keep its data in storage so concurrent writers based on V0 can still finish.
    lib.write(sym, df1, prune_previous_version=True)

    # V0 is tombstoned — only V1 is visible.
    live_versions = [v for v in lib.list_versions(sym) if not v["deleted"]]
    assert len(live_versions) == 1, f"Expected only V1 to be visible (V0 tombstoned), got {live_versions}"

    # But V0's TABLE_DATA segments are still in storage (protected by the window).
    lib_tool = lib.library_tool()
    data_keys = lib_tool.find_keys(KeyType.TABLE_DATA)
    assert len(data_keys) == 2, f"Expected V0 and V1 data segments to remain in storage, got {data_keys}"


def test_prune_previous_prunes_sole_preexisting_version(version_store_factory):
    """With protection=0 a sole pre-existing version is still tombstoned (no anchor visibility)."""
    # conftest sets PrunePreviousProtectionSecs=0
    lib = version_store_factory()
    sym = "test_sym"

    df0 = pd.DataFrame({"x": [1, 2, 3]})
    df1 = pd.DataFrame({"x": [4, 5, 6]})

    lib.write(sym, df0)
    lib.write(sym, df1, prune_previous_version=True)

    live_versions = [v for v in lib.list_versions(sym) if not v["deleted"]]
    assert len(live_versions) == 1, f"V0 must be tombstoned (no anchor visibility), got {live_versions}"


def test_prune_previous_prunes_when_old_enough(version_store_factory):
    """With protection=0 every prune tombstones every pre-existing version.

    After N sequential prune writes, exactly 1 live version survives (the latest).
    """
    # conftest already sets PrunePreviousProtectionSecs=0
    lib = version_store_factory()
    sym = "test_sym"

    lib.write(sym, pd.DataFrame({"x": range(5)}))  # V0
    lib.write(sym, pd.DataFrame({"x": range(5, 10)}), prune_previous_version=True)  # V1, V0 tombstoned
    lib.write(sym, pd.DataFrame({"x": range(10, 15)}), prune_previous_version=True)  # V2, V1 tombstoned
    lib.write(sym, pd.DataFrame({"x": range(15, 20)}), prune_previous_version=True)  # V3, V2 tombstoned

    live_versions = [v for v in lib.list_versions(sym) if not v["deleted"]]
    assert len(live_versions) == 1, f"Only the latest version should remain visible, got {live_versions}"


def test_prune_previous_data_remains_in_storage_during_window(version_store_factory):
    """With a 600 s window, pre-existing data stays in storage even though the version is tombstoned.

    Then once the window is set back to 0 and another prune fires, the previously-held data is
    cleaned up by the aging-tombstones sweep.
    """
    lib = version_store_factory()
    sym = "test_sym"
    lib_tool = lib.library_tool()

    df0 = pd.DataFrame({"x": range(5)})
    df1 = pd.DataFrame({"x": range(5, 10)})
    df2 = pd.DataFrame({"x": range(10, 15)})

    lib.write(sym, df0)
    # With window=600 s, V0 is tombstoned but its data stays in storage.
    with config_context("VersionStore.PrunePreviousProtectionSecs", 600):
        lib.write(sym, df1, prune_previous_version=True)

    live_versions = [v for v in lib.list_versions(sym) if not v["deleted"]]
    assert len(live_versions) == 1, f"Only V1 should be visible, got {live_versions}"
    # Both V0 and V1 data segments are still in storage (window protects V0, V1 is live).
    data_keys_during_window = lib_tool.find_keys(KeyType.TABLE_DATA)
    assert (
        len(data_keys_during_window) == 2
    ), f"Expected V0 and V1 data segments during window, got {data_keys_during_window}"

    # Now set the window to 0 and prune again — the aging-tombstones sweep should pick up V0's
    # tombstoned-but-not-deleted data. (V1 is the new anchor and stays in storage.)
    lib.write(sym, df2, prune_previous_version=True)

    data_keys_after_sweep = lib_tool.find_keys(KeyType.TABLE_DATA)
    # V1 (new anchor, tombstoned) + V2 (current latest). V0 should have been swept.
    assert len(data_keys_after_sweep) == 2, (
        f"Expected V0 data segments to be swept; only V1 (anchor) and V2 (latest) should remain, "
        f"got {data_keys_after_sweep}"
    )


def test_prune_previous_burst_within_window_all_cleaned_in_one_sweep(version_store_factory):
    """A burst of versions written within the window must all be reclaimed in a single later sweep.

    Regression test for the bounded aging sweep: writing N versions inside the protection window
    retains all of them (each is tombstoned but kept). When they later age out of the window
    *together*, the next prune must collect the whole still-present block in one sweep — not just
    the newest aged version, which would leak the rest (and, because the bounded walk stops at the
    first already-cleaned-up key, leak them permanently). The sweep only stops once it reaches a
    key that has actually been physically deleted, so an un-deleted burst is fully cleaned.
    """
    lib = version_store_factory()
    sym = "test_sym"
    lib_tool = lib.library_tool()

    # Distinct data per version so there is no dedup/append sharing: each write owns one segment.
    dfs = [pd.DataFrame({"x": range(i * 5, i * 5 + 5)}) for i in range(6)]

    # Burst of 5 prune-writes inside the window -> V0..V3 tombstoned-but-kept, V4 live.
    with config_context("VersionStore.PrunePreviousProtectionSecs", 600):
        for i in range(5):
            lib.write(sym, dfs[i], prune_previous_version=(i > 0))

    assert (
        len(lib_tool.find_keys(KeyType.TABLE_DATA)) == 5
    ), "All 5 versions' data should be retained while within the window"

    # The burst has now aged out (session default window=0). A single prune must reclaim the whole
    # aged block, leaving only the new anchor (V4) and the latest (V5).
    lib.write(sym, dfs[5], prune_previous_version=True)
    data_keys = lib_tool.find_keys(KeyType.TABLE_DATA)
    assert len(data_keys) == 2, (
        f"The whole aged-out burst must be reclaimed in one sweep (anchor V4 + latest V5 remain); " f"got {data_keys}"
    )

    # Steady state: a subsequent prune stays bounded and does not re-leak (V5 anchor + V6 latest).
    lib.write(sym, dfs[0], prune_previous_version=True)
    assert len(lib_tool.find_keys(KeyType.TABLE_DATA)) == 2, "Subsequent prunes must stay bounded at anchor + latest"


def test_prune_previous_anchor_protects_long_stable_head(version_store_factory, _with_protection_window):
    """The anchor rule keeps the newest pre-existing version in storage even if it is older than the window.

    Scenario: V0 was written long ago and is the head. A concurrent writer is mid-append on V0
    when another writer prunes. V0 is older than protection_secs (stale head), but the anchor
    rule retains V0's data in storage so the concurrent writer can still complete.
    """
    lib = version_store_factory()
    sym = "test_sym"
    lib_tool = lib.library_tool()

    df0 = pd.DataFrame({"x": range(5)})
    df1 = pd.DataFrame({"x": range(5, 10)})

    lib.write(sym, df0)
    # Force V0 to look old to the prune path: set a 0-second window for this single call so
    # the time guard would otherwise cause physical deletion; only the anchor saves it.
    with config_context("VersionStore.PrunePreviousProtectionSecs", 0):
        lib.write(sym, df1, prune_previous_version=True)

    # V0 is tombstoned but its data is retained as the anchor (it is the max version_id in
    # pruned_indexes at the moment of the prune call).
    live_versions = [v for v in lib.list_versions(sym) if not v["deleted"]]
    assert len(live_versions) == 1, f"Only V1 should be visible, got {live_versions}"
    data_keys = lib_tool.find_keys(KeyType.TABLE_DATA)
    assert len(data_keys) == 2, f"Anchor rule must keep V0's data even with the time window at 0; got {data_keys}"


def test_prune_previous_anchor_append_chain_keeps_inherited_data(version_store_factory):
    """The anchor's data must be preserved when its inherited segments are in aging_tombstones.

    Chain scenario:
      V0 = write(df0)                                          -> data {A}
      V1 = append(df1, prune_previous_version=True)            -> V0 anchor-kept; V1 refs {A, B}
      V2 = write(df_unrelated, prune_previous_version=True)    -> V1 anchor-kept; V0 in aging.

    With PrunePreviousProtectionSecs=0 (session fixture default), V0 is outside the window and
    is not the anchor of V2's prune, so it is eligible for sweep. But V1's TABLE_INDEX (the new
    anchor, retained in storage) references V0's data {A} via append-inheritance. The sweep
    must not delete {A}, or V1's index dangles.
    """
    lib = version_store_factory()
    sym = "test_sym"
    lib_tool = lib.library_tool()

    df0 = pd.DataFrame({"x": range(5)}, index=pd.date_range("2000-01-01", periods=5))
    df1 = pd.DataFrame({"x": range(5, 10)}, index=pd.date_range("2000-01-06", periods=5))
    # Different column name and date range so there is no append-inheritance or dedup overlap
    # with V0 or V1: V2's recurse-set does not cover V0's data.
    df_unrelated = pd.DataFrame({"y": [1, 2]}, index=pd.date_range("2050-01-01", periods=2))

    lib.write(sym, df0)
    lib.append(sym, df1, prune_previous_version=True)
    lib.write(sym, df_unrelated, prune_previous_version=True)

    v1_index_keys = [k for k in lib_tool.find_keys_for_id(KeyType.TABLE_INDEX, sym) if k.version_id == 1]
    assert len(v1_index_keys) == 1, f"V1's index key must be retained as anchor, got {v1_index_keys}"

    v1_referenced_data = [k for k in lib_tool.read_to_keys(v1_index_keys[0]) if k.type == KeyType.TABLE_DATA]
    data_in_storage = lib_tool.find_keys_for_id(KeyType.TABLE_DATA, sym)
    missing = [k for k in v1_referenced_data if k not in data_in_storage]
    assert not missing, (
        f"V1 (anchor) references data keys that were deleted by V2's aging sweep; V1 is now "
        f"dangling. Missing keys: {missing}. Present in storage: {data_in_storage}."
    )


def test_prune_previous_anchor_dedup_chain_keeps_shared_data(version_store_factory):
    """Same hazard as the append chain, but via dedup instead of append-inheritance.

      V0 = write(df_a)                                     -> data {A}
      V1 = write(df_a, prune=True)  [dedups vs V0]         -> V0 anchor; V1 refs {A}
      V2 = write(df_b, prune=True)  [no dedup match]       -> V1 anchor; V0 in aging.

    V1 deduped against V0, so V1's TABLE_INDEX references V0's data atom keys. When V0 ages
    out, its data must be preserved because V1 still holds references.
    """
    lib = version_store_factory(de_duplication=True)
    sym = "test_sym"
    lib_tool = lib.library_tool()

    df_a = pd.DataFrame({"x": range(20)}, index=pd.date_range("2000-01-01", periods=20))
    df_b = pd.DataFrame({"y": [1, 2]}, index=pd.date_range("2050-01-01", periods=2))

    lib.write(sym, df_a)
    lib.write(sym, df_a, prune_previous_version=True)  # full dedup against V0
    lib.write(sym, df_b, prune_previous_version=True)  # no dedup with V1; V0 ages out

    v1_index_keys = [k for k in lib_tool.find_keys_for_id(KeyType.TABLE_INDEX, sym) if k.version_id == 1]
    assert len(v1_index_keys) == 1, f"V1's index key must be retained as anchor, got {v1_index_keys}"

    v1_referenced_data = [k for k in lib_tool.read_to_keys(v1_index_keys[0]) if k.type == KeyType.TABLE_DATA]
    data_in_storage = lib_tool.find_keys_for_id(KeyType.TABLE_DATA, sym)
    missing = [k for k in v1_referenced_data if k not in data_in_storage]
    assert not missing, (
        f"V1 (anchor, dedup'd against V0) references data keys that were deleted when V0 was "
        f"swept. Missing: {missing}. Present in storage: {data_in_storage}."
    )


def test_delete_all_reclaims_prune_retained_version(version_store_factory):
    """delete_all_versions must reclaim a version that prune retained (anchor) but did not delete.

    With protection=0 (session default) V0 is tombstoned by V1's prune but kept in storage as the
    anchor. Deleting the whole symbol must clean up V0's data too — otherwise it leaks forever.
    """
    lib = version_store_factory()
    sym = "test_sym"
    lib_tool = lib.library_tool()

    lib.write(sym, pd.DataFrame({"x": range(5)}))  # V0
    lib.write(sym, pd.DataFrame({"x": range(5, 10)}), prune_previous_version=True)  # V1; V0 retained anchor
    assert len(lib_tool.find_keys(KeyType.TABLE_DATA)) == 2, "V0 (anchor) + V1 data should be present"

    lib.delete(sym)
    assert (
        len(lib_tool.find_keys(KeyType.TABLE_DATA)) == 0
    ), "delete_all must reclaim the prune-retained anchor's data, not leak it"


def test_delete_all_after_prune_preserves_snapshotted_version(version_store_factory):
    """delete_all_versions must NOT delete a prune-retained version's data if a snapshot pins it.

    Safety counterpart to the reclaim test: when delete_all collects the retained-but-tombstoned
    keys, the snapshot pre-delete check must still protect any that a snapshot references.
    """
    lib = version_store_factory()
    sym = "test_sym"
    df0 = pd.DataFrame({"x": range(5)})

    lib.write(sym, df0)  # V0
    lib.snapshot("snap")  # snapshot pins V0
    lib.write(sym, pd.DataFrame({"x": range(5, 10)}), prune_previous_version=True)  # V1; V0 retained + snapshotted

    lib.delete(sym)

    # V0's data must survive (the snapshot still references it), so the snapshot read works.
    assert_frame_equal(lib.read(sym, as_of="snap").data, df0)
