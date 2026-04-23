"""
Copyright 2026 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
will be governed by the Apache License, version 2.0.
"""

import pandas as pd
import pytest

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


def test_prune_previous_preserves_recent_versions(version_store_factory, _with_protection_window):
    """Writing with prune_previous_version=True must not delete versions written less than 10 minutes ago.

    Regression test for a race condition: if two writers concurrently append to the same base version,
    Writer 1's prune must not delete the base version's data before Writer 2 has finished using it.
    Keeping versions younger than 10 minutes prevents this data loss.
    """
    lib = version_store_factory()
    sym = "test_sym"

    df0 = pd.DataFrame({"x": range(5)})
    df1 = pd.DataFrame({"x": range(5, 10)})

    lib.write(sym, df0)
    # V0 is freshly written (< 10 minutes old). Writing V1 with prune must NOT delete V0.
    lib.write(sym, df1, prune_previous_version=True)

    live_versions = [v for v in lib.list_versions(sym) if not v["deleted"]]
    assert len(live_versions) == 2, f"Expected both V0 and V1 to be live (V0 too recent to prune), got {live_versions}"

    result0 = lib.read(sym, as_of=0)
    assert_frame_equal(result0.data, df0)

    result1 = lib.read(sym)
    assert_frame_equal(result1.data, df1)


def test_prune_previous_single_preexisting_version_not_pruned(version_store_factory):
    """When there is only one pre-existing version, prune_previous_version=True must not delete it.

    The anchor rule keeps the most-recently-eligible version alive so that concurrent writers
    which appended to it can still read back their own version's data even after another writer
    has pruned.  With only one eligible candidate, that candidate is the anchor — nothing is pruned.
    """
    # conftest sets PrunePreviousProtectionSecs=0; V0 is immediately eligible but is the sole
    # candidate, so it acts as anchor and is not tombstoned.
    lib = version_store_factory()
    sym = "test_sym"

    df0 = pd.DataFrame({"x": [1, 2, 3]})
    df1 = pd.DataFrame({"x": [4, 5, 6]})

    lib.write(sym, df0)
    lib.write(sym, df1, prune_previous_version=True)

    live_versions = [v for v in lib.list_versions(sym) if not v["deleted"]]
    assert len(live_versions) == 2, f"Single pre-existing version must not be pruned (anchor rule), got {live_versions}"


def test_prune_previous_prunes_when_old_enough(version_store_factory):
    """When versions are older than the protection window they should be pruned normally.

    With a 0-second protection window (set by conftest) every version is immediately eligible.
    The anchor rule keeps the newest eligible version alive, so after N ≥ 3 sequential writes
    with prune exactly 2 live versions survive: the anchor and the latest.

    Walk-through with protection=0:
      V0 written — 1 live
      V1(prune) — V0 sole eligible → anchor, nothing pruned → 2 live (V0, V1)
      V2(prune) — V0 and V1 eligible; anchor=V1, boundary=V0 → V0 pruned → 2 live (V1, V2)
      V3(prune) — V1 and V2 eligible; anchor=V2, boundary=V1 → V1 pruned → 2 live (V2, V3)
    """
    # conftest already sets PrunePreviousProtectionSecs=0; no override needed.
    lib = version_store_factory()
    sym = "test_sym"

    lib.write(sym, pd.DataFrame({"x": range(5)}))  # V0
    lib.write(sym, pd.DataFrame({"x": range(5, 10)}), prune_previous_version=True)  # V1
    lib.write(sym, pd.DataFrame({"x": range(10, 15)}), prune_previous_version=True)  # V2 (V0 pruned)
    lib.write(sym, pd.DataFrame({"x": range(15, 20)}), prune_previous_version=True)  # V3 (V1 pruned)

    live_versions = [v for v in lib.list_versions(sym) if not v["deleted"]]
    assert (
        len(live_versions) == 2
    ), f"With 0-second protection window, exactly 2 versions (anchor + latest) should survive, got {live_versions}"
