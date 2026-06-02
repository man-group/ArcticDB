# Branch Work Log: fix-prune-previous

## Session 1 (2026-04-13)

### Goal
Fix a race condition where concurrent writers using `prune_previous_version=True` could produce unreadable versions. Writer 1's prune would delete data that Writer 2's new version still referenced, because both writers started from the same base version.

### What was done

- **Created Pegasus venv** `fix-prune-previous` (distribution `311-1`, Python 3.11) and installed ArcticDB in editable mode with `ARCTIC_CMAKE_PRESET=skip ARCTICDB_PROTOC_VERS=4`.

- **Wrote 4 regression tests** in `python/tests/unit/arcticdb/version_store/test_prune_previous.py`:
  - `test_prune_previous_preserves_recent_versions` — prune must not delete versions < 10 min old
  - `test_prune_previous_single_preexisting_version_not_pruned` — never prune sole pre-existing version
  - `test_prune_previous_prunes_when_old_enough` — with 0-second window, exactly 2 versions survive (anchor + latest)
  - `test_concurrent_appends_with_prune_all_readable` — all versions readable after concurrent appends with prune

- **Confirmed tests failed** against unmodified code (3/4 deterministic tests failed; concurrent test is probabilistic).

- **Fixed `cpp/arcticdb/version/version_map.hpp`**:
  - Added private `get_prune_previous_boundary()` helper that:
    1. Reads `VersionStore.PrunePreviousProtectionSecs` config (default 600 = 10 min)
    2. Finds all undeleted index keys ≤ requested boundary that are older than the cutoff
    3. If fewer than 2 candidates exist, returns nullopt (nothing to prune)
    4. Returns the second-newest candidate as the tombstone boundary, keeping the newest as an anchor
  - Modified `write_and_prune_previous()` to call `get_prune_previous_boundary()` and only tombstone if it returns a value; also fixed the `tombstone_all_key` construction to use `effective_tombstone_key` (not `previous_key`) so the logical deletion range is correct.

- **Ran `make lint`** — clang-format and black reformatted code; all tests still pass 4/4.

## Session 2 (2026-04-14)

### Goal
Address code review feedback from Session 1; fix vacuously-passing concurrent Python test; harden the C++ implementation.

### What was done

- **Replaced Python concurrent test** (`test_concurrent_appends_with_prune_all_readable`) with a deterministic C++ test `PrunePreviousProtectsBaseVersionForConcurrentWriters` in `test_version_map.cpp`. Root cause: LMDB serialises all write transactions so the race window never opened in Python.

- **Fixed `FollowingVersionChainWithWriteAndPrunePrevious`** after anchor-rule changes:
  - Added `ScopedConfig no_protection("VersionStore.PrunePreviousProtectionSecs", 0)` so PilotedClock timestamps (near 0 ns) fall below the cutoff and versions are eligible for pruning.
  - Corrected LATEST+UNDELETED_ONLY expectation: uses the 3-item ref-entry fast-path, copying `oldest_loaded_index_version_ = min(3, 2) = 2` from the symbol ref.
  - Corrected UNDELETED_ONLY traversal expectation: with anchor=V2, boundary=V1, TOMBSTONE_ALL lands at V1, so oldest = 1 (not old V2).

- **Optimised `get_prune_previous_boundary`**: replaced vector-collect-all with early-exit two-pointer scan. We only need the first two eligible versions; allocating a vector for all candidates wastes memory on symbols with many versions.

- **Fixed pre-existing `test_segment_reslicer.cpp` bug**: `str32.resize(width, '\0\0\0\0')` is a multi-character literal (triggers `-Werror=multichar`); changed to `U'\0'` (correct `char32_t` null). This was blocking the entire `test_unit_arcticdb` binary from building.

- **Ran full VersionMap test suite** (27/27 passed) and `make lint`.

## Session 3 (2026-04-14)

### Goal
Address code review comments and update all documentation for the `prune_previous_versions` behaviour change.

### What was done

- **Updated `library.py` docstrings** for all 9 occurrences of `prune_previous_versions` parameter across `write`, `write_pickle`, `write_batch`, `append_batch`, `finalize_staged_data`, `write_metadata`, `write_metadata_batch`, `delete_data_in_range`, `compact_incomplete`, `defragment_symbol_data`, and `update_by_query`.  New description explains the protection window, anchor rule, and directs users to explicit `prune_previous_versions()` for unconditional pruning.

- **Updated explicit `prune_previous_versions()` docstring** to clarify it is unconditional (no protection window).

- **Added `VersionStore.PrunePreviousProtectionSecs` to `docs/mkdocs/docs/runtime_config.md`** — user-tunable config; explains 2-version minimum, anchor rule, use cases.

- **Updated `docs/claude/cpp/VERSIONING.md`** — replaced the "Hard Delete (Prune)" section with a detailed explanation of the two modes (prune-on-write with protection window/anchor vs. explicit unconditional prune); added note on prune+concurrency to the Concurrency section.

- **Fixed failing regression tests in `test_nonreg_specific.py`** (89 failures from `should_be_pruned=True` cases): the old tests expected 1 surviving TABLE_INDEX key after pruning, but the anchor rule always keeps the newest eligible version so the minimum is 2.  Fixed by:
  - Adding `set_config_int("VersionStore.PrunePreviousProtectionSecs", 0)` to each test
  - Adding an intermediate write so that there are ≥2 eligible candidates before the method-under-test (enabling the anchor to keep one while deleting the other)
  - Updating assertions: `2 if should_be_pruned else 3`
  - Exception: `test_prune_previous_defragment_symbol_data` already has 3 writes; its intermediate assertion is now `== 2` (can't distinguish with only 1 candidate) and the final is `2/3`.
  - All 144 prune regression tests now pass.

- **Updated PR description** with explicit "Behaviour change" section: after N ≥ 3 sequential writes, exactly 2 versions survive instead of 1.

### Key design decisions
- The 10-minute window is the primary protection: any version younger than `PrunePreviousProtectionSecs` is immune to pruning, giving concurrent writers time to commit.
- The anchor is the secondary protection: even with a 0-second window, the newest eligible version is always kept so there is always a safe base for future writers.
- `PrunePreviousProtectionSecs` is a runtime config (not `static const`) so tests can override it via `set_config_int`.
- Explicit admin `prune_previous_versions()` is NOT modified — it keeps original aggressive behaviour.

## Session 4 (2026-04-14)

### Goal
Complete test fix-up: get all 25 remaining failing unit tests to pass after reverting sole-candidate pruning.

### What was done

- **Reverted sole-candidate C++ change** (confirmed already done at session start): `get_prune_previous_boundary` returns `boundary` (not `boundary.has_value() ? boundary : anchor`), so sole pre-existing candidates are kept as anchor and NOT tombstoned.

- **Added global conftest fixture** `_disable_prune_protection_window` — autouse, function-scoped, sets `PrunePreviousProtectionSecs=0` before each test and restores it after.  Tests that specifically exercise the window (e.g. `test_prune_previous_preserves_recent_versions`) override back to 600 at the start.

- **Fixed 7 `test_append.py` / `test_parallel.py` failures** (session carried over from previous session's edits):
  - `test_defragment_read_prev_versions`, `test_append_out_of_order_and_sort`, `test_sort_merge_append`: changed `== 1` → `== 2` with anchor-rule comments.

- **Fixed 25 additional unit test failures** across 5 files by updating assertions to reflect the anchor rule (sole pre-existing candidate is never pruned):
  - `test_deletion.py::test_delete_snapshot`: updated key-version assertions to accept both version_id 0 and 1.
  - `test_deletion.py::test_tombstones_deleted_data_keys_prune`: `len(data_keys) == 1` → `== 2`.
  - `test_deletion_batch.py::test_batch_write_with_pruning` and `test_delete_tree_via_prune_previous`: `len(versions) == 1` → `== 2`.
  - `test_stage.py::test_finalize_with_tokens_and_prune_previous`: prune_previous branch now reads V0 and asserts it equals df_1 (V0 is alive as anchor).
  - `test_parallel.py::test_parallel_write_sort_merge`: `0 not in versions` → `0 in versions`.
  - `test_parallel.py::test_compact_incomplete_prune_previous`: `has_symbol("sym", 0) != should_prune` → `has_symbol("sym", 0)` (always True due to anchor).

- **Full suite**: 774 passed, 0 failed, 132 skipped — all unit tests green.

### Key anchor rule reminder
With `PrunePreviousProtectionSecs=0`, a sole pre-existing version is always the anchor and is NOT tombstoned.  Pruning only starts removing versions on the **second** prune operation (once there are ≥2 eligible candidates).  After N≥3 sequential prune writes, exactly 2 versions survive (anchor + latest).

## Session 5 (2026-04-14)

### Goal
Fix vacuous tests, upgrade conftest to session-scoped fixture, and rebase onto origin/master.

### What was done

- **Refactored conftest fixture** `_disable_prune_protection_window` to `scope="session"` — sets `PrunePreviousProtectionSecs=0` once per test session rather than before every test. Added `_with_protection_window` fixture in `test_prune_previous.py` using `config_context` (from `arcticdb.util.test`) to temporarily restore the 600s window for the specific tests that exercise it; `config_context` saves the current value and restores it in `finally`.

- **Fixed `test_finalize_with_tokens_and_prune_previous` (test_stage.py)** — was vacuous: both `prune=True` and `prune=False` branches asserted identical data. Added a `df_0` pre-write (V0) so that when pruning fires it removes V0; `prune=True` now raises `NoSuchVersionException` reading V0, while `prune=False` returns `df_0`.

- **Fixed `test_compact_incomplete_prune_previous` (test_parallel.py)** — was vacuous: `should_prune` flag was computed but not used in assertion (`assert lib.has_symbol("sym", 0)` always True). Added a pre-write at timestamp `-1` (V0), changing V1 write to timestamp `0`; compaction creates V2. With 2 eligible candidates, pruning now removes V0, restoring `assert lib.has_symbol("sym", 0) != should_prune`.

- **Confirmed test suite**: all changed tests pass (0 failures).

- **Resolved rebase conflicts for CLAUDE.md and PIPELINE.md**: discovered the earlier rebase was accidentally run on `jb/duckdb_incremental` (wrong branch). After aborting, switched to `fix-prune-previous` and confirmed it is already on top of origin/master — rebase was a no-op.

## Session 8 (2026-04-14)

### Goal
Fix remaining failures in `test_basic_version_store.py` integration tests after ref-key structure changed with the anchor rule.

### What was done

- **Fixed `check_regular_write_ref_key_structure`**: after a prune write with the anchor rule the ref key has 3 entries (latest TABLE_INDEX, anchor TABLE_INDEX, VERSION), not 2. Updated the helper to expect 3 entries mirroring `check_append_ref_key_structure`. Updated docstring.

- **Fixed `test_prune_previous_versions_batch_write_metadata`**: TABLE_DATA assertion `== 2` → `== 1`.  `batch_write_metadata` is a metadata-only write so V2 reuses V1's data segments; only V1's (anchor) TABLE_DATA key survives after pruning V0.

## Session 7 (2026-04-14)

### Goal
Fix integration tests in test_basic_version_store.py, test_arctic.py, and test_dedup.py missed by
the earlier unit-test sweep.

### What was done

- **Fixed `test_basic_version_store.py`** (6 locations):
  - `test_with_prune`: `== 1` → `== 2` at both version-count assertions (lines 371, 380).
  - `check_write_and_prune_previous_version_keys` helper: `TOMBSTONE_ALL.version_id == latest_version_id - 1` → `latest_version_id - 2` (the TOMBSTONE_ALL now marks the boundary, which is one below the anchor, not the anchor itself).
  - `test_prune_previous_versions_write`, `test_prune_previous_versions_write_batch`, `test_prune_previous_versions_batch_write_metadata`: `list_versions == 1`, `TABLE_INDEX == 1`, `TABLE_DATA == 1` → `== 2` each.
  - `test_prune_previous_versions_append_batch`: same INDEX/versions fix; TABLE_DATA count unchanged (stays 3 because append inheritance means V2 references all earlier data segments, protecting them via `delete_unreferenced_pruned_indexes`).

- **Fixed `test_arctic.py`** (5 locations):
  - `test_delete_version_after_tombstone_all`: added extra write (V0) as the boundary so the prune write (V2) fires a real TOMBSTONE_ALL; adjusted final delete from `[1,2]` → `[1,2,3]`.
  - `test_prune_previous_versions_with_write`: replaced single prune write + two `NoDataFoundException` checks with two sequential prune writes (each removes one version rolling through the anchor rule), plus an intermediate assertion that V1 is still readable as anchor after the first prune.
  - `test_append_prune_previous_versions`, `test_update_prune_previous_versions`: `len == 1` → `== 2`, added `("symbol", 0) in symbols`.
  - `test_compact_data`: `1 if prune` → `2 if prune`.

- **Fixed `test_dedup.py`** (3 locations):
  - `test_de_dup_same_value_written`: `list_versions == 1` → `== 2` (data key count unchanged since de-dup with same data protects all keys).
  - `test_de_dup_with_delete`, `test_de_dup_with_delete_multiple`: `== num_elements` (100) → `== 3 * num_elements / 2` (150) because V0 (sole eligible anchor) retains its 50 df1 data keys alongside V3's 100 new keys (df2+df3 has no overlap with df1 so those keys aren't protected by V3's reference).
  - `test_de_dup_with_tombstones`, `test_de_dup_with_tombstones_multiple`: **no change needed** — in those tests there are 2 eligible candidates so the anchor/boundary split still produces the same total data key count (150) via de-dup inheritance.

## Session 6 (2026-04-14)

### Goal
Fix two more test files missed by the anchor-rule update sweep.

### What was done

- **Fixed `test_column_stats_creation.py::test_column_stats_object_deleted_with_index_key`** (4 parametrised variants): `test_prune_previous_kwarg` and `test_prune_previous_kwarg_batch_methods` both set `expected_count = 0` after writing with `prune_previous_version=True`. With the anchor rule the sole pre-existing version is kept, so its column stats key survives; changed to `expected_count = 1`.

- **Fixed `test_recursive_normalizers.py::test_data_layout`** (2 parametrised variants): after writing v2 with `prune_previous_version=True`, v1 survives as anchor alongside v2. Updated key-count assertions: `MULTI_KEY 1→2`, `TABLE_INDEX 3→6`, `TABLE_DATA 3→6`. The subsequent `lib.delete("sym")` path is unaffected (still cleans up all keys to 0).

## Session 9 (2026-05-29)

### Goal
Decouple the tombstone gate from the physical-delete gate, so `lib.write(sym, df, prune_previous_version=True)` produces the intuitive "only the latest version is visible" result while still keeping concurrent-writer safety.

### What was done

**C++ refactor**

- **`cpp/arcticdb/version/version_map.hpp`**: removed the old `get_prune_previous_boundary` helper. `write_and_prune_previous` now unconditionally tombstones every pre-existing undeleted index (boundary = `entry->get_first_index(false).first`). Switched the entry load to `INCLUDE_DELETED` so an aging-tombstones sweep can pick up index keys that were previously tombstoned but not yet physically removed. Added a `WriteAndPrunePreviousResult` struct exposing `freshly_tombstoned` (anchor candidates) and `aging_tombstones` (eligible for cleanup) so the engine layer can compute the anchor from the freshly-tombstoned set only, avoiding mis-anchoring on zombie tombstones left by `delete_version`.

- **`cpp/arcticdb/version/version_tasks.hpp`**: `WriteAndPrunePreviousTask` no longer takes `delayed_deletes`; returns the new struct.

- **`cpp/arcticdb/version/local_versioned_engine.cpp`**: `delete_unreferenced_pruned_indexes` now takes two vectors (freshly + aging). Anchor = max version_id in freshly only. A key is kept in storage iff it is the anchor OR its `creation_ts >= now - PrunePreviousProtectionSecs`. Aging keys go straight to the snapshot/dedup filter without anchor protection.

- **`cpp/arcticdb/version/version_store_api.cpp`**: admin `prune_previous_versions` also calls `collect_aging_tombstones` and passes both vectors. `delete_all_versions` extends `all_index_keys` with previously-tombstoned-but-not-deleted index keys so `lib.delete(sym)` cleans up anchor-retained data instead of leaking it.

**Docs**

- `docs/mkdocs/docs/runtime_config.md`: rewrote `VersionStore.PrunePreviousProtectionSecs` description to reflect the new "tombstone immediately, physical delete deferred" semantics + the anchor rule.
- `docs/claude/cpp/VERSIONING.md`: replaced the "two modes" prune section with a single-policy diagram covering both the write and admin paths. Added a note about the prior anchor-at-tombstone-time iteration for git archaeology.
- All `library.py` and `_store.py` docstrings for `prune_previous_version` (write/append/update/etc) and the admin `prune_previous_versions()` method.

**Tests — Python**

- Reverted assertions in `test_basic_version_store.py`, `test_arctic.py`, `test_dedup.py`, `test_deletion.py`, `test_deletion_batch.py`, `test_nonreg_specific.py`, `test_stage.py`, `test_parallel.py`, `test_recursive_normalizers.py`, `test_column_stats_creation.py`, `test_sort_merge.py`, `test_update.py`, `test_append.py`, `test_snapshot.py`, `test_num_storage_operations.py`, hypothesis test, and the new `test_prune_previous.py`: visible-version counts go back to `== 1` after prune; storage-key counts often stay at `== 2` because the anchor rule keeps the newest pre-existing version's index/data in storage. Removed the intermediate-write workarounds that sessions 4/6/7/8 added.
- Added `test_prune_previous_data_remains_in_storage_during_window` and `test_prune_previous_anchor_protects_long_stable_head` to exercise the new safety properties directly.

**Tests — C++**

- Renamed `PrunePreviousProtectsBaseVersionForConcurrentWriters` to `PrunePreviousTombstonesAllPreExisting`. It now asserts that the version-map tombstones V0 (no longer visible) and reports it in `freshly_tombstoned` so the engine layer can apply the protection-window + anchor rule.
- `FollowingVersionChainWithWriteAndPrunePrevious`: restored to its pre-branch form (matches new policy because all pre-existing versions are tombstoned).
- `rapidcheck_version_map.cpp`: removed the `PrunePreviousProtectionSecs=0` scoped config (no longer needed).
- `version_map_model.hpp::VersionMapTombstonesModel::write_and_prune_previous`: reverted to "tombstone everything below the new version".

### Verification

- `cpp test_unit_arcticdb --gtest_filter='VersionMap.*'`: 30/30 passing.
- `cpp arcticdb_rapidcheck_tests --gtest_filter='VersionMap.*'`: 2/2 passing.
- Unit tests for prune-impact files (test_prune_previous, test_deletion, test_deletion_batch, test_parallel, test_stage, test_sort_merge, test_recursive_normalizers, test_nonreg_specific): 1254/1254 passing.
- Hypothesis tests: 3/3 passing.
- `make lint` (black check): clean.
- Pre-existing failures unrelated to this change (test_append_mismatched_object_kind, test_find_version, missing protobuf module for column_stats) remain.

### Key design decisions

- **Anchor + time-based, both at physical-delete time.** Tombstoning is unconditional; the visible API matches user intuition. The anchor (max version_id in just-tombstoned set) is kept regardless of age — protects writers sitting on a long-stable head. The time window protects recently-written versions whose data may still be referenced by in-flight appends.
- **Aging-sweep keys are NOT anchor candidates.** Otherwise zombie tombstones left by `delete_version` would mis-anchor the prune call and let the live anchor get deleted. Tracked separately via `WriteAndPrunePreviousResult.aging_tombstones`.
- **`delete(sym)` cleans up anchor-retained keys.** Added an INCLUDE_DELETED reload after `tombstone_all_async` so previously-tombstoned-but-not-deleted index keys also get fed to `delete_tree`. Without this, the anchor protection would permanently leak the symbol's prior version data once you delete it.

---

## Session: bound prune/delete via TOMBSTONE_ALL high-water mark + individual tombstones

### Problem
The prior iteration wrote a single `TOMBSTONE_ALL` at the prior head and used an `INCLUDE_DELETED`
aging-sweep to find retained anchors below it. But `INCLUDE_DELETED` loads never stop at a
`TOMBSTONE_ALL`, so every prune-write and `delete_all_versions` re-read the entire deletion history
— O(total versions). This failed `test_num_storage_operations::{test_write_and_prune_previous_over_time,
test_delete_over_time}` which require a *constant* number of storage ops.

### Fix
Treat `TOMBSTONE_ALL` as a "everything below here is gone" high-water mark. Retained versions
(anchor + within-window) are kept as **individual** `KeyType::TOMBSTONE` keys *above* the line; a
bounded `UNDELETED_ONLY` load stops at the line but still loads the individual tombstones above it.
When versions age out and are physically deleted, the line is **advanced** to bury them. The loaded
region is the retained set (bounded by window content), not history.

The window/anchor partition and the line advance now live in `version_map` under the per-symbol
lock (`partition_and_advance_tombstone_all`), so tombstones + line move atomically. The engine
(`delete_unreferenced_pruned_indexes`) is a dumb executor: delete `to_delete`, protect `could_share`
+ `key_to_keep`, honour snapshots.

### Changes
- `version_map.hpp`: result struct → `{to_delete, could_share}`; removed `collect_aging_tombstones`;
  rewrote `write_and_prune_previous` (bounded `UNDELETED_ONLY` load; `no_retention` flag keeps the
  legacy single-`TOMBSTONE_ALL` behaviour for `delayed_deletes`); added
  `partition_and_advance_tombstone_all` (shared by write + admin paths).
- `version_tasks.hpp`: `WriteAndPrunePreviousTask` threads the `no_retention` flag.
- `local_versioned_engine.cpp/.hpp`: `delete_unreferenced_pruned_indexes(to_delete, could_share,
  key_to_keep)` — dropped the time/anchor gate (now in version_map); updated both callers to pass
  `delayed_deletes()` as `no_retention`.
- `version_store_api.cpp`: `delete_all_versions` reads retained tombstoned indexes from the cached
  `UNDELETED_ONLY` entry (no extra `INCLUDE_DELETED` reload); admin `prune_previous_versions` uses
  bounded load + `partition_and_advance_tombstone_all(keep_anchor=false, protection_secs=0)` for
  thorough reclaim.
- Tests: updated `FollowingVersionChainWithWriteAndPrunePrevious` (line now lands at anchor-1) and
  `PrunePreviousTombstonesAllPreExisting` (anchor is retained → reported in `could_share`, not
  `to_delete`). rapidcheck model unchanged (visible semantics identical).

### Verification
- C++ `VersionMap.*`: 30/30. rapidcheck `VersionMap.*`: 2/2.
- Python: pending (see below).

### Revised to mark/sweep two-phase (per maintainer review)

Maintainer pushed back on complexity/sprawl. Restructured into a clean two-phase split that reuses
existing tombstone machinery:

- **Phase 1 — mark** (`version_map::write_and_prune_previous`): write the new head + an individual
  `TOMBSTONE` for every live previous version, in one journal entry. No `TOMBSTONE_ALL`, no delete.
  Reuses the same representation as `delete_version`. ~10-line change from master.
- **Phase 2 — sweep** (`local_versioned_engine::delete_unreferenced_pruned_indexes(stream_id, anchor_version)`):
  collect the above-line tombstoned set, retain anchor + within-window, delete the rest (skip under
  `delayed_deletes`), then `version_map::advance_tombstone_all(line)` to bury the removed block.
- Removed `WriteAndPrunePreviousResult`, `partition_and_advance_tombstone_all`, and the `no_retention`
  flag plumbing. `version_map.hpp` diff dropped from +167 to +76 vs master.
- **Anchor correctness**: the anchor is passed explicitly (the caller's `previous_key`), not
  re-derived as `max(tombstoned)` in the sweep — an earlier `delete_version` of the head could
  otherwise leave a higher tombstoned version that was never an append base.
- **delete_all_versions reverted to master**; the retained-key cleanup moved to
  `finalize_tombstone_all_result` (shared by delete + batch-delete), where the `UNDELETED_ONLY` entry
  is already loaded → zero extra I/O. Retained keys go through the same `delete_tree` snapshot/dedup
  checks as live keys, so snapshotted retained versions are protected.
- Added Python tests: `test_delete_all_reclaims_prune_retained_version` (no leak) and
  `test_delete_all_after_prune_preserves_snapshotted_version` (snapshot safety).

### Optimized mark/sweep (entry reuse) — per maintainer review of read amplification

The two-phase split initially did 3 version-map loads per prune (mark, sweep-collect, advance) — with
the op-count guard test disabling the cache (ReloadInterval=0) that measured ~22 VERSION GETs/prune
(5× master). Reduced to 1 load by threading the loaded+mutated entry through the phases:
- `write_and_prune_previous` now returns the entry (the cached shared_ptr it loaded under the lock).
- `delete_unreferenced_pruned_indexes(entry, anchor_version)` reuses it (no reload) to collect the
  tombstoned set and partition.
- `advance_tombstone_all(entry, line)` writes the line on the same entry (re-acquiring the lock).
Result: 1 chain read + 2 writes per prune. Probe (ReloadInterval=0, moto): VERSION GETs plateau at 8
(total 25) after a ~5-prune transient while the TOMBSTONE_ALL line catches up.

Also fixed `finalize_tombstone_all_result` to collect only *individually*-tombstoned keys
(`has_individual_tombstone`) rather than all `is_tombstoned` — TOMBSTONE_ALL-buried versions from a
prior delete_all are already physically gone, so re-adding them issued a wasted delete op (delete
steady-state went 17→16, matching master).

Adjusted `test_write_and_prune_previous_over_time` to warm up 6 prunes before capturing its baseline
(it asserts steady-state constancy; the line-catch-up transient is expected).

### Verification (final)
- C++ `VersionMap.*`: 30/30. rapidcheck `VersionMap.*`: 2/2.
- Python: test_prune_previous (10/10, incl. delete-all reclaim + snapshot-safety), test_deletion,
  test_num_storage_operations::{test_write_and_prune_previous_over_time, test_delete_over_time} — 50
  passed. delete steady = 16 (== master); prune VERSION GETs bounded/constant in steady state.

### Concurrency note (maintainer Q)
The anchor protects only the *immediately* previous head (1-back). Protection against multiple fast
successive writes is the **window's** job (retains everything younger than PrunePreviousProtectionSecs),
covered by the burst test. The 2-back race only bites if an append outlives the window AND >=2 newer
writes land — the documented limitation; tune the window above slowest append latency.

## Session: Collapse mark/sweep into a single-write fold (2026-06-02)

### Goal
The two-phase mark/sweep prune produced 96 integration-test failures (not data bugs): the per-prune
`advance_tombstone_all` wrote a *second* journal entry whose head was a TOMBSTONE_ALL, so the symbol
ref's first key was no longer a TABLE_INDEX — killing the LATEST read fast-path (extra VERSION GET on
every freshly-pruned read) and inflating VERSION-key counts / chain length. Goal: keep the
anchor + protection-window retention but write it in a **single journal entry whose head is the new
TABLE_INDEX**.

### What changed
- **`version_map.hpp::write_and_prune_previous`** rewritten to a single `do_write`:
  - Loads `UNDELETED_ONLY` as before.
  - Computes the **anchor internally** as the latest *undeleted* previous index (an appender's base is
    always the latest live version, never a tombstoned one). This replaced the planned threaded
    `anchor_version` arg — the caller's `maybe_prev`/`previous_index_key_` is `get_latest_version`
    (highest, *including deleted*), which after a `delete_version` of the head points at a tombstoned
    version and would wrongly bury the real live base. Computing from the loaded entry fixes the
    dedup/delete tests.
  - Partitions present previous indexes (incl. already-individually-tombstoned ones, so an aged-out
    burst from earlier prunes is finally reclaimed) into retained (anchor + within-window) vs others.
  - Writes one entry `[TABLE_INDEX(new), TOMBSTONE(retained…), TOMBSTONE_ALL(floor-1)]` where
    floor = min(retained). Only buries versions strictly **below** the floor — a non-retained version
    *above* the floor (delete_version'd former head) is left individually tombstoned, never buried
    (a low-enough line would also bury the anchor); a later prune reclaims it once the floor rises.
    This also avoids the `floor==0` underflow.
  - Head stays the new TABLE_INDEX ⇒ ref[0] is an index key ⇒ LATEST fast-path restored.
- Deleted `advance_tombstone_all` (no longer used).
- **`local_versioned_engine.cpp::delete_unreferenced_pruned_indexes`** simplified to delete-only, no
  version-map write: derives to_delete (tombstoned indexes ≤ line) and retained (> line) straight from
  the mutated entry's `tombstone_all_`; protects retained + snapshotted; early-returns under
  `delayed_deletes` (line already persisted by phase 1). Added back master's
  `(vector pruned, key_to_keep)` overload for the admin path. Both overloads needed
  `.thenValue(→Unit)` before `.thenError` because `delete_trees_responsibly` now returns
  `Future<DeleteTreesStats>`.
- **`version_store_api.cpp::prune_previous_versions`** reverted to master's aggressive body
  (`tombstone_from_key_or_all` + vector-form sweep).
- `version_tasks.hpp` and `version_map_model.hpp` net-unchanged (anchor arg added then removed).
- Tests updated: `test_version_map.cpp` (folded single-entry structure, no separate advance);
  `test_basic_version_store.py::check_write_and_prune_previous_version_keys` now expects the 4-key
  head `[TABLE_INDEX(latest), TOMBSTONE(anchor=latest-1), TOMBSTONE_ALL(latest-2), VERSION]`.

### Verification
- C++ `VersionMap.*` 30/30; rapidcheck `VersionMap.*` 2/2.
- Python (3.11 debug build): test_prune_previous 10/10, test_dedup full suite, test_basic prune set
  (30), test_deletion::test_normal_flow_with_snapshot_and_pruning, test_library_tool iterate-chain,
  test_version_map_cache_storage_ops, and storage-op guards
  test_num_storage_operations::{test_write_and_prune_previous_over_time, test_delete_over_time,
  test_read_after_write_and_prune_previous} — all pass.
- lint: clang-format 19.1.2 + black 25.1.0 clean on changed files.

## Concurrent reader vs eager-prune stress test (2026-06-02)

- Added `test_concurrent_read_write_eager_prune` to
  `python/tests/stress/arcticdb/version_store/test_concurrent_read_and_write.py`, mirroring the
  existing delayed-deletes `test_concurrent_read_write` but against the eager (non-delayed-delete)
  `lmdb_version_store_v2` store. Verifies a concurrent reader never sees `NoDataFoundException`
  while another process writes with `prune_previous_version=True`.
- The read path does NOT retry on a missing data/index key: `version_core.cpp` throws
  `NoDataFoundException` and batch reads surface `E_KEY_NOT_FOUND` — errors are exposed to the
  user. Concurrent read safety comes purely from data surviving in storage, not from retries.
- Root cause investigation: the eager test initially failed with `KeyNotFoundException` on the
  just-superseded version's index key, because the session-autouse fixture
  `_disable_prune_protection_window` (conftest.py:230) sets `PrunePreviousProtectionSecs=0` for the
  whole session. With the window disabled, eager prune retains only the immediate anchor, so a
  reader that resolved an older version before it was pruned hits a physically-deleted key.
- Fix in the test: wrap the body in `config_context("VersionStore.PrunePreviousProtectionSecs",
  600)`, set in the parent before forking so the child reader/writer inherit it. With the window
  enabled the recently superseded version stays in storage and the reader always succeeds. This
  makes the test a regression guard for the protection-window reader-safety guarantee on the eager
  path (it fails with window=0, passes with window=600).

## Doc sync to final single-write-fold implementation (2026-06-02)

Brought the documentation back in line with the code after the single-write-fold collapse, which
had left several docs describing the prior (now-deleted) two-phase `advance_tombstone_all` design.

- **`docs/claude/cpp/VERSIONING.md`** — rewrote the "Hard Delete (Prune)" section: the
  `TOMBSTONE_ALL` line is now written in **phase 1** (`write_and_prune_previous`) as part of the
  single journal entry, not advanced separately in phase 2; phase 2
  (`delete_unreferenced_pruned_indexes(entry)`) is now delete-only with no version-map write.
  Removed all `advance_tombstone_all` references. Documented the **two distinct prune flavours**:
  per-write flag = windowed/anchored (safe); explicit admin `prune_previous_versions()` = aggressive
  (no window), via `tombstone_from_key_or_all` + vector-form sweep — *not* `write_and_prune_previous`.
- **`docs/mkdocs/docs/runtime_config.md`** — clarified `PrunePreviousProtectionSecs` applies only to
  the per-write flag; the explicit admin method ignores it (unconditional, keeps only the latest).
- **`library.py` / `_store.py`** — corrected the explicit `prune_previous_versions()` docstrings:
  they previously claimed the window/anchor deferral, which only the per-write flag has.

Decision (confirmed with maintainer): the explicit admin method's aggressive behaviour is
**intended** (matches the work-log session-9 decision and the final collapse revert), so this was a
doc fix, not a code change.
