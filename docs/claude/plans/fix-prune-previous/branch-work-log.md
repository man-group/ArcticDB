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
