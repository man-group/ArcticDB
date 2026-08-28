# Branch work log: gpetrov/ci_lmdb_nosync

Branched from `gpetrov/ci_speedup` (PR #3350, Windows Defender exclusion) so CI numbers isolate this change.

## 2026-08-28
- Context: with Defender off, Windows `unit` tests still take 144-172 CPU-min vs 36 on Linux; `test_realistic`
  475s vs 25s. All slow tests are small-write-heavy LMDB workloads and the env is opened with only MDB_NOTLS,
  so every commit fsyncs (FlushFileBuffers on NTFS is ~ms).
- Added `lmdb_env_flags(conf)` (`storage/lmdb/lmdb_storage.{hpp,cpp}`) which ORs `LMDBStorage.ExtraFlags`
  (ConfigsMap; env `ARCTICDB_LMDBStorage_ExtraFlags_int`) into the env flags. Default 0.
- gtests `storage/test/test_lmdb_flags.cpp` (pass locally, debug build, venv ci-speedup).
- CI: Windows pytest jobs set `ARCTICDB_LMDBStorage_ExtraFlags_int=327680` (MDB_NOSYNC|MDB_NOMETASYNC).
- Docs: docs/mkdocs/docs/runtime_config.md, docs/claude/cpp/STORAGE_BACKENDS.md.
- Local Linux `test_realistic`: 28s with or without the flags (fsync is cheap on ext4), so the Windows CI run
  is the real test.
- Run 33162500710, first result: `3.9 Windows / unit` job 51 (baseline) -> 44 (Defender) -> **17 min**;
  per-test CPU sum 171 -> 144 -> **34.5 min** (Linux: 36). `test_realistic` 684 -> 549 -> **20s** (Linux 25s).
  All 20,174 tests pass. The fsync-per-commit hypothesis is confirmed: Windows now matches Linux per test.
- Run 33162500710: all Windows `unit` jobs 17-20 min (were 44-107), `stress` 20-22 (were 40-65). Two failures:
  (a) `LmdbFlags.StorageOpensAndWritesWithExtraFlags` on Windows: `remove_all` while the env was still open
  (file lock) - fixed by scoping the storage; (b) `3.13 Windows / unit`:
  `test_parallel_write_static_schema_type_changing_cleans_up_data_keys[True-True]` failed with
  `MDB_MAP_RESIZED` on `mdb_txn_begin` (1 of 6 Windows unit jobs; passed on the others). No occurrence of that
  error in the Windows unit/integration jobs of the last 12 failed build runs, so it cannot yet be written
  off as a pre-existing flake. Plan: rerun the failed job(s) and watch the repeat rate.
- Run 33166902887 (NOSYNC|NOMETASYNC + Azurite cache): Windows unit 13-17 min, gtest passes on all OSes, but
  MDB_MAP_RESIZED hit 3 of 6 Windows unit jobs (test_parallel x2, test_recursive_normalizers) -> 4 occurrences in
  2 runs, none in 12 recent master failures. Mechanism candidates exhausted in mdb.c 0.9.35 (no overlapped I/O, meta
  goes via the buffered handle under NOSYNC); Windows does not guarantee WriteFile/mapped-view coherence.
- Experiment 4: switch to MDB_WRITEMAP|MDB_NOSYNC (0x90000): pages written through the map, no WriteFile, no
  flush. WRITEMAP is open-only, so the hook now ORs the extra flags into mdb_env_open (renamed
  `lmdb_extra_env_flags()`). Disk footprint unchanged on Windows (file is pre-extended to map_size either way);
  ArcticDB uses no nested txns. Control run 33169143201 (ci_speedup, test_parallel.py x5, no flag) in flight.
- Note: LMDB 0.9.36 (2026-08-06) fixes >2GB WriteFile on Windows (ITS#10538); we pin 0.9.35. Unrelated, but worth
  a bump.
- Control run 33169143201 (ci_speedup branch, no ExtraFlags code, test_parallel.py x5 per job): 1 Windows job
  of ~118 failed with `MDB_CORRUPTED: Located page was wrong type` in mdb_get. Same "garbage read from the map"
  family as MAP_RESIZED, so the Windows WriteFile-vs-mapped-view coherence problem pre-exists in sync mode; NOSYNC
  only raised the hit rate by making commits ~20x faster. Supports MDB_WRITEMAP (no WriteFile) as the real fix.
  (5 macOS failures in that run are `setup.py protoc` needing pkg_resources on py3.10 - unrelated CI bug in the
  custom pytest_args path.)
- History (last 20 failed build runs, Windows test jobs): 3 jobs on other branches incl. master already show
  LMDB read-corruption errors in sync mode (`MDB_CORRUPTED` mdb_get on master run 30895908105; `MDB_BAD_TXN` on
  30527747377, 30466219731). Pre-existing Windows flakiness; WRITEMAP is expected to remove it as well.
