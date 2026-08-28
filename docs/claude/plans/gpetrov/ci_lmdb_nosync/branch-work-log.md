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
