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
