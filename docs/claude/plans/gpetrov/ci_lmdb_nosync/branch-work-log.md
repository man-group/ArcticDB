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
- Run 33172769498 (WRITEMAP|NOSYNC): Windows unit still 13-17 min, but 4 of 8 Windows unit jobs failed with LMDB
  errors (`MDB_BAD_TXN` mdb_get x8, `MDB_PAGE_NOTFOUND` mdb_put x2) in test_parallel, test_empty_column_type,
  test_recursive_normalizers. WRITEMAP removes the WriteFile/mapping split, so the coherence theory is wrong.
  Whatever breaks scales with commit rate -> suspect a latent ArcticDB-side LMDB usage bug (txn/thread) that the
  fsync per commit was masking. Next: does Linux break under the same flags (local full unit run); audit
  lmdb_storage.cpp txn usage.
- Linux under WRITEMAP|NOSYNC (local, debug build, 8 xdist workers): full `unit` suite 18462 passed / 0 LMDB
  errors (48 errors = missing local mongod/azurite), and the 4 Windows-failing tests x20 = 24380 passed. So the
  fast-commit breakage is Windows-specific; not an ArcticDB txn/thread bug reproducible on Linux.
- Observation: on Windows every LMDB failure was in a 4-worker xdist job; the serial (`-n 0`) `stress` jobs never
  failed in any run. Next probe: the 3 failing test files x3 with `-n 0` on Windows via `pytest_args`.
- Probe run 33182222047 (WRITEMAP|NOSYNC, `-n 0`, 3 files x3 per job): 30 of 37 Windows jobs hit LMDB errors, i.e.
  worse serially. Root cause found in the job logs: C: (where %TEMP% and every LMDB test library live) ended at
  100% full, with 34x `mdb_env_open: There is not enough space on the disk`. Sync-mode control jobs end at 68% used,
  plain-NOSYNC jobs at 84%, WRITEMAP jobs at 100%: a writable file mapping makes Windows back the whole map_size
  (2 GiB per URI-created library, 100 GB for the `map_size=100GB` fixture) whereas LMDB's SetEndOfFile
  pre-extension is lazy. With NOSYNC there is no flush to report ERROR_DISK_FULL, so pages are lost silently ->
  PAGE_NOTFOUND / BAD_TXN. WRITEMAP is therefore unusable for the Windows test suite regardless of temp location.
- Plain NOSYNC (exp 3) jobs were NOT disk-full (41-46 GB free), so its MDB_MAP_RESIZED is a separate Windows-only
  effect; mechanism still unknown after reading mdb.c 0.9.35 (no overlapped I/O; meta goes via the buffered
  handle; reader txnid retry loop present). Failure rate ~1 per 2 Windows unit jobs; not shippable.
- Side findings: (1) fixture cleanup warnings (`ExceptionInCleanUpWarning`, file in use) are 0 in sync-mode jobs
  and ~24 in NOSYNC jobs - Windows keeps a closed file "in use" while the cache manager still holds dirty pages;
  (2) `test_recursive_normalizer_with_custom_class` is not repeat-safe (fails on `--count` repeats 2+);
  (3) the custom `pytest_args` path breaks on macOS py3.10 (`setup.py protoc` needs pkg_resources).
- Decision: CI value reverted to 0 (knob kept, opt-in, documented with the caveats). Windows LMDB speed-up is
  parked; remaining CI levers are matrix policy, hypothesis max_examples on Windows/macOS, and sharding.

## Root-cause investigation (started 2026-08-28)
- Added `LmdbEnvDiagnostics` (`lmdb_storage.{hpp,cpp}`): on MAP_RESIZED / PAGE_NOTFOUND / CORRUPTED / BAD_TXN /
  PANIC / INVALID the raised message and the storage log now carry mdb_env_info/stat (mapsize, last_pgno,
  last_txnid, readers, flags) plus both meta pages read from data.mdb via pread/ReadFile (bypassing the map):
  magic, version, mapsize, psize, roots, last_pg, txnid. Purpose: at the failure point, tell apart "map and file
  disagree" (Windows mapping coherence), "both garbage" (earlier corruption) and "both sane" (LMDB in-memory state).
  gtests `LmdbDiagnostics.*` pass locally; `LmdbStorage::diagnostics()` is public for tooling.
- Repro plan on CI (no workflow change): dispatch with
  pytest_args = "ARCTICDB_LMDBStorage_ExtraFlags_int=327680 pytest -n 4 --count=2 -v <the 3 files>" so Windows jobs
  run NOSYNC|NOMETASYNC (~50% hit rate per job) and Linux jobs act as control. Then the same with 262144
  (NOMETASYNC only: data still flushed, meta via the buffered handle) to split meta-path vs data-flush.

## Root cause found and fixed (2026-08-28)

- Diagnostics from run 33190522184 showed ASCII log text (`arcticdb | ... V1 ... deprecated`) inside LMDB's meta
  pages, both in memory and in `data.mdb` on disk.
- Cause: spdlog's `stderr_sink_mt` (ArcticDB's default sink) caches `_get_osfhandle(_fileno(stderr))` at
  construction on Windows and writes with `WriteFile(handle_, ...)`. pytest capture `dup2`s a temp file over fd 2,
  the CRT closes the original HANDLE, Windows recycles that handle value for the next `CreateFileW` — LMDB's
  `data.mdb` — and every later log line lands in the database at its current file offset (the meta pages at the
  start of the file). Windows-only, test-only; explains the ~1/50 master flakes (`MDB_CORRUPTED`, `MDB_PAGE_NOTFOUND`)
  and why NOSYNC raised the rate (more tests hit the warn path between syncs).
- Fix: `cpp/arcticdb/log/console_sink.hpp` — `ConsoleSink` writes via `fwrite(stderr/stdout)` + `fflush`, which
  resolves the handle from the fd on each call. `make_console_sink()` in `log.cpp` is used for the default logger
  and for `console` sinks from config (ANSI colour sinks kept off Windows only). Regression gtest
  `TestLog.ConsoleSinkFollowsStderrRedirection` dup2s a file over fd 2 after the sink is created and asserts the
  line lands in that file.
- Next: commit/push, rerun the NOSYNC repro dispatch (expect 0 LMDB errors), then set
  `ARCTICDB_LMDBStorage_ExtraFlags_int: 327680` on Windows test jobs and re-measure a full build.

### Second iteration: fwrite was not enough (run 33200293066)

- With the fwrite-based sink, 20/56 Windows jobs still failed with `MDB_MAP_RESIZED`/`MDB_INVALID`/`MDB_BAD_TXN`,
  and the meta page still held the ArcticDB log line (`arcticdb | ... DB v7.0.0 release. Pleas...`).
- Reason: `arcticdb_ext` is built with `/MT` (`x64-windows-static-msvc`, `VCPKG_CRT_LINKAGE static`), so it has its
  own CRT fd table. pytest's `os.dup2` goes through Python's CRT (`ucrtbase.dll`): it closes the OS handle behind
  fd 2 there, but our CRT's fd 2 still names that (now recycled) handle, so `fwrite(stderr)` hits `data.mdb` just as
  `WriteFile(cached_handle)` did.
- Fix: `write_to_console()` (`log/console_sink.cpp`) looks up `_get_osfhandle` in `ucrtbase.dll` when it is loaded
  and writes to the fd's current handle in that table; fallback is fwrite. Python end-to-end regression test
  `python/tests/unit/arcticdb/test_log_capture.py` (capfd must contain a C++ log line) — fails on Windows before
  the fix, passes on Linux regardless.
