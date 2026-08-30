# Branch work log: gpetrov/ci_lmdb_extra_flags

First of a stack of CI-time PRs. Adds the LMDB knob and the diagnostics the investigation needed; the CI value
stays 0 here and is turned on in `gpetrov/ci_win_console_sink`, which fixes what made it unsafe.

## Why

Windows `unit` jobs took 144-172 CPU-min against 36 on Linux, and every slow test was a small-write-heavy LMDB
workload. The env was opened with only `MDB_NOTLS`, so every commit fsyncs, and `FlushFileBuffers` on NTFS costs
about a millisecond.

## What is here

- `LMDBStorage.ExtraFlags` (`ConfigsMap`, env `ARCTICDB_LMDBStorage_ExtraFlags_int`), ORed into the flags passed
  to `mdb_env_open`. Opt-in, default 0, so nothing changes unless it is set. gtests in
  `cpp/arcticdb/storage/test/test_lmdb_flags.cpp`.
- `LmdbEnvDiagnostics`: on `MDB_MAP_RESIZED`/`PAGE_NOTFOUND`/`CORRUPTED`/`BAD_TXN`/`PANIC`/`INVALID` the raised
  message and the storage log carry `mdb_env_info`/`mdb_stat` plus both meta pages read straight from `data.mdb`
  with pread/ReadFile, bypassing the map. That is what told apart "map and file disagree" from "both garbage",
  and it is what found the root cause: ASCII log text sitting inside LMDB's meta pages.

## Measurements

- Windows `3.9 unit` with `327680` (`MDB_NOSYNC|MDB_NOMETASYNC`): 51 -> 17 min, per-test CPU sum 171 -> 34.5 min
  against Linux's 36, `test_realistic` 684 -> 20 s against Linux's 25 s. All 20,174 tests passed.
- `MDB_WRITEMAP` is not usable on Windows runners: a writable mapping makes Windows back the whole `map_size`
  (2 GiB per library, and the fixture asks for 100 GB), which filled C: to 100% and lost pages silently.
- Linux is indifferent to the flags: `test_realistic` is 28 s either way, since fsync on ext4 is cheap.
