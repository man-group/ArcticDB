# Branch work log: gpetrov/ci_win_console_sink

Stacked on `gpetrov/ci_lmdb_extra_flags`. Fixes a real Windows bug and, because of it, turns the LMDB knob on.

## The bug

spdlog's `stderr_sink_mt`, ArcticDB's default sink, caches `_get_osfhandle(_fileno(stderr))` when it is built and
writes with `WriteFile(handle_, ...)`. pytest capture `dup2`s a temp file over fd 2, the CRT closes the original
HANDLE, Windows recycles that handle value for the next `CreateFileW` - which in these tests is LMDB's `data.mdb` -
and every later log line is written into the database at its current file offset, i.e. over the meta pages.

Windows-only and, in practice, test-only. It explains the roughly 1-in-50 `MDB_CORRUPTED` / `MDB_PAGE_NOTFOUND`
flakes seen on master for months, and why `MDB_NOSYNC` made them far more frequent: commits get ~20x faster, so
more log lines land between syncs.

## The fix, in two steps

1. `ConsoleSink` writes through `fwrite(stderr)` + `fflush` instead of a cached handle. Not enough on its own:
   with it, 20 of 56 Windows jobs still failed and the meta page still held `arcticdb | ... DB v7.0.0 release`.
2. `arcticdb_ext` is built `/MT` (`x64-windows-static-msvc`), so it has its own CRT fd table. pytest's `os.dup2`
   goes through Python's `ucrtbase.dll`: it closes the OS handle behind *that* fd 2, while our CRT's fd 2 still
   names the recycled handle. `write_to_console()` therefore resolves the fd through the shared CRT's
   `_get_osfhandle` when `ucrtbase.dll` is loaded, and falls back to fwrite when it is not (a static-CRT
   executable, where nothing can dup2 our fds anyway). The lookup is cached in a function-local static, since
   `GetModuleHandle`/`GetProcAddress` take the loader lock; the *handle* is still resolved per write, which is
   the entire point.

## Tests and result

- `TestLog.ConsoleSinkFollowsStderrRedirection` dup2s a file over fd 2 after the sink exists and asserts the line
  lands there.
- `python/tests/unit/arcticdb/test_log_capture.py` asserts a C++ log line reaches pytest's `capfd`. Fails on
  Windows before the fix, passes on Linux either way.
- Run 33206552356: 0 of 54 Windows jobs with LMDB errors, against 20 of 56 before; the capfd test passed 108/108
  on Windows. `ARCTICDB_LMDBStorage_ExtraFlags_int=327680` is enabled for Windows test jobs on the strength of
  that, taking Windows `unit` from 40-107 min to 10-11.
