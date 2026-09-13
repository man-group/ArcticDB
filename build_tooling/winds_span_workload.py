#!/usr/bin/env python3
# ===========================================================================
# EXPERIMENT — NOT PART OF THE PIPELINE. Delete this file and the branch it
# lives on (`benchmark-winds-real-ab`) when the experiment is over.
# ===========================================================================
"""One windowed workload per benchmark family, for arctic-winds gha 99.

Run under ``SPANS_ENV`` (``ARCTICDB_TaskScheduler_LogTaskStats_int=1`` +
``ARCTICDB_schedule_loglevel=DEBUG``) with stderr redirected to a capture file.
ArcticDB's ``TaskStatsLoggingObserver`` writes one ``task_stats pool= thread=
task_id= enqueue_ns= wait_ns= run_ns=`` line per completed task to that stderr;
this script interleaves gha 91's positional window markers into the same file
descriptor, so ``arctic-winds critical-path --spans <capture> --window <name>``
can slice out one family's chain without ever comparing two clocks.

Why markers and not timestamps: ``enqueue_ns`` is a ``steady_clock`` of
arbitrary origin (on macOS ``CLOCK_MONOTONIC_RAW``), so a wall-clock cut is
meaningless. ``arctic_winds.task_spans.window_text`` therefore slices
POSITIONALLY, between ``arctic-winds-span-window <name> begin`` and ``… end``,
and the only thing that has to be true is that the marker and the records share
one append-ordered stream. ``os.write(2, …)`` and spdlog's stderr sink do.

The window names are the benchmark families of ``python/benchmarks/workers.toml``
at one representative combination each — the coverage gha 99 can afford. The
shapes are chosen from gha 97's local sizing curve so the two regimes are
comparable:

  * ``read_filtered`` / ``read_bulk`` — 10M rows in ``ROWS_PER_SEGMENT``-row
    segments, i.e. ``SEGMENTS`` storage operations for one read. gha 97's knee
    is ``io ≈ n_segments / 2``, a property of the read and not of the latency,
    so the segment count is the number that has to be pinned, not the row count.
  * ``list_versions`` / ``list_symbols`` / ``list_snapshots`` — decode-trivial,
    RTT-dominated, the family gha 97 measured at 6.1x between io=12 and io=128.
  * ``write_wide`` / ``append_small`` / ``update_small`` — the write side.
  * ``read_batch`` and ``column_stats`` — the two remaining families' shapes.

Nothing here writes to any results library. Keys go under
``ARCTICDB_PERSISTENT_STORAGE_SHARED_PATH_PREFIX``, which the caller sets to
``spans-<lane>`` so the capture's own traffic can never be mistaken for the
benchmarks'.
"""

import os
import re
import sys
import time

import numpy as np
import pandas as pd

from arcticdb import Arctic, LibraryOptions, QueryBuilder
import arcticdb_ext.cpp_async as cpp_async

# `arctic_winds.task_spans.WINDOW_MARKER`, spelled out rather than imported: this
# script runs in the wheel's venv, which has no arctic-winds in it.
WINDOW_MARKER = "arctic-winds-span-window "

ROWS = 10_000_000
COLUMNS = 5
ROWS_PER_SEGMENT = 100_000  # -> 100 segments, gha 97's decode-heavy shape
SEGMENTS = ROWS // ROWS_PER_SEGMENT
BATCH_SYMBOLS = 5
BATCH_ROWS = 1_000_000
LIST_SYMBOLS = 100
LIST_VERSIONS = 3
SNAPSHOTS = 20
APPENDS = 40
UPDATES = 20

timings = {}


def mark(name, edge):
    """One positional window marker, straight onto the capture's file descriptor."""
    os.write(2, f"{WINDOW_MARKER}{name} {edge}\n".encode())


class window:
    """`with window("read_filtered"): ...` — a named, positionally-delimited window."""

    def __init__(self, name):
        self.name = name

    def __enter__(self):
        sys.stdout.flush()
        mark(self.name, "begin")
        self.started = time.perf_counter()
        return self

    def __exit__(self, *exc):
        elapsed = time.perf_counter() - self.started
        mark(self.name, "end")
        timings[self.name] = elapsed
        print(f"WINDOW {self.name} = {elapsed:.4f}s", flush=True)
        return False


def arctic_uri():
    """The URI the suite's own fixture would build, spelled out.

    `S3Bucket.get_arctic_uri` (`storage_fixtures/s3.py:139-168`) is three lines of
    string building over the same five `ARCTICDB_REAL_S3_*` variables the
    benchmarks read, and `real_s3_from_environment_variables` (`:386-404`) is
    where `shared_path=True` turns
    `ARCTICDB_PERSISTENT_STORAGE_SHARED_PATH_PREFIX` into `path_prefix=`. Those
    are reproduced here rather than imported because importing
    `arcticdb.storage_fixtures.s3` drags in `moto`, `werkzeug` and `requests`,
    which are test-only dependencies this capture's venv has no reason to hold.
    The shape is asserted against the fixture's, line for line:

      * `ssl` is `endpoint.startswith("https://")` — the nightly's endpoint is
        scheme-less, so no `&ssl=True`, exactly as the benchmarks run;
      * the region is NOT in the URI (the fixture does not put it there);
      * `&port=` only when the endpoint carries one, which AWS's does not.
    """
    endpoint = os.environ["ARCTICDB_REAL_S3_ENDPOINT"]
    secure, host, port = re.match(r"(?:http(s?)://)?([^:/]+)(?::(\d+))?", endpoint).groups()
    uri = (
        f"s3{secure or ''}://{host}:{os.environ['ARCTICDB_REAL_S3_BUCKET']}?"
        f"access={os.environ['ARCTICDB_REAL_S3_ACCESS_KEY']}"
        f"&secret={os.environ['ARCTICDB_REAL_S3_SECRET_KEY']}"
    )
    if port:
        uri += f"&port={port}"
    prefix = os.environ.get("ARCTICDB_PERSISTENT_STORAGE_SHARED_PATH_PREFIX")
    if prefix:
        uri += f"&path_prefix={prefix}"
    if endpoint.startswith("https://"):
        uri += "&ssl=True"
    return uri


def main():
    ac = Arctic(arctic_uri())
    print(
        f"pools cpu/io = {cpp_async.cpu_thread_count()} / {cpp_async.io_thread_count()}"
        f"  (defaults {cpp_async.get_default_cpu_count()} / {cpp_async.get_default_io_count()})",
        flush=True,
    )
    print(f"prefix = {os.environ.get('ARCTICDB_PERSISTENT_STORAGE_SHARED_PATH_PREFIX')}", flush=True)
    print(f"segments per wide symbol = {SEGMENTS}", flush=True)

    for name in ("spanwide", "spanbatch", "spanlist", "spanmod"):
        try:
            ac.delete_library(name)
        except Exception:  # noqa: BLE001 — a fresh prefix has nothing to delete
            pass

    wide = ac.create_library("spanwide", LibraryOptions(rows_per_segment=ROWS_PER_SEGMENT))
    batch = ac.create_library("spanbatch", LibraryOptions(rows_per_segment=ROWS_PER_SEGMENT))
    listing = ac.create_library("spanlist")
    modification = ac.create_library("spanmod", LibraryOptions(rows_per_segment=ROWS_PER_SEGMENT))

    index = pd.date_range("2020-01-01", periods=ROWS, freq="s")
    frame = pd.DataFrame(
        np.random.rand(ROWS, COLUMNS),
        columns=[f"c{i}" for i in range(COLUMNS)],
        index=index,
    )
    # One string column with nulls, so the filtered read is gha 97's shape rather
    # than a float comparison.
    frame["s"] = np.where(np.arange(ROWS) % 7 == 0, None, "abc")

    # --- basic_functions: the write half ------------------------------------
    with window("write_wide"):
        wide.write("wide", frame)

    # --- query_builder: the filtered read gha 97 measured at 2.1x -----------
    with window("read_filtered"):
        query = QueryBuilder()
        query = query[query["s"] == "abc"]
        wide.read("wide", query_builder=query)

    # --- basic_functions: the plain bulk read -------------------------------
    with window("read_bulk"):
        wide.read("wide")

    # --- basic_functions: the batch read ------------------------------------
    small = frame.iloc[:BATCH_ROWS]
    for i in range(BATCH_SYMBOLS):
        batch.write(f"b{i}", small)
    with window("read_batch"):
        batch.read_batch([f"b{i}" for i in range(BATCH_SYMBOLS)])

    # --- column_stats: `time_create_column_stats`, the family's own shape ----
    # `python/benchmarks/column_stats.py:408` calls exactly this.
    with window("column_stats"):
        try:
            batch._nvs.create_column_stats_experimental("b0")
        except Exception as exc:  # noqa: BLE001
            print(f"column_stats unavailable: {exc!r}", flush=True)

    # --- listing / list_versions: many symbols, many versions ----------------
    tiny = pd.DataFrame({"a": np.arange(10)}, index=pd.date_range("2020-01-01", periods=10, freq="s"))
    for i in range(LIST_SYMBOLS):
        for _ in range(LIST_VERSIONS):
            listing.write(f"s{i}", tiny)
    for i in range(SNAPSHOTS):
        listing.snapshot(f"snap{i}")

    with window("list_symbols"):
        listing.list_symbols()

    with window("list_versions"):
        listing.list_versions()

    with window("list_snapshots"):
        listing.list_snapshots()

    # --- modification: appends and updates -----------------------------------
    modification.write("m", frame.iloc[: ROWS_PER_SEGMENT * 4])
    tail = frame.iloc[ROWS_PER_SEGMENT * 4 : ROWS_PER_SEGMENT * 4 + 1000]
    with window("append_small"):
        for i in range(APPENDS):
            shifted = tail.copy()
            shifted.index = tail.index + pd.Timedelta(seconds=1000 * (i + 1))
            modification.append("m", shifted)

    head = frame.iloc[:1000]
    with window("update_small"):
        for _ in range(UPDATES):
            modification.update("m", head)

    print("SPANLOAD " + " ".join(f"{k}={v:.3f}s" for k, v in timings.items()), flush=True)


if __name__ == "__main__":
    main()
