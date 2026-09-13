#!/usr/bin/env python3
# ===========================================================================
# EXPERIMENT — NOT PART OF THE PIPELINE. Delete this file and the branch it
# lives on (`benchmark-winds-real-ab`) when the experiment is over.
# ===========================================================================
"""The string-decode ladder on real S3, for arctic-winds gha 121.

A SECOND windowed workload beside `winds_span_workload.py`, which is left
byte-identical: gha 99's numeric anchors and gha 114's replicates are read
against that file, and a changed workload would be a second difference between
every arm on this branch.

THE QUESTION (gha 100's U3, which has zero S3-regime evidence). Materialising a
pandas object column allocates one `PyObject*` per DISTINCT string per column
block, and the whole block's unique set is allocated under ONE
`py::gil_scoped_acquire` (`python/python_strings.cpp:104-121`,
`assign_strings_local`). That runs INLINE on the IO thread — `thenValueInline`,
`async_store.hpp:32-43` — so N IO threads serialise on one lock inside their own
`run_ns`. On LMDB (gha 100 §U3) 27 IO threads bought only 5.4x / 2.0x / 1.55x at
100 / 10k / 1M distinct strings. On real S3 a fetch takes 23.3 ms, and the
question is whether that serial term hides underneath it or becomes the pole.

THE LADDER. Five symbols, **one hundred segments each**, ten million rows each,
one `100_000`-row segment apiece — gha 99's `read_bulk` shape exactly, so the
numeric control re-anchors against a capture that already exists:

  | library | columns | what varies |
  | `n1`    | 1 x float64                    | the byte-cheap numeric control |
  | `n6`    | 5 x float64 + `s` (2 distinct) | gha 99's `wide` frame, verbatim |
  | `s100`  | 1 x str, 100 distinct          | the ladder's bottom rung |
  | `s10k`  | 1 x str, 10 000 distinct       | the middle rung |
  | `s1m`   | 1 x str, 1 000 000 distinct    | the top rung |

Every string is twelve characters (`k00000000042`), in every arm, so the only
thing that changes across the three rungs is the number of DISTINCT strings —
gha 100's discipline, and it keeps per-allocation cost and pool bytes per
distinct string constant.

`n1` and `n6` are two points on one bytes-to-time line at a fixed segment count,
which is what prices the fetch under a string arm whose stored bytes are its own.
Each arm gets its own LIBRARY so the S3 key prefix separates it, and this script
reads the byte volumes back off the bucket and prints them: gha 113's wire gate
needs the numbers, and string columns do not compress like floats.

Every window is read TWICE, `_a` then `_b`, for a within-lane repeat error that
gha 99's single-shot windows did not have. Nothing is written to any results
library; keys go under `ARCTICDB_PERSISTENT_STORAGE_SHARED_PATH_PREFIX`, which
the caller sets to `spans-<lane>`.
"""

import os
import re
import sys
import time

import numpy as np
import pandas as pd

from arcticdb import Arctic, LibraryOptions
import arcticdb_ext.cpp_async as cpp_async

# `arctic_winds.task_spans.WINDOW_MARKER`, spelled out rather than imported: this
# script runs in the wheel's venv, which has no arctic-winds in it.
WINDOW_MARKER = "arctic-winds-span-window "

ROWS = 10_000_000
ROWS_PER_SEGMENT = 100_000  # -> 100 segments, gha 99's `read_bulk` shape
SEGMENTS = ROWS // ROWS_PER_SEGMENT
NUMERIC_COLUMNS = 5  # gha 99's `wide` frame is 5 floats + one string column
STRING_WIDTH = 12
CARDINALITIES = (("s100", 100), ("s10k", 10_000), ("s1m", 1_000_000))
REPLICATES = ("a", "b")

timings = {}


def mark(name, edge):
    """One positional window marker, straight onto the capture's file descriptor."""
    os.write(2, f"{WINDOW_MARKER}{name} {edge}\n".encode())


class window:
    """`with window("s1m_a"): ...` — a named, positionally-delimited window."""

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

    Byte-for-byte `winds_span_workload.py`'s, and for the same reason: importing
    `arcticdb.storage_fixtures.s3` drags in `moto`, `werkzeug` and `requests`,
    which are test-only dependencies this capture's venv has no reason to hold.
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


def string_pool(distinct):
    """`distinct` twelve-character keys, as an object array ready for `choice`."""
    pool = np.empty(distinct, dtype=object)
    for i in range(distinct):
        pool[i] = f"k{i:011d}"
    return pool


def stored_bytes(prefix):
    """Per-library stored bytes, read off the bucket the arms were just written to.

    The teardown job's ledger splits keys on the FIRST `/`, which under a
    `path_prefix` is the lane — one number for the whole capture. This splits on
    the second and third segments instead, so each arm's data keys (`tdata`) are
    countable apart from its index and version keys. Those are the bytes a read
    of that arm actually pulls over the wire, which is the number gha 113's wire
    gate wants and the number that prices the fetch under each string arm.
    """
    import boto3

    endpoint = os.environ["ARCTICDB_REAL_S3_ENDPOINT"]
    client = boto3.client(
        "s3",
        region_name=os.environ.get("ARCTICDB_REAL_S3_REGION", "eu-west-1"),
        endpoint_url=endpoint if endpoint.startswith("http") else f"https://{endpoint}",
        aws_access_key_id=os.environ["ARCTICDB_REAL_S3_ACCESS_KEY"],
        aws_secret_access_key=os.environ["ARCTICDB_REAL_S3_SECRET_KEY"],
    )
    bucket = os.environ["ARCTICDB_REAL_S3_BUCKET"]
    tally, token = {}, None
    while True:
        kwargs = {"Bucket": bucket, "Prefix": f"{prefix}/", "MaxKeys": 1000}
        if token:
            kwargs["ContinuationToken"] = token
        page = client.list_objects_v2(**kwargs)
        for item in page.get("Contents", []):
            parts = item["Key"].split("/")
            library = parts[1] if len(parts) > 1 else "?"
            kind = parts[2] if len(parts) > 2 else "?"
            counts = tally.setdefault((library, kind), [0, 0])
            counts[0] += 1
            counts[1] += item["Size"]
        token = page.get("NextContinuationToken")
        if not page.get("IsTruncated"):
            break
    return tally


def report_bytes(prefix):
    try:
        tally = stored_bytes(prefix)
    except Exception as exc:  # noqa: BLE001 — the measurement must not cost the capture
        print(f"STORED_BYTES unavailable: {exc!r}", flush=True)
        return
    print("STORED_BYTES_TABLE_BEGIN", flush=True)
    print("library  keytype  objects  bytes", flush=True)
    for (library, kind), (objects, octets) in sorted(tally.items()):
        print(f"STORED_BYTES {library} {kind} {objects} {octets}", flush=True)
    for library in sorted({lib for lib, _ in tally}):
        data = sum(v[1] for (lib, kind), v in tally.items() if lib == library and kind == "tdata")
        total = sum(v[1] for (lib, _), v in tally.items() if lib == library)
        print(
            f"STORED_BYTES_LIBRARY {library} data_MB={data / 1e6:.1f} total_MB={total / 1e6:.1f}",
            flush=True,
        )
    print("STORED_BYTES_TABLE_END", flush=True)


def main():
    ac = Arctic(arctic_uri())
    print(
        f"pools cpu/io = {cpp_async.cpu_thread_count()} / {cpp_async.io_thread_count()}"
        f"  (defaults {cpp_async.get_default_cpu_count()} / {cpp_async.get_default_io_count()})",
        flush=True,
    )
    prefix = os.environ.get("ARCTICDB_PERSISTENT_STORAGE_SHARED_PATH_PREFIX")
    print(f"prefix = {prefix}", flush=True)
    print(f"rows = {ROWS}  rows_per_segment = {ROWS_PER_SEGMENT}  segments = {SEGMENTS}", flush=True)

    names = ["n1", "n6"] + [name for name, _ in CARDINALITIES]
    for name in names:
        try:
            ac.delete_library(name)
        except Exception:  # noqa: BLE001 — a fresh prefix has nothing to delete
            pass

    options = LibraryOptions(rows_per_segment=ROWS_PER_SEGMENT)
    libraries = {name: ac.create_library(name, options) for name in names}

    index = pd.date_range("2020-01-01", periods=ROWS, freq="s")
    rng = np.random.default_rng(121)

    # --- the numeric controls -------------------------------------------------
    # `n1` is one float64 column: the same eight bytes per row the string arms
    # spend on their string-pool OFFSETS (`position_t` is an int64), at the same
    # segment count, so the two differ in what the decode has to DO with the
    # eight bytes and not in how many of them arrive.
    started = time.perf_counter()
    libraries["n1"].write("sym", pd.DataFrame({"c0": rng.random(ROWS)}, index=index))
    print(f"WROTE n1 in {time.perf_counter() - started:.1f}s", flush=True)

    # `n6` is gha 99's `wide` frame verbatim (`winds_span_workload.py:154-162`):
    # five float64 columns and one string column of two distinct values, one of
    # them None. It is the re-anchor — the SAME symbol shape whose `read_bulk`
    # window gha 99 captured on this lane type at both widths.
    started = time.perf_counter()
    wide = pd.DataFrame(
        rng.random((ROWS, NUMERIC_COLUMNS)),
        columns=[f"c{i}" for i in range(NUMERIC_COLUMNS)],
        index=index,
    )
    wide["s"] = np.where(np.arange(ROWS) % 7 == 0, None, "abc")
    libraries["n6"].write("sym", wide)
    del wide
    print(f"WROTE n6 in {time.perf_counter() - started:.1f}s", flush=True)

    # --- the cardinality ladder ----------------------------------------------
    for name, distinct in CARDINALITIES:
        started = time.perf_counter()
        pool = string_pool(distinct)
        values = pool[rng.integers(0, distinct, ROWS)]
        frame = pd.DataFrame({"s": values}, index=index)
        libraries[name].write("sym", frame)
        del frame, values, pool
        print(f"WROTE {name} ({distinct} distinct) in {time.perf_counter() - started:.1f}s", flush=True)

    # Expected distinct strings IN ONE SEGMENT, which is what one
    # `assign_strings_local` call allocates: `d * (1 - (1 - 1/d)^R)`. It is the
    # predictor, not the symbol-wide distinct count, and at the top rung of a
    # 100 000-row segment it is ~95 163 rather than 1 000 000.
    for name, distinct in CARDINALITIES:
        per_segment = distinct * (1.0 - (1.0 - 1.0 / distinct) ** ROWS_PER_SEGMENT)
        print(
            f"ALLOCATIONS {name} distinct={distinct} per_segment={per_segment:,.0f} "
            f"total={per_segment * SEGMENTS:,.0f}",
            flush=True,
        )

    if prefix:
        report_bytes(prefix)

    # --- the windows ----------------------------------------------------------
    # Interleaved so a drift in the bucket's mood falls on every arm equally, and
    # replicated so the lane carries its own repeat error.
    for replicate in REPLICATES:
        for name in names:
            with window(f"{name}_{replicate}"):
                libraries[name].read("sym")

    print("SPANLOAD " + " ".join(f"{k}={v:.3f}s" for k, v in timings.items()), flush=True)


if __name__ == "__main__":
    main()
