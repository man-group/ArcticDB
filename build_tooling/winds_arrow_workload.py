#!/usr/bin/env python3
# ===========================================================================
# EXPERIMENT — NOT PART OF THE PIPELINE. Delete this file and the branch it
# lives on (`benchmark-winds-real-ab`) when the experiment is over.
# ===========================================================================
"""The string ladder read as PANDAS and as ARROW, for arctic-winds gha 123.

A THIRD windowed workload beside `winds_span_workload.py` (gha 99's, untouched)
and `winds_string_workload.py` (gha 121's, untouched). Both of those files stay
byte-identical, because gha 99's numeric anchors, gha 114's replicates and gha
121's ladder are all read against them and a changed workload would be a second
difference between every arm on this branch.

THE CLAIM UNDER TEST, quoted from gha 103's gha-121 appendix verbatim:

    "at 1 M distinct the serial GIL work is <=488 ms of an 820 ms real-S3
    window, around 60 %, and none of (a), (b) or (c) reaches it. Removing it
    means not producing PyObject*s at all -- output_format=arrow ... For a
    real-S3 user with a high-cardinality string column that is the first move,
    ahead of every fix on this list."

That claim rests on source reasoning plus gha 100's LMDB-era control (the arrow
read path registers no `PythonStringHandler`). It has never been measured on the
S3 string ladder. This workload measures it.

THE RIG is gha 121's, unchanged: five libraries, each ten million rows in one
hundred 100 000-row segments, every string twelve ASCII characters, the same
`default_rng(121)` seed, so the bytes on the bucket are the same bytes gha 121
read and the pandas arms RE-ANCHOR against its published windows. The one new
dimension is the output format, and the arms are read back-to-back inside a
replicate so a drift in the bucket's mood falls on both formats of an arm
equally:

  | format     | spelling on this wheel (4cb644a4a)                          |
  | pandas     | `lib.read("sym")`                                            |
  | arrow      | `lib.read("sym", output_format="PYARROW")`                    |
  | arrowcat   | ... + `arrow_string_format_default="CATEGORICAL"`             |

`arrowcat` is additional to the ticket's contract. It is cheap (the symbols are
already written and the windows are sub-second) and it is the difference between
the two things "arrow" can mean for a string column at this wheel: LARGE_STRING,
the DEFAULT for data not written through an arrow format, copies every row's
bytes into one contiguous buffer and writes one int64 offset per row;
CATEGORICAL keeps a per-block dictionary and writes one int32 index per row
(`cpp/arcticdb/arrow/arrow_handlers.cpp`, `encode_variable_length` vs
`encode_dictionary`). They have different cost shapes in cardinality and the
advice this ticket re-prices has to name which one it means.

WHAT THE SPAN WINDOW CANNOT SEE, and why this workload prints two clocks. The
span window is first-enqueue to last-task-completion (gha 91). The arrow output
format's final assembly -- `segment_to_arrow_data` (`pipeline_utils.hpp:81`),
then one `pa.RecordBatch._import_from_c` per column block and one
`pa.Table.from_batches` (`_store.py:2965-2972`), then `ArrowTableNormalizer.
denormalize` -- runs SERIALLY on the calling thread AFTER the last task has
completed, i.e. entirely OUTSIDE the span window. A serial term that moved there
rather than vanishing would make the span table flatter arrow than it deserves.
So every window also records `time.perf_counter()` across the same `read` call,
which gha 121 already printed for its own arms (`WINDOW <name> = <secs>s`), and
the analysis reads the difference as the out-of-window serial tail.

CORRECTNESS. `tiny` is a small symbol (200 000 rows, 1 000 distinct, every
seventh value None) read in all three formats and compared element-by-element
against the pandas frame, nulls included. The 10 M-row `s1m` arm is compared in
full as well -- materialising the arrow column back to Python objects is the
expensive direction and is timed and reported rather than assumed -- plus four
aggregate invariants (row count, null count, total string bytes, distinct count)
computed independently on each side. All of it happens after the last window
closes, so none of it is inside a measured window.
"""

import os
import re
import sys
import time
import traceback

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

# The correctness symbol. Small enough that a full element-wise compare of a
# 10 M-row object column is not the only evidence, and shaped to carry nulls.
TINY_ROWS = 200_000
TINY_DISTINCT = 1_000

# `n1` has no string column at all, so it is the control for "does the arrow
# output format change the NUMERIC path", and with `n6` it is the second point
# of the bytes-to-time line that prices the fetch under each string arm. The
# line has to be fitted per format, which is why both numeric arms are read in
# both formats.
FORMATS = ("pandas", "arrow", "arrowcat")
# Which formats each arm is read in. `arrowcat` only where a dictionary can
# plausibly differ from a flat string array, i.e. the string ladder.
ARM_FORMATS = {
    "n1": ("pandas", "arrow"),
    "n6": ("pandas", "arrow"),
    "s100": FORMATS,
    "s10k": FORMATS,
    "s1m": FORMATS,
}

timings = {}


def mark(name, edge):
    """One positional window marker, straight onto the capture's file descriptor."""
    os.write(2, f"{WINDOW_MARKER}{name} {edge}\n".encode())


class window:
    """`with window("s1m_arrow_a"): ...` — a named, positionally-delimited window."""

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


def read_kwargs(fmt):
    """The user-facing spelling of each format on this wheel, in one place.

    Verified at 4cb644a4a: `Library.read` takes `output_format` (the
    `OutputFormat` enum or a case-insensitive string) and
    `arrow_string_format_default` (`version_store/library.py:2066-2078`,
    `options.py:190-235`).
    """
    if fmt == "pandas":
        return {}
    if fmt == "arrow":
        return {"output_format": "PYARROW"}
    if fmt == "arrowcat":
        return {"output_format": "PYARROW", "arrow_string_format_default": "CATEGORICAL"}
    raise ValueError(fmt)


def arctic_uri():
    """The URI the suite's own fixture would build, spelled out.

    Byte-for-byte `winds_span_workload.py`'s and `winds_string_workload.py`'s,
    and for the same reason: importing `arcticdb.storage_fixtures.s3` drags in
    `moto`, `werkzeug` and `requests`, which are test-only dependencies this
    capture's venv has no reason to hold.
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

    gha 121's, unchanged: splits the key on its second and third segments so each
    arm's data keys (`tdata`) are countable apart from its index and version
    keys. Those are the bytes a read of that arm actually pulls over the wire.
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


# ---------------------------------------------------------------------------
# Correctness
# ---------------------------------------------------------------------------


def arrow_string_column_to_objects(table, name):
    """The arrow table's string column as a numpy object array of `str`/`None`.

    This is the EXPENSIVE direction on purpose — it is the comparison's cost and
    the ticket asks for it to be stated rather than hidden. For LARGE_STRING it
    allocates one `PyObject*` per row, which is exactly the work the arrow read
    path avoided; for CATEGORICAL pandas gives a `Categorical`, which is
    converted to objects here so both sides compare as values.
    """
    series = table.column(name).to_pandas()
    if isinstance(series.dtype, pd.CategoricalDtype):
        series = series.astype(object)
    values = series.to_numpy(dtype=object, copy=False)
    # pyarrow renders arrow nulls as None for object dtype, but a categorical
    # round-trip can produce NaN; normalise both to None so `None == None`.
    return np.array([None if (v is None or (isinstance(v, float) and np.isnan(v))) else v for v in values], dtype=object)


def pandas_string_column_to_objects(frame, name):
    values = frame[name].to_numpy(dtype=object, copy=False)
    return np.array([None if (v is None or (isinstance(v, float) and np.isnan(v))) else v for v in values], dtype=object)


def aggregate_invariants_arrow(table, name):
    import pyarrow as pa
    import pyarrow.compute as pc

    column = table.column(name)
    if pa.types.is_dictionary(column.type):
        column = column.cast(pa.large_string())
    rows = len(column)
    nulls = column.null_count
    total_bytes = pc.sum(pc.binary_length(column)).as_py()
    distinct = len(pc.unique(column.combine_chunks()))
    return {"rows": rows, "nulls": nulls, "bytes": total_bytes, "distinct": distinct}


def aggregate_invariants_pandas(frame, name):
    column = frame[name]
    values = column.to_numpy(dtype=object, copy=False)
    is_null = np.array([v is None or (isinstance(v, float) and np.isnan(v)) for v in values])
    present = values[~is_null]
    total_bytes = int(sum(len(v.encode("utf-8")) for v in present))
    # `distinct` counts non-null distinct values, which is what pyarrow's
    # `unique` does NOT do (it keeps null as a value), so the null is subtracted
    # on the arrow side by the caller when present.
    return {
        "rows": len(values),
        "nulls": int(is_null.sum()),
        "bytes": total_bytes,
        "distinct": int(len(set(present))),
    }


def compare(label, frame, table, column):
    """Full element-wise value equality between a pandas frame and an arrow table."""
    started = time.perf_counter()
    try:
        left = pandas_string_column_to_objects(frame, column)
        right = arrow_string_column_to_objects(table, column)
        same_length = len(left) == len(right)
        equal = bool(same_length and np.array_equal(left, right))
        mismatches = 0 if equal else int(np.sum(left[: len(right)] != right[: len(left)]))
        first_bad = ""
        if not equal and same_length:
            bad = np.flatnonzero(left != right)
            if len(bad):
                i = int(bad[0])
                first_bad = f" first_mismatch_at={i} pandas={left[i]!r} arrow={right[i]!r}"
        elapsed = time.perf_counter() - started
        print(
            f"CORRECTNESS {label} column={column} elementwise_equal={equal} "
            f"rows={len(left)} mismatches={mismatches} compare_s={elapsed:.3f}{first_bad}",
            flush=True,
        )
    except Exception as exc:  # noqa: BLE001
        print(f"CORRECTNESS {label} column={column} FAILED {exc!r}", flush=True)
        traceback.print_exc(file=sys.stdout)


def compare_aggregates(label, frame, table, column):
    try:
        left = aggregate_invariants_pandas(frame, column)
        right = aggregate_invariants_arrow(table, column)
        # pyarrow's `unique` keeps null as one of the values.
        if right["nulls"] > 0:
            right["distinct"] -= 1
        agree = left == right
        print(f"AGGREGATES {label} column={column} agree={agree} pandas={left} arrow={right}", flush=True)
    except Exception as exc:  # noqa: BLE001
        print(f"AGGREGATES {label} column={column} FAILED {exc!r}", flush=True)
        traceback.print_exc(file=sys.stdout)


def describe_table(label, table, frame):
    """What a user actually gets back — the API caveat, measured not asserted."""
    try:
        print(f"SCHEMA {label} type={type(table).__name__} shape={table.num_rows}x{table.num_columns}", flush=True)
        print(f"SCHEMA {label} column_names={table.column_names}", flush=True)
        for field in table.schema:
            print(f"SCHEMA {label} field {field.name}: {field.type}", flush=True)
        has_pandas_meta = table.schema.metadata is not None and b"pandas" in table.schema.metadata
        print(f"SCHEMA {label} pandas_metadata_present={has_pandas_meta}", flush=True)
        print(
            f"SCHEMA {label} pandas_index_name={frame.index.name!r} pandas_index_dtype={frame.index.dtype} "
            f"pandas_columns={list(frame.columns)}",
            flush=True,
        )
        # Does the index survive as a column, and with the same values?
        index_name = table.column_names[0]
        index_equal = bool(
            np.array_equal(
                table.column(index_name).to_pandas().to_numpy(),
                frame.index.to_numpy(),
            )
        )
        print(f"SCHEMA {label} index_as_column={index_name!r} index_values_equal={index_equal}", flush=True)
    except Exception as exc:  # noqa: BLE001
        print(f"SCHEMA {label} FAILED {exc!r}", flush=True)
        traceback.print_exc(file=sys.stdout)


def main():
    try:
        import pyarrow as pa

        print(f"pyarrow {pa.__version__}", flush=True)
    except Exception as exc:  # noqa: BLE001
        print(f"PYARROW MISSING {exc!r} — the arrow arms cannot run", flush=True)
        raise

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
    for name in names + ["tiny"]:
        try:
            ac.delete_library(name)
        except Exception:  # noqa: BLE001 — a fresh prefix has nothing to delete
            pass

    options = LibraryOptions(rows_per_segment=ROWS_PER_SEGMENT)
    libraries = {name: ac.create_library(name, options) for name in names}
    libraries["tiny"] = ac.create_library("tiny", options)

    index = pd.date_range("2020-01-01", periods=ROWS, freq="s")
    rng = np.random.default_rng(121)  # gha 121's seed, so the bytes are its bytes

    # --- the numeric controls -------------------------------------------------
    started = time.perf_counter()
    libraries["n1"].write("sym", pd.DataFrame({"c0": rng.random(ROWS)}, index=index))
    print(f"WROTE n1 in {time.perf_counter() - started:.1f}s", flush=True)

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

    # --- the correctness symbol ----------------------------------------------
    started = time.perf_counter()
    tiny_index = pd.date_range("2020-01-01", periods=TINY_ROWS, freq="s")
    tiny_pool = string_pool(TINY_DISTINCT)
    tiny_values = tiny_pool[rng.integers(0, TINY_DISTINCT, TINY_ROWS)]
    tiny_values = np.where(np.arange(TINY_ROWS) % 7 == 0, None, tiny_values)
    libraries["tiny"].write("sym", pd.DataFrame({"s": tiny_values}, index=tiny_index))
    del tiny_values, tiny_pool
    print(f"WROTE tiny ({TINY_DISTINCT} distinct, {TINY_ROWS} rows, every 7th None) "
          f"in {time.perf_counter() - started:.1f}s", flush=True)

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
    # Format inside arm inside replicate: the two formats of one arm are read
    # back to back, so a drift in the bucket's mood is the most nearly shared
    # between the two numbers the ticket compares.
    for replicate in REPLICATES:
        for name in names:
            for fmt in ARM_FORMATS[name]:
                try:
                    with window(f"{name}_{fmt}_{replicate}"):
                        libraries[name].read("sym", **read_kwargs(fmt))
                except Exception as exc:  # noqa: BLE001 — one arm must not cost the lane
                    print(f"WINDOW_FAILED {name}_{fmt}_{replicate} {exc!r}", flush=True)
                    traceback.print_exc(file=sys.stdout)

    print("SPANLOAD " + " ".join(f"{k}={v:.3f}s" for k, v in timings.items()), flush=True)

    # --- correctness, after the last window has closed -------------------------
    print("CORRECTNESS_BEGIN", flush=True)
    try:
        tiny_pandas = libraries["tiny"].read("sym").data
        for fmt in ("arrow", "arrowcat"):
            tiny_arrow = libraries["tiny"].read("sym", **read_kwargs(fmt)).data
            describe_table(f"tiny/{fmt}", tiny_arrow, tiny_pandas)
            compare(f"tiny/{fmt}", tiny_pandas, tiny_arrow, "s")
            compare_aggregates(f"tiny/{fmt}", tiny_pandas, tiny_arrow, "s")
            del tiny_arrow
        del tiny_pandas
    except Exception as exc:  # noqa: BLE001
        print(f"CORRECTNESS tiny FAILED {exc!r}", flush=True)
        traceback.print_exc(file=sys.stdout)

    try:
        big_pandas = libraries["s1m"].read("sym").data
        for fmt in ("arrow", "arrowcat"):
            big_arrow = libraries["s1m"].read("sym", **read_kwargs(fmt)).data
            describe_table(f"s1m/{fmt}", big_arrow, big_pandas)
            compare_aggregates(f"s1m/{fmt}", big_pandas, big_arrow, "s")
            compare(f"s1m/{fmt}", big_pandas, big_arrow, "s")
            del big_arrow
        del big_pandas
    except Exception as exc:  # noqa: BLE001
        print(f"CORRECTNESS s1m FAILED {exc!r}", flush=True)
        traceback.print_exc(file=sys.stdout)

    try:
        n6_pandas = libraries["n6"].read("sym").data
        n6_arrow = libraries["n6"].read("sym", **read_kwargs("arrow")).data
        describe_table("n6/arrow", n6_arrow, n6_pandas)
        compare("n6/arrow", n6_pandas, n6_arrow, "s")
        compare_aggregates("n6/arrow", n6_pandas, n6_arrow, "s")
        del n6_pandas, n6_arrow
    except Exception as exc:  # noqa: BLE001
        print(f"CORRECTNESS n6 FAILED {exc!r}", flush=True)
        traceback.print_exc(file=sys.stdout)
    print("CORRECTNESS_END", flush=True)


if __name__ == "__main__":
    main()
