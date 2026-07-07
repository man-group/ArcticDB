"""
Copyright 2026 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

# Memory benchmark for the pandas string dtype vs the established pyarrow path.
#
# The same string columns/values are written and read four ways:
#   - object: pandas object dtype (future.infer_string off)
#   - str:    pandas str dtype    (future.infer_string on)   -- the new path
#   - arrow:  a pyarrow Table in / out (output_format="pyarrow") -- the established, already-tested path
#   - polars: a polars DataFrame in / out (output_format="polars") -- also arrow-backed
# Each is built natively in its format (no dtype conversion in the benchmark), so the write peak reflects
# ArcticDB's actual cost rather than input construction. WRITE and READ each run in their own subprocess so the
# peaks are isolated (a read peak is never contaminated by the write and vice versa).
#
# col mem: pandas memory_usage(deep=True) for object/str; Table.get_total_buffer_size() for arrow;
#          DataFrame.estimated_size() for polars.
#
# Run from the python/ dir: PYTHONPATH=. python mem_infer_string.py
# Override the temp/storage dir (needs room for the LMDB data) with MEM_BENCH_TMPDIR.

FORMATS = ("object", "str", "arrow", "polars")

import os
import resource
import subprocess
import sys
import tempfile

SCENARIOS = {
    # label: (num_rows, string_length, num_cols)
    "small": (2_000_000, 8, 1),
    "large": (100_000, 1_000, 1),
}
SEED = 17


def _peak_rss_mb() -> float:
    # High-water RSS of this process over its lifetime; ru_maxrss is in KiB on Linux.
    return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024


def _random_bytes(num_rows: int, length: int, seed: int) -> bytes:
    import numpy as np

    rng = np.random.default_rng(seed)
    return (rng.integers(0, 26, size=num_rows * length, dtype=np.uint8) + ord("a")).tobytes()


def _make_object_column(num_rows: int, length: int, seed: int):
    # Native object column: Python str objects straight from the byte buffer (no dtype conversion later).
    buf = _random_bytes(num_rows, length, seed)
    return [buf[i * length : (i + 1) * length].decode("ascii") for i in range(num_rows)]


def _make_numeric_column(num_rows: int, seed: int):
    # A float64 column: identical representation across all formats, so it acts as a control.
    import numpy as np

    return np.random.default_rng(seed).random(num_rows)


def _make_str_column(num_rows: int, length: int, seed: int):
    # Native str column: build the Arrow large_string from raw buffers (no Python-object intermediate, no
    # conversion), then adopt it into the str dtype zero-copy. This isolates ArcticDB's write cost from the
    # cost of constructing str data.
    import numpy as np
    import pyarrow as pa
    import pandas as pd

    data = _random_bytes(num_rows, length, seed)
    offsets = np.arange(0, (num_rows + 1) * length, length, dtype=np.int64).tobytes()
    pa_arr = pa.LargeStringArray.from_buffers(num_rows, pa.py_buffer(offsets), pa.py_buffer(data))
    return pd.array(pa_arr, dtype=pd.StringDtype(storage="pyarrow", na_value=np.nan))


def _lib(lib_path: str):
    from arcticdb import Arctic

    return Arctic(f"lmdb://{lib_path}").get_library("bench", create_if_missing=True)


def _sym(label: str, fmt: str) -> str:
    return f"{label}_{fmt}"


def _arrow_table(label: str):
    import pyarrow as pa

    num_rows, length, num_cols = SCENARIOS[label]
    data = {f"s{c}": _make_str_column(num_rows, length, SEED + c)._pa_array for c in range(num_cols)}
    data["num"] = pa.array(_make_numeric_column(num_rows, SEED + 100))
    return pa.table(data)


def _write(lib_path: str, label: str, fmt: str):
    import pandas as pd

    lib = _lib(lib_path)
    num_rows, length, num_cols = SCENARIOS[label]
    if fmt == "arrow":
        lib._nvs._set_allow_arrow_input()
        payload = _arrow_table(label)
    elif fmt == "polars":
        import polars as pl

        lib._nvs._set_allow_arrow_input()
        payload = pl.from_arrow(_arrow_table(label))
    else:
        make = _make_str_column if fmt == "str" else _make_object_column
        cols = {f"s{c}": make(num_rows, length, SEED + c) for c in range(num_cols)}
        cols["num"] = _make_numeric_column(num_rows, SEED + 100)
        payload = pd.DataFrame(cols)
    lib.write(_sym(label, fmt), payload)
    print(f"WRITE\t{_peak_rss_mb():.1f}")


def _read(lib_path: str, label: str, fmt: str):
    import pandas as pd

    lib = _lib(lib_path)
    sym = _sym(label, fmt)
    if fmt == "arrow":
        table = lib.read(sym, output_format="pyarrow").data
        col_mem = table.get_total_buffer_size() / (1024 * 1024)
        dtype = str(table.schema.field(0).type)
    elif fmt == "polars":
        pl_df = lib.read(sym, output_format="polars").data
        col_mem = pl_df.estimated_size() / (1024 * 1024)
        dtype = str(pl_df.dtypes[0])
    else:
        with pd.option_context("future.infer_string", fmt == "str"):
            df = lib.read(sym).data
        col_mem = df.memory_usage(deep=True).sum() / (1024 * 1024)
        dtype = str(df.iloc[:, 0].dtype)
    print(f"READ\t{dtype}\t{col_mem:.1f}\t{_peak_rss_mb():.1f}")


def _run(mode: str, lib_path: str, label: str, fmt: str) -> list:
    marker = "WRITE" if mode == "--write" else "READ"
    out = subprocess.run(
        [sys.executable, __file__, mode, lib_path, label, fmt],
        capture_output=True,
        text=True,
    )
    lines = [l for l in out.stdout.splitlines() if l.startswith(marker + "\t")]
    if not lines:
        raise RuntimeError(f"{mode} failed for {label} fmt={fmt}:\n{out.stdout}\n{out.stderr}")
    return lines[-1].split("\t")[1:]


def main():
    with tempfile.TemporaryDirectory(dir=os.environ.get("MEM_BENCH_TMPDIR")) as lib_path:
        header = (
            f"{'scenario':<8} {'format':<8} {'dtype':<14} "
            f"{'col mem (MiB)':>13} {'write peak (MiB)':>17} {'read peak (MiB)':>16}"
        )
        print(header)
        print("-" * len(header))
        for label in SCENARIOS:
            for fmt in FORMATS:
                (write_peak,) = _run("--write", lib_path, label, fmt)
                dtype, col_mem, read_peak = _run("--read", lib_path, label, fmt)
                print(
                    f"{label:<8} {fmt:<8} {dtype:<14} "
                    f"{float(col_mem):>13.1f} {float(write_peak):>17.1f} {float(read_peak):>16.1f}"
                )
            print()
        for label, (num_rows, length, num_cols) in SCENARIOS.items():
            print(f"  {label}: {num_rows:,} rows x {num_cols} string col(s) ({length} chars) + 1 numeric col")


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--write":
        _write(sys.argv[2], sys.argv[3], sys.argv[4])
    elif len(sys.argv) > 1 and sys.argv[1] == "--read":
        _read(sys.argv[2], sys.argv[3], sys.argv[4])
    else:
        main()
