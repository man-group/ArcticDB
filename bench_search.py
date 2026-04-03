"""Benchmark binary search + slice construction on PyArrow ChunkedArray."""

import time
import numpy as np
import pyarrow as pa


def make_table(num_chunks, rows_per_chunk):
    """Create a sorted table with timestamp[ns] index and an int column."""
    base = np.datetime64("2020-01-01", "ns").view("int64")
    ts_arrays = []
    int_arrays = []
    offset = 0
    for _ in range(num_chunks):
        vals = np.arange(offset, offset + rows_per_chunk, dtype="int64")
        ts_arrays.append(pa.array((vals + base).view("datetime64[ns]"), type=pa.timestamp("ns")))
        int_arrays.append(pa.array(vals, type=pa.int64()))
        offset += rows_per_chunk
    return pa.table({
        "ts": pa.chunked_array(ts_arrays),
        "val": pa.chunked_array(int_arrays),
    })


def approach_combine_chunks(table, start_np, end_np):
    """combine_chunks + numpy searchsorted + table.slice."""
    col = table.column("ts")
    arr = col.combine_chunks().to_numpy(zero_copy_only=False)
    left = int(np.searchsorted(arr, start_np, side="left"))
    right = int(np.searchsorted(arr, end_np, side="right"))
    return table.slice(left, right - left)


def approach_python_bisect(table, start_py, end_py):
    """Python bisect on ChunkedArray + table.slice."""
    col = table.column("ts")
    n = len(col)

    lo, hi = 0, n
    while lo < hi:
        mid = (lo + hi) // 2
        if col[mid].as_py() < start_py:
            lo = mid + 1
        else:
            hi = mid
    left = lo

    lo, hi = 0, n
    while lo < hi:
        mid = (lo + hi) // 2
        if col[mid].as_py() <= end_py:
            lo = mid + 1
        else:
            hi = mid
    right = lo

    return table.slice(left, right - left)


def approach_chunk_bisect_manual_view(table, start_np, end_np):
    """Bisect chunk boundaries, then assemble new table from sliced first/last chunks."""
    col = table.column("ts")
    chunks = col.chunks
    num_chunks = len(chunks)

    # Binary search for start chunk: first chunk whose last value >= start_np
    lo, hi = 0, num_chunks
    while lo < hi:
        mid = (lo + hi) // 2
        if chunks[mid][-1].as_py().asm8 < start_np:
            lo = mid + 1
        else:
            hi = mid
    start_chunk_idx = lo

    # Binary search for end chunk: last chunk whose first value <= end_np
    lo, hi = 0, num_chunks
    while lo < hi:
        mid = (lo + hi) // 2
        if chunks[mid][0].as_py().asm8 <= end_np:
            lo = mid + 1
        else:
            hi = mid
    end_chunk_idx = lo - 1

    if start_chunk_idx >= num_chunks or end_chunk_idx < 0 or start_chunk_idx > end_chunk_idx:
        return table.slice(0, 0)

    # searchsorted within start and end chunks
    start_arr = chunks[start_chunk_idx].to_numpy(zero_copy_only=True)
    left_local = int(np.searchsorted(start_arr, start_np, side="left"))

    end_arr = chunks[end_chunk_idx].to_numpy(zero_copy_only=True)
    right_local = int(np.searchsorted(end_arr, end_np, side="right"))

    # Assemble new chunked arrays per column by slicing first/last chunks
    new_columns = {}
    for name in table.column_names:
        col_chunks = table.column(name).chunks
        if start_chunk_idx == end_chunk_idx:
            new_chunks = [col_chunks[start_chunk_idx].slice(left_local, right_local - left_local)]
        else:
            new_chunks = [col_chunks[start_chunk_idx].slice(left_local)]
            for i in range(start_chunk_idx + 1, end_chunk_idx):
                new_chunks.append(col_chunks[i])
            new_chunks.append(col_chunks[end_chunk_idx].slice(0, right_local))
        new_columns[name] = pa.chunked_array(new_chunks)
    return pa.table(new_columns)


def benchmark(func, args, n_iter=100):
    # Warmup
    for _ in range(5):
        func(*args)
    start = time.perf_counter()
    for _ in range(n_iter):
        result = func(*args)
    elapsed = (time.perf_counter() - start) / n_iter
    return elapsed, result


def run_benchmarks(num_chunks, rows_per_chunk):
    total_rows = num_chunks * rows_per_chunk
    table = make_table(num_chunks, rows_per_chunk)
    col = table.column("ts")
    assert len(col) == total_rows

    mid = total_rows // 2
    quarter = total_rows // 4
    start_py = col[mid - quarter].as_py()
    end_py = col[mid + quarter].as_py()
    start_np = start_py.asm8
    end_np = end_py.asm8

    # Reference
    ref = approach_combine_chunks(table, start_np, end_np)
    ref_len = len(ref)

    print(f"\n{'='*70}")
    print(f"chunks={num_chunks}, rows/chunk={rows_per_chunk}, total={total_rows}")
    print(f"expected slice length: {ref_len}")
    print(f"{'='*70}")

    approaches = [
        ("combine_chunks + searchsorted + slice", approach_combine_chunks, (table, start_np, end_np)),
        ("python bisect + table.slice", approach_python_bisect, (table, start_py, end_py)),
        ("chunk bisect + manual view", approach_chunk_bisect_manual_view, (table, start_np, end_np)),
    ]

    n_iter = 100
    for name, func, args in approaches:
        t, r = benchmark(func, args, n_iter)
        ok = "OK" if len(r) == ref_len else f"WRONG(len={len(r)})"
        print(f"  {name:42s} {t*1e6:10.1f} µs  {ok}")


if __name__ == "__main__":
    run_benchmarks(num_chunks=3, rows_per_chunk=10_000_000)
    run_benchmarks(num_chunks=10, rows_per_chunk=10_000_000)
    run_benchmarks(num_chunks=100, rows_per_chunk=1_000_000)
    run_benchmarks(num_chunks=1000, rows_per_chunk=100_000)
    run_benchmarks(num_chunks=10000, rows_per_chunk=10_000)
    run_benchmarks(num_chunks=1, rows_per_chunk=100_000_000)
