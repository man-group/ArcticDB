import numpy as np
import pandas as pd
from arcticdb.toolbox.query_stats import query_stats, get_query_stats, reset_stats


def test_batch_read_not_slower_than_serial_wide_schema(lmdb_library_dynamic_schema):
    # Regression test for the O(N) realloc-per-column bug in Buffer::ensure
    # that caused `read_batch` to run ~2x slower than serial `read` calls for
    # wide schemas. The per-column reallocs contend on the process heap across
    # the batch threads and the total time inside `LMDB_SegmentFromBytes`
    # (which wraps the decode + per-column allocations) balloons.
    #
    # We use query stats rather than wall time so the signal is deterministic
    # and independent of storage-fetch noise. `LMDB_SegmentFromBytes` is a
    # sum-across-threads total, so:
    #   - With the fix, batch total ≈ serial total (same work, just scheduled
    #     in parallel; each op costs the same wall time whether alone or
    #     concurrent).
    #   - Without the fix, batch total >> serial total because each op stalls
    #     waiting on the heap mutex and that wait is included inside the timer.
    #
    # Observed (release): with fix ~1.0x, without fix ~6x.

    num_symbols = 10
    num_rows = 30
    num_cols = 30_000
    # Ratio threshold: batch total segment-from-bytes time vs serial total.
    # Measured on release build at 30k cols x 10 syms:
    #   with the fix:    2.2x - 4.6x  (some heap contention + noise)
    #   without the fix: 16x - 49x    (catastrophic mutex starvation)
    # 8x sits comfortably between, catching the regression while tolerating
    # worst-case noise on a fixed build.
    ratio_threshold = 8.0

    lib = lmdb_library_dynamic_schema
    local_rng = np.random.default_rng(seed=0)
    df = pd.DataFrame({f"col_{idx}": local_rng.random(num_rows) for idx in range(num_cols)})
    symbols = [f"sym_{idx}" for idx in range(num_symbols)]
    for sym in symbols:
        lib.write(sym, df)

    def segment_from_bytes_total_ms(stats):
        # Sum across all key types (TABLE_DATA, TABLE_INDEX, VERSION_REF, ...).
        op_stats = stats.get("storage_operations", {}).get("LMDB_SegmentFromBytes", {})
        return sum(kt.get("total_time_ms", 0) for kt in op_stats.values())

    # Warmup so OS page cache / JITs / allocator state are comparable.
    for sym in symbols:
        lib.read(sym)
    lib.read_batch(symbols)

    reset_stats()
    with query_stats():
        for sym in symbols:
            lib.read(sym)
    serial_ms = segment_from_bytes_total_ms(get_query_stats())

    reset_stats()
    with query_stats():
        lib.read_batch(symbols)
    batch_ms = segment_from_bytes_total_ms(get_query_stats())

    ratio = batch_ms / serial_ms if serial_ms else float("inf")
    print(f"wide-schema regression: LMDB_SegmentFromBytes serial={serial_ms}ms, batch={batch_ms}ms, ratio={ratio:.2f}")
    assert ratio < ratio_threshold, (
        f"read_batch's LMDB_SegmentFromBytes total ({batch_ms}ms) is "
        f"{ratio:.2f}x serial's ({serial_ms}ms) for {num_symbols} symbols of "
        f"{num_cols} columns. Expected <{ratio_threshold}x. This suggests the "
        f"Buffer::ensure geometric-growth fix has regressed and per-column "
        f"reallocs are contending on the heap across batch threads."
    )
