# Lazy Iterator vs Making the Eager Pipeline Lazy

**Question**: Could the existing eager read pipeline be made lazy (streaming) instead of maintaining two separate paths (eager + lazy iterator)?

**Verdict**: The plan's dual-path approach is the right call. Making the eager pipeline fully lazy would regress pandas performance by 3-60x on common workloads and require a fundamental redesign of the clause processing system.

---

## How the Eager Pipeline Works Today

The eager read path follows a rigid materialization sequence:

1. **`setup_pipeline_context()`** — collects segment keys, builds column/row range maps
2. **`allocate_frame()`** — pre-allocates the **entire** output buffer (one contiguous `SegmentInMemory`)
3. **`fetch_data()`** → `batch_read_compressed()` — issues **all** segment reads in parallel, decodes each segment directly into its pre-allocated row offset
4. **`reduce_and_fix_columns()`** — backfills nulls across the full frame (dynamic schema)
5. **`create_python_read_result()`** — zero-copy handoff to pandas (DETACHABLE pointer swap)

Key code locations:
- `version_core.cpp:2061-2088` — `do_direct_read_or_process()` fork
- `read_frame.cpp:109-158` — `allocate_contiguous_frame()` / `allocate_chunked_frame()`
- `read_frame.cpp:1071-1116` — `fetch_data()` with `batch_read_compressed()`
- `read_frame.cpp:821-929` — `NullValueReducer` backfill pass

### Why This Pattern Is Fast

- **Single allocation**: One `malloc` for the entire column, not N allocations per segment
- **Scatter-write decode**: Each segment decodes directly into its row-offset slot — no intermediate buffers, no copies
- **Zero-copy to pandas**: The `DETACHABLE` flag lets pandas take ownership of the buffer pointer — no memcpy
- **Parallel I/O**: `batch_read_compressed()` issues all storage reads in one call; `folly::collect()` waits for all

---

## Component-by-Component Analysis

### A. Direct Reads (No Clauses)

| Aspect | Plan's Approach (Dual Path) | Making Eager Lazy |
|--------|----------------------------|-------------------|
| pandas output | Pre-allocated scatter-write, zero-copy | Must concat N Arrow batches → 1 extra full copy minimum |
| Arrow/DuckDB output | Lazy iterator, one batch at a time | Same streaming benefit |
| Memory peak | Full symbol in memory (pandas) / 1 batch (Arrow) | Full symbol either way (pandas needs final concat) |
| Dynamic schema | `NullValueReducer` over full frame | Must buffer all batches to discover union schema, then pad |
| I/O parallelism | `batch_read_compressed()` — all keys at once | Must implement prefetch window (plan already has this) |

**Making eager lazy for direct reads would regress pandas by ~2-4x** (extra allocation + copy for concat) and gain nothing — pandas needs the full frame anyway. The lazy iterator already handles Arrow/DuckDB efficiently.

### B. Filter / Project Clauses

These are inherently streaming-compatible:
- `FilterClause::process()` (`clause.cpp:254-281`): `structure_by_row_slice`, processes one segment at a time
- `ProjectClause::process()` (`clause.cpp:298-355`): same pattern

Both paths could stream here. But the clause scheduling system (`version_core.cpp:810-848`) uses synchronous barriers between iterations (`folly::collect()` on all segments), which prevents true streaming without a redesign of the scheduling loop.

### C. GROUP BY / Aggregation

`AggregationClause::process()` (`clause.cpp:488-687`):
- Uses `ProcessingStructure::HASH_BUCKETED`
- Builds a **global `GroupingMap`** shared across all segments
- Each segment's `process()` inserts into shared hash buckets
- `finalize()` produces the final aggregated result

**This fundamentally requires full materialization.** A streaming GROUP BY would need:
- Watermarks to know when a group key is "complete"
- Incremental aggregation state per key
- A complete redesign of `GroupingMap` lifecycle

Neither approach avoids this — the plan correctly doesn't try to stream GROUP BY.

### D. Resample

`ResampleClause::process()` (`clause.cpp:986-1093`):
- Uses `ProcessingStructure::TIME_BUCKETED`
- Already partially streaming: processes data per time bucket
- But needs **all data within each bucket range** before producing output

A lazy pipeline wouldn't improve this — the bottleneck is bucket completeness, not materialization strategy. The plan's approach (eager pipeline handles resample) is correct.

### E. Merge (as_of / sorted reads)

`MergeClause` (`clause.cpp:1342-1426`):
- Uses `ProcessingStructure::ALL` — needs every segment head available
- Priority queue merge-sort across all segments
- `process()` is a NO-OP; all work happens in the scheduling/merge phase

**This is the most hostile to streaming.** You need all segment iterators simultaneously for merge-sort. Neither approach can avoid loading all data.

---

## Performance Comparison

| Workload | Plan (Dual Path) | Single Lazy Path |
|----------|------------------|------------------|
| `lib.read(sym)` → pandas | **~1x** (scatter-write, zero-copy) | **~2-4x slower** (concat overhead) |
| `lib.read(sym)` → Arrow | ~1x (lazy iterator) | ~1x (same) |
| `lib.sql("SELECT * FROM sym")` | ~1x (lazy iterator → DuckDB) | ~1x (same) |
| `lib.sql("SELECT ... WHERE ...")` | ~1x (DuckDB pushdown) | ~1x (same) |
| `lib.read(sym, columns=[...])` → pandas | **~1x** (scatter-write subset) | **~1.5-2x slower** |
| Wide table (>127 cols) → pandas | ~1x (column-slice merge in C++) | ~1x (same merge needed) |
| Dynamic schema read | ~1x (`NullValueReducer`) | ~1.2x slower (double-pass: discover schema + pad) |
| GROUP BY / resample | ~1x (clause pipeline) | ~1x (same — must materialize either way) |

---

## Why Two Paths Are Necessary

1. **The pandas zero-copy path cannot be made lazy.** `allocate_frame()` needs total row count upfront. `decode_into_frame_static()` writes directly into a pre-allocated buffer at a row offset. Converting this to streaming would eliminate both the single-allocation and scatter-write optimizations, adding at minimum one full-frame copy.

2. **`reduce_and_fix_columns()` is inherently non-streaming.** The `NullValueReducer` walks column-by-column across all row ranges to discover which columns need null backfill. It needs the full frame allocated and all segments decoded before it can start.

3. **The clause scheduling loop has synchronous barriers.** `schedule_remaining_iterations()` (`version_core.cpp:810-848`) collects all processing units per iteration before scheduling the next. Making this streaming would require rewriting the entire scheduling system with an async producer-consumer pattern.

4. **The lazy iterator already exists and works.** `LazyRecordBatchIterator` with prefetch futures provides the streaming semantics needed for DuckDB/Arrow consumers. The plan just needs to harden it (schema padding, column-slice merging, backpressure).

## Benefits of the Plan's Approach

- **Zero regression risk** for existing pandas users
- **Incremental implementation** — each step is independently testable
- **Shared helpers** between paths (schema padding, column-slice merge) avoid code duplication
- **Clean separation of concerns** — eager path optimizes for pandas, lazy path optimizes for Arrow/DuckDB

## Downsides of the Plan's Approach

- **Two code paths to maintain** — bug fixes may need to be applied in both places
- **Testing surface doubles** — need to verify both paths handle edge cases (dynamic schema, wide tables, empty results, etc.)
- **Potential for divergence** — over time the paths may drift if not kept in sync
- **`setup_pipeline_context()` is shared** but downstream logic branches — adds complexity to the read flow

## Mitigations for Downsides

- Shared C++ helpers (schema padding, column-slice merge) reduce duplication
- The plan's test matrix covers both paths systematically
- The lazy path reuses `setup_pipeline_context()` and segment key resolution — only the materialization strategy differs
