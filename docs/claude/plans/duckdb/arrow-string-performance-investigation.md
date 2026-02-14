# Arrow String Conversion Performance Investigation

## Problem

SQL queries via `lib.sql()` are 5-7x slower than `lib.read()` for DataFrames with string columns. Even `lib.read(output_format='pyarrow')` is 4x slower than the pandas path for strings.

## Profiling Results (1M rows, 9 columns: 3 string + 6 numeric)

| Path | Time | Notes |
|------|------|-------|
| `lib.read()` pandas | 0.69s | Baseline — strings stay as pool offsets until pandas conversion |
| `lib.read(output_format='pyarrow')` | 2.81s | Pre-existing Arrow string bottleneck |
| `lib.sql()` SELECT * pandas | 4.95s | Lazy iterator + DuckDB + pandas conversion |
| `lib.sql()` SELECT * arrow | 4.55s | Lazy iterator + DuckDB, no pandas conversion |

### Numeric-only (4 columns, no strings)

| Path | Time |
|------|------|
| `lib.read()` pandas | 0.01s |
| `lib.read(output_format='pyarrow')` | 0.34s |
| `lib.sql()` SELECT * | 0.45s |

Numeric overhead is modest (0.34s Arrow vs 0.01s pandas). The string columns dominate.

### Per-segment breakdown (lazy iterator, 10 segments)

| Step | Time |
|------|------|
| Storage read + decompress | ~0.06s |
| `prepare_segment_for_arrow()` | ~0.42s per segment |
| Total 10 segments | ~4.2s |

## Root Cause

### ArcticDB String Pool Architecture

ArcticDB stores strings as offsets into a shared string pool (dictionary encoding). The pandas path resolves these lazily during final DataFrame construction. The Arrow path must resolve them eagerly:

1. **`prepare_segment_for_arrow()`** (`arrow_output_frame.cpp:53-117`): Called per-segment in the lazy iterator. For each string column:
   - Creates a new DETACHABLE column
   - Calls `ArrowStringHandler::convert_type()` to resolve pool offsets → Arrow string buffers
   - 10 segments × 3 string columns = 30 expensive resolution operations

2. **`segment_to_arrow_data()`**: Used by both the eager `RecordBatchIterator` and `lib.read(output_format='pyarrow')`. Also resolves strings per-segment.

3. **Normal pandas pipeline** (`reduce_and_fix_columns()`): Merges all segments first, then resolves strings once on the merged frame. This is faster because the string pool is shared across the merged segment.

### Why prefetch doesn't help

Tested prefetch sizes 2, 10, 50 — no difference. The bottleneck is CPU-bound string resolution, not storage I/O latency.

### Pre-existing issue

The 2.81s for `lib.read(output_format='pyarrow')` shows this is NOT a lazy iterator regression — the Arrow string conversion has always been expensive. The lazy iterator adds ~2x on top due to per-segment overhead.

## Fix Options Evaluated

### Option A: Use merged frame for Arrow export
- **Approach**: Merge all segments first (like pandas path), then convert to Arrow once
- **Pros**: Single string resolution pass
- **Cons**: Materializes entire dataset in memory — defeats streaming benefit for large/remote data
- **Verdict**: Rejected for SQL path

### Option B: Resolve strings during decode (before `prepare_segment_for_arrow`)
- **Approach**: Move string resolution into `DecodeSliceTask` so decoded segments already have Arrow-ready strings
- **Pros**: Resolution happens in parallel decode tasks; eliminates per-segment overhead in iterator
- **Cons**: Same total work, just relocated. Would save the ~0.5s per-segment overhead but not the core 2.8s
- **Risk**: Could affect normal (non-SQL) read performance if decode tasks become slower
- **Verdict**: Marginal improvement, doesn't address root cause

### Option C: Batch string resolution across segments
- **Approach**: Accumulate string pools from multiple segments, resolve in larger batches
- **Pros**: Better cache locality, fewer allocations
- **Cons**: Complex implementation, same total bytes processed
- **Verdict**: Modest improvement possible

### Option D: Dictionary-encoded Arrow export (recommended future work)
- **Approach**: Export string columns as Arrow `DictionaryArray` — the string pool IS the dictionary, offsets map directly
- **Pros**: Near zero-copy for string data; DuckDB handles dictionary arrays natively
- **Cons**: Major C++ change to Arrow export path; dictionary arrays have different semantics for consumers
- **Verdict**: Best long-term solution — addresses root cause by eliminating string resolution entirely

### Option E: Hybrid approach for local storage
- **Approach**: For LMDB (local, fast storage), use eager `lib.read(output_format='pyarrow')` path (~2.8s) instead of lazy iterator (~5s)
- **Pros**: Immediate 1.7x speedup for local storage; no C++ changes
- **Cons**: Doesn't help remote storage (S3); two code paths to maintain
- **Verdict**: Quick win, but band-aid

### Option F: SharedStringDictionary — build pool mapping once per segment (implemented)
- **Approach**: Walk the StringPool once per segment to build `offset→dict_index` mapping + Arrow dictionary buffers. Each string column then does read-only hash map lookups (no insert), sharing the dictionary buffers.
- **Implementation**: `SharedStringDictionary` struct + `build_shared_dictionary()` + `encode_dictionary_with_shared_dict()` in `arrow_output_frame.cpp`
- **Results**: **No significant improvement**. Varying unique string count from 10 to 10,000 showed constant ~0.12s per 100K rows — hash map operations are NOT the bottleneck.
- **Root cause confirmed**: The per-row Arrow buffer operations (iteration via `for_each_enumerated`, offset writes, null tracking) dominate. Lazy iterator (0.120s/100K) is already competitive with `lib.read(pyarrow)` (0.134s/100K). The 3x gap to pandas (0.046s/100K) is inherent to Arrow dictionary encoding format.
- **Verdict**: Kept in code (marginally cleaner, slightly smaller hash maps), but does not address root cause

## Current Status

- Investigation complete, findings documented
- SharedStringDictionary optimization implemented — marginal improvement only
- The `lazy=False` simplification (always-lazy) is implemented but needs a C++ rebuild to test the `descriptor()` binding
- **Conclusion**: Only Option D (dictionary-encoded Arrow export) can meaningfully improve string performance by eliminating per-row resolution entirely

## Key Files

| File | Relevance |
|------|-----------|
| `cpp/arcticdb/arrow/arrow_output_frame.cpp:53-117` | `prepare_segment_for_arrow()` — main bottleneck |
| `cpp/arcticdb/arrow/arrow_output_frame.cpp:324-380` | `LazyRecordBatchIterator::next()` |
| `cpp/arcticdb/arrow/arrow_output_frame.hpp:161-214` | `LazyRecordBatchIterator` class |
| `cpp/arcticdb/pipeline/pipeline_utils.hpp:86` | Eager path: `segment_to_arrow_data()` |
| `cpp/arcticdb/column_store/string_reducers.hpp` | `ArrowStringHandler::convert_type()` |
