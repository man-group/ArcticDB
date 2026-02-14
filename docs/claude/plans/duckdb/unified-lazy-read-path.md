# Unified Lazy Read Path: DuckDB, Polars, and Arrow

## Problem Statement

The C++ read path has two parallel flows that share `setup_pipeline_context()` then diverge completely. The lazy iterator built for DuckDB is strictly more general than the eager path for Arrow output, and is exactly what Polars LazyFrame needs. Unifying these paths would reduce duplication, simplify the Python `sql()` interface, and give Polars/Arrow reads streaming (bounded-memory) semantics for free.

> **Design rationale**: See [lazy-vs-eager-analysis.md](lazy-vs-eager-analysis.md) for a detailed analysis of why a dual-path approach (eager for pandas, lazy iterator for Arrow/DuckDB) is preferred over making the existing eager pipeline lazy. Key finding: the eager path's pre-allocated scatter-write pattern provides 2-4x performance advantage for pandas that cannot be preserved in a streaming model.

---

## Current Architecture: Two Divergent Paths

### Eager Path (`read_dataframe_version` → [`version_core.cpp:2714`](../../../../cpp/arcticdb/version/version_core.cpp#L2714))

```
setup_pipeline_context()
    → do_direct_read_or_process()                    [version_core.cpp:2061]
        → allocate_frame() + fetch_data()             [direct reads, no clauses]
        → read_process_and_collect()                  [with clauses: GROUP BY, resample]
    → reduce_and_fix_columns()
    → create_python_read_result()                     [pipeline_utils.hpp:49]
        → OutputFormat::ARROW  → segment_to_arrow_data(full_frame)
        → OutputFormat::PANDAS → PandasOutputFrame{frame}
```

Loads **all segments into a single `SegmentInMemory`**, then converts in one shot. Memory = entire symbol.

The eager path avoids column-slice and dynamic-schema problems because `reduce_and_fix_columns()` + `allocate_frame()` assemble all column slices into one unified `SegmentInMemory` with a single schema before `segment_to_arrow_data()` ever runs.

### Lazy Path (`create_lazy_record_batch_iterator` → [`version_store_api.cpp:1061`](../../../../cpp/arcticdb/version/version_store_api.cpp#L1061))

```
setup_pipeline_context()
    → sort slice_and_keys_ by (row_range, col_range)
    → build columns_to_decode set
    → LazyRecordBatchIterator(slice_and_keys, store, ...)   [arrow_output_frame.cpp:342]
        → per segment: read_decode → truncate → filter → prepare_for_arrow → to_arrow
```

Reads **one segment at a time** via prefetch. Memory = prefetch_size × segment_size.

The lazy path yields one `RecordBatchData` per slice per segment, with potentially different schemas per batch. Python `ArcticRecordBatchReader` (`arrow_reader.py`) papers over this via column-slice merging (`_merge_slices_for_group`), schema padding (`_pad_batch_to_schema`), and schema discovery (`_ensure_schema`).

### Shared Setup, Duplicated Orchestration

| Step | Eager (`version_core.cpp`) | Lazy (`version_store_api.cpp`) |
|------|---------------------------|-------------------------------|
| Version resolution | `get_version_to_read()` | `get_version_to_read()` (copy-pasted) |
| Pipeline context | `setup_pipeline_context()` | `setup_pipeline_context()` (same call) |
| Slice ordering | Implicit (by col_range) | Explicit re-sort by (row_range, col_range) |
| Column projection | `overall_column_bitset_` | Copied into `cols_to_decode` set |
| Filter setup | Via `ReadQuery::clauses_` | Via `FilterClause → ExpressionContext` |
| Data fetch | `batch_read_uncompressed` (all at once) | `batch_read_uncompressed` (per segment, prefetched) |
| Arrow conversion | `segment_to_arrow_data(full_frame)` | `prepare_segment_for_arrow()` + `segment_to_arrow_data()` per segment |

---

## Why the Lazy Path is Strictly More General

The lazy iterator already does everything the eager path does for direct reads (no clauses), plus:

- **Streaming** — bounded memory via prefetch window
- **Per-segment filter clause** — `apply_filter_clause()` via `ExpressionContext`
- **Per-segment truncation** — `apply_truncation()` for date_range/row_range
- **Prefetch-based latency hiding** — parallel I/O via Folly futures

The **only** capabilities the eager path has that the lazy path lacks:

1. **Processing clauses** (GROUP BY, resample, merge) — require `read_process_and_collect()` + `ComponentManager`
2. **PandasOutputFrame** — pandas zero-copy path avoiding Arrow conversion (3–60x faster for full scans)
3. **Column-slice merging** — the eager path assembles all slices in C++; the lazy path defers this to Python
4. **Schema uniformity** — the eager path produces one schema; the lazy path can yield heterogeneous schemas across segments (dynamic schema) and slices (column tiling)

---

## Edge Case Analysis

### Critical Architecture Finding: Prefetch Runs the Full Pipeline

The current `LazyRecordBatchIterator` stores `Future<vector<RecordBatchData>>` in its prefetch buffer ([`arrow_output_frame.hpp:186`](../../../../cpp/arcticdb/arrow/arrow_output_frame.hpp#L186)). The entire chain — I/O, decode, truncation, filter, `prepare_segment_for_arrow()`, and `segment_to_arrow_data()` — runs inside a Folly future on the CPU thread pool ([`arrow_output_frame.cpp:363-413`](../../../../cpp/arcticdb/arrow/arrow_output_frame.cpp#L363-L413)). The consumer's `next()` call merely does `.get()` on the already-resolved future.

**This means a naive segment-yielding iterator (without Arrow conversion in the prefetch) would REGRESS throughput** by moving Arrow conversion out of the prefetch pipeline and into the consumer thread, serializing the dominant cost (`prepare_segment_for_arrow()`).

The architecture must preserve this property: Arrow conversion stays inside the prefetch future.

### Edge cases handled correctly by the current lazy path

| Edge Case | Current Location | Why It's Safe |
|-----------|-----------------|---------------|
| **String handling** (SharedStringDictionary, CATEGORICAL, LARGE/SMALL_STRING, UTF-32→UTF-8) | `prepare_segment_for_arrow()` ([`arrow_output_frame.cpp:204-304`](../../../../cpp/arcticdb/arrow/arrow_output_frame.cpp#L204-L304)) | Runs inside prefetch future; Arrow string handlers (`ArrowStringHandler`) registered for all string types |
| **Sparse floats / missing values** | `prepare_segment_for_arrow():292-300` — validity bitmap + `unsparsify()` | Bitmap extracted BEFORE `unsparsify()` clears sparse map; runs inside future |
| **Multi-block segments** | `pending_batches_` deque in `LazyRecordBatchIterator::next()` | `segment_to_arrow_data()` can produce multiple 64KB blocks per segment; deque drains them before pulling next future |
| **Filter clause + dynamic schema** | `apply_filter_clause()` uses `ExpressionContext::dynamic_schema_` → `EmptyResult` on missing column | Returns empty vector; `next()` skips to next future |
| **Filter → all rows removed** | `bitset.count() == 0` or `EmptyResult` → return `{}` | Segment skipped entirely, no Arrow conversion wasted |
| **Filter → all rows match** | `FullResult` variant → skip `apply_filter()` | No unnecessary copy |
| **Appends** | Transparent — `setup_pipeline_context()` discovers all segments from index | Segments from `write()` vs `append()` indistinguishable |
| **Date range / row range truncation** | `apply_truncation()` in prefetch future | Binary search O(log n) on index column; zero-row result produces one empty RecordBatch (schema preserved) |
| **Empty symbols** | `has_next()=false` immediately, `descriptor()` still valid | 0 `slice_and_keys_` → iterator exhausted; descriptor from pipeline context |
| **MultiIndex (`__idx__` prefix)** | `_strip_idx_prefix_from_names()` in Python `ArcticRecordBatchReader` | Python layer, unaffected by C++ changes |
| **Pickled data** | `!pipeline_context->multi_key_` check in `create_lazy_record_batch_iterator` | Rejected before iterator construction |
| **Multi-key (recursive normalizer) data** | `pipeline_context->multi_key_` check at [`version_store_api.cpp:1085-1088`](../../../../cpp/arcticdb/version/version_store_api.cpp#L1085-L1088) | Multi-key stores a `MULTI_KEY` index referencing leaf sub-symbols; the read pipeline detects this at [`version_core.cpp:1263-1265`](../../../../cpp/arcticdb/version/version_core.cpp#L1263-L1265) (in `read_indexed_keys_to_pipeline`), sets `multi_key_`, and `setup_pipeline_context()` returns early at [`version_core.cpp:2664-2666`](../../../../cpp/arcticdb/version/version_core.cpp#L2664-L2666) without populating `slice_and_keys_`. The lazy iterator rejects multi-key with an explicit error. Multi-key data is structurally incompatible with lazy iteration (no row/column slice metadata, requires Python-side reconstruction via `Flattener`). The eager path handles it via `read_multi_key()` ([`version_core.cpp:634-660`](../../../../cpp/arcticdb/version/version_core.cpp#L634-L660)). **Completely orthogonal to this plan.** |
| **EMPTYVAL type fields** | Decoder handles transparently; zero bytes stored, type handler reconstructs | `batch_read_uncompressed()` encounters zero-byte fields, decoder skips them |
| **RangeIndex backward compat (step=0 patch)** | `create_python_read_result()` in eager path only | Arrow/Polars don't use pandas normalization metadata; patch not needed |
| **Boolean dense packing** | [`arrow_utils.cpp:46-58`](../../../../cpp/arcticdb/arrow/arrow_utils.cpp#L46-L58) — 1 bool/byte → 8 bools/byte | Runs inside `segment_to_arrow_data()` in prefetch future |
| **Empty string dictionary** | `minimal_strings_for_dict()` inserts dummy `"a"` ([`arrow_utils.cpp:103-105`](../../../../cpp/arcticdb/arrow/arrow_utils.cpp#L103-L105)) | Workaround for Sparrow requiring ≥1 dictionary entry; only triggered for zero-row or all-null string columns |
| **Zero-row segments (after truncation)** | `segment_to_arrow_data()` returns 1 empty RecordBatch when `column_blocks == 0` | Schema preserved for DuckDB schema inference |
| **Old data with incorrect end_index** | `apply_truncation()` binary search handles `time_filter.first > last_ts` → `truncate(0, 0)` | Produces zero-row segment, not crash |
| **CPython 3.13 double `__iter__`** | `ArcticRecordBatchReader._iteration_started` flag | Python layer, must be preserved in Step 6 cleanup |
| **Filter + column projection** | [`library.py:2415`](../../../../python/arcticdb/version_store/library.py#L2415) sets `qb = None` when `columns is not None` | Filter and columns are **never both set** — filter deferred to DuckDB when columns projected |

### Edge cases that block routing `lib.read(format=Arrow)` through the lazy path

These are currently solved by Python `ArcticRecordBatchReader` for DuckDB but would break `_adapt_frame_data()` ([`_store.py:2821-2845`](../../../../python/arcticdb/version_store/_store.py#L2821-L2845)) which calls `pa.Table.from_batches(record_batches)` — requiring **all batches to have identical schemas**.

**1. Column slicing (wide tables, static schema only)**

Column slicing is controlled by `columns_per_segment` (default 127, set in `options.py`). A 400-column table is tiled into ~4 column slices per row group. The lazy iterator yields one `RecordBatchData` per slice, each with a different ~100-column schema. `pa.Table.from_batches()` raises because schemas don't match.

**Column slicing only applies to static schema libraries.** Dynamic schema libraries always use a single column slice (`columns_per_segment` is effectively infinite — see `options.py:121-122`). This is a critical distinction for Amendment A (filter pushdown).

- **Eager path avoids this**: `reduce_and_fix_columns()` + `allocate_frame()` assemble all slices into one `SegmentInMemory` before `segment_to_arrow_data()`.
- **DuckDB path avoids this**: `ArcticRecordBatchReader._merge_slices_for_group()` ([`arrow_reader.py:219-239`](../../../../python/arcticdb/version_store/duckdb/arrow_reader.py#L219-L239)) horizontally concatenates consecutive slices in Python. Detection via `_is_same_row_group()` checks: same row count, at least one new column, overlapping columns (index) have identical values.
- **Resolution**: Must be solved in C++ before `lib.read` can use the lazy path. See Step 3.

**2. Dynamic schema (different columns per segment)**

With dynamic schema, different segments have different column subsets. The lazy iterator yields batches with heterogeneous schemas. `pa.Table.from_batches()` fails.

- **Eager path avoids this**: `allocate_frame()` pre-allocates the full merged schema, `fetch_data()` fills in only the columns present per segment (rest stays zero/null). `NullValueReducer` ([`read_frame.cpp:821-929`](../../../../cpp/arcticdb/pipeline/read_frame.cpp#L821-L929)) backfills gaps with null arrays and all-zero validity bitmaps.
- **DuckDB path avoids this**: `_pad_batch_to_schema()` ([`arrow_reader.py:81-108`](../../../../python/arcticdb/version_store/duckdb/arrow_reader.py#L81-L108)) adds null columns per batch to match the full schema.
- **Resolution**: Must be solved in C++ before `lib.read` can use the lazy path. See Step 4.

**3. Type widening (int→float across appends)**

When the first segment has `int64` and a later append promotes to `float64`:

- **Current bug in DuckDB path**: `_ensure_schema()` ([`arrow_reader.py:302-346`](../../../../python/arcticdb/version_store/duckdb/arrow_reader.py#L302-L346)) uses first batch's Arrow type (`int64`), overriding the merged descriptor's correct promoted type (`float64`). Later `float64` batches cause `ArrowInvalid` on downcast in `_pad_batch_to_schema`.
- **Eager path avoids this**: `allocate_frame()` uses the merged descriptor type (correct). `promote_integral_type()` ([`read_frame.cpp:676-716`](../../../../cpp/arcticdb/pipeline/read_frame.cpp#L676-L716)) iterates backwards through values, casting in-place.
- **Resolution**: C++ schema padding (Step 4) should use the merged descriptor type as the source of truth, which fixes this bug as a side effect. See Step 4.

---

## Proposed Unified Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│  C++ Unified Read API                                           │
│                                                                 │
│  Shared helper functions (free functions):                      │
│  ├── read_and_decode_segment()    // I/O + decode               │
│  ├── apply_truncation()           // date_range / row_range     │
│  └── apply_filter_clause()        // WHERE filter               │
│                                                                 │
│  LazyRecordBatchIterator (refactored, Steps 1+3+4)             │
│  │   ├─ setup_pipeline_context() [shared, unchanged]            │
│  │   ├─ SINGLE prefetch: I/O → decode → truncate → filter →    │
│  │   │   Arrow convert — all in one Folly future (same as today)│
│  │   ├─ next() does .get() + column-slice merge (Step 3) +     │
│  │   │   schema padding (Step 4) — zero-copy pointer ops        │
│  │   ├─ yields: RecordBatchData with uniform schema             │
│  │   └─ Used by: DuckDB, Polars (via Arrow), lib.read(pyarrow) │
│  │                                                              │
│  Other consumers (unchanged):                                   │
│  ├── Eager path (do_direct_read_or_process, unchanged)          │
│  │   └─ Assembles all segments into single SegmentInMemory      │
│  │   └─ Returns PandasOutputFrame (numpy zero-copy)             │
│  │   └─ Used by: lib.read(format=pandas) without clauses        │
│  │                                                              │
│  └── ProcessingPipeline (existing clause system, unchanged)     │
│      └─ Feeds segments into ComponentManager + clauses          │
│      └─ Used by: QueryBuilder, resample, merge                  │
└─────────────────────────────────────────────────────────────────┘
```

### Key Design Decision: Extract Shared Helpers, Preserve Monolithic Pipeline

The current `LazyRecordBatchIterator` stores `Future<vector<RecordBatchData>>` — the entire pipeline (I/O → decode → truncate → filter → Arrow convert) runs inside a single prefetch future with count cap=200. This plan extracts the reusable stages (I/O, truncation, filtering) as shared free functions and adds a byte-based cap (4GB default) alongside the count cap to prevent OOM with wide tables.

**Why not a two-stage pipeline?** An earlier version of this plan considered splitting the pipeline into a segment-yielding stage and a separate Arrow conversion stage. This introduces a **two-queue pipeline with a serialization point**: the Arrow conversion future can only be kicked off after the segment future resolves and `next()` is called. Today, Arrow conversion runs inside the same 200-deep prefetch pipeline as I/O, fully overlapping with storage latency. A two-stage architecture would either:

1. **Regress throughput** — if the Arrow lookahead is shallow, `prepare_segment_for_arrow()` (the dominant CPU cost: `make_column_blocks_detachable()` memcpy, `SharedStringDictionary` construction) becomes a serial bottleneck between I/O completion and consumer delivery.
2. **Increase memory** — if the Arrow lookahead is deep to compensate, you're holding decoded `SegmentInMemory` objects in the segment buffer AND `RecordBatchData` in the Arrow buffer simultaneously.

**Chosen approach: refactor in place with shared helpers.** `LazyRecordBatchIterator` keeps its monolithic prefetch pipeline. The I/O, truncation, and filter stages are extracted as free functions for testability and potential future reuse, but Arrow conversion remains inside the same future — zero regression.

```
LazyRecordBatchIterator (refactored)
┌──────────────────────────────┐
│ prefetch_buffer_:            │
│   deque<Future<              │
│     vector<RecordBatchData>>>│
│                              │
│ SINGLE future pipeline:      │
│   I/O → decode → truncate →  │
│   filter → Arrow convert     │
│                              │
│ Dual-cap backpressure:       │
│   count ≤ 200                │
│   bytes ≤ 4 GB (default)     │
│                              │
│ next() does:                 │
│   .get() on resolved future  │
│   + column-slice merge       │
│   + schema padding           │
└──────────────────────────────┘

Shared free functions (extracted from arrow_output_frame.cpp):
  read_and_decode_segment(store, slice_and_key, columns_to_decode)
  apply_truncation(segment, row_filter)
  apply_filter_clause(segment, expression_context, filter_root_node_name)
```

---

## Memory Model (Summary)

**Prefetch**: Dual-cap backpressure — `count_cap = min(max(prefetch_size, num_segments), 200)` AND `byte_cap = max_prefetch_bytes` (default 4GB). Whichever limit is reached first stops prefetching. `LazyRecordBatchIterator` runs the full pipeline (including Arrow conversion) inside each prefetch future — same as today, no two-tier change. See "Memory Model (Detailed)" section below for analysis including column-slice merging memory, DETACHABLE optimization, and null-array caching.

**Peak memory** ≈ `min(count_cap × segment_size, byte_cap) × 2` (DETACHABLE copy factor). For typical segments (≤40MB), the count cap dominates as before. For wide tables (400MB+ segments), the byte cap limits memory to ~8GB instead of 160GB.

**Column-slice merging**: Arrow-level incremental merge in `next()` on consumer thread. Peak = `2 × row_group_arrow_size` (current merged + one incoming slice). Avoids string pool merging entirely.

---

## Refactoring Steps

### Step 1: Extract Shared Helpers and Refactor `LazyRecordBatchIterator`

The current `LazyRecordBatchIterator` ([`arrow_output_frame.hpp:135-213`](../../../../cpp/arcticdb/arrow/arrow_output_frame.hpp#L135-L213), [`arrow_output_frame.cpp:340-549`](../../../../cpp/arcticdb/arrow/arrow_output_frame.cpp#L340-L549)) contains both reusable segment-reading logic and Arrow-specific conversion. The goal is to extract the reusable parts as shared free functions for testability, add dual-cap backpressure, and **leave `LazyRecordBatchIterator`'s prefetch pipeline structure intact**.

#### Shared free functions (new file: `cpp/arcticdb/version/lazy_read_helpers.hpp/cpp`)

These are extracted from `arrow_output_frame.cpp` and used by `LazyRecordBatchIterator`:

```cpp
// I/O + decode for a single segment. Returns a Future that resolves to the decoded segment.
// Returns SegmentAndSlice (not just SegmentInMemory) because:
//   - batch_read_uncompressed() returns SegmentAndSlice (segment + RangesAndKey metadata)
//   - RangesAndKey contains row_range, col_range, AtomKey needed by apply_truncation
//   - The processing pipeline (ComponentManager) also needs these fields
//   - Downstream helpers unwrap via segment_and_slice.segment_in_memory_
folly::Future<pipelines::SegmentAndSlice> read_and_decode_segment(
    std::shared_ptr<stream::StreamSource> store,
    const pipelines::SliceAndKey& slice_and_key,
    const std::shared_ptr<std::unordered_set<std::string>>& columns_to_decode
);

// Truncate segment to date_range / row_range. Modifies segment in place.
// slice_row_range is from SegmentAndSlice::ranges_and_key_.row_range_ —
// needed to compute the truncation boundaries relative to the segment's position.
void apply_truncation(SegmentInMemory& segment, const pipelines::RowRange& slice_row_range,
                      const FilterRange& row_filter);

// Apply WHERE filter clause. Returns false if all rows filtered out (segment should be skipped).
bool apply_filter_clause(
    SegmentInMemory& segment,
    const std::shared_ptr<ExpressionContext>& expression_context,
    const std::string& filter_root_node_name
);
```

#### Refactored `LazyRecordBatchIterator`

`LazyRecordBatchIterator` keeps its own prefetch buffer with the **full monolithic pipeline** — same as today. It uses the shared helpers for the I/O/truncation/filter stages, then appends Arrow conversion within the same future. **No second queue. No serialization point.**

```cpp
// cpp/arcticdb/arrow/arrow_output_frame.hpp
class LazyRecordBatchIterator {
public:
    LazyRecordBatchIterator(
        std::vector<pipelines::SliceAndKey> slice_and_keys,
        StreamDescriptor descriptor,
        std::shared_ptr<stream::StreamSource> store,
        std::shared_ptr<std::unordered_set<std::string>> columns_to_decode,
        FilterRange row_filter,
        std::shared_ptr<ExpressionContext> expression_context,
        std::string filter_root_node_name,
        size_t prefetch_size,          // count cap: min(max(prefetch_size, num_segments), 200)
        size_t max_prefetch_bytes = 4ULL << 30  // byte cap: 4 GB default
    );

    std::optional<RecordBatchData> next();
    bool has_next() const;
    size_t num_batches() const;
    size_t current_index() const;
    const StreamDescriptor& descriptor() const;
    size_t field_count() const;

    // Returns the target Arrow schema (built from merged descriptor in Step 4).
    // Exported to Python via Arrow C Data Interface for ArcticRecordBatchReader.schema.
    // Added in Step 4; returns nullptr before Step 4 is implemented.
    std::pair<ArrowArray, ArrowSchema> arrow_schema() const;

    // Expose slice metadata for column-slice merging (Step 3).
    const pipelines::SliceAndKey& current_slice_and_key() const;
    const pipelines::SliceAndKey* peek_next_slice_and_key() const;

private:
    std::vector<pipelines::SliceAndKey> slice_and_keys_;
    StreamDescriptor descriptor_;
    std::shared_ptr<stream::StreamSource> store_;
    std::shared_ptr<std::unordered_set<std::string>> columns_to_decode_;
    FilterRange row_filter_;
    std::shared_ptr<ExpressionContext> expression_context_;
    std::string filter_root_node_name_;
    size_t prefetch_size_;
    size_t max_prefetch_bytes_;

    // SINGLE prefetch buffer: full pipeline in each future (same as today)
    std::deque<folly::Future<std::vector<RecordBatchData>>> prefetch_buffer_;
    std::deque<RecordBatchData> pending_batches_;  // multi-block drain
    size_t next_prefetch_index_ = 0;
    size_t current_index_ = 0;
    size_t current_prefetch_bytes_ = 0;  // sum of estimated uncompressed bytes in flight
    const pipelines::SliceAndKey* current_sak_ = nullptr;

    // Full pipeline: read_and_decode → truncate → filter → Arrow convert
    folly::Future<std::vector<RecordBatchData>>
    read_decode_filter_and_convert_segment(size_t idx);
    void fill_prefetch_buffer();  // dual-cap: stops when count OR bytes exceeded
};
```

**Full pipeline inside each prefetch future** (identical to today's behavior):
```
read_and_decode_segment()                  // shared helper — I/O + decode
  .via(&cpu_executor()).thenValue()         // CPU thread pool:
    → apply_truncation()                    //   shared helper
    → apply_filter_clause()                 //   shared helper — returns nullopt if empty
    → if nullopt: return {}                 //   skip (no Arrow conversion wasted)
    → prepare_segment_for_arrow(segment)    //   string dict, sparse, detachable
    → segment_to_arrow_data(segment)        //   split into 64KB blocks
    → return vector<RecordBatchData>
```

**Why this preserves throughput**: The Arrow path's prefetch pipeline is unchanged — same monolithic future, same overlap between I/O and `prepare_segment_for_arrow()`. The consumer's `next()` does `.get()` on an already-resolved future, exactly as today. Steps 3 and 4 add column-slice merging and schema padding to `next()`, but these are zero-copy pointer operations (see Step 3). The dual-cap (count=200, bytes=4GB) replaces the old count-only cap — for typical segment sizes (≤40MB) the count cap dominates as before; for wide tables with large segments, the byte cap prevents OOM.

**Edge cases handled correctly:**
- String handling, sparse floats, multi-block segments: inside prefetch future (unchanged)
- Filter clauses, truncation, column pruning: inside prefetch future (unchanged, now via shared helpers)
- Appends, empty symbols, pickled data: unchanged
- EMPTYVAL fields: decoder handles transparently (zero bytes stored)
- Empty string dictionary: `minimal_strings_for_dict()` runs in Arrow conversion inside future
- Zero-row after truncation/filter: future returns empty vector, `next()` skips to next future
- Dynamic schema filter on missing column: `ExpressionContext` with `dynamic_schema_` flag propagated

**Edge cases NOT addressed** (deferred to Steps 3 and 4):
- Column slicing: still yields one batch per slice (Python merging still required)
- Dynamic schema: still yields heterogeneous schemas (Python padding still required)

**Key files to modify:**
- New: `cpp/arcticdb/version/lazy_read_helpers.hpp/cpp` — shared free functions extracted from `arrow_output_frame.cpp`
- `cpp/arcticdb/arrow/arrow_output_frame.hpp/cpp` — refactor `LazyRecordBatchIterator` to use shared helpers (prefetch structure unchanged)
- `cpp/arcticdb/version/version_store_api.cpp:1061-1152` — constructor calls updated (same parameters)

### Step 2: Add C++ Tests for the Lazy Path

The current test suite has **zero C++ tests** for `LazyRecordBatchIterator` or the streaming read path. All testing is via Python. This is a risk.

**Shared helper unit tests (`cpp/arcticdb/version/test/test_lazy_read_helpers.cpp`):**

| Test | What It Verifies |
|------|-----------------|
| `ReadAndDecode_Basic` | Write segment via InMemoryStore, `read_and_decode_segment()` returns decoded segment |
| `ApplyTruncation_DateRange` | Segments spanning [0,100), date_range=[25,75) truncates boundary segments |
| `ApplyTruncation_RowRange` | row_range limits rows across segment boundaries |
| `ApplyTruncation_ZeroRow` | date_range outside segment range → zero-row segment |
| `ApplyFilterClause_Basic` | WHERE clause filters rows within segments |
| `ApplyFilterClause_EmptyResult` | All rows filtered out → returns false |
| `ApplyFilterClause_MissingColumn_DynamicSchema` | Filter references column not in segment, `dynamic_schema_=true` → EmptyResult |
| `ApplyFilterClause_FullResult` | All rows match → no filtering applied |
| `SparseFloatWithTruncation` | Sparse column truncated mid-segment → bitmap and data consistent |

**Arrow round-trip tests (`cpp/arcticdb/arrow/test/test_lazy_record_batch_iterator.cpp`):**

| Test | What It Verifies |
|------|-----------------|
| `RoundTripArrowData` | Write → read via iterator → verify RecordBatchData matches original |
| `MultiBlockSegment` | Large segment produces multiple 64KB blocks; pending_batches_ drains correctly |
| `StringDictionaryEncoding` | String columns encoded via SharedStringDictionary, decoded correctly |
| `SparseFloatColumns` | Sparse columns produce correct Arrow validity bitmaps |
| `ZeroRowStringSegment` | Zero-row segment with string columns → empty dict workaround triggers correctly |
| `PrefetchOverlap` | Verify segment I/O overlaps with Arrow conversion (timing-based) |
| `BasicIteration` | Write 5 segments via InMemoryStore, iterator returns all 5 in order |
| `EmptySymbol` | 0 segments → has_next()=false immediately, descriptor still valid |
| `SingleSegment` | Degenerate case: 1 segment, prefetch_size > 1 |
| `ColumnPruning` | 10 columns, decode only 3, verify others not in output |
| `PrefetchBehavior` | prefetch_size controls concurrent reads (instrument store) |
| `BoundedMemory` | 100 segments, prefetch=5 → peak alloc ≈ O(prefetch × segment_size) |
| `DateRangeTruncation_EndToEnd` | Segments spanning [0,100), date_range=[25,75) → correct Arrow output |
| `FilterClause_EndToEnd` | WHERE clause filters rows, correct Arrow output |
| `EmptyAfterFilter` | All rows filtered out → segments skipped, iterator exhausted |

**Edge case tests that must exist before Steps 3–5:**

| Test | What It Verifies | Blocks |
|------|-----------------|--------|
| `TypeWidening_IntToFloat` | int64 first segment, float64 second → both read correctly | Step 4 |
| `WideTable_ColumnSlicing` | 400 columns → 4 slices per row group, all slices returned | Step 3 |
| `WideTable_DynamicSchema_Combined` | 200+ cols, different cols per segment, column-sliced | Steps 3+4 |
| `DynamicSchema_ProjectedColumnMissing` | Column projection + dynamic schema, projected col absent from some segments | Step 4 |
| `AllNullSegment` | Segment where all values are Arrow null (not NaN) | Step 4 |
| `MultiBlock_WithColumnSlicing` | Single large segment (>64KB) split into blocks, also column-sliced | Step 3 |

**Benchmarks (`cpp/arcticdb/arrow/test/benchmark_lazy_read.cpp`):**

| Benchmark | What It Measures |
|-----------|-----------------|
| `BM_LazyVsEagerRead_Numeric` | Throughput: lazy iterator vs eager read, numeric columns |
| `BM_LazyVsEagerRead_String` | Same, with string columns (dictionary encoding cost) |
| `BM_ArrowConversionOverhead` | Isolate `prepare_segment_for_arrow` cost vs raw segment read |
| `BM_PrefetchScaling` | prefetch_size = 1, 2, 4, 8, 16 — throughput scaling |
| `BM_ColumnSliceMerge` | Wide table (400 cols, ~4 slices per row group) |
| `BM_LazyVsEagerOverall` | End-to-end throughput: lazy path vs current eager path, verify no regression |

### Step 3: Column-Slice Merging in C++

**Prerequisite for** routing `lib.read(format=Arrow)` through the lazy path (Step 5).

ArcticDB tiles wide tables into ~127-column slices. The lazy iterator yields one batch per slice. Without merging, a 400-column table produces ~4 batches per row group, each with a different schema. `pa.Table.from_batches()` requires uniform schemas and would fail.

Currently solved in Python by `ArcticRecordBatchReader._merge_slices_for_group()` ([`arrow_reader.py:219-239`](../../../../python/arcticdb/version_store/duckdb/arrow_reader.py#L219-L239)).

#### Approach: Arrow-Level Incremental Merge in Prefetch

Add column-slice merging to `LazyRecordBatchIterator`. Merging happens at the **Arrow level** (after per-slice conversion), not at the `SegmentInMemory` level. This avoids string pool merging entirely.

**Why Arrow-level, not Segment-level**: Each column slice has its own string pool. `SharedStringDictionary` is built from the segment's pool — if we merged `SegmentInMemory` objects first, we'd need to merge string pools or build the dictionary per-slice anyway. Arrow-level merging sidesteps this: each slice's strings are already Arrow format (dictionary-encoded or UTF-8 offsets).

**Algorithm in `LazyRecordBatchIterator::next()`:**

```
1. Pull next RecordBatchData from arrow_prefetch_ (or pending_batches_)
2. Check: does the NEXT slice in segment_iter_ have the same row_range?
   (Use segment_iter_.peek_next_slice_and_key() — a non-consuming peek)
3. If YES: it's a column slice of the same row group.
   a. Drain all consecutive same-row-range slices (convert each to Arrow via arrow_prefetch_)
   b. Merge horizontally: concatenate Arrow arrays, deduplicating index columns
   c. Reorder columns to match merged descriptor field order
   d. Return single merged RecordBatchData
4. If NO: return the RecordBatchData as-is (single-slice or narrow table)
```

**Incremental merge to bound memory:**
```cpp
// Instead of collecting all slices then merging:
RecordBatchData merged = first_slice;
while (peek_is_same_row_group()) {
    auto next_slice = pull_next_arrow_batch();
    merged = horizontal_merge(merged, next_slice, descriptor_);
    // next_slice is released immediately
}
```

Peak memory during merge: `2 × row_group_arrow_size` (current merged + one incoming slice). Much better than holding all N slices simultaneously.

**Index column deduplication**: The index column (column 0) appears in every slice. During merge, skip columns from subsequent slices that already exist in the merged batch. Match by column name. The eager path does this via `mark_index_slices()` / `fetch_index_` bitset; the Arrow merge uses name-based deduplication matching `_merge_slices_for_group()`'s approach.

**Column ordering**: After merge, columns must match the merged descriptor's field order. The merged descriptor (`LazyRecordBatchIterator::descriptor()`) defines the authoritative field ordering. Reorder the merged batch's columns to match.

**Same-row-group detection**: Must be more robust than Python's `_is_same_row_group()` which compares actual index column values (expensive for large batches). In C++, use `SliceAndKey` metadata directly:

```cpp
bool is_same_row_group(const SliceAndKey& a, const SliceAndKey& b) {
    return a.slice_.row_range == b.slice_.row_range;
}
```

This is cheaper (integer comparison) and correct because `setup_pipeline_context()` already assigns row ranges from the index.

**Edge cases to handle:**
- Index column (column 0) appears in every slice — deduplicate by name
- Column order must match the merged descriptor field order
- Narrow table (< 127 cols) → single slice, no merging, passthrough (fast path)
- Column projection: only projected columns appear in slices; merge is still correct
- Multi-block segments: a single slice may produce multiple 64KB blocks. pending_batches_ must drain all blocks of all slices for a row group before returning

**Multi-block interaction**: If a column slice produces multiple Arrow blocks (rare — only for very tall segments with >64KB per column), each block is a separate `RecordBatchData`. The merging algorithm must handle this correctly:

1. For each slice, `convert_segment_to_arrow()` returns `vector<RecordBatchData>` (usually size 1, but can be N blocks)
2. All slices of the same row group have the same row count, so they produce the same number of blocks with aligned row boundaries
3. The merge must operate **block-by-block across slices**, not block-by-block within a slice:

```
Row group with 3 slices, each producing 2 blocks:
  Slice A: [block_0_A, block_1_A]   (cols 0-99)
  Slice B: [block_0_B, block_1_B]   (cols 100-199)
  Slice C: [block_0_C, block_1_C]   (cols 200-299)

  Merged: [merge(block_0_A, block_0_B, block_0_C),
           merge(block_1_A, block_1_B, block_1_C)]
```

4. Return merged block 0 immediately, queue remaining merged blocks in `pending_batches_`

**Simplification for typical case**: Most segments produce 1 block (100K rows × 127 cols fits in 64KB per column). Multi-block is only triggered for very tall segments. The implementation can optimize for the 1-block case (no block alignment needed — just merge the single batch from each slice).

**Sparrow zero-copy merge (verified)**: The merge operates at the Arrow C Data Interface level and is zero-copy for column data buffers. Sparrow's API supports extracting child `ArrowArray` structs via `record_batch::extract_struct_array() → arrow_proxy::children() → child.extract_array()` — each extraction is an O(1) ownership transfer. The merged parent `ArrowArray` is constructed with a new `children` pointer array referencing the extracted children. See Implementation Note #9 for the full extraction chain. **The release callback for the merged parent is the highest-risk piece of this step** — it must correctly release each child without double-freeing. Recommended pattern: capture children in a `vector<ArrowArray>` owned by the release closure.

**Tests to add:**
- `ColumnSliceMerge_TwoSlices` — 200 columns → 2 slices merged into 1 batch
- `ColumnSliceMerge_FourSlices` — 500 columns → 4 slices merged
- `ColumnSliceMerge_IndexDeduplication` — index column not duplicated in output
- `ColumnSliceMerge_MixedTypes` — slices with numeric + string + timestamp columns
- `ColumnSliceMerge_SingleSlice` — narrow table (< 127 cols) → no merging, passthrough
- `ColumnSliceMerge_ColumnOrdering` — output columns match descriptor field order
- `ColumnSliceMerge_WithProjection` — projected subset of wide table
- `ColumnSliceMerge_IncrementalMemory` — verify peak memory ≈ 2× row_group, not N×
- `SparrowExtract_BufferPointersPreserved` — assert data buffer pointers are identical before/after extraction (zero-copy proof)
- `HorizontalMerge_NoDoubleFree` — merge + release under ASan
- `HorizontalMerge_NoUseAfterFree` — destroy sources, read from merged under ASan
- `ColumnSliceMerge_PartialConsume_NoLeak` — consume partial iterator, destroy, ASan leak check

**Key files:**
- [`cpp/arcticdb/arrow/arrow_output_frame.cpp`](../../../../cpp/arcticdb/arrow/arrow_output_frame.cpp) — merging logic in `LazyRecordBatchIterator::next()`
- [`cpp/arcticdb/arrow/arrow_utils.hpp`](../../../../cpp/arcticdb/arrow/arrow_utils.hpp) — helper `horizontal_merge_arrow_batches()`

### Step 4: Schema Padding in C++ (Dynamic Schema)

**Prerequisite for** routing `lib.read(format=Arrow)` through the lazy path (Step 5).

With dynamic schema, different segments have different column subsets. Without padding, batches have heterogeneous schemas. `pa.Table.from_batches()` fails.

Currently solved in Python by `_pad_batch_to_schema()` ([`arrow_reader.py:81-108`](../../../../python/arcticdb/version_store/duckdb/arrow_reader.py#L81-L108)) which adds null columns for missing fields.

**Approach:** Add schema padding to `LazyRecordBatchIterator` after Arrow conversion and column-slice merging. The iterator holds the merged `StreamDescriptor` (from its own `descriptor()` method, populated by `setup_pipeline_context()`), which is the authoritative superset schema.

For each `RecordBatchData` produced (post-merge if column-sliced):
1. Compare its schema against the target schema (derived from merged descriptor)
2. For missing columns: create Arrow null arrays of the correct type and length
3. For type-mismatched columns: cast to target type (safe upcast only)
4. Reorder columns to match the target field order
5. Return a `RecordBatchData` with the full uniform schema

**Target schema construction**: Built once during `LazyRecordBatchIterator` construction from the merged descriptor. For each field in the descriptor, map the ArcticDB type to the corresponding Arrow type. This is the same mapping `_descriptor_to_arrow_schema()` does in Python, but in C++.

**Static schema fast path**: When `batch_schema == target_schema` (the common case for static-schema symbols), skip padding entirely. This is an O(1) pointer comparison if we cache the schema.

**Type widening fix:** The merged descriptor contains the correct promoted type (e.g. `float64` when `int64` was widened). By using the descriptor as the schema source of truth (not the first batch's types), the existing int→float type widening bug is fixed. When a batch has `int64` but the descriptor says `float64`, cast the batch column to `float64` before including it.

**Null array creation per type:**

| Arrow Type | Null Array | Cost |
|-----------|-----------|------|
| Numeric (int, float, timestamp) | Zero-filled buffer + all-zero validity bitmap | O(n) memset |
| Dictionary-encoded string | Empty dictionary + all-null indices | O(1) |
| Large/small string | Zero offsets buffer + all-zero validity bitmap | O(n) for offsets |
| Boolean | Zero-filled byte buffer + all-zero validity bitmap | O(n/8) |

**Null array caching**: Most segments in a symbol have the same row count. Cache null arrays keyed by `(arrow_type, row_count)`. Cache hit rate will be high for uniform-length segments, eliminating repeated allocation.

**Dynamic schema + column projection**: Pad only to the **projected** column subset, not the full descriptor. The projected columns are known from `columns_to_decode`. This avoids creating null arrays for columns the consumer didn't ask for.

**Edge cases to handle:**
- Null array creation per Arrow type: numeric (zero-filled with null bitmap), string (empty dictionary or empty offsets), timestamp (zero-filled with null bitmap)
- Type casting: batch column type differs from descriptor type → upcast (safe: int→float, smaller→larger)
- Static schema: no-op path (batch schema == target schema, skip padding entirely)
- Dynamic schema + column projection: pad only to the projected subset, not the full descriptor
- All-null segment: every column is a null array (no data columns present) — valid, produces all-null row
- Empty string dictionary with null padding: must use `minimal_strings_for_dict()` workaround for dictionary-encoded null columns

**Tests to add:**
- `SchemaPadding_MissingNumericColumn` — segment lacks a float64 column → null float64 column added
- `SchemaPadding_MissingStringColumn` — segment lacks a string column → null dictionary column added
- `SchemaPadding_TypeWidening` — first segment int64, second segment float64 → both output as float64
- `SchemaPadding_StaticSchema` — all segments same schema → no-op, no overhead
- `SchemaPadding_ColumnOrdering` — columns reordered to match descriptor regardless of segment order
- `SchemaPadding_WithColumnProjection` — only projected columns padded
- `SchemaPadding_AllNullSegment` — segment with no data columns → all null arrays
- `SchemaPadding_NullArrayCaching` — verify null arrays reused across same-size segments

**Key files:**
- [`cpp/arcticdb/arrow/arrow_output_frame.cpp`](../../../../cpp/arcticdb/arrow/arrow_output_frame.cpp) — padding logic in `LazyRecordBatchIterator::next()` (after merge, before returning)
- [`cpp/arcticdb/arrow/arrow_utils.hpp`](../../../../cpp/arcticdb/arrow/arrow_utils.hpp) — helpers for null array creation, type casting, and target schema construction

### Step 5: Route `lib.read(output_format='pyarrow')` Through `LazyRecordBatchIterator`

**Depends on:** Steps 3 (column-slice merging) and 4 (schema padding). Without these, `_adapt_frame_data()` would receive batches with heterogeneous schemas and fail on `pa.Table.from_batches()`.

Currently, `create_python_read_result()` ([`pipeline_utils.hpp:84-90`](../../../../cpp/arcticdb/pipeline/pipeline_utils.hpp#L84-L90)) handles Arrow output by calling `segment_to_arrow_data(full_frame)` on the entire assembled `SegmentInMemory`. This means `lib.read(output_format='pyarrow')` materializes the entire symbol in memory even though the consumer gets Arrow batches.

**Change:** For `OutputFormat::ARROW` without processing clauses (direct reads), skip the eager `do_direct_read_or_process()` path and return a `LazyRecordBatchIterator` instead. With Steps 3 and 4 complete, all batches have uniform schemas, so `_adapt_frame_data()` works unchanged.

**Impact:**
- `lib.read(output_format='pyarrow')` gets streaming semantics (bounded memory)
- `lib.read(output_format='polars')` gets the same benefit (goes through Arrow)
- No change to `lib.read(output_format='pandas')` — keeps the zero-copy fast path
- No change to reads with processing clauses (GROUP BY, resample) — keeps eager path

**Python-side changes to `_adapt_frame_data()` ([`_store.py:2821-2845`](../../../../python/arcticdb/version_store/_store.py#L2821-L2845)):**
- Accept either `ArrowOutputFrame` (eager, existing) or `LazyRecordBatchIterator` (lazy, new)
- For lazy: iterate `next()`, import each `RecordBatchData` via `pa.RecordBatch._import_from_c()`, collect into `pa.Table`
- Schemas are already uniform (Steps 3+4), so `pa.Table.from_batches()` works

**Polars LazyFrame integration** becomes straightforward once this is in place:

```python
def to_polars_lazy(self, symbol, **read_kwargs):
    iterator = self._nvs.create_lazy_record_batch_iterator(symbol, ...)
    reader = ArcticRecordBatchReader(iterator)
    return pl.from_arrow(reader.to_pyarrow_reader())  # Polars consumes lazily
```

**Note on type handlers**: [`pipeline_utils.hpp:30-35`](../../../../cpp/arcticdb/pipeline/pipeline_utils.hpp#L30-L35) asserts `output_format == PANDAS` for type-handler-based reads. The Arrow path does NOT use this code — Arrow string handlers are registered separately (`ArrowStringHandler` for `UTF_DYNAMIC64`, `ASCII_DYNAMIC64`, etc.) and invoked by `prepare_segment_for_arrow()`. Pandas-only types (`BOOL_OBJECT8`, `PythonEmptyHandler`, `PythonArrayHandler`) don't have Arrow handlers, but these are rare edge-case types. If encountered in Arrow output, they should raise a clear error message: "Type X requires pandas output format".

**Key files:**
- [`cpp/arcticdb/version/version_store_api.cpp`](../../../../cpp/arcticdb/version/version_store_api.cpp) — `read_dataframe_version()` branches on output_format for clause-free reads
- [`cpp/arcticdb/pipeline/pipeline_utils.hpp`](../../../../cpp/arcticdb/pipeline/pipeline_utils.hpp) — `create_python_read_result()` for ARROW path
- [`python/arcticdb/version_store/_store.py`](../../../../python/arcticdb/version_store/_store.py) — `_adapt_frame_data()` handles iterator case
- [`cpp/arcticdb/version/python_bindings.cpp`](../../../../cpp/arcticdb/version/python_bindings.cpp) — return type for Arrow reads changes to lazy iterator

### Step 6: Simplify Python `ArcticRecordBatchReader` (Cleanup)

With Steps 3 and 4 complete, the C++ `LazyRecordBatchIterator` produces batches with uniform, full schemas. The following Python code in `ArcticRecordBatchReader` becomes dead:

- `_merge_slices_for_group()` / `_read_next_merged_batch()` — column-slice merging now in C++
- `_pad_batch_to_schema()` — schema padding now in C++
- `_ensure_schema()` first-batch type sniffing — schema comes from C++ (merged descriptor types)
- `_is_same_row_group()` — no longer needed

`ArcticRecordBatchReader` reduces to a thin wrapper: iterate C++ `next()`, import via `_import_from_c()`, strip `__idx__` prefix. The DuckDB `sql()` path in `library.py` simplifies correspondingly.

**Must preserve:**
- `_iteration_started` / `_exhausted` flags for single-use constraint (CPython 3.13 double `__iter__` workaround)
- `_strip_idx_prefix_from_names()` for MultiIndex column name cleaning
- `_projected_columns` filtering for column projection

---

## What NOT to Change

1. **Don't eliminate the pandas fast path.** `PandasOutputFrame` avoids `memcpy` to detachable blocks and is 3–60x faster than Arrow for full scans. `lib.read(output_format='pandas')` without clauses should remain eager.

2. **Don't change the `ProcessingPipeline` (clause system).** GROUP BY, resample, and merge require cross-segment materialization via `ComponentManager`. These are well-architected and orthogonal to the lazy read refactor.

3. **Don't change `lib.read(output_format='pandas')` with clauses.** The eager path + clause system handles this correctly. Only the Arrow output path benefits from the lazy iterator.

4. **Don't change the filter+column-projection interaction (until Step 3).** Currently `query_builder` is set to `None` when columns are projected ([`library.py:2415`](../../../../python/arcticdb/version_store/library.py#L2415)). After Step 3 merges column slices in C++, this workaround can be **removed** — filters can be pushed even with column projection because all columns are available in the merged batch. See "sql() Simplifications" section.

5. **Don't apply the RangeIndex step=0 backward compatibility patch in the lazy path.** This patch ([`pipeline_utils.hpp:64-80`](../../../../cpp/arcticdb/pipeline/pipeline_utils.hpp#L64-L80)) only applies to Pandas normalization metadata and runs in `create_python_read_result()`. Arrow and Polars don't need it.

---

## Comprehensive Edge-Case Analysis

Deep analysis of every edge case in both read paths, whether the unified plan handles it, and required amendments.

### Edge Cases Fully Covered by the Plan

| # | Edge Case | Eager Path Handling | Lazy Path Handling | Unified Plan Coverage |
|---|-----------|--------------------|--------------------|----------------------|
| 1 | **Column slicing (wide tables, static schema only)** | `reduce_and_fix_columns()` + `FrameSliceMap` in C++ | Python `_merge_slices_for_group()` | **Step 3** — C++ horizontal Arrow merge. Note: column slicing only applies to static schema; dynamic schema uses a single column slice (`options.py:121-122`). |
| 2 | **Dynamic schema (heterogeneous cols)** | `NullValueReducer` backfills nulls in C++ | Python `_pad_batch_to_schema()` | **Step 4** — C++ schema padding to merged descriptor |
| 3 | **Type widening (int→float)** | `promote_integral_type()` backward iteration in C++ | **BUG**: `_ensure_schema()` uses first batch types | **Step 4** — fixed by using descriptor as source of truth |
| 4 | **Sparse floats / validity bitmaps** | `backfill_all_zero_validity_bitmaps_up_to()` in `NullValueReducer` | `prepare_segment_for_arrow():292-300` extracts bitmap BEFORE `unsparsify()` | Stays in `LazyRecordBatchIterator` Arrow prefetch — no change needed |
| 5 | **String dictionary encoding** | N/A (eager path doesn't use `SharedStringDictionary`) | `SharedStringDictionary` + `ArrowStringHandler` in `arrow_output_frame.cpp:62-202` | Stays in Arrow prefetch layer — no change needed |
| 6 | **Multi-block segments (>64KB)** | Single frame, no blocks | `pending_batches_` deque drains blocks | Stays in `LazyRecordBatchIterator` — no change needed |
| 7 | **Date range / row range truncation** | Via `ColumnMapping::set_truncate()` + `get_truncate_range()` | `apply_truncation()` binary search in prefetch future | Extracted to shared helper — no change in semantics |
| 8 | **Filter → empty result** | N/A for lazy path | `bitset.count() == 0` → `EmptyResult` → skip segment | Extracted to shared helper — `next()` skips empty segments |
| 9 | **Filter → full result** | N/A | `FullResult` variant → no filtering | Same — shared helper passes segment through |
| 10 | **Empty symbols (0 segments)** | `frame.empty()` early return | `has_next()=false` immediately | No change — 0 slices → exhausted |
| 11 | **Pickled data** | Handled in eager path | `!pipeline_context->multi_key_` rejection | No change — rejected before iterator |
| 12 | **Column projection / pruning** | `overall_column_bitset_` | `columns_to_decode` set | Same mechanism, passed to shared helpers |
| 13 | **MultiIndex `__idx__` prefix** | Python `_denormalize()` | Python `_strip_idx_prefix_from_names()` | **Must preserve** in Step 6 cleanup |
| 14 | **CPython 3.13 double `__iter__`** | N/A | `_iteration_started` flag | **Must preserve** in Step 6 cleanup |
| 15 | **Boolean dense packing** | Handled by Sparrow | `arrow_utils.cpp:45-58` bool → bitset | Stays in Arrow conversion layer |
| 16 | **Empty string dictionary** | N/A | `minimal_strings_for_dict()` adds dummy `"a"` | Stays in Arrow conversion layer |
| 17 | **Appends** | Transparent — segments indistinguishable | Same | No change |
| 18 | **Index deduplication across slices** | `mark_index_slices()` + `fetch_index_` bitset | Python name-based dedup in `_merge_slices_for_group()` | **Step 3** — C++ dedup by name during horizontal merge |
| 19 | **Prefetch I/O parallelism** | N/A (eager is all-at-once) | Folly futures with prefetch buffer | Preserved — same prefetch pipeline in `LazyRecordBatchIterator` |
| 20 | **GIL release** | `py::gil_scoped_release` before read | Same | No change |

### Edge Cases Requiring Plan Amendments

#### A. Filter Pushdown with Column Projection (BLOCKER — cannot remove workaround)

**Current state**: [`library.py:2409-2415`](../../../../python/arcticdb/version_store/library.py#L2409-L2415) disables `FilterClause` pushdown when `columns is not None` because the filter column may be in a different column slice than the data being processed.

**Root cause (verified)**: Column slicing (controlled by `columns_per_segment`, default 127) tiles wide static-schema tables into multiple column slices per row group. **Column slicing only applies to static schema libraries** — dynamic schema libraries always use a single column slice ([`options.py:121-122`](../../../../python/arcticdb/options.py#L121-L122)), so this issue is exclusive to static schema.

With a 400-column static-schema table tiled into ~4 column slices per row group, filters are applied **per-segment** in `apply_filter_clause()` ([`arrow_output_frame.cpp:481-518`](../../../../cpp/arcticdb/arrow/arrow_output_frame.cpp#L481-L518)). Each segment is a single column slice. When the filter references column `b` but the current segment is a slice that doesn't contain `b`:

- **Static schema** (`dynamic_schema_=false`): `ProcessingUnit::get()` ([`processing_unit.cpp:59-63`](../../../../cpp/arcticdb/processing/processing_unit.cpp#L59-L63)) raises `E_ASSERTION_FAILURE` — **crash**. This is the real concern because static schema is the only mode that column-slices.
- **Dynamic schema** (`dynamic_schema_=true`): Would return `EmptyResult{}`, silently dropping data. However, this path is **not a practical concern** for column slicing because dynamic schema doesn't column-slice. It could only be hit via Bug 4 (passing the wrong `dynamic_schema` flag for a static-schema library), which is already mitigated.

Although `columns_to_decode` is augmented with filter input columns ([`version_store_api.cpp:1117-1120`](../../../../cpp/arcticdb/version/version_store_api.cpp#L1117-L1120)), this only controls which columns are decoded from storage. It does NOT change which columns are physically present in a given column slice's segment. The filter column is stored in exactly one column slice per row group.

**After Step 3**: Column-slice merging (Step 3) happens in `LazyRecordBatchIterator::next()` at the **Arrow level**, AFTER the per-segment prefetch pipeline (I/O → decode → truncate → filter → Arrow convert). Since filtering runs inside the prefetch future BEFORE merging, Step 3 does NOT fix this problem.

**Decision**: The Python workaround (`qb = None if columns is not None`) **MUST be kept** through Step 6 and beyond. Two future options exist (both out of scope for this plan):

1. **Post-merge filter in `LazyRecordBatchIterator::next()`**: Add a second filter evaluation point after column-slice merging. The merged batch has all columns, so the filter can run. This enables pushdown but adds complexity.

2. **Pre-merge bitset broadcast**: Apply the filter to the slice containing the filter column, extract the resulting row bitset, broadcast it to all other slices in the same row group before converting to Arrow. More efficient (avoids merging filtered-out rows) but requires coordinating across slices within the prefetch pipeline.

**Amendment**: Keep the workaround in Step 6. Do NOT remove `qb = None if columns is not None` from `library.py:sql()`.

#### B. EMPTYVAL Type Handling in Arrow Output

**Eager path**: `PythonEmptyHandler` ([`python_handlers_common.hpp:18`](../../../../cpp/arcticdb/python/python_handlers_common.hpp#L18)) converts EMPTYVAL to Python `None` objects, but only for `OutputFormat::PANDAS` (assertion at [`pipeline_utils.hpp:32-35`](../../../../cpp/arcticdb/pipeline/pipeline_utils.hpp#L32-L35)).

**Lazy path**: `batch_read_uncompressed()` encounters zero-byte EMPTYVAL fields. The decoder handles them transparently (zero bytes stored, type handler reconstructs). But for Arrow output, there is no `ArrowEmptyHandler` registered.

**Risk**: If a segment has an EMPTYVAL-typed column (column added in dynamic schema but never written), the Arrow path may produce garbage.

**In practice**: EMPTYVAL columns in the lazy path are handled by schema padding — the column is simply absent from the segment, and `_pad_batch_to_schema()` (or C++ Step 4 padding) adds a null array. The EMPTYVAL type in the descriptor maps to `pa.string()` fallback in `_descriptor_to_arrow_schema()`.

**Amendment to Step 4**: When creating null arrays for missing columns, check if the descriptor type is EMPTYVAL and map to `pa.null()` or `pa.large_string()` (matching the Python fallback). Add a test case.

#### C. `_descriptor_to_arrow_schema()` Type Mapping Gaps

**Current issue** ([`arrow_reader.py:27-47`](../../../../python/arcticdb/version_store/duckdb/arrow_reader.py#L27-L47)): The `_DATATYPE_TO_ARROW` mapping only covers 14 types (UINT8–UINT64, INT8–INT64, FLOAT32, FLOAT64, BOOL8, NANOSECONDS_UTC64, ASCII_DYNAMIC64, UTF_DYNAMIC64). Missing types fall back to `pa.string()`, which is wrong for:
- `NANOSECONDS_UTC64` variants (different timezones) — mapped correctly
- `UTF_FIXED64` / `ASCII_FIXED64` — not mapped, falls to `pa.string()` (should be `pa.large_string()`)
- `EMPTYVAL` — falls to `pa.string()` (acceptable for null columns)
- `BYTES_DYNAMIC64` — not mapped (binary data)

**Impact on Step 4**: The C++ schema padding must build its target schema from the merged descriptor. The C++ type mapping must be complete (it already is — `segment_to_arrow_data()` handles all types). No Python fallback needed once C++ does the mapping.

**Amendment**: Step 4's target schema construction should use the C++ type system directly (mapping `DataType` → Arrow type in C++), not replicate the incomplete Python mapping. Add coverage for `UTF_FIXED64`, `ASCII_FIXED64`, `BYTES_DYNAMIC64`.

#### D. Sparse Bitmap + Truncation Interaction

**Eager path**: `handle_truncation()` ([`read_frame.cpp:326-335`](../../../../cpp/arcticdb/pipeline/read_frame.cpp#L326-L335)) modifies column metadata for the truncated range. Sparse bitmaps are handled by `NullValueReducer::backfill_all_zero_validity_bitmaps_up_to()`.

**Lazy path**: `apply_truncation()` truncates the `SegmentInMemory`. Then `prepare_segment_for_arrow()` extracts the sparse bitmap from the truncated segment. The bitmap must reflect only the truncated rows.

**Risk**: If `sparse_map()` is not truncated in sync with the data, the bitmap has wrong length.

**Validation**: `segment.row_count()` after truncation returns the truncated count. `bv.resize(segment.row_count())` at [`arrow_output_frame.cpp:297`](../../../../cpp/arcticdb/arrow/arrow_output_frame.cpp#L297) uses this count. The sparse map itself is a `util::BitMagic` bitset keyed by row index — after truncation, indices beyond the new row_count are simply ignored by `resize()`. **This is correct.**

**Amendment**: Add test case to Step 2: sparse float column truncated mid-segment, verify validity bitmap matches truncated data.

#### E. Old Data with Incorrect end_index

**Eager path**: Binary search in `apply_truncation()` handles `time_filter.first > last_ts` by returning `truncate(0, 0)`.

**Lazy path**: Same `apply_truncation()` code ([`arrow_output_frame.cpp:448-449`](../../../../cpp/arcticdb/arrow/arrow_output_frame.cpp#L448-L449)).

**Amendment**: Already handled. Add regression test.

#### F. Column Ordering After Merge

**Eager path**: `reduce_and_fix_columns()` iterates fields in descriptor order, so output columns match descriptor.

**Lazy path (current)**: Python `_merge_slices_for_group()` produces columns in slice encounter order, which happens to match because slices are sorted by `col_range`.

**After Step 3**: C++ merge must reorder columns to match the merged descriptor's field order. The sort by `(row_range, col_range)` in [`version_store_api.cpp:1096-1103`](../../../../cpp/arcticdb/version/version_store_api.cpp#L1096-L1103) ensures slices arrive in col_range order, so incremental merging naturally produces correct column order. BUT: index column deduplication (removing duplicates from slices 2+) could shift column positions.

**Amendment**: Step 3's horizontal merge must:
1. Skip duplicate index columns from subsequent slices
2. After all slices merged, verify column order matches descriptor field order
3. If not (due to dedup), reorder columns

Add test: write 400-column table, read back, verify `batch.schema.names == descriptor.field_names()`.

#### G. Validity Bitmap Size Limit

**Current code** ([`arrow_utils.cpp:22-25`](../../../../cpp/arcticdb/arrow/arrow_utils.cpp#L22-L25)): Asserts `bitmap_buffer.blocks().size() == 1` — bitmap must fit in a single block.

**When violated**: A segment with >512K rows (64KB bitmap = 512K bits) would exceed one block.

**Risk**: Very rare in practice (default segment size is 100K rows), but possible with custom segment sizes.

**Amendment**: This is a pre-existing limitation, not introduced by the unified path. Note as known limitation; no plan change needed.

### Specific Bugs in Current DuckDB Implementation

#### Bug 1: Type Widening in `_ensure_schema()` (CONFIRMED)

**Location**: [`arrow_reader.py:302-346`](../../../../python/arcticdb/version_store/duckdb/arrow_reader.py#L302-L346)

**Description**: `_ensure_schema()` builds the output schema using the first batch's actual Arrow types (line 341-342). For type-widened columns (e.g., `int64` first segment, `float64` after append promotion), the first batch's type (`int64`) is used as the schema type. Later `float64` batches are cast to `int64` in `_pad_batch_to_schema()` (line 101-102), causing either:
- `ArrowInvalid` on incompatible downcast (float64 → int64 with fractional values)
- Silent precision loss (float64 → int64 truncation)

**Severity**: Medium — only affects dynamic schema or post-append reads where type promotion occurred.

**Fix**: Step 4 uses the merged descriptor type (correct promoted type) as source of truth. Bug is fixed as a side effect of the unified path.

**Standalone fix** (if needed before Step 4): Change `_ensure_schema()` to use descriptor type when it's wider than first batch type.

#### Bug 2: Dictionary vs Plain String Type Mismatch in Schema Padding

**Location**: [`arrow_reader.py:81-108`](../../../../python/arcticdb/version_store/duckdb/arrow_reader.py#L81-L108), [`arrow_reader.py:27-42`](../../../../python/arcticdb/version_store/duckdb/arrow_reader.py#L27-L42)

**Description**: `_descriptor_to_arrow_schema()` maps `UTF_DYNAMIC64` → `pa.large_string()`. But C++ Arrow conversion produces `pa.dictionary(pa.int32(), pa.large_string())` for dictionary-encoded string columns. When `_ensure_schema()` uses the first batch's actual type (dictionary-encoded), but a later batch has a different dictionary encoding or plain string, `_pad_batch_to_schema()` must cast between string representations.

**Severity**: Low — DuckDB transparently handles both dictionary-encoded and plain string Arrow arrays. The cast in `_pad_batch_to_schema()` works because PyArrow can cast `dictionary<large_string>` ↔ `large_string`.

**Fix**: Step 4's C++ schema padding should use the same string type consistently. Since `prepare_segment_for_arrow()` always produces dictionary-encoded strings when using `SharedStringDictionary`, the target schema should declare `dictionary(int32, large_string)` for string columns. Only `UTF_FIXED64`/`ASCII_FIXED64` (which bypass `SharedStringDictionary`) would produce plain `large_string`.

#### Bug 3: Fast-Path Column Projection Opportunity (NOT a bug, but optimization)

**Location**: [`library.py:2350`](../../../../python/arcticdb/version_store/library.py#L2350)

**Description**: The fast-path check `pushdown.columns is None` prevents `self.read()` from being used when columns are projected. Since `self.read()` handles column projection correctly, this could be relaxed to also fast-path queries with column projection (bypassing DuckDB when fully pushed).

**Severity**: Performance-only — correctness is fine. Reads with column projection go through DuckDB unnecessarily.

**Fix**: Change condition to allow fast-path with column projection:
```python
if pushdown and pushdown.fully_pushed:
```
This is safe because `self.read(columns=pushdown.columns, ...)` handles projection correctly. However, `fully_pushed` already accounts for whether column projection is compatible. Validate that `fully_pushed=True` is never set when column projection would break semantics.

#### Bug 4: `dynamic_schema` Flag Misuse Risk

**Location**: [`library.py:2370-2374`](../../../../python/arcticdb/version_store/library.py#L2370-L2374)

**Description**: Passing `dynamic_schema=True` for a static-schema library disables the C++ column-slice filter, causing all column slices to be returned (even those without projected columns). Extra slices are null-padded, producing spurious NULL rows in GROUP BY.

**Severity**: Was high before the fix at line 2374. Now mitigated by using `lib.options().dynamic_schema`. But the API still accepts `dynamic_schema` as a parameter to `_read_as_record_batch_reader()`, making it easy for internal callers to pass the wrong value.

**Fix**: Step 3's C++ column-slice merging makes this moot — slices are merged before reaching the consumer, so extra null-padded slices can't produce spurious rows.

---

## sql() Simplifications (Post-Unification)

With the unified lazy path producing uniform-schema Arrow batches, the following Python code becomes dead or simplified:

### Code Eliminated (~258 lines)

| File | Section | Lines | Reason |
|------|---------|-------|--------|
| `arrow_reader.py` | `_is_same_row_group()` | ~35 | C++ uses `SliceAndKey.row_range` (O(1) integer compare) instead of O(N) value comparison |
| `arrow_reader.py` | `_merge_slices_for_group()` / `_merge_column_slices()` | ~40 | C++ merges in Step 3 |
| `arrow_reader.py` | `_read_next_merged_batch()` + peek-ahead state | ~60 | No lookahead needed — C++ yields complete rows |
| `arrow_reader.py` | `_pad_batch_to_schema()` | ~30 | C++ pads in Step 4 |
| `arrow_reader.py` | `_ensure_schema()` + first-batch caching | ~50 | Schema comes from C++ descriptor, not first batch |
| `arrow_reader.py` | `_descriptor_to_arrow_schema()` | ~25 | C++ builds target schema directly |
| `arrow_reader.py` | `_pending_raw_batch` state management | ~15 | No peek-ahead state |
| `library.py` | `dynamic_schema` workaround (lines 2370-2374) | ~5 | C++ only returns relevant slices |

**Code NOT eliminated (kept as-is):**

| File | Section | Lines | Reason |
|------|---------|-------|--------|
| `library.py` | Filter pushdown workaround (lines 2409-2415) | ~7 | **MUST KEEP**: Per-segment filtering runs BEFORE column-slice merging; filter column is only in one slice per row group. Static schema (the only mode that column-slices) would crash with `E_ASSERTION_FAILURE` on the missing column. See Amendment A. |

### Code Retained (Correctly, ~100 lines)

| Section | Lines | Why It Stays |
|---------|-------|-------------|
| `_strip_idx_prefix_from_names()` | ~25 | Storage-level naming convention |
| `_iteration_started` / `_exhausted` guards | ~15 | Python iterator protocol + CPython 3.13 workaround |
| `_projected_columns` filtering | ~10 | May still filter columns in Python for non-C++ consumers |
| `to_pyarrow_reader()` column renaming | ~15 | `__idx__` → clean names for DuckDB |
| `read_all()` materialization | ~15 | Convenience method |
| ArrowCDataInterface import/export | ~20 | Bridge between C++ and PyArrow |

### Simplified `ArcticRecordBatchReader` (Post-Step 6)

```python
class ArcticRecordBatchReader:
    """Thin wrapper around C++ LazyRecordBatchIterator.

    C++ produces uniform-schema, column-merged, schema-padded Arrow batches.
    Python only handles __idx__ name stripping and PyArrow interop.
    """
    def __init__(self, cpp_iterator, projected_columns=None):
        self._cpp_iterator = cpp_iterator
        self._projected_columns = projected_columns  # for column filtering if needed
        self._schema = None
        self._iteration_started = False
        self._exhausted = False

    @property
    def schema(self) -> pa.Schema:
        if self._schema is None:
            # C++ provides the uniform schema from merged descriptor
            self._schema = self._cpp_iterator.arrow_schema()  # New C++ method
        return self._schema

    def __iter__(self):
        if self._iteration_started:
            raise RuntimeError("Cannot iterate over exhausted reader")
        self._iteration_started = True
        return self

    def __next__(self) -> pa.RecordBatch:
        if self._exhausted:
            raise StopIteration
        batch_data = self._cpp_iterator.next()
        if batch_data is None:
            self._exhausted = True
            raise StopIteration
        return pa.RecordBatch._import_from_c(*batch_data.release())
```

### sql() Method Simplifications

**Current** [`library.py:2395-2425`](../../../../python/arcticdb/version_store/library.py#L2395-L2425) (per-symbol registration):
```python
# Current: 30+ lines of workarounds
if columns is not None:
    expanded = []
    for c in columns:
        expanded.append(c)
        if not c.startswith(_IDX_PREFIX):
            expanded.append(_IDX_PREFIX + c)
    columns = expanded
qb = pushdown.query_builder if columns is None else None  # WORKAROUND
reader = self._read_as_record_batch_reader(
    real_symbol, columns=columns, date_range=pushdown.date_range,
    row_range=row_range, query_builder=qb,
    dynamic_schema=lib_dynamic_schema,  # WORKAROUND
)
```

**After unification** (per-symbol registration):
```python
# Unified: dynamic_schema workaround removed; filter workaround KEPT (see Amendment A)
if columns is not None:
    expanded = [c for c in columns]
    for c in columns:
        if not c.startswith(_IDX_PREFIX):
            expanded.append(_IDX_PREFIX + c)
    columns = expanded
qb = pushdown.query_builder if columns is None else None  # KEPT — see Amendment A
reader = self._read_as_record_batch_reader(
    real_symbol, columns=columns, date_range=pushdown.date_range,
    row_range=row_range, query_builder=qb,
    # dynamic_schema workaround REMOVED — C++ merging prevents spurious NULL rows
)
```

### Performance Impact of sql() Simplifications

| Scenario | Current | After Unification | Improvement |
|----------|---------|-------------------|-------------|
| **Wide table (400 cols) `SELECT *`** | 4 Python merges/row-group + value comparison | C++ merge in prefetch future | ~2-3x throughput (no GIL, no intermediate allocations) |
| **Wide static-schema table with `WHERE` + column projection** | Filter NOT pushed, DuckDB applies WHERE to all rows | Same — filter pushdown with column projection requires post-merge evaluation (future work, see Amendment A). Only affects static schema (dynamic schema doesn't column-slice). | No change |
| **Dynamic schema `GROUP BY`** | Python padding per batch (null array creation, no caching) | C++ padding with null array caching | ~1.5x throughput |
| **Type-widened columns** | **Broken** (ArrowInvalid or precision loss) | Correct (descriptor types used) | Correctness fix |
| **Narrow table (< 127 cols) `SELECT *`** | No merging needed, direct passthrough | Same — single slice, no merging | No change |

---

## Memory Model (Detailed)

### Prefetch Sizing and Bounds

**Current behavior**: [`version_store_api.cpp:1133-1140`](../../../../cpp/arcticdb/version/version_store_api.cpp#L1133-L1140) sets `effective_prefetch = min(max(prefetch_size, num_segments), 200)`. This fires up to 200 concurrent reads to hide storage latency.

**Problem**: Count-based cap alone can OOM on wide tables. 200 segments × 400MB (wide table with many columns) = 80GB in prefetch, before Arrow conversion doubles it via `make_column_blocks_detachable()`.

**Solution (Phase 1)**: Dual-cap backpressure. `LazyRecordBatchIterator` uses:
- `count_cap = min(max(prefetch_size, num_segments), 200)` — existing count limit
- `byte_cap = max_prefetch_bytes` (default 4GB, configurable via constructor) — new byte limit
- `fill_prefetch_buffer()` stops when EITHER cap is reached
- Segment size estimated from `SliceAndKey` descriptor metadata
- `next()` decrements `current_prefetch_bytes_` when consuming a resolved future

### Prefetch Memory Model

`LazyRecordBatchIterator` uses dual-cap sizing for its prefetch buffer:

| Future Contents | Memory per Future |
|----------------|-------------------|
| I/O + decode + truncate + filter + Arrow convert → `vector<RecordBatchData>` | `segment_size × ~2` (DETACHABLE copy) |

**Peak memory formula**: `min(count_cap × segment_size, byte_cap) × detachable_factor`.

For typical segments (≤40MB): count cap dominates, `min(num_segments, 200) × segment_size × 2` — same as today.
For wide tables (400MB+ segments): byte cap dominates, `4GB × 2 = 8GB` — prevents OOM.
Post-filter segments may be much smaller or empty.

### Column-Slice Merging Memory

For a 400-column table with 4 column slices per row group:

**Chosen approach (Arrow-level incremental merge)**:
- Convert slice 1 to Arrow → hold as `merged`
- Convert slice 2 to Arrow → `merged = horizontal_merge(merged, slice_2)` → release slice_2
- Continue for slices 3, 4
- Peak memory: `2 × row_group_arrow_size` (current merged + one incoming slice)

This is better than holding all 4 slices simultaneously (`4 × row_group_arrow_size`).

### DETACHABLE Block Optimization (Future)

`make_column_blocks_detachable()` copies every column buffer DYNAMIC → DETACHABLE, costing 1 memcpy per column per segment (~2x peak memory per segment during conversion).

**Optimization** (orthogonal to this plan): Modify `batch_read_uncompressed()` to allocate DETACHABLE directly when caller is the Arrow path. This eliminates the copy entirely. Requires threading `AllocationType` through the decode pipeline.

### Schema Padding Null Array Caching

Step 4 creates null arrays for missing columns in dynamic schema. Without caching, a symbol with 1000 segments where each is missing 500 columns creates 500,000 null arrays.

**Cache design**: `std::unordered_map<pair<ArrowTypeId, size_t>, shared_ptr<ArrowArray>>` keyed by `(arrow_type, row_count)`. Most segments have the same row count (default 100K), so cache hit rate approaches 100% for steady-state.

---

## Dependency Graph and Priority

```
Step 1 (extract helpers, refactor LazyRecordBatchIterator)  ───┐
                                                               │
Step 2 (C++ tests for lazy path)  ─── independent ───────────┤
                                                               │
Step 3 (column-slice merging in C++) ── depends on 1 ────────┤
                                                               │
Step 4 (schema padding in C++) ──────── depends on 1 ────────┤
                                                               │
Step 5 (route Arrow/Polars reads) ───── depends on 3+4 ──────┤
                                                               │
Step 6 (simplify ArcticRecordBatchReader) ─ depends on 5 ────┘
```

| Priority | Step | Effort | Impact |
|----------|------|--------|--------|
| **P0** | Step 1: Extract shared helpers, refactor `LazyRecordBatchIterator` + dual-cap backpressure | Medium | Architectural foundation; testable helpers; OOM protection for wide tables |
| **P0** | Step 2: C++ tests for lazy path | Medium | Fills critical test gap; no C++ tests exist for this code path; required before any refactoring |
| **P1** | Step 3: Column-slice merging in C++ | Medium | Prerequisite for unified Arrow output; eliminates Python merging |
| **P1** | Step 4: Schema padding in C++ | Medium | Prerequisite for unified Arrow output; fixes type-widening bug |
| **P1** | Step 5: Route Arrow/Polars reads via lazy iterator | Medium | Memory win for `lib.read(format=pyarrow/polars)` |
| **P2** | Step 6: Simplify `ArcticRecordBatchReader` | Low | Cleanup; remove dead Python code |

---

## Performance Invariants

These must hold after each step. Benchmark against the Phase 0.5 baseline (captured before any refactoring).

| Invariant | Metric | Acceptable Threshold |
|-----------|--------|---------------------|
| Pandas read not regressed | `lib.read(format='pandas')` throughput | ≤ 2% regression |
| Arrow throughput preserved | `lib.sql("SELECT * ...")` throughput | ≤ 5% regression (Step 1), improvement (Steps 3-5) |
| Memory bounded | `lib.read(format='pyarrow')` peak RSS | ≤ `8 × segment_size` (vs current `symbol_size`) |
| Wide table improved | `lib.sql()` on 400-column table | ≥ 2x improvement (Steps 3-5 vs Python merging) |
| DuckDB string queries | `lib.sql()` with string-heavy data | No regression (SharedStringDictionary preserved) |

---

## Implementation Checklist

Ordered, commit-sized work items. Each item is self-consistent: existing tests pass at every commit boundary. Phase ordering: **0 → 0.5 → 1 → 2 → 3 → 4 → 5 → 6 → 7**. Phase 0.5 (baseline benchmarks) MUST run before Phase 1 begins.

### Implementation Notes (from code review)

These are details discovered during review that the implementer must handle:

1. **`batch_read_uncompressed` returns `SegmentAndSlice`**, not `SegmentInMemory`. The segment is at `segment_and_slice.segment_in_memory_`. See [`arrow_output_frame.cpp:389`](../../../../cpp/arcticdb/arrow/arrow_output_frame.cpp#L389).

2. **`columns_to_decode` is already augmented** with filter input columns by [`version_store_api.cpp:1117-1120`](../../../../cpp/arcticdb/version/version_store_api.cpp#L1117-L1120) before the iterator is constructed. `LazyRecordBatchIterator` receives the complete set and passes it through unchanged.

3. **`RecordBatchData` wraps Arrow C Data Interface** (`ArrowArray` + `ArrowSchema`) via Sparrow. Constructed at [`arrow_output_frame.cpp:407-411`](../../../../cpp/arcticdb/arrow/arrow_output_frame.cpp#L407-L411) from `sparrow::extract_arrow_structures()`. Move-only semantics.

4. **`has_next()`**: `LazyRecordBatchIterator` checks `!prefetch_buffer_.empty() || !pending_batches_.empty()` — same as today.

5. **Existing C++ test coverage for lazy path is ZERO**: 5 Arrow read tests exist (all for low-level `segment_to_arrow_data` / string handlers). No tests for `LazyRecordBatchIterator`, `apply_truncation`, `apply_filter_clause`, or `prepare_segment_for_arrow`. All lazy path testing is via Python. Phase 2 tests are critical safety net before any refactoring.

6. **Python test gaps to fill in Phase 0/2**: No tests for integer type widening (int32→int64) via SQL, sparse columns created via append + SQL, or wide tables (>127 cols) with append. These should be added alongside the C++ tests.

7. **`segment_to_arrow_data` returns `shared_ptr<vector<sparrow::record_batch>>`** (see [`arrow_utils.hpp:30`](../../../../cpp/arcticdb/arrow/arrow_utils.hpp#L30)). The conversion to `RecordBatchData` happens via `sparrow::extract_arrow_structures()` on each record batch. This is where the Arrow C Data Interface ownership transfer occurs.

8. **`FilterRange` type**: `std::variant<std::monostate, entity::IndexRange, pipelines::RowRange>` (defined at [`arrow_output_frame.hpp:45`](../../../../cpp/arcticdb/arrow/arrow_output_frame.hpp#L45)). `std::monostate` means no truncation.

9. **Sparrow zero-copy child extraction chain (verified)**: Horizontal merging of `RecordBatchData` can be done without copying data buffers, but requires manual C struct manipulation. **Risk**: the extraction chain uses `detail::array_access::get_arrow_proxy()` which is Sparrow's **internal/detail API**, not part of the public API surface. ArcticDB already uses it ([`arrow_output_frame.cpp:331`](../../../../cpp/arcticdb/arrow/arrow_output_frame.cpp#L331)), but it may break across Sparrow upgrades. If Sparrow's internal API changes, consider requesting a public `record_batch::extract_column(size_t idx)` API from the Sparrow maintainers. The verified extraction path:
   ```
   record_batch::extract_struct_array()          // empties record_batch, moves struct_array out
     → detail::array_access::get_arrow_proxy()   // mutable access to arrow_proxy
       → arrow_proxy::children()                 // returns vector<arrow_proxy>&
         → child_proxy.extract_array()           // extracts ArrowArray by value (ownership transfer, O(1))
         → child_proxy.extract_schema()          // extracts ArrowSchema by value (ownership transfer, O(1))
   ```
   Reconstruction via `record_batch(ArrowArray&&, ArrowSchema&&)` constructor. Each step is a move — column data buffers stay in place. **Risk**: the parent `ArrowArray`'s `release` callback must only free the `children` pointer array, not recurse into children (which own their own release callbacks). Getting this wrong causes double-frees. Must be validated under ASan. See Phase 3 tests for required assertions.

10. **Multi-key data is orthogonal to lazy reads**: The read pipeline detects `KeyType::MULTI_KEY` at [`version_core.cpp:1263-1265`](../../../../cpp/arcticdb/version/version_core.cpp#L1263-L1265) (in `read_indexed_keys_to_pipeline`), sets `pipeline_context->multi_key_`, and `setup_pipeline_context()` returns early at [`version_core.cpp:2664-2666`](../../../../cpp/arcticdb/version/version_core.cpp#L2664-L2666) without populating `slice_and_keys_`. The lazy iterator rejects multi-key at [`version_store_api.cpp:1085-1088`](../../../../cpp/arcticdb/version/version_store_api.cpp#L1085-L1088). Multi-key data (from recursive normalizer) stores a `MULTI_KEY` index referencing leaf sub-symbols and requires Python-side reconstruction via `Flattener`. This plan does not change multi-key handling.

11. **`prepare_output_frame()` is related tech debt** (out of scope): [`version_core.cpp:1893-1912`](../../../../cpp/arcticdb/version/version_core.cpp#L1893-L1912) bridges between the processing clause path and direct reads. It duplicates `allocate_frame()`, `mark_index_slices()`, and segment copy logic also found in the eager direct-read path. While related to the lazy path's frame assembly, it's used by the processing clauses path (`read_process_and_collect()`), which this plan deliberately leaves unchanged.

---

### Phase 0: Standalone Bug Fixes (independent of refactor)

These can land on `master` immediately. No architectural changes.

- [ ] **0.1 Fix type-widening bug in `_ensure_schema()`**
  - [`arrow_reader.py:302-346`](../../../../python/arcticdb/version_store/duckdb/arrow_reader.py#L302-L346) — when a descriptor field type is wider than the first batch's type, use the descriptor type instead of the first batch type
  - Change: ~5 lines in `_ensure_schema()`, prefer descriptor type when `_is_wider_type(desc_type, batch_type)`
  - Test: write `int64` segment, append `float64` segment, `lib.sql("SELECT * FROM sym")` — currently crashes, should return `float64`
  - Additional test: write `int32` segment, append `int64` segment, verify `lib.sql()` returns int64 (integer widening, currently untested)
  - Additional test: type-widened column + WHERE filter + GROUP BY on that column
  - Test file: `python/tests/unit/arcticdb/version_store/duckdb/test_arrow_reader.py`

- [ ] **0.2 Expand fast-path to allow column projection**
  - [`library.py:2350`](../../../../python/arcticdb/version_store/library.py#L2350) — change `pushdown.columns is None` to just check `pushdown.fully_pushed`
  - Verify `fully_pushed` is never True when column projection would break semantics (audit `pushdown.py`)
  - Test: `lib.sql("SELECT a, b FROM sym")` where query is fully pushable — should skip DuckDB
  - Test file: `python/tests/unit/arcticdb/version_store/duckdb/test_pushdown.py`

---

### Phase 0.5: Capture Baseline Benchmarks (BEFORE Phase 1)

**Critical**: Baseline measurements must be captured before any refactoring begins. These become the regression threshold for every subsequent phase. Run on the current `duckdb` branch tip.

All results are saved to `docs/claude/plans/duckdb/benchmarks/` (committed to the branch) so they survive across sessions and can be compared in Phase 7.

- [ ] **0.5.1 ASV baseline**
  - Run from repo root with active venv: `asv run --python=$(which python) -v --show-stderr --bench "BasicFunctions|Arrow|SQLQueries"`
  - Copy results out of `python/.asv/results/` (git-ignored) into `docs/claude/plans/duckdb/benchmarks/baseline_asv.json`
  - Key metrics to capture: `time_read` (pandas), `time_read` (arrow), `peakmem_read` (arrow), `time_select_all` (SQL)
  - Commit baseline file to the branch

- [ ] **0.5.2 C++ benchmark baseline**
  - Build: `cmake -DTEST=ON --preset linux-debug cpp && cmake --build cpp/out/linux-debug-build --target benchmarks`
  - Run: `cpp/out/linux-debug-build/arcticdb/benchmarks --benchmark_filter="BM_arrow_string_handler" --benchmark_time_unit=ms --benchmark_min_time=5x --benchmark_out=docs/claude/plans/duckdb/benchmarks/baseline_cpp_arrow.json --benchmark_out_format=json`
  - Commit baseline file to the branch

- [ ] **0.5.3 Memory baseline**
  - Run: `python -m pytest python/tests/stress/arcticdb/version_store/test_stress_write_peakmem.py -v -s 2>&1 | tee docs/claude/plans/duckdb/benchmarks/baseline_memory.txt`
  - Also capture RSS during a representative Arrow read:
    ```python
    # Save output to docs/claude/plans/duckdb/benchmarks/baseline_arrow_read_rss.txt
    import arcticdb as adb, numpy as np, pandas as pd
    ac = adb.Arctic("lmdb://baseline_bench")
    lib = ac.create_library("bench", adb.LibraryOptions(rows_per_segment=100_000))
    df = pd.DataFrame(np.random.randn(1_000_000, 10), columns=[f"c{i}" for i in range(10)])
    lib.write("sym", df)
    # Measure peak RSS before/after read
    with open("/proc/self/status") as f: rss_before = [l for l in f if "VmHWM" in l][0]
    lib.read("sym", output_format=adb.OutputFormat.PYARROW)
    with open("/proc/self/status") as f: rss_after = [l for l in f if "VmHWM" in l][0]
    print(f"Before: {rss_before.strip()}, After: {rss_after.strip()}")
    ```
  - Commit baseline file to the branch

---

### Phase 1: Extract Shared Helpers and Refactor `LazyRecordBatchIterator` (Step 1)

Foundation commit. Refactors C++ only. Python is unchanged — existing `LazyRecordBatchIterator` behaviour is preserved exactly. The Arrow path's prefetch pipeline structure is untouched except for adding dual-cap backpressure (count + bytes) to prevent OOM with wide tables.

- [ ] **1.1 Extract shared free functions into `lazy_read_helpers.hpp/cpp`**
  - New files: `cpp/arcticdb/version/lazy_read_helpers.hpp`, `cpp/arcticdb/version/lazy_read_helpers.cpp`
  - Extract from `arrow_output_frame.cpp`: `read_and_decode_segment()`, `apply_truncation()`, `apply_filter_clause()`
  - These are pure functions with no iterator state — they take a segment/store/params and return a result
  - Register new `.cpp` in `CMakeLists.txt`

- [ ] **1.2 Refactor `LazyRecordBatchIterator` to use shared helpers + dual-cap backpressure**
  - `arrow_output_frame.hpp/cpp` — replace inline I/O + decode + truncate + filter code with calls to the shared helpers
  - **Prefetch pipeline structure is UNCHANGED** — same `deque<Future<vector<RecordBatchData>>>`, same monolithic future (I/O → decode → truncate → filter → Arrow convert)
  - `pending_batches_` deque for multi-block segments stays as-is
  - Add `current_slice_and_key()` and `peek_next_slice_and_key()` methods (needed for Step 3 column-slice merging)
  - **Add dual-cap backpressure** to `fill_prefetch_buffer()`:
    - New constructor param: `max_prefetch_bytes` (default 4GB)
    - New member: `current_prefetch_bytes_` — tracks estimated uncompressed bytes in flight
    - `fill_prefetch_buffer()` stops when EITHER `prefetch_buffer_.size() >= prefetch_size_` OR `current_prefetch_bytes_ >= max_prefetch_bytes_`
    - Segment size estimate from `slice_and_keys_[idx]` descriptor metadata (row_count × col_count × avg_type_size)
    - `next()` decrements `current_prefetch_bytes_` when consuming a resolved future
    - For typical segments (≤40MB), the count cap (200) triggers first — no behaviour change vs today
    - For wide tables (400MB+ segments), the byte cap prevents 200 × 400MB = 80GB OOM

- [ ] **1.3 Verify all existing tests pass**
  - Run: `python -m pytest -n 8 python/tests/unit/arcticdb/version_store/duckdb/`
  - Run: `cmake --build ... --target test_unit_arcticdb && ./test_unit_arcticdb --gtest_filter="Arrow*"`
  - This is a pure refactor — zero behaviour change. Any test failure means the extraction introduced a bug.
  - **Critical**: verify no throughput regression on the Arrow path. The monolithic prefetch pipeline in `LazyRecordBatchIterator` must be identical to before — same future depth, same overlap between I/O and `prepare_segment_for_arrow()`. For typical segments the dual-cap should behave identically to the old count-only cap.

- [ ] **1.4 Update documentation**
  - Update `docs/claude/cpp/ARROW.md`: document `lazy_read_helpers.hpp/cpp` shared helpers
  - Update `docs/claude/plans/duckdb/branch-work-log.md` with Phase 1 summary

---

### Phase 2: C++ Tests for Lazy Path (Step 2)

Fills the critical gap: no C++ tests exist for `LazyRecordBatchIterator`. These tests exercise the *existing* behaviour, serving as a regression safety net for later steps.

- [ ] **2.1 Shared helper unit tests**
  - New file: `cpp/arcticdb/version/test/test_lazy_read_helpers.cpp`
  - Add to `CMakeLists.txt` under `test_unit_arcticdb` target
  - Tests (each writes segments via InMemoryStore, exercises helpers directly):
    - `ReadAndDecode_Basic` — write segment, decode via `read_and_decode_segment()`
    - `ApplyTruncation_DateRange` — segments spanning [0,100), date_range=[25,75)
    - `ApplyTruncation_RowRange` — row_range limits across segment boundaries
    - `ApplyTruncation_ZeroRow` — date_range outside segment → zero-row segment
    - `ApplyFilterClause_Basic` — WHERE clause filters rows
    - `ApplyFilterClause_EmptyResult` — all rows filtered → returns false
    - `ApplyFilterClause_MissingColumn_DynamicSchema` — filter column absent, `dynamic_schema_=true` → EmptyResult
    - `SparseFloatWithTruncation` — sparse column truncated mid-segment, verify bitmap correct

- [ ] **2.2 `LazyRecordBatchIterator` Arrow round-trip tests**
  - New file: `cpp/arcticdb/arrow/test/test_lazy_record_batch_iterator.cpp`
  - Add to `CMakeLists.txt` under `test_unit_arcticdb` target
  - Tests:
    - `RoundTripNumeric` — write 5 numeric segments, verify Arrow batches match
    - `RoundTripString` — string columns through `SharedStringDictionary`
    - `MultiBlockSegment` — large segment → multiple 64KB blocks, `pending_batches_` drains correctly
    - `SparseFloatColumns` — validity bitmaps correct in Arrow output
    - `ZeroRowStringSegment` — `minimal_strings_for_dict()` workaround triggers
    - `TypeWidening_IntToFloat` — int64 first segment, float64 second → both readable without crash (pre-Step 4: batches have their ORIGINAL types, not unified types — unification happens in Step 4)

- [ ] **2.3 Verify all tests pass including new ones**
  - `./test_unit_arcticdb --gtest_filter="LazyReadHelpers*:LazyRecordBatch*"`

- [ ] **2.4 Python integration: lazy-vs-eager comparison test**
  - New file: `python/tests/unit/arcticdb/version_store/duckdb/test_lazy_vs_eager.py`
  - For each test scenario, read via `lib.read(output_format='pyarrow')` (eager) and via `lib.sql("SELECT * ...")` (lazy), compare results
  - Scenarios: numeric, string, sparse floats, date_range, row_range, column projection, dynamic schema, append-with-widening (int64 → float64)
  - This is the strongest correctness assertion: both paths must produce identical output

---

### Phase 3: Column-Slice Merging in C++ (Step 3)

After this commit, `LazyRecordBatchIterator` yields one merged batch per row group for column-sliced tables. Python `_merge_slices_for_group()` is still present but no longer exercised for the common path.

- [ ] **3.1 Verify `peek_next_slice_and_key()` on `LazyRecordBatchIterator`**
  - Already added in Step 1.2 — uses `LazyRecordBatchIterator`'s own `slice_and_keys_` vector
  - Verify it correctly peeks at the next unconsumed slice without advancing the iterator state
  - Column-slice merging in `next()` uses this to detect same-row-group slices

- [ ] **3.2 Implement `horizontal_merge_arrow_batches()` helper**
  - New function in `cpp/arcticdb/arrow/arrow_utils.hpp/cpp`
  - Takes two `RecordBatchData`, returns merged `RecordBatchData`
  - Deduplicates columns by name (skip columns from batch B that already exist in batch A — handles index column)
  - Columns ordered by merged descriptor field order
  - **Verified Sparrow extraction path** (see Implementation Note #9): For each source `RecordBatchData`, reconstruct a `sparrow::record_batch` from the owned `ArrowArray`/`ArrowSchema`, then extract child columns via `extract_struct_array() → get_arrow_proxy() → children() → child.extract_array()`. Each extraction is O(1) ownership transfer — column data buffers are NOT copied. Construct the merged parent `ArrowArray` with a new `children` pointer array (`ArrowArray**`) pointing to the extracted child arrays. Wrap in a new `RecordBatchData`.
  - **Release callback safety (CRITICAL)**: The merged parent `ArrowArray`'s `release` callback must:
    1. Free the `children` pointer array (`ArrowArray**`) and `ArrowSchema**`
    2. Call `release` on each child `ArrowArray`/`ArrowSchema` (they own their own buffers)
    3. NOT assume children are contiguous in memory — each was independently extracted
    4. Handle the case where a child's `release` is already `nullptr` (already released)
  - The safest pattern: store extracted children in a `vector<ArrowArray>` + `vector<ArrowSchema>` owned by the parent's release callback closure. The parent `ArrowArray.children[i]` points into this vector. On release, iterate the vector and call each child's release. This avoids manual `malloc`/`free` of the pointer array.
  - **Must be validated under ASan** (AddressSanitizer) — see Phase 3.4 tests

- [ ] **3.3 Add merging logic to `LazyRecordBatchIterator::next()`**
  - After pulling a batch from Arrow prefetch, check `peek_next_slice_and_key()` — same `row_range`?
  - If yes: pull all consecutive same-row-range slices, convert each to Arrow, merge incrementally
  - If no: return batch as-is (single-slice fast path)
  - Multi-block interaction: merge block-by-block across slices (see "Multi-block interaction" in Step 3 above). Return merged block 0, queue rest in `pending_batches_`

  **DECIDED — Merging in `next()` on consumer thread.** Merge is fast (pointer concatenation of Arrow arrays). I/O and Arrow conversion are the expensive parts and already run in prefetch futures. No third prefetch layer.

- [ ] **3.4 C++ tests for column-slice merging**
  - Add to `test_lazy_record_batch_iterator.cpp`:
    - `ColumnSliceMerge_TwoSlices` — 200 columns → 2 slices merged into 1 batch
    - `ColumnSliceMerge_FourSlices` — 500 columns → 4 slices merged
    - `ColumnSliceMerge_IndexDeduplication` — index column not duplicated in output
    - `ColumnSliceMerge_SingleSlice` — narrow table → no merging, passthrough
    - `ColumnSliceMerge_ColumnOrdering` — output column order matches descriptor
    - `ColumnSliceMerge_WithProjection` — projected subset of wide table
    - `ColumnSliceMerge_MixedTypes` — numeric + string + timestamp columns

- [ ] **3.4b Sparrow zero-copy validation and memory safety tests**
  - **Must run under ASan** (`-fsanitize=address`). The `linux-debug` preset should already include ASan. If not, add a CMake option or use a dedicated ASan preset.
  - Add to `test_lazy_record_batch_iterator.cpp` (or a dedicated `test_horizontal_merge.cpp`):

  **Sparrow API contract assertions** (assert the extraction chain works as expected):
    - `SparrowExtract_ChildOwnershipTransfer` — Extract a child via `extract_array()`, verify source `arrow_proxy` no longer owns it (child slot is null/empty). Ensures extraction is a move, not a copy.
    - `SparrowExtract_BufferPointersPreserved` — Before extraction, record the `ArrowArray.buffers[i]` pointers of a column. After extraction and re-wrapping in a new parent, verify the buffer pointers are identical. This directly asserts zero-copy: same memory addresses, no `memcpy`.
    - `SparrowExtract_SourceBatchEmptyAfterExtraction` — After extracting all children from a `record_batch` via `extract_struct_array()`, verify the source batch reports 0 columns / is in a moved-from state.

  **Release callback / memory safety assertions** (ASan will catch these if wrong):
    - `HorizontalMerge_NoDoubleFree` — Merge two batches, release the merged batch. ASan detects double-free if release callbacks are wrong. Also verify source batches are in a moved-from state and safe to destroy.
    - `HorizontalMerge_NoUseAfterFree` — Merge two batches, destroy source batches first (they should be empty), then read data from merged batch. ASan detects use-after-free if buffers were prematurely freed.
    - `HorizontalMerge_NoLeak` — Merge two batches, drop all references. ASan leak detector (or valgrind) confirms no leaked `ArrowArray`/`ArrowSchema` structs or data buffers.
    - `HorizontalMerge_ReleaseIdempotent` — Call release on merged batch, verify calling release again is safe (release sets itself to `nullptr`, second call is a no-op per Arrow C Data Interface spec).

  **End-to-end memory correctness through the full pipeline:**
    - `ColumnSliceMerge_ASan_FullPipeline` — Write a wide table (200+ cols), read back through `LazyRecordBatchIterator` with merging enabled, consume all batches, destroy iterator. Run under ASan. This is the most important test: exercises the real code path, not a unit-test mock.
    - `ColumnSliceMerge_PartialConsume_NoLeak` — Create iterator over wide table, consume only 2 of 10 row groups, destroy iterator. Remaining prefetched/merged batches must be released cleanly. ASan detects leaks.

  **How to run under ASan:**
  ```bash
  # ASan is NOT enabled by default in linux-debug — must be explicitly enabled:
  cmake -DTEST=ON -DARCTICDB_USING_ADDRESS_SANITIZER=ON --preset linux-debug cpp
  cmake --build cpp/out/linux-debug-build --target test_unit_arcticdb
  # Run the merge tests — ASan is active via the ARCTICDB_USING_ADDRESS_SANITIZER option
  cpp/out/linux-debug-build/arcticdb/test_unit_arcticdb --gtest_filter="*HorizontalMerge*:*SparrowExtract*:*ColumnSliceMerge*"
  ```
  Note: ASan build is controlled by `cpp/CMake/sanitizers.cmake` via the `ARCTICDB_USING_ADDRESS_SANITIZER` CMake option. There are also `ARCTICDB_USING_THREAD_SANITIZER` and `ARCTICDB_USING_UB_SANITIZER` options available.

- [ ] **3.5 Python integration test for wide table via sql()**
  - Add to `test_arctic_duckdb.py`:
    - Write 400-column table, `lib.sql("SELECT * FROM sym")` — verify all columns present, correct values
    - Write 400-column table with column projection, `lib.sql("SELECT col1, col200, col399 FROM sym")` — verify correct
  - Existing Python tests must still pass (Python merging code is still present but now redundant for this path)

- [ ] **3.6 Verify all tests pass**

- [ ] **3.7 Update documentation**
  - Update `docs/claude/cpp/ARROW.md`: document `horizontal_merge_arrow_batches()`, Sparrow extraction chain usage, column-slice merging in `next()`
  - Update `docs/claude/python/DUCKDB.md`: note that column-slice merging now happens in C++ (Python `_merge_slices_for_group()` still present but bypassed)
  - Update `docs/claude/plans/duckdb/branch-work-log.md` with Phase 3 summary

---

### Phase 4: Schema Padding in C++ (Step 4)

After this commit, `LazyRecordBatchIterator` yields uniform-schema batches for dynamic schema symbols. Fixes the type-widening bug.

- [ ] **4.1 Build target schema from merged descriptor in C++**
  - At `LazyRecordBatchIterator` construction, build `target_schema` (Arrow schema) from the merged `StreamDescriptor`
  - Map each `DataType` → Arrow type using the *C++ type system* (not the incomplete Python mapping)
  - Handle: all numeric types, `NANOSECONDS_UTC64`, `UTF_DYNAMIC64`/`ASCII_DYNAMIC64` → `dictionary(int32, large_string)`, `UTF_FIXED64`/`ASCII_FIXED64` → `large_string`, `BOOL8` → `bool`, `EMPTYVAL` → `large_string` (matching Python fallback)
  - When `columns_to_decode` is set (column projection), restrict target schema to projected columns only

- [ ] **4.2 Implement schema padding in `LazyRecordBatchIterator::next()`**
  - After merge (Step 3), compare batch schema to target schema
  - **Static schema fast path**: if `batch.schema == target_schema` (pointer or field-by-field compare), return as-is — zero overhead for the common case
  - For mismatched batches:
    - Missing columns → create null Arrow array of target type and batch row count
    - Type mismatch → safe upcast (int→float, smaller→larger int)
    - Reorder columns to match target field order
  - Null array cache: `unordered_map<pair<ArrowTypeId, size_t>, RecordBatchData::array_ptr>` — cache by `(type, row_count)` for reuse across batches

- [ ] **4.3 C++ tests for schema padding**
  - Add to `test_lazy_record_batch_iterator.cpp`:
    - `SchemaPadding_MissingNumericColumn` — segment lacks float64 col → null added
    - `SchemaPadding_MissingStringColumn` — segment lacks string col → null dictionary added
    - `SchemaPadding_TypeWidening` — int64 first, float64 second → both output as float64
    - `SchemaPadding_StaticSchema` — all segments same schema → no-op, no overhead
    - `SchemaPadding_ColumnOrdering` — columns reordered to match descriptor
    - `SchemaPadding_WithColumnProjection` — only projected columns padded
    - `SchemaPadding_NullArrayCaching` — verify cache hit (second batch same size → no new allocation)

- [ ] **4.4 Python integration tests for dynamic schema**
  - Add to `test_duckdb_dynamic_schema.py`:
    - Write segment 1 with cols `{a, b}`, segment 2 with cols `{a, c}` → `lib.sql("SELECT * FROM sym")` returns `{a, b, c}` with nulls
    - Write int64 segment, append float64 segment → `lib.sql("SELECT * FROM sym")` returns float64 (verifies Bug 1 fix)
  - Existing dynamic schema tests must still pass

- [ ] **4.5 Verify all tests pass**

- [ ] **4.6 Update documentation**
  - Update `docs/claude/cpp/ARROW.md`: document `arrow_schema()` method, target schema construction from descriptor, schema padding logic, null array caching
  - Update `docs/claude/python/DUCKDB.md`: update "Schema Discovery" section — schema now comes from C++ descriptor (remove first-batch type refinement), update "Known Limitation: int → float Type Widening" to mark as FIXED
  - Update `docs/claude/plans/duckdb/branch-work-log.md` with Phase 4 summary

---

### Phase 5: Route `lib.read(format='pyarrow')` Through Lazy Path (Step 5)

After this commit, `lib.read(output_format='pyarrow')` uses streaming instead of full materialization. Memory = O(prefetch × segment_size) instead of O(symbol_size).

- [ ] **5.1 Add `OutputFormat::ARROW` branch in `read_dataframe_version()`**
  - `version_store_api.cpp` — for `OutputFormat::ARROW` without processing clauses, construct `LazyRecordBatchIterator` and return it instead of calling `do_direct_read_or_process()`
  - Keep eager path for: `OutputFormat::PANDAS`, any read with processing clauses (GROUP BY, resample)
  - **Error handling guards** (must check before constructing lazy iterator):
    - `pipeline_context->multi_key_` → reject with error (recursive normalizer data requires eager denormalization)
    - `!read_query->clauses_.empty()` → fall back to eager path (processing clauses need `ComponentManager`)
    - Type handler assertion: if any column uses a Pandas-only type handler (`BOOL_OBJECT8`, `PythonEmptyHandler`, `PythonArrayHandler`), raise `"Type X requires pandas output format"` — these types have no `ArrowStringHandler` equivalent
    - Pickled data: already rejected by existing multi_key_ check (pickled uses MULTI_KEY storage)

- [ ] **5.2 Update `_adapt_frame_data()` to accept lazy iterator**
  - [`_store.py:2821-2845`](../../../../python/arcticdb/version_store/_store.py#L2821-L2845) — detect if return type is lazy iterator
  - Iterate `next()`, import each `RecordBatchData` via `pa.RecordBatch._import_from_c()`, collect into `pa.Table`
  - Schemas are uniform (Steps 3+4), so `pa.Table.from_batches()` works directly

- [ ] **5.3 Update Python bindings**
  - `python_bindings.cpp` — return type for Arrow reads changes to lazy iterator when appropriate
  - Ensure `py::gil_scoped_release` is held during iterator construction (already the case)

- [ ] **5.4 Test: `lib.read(output_format='pyarrow')` functional correctness**
  - Numeric, string, dynamic schema, wide table, date_range, empty symbol
  - Compare output to eager path result for each case
  - Add to `python/tests/unit/arcticdb/version_store/duckdb/test_lazy_streaming.py`

- [ ] **5.5 Test: memory bounded**
  - Write 100-segment symbol (100K rows × 10 float64 cols each = ~80MB per segment)
  - `lib.read(output_format='pyarrow')` — peak RSS should be ≤ `8 × segment_size` (~640MB), NOT `100 × segment_size` (~8GB)
  - Can use `/proc/self/status` VmRSS tracking or `tracemalloc` for the Python side

- [ ] **5.6 Test: pandas path NOT regressed**
  - `lib.read()` (default pandas format) throughput benchmark before/after
  - Must use eager path, NOT the new lazy path
  - ≤ 2% regression threshold

- [ ] **5.7 Verify all tests pass**

- [ ] **5.8 Update documentation**
  - Update `docs/claude/cpp/ARROW.md`: document `OutputFormat::ARROW` routing in `read_dataframe_version()`, error handling guards
  - Update `docs/claude/python/DUCKDB.md`: update "ArrowOutputFrame" section to note it's bypassed for `lib.read(format='pyarrow')`, add note about streaming memory profile
  - Update `docs/mkdocs/docs/api/` or docstrings: document `output_format='pyarrow'` memory characteristics (streaming, bounded by prefetch window)
  - Update `docs/claude/plans/duckdb/branch-work-log.md` with Phase 5 summary

---

### Phase 6: Simplify Python `ArcticRecordBatchReader` (Step 6)

Cleanup commit. Removes ~258 lines of dead Python code. Note: the filter pushdown workaround (`qb = None if columns is not None`) is KEPT — see Amendment A.

- [ ] **6.1 Remove dead code from `arrow_reader.py`**
  - Delete: `_descriptor_to_arrow_schema()`, `_is_same_row_group()`, `_merge_slices_for_group()`, `_merge_column_slices()`, `_pad_batch_to_schema()`, `_ensure_schema()`, `_read_next_merged_batch()`, `_pending_raw_batch` state
  - Keep: `_strip_idx_prefix_from_names()`, `_iteration_started`/`_exhausted` guards, `to_pyarrow_reader()` (simplified), `read_all()`
  - Add `schema` property that gets Arrow schema from C++ iterator (new C++ method `arrow_schema()`)

- [ ] **6.2 Remove workarounds from `library.py:sql()`**
  - **KEEP** `qb = pushdown.query_builder if columns is None else None` — per-segment filtering runs before column-slice merging; filter column is only in one slice per row group; static schema would crash on missing column (see Amendment A)
  - Remove `lib_dynamic_schema` workaround — C++ merging prevents spurious NULL rows
  - Keep `__idx__` prefix expansion (storage-level concern)

- [ ] **6.3 Update tests that relied on Python-side merging/padding**
  - Any test mocking or patching `_merge_slices_for_group`, `_pad_batch_to_schema`, `_ensure_schema` → update or remove
  - Integration tests should be unchanged (same external behaviour)

- [ ] **6.4 Verify all tests pass**
  - `python -m pytest -n 8 python/tests/unit/arcticdb/version_store/duckdb/`
  - Full test suite to catch any regressions

- [ ] **6.5 Update documentation**
  - Rewrite `docs/claude/python/DUCKDB.md` "Module: arrow_reader.py" section: remove references to deleted functions (`_pad_batch_to_schema`, `_ensure_schema`, `_is_same_row_group`, `_merge_slices_for_group`, `_descriptor_to_arrow_schema`), update "Key Properties" and "Schema Discovery" sections to reflect simplified `ArcticRecordBatchReader`
  - Update `docs/claude/plans/duckdb/branch-work-log.md` with Phase 6 summary

---

### Phase 7: Benchmarks and Validation

Verification that performance invariants hold. Includes C++ microbenchmarks, Python microbenchmarks, and ASV regression tests.

#### 7A. C++ Microbenchmarks for Memory and CPU

New file: `cpp/arcticdb/arrow/test/benchmark_lazy_read.cpp`

These are Google Benchmark tests that measure the lazy read path directly, complementing the higher-level ASV benchmarks.

- [ ] **7.1 C++ throughput benchmarks (Google Benchmark)**

  | Benchmark | What It Measures |
  |-----------|-----------------|
  | `BM_LazyVsEagerRead_Numeric` | Throughput: lazy iterator vs eager `read_dataframe_version`, numeric columns (10 float64 cols × 100K rows × 10 segments) |
  | `BM_LazyVsEagerRead_String` | Same with string columns — measures `SharedStringDictionary` overhead in lazy path |
  | `BM_ArrowConversionOverhead` | Isolate `prepare_segment_for_arrow` cost: time from `SegmentInMemory` to `RecordBatchData` |
  | `BM_PrefetchScaling` | prefetch_size = 1, 2, 4, 8, 16, 32 — throughput scaling curve |
  | `BM_ColumnSliceMerge` | Wide table (400 cols, ~4 slices/row group): merge overhead per row group |
  | `BM_SchemaPadding` | Dynamic schema (500 cols, 50% missing per segment): padding overhead per batch |
  | `BM_LazyVsEagerOverall` | End-to-end: lazy path → collect all batches vs eager path → single frame, same data |

- [ ] **7.2 C++ memory leak detection benchmarks (Google Benchmark)**

  | Benchmark | What It Measures |
  |-----------|-----------------|
  | `BM_LazyIterator_PeakRSS` | Read N segments (N=10,50,100) with prefetch_size=K (K=5,10,20). Measure VmHWM from `/proc/self/status`. Peak should be O(K × segment_size), not O(N × segment_size). |
  | `BM_LazyIterator_RepeatedRead` | Create iterator → consume all → destroy, repeat 1000 times. Measure RSS after each cycle. Linear growth = leak. |
  | `BM_LazyIterator_SegmentLifetime` | Read 100 segments. After consuming segment i, instrument allocator to verify segments 0..i-1 are freed. Uses `ARCTICDB_COUNT_ALLOCATIONS` or a custom counting allocator. |
  | `BM_HorizontalMerge_AllocationCount` | Merge N column slices (N=2,4,8). Count total heap allocations. Should be O(N), not O(N²). |
  | `BM_PartialConsume_NoLeak` | Create iterator over 100 segments. Consume only 10. Destroy. Measure RSS before/after — must not leak prefetched-but-unconsumed segments. |

- [ ] **7.3 C++ CPU utilization benchmarks (Google Benchmark)**

  | Benchmark | What It Measures |
  |-----------|-----------------|
  | `BM_LazyIterator_CPUOverhead` | Compare CPU time (`std::clock()`) for lazy vs eager on same data. Lazy path should spend <10% more CPU than eager for identical data (overhead from prefetch management, future resolution). |
  | `BM_PrefetchPipeline_Utilization` | Measure wall-clock time breakdown: I/O wait vs CPU processing (Arrow conversion) vs idle (consumer blocked on `.get()`). Validates prefetch design overlaps I/O with computation. Use custom instrumentation: record timestamps at I/O start, I/O complete, Arrow convert start, Arrow convert complete. |
  | `BM_ColumnSliceMerge_CPUCost` | Given N pre-converted Arrow batches, measure CPU cost of `horizontal_merge_arrow_batches()` alone. Verify dominated by pointer manipulation (O(cols)), not memcpy (O(data)). |
  | `BM_SchemaPadding_CPUCost` | Measure CPU cost of null array creation + column reordering. Verify caching eliminates repeated allocation (second invocation with same row_count should be ~0 cost). |

#### 7B. Python Microbenchmarks for Memory and CPU

- [ ] **7.4 Python ASV benchmark suite for lazy read path**
  New file: `python/benchmarks/lazy_read.py`

  ```
  class LazyReadThroughput:
      """Compare lazy vs eager read throughput."""
      params = ([100_000, 1_000_000], ['numeric', 'string', 'mixed'])
      param_names = ['rows', 'data_type']

      - time_read_arrow: lib.read(format='pyarrow') — lazy path (after Step 5)
      - time_read_pandas_then_arrow: lib.read(format='pandas').to_arrow() — eager baseline
      - time_sql_select_all: lib.sql("SELECT * FROM sym") — DuckDB path
      - peakmem_read_arrow: peak RSS during lib.read(format='pyarrow')
      - peakmem_sql_select_all: peak RSS during lib.sql("SELECT * FROM sym")

  class LazyReadWideTable:
      """Wide table benchmarks (column-slice merging)."""
      params = ([200, 400, 800], [1_000_000])  # num_cols, rows
      param_names = ['num_cols', 'rows']

      - time_sql_select_all: lib.sql("SELECT * FROM sym") — measures C++ merge
      - time_sql_select_projected: lib.sql("SELECT col1, col200 FROM sym") — projection
      - peakmem_sql_select_all: peak RSS during wide table read
      - peakmem_read_arrow: peak RSS during lib.read(format='pyarrow') on wide table

  class LazyReadDynamicSchema:
      """Dynamic schema benchmarks (schema padding)."""
      params = ([100_000], [50, 200])  # rows, num_cols_per_segment_variation
      param_names = ['rows', 'schema_variation']

      - time_sql_dynamic_schema: lib.sql("SELECT * FROM sym") with dynamic schema
      - peakmem_sql_dynamic_schema: peak RSS
  ```

- [ ] **7.5 Python stress tests for memory leak detection**
  New file: `python/tests/stress/arcticdb/version_store/test_stress_lazy_read.py`

  | Test | What It Measures |
  |------|-----------------|
  | `test_lazy_read_no_memory_growth` | Read same symbol 1000 times via `lib.read(format='pyarrow')`. Track RSS via `/proc/self/status` VmRSS every 100 iterations. Assert RSS growth < 5% of single-read RSS. |
  | `test_lazy_partial_consume_no_leak` | Create `LazyRecordBatchIterator` via `_read_as_record_batch_reader()`, consume 10 of 100 segments, destroy reader. Repeat 100 times. Assert no RSS growth. |
  | `test_lazy_wide_table_memory_bound` | 400 cols × 1M rows, `lib.read(format='pyarrow')` peak RSS < 8× segment_size (bounded, not proportional to symbol size). |
  | `test_sql_repeated_no_memory_growth` | Call `lib.sql("SELECT * FROM sym")` 500 times. Assert RSS growth < 5%. Catches DuckDB-side or Arrow-side leaks in the integration. |
  | `test_lazy_read_mixed_types_no_leak` | Symbol with int, float, string, timestamp, bool columns. Read 500 times. Assert no RSS growth. Catches type-handler-specific leaks. |

#### 7C. ASV Regression Tests (Before/After Comparison)

- [ ] **7.6 ASV benchmarks: before/after comparison**
  - Compare against Phase 0.5 baseline
  - Run `BasicFunctions`, `Arrow`, `SQLQueries` suites on the branch vs baseline
  - `lib.read(format='pandas')` throughput: ≤ 2% regression
  - `lib.sql("SELECT * ...")` throughput: ≤ 5% regression (Steps 1-2), improvement (Steps 3-6)

- [ ] **7.7 Wide table benchmark**
  - 400-column table, 1M rows, `lib.sql("SELECT * FROM sym")` and `lib.sql("SELECT col1 FROM sym WHERE col200 > 0")`
  - Target: ≥ 2x improvement over Python merging

- [ ] **7.8 Memory profiling**
  - `lib.read(format='pyarrow')` on 100-segment symbol
  - Verify peak RSS ≈ `prefetch_size × segment_size × 2` (same as current behavior)

- [ ] **7.9 C++ benchmark comparison**
  - Compare `benchmark_lazy_read.cpp` results against Phase 0.5.2 baseline
  - Verify: `BM_LazyVsEagerRead_Numeric` throughput ≤ 5% regression vs eager
  - Verify: `BM_LazyIterator_PeakRSS` is O(prefetch) not O(total_segments)
  - Verify: `BM_LazyIterator_RepeatedRead` shows no RSS growth over 1000 iterations

---

### Open Questions — All Resolved

1. ~~**Polars LazyFrame exposure**~~ — **DECIDED: defer.** `pl.from_arrow()` is already zero-copy from Arrow RecordBatches. A `SegmentInMemory` path would require `pl.from_numpy()` which *clones* data — strictly worse. Not needed for correctness or performance.

2. ~~**`max_prefetch_bytes` backpressure**~~ — **DECIDED: implement in Phase 1.** Count-based cap alone is insufficient — 200 segments × 400MB (wide tables) = 80GB. Added dual-cap design to Phase 1 (Step 1.2): byte-based cap (default 4GB) alongside existing count-based cap (200). Whichever limit is reached first stops prefetching. Segment size estimated from `SliceAndKey` descriptor metadata. ~20 lines of C++. See Phase 1, Step 1.2 for details.

3. ~~**Separate `LazySegmentIterator`**~~ — **DECIDED: not needed.** Originally planned as a sibling iterator yielding `SegmentInMemory` for non-Arrow consumers. No consumer exists in this plan (Polars deferred, eager path untouched). The shared free functions (`read_and_decode_segment`, `apply_truncation`, `apply_filter_clause`) provide the reusable building blocks; a `LazySegmentIterator` can be trivially built from them if a consumer materializes later.

4. ~~**`__idx__` stripping in C++**~~ — **DECIDED: keep in Python.** Measured cost: <1ms for 400 columns. PyArrow `rename_columns()` is a zero-copy metadata update. Moving to C++ adds complexity for no measurable gain.

---

## Key File References

| File | Relevant Lines | Content |
|------|---------------|---------|
| [`cpp/arcticdb/arrow/arrow_output_frame.hpp`](../../../../cpp/arcticdb/arrow/arrow_output_frame.hpp#L135-L213) | 135–213 | `LazyRecordBatchIterator` class definition |
| [`cpp/arcticdb/arrow/arrow_output_frame.cpp`](../../../../cpp/arcticdb/arrow/arrow_output_frame.cpp#L340-L549) | 340–549 | `LazyRecordBatchIterator` implementation |
| [`cpp/arcticdb/arrow/arrow_output_frame.cpp`](../../../../cpp/arcticdb/arrow/arrow_output_frame.cpp#L363-L413) | 363–413 | `read_decode_and_prepare_segment()` — **full pipeline runs in future** |
| [`cpp/arcticdb/arrow/arrow_output_frame.cpp`](../../../../cpp/arcticdb/arrow/arrow_output_frame.cpp#L38-L56) | 38–56 | `make_column_blocks_detachable()` — DYNAMIC→DETACHABLE copy |
| [`cpp/arcticdb/arrow/arrow_output_frame.cpp`](../../../../cpp/arcticdb/arrow/arrow_output_frame.cpp#L62-L202) | 62–202 | `SharedStringDictionary` struct + `build_shared_dictionary()` + `encode_dictionary_with_shared_dict()` |
| [`cpp/arcticdb/arrow/arrow_output_frame.cpp`](../../../../cpp/arcticdb/arrow/arrow_output_frame.cpp#L204-L304) | 204–304 | `prepare_segment_for_arrow()` — string handling, sparse floats, detachable blocks |
| [`cpp/arcticdb/arrow/arrow_output_frame.cpp`](../../../../cpp/arcticdb/arrow/arrow_output_frame.cpp#L481-L518) | 481–518 | `apply_filter_clause()` — EmptyResult/FullResult/BitSet |
| [`cpp/arcticdb/arrow/arrow_output_frame.cpp`](../../../../cpp/arcticdb/arrow/arrow_output_frame.cpp#L423-L478) | 423–478 | `apply_truncation()` — date_range/row_range |
| [`cpp/arcticdb/arrow/arrow_utils.cpp`](../../../../cpp/arcticdb/arrow/arrow_utils.cpp#L103-L105) | 103–105 | `minimal_strings_for_dict()` — empty dict workaround |
| [`cpp/arcticdb/arrow/arrow_utils.cpp`](../../../../cpp/arcticdb/arrow/arrow_utils.cpp#L295-L310) | 295–310 | `segment_to_arrow_data()` — zero-row handling |
| [`cpp/arcticdb/version/version_store_api.cpp`](../../../../cpp/arcticdb/version/version_store_api.cpp#L1061-L1152) | 1061–1152 | `create_lazy_record_batch_iterator()` — lazy path setup |
| [`cpp/arcticdb/version/version_store_api.cpp`](../../../../cpp/arcticdb/version/version_store_api.cpp#L1133-L1140) | 1133–1140 | prefetch size calculation (count cap=200; byte cap added in Phase 1) |
| [`cpp/arcticdb/version/version_store_api.cpp`](../../../../cpp/arcticdb/version/version_store_api.cpp#L1045-L1059) | 1045–1059 | `read_dataframe_version()` — eager path entry |
| [`cpp/arcticdb/version/version_core.cpp`](../../../../cpp/arcticdb/version/version_core.cpp#L2061-L2088) | 2061–2088 | `do_direct_read_or_process()` — eager read/process fork |
| [`cpp/arcticdb/version/version_core.cpp`](../../../../cpp/arcticdb/version/version_core.cpp#L2647-L2694) | 2647–2694 | `setup_pipeline_context()` — shared setup |
| [`cpp/arcticdb/version/version_core.cpp`](../../../../cpp/arcticdb/version/version_core.cpp#L2714-L2765) | 2714–2765 | `read_frame_for_version()` — eager path orchestration |
| [`cpp/arcticdb/pipeline/pipeline_utils.hpp`](../../../../cpp/arcticdb/pipeline/pipeline_utils.hpp#L30-L35) | 30–35 | Type handler assertion (PANDAS-only) |
| [`cpp/arcticdb/pipeline/pipeline_utils.hpp`](../../../../cpp/arcticdb/pipeline/pipeline_utils.hpp#L64-L80) | 64–80 | RangeIndex step=0 backward compatibility patch |
| [`cpp/arcticdb/pipeline/pipeline_utils.hpp`](../../../../cpp/arcticdb/pipeline/pipeline_utils.hpp#L84-L90) | 84–90 | `create_python_read_result()` — output format branching |
| [`cpp/arcticdb/pipeline/read_frame.cpp`](../../../../cpp/arcticdb/pipeline/read_frame.cpp#L821-L929) | 821–929 | `NullValueReducer` — dynamic schema null backfill (eager path) |
| [`cpp/arcticdb/pipeline/read_frame.cpp`](../../../../cpp/arcticdb/pipeline/read_frame.cpp#L676-L716) | 676–716 | `promote_integral_type()` — type widening (eager path) |
| [`cpp/arcticdb/pipeline/read_frame.cpp`](../../../../cpp/arcticdb/pipeline/read_frame.cpp#L1033-L1069) | 1033–1069 | `reduce_and_fix_columns()` — column-slice merging (eager path) |
| [`cpp/arcticdb/python/python_handlers_common.hpp`](../../../../cpp/arcticdb/python/python_handlers_common.hpp#L15-L31) | 15–31 | Type handler registration (PANDAS and ARROW) |
| [`cpp/arcticdb/arrow/arrow_handlers.hpp`](../../../../cpp/arcticdb/arrow/arrow_handlers.hpp#L50-L65) | 50–65 | `ArrowStringHandler` registration |
| [`python/arcticdb/version_store/duckdb/arrow_reader.py`](../../../../python/arcticdb/version_store/duckdb/arrow_reader.py#L81-L108) | 81–108 | `_pad_batch_to_schema()` — Python schema padding |
| [`python/arcticdb/version_store/duckdb/arrow_reader.py`](../../../../python/arcticdb/version_store/duckdb/arrow_reader.py#L183-L216) | 183–216 | `_is_same_row_group()` — Python same-row-group detection |
| [`python/arcticdb/version_store/duckdb/arrow_reader.py`](../../../../python/arcticdb/version_store/duckdb/arrow_reader.py#L219-L239) | 219–239 | `_merge_slices_for_group()` — Python column-slice merging |
| [`python/arcticdb/version_store/duckdb/arrow_reader.py`](../../../../python/arcticdb/version_store/duckdb/arrow_reader.py#L302-L346) | 302–346 | `_ensure_schema()` — Python schema discovery |
| [`python/arcticdb/version_store/library.py`](../../../../python/arcticdb/version_store/library.py#L2409-L2415) | 2409–2415 | Filter pushdown disabled when columns projected |
| [`python/arcticdb/version_store/_store.py`](../../../../python/arcticdb/version_store/_store.py#L2304-L2365) | 2304–2365 | `NativeVersionStore.read()` — Python read entry point |
| [`python/arcticdb/version_store/_store.py`](../../../../python/arcticdb/version_store/_store.py#L2821-L2845) | 2821–2845 | `_adapt_frame_data()` — output format conversion |
| [`python/arcticdb/version_store/library.py`](../../../../python/arcticdb/version_store/library.py#L2226-L2460) | 2226–2460 | `Library.sql()` — SQL orchestration |
