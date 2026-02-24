# Arrow Output & Lazy Streaming

Arrow C Data Interface integration for streaming ArcticDB data to DuckDB and PyArrow consumers.

## Location

```
cpp/arcticdb/arrow/
├── arrow_output_frame.hpp/cpp          # RecordBatchData, ArrowOutputFrame
├── lazy_record_batch_iterator.hpp/cpp  # LazyRecordBatchIterator, prepare_segment_for_arrow, SharedStringDictionary
├── arrow_output_options.hpp            # ArrowOutputStringFormat enum, ArrowOutputConfig struct
├── arrow_handlers.hpp/cpp              # Per-type Arrow conversion (string, numeric, timestamp)
└── arrow_utils.hpp/cpp                 # segment_to_arrow_data(), horizontal_merge, schema padding (TargetField, pad_batch_to_schema)

cpp/arcticdb/pipeline/
├── filter_range.hpp          # FilterRange type alias (variant of monostate, IndexRange, RowRange)
└── lazy_read_helpers.hpp/cpp # apply_truncation(), apply_filter_clause(), estimate_segment_bytes()
```

## Classes

### RecordBatchData

Single Arrow record batch: `ArrowArray` + `ArrowSchema` pair (Arrow C Data Interface).

```
RecordBatchData
├── array_   (ArrowArray)  — zero-initialized with std::memset
├── schema_  (ArrowSchema) — zero-initialized with std::memset
├── array() → uintptr_t    (reinterpret_cast<uintptr_t>(&array_), for Python bindings)
└── schema() → uintptr_t   (reinterpret_cast<uintptr_t>(&schema_), for Python bindings)
```

**Key design**: Zero-initialized in constructor via `std::memset` to ensure safe release callback behavior. The Arrow C Data Interface requires that `release` is either `NULL` (no-op) or a valid callback.

### LazyRecordBatchIterator

On-demand segment reader — the **primary path** for SQL/DuckDB queries. Reads and decodes one segment at a time from storage, with prefetch for latency hiding.

```
LazyRecordBatchIterator
├── slice_and_keys_         (vector<SliceAndKey> — segment metadata from index-only read)
├── descriptor_             (StreamDescriptor — schema, available even for empty symbols)
├── store_                  (shared_ptr<StreamSource> — storage backend)
├── columns_to_decode_      (shared_ptr<unordered_set<string>> — column projection)
├── prefetch_buffer_        (deque<Future<vector<RecordBatchData>>>, default size 2)
├── row_filter_             (FilterRange variant: IndexRange | RowRange | monostate)
├── expression_context_     (shared_ptr<ExpressionContext> — FilterClause from WHERE)
├── pending_batches_        (deque<RecordBatchData> — multi-block segment buffer)
│
├── next() → optional<RecordBatchData>
│   ├── drain pending_batches_ first (multi-block segments)
│   ├── block on prefetch_buffer_.front().get() — returns prepared batches
│   └── fill_prefetch_buffer() — kick off next reads
│
├── read_decode_and_prepare_segment(idx) → Future<vector<RecordBatchData>>
│   ├── batch_read_uncompressed() — I/O future
│   └── .via(&cpu_executor()).thenValue() — **parallel on CPU pool**:
│       ├── apply_truncation(segment, slice_row_range, row_filter)
│       ├── apply_filter_clause(segment, expr_ctx, filter_name)
│       ├── prepare_segment_for_arrow(segment)
│       └── segment_to_arrow_data() + RecordBatchData conversion
│
├── has_next() → bool
├── num_batches() → size_t
├── current_index() → size_t
├── descriptor() → StreamDescriptor
├── field_count() → size_t
├── current_slice_and_key() → const SliceAndKey& (current consumption position)
└── peek_slice_and_key(offset) → const SliceAndKey* (nullptr if out of range)
```

**Key member variables** (beyond those in diagram): `has_column_slicing_` (bool — detects column slicing at construction by scanning `slice_and_keys_`; when true, per-segment filter evaluation is skipped), `target_fields_` (vector of `TargetField` for schema padding — built from descriptor, formats resolved eagerly at construction from descriptor + ReadOptions), `read_options_` (controls string format output), `max_prefetch_bytes_` (default 4GB, dual-cap backpressure), `current_prefetch_bytes_` (tracks bytes in flight).

**Prefetch + Parallel Conversion**: `fill_prefetch_buffer()` maintains up to `prefetch_size_` (default 2) in-flight `folly::Future<vector<RecordBatchData>>` via `read_decode_and_prepare_segment()`. Each future chains I/O (`batch_read_uncompressed`) with CPU-intensive work (truncation, filter, Arrow conversion) via `.via(&async::cpu_executor())`. This means `prepare_segment_for_arrow()` runs on the **CPU thread pool in parallel** across segments — critical for wide tables where Arrow conversion takes seconds per segment.

**Truncation**: `apply_truncation()` is a free function in `pipeline/lazy_read_helpers.hpp` — handles `IndexRange` (timestamp binary search) and `RowRange` (row offset overlap) for date_range/row_range/LIMIT pushdown. Called inside the future chain lambda with captured (not member) state.

**Filter**: `apply_filter_clause()` is a free function in `pipeline/lazy_read_helpers.hpp` — evaluates `ExpressionContext` via `ProcessingUnit`, applying WHERE pushdown bitset filtering. For dynamic-schema symbols, `expression_context_->dynamic_schema_` must be `true` so that `ProcessingUnit::get()` returns `EmptyResult` instead of throwing when a filter column is missing from a segment.

**Thread safety**: All state needed by the CPU lambda (row_filter, expression_context, filter_name) is captured by value/move — no shared mutable state across threads. Each segment is processed independently.

### ArrowOutputFrame

Container for `lib.read(output_format='pyarrow')` results. **Not used by the SQL/DuckDB path**.

```
ArrowOutputFrame
├── data_           (shared_ptr<vector<sparrow::record_batch>>)
├── data_consumed_  (bool, default false)
├── extract_record_batches() → vector<RecordBatchData>  (sets data_consumed_)
└── num_blocks() → size_t
```

Single-use enforcement via `data_consumed_` flag — `extract_record_batches()` raises error if already consumed.

## Segment-to-Arrow Conversion

### prepare_segment_for_arrow() (anonymous namespace in lazy_record_batch_iterator.cpp)

Converts a decoded `SegmentInMemory` for Arrow consumption. **This is the dominant cost** in the SQL pipeline.

| Column Type | Action | Cost |
|------------|--------|------|
| Non-string (DETACHABLE) | `make_column_blocks_detachable()` — **no-op** (early return) | Zero (lazy path decodes with DETACHABLE) |
| Non-string (sparse) | `unsparsify()` → `make_column_blocks_detachable()` memcpy | O(data_size); only with `sparsify_floats=True` |
| Non-string (fixed-width string) | `make_column_blocks_detachable()` memcpy | O(data_size); legacy `ASCII_FIXED64`/`UTF_FIXED64` only |
| Dynamic string (CATEGORICAL) | `encode_dictionary_with_shared_dict()` using `SharedStringDictionary` | O(rows) lookups + buffer copy |
| Dynamic string (LARGE/SMALL) | `ArrowStringHandler::convert_type()` | O(rows) full conversion |
| Fixed string (UTF_FIXED64) | `ArrowStringHandler::convert_type()` (handles UTF-32→UTF-8) | Rare/legacy |

### SharedStringDictionary

Built once per segment from the string pool, shared across all string columns in that segment:

```cpp
struct SharedStringDictionary {
    ankerl::unordered_dense::map<StringPool::offset_t, int32_t> offset_to_index;
    std::vector<int64_t> dict_offsets;   // Arrow cumulative byte offsets
    std::vector<char> dict_strings;       // Concatenated UTF-8 data
    int32_t unique_count = 0;
};
```

`build_shared_dictionary()` iterates CATEGORICAL column data collecting referenced pool offsets, then resolves each offset via `string_pool->get_const_view()`. O(R) where R = total rows across CATEGORICAL columns, plus O(U) lookups where U = unique strings.

`encode_dictionary_with_shared_dict()` does read-only hash map lookups per row (no insert), then copies the shared dictionary buffers into each column's extra buffers.

### make_column_blocks_detachable()

Ensures a column's `ChunkedBuffer` uses `AllocationType::DETACHABLE` (ExternalMemBlock) so `block.release()` can transfer ownership to Sparrow. **In the lazy iterator path, this is a no-op for numeric columns** because `batch_read_uncompressed()` is called with `AllocationType::DETACHABLE`, so columns are decoded directly into detachable blocks. The memcpy path is only hit for:
- **Sparse columns**: `unsparsify()` creates a `ChunkedBuffer::presized()` (PRESIZED allocation)
- **Fixed-width string columns**: `create_columns()` explicitly downgrades to PRESIZED

### segment_to_arrow_data() (arrow_utils.cpp)

Iterates columns, calls `arrow_arrays_from_column()` which calls `block.release()` on each block to transfer memory ownership. Produces `vector<sparrow::record_batch>` (one per block when columns span multiple ChunkedBuffer blocks).

### arrow_utils.hpp — Schema Padding & Column-Slice Merging

| Function / Struct | Purpose |
|---|---|
| `TargetField` | Describes target column: `name`, `arrow_format`, `is_dictionary`, `format_resolved`. Formats resolved eagerly at iterator construction. |
| `default_arrow_format_for_type(DataType)` | Maps ArcticDB DataType → Arrow format string (used during eager resolution) |
| `resolve_target_fields_from_batch(fields, schema)` | Safety net: captures Arrow formats from batch for any fields still unresolved after eager resolution |
| `pad_batch_to_schema(batch, target_fields)` | Pads/reorders batch to match target schema; null-fills missing columns. Fast path returns unchanged if batch already matches. |
| `horizontal_merge_arrow_batches(batch_a, batch_b)` | Zero-copy horizontal merge of column slices; deduplicates index columns by name |

#### Null Column Creation & Ownership

`create_null_column()` (anonymous namespace) creates null-filled `ArrowArray` + `ArrowSchema` pairs for missing columns in dynamic schema. Returns a `NullColumnOwner*` that owns all buffers.

**`NullColumnOwner`** struct owns: `validity_bitmap` (all zeros = all null), `data_buffer` (zeros), `name`, `format`, plus optional `DictValues` for dictionary-encoded null columns. For `large_string` ("U") null columns, a dictionary-encoded representation is used (int32 keys + minimal large_string dictionary) because the static `buffers[2]` array can't hold the 3 buffer pointers needed for Arrow's variable-length string layout.

**`PaddedBatchData`** struct owns the reordered child arrays/schemas and `std::vector<std::unique_ptr<NullColumnOwner>> null_column_owners` for RAII cleanup. The `unique_ptr` ensures that if `pad_batch_to_schema()` throws after creating some null columns, the destructor frees them automatically. `null_column_array_release()` does NOT delete the owner — the `unique_ptr` handles cleanup when `PaddedBatchData` is destroyed.

**Release callback nullification pattern**: Throughout merge and padding code, `ArrowArray`/`ArrowSchema` structs are copied then the source's `release` is set to `nullptr` to prevent double-free. This pattern appears 10+ times and is the core memory safety mechanism for Arrow C Data Interface ownership transfer.

### ArrowOutputStringFormat (arrow_output_options.hpp)

Enum controlling string column Arrow format: `CATEGORICAL` (dictionary-encoded, default), `LARGE_STRING`, `SMALL_STRING`. `ArrowOutputConfig` struct wraps this for per-column overrides.

## Data Flow

```
Storage (LMDB/S3)
    │
    ▼ (batch_read_uncompressed — one segment at a time, with prefetch)
SegmentInMemory (decoded, inline blocks)
    │
    ▼ (prepare_segment_for_arrow)
SegmentInMemory (detachable blocks, Arrow-ready string columns)
    │
    ▼ (segment_to_arrow_data)
vector<sparrow::record_batch>
    │
    ▼ (extract_arrow_structures)
RecordBatchData (ArrowArray + ArrowSchema)
    │
    ▼ (pybind11 → Python)
pa.RecordBatch._import_from_c(array, schema)
    │
    ▼ (ArcticRecordBatchReader.to_pyarrow_reader)
pa.RecordBatchReader
    │
    ▼ (conn.register)
DuckDB queries data via streaming scan
```

## Python Bindings

### python_bindings.cpp

Two C++ → Python entry points for lazy iterator creation:

1. `create_lazy_record_batch_iterator(stream_id, version_query, read_query, read_options, filter_clause=None, prefetch_size=2)` — creates `LazyRecordBatchIterator` for SQL/DuckDB path
2. `create_lazy_record_batch_iterator_with_metadata(...)` — same params, returns `(VersionedItem, norm_meta, user_meta, iterator)` tuple for `lib.read(output_format='pyarrow')` path

Both call `PythonVersionStore` methods in `version_store_api.cpp` which:
- Sort `slice_and_keys_` by `(row_range, col_range)` for column-slice merging
- Populate `overall_column_bitset_` for column pushdown
- Build `columns_to_decode` including filter clause input columns
- Cap effective prefetch at `kMaxLazyPrefetchSegments = 200`

`LazyRecordBatchIterator` bindings:
- `next()` — `py::call_guard<py::gil_scoped_release>()` (does Folly async I/O)
- `has_next()`, `num_batches()`, `current_index()`, `descriptor()`, `field_count()`

### python_bindings_common.cpp

`ArrowOutputFrame` bindings: `extract_record_batches()`, `num_blocks()`

## Memory Safety

| Concern | Mitigation |
|---------|-----------|
| Sparrow deallocation | `allocate_detachable_memory()` uses `std::allocator` matching Sparrow's `deallocate()` |
| Dangling release callbacks | `std::memset` zero-init in `RecordBatchData` constructor |
| Ownership transfer | `block.release()` moves data out; `make_column_blocks_detachable()` ensures blocks are external (no-op when already DETACHABLE) |
| Null column cleanup | `PaddedBatchData::null_column_owners` uses `unique_ptr<NullColumnOwner>` for RAII; exception-safe |
| Release callback nullification | Copy struct then set `source->release = nullptr` to prevent double-free (merge, padding) |
| GIL safety | `next()` releases GIL for storage I/O via Folly futures |
| Single consumption | `ArrowOutputFrame::data_consumed_` flag; `ArcticRecordBatchReader._exhausted` in Python |

## Performance

**IMPORTANT: All benchmarks below use release builds (`ARCTIC_CMAKE_PRESET=linux-release`). Debug builds are 100-400x slower for Arrow conversion due to unoptimized sparrow template instantiation and disabled inlining.**

### Lazy vs Eager Path Comparison (release build, LMDB)

**1M rows × 10 cols:**

| Read Method | Numeric | String | Mixed | Notes |
|---|---|---|---|---|
| `lib.read()` (pandas) | 11.2ms | 67.0ms | 28.9ms | Numpy arrays reference `ChunkedBuffer` directly |
| `lib.read(output_format='pyarrow')` | 11.7ms | 84.2ms | 48.0ms | Zero-copy via `block.release()` for numeric |
| `lib.read(output_format='polars')` | 11.7ms | 167ms | 82.5ms | Arrow + `pl.from_arrow()` overhead |
| `lib.sql("SELECT * FROM sym")` | 70.2ms | 127ms | 92.5ms | Arrow + DuckDB registration + query execution |

**100K rows × 10 cols:**

| Read Method | Numeric | String | Mixed |
|---|---|---|---|
| `lib.read()` (pandas) | 8.48ms | 37.8ms | 20.3ms |
| `lib.read(output_format='pyarrow')` | 8.55ms | 46.3ms | 24.8ms |
| `lib.read(output_format='polars')` | 9.36ms | 60.9ms | 30.8ms |
| `lib.sql("SELECT * FROM sym")` | 56.2ms | 87.5ms | 68.2ms |

**With read options (1M rows, numeric):**

| Read Method | Time |
|---|---|
| Full read (Arrow) | 11.8ms |
| Date range filter | 16.7ms |
| Column projection (3/10 cols) | 5.47ms |
| Date range + column projection | 8.43ms |
| Filter clause (Arrow) | 36.7ms |
| Filter clause (Pandas) | 36.4ms |

**Numeric data**: Arrow and Pandas are at near-parity (1.0-1.05x ratio). The Arrow conversion is zero-copy for numeric columns — `make_column_blocks_detachable()` is a no-op (both eager and lazy paths allocate DETACHABLE blocks), and `block.release()` transfers ownership without copying.

**String data**: Arrow is 1.2-2.5x slower than Pandas due to per-row string pool resolution into Arrow dictionary/string buffers in `prepare_segment_for_arrow()`. At 1M rows × 10 string cols: Arrow ~84ms vs Pandas ~67ms. Polars is ~2.5x slower than Pandas due to additional `pl.from_arrow()` rechunking overhead.

### C++ Microbenchmarks (`BM_segment_to_arrow_data`, release build)

| Configuration | Time | Throughput |
|---|---|---|
| 100K × 10 cols, 1 block | 0.24ms | 31.6 GB/s |
| 1M × 10 cols, 1 block | 2.21ms | 33.7 GB/s |
| 1M × 10 cols, 10 blocks | 0.23ms | 324 GB/s |
| 100K × 100 cols, 1 block | 2.18ms | 34.2 GB/s |

Same benchmarks in **debug build** are 375-414x slower (90-916ms). This is due to sparrow's heavily-templated Arrow type construction lacking inlining and having bounds checking enabled.

### Key Performance Notes

- **Pandas path** (`lib.read()`): numpy arrays reference decoded `ChunkedBuffer` memory directly (zero-copy)
- **Arrow lazy path** (`lib.sql()`, numeric columns): blocks decoded as DETACHABLE — `make_column_blocks_detachable()` is a no-op, `block.release()` transfers ownership without copying
- **Arrow lazy path** (string columns): per-row string pool resolution into Arrow dictionary/string buffers dominates cost
- **Arrow eager path** (`lib.read(pyarrow)` via `allocate_chunked_frame`): copies decoded segment data into a pre-allocated DETACHABLE frame via `copy_segments_to_frame`

For string-heavy data at 10M rows, `prepare_segment_for_arrow()` accounts for ~90% of `lib.sql()` wall time due to string pool resolution. Numeric-only data is substantially faster. See profiling scripts in `python/benchmarks/non_asv/duckdb/` for detailed measurements.

## Unified Lazy Read Path

Implemented across Phases 0-9 (see `docs/claude/plans/duckdb/unified-lazy-read-path.md` for full plan). `LazyRecordBatchIterator` is used by:
- `lib.sql()` / `lib.duckdb()` — DuckDB SQL queries
- `lib.read(output_format='pyarrow')` — direct Arrow output
- `lib.read(output_format='polars')` — Polars output via Arrow

### Shared Helpers

`pipeline/lazy_read_helpers.hpp/cpp`: extracted pure functions shared by the iterator:
- `apply_truncation()` → modifies segment in place
- `apply_filter_clause()` → returns false if all rows filtered
- `estimate_segment_bytes()` → rough uncompressed size for backpressure

### Dual-Cap Prefetch Backpressure

`LazyRecordBatchIterator` uses dual-cap backpressure to prevent OOM:
- Count cap: `prefetch_size` (default 2)
- Byte cap: `max_prefetch_bytes` (default 4GB)
- `fill_prefetch_buffer()` stops when EITHER cap is reached
- For typical segments (≤40MB), the count cap dominates
- For wide tables (400MB+ segments), the byte cap prevents OOM

### C++ Column-Slice Merging

`LazyRecordBatchIterator::next()` merges column slices for the same row group at the Arrow level, using Sparrow's zero-copy extraction chain: `record_batch::extract_struct_array()` → `arrow_proxy::children()` → `extract_array()`/`extract_schema()`. Uses `detail::array_access::get_arrow_proxy()` (Sparrow internal API).

### C++ Schema Padding

Schema padding (null arrays for missing columns in dynamic schema) runs in C++ within `LazyRecordBatchIterator::next()`, using the merged descriptor as the authoritative type source. `TargetField` formats are resolved eagerly at constructor time from descriptor + ReadOptions (string format, dictionary encoding). `resolve_target_fields_from_batch()` is kept as a safety net but should be a no-op on the normal path.

### descriptor() Method

`LazyRecordBatchIterator::descriptor()` returns the merged `StreamDescriptor`, used by Python `ArcticRecordBatchReader` to build the `pyarrow.Schema` via `_descriptor_to_arrow_schema()`.

## Related Documentation

- [PYTHON_BINDINGS.md](PYTHON_BINDINGS.md) — pybind11 binding details
- [../python/DUCKDB.md](../python/DUCKDB.md) — Python DuckDB integration
- [PIPELINE.md](PIPELINE.md) — Read pipeline that produces segments
