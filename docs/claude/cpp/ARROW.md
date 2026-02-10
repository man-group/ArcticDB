# Arrow Output & Lazy Streaming

Arrow C Data Interface integration for streaming ArcticDB data to DuckDB and PyArrow consumers.

## Location

```
cpp/arcticdb/arrow/
├── arrow_output_frame.hpp   # RecordBatchData, LazyRecordBatchIterator, ArrowOutputFrame
├── arrow_output_frame.cpp   # Implementation: lazy iterator, prepare_segment_for_arrow, SharedStringDictionary
├── arrow_handlers.hpp/cpp   # Per-type Arrow conversion (string, numeric, timestamp)
└── arrow_utils.hpp/cpp      # segment_to_arrow_data(), arrow_arrays_from_column()
```

## Classes

### RecordBatchData

Single Arrow record batch: `ArrowArray` + `ArrowSchema` pair (Arrow C Data Interface).

```
RecordBatchData
├── array_   (ArrowArray)  — zero-initialized with std::memset
├── schema_  (ArrowSchema) — zero-initialized with std::memset
├── array() → ArrowArray&
└── schema() → ArrowSchema&
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
└── field_count() → size_t
```

**Prefetch + Parallel Conversion**: `fill_prefetch_buffer()` maintains up to `prefetch_size_` (default 2) in-flight `folly::Future<vector<RecordBatchData>>` via `read_decode_and_prepare_segment()`. Each future chains I/O (`batch_read_uncompressed`) with CPU-intensive work (truncation, filter, Arrow conversion) via `.via(&async::cpu_executor())`. This means `prepare_segment_for_arrow()` runs on the **CPU thread pool in parallel** across segments — critical for wide tables where Arrow conversion takes seconds per segment.

**Truncation**: `apply_truncation()` is `static` — handles `IndexRange` (timestamp binary search) and `RowRange` (row offset overlap) for date_range/row_range/LIMIT pushdown. Called inside the future chain lambda with captured (not member) state.

**Filter**: `apply_filter_clause()` is `static` — evaluates `ExpressionContext` via `ProcessingUnit`, applying WHERE pushdown bitset filtering. For dynamic-schema symbols, `expression_context_->dynamic_schema_` must be `true` so that `ProcessingUnit::get()` returns `EmptyResult` instead of throwing when a filter column is missing from a segment.

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

### prepare_segment_for_arrow() (anonymous namespace in arrow_output_frame.cpp)

Converts a decoded `SegmentInMemory` for Arrow consumption. **This is the dominant cost** in the SQL pipeline.

| Column Type | Action | Cost |
|------------|--------|------|
| Non-string | `make_column_blocks_detachable()` — `allocate_detachable_memory()` + `memcpy` | O(data_size) per block |
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

`build_shared_dictionary()` walks the pool buffer sequentially using `[uint32_t size][char data]` entry layout (min 8 bytes per entry). O(U) where U = unique strings in pool.

`encode_dictionary_with_shared_dict()` does read-only hash map lookups per row (no insert), then copies the shared dictionary buffers into each column's extra buffers.

### make_column_blocks_detachable()

For non-string columns, allocates new memory via `std::allocator` (`allocate_detachable_memory()`) and copies block data. Required because:
- Sparrow (Arrow library) calls `std::allocator::deallocate()` to free memory
- ArcticDB's decoded segments use `ChunkedBuffer` with different allocation
- `block.release()` in `arrow_arrays_from_column()` transfers ownership to Sparrow

### segment_to_arrow_data() (arrow_utils.cpp)

Iterates columns, calls `arrow_arrays_from_column()` which calls `block.release()` on each block to transfer memory ownership. Produces `vector<sparrow::record_batch>` (one per block when columns span multiple ChunkedBuffer blocks).

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

`read_as_lazy_record_batch_iterator(symbol, ...)` — creates `LazyRecordBatchIterator`:
- Parameters: `columns`, `date_range`, `row_range`, `as_of`, `filter_clause`, `filter_root_node_name`, `expression_context`, `prefetch_size`
- Calls `PythonVersionStore::create_lazy_record_batch_iterator()` in `version_store_api.cpp`
- When `dynamic_schema=True` is passed via `read_options`, `create_lazy_record_batch_iterator()` sets `expression_context->dynamic_schema_ = opt_false(read_options.dynamic_schema())` so FilterClause handles missing columns gracefully

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
| Ownership transfer | `block.release()` moves data out; `make_column_blocks_detachable()` ensures blocks are external |
| GIL safety | `next()` releases GIL for storage I/O via Folly futures |
| Single consumption | `ArrowOutputFrame::data_consumed_` flag; `ArcticRecordBatchReader._exhausted` in Python |

## Performance

The Arrow conversion path has an inherent overhead vs the pandas path:

- **Pandas path** (`lib.read()`): numpy arrays reference decoded `ChunkedBuffer` memory directly (zero-copy)
- **Arrow path** (`lib.sql()`): every block requires `allocate_detachable_memory()` + `memcpy` for ownership transfer to Sparrow

At 10M rows (100 segments, 100K rows each), `prepare_segment_for_arrow()` accounts for ~90% of `lib.sql()` wall time. See profiling scripts in `python/benchmarks/non_asv/duckdb/` for detailed measurements.

## Related Documentation

- [PYTHON_BINDINGS.md](PYTHON_BINDINGS.md) — pybind11 binding details
- [../python/DUCKDB.md](../python/DUCKDB.md) — Python DuckDB integration
- [PIPELINE.md](PIPELINE.md) — Read pipeline that produces segments
