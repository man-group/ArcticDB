# Pipeline Module

The pipeline module (`cpp/arcticdb/pipeline/`) handles the read and write data pipelines for ArcticDB.

## Overview

This module is responsible for:
- Writing DataFrames to storage (serialization)
- Reading DataFrames from storage (deserialization)
- Slicing data into segments
- Pipeline context management

## Write Pipeline

### Location

`cpp/arcticdb/pipeline/write_frame.cpp`

### Flow

```
Python DataFrame
       │
       ▼
┌─────────────────────────┐
│   Normalization         │  ← Convert to internal format
│   (Python layer)        │     (NormalizationMetadata)
└───────────┬─────────────┘
            │
            ▼
┌─────────────────────────┐
│   write_frame()         │  ← Entry point in C++
└───────────┬─────────────┘
            │
            ▼
┌─────────────────────────┐
│   slice_and_write()     │  ← Split into row/column slices
│                         │     Default: 100K rows, all columns
└───────────┬─────────────┘
            │
            ▼
┌─────────────────────────┐
│   Parallel encoding     │  ← Compress each slice
│   (codec layer)         │     LZ4/ZSTD per segment
└───────────┬─────────────┘
            │
            ▼
┌─────────────────────────┐
│   Storage write         │  1. Write TABLE_DATA keys
│                         │  2. Write TABLE_INDEX key
│                         │  3. Write VERSION key
│                         │  4. Update VERSION_REF
└─────────────────────────┘
```

### Key Functions

In `cpp/arcticdb/pipeline/write_frame.hpp`:
- `write_frame()` - Main entry point, returns `folly::Future<AtomKey>` for the index key
- `slice_and_write()` - Slices and writes, returns futures for slice keys

`WriteOptions` struct configures write behavior including `prune_previous_version` and `validate_index`.

## Read Pipeline

### Location

`cpp/arcticdb/pipeline/read_frame.cpp`

### Flow

```
Read Request
       │
       ▼
┌─────────────────────────┐
│   Version Resolution    │  ← Find VERSION key
│   (version_map)         │     Via cache or storage
└───────────┬─────────────┘
            │
            ▼
┌─────────────────────────┐
│   read_frame()          │  ← Entry point
└───────────┬─────────────┘
            │
            ▼
┌─────────────────────────┐
│   Index lookup          │  ← Read TABLE_INDEX
│                         │     Get list of TABLE_DATA keys
└───────────┬─────────────┘
            │
            ▼
┌─────────────────────────┐
│   Segment filtering     │  ← Determine required segments
│                         │     Based on row range, columns
└───────────┬─────────────┘
            │
            ▼
┌─────────────────────────┐
│   Parallel fetch        │  ← Retrieve TABLE_DATA keys
│   (async I/O)           │     Decompress in parallel
└───────────┬─────────────┘
            │
            ▼
┌─────────────────────────┐
│   Frame reconstruction  │  ← Assemble segments
│                         │     Build output DataFrame
└─────────────────────────┘
```

### Key Functions

In `cpp/arcticdb/pipeline/read_frame.hpp`:
- `read_frame()` - Main entry point taking stream_id, version_query, and read_query
- `fetch_data()` - Fetch and decode data from keys
- `decode_into_frame()` - Decode segment into SegmentInMemory

## Lazy Read Path (Arrow/SQL Output)

When the output format is Arrow or Polars (not Pandas), or when the read is for a SQL query, the read pipeline uses `LazyRecordBatchIterator` instead of the eager `read_frame()` path.

### Location

- `cpp/arcticdb/arrow/lazy_record_batch_iterator.hpp/cpp` — `LazyRecordBatchIterator`
- `cpp/arcticdb/pipeline/lazy_read_helpers.hpp/cpp` — shared helper functions
- `cpp/arcticdb/version/version_store_api.cpp` — `create_lazy_record_batch_iterator()`, `create_lazy_record_batch_iterator_with_metadata()`

### Flow

```
Read Request (format=ARROW/POLARS, or SQL query)
       │
       ▼
┌─────────────────────────┐
│   Version Resolution    │  ← Same as eager path
│   (version_map)         │
└───────────┬─────────────┘
            │
            ▼
┌─────────────────────────┐
│   Index lookup          │  ← Get SliceAndKey list
│   + Segment filtering   │     (date_range, columns)
└───────────┬─────────────┘
            │
            ▼
┌─────────────────────────┐
│ LazyRecordBatchIterator │  ← Prefetch buffer with dual-cap
│   (on-demand decode)    │     backpressure (count + bytes)
│                         │     Max: kMaxLazyPrefetchSegments=200
└───────────┬─────────────┘
            │ .next()
            ▼
┌─────────────────────────┐
│ Per-segment future:     │  ← Runs on CPU thread pool
│   batch_read_uncompr()  │     via folly::Future chain
│   apply_truncation()    │     (.via(&cpu_executor()))
│   apply_filter_clause() │
│   prepare_for_arrow()   │
│   segment_to_arrow()    │
└───────────┬─────────────┘
            │
            ▼ (in next())
┌─────────────────────────┐
│   column-slice merge    │  ← Merges slices with same
│   schema padding        │     row_range in next()
└───────────┬─────────────┘
            │
            ▼
   RecordBatchData (Arrow C structs)
```

### Key Differences from Eager Path

| Aspect | Eager (`read_frame()`) | Lazy (`LazyRecordBatchIterator`) |
|--------|----------------------|--------------------------------|
| Output | Single `SegmentInMemory` frame | Stream of `RecordBatchData` |
| Memory | O(symbol_size) during decode | O(prefetch_size × segment_size) |
| Parallelism | All segments fetched in parallel | Prefetch window with backpressure |
| Used by | `lib.read(format='pandas')` | `lib.read(format='pyarrow'/'polars')`, `lib.sql()` |
| Fallback | — | Falls back to eager for complex clauses (aggregation, resample), not simple FilterClause |

### Shared Helpers (`pipeline/lazy_read_helpers.hpp/cpp`)

| Function | Purpose |
|---|---|
| `apply_truncation(segment, slice_row_range, row_filter)` | Row-level truncation for date_range (timestamp binary search) and row_range/LIMIT (row offset overlap). Modifies segment in place. |
| `apply_filter_clause(segment, expression_context, filter_root_node_name)` | Evaluates FilterClause expression via ProcessingUnit. Returns false if all rows filtered. For dynamic schema, `expression_context->dynamic_schema_` must be true. |
| `estimate_segment_bytes(sk, descriptor)` | Rough uncompressed size estimate (rows × cols × 8 bytes) for dual-cap backpressure. |

### Iterator Construction (`version_store_api.cpp`)

`create_lazy_record_batch_iterator()` and `create_lazy_record_batch_iterator_with_metadata()`:

1. **Slice re-sorting**: `slice_and_keys_` sorted by `(row_range.first, col_range.first)` — makes column slices for each row group consecutive, enabling incremental merging in `next()`
2. **Column pushdown**: `get_column_bitset_in_context()` populates `overall_column_bitset_` from `ReadQuery.columns`, then builds `columns_to_decode` set. Filter clause input columns merged into this set even if not in user's column selection.
3. **Prefetch sizing**: `effective_prefetch = min(max(prefetch_size, total_segments), kMaxLazyPrefetchSegments)` where `kMaxLazyPrefetchSegments = 200`. Prefetches all segments when count is small (hides S3 latency); caps at 200 for large symbols.

### Python Bindings

`python_bindings.cpp`:
- `create_lazy_record_batch_iterator(stream_id, version_query, read_query, read_options, filter_clause, prefetch_size)` — for SQL/DuckDB path
- `create_lazy_record_batch_iterator_with_metadata(...)` — returns `(VersionedItem, norm, user_meta, iterator)` tuple for `lib.read(output_format='pyarrow')` path

See [ARROW.md](ARROW.md) for details on the Arrow conversion pipeline.

## Slicing

### Location

`cpp/arcticdb/pipeline/slicing.cpp`

### Purpose

Splits large DataFrames into manageable segments for storage.

### Row Slicing

```
Original DataFrame (500K rows)
┌─────────────────────────────────┐
│  Rows 0 - 499,999               │
└─────────────────────────────────┘
            │
            ▼ slice by rows (100K default)
┌─────────┐ ┌─────────┐ ┌─────────┐ ┌─────────┐ ┌─────────┐
│ 0-100K  │ │100K-200K│ │200K-300K│ │300K-400K│ │400K-500K│
└─────────┘ └─────────┘ └─────────┘ └─────────┘ └─────────┘
```

### Column Slicing

```
Original DataFrame (100 columns)
┌──────────────────────────────────────────┐
│  Columns A, B, C, ... , CV               │
└──────────────────────────────────────────┘
            │
            ▼ slice by columns (if column_group_size set)
┌──────────────────┐ ┌──────────────────┐
│  Columns A-AZ    │ │  Columns BA-CV   │
└──────────────────┘ └──────────────────┘
```

### Slicing Policy

`SlicingPolicy` (in `cpp/arcticdb/pipeline/slicing.hpp`) is a `std::variant<NoSlicing, FixedSlicer, HashedSlicer>`:
- `NoSlicing` - Single segment, no slicing
- `FixedSlicer` - Fixed row/column counts (default: 100,000 rows)
- `HashedSlicer` - Hash-based slicing for partitioning

Use `get_slicing_policy(WriteOptions, InputFrame)` to create the appropriate policy.

## Pipeline Context

### Location

`cpp/arcticdb/pipeline/pipeline_context.hpp`

### Purpose

Holds state shared across pipeline operations.

### Structure

`PipelineContext` holds: `descriptor_` (schema), `slices_` (slice metadata), optional `filter_`, and requested `columns_`.

## Query Types

### Location

`cpp/arcticdb/pipeline/query.hpp`

### VersionQuery

`VersionQuery` (in `cpp/arcticdb/pipeline/query.hpp`) is a `std::variant` of:
- `std::monostate` - Latest version
- `SpecificVersionQuery` - By version ID (as_of=5)
- `TimestampVersionQuery` - By timestamp
- `SnapshotVersionQuery` - By snapshot name

### ReadQuery

`ReadQuery` (in `cpp/arcticdb/pipeline/read_query.hpp`) contains:
- `columns` - `std::nullopt` for all columns, empty vector for index only
- `row_range` - Optional row range filter
- `row_filter` - Date/row range filter
- `clauses_` - Processing clauses (filter, aggregate, etc.)

## Append and Update

### Append

`cpp/arcticdb/pipeline/write_frame.cpp`

Appends rows to the latest version. The main append logic is in the version store layer (`local_versioned_engine.cpp`), which uses the pipeline functions internally.

Append creates a new version that references both old and new data segments.

### Update

Updates rows within an index range. Like append, the main logic is in the version store layer.

## Parallel Processing

Multiple segments are fetched and decoded in parallel using Folly futures. Encoding during writes is also parallelized across slices.

## Key Files

| File | Purpose |
|------|---------|
| `write_frame.cpp` | Write pipeline implementation |
| `write_frame.hpp` | Write interface |
| `read_frame.cpp` | Read pipeline implementation |
| `read_frame.hpp` | Read interface |
| `slicing.cpp` | Data slicing logic |
| `slicing.hpp` | Slicing interface |
| `pipeline_context.hpp` | Pipeline state |
| `query.hpp` | Query types |
| `frame_slice.hpp` | Slice data structures |
| `input_frame.hpp` | Input data format |

## Usage

Write using `write_frame(IndexPartialKey, frame, slicing, store)`. Read using `read_frame(stream_id, VersionQuery, ReadQuery)`. Configure column selection via `ReadQuery.columns` and filtering via `ReadQuery.clauses`. See `cpp/arcticdb/version/version_store_api.cpp` for integration examples.

## Performance Considerations

### Segment Size

- Larger segments = better compression, more memory during read
- Smaller segments = faster random access, more overhead
- Default 100K rows is a good balance

### Column Selection

- Always specify needed columns to avoid reading unnecessary data
- Column pruning happens at segment fetch time

### Parallel I/O

- Multiple segments fetched concurrently
- Decompression parallelized across threads
- Network latency hidden by pipelining

## Related Documentation

- [CODEC.md](CODEC.md) - Compression details
- [COLUMN_STORE.md](COLUMN_STORE.md) - In-memory format
- [PROCESSING.md](PROCESSING.md) - Query execution
- [VERSIONING.md](VERSIONING.md) - Version chain structure
