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
│                         │     + column stats pruning
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

## Column Stats Filtering

### Location

`cpp/arcticdb/pipeline/column_stats_filter.hpp`, `column_stats_filter.cpp`, `column_stats_dispatch.hpp`, `column_stats_dispatch.cpp`

### Purpose

Column stats store per-segment min/max values for selected columns. At read time, if a `FilterClause` is present, the column stats are evaluated against the filter expression to prune segments that cannot contain matching rows. This avoids fetching data segments from storage unnecessarily.

### Gating

Controlled by the `ColumnStats.UseForQueries` config flag (checked in `is_column_stats_enabled()`). The function `should_try_column_stats_read()` returns true only when the flag is on and the query's first non-range clause is a `FilterClause`.

### Data Model

Column stats are stored as a separate `COLUMN_STATS` key derived from the index key (see `index_key_to_column_stats_key()` in `version_core.cpp`). The stats segment has `start_index` and `end_index` columns identifying each row-slice, plus columns like `v1.0_MIN(col)` and `v1.0_MAX(col)` for each tracked column.

Key types:

| Type | Location | Purpose |
|------|----------|---------|
| `ColumnStatsData` | `column_stats_filter.hpp` | Parsed stats segment, indexed by `(start_index, end_index)` |
| `ColumnStatsRow` | `column_stats_filter.hpp` | Stats for a single row-slice: start/end index + per-column min/max |
| `ColumnStatsValues` | `column_stats_filter.hpp` | Min/max `Value` pair for one column in one row-slice |
| `ColumnStatElement` | `column_stats.hpp` | `MIN` or `MAX` — the individual stat within a `MINMAX` stat type |

`StatsVariantData` is a `std::variant` over `std::vector<StatsComparison>`, `std::shared_ptr<Value>`, `std::vector<ColumnStatsValues>`, and `std::shared_ptr<ValueSet>`. The `ValueSet` alternative is used by `isin`/`isnotin` expressions.

### Evaluation

The filter expression AST is walked by `compute_stats()` / `evaluate_ast_node_against_stats()` in `column_stats_filter.cpp`. Leaf `ColumnName` nodes produce `ColumnStatsValues` vectors; leaf `ValueName` nodes produce `Value` pointers; leaf `ValueSetName` nodes produce `ValueSet` pointers. Binary comparison operators (in `column_stats_dispatch.hpp`) apply three-valued logic (`StatsComparison`: `ALL_MATCH`, `NONE_MATCH`, `UNKNOWN`) using `ValueRange<T>` overloads on the existing comparison operator structs. Boolean operators (`AND`, `OR`, `XOR`, `NOT`) compose these results. Membership operators (`ISIN`, `ISNOTIN`) are handled by `visit_binary_membership_stats()`, which delegates to `stats_membership_comparator()` for each row-slice.

If all filter clauses AND together to `NONE_MATCH` for a row-slice, that slice is excluded from the read via `create_column_stats_filter()`, which returns a `FilterQuery` lambda passed to `filter_index()`.

### Integration with Read Path

In `version_core.cpp`, `fetch_index_and_column_stats()` issues parallel async reads for the index key and the column stats key. The result is bundled into `IndexInformation` (index segment + optional stats segment). `read_indexed_keys_to_pipeline()` then calls `create_column_stats_filter()` if stats are present, adding the filter to the index query list before `filter_index()`.

When multiple filter clauses exist, their `ExpressionContext` objects are combined with `and_filter_expression_contexts()` (in `query_planner.cpp`).

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
| `column_stats.hpp` | Column stats creation and segment column name parsing |
| `column_stats_filter.hpp` | Column stats read-time filter construction |
| `column_stats_dispatch.hpp` | Three-valued logic evaluation of expressions against stats |

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
