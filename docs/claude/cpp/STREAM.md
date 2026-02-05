# Stream Module

The stream module (`cpp/arcticdb/stream/`) handles data streaming, aggregation, and index management.

## Overview

This module provides:
- Stream aggregation for building segments
- Index management (row and timestamp indices)
- Incomplete write handling
- Row builder utilities

## Stream Aggregator

### Location

`cpp/arcticdb/stream/aggregator.hpp`

### Purpose

Aggregates incoming data into segments for storage.

### How It Works

```
Incoming Rows
      │
      ▼
┌──────────────────────────┐
│      Aggregator          │
│                          │
│  ┌─────────────────┐     │
│  │ Current Segment │     │ ◄── Accumulate rows
│  │ (building)      │     │
│  └────────┬────────┘     │
│           │              │
│   when full (100K rows)  │
│           │              │
│           ▼              │
│  ┌─────────────────┐     │
│  │ Finalize & emit │     │ ──► Write to storage
│  └─────────────────┘     │
│                          │
│  ┌─────────────────┐     │
│  │ Start new       │     │ ◄── Reset for next batch
│  │ segment         │     │
│  └─────────────────┘     │
└──────────────────────────┘
```

### Key Methods

`Aggregator` template class (parameterized by Index, Schema, SegmentingPolicy, DensityPolicy) provides:
- `start_row(idx)` - Start a row for writing (returns RowBuilder reference)
- `end_row()` - End the current row
- `commit()` - Commit current segment (triggers callback)
- `finalize()` - Finalize and return any remaining segment

## Index Types

### Location

`cpp/arcticdb/stream/index.hpp`

### Supported Indices

| Index Type | Description | Use Case |
|------------|-------------|----------|
| `TimeseriesIndex` | Timestamp-based | Time series data (default) |
| `RowCountIndex` | Row counter | Non-time-indexed data |
| `TableIndex` | String key | Key-value storage |
| `EmptyIndex` | No index | Special cases |

### TimeseriesIndex

`TimeseriesIndex` uses `NANOSECONDS_UTC64` for timestamp-based indexing. Default column name is "time". Provides `min_index_value()` and `max_index_value()` for segment time range tracking.

### RowCountIndex

`RowCountIndex` uses row numbers as index. No index column is stored (`field_count() == 0`).

### Index Resolution

```
Read with date_range
        │
        ▼
┌─────────────────────────┐
│ Check TABLE_INDEX       │
│ Get segment time ranges │
└───────────┬─────────────┘
            │
            ▼
┌─────────────────────────┐
│ Filter segments         │
│ Keep only overlapping   │
└───────────┬─────────────┘
            │
            ▼
Fetch only needed segments
```

## Row Builder

### Location

`cpp/arcticdb/stream/row_builder.hpp`

### Purpose

Builds rows for writing into segments.

### Usage

Create `RowBuilder(descriptor)`, set column values with `set_scalar<T>(col, val)` and `set_string(col, str)`, then add to aggregator with `add_row(index, builder)` and `clear()`.

## Incomplete Writes

### Location

`cpp/arcticdb/stream/incompletes.hpp`

### Purpose

Handles staged/incomplete appends that haven't been committed.

### Structure

```
APPEND_REF ("my_symbol")
    │
    └── Points to incomplete segment chain
            │
            └── Segment 1 (uncommitted)
                    │
                    └── Segment 2 (uncommitted)
```

### Lifecycle

```
1. Start append
   └── Create APPEND_REF pointing to first incomplete segment

2. Add more data
   └── Chain additional segments to APPEND_REF

3. Finalize append
   └── Create new VERSION with all data
   └── Delete APPEND_REF

4. On crash recovery
   └── APPEND_REF points to recoverable data
   └── Can discard or complete the append
```

## Segment Aggregator

### Location

`cpp/arcticdb/stream/segment_aggregator.hpp`

### Purpose

`SegmentAggregator` manages segment lifecycle with `start_segment(desc)`, `add_segment(segment)`, and `finalize()`.

## Protobuf Segment

### Location

`cpp/arcticdb/stream/protobuf_mappings.hpp`

### Purpose

Convert between in-memory segments and protobuf format using `to_proto(SegmentInMemory)` and `from_proto(arcticc::pb2::Segment)`.

## Key Files

| File | Purpose |
|------|---------|
| `aggregator.hpp` | Aggregator template |
| `index.hpp` | Index type definitions |
| `row_builder.hpp` | Row construction |
| `incompletes.hpp` | Incomplete append handling |
| `segment_aggregator.hpp` | Segment lifecycle |
| `protobuf_mappings.hpp` | Protobuf conversion |
| `stream_sink.hpp` | Output sink interface |

## Usage

Create an `Aggregator` with a `StreamDescriptor` and segment size (default 100,000 rows). Add rows via `RowBuilder`, call `add_row()` and `clear()`. When segment fills up, aggregator automatically emits it. Call `finalize()` to get remaining data. See `cpp/arcticdb/stream/aggregator.hpp` for details.

## Performance Considerations

### Segment Size

- Larger segments = fewer keys, better compression
- Smaller segments = faster random access
- Default 100K rows balances these trade-offs

### Index Efficiency

- TimeseriesIndex enables efficient date range queries
- Index metadata stored in TABLE_INDEX for quick pruning
- Sorted data enables index-based segment skipping

### Memory Management

- Aggregator flushes when segment is full
- Streaming approach limits memory usage
- Preallocated buffers reduce allocation overhead

## Related Documentation

- [PIPELINE.md](PIPELINE.md) - How streaming fits in write path
- [COLUMN_STORE.md](COLUMN_STORE.md) - Segment structure
- [VERSIONING.md](VERSIONING.md) - Version chain structure
