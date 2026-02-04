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

```cpp
// Template class parameterized by Index, Schema, SegmentingPolicy, DensityPolicy
template<typename Index, typename Schema, typename SegmentingPolicy, typename DensityPolicy>
class Aggregator {
public:
    using RowBuilderType = RowBuilder<IndexType, Schema, SelfType>;
    using Callback = folly::Function<void(SegmentInMemory&&)>;

    // Start a row for writing
    RowBuilderType& start_row(const IndexType& idx);

    // End the current row
    void end_row();

    // Commit current segment (calls callback)
    void commit();

    // Finalize and return any remaining segment
    void finalize();
};
```

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

```cpp
class TimeseriesIndex : public BaseIndex<TimeseriesIndex> {
public:
    static constexpr const char* DefaultName = "time";
    using TypeDescTag = TypeDescriptorTag<DataTypeTag<DataType::NANOSECONDS_UTC64>, DimensionTag<Dimension::Dim0>>;

    static constexpr IndexDescriptorImpl::Type type() {
        return IndexDescriptorImpl::Type::TIMESTAMP;
    }

    // Used for segment time range tracking
    static constexpr timestamp min_index_value();
    static constexpr timestamp max_index_value();
};
```

### RowCountIndex

```cpp
class RowCountIndex : public BaseIndex<RowCountIndex> {
public:
    static constexpr IndexDescriptorImpl::Type type() {
        return IndexDescriptorImpl::Type::ROWCOUNT;
    }

    static constexpr size_t field_count() { return 0; }  // No index column stored
};
```

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

```cpp
RowBuilder builder(descriptor);

// Set column values
builder.set_scalar<int64_t>(0, 42);
builder.set_scalar<double>(1, 3.14);
builder.set_string(2, "hello");

// Add to aggregator
aggregator.add_row(index, builder);
builder.clear();
```

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

Higher-level aggregator that manages segment lifecycle.

```cpp
class SegmentAggregator {
public:
    // Start new segment
    void start_segment(const StreamDescriptor& desc);

    // Add data
    void add_segment(SegmentInMemory&& segment);

    // Finalize and get output
    SegmentInMemory finalize();
};
```

## Protobuf Segment

### Location

`cpp/arcticdb/stream/protobuf_mappings.hpp`

### Purpose

Convert between in-memory segments and protobuf format for storage.

```cpp
// To protobuf
arcticc::pb2::Segment to_proto(const SegmentInMemory& segment);

// From protobuf
SegmentInMemory from_proto(const arcticc::pb2::Segment& proto);
```

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

## Usage Examples

### Building Segments

```cpp
#include <arcticdb/stream/aggregator.hpp>

// Create descriptor
StreamDescriptor desc;
desc.add_field(scalar_field(DataType::INT64, "id"));
desc.add_field(scalar_field(DataType::FLOAT64, "value"));

// Create aggregator
Aggregator<RowCountIndex> agg(desc, 100'000);

// Add rows
RowBuilder builder(desc);
for (int i = 0; i < 250'000; ++i) {
    builder.set_scalar<int64_t>(0, i);
    builder.set_scalar<double>(1, i * 1.5);
    agg.add_row(RowCountIndex{i}, builder);
    builder.clear();
}

// Get completed segments
auto segments = agg.finalize();
// segments.size() == 3 (two full + one partial)
```

### Time Series Index

```cpp
// Create timeseries aggregator
Aggregator<TimeseriesIndex> ts_agg(desc, 100'000);

// Add timestamped rows
for (const auto& row : data) {
    builder.set_scalar<timestamp>(0, row.time);
    builder.set_scalar<double>(1, row.value);
    ts_agg.add_row(TimeseriesIndex{row.time, row.time + 1}, builder);
    builder.clear();
}
```

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
