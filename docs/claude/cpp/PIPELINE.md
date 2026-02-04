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

```cpp
// Main entry point - returns a future with the index key
folly::Future<entity::AtomKey> write_frame(
    IndexPartialKey&& key,
    const std::shared_ptr<InputFrame>& frame,
    const SlicingPolicy& slicing,
    const std::shared_ptr<Store>& store,
    const std::shared_ptr<DeDupMap>& de_dup_map = std::make_shared<DeDupMap>(),
    bool allow_sparse = false
);

// Slicing and writing - returns futures for slice keys
folly::Future<std::vector<SliceAndKey>> slice_and_write(
    const std::shared_ptr<InputFrame>& frame,
    const SlicingPolicy& slicing,
    IndexPartialKey&& partial_key,
    const std::shared_ptr<stream::StreamSink>& sink,
    const std::shared_ptr<DeDupMap>& de_dup_map = std::make_shared<DeDupMap>(),
    bool allow_sparse = false
);
```

### WriteOptions (used at higher level)

```cpp
struct WriteOptions {
    bool prune_previous_version = false;  // Note: singular
    bool validate_index = true;
    // ... configured via LibraryOptions
};
```

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

```cpp
// Main entry point
ReadResult read_frame(
    const StreamId& stream_id,
    const VersionQuery& version_query,
    const ReadQuery& read_query
);

// Fetch and decode data
void fetch_data(
    const std::vector<AtomKey>& keys,
    const Store& store,
    DecodeCallback callback
);

// Decode segment into frame
void decode_into_frame(
    Segment&& segment,
    PipelineContext& context,
    SegmentInMemory& output
);
```

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

SlicingPolicy is a variant type that supports different slicing strategies:

```cpp
// In slicing.hpp
using SlicingPolicy = std::variant<NoSlicing, FixedSlicer, HashedSlicer>;

// NoSlicing - no slicing, single segment
struct NoSlicing {};

// FixedSlicer - fixed row/column counts per segment
struct FixedSlicer {
    size_t col_per_slice_;  // Columns per segment
    size_t row_per_slice_;  // Rows per segment (default: 100,000)
};

// HashedSlicer - hash-based slicing for partitioning
struct HashedSlicer { ... };

// Get slicing policy from write options
SlicingPolicy get_slicing_policy(const WriteOptions& options, const InputFrame& frame);
```

## Pipeline Context

### Location

`cpp/arcticdb/pipeline/pipeline_context.hpp`

### Purpose

Holds state shared across pipeline operations.

### Structure

```cpp
struct PipelineContext {
    StreamDescriptor descriptor_;     // Schema information
    std::vector<SliceInfo> slices_;   // Slice metadata
    std::optional<FilterClause> filter_;
    std::vector<std::string> columns_; // Requested columns
    // ...
};
```

## Query Types

### Location

`cpp/arcticdb/pipeline/query.hpp`

### VersionQuery

```cpp
using VersionQuery = std::variant<
    std::monostate,           // Latest version
    SpecificVersionQuery,     // By version ID (as_of=5)
    TimestampVersionQuery,    // By timestamp (as_of="2024-01-01")
    SnapshotVersionQuery      // By snapshot name
>;
```

### ReadQuery

```cpp
// In read_query.hpp
struct ReadQuery {
    // std::nullopt -> all columns
    // empty vector -> only the index
    mutable std::optional<std::vector<std::string>> columns;
    std::optional<SignedRowRange> row_range;
    FilterRange row_filter;  // Date/row range filter (no filter by default)
    std::vector<std::shared_ptr<Clause>> clauses_;  // Processing clauses
    bool needs_post_processing{true};
};
```

## Append and Update

### Append

`cpp/arcticdb/pipeline/write_frame.cpp`

Appends rows to the latest version. The main append logic is in the version store layer (`local_versioned_engine.cpp`), which uses the pipeline functions internally.

Append creates a new version that references both old and new data segments.

### Update

Updates rows within an index range. Like append, the main logic is in the version store layer.

## Parallel Processing

### Async Fetch

```cpp
// Fetch multiple segments in parallel
auto futures = schedule_fetch(keys, store);

for (auto& future : futures) {
    auto segment = future.get();
    process(segment);
}
```

### Parallel Encoding

```cpp
// Encode segments in parallel during write
parallel_for(slices, [&](const Slice& slice) {
    auto segment = encode(slice);
    store.write(key, segment);
});
```

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

## Usage Examples

### Writing Data

```cpp
// In version_store_api.cpp
auto frame = std::make_shared<InputFrame>();
// ... populate frame from Python ...

SlicingPolicy slicing;
// Slicing configured via LibraryOptions

auto result = write_frame(
    IndexPartialKey{"my_symbol", KeyType::TABLE_INDEX},
    frame,
    slicing,
    store
);
```

### Reading Data

```cpp
// Read specific columns
ReadQuery query;
query.columns = {"price", "volume"};

auto result = read_frame(
    "my_symbol",
    VersionQuery{},  // Latest version
    query
);
```

### Filtered Read

```cpp
// Read with row filter
ReadQuery query;
query.clauses = build_filter_clause("price > 100");

auto result = read_frame("my_symbol", VersionQuery{}, query);
```

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
