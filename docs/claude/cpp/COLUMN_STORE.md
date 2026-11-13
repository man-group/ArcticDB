# Column Store Module

The column store module (`cpp/arcticdb/column_store/`) manages the in-memory columnar representation of data.

## Overview

This module provides:
- In-memory segment representation (`SegmentInMemory`)
- Column abstraction for different data types
- String pooling for efficient string storage
- Memory management utilities

## SegmentInMemory

### Location

`cpp/arcticdb/column_store/memory_segment.hpp`

### Purpose

`SegmentInMemory` is the primary in-memory representation of data before it's encoded to storage or after it's decoded from storage.

### Structure

```
SegmentInMemory
├── descriptor_       ← StreamDescriptor with field info
├── columns_[]        ← Vector of Column objects
│   ├── Column 0 (index column)
│   ├── Column 1
│   ├── Column 2
│   └── ...
├── string_pool_      ← Shared strings storage
├── row_count_        ← Number of rows
└── metadata_         ← Optional user metadata
```

### Key Methods

`SegmentInMemory` class provides:
- `end_row()` - Finalize current row
- `row_count()` - Number of rows
- `column(idx)` / `num_columns()` - Column access
- `string_pool()` - Access shared string storage
- `begin()` / `end()` - Iterate over columns

## Column

### Location

`cpp/arcticdb/column_store/column.hpp`

### Purpose

Represents a single column of data with type information and efficient storage.

### Structure

```
Column
├── type_             ← TypeDescriptor (data type + dimension)
├── data_             ← ChunkedBuffer containing values
├── shapes_           ← Shape data (for multi-dimensional)
├── sparse_map_       ← Bitmap for sparse columns
└── last_logical_row_ ← Row count
```

### Key Methods

`Column` class provides:
- `type()` - Get `TypeDescriptor` (access data_type via `type().data_type()`)
- `set_scalar<T>(row, val)` / `scalar_at<T>(row)` - Type-safe value access
- `buffer()` - Access underlying `ChunkedBuffer`
- `data()` - Get `ColumnData` wrapper for iteration
- `is_sparse()` / `sparse_map()` - Sparse column support
- `change_type(target)` - Type modification

## String Pool

### Location

`cpp/arcticdb/column_store/string_pool.hpp`

### Purpose

Efficiently stores variable-length strings by deduplication and offset-based referencing.

### Structure

```
StringPool
├── data_        ← Concatenated string data
├── offsets_     ← Start offset for each string
└── shapes_      ← Length of each string

Example:
  data_:    "helloworldfoo"
  offsets_: [0, 5, 10]
  shapes_:  [5, 5, 3]

  String 0: "hello" (offset 0, length 5)
  String 1: "world" (offset 5, length 5)
  String 2: "foo"   (offset 10, length 3)
```

### Key Methods

`StringPool` class provides:
- `get(string_view, deduplicate=true)` - Add string and get offset (note: method is named `get()`, not `add()`)
- `get_view(offset)` / `get_const_view(offset)` - Retrieve string by offset
- `clear()` / `clone()` - Memory management

### String Column Storage

String columns store offsets into the string pool rather than the strings themselves. Call `segment.string_pool().get("hello")` to get an `OffsetString` containing the offset.

## ChunkedBuffer

### Location

`cpp/arcticdb/column_store/chunked_buffer.hpp`

### Purpose

Memory-efficient buffer that grows in chunks rather than requiring contiguous reallocation.

### Structure

```
ChunkedBuffer
├── blocks_[]        ← Vector of memory blocks
│   ├── Block 0 (64KB)
│   ├── Block 1 (64KB)
│   └── ...
├── bytes_           ← Total bytes written
└── block_size_      ← Size of each block
```

### Benefits

- No expensive reallocation when growing
- Better memory locality for large data
- Efficient append operations

## Memory Management

### Buffer Lifecycle

```
Allocation:
1. SegmentInMemory created
2. Columns allocated with initial capacity
3. Data pushed to columns (may allocate more chunks)

Deallocation:
1. SegmentInMemory destructor called
2. Columns release ChunkedBuffer blocks
3. StringPool releases data buffer
```

### Memory Pools

ArcticDB uses memory pools for frequently allocated objects:

```cpp
// Recycle small allocations
#include <arcticdb/util/memory_tracing.hpp>

// Track memory usage
ARCTICDB_TRACE_ALLOC(size);
ARCTICDB_TRACE_FREE(size);
```

## Key Files

| File | Purpose |
|------|---------|
| `memory_segment.hpp` | SegmentInMemory class |
| `memory_segment.cpp` | Implementation |
| `column.hpp` | Column class |
| `column.cpp` | Column implementation |
| `string_pool.hpp` | StringPool class |
| `chunked_buffer.hpp` | ChunkedBuffer class |
| `column_data.hpp` | Column data accessors |

## Usage

Create segments via `SegmentInMemory(StreamDescriptor)`. Add rows using `set_scalar<T>(col, val)` and `end_row()`. Access strings via `set_string(col, str)` and `string_at(col, row)`. See `cpp/arcticdb/column_store/memory_segment.hpp` for full API.

## Performance Considerations

### Column Access Patterns

- Sequential access is fastest (good cache locality)
- Random access is supported but slower
- Prefer batch operations over row-by-row

### String Handling

- String pool deduplication saves memory for repeated values
- Long strings are stored efficiently (no fixed-size padding)
- Consider string interning for high-cardinality columns

### Memory Alignment

- Numeric columns are aligned for SIMD operations
- Use `Column::data()` for direct buffer access in hot paths

## Related Documentation

- [CODEC.md](CODEC.md) - How segments are compressed
- [PIPELINE.md](PIPELINE.md) - How segments flow through read/write
- [ENTITY.md](ENTITY.md) - Data type definitions
