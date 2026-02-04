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

```cpp
class SegmentInMemory {
public:
    // Construction
    SegmentInMemory(const StreamDescriptor& desc);

    // Row operations
    void end_row();                    // Finalize current row
    size_t row_count() const;

    // Column access
    Column& column(size_t idx);
    const Column& column(size_t idx) const;
    size_t num_columns() const;

    // String pool
    StringPool& string_pool();

    // Iteration
    auto begin() { return columns_.begin(); }
    auto end() { return columns_.end(); }
};
```

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

```cpp
class Column {
public:
    // Type information
    TypeDescriptor type() const;  // Access data_type via type().data_type()

    // Data access
    template<typename T>
    void set_scalar(size_t row, T val);

    template<typename T>
    T scalar_at(size_t row) const;

    // Buffer access
    ChunkedBuffer& buffer();
    const ChunkedBuffer& buffer() const;

    // Column data wrapper for iteration
    ColumnData data() const;

    // Sparse support
    bool is_sparse() const;
    util::BitSet& sparse_map();

    // Type modification
    void change_type(DataType target_type);
};
```

### Type-Safe Access

```cpp
// Reading values
const Column& col = segment.column(0);
if (col.type().data_type() == DataType::FLOAT64) {
    double val = col.scalar_at<double>(row_idx);
}

// Writing values (via segment set_scalar)
segment.set_scalar<int64_t>(col_idx, 42);
```

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

```cpp
class StringPool {
public:
    using offset_t = position_t;

    // Add string and get offset (deduplicate by default)
    // Note: Method is called get(), not add()
    OffsetString get(std::string_view s, bool deduplicate = true);
    OffsetString get(const char* data, size_t size, bool deduplicate = true);

    // Retrieve string by offset
    std::string_view get_view(offset_t o);
    std::string_view get_const_view(offset_t o) const;

    // Memory management
    void clear();
    std::shared_ptr<StringPool> clone() const;
};
```

### String Column Storage

String columns store offsets into the string pool rather than the strings themselves:

```cpp
// Column data contains offsets
Column string_col(TypeDescriptor{DataType::UTF_DYNAMIC64, Dimension::SCALAR});

// Add string to pool using get(), store offset in column
// get() returns OffsetString which contains the offset
auto off_string = segment.string_pool().get("hello world");
string_col.push_back(off_string);
```

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

## Usage Examples

### Creating a Segment

```cpp
#include <arcticdb/column_store/memory_segment.hpp>

// Define schema
StreamDescriptor desc;
desc.add_field(FieldRef{TypeDescriptor{DataType::INT64, Dimension::SCALAR}, "id"});
desc.add_field(FieldRef{TypeDescriptor{DataType::FLOAT64, Dimension::SCALAR}, "value"});

// Create segment
SegmentInMemory segment(desc);

// Add rows
segment.set_scalar<int64_t>(0, 1);     // Column 0
segment.set_scalar<double>(1, 3.14);   // Column 1
segment.end_row();

segment.set_scalar<int64_t>(0, 2);
segment.set_scalar<double>(1, 2.71);
segment.end_row();
```

### Iterating Over Columns

```cpp
for (size_t i = 0; i < segment.num_columns(); ++i) {
    const Column& col = segment.column(i);
    std::cout << "Column " << i << ": type=" << col.type()
              << ", rows=" << col.row_count() << std::endl;
}
```

### Working with Strings

```cpp
// Add string column
desc.add_field(FieldRef{TypeDescriptor{DataType::UTF_DYNAMIC64, Dimension::SCALAR}, "name"});
SegmentInMemory segment(desc);

// Add string value
segment.set_string(2, "Alice");
segment.end_row();

// Read string value
std::string_view name = segment.string_at(2, 0);  // Column 2, row 0
```

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
