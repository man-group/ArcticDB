# Entity Module

The entity module (`cpp/arcticdb/entity/`) defines the core data types used throughout ArcticDB.

## Overview

This module contains:
- Key types for storage (VERSION, TABLE_DATA, etc.)
- Data type definitions (integers, floats, strings, etc.)
- Type descriptors for columns
- Error codes and exceptions

## Key Types

### Location

`cpp/arcticdb/entity/key.hpp`

### Key Classes

```cpp
// Two main key types:
// 1. AtomKey - immutable, content-addressed keys
// 2. RefKey - mutable reference keys (pointers to AtomKeys)

enum class KeyType : int {
    STREAM_GROUP = 0,        // For streaming (not in general use)
    GENERATION = 1,          // For streaming (not in general use)
    TABLE_DATA = 2,          // Actual data segments (leaf nodes)
    TABLE_INDEX = 3,         // Index pointing to TABLE_DATA keys
    VERSION = 4,             // Version metadata + link to TABLE_INDEX
    VERSION_JOURNAL = 5,     // Legacy, not used anymore
    METRICS = 6,             // Legacy, not used anymore
    SNAPSHOT = 7,            // Legacy, replaced by SNAPSHOT_REF
    SYMBOL_LIST = 8,         // Symbol list entries
    VERSION_REF = 9,         // Reference to latest VERSION
    STORAGE_INFO = 10,       // Storage information (last sync time)
    APPEND_REF = 11,         // Incomplete append chain head
    MULTI_KEY = 12,          // Index of indexes
    LOCK = 13,               // Distributed lock key
    SNAPSHOT_REF = 14,       // Named snapshot reference
    TOMBSTONE = 15,          // Deleted version marker (lives inside VERSION)
    APPEND_DATA = 16,        // Incomplete appended segment
    LOG = 17,                // Log entries (for replication)
    PARTITION = 18,          // Partition references
    OFFSET = 19,             // Stream offsets
    BACKUP_SNAPSHOT_REF = 20,// Temporary backup snapshot reference
    TOMBSTONE_ALL = 21,      // Marks all prior versions deleted
    LIBRARY_CONFIG = 22,     // Library configuration
    SNAPSHOT_TOMBSTONE = 23, // Marks SNAPSHOT_REF for removal
    LOG_COMPACTED = 24,      // Compacted log entries
    COLUMN_STATS = 25,       // Column statistics
    REPLICATION_FAIL_INFO = 26, // Failed replication info
    BLOCK_VERSION_REF = 27,  // Block version reference for background jobs
    ATOMIC_LOCK = 28,        // List-based reliable storage lock
    UNDEFINED                // Sentinel value
};
```

### AtomKey Structure

```cpp
struct AtomKey {
    StreamId id_;              // Symbol name
    VersionId version_id_;     // Version number
    timestamp creation_ts_;    // Creation timestamp
    ContentHash content_hash_; // Hash of content
    KeyType type_;             // Key type
    // ... methods
};
```

### Key Hierarchy

```
RefKey (mutable)
├── VERSION_REF   → Points to latest VERSION AtomKey
├── SNAPSHOT_REF  → Points to snapshot metadata
├── APPEND_REF    → Points to incomplete append head
└── LOCK          → Distributed lock (RefKey, not AtomKey)

AtomKey (immutable, content-addressed)
├── VERSION       → Version metadata + link to TABLE_INDEX
├── TABLE_INDEX   → List of TABLE_DATA keys
├── TABLE_DATA    → Compressed data segment
└── SYMBOL_LIST   → Symbol list delta
```

## Data Types

### Location

`cpp/arcticdb/entity/types.hpp`

### Type Enumeration

```cpp
enum class DataType : uint8_t {
    UINT8,
    UINT16,
    UINT32,
    UINT64,
    INT8,
    INT16,
    INT32,
    INT64,
    FLOAT32,
    FLOAT64,
    BOOL8,
    NANOSECONDS_UTC64,  // Timestamp (nanoseconds since epoch)
    ASCII_FIXED64,      // Fixed-width ASCII string
    ASCII_DYNAMIC64,    // Variable-length ASCII string
    UTF_FIXED64,        // Fixed-width UTF-8 string
    UTF_DYNAMIC64,      // Variable-length UTF-8 string
    EMPTYVAL,           // Empty/null value
    BOOL_OBJECT8,       // Boolean with null support
    UTF_DYNAMIC32,      // UTF-8 string (32-bit size)
    UNKNOWN = 0,
};
```

### Type Properties

```cpp
// Get size of a data type
constexpr size_t get_type_size(DataType dt) noexcept;

// Check if type is floating point
constexpr bool is_floating_point_type(DataType dt);

// Check if type is integer
constexpr bool is_integer_type(DataType dt);

// Check if type is numeric
constexpr bool is_numeric_type(DataType dt);

// Check if type is sequence (string/array)
constexpr bool is_sequence_type(DataType dt);
```

## Type Descriptors

### Location

`cpp/arcticdb/entity/types.hpp`, `cpp/arcticdb/entity/field_collection.hpp`

### TypeDescriptor

Describes a column's type:

```cpp
struct TypeDescriptor {
    DataType data_type_;
    Dimension dimension_;  // Dim0 (scalar), Dim1 (1D array), Dim2 (2D array)

    // Helper methods
    DataType data_type() const;
    Dimension dimension() const;
    size_t get_type_byte_size() const;
};
```

### Dimension Enum

```cpp
enum class Dimension : uint8_t {
    Dim0 = 0,  // Scalar value
    Dim1 = 1,  // 1-dimensional array (e.g., string, list)
    Dim2 = 2,  // 2-dimensional array
};
```

### FieldRef

Reference to a column in a segment:

```cpp
struct FieldRef {
    TypeDescriptor type_;
    std::string_view name_;
};
```

## Error Handling

### Location

`cpp/arcticdb/util/error_code.hpp`

### Error Codes

Error codes are defined using a macro:

```cpp
// In error_code.hpp
enum class ErrorCode : detail::BaseType {
    // Codes defined via ARCTIC_ERROR_CODES macro
    E_UNSPECIFIED,
    E_KEY_NOT_FOUND,
    E_SYMBOL_NOT_FOUND,
    E_VERSION_NOT_FOUND,
    E_STORAGE_ERROR,
    E_INVALID_ARGUMENT,
    E_INTERNAL_ERROR,
    // ... additional codes
};
```

### Exceptions

ArcticDB uses custom exception types. Key exception classes:

```cpp
// Defined in cpp/arcticdb/util/
class ArcticException : public std::exception;

// Storage-related exceptions
class StorageException;

// User errors (invalid arguments, not found, etc.)
class UserInputException;

// Internal errors (bugs, assertions)
class InternalException;
```

## Stream ID

### Location

`cpp/arcticdb/entity/types.hpp`

### Definition

```cpp
using StreamId = std::variant<StringId, NumericId>;

// StringId: Symbol name as string ("my_symbol")
// NumericId: Symbol as numeric ID (for internal use)
```

## Key Files

| File | Purpose |
|------|---------|
| `key.hpp` | Key type definitions |
| `atom_key.hpp` | AtomKey implementation |
| `ref_key.hpp` | RefKey implementation |
| `types.hpp` | Data types and type descriptors |
| `field_collection.hpp` | Field/column collection |
| `protobufs.hpp` | Protobuf conversion helpers |

## Usage Examples

### Creating a Key

```cpp
#include <arcticdb/entity/atom_key.hpp>

// Create a TABLE_DATA key
auto key = atom_key_builder()
    .gen_id(1)
    .start_index(0)
    .end_index(100000)
    .creation_ts(now())
    .content_hash(hash)
    .build("my_symbol", KeyType::TABLE_DATA);
```

### Working with Types

```cpp
#include <arcticdb/entity/types.hpp>

TypeDescriptor float_type{DataType::FLOAT64, Dimension::Dim0};
bool is_fp = is_floating_point_type(float_type.data_type());  // true

size_t size = get_type_size(DataType::INT32);  // 4
```

## Related Documentation

- [CODEC.md](CODEC.md) - How entities are serialized
- [COLUMN_STORE.md](COLUMN_STORE.md) - How columns use types
