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

Two main key types exist:
1. **AtomKey** - immutable, content-addressed keys (defined in `cpp/arcticdb/entity/atom_key.hpp`)
2. **RefKey** - mutable reference keys that point to AtomKeys (defined in `cpp/arcticdb/entity/ref_key.hpp`)

The `KeyType` enum in `cpp/arcticdb/entity/key.hpp` defines all key types. Key types include:
- `TABLE_DATA` (2) - Actual data segments (leaf nodes)
- `TABLE_INDEX` (3) - Index pointing to TABLE_DATA keys
- `VERSION` (4) - Version metadata + link to TABLE_INDEX
- `SYMBOL_LIST` (8) - Symbol list entries
- `VERSION_REF` (9) - Reference to latest VERSION
- `SNAPSHOT_REF` (14) - Named snapshot reference
- `TOMBSTONE` (15) - Deleted version marker
- `TOMBSTONE_ALL` (21) - Marks all prior versions deleted
- `LOCK` (13) / `ATOMIC_LOCK` (28) - Distributed lock keys

The `AtomKey` struct contains: `StreamId` (symbol name), `VersionId`, `timestamp` (creation time), `ContentHash`, and `KeyType`.

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

The `DataType` enum in `cpp/arcticdb/entity/types.hpp` defines supported data types:

| Category | Types |
|----------|-------|
| Integers | `INT8`, `INT16`, `INT32`, `INT64`, `UINT8`, `UINT16`, `UINT32`, `UINT64` |
| Floats | `FLOAT32`, `FLOAT64` |
| Boolean | `BOOL8`, `BOOL_OBJECT8` (nullable) |
| Timestamp | `NANOSECONDS_UTC64` (nanoseconds since epoch) |
| Strings | `ASCII_FIXED64`, `ASCII_DYNAMIC64`, `UTF_FIXED64`, `UTF_DYNAMIC64`, `UTF_DYNAMIC32` |
| Special | `EMPTYVAL` (null), `UNKNOWN` |

### Type Properties

Helper functions in `cpp/arcticdb/entity/types.hpp`:
- `get_type_size(DataType)` - Returns byte size of type
- `is_floating_point_type(DataType)` - Check if float type
- `is_integer_type(DataType)` - Check if integer type
- `is_numeric_type(DataType)` - Check if numeric (int or float)
- `is_sequence_type(DataType)` - Check if string/array type

## Type Descriptors

### Location

`cpp/arcticdb/entity/types.hpp`, `cpp/arcticdb/entity/field_collection.hpp`

### TypeDescriptor

`TypeDescriptor` (in `cpp/arcticdb/entity/types.hpp`) describes a column's type, combining:
- `DataType data_type_` - The underlying data type
- `Dimension dimension_` - `Dim0` (scalar), `Dim1` (1D array), or `Dim2` (2D array)

### FieldRef

`FieldRef` (in `cpp/arcticdb/entity/field_collection.hpp`) references a column in a segment with its `TypeDescriptor` and `name_`.

## Error Handling

### Location

`cpp/arcticdb/util/error_code.hpp`

### Error Codes

Error codes are defined via the `ARCTIC_ERROR_CODES` macro in `error_code.hpp`. Common codes include:
- `E_KEY_NOT_FOUND`, `E_SYMBOL_NOT_FOUND`, `E_VERSION_NOT_FOUND` - Lookup failures
- `E_STORAGE_ERROR` - Storage backend errors
- `E_INVALID_ARGUMENT` - User input errors
- `E_INTERNAL_ERROR` - Internal bugs/assertions

### Exceptions

ArcticDB exception classes (in `cpp/arcticdb/util/`):
- `ArcticException` - Base exception class
- `StorageException` - Storage-related errors
- `UserInputException` - Invalid arguments, not found, etc.
- `InternalException` - Internal errors (bugs, assertions)

## Stream ID

`StreamId` (in `cpp/arcticdb/entity/types.hpp`) is a `std::variant<StringId, NumericId>`:
- `StringId` - Symbol name as string (e.g., "my_symbol")
- `NumericId` - Symbol as numeric ID (internal use)

## Key Files

| File | Purpose |
|------|---------|
| `key.hpp` | Key type definitions |
| `atom_key.hpp` | AtomKey implementation |
| `ref_key.hpp` | RefKey implementation |
| `types.hpp` | Data types and type descriptors |
| `field_collection.hpp` | Field/column collection |
| `protobufs.hpp` | Protobuf conversion helpers |

## Usage

Keys are created using `atom_key_builder()` in `cpp/arcticdb/entity/atom_key.hpp`. Type descriptors are constructed with `TypeDescriptor{DataType, Dimension}`.

## Related Documentation

- [CODEC.md](CODEC.md) - How entities are serialized
- [COLUMN_STORE.md](COLUMN_STORE.md) - How columns use types
