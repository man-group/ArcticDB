# Toolbox Module

The toolbox module (`python/arcticdb/toolbox/`) provides administrative and debugging utilities.

## Overview

This module provides:
- Library inspection tools
- Key examination utilities
- Low-level segment access

## Location

`python/arcticdb/toolbox/`

## LibraryTool

### Location

`python/arcticdb/toolbox/library_tool.py`

### Purpose

Low-level inspection of library internals. LibraryTool wraps a C++ implementation (`LibraryToolImpl`) and provides methods to read and examine storage keys and segments.

### Usage

```python
from arcticdb.toolbox.library_tool import LibraryTool

# Create tool from library (requires both library and NativeVersionStore)
lib = ac.get_library("my_lib")
nvs = lib._nvs  # Internal NativeVersionStore
tool = LibraryTool(lib._library, nvs)

# Or get it directly from the library
tool = lib.library_tool()
```

## Key Types

Available key types for inspection (from `arcticdb_ext.storage`):

```python
from arcticdb_ext.storage import KeyType

KeyType.VERSION_REF    # Head of version chain
KeyType.VERSION        # Version metadata
KeyType.TABLE_INDEX    # Index pointing to data
KeyType.TABLE_DATA     # Actual data segments
KeyType.SNAPSHOT_REF   # Snapshot metadata
KeyType.SYMBOL_LIST    # Symbol list entries
KeyType.APPEND_REF     # Incomplete append head
KeyType.APPEND_DATA    # Incomplete append data
KeyType.LOCK           # Distributed locks
```

### List Available Key Types

```python
# Get all available key types
all_types = LibraryTool.key_types()
```

## Key Inspection

### Find Keys for Symbol

```python
from arcticdb_ext.storage import KeyType

# Find keys of a specific type for a symbol
version_keys = tool.find_keys_for_symbol(KeyType.VERSION, "my_symbol")
data_keys = tool.find_keys_for_symbol(KeyType.TABLE_DATA, "my_symbol")
index_keys = tool.find_keys_for_symbol(KeyType.TABLE_INDEX, "my_symbol")
```

### Read Key to Segment

```python
# Read a key's segment into memory
segment = tool.read_to_segment_in_memory(key)

# Get segment info
print(f"Row count: {segment.row_count}")
```

### Read Key to DataFrame

```python
# Read segment as DataFrame (useful for examining VERSION/INDEX keys)
df = tool.read_to_dataframe(key)
print(df)

# Example output for a VERSION key:
#   start_index  end_index  version_id  stream_id  creation_ts  content_hash  index_type  key_type
# 0  2023-01-01  2023-01-02          0       None  1681399019...  356343364...          84         3
```

### Read Key to Keys

```python
# Read a key and extract the AtomKeys stored in its segment
# Useful for reading VERSION keys to get TABLE_INDEX keys, etc.
keys = tool.read_to_keys(version_key, id="my_symbol")

# Optionally filter by key type
index_keys = tool.read_to_keys(version_key, id="my_symbol", filter_key_type=KeyType.TABLE_INDEX)
```

## Reading Index Information

### Read Index for Symbol

```python
# Read the index key for a symbol (returns human-readable DataFrame)
index_df = tool.read_index("my_symbol")

# Read index for specific version
index_df = tool.read_index("my_symbol", as_of=5)
```

## Working with Segments

### Convert DataFrame to Segment

```python
import pandas as pd

# Convert a DataFrame to SegmentInMemory using library defaults
df = pd.DataFrame({"a": [1, 2, 3], "b": [4.0, 5.0, 6.0]})
segment = tool.dataframe_to_segment_in_memory("my_symbol", df)
```

### Convert Segment to DataFrame

```python
# Convert SegmentInMemory back to DataFrame
df = tool.segment_in_memory_to_dataframe(segment)
```

## Advanced Operations

### Append Incomplete Data

```python
# Append data to the APPEND_DATA linked list (for testing)
df = pd.DataFrame({"a": [1, 2, 3]})
tool.append_incomplete("my_symbol", df, validate_index=False)
```

### Overwrite Append Data

```python
# Overwrite append data with new DataFrame (use with extreme caution)
# Returns backup of original segment
backup_segment = tool.overwrite_append_data_with_dataframe(key, new_df)
```

### Update Column Type in Append Data

```python
# Change the type of a column in append data
backup = tool.update_append_data_column_type(key, "column_name", float)
```

## Storage Locks

The (unreliable) `StorageLock` is exposed through `LibraryTool` with user metadata normalized the
same way as symbol metadata (any msgpack-able object, not just dicts). See
`python/arcticdb/toolbox/storage_lock.py` for the wrapper and `cpp/arcticdb/util/storage_lock.hpp`
for the core. It is timestamp + TTL based and works on any backend; two processes can race (use
`ReliableStorageLock` when atomic writes are available). `list_storage_locks` only covers this
`StorageLock` (`KeyType::LOCK`) — it does not see `ReliableStorageLock` locks (`KeyType::ATOMIC_LOCK`).

`read_metadata` and `list_storage_locks` both return metadata even once the lock's TTL has expired
(`active` is `False` in that case) — a stale lock is exactly the one worth attributing when tracing.

```python
tool = lib.library_tool()

lock = tool.get_storage_lock("my_lock")
lock.lock(metadata={"job_name": "nightly"})   # metadata is optional, any msgpack-able object
lock.read_metadata()                           # -> {"job_name": "nightly"} from any process
lock.unlock()

# List the locks in the library, metadata denormalised
tool.list_storage_locks()
# [{"name": "my_lock", "active": True, "timestamp": ..., "metadata": {...}}]
```

### On-disk format and compatibility

Metadata is attached to the lock segment's `google::protobuf::Any` field (the same
`UserDefinedMetadata` path as snapshots), **not** a new column. The timestamp column old clients
read via `scalar_at(0, 0)` is unchanged, so:

- Old clients can read new (metadata-bearing) locks — they ignore the `Any` field.
- New code reads old locks and reports `metadata` as `None`.

## Helper Functions

### Key to Properties Dictionary

```python
from arcticdb.toolbox.library_tool import key_to_props_dict, props_dict_to_atom_key

# Convert key to dictionary
props = key_to_props_dict(key)
print(props)  # {'id': 'symbol', 'version_id': 0, 'creation_ts': ..., ...}

# Convert dictionary back to AtomKey
key = props_dict_to_atom_key(props)
```

## Example Usage

Get a `LibraryTool` via `lib.library_tool()`. Use `find_keys_for_symbol(KeyType, symbol)` to find keys, `read_to_dataframe(key)` to examine key contents, and `read_index(symbol)` to see data segments. See the [GitHub Wiki](https://github.com/man-group/ArcticDB/wiki/Using-the-LibraryTool-to-look-at-a-library's-internal-state) for detailed examples.

## Key Files

| File | Purpose |
|------|---------|
| `toolbox/library_tool.py` | LibraryTool class |
| `toolbox/storage_lock.py` | StorageLock wrapper with normalized metadata |
| `toolbox/__init__.py` | Module exports |

## Cautions

- LibraryTool accesses low-level internals
- Some operations may be slow on large datasets
- Don't use in production hot paths
- API may change between versions
- Operations like `overwrite_append_data_with_dataframe` can corrupt data if used incorrectly

## Related Documentation

- [LIBRARY_API.md](LIBRARY_API.md) - High-level API
- [NATIVE_VERSION_STORE.md](NATIVE_VERSION_STORE.md) - Store internals
- [../cpp/VERSIONING.md](../cpp/VERSIONING.md) - Version chain structure
- [GitHub Wiki: Using the LibraryTool](https://github.com/man-group/ArcticDB/wiki/Using-the-LibraryTool-to-look-at-a-library's-internal-state) - Detailed usage guide
