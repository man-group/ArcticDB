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
same way as symbol metadata (any msgpack-able object, not just dicts), but capped at 1MB rather
than the general 16MB user-metadata limit — the locking mechanism relies on reads and writes
happening much faster than the pre-emption window (`StorageLock.WaitMs`), so lock metadata must
stay small. See `python/arcticdb/toolbox/storage_lock.py` for the wrapper and
`cpp/arcticdb/util/storage_lock.hpp` for the core. It is timestamp + TTL based and works on any
backend; two processes can race (use `ReliableStorageLock` when atomic writes are available).
`list_storage_locks` only covers this `StorageLock` (`KeyType::LOCK`) — it does not see
`ReliableStorageLock` locks (`KeyType::ATOMIC_LOCK`).

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

## AdminTools: Size Scanning

`version_store/admin_tools.py` exposes size accounting, separately from `LibraryTool`. Obtained via
`Library.admin_tools()`.

| Method | Scans |
|--------|-------|
| `get_sizes(key_types=None)` | Whole library, grouped by key type |
| `get_sizes_by_symbol()` | Whole library, grouped by symbol then key type |
| `get_sizes_for_symbol(symbol)` | One symbol, grouped by key type |

### Call path

```
AdminTools.get_sizes
  -> PythonVersionStore::scan_object_sizes            (version/local_versioned_engine.cpp)
       one folly future per key type, run concurrently
  -> AsyncStore::get_object_sizes                     (async/async_store.hpp)
       aggregates count/bytes into storage::ObjectSizes, times the scan
  -> Storage::visit_object_sizes                      (storage/storage.hpp)
       primary storage only by default (storages.hpp)
  -> do_visit_object_sizes_for_type_impl              (storage/s3/detail-inl.hpp)
       paged ListObjectsV2, sizes taken from the listing - no HEAD per object
```

### Key type sets

`TYPES_FOR_SIZE_CALCULATION` in `local_versioned_engine.cpp` is the ten types scanned when `key_types` is not given.
There is no C++ list of the full set: the public `arcticdb.KeyType` enum is it, so `list(KeyType)` is how a caller
asks for everything. Absent from it are `TOMBSTONE` and `TOMBSTONE_ALL`, which are only ever written inside VERSION
key segments, and `STREAM_GROUP`, which cannot be scanned at all: it is key type 0, `foreach_key_type` iterates from
1, and `key_type_from_int` asserts `type_num > 0`, so ArcticDB treats 0 as "not a key type" throughout. LMDB never
opens a database for it, and asking for it raises `InternalException: std::out_of_range(_Map_base::at)` out of
`LmdbStorage::get_dbi`. Nothing can be stored under it, so it is left out of both enums rather than special-cased.

`_TO_NATIVE`/`_FROM_NATIVE` in `admin_tools.py` map between `KeyType` and `arcticdb_ext.storage.KeyType`, derived by
member name so a new type needs adding in one place. `ATOMIC_LOCK` is the sole exception — the extension binds it
under the name `SLOW_LOCK`, which `_NATIVE_NAMES` records. `test_get_sizes_key_types_everything` asserts that a
`list(KeyType)` scan returns exactly `set(KeyType)`, which is what catches an entry that cannot in fact be scanned.

Cost scales with object count *and* with the number of key types requested, and the per-key-type cost depends on
whether the storage overrides `supports_object_size_calculation()` — S3 and NFS-backed do (`s3_storage.cpp:279`,
`nfs_backed_storage.cpp:252`) and answer from a prefix listing. Everything else falls through to `iterate_type` plus
`read_ignoring_key_not_found` in `AsyncStore::visit_object_sizes`, reading and decoding every object of the key type,
so a `list(KeyType)` scan on LMDB or Azure reads the whole library. `ObjectSizes.scan_duration_ns` reports the
wall-clock time of an individual key type's scan.

### Partial failures

`scan_object_sizes` collects the per-key-type futures with `folly::collectAll`, so one key type's failure does not
cancel the rest. What happens to it is the caller's choice:

- `raise_on_failure=true` (default, and what `AdminTools.get_sizes` uses) — rethrow, so a caller totalling a library
  never silently under-reports.
- `raise_on_failure=false` — log a warning naming the key type and omit it from the result. For callers scanning
  every key type, where one unlistable prefix should not cost the whole library's numbers. The enterprise
  `populate_library_size` job uses this; an omitted key type is indistinguishable from an empty one.

Covered by `cpp/arcticdb/version/test/test_object_sizes.cpp`, which substitutes a store that fails one chosen key
type — not reachable from python, since every storage can list every key type in `KeyType`.

Only S3 and NFS-backed storages implement `do_visit_object_sizes`; the rest fall back to reading each key and summing
segment sizes (`AsyncStore::visit_object_sizes`), which is why LMDB and in-memory libraries also report sizes.

## Key Files

| File | Purpose |
|------|---------|
| `toolbox/library_tool.py` | LibraryTool class |
| `toolbox/storage_lock.py` | StorageLock wrapper with normalized metadata |
| `toolbox/__init__.py` | Module exports |
| `version_store/admin_tools.py` | AdminTools, KeyType, Size |

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
