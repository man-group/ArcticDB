# ArcticDB Versioning System

This document describes how ArcticDB manages versions of data, including the version chain structure, write/read paths, and deletion mechanisms.

## Overview

ArcticDB provides immutable versioning for DataFrame data. Each write creates a new version, and previous versions remain accessible until explicitly deleted.

## Version Chain Structure

### Key Types

```
VERSION_REF (symbol)          ← Fast "latest" lookup (ref key)
    │
    └── VERSION (v3)          ← Latest version (atom key)
            │
            ├── TABLE_INDEX   ← Points to data segments
            │       │
            │       ├── TABLE_DATA (rows 0-100K, cols A-M)
            │       ├── TABLE_DATA (rows 0-100K, cols N-Z)
            │       ├── TABLE_DATA (rows 100K-200K, cols A-M)
            │       └── ...
            │
            └── VERSION (v2)  ← Previous version (linked list)
                    │
                    ├── TABLE_INDEX → TABLE_DATA...
                    │
                    └── VERSION (v1)
                            └── TABLE_INDEX → TABLE_DATA...
```

### Key Type Definitions

Located in `cpp/arcticdb/entity/key.hpp`:

| Key Type | Class | Purpose |
|----------|-------|---------|
| `VERSION_REF` | REF | Points to latest VERSION for fast lookup |
| `VERSION` | ATOM | Contains version metadata and link to previous version |
| `TABLE_INDEX` | ATOM | Index of TABLE_DATA segments for one version |
| `TABLE_DATA` | ATOM | Actual compressed DataFrame data |
| `TOMBSTONE` | (virtual) | Marks a version as deleted (inside VERSION segment) |
| `TOMBSTONE_ALL` | (virtual) | Marks all versions before a point as deleted |
| `SNAPSHOT_REF` | REF | Named snapshot pointing to TABLE_INDEX keys |
| `APPEND_REF` | REF | Head of incomplete append chain |
| `SYMBOL_LIST` | ATOM | Symbol list modifications for `list_symbols()` |

## Write Path

### Standard Write Flow

```
DataFrame
    │
    ▼
┌─────────────────────┐
│    Normalization    │  ← Convert to internal representation
│    (Python layer)   │
└──────────┬──────────┘
           │
           ▼
┌─────────────────────┐
│      Slicing        │  ← Split into row/column segments
│   (segment_row_size │     Default: 100,000 rows
│    column_group_size)│
└──────────┬──────────┘
           │
           ▼
┌─────────────────────┐
│    Compression      │  ← LZ4 (default) or ZSTD
│    (codec layer)    │
└──────────┬──────────┘
           │
           ▼
┌─────────────────────┐
│   Storage Write     │  1. Write TABLE_DATA segments
│                     │  2. Write TABLE_INDEX
│                     │  3. Write VERSION (with prev link)
│                     │  4. Update VERSION_REF
└─────────────────────┘
```

### Key Files

| File | Purpose |
|------|---------|
| `cpp/arcticdb/pipeline/write_frame.cpp` | DataFrame serialization |
| `cpp/arcticdb/pipeline/slicing.cpp` | Row/column segmentation |
| `cpp/arcticdb/codec/codec.cpp` | Compression |
| `cpp/arcticdb/version/local_versioned_engine.cpp` | Version chain management |

### Write Operations

| Operation | Method | Behavior |
|-----------|--------|----------|
| `write()` | `write_versioned_dataframe()` | Creates new version, replaces all data |
| `append()` | `append_internal()` | Adds rows to latest version |
| `update()` | `update_internal()` | Modifies rows by index range |

## Read Path

### Standard Read Flow

```
Read Request (symbol, version_query)
           │
           ▼
┌─────────────────────┐
│  Version Resolution │  ← Find the VERSION key
│  (check_reload)     │     - Uses cache if fresh
│                     │     - Reloads from storage if stale
└──────────┬──────────┘
           │
           ▼
┌─────────────────────┐
│   Index Lookup      │  ← Read TABLE_INDEX
│                     │     Determine which segments needed
└──────────┬──────────┘
           │
           ▼
┌─────────────────────┐
│  Parallel Fetch     │  ← Retrieve TABLE_DATA segments
│  (async I/O)        │     Decompress in parallel
└──────────┬──────────┘
           │
           ▼
┌─────────────────────┐
│   Reconstruction    │  ← Assemble DataFrame
│   (read_frame)      │
└─────────────────────┘
```

### Version Query Types

```cpp
// In cpp/arcticdb/pipeline/query.hpp

VersionQuery can be:
├── std::monostate           → Read latest version
├── SpecificVersionQuery     → Read by version ID (e.g., as_of=5)
├── TimestampVersionQuery    → Read by timestamp (e.g., as_of="2024-01-01")
└── SnapshotVersionQuery     → Read from named snapshot
```

### Key Files

| File | Purpose |
|------|---------|
| `cpp/arcticdb/version/local_versioned_engine.cpp` | `get_version_to_read()` |
| `cpp/arcticdb/pipeline/read_frame.cpp` | DataFrame deserialization |
| `cpp/arcticdb/version/version_functions.hpp` | Version lookup helpers |

## Deletion and Tombstones

### Soft Delete (Tombstones)

ArcticDB uses lazy deletion via tombstones:

```
Before delete(v2):               After delete(v2):

VERSION_REF → v3                 VERSION_REF → v3
              │                                │
              v3 → INDEX                       v3 → INDEX
              │                                │
              v2 → INDEX    ──►                v2 → INDEX + TOMBSTONE
              │                                │
              v1 → INDEX                       v1 → INDEX
```

Tombstoned versions:
- Are not visible in `list_versions()`
- Return error if read explicitly
- Data remains in storage until compaction

### Hard Delete (Prune)

`prune_previous_versions` physically removes old version data:

```python
# Prune on write
lib.write("sym", df, prune_previous_version=True)

# Explicit prune
lib.prune_previous_versions("sym")
```

### TOMBSTONE_ALL

Marks all versions before a point as deleted:

```
VERSION_REF → v5
              │
              v5 → INDEX
              │
              v4 → INDEX + TOMBSTONE_ALL  ← v1, v2, v3 implicitly tombstoned
              │
              v3 → INDEX
              │
              ...
```

### Key Files

| File | Purpose |
|------|---------|
| `cpp/arcticdb/version/version_map_entry.hpp` | Tombstone tracking |
| `cpp/arcticdb/version/local_versioned_engine.cpp` | Delete operations |
| `cpp/arcticdb/version/version_tasks.hpp` | Async delete tasks |

## Snapshots

### Snapshot Structure

```
SNAPSHOT_REF ("my_snapshot")
        │
        └── Segment containing:
                ├── symbol_A → TABLE_INDEX_KEY (v3)
                ├── symbol_B → TABLE_INDEX_KEY (v2)
                └── symbol_C → TABLE_INDEX_KEY (v5)
```

Snapshots:
- Reference specific versions of multiple symbols
- Prevent referenced data from being deleted
- Are independent of the version chain

### Creating Snapshots

```python
lib.snapshot("my_snapshot")  # Snapshot all symbols at current versions
lib.snapshot("my_snapshot", versions={"sym_a": 3, "sym_b": 2})  # Specific versions
```

### Key Files

| File | Purpose |
|------|---------|
| `cpp/arcticdb/version/snapshot.cpp` | Snapshot helper functions (`write_snapshot_entry()`) |
| `cpp/arcticdb/version/version_store_api.cpp` | Snapshot API methods |

## Symbol List

### Purpose

The symbol list provides fast `list_symbols()` without scanning all VERSION_REF keys.

### Structure

```
SYMBOL_LIST keys form a concurrent data structure:

__symbols__ (compacted) → [sym_A, sym_B, sym_C, ...]
__add__     (delta)     → [sym_D]
__add__     (delta)     → [sym_E]
__delete__  (delta)     → [sym_A]
```

Periodically compacted into a single `__symbols__` key.

### Key Files

| File | Purpose |
|------|---------|
| `cpp/arcticdb/version/symbol_list.cpp` | Symbol list implementation |
| `cpp/arcticdb/version/symbol_list.hpp` | Symbol list interface |

## Version ID Assignment

### Rules

1. Version IDs are monotonically increasing per symbol
2. First version is always 0
3. Appends increment the version ID
4. Writes increment the version ID
5. Deletes don't change version IDs (use tombstones)

### Negative Version IDs

Python API supports negative indexing:

```python
lib.read("sym", as_of=-1)  # Latest version
lib.read("sym", as_of=-2)  # Second to latest
```

Resolved in `get_version_id_negative_index()`.

## Concurrency

### Write Locks

Concurrent writes to the same symbol are serialized using LOCK keys:

```
Process A: write("sym") ──► acquire_lock("sym") ──► write ──► release_lock
Process B: write("sym") ──► acquire_lock("sym") [blocks] ──► write ──► release_lock
```

### Read Concurrency

Reads are lock-free:
- Multiple processes can read simultaneously
- Reads see a consistent snapshot (version chain is immutable)
- Cache may return stale "latest" but specific version reads are accurate

### Key Files

| File | Purpose |
|------|---------|
| `cpp/arcticdb/util/storage_lock.hpp` | Storage lock interface |
| `cpp/arcticdb/util/reliable_storage_lock.hpp` | Reliable distributed locking |
| `cpp/arcticdb/version/local_versioned_engine.cpp` | Lock acquisition |
