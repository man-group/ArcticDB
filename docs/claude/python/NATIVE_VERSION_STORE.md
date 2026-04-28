# NativeVersionStore (V1 API)

The `NativeVersionStore` class (`python/arcticdb/version_store/_store.py`) is the V1 API and bridge to C++.

## Overview

NativeVersionStore provides:
- The original V1 API (now wrapped by Library V2)
- Direct interface to C++ bindings
- Lower-level control for advanced use cases

## Location

`python/arcticdb/version_store/_store.py`

## Relationship to Library (V2)

```
User Code
    │
    ▼
Library (V2 API)          ← Recommended for new code
    │
    └── Uses internally
            │
            ▼
NativeVersionStore (V1)   ← Direct C++ interface
    │
    ▼
arcticdb_ext (C++)
```

The V2 `Library` class wraps `NativeVersionStore` and provides a cleaner API. NativeVersionStore remains available for backwards compatibility and advanced use cases.

## Usage

### Direct Usage

```python
from arcticdb.version_store._store import NativeVersionStore

# Usually obtained via Library._nvs or for migration
nvs = lib._nvs

# Or construct directly (advanced)
nvs = NativeVersionStore.create_store_from_config(config, env)
```

### V1 API Methods

```python
# Write
nvs.write("symbol", df)
nvs.write("symbol", df, metadata={"key": "value"})

# Read
result = nvs.read("symbol")
result = nvs.read("symbol", as_of=5)

# Append
nvs.append("symbol", new_df)

# Update
nvs.update("symbol", update_df)

# Delete
nvs.delete("symbol")
nvs.delete_version("symbol", 3)

# List
symbols = nvs.list_symbols()
versions = nvs.list_versions("symbol")
```

## Key Methods

### Write Operations

- `write(symbol, data, metadata, prune_previous_version, pickle_on_failure)` - Write data (note: singular `prune_previous_version` in V1)
- `append(symbol, data, metadata, prune_previous_version, upsert)` - Append rows
- `update(symbol, data, metadata, upsert, date_range)` - Update rows by index range

### Read Operations

- `read(symbol, as_of, date_range, row_range, columns, query_builder)` - Read data
- `read_metadata(symbol, as_of)` - Read only metadata
- `get_info(symbol, as_of)` - Get symbol info without reading data

### Version Operations

- `list_versions(symbol, snapshot, skip_snapshots)` - List versions
- `delete_version(symbol, version)` - Delete specific version
- `prune_previous_versions(symbol)` - Delete all but latest

### Snapshot Operations

- `snapshot(snap_name, metadata, skip_symbols, versions)` - Create snapshot
- `list_snapshots()` - List all snapshots
- `delete_snapshot(snap_name)` - Delete snapshot

## C++ Interface

### How It Connects

`NativeVersionStore._library` is a `PythonVersionStore` instance created via pybind11. Write operations normalize Python data before calling C++; read operations denormalize C++ results back to Python.

### Binding to C++

`PythonVersionStore` is defined in `cpp/arcticdb/version/version_store_api.cpp` and provides `write()` and `read()` methods that work with `InputTensorFrame` and `OutputTensorFrame`.

## Configuration

Store configuration is passed to C++ via `NativeVersionStore.create_store_from_config(config)`. Runtime settings can be adjusted via `arcticdb_ext.set_config_int()` and `set_config_string()`.

## Advanced Features

### Batch Operations

```python
# Batch read (parallel)
results = nvs.batch_read(["sym1", "sym2", "sym3"])

# Batch write
nvs.batch_write({
    "sym1": df1,
    "sym2": df2,
})
```

### Head/Tail

```python
# Read first N rows
result = nvs.head("symbol", n=100)

# Read last N rows
result = nvs.tail("symbol", n=100)
```

### Index Operations

```python
# Get index range
index_range = nvs.get_index_range("symbol")

# Read by row range
result = nvs.read("symbol", row_range=(0, 1000))
```

## Differences from V2 API

| Feature | V1 (NativeVersionStore) | V2 (Library) |
|---------|-------------------------|--------------|
| Naming | `write()`, `read()` | Same |
| Return type | `VersionedItem` | `VersionedItem` |
| as_of syntax | Version number | Version, timestamp, or snapshot name |
| Error types | Mixed | Consistent exception hierarchy |
| Documentation | Minimal | Comprehensive |

## Lazy Arrow Read Path

### Logging

`_store.py` uses `logging.getLogger(__name__)` for debug diagnostics, primarily in the lazy Arrow fallback path.

### `_try_read_lazy_arrow()`

Core method for lazy Arrow/Polars reads (`_store.py:_try_read_lazy_arrow`). Returns `VersionedItem` on success or `None` to trigger fallback to the eager path. Used by `_read_dataframe()` when `output_format` is `PYARROW` or `POLARS`.

**Fallback triggers** (each logged at `logger.debug`):
1. `query_builder` with clauses other than `DateRangeClause`/`RowRangeClause` (groupby, projections, etc.)
2. Custom normalizer detected (non-standard `msg_pack_frame_meta`)
3. Empty result from C++ iterator (0 segments)
4. Arrow schema construction failure (type mismatch, unsupported type)

**DateRangeClause extraction**: When `date_range` comes from `QueryBuilder().date_range()` rather than the `date_range=` parameter, it's stored in `query_builder.clauses` as a `_DateRangeClause`, not in `read_query.row_filter`. The method extracts it and sets `read_query.row_filter = _IndexRange(clause.start, clause.end)` so C++ applies truncation.

### `read_as_lazy_record_batch_iterator()`

Returns `(LazyRecordBatchIterator, resolved_version)` tuple. Delegates to C++ `create_lazy_record_batch_iterator`. Supports:
- `date_range`, `row_range` — passed as `row_filter` to C++
- `columns` — column projection
- `query_builder` — `FilterClause` extracted and passed to C++ for per-segment WHERE evaluation
- `prefetch_size` — controls C++ prefetch buffer depth (default 2)

### OutputFormat Handling

`_get_read_options_and_output_format()` wraps the output format in `OutputFormat.resolve()`, returning `Tuple[ReadOptions, OutputFormat]`. All downstream code compares with `OutputFormat` enum instances directly (no `.lower()` string gymnastics).

## Key Files

| File | Purpose |
|------|---------|
| `version_store/_store.py` | NativeVersionStore class |
| `version_store/_normalization.py` | Data normalization |
| `cpp/arcticdb/version/version_store_api.cpp` | C++ implementation |

## Migration from V1 to V2

```python
# V1 code
from arcticdb.version_store._store import NativeVersionStore
nvs = ...
nvs.write("symbol", df)

# V2 code (recommended)
from arcticdb import Arctic
ac = Arctic("lmdb://./db")
lib = ac.create_library("my_lib")
lib.write("symbol", df)

# If you need NativeVersionStore from Library
nvs = lib._nvs
```

## Related Documentation

- [LIBRARY_API.md](LIBRARY_API.md) - V2 API (recommended)
- [NORMALIZATION.md](NORMALIZATION.md) - Data normalization
- [../cpp/PYTHON_BINDINGS.md](../cpp/PYTHON_BINDINGS.md) - C++ bindings
