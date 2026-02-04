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

```python
def write(
    self,
    symbol: str,
    data: Any,
    metadata: Any = None,
    prune_previous_version: bool = False,
    pickle_on_failure: bool = False
) -> VersionedItem:
    """
    Write data to storage.

    Args:
        symbol: Symbol name
        data: DataFrame or compatible data
        metadata: User metadata (pickled)
        prune_previous_version: Delete old versions after write
        pickle_on_failure: Fall back to pickle if normalization fails
    """

def append(
    self,
    symbol: str,
    data: Any,
    metadata: Any = None,
    prune_previous_version: bool = False,
    upsert: bool = False
) -> VersionedItem:
    """Append rows to existing symbol."""

def update(
    self,
    symbol: str,
    data: Any,
    metadata: Any = None,
    upsert: bool = False,
    date_range: Tuple = None
) -> VersionedItem:
    """Update rows by index range."""
```

### Read Operations

```python
def read(
    self,
    symbol: str,
    as_of: Optional[int] = None,
    date_range: Tuple = None,
    row_range: Tuple = None,
    columns: List[str] = None,
    query_builder: QueryBuilder = None
) -> VersionedItem:
    """Read data from storage."""

def read_metadata(
    self,
    symbol: str,
    as_of: Optional[int] = None
) -> VersionedItem:
    """Read only metadata (no data)."""

def get_info(
    self,
    symbol: str,
    as_of: Optional[int] = None
) -> Dict:
    """Get symbol information without reading data."""
```

### Version Operations

```python
def list_versions(
    self,
    symbol: str = None,
    snapshot: str = None,
    skip_snapshots: bool = False
) -> List[VersionInfo]:
    """List versions for a symbol."""

def delete_version(
    self,
    symbol: str,
    version: int
) -> None:
    """Delete a specific version."""

def prune_previous_versions(
    self,
    symbol: str
) -> List[int]:
    """Delete all but the latest version."""
```

### Snapshot Operations

```python
def snapshot(
    self,
    snap_name: str,
    metadata: Any = None,
    skip_symbols: List[str] = None,
    versions: Dict[str, int] = None
) -> None:
    """Create a named snapshot."""

def list_snapshots(self) -> Dict[str, Any]:
    """List all snapshots."""

def delete_snapshot(self, snap_name: str) -> None:
    """Delete a snapshot."""
```

## C++ Interface

### How It Connects

```python
class NativeVersionStore:
    def __init__(self, library):
        # library is C++ PythonVersionStore
        self._library = library

    def write(self, symbol, data, ...):
        # Normalize Python data
        normalized = normalize(data)

        # Call C++ write
        return self._library.write(
            symbol,
            normalized,
            ...
        )

    def read(self, symbol, ...):
        # Call C++ read
        result = self._library.read(symbol, ...)

        # Denormalize to Python
        return denormalize(result)
```

### Binding to C++

The `_library` attribute is a `PythonVersionStore` instance created via pybind11:

```cpp
// In cpp/arcticdb/version/version_store_api.cpp
class PythonVersionStore {
    void write(
        const StreamId& stream_id,
        const InputTensorFrame& frame,
        ...
    );

    OutputTensorFrame read(
        const StreamId& stream_id,
        const VersionQuery& version_query,
        ...
    );
};
```

## Configuration

### Store Configuration

```python
# Configuration passed to C++
config = {
    "lib_cfg": library_config_proto,
    "env_cfg": environment_config_proto,
    "open_mode": "write",  # or "read"
}

nvs = NativeVersionStore.create_store_from_config(config)
```

### Runtime Settings

```python
from arcticdb_ext import set_config_int

# Set cache reload interval
set_config_int("VersionMap.ReloadInterval", 5_000_000_000)
```

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
