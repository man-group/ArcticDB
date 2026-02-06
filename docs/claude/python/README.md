# ArcticDB Python Documentation

> **Note**: This documentation is **owned and maintained by Claude**. It is automatically kept up-to-date as Claude makes changes to the codebase.

This directory contains detailed documentation for the Python layer of ArcticDB.

## Module Index

| Module | Documentation | Description |
|--------|---------------|-------------|
| **Arctic Class** | [ARCTIC_CLASS.md](ARCTIC_CLASS.md) | Top-level entry point and library management |
| **Library API** | [LIBRARY_API.md](LIBRARY_API.md) | Main user-facing V2 API |
| **NativeVersionStore** | [NATIVE_VERSION_STORE.md](NATIVE_VERSION_STORE.md) | V1 API and C++ bridge |
| **Query Processing** | [QUERY_PROCESSING.md](QUERY_PROCESSING.md) | QueryBuilder and expressions |
| **Normalization** | [NORMALIZATION.md](NORMALIZATION.md) | DataFrame normalization |
| **Adapters** | [ADAPTERS.md](ADAPTERS.md) | Storage adapters |
| **Toolbox** | [TOOLBOX.md](TOOLBOX.md) | Library inspection tools |
| **DuckDB** | [DUCKDB.md](DUCKDB.md) | DuckDB SQL integration, pushdown, Arrow streaming |

## Python Code Location

All Python source code is in `python/arcticdb/`:

```
python/arcticdb/
├── arctic.py              # Arctic class - entry point
├── version_store/
│   ├── library.py         # Library class - V2 API
│   ├── _store.py          # NativeVersionStore - V1/C++ bridge
│   ├── processing.py      # QueryBuilder
│   └── _normalization.py  # Data normalization
├── adapters/              # Storage adapters
│   ├── s3_library_adapter.py
│   ├── azure_library_adapter.py
│   ├── lmdb_library_adapter.py
│   └── ...
├── version_store/duckdb/  # DuckDB SQL integration
│   ├── duckdb.py          # Context managers
│   ├── pushdown.py        # SQL pushdown optimization
│   └── arrow_reader.py    # Arrow RecordBatchReader
├── options.py             # LibraryOptions
├── config.py              # Configuration
├── toolbox/               # Admin utilities
│   └── library_tool.py
└── util/                  # Utilities
```

## API Hierarchy

```
Arctic (arctic.py)
  │
  ├── create_library(name) → Library
  ├── get_library(name) → Library
  ├── delete_library(name)
  ├── list_libraries() → List[str]
  ├── sql("SHOW DATABASES") → DataFrame
  ├── duckdb() → ArcticDuckDBContext
  └── duckdb_register(conn) → List[str]
        │
        ▼
Library (version_store/library.py)
  │
  ├── write(symbol, data) → VersionedItem
  ├── read(symbol, as_of=None) → VersionedItem
  ├── append(symbol, data) → VersionedItem
  ├── update(symbol, data, date_range) → VersionedItem
  ├── delete(symbol)
  ├── list_symbols() → List[str]
  ├── list_versions(symbol) → List[VersionInfo]
  ├── snapshot(name)
  ├── sql(query) → DataFrame
  ├── explain(query) → dict
  ├── duckdb() → DuckDBContext
  └── duckdb_register(conn) → List[str]
        │
        ▼
NativeVersionStore (version_store/_store.py)
  │  (V1 API - methods mirror Library)
  │
  ▼
arcticdb_ext (C++ bindings)
```

## Quick Reference

### Installation

```bash
pip install arcticdb
```

### Basic Usage

```python
from arcticdb import Arctic
import pandas as pd

# Connect to storage
ac = Arctic("lmdb://./my_db")

# Create library
lib = ac.create_library("my_lib")

# Write data
df = pd.DataFrame({"col": [1, 2, 3]})
lib.write("symbol", df)

# Read data
result = lib.read("symbol")
print(result.data)
```

### QueryBuilder Usage

```python
from arcticdb import QueryBuilder

q = QueryBuilder()
q = q[q["price"] > 100]  # Use bracket notation for filtering

result = lib.read("symbol", query_builder=q)
```

## Testing

```bash
# Run all tests
python -m pytest python/tests/

# Run unit tests only
python -m pytest python/tests/unit/

# Run specific test file
python -m pytest python/tests/unit/arcticdb/test_arctic.py
```

## Related Documentation

- [ARCHITECTURE.md](../ARCHITECTURE.md) - Overall system architecture
- [../cpp/](../cpp/) - C++ layer documentation
- [VERSIONING.md](../cpp/VERSIONING.md) - Version chain management
- [STORAGE_BACKENDS.md](../cpp/STORAGE_BACKENDS.md) - Storage implementations
