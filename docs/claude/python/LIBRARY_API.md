# Library API (V2)

The `Library` class (`python/arcticdb/version_store/library.py`) is the main user-facing API for ArcticDB.

## Overview

The Library class provides:
- Write, read, update, and append operations
- Version management
- Snapshots
- Query builder integration

## Location

`python/arcticdb/version_store/library.py`

## Basic Operations

### Write

```python
import pandas as pd
from arcticdb import Arctic

ac = Arctic("lmdb://./db")
lib = ac.create_library("my_lib")

# Simple write
df = pd.DataFrame({"a": [1, 2, 3], "b": [4.0, 5.0, 6.0]})
result = lib.write("my_symbol", df)
print(f"Written version: {result.version}")

# Write with metadata
lib.write("my_symbol", df, metadata={"source": "manual"})

# Write with prune (delete previous versions)
lib.write("my_symbol", df, prune_previous_versions=True)
```

### Read

```python
# Read latest version
result = lib.read("my_symbol")
df = result.data
metadata = result.metadata
version = result.version

# Read specific version
result = lib.read("my_symbol", as_of=0)

# Read by timestamp
result = lib.read("my_symbol", as_of="2024-01-15")

# Read from snapshot
result = lib.read("my_symbol", as_of="my_snapshot")

# Read specific columns
result = lib.read("my_symbol", columns=["a", "b"])

# Read with date range (for time-indexed data)
result = lib.read("my_symbol", date_range=(start_time, end_time))
```

### Append

```python
# Append new rows to latest version
new_data = pd.DataFrame({"a": [4, 5], "b": [7.0, 8.0]})
result = lib.append("my_symbol", new_data)
print(f"New version: {result.version}")
```

### Update

```python
# Update rows in a date range
update_data = pd.DataFrame(
    {"a": [10, 11]},
    index=pd.date_range("2024-01-01", periods=2, freq="D")
)
result = lib.update("my_symbol", update_data)
```

### Delete

```python
# Delete specific version
lib.delete("my_symbol", versions=[1, 2])

# Delete all versions
lib.delete("my_symbol")
```

## Class Definition

`Library` class in `python/arcticdb/version_store/library.py` provides:
- `write(symbol, data, metadata, prune_previous_versions, staged, validate_index)` - Write data
- `read(symbol, as_of, date_range, columns, query_builder)` - Read data
- `append(symbol, data, metadata, prune_previous_versions)` - Append rows
- `update(symbol, data, metadata, upsert, date_range)` - Update rows
- `delete(symbol, versions)` - Delete symbol or specific versions

Note: The parameter is `prune_previous_versions` (plural) in V2 API.

## Version Management

### List Versions

```python
# Get all versions of a symbol
versions = lib.list_versions("my_symbol")
for v in versions:
    print(f"Version {v.version}: {v.date}")
```

### Prune Versions

```python
# Delete old versions, keep only latest
lib.prune_previous_versions("my_symbol")
```

### Version Query Types

```python
# By version number
lib.read("symbol", as_of=5)

# By timestamp (reads version that existed at that time)
lib.read("symbol", as_of=datetime(2024, 1, 15))
lib.read("symbol", as_of="2024-01-15")

# By snapshot name
lib.read("symbol", as_of="my_snapshot")

# Negative indexing (-1 = latest, -2 = second to last)
lib.read("symbol", as_of=-1)
lib.read("symbol", as_of=-2)
```

## Snapshots

```python
# Create snapshot of all symbols at current versions
lib.snapshot("daily_backup")

# Create snapshot of specific symbols/versions
lib.snapshot("selective", versions={"sym_a": 3, "sym_b": 5})

# List snapshots
snapshots = lib.list_snapshots()

# Delete snapshot
lib.delete_snapshot("old_snapshot")
```

## Symbol Management

```python
# List all symbols
symbols = lib.list_symbols()

# Check if symbol exists
exists = lib.has_symbol("my_symbol")
```

## Query Builder Integration

```python
from arcticdb import QueryBuilder

# Filter rows (using bracket notation)
q = QueryBuilder()
q = q[q["price"] > 100]
result = lib.read("symbol", query_builder=q)

# Select columns with filter
q = QueryBuilder()
q = q[q["volume"] > 1000]
result = lib.read("symbol", columns=["price", "volume"], query_builder=q)

# Aggregation
q = QueryBuilder()
q = q.groupby("category").agg({"price": "sum", "volume": "mean"})
result = lib.read("symbol", query_builder=q)
```

## Batch Operations

```python
# Read multiple symbols
results = lib.read_batch(["sym1", "sym2", "sym3"])

# Write multiple symbols
lib.write_batch({
    "sym1": df1,
    "sym2": df2,
    "sym3": df3
})
```

## Return Types

### VersionedItem

Contains: `symbol`, `library`, `version`, `metadata`, `data` (pd.DataFrame, lazy-loaded), `date_range`.

### VersionInfo

Contains: `version`, `date` (write timestamp), `deleted` (tombstoned), `snapshots` (list of snapshot names).

## Error Handling

```python
from arcticdb.exceptions import (
    NoDataFoundException,
    NoSuchVersionException,
    SymbolNotFound,
    SchemaException
)

try:
    lib.read("nonexistent")
except NoDataFoundException:
    print("Symbol not found")

try:
    lib.read("symbol", as_of=999)
except NoSuchVersionException:
    print("Version does not exist")

try:
    # Append with incompatible schema
    lib.append("symbol", incompatible_df)
except SchemaException:
    print("Schema mismatch")
```

## Advanced Options

### Staged Writes

```python
# Stage data (not committed)
lib.write("symbol", df1, staged=True)
lib.write("symbol", df2, staged=True)

# Finalize staged writes
lib.finalize_staged_data("symbol")
```

### Dynamic Schema

```python
# Allow schema changes between writes
opts = LibraryOptions(dynamic_schema=True)
lib = ac.create_library("dynamic_lib", library_options=opts)

# First write
lib.write("symbol", pd.DataFrame({"a": [1, 2]}))

# Second write with different columns (allowed)
lib.write("symbol", pd.DataFrame({"b": [3, 4]}))
```

## DuckDB SQL Integration

```python
# One-shot SQL query (pushdown optimization, streaming)
df = lib.sql("SELECT ticker, AVG(price) FROM trades GROUP BY ticker")

# Per-symbol versioning via dict
df = lib.sql("SELECT * FROM trades t JOIN prices p ON t.ticker = p.ticker", as_of={"trades": 0, "prices": 1})

# Inspect pushdown optimizations (no data read)
info = lib.explain("SELECT price FROM trades WHERE price > 100")

# Context manager for advanced queries (streaming, per-symbol control)
with lib.duckdb() as ddb:
    ddb.register_symbol("trades", as_of=0)
    ddb.register_symbol("prices")
    result = ddb.sql("SELECT t.ticker, p.price FROM trades t JOIN prices p ON t.ticker = p.ticker")
```

See [DUCKDB.md](DUCKDB.md) for full details.

## Key Files

| File | Purpose |
|------|---------|
| `version_store/library.py` | Library class |
| `version_store/_store.py` | NativeVersionStore (underlying implementation) |
| `version_store/duckdb/` | DuckDB SQL integration module |
| `options.py` | LibraryOptions |

## Related Documentation

- [ARCTIC_CLASS.md](ARCTIC_CLASS.md) - Arctic class that creates libraries
- [NATIVE_VERSION_STORE.md](NATIVE_VERSION_STORE.md) - Underlying V1 API
- [QUERY_PROCESSING.md](QUERY_PROCESSING.md) - QueryBuilder details
- [DUCKDB.md](DUCKDB.md) - DuckDB SQL integration details
- [../cpp/VERSIONING.md](../cpp/VERSIONING.md) - Version chain internals
- [../cpp/ARROW.md](../cpp/ARROW.md) - Arrow output frame (C++ layer)
