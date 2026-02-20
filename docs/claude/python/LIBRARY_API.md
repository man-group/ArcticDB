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
# Read latest version (default: Pandas output via eager path)
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

# Arrow output — uses lazy streaming C++ path (memory-efficient)
result = lib.read("my_symbol", output_format=OutputFormat.PYARROW)
arrow_table = result.data  # pa.Table (may have chunked columns from segment boundaries)

# Polars output — also uses lazy streaming path
result = lib.read("my_symbol", output_format=OutputFormat.POLARS)
polars_df = result.data
```

**Output format routing**: `output_format='pyarrow'` and `output_format='polars'` use `LazyRecordBatchIterator` (streaming, memory-bounded). `output_format='pandas'` (default) uses the eager path. When `query_builder` is provided, all formats fall back to the eager path. See `version_store_api.cpp:read_dataframe_version()` and `_adapt_frame_data()` in `_store.py`.

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
- `read(symbol, as_of, date_range, columns, query_builder, lazy, output_format)` - Read data. `lazy=True` returns `LazyDataFrame` instead of executing immediately. `output_format='pyarrow'`/`'polars'` uses lazy streaming C++ path.
- `append(symbol, data, metadata, prune_previous_versions)` - Append rows
- `update(symbol, data, metadata, upsert, date_range)` - Update rows
- `delete(symbol, versions)` - Delete symbol or specific versions
- `sql(query, as_of, output_format)` - SQL query with pushdown optimization
- `explain(query)` - Pushdown introspection without executing query
- `duckdb(connection)` - Context manager for advanced SQL queries

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

## Lazy DataFrames

When `lazy=True` is passed to `read()` or `read_batch()`, a lazy wrapper is returned instead of executing the read immediately. Queries are chained and only executed on `.collect()`.

### LazyDataFrame (from `read(..., lazy=True)`)

Extends `QueryBuilder` — supports all QueryBuilder operations (filter, project, groupby, etc.). Returned by `Library.read()`, `Library.head()`, `Library.tail()` when `lazy=True`.

```python
lazy_df = lib.read("symbol", as_of=0, columns=["col1"], lazy=True)
lazy_df = lazy_df[lazy_df["col1"] > 100]  # Chain filters
lazy_df["new_col"] = lazy_df["col1"] + 1   # Chain projections
result = lazy_df.collect()                  # Execute read + queries
```

Key methods: `collect()` → `VersionedItem`, `_collect_schema()` → `pl.Schema` (for Polars LazyFrame integration).

### LazyDataFrameCollection (from `read_batch(..., lazy=True)`)

Extends `QueryBuilder` — applies queries to ALL symbols in the batch.

```python
lazy_dfs = lib.read_batch(["sym1", "sym2"], lazy=True)
lazy_dfs = lazy_dfs[lazy_dfs["col1"] > 0]  # Applied to both symbols
per_symbol = lazy_dfs.split()               # Split into individual LazyDataFrames
results = lazy_dfs.collect()                # Execute all reads
```

Key methods: `collect()` → `List[Union[VersionedItem, DataError]]`, `split()` → `List[LazyDataFrame]`.

### LazyDataFrameAfterJoin (from `adb.concat(lazy_dfs)`)

Extends `QueryBuilder` — for post-join query chaining.

```python
lazy_dfs = lib.read_batch(["sym1", "sym2"], lazy=True)
joined = adb.concat(lazy_dfs)
joined["new_col"] = joined["col1"] + joined["col2"]
result = joined.collect()  # Returns VersionedItemWithJoin
```

## Batch Operations

```python
# Read multiple symbols
results = lib.read_batch(["sym1", "sym2", "sym3"])

# Read batch with lazy=True
lazy_dfs = lib.read_batch(["sym1", "sym2"], lazy=True)  # Returns LazyDataFrameCollection

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

### `sql(query, as_of=None, output_format=None)` → DataFrame

One-shot SQL query with automatic symbol discovery and pushdown optimization. Returns DataFrame directly (not VersionedItem).

**Optimization paths**:
- **Fast path** (`fully_pushed=True`): bypasses DuckDB entirely for single-table SELECT * queries where all filters are pushed to C++ — uses `lib.read()` directly
- **Streaming path**: creates `LazyRecordBatchIterator` per symbol, registers as Arrow reader with DuckDB
- **Table discovery**: `SHOW TABLES` / `SHOW ALL TABLES` handled without DuckDB

**Index reconstruction**: For pandas output, retrieves index metadata via `get_description()` (~4ms/symbol) and calls `set_index()` with the most specific matching index across all symbols in the query.

```python
df = lib.sql("SELECT ticker, AVG(price) FROM trades GROUP BY ticker")
df = lib.sql("SELECT * FROM trades t JOIN prices p ON t.ticker = p.ticker", as_of={"trades": 0, "prices": 1})
```

### `explain(query)` → dict

Returns pushdown introspection without executing the query — shows which optimizations would be applied.

```python
info = lib.explain("SELECT price FROM trades WHERE price > 100")
# {'trades': {'columns_pushed_down': ['price'], 'filter_pushed_down': True, ...}}
```

### `duckdb(connection=None)` → `DuckDBContext`

Context manager for advanced multi-symbol queries with per-symbol control (versioning, date_range, columns).

```python
with lib.duckdb() as ddb:
    ddb.register_symbol("trades", as_of=0)
    ddb.register_symbol("prices")
    result = ddb.sql("SELECT t.ticker, p.price FROM trades t JOIN prices p ON t.ticker = p.ticker")
```

See [DUCKDB.md](DUCKDB.md) for full details.

### Internal SQL Helpers

- `_read_as_record_batch_reader(symbol, as_of, date_range, row_range, columns, query_builder, **kwargs)` → `Tuple[ArcticRecordBatchReader, int]` — Creates a lazy streaming `ArcticRecordBatchReader` for a symbol. Used internally by `sql()` and `duckdb()`. Delegates to `NativeVersionStore.read_as_lazy_record_batch_iterator()`.

- `_get_index_columns_for_symbol(library, symbol, as_of)` → `Optional[List[str]]` — Static method. Retrieves index column names from symbol metadata via `get_description()`. Used for pandas index reconstruction in SQL results and for date_range pushdown.

- `_resolve_index_columns_for_sql(sql_ast, as_of)` → `Optional[List[str]]` — Extracts symbols from SQL AST, finds datetime index columns via `_get_index_columns_for_symbol()`. Only returns columns with `_DATETIME_DTYPE_MARKERS` types (prevents numeric indexes from being misinterpreted as date_range). Exception handler splits expected failures (`IndexError`, `TypeError`, `ValueError`) from unexpected (`Exception` with `logger.debug`).

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
