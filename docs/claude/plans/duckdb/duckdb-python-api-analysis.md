# DuckDB Python API Analysis: Replacement Scans and Table Functions

**Date**: 2026-02-08
**DuckDB Version**: 1.2.2
**Environment**: /turbo/jblackburn/pyenvs/arcticdb-claude

## Summary

DuckDB Python API provides **automatic replacement scans** for Python objects (DataFrames, Arrow tables) without requiring explicit registration. The `register()` method allows explicit control over table name resolution. However, there is **no Python API to register custom replacement scan callbacks** in DuckDB 1.2.2.

## Key Findings

### 1. Automatic Replacement Scans (Auto-Scan)

DuckDB automatically resolves Python objects in the calling scope when they are referenced in SQL queries:

```python
import duckdb
import pandas as pd

conn = duckdb.connect()

# This works without calling register()
my_df = pd.DataFrame({'x': [1, 2, 3]})
result = conn.sql('SELECT * FROM my_df').df()  # ✓ Works!
```

**Supported types for auto-scan:**
- `pandas.DataFrame`
- `pyarrow.Table`
- `pyarrow.RecordBatchReader`
- `duckdb.DuckDBPyRelation`
- NumPy ndarrays (with supported format)

**Does NOT support:**
- Callables/functions (raises `Invalid Input Error`)
- Custom Python objects

### 2. The `register()` Method

`conn.register(view_name: str, python_object: object)` explicitly registers a Python object as a queryable view.

**Priority**: Registered objects take precedence over auto-scanned objects with the same name.

```python
# Create a DataFrame in local scope
my_table = pd.DataFrame({'col1': [1, 2, 3]})

# Query via auto-scan
result = conn.sql('SELECT * FROM my_table').df()  # Uses local DataFrame

# Register different data with the same name
conn.register('my_table', pd.DataFrame({'col2': [10, 20]}))
result = conn.sql('SELECT * FROM my_table').df()  # Uses registered DataFrame

# Unregister to fall back to auto-scan
conn.unregister('my_table')
result = conn.sql('SELECT * FROM my_table').df()  # Uses local DataFrame again
```

**Accepted object types:**
- `pandas.DataFrame`
- `pyarrow.Table`
- `pyarrow.RecordBatchReader`
- `duckdb.DuckDBPyRelation`
- Datasets, Scanner, NumPy ndarrays

**Does NOT accept:**
- Callables (error: "not suitable for replacement scans")

### 3. Creating Relations (Lazy Evaluation)

`DuckDBPyRelation` objects provide lazy query evaluation and can be registered:

```python
# Method 1: Create from DataFrame
df = pd.DataFrame({'x': [1, 2, 3]})
rel = conn.from_df(df)

# Method 2: Create from query
rel = conn.sql('SELECT * FROM range(10)')

# Register and query
conn.register('my_relation', rel)
result = conn.sql('SELECT * FROM my_relation WHERE x > 1').df()
```

Relations are composable and support chaining operations like `filter()`, `project()`, `aggregate()`.

### 4. User-Defined Functions (UDFs)

`conn.create_function(name, function, parameters, return_type, type, ...)`

**Supported UDF types** (from `duckdb.functional.PythonUDFType`):
- `PythonUDFType.NATIVE` - Native Python functions (row-by-row)
- `PythonUDFType.ARROW` - Arrow-based functions (batched, faster)

**Scalar UDFs (✓ Supported):**
```python
def add_one(x: int) -> int:
    return x + 1

conn.create_function('add_one', add_one)
result = conn.sql('SELECT add_one(5)').fetchone()  # Returns 6
```

**Table-Valued UDFs (✗ Not Supported in Python API):**
```python
# This does NOT work in DuckDB Python 1.2.2
def my_table_func():
    return pd.DataFrame({'a': [1,2,3]})

conn.create_function('my_func', my_table_func, type=PythonUDFType.ARROW)
# Error: "Could not infer the return type, please set it explicitly"

# Even with explicit return_type:
return_type = conn.struct_type({'a': 'BIGINT'})
conn.create_function('my_func', my_table_func, return_type=return_type)
# Error: "Table Function with name my_func does not exist!"
```

The C API supports table functions (`duckdb_create_table_function`), but the Python API does not expose this functionality as of version 1.2.2.

### 5. Custom Replacement Scan Callbacks

**No Python API exists** to register custom replacement scan callbacks in DuckDB 1.2.2.

The C API provides:
- `duckdb_add_replacement_scan(db, callback, extra_data)`
- Allows intercepting table name lookups and providing custom data sources

This functionality is **not exposed in the Python bindings**.

### 6. Workarounds for ArcticDB Integration

Given the API limitations, here are the viable approaches for integrating ArcticDB with DuckDB:

#### Option 1: Pre-register symbols via `register()`
```python
# User explicitly registers symbols
lib = arctic['mylib']
conn.register('mysymbol', lib['mysymbol'].read())
conn.sql('SELECT * FROM mysymbol')
```

**Pros:**
- Simple, explicit, works today
- User controls what's exposed to DuckDB

**Cons:**
- Requires explicit registration
- Materializes data immediately (no lazy loading)

#### Option 2: `duckdb_register()` helper method
```python
# ArcticDB provides convenience method
lib.duckdb_register(conn, 'mysymbol')
# or register all symbols:
lib.duckdb_register_all(conn)

conn.sql('SELECT * FROM mysymbol')
```

**Pros:**
- Cleaner API
- Can materialize on-demand when registering

**Cons:**
- Still requires explicit registration step

#### Option 3: Auto-scan via DuckDBPyRelation wrapper
```python
# Create a relation that wraps ArcticDB read
class ArcticRelation(duckdb.DuckDBPyRelation):
    def __init__(self, lib, symbol):
        # Lazy read when queried
        pass

mysymbol = lib.get_duckdb_relation('mysymbol')
conn.register('mysymbol', mysymbol)
```

**Pros:**
- Could support lazy evaluation
- Integrates with DuckDB's query optimizer

**Cons:**
- `DuckDBPyRelation` is not easily subclassable (C++ type)
- Still requires `register()` call

#### Option 4: Arrow RecordBatchReader (Streaming)
```python
# Return a streaming Arrow reader
reader = lib.read_batch_stream('mysymbol')
conn.register('mysymbol', reader)
conn.sql('SELECT * FROM mysymbol')
```

**Pros:**
- Supports streaming data
- No need to materialize entire dataset

**Cons:**
- Still requires `register()` call
- Single-pass iteration (can't rewind)

#### Option 5: C++ Extension (Future)
```python
# Build a DuckDB extension that registers replacement scans at C++ level
# Extension can intercept "arctic::<library>.<symbol>" table references
conn.load_extension('arcticdb_duckdb')
conn.sql("SELECT * FROM arctic::mylib.mysymbol")
```

**Pros:**
- True replacement scan behavior (no explicit registration)
- Can optimize query pushdown at C++ level

**Cons:**
- Requires building DuckDB extension in C++
- More complex deployment

## Recommended Approach

For the current DuckDB Python integration:

1. **Short term**: Implement `library.duckdb_register(conn, symbol)` helper method
   - Uses `conn.register()` under the hood
   - Can return Arrow table or RecordBatchReader for large datasets
   - Simple, explicit, works with current API

2. **Medium term**: Explore `pyarrow.RecordBatchReader` for streaming
   - Allows processing large symbols without full materialization
   - Integrates cleanly with existing Arrow support

3. **Long term**: Consider C++ DuckDB extension
   - True replacement scan behavior
   - Query pushdown optimization (filter/projection to ArcticDB)
   - Requires more investment but best user experience

## References

- [DuckDB Replacement Scans (C API)](https://duckdb.org/docs/stable/clients/c/replacement_scans)
- [DuckDB Python Function API](https://duckdb.org/docs/stable/clients/python/function)
- [Python replacement scans issue #9134](https://github.com/duckdb/duckdb/issues/9134)
- [Improved replacement scans discussion #14041](https://github.com/duckdb/duckdb/discussions/14041)

## Testing Notes

All tests performed on:
- Platform: Linux (Man Group headnode)
- Python: 3.11
- DuckDB: 1.2.2
- Virtual environment: /turbo/jblackburn/pyenvs/arcticdb-claude

Key test script: See above code examples for reproducible tests of each API pattern.
