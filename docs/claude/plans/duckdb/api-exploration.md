# DuckDB API Exploration

## Current State

Three layers today:

| API | Entry Point | Pattern |
|-----|-------------|---------|
| `lib.sql(query)` | One-shot call | Auto-discovers symbols, pushdown, returns `VersionedItem` |
| `lib.duckdb()` | Context manager | Manual `register_symbol()`, then `query()` |
| `arctic.duckdb()` | Context manager | Cross-library, manual `register_symbol(lib, sym)` |

## Problems

### 1. `lib.sql()` returns `VersionedItem` — awkward fit

SQL results aren't versioned. The `symbol` field gets stuffed with a comma-separated string, `version` is `None`. It wraps pushdown metadata into `.metadata`, which is a different meaning than the user-metadata on a symbol. Forced fit.

### 2. Context manager adds ceremony for the common case

For interactive/CLI use — the most likely entry point — users have to write a `with` block just to run a query:

```python
# Current
with lib.duckdb() as ddb:
    ddb.register_symbol("trades")
    result = ddb.query("SELECT * FROM trades WHERE price > 100")

# vs what lib.sql() already does
result = lib.sql("SELECT * FROM trades WHERE price > 100")
```

The context manager is useful for the advanced case (JOINs with different `as_of`, external connections), but that's the minority use case.

### 3. "Register into DuckDB" is the idiomatic DuckDB pattern

DuckDB users are accustomed to `conn.register()` and querying named tables. The question is whether ArcticDB should expose this or handle it transparently.

## Design Options

### Option A: Keep `lib.sql()` as primary API, fix the return type

```python
# Simple — just works
df = lib.sql("SELECT * FROM trades WHERE price > 100")
df = lib.sql("SELECT * FROM trades", output_format="arrow")

# Return DataFrame directly, not VersionedItem
# Pushdown metadata accessible via lib.sql(..., explain=True) if needed
```

Lowest friction for CLI and notebook use. Context manager stays for advanced cases.

### Option B: Register ArcticDB as a DuckDB data source (replacement scan)

```python
conn = duckdb.connect()
lib.duckdb_register(conn)  # or arctic.duckdb_register(conn)

# Now all symbols are queryable — DuckDB resolves them lazily
conn.sql("SELECT * FROM trades WHERE price > 100").df()
conn.sql("SHOW TABLES").df()  # lists symbols
```

Most idiomatic DuckDB pattern. Delta-rs and Iceberg do this via C++ extensions.

**BLOCKER**: DuckDB's replacement scan API is **only available from C/C++** (`duckdb_add_replacement_scan`). There is no Python-level `conn.register_replacement_scan()`. Projects like duckdb-delta and duckdb-lance implement this as compiled C++ extensions. Pure Python cannot achieve transparent table name resolution.

### Option C: Support `ATTACH` semantics

```python
conn = duckdb.connect()
arctic.attach(conn)  # all libraries available as schemas

conn.sql("SELECT * FROM market_data.trades").df()
conn.sql("SELECT * FROM reference_data.securities").df()
conn.sql("SHOW DATABASES").df()  # lists libraries
```

Maps naturally to the `database.library` hierarchy. Also requires a C++ DuckDB extension.

### Option D: Eager `lib.duckdb_register(conn)` — register all symbols upfront

```python
conn = duckdb.connect()
lib.duckdb_register(conn)  # registers all symbols as Arrow tables

# Works with standard DuckDB API
conn.sql("SELECT * FROM trades WHERE price > 100").df()
conn.sql("SHOW TABLES").df()
```

Pure Python, uses `conn.register(name, arrow_reader)` for each symbol. No replacement scan needed — just registers everything upfront. Trade-off: must read schema for all symbols at registration time, though data is still streamed lazily via Arrow RecordBatchReader.

**Limitation**: Registered RecordBatchReaders are single-use. After the first query consumes a reader, subsequent queries on the same table fail. Would need to re-register, or read into materialized Arrow tables (losing streaming benefit for large data).

### Option E: `lib.duckdb_register(conn)` with lazy re-registration

```python
conn = duckdb.connect()
lib.duckdb_register(conn)  # sets up the library reference

# Each query triggers fresh registration behind the scenes
conn.sql("SELECT * FROM trades WHERE price > 100").df()
conn.sql("SELECT * FROM trades LIMIT 10").df()  # works — re-registered
```

Would require wrapping the DuckDB connection to intercept `execute()`/`sql()` calls, parse out table names, and re-register fresh readers before each query. Fragile, non-idiomatic, and defeats the purpose of "just use DuckDB normally".

## Revised Recommendation

### What to build now: Option A (fix return type) + Option D (simple register)

#### Step 1: Fix `lib.sql()` return type

Return a DataFrame directly, not `VersionedItem`. This is the 80% use case.

```python
# Before
result = lib.sql("SELECT * FROM trades")
df = result.data  # have to unwrap VersionedItem

# After
df = lib.sql("SELECT * FROM trades")  # returns DataFrame directly
```

Keep pushdown working as-is. For users who want pushdown metadata, add an `explain` parameter:

```python
df, info = lib.sql("SELECT * FROM trades WHERE x > 5", explain=True)
# info = {"columns_pushed_down": ["x"], "filter_pushed_down": True, ...}
```

#### Step 2: Add `lib.duckdb_register(conn)` for one-off registration

Register all symbols as materialized Arrow tables (not streaming readers) so they're reusable:

```python
conn = duckdb.connect()
lib.duckdb_register(conn)  # registers all symbols as Arrow virtual tables

conn.sql("SELECT * FROM trades WHERE price > 100").df()
conn.sql("SHOW TABLES").df()
conn.sql("DESCRIBE trades").df()
# Multiple queries work because data is materialized
```

Implementation: for each symbol, call `lib.read(symbol, output_format="arrow")` and `conn.register(symbol, arrow_table)`. This materializes data in memory but makes it reusable across queries.

For large libraries, support selective registration:

```python
lib.duckdb_register(conn, symbols=["trades", "prices"])  # only these two
```

#### Step 3: Keep `lib.duckdb()` context manager

Position as the advanced API for:
- Per-symbol `as_of`/`date_range` control
- Streaming (non-materialized) reads for very large datasets
- Joining with external DuckDB data

#### Future: Option B via C++ extension

If demand warrants it, implement a proper DuckDB C++ extension that:
- Registers a replacement scan callback
- Lazily resolves ArcticDB symbols when referenced in SQL
- Supports pushdown via DuckDB's table function filter pushdown API
- Maps libraries to schemas for `ATTACH`-like semantics

This is a significant undertaking but would give the most idiomatic DuckDB experience.

## CLI / Interactive Use

With Steps 1+2, interactive use becomes:

```python
# Quick one-liner (pushdown, streaming, no setup)
lib.sql("SELECT * FROM trades LIMIT 10")

# Full DuckDB experience (materialized, reusable, .show() etc.)
import duckdb
conn = duckdb.connect()
lib.duckdb_register(conn)
conn.sql("SELECT * FROM trades WHERE price > 100 LIMIT 10").show()
```

## Implementation Plan

### Step 1: `lib.sql()` returns DataFrame

Changes to `python/arcticdb/version_store/library.py`:
- `sql()` returns the data directly (DataFrame/Arrow/Polars) instead of VersionedItem
- Add `explain: bool = False` parameter — when True, returns `(data, metadata_dict)` tuple
- Keep all pushdown logic intact

### Step 2: `lib.duckdb_register(conn)`

New method on Library:
- `duckdb_register(conn, symbols=None, as_of=None)` — registers symbols into a DuckDB connection
- If `symbols` is None, registers all symbols from `list_symbols()`
- Reads each symbol as Arrow table and calls `conn.register(name, table)`
- Returns list of registered symbol names

New method on Arctic:
- `duckdb_register(conn, libraries=None)` — registers symbols from multiple libraries
- Prefixes table names with library name to avoid collisions: `lib_name__symbol`

### Step 3: Update documentation

- Update `sql_queries.md` tutorial with new return type
- Add `duckdb_register()` examples
- Update comparison table (sql vs register vs duckdb context)

## Pushdown Considerations

- `lib.sql()`: Full pushdown (column projection, filters, date range, LIMIT) — unchanged
- `lib.duckdb_register(conn)`: No pushdown (data is materialized). Users trade efficiency for reusability.
- `lib.duckdb()`: Per-symbol pushdown via `register_symbol()` parameters — unchanged
- Future C++ extension: Could support DuckDB's native filter pushdown API
