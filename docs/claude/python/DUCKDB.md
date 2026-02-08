# DuckDB SQL Integration

SQL query engine for ArcticDB using DuckDB, with pushdown optimization and Arrow-based streaming.

## Location

```
python/arcticdb/version_store/duckdb/
├── __init__.py        # Public exports: DuckDBContext, ArcticDuckDBContext, ArcticRecordBatchReader
├── duckdb.py          # Context managers and connection management
├── pushdown.py        # SQL AST parsing and pushdown extraction
└── arrow_reader.py    # Arrow RecordBatchReader wrapper
```

Entry points on `Library` (`version_store/library.py`):
- `sql()` — one-shot query, auto-discovers symbols, pushdown optimization
- `explain()` — pushdown introspection without executing query
- `duckdb()` — context manager for advanced multi-symbol queries

Entry points on `Arctic` (`arctic.py`):
- `sql()` — database discovery (`SHOW DATABASES`)
- `duckdb()` — cross-library context manager

## Architecture

```
User Query
    │
    ▼
lib.sql(query) ──────────────────────────────────► DataFrame
    │                                                  ▲
    ├─ parse SQL AST (pushdown.py)                     │
    │   ├─ extract_pushdown_from_sql()                 │
    │   │   ├─ columns, filters, date_range, limit     │
    │   │   └─ symbol names from FROM/JOIN              │
    │   └─ returns PushdownInfo per table               │
    │                                                   │
    ├─ read with pushdown applied                       │
    │   └─ _read_as_record_batch_reader()               │
    │       └─ C++ RecordBatchIterator (Arrow streaming) │
    │                                                   │
    └─ DuckDB in-memory connection                      │
        ├─ conn.register(symbol, arrow_reader)          │
        ├─ conn.execute(query).arrow()  ────────────────┘
        └─ conn.close()
```

## API Summary

| Method | Returns | Pushdown | Streaming | Multi-query | Use Case |
|--------|---------|----------|-----------|-------------|----------|
| `lib.sql(query)` | DataFrame | Yes | Yes | No | Simple queries, CLI |
| `lib.explain(query)` | dict | N/A | No I/O | N/A | Inspect optimizations |
| `lib.duckdb()` | Context manager | Per-symbol | Yes | Yes | Advanced: JOINs, aliases, versions |

## Module: duckdb.py

### Class Hierarchy

```
_BaseDuckDBContext
├── Connection lifecycle (__enter__/__exit__)
├── _validate_external_connection() (static)
├── _convert_arrow_table(arrow_table, output_format) (static)
├── _execute_sql(query, output_format)
├── execute(sql) → self
├── Properties: connection, registered_symbols
│
├── DuckDBContext — single library
│   ├── register_symbol(symbol, alias, as_of, date_range, row_range, columns, query_builder)
│   ├── register_all_symbols(as_of)
│   ├── _auto_register(query) — resolves unregistered symbols from library
│   └── sql(query, output_format)
│
└── ArcticDuckDBContext — cross-library
    ├── register_library(library_name)
    ├── register_all_libraries()
    ├── register_symbol(library_name, symbol, ...)
    ├── sql(query, output_format) — handles SHOW DATABASES
    └── _execute_show_databases(output_format)
```

### Connection Ownership

- **Internal** (default): `duckdb.connect(":memory:")`, closed on `__exit__`
- **External** (user-provided): validated with `SELECT 1`, NOT closed on `__exit__`
- Tracked via `_owns_connection` flag

### Helper Functions

- `_check_duckdb_available()` — import guard, raises `ImportError` with install instructions
- `_parse_library_name(name)` — splits `"db.lib"` → `("db", "lib")`, top-level → `("__default__", name)`
- `_extract_symbols_from_query(query)` — delegates to `extract_pushdown_from_sql()`
- `_resolve_symbol(sql_name, library)` — O(1) exact match via `has_symbol()`, case-insensitive fallback via `list_symbols()`

## Module: pushdown.py

SQL-to-ArcticDB pushdown optimization via DuckDB's `json_serialize_sql()` AST.

### Key Function

`extract_pushdown_from_sql(query, symbols=None)` → `(dict[str, PushdownInfo], list[str])`

Parses SQL into AST once, extracts per-table:

| Pushdown | AST Source | ArcticDB Parameter |
|----------|-----------|-------------------|
| Column projection | SELECT clause columns | `columns=` |
| WHERE filters | `where_clause` node | `query_builder=` |
| Date range | Index comparisons in WHERE | `date_range=` |
| LIMIT | `limit.limit_val` node | Internal limit |

### PushdownInfo Dataclass

```python
@dataclass
class PushdownInfo:
    columns: Optional[List[str]] = None
    query_builder: Optional[QueryBuilder] = None
    date_range: Optional[Tuple] = None
    limit: Optional[int] = None
    columns_pushed_down: Optional[List[str]] = None  # tracking
    filter_pushed_down: bool = False                  # tracking
    date_range_pushed_down: bool = False              # tracking
    limit_pushed_down: Optional[int] = None           # tracking
```

### Pushdown Rules

- **Columns**: Pushed for single-table queries. Disabled for JOINs (columns may be needed for join conditions).
- **Filters**: Comparison ops (`=`, `!=`, `<`, `>`, `<=`, `>=`), `IN`, `NOT IN`, `IS NULL`, `IS NOT NULL`, `BETWEEN`. OR conditions and functions NOT pushed down.
- **Date range**: Filters on `index` column converted to `date_range` tuple. Requires timestamp comparisons.
- **LIMIT**: Pushed only for single-table, non-aggregation queries.

### Exception Handling

Pushdown failures are non-fatal — logged as warnings, query falls through to DuckDB:
- Specific exceptions (`ValueError`, `KeyError`, `TypeError`, `IndexError`) caught in filter/date/limit extraction
- Broad `except Exception` only in `_get_sql_ast()` (DuckDB can throw anything during parsing)

## Module: arrow_reader.py

`ArcticRecordBatchReader` wraps the C++ `RecordBatchIterator` for Python/DuckDB consumption.

### Key Properties

- `_iteration_started` / `_exhausted` — guards against multiple iteration or re-iteration
- `to_pyarrow_reader()` — converts to `pyarrow.RecordBatchReader` for DuckDB registration
- `read_all()` → `pyarrow.Table` — materializes all batches
- `schema` → `pyarrow.Schema` — from first batch

### Single-Use Constraint

Arrow RecordBatchReaders are **single-use**. After iteration, data is consumed. This is why:
- `lib.sql()` creates a fresh reader per query
- C++ side enforces with `data_consumed_` flag in `ArrowOutputFrame`

## C++ Layer

### Arrow Output Frame (`cpp/arcticdb/arrow/`)

| File | Key Classes |
|------|------------|
| `arrow_output_frame.hpp` | `RecordBatchData`, `RecordBatchIterator`, `ArrowOutputFrame` |
| `arrow_output_frame.cpp` | Implementation of iterator and frame extraction |

### RecordBatchData

Holds one Arrow record batch via `ArrowArray` + `ArrowSchema` (Arrow C Data Interface). Zero-initialized with `std::memset`. Release callbacks clean up memory.

### RecordBatchIterator

C++ iterator over `vector<RecordBatchData>`. Destructively extracts `sparrow::record_batch` data via `extract_struct_array()`. Exposed to Python via pybind11.

### ArrowOutputFrame

Container for query results as Arrow batches. Enforces single consumption via `data_consumed_` flag — calling `create_iterator()` or `extract_record_batches()` twice raises an error.

### Python Bindings

`cpp/arcticdb/version/python_bindings.cpp` — binds `read_as_record_batch_iterator()` which returns a `RecordBatchIterator` to Python.

`cpp/arcticdb/version/python_bindings_common.cpp` — binds `RecordBatchIterator` class with `__next__`, `schema`, `num_batches` properties.

## Testing

```bash
# All DuckDB tests (201 tests)
python -m pytest python/tests/unit/arcticdb/version_store/duckdb/ -v

# Just pushdown tests
python -m pytest python/tests/unit/arcticdb/version_store/duckdb/test_pushdown.py

# Just integration tests
python -m pytest python/tests/unit/arcticdb/version_store/duckdb/test_duckdb.py

# Just Arrow reader tests
python -m pytest python/tests/unit/arcticdb/version_store/duckdb/test_arrow_reader.py
```

### Test Structure

| File | Tests | Coverage |
|------|-------|----------|
| `test_pushdown.py` | AST parsing, filter conversion, QueryBuilder generation, end-to-end pushdown | Column, filter, date range, limit pushdown; edge cases for types, OR, LIKE, functions |
| `test_duckdb.py` | Context managers, sql(), external connections, schema DDL, SHOW DATABASES | Simple queries, JOINs, output formats, edge cases, cross-library |
| `test_arrow_reader.py` | RecordBatchReader iteration, exhaustion, DuckDB integration | Streaming, single-use enforcement, schema |

## Related Documentation

- [LIBRARY_API.md](LIBRARY_API.md) — Library class (sql, explain, duckdb methods)
- [ARCTIC_CLASS.md](ARCTIC_CLASS.md) — Arctic class (sql, duckdb methods)
- [QUERY_PROCESSING.md](QUERY_PROCESSING.md) — QueryBuilder used by pushdown
- [../cpp/PYTHON_BINDINGS.md](../cpp/PYTHON_BINDINGS.md) — C++ bindings for RecordBatchIterator
