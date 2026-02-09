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

## MultiIndex Schema in SQL

ArcticDB stores pandas `MultiIndex` levels as columns with an `__idx__` prefix (in `_normalization.py`).
The SQL interface **strips this prefix transparently** so users write original index names.

| Storage Column | SQL Column | Source |
|---------------|-----------|--------|
| `date` (level 0) | `date` | Unchanged — first index level has no prefix |
| `__idx__security_id` (level 1+) | `security_id` | `__idx__` stripped in `arrow_reader.py:to_pyarrow_reader()` |
| `momentum` (data column) | `momentum` | Unchanged |

### Implementation

- **Strip**: `arrow_reader.py:_strip_idx_prefix_from_names()` renames schema fields, `to_pyarrow_reader()` yields renamed batches
- **Reverse-map for pushdown**: `library.py:sql()` and `duckdb.py:register_symbol()` expand column names to include both clean and `__idx__`-prefixed variants so the C++ `build_column_bitset` matches whichever form is in storage
- **Filter pushdown**: C++ `column_index_with_name_demangling()` already tries `__idx__ + name` as fallback, so `QueryBuilder` filters with clean names work without additional mapping
- **Collision safety**: `_strip_idx_prefix_from_names()` appends underscores if stripping would create duplicates (mirroring `_normalization.py` denormalization)

### Join Patterns

**Two MultiIndex symbols** — join on all index columns using clean names:
```sql
SELECT m.date, m.security_id, m.momentum, i.inflow
FROM momentum m
JOIN inflow i ON m.date = i.date AND m.security_id = i.security_id
```

**MultiIndex + single-index** — join on shared index (broadcasts single-index values):
```sql
SELECT m.date, m.security_id, m.momentum, a.analyst_mom
FROM momentum m
JOIN analyst a ON m.date = a.date
```

### Index Reconstruction

For **pandas** output (`output_format=None` or `OutputFormat.PANDAS`), the SQL result reconstructs the original index using `set_index()`. When multiple symbols are involved (JOINs), the **most specific** matching index (most levels) is chosen.

| Condition | Behaviour |
|-----------|-----------|
| All index columns in result | `set_index(index_cols)` — reconstructs original index |
| JOIN with index columns in result | Reconstructs the most specific matching index across all symbols |
| Partial index columns | No reconstruction — flat DataFrame with RangeIndex |
| Aggregation dropping index columns | No reconstruction |
| RangeIndex symbol | No reconstruction — nothing to restore |
| Arrow/Polars output | No reconstruction — only applies to pandas |

**Implementation**: `Library._get_index_columns_for_symbol()` calls `get_description()` (~4ms) to retrieve index metadata. `Library.sql()` and `DuckDBContext.sql()` iterate all symbols in the query, find which ones have all their index columns present in the result, and pick the one with the most levels.

### Test Coverage

`test_duckdb.py::TestMultiIndexJoins` — 8 tests:

| Test | Pattern |
|------|---------|
| `test_inner_join_two_multiindex_symbols` | INNER JOIN on `(date, security_id)`, non-matching sids excluded |
| `test_left_join_two_multiindex_symbols` | LEFT JOIN preserving unmatched rows with NULLs |
| `test_join_multiindex_with_single_index` | MultiIndex ⋈ single DatetimeIndex (broadcast join) |
| `test_multiindex_join_with_aggregation` | JOIN + GROUP BY date with AVG/SUM |
| `test_multiindex_join_with_date_filter` | JOIN + WHERE date filter |
| `test_select_star_shows_clean_column_names` | SELECT * reconstructs MultiIndex, no `__idx__` prefix |
| `test_describe_shows_clean_column_names` | DESCRIBE shows clean names |
| `test_multiindex_filter_on_index_column` | WHERE filter on MultiIndex level, index reconstructed |

`test_duckdb.py::TestIndexReconstruction` — 9 tests:

| Test | Pattern |
|------|---------|
| `test_multiindex_roundtrip_via_sql` | MultiIndex round-trips through `lib.sql()` with `pd.testing.assert_frame_equal` |
| `test_single_named_index_roundtrip_via_sql` | Single DatetimeIndex round-trips through `lib.sql()` |
| `test_rangeindex_stays_flat` | RangeIndex preserved as-is |
| `test_aggregation_drops_index` | Aggregation → no index reconstruction |
| `test_partial_index_columns_no_reconstruction` | Partial index columns → flat DataFrame |
| `test_join_reconstructs_best_index` | JOINs → most specific matching index |
| `test_arrow_output_no_reconstruction` | Arrow output → no reconstruction |
| `test_duckdb_context_single_symbol_reconstruction` | `DuckDBContext.sql()` single-symbol reconstruction |
| `test_duckdb_context_multi_symbol_reconstruction` | `DuckDBContext.sql()` multi-symbol → best index |

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
| `test_duckdb.py` | Context managers, sql(), external connections, MultiIndex joins | Simple queries, JOINs, MultiIndex schema, output formats, edge cases |
| `test_arrow_reader.py` | RecordBatchReader iteration, exhaustion, DuckDB integration | Streaming, single-use enforcement, schema |

## Related Documentation

- [LIBRARY_API.md](LIBRARY_API.md) — Library class (sql, explain, duckdb methods)
- [ARCTIC_CLASS.md](ARCTIC_CLASS.md) — Arctic class (sql, duckdb methods)
- [QUERY_PROCESSING.md](QUERY_PROCESSING.md) — QueryBuilder used by pushdown
- [../cpp/PYTHON_BINDINGS.md](../cpp/PYTHON_BINDINGS.md) — C++ bindings for RecordBatchIterator
