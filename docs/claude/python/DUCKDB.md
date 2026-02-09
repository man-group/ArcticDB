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
    ├─ create lazy iterator per symbol                  │
    │   └─ C++ LazyRecordBatchIterator                  │
    │       ├─ reads+decodes segments on-demand         │
    │       ├─ applies truncation (date_range/row_range)│
    │       ├─ applies FilterClause (WHERE pushdown)    │
    │       └─ prepare_segment_for_arrow() per segment  │
    │                                                   │
    ├─ Python ArcticRecordBatchReader                   │
    │   └─ to_pyarrow_reader() → pa.RecordBatchReader   │
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

### Index Reconstruction

For **pandas** output, the SQL result reconstructs the original index using `set_index()`. When multiple symbols are involved (JOINs), the **most specific** matching index (most levels) is chosen.

| Condition | Behaviour |
|-----------|-----------|
| All index columns in result | `set_index(index_cols)` — reconstructs original index |
| JOIN with index columns in result | Reconstructs the most specific matching index across all symbols |
| Partial index columns | No reconstruction — flat DataFrame with RangeIndex |
| Aggregation dropping index columns | No reconstruction |
| RangeIndex symbol | No reconstruction — nothing to restore |
| Arrow/Polars output | No reconstruction — only applies to pandas |

**Implementation**: `Library._get_index_columns_for_symbol()` calls `get_description()` (~4ms) to retrieve index metadata. `Library.sql()` and `DuckDBContext.sql()` iterate all symbols in the query, find which ones have all their index columns present in the result, and pick the one with the most levels.

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
- **LIMIT**: Pushed only for single-table, non-aggregation queries without ORDER BY, GROUP BY, DISTINCT, or WHERE.

### Exception Handling

Pushdown failures are non-fatal — logged as warnings, query falls through to DuckDB:
- Specific exceptions (`ValueError`, `KeyError`, `TypeError`, `IndexError`) caught in filter/date/limit extraction
- Broad `except Exception` only in `_get_sql_ast()` (DuckDB can throw anything during parsing)

## Module: arrow_reader.py

`ArcticRecordBatchReader` wraps the C++ `LazyRecordBatchIterator` for Python/DuckDB consumption.

### Key Properties

- `_iteration_started` / `_exhausted` — guards against multiple iteration or re-iteration
- `to_pyarrow_reader()` — converts to `pyarrow.RecordBatchReader` for DuckDB registration; strips `__idx__` prefix from MultiIndex column names
- `read_all(strip_idx_prefix=True)` → `pyarrow.Table` — materializes all batches
- `schema` → `pyarrow.Schema` — lazily extracted from first batch, or from `descriptor()` for empty symbols

### Schema Discovery for Empty Symbols

When a symbol has no data segments, `_ensure_schema()` uses `_descriptor_to_arrow_schema()` to build a PyArrow schema from the C++ `StreamDescriptor` (available from the index-only read). This maps `arcticdb_ext.types.DataType` enums to PyArrow types.

### Single-Use Constraint

Arrow RecordBatchReaders are **single-use**. After iteration, data is consumed. This is why:
- `lib.sql()` creates a fresh reader per query
- `ArcticRecordBatchReader` tracks `_iteration_started` and `_exhausted` flags

## C++ Layer: Lazy Streaming

### LazyRecordBatchIterator (`cpp/arcticdb/arrow/arrow_output_frame.hpp/cpp`)

On-demand segment reader that streams Arrow record batches from storage. This is the **only** iterator used by the SQL/DuckDB path (the eager `RecordBatchIterator` was removed).

```
LazyRecordBatchIterator
├── slice_and_keys_         (segment metadata from index-only read)
├── store_                  (StreamSource for storage I/O)
├── prefetch_buffer_        (deque<Future<SegmentAndSlice>>, default size 2)
├── row_filter_             (FilterRange: date_range/row_range/none)
├── expression_context_     (FilterClause from WHERE pushdown)
├── descriptor_             (StreamDescriptor for schema discovery)
│
├── next() → optional<RecordBatchData>
│   ├── apply_truncation()       — date_range/row_range row-level filtering
│   ├── apply_filter_clause()    — WHERE pushdown via ProcessingUnit
│   ├── prepare_segment_for_arrow() — convert to Arrow-compatible format
│   └── segment_to_arrow_data()  — produce sparrow::record_batch
│
├── has_next(), num_batches(), current_index()
├── descriptor() → StreamDescriptor (for empty symbol schema)
└── field_count() → size_t
```

### prepare_segment_for_arrow() (anonymous namespace)

Converts decoded segments for Arrow consumption. This is the **dominant cost** in the SQL pipeline:

- **Non-string columns**: `make_column_blocks_detachable()` — allocates detachable memory via `std::allocator` and memcpys block data (required for Sparrow ownership transfer via `block.release()`)
- **String columns (CATEGORICAL)**: `encode_dictionary_with_shared_dict()` — uses a `SharedStringDictionary` built once per segment from the string pool, then read-only hash map lookups per row
- **String columns (LARGE/SMALL_STRING)**: falls back to `ArrowStringHandler::convert_type()`

### SharedStringDictionary

Built once per segment from the string pool, shared across all string columns:

```
SharedStringDictionary
├── offset_to_index   (pool_offset → sequential dict index)
├── dict_offsets      (Arrow cumulative byte offsets)
├── dict_strings      (concatenated UTF-8 data)
└── unique_count
```

`build_shared_dictionary()` walks the pool buffer sequentially using `[uint32_t size][char data]` entry layout. O(U) where U = unique strings, typically much smaller than row count.

### RecordBatchData

Holds one Arrow record batch via `ArrowArray` + `ArrowSchema` (Arrow C Data Interface). Zero-initialized with `std::memset`. Used by both the lazy iterator and `ArrowOutputFrame::extract_record_batches()`.

### ArrowOutputFrame

Container for `lib.read(output_format='pyarrow')` results. Holds `vector<sparrow::record_batch>`. **Not used by the SQL/DuckDB path** (which uses `LazyRecordBatchIterator` directly). Enforces single consumption via `data_consumed_` flag.

### Python Bindings

`cpp/arcticdb/version/python_bindings.cpp`:
- `read_as_lazy_record_batch_iterator()` — creates `LazyRecordBatchIterator` with pushdown params
- `LazyRecordBatchIterator` bindings: `next()` (GIL-released), `has_next()`, `num_batches()`, `current_index()`, `descriptor()`, `field_count()`

`cpp/arcticdb/version/python_bindings_common.cpp`:
- `ArrowOutputFrame`: `extract_record_batches()`, `num_blocks()`

## Performance Characteristics

### Bottleneck: prepare_segment_for_arrow()

The Arrow conversion in `prepare_segment_for_arrow()` dominates SQL query time. For 10M rows (100 segments):

| Data Type | C++ Iterator | lib.sql() | lib.read() (pandas) | SQL/read Ratio |
|-----------|-------------|-----------|---------------------|---------------|
| Numeric-only (6 cols) | 5.0s | 5.7s | 0.09s | 63x |
| String-heavy (3 str + 6 num) | 43s | 47s | 10s | 5x |

The cost is inherent: the pandas path returns numpy arrays referencing decoded buffer memory (zero-copy), while Arrow requires `allocate_detachable_memory()` + `memcpy` per block so Sparrow can own/free the memory.

### Where SQL Wins

| Query Pattern | SQL vs QueryBuilder | Why |
|--------------|-------------------|-----|
| GROUP BY (low cardinality, 10M rows) | **SQL 0.6x faster** | DuckDB's columnar aggregation engine |
| Filter + GROUP BY | ~2x slower | Competitive after pushdown |
| Full scan (SELECT *) | 3-60x slower | Arrow conversion overhead dominates |
| Memory (SELECT *, 10M rows) | **3x less** (337 vs 1033 MB) | Streaming avoids full materialization |

### Profiling Scripts

Non-ASV profiling scripts for investigating performance:

```
python/benchmarks/non_asv/duckdb/
├── bench_sql_vs_qb.py          # Head-to-head SQL vs QueryBuilder (1M & 10M rows)
├── bench_sql_numeric_only.py   # Numeric-only variant isolating string overhead
├── profile_lazy_iterator.py    # Time breakdown: C++ iterator vs DuckDB vs lib.read
├── profile_per_step.py         # Per-segment + per-step profiling (numeric & string)
└── profile_warm_cache.py       # Warm-cache profiling for true CPU cost
```

All scripts are self-contained (generate own data in tempdir). Run with:
```bash
python python/benchmarks/non_asv/duckdb/bench_sql_vs_qb.py
```

## Testing

```bash
# All DuckDB tests (~307 tests)
python -m pytest -n 8 python/tests/unit/arcticdb/version_store/duckdb/

# Specific test files
python -m pytest python/tests/unit/arcticdb/version_store/duckdb/test_pushdown.py
python -m pytest python/tests/unit/arcticdb/version_store/duckdb/test_duckdb.py
python -m pytest python/tests/unit/arcticdb/version_store/duckdb/test_arrow_reader.py
python -m pytest python/tests/unit/arcticdb/version_store/duckdb/test_lazy_streaming.py
python -m pytest python/tests/unit/arcticdb/version_store/duckdb/test_doc_examples.py
```

### Test Structure

| File | Tests | Coverage |
|------|-------|----------|
| `test_pushdown.py` | AST parsing, filter conversion, QueryBuilder generation, end-to-end pushdown | Column, filter, date range, limit pushdown; edge cases for types, OR, LIKE, functions |
| `test_duckdb.py` | Context managers, sql(), external connections, MultiIndex joins, index reconstruction | Simple queries, JOINs, MultiIndex schema, output formats, case sensitivity |
| `test_arrow_reader.py` | RecordBatchReader iteration, exhaustion, DuckDB integration | Streaming, single-use enforcement, schema |
| `test_lazy_streaming.py` | Lazy iterator: basic SQL, groupby, filter, joins, versioning, multi-segment, truncation, FilterClause | Direct iterator, date_range/row_range, empty symbols, DuckDB context |
| `test_doc_examples.py` | Tutorial code examples, as_of with dict/timestamp, explain() | End-to-end validation of documented examples |

## Related Documentation

- [LIBRARY_API.md](LIBRARY_API.md) — Library class (sql, explain, duckdb methods)
- [ARCTIC_CLASS.md](ARCTIC_CLASS.md) — Arctic class (sql, duckdb methods)
- [QUERY_PROCESSING.md](QUERY_PROCESSING.md) — QueryBuilder used by pushdown
- [../cpp/ARROW.md](../cpp/ARROW.md) — C++ Arrow output frame and lazy iterator
