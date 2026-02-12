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
- `_projected_columns` — `set` of column names when column projection is active; filters the descriptor-derived schema to only projected columns
- `to_pyarrow_reader()` — converts to `pyarrow.RecordBatchReader` for DuckDB registration; pads each batch to full schema via `_pad_batch_to_schema()`; strips `__idx__` prefix from MultiIndex column names
- `read_all(strip_idx_prefix=True)` → `pyarrow.Table` — materializes all batches (padded to full schema)
- `schema` → `pyarrow.Schema` — lazily derived from merged descriptor (all columns), refined with first batch's actual Arrow types

### Schema Discovery (including Dynamic Schema)

`_ensure_schema()` builds the authoritative schema from the **merged descriptor** (`_cpp_iterator.descriptor()`), which contains ALL columns across ALL segments from the version key's `TimeseriesDescriptor`. This handles:

| Case | Behaviour |
|------|-----------|
| Empty symbol (0 segments) | Schema from descriptor only |
| Fixed schema (all segments same cols) | Descriptor = first batch schema (no-op padding) |
| Dynamic schema (different cols per segment) | Descriptor has superset; first batch refines types; missing cols padded with nulls |
| Column projection active | Descriptor filtered by `_projected_columns` before use |

**Type refinement**: The descriptor maps `UTF_DYNAMIC64` → `pa.large_string()`, but C++ actually produces `pa.dictionary(pa.int32(), pa.large_string())` for CATEGORICAL strings. `_ensure_schema()` peeks at the first batch and uses its actual Arrow types where available, falling back to descriptor types only for columns absent from the first batch.

### Batch Padding for Dynamic Schema

`_pad_batch_to_schema(batch, target_schema)` ensures every batch matches the full schema:
- Columns present in batch but with different type → `cast()` to target type
- Columns missing from batch → `pa.nulls(num_rows, type=field.type)`
- Columns reordered to match `target_schema` field order
- No-op when batch already matches schema (common case for fixed-schema symbols)

Used in both `to_pyarrow_reader()` (per-batch padding) and `read_all()` (batch list padding).

### Dynamic Schema in SQL Paths

`lib.sql()`, `lib.duckdb()`, and `DuckDBContext.register_symbol()` pass `dynamic_schema=self.options().dynamic_schema` (the **library's actual setting**) to `_read_as_record_batch_reader()`. Passing `dynamic_schema=True` when the library is static-schema disables the C++ column-slice filter, causing all column slices to be returned (even those without projected columns) and producing spurious NULL rows in GROUP BY results.

For dynamic-schema libraries, `ExpressionContext::dynamic_schema_` is set so FilterClause returns `EmptyResult` (instead of crashing) when a WHERE column is missing from a segment.

### Fast Path

`lib.sql()` has a fast path that **bypasses DuckDB entirely** for simple queries. When `fully_pushed=True` (single table, no GROUP BY/ORDER BY/DISTINCT/LIMIT/JOINs/CTEs, all filters pushed) and `columns is None` (SELECT *), it falls back to `lib.read()` which avoids Arrow conversion overhead. This is critical for static-schema performance on wide tables where Arrow conversion dominates.

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
├── prefetch_buffer_        (deque<Future<vector<RecordBatchData>>>, default size 2)
├── row_filter_             (FilterRange: date_range/row_range/none)
├── expression_context_     (FilterClause from WHERE pushdown)
├── descriptor_             (StreamDescriptor for schema discovery)
│
├── next() → optional<RecordBatchData>
│   ├── drain pending_batches_ first (multi-block segments)
│   ├── block on prefetch_buffer_.front().get() — returns prepared batches
│   └── fill_prefetch_buffer() — kick off next reads
│
├── read_decode_and_prepare_segment(idx) → Future<vector<RecordBatchData>>
│   ├── batch_read_uncompressed() — I/O (already parallel)
│   └── .via(&cpu_executor()).thenValue() — **parallel on CPU thread pool**:
│       ├── apply_truncation()
│       ├── apply_filter_clause()
│       ├── prepare_segment_for_arrow()
│       └── segment_to_arrow_data() + RecordBatchData conversion
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

Non-ASV profiling scripts, numbered by usefulness (most → least):

```
python/benchmarks/non_asv/duckdb/
├── 1_bench_sql_vs_querybuilder.py  # Day-to-day: SQL vs QB vs pandas (1M & 10M rows, operations)
├── 2_bench_sql_scaling.py          # Width scaling: 6→100→400 cols, static vs dynamic schema
├── 3_profile_sql_breakdown.py      # Step-by-step: pushdown, iterator creation, DuckDB exec
└── 4_profile_iterator_pipeline.py  # Lowest-level: per-segment C++ timing, streaming vs materialized
```

All scripts are self-contained (generate own data in tempdir). Run with:
```bash
python python/benchmarks/non_asv/duckdb/1_bench_sql_vs_querybuilder.py
```

## Append Handling

The DuckDB path reads data via `LazyRecordBatchIterator`, which iterates over **all segments** of a symbol regardless of how they were created (`write()` vs `append()`). There is no special "append-aware" logic — the segment abstraction makes the distinction transparent to the read path.

### Static Schema + Append

All appended segments have identical columns. Each segment becomes a RecordBatch with the same schema — no padding needed. Covered by `TestAppendStaticSchema` in `test_duckdb.py`:

| Test | What It Verifies |
|------|-----------------|
| `test_append_select_all` | SELECT * returns all rows from write + append |
| `test_append_multiple_appends` | 4 chained segments, COUNT/SUM correct |
| `test_append_date_range_spanning_segments` | WHERE on index crossing the segment boundary |
| `test_append_column_projection` | SELECT specific columns across segments |
| `test_append_aggregation` | GROUP BY + SUM across segments |
| `test_append_filter_on_appended_data` | WHERE matching only the appended segment |
| `test_append_join` | JOIN where one symbol built via append |
| `test_append_as_of_versioning` | `as_of=0` (pre-append) vs `as_of=1` (post-append) |
| `test_append_to_empty_symbol` | Write empty DataFrame, append data, query |
| `test_append_duckdb_context` | DuckDB context manager with appended symbol |

### Dynamic Schema + Append

Appended segments can have different column subsets. `ArcticRecordBatchReader` pads each batch to the full schema (from the merged `TimeseriesDescriptor`), filling missing columns with nulls. Covered by tests in `test_duckdb_dynamic_schema.py`:

| Test | What It Verifies |
|------|-----------------|
| `_write_dynamic_schema_symbol` helper (used by 11 tests) | Segments with cols `{a,b}` then `{b,c}` — null padding |
| `test_sql_group_by_non_column_sliced_dynamic_schema` | GROUP BY with extra columns varying per segment |
| `test_sql_string_columns` | String columns varying across append segments |
| `test_append_type_widening_float` | float32 → float64 type promotion works |
| `test_append_multiple_different_column_sets` | 3 appends with disjoint column sets, null verification per segment |
| `test_append_aggregation_across_sparse_segments` | SUM correctly ignores nulls from sparse columns |

### Known Limitation: int → float Type Widening

When the first segment has an integer column and a later append promotes it to float (e.g. `int64` → `float64`), the DuckDB path fails. `_ensure_schema()` (`arrow_reader.py:337-345`) uses the **first batch's** Arrow type for columns present in that batch, so the schema is set to `int64`. When a later batch arrives with `float64`, `_pad_batch_to_schema()` attempts a lossy downcast (`float64` → `int64`) and raises `ArrowInvalid`. The merged descriptor has the correct promoted type, but the first-batch override takes precedence.

**Workaround**: Use float types from the start, or ensure all segments share the same numeric type.

## Testing

```bash
# All DuckDB tests (~350 tests)
python -m pytest -n 8 python/tests/unit/arcticdb/version_store/duckdb/
```

### Test Structure

| File | Tests | Coverage |
|------|-------|----------|
| `test_pushdown.py` | AST parsing, filter conversion, QueryBuilder generation, end-to-end pushdown | Column, filter, date range, limit pushdown; edge cases for types, OR, LIKE, functions |
| `test_duckdb.py` | Context managers, sql(), external connections, MultiIndex joins, index reconstruction, **static-schema append** | Simple queries, JOINs, MultiIndex schema, output formats, case sensitivity; write+append SELECT/filter/aggregation/JOIN/versioning |
| `test_arrow_reader.py` | RecordBatchReader iteration, exhaustion, DuckDB integration | Streaming, single-use enforcement, schema |
| `test_lazy_streaming.py` | Lazy iterator: basic SQL, groupby, filter, joins, versioning, multi-segment, truncation, FilterClause | Direct iterator, date_range/row_range, empty symbols, DuckDB context |
| `test_doc_examples.py` | Tutorial code examples, as_of with dict/timestamp, explain() | End-to-end validation of documented examples |
| `test_duckdb_dynamic_schema.py` | Dynamic schema: SELECT *, WHERE filter, aggregation, JOIN, DuckDBContext, strings, **append edge cases** | Symbols where segments have different column subsets; null padding, missing-column filters; type widening, multi-append with disjoint columns, sparse aggregation |
| `test_schema_ddl.py` | DESCRIBE, SHOW TABLES, SHOW DATABASES, schema discovery | DDL queries, column metadata, database/library hierarchy |
| `test_arctic_duckdb.py` | Arctic-level SQL: cross-library joins, ArcticDuckDBContext, SHOW DATABASES | Cross-library/cross-instance queries, library registration |

## Related Documentation

- [LIBRARY_API.md](LIBRARY_API.md) — Library class (sql, explain, duckdb methods)
- [ARCTIC_CLASS.md](ARCTIC_CLASS.md) — Arctic class (sql, duckdb methods)
- [QUERY_PROCESSING.md](QUERY_PROCESSING.md) — QueryBuilder used by pushdown
- [../cpp/ARROW.md](../cpp/ARROW.md) — C++ Arrow output frame and lazy iterator
