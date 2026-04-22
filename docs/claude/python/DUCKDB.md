# DuckDB SQL Integration

SQL query engine for ArcticDB using DuckDB, with pushdown optimization and Arrow-based streaming.

## Location

```
python/arcticdb/version_store/duckdb/
├── __init__.py        # Public exports (__all__): DuckDBContext, ArcticDuckDBContext
├── duckdb.py          # Context managers and connection management
├── pushdown.py        # SQL AST parsing and pushdown extraction
├── arrow_reader.py    # Arrow RecordBatchReader wrapper
└── index_utils.py     # Index column resolution, as_of helpers, index reconstruction
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
│   ├── _auto_register(query) — resolves unregistered symbols from library (checks _registered_symbols + has_symbol() guard)
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

### Helper Functions (duckdb.py)

- `_check_duckdb_available()` — import guard, raises `ImportError` with install instructions
- `_parse_library_name(name)` — splits `"db.lib"` → `("db", "lib")`, top-level → `("__default__", name)`. Handles both dotted and plain names.
- `_resolve_symbol(sql_name, library)` — O(1) exact match via `has_symbol()`, case-insensitive fallback via `list_symbols()`
- `_extract_symbols_from_query(query)` — delegates to `extract_pushdown_from_sql()` to extract table names from SQL AST

### Helper Functions (index_utils.py)

- `_resolve_symbol_as_of(as_of, real_symbol, sql_name)` — resolves per-symbol as_of from dict or scalar. Used by `Library.sql()`, `DuckDBContext.sql()`, `resolve_index_columns_for_sql()`
- `reconstruct_pandas_index(result, symbol_versions, library)` — shared index reconstruction for pandas output. Used by `Library.sql()` and `DuckDBContext.sql()`
- `get_index_columns_for_symbol(library, symbol, as_of)` — returns index column names via `get_description()`
- `get_datetime_index_columns_for_symbol(library, symbol, as_of)` — like above but only datetime index columns (for date_range pushdown)
- `resolve_index_columns_for_sql(library, sql_ast, as_of)` — resolves datetime index columns for all symbols in a SQL AST

### Class Properties

`ArcticDuckDBContext` also exposes:
- `registered_libraries` — property returning set of registered library names

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
- **Reverse-map for pushdown**: `library.py:_read_as_record_batch_reader()` expands column names to include both clean and `__idx__`-prefixed variants so the C++ `build_column_bitset` matches whichever form is in storage. All callers (`sql()`, `register_symbol()`) benefit automatically.
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

**Implementation**: `duckdb.index_utils.reconstruct_pandas_index()` is the shared helper used by both `Library.sql()` and `DuckDBContext.sql()`. It calls `get_index_columns_for_symbol()` → `get_description()` (~4ms/symbol) to retrieve index metadata, finds which symbols have all their index columns present in the result, and picks the one with the most levels.

## Module: pushdown.py

SQL-to-ArcticDB pushdown optimization via DuckDB's `json_serialize_sql()` AST.

### Key Functions

`extract_pushdown_from_sql(query, table_names=None, index_columns=None)` → `(dict[str, PushdownInfo], list[str])`

Parses SQL into AST via `_get_sql_ast_or_raise()`, extracts per-table:
- `table_names` — optional pre-resolved table names (avoids redundant AST extraction)
- `index_columns` — optional datetime index column names for date_range pushdown

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
    limit: Optional[int] = None
    date_range: Optional[Tuple[Any, Any]] = None

    # Tracking what was pushed down
    filter_pushed_down: bool = False
    columns_pushed_down: Optional[List[str]] = None
    limit_pushed_down: Optional[int] = None
    date_range_pushed_down: bool = False

    # Filters that couldn't be pushed (will be applied by DuckDB)
    unpushed_filters: List[str] = field(default_factory=list)

    # True when ArcticDB can handle the entire query natively (single table,
    # no GROUP BY/ORDER BY/DISTINCT/JOINs/CTEs/LIMIT with ordering)
    fully_pushed: bool = False

    # SELECT-list-only columns (excludes WHERE-only columns)
    select_columns: Optional[List[str]] = None
```

### Pushdown Rules

- **Columns**: Pushed for single-table queries. Disabled for JOINs (columns may be needed for join conditions) and CTEs.
- **Filters**: Comparison ops (`=`, `!=`, `<`, `>`, `<=`, `>=`), `IN`, `NOT IN`, `BETWEEN`. OR conditions and functions NOT pushed down. `IS NULL` / `IS NOT NULL` parsed but NOT pushed to C++ QueryBuilder (NaN semantics differ: C++ treats NaN as null, DuckDB treats NaN as valid float) — tracked in `unpushed_filters`.
- **Date range**: Filters on datetime index column converted to `date_range` tuple. Requires `index_columns` parameter to identify which column is the index. ISO date strings (e.g. `'2024-01-03'`) auto-converted to timestamps via `_ISO_DATE_RE` pattern matching + `pd.Timestamp()`.
- **LIMIT**: Pushed only for single-table, non-aggregation queries without ORDER BY, GROUP BY, DISTINCT, WHERE, or CTEs.
- **CTEs**: Queries with `WITH` clauses disable all pushdown (columns/filters/LIMIT). CTE names extracted by `_extract_cte_names()` and excluded from symbol list.

### Key Constants

- `_IDX_PREFIX = "__idx__"` — MultiIndex level column prefix in storage
- `_ONLY_SELECT_ERROR` — Error substring from DuckDB for non-SELECT statements
- `_ISO_DATE_RE` — Regex for auto-converting ISO date strings to `pd.Timestamp` in WHERE clauses
- `_TIMESTAMP_TYPES`, `_INTEGER_TYPES`, `_FLOAT_TYPES` — Type constant sets for CAST node handling

### Query Validation

`_get_sql_ast_or_raise(query)` uses DuckDB's `json_serialize_sql()` which only accepts SELECT-like statements. Non-SELECT statements (INSERT, UPDATE, DELETE, CREATE) produce a `ValueError` with a clear "read-only" message.

### Discovery Functions

- `is_table_discovery_query(query, _ast=None)` — detects `SHOW TABLES` / `SHOW ALL TABLES` via AST `SHOW_REF` node. In `Library.sql()`, triggers schema-only registration via `_description_to_arrow_schema()` (no data read)
- `is_database_discovery_query(query)` — detects `SHOW DATABASES` via AST `SHOW_REF` node

### Exception Handling

Pushdown failures are non-fatal — logged as warnings, query falls through to DuckDB:
- Specific exceptions (`ValueError`, `KeyError`, `TypeError`, `IndexError`) caught in filter/date/limit extraction
- Broad `except Exception` only in `_get_sql_ast()` (DuckDB can throw anything during parsing)

## Module: arrow_reader.py

`ArcticRecordBatchReader` wraps the C++ `LazyRecordBatchIterator` for Python/DuckDB consumption. Column-slice merging and schema padding are handled in C++ by the `LazyRecordBatchIterator`, so each batch arrives with the full column set.

### Key Functions

- `_descriptor_to_arrow_schema(descriptor, projected_columns)` — Converts C++ `StreamDescriptor` to `pyarrow.Schema`. Maps ArcticDB DataType → Arrow types. Uses `_IDX_PREFIX = "__idx__"` for MultiIndex column name handling.

### Key Properties

- `_iteration_started` / `_exhausted` — guards against multiple iteration or re-iteration
- `_projected_columns` — `set` of column names when column projection is active; filters the descriptor-derived schema to only projected columns
- `_first_batch` / `_first_batch_returned` — caches the first batch from the C++ iterator for schema refinement; `_first_batch_returned` ensures it's yielded exactly once during iteration
- `to_pyarrow_reader()` — converts to `pyarrow.RecordBatchReader` for DuckDB registration; uses a generator (avoids PyArrow's double `__iter__` call); aligns each batch to the schema (select/reorder/cast/null-pad columns); strips `__idx__` prefix from MultiIndex column names
- `read_all(strip_idx_prefix=True)` → `pyarrow.Table` — materializes all batches
- `schema` → `pyarrow.Schema` — lazily derived from merged descriptor (all columns), refined with first batch's actual Arrow types
- `__len__()` → `int` — returns total number of batches via `_cpp_iterator.num_batches()`

### Schema Discovery

`_ensure_schema()` builds the authoritative schema from the **merged descriptor** (`_cpp_iterator.descriptor()`) and the first batch's actual Arrow types. The descriptor contains ALL column names across ALL segments; the first batch provides actual Arrow types (e.g. dictionary-encoded strings). For columns wider in the descriptor than the first batch (type widening across segments), the descriptor type is preferred via `_is_wider_numeric_type()`.

| Case | Behaviour |
|------|-----------|
| Empty symbol (0 segments) | Schema from descriptor only |
| Fixed schema (all segments same cols) | Descriptor = first batch schema |
| Dynamic schema (different cols per segment) | Descriptor has superset; first batch refines types; C++ pads missing cols |
| Column projection active | Descriptor filtered by `_projected_columns` before use |
| Type widening (e.g. int64 → float64) | Descriptor's wider type used instead of first batch's narrower type |

### Batch Alignment in to_pyarrow_reader()

`to_pyarrow_reader()` aligns each batch to the storage schema before yielding to DuckDB. C++ handles column-slice merging and dynamic-schema padding, but with column projection the batch may have extra or differently-ordered columns compared to the projected schema. The alignment step selects, reorders, casts types, and null-pads to match the target schema exactly. This is a no-op for the common case where the batch already matches the schema.

### Fast Path

`lib.sql()` delegates to `_try_sql_fast_path()` which **bypasses DuckDB entirely** for simple queries. When `fully_pushed=True` (single table, no GROUP BY/ORDER BY/DISTINCT/LIMIT/JOINs/CTEs, all filters pushed) and `columns is None` (SELECT *), it falls back to `lib.read()` which avoids Arrow conversion overhead. This is critical for static-schema performance on wide tables where Arrow conversion dominates.

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

**IMPORTANT: All benchmarks use release builds (`ARCTIC_CMAKE_PRESET=linux-release`). Debug builds are 100-400x slower for Arrow conversion.**

### lib.sql() vs lib.read() (release build, LMDB)

**1M rows × 10 cols (LazyReadThroughput ASV suite):**

| Read Method | Numeric | String | Mixed |
|---|---|---|---|
| `lib.read()` (pandas) | 11.2ms | 67.0ms | 28.9ms |
| `lib.read(output_format='pyarrow')` | 11.7ms | 84.2ms | 48.0ms |
| `lib.sql("SELECT * FROM sym")` | 70.2ms | 127ms | 92.5ms |

**SQL query benchmarks (SQLQueries ASV suite, 1M / 10M rows):**

| Query | 1M rows | 10M rows |
|---|---|---|
| SELECT * (pandas result) | 368ms | 4.22s |
| SELECT * (Arrow result) | 84.9ms | 324ms |
| SELECT columns | 94.6ms | 418ms |
| Filter numeric | 63.3ms | 83.8ms |
| Filter string equality | 72.3ms | 119ms |
| Filter + GROUP BY | 96.8ms | 371ms |
| GROUP BY high cardinality | 72.5ms | 97.4ms |
| GROUP BY sum | 176ms | 994ms |
| GROUP BY multi-agg | 203ms | 1.05s |
| JOIN | 367ms | 2.49s |
| LIMIT | 65.1ms | 67.7ms |

**Key: `time_select_all_arrow` (84.9ms) is 4.3x faster than `time_select_all` (368ms)** — the Arrow output path avoids DuckDB→pandas DataFrame conversion.

### Where SQL Wins

| Query Pattern | SQL vs QueryBuilder | Why |
|--------------|-------------------|-----|
| GROUP BY (low cardinality, 10M rows) | **SQL 0.6x faster** | DuckDB's columnar aggregation engine |
| Filter + GROUP BY | ~2x slower | Competitive after pushdown |
| Full scan (SELECT *) | 6x slower (pandas), 1.5x (Arrow) | DuckDB overhead; Arrow avoids DataFrame conversion |
| Memory (SELECT *, 10M rows) | **3x less** (337 vs 1033 MB) | Streaming avoids full materialization |
| LIMIT queries | ~65ms regardless of data size | Early termination via row_range pushdown |

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

Appended segments can have different column subsets. C++ `LazyRecordBatchIterator` pads each batch to the full schema (from the merged `TimeseriesDescriptor`), filling missing columns with nulls. Covered by tests in `test_duckdb_dynamic_schema.py`:

| Test | What It Verifies |
|------|-----------------|
| `_write_dynamic_schema_symbol` helper (used by 11 tests) | Segments with cols `{a,b}` then `{b,c}` — null padding |
| `test_sql_group_by_non_column_sliced_dynamic_schema` | GROUP BY with extra columns varying per segment |
| `test_sql_string_columns` | String columns varying across append segments |
| `test_append_type_widening_float` | float32 → float64 type promotion works |
| `test_append_multiple_different_column_sets` | 3 appends with disjoint column sets, null verification per segment |
| `test_append_aggregation_across_sparse_segments` | SUM correctly ignores nulls from sparse columns |

### Type Widening Across Segments

When the first segment has an integer column and a later append promotes it to float (e.g. `int64` → `float64`), the merged descriptor contains the widened type. `_ensure_schema()` detects this via `_is_wider_numeric_type()` which uses `_NUMERIC_TYPE_RANK` (a dict mapping Arrow types to rank integers, e.g. `pa.int8()→0`, `pa.int64()→3`, `pa.float64()→5`) and uses the descriptor's wider type instead of the first batch's narrower type. This ensures consistent schema across all batches.

### Column-Slice-Aware Filter Pushdown

`FilterClause` pushdown is always sent to C++ (`library.py` always passes `qb = pushdown.query_builder`). The C++ `LazyRecordBatchIterator` detects column slicing at construction (`has_column_slicing_` bool from scanning `slice_and_keys_`) and decides:

- **Row-sliced only** (`has_column_slicing_=false`): filter applied per-segment in parallel (all columns present in every segment)
- **Column-sliced** (`has_column_slicing_=true`): filter skipped per-segment; DuckDB applies WHERE post-merge

`IS NULL` / `IS NOT NULL` are NOT pushed to C++ regardless — `pushdown.py` excludes null-check filters from the QueryBuilder because C++ treats NaN as null (pandas semantics) while DuckDB treats NaN as a valid float (SQL semantics). This avoids double-filtering where C++ and DuckDB disagree.

### Multi-Key (Recursive Normalizer) Data

Multi-key data (nested dicts/lists written with `recursive_normalizers=True`) is **completely orthogonal** to the lazy read path. `setup_pipeline_context()` detects `KeyType::MULTI_KEY` at `version_core.cpp:1263-1265`, sets `pipeline_context->multi_key_`, and returns without populating `slice_and_keys_`. The lazy iterator rejects multi-key with an explicit error at `version_store_api.cpp:1085-1088`. Multi-key reads follow a separate eager path via `read_multi_key()` + Python-side `Flattener` reconstruction.

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
