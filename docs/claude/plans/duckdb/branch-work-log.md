# DuckDB Branch — Work Log

Chronological summary of work done on the `duckdb` branch.

---

## 1. Initial DuckDB SQL Integration (3a237286b)

- Designed and implemented full DuckDB SQL integration for querying ArcticDB data
- C++ `RecordBatchIterator` for streaming Arrow record batches one at a time from `ArrowOutputFrame`
- `RecordBatchData` struct with Arrow C Data Interface, Rule of Five (move-only, release callbacks)
- Python `ArcticRecordBatchReader` wrapping C++ iterator, implementing PyArrow RecordBatchReader protocol
- `Library.sql()` method for single-shot SQL queries with automatic symbol discovery
- `DuckDBContext` context manager for multi-symbol JOINs and complex queries
- Zero-copy data path: ArcticDB segments → sparrow record_batch → Arrow C Data Interface → PyArrow → DuckDB

## 2. SQL Predicate Pushdown (fda5fe456)

- Pure DuckDB AST parsing (`duckdb.query(...).description`) to extract pushdown info from SQL
- Column projection: extracts referenced columns from SELECT/WHERE to avoid reading unused columns
- Filter pushdown: converts SQL WHERE clauses to ArcticDB `QueryBuilder` filters
- Date range extraction: detects timestamp filters and pushes down as `date_range` parameter
- LIMIT pushdown: extracts SQL LIMIT as `row_range` for early termination

## 3. Package Refactoring (0cd71ef26, e30022b60)

- Moved DuckDB modules from flat files to `duckdb/` subpackage under `version_store`
- `duckdb_integration.py` → `duckdb/integration.py` → later `duckdb/duckdb.py`
- Reorganized tests to mirror the `duckdb/` package layout

## 4. Code Review Fixes — Round 1 (26f7c6eca)

- **C++ memory safety**: Added destructor to `RecordBatchData`, removed dangerous `peek_schema_batch()`
- **Python quality**: Eliminated duplicate SQL parsing (combined into single `extract_pushdown_from_sql()` call), added `logger.debug()` to all exception handlers, extracted `_extract_column_refs_from_node()` shared helper
- **Exhausted reader**: Added `is_exhausted` property, `RuntimeError` on re-iteration
- **Validation**: Added check for registered symbols before `query()` execution

## 5. JOIN Column Pushdown Fix (9717a13ac)

- Fixed bug where column pushdown broke JOIN queries by pushing down only columns from one side
- Column pushdown now disabled when query contains JOINs (multiple table references)

## 6. Documentation (49df7732d)

- Wrote `docs/mkdocs/docs/tutorials/sql_queries.md` — full tutorial covering `lib.sql()`, `lib.duckdb()`, `arctic.duckdb()`, pushdown, output formats
- Updated `CLAUDE.md` with DuckDB doc references

## 7. Output Format Tests & External Connections (886367116, f26efe66f)

- Added explicit tests for pandas and polars output formats
- Added support for passing an external DuckDB connection to `DuckDBContext` and `ArcticDuckDBContext`

## 8. Rename integration.py → duckdb.py (d4be9ddc0)

- Renamed for clarity: `integration.py` → `duckdb.py` within the `duckdb/` package

## 9. SQL Documentation Example Tests (fc764f0d5)

- Added tests verifying that the SQL tutorial code examples actually work

## 10. Schema DDL & Data Discovery (6d49f4122)

- `SHOW TABLES` / `SHOW COLUMNS` virtual queries handled by intercepting SQL before DuckDB execution
- `SHOW TABLES` returns symbol list; `SHOW COLUMNS FROM <symbol>` returns column names/types

## 11. Database.Library Namespace Hierarchy (ea57edc65)

- `SHOW DATABASES` returns grouped database names with library counts
- `SHOW SCHEMAS IN <database>` lists libraries within a database
- Top-level libraries (no dots) grouped under `__default__` namespace
- `_parse_library_name()` splits `jblackburn.test_lib` → `("jblackburn", "test_lib")`

## 12. API Improvements (6a3b2fbfd)

- `lib.sql()` now returns DataFrame directly (not `VersionedItem`) — less ceremony for the common case
- Added `explain=True` parameter for pushdown metadata when needed
- Added `lib.duckdb_register(conn)` to register all symbols into a DuckDB connection for reusable queries
- Added `lib.sql(..., explain=True)` returning `(data, explain_dict)` tuple

## 13. Design Docs (832d83f5f)

- Added `docs/claude/python/DUCKDB.md` — technical design doc for the DuckDB integration
- Added `docs/claude/cpp/ARROW.md` — technical doc for Arrow output frame and C Data Interface

## 14. Code Review Fixes — Round 2

- `memset` zeroing for `RecordBatchData` default constructor (fix uninitialized Arrow structs)
- Identified remaining issues: broad exception handling in pushdown, code duplication between context managers, DuckDB connection leak in `Library.sql()` error path

## 15. pybind11 Holder Type Fix (current session)

- **Bug**: CI failure — `pybind11::cast_error(Unable to load a custom holder type from a default-holder instance)` on `write_versioned_composite_data`
- **Root cause**: `RecordBatchData` bound with default holder (`unique_ptr`) but `InputItem` variant uses `shared_ptr<RecordBatchData>`
- **Fix**: Changed binding to `py::class_<RecordBatchData, std::shared_ptr<RecordBatchData>>` in `python_bindings.cpp:242`

## 16. Optional Dependency Declaration (current session)

- Added `duckdb` as an optional extra in `setup.cfg` (`[duckdb]` extra)
- Added `duckdb` to the `Testing` extra so CI installs it and runs the DuckDB tests (previously they were silently skipped via `pytest.importorskip`)
- Tests already use `pytest.importorskip("duckdb")` for graceful skip when not installed

## 17. Parallel Test Execution Guidance (current session)

- Updated `CLAUDE.md` "Running Python Tests" section to recommend `-n 8` for parallel execution via pytest-xdist
- Added example for running DuckDB test subdirectory in parallel

## 18. Read-Only SQL Validation (current session)

- Added `check_sql_is_read_only()` in `pushdown.py` — uses a tight **allowlist** of 4 supported prefixes: SELECT, WITH, SHOW, DESCRIBE
- Allowlist only includes statements that `Library.sql()` can actually handle (i.e., statements that reference symbols or are special-cased like SHOW TABLES). Statements like EXPLAIN, PRAGMA, CALL are rejected even though they're read-only in DuckDB, because `Library.sql()` can't extract symbols from them
- Validation applied only to `Library.sql()` — the `DuckDBContext.query()` path doesn't need it since symbols are pre-registered and DuckDB enforces read-only on Arrow readers
- Added unit tests in `test_pushdown.py::TestCheckSqlIsReadOnly` (6 allowed, 17 rejected incl. EXPLAIN/PRAGMA/CALL, plus edge cases)
- Added integration tests in `test_duckdb.py::TestDuckDBSimpleSQL::test_rejects_mutating_sql`

## 19. Code Quality Notes (current session)

- Added SQL parsing policy note to top of `pushdown.py` — always use DuckDB's `json_serialize_sql()` AST parser, never regex/string matching for SQL structure
- Added "Branch Work Logs" section to `CLAUDE.md` — maintain `docs/claude/plans/<branch>/branch-work-log.md` when working on feature branches
- No regex found in `pushdown.py` — confirmed clean
- `dependencies.py` does not need a duckdb entry — it's for module-level imports with availability flags (pyarrow, polars). DuckDB is lazily imported with its own `_check_duckdb_available()` helper

## 20. Tighten Allowlist & Fix CTE Support (current session)

- Narrowed `_SUPPORTED_SQL_PREFIXES` from 8 to 4: SELECT, WITH, SHOW, DESCRIBE — only statements `Library.sql()` can actually handle
- Removed EXPLAIN, PRAGMA, CALL, SUMMARIZE — they pass the allowlist but fail later with confusing "Could not extract symbol names" errors because they don't reference tables
- Removed `check_sql_is_read_only()` from `_BaseDuckDBContext._format_query_result()` — the context manager doesn't need it since symbols are pre-registered and DuckDB enforces read-only on Arrow readers
- **CTE bug found and fixed**: `_extract_tables_from_ast()` was picking up CTE alias names (e.g. `filtered`) as real table references, causing `Library.sql()` to try reading them as ArcticDB symbols
- Added `_extract_cte_names()` to collect WITH clause names and exclude them from the symbol list
- Disabled column/filter pushdown for CTE queries (same as JOINs) — the outer query's SELECT/WHERE doesn't reflect columns needed inside the CTE body
- Tests: 3 CTE unit tests in `test_pushdown.py::TestExtractPushdownFromSql`, 1 integration test in `test_duckdb.py::TestDuckDBSimpleSQL::test_cte_query`

## 21. AST-Based Read-Only Validation (replaces string-based check)

- Replaced `check_sql_is_read_only()` (keyword allowlist) with `_get_sql_ast_or_raise()` — uses DuckDB's `json_serialize_sql()` which only serializes SELECT-like statements
- Non-SELECT statements (INSERT, UPDATE, DELETE, CREATE, etc.) produce a specific error "Only SELECT statements can be serialized to json!" which we translate into a clear user-facing `ValueError`
- Removed `_SUPPORTED_SQL_PREFIXES`, `check_sql_is_read_only()`, and all string-based validation from `pushdown.py`
- Removed `check_sql_is_read_only` call from `Library.sql()` — validation now happens inside `extract_pushdown_from_sql` via `_get_sql_ast_or_raise`
- Updated test class: `TestCheckSqlIsReadOnly` → `TestReadOnlyValidation` (tests through `extract_pushdown_from_sql`)

## 22. Case-Insensitive Symbol Resolution

- **Bug**: SQL identifiers are conventionally case-insensitive, but ArcticDB symbols are case-sensitive. `SELECT * FROM TRADES` would fail with `NoSuchVersionException` if the symbol was stored as `trades`
- **Fix**: In `Library.sql()`, after extracting symbol names from SQL AST, resolve each against actual library symbols using case-insensitive matching (exact match preferred, then `.lower()` fallback)
- `symbol_lookup = {s.lower(): s for s in lib.list_symbols()}` built once per query
- Registers in DuckDB under the SQL name (so DuckDB query finds it) but reads from ArcticDB using the real symbol name
- Context manager (`DuckDBContext`) doesn't need this — user passes exact symbol name to `register_symbol()`, and DuckDB's `conn.register()` is already case-insensitive for queries
- Tests: `TestDuckDBCaseSensitivity` — 6 tests covering uppercase/mixed/lowercase SQL vs symbol case, exact match priority, pushdown with case mismatch, nonexistent symbol

## 23. Implicit String-to-Timestamp Conversion in Pushdown

- **Bug**: `WHERE ts < '2024-01-03'` fails with `E_INVALID_USER_ARGUMENT Invalid comparison timestamp ... < ... STRING` because the VARCHAR literal is pushed down as a raw string to ArcticDB's C++ engine
- Standard SQL behaviour: all SQL engines implicitly cast ISO date strings to timestamps in comparisons
- DuckDB's AST parser represents `'2024-01-03'` as VARCHAR; only `TIMESTAMP '2024-01-03'` or `CAST(... AS TIMESTAMP)` produce CAST nodes
- **Fix**: In `_extract_constant_value()` VARCHAR case, detect ISO date patterns (`^\d{4}-\d{2}-\d{2}`) and auto-convert via `pd.Timestamp()`. Non-date strings (e.g. `'call'`, `'AAPL'`) are unaffected
- Added `_ISO_DATE_RE` compiled regex at module level in `pushdown.py`
- Tests: 3 unit tests in `TestParseWhereClause` (ISO date, ISO datetime, non-date string preserved), 4 integration tests in `TestDuckDBTimestampFilters` (string date, string datetime, explicit TIMESTAMP keyword, regular string filter)

## 24. SQL Demo Notebook

- Created `docs/mkdocs/docs/notebooks/ArcticDB_demo_sql.ipynb` — comprehensive SQL demo notebook
- ArcticDB logo banner matching other demo notebooks, `!pip install arcticdb duckdb` cell
- 9 sections: Setup, SQL Basics, Aggregation, OHLC Bars, VWAP, Options Greeks, Window Functions, CTEs, JOINs
- Uses real AAPL options CSV data from `data/` + synthetic tick/trade data
- All code cells execute successfully with outputs stored for GitHub rendering

## 25. Notebook Streamlining — Remove QB Comparison, Add JOIN+Resample

- Removed QueryBuilder vs SQL comparison section (5 cells: header + filter/groupby/resample/vwap) and 2 inline QB cells (ohlc-qb, vwap-qb)
- Added `join-resample` cell: CTE query joining hourly OHLC bars (from `ticks`) with AAPL trade flow (from `trades`) via `TIME_BUCKET` + `JOIN`
- Demonstrates CTEs, resample via `TIME_BUCKET`, multi-symbol `JOIN`, and `CASE WHEN` in a single realistic query
- Updated summary table to SQL-only format
- 35 cells (11 markdown, 24 code), all execute successfully with outputs stored

## 26. ASV Performance Benchmarks for SQL

- Created `python/benchmarks/sql.py` with 4 benchmark classes:
  - **`SQLQueries`**: Core SQL throughput — SELECT *, column projection, WHERE filter, GROUP BY (low/high cardinality), JOIN, filter+groupby, LIMIT, Arrow output. Params: [1M, 10M] rows
  - **`SQLStreamingMemory`**: Peak memory comparison — streaming SQL vs full materialization via `lib.read()`. Params: [aggregation, filtered_1pct, full_scan] at 10M rows
  - **`SQLLargeGroupBy`**: GROUP BY on large data with result fitting in memory. Params: [id1, id6] × [sum, mean, count] at 10M rows
  - **`SQLFilteringMemory`**: Selectivity scaling — varying filter thresholds (0.1%, 1%, 10%, 50%) to verify memory scales with result, not source. 10M rows
- All benchmarks validated: import, setup_cache, execution, teardown

## 27. Fix LIMIT Pushdown — Actually Push to Storage

- **Bug**: `lib.sql("SELECT * FROM t LIMIT 100")` took 2.8s on 1M rows because LIMIT was extracted by `extract_pushdown_from_sql()` into `PushdownInfo.limit` but never converted to `row_range` in `Library.sql()`
- **Fix**: In `library.py`, convert `pushdown.limit` to `row_range=(0, limit)` when calling `_read_as_record_batch_reader()`
- **Safety guards** in `pushdown.py`: LIMIT only pushed to storage (`can_push_limit`) for simple scans. Disabled when: ORDER BY (sort needs all rows), GROUP BY (LIMIT on aggregated result), DISTINCT (LIMIT on deduplicated result), multi-table/CTEs (LIMIT on joined result), WHERE clause (filter may discard rows, making first-N-rows ≠ first-N-results)
- Added `_has_order_by()` helper to detect ORDER BY modifier in DuckDB AST
- **Result**: `SELECT * FROM t LIMIT 100` on 1M rows: 2.8s → 0.36s (7.8x faster)
- **Tests**: 3 new unit tests (ORDER BY, GROUP BY, DISTINCT prevent LIMIT pushdown), 2 new integration tests (ORDER BY, GROUP BY correctness with unpushed LIMIT), updated 3 existing tests for corrected semantics

## 28. DuckDB Streaming Memory Investigation

- Investigated whether DuckDB materializes full RecordBatchReader or streams incrementally
- **Batch consumption is eager**: DuckDB reads ALL batches (no early termination for LIMIT)
- **Pipeline processing is streaming**: batches processed incrementally, memory proportional to result/aggregation state, not source
- 8 GB dataset: GROUP BY used +1.3 GB (6x less), WHERE 1% used +219 MB (37x less), full scan +16.5 GB (2x source+pandas)
- Researched DuckDB integration patterns: Parquet/Delta/Iceberg all use C++ extensions for pushdown, no Python API exists
- See `docs/claude/plans/duckdb/duckdb-streaming-memory-investigation.md`

## 29. ArcticDB Backend Eagerness Investigation (current session)

- **Question**: Does ArcticDB read all data eagerly from storage (e.g., S3) before the RecordBatchIterator is created, or is there on-demand/backpressure?
- **Answer**: All data is read eagerly. The RecordBatchIterator is a memory cursor, not a lazy reader.
- **Code trace**: `read_dataframe_version_internal()` calls `fetch_data()` which queues reads for ALL segments via `batch_read_compressed()` using `folly::window(batch_size=200)` for up to 200 parallel reads, then `folly::collect()` blocks until all complete
- `RecordBatchIterator::next()` just returns `(*data_)[current_index_++]` — no storage I/O
- **Two layers of streaming**: Storage→Memory is eager (all data materialized); Memory→DuckDB is incremental (DuckDB processes batches through pipeline)
- **Implication**: For a 10 GB S3 symbol, all 10 GB downloaded before DuckDB sees any data. Pushdown is the only mechanism to reduce data read.
- Updated `duckdb-streaming-memory-investigation.md` with full code trace and corrected summary

## 30. Lazy Streaming RecordBatchIterator

- **Goal**: Avoid reading all data eagerly from storage before DuckDB gets any batches. For large remote datasets (S3), the eager path materializes everything into memory before streaming begins.
- **New C++ class `LazyRecordBatchIterator`** in `arrow_output_frame.hpp/cpp`:
  - Holds `slice_and_keys_` (segment metadata from index), reads+decodes one segment at a time in `next()`
  - Prefetch buffer (`std::deque<folly::Future>`) with configurable `prefetch_size` (default 2) to hide storage latency
  - `read_and_decode_segment()` uses `store_->batch_read_uncompressed()` (reuses existing `DecodeSliceTask` pattern)
  - `prepare_segment_for_arrow()` handles conversion of decoded segments to Arrow-compatible format:
    - Non-string columns: makes inline blocks detachable (for `block.release()` ownership transfer)
    - String columns: creates new DETACHABLE column with proper Arrow buffers via `ArrowStringHandler::convert_type()`
- **Key bug fix**: `extra_bytes_per_block` double-counting. `create_detachable_block()` already adds `extra_bytes_per_block_` to block capacity internally. The allocation must pass only `dest_bytes` (= `num_rows * data_size`), NOT `dest_bytes + extra_bytes`. Double-counting caused `block.row_count()` to return `num_rows + 1`, triggering Sparrow's `child.size() == size` assertion.
- **C++ entry point**: `PythonVersionStore::create_lazy_record_batch_iterator()` in `version_store_api.cpp` — calls `setup_pipeline_context()` (index-only read), extracts `slice_and_keys_` and `columns_to_decode`, creates `LazyRecordBatchIterator`
- **Python bindings**: `LazyRecordBatchIterator` bound with `py::call_guard<py::gil_scoped_release>()` on `next()` (does Folly async I/O)
- **Python integration**: `NativeVersionStore.read_as_lazy_record_batch_iterator()` in `_store.py`, `Library._read_as_record_batch_reader(lazy=True)` in `library.py`
- **Fallback to eager**: When `date_range` or `row_range` specified (lazy path only filters at segment granularity), when `query_builder` has clauses, or when symbol is empty (lazy iterator can't discover schema without data)
- **`Library.sql()`** now uses lazy=True by default for all symbol registration
- **Tests**: 20 tests in `test_lazy_streaming.py` (4 classes: SQL queries, direct iterator, lazy-vs-eager consistency, DuckDB context), all passing
- **No regressions**: All 285 DuckDB tests pass

---

## Open Items

- Type handling gaps: timestamp precision variants (s/ms/us), DATE types, DECIMAL types, BINARY types (see `duckdb-branch-review.md` section 4)
- Broad `except Exception` handlers in `pushdown.py` should catch specific exceptions
- Complex SQL pattern tests (window functions, OUTER JOINs, subqueries)
- DuckDB connection created outside try/finally in `Library.sql()`
