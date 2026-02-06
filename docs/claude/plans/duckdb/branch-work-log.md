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

---

## Open Items

- Type handling gaps: timestamp precision variants (s/ms/us), DATE types, DECIMAL types, BINARY types (see `duckdb-branch-review.md` section 4)
- Broad `except Exception` handlers in `pushdown.py` should catch specific exceptions
- Complex SQL pattern tests (window functions, OUTER JOINs, subqueries)
- DuckDB connection created outside try/finally in `Library.sql()`
