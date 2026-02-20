# DuckDB Branch Work Log

## 2026-02-20: Coverage gap tests

- Analyzed Python and C++ test coverage across the DuckDB branch
- **Python (~85-90% coverage)**: Added coverage gap tests to 3 test files:
  - `test_arrow_reader.py`: 10 new tests covering `_is_wider_numeric_type` full hierarchy, `_expand_columns_with_idx_prefix` edge cases, `_strip_idx_prefix_from_names` collision resolution, `_build_clean_to_storage_map`, all DataType variants via round-trip, empty symbol + column projection, `current_index` advancement during iteration
  - `test_pushdown.py`: 28 new tests covering DECIMAL edge cases (scale=0, negative), HUGEINT constants, CAST type families (all timestamp/integer variants), deeply nested AND chains, OR subexpression handling, BETWEEN/IN with various types, column-on-both-sides comparisons, strict/inclusive date range flag combinations, `fully_pushed` flag conditions (OR, IS NULL, LIMIT, DISTINCT, aggregation), `select_columns` vs `columns` separation, subquery table extraction
  - `test_duckdb.py`: 9 new tests covering special characters in data, external connection failure propagation, CTE auto-registration, execute+sql temp table interaction, combined date_range+columns, connection property access, `_parse_library_name` edge cases, output_format=None default, empty string columns
- **C++ (~65-85% coverage)**: Added coverage gap tests to 2 test files:
  - `test_lazy_record_batch_iterator.cpp`: 6 new tests covering all numeric types in `default_arrow_format_for_type`, padding when all columns missing, timestamp null column padding, bool null column padding, empty string pool segments, multi-row-group padding with different column sets
  - `test_lazy_read_helpers.cpp`: 7 new tests covering date range before segment, row range before/after segment, exact date bounds, single-column byte estimation, empty slice estimation, `apply_filter_clause` with actual ExpressionContext (matches some/no/all rows)
- Fixed stale test `test_numeric_index_not_pushed_as_date_range` to match updated `_extract_date_range` behavior that skips numeric values
- All 457 Python DuckDB tests pass

## 2026-02-20: Fix SHOW TABLES + Refactor DuckDB duplication

- **BUG-2 fix**: `Library.sql()` SHOW TABLES no longer reads symbol data — registers empty schema-only tables so DuckDB sees table names without reading storage
- **ARCH-1**: Extracted `reconstruct_pandas_index()` helper in `index_utils.py` — replaces duplicate 9-line index reconstruction blocks in both `library.py` and `duckdb.py`
- **ARCH-2**: Removed `information_schema.tables` catalog query from `_auto_register()` — uses `self._registered_symbols` + `has_symbol()` guard for external DuckDB tables
- **ARCH-3**: Internalized `_expand_columns_with_idx_prefix` into `_read_as_record_batch_reader()` — removed 3 duplicate call sites
- **DUP-1**: Extracted `_try_sql_fast_path()` from `Library.sql()` — 34-line nested conditional replaced with clean helper method
- Added `_resolve_symbol_as_of()` helper in `index_utils.py` — replaces 3 occurrences of inline `isinstance(as_of, dict)` pattern
- All 457 Python DuckDB tests pass
