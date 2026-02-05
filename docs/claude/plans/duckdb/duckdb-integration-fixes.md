# DuckDB Integration Fixes Plan

This document tracks all identified issues from the code review and their fixes.

## Overview

The DuckDB integration code review identified issues in four categories:
1. C++ Memory Safety
2. Python Code Quality
3. Type Handling Gaps
4. Test Coverage Gaps

---

## 1. C++ Memory Safety Issues

### 1.1 CRITICAL: RecordBatchData Missing Destructor

**Problem**: `RecordBatchData` struct contains `ArrowArray` and `ArrowSchema` members but lacks a destructor. The Arrow C Data Interface requires calling `release` callbacks to free memory.

**Location**: `cpp/arcticdb/arrow/arrow_output_frame.hpp:28-39`

**Fix**:
- Add destructor that calls release callbacks
- Add move constructor/assignment (delete copy semantics)
- Follow Rule of Five

**Test**: Add test that creates and destroys RecordBatchData, verify no memory leaks with valgrind/ASAN.

**Status**: [x] Completed - Added destructor, move semantics, deleted copy semantics.

### 1.2 CRITICAL: peek_schema_batch() Use-After-Free Risk

**Problem**: `peek_schema_batch()` is documented as non-consuming but actually calls `extract_struct_array()` which is destructive.

**Location**: `cpp/arcticdb/arrow/arrow_output_frame.cpp:80-92`

**Fix**:
- Option A: Cache schema separately in iterator constructor
- Option B: Remove peek_schema_batch() since Python already caches first batch
- Option C: Rename to make destruction explicit

**Decision**: Option B - Remove `peek_schema_batch()`. The Python `ArcticRecordBatchReader` already caches the first batch and extracts schema from it. The C++ method is unused and dangerous.

**Test**: Verify Python code works without peek_schema_batch().

**Status**: [x] Completed - Removed from C++ and Python bindings.

### 1.3 MEDIUM: Missing Copy/Move Semantics

**Problem**: `RecordBatchData` uses default copy/move which can cause double-free.

**Fix**: Addressed by 1.1 - delete copy, implement move.

**Status**: [x] Completed (part of 1.1)

---

## 2. Python Code Quality Issues

### 2.1 HIGH: Duplicate SQL Parsing

**Problem**: `Library.sql()` parses the SQL query twice - once in `_extract_symbols_from_query()` and again in `extract_pushdown_from_sql()`.

**Location**: `python/arcticdb/version_store/library.py:2217-2221`

**Fix**:
- Modify `_extract_symbols_from_query()` to accept optional pre-parsed AST
- Or combine both operations into a single function
- Or have `extract_pushdown_from_sql()` also return symbols

**Decision**: Modify `extract_pushdown_from_sql()` to also return extracted symbols, eliminating need for separate `_extract_symbols_from_query()` call.

**Test**: Existing tests should pass; add performance test confirming single parse.

**Status**: [x] Completed - `extract_pushdown_from_sql()` now returns tuple `(Dict[str, PushdownInfo], List[str])`.

### 2.2 HIGH: Silent Exception Swallowing

**Problem**: Multiple `except Exception: pass` blocks hide bugs and make debugging difficult.

**Locations**:
- `pushdown.py:55` - `_extract_limit_from_ast()`
- `pushdown.py:207` - `_extract_tables_from_ast()`
- `pushdown.py:539` - `_build_query_builder()`
- `pushdown.py:624` - `_get_sql_ast()`

**Fix**:
- Add logging at DEBUG level for caught exceptions
- Catch specific exceptions where possible
- Document why exceptions are expected/acceptable

**Test**: Add tests that trigger these exception paths and verify appropriate behavior.

**Status**: [x] Completed - Added `logger.debug()` calls to all exception handlers.

### 2.3 HIGH: Duplicate Column Extraction Logic

**Problem**: `_extract_columns_from_select_list()` and `_extract_columns_from_where()` contain nearly identical nested `extract_column_refs()` functions.

**Location**: `pushdown.py:77-114` vs `pushdown.py:126-158`

**Fix**: Extract shared helper function `_extract_column_refs_from_node()`.

**Test**: Existing tests should pass.

**Status**: [x] Completed - Extracted `_extract_column_refs_from_node()` shared helper.

### 2.4 MEDIUM: Exhausted Reader State

**Problem**: `ArcticRecordBatchReader._exhausted` flag is set but never reset. Reader can't be reused and gives no error on reuse attempt.

**Location**: `python/arcticdb/version_store/duckdb/arrow_reader.py:43-44`

**Fix**: Raise `StopIteration` or custom exception when attempting to iterate exhausted reader.

**Test**: Add test that tries to iterate twice and expects appropriate error.

**Status**: [x] Completed - Added `is_exhausted` property, `RuntimeError` on re-iteration, `RuntimeError` on `read_all()` after partial iteration.

### 2.5 MEDIUM: No Validation Before Query

**Problem**: `DuckDBContext.query()` doesn't validate if any symbols are registered.

**Location**: `python/arcticdb/version_store/duckdb/integration.py:200-243`

**Fix**: Add check for `len(self._registered_symbols) > 0` with helpful error message.

**Test**: Add test calling `query()` without registering symbols.

**Status**: [x] Completed - Added validation with helpful error message.

---

## 3. Type Handling Gaps

### 3.1 CRITICAL: Timestamp Precision Variants

**Problem**: Only `TIMESTAMP_NANOSECONDS` is supported. `TIMESTAMP_SECONDS`, `TIMESTAMP_MILLISECONDS`, `TIMESTAMP_MICROSECONDS` will fail.

**Location**: `cpp/arcticdb/arrow/arrow_utils.cpp`

**Fix**: Add cases to convert all timestamp precisions to nanoseconds.

**Test**: Add tests writing/reading data with different timestamp precisions.

**Status**: [ ] Not Started

### 3.2 CRITICAL: Date Types

**Problem**: `DATE_DAYS` and `DATE_MILLISECONDS` not supported.

**Location**: `cpp/arcticdb/arrow/arrow_utils.cpp`

**Fix**: Add cases to convert date types to nanosecond timestamps at midnight.

**Test**: Add tests with date-only columns.

**Status**: [ ] Not Started

### 3.3 CRITICAL: Decimal Types

**Problem**: `DECIMAL32`, `DECIMAL64`, `DECIMAL128`, `DECIMAL256` not supported.

**Location**: `cpp/arcticdb/arrow/arrow_utils.cpp`

**Fix**: Convert decimals to FLOAT64 (with precision loss warning in docs).

**Test**: Add tests with decimal columns.

**Status**: [ ] Not Started

### 3.4 HIGH: Binary Types

**Problem**: `BINARY`, `LARGE_BINARY` not supported.

**Location**: `cpp/arcticdb/arrow/arrow_utils.cpp`

**Fix**: Map to appropriate ArcticDB type or raise clear error.

**Test**: Add tests with binary columns.

**Status**: [ ] Not Started

---

## 4. Test Coverage Gaps

### 4.1 Error Handling Tests

**Missing**:
- Non-existent symbol in query
- Invalid SQL syntax
- DuckDB execution errors
- Invalid parameters (as_of, output_format)

**Status**: [x] Completed - Added tests: `test_query_without_registration_raises`, `test_raises_on_empty_sql`, `test_raises_on_invalid_sql`, `test_iterate_exhausted_reader_raises`, `test_read_all_after_iteration_raises`.

### 4.2 Edge Case Tests

**Missing**:
- Empty DataFrames (0 rows)
- All-null columns
- Special characters in column names
- Unicode column names

**Status**: [x] Completed - Added `TestDuckDBEdgeCases` class with tests for empty dataframes, nulls, special characters, large strings, float special values, mixed numeric types, boolean columns.

### 4.3 Untested Parameters

**Missing**:
- `as_of` parameter in `Library.sql()`
- `row_range` parameter in `DuckDBContext.register_symbol()`

**Status**: [x] Completed - Added `test_with_as_of_version` and `test_with_row_range` in both integration and arrow_reader tests.

### 4.4 Untested Code Paths

**Missing**:
- `num_batches` property
- `current_index` property
- `connection` property
- Empty result path (0 batches)

**Status**: [x] Completed - Added `test_is_exhausted_property`, `test_num_batches_property`, `test_current_index_property`, `test_len_returns_batch_count`, `test_read_next_batch_returns_none_when_exhausted`.

### 4.5 Complex SQL Patterns

**Missing**:
- Subqueries
- CTEs
- Window functions
- OUTER JOINs
- UNION/INTERSECT

**Status**: [ ] Not Started

### 4.6 Consolidate Duplicate Tests

**Action**: Use `pytest.mark.parametrize` for:
- Comparison operator tests
- Output format tests

**Status**: [ ] Not Started

---

## Implementation Order

### Phase 1: Critical Bugs (Must Fix)
1. [x] 1.1 RecordBatchData destructor
2. [x] 1.2 Remove peek_schema_batch()
3. [x] 2.1 Eliminate duplicate SQL parsing
4. [x] 2.2 Add logging for caught exceptions

### Phase 2: High Priority
5. [x] 2.3 Refactor duplicate column extraction
6. [x] 2.4 Fix exhausted reader behavior
7. [x] 2.5 Add validation before query
8. [ ] 3.1-3.4 Type handling (C++ changes - separate PR recommended)

### Phase 3: Test Coverage
9. [x] 4.1 Error handling tests
10. [x] 4.2 Edge case tests
11. [x] 4.3 Parameter tests
12. [x] 4.4 Code path tests
13. [ ] 4.5 Complex SQL tests (lower priority)
14. [ ] 4.6 Test consolidation (lower priority)

---

## Notes

- C++ type handling changes (3.1-3.4) are significant and may warrant a separate PR
- Complex SQL pattern tests (4.5) are nice-to-have but not blocking
- Test consolidation (4.6) is cleanup, not urgent
