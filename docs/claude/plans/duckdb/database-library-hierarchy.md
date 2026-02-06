# Database.Library Namespace Hierarchy Plan

## Overview

Update DuckDB integration to properly handle `database.library` namespace hierarchy:
- Libraries use `database.library` format (e.g., `jblackburn.test_lib`)
- Databases are permissioning units (one per user)
- Top-level libraries (no dots) grouped under `__default__`

## Requirements

1. **SHOW DATABASES** returns hierarchy info:
   - `database_name` column - unique database names
   - `library_count` column - number of libraries per database

2. **Top-level libraries** grouped under `__default__` namespace

3. **SHOW SCHEMAS IN \<database\>** lists libraries within a database

## Implementation

### 1. Add Library Name Parser

**File**: `python/arcticdb/version_store/duckdb/duckdb.py` (after imports, ~line 30)

```python
def _parse_library_name(library_name: str) -> Tuple[str, str]:
    """
    Parse library name into (database, library) tuple.

    Split on first dot only. Libraries without dots use '__default__'.

    Examples:
        "jblackburn.test_lib" -> ("jblackburn", "test_lib")
        "jblackburn.test.lib" -> ("jblackburn", "test.lib")
        "global_data" -> ("__default__", "global_data")
    """
    if '.' not in library_name:
        return "__default__", library_name
    parts = library_name.split('.', 1)
    return parts[0], parts[1]
```

### 2. Add Schema Discovery Query Detection

**File**: `python/arcticdb/version_store/duckdb/pushdown.py` (after `is_database_discovery_query`)

```python
def is_schema_discovery_query(query: str) -> Optional[str]:
    """
    Check if query is SHOW SCHEMAS IN <database> and extract database name.

    Returns database name if match, None otherwise.
    """
    import re
    match = re.match(r'^\s*SHOW\s+SCHEMAS\s+IN\s+(\w+)\s*$', query, re.IGNORECASE)
    if match:
        return match.group(1)
    return None
```

### 3. Update Arctic.sql()

**File**: `python/arcticdb/arctic.py`

**Changes to `sql()` method:**
- Check for `SHOW SCHEMAS IN` before `SHOW DATABASES`
- Group libraries by database and count for `SHOW DATABASES`
- Add `_execute_show_schemas()` method
- Extract `_format_query_result()` helper

**SHOW DATABASES output:**
```
   database_name  library_count
0     jblackburn              2
1     __default__             1
```

**SHOW SCHEMAS IN jblackburn output:**
```
     schema_name
0      test_lib1
1      test_lib2
```

### 4. Update ArcticDuckDBContext

**File**: `python/arcticdb/version_store/duckdb/duckdb.py`

**Changes:**
- Update `_execute_show_databases()` to group by database with counts
- Add `_execute_show_schemas()` method
- Add `_format_output()` helper
- Update `query()` to route schema queries

### 5. Update __init__.py Export

**File**: `python/arcticdb/version_store/duckdb/__init__.py`

Export `_parse_library_name` for use by `arctic.py`.

## Test Updates

**File**: `python/tests/unit/arcticdb/version_store/duckdb/test_duckdb.py`

### New Test Class: TestDatabaseLibraryNamespace

```python
class TestDatabaseLibraryNamespace:
    """Tests for database.library namespace handling."""

    def test_parse_library_name_with_database(self):
        """Test parsing jblackburn.test_lib format."""

    def test_parse_library_name_multi_dot(self):
        """Test parsing jblackburn.test.lib - split on first dot only."""

    def test_parse_library_name_top_level(self):
        """Test top-level library goes to __default__."""

    def test_show_databases_groups_by_database(self, lmdb_storage):
        """Test SHOW DATABASES returns database_name and library_count."""
        # Create: jblackburn.lib1, jblackburn.lib2, global_data
        # Expect: jblackburn(2), __default__(1)

    def test_show_databases_default_namespace(self, lmdb_storage):
        """Test top-level libraries grouped under __default__."""

    def test_show_schemas_in_database(self, lmdb_storage):
        """Test SHOW SCHEMAS IN jblackburn lists libraries."""

    def test_show_schemas_in_default(self, lmdb_storage):
        """Test SHOW SCHEMAS IN __default__ lists top-level libraries."""

    def test_show_schemas_empty_database(self, lmdb_storage):
        """Test SHOW SCHEMAS IN nonexistent returns empty."""

    def test_context_show_databases_with_hierarchy(self, lmdb_storage):
        """Test arctic.duckdb() SHOW DATABASES with hierarchy."""

    def test_context_show_schemas(self, lmdb_storage):
        """Test arctic.duckdb() SHOW SCHEMAS IN."""
```

### Update Existing Tests

Update `TestArcticDuckDBShowDatabases` to use `database.library` format libraries:
- Change `"market_data"` → `"testuser.market_data"`
- Change `"reference_data"` → `"testuser.reference_data"`
- Update assertions for new column schema

## Documentation Updates

**File**: `docs/mkdocs/docs/tutorials/sql_queries.md`

Add section on database hierarchy:

```markdown
## Database Hierarchy

ArcticDB organizes data in a `database.library` hierarchy:
- **Database**: Permissioning unit, typically one per user (e.g., `jblackburn`)
- **Library**: Collection of symbols within a database (e.g., `jblackburn.market_data`)
- **Symbol**: Individual table/dataset within a library

### Discovering Databases

```python
# List all databases with library counts
arctic.sql("SHOW DATABASES")
#    database_name  library_count
# 0     jblackburn              3
# 1     __default__             1

# List libraries in a specific database
arctic.sql("SHOW SCHEMAS IN jblackburn")
#      schema_name
# 0    market_data
# 1    reference_data
# 2    portfolios
```

### Top-Level Libraries

Libraries without a database prefix are grouped under `__default__`:

```python
arctic.create_library("global_config")  # No dot = top-level
arctic.sql("SHOW SCHEMAS IN __default__")
#      schema_name
# 0    global_config
```
```

## File Summary

| File | Changes |
|------|---------|
| `python/arcticdb/version_store/duckdb/duckdb.py` | Add `_parse_library_name()`, update `_execute_show_databases()`, add `_execute_show_schemas()` |
| `python/arcticdb/version_store/duckdb/pushdown.py` | Add `is_schema_discovery_query()` |
| `python/arcticdb/version_store/duckdb/__init__.py` | Export `_parse_library_name` |
| `python/arcticdb/arctic.py` | Update `sql()`, add `_execute_show_schemas()`, `_format_query_result()` |
| `python/tests/.../test_duckdb.py` | Add `TestDatabaseLibraryNamespace`, update existing tests |
| `docs/mkdocs/docs/tutorials/sql_queries.md` | Add database hierarchy section |

## Verification

1. Run new namespace tests:
   ```bash
   pytest python/tests/unit/arcticdb/version_store/duckdb/test_duckdb.py::TestDatabaseLibraryNamespace -v
   ```

2. Run all DuckDB tests to verify no regressions:
   ```bash
   pytest python/tests/unit/arcticdb/version_store/duckdb/test_duckdb.py -v
   ```

3. Manual verification:
   ```python
   arctic = adb.Arctic('lmdb:///tmp/test')
   arctic.create_library('jblackburn.lib1')
   arctic.create_library('jblackburn.lib2')
   arctic.create_library('global')

   print(arctic.sql("SHOW DATABASES"))
   print(arctic.sql("SHOW SCHEMAS IN jblackburn"))
   print(arctic.sql("SHOW SCHEMAS IN __default__"))
   ```
