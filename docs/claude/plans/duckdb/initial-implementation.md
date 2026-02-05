# DuckDB Integration Plan for ArcticDB

## Goal
Enable SQL reads from ArcticDB via DuckDB with memory-efficient segment-by-segment processing using Arrow for zero-copy data interchange.

## Key Decision: Python-only Integration
Use `duckdb` pip package (not vcpkg bundling). Rationale:
- Simpler implementation, no C++ ABI compatibility issues
- Users can control DuckDB version independently
- DuckDB Python bindings are mature and sufficient for this use case

## Architecture Overview

```
User SQL Query
     │
     ▼
Library.sql() / DuckDBContext
     │
     ▼
ArcticRecordBatchReader (implements PyArrow RecordBatchReader protocol)
     │
     ▼ (lazy iteration - one batch at a time)
C++ RecordBatchIterator
     │
     ▼
ArrowOutputFrame (existing - has vector<record_batch>)
     │
     ▼
DuckDB.from_arrow() (zero-copy streaming scan)
```

## Proposed API

### 1. Simple SQL method (single symbol queries)
```python
result = lib.sql("SELECT col1, SUM(col2) FROM my_symbol WHERE col1 > 100 GROUP BY col1")
```

### 2. Context manager for JOINs and complex queries
```python
with lib.duckdb_context() as ddb:
    ddb.register_symbol("trades", as_of=0, date_range=(start, end))
    ddb.register_symbol("prices", as_of=-1)
    result = ddb.query("""
        SELECT t.ticker, t.quantity * p.price as notional
        FROM trades t
        JOIN prices p ON t.ticker = p.ticker
    """)
```

### 3. Low-level streaming reader (for advanced use)
```python
reader = lib.read_as_record_batch_reader("symbol")
# Can be passed directly to DuckDB, Polars, or other Arrow-compatible tools
```

## Implementation Steps

### Phase 1: Expose Streaming Arrow Interface

**1.1 C++ Layer - Add RecordBatchIterator**

File: `cpp/arcticdb/arrow/arrow_output_frame.hpp`
```cpp
class RecordBatchIterator {
public:
    RecordBatchIterator(std::shared_ptr<std::vector<sparrow::record_batch>> data);
    std::optional<RecordBatchData> next();
    bool has_next() const;
    RecordBatchData peek_schema_batch();  // For schema extraction
private:
    std::shared_ptr<std::vector<sparrow::record_batch>> data_;
    size_t current_index_;
};
```

**1.2 C++ Python Bindings**

File: `cpp/arcticdb/version/python_bindings.cpp`
- Add `_read_record_batch_iterator()` method to PythonVersionStore
- Bind RecordBatchIterator class to Python

**1.3 Python Layer - ArcticRecordBatchReader**

File: `python/arcticdb/version_store/arrow_reader.py` (NEW)
- Implements PyArrow RecordBatchReader protocol
- Wraps C++ RecordBatchIterator
- Converts batches via Arrow C Data Interface

**1.4 Library Method**

File: `python/arcticdb/version_store/library.py`
```python
def read_as_record_batch_reader(self, symbol, as_of=None, date_range=None,
                                 columns=None, query_builder=None) -> ArcticRecordBatchReader:
    """Returns lazy Arrow RecordBatchReader for streaming."""
```

### Phase 2: DuckDB SQL Interface

**2.1 Simple SQL Method**

File: `python/arcticdb/version_store/library.py`
```python
def sql(self, query: str, as_of=None, output_format=None) -> VersionedItem:
    """Execute SQL on symbols using DuckDB."""
    # Parse query to extract symbol names
    # Register each symbol as RecordBatchReader
    # Execute via DuckDB
    # Return result in requested format
```

**2.2 Context Manager for Complex Queries**

File: `python/arcticdb/version_store/duckdb_integration.py` (NEW)
```python
class DuckDBContext:
    def register_symbol(self, symbol, alias=None, as_of=None, date_range=None, columns=None):
        """Register ArcticDB symbol as DuckDB table."""

    def query(self, sql, output_format="pandas"):
        """Execute SQL and return results."""
```

### Phase 3: Tests

File: `python/tests/unit/arcticdb/version_store/test_duckdb_integration.py` (NEW)

**Test Cases:**
1. `test_simple_sql_select` - Basic SELECT with WHERE clause
2. `test_sql_aggregation` - GROUP BY, SUM, AVG
3. `test_record_batch_reader_streaming` - Verify multiple batches returned
4. `test_duckdb_context_join` - JOIN across two symbols
5. `test_memory_efficiency` - Large dataset with filter, verify memory stays low
6. `test_time_series_queries` - Date-based filtering and aggregation

## Files to Modify/Create

| File | Action | Purpose |
|------|--------|---------|
| `cpp/arcticdb/arrow/arrow_output_frame.hpp` | Modify | Add RecordBatchIterator class |
| `cpp/arcticdb/arrow/arrow_output_frame.cpp` | Modify | Implement RecordBatchIterator |
| `cpp/arcticdb/version/python_bindings.cpp` | Modify | Expose iterator to Python |
| `python/arcticdb/version_store/arrow_reader.py` | Create | ArcticRecordBatchReader class |
| `python/arcticdb/version_store/duckdb_integration.py` | Create | DuckDBContext class |
| `python/arcticdb/version_store/library.py` | Modify | Add sql(), read_as_record_batch_reader(), duckdb_context() |
| `python/tests/.../test_duckdb_integration.py` | Create | Unit tests |

## Memory Efficiency Validation

The key to memory efficiency is that:
1. ArcticDB already stores data in segments (TABLE_DATA keys)
2. The read pipeline already decodes segments one at a time
3. `ArrowOutputFrame` already contains a vector of record batches (one per segment)
4. Currently Python materializes all batches into one `pa.Table` - we bypass this
5. DuckDB's `from_arrow(RecordBatchReader)` processes batches lazily

**Validation approach:**
```python
# Large dataset query with selective filter
# Should NOT load entire dataset into memory
process = psutil.Process(os.getpid())
mem_before = process.memory_info().rss

result = lib.sql("SELECT x FROM large_symbol WHERE x < 100")  # 100 of 1M rows

mem_after = process.memory_info().rss
assert (mem_after - mem_before) < 50_MB  # Much less than full dataset
```

## Dependencies

- `duckdb` - Optional Python dependency (users install separately)
- No vcpkg changes needed
- No new C++ library dependencies

## Success Criteria

- [ ] Can execute SELECT queries on single symbol
- [ ] Can execute JOIN queries across multiple symbols
- [ ] Memory usage for filtered query < 10% of full dataset size
- [ ] Works with existing ArcticDB features (date_range, columns, query_builder)
- [ ] All tests pass
