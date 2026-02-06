# Arrow Output Frame

Arrow C Data Interface integration for streaming ArcticDB query results to DuckDB.

## Location

```
cpp/arcticdb/arrow/
├── arrow_output_frame.hpp   # RecordBatchData, RecordBatchIterator, ArrowOutputFrame (132 lines)
└── arrow_output_frame.cpp   # Implementation (89 lines)
```

## Classes

### RecordBatchData

Single Arrow record batch: `ArrowArray` + `ArrowSchema` pair (Arrow C Data Interface).

```
RecordBatchData
├── array_   (ArrowArray)  — zero-initialized with std::memset
├── schema_  (ArrowSchema) — zero-initialized with std::memset
├── extract_struct_array() → sparrow::record_batch  (destructive, moves data out)
├── array() → ArrowArray&
└── schema() → ArrowSchema&
```

**Key design**: Zero-initialized in constructor via `std::memset` to ensure safe release callback behavior. The Arrow C Data Interface requires that `release` is either `NULL` (no-op) or a valid callback. Zero-init guarantees `release == NULL` before data is populated.

### RecordBatchIterator

Iterates over `vector<RecordBatchData>`, yielding `sparrow::record_batch` objects.

```
RecordBatchIterator
├── data_            (vector<RecordBatchData>)
├── current_index_   (size_t)
├── next() → optional<sparrow::record_batch>  (destructive extraction)
├── schema() → ArrowSchema*   (from first batch)
├── num_batches() → size_t
└── current_index() → size_t
```

Exposed to Python via pybind11 in `python_bindings_common.cpp` with `__next__` protocol.

### ArrowOutputFrame

Container holding all record batches from a query result.

```
ArrowOutputFrame
├── data_           (vector<RecordBatchData>)
├── data_consumed_  (bool, default false)
├── create_iterator() → shared_ptr<RecordBatchIterator>  (sets data_consumed_)
├── extract_record_batches() → vector<RecordBatchData>   (sets data_consumed_)
└── num_record_batches() → size_t
```

**Single-use enforcement**: `data_consumed_` flag prevents double consumption. Both `create_iterator()` and `extract_record_batches()` check this flag and raise `util::check` error if already consumed. This is critical because `extract_struct_array()` destructively moves data — a second read would get empty/invalid data.

## Data Flow

```
C++ read pipeline
    │
    ▼
ArrowOutputFrame (holds vector<RecordBatchData>)
    │
    ├── create_iterator()  ──→  RecordBatchIterator (C++)
    │                                │
    │                                ▼ (pybind11)
    │                           Python RecordBatchIterator
    │                                │
    │                                ▼
    │                           ArcticRecordBatchReader (arrow_reader.py)
    │                                │
    │                                ├── to_pyarrow_reader() ──→ pyarrow.RecordBatchReader
    │                                │                                │
    │                                │                                ▼
    │                                │                         conn.register(name, reader)
    │                                │                                │
    │                                │                                ▼
    │                                │                         DuckDB queries data lazily
    │                                │
    │                                └── read_all() ──→ pyarrow.Table (materialized)
    │
    └── extract_record_batches()  ──→  Direct batch access (not used by DuckDB path)
```

## Python Bindings

### `python_bindings.cpp`

`read_as_record_batch_iterator()` — calls the C++ read pipeline, returns `ArrowOutputFrame::create_iterator()`.

### `python_bindings_common.cpp`

Binds `RecordBatchIterator` as Python class with:
- `__next__` → calls `next()`, converts `sparrow::record_batch` to Python `(ArrowArray, ArrowSchema)` tuple
- `schema` property → returns first batch's schema
- `num_batches` property
- `current_index` property

## Memory Safety

| Concern | Mitigation |
|---------|-----------|
| Double consumption | `data_consumed_` flag in `ArrowOutputFrame` |
| Dangling release callbacks | `std::memset` zero-init in `RecordBatchData` constructor |
| Ownership transfer | `extract_struct_array()` moves data out, source becomes empty |
| Rule of Five | `RecordBatchData` uses default move semantics; Arrow structs are C structs with release callbacks |

## Related Documentation

- [PYTHON_BINDINGS.md](PYTHON_BINDINGS.md) — pybind11 binding details
- [../python/DUCKDB.md](../python/DUCKDB.md) — Python DuckDB integration using these Arrow types
- [PIPELINE.md](PIPELINE.md) — Read pipeline that produces `ArrowOutputFrame`
