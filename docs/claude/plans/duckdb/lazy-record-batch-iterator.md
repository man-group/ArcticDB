# Plan: Lazy Streaming RecordBatchIterator

## Problem

When `lib.sql()` queries a symbol, ArcticDB reads **all** segments from storage eagerly before creating the RecordBatchIterator. For a 10 GB S3 symbol with `GROUP BY`, all 10 GB is downloaded and decompressed into memory before DuckDB sees any data. DuckDB processes batches incrementally (memory-efficient), but the bottleneck is ArcticDB's upfront materialization.

## Approach: New `LazyRecordBatchIterator` class

Create a new C++ iterator that holds segment metadata (keys) and reads+decodes+converts one segment at a time in `next()`, with a configurable prefetch buffer for latency hiding. This avoids modifying the existing eager read path — it's a parallel code path.

### Key Insight

The existing `DecodeSliceTask` (`async/tasks.cpp:40`) already decodes a single compressed storage segment into an independent `SegmentInMemory` — no shared frame needed. We reuse this pattern: read one segment → decode → convert to Arrow batch → return. The existing `segment_to_arrow_data()` handles conversion of a single-segment `SegmentInMemory` to `sparrow::record_batch`.

## Files to Modify

### C++ Changes

| File | Change |
|------|--------|
| `cpp/arcticdb/arrow/arrow_output_frame.hpp` | Add `LazyRecordBatchIterator` class declaration |
| `cpp/arcticdb/arrow/arrow_output_frame.cpp` | Implement `LazyRecordBatchIterator` — next(), prefetch logic |
| `cpp/arcticdb/version/version_store_api.hpp` | Add `setup_read_context()` method declaration |
| `cpp/arcticdb/version/version_store_api.cpp` | Implement `setup_read_context()` — metadata-only read |
| `cpp/arcticdb/version/python_bindings.cpp` | Bind `LazyRecordBatchIterator` to Python |

### Python Changes

| File | Change |
|------|--------|
| `python/arcticdb/version_store/_store.py` | Add `read_as_lazy_record_batch_iterator()` method |
| `python/arcticdb/version_store/duckdb/arrow_reader.py` | Support both iterator types in `ArcticRecordBatchReader` |
| `python/arcticdb/version_store/duckdb/duckdb.py` | Wire lazy iterator into DuckDB context |
| `python/arcticdb/version_store/library.py` | Add `lazy` parameter to `_read_as_record_batch_reader()` |

### Tests

| File | Change |
|------|--------|
| `python/tests/unit/arcticdb/version_store/duckdb/test_lazy_streaming.py` | New: lazy iterator integration tests |
| `python/tests/unit/arcticdb/version_store/duckdb/test_duckdb.py` | Add lazy mode variants of existing SQL tests |

## Implementation Steps

### Step 1: C++ `LazyRecordBatchIterator` class

In `arrow_output_frame.hpp`, add alongside existing `RecordBatchIterator`:

```cpp
class LazyRecordBatchIterator {
public:
    LazyRecordBatchIterator(
        std::vector<pipelines::SliceAndKey> slice_and_keys,
        StreamDescriptor descriptor,
        std::shared_ptr<stream::StreamSource> store,
        std::shared_ptr<std::unordered_set<std::string>> columns_to_decode,
        size_t prefetch_size = 2
    );

    std::optional<RecordBatchData> next();
    bool has_next() const;
    size_t num_batches() const;
    size_t current_index() const;

private:
    std::vector<pipelines::SliceAndKey> slice_and_keys_;
    StreamDescriptor descriptor_;
    std::shared_ptr<stream::StreamSource> store_;
    std::shared_ptr<std::unordered_set<std::string>> columns_to_decode_;
    size_t prefetch_size_;
    size_t current_index_ = 0;

    // Prefetch: futures for upcoming segments
    std::deque<folly::Future<SegmentInMemory>> prefetch_buffer_;

    folly::Future<SegmentInMemory> read_and_decode_segment(size_t idx);
    void fill_prefetch_buffer();
};
```

### Step 2: Implement `LazyRecordBatchIterator::next()`

In `arrow_output_frame.cpp`:

```
next():
  1. If no more segments and prefetch empty → return nullopt
  2. Pop front future from prefetch_buffer_
  3. .get() → blocks until this ONE segment is read+decoded (prefetch hides latency)
  4. Convert SegmentInMemory → sparrow::record_batch via segment_to_arrow_data()
  5. Extract Arrow C Data structures → return RecordBatchData
  6. fill_prefetch_buffer() → kick off reads for next segments
  7. Increment current_index_
```

`read_and_decode_segment(idx)`:
  - Build `RangesAndKey` from `slice_and_keys_[idx]`
  - Use `store_->batch_read_uncompressed()` with a single key (reuses existing `DecodeSliceTask` pattern)
  - Returns `Future<SegmentAndSlice>` → extract `SegmentInMemory`
  - Or use `read_and_continue()` directly with an inline decode continuation

`fill_prefetch_buffer()`:
  - While `prefetch_buffer_.size() < prefetch_size_` and segments remain beyond current:
    - Launch `read_and_decode_segment(current_index_ + prefetch_buffer_.size())`
    - Push future to back of deque

### Step 3: C++ entry point `setup_read_context()`

Add to `PythonVersionStore` in `version_store_api.hpp/cpp`:

```cpp
struct LazyReadContext {
    VersionedItem versioned_item;
    std::vector<SliceAndKey> slice_and_keys;
    StreamDescriptor descriptor;
};

LazyReadContext PythonVersionStore::setup_read_context(
    const StreamId& stream_id,
    const VersionQuery& version_query,
    const std::shared_ptr<ReadQuery>& read_query,
    const ReadOptions& read_options
);
```

Implementation: calls existing `setup_pipeline_context()` (which reads only the index — cheap I/O) and extracts the `slice_and_keys_` + `descriptor`. Does NOT call `fetch_data()`.

### Step 4: Python bindings

In `python_bindings.cpp`, bind:
- `LazyRecordBatchIterator` with same interface as `RecordBatchIterator` (next, has_next, num_batches, current_index)
- `PythonVersionStore::setup_read_context()` → returns context for creating lazy iterator
- Factory: `create_lazy_iterator(context, store, columns, prefetch_size)` → `LazyRecordBatchIterator`

### Step 5: Python `_store.py` integration

Add `read_as_lazy_record_batch_iterator()` to `NativeVersionStore`:

```python
def read_as_lazy_record_batch_iterator(self, symbol, as_of=None,
    date_range=None, row_range=None, columns=None, prefetch_size=2):
    """Lazy variant — reads segments on-demand with prefetch."""
    version_query, read_options, read_query, _ = self._get_queries(
        as_of=as_of, date_range=date_range, row_range=row_range,
        columns=columns, output_format=OutputFormat.PYARROW)

    # Metadata-only read (cheap)
    context = self.version_store.setup_read_context(
        symbol, version_query, read_query, read_options)

    # Create lazy C++ iterator
    return self.version_store.create_lazy_iterator(
        context.slice_and_keys, context.descriptor,
        columns, prefetch_size)
```

### Step 6: Wire into `Library.sql()` and DuckDB context

In `library.py:_read_as_record_batch_reader()`, add `lazy=True` parameter:

```python
if lazy:
    cpp_iterator = self._nvs.read_as_lazy_record_batch_iterator(
        symbol, as_of=as_of, columns=columns,
        date_range=date_range, row_range=row_range,
        query_builder=query_builder)
else:
    cpp_iterator = self._nvs.read_as_record_batch_iterator(...)

return ArcticRecordBatchReader(cpp_iterator)
```

`ArcticRecordBatchReader` already works with any object that has `.next()` returning `RecordBatchData` — both iterator types are compatible.

### Step 7: Tests

**Integration tests** (`test_lazy_streaming.py`):
- `test_lazy_basic_query` — `SELECT *` returns same data as eager
- `test_lazy_groupby` — GROUP BY with lazy vs eager → same result
- `test_lazy_filter` — WHERE filter → same result
- `test_lazy_with_columns` — Column projection works
- `test_lazy_with_date_range` — Date range pushdown works
- `test_lazy_limit_pushed` — LIMIT pushdown still works with lazy
- `test_lazy_exhausted_iterator` — Re-iteration raises error
- `test_lazy_empty_symbol` — Empty symbol returns empty result

**Memory tests** (same file or separate):
- `test_lazy_memory_groupby` — RSS with lazy << RSS with eager for GROUP BY on large symbol
- `test_lazy_memory_filter` — RSS with lazy << RSS with eager for filtered query

**Correctness**: For each test, run both eager and lazy paths, assert identical results.

## Design Decisions

1. **Separate class, not flag on existing path**: `LazyRecordBatchIterator` is a new class parallel to `RecordBatchIterator`. Avoids touching the existing eager read path (which serves `lib.read()`, write operations, etc.) and minimizes blast radius.

2. **Reuse `DecodeSliceTask` pattern**: The processing pipeline already decodes individual segments independently. We reuse this approach rather than inventing new decode logic.

3. **Prefetch buffer with `std::deque<Future>`**: Hides S3 latency (50-100ms per segment) by reading 2-3 segments ahead. When `next()` is called, the front segment is likely already decoded. Default `prefetch_size=2`.

4. **No `reduce_and_fix_columns`**: This post-processing step operates across all segments for dynamic schema reconciliation. For the lazy path, each segment is self-contained. If dynamic schema is requested, we fall back to the eager path (documented limitation).

5. **GIL release**: `LazyRecordBatchIterator::next()` does C++ I/O and compute — the Python binding wraps it with `py::gil_scoped_release`.

6. **QueryBuilder clauses not supported in lazy mode**: If `query_builder` has clauses (filter, project, aggregate), fall back to eager. The clause pipeline needs all segments for operations like GROUP BY. Column selection and date_range work fine because they're applied at the storage read level, not as clauses.

## Limitations / Future Work

- **Dynamic schema**: Lazy mode requires static schema (all segments have same columns). Dynamic schema falls back to eager.
- **QueryBuilder clauses**: Filter/aggregation clauses require eager read. Only pushdown-via-SQL (our `pushdown.py`) works with lazy.
- **Prefetch is simple**: Fixed-size buffer. Could be adaptive (expand when consumer is fast, shrink when slow).
- **Not available for `lib.read()`**: Only wired into the DuckDB/SQL path initially. Could extend to `lib.read(output_format="pyarrow")` later.

## Verification

1. **Build**: `CMAKE_BUILD_PARALLEL_LEVEL=16 ARCTICDB_PROTOC_VERS=4 ARCTIC_CMAKE_PRESET=linux-debug pip install -ve .`
2. **Unit tests**: `python -m pytest -n 8 python/tests/unit/arcticdb/version_store/duckdb/test_lazy_streaming.py -v`
3. **Existing tests pass**: `python -m pytest -n 8 python/tests/unit/arcticdb/version_store/duckdb/ -v`
4. **Memory validation** (manual): Create 1M row symbol, run `lib.sql("SELECT id1, SUM(v1) FROM sym GROUP BY id1")` with lazy=True, verify RSS stays well below source data size
5. **Format**: `git diff --name-only origin/master..HEAD -- '*.py' | xargs -r -n1 python ./build_tooling/format.py --in-place --type python --file`
6. **Format C++**: `git diff --name-only origin/master..HEAD -- '*.cpp' '*.hpp' | xargs -r -n1 python ./build_tooling/format.py --in-place --type cpp --file`
