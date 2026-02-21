# Branch Work Log: jb/lang-bindings

## 2026-02-21: C API & ArrowArrayStream Read Path

### What was done
- Created `cpp/arcticdb/bindings/arrow_stream.hpp` — ArrowArrayStream wrapper for LazyRecordBatchIterator
  - Defines ArrowArrayStream struct (not provided by sparrow) per Arrow C Stream Interface spec
  - Implements get_schema, get_next, get_last_error, release callbacks
  - Uses empty_record_batch_from_descriptor() for schema export
- Created `cpp/arcticdb/bindings/arcticdb_c.h` — Public C API header
  - extern "C" with opaque ArcticLibrary handle
  - ARCTICDB_C_API visibility macro for symbol export
  - ArcticArrowArrayStream struct matching Arrow C Stream Interface
  - Functions: open_lmdb, close, write_test_data, read_stream, list_symbols, free_symbols
- Created `cpp/arcticdb/bindings/arcticdb_c.cpp` — C API implementation
  - Wraps LocalVersionedEngine for LMDB backend
  - Read path replicates PythonVersionStore::create_lazy_record_batch_iterator_with_metadata() logic without Python
  - Write uses write_segment() with constructed SegmentInMemory
- Created `cpp/arcticdb/bindings/test_c_api_smoke.cpp` — Standalone smoke test (assert-based)
  - Tests open/close, write+list, read stream, error on missing symbol
- Created `cpp/arcticdb/bindings/test_c_api_stream_smoke.cpp` — GTest smoke test
  - 6 tests: round-trip, missing symbol error, list empty, list after write, specific version, null args
- Modified `cpp/arcticdb/CMakeLists.txt`
  - Added `arcticdb_c` shared library target
  - Added `test_c_api_smoke` and `test_c_api_stream_smoke` test targets

### Build verification
- `libarcticdb_c.so` builds and exports all 6 C API symbols
- Both test executables build and all tests pass
- Existing LazyRecordBatchIterator tests (24) still pass
