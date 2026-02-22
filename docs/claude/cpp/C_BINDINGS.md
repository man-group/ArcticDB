# C API & Language Bindings

The C bindings module (`cpp/arcticdb/bindings/`) exposes ArcticDB's read path through a stable `extern "C"` API, enabling zero-copy data access from any language with Arrow FFI support (Java, .NET, Excel, Rust, etc.).

## Architecture

```
Language Bindings
Java (Panama FFM)  │ .NET (P/Invoke)  │ Excel (XLL, future)
java/              │ dotnet/           │
────────────────────────────────────────────────
                    │
C API               │  arcticdb_c.h — extern "C", opaque handles
                    │  arcticdb_c.cpp — wraps LocalVersionedEngine
                    │  ArrowArrayStream wrapping LazyRecordBatchIterator
────────────────────────────────────────────────
                    │
Existing C++        │  LocalVersionedEngine → Store → Storage backends
(no changes)        │  LazyRecordBatchIterator → RecordBatchData
```

## Files

| File | Purpose |
|------|---------|
| `bindings/arcticdb_c.h` | Public C API header (the contract for downstream consumers) |
| `bindings/arcticdb_c.cpp` | Implementation wrapping `LocalVersionedEngine` |
| `bindings/arrow_stream.hpp` | `ArrowArrayStream` wrapper for `LazyRecordBatchIterator` |
| `bindings/test_c_api_smoke.cpp` | Standalone smoke test (assert-based) |
| `bindings/test_c_api_stream_smoke.cpp` | GTest: exercises ArrowArrayStream consumption pattern |

## C API Surface

All functions use `extern "C"` linkage with `ARCTICDB_C_API` visibility. Error handling via `ArcticError` out-parameter (code + message buffer).

| Function | Purpose |
|----------|---------|
| `arctic_library_open_lmdb()` | Open LMDB-backed library at a filesystem path |
| `arctic_library_close()` | Destroy library handle |
| `arctic_write_test_data()` | Write synthetic numeric data (test helper) |
| `arctic_read_stream()` | Open `ArcticArrowArrayStream` for a symbol/version |
| `arctic_list_symbols()` | List all symbols (caller frees with `arctic_free_symbols`) |
| `arctic_free_symbols()` | Free symbol list |

## ArrowArrayStream Wrapper

`bindings/arrow_stream.hpp` defines `ArrowArrayStream` (not provided by sparrow) per the [Arrow C Stream Interface spec](https://arrow.apache.org/docs/format/CStreamInterface.html).

### Callbacks

| Callback | Implementation |
|----------|---------------|
| `get_schema` | `empty_record_batch_from_descriptor()` → extract `ArrowSchema` |
| `get_next` | `LazyRecordBatchIterator::next()` → transfer `ArrowArray` ownership |
| `get_last_error` | Return last exception message |
| `release` | Delete `StreamPrivateData` (iterator + descriptor) |

### Consumption Pattern

```c
ArcticArrowArrayStream stream;
arctic_read_stream(lib, "symbol", -1, &stream, &err);

ArrowSchema schema;
stream.get_schema(&stream, &schema);
// inspect schema.n_children, schema.children[i]->name, etc.
schema.release(&schema);

ArrowArray array;
while (stream.get_next(&stream, &array) == 0 && array.release != NULL) {
    // process array.length rows, array.n_children columns
    array.release(&array);
}
stream.release(&stream);
```

## Read Path (C API → LazyRecordBatchIterator)

`arctic_read_stream()` in `arcticdb_c.cpp` replicates the logic from `PythonVersionStore::create_lazy_record_batch_iterator_with_metadata()` without Python dependencies:

1. `get_version_to_read()` — resolve symbol + version query
2. `setup_pipeline_context()` — read index, build `SliceAndKey` vector
3. Sort `slice_and_keys` by (row_range, col_range)
4. `get_column_bitset_in_context()` — populate column bitset for pushdown
5. Build `columns_to_decode` from bitset
6. Construct `LazyRecordBatchIterator` with prefetch
7. `wrap_iterator_as_arrow_stream()` — fill `ArcticArrowArrayStream`

## Opaque Handle

```cpp
struct ArcticLibrary {
    std::shared_ptr<storage::Library> library;
    std::unique_ptr<version_store::LocalVersionedEngine> engine;
};
```

Created by `arctic_library_open_lmdb()` using `lmdb::pack_config()` + `create_storages()` + `LocalVersionedEngine(library)`.

## Build

```bash
# Shared library
cmake --build cpp/out/linux-debug-build --target arcticdb_c

# Tests
cmake --build cpp/out/linux-debug-build --target test_c_api_smoke test_c_api_stream_smoke
```

The `libarcticdb_c.so` is the distributable artifact — downstream languages only need this shared library plus `arcticdb_c.h`.

## Language Bindings

### Java (`java/`)

Uses Java 21 Panama FFM API (preview) for zero-JNI native access.

| File | Purpose |
|------|---------|
| `ArcticNative.java` | Low-level FFM bindings: struct layouts, `dlopen(RTLD_LAZY)` loading, function pointer helpers for ArrowArrayStream callbacks |
| `ArcticLibrary.java` | High-level `AutoCloseable` wrapper: `openLmdb()`, `readStream()`, `listSymbols()`, `writeTestData()` |
| `ArcticReadTest.java` | JUnit 5 integration tests (5 tests) |

Build: `JAVA_HOME=<java21> mvn test -Darcticdb.native.path=<dir-containing-libarcticdb_c.so>`

Key pattern: loads `libarcticdb_c.so` with `dlopen(RTLD_LAZY)` via FFM to avoid resolving unused Python symbols at load time. `SymbolLookup` is backed by `dlsym` calls.

### .NET (`dotnet/`)

Uses P/Invoke (`DllImport`) with `DllImportResolver` for native library path.

| File | Purpose |
|------|---------|
| `ArcticNative.cs` | P/Invoke bindings: `StructLayout` structs, delegate types for Arrow function pointers, `DllImportResolver` |
| `ArcticLibrary.cs` | High-level `IDisposable` wrapper: `OpenLmdb()`, `ReadStream()`, `ListSymbols()`, `WriteTestData()` |
| `ArcticReadTest.cs` | xUnit integration tests (5 tests) |

Build: `ARCTICDB_NATIVE_PATH=<dir> dotnet test`

Key pattern: `Marshal.GetDelegateForFunctionPointer<T>()` converts Arrow function pointers to callable delegates for schema/batch consumption.

## Design Decisions

- **LMDB-only initially** — simplest backend, no credentials. S3/Azure added later via `arctic_library_open_*()`.
- **`ArcticArrowArrayStream`** prefixed to avoid collisions with the standard `ArrowArrayStream` name (which we also define internally in `bindings::ArrowArrayStream`). Layout-compatible via `static_assert` + `reinterpret_cast`.
- **Symbol visibility** — `ARCTICDB_C_API` macro handles `__attribute__((visibility("default")))` since the project compiles with `-fvisibility=hidden`.
- **Python linkage** — `arcticdb_core_static` contains pybind11 code with static constructors that reference Python symbols. `libarcticdb_c.so` links against `Python3::Python` to resolve these at load time. The C API path never calls Python at runtime.
- **CMake link order** — `arcticdb_core_static` and AWS SDK `.a` files are duplicated on the linker line to satisfy the single-pass static archive resolution order.

## Related Documentation

- [ARROW.md](ARROW.md) — LazyRecordBatchIterator, RecordBatchData
- [PYTHON_BINDINGS.md](PYTHON_BINDINGS.md) — pybind11 bindings (the Python-specific entry point)
- [STORAGE_BACKENDS.md](STORAGE_BACKENDS.md) — LMDB and other storage backends
