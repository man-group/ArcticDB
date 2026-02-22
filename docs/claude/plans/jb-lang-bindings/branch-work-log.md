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

## 2026-02-21: Java and .NET Language Bindings

### What was done
- Created `java/` — Java Panama FFM bindings (Java 21 preview)
  - `ArcticNative.java`: low-level FFM bindings with struct layouts, dlopen(RTLD_LAZY) loading, function pointer invocation helpers for ArrowArrayStream callbacks
  - `ArcticLibrary.java`: high-level AutoCloseable wrapper with openLmdb(), writeTestData(), readStream(), listSymbols()
  - `ArcticReadTest.java`: 5 JUnit 5 integration tests (open/close, write+list, read stream 100×3, versioned reads, missing symbol error)
  - `pom.xml`: Maven project with Java 21 + --enable-preview, JUnit 5, surefire with --enable-native-access
- Created `dotnet/` — .NET P/Invoke bindings (C# 12 / .NET 8)
  - `ArcticNative.cs`: P/Invoke DllImport bindings with StructLayout matching C structs, delegate types for Arrow function pointers, DllImportResolver for native library path
  - `ArcticLibrary.cs`: high-level IDisposable wrapper with OpenLmdb(), WriteTestData(), ReadStream(), ListSymbols()
  - `ArcticReadTest.cs`: 5 xUnit integration tests (same coverage as Java)
  - Solution with `ArcticDB.csproj` (library) + `ArcticDB.Tests.csproj` (tests)
- Fixed `cpp/arcticdb/CMakeLists.txt` — arcticdb_c link issues
  - Added duplicate arcticdb_core_static + AWS SDK libs to fix single-pass linker symbol resolution
  - Added `find_package(Python3)` + link against `Python3::Python` to resolve Python symbols from static constructors in arcticdb_core_static

### Build verification
- Java: 5/5 tests pass (`JAVA_HOME=java21 mvn test -Darcticdb.native.path=...`)
- .NET: 5/5 tests pass (`DOTNET_VERSION=8 dotnet test` with `ARCTICDB_NATIVE_PATH=...`)

## 2026-02-22: Documentation Updates

### What was done
- Updated `docs/claude/cpp/C_BINDINGS.md` — added Java and .NET binding sections with file tables, build commands, key patterns; updated architecture diagram; updated design decisions with Python linkage and CMake link order notes
- Updated `docs/claude/ARCHITECTURE.md` — added `java/`, `dotnet/`, `bindings/` to directory structure; added language bindings layer to architecture diagram; added bindings module to C++ module table; added Java/dotnet to testing table
- Created `docs/mkdocs/docs/tutorials/language_bindings.md` — user-facing tutorial covering prerequisites, setup, usage examples, and test commands for both Java and .NET
- Updated `docs/mkdocs/mkdocs.yml` — added Language Bindings tutorial to nav
