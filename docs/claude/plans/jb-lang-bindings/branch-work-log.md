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

## 2026-02-22: Rust Bindings (read_dataframe)

### What was done
- Updated `rust/Cargo.toml` — added `serde` dependency with `derive` feature
- Updated `rust/src/lib.rs`:
  - Added `ColumnData` enum (Float64, Int64) with `Serialize` derive and `#[serde(untagged)]`
  - Added `DataFrame` struct with column_names, column_types, columns, num_rows
  - Added `read_dataframe(symbol, version)` method that reads Arrow schema formats and copies data from `ArrowArray.children[i].buffers[1]`
  - Supports float64/float32/int64/int32 and timestamp formats

## 2026-02-22: Excel Integration (Gateway + Add-in)

### What was done
- Created `excel/gateway/` — Rust HTTP gateway server using axum
  - `Cargo.toml`: deps on arcticdb, axum 0.7, tokio, serde, tower-http (cors), clap
  - `build.rs`: same native lib linking as rust/build.rs
  - `src/main.rs`: 6 endpoints (health, open/close library, list symbols, read data, write test)
  - Row-oriented DataFrame JSON wire format for Excel's Range.values compatibility
  - CORS permissive, configurable port (default 8787, --port or ARCTICDB_GATEWAY_PORT env)
- Created `excel/addin/` — Office.js Excel add-in (TypeScript)
  - `manifest.xml`: shared runtime, ARCTICDB namespace, ribbon tab with Connect/Refresh buttons
  - `functions.json`: static custom functions metadata (READ, LIST)
  - `src/functions/functions.ts`: ARCTICDB.READ(symbol, version?), ARCTICDB.LIST() custom functions
  - `src/taskpane/taskpane.{html,ts}`: server URL, library open/close, symbol list, click-to-load, write test data
  - `src/commands/commands.ts`: ribbon Refresh command (full recalc)
  - `src/globals.d.ts`: type declarations for Office.js, CustomFunctions, Excel APIs
  - webpack config for 3 entry points + HTML + copy manifest/metadata

### Build verification
- Gateway: `cargo build` succeeds, all 6 curl endpoints tested end-to-end
- Add-in: `npm install && npm run build` succeeds (webpack, 0 errors)
