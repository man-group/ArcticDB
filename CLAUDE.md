# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

ArcticDB is a high-performance, serverless DataFrame database for the Python Data Science ecosystem. It provides a Python API backed by a C++ data-processing and compression engine, supporting S3, LMDB, Azure Blob Storage, and MongoDB backends.

## Claude-Maintained Documentation

Technical documentation in `docs/claude/` is **owned and maintained by Claude**. This documentation is automatically kept up-to-date as Claude makes changes to the codebase. Always consult these documents when working on related areas.

### Core Documentation

| Document | Content |
|----------|---------|
| [docs/claude/README.md](docs/claude/README.md) | Documentation index |
| [docs/claude/ARCHITECTURE.md](docs/claude/ARCHITECTURE.md) | Overall architecture, repository structure, build system |

### C++ Documentation (`docs/claude/cpp/`)

| Document | Content |
|----------|---------|
| [CACHING.md](docs/claude/cpp/CACHING.md) | Version map caching system, multi-process behavior |
| [VERSIONING.md](docs/claude/cpp/VERSIONING.md) | Version chain structure, write/read paths, tombstones |
| [STORAGE_BACKENDS.md](docs/claude/cpp/STORAGE_BACKENDS.md) | S3, Azure, LMDB, MongoDB backend implementations |
| [ENTITY.md](docs/claude/cpp/ENTITY.md) | Core data types, keys, type system |
| [CODEC.md](docs/claude/cpp/CODEC.md) | Compression, encoding, segment format |
| [COLUMN_STORE.md](docs/claude/cpp/COLUMN_STORE.md) | Columnar data layout, memory management |
| [PIPELINE.md](docs/claude/cpp/PIPELINE.md) | Read/write data pipelines |
| [PROCESSING.md](docs/claude/cpp/PROCESSING.md) | Query processing, clauses, expressions |
| [STREAM.md](docs/claude/cpp/STREAM.md) | Data streaming, aggregation |
| [ASYNC.md](docs/claude/cpp/ASYNC.md) | Task scheduling, thread pools |
| [PYTHON_BINDINGS.md](docs/claude/cpp/PYTHON_BINDINGS.md) | pybind11 bindings to Python

### Python Module Documentation (`docs/claude/python/`)

| Document | Content |
|----------|---------|
| [ARCTIC_CLASS.md](docs/claude/python/ARCTIC_CLASS.md) | Arctic class, library management, URI parsing |
| [LIBRARY_API.md](docs/claude/python/LIBRARY_API.md) | Library class V2 API (read/write/update) |
| [NATIVE_VERSION_STORE.md](docs/claude/python/NATIVE_VERSION_STORE.md) | V1 API, C++ bridge |
| [QUERY_PROCESSING.md](docs/claude/python/QUERY_PROCESSING.md) | QueryBuilder, expressions |
| [NORMALIZATION.md](docs/claude/python/NORMALIZATION.md) | DataFrame normalization |
| [ADAPTERS.md](docs/claude/python/ADAPTERS.md) | Storage adapters |
| [TOOLBOX.md](docs/claude/python/TOOLBOX.md) | Library inspection tools |

**Important**: When making changes to architecture, caching, versioning, storage backends, or any C++/Python modules, Claude will update the corresponding documentation to keep it current.

## User-Specific Settings

Check `CLAUDE_USER_SETTINGS.md` (git-ignored) for user-specific configuration:
- Python virtual environment paths (Claude should use its own venvs, not user's)
- Preferred CMake presets for debug/release/profiling builds

## Build Commands

### Building the Python Wheel

```bash
# Build with a specific CMake preset (limit parallelism to avoid overloading the system)
CMAKE_BUILD_PARALLEL_LEVEL=16 ARCTIC_CMAKE_PRESET=linux-debug pip install -ve .
```

Note: Limit `CMAKE_BUILD_PARALLEL_LEVEL` to min(16, nproc) to avoid memory pressure during compilation.

### Building on Man Linux VMs

The vcpkg-based build requires certain system packages that may not be installed by default:

```bash
# Required system packages for vcpkg build
sudo apt install pkg-config flex bison libsasl2-dev -y
```

Use Pegasus for Python environment management:

```bash
# Create a Python 3.11 environment
pegasus create -d 311-1 /turbo/<username>/pyenvs/arcticdb-dev
source /turbo/<username>/pyenvs/arcticdb-dev/bin/activate

# Initialize git submodules (required for vcpkg)
git submodule update --init --recursive

# Build with linux-debug preset (limit parallelism)
CMAKE_BUILD_PARALLEL_LEVEL=16 ARCTIC_CMAKE_PRESET=linux-debug pip install -ve .
```

**Note**: The protobuf 5 compilation step may fail due to `grpcio-tools>=1.68.1` not being available in the internal Pegasus registry (only 1.56.2). This is non-fatal - protobuf 4 is compiled successfully and the package works correctly.

### Building a Wheel

```bash
# Build wheel (use ARCTICDB_PROTOC_VERS=4 to skip protobuf 5 on Man VMs)
ARCTICDB_PROTOC_VERS=4 CMAKE_BUILD_PARALLEL_LEVEL=16 ARCTIC_CMAKE_PRESET=linux-debug pip wheel . --no-deps -w dist/
```

The wheel will be created in `dist/arcticdb-<version>-cp311-cp311-linux_x86_64.whl`.

### CMake Presets

Key presets in `cpp/CMakePresets.json`:
- `linux-debug` / `linux-release` - Linux with vcpkg
- `linux-conda-debug` / `linux-conda-release` - Linux with conda-forge deps (set `ARCTICDB_USING_CONDA=1`)
- `windows-cl-debug` / `windows-cl-release` - Windows with MSVC
- `macos-debug` / `macos-release` - macOS

User-specific presets can be defined in `cpp/CMakeUserPresets.json` (git-ignored).

## Git Submodules

The project uses several git submodules. **Do not directly edit files inside submodule directories** - instead update the submodule reference.

### Submodule Locations

| Submodule | Path | Purpose |
|-----------|------|---------|
| vcpkg | `cpp/vcpkg` | Package manager with custom ports (e.g., `arcticdb-sparrow`) |
| pybind11 | `cpp/third_party/pybind11` | Python bindings |
| lmdb | `cpp/third_party/lmdb` | LMDB storage backend |
| lmdbxx | `cpp/third_party/lmdbxx` | C++ wrapper for LMDB |
| recycle | `cpp/third_party/recycle` | Memory recycling |
| rapidcheck | `cpp/third_party/rapidcheck` | Property-based testing |
| entt | `cpp/third_party/entt` | Entity component system |

### Upgrading a Dependency via vcpkg Submodule

When upgrading a dependency like `sparrow` that has a custom port in vcpkg:

1. **Fetch and checkout the vcpkg commit** containing the new version:
   ```bash
   cd cpp/vcpkg
   git fetch origin
   git log --oneline origin/master | grep -i <package-name>  # Find the commit
   git checkout <commit-hash>
   cd ../..
   ```

2. **Update the version override** in `cpp/vcpkg.json`:
   ```json
   "overrides": [
     { "name": "arcticdb-sparrow", "version": "X.Y.Z" }
   ]
   ```

3. **Update conda environment** in `environment-dev.yml` if applicable

4. **Rebuild** - vcpkg will fetch the new version on next build

### Building C++ Tests

Use the preset from `CLAUDE_USER_SETTINGS.md` (or `linux-debug` as default):

```bash
cmake -DTEST=ON --preset <preset> cpp
cmake --build cpp/out/<preset>-build --target test_unit_arcticdb

# Run a single test
cpp/out/<preset>-build/arcticdb/test_unit_arcticdb --gtest_filter="TestSuite.TestName"
```

## Running Python Tests

```bash
# Run all tests
python -m pytest python/tests

# Run a single test file
python -m pytest python/tests/unit/arcticdb/test_arctic.py

# Run a specific test
python -m pytest python/tests/unit/arcticdb/test_arctic.py::test_function_name
```

## Benchmarking

### C++ Benchmarks (Google Benchmark)

```bash
cmake -DTEST=ON --preset <preset> cpp
cmake --build cpp/out/<preset>-build --target benchmarks

# Run specific benchmarks
cpp/out/<preset>-build/arcticdb/benchmarks --benchmark_filter=<regex>
```

Benchmark sources are in `cpp/arcticdb/*/test/benchmark_*.cpp`.

### Python Benchmarks (ASV)

ASV benchmarks live in `python/benchmarks/`. Requires `asv` and `virtualenv` installed.

```bash
cd python
python -m asv run -v --show-stderr HEAD^!              # Benchmark current commit
python -m asv run -v --show-stderr --bench <regex>     # Run subset matching regex
python -m asv run --python=$(which python) -v          # Use current env (faster)
```

See: [ASV Benchmarks Wiki](https://github.com/man-group/ArcticDB/wiki/Dev:-ASV-Benchmarks)

## Storage Model

ArcticDB stores data as **keys** in the underlying storage backend. Each key contains a segment with either data or references to other keys, forming a tree structure called the **version chain**.

### Key Types (defined in `cpp/arcticdb/entity/key.hpp`)

- **VERSION_REF**: Head of the version chain for a symbol, points to the latest VERSION key
- **VERSION**: Contains a link to TABLE_INDEX for this version plus link to previous version (forming a linked list) 
- **TABLE_INDEX**: Points to TABLE_DATA keys containing the actual data segments
- **TABLE_DATA**: Leaf nodes containing compressed data for a row and columns slice from the original dataframe
- **TOMBSTONE**: Marks a deleted version (exists only inside VERSION key segments, not standalone)
- **TOMBSTONE_ALL**: Marks all versions before its version as deleted (exists only inside VERSION key segments, not standalone)
- **SNAPSHOT_REF**: References multiple TABLE_INDEX keys for a snapshot
- **SYMBOL_LIST**: Contains symbol list modifications (adds and removes) to form a concurrent data structure for fast `list_symbols()` operations

### Version Chain Structure

```
VERSION_REF (symbol)
    └── VERSION (latest, v3)
            ├── TABLE_INDEX ──► TABLE_DATA (actual data)
            └── VERSION (v2)
                    ├── TABLE_INDEX ──► TABLE_DATA
                    └── VERSION (v1)
                            └── TABLE_INDEX ──► TABLE_DATA
```

## Code Architecture

### Python Layer (`python/arcticdb/`)

- `arctic.py` - `Arctic` class: top-level library management, URI parsing, library creation
- `version_store/library.py` - `Library` class: main user-facing V2 API for read/write/update operations
- `version_store/_store.py` - `NativeVersionStore`: V1 legacy API wrapping C++ bindings. It is called by the V2 API

### C++ Layer (`cpp/arcticdb/`)

- `*/python_bindings.cpp` - Python bindings for C++ methods
- `version/` - Version management, symbol lists, snapshots
  - `local_versioned_engine.cpp` - Core versioned storage engine
  - `version_store_api.cpp` - Main C++ API exposed to Python
- `storage/` - Storage backend implementations
  - `s3/`, `lmdb/`, `azure/`, `mongo/`, `memory/` - Backend-specific code
  - `library_manager.cpp` - Library configuration and management
- `pipeline/` - Read/write pipeline for data processing
  - `read_frame.cpp` / `write_frame.cpp` - Data serialization/deserialization
- `processing/` - Query processing
  - `clause.cpp` - Query clause execution (filter, project, aggregate)
  - `expression_node.cpp` - Expression tree for query builder
  - `read_and_schedule_processing()` - Entrypoint method for reading with processing
- `codec/` - Compression and encoding (LZ4, ZSTD)
- `column_store/` - Column-oriented data layout and memory management
- `entity/` - Core data types (keys, data types, error codes)

## Key Development Guidelines

### Backwards Compatibility

- Data written by newer clients should be readable by older clients - document breaking changes clearly
- API changes affecting V1 or V2 public APIs must be highlighted in PR descriptions

### Code Style

Code style is enforced by `./build_tooling/format.py`. **Always run the formatter after making code changes:**

```bash
# Format all code
python ./build_tooling/format.py --in-place --type all
```
