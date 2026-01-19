# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

ArcticDB is a high-performance, serverless DataFrame database for the Python Data Science ecosystem. It provides a Python API backed by a C++ data-processing and compression engine, supporting S3, LMDB, Azure Blob Storage, and MongoDB backends.

## User-Specific Settings

Check `CLAUDE_USER_SETTINGS.md` (git-ignored) for user-specific configuration:
- Python virtual environment paths (Claude should use its own venvs, not user's)
- Preferred CMake presets for debug/release/profiling builds

## Build Commands

### Building the Python Wheel

```bash
# Build with a specific CMake preset
ARCTIC_CMAKE_PRESET=linux-debug pip install -ve .
```

### CMake Presets

Key presets in `cpp/CMakePresets.json`:
- `linux-debug` / `linux-release` - Linux with vcpkg
- `linux-conda-debug` / `linux-conda-release` - Linux with conda-forge deps (set `ARCTICDB_USING_CONDA=1`)
- `windows-cl-debug` / `windows-cl-release` - Windows with MSVC
- `macos-debug` / `macos-release` - macOS

User-specific presets can be defined in `cpp/CMakeUserPresets.json` (git-ignored).

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

- Code style enforced by `./build_tooling/format.py`
