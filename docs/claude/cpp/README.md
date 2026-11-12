# ArcticDB C++ Documentation

> **Note**: This documentation is **owned and maintained by Claude**. It is automatically kept up-to-date as Claude makes changes to the codebase.

This directory contains detailed documentation for the C++ layer of ArcticDB.

## System Documentation

High-level C++ system documentation:

| Document | Description |
|----------|-------------|
| **[CACHING.md](CACHING.md)** | Version map caching system, multi-process behavior |
| **[VERSIONING.md](VERSIONING.md)** | Version chain structure, write/read paths, tombstones |
| **[STORAGE_BACKENDS.md](STORAGE_BACKENDS.md)** | S3, Azure, LMDB, MongoDB backend implementations |

## Module Documentation

Detailed documentation for C++ modules in `cpp/arcticdb/`:

| Module | Documentation | Description |
|--------|---------------|-------------|
| **Entity** | [ENTITY.md](ENTITY.md) | Core data types, keys, type system |
| **Codec** | [CODEC.md](CODEC.md) | Compression, encoding, segment format |
| **Column Store** | [COLUMN_STORE.md](COLUMN_STORE.md) | Columnar data layout, memory management |
| **Pipeline** | [PIPELINE.md](PIPELINE.md) | Read/write data pipelines |
| **Processing** | [PROCESSING.md](PROCESSING.md) | Query processing, clauses, expressions |
| **Stream** | [STREAM.md](STREAM.md) | Data streaming, aggregation |
| **Async** | [ASYNC.md](ASYNC.md) | Task scheduling, thread pools |
| **Python Bindings** | [PYTHON_BINDINGS.md](PYTHON_BINDINGS.md) | pybind11 bindings to Python |

## C++ Code Location

All C++ source code is in `cpp/arcticdb/`:

```
cpp/arcticdb/
├── entity/         # Core types (keys, descriptors, types)
├── codec/          # Compression and encoding
├── column_store/   # Columnar data layout
├── pipeline/       # Read/write pipelines
├── processing/     # Query processing
├── stream/         # Data streaming
├── async/          # Async task management
├── python/         # Python bindings
├── version/        # Version management
├── storage/        # Storage backends
├── util/           # Utilities
└── log/            # Logging
```

## Module Layering

Today every module under `cpp/arcticdb/` can reach every other through `#include`s, so a header
change can recompile almost everything. The direction we are moving towards is:

```
log < util < entity < codec < column_store < [stream + pipeline] < storage < async
    < processing < version < toolbox < python
```

`arrow` sits beside `column_store`/`pipeline` and depends only downward. A module should include
only modules to its left. This is a direction, not a rule the code follows yet.

`build_tooling/check_cpp_layering.py` enforces a ratchet in the `Linting checks` job of `build.yml`.
It collects the module-to-module edges from the includes of production files (test directories and
benchmarks are ignored) and compares them with `build_tooling/cpp_layering_allowlist.txt`:

- A new edge fails the check, with the `file:line` of each include that creates it. Move the code
  to a lower module instead, or, if the dependency is really intended, add the edge.
- An allowlisted edge that no longer exists also fails. When a change removes the last include
  between two modules, delete that line in the same PR, so the allowlist only shrinks.

`python build_tooling/check_cpp_layering.py --update` regenerates the allowlist from the code.

## Quick Reference

### Building C++

```bash
# Configure and build
make build-debug        # or: make build (release)

# Run C++ tests
make test-cpp-debug     # or: make test-cpp (release), FILTER= for gtest_filter
```

### Key Header Files

| Header | Purpose |
|--------|---------|
| `entity/key.hpp` | Key types (VERSION, TABLE_DATA, etc.) |
| `entity/types.hpp` | Data type definitions |
| `codec/segment.hpp` | Segment structure |
| `column_store/memory_segment.hpp` | In-memory segment representation |
| `version/version_map.hpp` | Version cache |
| `processing/clause.hpp` | Query clause interface |

## Related Documentation

- [ARCHITECTURE.md](../ARCHITECTURE.md) - Overall system architecture
- [Python Documentation](../python/README.md) - Python layer documentation
