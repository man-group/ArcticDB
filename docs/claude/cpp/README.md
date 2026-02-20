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
| **Arrow** | [ARROW.md](ARROW.md) | Arrow output frame, record batch iterator |

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
├── arrow/          # Arrow output frames (DuckDB integration)
├── version/        # Version management
├── storage/        # Storage backends
├── util/           # Utilities
└── log/            # Logging
```

## Quick Reference

### Building C++

```bash
# Configure and build
cmake -DTEST=ON --preset linux-debug cpp
cmake --build cpp/out/linux-debug-build

# Run C++ tests
cpp/out/linux-debug-build/arcticdb/test_unit_arcticdb
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
