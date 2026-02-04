# ArcticDB Developer Documentation

> **Note**: This documentation is **owned and maintained by Claude**. It is automatically kept up-to-date as Claude makes changes to the codebase. Human developers should treat this as authoritative technical reference.

This directory contains technical documentation for developing and maintaining ArcticDB. These documents are intended for contributors and maintainers.

## Documentation Index

### Architecture & Design

- **[ARCHITECTURE.md](ARCHITECTURE.md)** - Comprehensive architecture overview
  - Repository structure
  - C++ and Python layer organization
  - Storage model and key types
  - Build system details
  - Core functionality areas

### C++ Documentation (`cpp/`)

Detailed documentation for the C++ layer (`cpp/arcticdb/`):

| Document | Description |
|----------|-------------|
| [cpp/README.md](cpp/README.md) | C++ module index |
| [cpp/CACHING.md](cpp/CACHING.md) | Version map caching system |
| [cpp/VERSIONING.md](cpp/VERSIONING.md) | Version chain management |
| [cpp/STORAGE_BACKENDS.md](cpp/STORAGE_BACKENDS.md) | Storage backend implementations |
| [cpp/ENTITY.md](cpp/ENTITY.md) | Core data types, keys, type system |
| [cpp/CODEC.md](cpp/CODEC.md) | Compression, encoding, segment format |
| [cpp/COLUMN_STORE.md](cpp/COLUMN_STORE.md) | Columnar data layout, memory management |
| [cpp/PIPELINE.md](cpp/PIPELINE.md) | Read/write data pipelines |
| [cpp/PROCESSING.md](cpp/PROCESSING.md) | Query processing, clauses, expressions |
| [cpp/STREAM.md](cpp/STREAM.md) | Data streaming, aggregation |
| [cpp/ASYNC.md](cpp/ASYNC.md) | Task scheduling, thread pools |
| [cpp/PYTHON_BINDINGS.md](cpp/PYTHON_BINDINGS.md) | pybind11 bindings to Python |

### Python Documentation (`python/`)

Detailed documentation for the Python layer (`python/arcticdb/`):

| Document | Description |
|----------|-------------|
| [python/README.md](python/README.md) | Python module index |
| [python/ARCTIC_CLASS.md](python/ARCTIC_CLASS.md) | Arctic class, library management |
| [python/LIBRARY_API.md](python/LIBRARY_API.md) | Library class V2 API |
| [python/NATIVE_VERSION_STORE.md](python/NATIVE_VERSION_STORE.md) | V1 API, C++ bridge |
| [python/QUERY_PROCESSING.md](python/QUERY_PROCESSING.md) | QueryBuilder, expressions |
| [python/NORMALIZATION.md](python/NORMALIZATION.md) | DataFrame normalization |
| [python/ADAPTERS.md](python/ADAPTERS.md) | Storage adapters |
| [python/TOOLBOX.md](python/TOOLBOX.md) | Library inspection tools |

### Quick Reference

| Topic | File | Description |
|-------|------|-------------|
| Overall Architecture | [ARCHITECTURE.md](ARCHITECTURE.md) | Start here for high-level understanding |
| Caching System | [cpp/CACHING.md](cpp/CACHING.md) | Version map cache internals |
| Version Management | [cpp/VERSIONING.md](cpp/VERSIONING.md) | How versioning works |
| Storage Backends | [cpp/STORAGE_BACKENDS.md](cpp/STORAGE_BACKENDS.md) | C++ backend implementations |
| C++ Modules | [cpp/README.md](cpp/README.md) | C++ layer details |
| Python Modules | [python/README.md](python/README.md) | Python layer details |

## Directory Structure

```
docs/claude/
├── README.md              # This file
├── ARCHITECTURE.md        # System architecture overview
├── cpp/                   # C++ documentation
│   ├── README.md          # C++ module index
│   ├── CACHING.md         # Version map caching
│   ├── VERSIONING.md      # Version chain management
│   ├── STORAGE_BACKENDS.md# Storage backends
│   ├── ENTITY.md          # Core data types
│   ├── CODEC.md           # Compression/encoding
│   ├── COLUMN_STORE.md    # Columnar data layout
│   ├── PIPELINE.md        # Read/write pipelines
│   ├── PROCESSING.md      # Query processing
│   ├── STREAM.md          # Data streaming
│   ├── ASYNC.md           # Task scheduling
│   └── PYTHON_BINDINGS.md # Python bindings
└── python/                # Python documentation
    ├── README.md          # Python module index
    ├── ARCTIC_CLASS.md    # Arctic class
    ├── LIBRARY_API.md     # Library V2 API
    ├── NATIVE_VERSION_STORE.md
    ├── QUERY_PROCESSING.md
    ├── NORMALIZATION.md
    ├── ADAPTERS.md
    └── TOOLBOX.md
```

## Documentation Maintenance

This documentation is maintained by Claude. When Claude makes significant changes to ArcticDB, it will automatically update the relevant documentation:

1. **Architecture changes** - [ARCHITECTURE.md](ARCHITECTURE.md)
2. **Cache behavior changes** - [cpp/CACHING.md](cpp/CACHING.md)
3. **Version chain changes** - [cpp/VERSIONING.md](cpp/VERSIONING.md)
4. **Storage backend changes** - [cpp/STORAGE_BACKENDS.md](cpp/STORAGE_BACKENDS.md)
5. **C++ module changes** - Relevant file in [cpp/](cpp/)
6. **Python module changes** - Relevant file in [python/](python/)

## Related Documentation

- **User Documentation**: `docs/mkdocs/` - MkDocs-based user documentation
- **API Reference**: Generated from docstrings
- **C++ API Documentation**: `docs/doxygen/` - Doxygen-generated C++ docs
- **GitHub Wiki**: https://github.com/man-group/ArcticDB/wiki
