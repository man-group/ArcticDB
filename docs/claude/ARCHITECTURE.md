# ArcticDB Architecture Overview

## What is ArcticDB?

ArcticDB is a **high-performance, serverless DataFrame database** for Python data science. It provides a Python API backed by a C++ processing engine with columnar storage, compression, and versioning. Data can be stored on S3, Azure Blob, LMDB, MongoDB, or in-memory.

---

## 1. Top-Level Repository Structure

```
ArcticDB/
├── cpp/                    # C++ engine (core data processing)
├── python/                 # Python package and tests
├── docs/                   # Documentation (MkDocs + Doxygen)
├── docker/                 # Docker build configurations
├── build_tooling/          # Code formatting and build scripts
├── static/                 # Static assets
├── .github/                # CI/CD workflows
├── setup.py                # Python build orchestration
├── setup.cfg               # Python package metadata & dependencies
├── pyproject.toml          # Modern Python build config
├── environment-dev.yml     # Conda dev environment
└── README.md, LICENSE.txt  # Project documentation
```

---

## 2. C++ Layer (`cpp/`)

The C++ layer is the performance-critical core, handling all data storage, compression, and query processing.

```
cpp/
├── arcticdb/               # Main C++ source code
│   ├── storage/            # Storage backends (S3, Azure, LMDB, MongoDB, Memory)
│   ├── version/            # Versioning engine, symbol lists, snapshots
│   ├── pipeline/           # Read/write data pipelines
│   ├── processing/         # Query processing (filter, aggregate, project)
│   ├── codec/              # Compression codecs (LZ4, ZSTD, passthrough)
│   ├── column_store/       # Columnar data layout and memory management
│   ├── entity/             # Core types (keys, data types, descriptors)
│   ├── async/              # Async task management
│   ├── stream/             # Streaming data handling
│   ├── arrow/              # Apache Arrow integration
│   ├── python/             # Python bindings (pybind11)
│   ├── util/               # Utilities
│   └── log/                # Logging
├── proto/                  # Protobuf definitions
├── third_party/            # Vendored dependencies
├── vcpkg/                  # vcpkg package manager (submodule)
├── vcpkg.json              # C++ dependency manifest
├── CMakeLists.txt          # Build configuration
└── CMakePresets.json       # Build presets (linux-debug, linux-release, etc.)
```

### C++ Module Breakdown

| Module | Purpose | Key Files |
|--------|---------|-----------|
| **storage/** | Backend abstraction for different storage systems | `s3/`, `azure/`, `lmdb/`, `mongo/`, `memory/`, `storage_factory.cpp` |
| **version/** | Version chain management, symbol lists, snapshots | `local_versioned_engine.cpp`, `version_store_api.cpp`, `symbol_list.cpp` |
| **pipeline/** | Data serialization/deserialization pipelines | `read_frame.cpp`, `write_frame.cpp`, `slicing.cpp` |
| **processing/** | Query execution (filter, project, aggregate) | `clause.cpp`, `expression_node.cpp`, `operation_dispatch.cpp` |
| **codec/** | Data compression and encoding | `codec.cpp`, `lz4.hpp`, `zstd.hpp`, `segment.cpp` |
| **column_store/** | In-memory columnar representation | `memory_segment.cpp`, `column.cpp`, `string_pool.cpp` |
| **entity/** | Core domain types | `key.hpp`, `types.hpp`, `descriptors.hpp` |

---

## 3. Python Layer (`python/`)

The Python layer provides the user-facing API and test infrastructure.

```
python/
├── arcticdb/
│   ├── arctic.py              # Arctic class - top-level entry point
│   ├── version_store/
│   │   ├── library.py         # Library class - main user API (V2)
│   │   ├── _store.py          # NativeVersionStore - V1 API wrapper
│   │   ├── processing.py      # QueryBuilder for filtering/aggregation
│   │   └── _normalization.py  # DataFrame normalization
│   ├── adapters/              # Storage adapter configuration
│   ├── options.py             # LibraryOptions configuration
│   ├── config.py              # Configuration management
│   ├── proto/                 # Generated protobuf bindings
│   ├── toolbox/               # Admin utilities
│   └── util/                  # Python utilities
├── tests/
│   ├── unit/                  # Unit tests
│   ├── integration/           # Integration tests
│   ├── hypothesis/            # Property-based tests
│   ├── stress/                # Stress/performance tests
│   └── nonreg/                # Non-regression tests
├── benchmarks/                # ASV performance benchmarks
└── arcticc/                   # Legacy compatibility layer
```

### Python API Hierarchy

```
Arctic (arctic.py)
  └── create_library() / get_library()
        │
        ▼
Library (version_store/library.py)  ◄── User-facing V2 API
  ├── write(), read(), update(), append()
  ├── delete(), list_symbols(), list_versions()
  └── QueryBuilder for filtering/aggregation
        │
        ▼
NativeVersionStore (version_store/_store.py)  ◄── V1 API / C++ bridge
        │
        ▼
arcticdb_ext (C++ Python bindings)
```

---

## 4. Storage Model & Key Types

ArcticDB stores data as **keys** in the underlying storage. Each key contains a **segment** with either data or references to other keys, forming a tree called the **version chain**.

### Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────────┐
│                         PYTHON LAYER                                │
│  ┌──────────┐    ┌─────────┐    ┌──────────────────┐               │
│  │  Arctic  │───►│ Library │───►│ NativeVersionStore│              │
│  └──────────┘    └─────────┘    └────────┬─────────┘               │
└───────────────────────────────────────────┼─────────────────────────┘
                                            │ pybind11
┌───────────────────────────────────────────┼─────────────────────────┐
│                         C++ LAYER         ▼                         │
│  ┌────────────────────────────────────────────────────────────────┐ │
│  │                    version_store_api                            │ │
│  │                 (local_versioned_engine)                        │ │
│  └───────────────────────────┬────────────────────────────────────┘ │
│                              │                                      │
│  ┌───────────────────────────┼────────────────────────────────────┐ │
│  │                      PIPELINE LAYER                            │ │
│  │  ┌──────────────┐    ┌────┴─────┐    ┌──────────────────────┐  │ │
│  │  │ write_frame  │    │  codec   │    │    read_frame        │  │ │
│  │  │ (serialize)  │───►│ (LZ4/   │───►│   (deserialize)      │  │ │
│  │  └──────────────┘    │  ZSTD)   │    └──────────────────────┘  │ │
│  │                      └──────────┘                              │ │
│  └────────────────────────────────────────────────────────────────┘ │
│                              │                                      │
│  ┌───────────────────────────┼────────────────────────────────────┐ │
│  │                    PROCESSING LAYER                            │ │
│  │       (filter, project, aggregate - pushdown queries)          │ │
│  └───────────────────────────┬────────────────────────────────────┘ │
│                              │                                      │
│  ┌───────────────────────────┼────────────────────────────────────┐ │
│  │                    STORAGE LAYER                               │ │
│  │  ┌─────┐  ┌───────┐  ┌──────┐  ┌─────────┐  ┌────────┐        │ │
│  │  │ S3  │  │ Azure │  │ LMDB │  │ MongoDB │  │ Memory │        │ │
│  │  └─────┘  └───────┘  └──────┘  └─────────┘  └────────┘        │ │
│  └────────────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────────────┘
```

### Version Chain Structure

```
VERSION_REF (symbol "my_data")     ◄── Head pointer (fast latest lookup)
    │
    └── VERSION (v3, latest)
            ├── TABLE_INDEX ──────► TABLE_DATA (rows 0-100K, cols 0-50)
            │                       TABLE_DATA (rows 0-100K, cols 51-100)
            │                       TABLE_DATA (rows 100K-200K, cols 0-50)
            │                       ...
            └── VERSION (v2)
                    ├── TABLE_INDEX ──► TABLE_DATA ...
                    └── VERSION (v1)
                            └── TABLE_INDEX ──► TABLE_DATA ...
```

### Key Types

| Key Type | Class | Purpose |
|----------|-------|---------|
| `VERSION_REF` | REF | Points to latest VERSION for a symbol |
| `VERSION` | ATOM | Contains link to TABLE_INDEX + previous VERSION (linked list) |
| `TABLE_INDEX` | ATOM | Points to TABLE_DATA segments for one version |
| `TABLE_DATA` | ATOM | Leaf nodes with actual compressed data |
| `SNAPSHOT_REF` | REF | Named snapshot pointing to multiple TABLE_INDEX keys |
| `SYMBOL_LIST` | ATOM | Concurrent symbol list for fast `list_symbols()` |
| `TOMBSTONE` | (virtual) | Marks deleted version (inside VERSION segment only) |
| `APPEND_REF` | REF | Head of incomplete append chain |
| `LOCK` | REF | Distributed locking for concurrent writes |

---

## 5. Dependencies

### C++ Dependencies (vcpkg - statically linked)

| Category | Libraries |
|----------|-----------|
| **Storage** | AWS SDK (S3), Azure SDK, MongoDB C++ driver, LMDB |
| **Serialization** | Protobuf 3.21, msgpack-c |
| **Compression** | LZ4, ZSTD, zlib |
| **Data Processing** | Apache Arrow 21, Sparrow 1.4 |
| **Concurrency** | Folly, Boost (interprocess, circular_buffer) |
| **Utilities** | fmt, spdlog, xxHash, pcre2, double-conversion |
| **Crypto/TLS** | OpenSSL 3.3 |
| **Testing** | Google Test, Google Benchmark, RapidCheck |
| **Metrics** | Prometheus-cpp |

### Vendored (`cpp/third_party/`)

| Library | Purpose |
|---------|---------|
| pybind11 | Python C++ bindings |
| lmdb/lmdbxx | LMDB embedded database |
| msgpack-c | MessagePack serialization |
| entt | Entity-component system |
| recycle | Object pooling |
| semimap | Semi-static map |
| Remotery | Profiling |

### Python Runtime Dependencies (`setup.cfg`)

```
pandas <3
numpy
attrs
protobuf >=3.5.0.post1, <7
msgpack >=0.5.0
pyyaml
packaging
pytz
```

**Optional:** `pyarrow`, `polars` (for Arrow output format)

---

## 6. Build System

### Build Flow

```
setup.py
    │
    ├── CompileProto ────► Generate *_pb2.py from .proto files
    │                      (for protobuf 4, 5, 6 compatibility)
    │
    └── CMakeBuild ──────► CMake configure + build
                               │
                               ├── vcpkg ────► Download/build C++ dependencies
                               │
                               └── ninja ────► Compile arcticdb_ext.so
```

### Key Build Commands

```bash
# Editable install (development)
CMAKE_BUILD_PARALLEL_LEVEL=16 ARCTIC_CMAKE_PRESET=linux-debug pip install -ve .

# Build wheel
pip wheel .

# CMake presets available
linux-debug, linux-release, linux-conda-debug, linux-conda-release
windows-cl-debug, windows-cl-release, macos-debug, macos-release
```

### Environment Variables

| Variable | Purpose |
|----------|---------|
| `ARCTIC_CMAKE_PRESET` | Select CMake preset (e.g., `linux-debug`) |
| `CMAKE_BUILD_PARALLEL_LEVEL` | Number of parallel compile jobs |
| `ARCTICDB_PROTOC_VERS` | Protobuf versions to compile (e.g., `4` or `456`) |
| `ARCTICDB_USING_CONDA` | Use conda-forge dependencies instead of vcpkg |
| `ARCTICDB_KEEP_VCPKG_SOURCES` | Retain vcpkg buildtrees after build |
| `VCPKG_BINARY_SOURCES` | Control vcpkg binary caching (`clear` to disable) |

---

## 7. Testing

| Test Type | Location | Framework |
|-----------|----------|-----------|
| Python Unit | `python/tests/unit/` | pytest |
| Python Integration | `python/tests/integration/` | pytest |
| Property-based | `python/tests/hypothesis/` | hypothesis |
| Stress | `python/tests/stress/` | pytest |
| C++ Unit | `cpp/arcticdb/*/test/` | Google Test |
| C++ Benchmarks | `cpp/arcticdb/*/test/benchmark_*.cpp` | Google Benchmark |
| Python Benchmarks | `python/benchmarks/` | ASV |

### Running Tests

```bash
# Python tests
python -m pytest python/tests/unit/
python -m pytest python/tests/integration/

# C++ tests (after building with -DTEST=ON)
cpp/out/linux-debug-build/arcticdb/test_unit_arcticdb --gtest_filter="TestSuite.*"

# Python benchmarks
cd python && python -m asv run HEAD^!
```

---

## 8. Core Functionality Areas

### Write Path

1. **Normalization** - Convert pandas DataFrame to internal representation
2. **Slicing** - Split into row/column slices based on segment size (default 100K rows)
3. **Encoding** - Apply compression (LZ4/ZSTD) to each segment
4. **Storage** - Write TABLE_DATA keys, then TABLE_INDEX, then VERSION

```
DataFrame ──► Normalize ──► Slice ──► Compress ──► Store
                │            │           │           │
                ▼            ▼           ▼           ▼
            Internal      Segments    Encoded     Keys in
            Format        (row/col)   Segments    Storage
```

### Read Path

1. **Version Resolution** - Find VERSION key (via VERSION_REF or explicit version)
2. **Index Lookup** - Read TABLE_INDEX to find required TABLE_DATA keys
3. **Parallel Fetch** - Retrieve and decompress relevant segments
4. **Reconstruction** - Assemble DataFrame from segments

```
Request ──► Resolve Version ──► Read Index ──► Fetch Data ──► Decompress ──► DataFrame
               │                    │              │              │
               ▼                    ▼              ▼              ▼
           VERSION_REF         TABLE_INDEX    TABLE_DATA      Segments
           + VERSION                          (parallel)      reassembled
```

### Query Processing (Pushdown)

ArcticDB supports pushing filter and projection operations to the storage layer, avoiding full data materialization:

- **Filter** - Row filtering pushed to storage layer (`col > 5`)
- **Project** - Column selection (only read needed columns)
- **Aggregate** - Groupby/aggregation with sorted and unsorted paths
- **Expression Engine** - `expression_node.cpp` evaluates filter expressions

```python
# Example: Only reads required columns and filters at storage level
lib.read("symbol", columns=["price", "volume"], query_builder=q.filter(col("price") > 100))
```

### Versioning Operations

| Operation | Description |
|-----------|-------------|
| **write** | Create new version (overwrites symbol) |
| **append** | Add rows to latest version |
| **update** | Modify rows by index range |
| **delete** | Tombstone a version (lazy deletion) |
| **snapshot** | Create named point-in-time reference |
| **prune_previous_versions** | Remove old versions and reclaim storage |

### Concurrency Model

- **No locks for reads or writes** - ArcticDB does not use locks for symbol reads or writes
- **Last writer wins** - Concurrent writes to the same symbol use a last-writer-wins policy. This is guaranteed by writing unique atom keys (data keys, index keys, version keys) first, then updating the non-unique VERSION_REF key. The last writer to update VERSION_REF wins.
- **Concurrent write caveats** - Last-writer-wins can have surprising consequences for modification operations like `append()`. Concurrent appends may appear out of order or one may be dropped. Parallel writes to the same symbol are not recommended.
- **Symbol list** - Lock-free concurrent data structure for `list_symbols()`. LOCK keys are only used for the compaction phase of the symbol list.
- **Async I/O** - Parallel segment fetches during read

---

## 9. Key Source Files Reference

### C++ Entry Points

| File | Purpose |
|------|---------|
| `version/version_store_api.cpp` | Main C++ API exposed to Python |
| `version/local_versioned_engine.cpp` | Core versioned storage engine |
| `pipeline/write_frame.cpp` | DataFrame serialization |
| `pipeline/read_frame.cpp` | DataFrame deserialization |
| `processing/clause.cpp` | Query clause execution |
| `storage/storage_factory.cpp` | Storage backend instantiation |

### Python Entry Points

| File | Purpose |
|------|---------|
| `arcticdb/arctic.py` | `Arctic` class - library management |
| `arcticdb/version_store/library.py` | `Library` class - user API |
| `arcticdb/version_store/_store.py` | `NativeVersionStore` - C++ bridge |
| `arcticdb/version_store/processing.py` | `QueryBuilder` for queries |

### Proto Definitions

| File | Purpose |
|------|---------|
| `cpp/proto/arcticc/pb2/storage.proto` | Library and storage configuration |
| `cpp/proto/arcticc/pb2/descriptors.proto` | Data type descriptors |
| `cpp/proto/arcticc/pb2/encoding.proto` | Segment encoding format |
| `cpp/proto/arcticc/pb2/s3_storage.proto` | S3 storage configuration |

---

## 10. Documentation

- **User Documentation**: `docs/mkdocs/` (MkDocs site)
- **API Reference**: Generated from docstrings
- **C++ Documentation**: `docs/doxygen/` (Doxygen)
- **GitHub Wiki**: Development guides and architecture details
