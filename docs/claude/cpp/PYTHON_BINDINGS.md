# Python Bindings Module

The Python bindings module (`cpp/arcticdb/python/`) exposes C++ functionality to Python via pybind11.

## Overview

This module provides:
- pybind11 bindings for C++ classes and functions
- Type conversions between Python and C++
- GIL (Global Interpreter Lock) management
- Python-specific error handling

## Architecture

```
Python Layer                          C++ Layer
─────────────                         ─────────────

arcticdb.Library
    │
    ▼
NativeVersionStore
    │
    ▼ pybind11
────────────────────────────────────────────────
    │
    ▼
arcticdb_ext module                   python_module.cpp
    │
    ├── PythonVersionStore ◄────────► version_store_api.cpp
    ├── Library management  ◄────────► library_manager.cpp
    └── Query processing    ◄────────► processing/clause.cpp
```

## Module Structure

### Location

`cpp/arcticdb/python/`

### Files

| File | Purpose |
|------|---------|
| `python_module.cpp` | Main module definition |
| `python_bindings_common.cpp` | Common binding utilities |
| `python_handlers.hpp` | Type conversion handlers |
| `gil_lock.hpp` | GIL management utilities |
| `python_to_tensor_frame.hpp` | DataFrame conversion |
| `python_utils.hpp` | Python utility functions |

Note: Bindings are distributed across modules. Each module directory (e.g., `async/`, `codec/`, `version/`) may contain its own `python_bindings.hpp` or `python_bindings.cpp` file.

## Binding Definitions

The main module is defined in `cpp/arcticdb/python/python_module.cpp` using `PYBIND11_MODULE(arcticdb_ext, m)`. It registers types (`bind_version_store`, `bind_library_manager`, etc.) and configuration functions.

Bindings are distributed across modules - each module directory may contain its own `python_bindings.hpp` or `python_bindings.cpp`. Class bindings use pybind11's `py::class_<>` with method definitions via `.def()`.

## Type Conversions

### Python to C++

Conversions in `cpp/arcticdb/python/python_to_tensor_frame.hpp` handle pandas DataFrame → `InputFrame`, NumPy arrays → Column data, Python lists → C++ containers, and scalar values → typed values.

### C++ to Python

Conversions in `cpp/arcticdb/python/python_handlers.hpp` produce pandas DataFrame (default), PyArrow Table (if requested), or NumPy arrays.

### Type Mapping

| Python Type | C++ Type |
|-------------|----------|
| `int` | `int64_t` |
| `float` | `double` |
| `str` | `std::string` |
| `bool` | `bool` |
| `datetime` | `timestamp` (int64_t nanoseconds) |
| `np.ndarray` | `Column` / `Buffer` |
| `pd.DataFrame` | `pipelines::InputFrame` |
| `None` | `std::nullopt` / `std::monostate` |

## GIL Management

**Warning: Operate on the GIL with extreme care.** Incorrect GIL handling can produce very surprising and hard-to-test bugs.

### Location

`cpp/arcticdb/python/gil_lock.hpp`

### Why It Matters

The Python GIL must be:
- **Released** during long C++ operations (for parallelism)
- **Acquired** when calling Python code or manipulating Python objects

### Utilities

Use `py::gil_scoped_release` for long C++ operations and `py::gil_scoped_acquire` when calling Python code. Typical pattern: release GIL, do C++ work, reacquire GIL, convert to Python objects.

## Error Handling

C++ exceptions are registered with pybind11 and automatically translate to Python exceptions:
- `NoSuchVersionException` → `KeyError`
- `StorageException` → `IOError`
- `SchemaException` → `ValueError`

Exception registration is done via `py::register_exception<>()` in the binding code.

## DataFrame Handling

### Input (Python → C++)

`process_dataframe()` in `python_to_tensor_frame.hpp` extracts index and columns from pandas DataFrame, converts NumPy arrays to Column data.

### Output (C++ → Python)

`create_dataframe()` in `python_handlers.hpp` converts `OutputTensorFrame` columns to NumPy arrays and constructs a pandas DataFrame.

## NumPy Integration

### Zero-Copy Where Possible

For large numeric arrays, `py::array_t<>` can wrap C++ buffers without copying using `py::capsule` for memory management.

### Type Mapping

| NumPy dtype | ArcticDB DataType |
|-------------|-------------------|
| `np.int64` | `INT64` |
| `np.float64` | `FLOAT64` |
| `np.bool_` | `BOOL8` |
| `np.datetime64[ns]` | `NANOSECONDS_UTC64` |
| `np.object_` (strings) | `UTF_DYNAMIC64` |

## Configuration Interface

Configuration functions `set_config_int()` and `set_config_string()` are exposed to Python via `arcticdb_ext`. These call into `ConfigsMap::instance()` on the C++ side.

## Key Files

| File | Purpose |
|------|---------|
| `python_module.cpp` | Module entry point |
| `python_bindings.cpp` | Binding definitions |
| `python_handlers.hpp` | Type handlers |
| `gil_lock.hpp` | GIL utilities |
| `python_to_tensor_frame.hpp` | Input conversion |
| `python_utils.hpp` | Utility functions |

## Performance Considerations

### GIL Contention

- Release GIL during long operations
- Minimize Python object creation in hot paths
- Batch Python calls where possible

### Memory Copies

- Prefer zero-copy for large arrays
- Use `py::array_t` with external data
- Be careful with string data (often requires copy)

### Callback Overhead

- C++ → Python callbacks are expensive
- Batch results instead of per-row callbacks
- Consider progress callbacks carefully

## Debugging

Enable debug logging: `arcticdb_ext.set_config_string("Log.Level", "DEBUG")`. Inspect bindings with `dir(arcticdb_ext)` and `help(arcticdb_ext.VersionStore)`.

## Related Documentation

- [../ARCHITECTURE.md](../ARCHITECTURE.md) - Overall structure
- [ENTITY.md](ENTITY.md) - C++ types being exposed
