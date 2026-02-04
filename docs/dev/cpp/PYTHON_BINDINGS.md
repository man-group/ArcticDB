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

### Module Definition

```cpp
// python_module.cpp

PYBIND11_MODULE(arcticdb_ext, m) {
    m.doc() = "ArcticDB C++ extension module";

    // Register types
    bind_version_store(m);
    bind_library_manager(m);
    bind_storage(m);
    bind_processing(m);

    // Register functions
    m.def("set_config_int", &set_config_int);
    m.def("set_config_string", &set_config_string);
}
```

### Class Bindings

```cpp
// Example: Binding PythonVersionStore

void bind_version_store(py::module& m) {
    py::class_<PythonVersionStore>(m, "VersionStore")
        .def(py::init<>())
        .def("write", &PythonVersionStore::write,
             py::arg("symbol"),
             py::arg("data"),
             py::arg("prune_previous") = false)
        .def("read", &PythonVersionStore::read,
             py::arg("symbol"),
             py::arg("as_of") = py::none())
        .def("list_symbols", &PythonVersionStore::list_symbols)
        // ...
    ;
}
```

## Type Conversions

### Python to C++

```cpp
// python_to_tensor_frame.hpp

// Input type is a variant that can hold different input formats
using InputItem = std::variant<py::tuple, std::vector<RecordBatchData>>;

// Conversion functions handle:
// - pandas DataFrame → pipelines::InputFrame
// - NumPy arrays → Column data
// - Python lists → appropriate C++ containers
// - Scalar values → typed values
```

### C++ to Python

```cpp
// python_handlers.hpp

// Produces:
// - pandas DataFrame (default)
// - PyArrow Table (if requested)
// - NumPy arrays (for specific columns)
```

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

### Location

`cpp/arcticdb/python/gil_lock.hpp`

### Why It Matters

The Python GIL must be:
- **Released** during long C++ operations (for parallelism)
- **Acquired** when calling Python code or manipulating Python objects

### Utilities

```cpp
// Release GIL for C++ work
{
    py::gil_scoped_release release;
    // C++ code runs without holding GIL
    do_heavy_computation();
}
// GIL automatically reacquired

// Acquire GIL for Python interaction
{
    py::gil_scoped_acquire acquire;
    // Safe to call Python
    py::object result = py::eval("1 + 1");
}
```

### Patterns

```cpp
// Typical read operation
py::object read(const std::string& symbol) {
    OutputTensorFrame frame;

    {
        py::gil_scoped_release release;
        // Do all C++ work without GIL
        frame = version_store_.read(symbol);
    }

    // GIL held, safe to create Python objects
    return tensor_frame_to_python(frame);
}
```

## Error Handling

### Exception Translation

```cpp
// C++ exceptions automatically translate to Python

void bind_exceptions(py::module& m) {
    py::register_exception<NoSuchVersionException>(
        m, "NoSuchVersionException", PyExc_KeyError
    );
    py::register_exception<StorageException>(
        m, "StorageException", PyExc_IOError
    );
    py::register_exception<SchemaException>(
        m, "SchemaException", PyExc_ValueError
    );
}
```

### Error Propagation

```cpp
// C++ code
if (!symbol_exists(symbol)) {
    throw NoSuchVersionException(
        "Symbol '{}' not found", symbol
    );
}

// In Python
try:
    lib.read("nonexistent")
except KeyError as e:
    print(f"Symbol not found: {e}")
```

## DataFrame Handling

### Input (Python → C++)

```cpp
// python_to_tensor_frame.hpp

InputTensorFrame process_dataframe(py::object df) {
    // Extract index
    auto index = df.attr("index");

    // Extract columns
    auto columns = df.attr("columns").cast<std::vector<std::string>>();

    // Extract data arrays
    for (const auto& col : columns) {
        auto array = df[col.c_str()].attr("values");
        // Convert numpy array to Column
        process_array(array, col);
    }

    return frame;
}
```

### Output (C++ → Python)

```cpp
// python_handlers.hpp

py::object create_dataframe(const OutputTensorFrame& frame) {
    py::dict data;

    for (const auto& col : frame.columns()) {
        // Convert Column to numpy array
        py::array arr = column_to_numpy(col);
        data[col.name().c_str()] = arr;
    }

    // Create DataFrame
    py::object pd = py::module::import("pandas");
    return pd.attr("DataFrame")(data);
}
```

## NumPy Integration

### Zero-Copy Where Possible

```cpp
// Create numpy array from C++ buffer without copy
py::array_t<double> create_array(const Column& col) {
    return py::array_t<double>(
        {col.row_count()},                    // Shape
        {sizeof(double)},                      // Strides
        reinterpret_cast<double*>(col.data()), // Data pointer
        py::capsule(col.data(), [](void*){})   // Prevent deallocation
    );
}
```

### Type Mapping

| NumPy dtype | ArcticDB DataType |
|-------------|-------------------|
| `np.int64` | `INT64` |
| `np.float64` | `FLOAT64` |
| `np.bool_` | `BOOL8` |
| `np.datetime64[ns]` | `NANOSECONDS_UTC64` |
| `np.object_` (strings) | `UTF_DYNAMIC64` |

## Configuration Interface

### From Python

```python
from arcticdb_ext import set_config_int, set_config_string

# Set cache reload interval
set_config_int("VersionMap.ReloadInterval", 5_000_000_000)

# Set log level
set_config_string("Log.Level", "DEBUG")
```

### C++ Implementation

```cpp
void set_config_int(const std::string& key, int64_t value) {
    ConfigsMap::instance()->set_int(key, value);
}
```

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

### Enable Debug Output

```python
import arcticdb_ext
arcticdb_ext.set_config_string("Log.Level", "DEBUG")
```

### Check Binding

```python
import arcticdb_ext
print(dir(arcticdb_ext))  # List available bindings
help(arcticdb_ext.VersionStore)  # Get docstrings
```

## Related Documentation

- [../ARCHITECTURE.md](../ARCHITECTURE.md) - Overall structure
- [ENTITY.md](ENTITY.md) - C++ types being exposed
