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

## Fork Handling

`PYBIND11_MODULE` registers `pthread_atfork` handlers on non-Windows platforms. `fork()` duplicates only the calling thread, so the folly pools and the storage SDKs' background threads do not exist in the child, and any mutex they held stays locked forever.

| Handler | Phase | Purpose |
|---------|-------|---------|
| `warn_about_fork` | prepare (parent) | Warn that inherited ArcticDB objects must not be used in the child, if pool threads exist |
| `SingleThreadMutexHolder::reset_mutex` | child | Leak and replace the pybind entry mutex, which may have been locked by a thread that did not survive |
| `reinit_scheduler` | child | `TaskScheduler::reattach_instance()` — leak the inherited scheduler and construct a new one with live threads |
| `reinit_lmdb_warning` | child | Reset `LmdbStorage::times_path_opened` so the child does not inherit the parent's open counts |
| `register_python_handler_data_factory` | child | Replace the `PythonHandlerData` factory, which holds a `py::handle` and atomic refcounts from the parent |

The child handlers all leak rather than destroy inherited state, because running those destructors would join threads that do not exist.

`warn_about_fork` is the only prepare handler, and the only one compiled conditionally, on `PY_VERSION_HEX >= ARCTICDB_PY_FORK_DEPRECATED_HEX` (3.12) — the CPython version that began raising its own `DeprecationWarning` for forking a multi-threaded process. That warning is invisible in practice: it is attributed to `multiprocessing/popen_fork.py` rather than `__main__`, where the default filters would show it.

It runs in the parent deliberately. Logging from a child handler can hang, because the sink mutex may be inherited locked and an `async_logger` configured with `async_overflow_policy::block` has no flusher thread in the child. It is suppressible with the `Fork.WarnOnFork` config and fires at most once per process via a `std::atomic_flag`. The flag is not cleared in the child, so a child that forks again is silent.

### Gating the warning on live threads

Importing `arcticdb`, constructing an `Arctic`, calling `get_library()` and calling `list_symbols()` all start zero pool threads; the first real read or write starts them. So the warning is gated on `async::io_pool_thread_started`, a latch in `task_scheduler.hpp` set from `InstrumentedNamedFactory::newThread`. That factory is the only choke point that catches every IO path — a great many call sites reach the executor directly via `.via(&async::io_executor())` rather than through `submit_io_task`.

A latch is exact rather than approximate here, because of how the pools retire threads:

| Pool | Idle reclamation |
|------|------------------|
| IO | None. `IOThreadPoolExecutor` is constructed with `minThreads == maxThreads`, and each thread owns an `EventBase`. Threads observed flat at 161 across 150 s idle. |
| CPU | Yes. `FLAGS_dynamic_cputhreadpoolexecutor` is true, so `minThreads` is 0 and idle workers exit after `FLAGS_threadtimeout_ms` (60 s). Observed dropping 20 → 1, then holding at a floor of 1 indefinitely. |

Neither pool is ever shut down mid-life. The only stop is at interpreter exit, `Py_AtExit(shutdown_globals)` → `TaskScheduler::stop_active_threads()`, followed by static destruction of `instance_` running `~TaskSchedulerPtrWrapper`. So once an IO thread exists the process has ArcticDB threads until it exits, and the latch cannot be stale-true in the process that set it.

It *is* stale-true in a child, where `reinit_scheduler` has installed a fresh scheduler with no threads. That is harmless only because `warned_about_fork` is also inherited set. Anything that later clears `warned_about_fork` in the child must clear the latch too.

Two cases still warn where the user can do little about it. A script doing ArcticDB IO at module level, outside the `if __name__ == "__main__"` guard, starts threads in the forkserver process, which then warns as it forks each worker. And `subprocess.run(..., preexec_fn=...)` warns because disabling CPython's `vfork` optimisation makes it a real `fork()` with live threads; a prepare handler cannot tell fork-for-exec from fork-for-work. Plain `subprocess.run` and the `spawn` start method are silent because they use `vfork`, which does not run `pthread_atfork` handlers.

Registering the handler through `os.register_at_fork(before=...)` instead would not change any of this: its coverage is identical across `os.fork`, all three start methods, and both `subprocess` forms.

Note that `TaskScheduler::forked_`, `is_forked()` and `set_forked()` are dead: nothing calls `set_forked(true)`, so the `reattach_instance()` branch in `LocalVersionedEngine`'s constructor never runs. Setting that flag from a fork handler would reattach the scheduler a second time and leak another `TaskScheduler`. `re_init()` is dead for the same reason, and with it `set_active_threads()` and `set_max_threads()`, which nothing else calls.

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
