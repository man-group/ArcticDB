# Query Statistics

> **Warning**: The Query Statistics API is unstable and not governed by ArcticDB's semantic versioning. It may change or be removed in future versions without notice.

ArcticDB provides a Query Statistics API that allows you to collect and analyze performance metrics for operations performed on your data stores.
This can be useful for debugging, performance optimization, and understanding the resource usage of your queries.
The Query Statistics feature uses a global container to store all measurements. This means statistics from all Python threads in your application will be collected in the same structure. 
For this reason, enabling or disabling Query Statistics in individual threads can lead to unpredictable results and should be avoided. It's best to treat Query Statistics as an application-wide setting rather than a thread-specific one.

Currently only S3 backend is supported. Support for other storage backend is pending.

## Basic Usage

There are two ways to enable query statistics collection:

### Using the Context Manager

The context manager automatically enables statistics at the beginning of a block and disables them at the end.
Please note that recursion is not supported:

```python
import arcticdb as adb
import arcticdb.toolbox.query_stats as qs

arctic = adb.Arctic(f"s3://{HOST}:{BUCKET}?access={ACCESS}&secret={SECRET}")
lib = arctic["library_name"]

# Collect statistics for specific operations
with qs.query_stats():
    lib.list_symbols()
    
# Get the collected statistics
stats = qs.get_query_stats()
print(stats)
```

### Using Enable/Disable Explicitly

For more control, you can manually enable and disable statistics collection:

```python
import arcticdb as adb
import arcticdb.toolbox.query_stats as qs

arctic = adb.Arctic(f"s3://{HOST}:{BUCKET}?access={ACCESS}&secret={SECRET}")
lib = arctic["library_name"]

# Enable statistics collection
qs.enable()

# Perform operations you want to measure
lib.write("symbol", data)
lib.read("symbol")

# Get the collected statistics
stats = qs.get_query_stats()
print(stats)

# Optionally, reset the statistics
qs.reset_stats()

# Continue with more operations
lib.list_symbols()

# Get new statistics
stats = qs.get_query_stats()
print(stats)

# Disable statistics collection when done
qs.disable()
```

## Output Structure

The statistics are returned as a nested dictionary organized by:
- Key type (e.g., `SYMBOL_LIST`, `TABLE_DATA`, `VERSION_REF`)
- Operation group (currently `storage_ops` only)
- Task type (e.g., `S3_ListObjectsV2`, `Encode`, `Decode`)

Each task contains measurements like:
- `count`: Number of times the operation was performed
- `total_time_ms`: Total execution time in milliseconds
- For data operations, additional metrics like `compressed_size_bytes` and `uncompressed_size_bytes`

Example output:

```python
{
    "SYMBOL_LIST": {
        "storage_ops": {
            "S3_ListObjectsV2": {
                "total_time_ms": 83,
                "count": 3
            }
        }
    },
    "VERSION_REF": {
        "storage_ops": {
            "S3_GetObject": {
                "total_time_ms": 50,
                "count": 3
            },
            "Decode": {
                "count": 3,
                "uncompressed_size_bytes": 300,
                "compressed_size_bytes": 1827
            }
        }
    }
}
```

## Common Use Cases

### Measure how many IOs have been made in one read

```python
import arcticdb as adb
import arcticdb.toolbox.query_stats as qs
import pandas as pd

arctic = adb.Arctic(f"s3://{HOST}:{BUCKET}?access={ACCESS}&secret={SECRET}")
lib = arctic["library_name"]

# Measure read performance
lib.read("test_symbol")
read_stats = qs.get_query_stats()
```

## Resetting Statistics

You can clear all collected statistics using `reset_stats()`:

```python
qs.reset_stats()
```

This is useful when you want to isolate statistics for specific operations or when you're done with one phase of analysis and want to start fresh.

## Note
Running an enormous number of operations with Query Statistics enabled risks overflowing the internal counters. To avoid this issue, please reset statistics periodically or keep sessions with Query Statistics enabled relatively short.
```