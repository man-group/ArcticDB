# Arctic Class

The `Arctic` class (`python/arcticdb/arctic.py`) is the top-level entry point for ArcticDB.

## Overview

The Arctic class provides:
- Connection to storage backends via URI
- Library creation and management
- Storage adapter resolution

## Location

`python/arcticdb/arctic.py`

## Usage

### Basic Connection

```python
from arcticdb import Arctic

# Connect to LMDB (local disk)
ac = Arctic("lmdb://./my_arctic_db")

# Connect to S3
ac = Arctic("s3://my-bucket/prefix?region=us-east-1")

# Connect to Azure
ac = Arctic("azure://container/prefix?account=myaccount&key=...")
```

### Library Management

```python
# Create a new library
lib = ac.create_library("my_library")

# Get existing library
lib = ac.get_library("my_library")
# or use shorthand
lib = ac["my_library"]

# List all libraries
libraries = ac.list_libraries()

# Delete a library
ac.delete_library("my_library")
```

## Class Definition

`Arctic` class in `python/arcticdb/arctic.py` provides:
- `__init__(uri, encoding_version, output_format=PANDAS, arrow_string_format_default=LARGE_STRING)` - Initialize connection with default output format and Arrow string format for all libraries
- `create_library(name, library_options, enterprise_library_options, output_format=None, arrow_string_format_default=None)` - Create a new library with optional per-library format overrides
- `get_library(name, create_if_missing, library_options, output_format=None, arrow_string_format_default=None)` - Get existing library with optional per-library format overrides
- `delete_library(name)` - Delete a library and all its data
- `list_libraries()` - List all library names
- `__getitem__(name)` - Shorthand for `get_library()`

## URI Formats

### LMDB (Local Disk)

```
lmdb://./relative/path
lmdb:///absolute/path
```

### S3

```
s3://bucket/prefix?region=us-east-1
s3://bucket/prefix?access=KEY&secret=SECRET&region=us-east-1
s3://bucket/prefix?endpoint=http://localhost:9000  # MinIO
```

### Azure Blob Storage

```
azure://container/prefix?account=myaccount&key=KEY
azure://container/prefix?account=myaccount&sas_token=TOKEN
azure://container/prefix?Connection_String=CONN_STRING
```

### MongoDB (Legacy)

```
mongodb://host:port/database
```

### In-Memory (Testing)

```
mem://
```

## Library Options

```python
from arcticdb import LibraryOptions

opts = LibraryOptions()

# Set encoding version
opts.encoding_version = 2

# Set dynamic schema (allows schema changes)
opts.dynamic_schema = True

# Set deduplication
opts.dedup = True

# Create library with options
lib = ac.create_library("my_lib", library_options=opts)
```

## URI Parsing

### How It Works

```
URI: "s3://bucket/prefix?region=us-east-1"
         │       │           │
         │       │           └── Query parameters
         │       └── Path prefix
         └── Bucket name

Parsed by: S3LibraryAdapter.parse_uri()
Returns: S3LibraryAdapter instance
```

### Adapter Resolution

The URI scheme (s3, s3s, azure, lmdb, mongodb, mem) determines which adapter class handles the connection. Adapters are defined in `python/arcticdb/adapters/`.

## Configuration

### Environment Variables

```python
import os

# AWS credentials (used by S3 adapter)
os.environ["AWS_ACCESS_KEY_ID"] = "..."
os.environ["AWS_SECRET_ACCESS_KEY"] = "..."
os.environ["AWS_REGION"] = "us-east-1"

# Azure credentials
os.environ["AZURE_STORAGE_ACCOUNT"] = "..."
os.environ["AZURE_STORAGE_KEY"] = "..."
```

### Encoding Version

```python
# Version 1: Original encoding
ac = Arctic("lmdb://./db", encoding_version=1)

# Version 2: Improved encoding (default)
ac = Arctic("lmdb://./db", encoding_version=2)
```

## Error Handling

```python
from arcticdb.exceptions import LibraryNotFound, ArcticException

try:
    lib = ac.get_library("nonexistent")
except LibraryNotFound:
    print("Library does not exist")

try:
    ac.create_library("existing_lib")
except ArcticException:
    print("Library already exists")
```

## Thread Safety

- `Arctic` instances are thread-safe for read operations
- Library creation/deletion should be serialized
- Individual `Library` instances have their own thread safety guarantees

## Implementation Details

The `Arctic` class uses lazy initialization for the adapter (created on first access). Libraries may be cached to avoid repeated lookups.

### RuntimeOptions Propagation

`Arctic.__init__` stores `output_format` and `arrow_string_format_default` as instance defaults. These are cascaded to each `Library` via `RuntimeOptions`:

```
Arctic(output_format=PYARROW)
  └─ get_library("lib") / create_library("lib")
       └─ Library._runtime_options = RuntimeOptions(output_format=PYARROW)
            └─ lib.read("sym")  →  uses PYARROW unless overridden per-call
```

Per-library overrides: `get_library(output_format=POLARS)` and `create_library(output_format=POLARS)` override the Arctic-level default. Per-call overrides (`lib.read(output_format=...)`) override the library-level default. Resolution uses `OutputFormat.resolve()` for case-insensitive string compatibility.

## DuckDB SQL Integration

### `sql(query, output_format=None)`

Only supports `SHOW DATABASES` — returns libraries grouped by database prefix. Raises `ValueError` for other queries (use `Library.sql()` for data queries).

```python
result = arctic.sql("SHOW DATABASES")
# Returns: database_name | library_name
```

### `duckdb(connection=None)` → `ArcticDuckDBContext`

Context manager for cross-library SQL queries. Optional `connection` parameter accepts an external `duckdb.DuckDBPyConnection` — if provided, ArcticDB registers symbols into it but does NOT close it on `__exit__`.

```python
with arctic.duckdb() as ddb:
    ddb.register_symbol("market_data", "trades")
    ddb.register_symbol("reference_data", "securities")
    result = ddb.sql("SELECT ... FROM trades JOIN securities ...")

# With external connection (for joining with non-ArcticDB data)
conn = duckdb.connect()
with arctic.duckdb(connection=conn) as ddb:
    ddb.register_symbol("market_data", "trades")
    # conn remains open after context exits
```

See [DUCKDB.md](DUCKDB.md) for full details.

## Key Files

| File | Purpose |
|------|---------|
| `arctic.py` | Arctic class definition |
| `options.py` | LibraryOptions class |
| `config.py` | Configuration management |
| `adapters/` | Storage adapter implementations |

## Related Documentation

- [LIBRARY_API.md](LIBRARY_API.md) - Library class returned by Arctic
- [DUCKDB.md](DUCKDB.md) - DuckDB SQL integration details
- [ADAPTERS.md](ADAPTERS.md) - Storage adapter details
- [../cpp/STORAGE_BACKENDS.md](../cpp/STORAGE_BACKENDS.md) - Backend configurations
