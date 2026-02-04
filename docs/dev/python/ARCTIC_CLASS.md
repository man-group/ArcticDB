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

```python
class Arctic:
    def __init__(
        self,
        uri: str,
        encoding_version: int = DEFAULT_ENCODING_VERSION
    ):
        """
        Initialize Arctic connection.

        Args:
            uri: Storage URI (e.g., "s3://bucket/prefix", "lmdb://./path")
            encoding_version: Data encoding version (1 or 2)
        """

    def create_library(
        self,
        name: str,
        library_options: Optional[LibraryOptions] = None
    ) -> Library:
        """Create a new library."""

    def get_library(
        self,
        name: str,
        create_if_missing: bool = False
    ) -> Library:
        """Get an existing library."""

    def delete_library(self, name: str) -> None:
        """Delete a library and all its data."""

    def list_libraries(self) -> List[str]:
        """List all library names."""

    def __getitem__(self, name: str) -> Library:
        """Shorthand for get_library()."""
```

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

```python
# In arctic.py
def _resolve_adapter(uri: str) -> ArcticLibraryAdapter:
    scheme = urlparse(uri).scheme

    adapters = {
        "s3": S3LibraryAdapter,
        "s3s": S3LibraryAdapter,  # HTTPS
        "azure": AzureLibraryAdapter,
        "lmdb": LmdbLibraryAdapter,
        "mongodb": MongoLibraryAdapter,
        "mem": MemoryLibraryAdapter,
    }

    adapter_class = adapters.get(scheme)
    return adapter_class.from_uri(uri)
```

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

### Lazy Initialization

```python
class Arctic:
    def __init__(self, uri: str):
        self._uri = uri
        self._adapter = None  # Lazy initialization

    @property
    def adapter(self):
        if self._adapter is None:
            self._adapter = _resolve_adapter(self._uri)
        return self._adapter
```

### Library Caching

Libraries may be cached to avoid repeated lookups:

```python
# First call - creates/fetches library
lib1 = ac["my_lib"]

# Second call - may return cached instance
lib2 = ac["my_lib"]
```

## Key Files

| File | Purpose |
|------|---------|
| `arctic.py` | Arctic class definition |
| `options.py` | LibraryOptions class |
| `config.py` | Configuration management |
| `adapters/` | Storage adapter implementations |

## Related Documentation

- [LIBRARY_API.md](LIBRARY_API.md) - Library class returned by Arctic
- [ADAPTERS.md](ADAPTERS.md) - Storage adapter details
- [../cpp/STORAGE_BACKENDS.md](../cpp/STORAGE_BACKENDS.md) - Backend configurations
