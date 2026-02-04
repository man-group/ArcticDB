# Storage Adapters

The adapters module (`python/arcticdb/adapters/`) handles storage backend configuration and URI parsing.

## Overview

This module provides:
- URI parsing for different storage backends
- Storage configuration creation
- Backend-specific options handling

## Location

`python/arcticdb/adapters/`

## Architecture

```
URI String (e.g., "s3://s3.amazonaws.com:mybucket?region=us-east-1")
        │
        ▼
┌───────────────────────────────────────┐
│         ArcticLibraryAdapter          │
│                                       │
│  supports_uri() → bool                │
│  config_library → Library config      │
│  add_library() → Library              │
│  get_library() → Library              │
└───────────────────────────────────────┘
        │
        ▼
C++ Storage Configuration (Protobuf)
        │
        ▼
Storage Backend Instance
```

## Adapter Base Class

```python
# python/arcticdb/adapters/arctic_library_adapter.py

class ArcticLibraryAdapter:
    """Base class for storage adapters."""

    @staticmethod
    def supports_uri(uri: str) -> bool:
        """Check if this adapter supports the given URI."""
        raise NotImplementedError

    @property
    def config_library(self):
        """Return the configuration library for this adapter."""
        raise NotImplementedError

    def add_library(self, name: str, options: LibraryOptions) -> Library:
        """Add a library with the given name and options."""

    def get_library(self, name: str) -> Library:
        """Get an existing library by name."""

    def delete_library(self, name: str) -> None:
        """Delete a library."""

    def list_libraries(self) -> List[str]:
        """List available libraries."""
```

## S3 Adapter

### Location

`python/arcticdb/adapters/s3_library_adapter.py`

### URI Format

The S3 URI format uses colon to separate endpoint and bucket:

```
s3://endpoint:bucket?query_params
s3s://endpoint:bucket?query_params  # Force HTTPS
```

**Examples:**

```
# AWS S3
s3://s3.amazonaws.com:my-bucket?region=us-east-1
s3://s3.us-west-2.amazonaws.com:my-bucket?region=us-west-2

# With explicit credentials
s3://s3.amazonaws.com:my-bucket?access=AKID&secret=SECRET&region=us-east-1

# MinIO or S3-compatible (custom endpoint)
s3://localhost:9000:my-bucket?region=us-east-1

# With path prefix
s3://s3.amazonaws.com:my-bucket?region=us-east-1&path_prefix=arcticdb/data

# Force HTTPS
s3s://s3.amazonaws.com:my-bucket?region=us-east-1
```

### ParsedQuery

```python
@dataclass
class ParsedQuery:
    region: str = ""
    access: str = ""                    # Access key
    secret: str = ""                    # Secret key
    path_prefix: str = ""
    aws_auth: AWSAuthMethod = AWSAuthMethod.DEFAULT
    port: Optional[int] = None
    ssl: Optional[bool] = False
    CA_cert_path: Optional[str] = ""
    CA_cert_dir: Optional[str] = ""
```

### Usage

```python
from arcticdb import Arctic

# AWS S3 with default credentials
ac = Arctic("s3://s3.amazonaws.com:my-bucket?region=us-east-1")

# With explicit credentials
ac = Arctic("s3://s3.amazonaws.com:my-bucket?access=AKID&secret=SECRET&region=us-east-1")

# MinIO or S3-compatible
ac = Arctic("s3://localhost:9000:my-bucket?region=us-east-1")
```

### Environment Variables

```bash
AWS_ACCESS_KEY_ID=...
AWS_SECRET_ACCESS_KEY=...
AWS_SESSION_TOKEN=...  # For temporary credentials
AWS_REGION=us-east-1
```

## Azure Adapter

### Location

`python/arcticdb/adapters/azure_library_adapter.py`

### URI Format

Azure URIs use semicolon-separated key=value pairs:

```
azure://Container=container;AccountName=account;AccountKey=key
azure://Container=container;AccountName=account;SharedAccessSignature=token
azure://BlobEndpoint=endpoint;Container=container;AccountName=account;AccountKey=key
```

**Examples:**

```
# With account key
azure://Container=mycontainer;AccountName=myaccount;AccountKey=BASE64KEY==

# With SAS token
azure://Container=mycontainer;AccountName=myaccount;SharedAccessSignature=sv=2021-06-08&ss=b...

# With path prefix
azure://Container=mycontainer;AccountName=myaccount;AccountKey=KEY;Path_prefix=arcticdb/data

# Custom endpoint
azure://BlobEndpoint=https://myaccount.blob.core.windows.net;Container=mycontainer;AccountName=myaccount;AccountKey=KEY
```

### ParsedQuery

```python
@dataclass
class ParsedQuery:
    Container: str = ""
    AccountName: str = ""
    AccountKey: str = ""
    SharedAccessSignature: str = ""
    BlobEndpoint: str = ""
    Path_prefix: str = ""
    CA_cert_path: Optional[str] = None
    CA_cert_dir: Optional[str] = None
```

### Usage

```python
# With account key
ac = Arctic("azure://Container=mycontainer;AccountName=myaccount;AccountKey=BASE64KEY==")

# With SAS token
ac = Arctic("azure://Container=mycontainer;AccountName=myaccount;SharedAccessSignature=sv=2021...")
```

## LMDB Adapter

### Location

`python/arcticdb/adapters/lmdb_library_adapter.py`

### URI Format

```
lmdb://./relative/path
lmdb:///absolute/path
lmdb://path?map_size=10737418240  # 10GB
```

### ParsedQuery

```python
@dataclass
class ParsedQuery:
    map_size: int  # Maximum database size in bytes
```

### Usage

```python
# Relative path
ac = Arctic("lmdb://./my_arctic_db")

# Absolute path
ac = Arctic("lmdb:///home/user/arcticdb")

# With custom map size (10GB)
ac = Arctic("lmdb://./my_db?map_size=10737418240")
```

### Limitations

- Single process writing at a time
- Map size must be set at creation time
- Uses memory-mapped files

## Memory Adapter

### Location

`python/arcticdb/adapters/in_memory_library_adapter.py`

### URI Format

```
mem://
```

### Usage

```python
# In-memory storage (testing)
ac = Arctic("mem://")
lib = ac.create_library("test")
# Data lost when process exits
```

## Creating Custom Adapters

### Step 1: Define ParsedQuery

```python
@dataclass
class MyParsedQuery:
    host: str = ""
    port: int = 0
    database: str = ""
    # ... other options
```

### Step 2: Implement Adapter

```python
class MyStorageAdapter(ArcticLibraryAdapter):
    REGEX = r"myscheme://(?P<host>[^:]+):(?P<port>\d+)/(?P<database>\w+)"

    @staticmethod
    def supports_uri(uri: str) -> bool:
        return uri.startswith("myscheme://")

    def __init__(self, uri: str, encoding_version: EncodingVersion, *args, **kwargs):
        match = re.match(self.REGEX, uri)
        self._host = match["host"]
        self._port = int(match["port"])
        self._database = match["database"]
        self._encoding_version = encoding_version
        super().__init__(uri, self._encoding_version)

    @property
    def config_library(self):
        # Create and return configuration library
        pass
```

### Step 3: Register Adapter

```python
# In arctic.py
def _get_adapter(uri: str, encoding_version) -> ArcticLibraryAdapter:
    adapters = [
        S3LibraryAdapter,
        AzureLibraryAdapter,
        LmdbLibraryAdapter,
        InMemoryLibraryAdapter,
        MyStorageAdapter,  # Add your adapter
    ]
    for adapter_class in adapters:
        if adapter_class.supports_uri(uri):
            return adapter_class(uri, encoding_version)
    raise ValueError(f"Unsupported URI: {uri}")
```

## Key Files

| File | Purpose |
|------|---------|
| `adapters/arctic_library_adapter.py` | Base adapter class |
| `adapters/s3_library_adapter.py` | S3 adapter |
| `adapters/azure_library_adapter.py` | Azure adapter |
| `adapters/lmdb_library_adapter.py` | LMDB adapter |
| `adapters/in_memory_library_adapter.py` | Memory adapter |

## Error Handling

```python
from arcticdb.exceptions import ArcticException

try:
    ac = Arctic("invalid://uri")
except ValueError as e:
    print(f"Unsupported URI scheme: {e}")

try:
    ac = Arctic("s3://invalid")
except ValueError as e:
    print(f"Invalid URI format: {e}")
```

## URI Format Summary

| Backend | Format | Example |
|---------|--------|---------|
| S3 | `s3://endpoint:bucket?params` | `s3://s3.amazonaws.com:mybucket?region=us-east-1` |
| S3 (HTTPS) | `s3s://endpoint:bucket?params` | `s3s://s3.amazonaws.com:mybucket?region=us-east-1` |
| Azure | `azure://key=value;key=value` | `azure://Container=c;AccountName=a;AccountKey=k` |
| LMDB | `lmdb://path` | `lmdb://./mydb` |
| Memory | `mem://` | `mem://` |

## Related Documentation

- [ARCTIC_CLASS.md](ARCTIC_CLASS.md) - Uses adapters
- [../cpp/STORAGE_BACKENDS.md](../cpp/STORAGE_BACKENDS.md) - Backend details
