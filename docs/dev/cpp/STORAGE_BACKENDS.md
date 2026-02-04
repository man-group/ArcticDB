# ArcticDB Storage Backends

This document describes the storage backend architecture in ArcticDB, including supported backends and how to work with them.

## Overview

ArcticDB abstracts storage through a common interface, allowing data to be stored on various backends:

- **S3** - Amazon S3 and compatible object stores (MinIO, etc.)
- **Azure Blob Storage** - Microsoft Azure blob storage
- **LMDB** - Embedded key-value store (local disk)
- **MongoDB** - Document database (legacy, for Arctic v1 migration)
- **Memory** - In-memory storage (testing)

## Architecture

### Storage Abstraction Layer

```
┌─────────────────────────────────────────────────────────────────┐
│                      Version Store API                           │
│              (local_versioned_engine.cpp)                        │
└───────────────────────────┬─────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────────┐
│                         Store Interface                          │
│                       (store.hpp)                                │
│                                                                  │
│   read() / write() / remove() / iterate_type() / key_exists()   │
└───────────────────────────┬─────────────────────────────────────┘
                            │
        ┌───────────────────┼───────────────────┐
        │                   │                   │
        ▼                   ▼                   ▼
┌───────────────┐   ┌───────────────┐   ┌───────────────┐
│  S3 Storage   │   │ Azure Storage │   │ LMDB Storage  │
│  (s3/)        │   │ (azure/)      │   │ (lmdb/)       │
└───────────────┘   └───────────────┘   └───────────────┘
```

### Key Interface

The storage layer has two main abstractions:

1. **Store** (`store.hpp`) - High-level interface inheriting from `StreamSink` and `StreamSource`
2. **Storage** (`storage.hpp`) - Base class for backend implementations

```cpp
// cpp/arcticdb/storage/storage.hpp
// Base class that backends (S3, Azure, LMDB, etc.) inherit from

class Storage {
protected:
    // Override these in backend implementations
    virtual void do_write(Composite<KeySegmentPair>&& kvs) = 0;
    virtual void do_read(Composite<VariantKey>&& ks,
                         const ReadVisitor& visitor,
                         ReadKeyOpts opts) = 0;
    virtual void do_remove(Composite<VariantKey>&& ks,
                           RemoveOpts opts) = 0;
    virtual bool do_key_exists(const VariantKey& key) = 0;
    virtual void do_iterate_type(KeyType key_type,
                                 const IterateTypeVisitor& visitor,
                                 const std::string& prefix) = 0;
};
```

## S3 Storage

### URI Format

```
s3://endpoint:bucket?access=key_id&secret=secret_key&region=us-east-1
s3s://endpoint:bucket?...   # HTTPS variant
```

The format is `s3://endpoint:bucket` where:
- `endpoint` is the S3 endpoint (e.g., `s3.amazonaws.com`, `s3.us-west-2.amazonaws.com`)
- `bucket` is the bucket name (separated by colon, not slash)

### Configuration

```python
from arcticdb import Arctic

# AWS S3 with default credentials (uses AWS credential provider chain)
ac = Arctic("s3://s3.us-east-1.amazonaws.com:my-bucket?aws_auth=true")

# With explicit credentials
ac = Arctic("s3://s3.us-east-1.amazonaws.com:my-bucket?access=AKID&secret=SECRET")

# With path prefix (data stored under prefix in bucket)
ac = Arctic("s3://s3.us-east-1.amazonaws.com:my-bucket?path_prefix=arcticdb/data")

# S3-compatible (MinIO, etc.)
ac = Arctic("s3://localhost:9000:my-bucket?access=minioadmin&secret=minioadmin")
```

### Implementation Details

| File | Purpose |
|------|---------|
| `cpp/arcticdb/storage/s3/s3_storage.cpp` | S3 storage implementation |
| `cpp/arcticdb/storage/s3/s3_api.cpp` | AWS SDK wrapper |
| `cpp/arcticdb/storage/s3/s3_client_wrapper.cpp` | Client management |

### Features

- **Multipart uploads**: Large segments are uploaded in parts
- **Batch operations**: Multiple keys read/written in parallel
- **Retry logic**: Automatic retry on transient failures
- **Path prefix**: Organize data under a prefix within the bucket

## Azure Blob Storage

### URI Format

```
azure://Container=container_name;AccountName=account;AccountKey=key
azure://Container=container_name;AccountName=account;SharedAccessSignature=SAS
```

The format is `azure://key=value;key=value;...` with semicolon-separated parameters.

### Configuration

```python
# With account key
ac = Arctic("azure://Container=mycontainer;AccountName=myaccount;AccountKey=BASE64KEY")

# With SAS token
ac = Arctic("azure://Container=mycontainer;AccountName=myaccount;SharedAccessSignature=TOKEN")

# With path prefix
ac = Arctic("azure://Container=mycontainer;AccountName=myaccount;AccountKey=KEY;Path_prefix=arcticdb/data")

# With CA certificate (Linux only)
ac = Arctic("azure://Container=mycontainer;AccountName=myaccount;AccountKey=KEY;CA_cert_path=/etc/ssl/certs/ca-certificates.crt")
```

### Implementation Details

| File | Purpose |
|------|---------|
| `cpp/arcticdb/storage/azure/azure_storage.cpp` | Azure storage implementation |
| `cpp/arcticdb/storage/azure/azure_client_impl.cpp` | Azure SDK wrapper |

## LMDB Storage

### URI Format

```
lmdb://path/to/database
lmdb:///absolute/path/to/database
```

### Configuration

```python
# Relative path
ac = Arctic("lmdb://./my_arctic_db")

# Absolute path
ac = Arctic("lmdb:///home/user/arcticdb")

# With map size (max database size)
lib = ac.create_library("mylib", library_options=LibraryOptions(
    lmdb_config={"map_size": 10 * 1024**3}  # 10 GB
))
```

### Implementation Details

| File | Purpose |
|------|---------|
| `cpp/arcticdb/storage/lmdb/lmdb_storage.cpp` | LMDB storage implementation |
| `cpp/third_party/lmdbxx/` | LMDB C++ wrapper |

### Limitations

- **Single process**: LMDB doesn't support multiple processes writing simultaneously
- **Map size**: Must be set at creation time, difficult to resize
- **Memory-mapped**: Database size affects virtual memory usage

## MongoDB Storage

### URI Format

```
mongodb://host:port/database
```

### Purpose

MongoDB storage is primarily for migration from Arctic v1:

```python
# For Arctic v1 migration
ac = Arctic("mongodb://localhost:27017/arctic")
```

### Implementation Details

| File | Purpose |
|------|---------|
| `cpp/arcticdb/storage/mongo/mongo_storage.cpp` | MongoDB storage implementation |

## Memory Storage

### URI Format

```
mem://
```

### Purpose

In-memory storage for testing:

```python
ac = Arctic("mem://")
lib = ac.create_library("test")
# Data is lost when process exits
```

### Implementation Details

| File | Purpose |
|------|---------|
| `cpp/arcticdb/storage/memory/memory_storage.cpp` | In-memory storage implementation |

## Storage Factory

### Key Enumeration

```cpp
// cpp/arcticdb/storage/storage_factory.cpp

Storage* create_storage(const LibraryDescriptor& library_desc) {
    switch (library_desc.storage_type()) {
        case StorageType::S3:
            return new S3Storage(...);
        case StorageType::AZURE:
            return new AzureStorage(...);
        case StorageType::LMDB:
            return new LMDBStorage(...);
        // ...
    }
}
```

### URI Parsing

URI parsing is handled by each storage adapter in Python. Each adapter has a `ParsedQuery` class and parsing logic:

```python
# Each adapter in python/arcticdb/adapters/ handles its own URI scheme:
# - s3_library_adapter.py      → s3://, s3s://
# - azure_library_adapter.py   → azure://
# - lmdb_library_adapter.py    → lmdb://
# - gcpxml_library_adapter.py  → gcpxml://
# - mongo_library_adapter.py   → mongodb://
```

## Adding a New Storage Backend

### Steps

1. **Create storage class** in `cpp/arcticdb/storage/<backend>/`
   - Inherit from `Storage` base class
   - Implement required methods

2. **Add protobuf config** in `cpp/proto/arcticc/pb2/<backend>_storage.proto`

3. **Update storage factory** in `cpp/arcticdb/storage/storage_factory.cpp`

4. **Add URI adapter** in `python/arcticdb/adapters/`

5. **Add tests** in `python/tests/` and `cpp/arcticdb/storage/<backend>/test/`

### Required Methods

```cpp
class NewStorage : public Storage {
    void do_write(Composite<KeySegmentPair>&& kvs) override;
    void do_read(Composite<VariantKey>&& ks,
                 const ReadVisitor& visitor) override;
    void do_remove(Composite<VariantKey>&& ks) override;
    bool do_key_exists(const VariantKey& key) override;
    void do_iterate_type(KeyType key_type,
                         const IterateTypeVisitor& visitor) override;
};
```

## Backend-Specific Considerations

### S3

- **Consistency**: S3 provides strong read-after-write consistency
- **Costs**: Consider request costs for high-frequency operations
- **Multipart**: Large objects use multipart upload (default threshold: 5MB)

### Azure

- **Throttling**: Azure may throttle high-frequency requests
- **Tiers**: Consider hot/cool/archive tiers for cost optimization

### LMDB

- **Single writer**: Only one process can write at a time
- **Memory mapping**: Large databases require significant virtual address space
- **Resize**: Cannot easily resize the database after creation

### Testing

Use fixtures in `python/arcticdb/storage_fixtures/`:

```python
# python/tests/conftest.py

@pytest.fixture
def lmdb_storage(tmp_path):
    yield LmdbStorageFixture(tmp_path)

@pytest.fixture
def s3_storage():
    yield S3StorageFixture()  # Uses moto mock
```
