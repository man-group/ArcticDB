# Codec Module

The codec module (`cpp/arcticdb/codec/`) handles data compression, encoding, and the segment format.

## Overview

This module is responsible for:
- Segment structure and serialization
- Compression (LZ4, ZSTD, passthrough)
- V1 and V2 encoding formats
- Data encoding/decoding pipelines

## Segment Structure

### What is a Segment?

A segment is the fundamental unit of storage in ArcticDB. It contains:
- Header with metadata
- Column data (compressed)
- String pool (for string columns)
- Optional index data

### Segment Layout

```
┌─────────────────────────────────────────────────┐
│                  HEADER                         │
│  - Magic number                                 │
│  - Encoding version (V1 or V2)                  │
│  - Field descriptors                            │
│  - Row count                                    │
└─────────────────────────────────────────────────┘
│                 COLUMN DATA                     │
│  ┌─────────────────────────────────────────┐   │
│  │ Column 0: [compressed data]             │   │
│  ├─────────────────────────────────────────┤   │
│  │ Column 1: [compressed data]             │   │
│  ├─────────────────────────────────────────┤   │
│  │ ...                                     │   │
│  └─────────────────────────────────────────┘   │
└─────────────────────────────────────────────────┘
│                 STRING POOL                     │
│  (Variable-length strings stored separately)   │
└─────────────────────────────────────────────────┘
```

### Key Files

| File | Purpose |
|------|---------|
| `segment.hpp` | Segment class definition |
| `segment.cpp` | Segment implementation |
| `segment_header.hpp` | Header structure |

## Compression

### Supported Codecs

| Codec | ID | Description | Use Case |
|-------|-----|-------------|----------|
| LZ4 | `lz4` | Fast compression | Default, balanced speed/ratio |
| ZSTD | `zstd` | High compression | Better ratio, slower |
| Passthrough | `pass` | No compression | Already compressed data |

### Codec Selection

The `Codec` enum in `cpp/arcticdb/storage/memory_layout.hpp` defines: `UNKNOWN`, `ZSTD`, `PFOR` (integers), `LZ4` (default), and `PASS` (passthrough).

### Compression Interface

Encoding/decoding functions in `cpp/arcticdb/codec/codec.cpp`:
- `encode()` - Compress column data into an EncodedField
- `decode()` - Decompress EncodedField back to ColumnData

## Encoding Versions

### V1 Encoding

Original encoding format:
- Simple column-by-column storage
- LZ4 or ZSTD compression per column
- Good for general data

Location: `cpp/arcticdb/codec/encode_v1.cpp`

### V2 Encoding

**Note: V2 Encoding is experimental and not currently used by any clients.**

Newer, more sophisticated encoding:
- Shape encoding for sparse data
- Better handling of repeated values
- Sub-codecs (PFOR, delta) for numeric data

Location: `cpp/arcticdb/codec/encode_v2.cpp`

### Format Selection

`EncodingVersion` enum in `cpp/arcticdb/codec/segment_header.hpp` defines `V1` (0) and `V2` (1). Configured via `LibraryOptions.encoding_version`.

## Encoding Pipeline

### Write Path

```
Raw Data
    │
    ▼
┌─────────────────────┐
│  Type Coercion      │  ← Ensure consistent types
└──────────┬──────────┘
           │
           ▼
┌─────────────────────┐
│  Block Encoding     │  ← Split into blocks
└──────────┬──────────┘
           │
           ▼
┌─────────────────────┐
│  Compression        │  ← LZ4/ZSTD/PFOR/etc.
└──────────┬──────────┘
           │
           ▼
┌─────────────────────┐
│  Segment Assembly   │  ← Build final segment
└─────────────────────┘
```

### Read Path

```
Segment from Storage
    │
    ▼
┌─────────────────────┐
│  Header Parse       │  ← Read metadata
└──────────┬──────────┘
           │
           ▼
┌─────────────────────┐
│  Decompression      │  ← Uncompress blocks
└──────────┬──────────┘
           │
           ▼
┌─────────────────────┐
│  Type Promotion     │  ← Widen types if needed
└──────────┬──────────┘
           │
           ▼
Raw Data
```

## Key Classes

### Segment

`Segment` class in `cpp/arcticdb/codec/segment.hpp` provides access to:
- `header()` - Segment metadata (encoding version, field descriptors, row count)
- `buffer()` - Compressed data buffer
- `fields()` - Field/column information

### EncodedField

`EncodedField` represents a compressed column with a descriptor, shapes (for V2), and values (compressed data blocks).

### Buffer

`Buffer` class manages memory for segment data with `data()`, `size()`, and `resize()` methods.

## Configuration Options

Encoding version is configured via `LibraryOptions.encoding_version`. Segment row size (default 100,000 rows) is configured via `segment_row_size` parameter in write operations.

## Key Files

| File | Purpose |
|------|---------|
| `codec.cpp` | Main encode/decode entry points |
| `codec.hpp` | Codec interface definitions |
| `encode_v1.cpp` | V1 encoding implementation |
| `encode_v2.cpp` | V2 encoding implementation |
| `segment.cpp` | Segment class |
| `segment_header.hpp` | Segment header structure |
| `slice_data_sink.hpp` | Buffer management |

## Performance Considerations

### LZ4 vs ZSTD

| Aspect | LZ4 | ZSTD |
|--------|-----|------|
| Compression Speed | Very fast | Moderate |
| Decompression Speed | Very fast | Fast |
| Compression Ratio | Good | Better |
| CPU Usage | Low | Moderate |
| Recommended For | Real-time, large data | Archival, smaller data |

### Block Size

Larger blocks = better compression ratio but more memory during decompression.

## Related Documentation

- [ENTITY.md](ENTITY.md) - Data types being encoded
- [COLUMN_STORE.md](COLUMN_STORE.md) - In-memory representation
- [PIPELINE.md](PIPELINE.md) - How codec fits in read/write
