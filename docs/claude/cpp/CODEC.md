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

### String Pool Blocks

The string pool is built in 4 KB `ChunkedBuffer` pages, plus an irregular page for each string longer than a page. Pool
offsets are positions in the pages' concatenation. `encode_string_pool()` (`encode_common.hpp`) re-cuts that concatenation into
blocks of `Codec.StringPoolBlockBytes` (default 256 KiB; 0 keeps one block per page, the older layout) and compresses
each, in V1 and V2. A string can straddle two blocks. `string_pool_encoded_value_blocks()` gives the block count, which
also sizes the V2 header (`calc_required_header_fields_bytes`). The writer makes one transient copy of the pool.
`encode_v1`/`encode_v2` read the setting once (`string_pool_block_bytes()`, values below 4096 are raised to 4096) and
pass it down, so a concurrent `set_config_int` cannot make the header and the blocks disagree. The pool is never cut
into more blocks than it has pages, because the output buffer is sized from the pages (a pool of strings longer than a
block keeps its pages).

Readers decode the pool into one contiguous chunk whatever its block count (`decode_ndarray`, `read_frame.cpp`
`decode_string_pool`), so every client reads either layout. Past LZ4's 64 KiB window a bigger block adds little: on
TPC-H SF1, 256 KiB blocks keep 3% of the per-page header bytes and the pool is within 0.8% of a single block. Tests:
`SegmentStringEncodingTest` in `codec/test/test_codec.cpp`, `test_compat_read_string_pool_blocks` (older clients).

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
