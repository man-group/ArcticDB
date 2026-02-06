# ArcticDB Caching System

This document describes the version map caching system in ArcticDB, including its configuration, behavior in multi-process scenarios, and troubleshooting.

## Overview

ArcticDB caches version chain information in memory to reduce storage round-trips. The primary cache is the **VersionMap**, which stores version chain entries for recently accessed symbols.

## Version Map Cache

### Location

- **Implementation**: `cpp/arcticdb/version/version_map.hpp`
- **Entry Structure**: `cpp/arcticdb/version/version_map_entry.hpp`

### How It Works

```
┌─────────────────────────────────────────────────────────────────┐
│                        VersionMapImpl                            │
│                                                                  │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │  map_: {StreamId -> VersionMapEntry}                      │   │
│  │                                                           │   │
│  │  "symbol_A" -> Entry(head=v3, keys=[v3,v2,v1], reload_t)  │   │
│  │  "symbol_B" -> Entry(head=v5, keys=[v5,v4], reload_t)     │   │
│  │  ...                                                      │   │
│  └──────────────────────────────────────────────────────────┘   │
│                                                                  │
│  reload_interval_: 2 seconds (default)                           │
│  clock_unsync_tolerance_: 200ms                                  │
└─────────────────────────────────────────────────────────────────┘
```

### Cache Entry Contents

Each `VersionMapEntry` contains:
- `head_`: Pointer to the latest VERSION key
- `keys_`: Deque of version/index keys loaded from storage
- `tombstones_`: Map of individually deleted version IDs
- `tombstone_all_`: Version ID marking that all versions before it are deleted (more efficient than storing thousands of entries in `tombstones_`)
- `last_reload_time_`: Timestamp of last reload from storage
- `load_progress_`: How much of the version chain has been loaded

### Cache Lookup Flow

```cpp
check_reload(store, stream_id, load_strategy)
    │
    ├─► has_cached_entry(stream_id, load_strategy)
    │       │
    │       ├─► Check if entry exists in map_
    │       ├─► Check if cache is fresh: (now - last_reload_time) < reload_interval
    │       └─► Check if entry satisfies load_strategy requirements
    │
    ├─► If cached: return get_entry(stream_id)
    │
    └─► If not cached: storage_reload(store, stream_id, load_strategy)
            │
            ├─► Clear existing entry
            ├─► Set last_reload_time_ = now - clock_unsync_tolerance
            └─► load_via_ref_key() from storage
```

## Configuration

### VersionMap.ReloadInterval

Controls how long cache entries are considered fresh.

```python
from arcticdb_ext import set_config_int

# Set reload interval to 5 seconds (in nanoseconds)
set_config_int("VersionMap.ReloadInterval", 5_000_000_000)

# Disable caching (always reload from storage)
set_config_int("VersionMap.ReloadInterval", 0)

# Very long cache (10 minutes)
set_config_int("VersionMap.ReloadInterval", 600_000_000_000)
```

**Default**: 2 seconds (`2_000_000_000` nanoseconds)

### VersionMap.UnsyncTolerance

Tolerance for clock skew between processes/nodes.

```python
# Set tolerance to 500ms (in nanoseconds)
set_config_int("VersionMap.UnsyncTolerance", 500_000_000)
```

**Default**: 200 milliseconds (`200_000_000` nanoseconds)

### VersionMap.MaxReadRefTrials

Number of retry attempts when reading the VERSION_REF key.

```python
set_config_int("VersionMap.MaxReadRefTrials", 5)
```

**Default**: 2 attempts

## Multi-Process Behavior

### Scenario: Two Processes, Shared Storage

```
Process A                           Process B
─────────                           ─────────
read("sym") → v0
  └─ cache: sym → [v0]
                                    write("sym", data) → v1
read("sym") → v0 (cached!)
  └─ cache still valid

# After cache expires (2 seconds):
read("sym") → v1
  └─ cache refreshed from storage
```

### Reading Specific Versions

When reading a specific version that doesn't exist in the cache:

```
Process A                           Process B
─────────                           ─────────
read("sym") → v0
  └─ cache: sym → [v0]
                                    write("sym", data) → v1
read("sym", as_of=1)
  └─ v1 not in cache
  └─ flush_entry("sym")  ◄── Cache bypass retry
  └─ reload from storage
  └─ return v1 ✓
```

The cache bypass retry mechanism (added in 2024) ensures that reading a specific version will succeed even if the cache is stale.

### Reading "Latest" Version

When reading the latest version, the cache returns what it believes is latest:

```
Process A                           Process B
─────────                           ─────────
read("sym") → v0 (latest)
  └─ cache: sym → [v0]
                                    write("sym", data) → v1
read("sym") → v0 (cached latest)
  └─ cache says v0 is latest
  └─ returns v0 (not v1!)

# This is expected caching behavior
# Wait for cache to expire, or request specific version
```

**Important**: Reading "latest" with a stale cache returns the cached latest version. This is expected caching behavior, not a bug.

## Cache Bypass

### Manual Cache Flush

To force a cache refresh for a specific symbol:

```cpp
// C++ - flush entire cache
version_map()->flush();

// C++ - flush single entry
version_map()->flush_entry(stream_id);
```

### Automatic Cache Bypass

The following operations automatically bypass or refresh the cache:

1. **Version not found**: When requesting a specific version that doesn't exist in cache, the code retries with cache bypass
2. **Write operations**: Writing to a symbol updates the cache entry
3. **Delete operations**: Deleting a version updates the cache entry

## Testing Cache Behavior

### Stress Tests

Cache-related stress tests are in `python/tests/stress/arcticdb/version_store/`:

- `test_stale_version_cache.py` - Tests multi-process cache scenarios
- `test_stress_version_map_compact.py` - Tests concurrent compaction with caching

### Example: Testing Cache Staleness

```python
from multiprocessing import Process
from arcticdb import Arctic
from arcticdb_ext import set_config_int

def writer_process(uri, lib_name, symbol):
    ac = Arctic(uri)
    lib = ac[lib_name]
    lib.write(symbol, pd.DataFrame({"v": [1]}))

def reader_process(uri, lib_name, symbol, cache_interval_ns):
    set_config_int("VersionMap.ReloadInterval", cache_interval_ns)
    ac = Arctic(uri)
    lib = ac[lib_name]

    # First read - warms cache
    v0 = lib.read(symbol)

    # ... writer process writes v1 ...

    # Second read - may use cache
    v_maybe_stale = lib.read(symbol)

    # Read specific version - bypasses stale cache
    v1 = lib.read(symbol, as_of=1)  # Always works
```

## Troubleshooting

### "Version not found" errors in multi-process setup

**Symptoms**: `NoSuchVersionException` when reading a version that exists in storage

**Cause**: Stale cache doesn't include the new version

**Solution**: This should be handled automatically by the cache bypass retry. If still occurring:
1. Check that you're using a recent version of ArcticDB
2. Try setting `VersionMap.ReloadInterval` to 0 to diagnose
3. Ensure storage is consistent (not eventually consistent)

### Stale data when reading "latest"

**Symptoms**: Reading latest returns old data even though new data was written

**Cause**: Expected caching behavior - cache thinks old version is latest

**Solutions**:
1. Wait for cache to expire (default 2 seconds)
2. Read by specific version number if you know it
3. Reduce `VersionMap.ReloadInterval` for faster cache expiration
4. Set `VersionMap.ReloadInterval` to 0 to disable caching (performance impact)

### High storage latency in multi-process setup

**Symptoms**: Many storage round-trips, slow reads

**Cause**: Cache interval too short or caching disabled

**Solution**: Increase `VersionMap.ReloadInterval` for longer cache validity

## Implementation Details

### Key Files

| File | Purpose |
|------|---------|
| `cpp/arcticdb/version/version_map.hpp` | VersionMapImpl class |
| `cpp/arcticdb/version/version_map_entry.hpp` | VersionMapEntry structure |
| `cpp/arcticdb/version/local_versioned_engine.cpp` | Uses version map for lookups |
| `cpp/arcticdb/version/version_functions.hpp` | Version lookup functions |

### Key Functions

| Function | Purpose |
|----------|---------|
| `check_reload()` | Main cache check and reload logic |
| `has_cached_entry()` | Validates cache freshness |
| `storage_reload()` | Loads version chain from storage |
| `flush()` | Clears entire cache |
| `flush_entry()` | Clears cache for one symbol |
| `get_version_to_read()` | Version lookup with cache bypass retry |
