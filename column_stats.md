# Column Stats

Column stats are per-row-slice statistics (currently min/max values) for specified columns. They are stored separately from the data and can be used in the future to optimize query filtering by skipping row-slices that don't match filter criteria.

## Python API

The column stats API is exposed through `NativeVersionStore` (V1 API):

### `create_column_stats(symbol, column_stats, as_of=None)`

Creates column statistics for specified columns. If column stats already exist for the symbol, the new stats are merged with the existing ones.

```python
lib.create_column_stats("my_symbol", {"col_1": {"MINMAX"}, "col_2": {"MINMAX"}})
```

**Parameters:**
- `symbol`: The symbol name
- `column_stats`: A dict mapping column names to sets of stat types. Currently only `"MINMAX"` is supported.
- `as_of`: Optional version query (version number, timestamp, or snapshot name)

### `read_column_stats(symbol, as_of=None)`

Returns a DataFrame containing the column statistics for each row-slice.

```python
stats_df = lib.read_column_stats("my_symbol")
```

The returned DataFrame has:
- `start_index`: The start timestamp of each row-slice
- `end_index`: The end timestamp of each row-slice
- `v1.0_MIN(col_name)`: Minimum value for the column in that row-slice
- `v1.0_MAX(col_name)`: Maximum value for the column in that row-slice

### `get_column_stats_info(symbol, as_of=None)`

Returns the column stats dictionary showing which stats have been created.

```python
info = lib.get_column_stats_info("my_symbol")
# Returns: {"col_1": {"MINMAX"}, "col_2": {"MINMAX"}}
```

### `drop_column_stats(symbol, column_stats=None, as_of=None)`

Drops column statistics. If `column_stats` is None, drops all stats. Otherwise drops only the specified stats.

```python
# Drop all column stats
lib.drop_column_stats("my_symbol")

# Drop only col_2 stats
lib.drop_column_stats("my_symbol", {"col_2": {"MINMAX"}})
```

## Storage Structure

### Key Type

Column stats are stored using the `COLUMN_STATS` key type (value 25 in the KeyType enum). The key is derived from the associated TABLE_INDEX key:

- Same `stream_id` (symbol name)
- Same `version_id`
- Same `creation_ts` (from the TABLE_INDEX key)
- Same `content_hash` (from the TABLE_INDEX key)

This linkage provides a strong audit trail between column stats and the data they describe.

### Segment Structure

The column stats segment contains one row per row-slice (data segment) in the symbol. The columns are:

| Column | Type | Description |
|--------|------|-------------|
| `start_index` | NANOSECONDS_UTC64 | Start timestamp of the row-slice |
| `end_index` | NANOSECONDS_UTC64 | End timestamp of the row-slice |
| `v1.0_MIN(col_name)` | Same as source column | Minimum value in that row-slice |
| `v1.0_MAX(col_name)` | Same as source column | Maximum value in that row-slice |

The segment uses a `ROWCOUNT` index type and is sorted by `start_index`.

### Column Naming Convention

The stat columns follow a versioned naming scheme: `vX.Y_OPERATION(column_name)`

- `X`: Major version number (currently 1)
- `Y`: Minor version number (currently 0)
- `OPERATION`: The stat type (`MIN` or `MAX`)
- `column_name`: The source column name

This versioning allows the format to evolve while maintaining backward compatibility. When reading column stats, the version is parsed from the column names to ensure correct interpretation.

## Stat Types

Currently only `MINMAX` is supported:

| Stat Type | Description | Columns Created |
|-----------|-------------|-----------------|
| `MINMAX` | Min and max values per row-slice | `v1.0_MIN(col)`, `v1.0_MAX(col)` |

The `MINMAX` stat type creates two columns internally: one for minimum and one for maximum values.

## Implementation Details

### Creation Flow

1. `create_column_stats_version()` is called with the column stats dict
2. The `ColumnStats` class parses the input and creates a `ColumnStatsGenerationClause`
3. The clause uses `MinMaxAggregator` to compute min/max for each row-slice
4. Each row-slice is processed independently using `structure_by_row_slice()`
5. Results are merged using `merge_column_stats_segments()` which:
   - Finds common types across segments (for dynamic schema)
   - Concatenates all segments
   - Sorts by `start_index`
6. If stats already exist, old and new segments are concatenated
7. The final segment is written to storage with the `COLUMN_STATS` key type

### Type Handling

For dynamic schema symbols where column types may vary across row-slices:
- The common type is computed across all row-slices
- Types are promoted as needed (e.g., uint8 + uint16 -> uint16)
- Integer/float mixing promotes to float64
- Missing columns in a row-slice result in NaN values

### Lifecycle

Column stats keys are deleted when:
- The associated version is deleted (`delete`, `delete_version`, `delete_versions`)
- The version is pruned (`prune_previous_version=True`, `prune_previous_versions()`)
- A snapshot containing the version is deleted and no other references exist

## Constraints

- Only numeric columns support `MINMAX` stats (strings will raise `SchemaException`)
- Pickled symbols cannot have column stats (`SchemaException`)
- Multi-indexed symbols are not supported (`E_UNSUPPORTED_INDEX_TYPE`)
- String-indexed symbols are not supported (timestamps only)

## Embedded Per-Column Statistics

ArcticDB has a separate statistics mechanism: **embedded per-column statistics** stored within each data segment.

### How They Differ

| Feature | COLUMN_STATS Key (this document) | Embedded Stats (in segment) |
|---------|----------------------------------|---------------------------|
| Storage | Separate `COLUMN_STATS` key | Inside each `TABLE_DATA` segment header |
| Generation | `create_column_stats()` API call | Config flag at write time |
| Default | Opt-in (must call API) | Disabled |
| Contents | min, max per row-slice | min, max, unique_count, sorted |
| Granularity | One row per row-slice | Per column per segment |

### Enabling Embedded Statistics

Embedded statistics are controlled by a config flag and are **disabled by default**:

```cpp
// In write_frame.cpp:212-213
if (ConfigsMap().instance()->get_int("Statistics.GenerateOnWrite", 0) == 1)
    agg.segment().calculate_statistics();
```

To enable, set `Statistics.GenerateOnWrite=1` in the ArcticDB configuration.

### Embedded Statistics Structure

The `FieldStats` structure (`storage/memory_layout.hpp:103-114`):

```cpp
struct FieldStats {
    uint64_t min_ = 0UL;
    uint64_t max_ = 0UL;
    uint32_t unique_count_ = 0UL;
    UniqueCountType unique_count_precision_ = PRECISE;
    SortedValue sorted_ = UNKNOWN;
    uint8_t set_ = 0U;  // flag indicating if stats are populated
};
```

### Storage Location

When enabled, these statistics are serialized into the encoded segment header for each column field:

- **V1 encoding**: `encode_v1.cpp:156` - `column_field->set_statistics(column.get_statistics())`
- **V2 encoding**: `encode_v2.cpp:345-346` - `if (column.has_statistics()) column_field->set_statistics(column.get_statistics())`

At some point we should generate column stats using this data from the segment if it is present, rather than
re-scanning the block.

## Code References

- Python API: `python/arcticdb/version_store/_store.py` (lines 1167-1259)
- Column stats class: `cpp/arcticdb/pipeline/column_stats.hpp`
- Implementation: `cpp/arcticdb/pipeline/column_stats.cpp`
- Core functions: `cpp/arcticdb/version/version_core.cpp` (lines 1914-2059)
- Processing clause: `cpp/arcticdb/processing/clause.cpp` (lines 1428-1493)
- Aggregator: `cpp/arcticdb/processing/unsorted_aggregation.hpp` (MinMaxAggregator)
- Tests: `python/tests/unit/arcticdb/test_column_stats.py`
