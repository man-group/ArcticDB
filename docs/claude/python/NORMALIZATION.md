# Normalization Module

The normalization module (`python/arcticdb/version_store/_normalization.py`) handles conversion between user-facing data formats (pandas, PyArrow, polars) and ArcticDB's internal format.

## Overview

This module provides:
- Conversion from pandas DataFrame, PyArrow Table, or polars DataFrame to internal format
- Conversion from internal format back to the requested output format
- Type handling and coercion
- Custom normalizer support

## Location

`python/arcticdb/version_store/_normalization.py`

## Why Normalization?

```
pandas DataFrame / pa.Table / pl.DataFrame    ArcticDB Internal Format
──────────────────────────────────────────    ────────────────────────

Complex types (objects)     →                 Typed columns (int, float, string)
Various index types         →                 Standardized index handling
Missing value representations →               Consistent null handling
DatetimeIndex variations    →                 Nanosecond timestamps
```

## NormalizedInput

`NormalizedInput` is a named tuple with:
- `item` - NPDDataFrame ready for C++ (numpy arrays in structured format)
- `metadata` - NormalizationMetadata protobuf containing df/ts/np/msg_pack_frame metadata

### Conversion Flow

```
pandas.DataFrame / pa.Table / pl.DataFrame
        │
        ▼ normalize()
NormalizedInput (NPDDataFrame or RecordBatches + NormalizationMetadata)
        │
        ▼ to C++
InputFrame
        │
        ▼ storage
        .
        .
        │
        ▼ from C++
OutputFrame (pandas variant or arrow variant)
        │
        ▼ denormalize()
pandas.DataFrame / pa.Table / pl.DataFrame (based on OutputFormat)
```

## Normalization Process

### Input Normalization

`normalize(data, string_max_len)` converts input data to the internal format. For pandas DataFrames this produces an NPDDataFrame (numpy arrays). For PyArrow Tables and polars DataFrames this produces a vector of record batches.

### Type Conversion

| pandas dtype | ArcticDB Type |
|-------------|---------------|
| `int64` | `INT64` |
| `int32` | `INT32` |
| `float64` | `FLOAT64` |
| `float32` | `FLOAT32` |
| `bool` | `BOOL8` |
| `datetime64[ns]` | `NANOSECONDS_UTC64` |
| `object` (strings) | `UTF_DYNAMIC64` |
| `category` | Underlying type |

### Index Handling

```python
# RangeIndex (default)
df = pd.DataFrame({"a": [1, 2, 3]})
# Normalized to row numbers

# DatetimeIndex
df = pd.DataFrame(
    {"a": [1, 2, 3]},
    index=pd.date_range("2024-01-01", periods=3)
)
# Normalized to nanosecond timestamps

# Named index
df = pd.DataFrame({"a": [1, 2, 3]})
df.index.name = "my_index"
# Index stored with name preserved

# MultiIndex
df = pd.DataFrame(
    {"a": [1, 2, 3, 4]},
    index=pd.MultiIndex.from_tuples([
        ("A", 1), ("A", 2), ("B", 1), ("B", 2)
    ])
)
# Each level stored as separate column
```

## Denormalization Process

`denormalize(frame, original_columns)` converts the C++ output back to the target format. The C++ side returns an `OutputFrame` variant which is either a pandas-oriented frame or an arrow-oriented frame. The denormalization step reconstructs the final object (pandas DataFrame, PyArrow Table, or polars DataFrame depending on `OutputFormat`).

## Custom Normalizers

ArcticDB uses a type-based dispatch system. Normalizers inherit from `Normalizer` base class and implement `normalize()` and `denormalize()` methods. Custom normalizer registration is not directly exposed via public API.

### Built-in Normalizers

| Type | Normalizer | Notes |
|------|-----------|-------|
| `pd.DataFrame` | `DataFrameNormalizer` | |
| `pd.Series` | `SeriesNormalizer` | |
| `np.ndarray` | `NdArrayNormalizer` | |
| `pa.Table` | `ArrowTableNormalizer` | Uses `index_column=True` for timestamp index |
| `pl.DataFrame` | `ArrowTableNormalizer` | Converted to `pa.Table` via `.to_arrow()` first |
| TimeFrame | `TimeFrameNormalizer` | |
| Arbitrary objects | `MsgPackNormalizer` (fallback) | |

## Arrow and Polars Support

### Write Path

Both `pa.Table` and `pl.DataFrame` are handled by `ArrowTableNormalizer`. Polars DataFrames are converted to `pa.Table` via `.to_arrow()` before normalization. The normalizer converts the table into a vector of record batches for the C++ layer. The `index_column` parameter (boolean) tells the normalizer to treat the first column as a sorted timestamp index.

### Read Path

The `output_format` parameter on read methods controls the return type:
- `OutputFormat.PANDAS` (default) — returns `pd.DataFrame`
- `OutputFormat.PYARROW` — returns `pa.Table`
- `OutputFormat.POLARS` — returns `pl.DataFrame` (converted from `pa.Table` via `pl.from_arrow()`)

### Date-Range Filtering (`restrict_data_to_date_range_only`)

Used by `update()` with a `date_range` parameter to restrict input data before writing. Handles pandas, PyArrow, and polars inputs:

- **pandas**: Uses `data.loc[start:end]` with monotonicity check
- **PyArrow**: Uses `_filter_pyarrow_table_to_date_range` — a Python binary search on the sorted index column followed by `pa.Table.slice` (zero-copy)
- **polars**: Converts to `pa.Table` via `.to_arrow()` and reuses the PyArrow path. The resulting `pa.Table` (a zero-copy slice/view) is returned directly without converting back to polars, since the caller normalizes the result anyway. Note: `pl.from_arrow` on a sliced table is also zero-copy, but this conversion is unnecessary. Native polars filtering (`filter`/`is_between` with `set_sorted`) was benchmarked and found to be significantly slower because it materializes a boolean mask over the entire column rather than slicing

### String Types

Polars only supports `large_string` (64-bit offsets), not PyArrow's `string` (32-bit offsets). When reading with `OutputFormat.POLARS`, string format options that specify `pa.string()` are automatically promoted to `pa.large_string()`.

## String Handling

### Dynamic Strings

By default, strings are stored with dynamic length:

```python
df = pd.DataFrame({"name": ["Alice", "Bob", "Charlie"]})
lib.write("symbol", df)
# Strings stored efficiently with actual lengths
```

### Fixed-Length Strings

For performance with fixed-size strings:

```python
# Specify max length hint
lib.write("symbol", df, string_max_len=50)
```

## Missing Values

### pandas → ArcticDB

| pandas | ArcticDB |
|--------|----------|
| `np.nan` | Special NaN encoding |
| `pd.NA` | Null marker |
| `None` (object) | Null marker |
| `pd.NaT` | Special NaT timestamp |

### ArcticDB → pandas

```python
# NaN restored
df["col"].isna()  # Detects nulls correctly
```

## Type Coercion

### Automatic Coercion

```python
# Mixed int/float column
df = pd.DataFrame({"a": [1, 2.5, 3]})
# Normalized to FLOAT64

# Mixed int/None column
df = pd.DataFrame({"a": [1, None, 3]})
# Normalized to FLOAT64 (for NaN support)
```

### Schema Enforcement

```python
# With dynamic_schema=False (default)
# Subsequent writes must match original schema

lib.write("symbol", pd.DataFrame({"a": [1, 2, 3]}))
lib.write("symbol", pd.DataFrame({"b": [1, 2, 3]}))  # Error!

# With dynamic_schema=True
# Schema can change between versions
opts = LibraryOptions(dynamic_schema=True)
lib = ac.create_library("lib", library_options=opts)
lib.write("symbol", pd.DataFrame({"a": [1, 2, 3]}))
lib.write("symbol", pd.DataFrame({"b": [1, 2, 3]}))  # OK
```

## Performance Considerations

### Zero-Copy Where Possible

```python
# Numeric arrays often passed without copy
arr = df["price"].values  # numpy array
# Direct memory reference to C++
```

### String Conversion Overhead

```python
# Strings require encoding
df["name"] = ["Alice", "Bob", "Charlie"]
# Each string encoded to UTF-8 bytes
```

### Categorical Optimization

```python
# Categorical columns stored efficiently
df["status"] = pd.Categorical(["A", "B", "A", "A", "B"])
# Stored as codes + dictionary
```

## Key Files

| File | Purpose |
|------|---------|
| `version_store/_normalization.py` | Main normalization code |
| `version_store/processing.py` | Query result handling |
| `cpp/arcticdb/python/python_to_tensor_frame.hpp` | C++ side conversion |

## Debugging

Use `DataFrameNormalizer().normalize(df)` to inspect normalized form. Use `lib.get_info("symbol")` to inspect stored schema (access `info["dtype"]` for column types).

## Related Documentation

- [NATIVE_VERSION_STORE.md](NATIVE_VERSION_STORE.md) - Uses normalization
- [LIBRARY_API.md](LIBRARY_API.md) - User-facing write/read
- [../cpp/PYTHON_BINDINGS.md](../cpp/PYTHON_BINDINGS.md) - C++ conversion
