# Normalization Module

The normalization module (`python/arcticdb/version_store/_normalization.py`) handles conversion between pandas DataFrames and ArcticDB's internal format.

## Overview

This module provides:
- Conversion from pandas DataFrame to internal format
- Conversion from internal format back to pandas
- Type handling and coercion
- Custom normalizer support

## Location

`python/arcticdb/version_store/_normalization.py`

## Why Normalization?

```
pandas DataFrame                    ArcticDB Internal Format
─────────────────                   ────────────────────────

Complex types (objects)     →       Typed columns (int, float, string)
Various index types         →       Standardized index handling
Missing value representations →     Consistent null handling
DatetimeIndex variations    →       Nanosecond timestamps
```

## NormalizedInput

`NormalizedInput` is a named tuple with:
- `item` - NPDDataFrame ready for C++ (numpy arrays in structured format)
- `metadata` - NormalizationMetadata protobuf containing df/ts/np/msg_pack_frame metadata

### Conversion Flow

```
pandas.DataFrame
        │
        ▼ normalize()
NormalizedInput (data + NormalizationMetadata)
        │
        ▼ to C++
InputFrame
        │
        ▼ storage
        .
        .
        │
        ▼ from C++
Segment + NormalizationMetadata
        │
        ▼ denormalize()
pandas.DataFrame
```

## Normalization Process

### Input Normalization

`normalize(data, string_max_len)` converts pandas DataFrame to NPDDataFrame, handling index extraction, column type inference, string encoding, datetime conversion, and missing values.

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

`denormalize(frame, original_columns)` converts OutputTensorFrame back to pandas DataFrame, reconstructing the DataFrame structure, restoring index (RangeIndex or DatetimeIndex), and converting types.

## Custom Normalizers

ArcticDB uses a type-based dispatch system. Normalizers inherit from `Normalizer` base class and implement `normalize()` and `denormalize()` methods. Custom normalizer registration is not directly exposed via public API.

### Built-in Normalizers

| Type | Normalizer |
|------|-----------|
| `pd.DataFrame` | `DataFrameNormalizer` |
| `pd.Series` | `SeriesNormalizer` |
| `np.ndarray` | `NdArrayNormalizer` |
| `pa.Table` | `ArrowTableNormalizer` |
| TimeFrame | `TimeFrameNormalizer` |
| Arbitrary objects | `MsgPackNormalizer` (fallback) |

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
