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

### What It Is

`NormalizedInput` is the result of normalization, containing data and metadata:

```python
# NormalizedInput is a named tuple
NormalizedInput = NamedTuple("NormalizedInput", [("item", NPDDataFrame), ("metadata", NormalizationMetadata)])
# item: NPDDataFrame ready for C++ (numpy arrays in a structured format)
# metadata: NormalizationMetadata protobuf message

# NormalizationMetadata contains:
# - df: DataFrame-specific metadata
# - ts: TimeFrame-specific metadata
# - np: ndarray-specific metadata
# - msg_pack_frame: MsgPack fallback metadata
```

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

```python
def normalize(data, string_max_len=None):
    """
    Convert pandas DataFrame to NPDDataFrame.

    Handles:
    - Index extraction and standardization
    - Column type inference
    - String encoding
    - Datetime conversion
    - Missing value handling
    """
```

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

### Output Denormalization

```python
def denormalize(frame, original_columns=None):
    """
    Convert OutputTensorFrame back to pandas DataFrame.

    Handles:
    - Reconstruct DataFrame structure
    - Restore index
    - Convert types back to pandas equivalents
    - Handle metadata
    """
```

### Index Reconstruction

```python
# Row count index restored to RangeIndex
df.index  # RangeIndex(start=0, stop=100, step=1)

# Timestamp index restored to DatetimeIndex
df.index  # DatetimeIndex(['2024-01-01', ...], freq='D')
```

## Custom Normalizers

ArcticDB uses a type-based dispatch system for normalization. The normalizers are selected based on the input data type.

### Built-in Normalizer Selection

The normalization system uses `_normalize()` and the `Normalizer` abstract base class. Each normalizer implements `normalize()` and corresponding denormalization methods.

```python
# Example: Creating a custom normalizer (advanced usage)
from arcticdb.version_store._normalization import Normalizer

class MyCustomNormalizer(Normalizer):
    TYPE = "mycustom"  # Type identifier

    def normalize(self, data, **kwargs):
        # Convert custom type to NPDDataFrame
        ...
        return NormalizedInput(item=npd_dataframe, metadata=norm_metadata)

    def denormalize(self, item, norm_meta):
        # Convert back to custom type
        ...
```

Note: Custom normalizer registration is not directly exposed via a public API. The built-in normalizers handle most use cases.

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

### Check Normalized Form

```python
from arcticdb.version_store._normalization import DataFrameNormalizer

df = pd.DataFrame({"a": [1, 2, 3]})
normalizer = DataFrameNormalizer()
normalized = normalizer.normalize(df)

# normalized.item contains the data arrays
# normalized.norm_meta contains metadata protobuf
print(f"Metadata: {normalized.norm_meta}")
```

### Schema Inspection

```python
# Get info about stored schema
info = lib.get_info("symbol")
print(info["dtype"])  # Column types as stored
```

## Related Documentation

- [NATIVE_VERSION_STORE.md](NATIVE_VERSION_STORE.md) - Uses normalization
- [LIBRARY_API.md](LIBRARY_API.md) - User-facing write/read
- [../cpp/PYTHON_BINDINGS.md](../cpp/PYTHON_BINDINGS.md) - C++ conversion
