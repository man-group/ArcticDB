# Query Processing

The query processing module (`python/arcticdb/version_store/processing.py`) provides the QueryBuilder DSL for filtering and aggregation.

## Overview

This module provides:
- QueryBuilder for constructing queries
- Column reference syntax (`q["column"]`)
- Lazy evaluation of queries
- Push-down optimization to C++

## Location

`python/arcticdb/version_store/processing.py`

## QueryBuilder

### Basic Usage

```python
from arcticdb import QueryBuilder

# Create builder
q = QueryBuilder()

# Add filter using bracket notation
q = q[q["price"] > 100]

# Execute with read
result = lib.read("symbol", query_builder=q)
```

Note: The syntax uses bracket notation (`q["column"]`) rather than a `filter()` method. This is designed to be similar to Pandas DataFrame syntax.

### Method Chaining

```python
q = QueryBuilder()
q = q[q["price"] > 100]
q = q[q["volume"] > 1000]
# Equivalent to: WHERE price > 100 AND volume > 1000
```

### Alternative Column Syntax

```python
# If column name is a valid Python identifier, you can use attribute access
q = q[q.price > 100]  # Equivalent to q[q["price"] > 100]

# Use bracket notation for column names with spaces or special characters
q = q[q["column name"] > 100]
```

## Expression DSL

### Column Reference

```python
# Reference a column within QueryBuilder context
q = QueryBuilder()

# Using bracket notation (always works)
q["price"]
q["volume"]
q["timestamp"]

# Using attribute access (only for valid Python identifiers)
q.price
q.volume
```

### Comparison Operators

```python
q = QueryBuilder()

# Equality
q = q[q["status"] == "active"]
q = q[q["status"] != "inactive"]

# Numeric comparisons
q = q[q["price"] > 100]
q = q[q["price"] >= 100]
q = q[q["price"] < 200]
q = q[q["price"] <= 200]
```

### Logical Operators

```python
q = QueryBuilder()

# AND
q = q[(q["price"] > 100) & (q["volume"] > 1000)]

# OR
q = q[(q["status"] == "active") | (q["status"] == "pending")]

# NOT
q = q[~(q["price"] < 50)]

# XOR
q = q[(q["flag1"]) ^ (q["flag2"])]
```

### Arithmetic Operators

```python
q = QueryBuilder()

# Create computed expressions
q["price"] * q["volume"]  # Total value
q["close"] - q["open"]    # Price change
q["value"] / 100          # Percentage

# Unary negation
-q["value"]

# Absolute value
abs(q["change"])
```

### Null Checking

```python
q = QueryBuilder()

# Check for null/NaN values
q = q[q["col"].isna()]
q = q[q["col"].isnull()]  # Equivalent to isna()

# Check for non-null values
q = q[q["col"].notna()]
q = q[q["col"].notnull()]  # Equivalent to notna()
```

### List Membership

```python
q = QueryBuilder()

# Check if value is in list
q = q[q["category"].isin(["tech", "finance", "health"])]

# Check if value is not in list
q = q[q["status"].isnotin(["deleted", "archived"])]

# Alternative using == and != with lists
q = q[q["category"] == ["tech", "finance"]]  # Same as isin
q = q[q["status"] != ["deleted"]]  # Same as isnotin
```

### Regex Matching

```python
q = QueryBuilder()

# Match values against regex pattern
q = q[q["symbol"].regex_match("^AAPL.*")]
```

## Filter Clause

```python
q = QueryBuilder()

# Simple filter
q = q[q["price"] > 100]

# Complex filter
q = q[
    (q["price"] > 100) &
    (q["volume"] > 1000) &
    q["sector"].isin(["tech", "finance"])
]
```

## Aggregation

### GroupBy

```python
q = QueryBuilder()

# Group by single column (only single-column groupby is supported)
q = q.groupby("category")
```

Note: Currently GroupBy only supports single-column groupings. Multi-column groupby is not supported.

### Aggregation Functions

```python
q = QueryBuilder()
q = q.groupby("category").agg({
    "price": "sum",
    "volume": "mean",
    "trade_count": "count",
    "high": "max",
    "low": "min",
    "first_trade": "first",
    "last_trade": "last"
})
```

### Available Aggregations

| Name | Description |
|------|-------------|
| `sum` | Sum of values |
| `mean` | Average value |
| `min` | Minimum value |
| `max` | Maximum value |
| `count` | Count of non-null values |
| `first` | First value in group |
| `last` | Last value in group |

## Resampling

### Time-Based Resampling

```python
q = QueryBuilder()

# Resample to hourly
q = q.resample("1h").agg({
    "price": "last",
    "volume": "sum"
})

# Resample to daily
q = q.resample("1D").agg({
    "open": "first",
    "high": "max",
    "low": "min",
    "close": "last"
})
```

### Resample Frequencies

| Frequency | Description |
|-----------|-------------|
| `1s`, `5s` | Seconds |
| `1min`, `5min` | Minutes |
| `1h`, `4h` | Hours |
| `1D`, `1W` | Days, Weeks |
| `1M`, `1Y` | Months, Years |

## Row Operations

### Head/Tail

```python
q = QueryBuilder()

# First N rows
q = q.head(100)

# Last N rows
q = q.tail(50)
```

### Row Range

```python
q = QueryBuilder()

# Rows 100-199
q = q.row_range(100, 200)
```

## Projection (Apply)

### Adding Computed Columns

```python
q = QueryBuilder()

# Create new columns with apply
q = q.apply("total_value", q["price"] * q["volume"])
q = q.apply("price_change", q["close"] - q["open"])
```

## Combined Queries

```python
# Filter + Aggregate
q = QueryBuilder()
q = q[q["price"] > 100]
q = q.groupby("category").agg({"volume": "sum"})

# Filter + Resample
q = QueryBuilder()
q = q[q["volume"] > 0]
q = q.resample("1h").agg({"price": "mean"})

# Multiple operations
q = QueryBuilder()
q = q[q["price"] > 50]
q = q.groupby("sector").agg({"volume": "sum"})
# Note: Order matters - filter before groupby
```

## Execution

### With Library.read()

```python
# Query executes during read
result = lib.read("symbol", query_builder=q)

# Combine with other read options
result = lib.read(
    "symbol",
    columns=["price", "volume"],
    date_range=(start, end),
    query_builder=q
)
```

### Push-Down Optimization

Queries are pushed to the C++ layer for efficient execution:

```
Python QueryBuilder
        │
        ▼ serialize to C++
C++ Clause Pipeline
        │
        ▼ execute at storage layer
Filtered/Aggregated Result
        │
        ▼ return to Python
Python DataFrame
```

Benefits:
- Filters applied before data leaves storage
- Only required columns read
- Aggregation done in C++ (faster)

## LazyDataFrame

### Concept

`LazyDataFrame` allows building queries without immediate execution:

```python
# Read returns a LazyDataFrame
ldf = lib.read("symbol", lazy=True)

# Build query (uses different syntax than QueryBuilder)
# LazyDataFrame uses col() function from arcticdb
from arcticdb import col
ldf = ldf[col("price") > 100]

# Execute when needed
result = ldf.collect()
```

Note: LazyDataFrame uses `col()` function for column references, while QueryBuilder uses `q["column"]` syntax.

### Benefits

- Deferred execution
- Query optimization
- Reduced memory usage

## Class Definitions

### QueryBuilder

`QueryBuilder` in `python/arcticdb/version_store/processing.py` provides:
- `__getitem__(expr)` - Add filter clause (bracket notation)
- `groupby(name)` - Set groupby column (single column only)
- `agg(aggregations)` - Set aggregation functions
- `resample(rule)` - Set resample frequency
- `head(n)` / `tail(n)` - Limit rows
- `apply(name, expr)` - Add computed column

### ExpressionNode

`ExpressionNode` supports:
- Logical: `&` (AND), `|` (OR), `~` (NOT), `^` (XOR)
- Comparison: `>`, `<`, `>=`, `<=`, `==`, `!=`
- Null checking: `isna()`, `isnull()`, `notna()`, `notnull()`
- List membership: `isin(values)`, `isnotin(values)`
- Regex: `regex_match(pattern)`

## Key Files

| File | Purpose |
|------|---------|
| `version_store/processing.py` | QueryBuilder, expressions |
| `cpp/arcticdb/processing/clause.cpp` | C++ clause execution |
| `cpp/arcticdb/processing/expression_node.cpp` | C++ expression evaluation |

## Performance Tips

### Column Selection

```python
# Better: Specify only needed columns
result = lib.read("symbol", columns=["price", "volume"], query_builder=q)

# Worse: Read all columns then filter
result = lib.read("symbol", query_builder=q)
```

### Filter Early

```python
# Better: Filter reduces data early
q = QueryBuilder()
q = q[q["price"] > 100]
q = q.groupby("category").agg({"volume": "sum"})

# Worse: Aggregate then filter (if supported)
# - More data processed
```

### Use Indexes

```python
# Use date_range for time-indexed data
result = lib.read(
    "symbol",
    date_range=(start, end),
    query_builder=q
)
# Segments outside range are skipped entirely
```

## Related Documentation

- [LIBRARY_API.md](LIBRARY_API.md) - Library.read() method
- [../cpp/PROCESSING.md](../cpp/PROCESSING.md) - C++ query processing
