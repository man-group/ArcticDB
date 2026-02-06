# Processing Module

The processing module (`cpp/arcticdb/processing/`) handles query execution including filtering, projection, aggregation, and resampling.

## Overview

This module provides:
- Clause-based query execution (filter, project, aggregate, etc.)
- Expression evaluation engine
- Operation dispatch for different data types
- Processing unit management

## Clause System

### Location

`cpp/arcticdb/processing/clause.hpp`

### What is a Clause?

A clause is a discrete query operation that processes data. Clauses are chained to form a query pipeline.

### Clause Interface

`IClause` (in `cpp/arcticdb/processing/clause.hpp`) uses `folly::Poly` for type-erased polymorphism. Key methods:
- `structure_for_processing()` - Structure data and return groupings of entity IDs
- `process()` - Process entities and return processed entity IDs
- `clause_info()` - Get clause metadata
- `set_processing_config()` / `set_component_manager()` - Configuration
- `modify_schema()` / `join_schemas()` - Schema modification

The `Clause` type is an alias for `folly::Poly<IClause>`.

### Built-in Clause Types

| Clause | Purpose | Example |
|--------|---------|---------|
| `FilterClause` | Row filtering | `col("price") > 100` |
| `ProjectClause` | Column selection | `columns=["a", "b"]` |
| `AggregationClause` | Groupby/aggregate | `groupby("category").agg({"price": "sum"})` |
| `ResampleClause` | Time-based resampling | `resample("1h").agg(...)` |
| `SortClause` | Row sorting | `sort_values("timestamp")` |
| `DateRangeClause` | Date filtering | `date_range=(start, end)` |
| `RowRangeClause` | Row range selection | `head(100)`, `tail(50)` |

### Clause Pipeline

```
Data ──► FilterClause ──► ProjectClause ──► AggregationClause ──► Result
```

## Expression Engine

### Location

`cpp/arcticdb/processing/expression_node.hpp`

### Expression Tree

Expressions are represented as trees:

```
        (AND)
       /     \
    (>)       (<)
   /   \     /   \
col(a)  5  col(b) 10

Represents: (a > 5) AND (b < 10)
```

### Operation Types

`OperationType` enum in `cpp/arcticdb/processing/operation_types.hpp` defines:
- **Unary**: `ABS`, `NEG`, `ISNULL`, `NOTNULL`, `IDENTITY`, `NOT`
- **Comparison**: `EQ`, `NE`, `LT`, `LE`, `GT`, `GE`
- **Arithmetic**: `ADD`, `SUB`, `MUL`, `DIV`
- **Logical**: `AND`, `OR`, `XOR`
- **String**: `ISIN`, `ISNOTIN`

### Expression Evaluation

Expressions are evaluated via `compute()` on `ExpressionContext` which holds the expression tree.

## Operation Dispatch

### Location

`cpp/arcticdb/processing/operation_dispatch.cpp`

### Purpose

Dispatches operations based on data types at runtime.

### Supported Operations

| Category | Operations |
|----------|------------|
| Comparison | `<`, `<=`, `>`, `>=`, `==`, `!=` |
| Arithmetic | `+`, `-`, `*`, `/` |
| Logical | `AND`, `OR`, `NOT` |
| Aggregation | `SUM`, `MEAN`, `MIN`, `MAX`, `COUNT`, `FIRST`, `LAST` |
| String | `ISIN`, `ISNOTIN`, `STARTSWITH`, `ENDSWITH` |

### Type Dispatch

`dispatch_binary()` template function dispatches operations based on data types at runtime.

## Aggregation

### Location

`cpp/arcticdb/processing/sorted_aggregation.cpp`, `unsorted_aggregation.cpp`

### Sorted vs Unsorted

| Mode | Use Case | Performance |
|------|----------|-------------|
| Sorted | Data sorted by group key | O(n) single pass |
| Unsorted | Random order data | O(n log n) with hash table |

### Aggregation Functions

`AggregationOperator` enum in `cpp/arcticdb/processing/sorted_aggregation.hpp`: `SUM`, `MEAN`, `MIN`, `MAX`, `FIRST`, `LAST`, `COUNT`.

### Groupby Execution

```
Input Data
    │
    ▼
┌─────────────────────┐
│ Determine if sorted │
│ by group key        │
└───────────┬─────────┘
            │
    ┌───────┴───────┐
    ▼               ▼
┌────────┐    ┌──────────┐
│ Sorted │    │ Unsorted │
│ Path   │    │ Path     │
└───┬────┘    └────┬─────┘
    │              │
    ▼              ▼
┌─────────────────────┐
│ Apply aggregations  │
│ per group           │
└───────────┬─────────┘
            │
            ▼
       Aggregated Result
```

## Processing Unit

### Location

`cpp/arcticdb/processing/processing_unit.hpp`

### Purpose

A processing unit holds data being processed through the clause pipeline.

### Structure

`ProcessingUnit` (in `cpp/arcticdb/processing/processing_unit.hpp`) holds data for clause execution:
- `segments_`, `row_ranges_`, `col_ranges_`, `atom_keys_` - Associated data vectors (same length, elements at same index are related)
- `bucket_` - For partitioned operations
- `expression_context_` - AST for filter/projection
- `computed_data_` - Cached expression results

Key methods: `set_segments()`, `set_row_ranges()`, `set_col_ranges()`, `set_atom_keys()`, `apply_filter()`, `truncate()`.

## Component Manager

### Location

`cpp/arcticdb/processing/component_manager.hpp`

### Purpose

`ComponentManager` (in `cpp/arcticdb/processing/component_manager.hpp`) manages the lifecycle of processing components using the entt entity-component system.

Key methods:
- `get_new_entity_ids(count)` - Allocate new entity IDs
- `add_entity(id, args...)` / `add_entities(ids, tuples)` - Add entities with components
- `get_entities_and_decrement_refcount(ids)` - Get entities (removed when fetch count reaches 0)
- `get<T>(id)` - Get a single component for an entity

## Query Building (C++)

Build filters using `ExpressionNode` with `ExpressionNodeType::BINARY_OP` and column/constant nodes. Create clauses like `FilterClause(expr)` or `AggregationClause` with grouping and aggregation settings. See `cpp/arcticdb/processing/expression_node.hpp` and `cpp/arcticdb/processing/clause.hpp` for details.

## Key Files

| File | Purpose |
|------|---------|
| `clause.hpp` | Clause interface and implementations |
| `clause.cpp` | Clause implementations |
| `expression_node.hpp` | Expression tree |
| `expression_node.cpp` | Expression evaluation |
| `operation_dispatch.cpp` | Type-based dispatch |
| `sorted_aggregation.cpp` | Sorted groupby path |
| `unsorted_aggregation.cpp` | Unsorted groupby path |
| `processing_unit.hpp` | Processing unit structure |
| `component_manager.hpp` | Component lifecycle |
| `aggregation_utils.cpp` | Aggregation helpers |

## Python Integration

### QueryBuilder Mapping

Python `QueryBuilder` operations map to C++ clauses:

| Python | C++ Clause |
|--------|-----------|
| `q[expr]` (bracket notation) | `FilterClause` |
| `columns=[...]` | `ProjectClause` |
| `.groupby(...).agg(...)` | `AggregationClause` |
| `.resample(...).agg(...)` | `ResampleClause` |
| `.head(n)` | `RowRangeClause` |
| `.tail(n)` | `RowRangeClause` |

### Expression DSL

```python
# Python - uses bracket notation for filtering
from arcticdb import QueryBuilder
q = QueryBuilder()
q = q[(q["price"] > 100) & (q["volume"] > 1000)]
```

Translates to C++ expression tree internally.

## Performance Considerations

### Pushdown Optimization

- Filters pushed to storage layer avoid loading unnecessary data
- Column projection avoids reading unused columns
- Date range filters use index for segment pruning

### Memory Management

- Processing units released as soon as processed
- Streaming execution for large result sets
- Spill-to-disk for very large aggregations (future)

### Parallel Execution

- Multiple segments processed in parallel
- Aggregation partitioned across threads
- Final merge step for parallel aggregations

## Error Handling

Type mismatches throw `SchemaException`. Invalid operations throw `UserInputException`.

## Related Documentation

- [PIPELINE.md](PIPELINE.md) - How processing fits in read path
- [COLUMN_STORE.md](COLUMN_STORE.md) - Data format being processed
- [ENTITY.md](ENTITY.md) - Data types
