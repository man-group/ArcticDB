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

IClause uses `folly::Poly` for type-erased polymorphism (similar to concept-based polymorphism):

```cpp
struct IClause {
    template<class Base>
    struct Interface : Base {
        // Structure data for processing - returns groupings of entity IDs
        [[nodiscard]] std::vector<std::vector<size_t>> structure_for_processing(
            std::vector<RangesAndKey>& ranges_and_keys
        );

        // Process entities and return processed entity IDs
        [[nodiscard]] std::vector<EntityId> process(
            std::vector<EntityId>&& entity_ids
        ) const;

        // Get clause info (name, requirements, etc.)
        [[nodiscard]] const ClauseInfo& clause_info() const;

        // Configuration
        void set_processing_config(const ProcessingConfig& processing_config);
        void set_component_manager(std::shared_ptr<ComponentManager> component_manager);

        // Schema modification
        OutputSchema modify_schema(OutputSchema&& output_schema) const;
        OutputSchema join_schemas(std::vector<OutputSchema>&& input_schemas) const;
    };
};

// Clause type alias
using Clause = folly::Poly<IClause>;
```

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

```cpp
// In operation_types.hpp
enum class OperationType : uint8_t {
    // Unary operators
    ABS,
    NEG,
    ISNULL,
    NOTNULL,
    IDENTITY,
    NOT,
    // Binary operators (comparison)
    EQ,
    NE,
    LT,
    LE,
    GT,
    GE,
    // Binary operators (arithmetic)
    ADD,
    SUB,
    MUL,
    DIV,
    // Binary operators (logical)
    AND,
    OR,
    XOR,
    // String operations
    ISIN,
    ISNOTIN,
    // ...
};
```

### Expression Evaluation

```cpp
// Expressions are evaluated via the compute() method
// on ExpressionContext which holds the expression tree
VariantData compute(const ExpressionContext& context);
```

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

```cpp
// Dispatch based on input types
template<typename Op>
VariantData dispatch_binary(
    DataType left_type,
    DataType right_type,
    const ColumnData& left,
    const ColumnData& right,
    Op&& op
);
```

## Aggregation

### Location

`cpp/arcticdb/processing/sorted_aggregation.cpp`, `unsorted_aggregation.cpp`

### Sorted vs Unsorted

| Mode | Use Case | Performance |
|------|----------|-------------|
| Sorted | Data sorted by group key | O(n) single pass |
| Unsorted | Random order data | O(n log n) with hash table |

### Aggregation Functions

```cpp
// In sorted_aggregation.hpp
enum class AggregationOperator {
    SUM,
    MEAN,
    MIN,
    MAX,
    FIRST,
    LAST,
    COUNT
};
```

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

ProcessingUnit is used in conjunction with the clause processing framework. It holds data needed for clause execution:

```cpp
// In processing_unit.hpp
struct ProcessingUnit {
    // Vectors must be same length; elements at same index are associated
    std::optional<std::vector<std::shared_ptr<SegmentInMemory>>> segments_;
    std::optional<std::vector<std::shared_ptr<pipelines::RowRange>>> row_ranges_;
    std::optional<std::vector<std::shared_ptr<pipelines::ColRange>>> col_ranges_;
    std::optional<std::vector<std::shared_ptr<AtomKey>>> atom_keys_;
    std::optional<bucket_id> bucket_;  // For partitioned operations
    std::optional<std::vector<uint64_t>> entity_fetch_count_;

    // Expression context (AST for filter/projection)
    std::shared_ptr<ExpressionContext> expression_context_;
    // Cached computed expression results (avoids recomputation)
    std::unordered_map<std::string, VariantData> computed_data_;

    // Methods
    void set_segments(std::vector<std::shared_ptr<SegmentInMemory>>&& segments);
    void set_row_ranges(std::vector<std::shared_ptr<pipelines::RowRange>>&& row_ranges);
    void set_col_ranges(std::vector<std::shared_ptr<pipelines::ColRange>>&& col_ranges);
    void set_atom_keys(std::vector<std::shared_ptr<AtomKey>>&& atom_keys);
    void apply_filter(util::BitSet&& bitset, PipelineOptimisation optimisation);
    void truncate(size_t start_row, size_t end_row);
};
```

## Component Manager

### Location

`cpp/arcticdb/processing/component_manager.hpp`

### Purpose

Manages the lifecycle of processing components (segments, buffers) during query execution.

```cpp
// In component_manager.hpp
// Uses entt entity-component system for efficient entity management

class ComponentManager {
public:
    // Get new entity IDs
    std::vector<EntityId> get_new_entity_ids(size_t count);

    // Add a single entity with specified components
    template<class... Args>
    void add_entity(EntityId id, Args... args);

    // Bulk add entities with same components
    template<class... Args>
    void add_entities(
        std::vector<EntityId>&& ids,
        std::vector<std::tuple<Args...>>&& component_tuples
    );

    // Get entities and decrement their fetch count
    // Entities are removed when fetch count reaches 0
    template<typename ComponentType>
    std::vector<std::tuple<ComponentType, EntityFetchCount>>
    get_entities_and_decrement_refcount(const std::vector<EntityId>& ids);

    // Get a single component for an entity
    template<typename ComponentType>
    ComponentType& get(EntityId id);
};
```

## Query Building (C++)

### Filter Example

```cpp
// Build filter: price > 100
auto expr = std::make_shared<ExpressionNode>(
    ExpressionNodeType::BINARY_OP,
    BinaryOperation::GT,
    column_node("price"),
    constant_node(100)
);

auto filter_clause = std::make_shared<FilterClause>(expr);
```

### Aggregation Example

```cpp
// Build groupby: group by "category", sum "price"
AggregationClause agg_clause;
agg_clause.set_grouping_column("category");
agg_clause.add_aggregation("price", AggregationOperator::SUM);
```

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

```cpp
// Type mismatch
if (!are_types_compatible(left_type, right_type)) {
    throw SchemaException("Type mismatch in expression");
}

// Invalid operation
if (!is_valid_operation(op, data_type)) {
    throw UserInputException("Operation not supported for type");
}
```

## Related Documentation

- [PIPELINE.md](PIPELINE.md) - How processing fits in read path
- [COLUMN_STORE.md](COLUMN_STORE.md) - Data format being processed
- [ENTITY.md](ENTITY.md) - Data types
