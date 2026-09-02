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

`ExpressionContext` (`expression_context.hpp`) holds the expression tree as a graph of `ExpressionNode`s rooted at `root_`, along with a `dynamic_schema_` flag. Expressions are evaluated by calling `compute()` on `root_`, which recurses into its children. Results of subexpressions are memoized on the `ProcessingUnit` (keyed on each node's `label_`, deep-compared on a hit) so shared subtrees are evaluated only once.

Multiple filter contexts are combined into one by `and_filter_expression_contexts()` (`query_planner.cpp`), which AND-s their root nodes into a single graph. This is used when AND-ing together filter clause expressions for column stats evaluation.

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

### Column Stats Evaluation

The comparison operator structs (`LessThanOperator`, `GreaterThanOperator`, `EqualsOperator`, etc.) in `operation_types.hpp` have `ValueRange<T>` overloads that return `StatsComparison` instead of `bool`. These determine whether a min/max range satisfies a comparison against a constant value, using three-valued logic:

| Value | Meaning |
|-------|---------|
| `ALL_MATCH` | Every row in the segment must satisfy the predicate |
| `NONE_MATCH` | No row can satisfy — segment can be skipped |
| `UNKNOWN` | Some rows may match — segment must be fetched |

`ValueRange<T>` holds `min` and `max` fields. The `FlippedComparator` trait (in `column_stats_dispatch.hpp`) handles reversed operand order (e.g. `5 < col` becomes `col > 5`).

#### Column-to-Column Comparisons

The comparison operator structs also have `ValueRange<T>, ValueRange<U>` overloads for `col1 OP col2` predicates where both sides have per-segment stats. The `stats_comparator(ColumnStatsValues, ColumnStatsValues, Func)` overload in `column_stats_dispatch.hpp` dispatches on both sides' types and invokes the range-vs-range comparator. Ordering operators (`<`, `<=`, `>`, `>=`) return `ALL_MATCH` when the ranges are disjoint in the right direction and `NONE_MATCH` when disjoint in the wrong direction. `!=` returns `ALL_MATCH` when ranges are disjoint and `NONE_MATCH` only when both ranges collapse to the same single point.

`ColumnStatsValues::column_absent` distinguishes "column not present in segment" from "column present but stats unavailable". If either side is absent, the comparator returns `NONE_MATCH`; if either side lacks min/max but was present, it returns `UNKNOWN`. `ColumnStatsData::ColumnStatsData()` in `column_stats_filter.cpp` sets `column_absent` when both min and max are absent after reading the sparse bitmap.

NaN handling is factored into `check_range_for_nan()` and `check_scalar_for_nan()` helpers in `operation_types.hpp`, shared by the `ValueRange/ValueRange` and `ValueRange/scalar` overloads of `EqualsOperator` and `NotEqualsOperator`.

NaT handling follows Pandas semantics: any comparison involving NaT is False, except `!=` which is True. The `MinMaxAggregatorData` aggregator (in `unsorted_aggregation.cpp`) skips NaT values when computing per-slice min/max for time columns and only sets stats to `(NaT, NaT)` when the slice contains nothing but NaT. `stats_comparator` and `stats_membership_comparator` rely on that invariant: if either side's min is NaT they short-circuit to `ALL_MATCH` for `!=` and `NONE_MATCH` otherwise. The `check_time_stats_for_nat()` helper in `operation_types.hpp` covers the `ValueRange/scalar` case; the `ValueRange/ValueRange` and membership paths inline the same check. Per-row evaluation in `binary_comparator()` (in `operation_dispatch_binary.hpp`) applies the same rule, with a hoisted fast path that fills the bitset directly when the query value is NaT.

#### Membership Operators (isin / isnotin)

`stats_membership_comparator()` in `column_stats_dispatch.cpp` evaluates a segment's min/max stats against a `ValueSet`. It uses the `ValueSet`'s cached `min_value()` / `max_value()` (computed lazily via `std::call_once`, filtering out NaN values) for a fast range disjointness check. If the ranges overlap and the result is ambiguous, it falls back to iterating individual set elements against the segment's `ValueRange`. The `isnotin` result is the logical inverse of `isin`. `visit_binary_membership_stats()` applies this per row-slice across the stats vector.

`Value::is_nan()` supports the NaN handling: segments where both min and max are NaN are treated as all-NaN and produce `NONE_MATCH` for `isin`.

See [PIPELINE.md - Column Stats Filtering](PIPELINE.md#column-stats-filtering) for the full read-path integration.

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
- `computed_data_` - Memoized expression results, keyed on each node's `label_` (one slot per label, deep-compared on a hit so colliding labels never return a wrong result)

Key methods: `set_segments()`, `set_row_ranges()`, `set_col_ranges()`, `set_atom_keys()`, `apply_filter()`, `truncate()`.

## Component Manager

### Location

`cpp/arcticdb/processing/component_manager.hpp`

### Purpose

`ComponentManager` (in `cpp/arcticdb/processing/component_manager.hpp`) manages the lifecycle of processing components using the entt entity-component system.

Key methods:
- `get_new_entity_ids(count)` - Allocate new entity IDs
- `add_entity(id, args...)` / `add_entities(ids, tuples)` - Add entities with components
- `get_components_and_decrement_refcount(ids)` - Get entities (removed when fetch count reaches 0)
- `get<T>(id)` - Get a single component for an entity

## Read Admission Control

### Location

`cpp/arcticdb/version/admission_handler.hpp`, `cpp/arcticdb/version/admission_handler.cpp`

### Purpose

`ProcessingUnitAdmissionHandler` decides when each row-slice read is submitted to the IO pool. Reads used
to be submitted eagerly for every slice at once, so if clause processing could not keep up, decoded
`SegmentInMemory` objects accumulated ahead of it — for a whole-symbol operation such as manual column
stats creation, up to the entire symbol.

The handler is created in `read_and_schedule_processing()` and drives the futures that
`schedule_clause_processing()` chains clause execution onto. It applies two independent limits:

| Limit | Unit | Freed when | Config |
|-------|------|-----------|--------|
| Residency budget | processing units admitted but not yet processed | a unit finishes processing (`on_processing_unit_complete`) | `VersionStore.NumProcessingUnitsLive` |
| Read window | segment reads submitted but not yet decoded | a read completes (`on_read_complete`) | `VersionStore.SegmentReadWindow` |

The residency budget is what bounds memory: a unit's segments are decoded and held until its
`MemSegmentProcessingTask` completes. The read window reproduces the `folly::window(2*io_threads)`
behaviour the eager path had, and matters when the budget is much larger than the window — submitting all
the IO up front gives poor queue fairness, because the Folly IO pool schedules round-robin across threads.

### Mechanism

Reads are backed by promises created up front, so the whole downstream future chain can be built before
any work starts:

```
futures()                    ← one Future per row slice, from an unfulfilled Promise
   │                            (schedule_first_iteration builds the clause chain on these)
   ▼
admit_initial_processing_units()   ← makes the first K units' segments eligible
   │
   ▼
fill_read_window()           ← drains the eligible queue into the IO pool, up to read_window
   │
   ▼  read completes → promise fulfilled → slot freed → fill_read_window()
   │
   ▼  unit processed → .ensure → on_processing_unit_complete() → admit next unit
```

Two ordering constraints are load-bearing:

- `admit_initial_processing_units()` must be called *after* `schedule_first_iteration()`, so the
  `.ensure` release path exists before any unit can complete.
- `on_processing_unit_complete()` must not block. It runs inline on a CPU pool thread at the end of a
  processing task, and only submits IO work.

A segment shared by two units (resample bucket boundaries, for example) is enqueued once, tracked by
`i_th_segment_enqueued_`, and consumed by both units via `folly::splitFuture`. Because window slots are
freed on read completion rather than unit completion, a unit never waits on a segment it has itself
blocked, so any budget >= 1 and window >= 1 makes progress regardless of unit shape.

Because two units can complete their reads of a shared segment at the same time, the components for a
segment must be added to the `ComponentManager` exactly once, and the unit that does not add them must
not start processing the entity until they are there. `schedule_first_iteration` uses a `util::OnceFlags`
(`cpp/arcticdb/util/once_flags.hpp`), one mutex and one `uint8_t` flag per segment: the first caller runs
the add while holding the position's mutex, and a later caller returns from `call_once` only once that
add has finished. Both properties are load-bearing — the loser goes straight on to
`MemSegmentProcessingTask`, which gathers those components. A second add of a component an entity
already has now raises an `InternalException` from `ComponentManager::add_components`.

### Test instrumentation

`util::SegmentResidencyTracker` (`cpp/arcticdb/util/segment_residency_tracker.hpp`) counts segments
decoded from storage that are live in memory, and records a high-water mark. `DecodeSliceTask` calls
`SegmentInMemory::mark_from_disk()`; the `SegmentInMemoryImpl` destructor releases the count. Clause
outputs and `clone()` results are deliberately unmarked, so the tracker measures exactly what admission
controls. It is compiled into release builds but disabled by default, costing one relaxed atomic load per
segment destruction.

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
| `operation_types.hpp` | Operator structs, `StatsComparison`, `ValueRange` |
| `query_planner.cpp` | `plan_query()`, `and_filter_expression_contexts()` |
| `../version/admission_handler.hpp` | `ProcessingUnitAdmissionHandler`, residency budget and read window defaults |
| `../util/segment_residency_tracker.hpp` | Test-only decoded-segment residency high-water mark |

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
