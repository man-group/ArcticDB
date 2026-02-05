# Processing Pipeline

This document explains the processing pipeline architecture in ArcticDB, including the entity-component system, clause processing, and data flow.

## Entity-Component System (entt)

ArcticDB uses the [entt](https://github.com/skypjack/entt) Entity-Component-System framework to manage data segments during query processing.

### What Entities Represent

An **entity** represents a data segment (or a piece of one). Each entity is a lightweight ID (`entt::entity`, a 64-bit integer) that can have multiple **components** attached to it.

### Components Attached to Entities

| Component Type | Description |
|---------------|-------------|
| `std::shared_ptr<SegmentInMemory>` | The actual data segment in memory |
| `std::shared_ptr<RowRange>` | Which rows of the original dataframe this segment covers |
| `std::shared_ptr<ColRange>` | Which columns this segment covers |
| `std::shared_ptr<AtomKey>` | The storage key this segment was read from |
| `EntityFetchCount` | Reference count for how many processing units need this segment |

### Why This Design?

The ECS pattern provides several benefits:

1. **Decoupled Data Storage**: Components are stored in contiguous arrays per type, not bundled together. This allows clauses to request only the components they need.

2. **Dynamic Composition**: Different clauses can attach/detach components as processing proceeds.

3. **Reference Counting for Shared Segments**: The `EntityFetchCount` component tracks how many processing units need a segment. When multiple clauses need the same segment, it's shared and only freed when all consumers are done.

## ComponentManager

The `ComponentManager` class (`cpp/arcticdb/processing/component_manager.hpp`) wraps entt's registry with thread-safe access:

```cpp
class ComponentManager {
    entt::registry registry_;
    std::shared_mutex mtx_;

    // Add entities with components
    template<class... Args>
    std::vector<EntityId> add_entities(Args... args);

    // Fetch components for entities
    template<class... Args>
    std::tuple<std::vector<Args>...> get_entities(const std::vector<EntityId>& ids);
};
```

Key operations:
- `add_entities()` - Register new entities with their components
- `get_entities()` - Fetch components for a list of entity IDs
- `get_entities_and_decrement_refcount()` - Fetch and decrement reference count

## ProcessingUnit

A `ProcessingUnit` (`cpp/arcticdb/processing/processing_unit.hpp`) is a temporary container used during clause processing:

```cpp
struct ProcessingUnit {
    std::optional<std::vector<std::shared_ptr<SegmentInMemory>>> segments_;
    std::optional<std::vector<std::shared_ptr<RowRange>>> row_ranges_;
    std::optional<std::vector<std::shared_ptr<ColRange>>> col_ranges_;
    std::optional<std::vector<std::shared_ptr<AtomKey>>> atom_keys_;
    std::optional<bucket_id> bucket_;

    std::shared_ptr<ExpressionContext> expression_context_;  // AST for filter/project
    std::unordered_map<std::string, VariantData> computed_data_;  // Cached computations
};
```

## Pipeline Flow

### Phase 1: Initial Entity Creation

```
Storage Keys (from TABLE_INDEX)
        │
        ▼
┌─────────────────────────────────────┐
│  get_entity_ids_and_position_map()  │
│                                     │
│  Creates one EntityId per segment   │
│  Maps: EntityId <-> segment position│
└─────────────────────────────────────┘
        │
        ▼
  Entity IDs grouped by "work unit"
  (determined by structure_for_processing)
```

### Phase 2: Segment Loading & Component Population

When segment futures resolve, components are added:

```cpp
component_manager->add_entity(
    entity_id,
    segment_fetch_count,
    std::make_shared<SegmentInMemory>(seg),
    std::make_shared<RowRange>(slice.row_range),
    std::make_shared<ColRange>(slice.col_range),
    std::make_shared<AtomKey>(slice.key())
);
```

### Phase 3: Clause Processing

Each clause's `process()` method:

1. **Gathers** entities into a `ProcessingUnit`:
```cpp
auto proc = gather_entities<
    std::shared_ptr<SegmentInMemory>,
    std::shared_ptr<RowRange>,
    std::shared_ptr<ColRange>
>(*component_manager_, std::move(entity_ids));
```

2. **Transforms** the data (filter, project, aggregate, etc.)

3. **Pushes** new entities with transformed components:
```cpp
return push_entities(*component_manager_, std::move(transformed_proc));
```

### Phase 4: Restructuring Between Clauses

Some clauses need to restructure how entities are grouped. See the `structure_for_processing` section below.

## Clause Interface

All clauses implement the `IClause` interface (`cpp/arcticdb/processing/clause.hpp`):

```cpp
struct IClause {
    // Group segments for processing
    std::vector<std::vector<size_t>> structure_for_processing(std::vector<RangesAndKey>&);
    std::vector<std::vector<EntityId>> structure_for_processing(std::vector<std::vector<EntityId>>&&);

    // Process a work unit
    std::vector<EntityId> process(std::vector<EntityId>&& entity_ids) const;

    // Metadata
    const ClauseInfo& clause_info() const;
};
```

### Available Clause Types

| Clause | Purpose | Input Structure | Output Structure |
|--------|---------|-----------------|------------------|
| `FilterClause` | Filter rows by predicate | ROW_SLICE | ROW_SLICE |
| `ProjectClause` | Compute new columns | ROW_SLICE | ROW_SLICE |
| `PartitionClause` | Partition by column value | ROW_SLICE | HASH_BUCKETED |
| `AggregationClause` | Aggregate after groupby | HASH_BUCKETED | HASH_BUCKETED |
| `ResampleClause` | Time-based resampling | ROW_SLICE | TIME_BUCKETED |
| `RowRangeClause` | head/tail operations | ALL | ROW_SLICE |
| `DateRangeClause` | Filter by date range | ROW_SLICE | ROW_SLICE |
| `MergeClause` | Merge sorted segments | ALL | ROW_SLICE |
| `ConcatClause` | Concatenate symbols | MULTI_SYMBOL | ROW_SLICE |

## structure_for_processing

`structure_for_processing` determines **how entities are grouped into work units** before processing.

### Two Overloads

#### 1. Initial Structuring (before first clause)
```cpp
std::vector<std::vector<size_t>> structure_for_processing(std::vector<RangesAndKey>& ranges_and_keys)
```
- Called **once** at pipeline start
- Input: Raw segment references from storage
- Output: Groups of **indexes** into the input vector
- Can **reorder** and **filter** the input

#### 2. Inter-Clause Restructuring
```cpp
std::vector<std::vector<EntityId>> structure_for_processing(std::vector<std::vector<EntityId>>&& entity_ids_vec)
```
- Called **between clauses** when restructuring is needed
- Input: Entity IDs grouped by the previous clause's output structure
- Output: Entity IDs regrouped for this clause's input structure

### ProcessingStructure Enum

Each clause declares what structure it needs and produces:

```cpp
enum class ProcessingStructure {
    ROW_SLICE,      // Segments grouped by row range (most common)
    TIME_BUCKETED,  // Segments grouped by time buckets (resample)
    HASH_BUCKETED,  // Segments grouped by hash of grouping column (groupby)
    ALL,            // All segments in one group (head/tail)
    MULTI_SYMBOL    // Multiple symbols being processed together (concat)
};

struct ClauseInfo {
    ProcessingStructure input_structure_{ProcessingStructure::ROW_SLICE};
    ProcessingStructure output_structure_{ProcessingStructure::ROW_SLICE};
};
```

### When Restructuring Happens

The pipeline checks if consecutive clauses have compatible structures:

```cpp
// In MemSegmentProcessingTask::operator()
if ((*it)->clause_info().output_structure_ != (*next_it)->clause_info().input_structure_)
    break;  // Stop processing, restructure needed
```

### Structuring Strategies

#### `structure_by_row_slice` (most common)

Groups segments that share the same row range together:

```
Input segments:        Output grouping:
┌──────────────────┐
│ Seg0: rows 0-100 │   Group 0: [Seg0, Seg1]  (rows 0-100, different cols)
│ cols 0-5         │
├──────────────────┤
│ Seg1: rows 0-100 │
│ cols 5-10        │
├──────────────────┤
│ Seg2: rows 100-200│  Group 1: [Seg2, Seg3]  (rows 100-200, different cols)
│ cols 0-5         │
├──────────────────┤
│ Seg3: rows 100-200│
│ cols 5-10        │
└──────────────────┘
```

#### `structure_all_as_one`

All segments in a single group (for head/tail that need global row counting):

```
Input segments:        Output grouping:

Seg0, Seg1, Seg2...    Group 0: [Seg0, Seg1, Seg2, ...]  (all together)
```

## Pipeline Execution Flow

```
                          ┌─────────────────────────────┐
                          │     RangesAndKey list       │
                          │  (from TABLE_INDEX)         │
                          └─────────────────────────────┘
                                       │
                    ┌──────────────────┼──────────────────┐
                    │   Clause 0: structure_for_processing(RangesAndKey&)
                    │   Returns: [[0,1], [2,3], [4,5]]   │
                    └──────────────────┼──────────────────┘
                                       │
                                       ▼
               ┌───────────────────────────────────────────┐
               │  Create EntityIds, load segments async    │
               │  Map: EntityId -> segment position        │
               └───────────────────────────────────────────┘
                                       │
         ┌─────────────────────────────┼─────────────────────────────┐
         ▼                             ▼                             ▼
   Work Unit 0                   Work Unit 1                   Work Unit 2
   [E0, E1]                      [E2, E3]                      [E4, E5]
         │                             │                             │
         └──────────── process() ──────┴──────────── process() ──────┘
                              (FilterClause)
                                       │
                              output: ROW_SLICE
                              ─────────────────
                              next input: ALL  (RowRangeClause head/tail)
                                       │
                           *** RESTRUCTURE NEEDED ***
                                       │
                    ┌──────────────────┼──────────────────┐
                    │ RowRangeClause::structure_for_processing(EntityIds)
                    │ Groups ALL entities into one unit    │
                    └──────────────────┼──────────────────┘
                                       │
                                       ▼
                               Work Unit 0
                            [E0', E1', E2', E3', E4', E5']
                                       │
                                  process()
                             (RowRangeClause)
                                       │
                                       ▼
                              Final EntityIds
```

## Example: Filter Clause Processing

```cpp
std::vector<EntityId> FilterClause::process(std::vector<EntityId>&& entity_ids) const {
    // 1. Gather segment data from component manager
    auto proc = gather_entities<
        std::shared_ptr<SegmentInMemory>,
        std::shared_ptr<RowRange>,
        std::shared_ptr<ColRange>
    >(*component_manager_, std::move(entity_ids));

    // 2. Evaluate AST to get filter bitset
    auto variant_data = proc.get(expression_context_->root_node_name_);
    util::BitSet filter_bitset = std::get<util::BitSet>(variant_data);

    // 3. Apply filter to segments
    proc.apply_filter(std::move(filter_bitset), optimisation_);

    // 4. Push filtered segments as new entities
    return push_entities(*component_manager_, std::move(proc));
}
```

## QueryBuilder AST and Clauses

The Python `QueryBuilder` builds a list of `Clause` objects stored in `ReadQuery::clauses_`. The AST (ExpressionNode tree) is embedded inside specific clause types.

### Mapping: Python Operation to C++ Clause

| Python Operation | C++ Clause Type | Contains AST? |
|-----------------|-----------------|---------------|
| `q[q["col"] > 5]` | `FilterClause` | Yes |
| `q.apply("new_col", expr)` | `ProjectClause` | Yes |
| `q.groupby("col")` | `PartitionClause` | No |
| `q.agg({...})` | `AggregationClause` | No |
| `q.resample("1h")` | `ResampleClause` | No |
| `q.head(n)` / `q.tail(n)` | `RowRangeClause` | No |

### ExpressionContext

For `FilterClause` and `ProjectClause`, the AST is stored in an `ExpressionContext`:

```cpp
struct ExpressionContext {
    ConstantMap<ExpressionNode> expression_nodes_;  // All AST nodes by name
    ConstantMap<Value> values_;                     // Literal values
    ConstantMap<ValueSet> value_sets_;              // isin/isnotin sets
    ConstantMap<util::RegexGeneric> regex_matches_; // Regex patterns
    VariantNode root_node_name_;                    // Entry point to the AST
};
```

## Code References

- ComponentManager: `cpp/arcticdb/processing/component_manager.hpp`
- ProcessingUnit: `cpp/arcticdb/processing/processing_unit.hpp`
- Clause definitions: `cpp/arcticdb/processing/clause.hpp`
- Clause utilities: `cpp/arcticdb/processing/clause_utils.hpp`
- Pipeline scheduling: `cpp/arcticdb/version/version_core.cpp` (schedule_clause_processing, schedule_remaining_iterations)
- ExpressionContext: `cpp/arcticdb/processing/expression_context.hpp`
- ExpressionNode: `cpp/arcticdb/processing/expression_node.hpp`

### Query Processing Flow
1. `read_and_schedule_processing()` in `version_core.cpp:1147`
2. Generates `ranges_and_keys` from `PipelineContext` (one per TABLE_DATA key)
3. First clause's `structure_for_processing()` groups segments for processing
4. `filter_index()` in `read_pipeline.hpp:45` applies row/index filters to determine which segments to read
5. Currently, predicates in `FilterClause` are evaluated AFTER reading segment data

### QueryBuilder Clauses

  Overview

  The Python QueryBuilder stores two parallel lists:
  - clauses: List of C++ clause objects (via pybind11)
  - _python_clauses: Python representations for pickling/equality

  Each operation on QueryBuilder appends a Clause to clauses_. The AST (ExpressionNode tree) is embedded inside specific clause types, not stored separately.

  Mapping: Python Operation → C++ Clause
  ┌───────────────────────────┬──────────────────────────────────────────────┬──────────────────────────────┐
  │     Python Operation      │               C++ Clause Type                │        Contains AST?         │
  ├───────────────────────────┼──────────────────────────────────────────────┼──────────────────────────────┤
  │ q[q["col"] > 5] (filter)  │ FilterClause                                 │ Yes - in expression_context_ │
  ├───────────────────────────┼──────────────────────────────────────────────┼──────────────────────────────┤
  │ q.apply("new_col", expr)  │ ProjectClause                                │ Yes - in expression_context_ │
  ├───────────────────────────┼──────────────────────────────────────────────┼──────────────────────────────┤
  │ q.groupby("col")          │ PartitionClause<GrouperType, BucketizerType> │ No                           │
  ├───────────────────────────┼──────────────────────────────────────────────┼──────────────────────────────┤
  │ q.agg({...})              │ AggregationClause                            │ No                           │
  ├───────────────────────────┼──────────────────────────────────────────────┼──────────────────────────────┤
  │ q.resample("1h")          │ ResampleClause<closed_boundary>              │ No                           │
  ├───────────────────────────┼──────────────────────────────────────────────┼──────────────────────────────┤
  │ q.head(n) / q.tail(n)     │ RowRangeClause                               │ No                           │
  ├───────────────────────────┼──────────────────────────────────────────────┼──────────────────────────────┤
  │ q.row_range((start, end)) │ RowRangeClause                               │ No                           │
  ├───────────────────────────┼──────────────────────────────────────────────┼──────────────────────────────┤
  │ q.date_range(...)         │ DateRangeClause                              │ No                           │
  ├───────────────────────────┼──────────────────────────────────────────────┼──────────────────────────────┤
  │ q.concat(...)             │ ConcatClause                                 │ No                           │
  └───────────────────────────┴──────────────────────────────────────────────┴──────────────────────────────┘
  AST Storage: ExpressionContext

  For FilterClause and ProjectClause, the AST is stored in an ExpressionContext:

  struct ExpressionContext {
      ConstantMap<ExpressionNode> expression_nodes_;  // All AST nodes by name
      ConstantMap<Value> values_;                     // Literal values
      ConstantMap<ValueSet> value_sets_;              // isin/isnotin sets
      ConstantMap<util::RegexGeneric> regex_matches_; // Regex patterns
      VariantNode root_node_name_;                    // Entry point to the AST
      bool dynamic_schema_{false};
  };

  The ExpressionNode objects reference each other by name (string), forming a DAG.

  Example: How a Filter is Stored

  Python:
  q = q[(q["price"] > 100) & (q["volume"] < 1000)]

  Results in a FilterClause containing:

  FilterClause {
      clause_info_.input_columns_ = {"price", "volume"}
      expression_context_ = ExpressionContext {
          expression_nodes_ = {
              "(Column["price"] GT Num(100))": ExpressionNode(ColumnName("price"), ValueName("Num(100)"), GT),
              "(Column["volume"] LT Num(1000))": ExpressionNode(ColumnName("volume"), ValueName("Num(1000)"), LT),
              "((Column["price"] GT Num(100)) AND (Column["volume"] LT Num(1000)))": ExpressionNode(
                  ExpressionName("(Column["price"] GT Num(100))"),
                  ExpressionName("(Column["volume"] LT Num(1000))"),
                  AND
              )
          }
          values_ = {
              "Num(100)": Value(100),
              "Num(1000)": Value(1000)
          }
          root_node_name_ = ExpressionName("((Column["price"] GT Num(100)) AND (Column["volume"] LT Num(1000)))")
      }
      root_node_name_ = ExpressionName("...")  // Same as expression_context_.root_node_name_
  }

  Processing Pipeline

  When read() is called:

  1. ReadQuery is created with clauses_ from QueryBuilder
  2. Each clause's structure_for_processing() determines how segments are grouped
  3. Each clause's process() method is called on those groups
  4. For FilterClause/ProjectClause, process() evaluates the AST:
    - Looks up root_node_name_ in expression_context_.expression_nodes_
    - Recursively evaluates the tree
    - Column references fetch data from the segment
    - Values/ValueSets are fetched from the context maps

  Key Design Points

  1. Clauses are independent stages in a pipeline - each transforms data
  2. AST is only in filter/project clauses - other clauses have simpler parameters
  3. Names as references - ExpressionNodes reference children by string name, not pointer
  4. Flat storage - All nodes stored in a flat map, tree structure is implicit via names

For filters, the important case is where VariantData is an ExpressionName. We use this to look up an ExpressionNode
and execute it with `ExpressionNode::compute`. This brings us to the operation dispatch and `visit_binary_boolean`
in `dispatch_binary`.
