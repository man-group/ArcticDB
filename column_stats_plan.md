# Column Stats Query Optimization Plan

Epic with outline of tasks: https://github.com/man-group/ArcticDB/issues/1588

Docs are in `processing_pipeline.md` and `column_stats.md`

## Overview

Use column stats (MIN/MAX values per segment) to skip reading TABLE_DATA keys that cannot possibly match QueryBuilder predicates.

## Design Decisions

- **Automatic**: Apply pruning automatically when column stats exist (no API changes needed)
- **Simple predicates only**: Support `>`, `>=`, `<`, `<=`, `==` comparisons initially
- **Debug logging**: Log pruned segments at debug level for troubleshooting

## Implementation Plan

This is for the first task in the epic,

```
Use column stats to eliminate unnecessary IOs with the QueryBuilder. Should include logging how many data segment IOs were bypassed due to the stats.
```

### Step 1

The hook in the code is `read_indexed_keys_to_pipeline`. This reads the index key. Once we have the index key, we should read the column stats key, using existing
functionality to read column stats.

If there is no column stats key, proceed without pruning based on column stats, rather than erroring.

We should create a new `FilterQuery` object representing the filtering based on the column stats combined with the user's queries,
and then use it after the current `filter_index` call to filter out any rows from the index that column stats rules out (having already ruled
out any columns and date ranges that are not in the query).

We should read through the clauses, applying column stats filtering to any FilterClause that we see along the way.
As soon as we see any clause other than a FilterClause we should stop applying the column stats filtering.

Column stats segments only have the statistics themselves and a start and end index. They apply to any rows in the index segment
with the same start and end index.

We need to evaluate the FilterClause AST, creating a bitset of rows in the index segment that are ruled in or out for loading
based on the column statistics. At leaf nodes in the AST we will evaluate them against column stats, for example for a node:

```
"col1" > 5
```

with an index representing data keys and column stats:

```
DATA_KEY    MIN_col1    MAX_col1
1           1           2
2           1           6
3           1           7
```

we would create a bitset [011] since data keys 2,3 could satisfy the predicate whereas 1 could not. The bitset represents which
rows in the index we want to _keep_.

It is very important that the bitset represents rows to keep because that is compatible with normal AND/OR semantics.

If the column stats are not relevant to the given filter (eg a filter references column col_a but column stats only exist for col_b)
we should return an "all-true" bitset as no pruning is possible.

Null handling should be the same as with existing query builder filters, for example in this snippet q["a"] < 2 filters out the np.nan:

```

In [164]: df = pd.DataFrame({"a": [1,2,3,np.nan]})

In [165]: lib.write("tst", df)
Out[165]: VersionedItem(symbol='tst', library='tst', data=n/a, version=2, metadata=None, host='MEM()', timestamp=1770305640432028599)

In [166]: q = QueryBuilder()

In [167]: q = q[q["a"] < 2]

In [168]: lib.read("tst", query_builder=q).data
Out[168]:
     a
0  1.0
```

Note that we need to handle all ArcticDB's supported types except strings.

Above leaf nodes, we should combine nodes of the AST using the existing `dispatch_binary` functionality (and `dispatch_unary` if relevant).
We should be able to reuse some logic from `FilterClause` which has to evaluate the AST against data segments.

For equality col == value, pruning is possible when value < MIN OR value > MAX. You should also handle `OperationType::NE`.

We should add tests to `test_column_stats.py` to verify our work. They should use the query stats functionality (see `test_query_stats.py` for usage)
to verify that we are indeed skipping blocks where possible.

You should include debug logging recording how many segments you managed to prune using column stats

## Tests

- Filter on column with stats → segments pruned
- Filter on column without stats → no pruning, correct results
- AND filter (one column with stats, one without)
- OR filter
- All segments pruned → empty result
- Boundary: col > MAX_value_in_data (should prune all)
- Boundary: col == MIN or col == MAX (should NOT prune that segment)
- Negation in filters
- QueryBuilder instance with multiple filter clauses
- QueryBuilder instance with non-filter clauses followed by filter clauses - check we do not apply column stats based off the filter clauses
- Tests with dynamic schema where the column type varies across segments
- Tests where a new column is added to the dataframe, so is not present in older segments

## Future - Do not implement any of this section now

We should load the column stats key in parallel with the index key rather than doing the IO in serial.
Do not implement support for `isin` and `isnotin` yet.

## Development Approach

Work in small steps. Write tests as early as possible. Commit whenever you have a working piece of code with tests, even if it isn't enough to
implement the whole plan.
