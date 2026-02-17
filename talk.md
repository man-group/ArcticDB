## Hook

In `read_indexed_keys_to_pipeline`, once we have the index key:

- If the clauses has a structure like:

1                                                2                                                3
[DateRangeClause|RowRangeClause|FilterClause...] [ResampleClause|ProjectClause|GroupByClause...] [FilterClause...]

then apply column stats filtering to any filter clauses from group 1.

(resamples, aggregations, projections can change values in the data, so we can't apply stats once we see them)

Then add an extra filter to `queries = get_column_bitset_and_query_functions...`. This is `create_column_stats_filter`
The idea is that it creates a bitset out of each filter clause, by evaluating the AST of the filter against
the column stats.

Difficulty: negation

`q[~(q["a"] > 5)]` has an AST like

        NOT
        |
        >
    |       |
   'a'      5

There are 3 cases for query stats:

case | min | max |
1    |  1  |  3  |
2    |  4  |  6  |
3    |  6  |  8  |

in case 2 we need to include the block but naively evaluating the AST would prune it.

min / max arithmetic operations are another problem

Ideas: push the NOT down the AST (flip > to <=). Either rewrite the AST beforehand or pass a "negated" flag down
  while evaluating?
Or: three-valued logic instead of 1 keep 0 drop, do NONE_MATCH, ALL_MATCH, SOME_MATCH
and have ~NONE_MATCH = ALL_MATCH, ~SOME_MATCH = SOME_MATCH

## Aside - stats in the data blocks

There's a feature flag to write stats in the data blocks - can use those to generate the COLUMN_STATS key type

## Talk with aowens

### Key structure

Column stats keys are better for low bandwidth low latency. Folding in to index key is better for
high bandwidth high latency.

Some index keys are already very large and there are symbols with 100k+ columns. So a bit dangerous
to fold in to the index key.

- Consider tiling out the COLUMN_STATS keys across column offsets to limit how big they are. Only
load the ones that are relevant to columns in the filter queries (AST scan).

### Query Planning

Future: in query_planner.hpp could combine filters together in to one, and do some predicate pushdown.

### AST

There are two big functions for AST analysis at the moment:

```
expression_node.hpp
    // Has all the proper type promotion logic and stuff
    // Actually run the AST
    VariantData compute(ProcessingUnit& seg) const;

    // Calculate output types
    std::variant<BitSetTag, DataType> compute(
            const ExpressionContext& expression_context,
            const ankerl::unordered_dense::map<std::string, DataType>& column_types
    ) const;
```

hopefully we can reuse at least some stuff from these.

### Debt

Consider rewriting

```
// TODO aseaton this stuff is really complicated and could be rewritten
                          auto queries = get_column_bitset_and_query_functions<index::IndexSegmentReader>(
                                  read_query, pipeline_context, dynamic_schema, bucketize_dynamic
                          );

```

### Plan

Epic in Monday is the living document for the plan https://man312219.monday.com/boards/7852509420/pulses/7855219121 

#### Step 1

- Get the column stats keys right + PR
  - Could tile them across columns?
  - We figure out the column offset from the TSD, which is on the index key? Which kind of ruins the parallel IO idea.
  - Check how big a column stats key for 1M segments and 100k columns would be
    - Bear in mind new stats will make this bigger, check how much space a Bloom filter needs
  - Would it help to store a map<column_offset_wrt_data, map<stat_type, column_offset_to_stat>> in the segment header? Rather than parsing the column names?
  - Could store on the index key, but could be very large and users not using the stats would still pay the penalty of loading them

### Step 2 - Simple Reads

Only supporting MINMAX at first

- Plug in to read pipeline behind a feature flag
- First PR: Only support a few simple comparisons and int columns?
  Might not actually simplify things that much but let's see

### Step 3 - Decent reads

Still only MINMAX

- Support different types properly and raise PR
- Support a good subset of the FilterClause grammar (including negation!) raise PR
- Do not support arithmetic operators, like `q["a"] + 4 < 7` or `isin/isnotin`
- Implement support for isin/isnotin raise PR

### Step 4 - Generation

- Support generating stats over columns in a multi-index + PR
Behind a feature flag work on creating stats without users making explicit API calls:
- Generate stats at write time, using data embedded in the data segment (`Statistics.GenerateOnWrite`) + PR
- Maintain them for appends + PR
- Maintain them for updates + PR
- Make sure they are maintained for metadata writes + PR
- Keep a user facing API to generate stats explicitly like we have now (useful to get stats over existing data)

### Step 5 - Profiling and rollout

- Profile generating the stats. Turn it on by default.
- Profile using the column stats. Turn it on by default (with option to override and skip them)

### Step 6 - Gaps

- Support non-numeric columns (strings)?
- Add Bloom filters
- Support arithmetic operators and any other parts of the grammar that we excluded
