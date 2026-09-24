# Bug: groupby and resample discard the Arrow normalization metadata

Spun out of the Arrow/pandas interop plan. Pre-existing, not caused by that work.

## What happens

`AggregationClause::modify_schema` (`cpp/arcticdb/processing/clause.cpp:553`) and
`ResampleClause::modify_schema` (`cpp/arcticdb/processing/clause_resample.cpp:68-82`) write the
output index metadata through `norm_metadata_.mutable_df()`. `mutable_df()` on a norm whose
`input_type` is `experimental_arrow` switches the oneof, so the whole `ExperimentalArrow` message is
discarded and the symbol's output metadata becomes a `PandasDataFrame` describing an index that the
Arrow data never had.

It is silent: the read still produces a plausible table, because `PyArrowNormalizer.denormalize`
takes the pandas branch, renames column 0 to the grouping column's name and attaches
`pandas_metadata`.

```python
lib.write("sym", pa.table({"g": pa.array(["a", "a", "b"], pa.large_string()),
                           "v": pa.array([1, 2, 3], pa.int64())}))
q = QueryBuilder().groupby("g").agg({"v": "sum"})
lib.read("sym", query_builder=q)   # works, but the norm metadata is now `df`
```

## What is lost

Everything in `ExperimentalArrow`:

- `columns[...].timezone` — a tz-aware Arrow timestamp column comes back tz-naive after a groupby or
  resample.
- `columns[...].string_format` — a dictionary-encoded (categorical) string column comes back as
  `large_string`.
- `has_index`, `one_dimensional`, `polars_series_name`.

Once the interop work lands, it would also discard the embedded pandas metadata.

## Fix sketch

Route both sites through the pandas-common accessor added by the interop work instead of
`mutable_df()`, and handle the Arrow-only case explicitly:

- **Arrow-only norm**: leave it as Arrow. For groupby the output index is `ROWCOUNT` anyway, so the
  grouping column stays an ordinary leading column; for resample the index column stays the index.
  Drop `columns` entries for columns not in the output schema.
- **Arrow with embedded pandas**: apply today's index rewrite to the embedded message and keep the
  Arrow fields.
- **Pandas norm**: unchanged.

Roughly 30 lines plus tests.

## Why not fold it into the interop PR

It carries a user-visible behaviour decision of its own: today a groupby'd Arrow-written symbol read
as pandas gets the grouping column as its index, because of the clobbering. Keeping the norm as
Arrow gives a `RangeIndex` with the grouping column as a column — arguably right for data that never
had an index, but it is a separate change with its own tests, and it also needs a decision for
resample output on Arrow data.

Recommendation: separate PR, tracked on its own ticket.
