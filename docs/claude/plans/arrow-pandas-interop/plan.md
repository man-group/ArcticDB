# Arrow / pandas interop — implementation proposal

Target: the 48 `xfail`s in `python/tests/unit/arcticdb/version_store/test_arrow_pandas_interop.py`
(110 pass today). 46 are cleared here; 2 stay xfailed pending the column-rename API (§4.4). The
design is RFC option A: the Arrow normalization metadata may carry the pandas metadata.

No compatibility constraints: Arrow input is opt-in behind a private
`_nvs._set_allow_arrow_input()`, so nothing outside beta has written Arrow data.

## 1. Shape of the change

| # | Piece | xfails |
|---|-------|--------|
| A | Arrow → pandas denormalization reads timezones from `ExperimentalArrow.columns` | 2 |
| B | One-dimensional Arrow ↔ pandas Series: naming, Series output both ways, reject invalid Arrow column names | 4 |
| C | `combine_schema` combines Arrow with pandas, storing the pandas metadata inside the Arrow message | 40 |

A and B need no protobuf change and can land first.

## 2. Format change

```proto
message ExperimentalArrow {
    bool has_index = 1;
    map<string, ColumnMeta> columns = 2;
    bool one_dimensional = 3;
    optional string polars_series_name = 4;

    // Pandas metadata for data that also has a pandas provenance. Unset for Arrow-only data.
    // Mirrors the top-level input_type oneof, so a pandas norm is embedded by CopyFrom.
    oneof pandas_input_type {
        PandasDataFrame df = 5;
        PandasDataFrame series = 6;
        NormalisedTimeSeries ts = 7;
    }
}
```

A oneof is already optional — unset is a state of its own (`PANDAS_INPUT_TYPE_NOT_SET`,
`WhichOneof(...) is None`) — so Arrow-only data needs no "no pandas" arm, and no arm may be labelled
`optional` (protoc rejects labels on oneof members, which have explicit presence anyway).

The arms mirror the top-level oneof exactly, `NormalisedTimeSeries` included, rather than being
unified on `PandasDataFrame` or wrapped in an `EmbeddedPandas { PandasDataFrame; enum input_type }`.
Mirroring means embedding is a `CopyFrom` of the same message type, the existing reflection-based
`pandas_common()` helper generalises unchanged, and `WhichOneof` returns the same `"df"`/`"series"`/
`"ts"` strings the Python denormalizer already dispatches on. Unifying `NormalisedTimeSeries` into
`PandasDataFrame` (they differ only by the write-only `mark` field) is worthwhile cleanup, but as its
own change, at the top level too.

`TimeFrame` is `arcticdb.version_store._common.TimeFrame`, written with `lib.write(sym, tf)`. It is
the only producer of a `ts` norm and appears only in tests, so supporting it buys little — but with
the mirrored oneof it costs one arm and one dispatch branch, against a carve-out that raises.

### Invariants

1. **Timezones** are held in `columns[<field name>].timezone` **and** in the embedded pandas index
   metadata, kept in step. `columns` is the single comparison site during a combine; the merged
   result is written back to the embedded pandas index, so a cleared timezone is cleared in both.
   Reads take index timezones from either (they agree) and non-index column timezones from
   `columns`, which is the only place that can express them.
2. **Shape**: `has_index` and `one_dimensional` are authoritative; the embedded pandas message must
   agree (§4.2).
3. **Series name**: `polars_series_name` is authoritative, with the embedded `Pandas.name` /
   `has_name` kept in step.
4. Index names and `fake_name`, `RangeIndex` start/step, multi-index level positions and
   `fake_field_pos`, `col_names`, `has_synthetic_columns` live only in the embedded pandas message.

Limitation of 1: the timezone of a multi-index level beyond the first is not mirrored into `columns`,
because the metadata does not record what that level's column is called (§4.4), so it stays in the
embedded pandas metadata alone and does not reach Arrow output.

## 3. One-dimensional Arrow naming (piece B)

A 1-D Arrow input (`pa.Array`, `pa.ChunkedArray`, `pl.Series`) is stored as a single column named
`__array__`, with the real name in `polars_series_name`; a pandas Series stores its value column
under the series name, or `"0"` when unnamed. So the two can never reconcile:

```
E_DESCRIPTOR_MISMATCH Cannot append: Series names must match, '0' against '__array__'
```

`to_pyarrow_table` should name that column as pandas does: the series name when non-empty, else
`"0"`. An empty polars name (including `pl.Series(None, ...)`, which polars stores as `""`) means
unnamed and also maps to `"0"`, with the empty name still round-tripped from `polars_series_name`.
Empty Arrow input column names are rejected: sparrow aborts the process on one, and measured on
pyarrow 20 / polars 1.35, `pl.from_arrow` silently renames an empty field to `column_0`, so an empty
name cannot round-trip for a polars user however it is stored. That is also why `"0"` rather than
`""` is the right name for an unnamed one-dimensional structure.

`one_dimensional` then drives output type in both directions:

| Stored | Arrow output | pandas output |
|--------|--------------|---------------|
| 1-D Arrow, or Series with `RangeIndex` (`one_dimensional`) | `ChunkedArray` / `pl.Series` | `pd.Series` |
| Series with a stored index | `pa.Table` | `pd.Series` |
| Table / DataFrame | `pa.Table` | `pd.DataFrame` |

## 4. C++ changes (piece C)

### 4.1 `arrow_norm_from_pandas`

Combining Arrow with pandas is combining two Arrow metadatas, one of which has pandas metadata
embedded in it. So `combine_norm_metadata` describes each pandas schema in Arrow terms first,
whenever any schema is Arrow, and the fold itself only ever combines like with like — no
`accumulate_arrow_and_pandas_norm`, and no descriptors in `accumulate_norm_metadata`'s signature.
`arrow_norm_from_pandas` takes the whole `OutputSchema`, which is what the fold has to hand:

- the pandas message embedded by `CopyFrom`;
- `has_index` from the index descriptor. Not from `is_physically_stored`, which is also true for a
  non-datetime index — a string index, or a multi-index whose first level is not a timeseries — that
  is stored as a leading row-count column rather than as the symbol's timeseries index. Verified: a
  `MultiIndex(["ts", "grp"])` frame is `TIMESTAMP`/`field_count=1`, a
  `MultiIndex([[10, 20], ["a", "b"]])` frame is `ROWCOUNT`/`field_count=0`;
- `one_dimensional` for a Series whose index is not physically stored, with `polars_series_name`;
- an empty `columns` entry per timestamp column. Arrow records one for **every** timestamp column,
  timezone-naive ones included, so an absent entry means the schema does not have that column;
  without them a naive pandas column would be taken for an absent one and inherit the Arrow side's
  timezone. This is the one part that needs the descriptor — an index level's own name is in the
  metadata (`PandasIndex.name`, or `PandasMultiIndex.name` for level 0, both equal to the stored
  field name, `"index"` when unnamed);
- the index timezone mirrored into that entry.

`accumulate_arrow_and_arrow_norm` then merges the two, and with them their embedded pandas metadata:
when both sides have one it hands them to `accumulate_pandas_and_pandas_norm` unchanged; when only
one has, it is inherited whole rather than merged against a synthesised default, which is what
preserves `has_synthetic_columns` (the merge ANDs it).

### 4.2 Checks

Each raises `E_INCOMPATIBLE_OBJECTS` / `E_INCOMPATIBLE_INDEX`, so the user sees a
`NormalizationException`:

1. `has_index` agreement, which for a converted pandas schema is index-kind agreement (§4.1).
2. `one_dimensional` agreement — this is what makes `DataFrame` + `ChunkedArray` raise
   (`test_combine_dataframe_with_chunked_array_raises`; today append raises a bare descriptor
   mismatch and concat does not raise at all).
3. Both sides embedded ⇒ the embedded oneof arms must agree, reusing the existing
   `check_same_input_type` messages against the embedded oneof.

Not done: an embedded `series` combined with an Arrow table of two or more data columns should raise,
since a Series cannot acquire a second data column. It needs the column count, so it belongs in
`add_required_fields` (`field_count() == num_physical_required_columns()` when there is a value
column) rather than in the metadata merge. No test covers it; left out to keep the pandas Series
paths untouched.

### 4.3 `required_fields_info`

`pipeline/index_utils.cpp` must consult the embedded pandas message:

- `has_multi_index` and `num_physical_indices` from the embedded index metadata when present, else
  `has_index ? 1 : 0`.
- `has_series_value_column` = top-level `series`, or `one_dimensional && !has_index` — a
  one-dimensional structure written as the index has no value column, only the index field. The
  embedded arm needs no case of its own: a converted Series is `one_dimensional` when it has no index
  column, and has a leading index field otherwise.

This is what lets an Arrow table's leading columns be treated as index levels at all, and a 1-D
Arrow column reconcile against a pandas Series value column. It has to land with the naming change
in §3, not after it: once a 1-D structure is stored under its own name, name reconciliation is the
only thing that keeps Arrow-only concatenation of differently-named 1-D structures working.

### 4.4 Multi-index level names — needs a decision

pandas stores multi-index levels ≥ 1 as `__idx__<name>`, or `__fkidx__N` when unnamed (verified:
`MultiIndex(["ts", "grp"])` → fields `["ts", "__idx__grp", "col"]`; unnamed → `["index",
"__fkidx__1", "col"]`). Level 0 is not mangled. An Arrow table names its columns plainly.

No demangling. Required-field names must match as stored, because making them match would mean
renaming the incoming frame's columns before writing its data keys — otherwise the index key's
descriptor and the appended segments' descriptors disagree, which breaks static-schema reads.
Consequences:

- **Unnamed** pandas multi-index against Arrow: concat reconciles the mismatched level names to
  unnamed and the result reads back as `["__index_level_0__", "__index_level_1__", "col"]` (§5);
  append and update raise. Both are what the tests expect, and neither needs level-name matching.
- **Named** pandas multi-index against Arrow: raises unless the Arrow table happens to name the
  column `__idx__grp`. So `test_append_non_timeseries_multiindex_pandas_with_unindexed_arrow` (2
  parametrizations) stays xfailed, with `rename_columns_arrow_compat` as its reason.

That API (Alex, monday 12844033169, xfail suite in `test_arrow_col_rename.py`) is the escape hatch
for every pandas naming feature we decline to reconcile — duplicate, `None`, empty and integer
labels, `has_synthetic_columns`, `fake_name` — since it rewrites a symbol into names Arrow can
express. **Coordination needed**: does it also rewrite multi-index levels ≥ 1 from `__idx__<name>` to
plain `<name>`? Its tests assert the read-back level names and that stored names are unique, both of
which hold with or without the prefix, so it is not pinned. Arrow interop needs the prefix gone; if
it stays, a renamed symbol still cannot take an Arrow append that names the level plainly.

### 4.5 Name mismatch application

`apply_required_name_mismatches` gives up when `mutable_pandas_common` returns null, i.e. for Arrow.
It must reach the embedded pandas message and, on a Series name mismatch, also clear
`polars_series_name`. The inline `polars_series_name` reconciliation in
`accumulate_arrow_and_arrow_norm` then becomes a `mismatches.add_series_name(...)` call, so index,
multi-index level and Series name reconciliation all end in one place.

### 4.6 Accessors

Add `embedded_pandas_common()` / `mutable_embedded_pandas_common()` beside `pandas_common()` in
`entity/normalization_utils.{hpp,cpp}` rather than widening `pandas_common()` silently, and audit:

| Call site | Uses |
|-----------|------|
| `index_utils.cpp::required_fields_info` | top-level **or** embedded |
| `schema_checks.cpp::align_rowrange_norm_for_append`, `update_rowrange_norm_for_append` | top-level **or** embedded — a `RangeIndex` Arrow+pandas append still has to align `start` |
| `schema_combine.cpp::apply_required_name_mismatches` | top-level **or** embedded |
| `OutputSchema::is_inferred_from_empty_pandas` | top-level only — a zero-row combined symbol should not start having its schema ignored |
| `schema_combine.cpp::has_arrow_or_pandas` | unchanged |

`clause.cpp` (groupby) and `clause_resample.cpp` write index metadata through `mutable_df()`, which
discards an Arrow norm outright. Pre-existing bug, written up in
[`groupby-resample-arrow-norm-bug.md`](groupby-resample-arrow-norm-bug.md); no monday ticket exists
for it.

## 5. Python changes

`_normalization.py`, `PyArrowNormalizer.denormalize` (Arrow / polars output):

- `experimental_arrow` with an embedded pandas message → run the existing pandas branch (index
  column renames, `__index__` / `__index_level_N__` synthesis, `RangeIndex` reconstruction,
  `pandas_metadata` on the schema), but **skip the timezone step**: for an `experimental_arrow` norm
  the C++ output has already applied the timezone from `columns`, and applying it twice shifts
  values. This is also what gives the concat of an Arrow table with an unnamed-multi-index frame a
  sensible Arrow shape, with no further C++ metadata work.
- `one_dimensional` → return `item.column(0)`, including when the norm is a top-level `series` whose
  index is not physically stored.

`_normalization.py`, `CompositeNormalizer.denormalize` and `DataFrameNormalizer` (pandas output):

- `_pandas_norm_meta_from_arrow_norm_meta` uses the embedded pandas message when present, else
  synthesises as today, and in both cases takes non-index column timezones from `columns` — pandas
  metadata cannot express them.
- Dispatch on the embedded oneof, which returns the same `"df"` / `"series"` / `"ts"` as the
  top-level one, to `DataFrameNormalizer` / `SeriesNormalizer` / `TimeFrameNormalizer`. A
  `one_dimensional` Arrow norm with no embedded pandas also goes to `SeriesNormalizer`, named from
  `polars_series_name` with unset meaning `None`.

`_store.py`:

- `get_info` describes a combined symbol as Arrow, by the fields it stores, which needs no new code.
  `input_type` becomes `"arrow"` for a symbol that was pandas before an Arrow append — accepted.
- The polars Series name lookup (≈2968 and ≈3945) must also handle a pandas `series` norm, for
  piece B.

## 6. Staging

1. **Piece A** — Arrow → pandas timezones. Clears `test_write_arrow_{index,column}_timezone_read_pandas`.
2. **Piece B** — 1-D naming, Series output both ways, invalid Arrow column names. Clears
   `test_write_arrow_{array,chunked_array}_read_pandas`, `test_write_polars_series_read_pandas`,
   `test_write_pandas_series_rangeindex_read_arrow`, and unskips
   `test_write_empty_column_name_fails`.
3. **Piece C1** — protobuf field, `accumulate_arrow_and_pandas_norm`, the checks and the accessor
   audit, with C++ unit tests in `test_schema_combine.cpp`. Clears the timezone-mismatch, synthetic
   column, Series-with-1-D-Arrow and `DataFrame`-with-1-D-Arrow groups.
4. **Piece C2** — `required_fields_info` and the embedded reach of
   `apply_required_name_mismatches`. Clears `test_combine_unnamed_multiindex_*`.

New coverage rather than un-xfailing: `TimeFrame` combined with Arrow, both directions, append and
concat. The 110 tests passing today must keep passing — in particular
`test_combine_matching_schema_indexed`, which asserts through `to_pandas()` and so depends on
`pandas_metadata` still being attached to a combined symbol's Arrow output.

## 7. Open questions

1. **§4.4**: the named-multi-index case, and the `__idx__` question for `rename_columns_arrow_compat`
   to settle with Alex.
2. **Groupby / resample clobbering**: separate PR plus a new monday ticket, or fold the fix in? It is
   ~30 lines but carries its own behaviour change, so the write-up recommends separate.
