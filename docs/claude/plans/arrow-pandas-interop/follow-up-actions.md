# Arrow / pandas interop — agreed actions

Decisions taken while reviewing the prototype, and what each one means for this PR and beyond.

## 1. Empty column names

**Decided.** Writing an Arrow column named `""` is rejected outright. The read path should stop
emitting `""` too: a pandas column labelled `""` is stored as `__empty__N`, and that stored name is
what Arrow and polars output should show, so the two agree and the name a user sees is one they can
write back.

Why: measured on pyarrow 20 / polars 1.35, a pandas frame with a `""` column label reads back as

| output | today |
|--------|-------|
| pandas | `''` |
| pyarrow | `''` |
| polars | `column_1` — polars renames empty fields positionally |

so `""` is presented differently by the two Arrow outputs, and neither can be written back once this
PR rejects empty names. `rename_columns_arrow_compat` then closes the loop: it renames to the Arrow
name, so a user who wants a writable name gets `__empty__N` as a real column label.

**In this PR:** nothing beyond the rejection, which is already implemented. Replace the two TODOs
with the decision:

- `_normalization.py`, `ArrowTableNormalizer.normalize`, at the `raise NormalizationException`:
  ArcticDB neither accepts nor emits an empty Arrow column name — a pandas column labelled `""` is
  stored, and presented to Arrow, as `__empty__N`. sparrow aborts the process on an empty name, and
  polars renames it positionally, so it cannot round-trip.
- `test_arrow_writes.py`, `test_write_empty_column_name_fails`: same, in one line.

**Needs coordinating with Alex**, and a separate PR from #3418, because changing what the pandas →
Arrow denormalization emits is a breaking change for Arrow and polars readers of existing pandas
symbols. It belongs on `major-7-0-0`. Two further points for that conversation:

- #3418 emits `""` as the Arrow name of an unnamed Series' value column ("the polars convention").
  Same problem, same fix; if it emits the stored name instead — `"0"`, from `Series.to_frame()` —
  it also matches what this PR stores for a one-dimensional Arrow structure, so the two reconcile on
  append and concat.
- #3418's `assert_norm_meta_arrow_compatible` asserts `not len(common.col_names)`, but the pandas
  normalizer writes a `col_names` entry for every column, `{"col": {original_name: "col"}}` included.
  That assertion is unreachable by renaming alone.

## 2. Align incoming Arrow names with a pandas multi-index

**Decided.** When Arrow data is combined with a pandas symbol whose multi-index levels are stored
`__idx__<name>`, the incoming data's column names are aligned to the stored ones. Narrow: the
`__idx__` prefix and nothing else. Every other naming difference stays a mismatch, and
`rename_columns_arrow_compat` is the way out of those, so appending a named Arrow table to an unnamed
pandas index keeps raising.

Why it is a correctness fix, not tidiness: static reads map segment columns to the output frame
positionally (`StaticColumnMappingIterator`, names only reach error messages), but dynamic reads map
by name (`frame.column_index(field_name)`). Unaligned, a dynamic-schema Arrow append onto a pandas
multi-index symbol silently drops the appended level column.

One naming rule, applied wherever an incoming schema meets an existing one:

| Operation | Where |
|-----------|-------|
| append, update | `combine_existing_tsd_with_frame` — mutate the `InputFrame` before `combine_schema` |
| merge_update | its `combine_schema` call in `clause_merge_update.cpp` |
| finalize staged data | `read_incompletes_to_pipeline`, so the staged schema does not raise — and probably a second change where the segment itself is read, so its descriptor is renamed too |

Staging cannot do this at write time: `write_parallel_impl` never reads the existing symbol, only
`verify_symbol_key`, and writes each segment with `frame->desc()`. Finalize rewrites the data
(`do_compact` writes fresh `TABLE_DATA` keys), so renaming the incomplete's descriptor in memory is
enough.

**In this PR, as its own commit** so it reviews separately from the metadata work. It clears the two
remaining xfails. Also:

- extend the interop harness with stage + finalize as a fourth operation — the one path where the
  alignment cannot happen at write time, so a mistake would hide there. Two unknowns to resolve:
  whether `stage` accepts `index_column` for Arrow input, and whether unindexed staging is allowed,
  since `UNINDEXED_OPS` wants it;
- update §4.4 of the plan, whose conclusion is that this case raises.

## 3. Duplicate Arrow column names

**Decided.** Reject them, in `ArrowTableNormalizer.normalize` beside the empty-name check, so every
Arrow entry point — write, append, update, stage — is covered.

Today they write and read back for pyarrow, which allows duplicates, but polars raises
`DuplicateError` on read, and combining such a symbol with anything is unsafe because the column
metadata and the combine machinery are keyed by name. Arrow input is opt-in behind
`_set_allow_arrow_input`, so nothing released depends on the current behaviour.

**In this PR**, with the other name validation.

## 4. Convert the normalization metadata to Arrow form on the read path

**Decided.** For Arrow output, C++ converts the normalization metadata to Arrow form before the Arrow
output is generated, so Arrow data is always described as Arrow by the time Python sees it. Not by
exposing the conversion to Python: that would save a dispatch but not the timezone flag, because the
flag is about what C++ already did to the values, not about the metadata's shape.

`create_python_read_result` (`pipeline/pipeline_utils.hpp`) is the place. It already patches the norm
metadata in place — the pre-2020 RangeIndex fix — and then hands the same proto to both
`segment_to_arrow_data` and Python, so converting there, after that patch, gives:

- one path for applying timezones, in C++, for pandas-written and Arrow-written data alike;
- an Arrow denormalizer that only ever sees `experimental_arrow`, so the input-type dispatch,
  `apply_pandas_timezones` and the whole `timezones` plumbing through `apply_pyarrow_operations` go,
  and the `pa.Table` dispatch in `CompositeNormalizer.denormalize` collapses to "not msgpack";
- multi-index level timezones in Arrow output, which removes the limitation in §2 of the plan: the
  descriptor is available at that point, so levels beyond the first can be mirrored by position,
  which the combine-time conversion cannot do.

Nothing user-visible consumes that metadata, so the shape change is invisible: `VersionedItem` carries
only user metadata, and `get_info` reads the stored descriptor by another path.

### The pandas direction is not symmetrical

Converting Arrow metadata to pandas form for pandas output does **not** work the same way, because
non-index column timezones have nowhere to live in pandas metadata and C++ cannot apply them anyway —
it hands over raw numpy arrays, and tz-awareness is a pandas dtype. Converting would therefore lose
what Python needs and regress `test_write_arrow_column_timezone_read_pandas`. So pandas output keeps
a small Arrow branch: lift or synthesize the pandas view, and localize the columns from
`ExperimentalArrow.columns`.

Making it symmetrical means adding per-column timezones to the `Pandas` message, which would also
close the standing gap where a tz-aware non-index column loses its timezone on a pandas write
(`test_non_index_column_timezone_not_preserved`, xfailed). That is its own feature — "let pandas
metadata express column timezones" — with its own decision about whether the write path populates it.
Not part of this work.

### Placement

New file `entity/arrow_pandas_norm.{hpp,cpp}`, including both `normalization_utils.hpp` and
`stream_descriptor.hpp`. It cannot go in `normalization_utils.hpp`: the dependency today runs
`stream_descriptor.hpp` → `normalization_utils.hpp` → `descriptors.hpp`, because
`OutputSchema::is_inferred_from_empty_pandas` calls `is_pandas_input_type`, so a function there taking
a descriptor would invert it. It would compile behind a forward declaration — the pre-rebase version
of that file did exactly that — but #3406 deliberately slimmed those out. `entity/` rather than
`arrow/` because the conversion is protobuf plus descriptor, with no sparrow dependency.

The metadata-only accessors (`embedded_pandas_common`, `mutable_embedded_pandas_common`,
`is_pandas_input_type`) stay in `normalization_utils.hpp`. The conversion takes
`(const NormalizationMetadata&, const StreamDescriptor&)` rather than an `OutputSchema`, because the
read-path caller has a `TimeseriesDescriptor` and would otherwise have to build one to call it.

### Two call sites, not one

`_modify_schema` (the `python_bindings.cpp` binding behind `_collect_schema`) is a second path that
produces Arrow data and hands a norm to Python, and it does not go through
`create_python_read_result`. Both need the conversion, or the two disagree — and
`lazy_df._collect_schema() == lazy_df.collect().data.schema` is the invariant that catches it.

### Risk

This moves timezone application for every existing pandas symbol read as Arrow from pyarrow to
sparrow. The value semantics are identical — both are pure relabelling of UTC instants — but the
failure surface is not: `_tz_error_context` exists only to turn pyarrow's missing-Windows-tzdata
error into a useful message, and sparrow's `date::locate_zone` needs its own equivalent. It is the
most behaviour-sensitive commit of the set, and it gates the others.

Not a risk, checked: fixed-offset timezones. `date::locate_zone` would throw on `"+05:30"`, but such
a timezone never reaches the stored metadata — `get_timezone` returns a `pytz.FixedOffset` object
rather than a string for those, so the protobuf string field rejects it on write.

## Commit plan

The prototype in the working tree gets split into:

1. One-dimensional Arrow naming, Series output in both directions, Arrow column name validation, and
   the `required_fields_info` change they depend on. No protobuf change; clears 4 xfails.
2. `descriptors.proto`, the conversion utilities, and the read-path conversion (§4), with the Python
   simplification that follows and the Arrow → pandas timezone work absorbed into it.
3. Schema combining: the embedded merge, the accessors, the compatibility checks.
4. `__idx__` alignment (§2) with stage + finalize test coverage; clears the last 2 xfails.
