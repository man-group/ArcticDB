# Branch work log — arrow-pandas-interop

## 2026-09-16 — Plan for Arrow/pandas interop

- Baseline: `test_arrow_pandas_interop.py` 110 passed / 48 xfailed.
- Probed the behaviour the plan depends on: 1-D Arrow input is stored as `__array__` while an unnamed
  pandas Series is stored as `"0"`; pandas multi-index levels ≥ 1 are stored as `__idx__<name>`;
  Arrow writes already record per-column timezones and string formats in `ExperimentalArrow.columns`;
  `DataFrame` + `ChunkedArray` concat does not raise at all today.
- Wrote `plan.md` (RFC option A) and, after review, settled: the embedded oneof mirrors the top-level
  one (`df` / `series` / `ts`), timezones are kept in step in both places, no demangling of
  multi-index level names, invalid Arrow column names rejected outright.
- Spun the groupby/resample metadata clobbering out into
  `groupby-resample-arrow-norm-bug.md`; no monday ticket exists for it (searched Tasks, Subitems,
  Roadmap).

## 2026-09-16 — Implementation

Rebased onto the rebased `incompletes-v4` (the branch had no commits of its own; `normalization_utils`
now lives in `entity/`). Backup ref: `arrow-pandas-interop-prereset`.

`test_arrow_pandas_interop.py` is now **158 passed / 2 xfailed**, and two tests were added.

What landed, piece by piece:

- **A** — `_pandas_norm_meta_from_arrow_norm_meta` reads the index timezone from
  `ExperimentalArrow.columns`, and `_localize_arrow_timestamp_columns` applies non-index column
  timezones to the denormalized frame, which pandas metadata cannot express.
- **B** — `to_pyarrow_table` stores a 1-D structure under its own name, or `"0"` when unnamed; empty
  Arrow column names are rejected in `ArrowTableNormalizer.normalize`; `one_dimensional` (and a
  pandas Series with no index column) drives Series output in both directions.
- **C** — the `pandas_input_type` oneof in `ExperimentalArrow`; `accumulate_arrow_and_pandas_norm`
  converts the pandas side and delegates to `accumulate_arrow_and_arrow_norm`, which now merges or
  inherits the embedded pandas metadata; `pandas_common` widened to reach the embedded message with
  `is_pandas_input_type` for the one site that must stay narrow; Python read paths dispatch on the
  embedded arm.

Divergences from the plan, all recorded in `plan.md`:

- `required_fields_info`'s `has_series_value_column` had to land with piece B: once a 1-D structure is
  stored under its own name, Arrow-only concatenation of differently-named 1-D structures depends on
  name reconciliation. It is `one_dimensional && !has_index` — a 1-D timestamp array written with
  `index_column=True` has an index field and no value column.
- The descriptors are threaded into the norm fold after all. Arrow records a `ColumnMeta` entry for
  every timestamp column, naive ones included, so an absent entry means the schema lacks the column;
  a converted pandas schema has to say the same or a naive pandas column inherits the Arrow side's
  timezone. Enumerating those columns needs the descriptor. First attempt — clearing the timezone
  whenever only one side recorded one — would have regressed dynamic-schema appends, where an absent
  entry really does mean an absent column (`test_append_and_update_changing_timezones_dynamic`).
- `has_index` on the converted metadata is inherited from the Arrow side rather than derived from
  `is_physically_stored`, which is also true for a non-datetime index stored as a row-count column.

Test expectations changed, each matching an existing sibling test:

- `test_combine_series_with_polars_series` used an unnamed pandas Series with a named polars Series
  and expected append to succeed. Mismatched names must raise, as they do for two polars Series
  (`test_combine_polars_named_series`), so it now uses matching names, and two new tests cover the
  mismatch: append raises, concat reconciles to unnamed.
- `test_combine_synthetic_columns`: `has_synthetic_columns` is preserved (the stored metadata shows
  it, and the pandas read gives integer `RangeIndex` columns), but `to_pandas()` on Arrow output
  gives string labels — the limitation `test_write_pandas_synthetic_columns_read_arrow` already
  documents. Asserts both reads now.
- `test_write_empty_column_name_fails` expects `ArcticException`: `_try_normalize` re-raises every
  normalizer error as `ArcticNativeException`.

Still xfailed: `test_append_non_timeseries_multiindex_pandas_with_unindexed_arrow` (2).

## 2026-09-18 — Moved to `idilov.hn.res.ahl`

Transferred the uncommitted work by patch (`HEAD` was already on `origin/incompletes-v4`, so no
history needed copying). Setting the host up turned up several things worth recording, now in
`~/source/ArcticDB/CLAUDE_USER_SETTINGS.md`:

- Submodules were shared-but-unchecked-out in this worktree; `git submodule update --init --recursive`
  fixed it without refetching.
- `withproxy` **breaks** pip here and plain pypi is blocked. The working index is
  `https://repo.prod.m/artifactory/api/pypi/pegasus-311-1/simple/`, written into
  `~/pyenvs/claude/pip.conf`. `pegasus create` needs the pegasus profile in `~/.bashrc`, so the venv
  was built from an existing 3.11 interpreter instead.
- The old host's `linux-debug-py311-no-container` / `linux-profile` presets were user-local;
  `Makefile.local` now uses the stock `linux-debug` / `linux-release`.
- The vcpkg build needs `pkg-config`, `flex`, `bison` (thrift is a base dependency of the `arrow`
  port), installed by hand.

## 2026-09-17 — Review round

- `combine_norm_metadata` now describes each pandas schema in Arrow terms when any schema is Arrow, so
  the fold only combines like with like: `accumulate_arrow_and_pandas_norm` is gone, the descriptors
  are out of `accumulate_norm_metadata`'s signature, and `has_index` comes from the index descriptor
  inside `arrow_norm_from_pandas` instead of being assigned by the caller. Added
  `test_concat_three_symbols_one_arrow`, which exercises the fold with the Arrow schema in each
  position.
- `get_info` describes a combined symbol as Arrow, so the `_process_info` refactor is reverted.
- Comments and docstrings trimmed; `apply_timezones` renamed `apply_pandas_timezones` and set once.
- Read PR #3418: `rename_columns_arrow_compat` rewrites a symbol by reading it as pandas and writing
  it back through the pandas normalizer, so multi-index levels beyond the first get the `__idx__`
  prefix again. It therefore does **not** unblock the two remaining xfails.
- Measured on pyarrow 20 / polars 1.35: pyarrow accepts empty and duplicate field names, `pl.from_arrow`
  renames an empty field to `column_0`, and polars rejects duplicates outright. So an empty Arrow name
  cannot round-trip for a polars user however we store it.

## 2026-09-23 — Back on `dlonapatcs502`, commit 1 verified

Picked up the bundle built on the headnode. The tree now holds **commit 1 only** — 1-D Arrow naming,
Series output both ways, Arrow column name validation (empty *and* duplicate), and the
`required_fields_info` change they need — plus the uncompiled `entity/arrow_pandas_norm.{hpp,cpp}`
draft for commit 2, which is not yet in `cpp/arcticdb/CMakeLists.txt`.

Verified here after a debug rebuild: interop **120 passed / 40 xfailed**, matching the headnode, and
the 14-file regression set **7337 passed, 0 failed**. The headnode's single failure
(`test_arrow_dynamic_schema_missing_columns_hypothesis`, `allow_nan`) was its pinned hypothesis 4.39.3
and does not reproduce here, as expected. The 2 xpassed are the pre-existing stale xfails in
`test_api.py::test_column_names_by_timestamp`.

The material for commits 2-4 is in `full-with-validation.patch`, inside
`/users/is/idilov/arrow-pandas-interop-bundle.tar.gz` (extracted at `~/.tmp/bundle-in/`). The draft's
`mirror_index_timezones` improves on the in-combine version: with the descriptor to hand it mirrors
multi-index levels beyond the first by position, which removes the limitation noted in §2 of the plan.

## 2026-09-23 — Split into commits

Five commits, verified each in turn. The interop suite finishes at **180 passed, none xfailed**, from
110 passed / 48 xfailed.

- `Plan the Arrow/pandas interop work…` — the docs in this directory.
- `Name one-dimensional Arrow data as pandas names a Series` — as brought back from the headnode.
- `Describe pandas data in Arrow terms before generating Arrow output` — the conversion in
  `entity/arrow_pandas_norm`, called from the read path, and the Python simplification that follows.
- `Combine Arrow data with pandas data` — the embedded merge, adapted to call the conversion rather
  than keeping its own copy.
- `Align incoming Arrow index levels with a pandas multi-index`.

Three things the work turned up:

- **Recursive normalization is a third conversion call site.** Making the Arrow denormalizer *require*
  Arrow metadata found it, along with the fact that the conversion has to carry the `custom` field
  across, or `ArcticDbNotYetImplemented` stops being raised for custom-normalized data.
- **The accessors had to land with the conversion**, not with the combining work: once the metadata is
  converted, every consumer of `required_fields_info` must understand the embedded message, or an
  unnamed multi-index loses a level under column selection — which `test_collect_schema_multiindex`
  caught.
- **Aligning only the staged schema is worse than refusing.** The first attempt aligned the incomplete's
  schema at finalize; the combine then succeeded, the already-written segments still carried their own
  names, and the dynamic read aborted in sparrow on a column with no buffer. Refused instead, with a
  test, until each segment's descriptor can be renamed as compaction reads it.

`TimeFrame` combined with Arrow is now tested, which is the only coverage of the embedded `ts` arm.
