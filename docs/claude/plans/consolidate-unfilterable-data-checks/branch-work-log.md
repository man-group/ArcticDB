# Branch work log: consolidate-unfilterable-data-checks

## Unprocessable-data checks

- Added `check_can_perform_processing` in `cpp/arcticdb/version/version_core.cpp` and consolidated all
  unprocessable-data checks into `read_indexed_keys_to_pipeline`, called from two sites there: the
  multi-key early return, and the normal path immediately after `pipeline_context->set_tsd(...)` (the
  checks need the tsd to inspect `has_normalization()` / `is_pickled()` / `is_numpy_array()`).
- Every processing entry point reaches the check through `read_indexed_keys_to_pipeline`:
  `setup_pipeline_context` (reads, lazy reads, batch reads, concat, `SetupPipelineContextTask`),
  `create_column_stats_impl`, `compact_data_explain_plan_impl`, `async_compact_data_impl`,
  `merge_update_impl`, `read_modify_write_internal`, `read_and_process`.
- `CompactDataClause` is the one exempt clause, so compaction of pickled data and numpy arrays keeps
  working.

## Test layout

- Tests were briefly distributed into the existing per-feature test files (commit `79ae42334`) and have
  now been consolidated back into
  `python/tests/unit/arcticdb/version_store/test_unprocessable_data.py`, covering read (query builder,
  columns, date range, row range, head, tail, lazy), batch read `DataError` codes, concat with
  per-symbol processing, read-modify-write, merge, column stats, and the compaction exemption.
- The distributed copies and the shared `python/tests/util/unprocessable_data.py` helper were removed;
  the helpers now live in the consolidated test file.
- Pre-existing per-feature tests that overlap (e.g. `test_head_pickled_symbol`,
  `test_row_range_pickled_symbol`, `test_filter_unfilterable_data`, `test_partial_read_pickled_df`,
  `TestWithNormalizers`) were deliberately kept: each carries a dimension the consolidated file does not
  (dynamic schema, output formats, encoding versions, recursive metastructure versions, 3-D arrays).
  Several now assert the specific `ErrorCode` rather than just `SchemaException`.
- The compaction exemption is guarded only by `test_compact_pickled_data` and `test_compact_numpy_arrays`
  in `test_compact_data.py` (stronger than a plain "still allowed" smoke test), with a pointer comment
  from the consolidated file.
