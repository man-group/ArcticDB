# fix-index-end-overflow-at-int64-max

## 2026-11-07

- Reproduced: a segment whose last index value is `INT64_MAX` stores `end_index = INT64_MAX + 1`, which is signed
  overflow (UB) in `end_index_generator` and wraps to `INT64_MIN` in practice. User-visible on the unfixed build:
  - `read(date_range=...)` and `QueryBuilder.date_range` drop the last segment (an all-`INT64_MAX` index read with
    `date_range=(MAX, MAX)` returns 0 rows; `(None, MAX - 2)` on a one-segment symbol returns 0 rows).
  - `update` near `INT64_MAX` loses rows (dynamic schema: everything but the update).
  - `append` after a segment ending at `INT64_MAX` is rejected or mis-checked.
- Fix keeps the stored bits unchanged (no format change): the write computes `last + 1` with well-defined unsigned
  wrap, and readers treat `end_index == INT64_MIN` as "ends at `INT64_MAX` inclusive" through new helpers in
  `entity/index_range.hpp` (`end_index_from_last_index_value`, `last_index_value_from_end_index`,
  `segment_index_range`).
- Readers fixed: `AtomKey::index_range`, `build_bitset_for_index` (static loop and dynamic binary search, plus the
  `range_start + 1` overflow for updates starting at `INT64_MAX`), `DateRangeClause`, column-stats date window,
  `RangesAndKey::end_time`, `get_descriptor`, append sort check, staged-data overlap checks, `strictly_before`,
  `PipelineContext::index_range`, `get_index_segment_range`.
- Tests: `python/tests/unit/arcticdb/version_store/test_index_end_at_max_timestamp.py`; C++
  `BitsetForIndex.SegmentEndingAtInt64Max` and `EndIndex.WrapsAtInt64Max` in `pipeline/test/test_query.cpp`.
- Alternative not taken (needs a format decision): saturate `end_index` at `INT64_MAX` on write.
