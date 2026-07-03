# Branch Work Log: aseaton/column-stats/11649170310/col-col

## 2026-04-17: Sparse column stats for absent-column pruning

- Made the merged column stats segment sparse (`Sparsity::PERMITTED`) so absent columns use the validity bitmap instead of being filled with default values (0 for integers).
- Added `Sparsity`-only constructors to `SegmentInMemoryImpl` and `SegmentInMemory`.
- Added `bool column_absent` flag to `ColumnStatsValues` struct. Set to `true` during `ColumnStatsData` construction when both min and max are `std::nullopt` (sparse bitmap marks them absent).
- Updated all four comparator entry points (`stats_comparator` value overload, `stats_comparator` column-column overload, `stats_membership_comparator`, `unary_boolean_stats`) to return `NONE_MATCH` when `column_absent` is true.
- Added C++ unit test `ColumnStatsData.SparseColumnAbsentMarkedCorrectly` verifying that a two-column stats segment with sparse volume column correctly sets `column_absent = true`.
- Corrected Python test expectations for `gt` (3 -> 2) and `gte` (4 -> 3) in `test_column_stats_dynamic_schema_cross_column`. The original values overcounted because seg4 (a=[10.5,21], b=[21,200]) is already pruned by stats for `gt` (21.0 > 21.0 is false) and was not accounted for.
- All 15 C++ ColumnStats tests pass, all 189 Python column stats optimisation tests pass, lint clean.
