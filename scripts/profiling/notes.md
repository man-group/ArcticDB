Baseline from profile_zero_match.py: 20 runs, median 1.05ms, max=1.51ms

Showed ColumnStatsData ctor was main contributor. Also a bit from compute_stats

Created benchmark_column_stats.cpp to focus on these, baseline:

--------------------------------------------------------------------------------------------------------------------
Benchmark                                                          Time             CPU   Iterations UserCounters...
--------------------------------------------------------------------------------------------------------------------
BM_ColumnStatsData/rows:100/data_cols:100/iterations:20         2.09 ms         2.08 ms           20 data_cols=100 rows=100
BM_ColumnStatsData/rows:100/data_cols:10000/iterations:20        311 ms          311 ms           20 data_cols=10k rows=100
BM_ColumnStatsData/rows:10000/data_cols:100/iterations:20        235 ms          235 ms           20 data_cols=100 rows=10k

-----------------------------------------------------------------------------------------------------
Benchmark                                           Time             CPU   Iterations UserCounters...
-----------------------------------------------------------------------------------------------------
BM_ComputeStats/rows:100/data_cols:100           2.57 us         2.57 us       321508 data_cols=100 rows=100
BM_ComputeStats/rows:100/data_cols:10000         2.00 us         1.99 us       347732 data_cols=10k rows=100
BM_ComputeStats/rows:10000/data_cols:100          234 us          234 us         2957 data_cols=100 rows=10k
BM_ComputeStats/rows:10000/data_cols:10000        249 us          248 us         2833 data_cols=10k rows=10k

Note: `perf stat` command:

```
perf stat -e task-clock,cycles,instructions,branches,branch-misses,cache-references,cache-misses,L1-dcache-loads,L1-dcache-load-misses,LLC-loads,LLC-load-misses \
        /users/is/aseaton/source/ArcticDB-worktree/tree-two/cpp/out/linux-release-py310-build/arcticdb/benchmarks \
        --benchmark_filter='BM_ColumnStatsData/rows:10000/data_cols:100' \
        --benchmark_repetitions=5 --benchmark_report_aggregates_only=true 2>&1
```

Baseline from profile_zero_match.py: 20 runs, median 1.05ms, max=1.51ms
First change: columnar iteration in the ColumnStatsData ctor and flat vector

lib.read zero-match (rows=10000000, iters=20, column_stats=on):
  total block      = 20.1 ms
  mean             = 1.00 ms
  median           = 0.97 ms
  min              = 0.95 ms
  max              = 1.42 ms

Process finished with exit code 0

Second change: columnar iteration, no flat vector

(tree-two) aseaton@dlonapatcs502:~/source/ArcticDB-worktree/tree-two/python DEV $ /users/is/aseaton/venvs/tree-two-venv/bin/python ../scripts/profiling/profile_zero_match.py
warmup x2
timed x20

lib.read zero-match (rows=10000000, iters=20, column_stats=on):
  total block      = 20.7 ms
  mean             = 1.03 ms
  median           = 1.00 ms
  min              = 0.98 ms
  max              = 1.47 ms


Baseline:

20260430 16:39:05.438453 1132017 I arcticdb | ColumnStatsData::ColumnStatsData	0.000121
20260430 16:39:05.439843 1132017 I arcticdb | ColumnStatsData::ColumnStatsData	0.000131
20260430 16:39:05.441209 1132017 I arcticdb | ColumnStatsData::ColumnStatsData	0.000126

Columnar:

20260430 16:40:19.000285 1140825 I arcticdb | ColumnStatsData::ColumnStatsData	0.000068
20260430 16:40:19.001806 1140825 I arcticdb | ColumnStatsData::ColumnStatsData	0.000056
20260430 16:40:19.003279 1140825 I arcticdb | ColumnStatsData::ColumnStatsData	0.000068
20260430 16:40:19.004664 1140825 I arcticdb | ColumnStatsData::ColumnStatsData	0.000058

Columnar and flat vector:

20260501 09:25:29.742703 2603157 I arcticdb | ColumnStatsData::ColumnStatsData	0.000025
20260501 09:25:29.744241 2603157 I arcticdb | ColumnStatsData::ColumnStatsData	0.000026
20260501 09:25:29.745973 2603157 I arcticdb | ColumnStatsData::ColumnStatsData	0.000027

## 2026-04-30: re-profile after columnar iteration commit

Build at d1e49ff21 (Columnar iteration over stats segment). Symbol has 100 segments × 4 stats columns.

Timing reproduces: 30000-iter run, mean 1.02 ms, median 1.01 ms, min 0.85 ms.

`perf record -F 999 --call-graph dwarf` (50k samples). Top self-time, by thread:

  python (main):  6.18% _PyEval_EvalFrameDefault, 0.55% _upb_Decoder_DecodeMessage
  IOPool0:        3.20% _int_malloc, 1.44% _int_free, 1.26% malloc_consolidate, 1.01% malloc,
                  0.83% std::_Hash_bytes, 0.67% LZ4_decompress_safe, 0.61% bm::bvector::count,
                  0.51% unordered_map<string,ColumnStatsValues>::operator[],
                  0.47% unlink_chunk, 0.39% cfree, 0.37% protobuf Map<string,ColumnName> Find
  CPUPool0:       1.36% _int_malloc, 1.16% malloc_consolidate, 0.96% std::_Hash_bytes,
                  0.69% protobuf Map<string,ColumnName> FindHelper, 0.62% _int_free,
                  0.55% malloc, 0.42% protobuf Map InsertUnique,
                  0.31% bm::bvector::enumerator::decode_bit_group

Also 16.7% blas_thread_server — OpenBLAS pool spinning on import; not part of the read path.

py-spy --native captured only the main thread, where 90% is folly futex-wait while pools work,
so unhelpful here. perf with --call-graph dwarf is too slow on 30k-iter data; LBR refused under
per-thread mode on this kernel. perf report --no-callgraph is fast and sufficient — the symbol
list directly identifies the costs below.

### Bottlenecks (descending impact)

1. glibc malloc churn dominates. Across IOPool0 + CPUPool0, _int_malloc / _int_free /
   malloc_consolidate / malloc / cfree / unlink_chunk = ~10% of all samples (~30% of useful
   work-thread time). malloc_consolidate firing means many small allocations being merged on
   free, classic high-churn pattern. Per read we build:
     - 100 ColumnStatsRow objects, each owning a per-row std::unordered_map<string, ColumnStatsValues>
     - protobuf NormalizationMetadata with 100+ string→ColumnName entries
     - SegmentInMemory + 100+ Column objects for the data segment
   Most of these are reconstructed every read.

2. std::unordered_map ops (~3% combined). std::_Hash_bytes 1.8%, ColumnStatsValues map[] 0.51%,
   protobuf Map<string,ColumnName> Find/Insert 1.5%. Two specific hotspots:
   a) column_stats_filter.cpp:236 — `rows_[r].stats_for_column[col_name]` is called twice per
      (row × stats-col): MIN write then MAX write. With 100 rows × 4 cols × 2 = 800 hash ops
      per read into 100 separate small unordered_maps. col_name is constant per stats-column;
      could be replaced with direct indexed storage.
   b) protobuf string-keyed Map for column-name normalization metadata. Hot on both pools because
      every read deserializes it freshly.

3. bm::bvector ops (~1%). count() and enumerator::decode_bit_group on the row-level bitset.
   For zero-match these are walked but produce nothing — but the iteration cost is still real.

4. LZ4_decompress_safe: 0.67% on IOPool0. Index segment + column-stats segment decompression,
   re-done every read.

### Cheap wins to try next

- Replace the per-row `unordered_map<string, ColumnStatsValues>` in ColumnStatsRow with a flat
  array indexed by stats-col-index (built once from stats_at_column_index). Removes 100 small
  hashmap allocations + 800 string-hashed inserts per read. Lookup at column_stats_filter.cpp:59
  becomes `stats_rows[r]->stats[col_idx]`.

- Cache the parsed TimeseriesDescriptor / NormalizationMetadata on the version (or LRU). Currently
  rebuilt from protobuf per read; protobuf Map<string,ColumnName> shows up on both pools.

- Reuse SegmentInMemory / Column allocations across reads, or memoize the ColumnStatsData for
  repeated reads of the same symbol/version.

### Result of flat-array refactor

Replaced per-row `unordered_map<string, ColumnStatsValues>` with a flat
`vector<ColumnStatsValues>` indexed by slot. The slot map is owned by
`ColumnStatsData` and resolved once per AST column reference, not once per row.

Microbenchmark medians (best of 5 × 20 iterations, system loaded ~74):

| Benchmark                                | Master   | New      | Speedup |
|------------------------------------------|----------|----------|---------|
| BM_ColumnStatsData/rows:100/data_cols:100   | 0.97 ms  | 0.33 ms  | 2.9x    |
| BM_ColumnStatsData/rows:100/data_cols:10000 | 258  ms  | 60   ms  | 4.3x    |
| BM_ColumnStatsData/rows:10000/data_cols:100 | 190  ms  | 44   ms  | 4.3x    |
| BM_ComputeStats/rows:100/data_cols:100      | 5.83 us  | 1.33 us  | 4.4x    |
| BM_ComputeStats/rows:100/data_cols:10000    | 7.31 us  | 1.88 us  | 3.9x    |
| BM_ComputeStats/rows:10000/data_cols:100    | 617  us  | 151  us  | 4.1x    |
| BM_ComputeStats/rows:10000/data_cols:10000  | 330  us  | 390  us  | ~same   |

The earlier 234us baseline noted above for ComputeStats was a stale snapshot
from a less-loaded system; rerunning master on the same loaded host gave 617us.
After the change, `unordered_map<string, ColumnStatsValues>::operator[]` no
longer appears in perf hotlists.


### More profiling...

Wrote profile_many_columns to create a 10k x 100 column stats segment, and filter out everything.

With all changes, columnar, flat column major storage:
lib.read zero-match (rows=20000, num_cols=100, rows_per_segment=2, filter_cols=10, iters=100, column_stats=on):
  total block      = 3822.2 ms
  mean             = 38.22 ms
  median           = 36.86 ms
  min              = 34.78 ms
  max              = 50.53 ms

With columnar and flat storage:
lib.read zero-match (rows=20000, num_cols=100, rows_per_segment=2, filter_cols=10, iters=100, column_stats=on):
  total block      = 7185.8 ms
  mean             = 71.85 ms
  median           = 64.76 ms
  min              = 58.57 ms
  max              = 117.27 ms

With just the columnar change:
lib.read zero-match (rows=20000, num_cols=100, rows_per_segment=2, filter_cols=10, iters=100, column_stats=on):
  total block      = 23422.8 ms
  mean             = 234.22 ms
  median           = 211.18 ms
  min              = 185.53 ms
  max              = 425.16 ms

Without any changes:
lib.read zero-match (rows=20000, num_cols=100, rows_per_segment=2, filter_cols=10, iters=100, column_stats=on):
  total block      = 29363.7 ms
  mean             = 293.63 ms
  median           = 283.89 ms
  min              = 260.59 ms
  max              = 434.06 ms


### Flat column-major + memcpy fast path

Refactored `ColumnStatsData` to store mins/maxes as per-slot raw byte buffers in column-major
layout, with per-cell engagement bitsets. The ctor now does one `details::visit_type` per stats
column rather than per (row × stat). When the underlying `Column` is dense and single-block
(true for unscaled MIN/MAX produced by create_column_stats) it `memcpy`s straight from
`column.ptr()` into the flat buffer; multi-block dense paths copy via the typed iterator;
sparse paths fall back to per-row `scalar_at`. Materialisation into `ColumnStatsValues` happens
once per AST `ColumnName` reference inside `materialize_slot`.

profile_many_columns.py (rows=20000, num_cols=100, rows_per_segment=2, filter_cols=10):

  iters=20, column_stats=on
  mean   = 35.98 ms   (vs 71.85 ms previously — 2.0x)
  median = 35.82 ms
  min    = 34.73 ms
  max    = 38.82 ms

ScopedTimer logs per read:
  ColumnStatsData::ColumnStatsData ~8 ms (vs ~40 ms previously — 5x)
  create_column_stats_filter      ~2.3 ms

### How much does the memcpy fast path buy us?

Removed the `is_contiguous_dense` memcpy branch and re-ran (only the dense typed iterator
remains, plus the sparse `scalar_at` fallback):

  iters=20, column_stats=on (no memcpy)
  mean   = 35.72 ms
  median = 35.27 ms
  min    = 34.48 ms
  ctor   ~7.8 ms

vs with memcpy: mean 35.98 ms, ctor ~8.0 ms. Within run-to-run noise. The typed iterator
over a single-block dense column compiles down to effectively the same loop, so the memcpy
specialisation is not pulling its weight here.


 Also: make changes so that we only load the interesting subset of column stats in to memory, based on the date_range filters provided by the user


 Original branch: aseaton/stats/profiling
 Second way branch: aseaton/stats/profiling-attempt-two


 After all changes, including selective decode:

 ```
lib.read zero-match (rows=20000, num_cols=100, rows_per_segment=2, filter_cols=10, iters=20, column_stats=on):
  total block      = 210.1 ms
  mean             = 10.50 ms
  median           = 10.40 ms
  min              = 10.29 ms
  max              = 11.79 ms
```

Master:

```
(tree-two-venv) aseaton@dlonapatcs502:~/source/ArcticDB-worktree/tree-two/python DEV $ python ../scripts/profiling/profile_many_columns.py --rows 20000 --num-cols 100 --rows-per-segment 2
warmup x2
timed x20

lib.read zero-match (rows=20000, num_cols=100, rows_per_segment=2, filter_cols=10, iters=20, column_stats=on):
  total block      = 5281.3 ms
  mean             = 264.06 ms
  median           = 258.79 ms
  min              = 253.26 ms
  max              = 288.51 ms
```
