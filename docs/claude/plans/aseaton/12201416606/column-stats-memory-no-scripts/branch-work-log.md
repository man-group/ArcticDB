# Branch work log: make processing-unit admission the default read path

## Goal

Promote the `ProcessingUnitAdmissionHandler` introduced for column stats (PR #3191) to the default
read path. Previously admission was opt-in via a `ProcessingUnitAdmissionRequest` toggle and only
the column-stats path used it; every other read took the eager `batch_read_uncompressed` /
`folly::window` path. Now every read goes through admission, and the
`VersionStore.NumProcessingUnitsLive` config applies everywhere (same computed default, so default
behaviour is preserved).

## What was done

- Renamed `version/test/test_column_stats.cpp` -> `version/test/test_column_stats_memory.cpp`
  (updated `cpp/arcticdb/CMakeLists.txt`).
- `read_and_schedule_processing` always builds the `ProcessingUnitAdmissionHandler` with
  `K = num_processing_units_live(processing_unit_indexes)`. Removed the `else` eager branch and the
  `std::optional<ProcessingUnitAdmissionRequest>` parameter from it and from
  `read_process_and_collect`. Dropped the `ProcessingUnitAdmissionRequest{}` argument at the
  `create_column_stats_impl` call site.
- Deleted `ProcessingUnitAdmissionRequest` and `generate_segment_and_slice_futures` (now dead).
- Removed the eager `batch_read_uncompressed` from the `StreamSource` interface, `AsyncStore`, and
  the `InMemoryStore` test mock. `make_uncompressed_reader` is now the sole reader seam.
- Updated two stale comments in `pipeline/frame_slice.hpp` that named `batch_read_uncompressed`.

## Behavioural change

`VersionStore.NumProcessingUnitsLive` now throttles all reads, not just column stats. The default
`K = max(1, ceil(2 * io_thread_count / max_unit_size))` keeps ~2*io_thread_count segments live,
matching the old `folly::window(2*io_thread_count)` bound, so default memory/concurrency is
unchanged.

## Tests

- C++ `test_column_stats_memory.cpp` (controller unit tests + residency integration tests).
- Python `test_column_stats_creation.py`.

## Follow-up: residency tracking moved onto the segment

The residency tracker previously incremented at decode (`DecodeSliceTask`) but decremented at the
ComponentManager refcount-zero release, which also fires for processing-materialized outputs
(`push_entities`). That asymmetry let `live_` drift negative; it only gave the right high-water for
column stats by timing coincidence (output releases land after the input-residency peak).

Replaced with RAII on the segment itself:
- `SegmentInMemoryImpl` gains a `from_disk_` flag (wrapped in a small self-clearing `OnDiskFlag` so
  the defaulted move can't double-count) and `mark_from_disk()`. The destructor decrements when set
  and the tracker is enabled. `clone()` and clause outputs stay unmarked.
- `decode_into_slice` calls `segment_in_memory.mark_from_disk()` instead of poking the tracker.
- The increment (decode) and decrement (segment dtor) now cover the identical decoded-from-disk
  population, so the counter is symmetric and measures true decoded-segment lifetime.
- A short-lived `OnDiskSegment` entity-tag approach was tried first and reverted; the segment-level
  flag is leak-proof and tracks actual memory lifetime rather than CM residency.

## Follow-up: `K` default gains a CPU floor

`num_processing_units_live` previously used only `ceil(2*io_thread_count / max_unit_size)`, which
collapses toward 1 for wide column-sliced data and starves the CPU pool (the pre-admission path had
no unit cap, so the CPU pool ran all ready units). Default is now
`max(2*cpu_thread_count, ceil(2*io_thread_count / max_unit_size))`. The IO term reproduces the old
`folly::window` read budget; the `2*cpu` floor (mirroring the IO `2x` slack) keeps a decoded reserve
so a CPU worker finishing a unit doesn't stall on the next unit's read. The config override still
takes precedence. Exposed the function in `version_core.hpp` and added `NumProcessingUnitsLive`
unit tests (wide-unit floor, narrow-unit IO dominance, config override).

## Follow-up: kill switch

`VersionStore.NumProcessingUnitsLive = 0` now returns the total number of processing units, admitting
them all at once and disabling the memory bound (the computed default is always >= 2, so 0 is an
unambiguous sentinel). Added `NumProcessingUnitsLive.ZeroAdmitsAllUnits` (K = num units) and
`ColumnStatsStoreTest.KillSwitchStillProducesCorrectStats` (all-admitted path still correct).

## Follow-up: instrumented why K=1 peak RSS >> one unit (filter)

Investigated the `results.md` observation that `K=1` peaks grow with rows/pool size despite admitting
one unit. Added an experiment-only live-decoded-segment gauge (bytes/count with high-water) hooked via
a custom `shared_ptr<SegmentInMemory>` deleter in `add_slice_to_component_manager`, exposed to Python,
plus a VmRSS sampler / `malloc_trim` / smaps split in `measure_processing_memory.py`. Reproducer
`python/run_gauge_matrix.sh`; full analysis in `k1_memory_cause_analysis.md`.

- `K=1` bounds the **live decoded set to exactly one unit (77.8 MiB)** at IO=8/16/64 — decode does
  not run ahead. The "~60 units" read off `ru_maxrss` was an artifact.
- The excess is retention: (1) glibc malloc-arena high-water of transient decompress buffers, which
  scales with IO thread count (`malloc_trim` reclaims 454/847/3286 MiB for IO=8/16/64;
  `MALLOC_ARENA_MAX=1` cuts the IO=64 peak 4435->1143 MiB), and (2) the LMDB read-only mmap of the
  scanned data (~915 MiB file-backed, thread-independent, not malloc-reclaimable). Only ~80 MiB anon
  at the floor, so ArcticDB's allocator holds no large pool.
- Gauge is experiment-only (hot-path allocation); revert/guard before shipping. Overlaps the existing
  `SegmentInMemoryImpl::mark_from_disk` residency tracker and corroborates it.

## Fix: decouple read submission from unit admission (two throttles)

Fixed the filter read-path throughput regression (analysis in `python/read_memory_analysis.md`, plan
in `python/read_memory_fix_plan.md`). The admission handler clocked read submission on processing
completion, so at the kill switch it flooded the IO queue and at bounded K refilled in bursts —
either way losing `folly::window`'s smooth, self-clocking feed.

- Split the single `NumProcessingUnitsLive` knob into two independent throttles:
  - **Read window** `VersionStore.SegmentReadWindow` (throughput), default `2*io_thread_count`,
    matching the old `folly::window(2*io)`. New `segment_read_window()` in `version_core.cpp`.
  - **Residency budget** `VersionStore.NumProcessingUnitsLive` (memory), repurposed to residency
    only; default io term doubled to `ceil(4*io / max_unit_size)` so it sits ~2x the read window and
    stays non-binding on normal reads. Renamed `num_processing_units_live` ->
    `max_resident_processing_units`. `0` = kill switch = unbounded residency.
- Reworked `ProcessingUnitAdmissionHandler`: admission now enqueues a unit's segments (unit-contiguous)
  onto an eligible queue; `fill_read_window()` launches eligible reads up to the window and refills on
  read completion (mutex-guarded `active_reads_`/`eligible_`, `enqueued_` replaces the per-segment
  atomic). Read submission is `folly::window`-equivalent when residency is not binding; the kill
  switch now recovers base throughput through the single code path (no bypass).
- Tests: renamed `NumProcessingUnitsLive` suite -> `MaxResidentProcessingUnits` (io term doubled),
  added `SegmentReadWindow` tests and `ProcessingUnitAdmissionHandlerTest.ReadWindowLimitsInFlightReads`;
  updated the `test_parallel_processing.cpp` and `test_column_stats_memory.cpp` handler constructions
  for the new arg. All 13 affected C++ tests pass.
- Verified with `python/read_filter.py` (10M x 100 cols, S3, IO=96/CPU=64), results in
  `python/results_filter_fix.md`: old K=0 flood 2.8-4.0s (erratic) -> new kill switch ~2.1s (steady);
  computed default ~2.1s (non-binding); read window alone bounds resident reads (W=16 -> ~1330 MiB);
  residency alone throttles for memory (E=8 -> ~6s).

Follow-up recorded in the plan: bound residency by bytes rather than unit count.

## PURE comparative benchmark: master vs fixes

Ran the memory benchmark against PURE (S3), 100M-row x 100-col all-zeros symbol, master vs the fixes
build, under three allocators (glibc default, MALLOC_ARENA_MAX=4, tcmalloc). Harness changes (all in
untracked `python/` scratch): `--symbol` override in `write_data.py`/`measure_processing_memory.py`, a
PURE branch in `bench_memory.sh`, `run_pure_passes.sh` (allocator loop), and `make_comparison.py`.
Fixed `run_column_stats` to handle master's `create_column_stats_experimental(symbol, as_of)` (no
explicit spec) vs the branch's `create_column_stats(symbol, column_stats, as_of)` — the old name-only
fallback passed the spec dict as `as_of` and errored. Wrote 100M to PURE as `f64_100m_100c_zeros`.

Results in `python/results_pure_comparison.md` (+ per-pass `results_pure_{master,fixes}_{default,arena4,tcmalloc}.{md,csv}`).
Headlines (peak MiB, IO=64, glibc): the residency bound cuts column_stats 33745->4396 at K=8 (7.7x),
resample 67305->19555 (3.4x), groupby 114506->80876 (1.4x, least — needs most input resident). filter
is unchanged by K (tiny live set) and shows no regression (master 4782 vs K=default 4769, wall 1.96 vs
2.17). Allocator is a large independent lever; best config is the bound + tcmalloc (column_stats K=8
tcmalloc 1641 vs 33745 master glibc, 20x). K=0 reproduces master. Corroborates `memory_use_findings.md`.

Build note: master baseline was gathered by checking out master in-place (the fix is committed at
`dac5ce8c1`), building, running, then restoring the branch and rebuilding.

## Rebase onto origin/master, squashed to one commit

Squashed all 8 branch commits into a single commit (`git reset --soft` to the merge-base, one
commit, then `git rebase origin/master`). Branch was 14 commits behind; the only conflict was in
`python/tests/unit/arcticdb/test_column_stats_creation.py` against master's
`Rename APIs for column stats to _experimental and simplify their signatures` (#3141).

- `create_column_stats`/`get_column_stats_info`/`read_column_stats`/`drop_column_stats` renamed to
  `*_experimental` on master, and `create_column_stats_experimental` **no longer takes an explicit
  column-stats dict** — it always computes MINMAX over every eligible column, so
  `NativeVersionStore.create_column_stats_version` (C++ layer, unchanged) is now invoked with an
  auto-derived spec rather than a user-supplied one.
- Adapted the three new admission-ceiling tests
  (`test_column_stats_create_independent_of_admission_ceiling`,
  `test_column_stats_create_admission_tiny_thread_pool`, `test_column_stats_create_single_unit`) to
  call `create_column_stats_experimental(sym)` with no dict argument and read back via
  `get_column_stats_info_experimental`/`read_column_stats_experimental`. All three symbols only ever
  had numeric data columns, so the auto-selected columns are unchanged from the explicit dicts they
  replaced.
- No C++ changes were needed for the rename (Python-only).
- History was already pushed as `origin/aseaton/12201416606/column-stats-memory-no-scripts`; a
  force-push is required to publish the rebase (not done — awaiting confirmation).

## Split the admission handler out of `version_core.hpp`/`.cpp`

- Renamed `version/test/test_column_stats_memory.cpp` -> `version/test/test_admission_handler.cpp`
  (updated `CMakeLists.txt`, kept alphabetical ordering).
- Moved `ProcessingUnitAdmissionHandler`, the `SegmentReader` alias, and `max_resident_processing_units`
  out of `version_core.hpp`/`.cpp` into new `version/admission_handler.hpp` (class stays header-only,
  as-is) and `version/admission_handler.cpp` (holds `max_resident_processing_units`, which needs
  `TaskScheduler`/`ConfigsMap`). `segment_read_window` was left in `version_core.cpp` — only the two
  symbols named were moved.
- `version_core.hpp` now includes `admission_handler.hpp`, so existing consumers that only relied on
  the transitive include still compile unchanged. Added a direct `#include
  <arcticdb/version/admission_handler.hpp>` to `test_admission_handler.cpp` and
  `processing/test/test_parallel_processing.cpp` for clarity, since both use the handler directly.
- Registered `version/admission_handler.hpp` and `version/admission_handler.cpp` in `CMakeLists.txt`.

## Post-rebase/split verification

- `make build-debug CMAKE_JOBS=32`: clean build, no changes needed to fix compilation.
- C++: `make test-cpp-debug` filtered to the admission/column-stats suites — 13/13 passed
  (`ProcessingUnitAdmissionHandlerTest`, `MaxResidentProcessingUnits`, `SegmentReadWindow`,
  `ColumnStatsStoreTest`, `ColumnStatsMixedColSlicing`).
- Python: `test_column_stats_creation.py` in full — 136/136 passed.
