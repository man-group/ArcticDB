# Branch work log: column-stats write-time memory bound

## Goal

Bound read/decode memory when creating column stats. `create_column_stats_impl` read the whole
symbol eagerly via `folly::window`, which bounds in-flight reads but not decoded segments awaiting
processing, so residency could grow to the whole dataset. Add processing-unit admission: keep at
most `K` units in flight, admit the next when an outstanding one finishes processing.

## What was done

- **`ProcessingUnitAdmissionHandler` controller** (`version_core.hpp`): holds an injected `SegmentReader` closure,
  per-index promises, a `launched` dedup, and the unit groups. `admit_initial(K)` fires the first
  `K` units' reads; `on_unit_complete()` admits one more per completion. `fire` captures
  `shared_from_this` to outlive the async reads. Defined in the header so it is unit-testable.
- **Reader seam**: new non-pure `StreamSource::make_uncompressed_reader` (default throws,
  `InMemoryStore` untouched), overridden in `AsyncStore` where `library_` lives. Keeps the
  controller storage-agnostic.
- **Plumbing** (column-stats-narrow): a defaulted `std::optional<ProcessingUnitAdmissionRequest>` threads
  through `read_process_and_collect` -> `read_and_schedule_processing`, and a defaulted
  `std::shared_ptr<ProcessingUnitAdmissionHandler>` through `schedule_clause_processing` ->
  `schedule_first_iteration` (which wraps each per-unit future in `.ensure(on_unit_complete)`).
  Default off, so every other read path is unchanged. Only `create_column_stats_impl` opts in.
- **Config**: `VersionStore.NumProcessingUnitsLive`; default
  `K = max(1, ceil(2 * io_thread_count / max_unit_size))`, clamped to a positive floor.
- **Test instrumentation**: header-only `SegmentResidencyTracker` (atomic live/high-water, enabled
  flag). Increment at decode completion (`DecodeSliceTask::decode_into_slice`), decrement at
  fetch-count-zero in `ComponentManager`.

## Tests

- C++ `test_column_stats.cpp`:
  - `ProcessingUnitAdmissionHandlerTest.*` — deterministic proof of the bound: `admit_initial` fires
    exactly `K` units; each completion fires exactly one more; reads fired once (dedup).
  - `ColumnStatsStoreTest.ResidencyBoundedByAdmission` and `ColumnStatsMixedColSlicing.*` —
    integration smoke asserting residency stays within `K * max_unit_size` end-to-end. The
    MixedColSlicing tests use static schema and change `column_group_size` between the initial write
    and the append (both orders) to produce uneven units, self-validating the geometry via the data
    segment count. NB: a residency high-water count cannot distinguish working from broken admission
    in a fast in-memory test (processing keeps pace regardless of K), so the controller test is the
    real guard.
- Python `test_column_stats_creation.py`: stats independent of `K` (1/2/1000), tiny-thread-pool
  deadlock guard (K=1/2), single-unit edge.

## Status

All C++ tests pass. Python tests pending a current debug `arcticdb_ext` build (the symlinked module
was stale w.r.t. an unrelated S3 logger change).

## Notes / non-obvious

- The residency counter must increment at **decode** time, not at ComponentManager insertion. CM
  residency stays small regardless of admission because units are processed and freed promptly; the
  memory admission actually bounds is decoded segments parked in futures before CM insertion.
