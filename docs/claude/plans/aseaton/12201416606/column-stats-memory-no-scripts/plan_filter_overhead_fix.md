# Plan: fix read pool starvation in admission handler via read-completion gating

Status: **implementation plan, ready to start.**

## Goal

**Remove the ~0.47 s wall-time regression** in the filter path (from 0.67 s base to 1.16 s at HEAD) by
refactoring the admission handler's read refill loop to be clocked by *read completion* (as
`folly::window` does) instead of processing completion, while preserving per-unit memory admission.

The investigation plan established that `folly::window` refills immediately on read completion with a
bounded window (2·io_threads), keeping the decode pool well-fed; the current admission handler fires reads
and refills on processing completion, under-feeding the IO pool by ~2× per read even while the pool is
nominally well-fed (150+ reads in flight). Decoupling read refill from processing refill recovers the base
wall time.

Success: measured wall time on 100M empty filter returns to ~0.67–0.71 s (current ~1.16 s), in-flight
read count remains bounded to ~2·io_threads, and memory admission (per-processing-unit throttle at
K parameter) still works.

## Architecture

**Current state** (admission handler after refactor d53a481):

```
fire(unit_i):
  reader_() returns Future<SegmentData> via std::function + promise per segment
  then().setTry(promises_[i])  // segment i complete

collect_and_process():
  read_future_i.getFuture() → wait/decode
  process(unit_i)
  on_unit_complete():
    .via(cpu_executor)
    fire(next waiting unit)  // refill clocked by processing completion
```

The problem: decode pool blocks until `process(unit_i)` completes; by then, only 1 new read has been
fired to replace the one that completed. Wall time scales with num_segments + per-unit process latency,
not just num_segments.

**Desired state**:

Decouple read submission from processing completion, so that as soon as a read completes, the next
waiting unit's read is fired (bounded by a read-window count), regardless of whether its processing
is underway. This matches `folly::window(2·io_threads)` behavior: fires next as soon as one finishes,
not waiting for downstream work.

```
admit_initial():
  for i in [0..initial_batch_size]:
    fire(i)  // bounded by read_window_

read_completion_callback():  // fires immediately after reader_() resolves, before process()
  if (num_inflight_reads > read_window_):
    decrement in-flight and wait for next fire
  else:
    fire(next waiting unit)  // eager refill

collect_and_process():
  read_future_i.getFuture() → wait/decode
  process(unit_i)
  on_unit_complete():
    .via(cpu_executor)
    admit(unit_i, &memory_state)  // admission only, no refill
```

## Measurements

Base from investigation: `folly::window(2·io_threads)` at ~110 in-flight achieves ~713 ms per-segment
read+decode (mean of 6 runs), vs admission `fire` at matched in-flight (~110) at ~140 ms. With 100
segments × 30 ms per-segment overhead, that is ~3 s of avoidable delay. For filters, typical segment
count is 1–10, so the per-read overhead compounds faster.

Goal: reduce per-read dispatch overhead from ~30–50 µs to ~10–15 µs, by eliminating the
processing-completion indirection.

## Implementation

### Phase 1: Read-window tracking (localized changes to ProcessingUnitAdmissionHandler)

1. **Add read-window state**:
   - `read_window_size_` (atomic<int>): target max in-flight reads (default `2·io_threads`, ~128 on test
     box). Tunable for experiment.
   - `num_inflight_reads_` (atomic<int>): current count.
   - `read_queue_` (std::queue<ProcessingUnitBatch> or index range): units waiting for read dispatch,
     synchronized by a mutex (OR: dispense with queue and fire sequentially from idx counter, depends on
     refactoring).

2. **Refactor `fire()`**:
   - Move `.ensure(on_unit_complete)` callback *after* the reader resolves, not chained on the processing
     future. Use `.thenTryInline` on the reader result to invoke a new method:
     ```cpp
     reader_().thenTryInline([this, i](Try<SegmentData>&& t) {
       this->on_read_complete(i, std::move(t));
     });
     ```
   - Store `t` in an array per unit (alongside the promise), not in the processing future.
   - `on_read_complete(i, t)`:
     - `thenValue(t)` → `promises_[i].setTry(std::move(t))`; downstream processing consumes `promises_[i].getFuture()`.
     - **Immediately refill**: `num_inflight_reads_--`; if `read_queue_` has waiting units, pop and fire.

3. **Refactor `admit_initial()`**:
   - Fire reads in a loop bounded by `read_window_size_`, e.g.:
     ```cpp
     int to_fire = std::min((int)units_to_fire.size(), read_window_size_);
     for (int i = 0; i < to_fire; ++i) {
       fire(units_to_fire[i]);
       num_inflight_reads_++;
     }
     for (int i = to_fire; i < units_to_fire.size(); ++i) {
       read_queue_.push(units_to_fire[i]);  // or idx tracking
     }
     ```

4. **`on_unit_complete()`** (called at end of processing, before next refill):
   - Stays as-is: per-unit admission check (memory bound via K), call `admit()`.
   - **Remove the `fire()` call** — read refill is now decoupled and happens in `on_read_complete`.
   - Still increments counters for memory state, but does not dispatch reads.

### Phase 2: Validation & measurement

1. **Correctness**: 
   - All units fire exactly once, in order.
   - `on_read_complete` → `on_unit_complete` ordering is preserved (read completes, then processing).
   - Memory bound K still enforced: `num_units_in_flight_processing` or similar counter managed in
     `on_unit_complete`.

2. **Measurement**:
   - Run the filter benchmark (100M empty filter, K=192, IO=64/CPU=96) and confirm wall is ~0.67–0.71 s.
   - Confirm in-flight read count hovers near `read_window_size_` (~128) and doesn't spike.
   - Confirm K still throttles at low K (e.g., K=32 should still show slower wall as processing backs up).

3. **Regression check**:
   - Run full unit + integration tests (Python and C++).
   - Confirm no memory overhead; peak RSS on the filter should be identical to base.

### Phase 3: Cleanup

1. Remove `LogAdmissionTiming` and `AdmissionUseWindow` diagnostic flags from the investigation phase.
2. Update `ProcessingUnitAdmissionHandler` docstrings to document the new refill model (read-complete
   gated).
3. Optionally: add a config flag for `read_window_size_` if it should be tunable at runtime.

## Risks and mitigations

- **Ordering**: if `on_read_complete` fires before `collect_and_process` consumes the promise, the
  promise setter may be called before the future is constructed. This should be safe (folly promises are
  thread-safe and expect this), but needs testing. Mitigation: add an assertion in the test to verify
  the promise is ready before processing requests it.
- **Memory spike**: firing `read_window_size_` reads at once (vs processing-gated) could increase peak
  working set for very large segments. This is mitigated by the tunable `read_window_size_` and the per-unit
  memory admission (K still throttles processing). Measurement will confirm.
- **CPU executor contention**: if the read-completion callback and `on_unit_complete` both run on
  `cpu_executor` and interleave, they could contend. Mitigation: measure cpu_executor queue depth during
  the benchmark; if needed, use a separate executor for read-completion refill.

## Testing strategy

1. **Unit**: add a test that fires N reads, measures in-flight read count, and confirms it stays ≤
   `read_window_size_` while processing lags.
2. **Integration**: the existing filter benchmark harness; add assertions on wall time and in-flight
   count.
3. **Stress**: run with very small `read_window_size_` (e.g., 2) and very large segments (to delay
   processing) and confirm no deadlock or stall.

## Success criteria

- [ ] Filter wall time on 100M empty filter: **≤0.75 s** (baseline 0.67–0.71 s; allow 10% margin).
- [ ] In-flight read count: ~128 ± 20 (read_window_size_).
- [ ] Memory admission (K) still works: low K values (32, 64) show slowdown proportional to processing backlog.
- [ ] All tests pass; no regressions on other read paths or workloads.
- [ ] Peak RSS: identical to base on the same test.

## Files to touch

- `cpp/arcticdb/processing/processing_unit_admission_handler.hpp/cpp`: refactor as Phase 1 + Phase 3.
- `cpp/arcticdb/processing/processing_unit_admission_handler_test.cpp`: add Phase 2 unit test.
- (Optional) `python/measure_processing_memory.py`: add in-flight count assertion to the benchmark.

## Open questions

- Should read-completion callback run on `cpu_executor` (current `.via`), or on the IO executor for
  tighter latency? Experiment if Phase 2 shows contention.
- Is there a simpler way to track in-flight reads without a queue (e.g., just an atomic counter and
  sequential firing)? Explore after the first prototype.
- Should `read_window_size_` be derived from io_threads, or a separate config? Default to
  `2·io_thread_count` for now; make tunable if benchmarks suggest variation.
