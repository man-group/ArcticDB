# Filter read-path regression analysis (IO=64 CPU=96 K=0)

## Context

Branch `aseaton/12201416606/column-stats-memory-no-scripts` introduces a
`ProcessingUnitAdmissionHandler` that bounds the number of processing units in flight during
clause-based reads (filter, resample, groupby, column_stats). The investigation here is
restricted to the `filter` operation with IO=64/CPU=96 and K=0 (the kill switch: all units
admitted, memory bound disabled).

Benchmark data:
- `results_pure_baseline.md` — timings from master
- `results_pure.md` — timings from this branch

Relevant row from each (100M-row all-zeros symbol, IO=64 CPU=96, K=0):

| build | peak MiB | wall s |
|-------|----------|--------|
| master (`batch_read_uncompressed` + `folly::window`) | 4813.2 | 1.65 |
| branch (`ProcessingUnitAdmissionHandler`, K=0) | 4707.7 | 2.95 |

~1.8× wall-time regression. Memory is essentially unchanged. This is a throughput regression,
not a memory one.

The filter used is `q["f_0"] == -1.0` which matches no rows, so peak RSS reflects input-decode
residency only and the output frame size is zero. The entire regression is in the read/decode
pipeline, not in output construction.

The existing investigation is in
`docs/claude/plans/aseaton/12201416606/column-stats-memory-no-scripts/plan_filter_overhead_investigation.md`.
This document records the conclusion reached after reviewing that investigation together with the
code.

## Why the plan's round-2 conclusion is wrong

The investigation concludes the cause is "processing-gated refill" — that the admission handler's
refill is clocked on processing completion rather than read completion. That explanation is
internally contradicted by the investigation's own data:

- **Matched in-flight experiment** (K=128 fire vs `folly::window(128)`): same ~110 reads in flight,
  yet fire = 140 ms vs window = 72 ms per-segment. The refill mechanism does not differ between
  those two at matched in-flight count — only the dispatch mechanism does. The overhead is
  per-segment and present regardless of K.
- The investigation itself says "no K recovers the base" for K=64–256. At K ≥ num_processing_units,
  all units are admitted upfront; `on_unit_complete` fires but does nothing further. No refill
  starvation is possible, yet the penalty is the same.

The "processing-gated refill" explanation accounts for K=1 and K=10 (where throttling adds
serialisation on top), but does not explain K=0 or the core ~1.7× overhead that persists across
all high-K values.

## The real cause

**Old path** (`batch_read_uncompressed`, master):

```cpp
return folly::window(
    std::move(ranges_and_keys),
    [this, ...](RangesAndKey&& rk) { return read_and_continue(key, ...); },
    2 * io_thread_count  // = 128 at IO=64
);
```

`folly::window` returns the direct futures from `read_and_continue`. When item N completes
(inline, in the IO thread that processed it), window immediately submits item N+128 in that same
callback — no thread hops, no intermediaries. At steady state exactly 128 reads are in flight; the
pool is never under- or over-fed.

**New path** (`fire_segment_read` at K=0):

```cpp
// admit_initial_processing_units fires all ~1010 reads in a tight loop:
reader_(pipelines::RangesAndKey{ranges_and_keys_->at(i)})
    .thenTryInline([self, i](Try<SegmentAndSlice>&& t) {
        self->promises_->at(i).setTry(std::move(t));
    });
// downstream consumes promise.getFuture(), not the read future directly
```

All 1010 `submit_io_task` calls are issued at once before any read completes. The IO thread pool
queue receives 1010 tasks immediately, giving each of the 64 IO threads a backlog of ~16 items.
There is no self-clocking; submission rate is decoupled from completion rate.

### Mechanism 1 — queue flood (primary)

Average queue depth ≈ 1010/64 ≈ 16 tasks per thread vs ≈ 2 for `window(128)`. Each segment waits
proportionally longer before starting. `folly::collect` for a unit waits for its slowest segment,
and that last segment is delayed by the full queue depth. This directly explains why per-segment
latency is 152 ms (fire) vs 73 ms (window) in the round-2 experiment even at matched in-flight
count: the fire case's tasks are buried deeper in the queue when they are submitted.

### Mechanism 2 — promise indirection (secondary)

The old path chains continuations directly onto the `read_and_continue` future. The new path adds
`thenTryInline` + `setTry` + downstream-future awakening per segment. The perf profile found this
is ~0.1% of CPU so it does not dominate, but it is consistent with overhead being present even at
matched in-flight count.

### K=0 vs K=100/default

K=0 (2.95 s) is slightly worse than K=100/default (2.61–2.69 s) because K=100/default creates a
bounded refill loop (admit 100 or 192 units, then +1 per completion) that partially restores
`folly::window`-style bounded queueing. K=0 fires all 1010 reads at once.

## Summary

The ~1.8× regression is caused by replacing `folly::window`'s tight, self-clocking submission
pipeline with a burst submission of all reads into the IO thread pool. `folly::window` keeps
exactly 2·io_threads futures in flight and refills inline on each completion; the admission handler
dumps all reads into the queue at once. Per-segment queue wait time roughly doubles, `folly::collect`
on each unit is bounded by the slowest segment, and wall time doubles correspondingly.

The fix is not a special-case bypass. It is making the read submission inside the admission handler
behave like `folly::window`: clocked on read completion with a bounded in-flight window over
segments, while still tracking processing-unit completion for the memory bound. The round-2
diagnostic already confirmed this recovers base performance.
