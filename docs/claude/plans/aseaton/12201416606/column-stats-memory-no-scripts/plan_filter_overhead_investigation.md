# Plan: explain and remove the filter wall-time penalty from always-on admission

Status: **investigation plan, not started.**

## Goal

**Measure and explain** where the extra ~0.47 s goes. The empty-output filter over 100M rows regressed
from **0.69 s (base `cf370d6bf`, eager read path) to 1.16 s (HEAD, admission)** — ~1.7×, confirmed against
the branch base, peak RSS unchanged. This document is about producing a precise, per-phase attribution of
that cost — a proper picture of what is actually happening — **not** about fixing it yet.

Explicitly out of scope / not the intended direction:
- **No special-case fast path / bypass** (do not skip the machinery when the bound is non-binding). The
  intended eventual outcome is to make the admission machinery itself fast enough to leave on every read.
- No fixes at all until we have measured and understood the breakdown.

Success criterion for this investigation: a clear, reproducible breakdown of the extra time attributed to
specific parts of the admission read path (which phase, which mechanism, per-segment vs fixed), so a fix
can be designed from evidence rather than guesswork.

## What we already know

- The regression is **not the throttle**: it is present at `K=0` (kill switch, all units admitted up
  front) — ~1.28 s — and across `K∈{0,100,default}` (all ~1.3–1.55 s). Only `K∈{1,10}` add the expected
  serialization cost on top.
- It hits **processing reads only** (those with clauses, which go through `read_process_and_collect` →
  `read_and_schedule_processing` → `schedule_clause_processing`). Plain clause-free `read()` uses the
  direct `fetch_data` / `batch_read_compressed` path and never touched admission, so it is unaffected.
- For `filter`, **base and HEAD share the entire downstream** (`schedule_clause_processing`, the
  ComponentManager, entities, `MemSegmentProcessingTask`). The *only* difference is the read mechanism:
  - base: `generate_segment_and_slice_futures` → `store->batch_read_uncompressed` → `folly::window(2·io)`.
  - HEAD: `ProcessingUnitAdmissionHandler` — a `folly::Promise` per segment, `make_uncompressed_reader`
    std::function, `fire()`/`admit()`, `on_unit_complete` chained via `.ensure`.

So the overhead is localized to the admission read mechanism vs `folly::window`, feeding identical
downstream work.

## Hypotheses (ranked)

- **H1 — extra per-segment future hop.** Each read is wrapped in its own promise: `reader_()` future →
  `thenTryInline` → `setTry(promises_[i])`; downstream consumes `promises_[i].getFuture()`. That is an
  extra promise allocation + continuation per segment vs the eager path consuming the window future
  directly. Prime suspect if the delta scales with segment count.
- **H2 — burst-fire vs windowed scheduling.** `folly::window(2·io)` keeps a steady, bounded set of reads
  in flight with continuous refill; admission `admit_initial` fires many units' reads in a loop with no
  in-flight cap (at `K≥num_units`), flooding the IO queue. Could hurt cache locality / scheduler
  efficiency even when memory is fine.
- **H3 — std::function indirection.** `make_uncompressed_reader` returns a `std::function`, and (post
  refactor) the schema-check wrapper adds a second type-erased layer; each `fire` is an indirect call.
- **H4 — `RangesAndKey` copy per fire.** `fire()` does `reader_(RangesAndKey{ranges_and_keys_->at(i)})`
  — a copy (incl. an `AtomKey` with a `StreamId` string) per segment; the eager path moves.
- **H5 — `on_unit_complete` churn.** Per unit: `.ensure` callback + atomic `fetch_add` + `admit`. Runs
  `num_units` times even when admits are no-ops.
- **H6 — extra executor hops** (`.via`/`thenValueInline`/`thenTryInline`) adding context switches.

The K-independence points away from throttling (H6 partial) and toward the per-segment machinery
(H1/H3/H4) and/or scheduling shape (H2).

## Instrumentation and tools

- **ArcticDB sampling** (`ARCTICDB_SAMPLE`/`ARCTICDB_SUBSAMPLE`, Remotery) — capture a trace of a single
  100M filter on base vs HEAD and diff the named scopes (`FetchSlices`, decode, `GetEntities`, etc.).
  Requires a build with sampling enabled; check `cpp/CMakePresets.json` for a profiling preset.
- **`perf record -g` + flame graph** on a RelWithDebInfo/profiling build (the release preset likely lacks
  frame pointers/symbols) for base vs HEAD; look for time in folly future/promise machinery, allocation,
  and `std::function` dispatch.
- **Targeted timing logs** (`ARCTICDB_RUNTIME_DEBUG` with timestamps) at phase boundaries:
  ranges/keys built → handler constructed → `admit_initial` start → first future resolved → all decoded →
  `schedule_clause_processing` returns → total. Add equivalent markers on the base eager path for a phase
  A/B. Add these under a debug flag; remove or gate before landing.
- `cpp_async.print_scheduler_stats()` for pool utilisation differences.

## Experiments

1. **Scaling**: filter at 1M / 10M / 100M on base vs HEAD; is the delta ∝ segment count (→ per-segment,
   H1/H3/H4) or roughly fixed (→ setup)? Reuse the existing LMDB datasets and the repeated-run harness.
2. **Isolate the read mechanism**: in a scratch build, make HEAD's `read_and_schedule_processing` route
   the unbounded case through `batch_read_uncompressed` (bypassing the admission promises) and re-measure
   filter — if it returns to ~0.69 s, the admission read mechanism is confirmed as the cause and points
   straight at the fix.
3. **Micro-profile**: perf/flame-graph diff of the two.
4. **Phase timing**: compare the logged phase durations base vs HEAD to see whether the extra time is in
   read scheduling, decode dispatch, or the collect/processing hand-off.

Experiment 2 is an *attribution* measurement (confirm the read mechanism is where the cost lives), not a
proposed fix.

## Deliverable

A written breakdown answering: is the extra time per-segment or fixed? which phase (read scheduling /
decode dispatch / collect hand-off) carries it? which mechanism (promise hop, `std::function`,
`RangesAndKey` copy, scheduling shape) dominates, with profile/log evidence? That breakdown is the end of
this investigation. Fixes are a separate exercise, designed from the evidence, with the constraint above
(no special-case bypass — speed up the machinery itself).

## Findings (measured)

Measured on the box (128 cores, 527 GB), release = RelWithDebInfo, refactored HEAD, 100M-row zeros,
IO=64/CPU=96, empty filter. `OPENBLAS_NUM_THREADS=1` unless noted.

**1. It is not CPU cost of the handler.** Flat `perf` (aggregated by symbol) of the HEAD filter:
`LZ4_decompress_safe` 64%, kernel page-fault/zeroing (`clear_page_rep`, `__handle_mm_fault`,
`__pte_offset_map`, `rmqueue_bulk`) ~15% (faulting the ~80 GB of decompressed zeros into RAM), kernel
lock contention ~2.5%, the filter predicate 0.8%, and the **folly promise/future/executor/futex
machinery ≈ 0.1%**. Decode + page-faulting are identical in the base, so the machinery is not where the
time goes.

**2. OpenBLAS was a red herring for CPU%.** numpy's OpenBLAS spawns ~128 threads that idle-spin on
`sched_yield`; that dominated the first profile and ~half the 16 s system time. Pinning
`OPENBLAS_NUM_THREADS=1` halved system time but **did not** change the filter wall — the regression is
independent of it.

**3. The penalty is lost read-pipeline parallelism (off-CPU/scheduling).** Same decode work
(~30 CPU-s), very different effective parallelism (CPU-s ÷ wall):

| build | filter wall | CPU-s | effective cores |
|-------|-------------|-------|-----------------|
| base `cf370d6bf` (eager `folly::window(2·io)`) | 0.64–0.71 s (steady) | ~29–34 | **43–51 (~48)** |
| HEAD (admission) | 0.96–1.43 s (erratic) | ~23–30 | **16–31 (~24)** |

The eager path keeps ~48 cores saturated and steady; the admission path keeps only ~24 busy and
erratically — roughly half the parallelism, which accounts for the ~1.7× wall. (`perf sched` off-CPU
tracing was unavailable — no `/sys/kernel/tracing` access — so effective-cores from `getrusage` deltas
was used instead; the instrumentation is in `measure_processing_memory.py`.)

**Conclusion.** `folly::window` continuously self-clocks a bounded set of reads and keeps the IO/decode
pool full; the admission handler's fire/`on_unit_complete` refill keeps far fewer reads in flight, so the
decode pool is under-fed and wall time roughly doubles — with no extra CPU. The next question (not yet
measured) is *which* part of the admission refill chain starves the pool (per-unit granularity, the
`.via(cpu_executor)` hop before `on_unit_complete`, or too few units admitted before refill). That is the
lever for making the machinery itself fast — targeted timing logs around fire → decode → collect →
`on_unit_complete` → next fire would localize it.

## Findings (round 2 — refill chain instrumented, and folly::window diagnostic)

Added flag-gated timing to the admission handler (`VersionStore.LogAdmissionTiming`): per-segment fire→decode
latency, in-flight-reads low-water/avg, per-unit collect→process latency, `on_unit_complete` count; logged
as a summary. Ran 100M filter, IO=64/CPU=96, `OPENBLAS_NUM_THREADS=1`.

**Both earlier hypotheses were wrong (measured):**
- **Not refill starvation.** At default K=192 the pool is *over-fed*: ~150–165 reads in flight vs 64 IO
  threads; per-unit processing is ~2–4 ms; refill keeps up (1000 unit_completes). The pipeline is never
  short of reads.
- **Not K / in-flight count.** Sweeping K: only very low K (32 → 28 in-flight) starves and is slow (~3 s);
  from K=64 to 256 wall is ~1.2–1.5 s and *no* K recovers the base 0.67 s. So the penalty is independent of
  the in-flight bound.

**It is the read-submission mechanism (diagnostic confirms).** Added a diagnostic path
(`VersionStore.AdmissionUseWindow`, scratch) that dispatches the reads via `folly::window(2·io)` with the
same reader and downstream — mimicking the pre-admission eager path — instead of per-unit `fire`. 6 runs each:

| dispatch | wall mean (range) | read+decode avg |
|----------|-------------------|-----------------|
| `fire` (admission, K=192, ~150 in-flight) | ~1157 ms (947–1313) | ~152 ms |
| `folly::window` (128, ~110 in-flight) | ~713 ms (626–864) | ~73 ms |
| base `cf370d6bf` (reference) | ~670 ms | — |

`folly::window` recovers base wall and halves per-segment read+decode. At *matched* in-flight (~110:
window vs fire-at-K=128), window is 72 ms vs fire 140 ms — so it is the dispatch mechanism, not the
concurrency level.

**Conclusion.** `folly::window` refills clocked on *read completion* (immediate, bounded to 2·io); the
admission handler fires reads and refills clocked on *processing completion*
(`collect → .via(cpu_executor) → process → .ensure → on_unit_complete → fire`). That processing-gated
refill drives the IO pool ~2× less efficiently *per read* even while the pool is well-fed. The fix (later,
per the constraint: no special-case bypass) is to make the handler feed/refill the read pool the way
`folly::window` does — clocked on read completion with a bounded in-flight window — while still admitting at
processing-unit granularity for the memory bound.

Instrumentation status: `LogAdmissionTiming` timing logs are kept; the `AdmissionUseWindow` path and
`diagnostic_window_size_` are scratch-only and should be reverted before shipping.

## Notes

- This is independent of `plan_admission_refactor.md` (that is a structural cleanup; it does not change the
  per-segment machinery, so it will neither cause nor fix this penalty). Sequence the refactor first, then
  do this on top so measurements are against the cleaned-up code.
