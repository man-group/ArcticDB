# Plan: decouple read submission from unit admission on the clause read path

## Problem

`ProcessingUnitAdmissionHandler` bounds memory by admitting processing units and clocking read
submission on those admissions: `admit_initial_processing_units` fires a unit's reads immediately
(`fire_segment_read` → `reader_(...)` → `read_and_continue` → `submit_io_task`), and
`on_processing_unit_complete` admits the next unit when one finishes processing. Read submission is
therefore clocked by *processing* completion.

The old path (`batch_read_uncompressed`) submitted reads through `folly::window(2·io)`, which clocks
submission on *read* completion: when a read finishes, the same IO thread inline-submits the next,
keeping a smooth, bounded set of reads in flight.

Read feeding and the memory bound are two different resources that the handler has fused into one
knob. The result is a read-path throughput regression on clause reads (filter/resample/groupby/
column_stats):

| build | peak MiB | wall s |
|-------|----------|--------|
| master (`folly::window`) | 4813.2 | 1.65 |
| branch, kill switch (K=0) | 4707.7 | 2.95 |

Memory is unchanged; this is throughput only. See `read_memory_analysis.md` for the mechanism.

Two regimes:

- **Memory loose** (kill switch, and the default): reads should flow at `folly::window` speed. Today
  they do not — at the kill switch all reads are fired at once (queue flood); at bounded-but-loose K
  refill is processing-clocked and bursty. This is the regime that regressed.
- **Memory tight**: reads are necessarily paced by processing completion, because you cannot read
  further ahead without exceeding the residency you are bounding. That serialization is the memory
  bound working as intended, and no scheduling change removes it.

The fix targets the loose regime and leaves the tight regime's inherent pacing intact.

## Design: two decoupled throttles

Two caps on in-flight reads, each refilled by its own clock:

1. **Read window `W`** (throughput). At most `W` reads submitted-but-not-decoded. A slot frees on
   **read completion**, inline on the IO thread. `W` defaults to `2·io_threads`, matching the old
   `folly::window`.
2. **Residency budget `E`** (memory), in processing units. At most `E` units admitted-but-not-yet-
   processed. A slot frees on **processing completion** (`on_processing_unit_complete`). This bounds
   decoded segments resident in memory.

A segment is read only when it can take a slot from **both**: it must belong to an admitted
(eligible) unit *and* the read window must have a free slot. The slower-refilling cap governs the
steady state:

- **`E ≥ W` (loose):** `W` binds, refill is read-clocked → reduces exactly to `folly::window(2·io)` →
  base throughput. The kill switch (`E` unbounded) and the default (`E` a generous multiple of `W`)
  both land here, so the regression is fixed out-of-the-box.
- **`E < W` (tight):** `E` binds, refill is processing-clocked → reads paced by processing. This is
  the intended memory throttle; the design neither adds nor removes cost here.

The handler stops calling `submit_io_task` directly on admission. Admission adds a unit's segments to
an *eligible* set; `fill_read_window()` launches eligible segments into the IO pool, holding at most
`W` reads in flight and refilling on each read completion.

### Eligible set and window filling

- Admitting a unit (initially, or on processing completion) pushes its not-yet-launched segment
  indices onto an eligible queue, preserving unit contiguity so `folly::collect` on a unit is not
  waiting on segments submitted far apart.
- `fill_read_window()`: while `active_reads < W` and the eligible queue is non-empty, take the next
  segment and launch its read (`active_reads++`).
- Read-completion continuation: set the segment's promise (as today), then `active_reads--` and call
  `fill_read_window()`.
- The `launched_` per-segment CAS is kept: a segment shared by two units (resample boundaries) is
  launched at most once.

The window-fill state (`active_reads`, eligible queue) is guarded by a small `std::mutex`. Segment
indices to launch are collected under the lock and `submit_io_task` is called after releasing it.

### Correctness and liveness notes

- **Memory still bounded.** Residency (decoded-not-processed segments) ≤ segments of admitted-not-
  completed units ≤ `E` units. Eligible-but-unread segments hold no decoded memory. The window makes
  peak residency no worse and often tighter.
- **Deadlock safety.** The pump refills from the read-completion continuation via `submit_io_task`, a
  non-blocking enqueue — exactly what `folly::window` does internally. This does not violate the
  "don't block on the same pool from within a task" rule. The memory bound is enforced by *not
  submitting* (the eligibility gate), never by blocking inside a decode task.
- **`W` smaller than a unit.** If a unit has more than `W` segments, its reads trickle through as
  earlier reads complete; the pump always makes progress for `W ≥ 1`, so no deadlock.

## Config knob split

Today one knob, `VersionStore.NumProcessingUnitsLive`, sets a value whose default
(`max(2·cpu, ceil(2·io / max_unit_size))`) conflates IO read-ahead with CPU-worker feeding. Split
into:

| Concern | Knob | Default | Kill switch |
|---------|------|---------|-------------|
| Throughput — read window `W` (segments) | `VersionStore.SegmentReadWindow` (new) | `2·io_threads` | n/a (`≥ 1`) |
| Memory — residency budget `E` (units) | `VersionStore.NumProcessingUnitsLive` (repurposed) | `max(2·cpu, ceil(4·io / max_unit_size))` | `0` → unbounded (`= size`) |

- `NumProcessingUnitsLive` is repurposed to mean *residency budget only*. Its io-derived term doubles
  (`2·io` → `4·io`) so the default `E` is ≈ 2× the read window: non-binding for normal reads while
  still capping pathological residency. The CPU floor (`2·cpu`) is retained.
- Kill switch semantics improve: `NumProcessingUnitsLive = 0` now means *residency unbounded* → pure
  read window → `folly::window` speed. This alone turns the 2.95 s kill-switch number back toward
  base.
- These are heuristics. A principled byte-based residency default is a follow-up (below).

## Implementation steps

1. **Split `num_processing_units_live`** (`version_core.cpp:1162`) into:
   - `max_resident_processing_units(processing_unit_indexes)` — residency budget `E`, reading
     `NumProcessingUnitsLive`, default `max(2·cpu, ceil(4·io / max_unit_size))`, `0` → `size()`.
   - `segment_read_window()` — read window `W`, reading `SegmentReadWindow`, default `2·io_threads`,
     clamped `≥ 1`. Independent of unit structure.
   Update the header declaration (`version_core.hpp:198`) and the comment block (`version_core.cpp:1158`).

2. **Rework `ProcessingUnitAdmissionHandler`** (`version_core.hpp:119`):
   - Constructor takes `E` (residency, as now via `k`) and `W` (read window, new arg).
   - Add window-fill state: `std::mutex`, `size_t active_reads_`, an eligible-segment queue.
   - `admit_processing_unit` pushes the unit's not-yet-launched segments onto the eligible queue
     (under lock) then calls `fill_read_window()`, instead of calling `fire_segment_read` directly.
   - `fire_segment_read` keeps the `launched_` CAS and the promise wiring, but is now called only from
     `fill_read_window()`, and its continuation calls `on_read_complete()` (`active_reads_--`,
     `fill_read_window()`) after `setTry`.
   - `fill_read_window()`: under lock, while `active_reads_ < W` and eligible non-empty, pop and
     collect indices (incrementing `active_reads_`); release lock; `fire_segment_read` each collected
     index.
   - `on_processing_unit_complete` unchanged in its residency role (admit next unit), which now feeds
     the pump.

3. **Wire the two values** at the construction site (`version_core.cpp:1212`, `1231`): pass
   `max_resident_processing_units(...)` and `segment_read_window()` into the handler.

## Tests (TDD — write failing first)

- `cpp/arcticdb/version/test/test_column_stats_memory.cpp`:
  - Update `NumProcessingUnitsLive` unit tests for the new residency default (io term doubled):
    `IoReadAheadDominatesForNarrowUnits` and `CpuFloorAppliesForWideUnits` expected values change;
    `ConfigOverrideTakesPrecedence` and `ZeroAdmitsAllUnits` still hold.
  - New tests for `segment_read_window()`: default `2·io`, config override, floor at 1.
  - Behavioural test on the handler: with `E` unbounded and `W` set small, assert peak concurrent
    in-flight reads never exceeds `W`; with `E` small, assert peak admitted-not-completed units never
    exceeds `E`. Drive it with a stub `SegmentReader` that records concurrency and completes on demand.
  - Existing memory-bound assertions must still pass (residency ≤ `E`).

## Verification

Run `python/read_filter.py` (100M-row zeros, empty filter, IO=96/CPU=64) on this branch before and
after the change, and record wall/peak in a results file alongside `results_pure.md`.

Expected:
- Kill switch (`NumProcessingUnitsLive = 0`): ~2.95 s → ~base (≈ 0.67 s here / 1.65 s on the master
  table), peak unchanged.
- Default (knob unset): at or near base, since default `E` (≈ 2× `W`) is non-binding.
- A tight setting (e.g. `NumProcessingUnitsLive` well below `W`): slower than base by roughly the
  memory throttle, peak reduced — confirming the bound still bites when asked to.

## Follow-up: bound residency by bytes

Units/segments are a loose proxy for resident bytes when segment sizes vary. A byte-budgeted
residency gate — acquire an estimated decoded size (from the descriptor) before a segment becomes
eligible, release on processing completion — bounds memory precisely and lets the default be
expressed as a memory target rather than a unit count. Deferred; it also wants read and decode split
so compressed prefetch does not count against resident bytes.
