## Measurements

Baseline:

```
(pegasus-adb) aseaton@dlonapatcs502:~/source/ArcticDB-worktree/tree-one/python DEV $ python ./measure_column_stats_memory.py
baseline VmRSS = 169.5 MiB
create_column_stats wall = 0.97s
peak ru_maxrss        = 14496.6 MiB
peak - baseline       = 14327.1 MiB
(pegasus-adb) aseaton@dlonapatcs502:~/source/ArcticDB-worktree/tree-one/python DEV $ python ./measure_column_stats_memory.py --zeros
baseline VmRSS = 169.6 MiB
create_column_stats wall = 0.84s
peak ru_maxrss        = 7802.0 MiB
peak - baseline       = 7632.4 MiB
```

## Worked example: admission with overlapping units

`on_unit_complete` is wired in `schedule_first_iteration` (version_core.cpp). Each processing unit
gets one future:

```
unit_fut = folly::collect(its segment futures)
             .via(cpu_executor)
             .thenValueInline(... add segments to ComponentManager; run MemSegmentProcessingTask ...)
unit_fut = std::move(unit_fut).ensure([admission]{ admission->on_unit_complete(); });
```

So `on_unit_complete` fires when that unit's `MemSegmentProcessingTask` finishes — i.e. after the
unit's `ProcessingUnit` is destroyed and its input segments' fetch counts decremented (a shared
segment is freed only at fetch-count zero). `.ensure` runs on success and failure, so a throwing
unit still advances the window. Inside, `next_unit_.fetch_add(1)` hands out the next unit index
atomically and `admit(u)` fires that unit's reads. Admission only ever *submits* IO tasks; it never
blocks a thread.

### Units `{0,1}, {1,2}` (segment 1 shared), ceiling K=1, tiny thread pool (1 IO, 1 CPU)

`next_unit_` starts at K=1.

1. `admit_initial(1)` admits unit 0 → `fire(0)`, `fire(1)`, each marking `launched_` and submitting
   a read on the single IO thread. Unit 1 is not admitted, so segment 2 is unfired and unit 1's
   `collect` is parked on `promise[2]` (and a `FutureSplitter` share of `promise[1]`).
2. The IO thread decodes segments 0 and 1, fulfilling their promises. Unit 0's `collect` resolves on
   the CPU thread → both added to the ComponentManager (segment 1 with fetch_count 2, since two
   units reference it) → `MemSegmentProcessingTask` processes unit 0.
3. Unit 0's `ProcessingUnit` is destroyed: segment 0 → fetch 0 → freed; **segment 1 → fetch 1, stays
   resident**. The future resolves, so `.ensure` fires `on_unit_complete` on the CPU thread:
   `fetch_add` returns 1 → `admit(1)` → `fire(1)` is a no-op (already launched — the dedup) and
   `fire(2)` submits the one new read.
4. The IO thread decodes segment 2 → unit 1's `collect` (segment 1 already done, segment 2 now done)
   resolves → segment 2 added, unit 1 processed → segment 1 → fetch 0 freed, segment 2 freed.
   `on_unit_complete` fires again: `fetch_add` returns 2, not `< 2`, so no further admit.

No deadlock on the 1+1 pool because nothing blocks: the CPU thread returns after each unit and only
submits the next reads; the IO thread drains reads sequentially; `.get()` blocks the calling thread,
not a pool thread. Peak residency is 2 segments (`{0,1}` then `{1,2}`) = `K · max_unit_size`. The
shared segment 1 carries across the unit0→unit1 boundary, but segment 0 is freed *before* segment 2
is fired (the free happens inside `MemSegmentProcessingTask`, which precedes `.ensure`), so the bound
holds. This is the "sharing must be local to the admission window" property: it holds here because
the co-referencing units are adjacent (true for resample's `i`/`i+1`; column stats' row-slice
structure never overlaps at all).

