# Memory use on the clause read path: what peak RSS is actually made of

Supersedes `residency_findings.md` (see "Relation to the previous analysis" below).

## Question

With the two-throttle admission (read window `W` = `VersionStore.SegmentReadWindow`, residency budget
`E` = `VersionStore.NumProcessingUnitsLive`, in processing units), a *tight* residency (`E=8`) used
**more** peak RSS and ran **slower** than a loose one — the opposite of what a memory bound should do.
Why, and which knob actually controls peak memory?

## Setup

- Symbol `f64_10m_100c_zeros`: 10M rows x 101 float64/int64 cols, over **S3** (PURE), IO=96/CPU=64.
- Default slicing: 101 cols <= `column_group_size` (127) => 1 column slice; `segment_row_size` 100k =>
  100 row slices. So a filter unit = **1 segment**, ~80 MiB decoded; the whole symbol ~8 GiB.
- Empty filter `q["f_0"] == -1.0` (no output; peak reflects input decode, not the result frame).
- Probe (`python/mem_probe.py`): one measurement per process. Reports `ru_maxrss`, `VmHWM`, `VmRSS`
  before/after `malloc_trim(0)` (via `ctypes`), and the anon/file split from `/proc/self/smaps_rollup`.
  Allocator behaviour varied with `MALLOC_ARENA_MAX` and `LD_PRELOAD` of tcmalloc.

## Results

Peak `ru_maxrss` (MiB), wall (s):

| config | concurrency | glibc default | ARENA_MAX=1 | ARENA_MAX=4 | tcmalloc |
|--------|-------------|---------------|-------------|-------------|----------|
| E=8, W=192 (default) | ~8 | 4934 / 5.9 | 692 / 4.0 | 862 / 3.2 | 724 / 3.0 |
| E=8, W=16 | ~8 | 5008 / 5.9 | — | — | — |
| E=8, W=8 | ~8 | 4904 / 6.1 | — | — | — |
| E=0 (kill), W=192 | ~192 | 7157 / 2.3 | 6136 / 4.7 | 5509 / 1.7 | 5348 / 1.5 |
| E=0, W=16 | ~16 | 1327 / 2.3 | — | — | 1300 / 2.1 |
| E=32, W=16 | ~16 | 1319 / 2.6 | — | — | — |

`malloc_trim(0)` at end drops **every glibc run to ~200-230 MiB**.

## What peak RSS is made of

Peak RSS = **genuine concurrent working set** + **glibc arena fragmentation**. They behave differently
and must not be conflated.

1. **Genuine concurrent working set** scales with concurrency = `min(W, eligible)` where eligible is
   bounded by `E`. Read the allocator-independent (tcmalloc) column: ~8 concurrent -> ~0.7 GiB,
   ~16 -> ~1.3 GiB, ~192 -> ~5.3 GiB. This is dominated by **transient decompression scratch of the
   reads in flight**, not by decoded segments held for processing — the registered decoded set drains
   fast and stays small (that is the set `E` and the residency tracker bound). tcmalloc and arena caps
   cannot reduce this term; it is really live at peak.

2. **glibc arena fragmentation** is an *additional*, reclaimable term: freed transient buffers retained
   across many of the 96 IO-thread arenas. It is pathological exactly when the work is thin and slow
   (`E=8`: only ~8 reads in flight but spread over the 96-thread pool for ~6 s), adding ~4.2 GiB. It is
   near-zero when the run is fast and steady (`E=32/W=16`, `E=0/W=16`: peak ~= working set).

Evidence that separates them:
- `MALLOC_ARENA_MAX=1` collapses `E=8` from 4934 -> 692 MiB, and tcmalloc gives 724 MiB *and* halves the
  wall (5.9 -> 3.0 s). So the `E=8` excess was purely glibc fragmentation — an allocator artifact, not
  live data.
- The same caps barely touch the kill switch (`E=0/W=192`: 7157 -> 5509 with ARENA_MAX=4; tcmalloc
  5348). Its memory is genuine: 192 concurrent decodes really hold ~5.3 GiB of transient scratch at
  once. This is controlled by `W` (W=16 -> 1.3 GiB), not by arena tuning.

## Answer to the original question

The residency bound is doing its job on the live decoded set; **peak `ru_maxrss` was a misleading
proxy**. Small `E` looked bad for two reasons that have nothing to do with live residency: glibc arena
fragmentation (worst when work is thin and slow), and the fact that the dominant genuine term is
transient decode scratch bounded by `W`, not the registered decoded set bounded by `E`.

## Design implications

- **`W` (concurrent reads), not `E` (units), is the real lever on peak memory.** `E` bounds the
  registered decoded set, but that is the small term; the large term is transient decode scratch, which
  scales with concurrent reads. A byte-in-flight bound keyed on reads (or `W` directly) would control
  peak RSS far more predictably than a unit-count residency budget. This strengthens the byte-based
  follow-up in `python/read_memory_fix_plan.md` and argues it should count in-flight read/decode bytes,
  not just resident decoded units.
- **The allocator must be configured or RSS is dominated by fragmentation.** `MALLOC_ARENA_MAX=2-4` or
  tcmalloc removes the small-`E` blow-up; tcmalloc was strictly better here (lower peak *and* faster,
  e.g. kill switch 1.5 s vs 2.3 s). Worth evaluating tcmalloc as the default allocator, or at least
  documenting `MALLOC_ARENA_MAX`.
- **Validate the bound against live-set instrumentation, not raw glibc `ru_maxrss`.** Use the
  `SegmentResidencyTracker` count, the `decoded_segment_gauge` bytes, or a tcmalloc/after-trim peak.
- **Backend note (LMDB only):** on the LMDB backend a full scan also leaves the read-only mmap of the
  scanned data resident (~on-disk size, file-backed, not malloc-reclaimable). This is a separate,
  backend-specific term that does *not* appear on S3/PURE, which copies into buffers. Do not
  generalise LMDB peak numbers to object-store reads.

## Open items

- Not yet done: wire `cpp/arcticdb/util/decoded_segment_gauge.hpp` to Python and confirm the live
  decoded *bytes* are bounded by `E` and independent of the RSS fragmentation (one binding + rebuild).
- If a breakdown of the ~5.3 GiB kill-switch working set is wanted (decoded columns vs decompression
  scratch vs S3 client buffers), use tcmalloc's `HEAPPROFILE` + `pprof` (`libtcmalloc_and_profiler`
  is installed). Not needed to explain the anomaly above.

## Relation to the previous analysis (`residency_findings.md`)

That analysis was directionally right that `K` bounds the live decoded set and that small-`K` excess is
glibc arena retention. It is superseded here because it was measured only on LMDB (its second term, a
~915 MiB file-backed mmap, is an LMDB artifact absent on S3/PURE), it used end-of-run `malloc_trim`
reclaim as its fragmentation proxy (which over-attributes to fragmentation and misses that the kill
switch's memory is genuine concurrent transient scratch, as tcmalloc-at-peak shows), and it never
separated `W` from `E`, so it missed that concurrency is the real lever. The LMDB mmap observation is
preserved in the backend note above.
