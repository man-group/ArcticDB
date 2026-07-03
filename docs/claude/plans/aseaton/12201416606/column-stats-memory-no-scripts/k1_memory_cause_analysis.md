# Why K=1 peak RSS greatly exceeds one processing unit (filter)

## Question

`results.md` shows that at `K=1` (`VersionStore.NumProcessingUnitsLive=1`), which admits one
processing unit at a time, peak `ru_maxrss` still grows with row count and IO/CPU pool size — e.g.
filter over 100M rows peaks at 4.9 GiB at IO=64. One processing unit here is a single row-slice
(100,000 rows x 101 float64/int64 cols = **77.1 MiB** decoded), so naively that looked like ~60
units resident. Is decode running ahead of admission, or is the high-water mark inflated by
allocator retention?

## Method

Added a process-global gauge of live decoded-input-segment bytes/count, incremented in
`add_slice_to_component_manager` and decremented via a custom `shared_ptr<SegmentInMemory>` deleter,
so it measures the decoded-from-disk working set from registration to release (the set `K` bounds).
Exposed `get_peak_decoded_segment_bytes/count` to Python. `measure_processing_memory.py` also grew a
VmRSS sampler, an optional `malloc_trim(0)` + `/proc/self/smaps` file-vs-anon split. Filter over
100M rows, reusing the existing all-zeros LMDB data. Reproducer: `python/run_gauge_matrix.sh`.

## Results (filter, 100M rows)

| run | ru_maxrss | peak live decoded | post-trim RSS | trim reclaimed | MALLOC_ARENA_MAX=1 peak |
|-----|-----------|-------------------|---------------|----------------|-------------------------|
| IO=8  K=1 | 1603 MiB | **77.8 MiB / 1 seg** | 1091 MiB | 454 MiB  | 1157 MiB |
| IO=16 K=1 | 1999 MiB | **77.8 MiB / 1 seg** | 1094 MiB | 847 MiB  | — |
| IO=64 K=1 | 4435 MiB | **77.8 MiB / 1 seg** | 1110 MiB | 3286 MiB | 1143 MiB |
| IO=64 K=0 (all) | 5732 MiB | 466.9 MiB / 6 seg | 1171 MiB | 4390 MiB | — |

`smaps` split at the post-trim floor (IO=8 K=1): file-backed **1011 MiB (of which LMDB data 915 MiB)**
/ anon **80 MiB**.

## Conclusion

**Decode does not run ahead. `K=1` bounds the live decoded set to exactly one processing unit
(77.8 MiB) at every pool size.** The apparent "~60 units resident" read off `ru_maxrss` was wrong.
The excess over one unit is two kinds of *retention*, neither of which is live decoded data:

1. **glibc malloc-arena retention of transient read/decompress buffers** — anonymous, reclaimable.
   This is the term that scales with the IO pool: `malloc_trim(0)` reclaims 454 -> 847 -> 3286 MiB
   for IO 8 -> 16 -> 64, and `MALLOC_ARENA_MAX=1` collapses the IO=64 peak 4435 -> 1143 MiB (-74%).
   More IO threads means more per-thread arenas each holding freed decompression scratch, so the
   high-water climbs with thread count while nothing extra is ever live.

2. **LMDB read-only mmap of the scanned data** — file-backed ~915 MiB, matching the on-disk file.
   A full scan touches every data page; those pages are resident and counted in RSS but are not
   malloc-managed, so `malloc_trim` cannot return them and the arena cap does not affect them. This
   is thread-independent, scales with data size, and is an LMDB-backend artifact (an object store
   backend copies into buffers rather than mmapping the store). Only ~80 MiB is anonymous at the
   floor, so ArcticDB's own allocator is **not** holding a large pool.

The `K=0` control confirms the design: even admitting all 1000 units, filter keeps only 6 segments
live at peak (467 MiB) because processing drains them as fast as they decode. The difference between
`K=0` and `K=1` in `results.md` is therefore almost entirely arena high-water (all reads firing at
once maximises concurrent transient buffers), not live decoded residency.

### Implications

- `K` works as intended on the decoded working set; the memory it *doesn't* control is transient
  allocator churn and the mmap. To cut the reported peaks without changing `K`, cap arenas
  (`MALLOC_ARENA_MAX`) or trim, and note LMDB mmap residency separately from processing memory.
- `results.md`'s framing of the `K=1` numbers as decode residency is misleading for LMDB; the peak
  is dominated by mmap + arena high-water.

## Notes

- The gauge is experiment-only. Its custom-deleter hook adds a heap allocation + capturing lambda on
  the segment-registration path; revert or guard before shipping. It overlaps the existing segment
  residency tracker (`SegmentInMemoryImpl::mark_from_disk`, see branch work log) and independently
  corroborates it.
