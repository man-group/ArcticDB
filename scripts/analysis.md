# What `malloc_trim(0)` is freeing in the ArcticDB read path

## Summary

The 11–13 MiB recovered by `malloc_trim(0)` after N reads consists of pages
backing **transient per-read allocations** that have already been freed by
the time the trim runs. They sit as coalesced free chunks in non-main
arenas, where glibc has no automatic release path.

## Method

- LD_PRELOAD shim (`scripts/malloc_trace.c`) records every `malloc` /
  `calloc` / `realloc` / `posix_memalign` / `aligned_alloc` / `memalign`
  above a configurable threshold, with a 32-frame `backtrace()`, plus the
  matching `free` of any tracked allocation.
- `scripts/malloc_trim_read.py` opens an LMDB Arctic, reads the symbol
  N times, then dumps `malloc_info(0)`, `/proc/self/status`,
  `/proc/self/smaps_rollup`, and `/proc/self/maps` to disk.
- `scripts/symbolize_trace.py` (and a more targeted variant for the
  985 MB debug `arcticdb_ext.so`) resolves each backtrace frame via
  `addr2line` against the captured `/proc/self/maps` snapshot.
- Threshold lowered from the default 4 MiB to 32 KiB after observing that
  the 5.7 MiB free chunks in arenas 154 and 158 are *coalesced* — no single
  allocation in the trace is ≥ 4 MiB.

## Dominant transient per-read allocations

### 1. Arrow column buffers (~5.4 MiB per read)

`ChunkedBufferImpl::create_detachable_block` at
`cpp/arcticdb/column_store/chunked_buffer.hpp:703` calls
`allocate_detachable_memory`, which is

```cpp
// cpp/arcticdb/util/allocator.cpp:26-30
uint8_t* allocate_detachable_memory(size_t size) {
    return get_detachable_allocator().allocate(size);
}
std::allocator<uint8_t> get_detachable_allocator() {
    return std::allocator<uint8_t>();
}
```

Per read the trace shows:

| size      | count | total    | meaning                                      |
|-----------|-------|----------|----------------------------------------------|
| 800,000   | 6     | 4.58 MiB | 100,000 rows × 8 bytes (int64/uint64/float64/timestamp columns) |
| 400,000   | 2     | 0.76 MiB | 100,000 rows × 4 bytes (int32 / float32 columns) |

These buffers are handed to Arrow via sparrow. They go through
`operator new` → glibc malloc, **completely bypassing ArcticDB's
`Allocator` class**, so the `free_count_of` counter that drives
`maybe_trim()` is never incremented for them.

### 2. LMDB read transactions (~50 KB per read)

`mdb_txn_begin` at `cpp/third_party/lmdbcxx/mdb.c:2854`, called from
`LmdbStorage::do_read` at
`cpp/arcticdb/storage/lmdb/lmdb_storage.cpp:140`. `calloc`'d, freed when
the transaction commits or aborts.

| size   | count | source            |
|--------|-------|-------------------|
| 50,410 | 4     | `mdb_txn_begin`   |

### 3. String-column scratch vectors (~32 KB chunks)

`std::vector<std::string>::emplace_back` reallocations inside
`arcticdb_ext`. 32,768-byte chunks via `std::allocator<std::string>`.
Again — no ArcticDB `Allocator` involvement.

## Why ordinary `free()` cannot release these pages

`free()` has an automatic trim path on both main and non-main arenas
(`_int_free_maybe_trim` in glibc `malloc/malloc.c`, line ~4544 on
master), but it is gated on two things:

- The freed chunk itself must be `>= ATTEMPT_TRIMMING_THRESHOLD` (64 KiB,
  defined at `malloc/malloc.c:1701`).
- It only touches the arena's **top chunk**: on the main arena it calls
  `systrim` to `sbrk` back trailing free space; on a non-main arena it
  calls `heap_trim` (`malloc/arena.c:526`), which can `munmap` an
  entirely empty heap segment or `madvise` the trailing pages of the
  top chunk, but only if `chunksize(top_chunk) >= trim_threshold`.

Free chunks that are *interior* to an arena — i.e. not adjacent to
the top — are never reached by the auto-trim path. They are only
released by `malloc_trim`, whose body (`mtrim` at
`malloc/malloc.c:4770`) walks every bin in every arena and
`madvise(MADV_DONTNEED)`s the page-aligned interior of every free
chunk above one page in size.

The 5.7 MiB free chunks observed in arenas 154 and 158 are exactly
this case: they are single coalesced chunks in a bin, sitting in
the interior of their arena, not at the top, so no number of
subsequent `free()` calls can release their pages.

The trace ran with default `MALLOC_ARENA_MAX` (8 × ncpu = 160 on this
20-core box); folly executor / Python worker threads each pin to their
own arena. After many reads, arenas 154 and 158 each hold one
~5.7 MiB coalesced free chunk — the chunks that contained the Arrow
column buffers, plus padding from the LMDB and `std::vector` frees.

`malloc_trim(0)` walks every arena and issues
`madvise(MADV_DONTNEED)` on the page-aligned interior of every large
free chunk. That is the *only* mechanism that releases physical pages
in non-main arenas. The result is a ~11–13 MiB drop in `VmRSS` while
`VmSize` is unchanged, which is exactly what we observe.

## Implication for ArcticDB's `maybe_trim`

`AllocatorImpl::maybe_trim()` only fires on frees that go through
`Allocator::free` — i.e. `Buffer` and `ChunkedBuffer` block headers, but
**not** the detachable column data. On the read path most of the freed
bytes never increment that counter, so whether `maybe_trim` is
"necessary" depends on the secondary stream of small `Allocator` frees
still tripping `TrimCount=250` often enough.

When it does fire, the `malloc_trim(0)` call cleans up the bigger debt
(the std-allocator frees) as a side effect. That is the load-bearing
reason the trim call is doing useful work on the read path — not the
allocations the counter was nominally tracking.

## Reproduction artefacts

- `/tmp/malloc_trace.log` — 574 alloc events with backtraces.
- `/tmp/maps_snapshot.txt` — `/proc/self/maps` at trace end.
- `/tmp/malloc_info_after_reads.xml` — per-arena free-chunk histogram.

## Helper scripts

- `scripts/malloc_trace.c` — LD_PRELOAD shim with bootstrap allocator
  for dlsym pre-init mallocs.
- `scripts/malloc_trim_write.py` — writes the test symbol.
- `scripts/malloc_trim_read.py` — reads N times and dumps heap state.
- `scripts/malloc_trim_sweep.sh` — sweeps N over `1 5 10 20 50 100 200`.
- `scripts/symbolize_trace.py` — addr2line-based symbolizer.

addr2line works on the 985 MB debug `arcticdb_ext.so` but cannot batch
all addresses in one process without OOMing — symbolize one size at a
time.
