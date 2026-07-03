"""Build results_pure_comparison.md from the six results_pure_{build}_{alloc}.csv files."""

import csv
import os

DIR = os.path.dirname(os.path.abspath(__file__))
BUILDS = ["master", "fixes"]
ALLOCS = ["default", "arena4", "tcmalloc"]
ALLOC_LABEL = {"default": "glibc default", "arena4": "MALLOC_ARENA_MAX=4", "tcmalloc": "tcmalloc"}
MEASURES = ["filter", "column_stats", "resample", "groupby"]
COMBOS = [("8", "12"), ("16", "24"), ("64", "96")]


def load(build, alloc):
    d = {}
    path = os.path.join(DIR, f"results_pure_{build}_{alloc}.csv")
    with open(path) as f:
        for r in csv.DictReader(f):
            d[(r["measure"], r["io"], r["cpu"], r["K"])] = (r["peak_mib"], r["wall_s"], r["status"])
    return d


data = {(b, a): load(b, a) for b in BUILDS for a in ALLOCS}


def cell(build, alloc, measure, io, cpu, k):
    v = data[(build, alloc)].get((measure, io, cpu, k))
    if not v or v[2] != "ok":
        return "-"
    return f"{float(v[0]):.0f} / {v[1]}"


out = []
out.append("# PURE read-path memory: master vs admission fixes\n")
out.append(
    "Comparison of peak resident memory on the clause read/processing path between `master` and the admission "
    "branch (`fixes`), to check the memory bound helps without regressing throughput, and how it interacts with "
    "the process allocator.\n"
)

out.append("## What was measured\n")
out.append(
    "Each table cell is **peak `ru_maxrss` (MiB) / wall time (s)** for one operation. Every cell is a separate "
    "process running a single operation, so `ru_maxrss` (peak RSS over the process lifetime) is a clean "
    "high-water mark for that one operation. `master` has no admission bound, so it has a single column. `fixes` "
    "sweeps the residency budget `K` (`VersionStore.NumProcessingUnitsLive`, the max processing units in flight): "
    "`K=0` is the kill switch (bound disabled), `K=8` a tight bound, `K=default` the computed default; the read "
    "window `W` (`VersionStore.SegmentReadWindow`) is left at its default of `2*io`. Each build is run under "
    "three process allocators: glibc default, glibc with `MALLOC_ARENA_MAX=4`, and tcmalloc (`LD_PRELOAD`). "
    "`IO:CPU` is the IO/CPU threadpool sizes (`VersionStore.NumIOThreads` / `NumCPUThreads`).\n"
)

out.append("## The symbol\n")
out.append(
    "One symbol, `f64_100m_100c_zeros`, on PURE (S3): **100,000,000 rows x 100 `float64` data columns** "
    "(`f_0`..`f_99`), all zeros, plus an `int64` `category` column cycling through values 1-5, indexed by a "
    "nanosecond `DatetimeIndex` at 1-second frequency. All-zeros data is highly compressible (small on the wire) "
    "but decodes to ~80 GiB, so the reads stress input decode and residency, not storage bandwidth. Written with "
    "default slicing (100k rows/segment, all columns in one column slice), giving 1000 row-slice segments.\n"
)

out.append("## The operations\n")
out.append(
    '- **filter**: `read` with `QueryBuilder()[q["f_0"] == -1.0]`. `-1.0` never occurs (data is zeros), so the '
    "result is empty and peak RSS reflects input-decode residency rather than any output frame.\n"
    "- **column_stats**: build MINMAX column statistics for each row-slice. On `fixes`, "
    "`create_column_stats` with an explicit spec of MINMAX over the 100 float columns; on `master`, "
    "`create_column_stats_experimental`, which takes no spec and auto-selects every eligible numeric column "
    "(the 100 floats plus the int64 `category`) — a ~1% column-count difference.\n"
    '- **resample**: `read` with `QueryBuilder().resample("30D").agg(sum)` summing all 100 float columns into '
    "~30-day buckets (a few dozen output rows).\n"
    '- **groupby**: `read` with `QueryBuilder().groupby("category").agg(sum)` summing all 100 float columns '
    "across the 5 `category` groups (5 output rows).\n"
)
out.append("## Key findings\n")
out.append(
    "- **The residency bound delivers large memory cuts on the aggregations** (peak, IO=64, glibc): "
    "column_stats 33745 -> 4396 MiB at K=8 (7.7x); resample 67305 -> 19555 (3.4x); groupby 114506 -> 80876 "
    "(1.4x). groupby benefits least — its processing needs most of the decoded input resident at once, so the "
    "bound has little room.\n"
    "- **No filter regression, and no memory change from the bound on filter.** master, K=0 and K=default are "
    "within noise on both peak and wall (IO=64: 4782 vs 4763 vs 4769 MiB; 1.96 vs 2.00 vs 2.17 s). filter's live "
    "decoded set is tiny, so peak is set by concurrency/allocator, not K (see `memory_use_findings.md`).\n"
    "- **Allocator choice is a large, independent lever.** glibc default inflates master peak badly; arena cap "
    "or tcmalloc cut it hard (column_stats IO=64 master: 33745 glibc -> 10575 arena4 -> 7472 tcmalloc). The best "
    "config everywhere is the bound plus tcmalloc: filter 64:96 K=8 tcmalloc = 713 MiB vs 4782 master glibc "
    "(6.7x); column_stats K=8 tcmalloc = 1641 vs 33745 (20x).\n"
    "- **K=0 reproduces master**, confirming the kill switch restores pre-admission behaviour.\n"
    "- **The bound costs wall time when it binds** (K=8 columns are slower, e.g. column_stats IO=64 8.95 s vs "
    "~2.6 s), the expected throughput-for-memory trade; K=default keeps wall close to master.\n"
)

for measure in MEASURES:
    out.append(f"\n## {measure}\n")
    for alloc in ALLOCS:
        out.append(f"\n### {ALLOC_LABEL[alloc]} — peak MiB / wall s\n")
        out.append("| IO:CPU | master | fixes K=0 | fixes K=8 | fixes K=default |")
        out.append("|--------|--------|-----------|-----------|-----------------|")
        for io, cpu in COMBOS:
            row = (
                f"| {io}:{cpu} "
                f"| {cell('master', alloc, measure, io, cpu, 'default')} "
                f"| {cell('fixes', alloc, measure, io, cpu, '0')} "
                f"| {cell('fixes', alloc, measure, io, cpu, '8')} "
                f"| {cell('fixes', alloc, measure, io, cpu, 'default')} |"
            )
            out.append(row)

with open(os.path.join(DIR, "results_pure_comparison.md"), "w") as f:
    f.write("\n".join(out) + "\n")
print("wrote results_pure_comparison.md")
