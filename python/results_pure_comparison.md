# PURE read-path memory: master vs this branch

## Measurements

Each table cell is **peak `ru_maxrss` (MiB) / wall time (s)** for one operation. Every cell is a separate process running a single operation, so `ru_maxrss` (peak RSS over the process lifetime) is a clean high-water mark for that one operation.
`master` has no admission bound, so it has a single column. `fixes` varies the residency budget `K` (`VersionStore.NumProcessingUnitsLive`, the max processing units in flight):
`K=0` is the kill switch (bound disabled), `K=8` a tight bound, `K=default` the computed default; the read window `W` (`VersionStore.SegmentReadWindow`) is left at its default of `2*io`.
Each build is run under three process allocators: glibc default, glibc with `MALLOC_ARENA_MAX=4`, and tcmalloc (`LD_PRELOAD`). `IO:CPU` is the IO/CPU threadpool sizes (`VersionStore.NumIOThreads` / `NumCPUThreads`).

## Data

One symbol, `f64_100m_100c_zeros`, on PURE (S3): **100,000,000 rows x 100 `float64` data columns** (`f_0`..`f_99`), all zeros, plus an `int64` `category` column cycling through values 1-5,
indexed by a nanosecond `DatetimeIndex` at 1-second frequency. Written with default slicing (100k rows/segment, all columns in one column slice), giving 1000 row-slice segments.

Each segment is about 80MiB decoded, the symbol is about 80GiB decoded.

## Operations

- **filter**: `read` with `QueryBuilder()[q["f_0"] == -1.0]`. `-1.0` never occurs (data is zeros), so the result is empty and does not contribute to RSS
- **column_stats**: build MINMAX column statistics for each row-slice
- **resample**: `read` with `QueryBuilder().resample("30D").agg(sum)` summing all 100 float columns into ~30-day buckets (a few dozen output rows).
- **groupby**: `read` with `QueryBuilder().groupby("category").agg(sum)` summing all 100 float columns across the 5 `category` groups (5 output rows).

## Findings

- *Memory cuts on the aggregations* (peak, IO=64, glibc): column_stats 33745 -> 4396 MiB at K=8 (7.7x); resample 67305 -> 19555 (3.4x); groupby 114506 -> 80876 (1.4x)
- *Allocator makes a big difference* glibc default inflates peak RSS badly as there are so many arenas on the build servers, arena cap or tcmalloc cut RSS significantly
(column_stats IO=64 master: 33745 glibc -> 10575 arena4 -> 7472 tcmalloc).
The best config is the bound plus tcmalloc: filter 64:96 K=8 tcmalloc = 713 MiB vs 4782 master glibc (6.7x); column_stats K=8 tcmalloc = 1641 vs 33745 (20x).
- *K=0 reproduces master*, confirming the kill switch restores pre-admission behaviour.
- *The bound costs wall time* (K=8 columns are slower, e.g. column_stats IO=64 8.95 s vs ~2.6 s), an expected trade, K=default keeps wall close to master.

## filter


### glibc default — peak MiB / wall s

| IO:CPU | master | fixes K=0 | fixes K=8 | fixes K=default |
|--------|--------|-----------|-----------|-----------------|
| 8:12 | 754 / 3.85 | 782 / 3.67 | 767 / 4.62 | 775 / 3.36 |
| 16:24 | 1310 / 2.73 | 1331 / 2.38 | 1242 / 5.77 | 1317 / 2.76 |
| 64:96 | 4782 / 1.96 | 4763 / 2.00 | 3528 / 6.15 | 4769 / 2.17 |

### MALLOC_ARENA_MAX=4 — peak MiB / wall s

| IO:CPU | master | fixes K=0 | fixes K=8 | fixes K=default |
|--------|--------|-----------|-----------|-----------------|
| 8:12 | 760 / 3.52 | 717 / 3.38 | 751 / 5.78 | 729 / 3.13 |
| 16:24 | 1246 / 2.28 | 1242 / 1.90 | 805 / 2.84 | 1214 / 1.99 |
| 64:96 | 3684 / 1.60 | 4042 / 1.68 | 813 / 2.78 | 3742 / 1.62 |

### tcmalloc — peak MiB / wall s

| IO:CPU | master | fixes K=0 | fixes K=8 | fixes K=default |
|--------|--------|-----------|-----------|-----------------|
| 8:12 | 724 / 2.79 | 721 / 2.89 | 774 / 3.24 | 693 / 2.79 |
| 16:24 | 1296 / 2.36 | 1257 / 1.94 | 709 / 2.82 | 1255 / 2.18 |
| 64:96 | 3679 / 1.73 | 3977 / 1.60 | 713 / 3.03 | 3904 / 1.63 |

## column_stats


### glibc default — peak MiB / wall s

| IO:CPU | master | fixes K=0 | fixes K=8 | fixes K=default |
|--------|--------|-----------|-----------|-----------------|
| 8:12 | 2420 / 5.22 | 2456 / 3.91 | 1699 / 7.73 | 2459 / 3.88 |
| 16:24 | 4102 / 3.19 | 5301 / 3.02 | 2187 / 7.85 | 3597 / 3.00 |
| 64:96 | 33745 / 2.89 | 34669 / 2.64 | 4396 / 8.95 | 17860 / 2.52 |

### MALLOC_ARENA_MAX=4 — peak MiB / wall s

| IO:CPU | master | fixes K=0 | fixes K=8 | fixes K=default |
|--------|--------|-----------|-----------|-----------------|
| 8:12 | 2182 / 5.30 | 2239 / 4.75 | 1699 / 8.39 | 2223 / 4.88 |
| 16:24 | 2908 / 3.78 | 3120 / 3.33 | 1870 / 8.08 | 3026 / 3.51 |
| 64:96 | 10575 / 3.09 | 10629 / 2.94 | 1755 / 7.08 | 10684 / 2.99 |

### tcmalloc — peak MiB / wall s

| IO:CPU | master | fixes K=0 | fixes K=8 | fixes K=default |
|--------|--------|-----------|-----------|-----------------|
| 8:12 | 1993 / 5.32 | 2110 / 5.26 | 1703 / 7.94 | 2072 / 5.49 |
| 16:24 | 2806 / 4.03 | 2849 / 3.92 | 1705 / 8.12 | 2716 / 3.58 |
| 64:96 | 7472 / 2.44 | 8256 / 2.44 | 1641 / 8.23 | 7972 / 2.49 |

## resample


### glibc default — peak MiB / wall s

| IO:CPU | master | fixes K=0 | fixes K=8 | fixes K=default |
|--------|--------|-----------|-----------|-----------------|
| 8:12 | 27738 / 4.41 | 28330 / 4.39 | 17616 / 5.80 | 29181 / 4.37 |
| 16:24 | 36420 / 3.58 | 41407 / 3.52 | 17735 / 5.57 | 39342 / 3.56 |
| 64:96 | 67305 / 2.73 | 72794 / 2.55 | 19555 / 5.53 | 69743 / 2.57 |

### MALLOC_ARENA_MAX=4 — peak MiB / wall s

| IO:CPU | master | fixes K=0 | fixes K=8 | fixes K=default |
|--------|--------|-----------|-----------|-----------------|
| 8:12 | 26605 / 4.82 | 29255 / 4.54 | 17156 / 6.22 | 27049 / 4.78 |
| 16:24 | 34450 / 3.79 | 35539 / 3.94 | 17807 / 6.02 | 33574 / 3.85 |
| 64:96 | 47546 / 3.79 | 46191 / 3.81 | 17166 / 6.22 | 47240 / 3.83 |

### tcmalloc — peak MiB / wall s

| IO:CPU | master | fixes K=0 | fixes K=8 | fixes K=default |
|--------|--------|-----------|-----------|-----------------|
| 8:12 | 23101 / 5.87 | 24464 / 5.66 | 16538 / 6.00 | 25449 / 5.47 |
| 16:24 | 30853 / 4.97 | 30146 / 4.47 | 16505 / 5.82 | 33760 / 4.58 |
| 64:96 | 37698 / 4.55 | 38581 / 4.52 | 16488 / 6.10 | 39270 / 4.60 |

## groupby


### glibc default — peak MiB / wall s

| IO:CPU | master | fixes K=0 | fixes K=8 | fixes K=default |
|--------|--------|-----------|-----------|-----------------|
| 8:12 | 100648 / 17.63 | 105814 / 17.37 | 78832 / 19.61 | 80688 / 17.29 |
| 16:24 | 106685 / 19.40 | 106876 / 18.42 | 79327 / 19.26 | 82510 / 17.33 |
| 64:96 | 114506 / 18.82 | 117136 / 20.62 | 80876 / 19.89 | 93084 / 17.56 |

### MALLOC_ARENA_MAX=4 — peak MiB / wall s

| IO:CPU | master | fixes K=0 | fixes K=8 | fixes K=default |
|--------|--------|-----------|-----------|-----------------|
| 8:12 | 81761 / 17.31 | 81812 / 17.45 | 79306 / 20.35 | 81388 / 19.81 |
| 16:24 | 82330 / 19.95 | 82285 / 17.57 | 79413 / 27.60 | 82442 / 18.47 |
| 64:96 | 83657 / 18.57 | 83466 / 18.87 | 79150 / 26.88 | 82258 / 19.10 |

### tcmalloc — peak MiB / wall s

| IO:CPU | master | fixes K=0 | fixes K=8 | fixes K=default |
|--------|--------|-----------|-----------|-----------------|
| 8:12 | 85505 / 17.78 | 85473 / 19.62 | 83491 / 27.06 | 85458 / 17.39 |
| 16:24 | 88473 / 17.07 | 89082 / 17.07 | 83612 / 21.27 | 89188 / 15.67 |
| 64:96 | 92356 / 16.78 | 92444 / 16.03 | 83513 / 22.00 | 92484 / 17.13 |
