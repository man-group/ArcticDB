# Processing-memory benchmark (with admission)

Peak resident memory (`ru_maxrss`, MiB) for ArcticDB processing operations on a compressible
all-zeros symbol (100 float64 columns plus an int64 `category` column in [1, 5]), across
data size, the admission ceiling `K` (`VersionStore.NumProcessingUnitsLive`), and IO/CPU pool sizes.

## Approach

- `write_data.py` writes each row count to its own LMDB library as all zeros.
- Each cell is a **separate process** running a single operation, so `ru_maxrss` (peak RSS over the
  process lifetime) is a clean high-water mark for that one operation.
- Each table cell is `peak RSS (MiB) / wall time (s)`.
- Operations (`--measure`):
  - `column_stats`: `create_column_stats` with MINMAX over all 100 float columns.
  - `filter`: read with `QueryBuilder()[q["f_0"] == -1.0]` (matches no rows, so the result is empty
    and the peak reflects input-decode residency rather than the output frame size).
  - `resample`: `resample("30D").agg(sum)` over all float columns. Resample supports frequencies
    only up to "D", so a calendar month is approximated as 30 days.
  - `groupby`: `groupby("category").agg(sum)` over all float columns (5 groups).
- `K` is the number of processing units admitted in flight. `K=0` is the kill switch (admit all
  units, memory bound disabled). `K=default` is the computed default
  `max(2*cpu_threads, ceil(2*io_threads / max_unit_size))`.
- Thread combos are IO:CPU pool sizes set via `VersionStore.NumIOThreads` / `NumCPUThreads`.
- Data is written in 10M-row chunks (initial write + appends) so large row counts do not build
  the whole frame in memory. Raw data with wall times is in `results_pure_fixes_default.csv`.

Dimensions: rows = { 100M }, K = { 0 8 default }, IO:CPU = { 8:12 16:24 64:96 }.

## column_stats

### IO=8 CPU=12 — peak MiB / wall s

| rows | K=0 | K=8 | K=default |
|------|------|------|------|
| 100M | 2455.8 / 3.91 | 1699.1 / 7.73 | 2458.8 / 3.88 |

### IO=16 CPU=24 — peak MiB / wall s

| rows | K=0 | K=8 | K=default |
|------|------|------|------|
| 100M | 5300.7 / 3.02 | 2187.2 / 7.85 | 3596.9 / 3.00 |

### IO=64 CPU=96 — peak MiB / wall s

| rows | K=0 | K=8 | K=default |
|------|------|------|------|
| 100M | 34668.9 / 2.64 | 4396.3 / 8.95 | 17860.5 / 2.52 |

## filter

### IO=8 CPU=12 — peak MiB / wall s

| rows | K=0 | K=8 | K=default |
|------|------|------|------|
| 100M | 781.8 / 3.67 | 767.3 / 4.62 | 774.9 / 3.36 |

### IO=16 CPU=24 — peak MiB / wall s

| rows | K=0 | K=8 | K=default |
|------|------|------|------|
| 100M | 1331.0 / 2.38 | 1241.8 / 5.77 | 1316.9 / 2.76 |

### IO=64 CPU=96 — peak MiB / wall s

| rows | K=0 | K=8 | K=default |
|------|------|------|------|
| 100M | 4763.3 / 2.00 | 3528.2 / 6.15 | 4768.6 / 2.17 |

## resample

### IO=8 CPU=12 — peak MiB / wall s

| rows | K=0 | K=8 | K=default |
|------|------|------|------|
| 100M | 28330.5 / 4.39 | 17615.6 / 5.80 | 29180.9 / 4.37 |

### IO=16 CPU=24 — peak MiB / wall s

| rows | K=0 | K=8 | K=default |
|------|------|------|------|
| 100M | 41407.0 / 3.52 | 17734.9 / 5.57 | 39341.5 / 3.56 |

### IO=64 CPU=96 — peak MiB / wall s

| rows | K=0 | K=8 | K=default |
|------|------|------|------|
| 100M | 72794.3 / 2.55 | 19555.2 / 5.53 | 69743.3 / 2.57 |

## groupby

### IO=8 CPU=12 — peak MiB / wall s

| rows | K=0 | K=8 | K=default |
|------|------|------|------|
| 100M | 105813.9 / 17.37 | 78831.7 / 19.61 | 80688.4 / 17.29 |

### IO=16 CPU=24 — peak MiB / wall s

| rows | K=0 | K=8 | K=default |
|------|------|------|------|
| 100M | 106876.4 / 18.42 | 79326.9 / 19.26 | 82510.1 / 17.33 |

### IO=64 CPU=96 — peak MiB / wall s

| rows | K=0 | K=8 | K=default |
|------|------|------|------|
| 100M | 117136.0 / 20.62 | 80876.1 / 19.89 | 93083.5 / 17.56 |

