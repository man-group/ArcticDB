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
  the whole frame in memory. Raw data with wall times is in `results.csv`.

Dimensions: rows = { 100M }, K = { 0 1 10 100 default }, IO:CPU = { 8:12 16:24 64:96 }.

## column_stats

### IO=8 CPU=12 — peak MiB / wall s

| rows | K=0 | K=1 | K=10 | K=100 | K=default |
|------|------|------|------|------|------|
| 100M | 5885.7 / 3.20 | 2529.1 / 46.94 | 3060.0 / 5.70 | 6839.2 / 3.06 | 3960.3 / 3.90 |

### IO=16 CPU=24 — peak MiB / wall s

| rows | K=0 | K=1 | K=10 | K=100 | K=default |
|------|------|------|------|------|------|
| 100M | 20935.6 / 2.17 | 3008.7 / 47.23 | 3102.7 / 6.23 | 8093.5 / 2.08 | 6271.5 / 2.29 |

### IO=64 CPU=96 — peak MiB / wall s

| rows | K=0 | K=1 | K=10 | K=100 | K=default |
|------|------|------|------|------|------|
| 100M | 39448.1 / 3.32 | 5800.0 / 54.74 | 6122.4 / 5.84 | 11360.3 / 2.65 | 18718.1 / 3.17 |

## filter

### IO=8 CPU=12 — peak MiB / wall s

| rows | K=0 | K=1 | K=10 | K=100 | K=default |
|------|------|------|------|------|------|
| 100M | 1713.5 / 1.35 | 1603.0 / 13.54 | 1707.1 / 1.67 | 1705.5 / 1.37 | 1704.6 / 1.61 |

### IO=16 CPU=24 — peak MiB / wall s

| rows | K=0 | K=1 | K=10 | K=100 | K=default |
|------|------|------|------|------|------|
| 100M | 2326.0 / 0.77 | 2114.8 / 18.30 | 2211.9 / 2.67 | 2331.6 / 0.88 | 2306.6 / 0.87 |

### IO=64 CPU=96 — peak MiB / wall s

| rows | K=0 | K=1 | K=10 | K=100 | K=default |
|------|------|------|------|------|------|
| 100M | 5993.9 / 0.85 | 4858.2 / 19.30 | 5189.1 / 3.89 | 6043.2 / 1.19 | 5883.3 / 0.98 |

