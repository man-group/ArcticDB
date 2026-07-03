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
  the whole frame in memory. Raw data with wall times is in `results_pure_fixes_tcmalloc.csv`.

Dimensions: rows = { 100M }, K = { 0 8 default }, IO:CPU = { 8:12 16:24 64:96 }.

## column_stats

### IO=8 CPU=12 — peak MiB / wall s

| rows | K=0 | K=8 | K=default |
|------|------|------|------|
| 100M | 2110.0 / 5.26 | 1702.7 / 7.94 | 2072.5 / 5.49 |

### IO=16 CPU=24 — peak MiB / wall s

| rows | K=0 | K=8 | K=default |
|------|------|------|------|
| 100M | 2848.8 / 3.92 | 1704.7 / 8.12 | 2716.4 / 3.58 |

### IO=64 CPU=96 — peak MiB / wall s

| rows | K=0 | K=8 | K=default |
|------|------|------|------|
| 100M | 8256.0 / 2.44 | 1641.4 / 8.23 | 7972.1 / 2.49 |

## filter

### IO=8 CPU=12 — peak MiB / wall s

| rows | K=0 | K=8 | K=default |
|------|------|------|------|
| 100M | 720.9 / 2.89 | 774.5 / 3.24 | 693.0 / 2.79 |

### IO=16 CPU=24 — peak MiB / wall s

| rows | K=0 | K=8 | K=default |
|------|------|------|------|
| 100M | 1257.0 / 1.94 | 709.0 / 2.82 | 1254.8 / 2.18 |

### IO=64 CPU=96 — peak MiB / wall s

| rows | K=0 | K=8 | K=default |
|------|------|------|------|
| 100M | 3977.0 / 1.60 | 713.0 / 3.03 | 3904.5 / 1.63 |

## resample

### IO=8 CPU=12 — peak MiB / wall s

| rows | K=0 | K=8 | K=default |
|------|------|------|------|
| 100M | 24463.6 / 5.66 | 16537.7 / 6.00 | 25448.9 / 5.47 |

### IO=16 CPU=24 — peak MiB / wall s

| rows | K=0 | K=8 | K=default |
|------|------|------|------|
| 100M | 30145.9 / 4.47 | 16504.7 / 5.82 | 33759.9 / 4.58 |

### IO=64 CPU=96 — peak MiB / wall s

| rows | K=0 | K=8 | K=default |
|------|------|------|------|
| 100M | 38581.2 / 4.52 | 16488.4 / 6.10 | 39270.0 / 4.60 |

## groupby

### IO=8 CPU=12 — peak MiB / wall s

| rows | K=0 | K=8 | K=default |
|------|------|------|------|
| 100M | 85473.0 / 19.62 | 83491.3 / 27.06 | 85457.5 / 17.39 |

### IO=16 CPU=24 — peak MiB / wall s

| rows | K=0 | K=8 | K=default |
|------|------|------|------|
| 100M | 89082.0 / 17.07 | 83612.0 / 21.27 | 89188.0 / 15.67 |

### IO=64 CPU=96 — peak MiB / wall s

| rows | K=0 | K=8 | K=default |
|------|------|------|------|
| 100M | 92444.0 / 16.03 | 83513.0 / 22.00 | 92484.4 / 17.13 |

