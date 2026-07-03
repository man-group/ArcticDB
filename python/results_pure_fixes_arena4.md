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
  the whole frame in memory. Raw data with wall times is in `results_pure_fixes_arena4.csv`.

Dimensions: rows = { 100M }, K = { 0 8 default }, IO:CPU = { 8:12 16:24 64:96 }.

## column_stats

### IO=8 CPU=12 — peak MiB / wall s

| rows | K=0 | K=8 | K=default |
|------|------|------|------|
| 100M | 2238.6 / 4.75 | 1698.6 / 8.39 | 2223.0 / 4.88 |

### IO=16 CPU=24 — peak MiB / wall s

| rows | K=0 | K=8 | K=default |
|------|------|------|------|
| 100M | 3120.5 / 3.33 | 1870.1 / 8.08 | 3026.5 / 3.51 |

### IO=64 CPU=96 — peak MiB / wall s

| rows | K=0 | K=8 | K=default |
|------|------|------|------|
| 100M | 10629.4 / 2.94 | 1755.3 / 7.08 | 10684.4 / 2.99 |

## filter

### IO=8 CPU=12 — peak MiB / wall s

| rows | K=0 | K=8 | K=default |
|------|------|------|------|
| 100M | 717.0 / 3.38 | 751.0 / 5.78 | 728.8 / 3.13 |

### IO=16 CPU=24 — peak MiB / wall s

| rows | K=0 | K=8 | K=default |
|------|------|------|------|
| 100M | 1242.5 / 1.90 | 805.4 / 2.84 | 1214.0 / 1.99 |

### IO=64 CPU=96 — peak MiB / wall s

| rows | K=0 | K=8 | K=default |
|------|------|------|------|
| 100M | 4042.1 / 1.68 | 812.7 / 2.78 | 3741.9 / 1.62 |

## resample

### IO=8 CPU=12 — peak MiB / wall s

| rows | K=0 | K=8 | K=default |
|------|------|------|------|
| 100M | 29255.4 / 4.54 | 17156.0 / 6.22 | 27048.7 / 4.78 |

### IO=16 CPU=24 — peak MiB / wall s

| rows | K=0 | K=8 | K=default |
|------|------|------|------|
| 100M | 35539.4 / 3.94 | 17807.3 / 6.02 | 33574.5 / 3.85 |

### IO=64 CPU=96 — peak MiB / wall s

| rows | K=0 | K=8 | K=default |
|------|------|------|------|
| 100M | 46191.2 / 3.81 | 17166.5 / 6.22 | 47240.3 / 3.83 |

## groupby

### IO=8 CPU=12 — peak MiB / wall s

| rows | K=0 | K=8 | K=default |
|------|------|------|------|
| 100M | 81812.4 / 17.45 | 79306.1 / 20.35 | 81387.5 / 19.81 |

### IO=16 CPU=24 — peak MiB / wall s

| rows | K=0 | K=8 | K=default |
|------|------|------|------|
| 100M | 82285.1 / 17.57 | 79413.0 / 27.60 | 82442.4 / 18.47 |

### IO=64 CPU=96 — peak MiB / wall s

| rows | K=0 | K=8 | K=default |
|------|------|------|------|
| 100M | 83465.5 / 18.87 | 79150.2 / 26.88 | 82258.3 / 19.10 |

