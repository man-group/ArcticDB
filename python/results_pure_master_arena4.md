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
  the whole frame in memory. Raw data with wall times is in `results_pure_master_arena4.csv`.

Dimensions: rows = { 100M }, K = { default }, IO:CPU = { 8:12 16:24 64:96 }.

## column_stats

### IO=8 CPU=12 — peak MiB / wall s

| rows | K=default |
|------|------|
| 100M | FAIL |

### IO=16 CPU=24 — peak MiB / wall s

| rows | K=default |
|------|------|
| 100M | FAIL |

### IO=64 CPU=96 — peak MiB / wall s

| rows | K=default |
|------|------|
| 100M | FAIL |

## filter

### IO=8 CPU=12 — peak MiB / wall s

| rows | K=default |
|------|------|
| 100M | 760.2 / 3.52 |

### IO=16 CPU=24 — peak MiB / wall s

| rows | K=default |
|------|------|
| 100M | 1245.5 / 2.28 |

### IO=64 CPU=96 — peak MiB / wall s

| rows | K=default |
|------|------|
| 100M | 3683.7 / 1.60 |

## resample

### IO=8 CPU=12 — peak MiB / wall s

| rows | K=default |
|------|------|
| 100M | 26604.8 / 4.82 |

### IO=16 CPU=24 — peak MiB / wall s

| rows | K=default |
|------|------|
| 100M | 34450.3 / 3.79 |

### IO=64 CPU=96 — peak MiB / wall s

| rows | K=default |
|------|------|
| 100M | 47546.2 / 3.79 |

## groupby

### IO=8 CPU=12 — peak MiB / wall s

| rows | K=default |
|------|------|
| 100M | 81761.3 / 17.31 |

### IO=16 CPU=24 — peak MiB / wall s

| rows | K=default |
|------|------|
| 100M | 82329.6 / 19.95 |

### IO=64 CPU=96 — peak MiB / wall s

| rows | K=default |
|------|------|
| 100M | 83656.8 / 18.57 |

