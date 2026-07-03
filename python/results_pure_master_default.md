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
  the whole frame in memory. Raw data with wall times is in `results_pure_master_default.csv`.

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
| 100M | 753.8 / 3.85 |

### IO=16 CPU=24 — peak MiB / wall s

| rows | K=default |
|------|------|
| 100M | 1309.9 / 2.73 |

### IO=64 CPU=96 — peak MiB / wall s

| rows | K=default |
|------|------|
| 100M | 4781.7 / 1.96 |

## resample

### IO=8 CPU=12 — peak MiB / wall s

| rows | K=default |
|------|------|
| 100M | 27737.7 / 4.41 |

### IO=16 CPU=24 — peak MiB / wall s

| rows | K=default |
|------|------|
| 100M | 36420.1 / 3.58 |

### IO=64 CPU=96 — peak MiB / wall s

| rows | K=default |
|------|------|
| 100M | 67304.9 / 2.73 |

## groupby

### IO=8 CPU=12 — peak MiB / wall s

| rows | K=default |
|------|------|
| 100M | 100647.5 / 17.63 |

### IO=16 CPU=24 — peak MiB / wall s

| rows | K=default |
|------|------|
| 100M | 106685.0 / 19.40 |

### IO=64 CPU=96 — peak MiB / wall s

| rows | K=default |
|------|------|
| 100M | 114505.5 / 18.82 |

