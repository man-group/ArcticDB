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

## filter

### IO=8 CPU=12 — peak MiB / wall s

| rows | K=0 | K=1 | K=10 | K=100 | K=default |
|------|------|------|------|------|------|
| 100M | 756.4 / 3.87 | 704.9 / 29.44 | 764.5 / 3.95 | 766.1 / 4.06 | 761.5 / 3.79 |

### IO=16 CPU=24 — peak MiB / wall s

| rows | K=0 | K=1 | K=10 | K=100 | K=default |
|------|------|------|------|------|------|
| 100M | 1343.5 / 2.42 | 1204.0 / 25.30 | 1272.3 / 4.55 | 1311.6 / 3.19 | 1296.0 / 3.60 |

### IO=64 CPU=96 — peak MiB / wall s

| rows | K=0 | K=1 | K=10 | K=100 | K=default |
|------|------|------|------|------|------|
| 100M | 4707.7 / 2.95 | 3902.6 / 32.67 | 3885.3 / 5.88 | 4783.9 / 2.61 | 4761.4 / 2.69 |

