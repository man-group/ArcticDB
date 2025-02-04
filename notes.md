Run minio `run.sh` in `/code/minio`

```
NUM_CHUNKS_EXP=1 python stage_data.py
S3_SLEEP_MS=1 NUM_THREADS=10 QUEUE_SIZE=20 python finalize_data.py
```

```
valgrind --tool=massif python S3_SLEEP_MS=1 NUM_THREADS=10 QUEUE_SIZE=20 python finalize_data.py
```

## Profiles

With 100k rows per incomplete:

1000 incompletes - 11801 (massif) 62 MiB at peak
Repeat 194805 62 MiB at peak
With 3sec sleep on writes 190180 (massif) 209 MiB at peak
With 10sec sleep on writes 199318 3.7 GiB at peak
10_000 incompletes - 14137 (massif) 118 MiB at peak
With 3sec sleep on writes 205911 891 MiB at peak

### New Profiles

#### Memory Fix Validation

To be executed by `./run_experiments.sh`

Using debug build

1000 incompletes
With 3sec sleep 343487 216 MiB at peak
With 3sec sleep with blocking queue 10 threads 234378 274 MiB
With 10sec sleep 199318 3.7GiB at peak
With 10sec sleep with blocking queue 10 threads 236356 263 MiB

10_000 incompletes
With 3sec sleep 205911 891 MiB at peak
With 3sec sleep with blocking queue 10 threads 241344 311 MiB
With 3sec sleep with blocking queue 20 threads 250365 277 MiB

#### Performance Impact

To be executed by `./run_experiments_perf.sh`

Using release build

10_000 incompletes
With 3sec sleep without my code changes
With 3sec sleep with blocking queue 10 threads TODO
With 3sec sleep with blocking queue 20 threads TODO

#### TODO

See if I can stop loading the incompletes twice?
