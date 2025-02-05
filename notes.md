Run minio `run.sh` in `/code/minio`

```
NUM_CHUNKS_EXP=1 python stage_data.py
S3_SLEEP_MS=1 NUM_THREADS=10 QUEUE_SIZE=20 python finalize_data.py
```

```
valgrind --tool=massif python S3_SLEEP_MS=1 NUM_THREADS=10 QUEUE_SIZE=20 python finalize_data.py
```

## Profiles

All on my 20 core laptop (so IO pool size 30)

All with 100k rows per incomplete

1000 incompletes
With 10sec sleep on writes 199318 3.7 GiB at peak

### New Profiles

#### Memory Fix Validation

To be executed by `./run_experiments.sh`

Using debug build

1_000 incompletes with 10sec sleep
Queue size 10 75406 125 MiB at peak
Queue size 100 79632 755 MiB at peak
Queue size 1000 81310 3.7 GiB at peak

#### Performance Impact

To be executed by `./run_experiments_perf.sh`

Using release build

1_000 incompletes with 3sec sleep
without the semaphore 120s
with queue size 10 325s
with queue size 60 (proposed default) 120s
with queue size 100 119s
with queue size 1000 121s

#### TODO

See if I can stop loading the incompletes twice?
