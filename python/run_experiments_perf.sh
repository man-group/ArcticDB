#!/bin/bash
# Validating performance - use release build
source /home/alex/venvs/310/bin/activate
which python
python -c "import arcticdb; import arcticdb_ext; print(arcticdb.__version__);  print(arcticdb_ext.__file__)"

export TYPE=perf

# 1_000 chunks, 3000ms sleep, 10 queue
NUM_CHUNKS_EXP=3 python stage_data.py
S3_SLEEP_MS=3000 QUEUE_SIZE=60 python finalize_data.py

## 1_000 chunks, 3000ms sleep, 100 queue
#NUM_CHUNKS_EXP=3 python stage_data.py
#S3_SLEEP_MS=3000 QUEUE_SIZE=100 python finalize_data.py
#
## 1_000 chunks, 3000ms sleep, 1000 queue
#NUM_CHUNKS_EXP=3 python stage_data.py
#S3_SLEEP_MS=3000 QUEUE_SIZE=1000 python finalize_data.py
