#!/bin/bash
# Validating memory - using debug build
source /home/alex/venvs/310/bin/activate
which python
python -c "import arcticdb; import arcticdb_ext; print(arcticdb.__version__);  print(arcticdb_ext.__file__)"

export TYPE=perf

# 10_000 chunks, 3000ms sleep, 10 threads, 20 queue
NUM_CHUNKS_EXP=4 python stage_data.py
S3_SLEEP_MS=3000 NUM_THREADS=10 QUEUE_SIZE=20 python finalize_data.py

# 10_000 chunks, 3000ms sleep, 20 threads, 20 queue
NUM_CHUNKS_EXP=4 python stage_data.py
S3_SLEEP_MS=3000 NUM_THREADS=20 QUEUE_SIZE=20 python finalize_data.py

# 10_000 chunks, 3000ms sleep, 20 threads, 40 queue
NUM_CHUNKS_EXP=4 python stage_data.py
S3_SLEEP_MS=3000 NUM_THREADS=20 QUEUE_SIZE=40 python finalize_data.py

# 10_000 chunks, 3000ms sleep, 20 threads, 60 queue
NUM_CHUNKS_EXP=4 python stage_data.py
S3_SLEEP_MS=3000 NUM_THREADS=20 QUEUE_SIZE=40 python finalize_data.py

# 10_000 chunks, 3000ms sleep, 40 threads, 40 queue
NUM_CHUNKS_EXP=4 python stage_data.py
S3_SLEEP_MS=3000 NUM_THREADS=20 QUEUE_SIZE=40 python finalize_data.py

# 10_000 chunks, 3000ms sleep, 40 threads, 80 queue
NUM_CHUNKS_EXP=4 python stage_data.py
S3_SLEEP_MS=3000 NUM_THREADS=20 QUEUE_SIZE=40 python finalize_data.py

# 10_000 chunks, 3000ms sleep, 40 threads, 120 queue
NUM_CHUNKS_EXP=4 python stage_data.py
S3_SLEEP_MS=3000 NUM_THREADS=20 QUEUE_SIZE=40 python finalize_data.py
