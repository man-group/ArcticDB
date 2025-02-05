#!/bin/bash
# Validating memory - using debug build
source /home/alex/venvs/310/bin/activate
which python
python -c "import arcticdb; import arcticdb_ext; print(arcticdb.__version__);  print(arcticdb_ext.__file__)"

# 1000 chunks, 10_000ms sleep, 10 queue
NUM_CHUNKS_EXP=3 python stage_data.py
S3_SLEEP_MS=10000 QUEUE_SIZE=10 valgrind --tool=massif --massif-out-file="massif.%p.sleep_10000_queue_10_chunks_1k.bin" python finalize_data.py

# 1000 chunks, 10_000ms sleep, 100 queue
NUM_CHUNKS_EXP=3 python stage_data.py
S3_SLEEP_MS=10000 QUEUE_SIZE=100 valgrind --tool=massif --massif-out-file="massif.%p.sleep_10000_queue_100_chunks_1k.bin" python finalize_data.py

# 1000 chunks, 10_000ms sleep, 1000 queue
NUM_CHUNKS_EXP=3 python stage_data.py
S3_SLEEP_MS=10000 QUEUE_SIZE=1000 valgrind --tool=massif --massif-out-file="massif.%p.sleep_10000_queue_1000_chunks_1k.bin" python finalize_data.py
