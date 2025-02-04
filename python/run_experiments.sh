#!/bin/bash
# Validating memory - using debug build
source /home/alex/venvs/310/bin/activate
which python
python -c "import arcticdb; import arcticdb_ext; print(arcticdb.__version__);  print(arcticdb_ext.__file__)"

# 1000 chunks, 3000ms sleep, 10 threads, 20 queue
NUM_CHUNKS_EXP=3 python stage_data.py
S3_SLEEP_MS=3000 NUM_THREADS=10 QUEUE_SIZE=20 valgrind --tool=massif --massif-out-file="massif.%p.sleep_3000_threads_10_queue_20_chunks_1k.bin" python finalize_data.py

# 1000 chunks, 10_000ms sleep, 10 threads, 20 queue
NUM_CHUNKS_EXP=3 python stage_data.py
S3_SLEEP_MS=10000 NUM_THREADS=10 QUEUE_SIZE=20 valgrind --tool=massif --massif-out-file="massif.%p.sleep_10000_threads_10_queue_20_chunks_1k.bin" python finalize_data.py

# 10_000 chunks, 3000ms sleep, 10 threads, 20 queue
NUM_CHUNKS_EXP=4 python stage_data.py
S3_SLEEP_MS=3000 NUM_THREADS=10 QUEUE_SIZE=20 valgrind --tool=massif --massif-out-file="massif.%p.sleep_3000_threads_10_queue_20_chunks_10k.bin" python finalize_data.py

# 10_000 chunks, 3000ms sleep, 20 threads, 40 queue
NUM_CHUNKS_EXP=4 python stage_data.py
S3_SLEEP_MS=3000 NUM_THREADS=20 QUEUE_SIZE=40 valgrind --tool=massif --massif-out-file="massif.%p.sleep_3000_threads_20_queue_40_chunks_10k.bin" python finalize_data.py
