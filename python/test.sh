#!/bin/bash
export ASAN_OPTIONS=detect_leaks=0

set -euo pipefail

RUNS=100
CMD='LD_PRELOAD=/usr/local/lib/clang/19/lib/x86_64-unknown-linux-gnu/libclang_rt.asan.so python -m pytest -s -v /home/vasil/Documents/source/ArcticDB/python/tests/hypothesis/arcticdb/test_merge_update.py'

for i in $(seq 1 $RUNS); do
    echo "========================================
Run $i / $RUNS
========================================"
    if ! eval "$CMD"; then
        echo "========================================
FAILURE on run $i — stopping.
========================================"
        exit 1
    fi
done

echo "========================================
All $RUNS runs passed.
========================================"
