MAX_RUNS=1000
export LD_PRELOAD=/usr/local/lib/clang/19/lib/x86_64-unknown-linux-gnu/libclang_rt.asan.so
export ASAN_OPTIONS=detect_leaks=0
TEST_CMD="python -m pytest tests/unit/arcticdb/version_store/test_merge.py::TestMergeTimeseries::test_merge_update_row_from_source_matches_multiple_rows_from_target_in_separate_slices"

echo "Running test up to $MAX_RUNS times or until ASan failure..."

for i in $(seq 1 $MAX_RUNS); do
    echo "=== Run $i/$MAX_RUNS ==="
    $TEST_CMD
    EXIT_CODE=$?
    
    if [ $EXIT_CODE -ne 0 ]; then
        echo "Test failed on run $i with exit code $EXIT_CODE"
        exit $EXIT_CODE
    fi
    
    echo "Run $i completed successfully"
done

echo "All $MAX_RUNS runs completed without ASan failures"
