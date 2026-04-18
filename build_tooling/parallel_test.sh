#!/bin/bash
set -e

tooling_dir="$(dirname $BASH_SOURCE)"
echo Saving results to ${TEST_OUTPUT_DIR:="$(realpath "$tooling_dir/../cpp/out")"}
[[ -d "$TEST_OUTPUT_DIR" ]] || mkdir -p "$TEST_OUTPUT_DIR"

[[ -e ${PARALLEL_TEST_ROOT:=/tmp/parallel_test} ]] && rm -rf $PARALLEL_TEST_ROOT

catch=`{ which catchsegv 2>/dev/null || echo ; } | tail -n 1`

set -o xtrace -o pipefail

# Build a directory that's just the test assets, so can't access other Python source not in the wheel
mkdir -p $PARALLEL_TEST_ROOT
MSYS=winsymlinks:nativestrict ln -s "$(realpath "$tooling_dir/../python/tests")" $PARALLEL_TEST_ROOT/
cd $PARALLEL_TEST_ROOT

export ARCTICDB_RAND_SEED=$RANDOM
if [ "$VERSION_MAP_RELOAD_INTERVAL" != "-1" ]; then
    export ARCTICDB_VersionMap_ReloadInterval_int=$VERSION_MAP_RELOAD_INTERVAL
fi

export ARCTICDB_WARN_ON_WRITING_EMPTY_DATAFRAME=0
# Enable faulthandler so SIGSEGV/SIGBUS dump tracebacks to stderr
export PYTHONFAULTHANDLER=1
# Arm a C-level per-test watchdog that dumps tracebacks and kills the worker
# if a test hangs with the GIL held (where pytest-timeout's thread method can't fire).
# Crash tracebacks are written to per-PID files in ARCTICDB_FAULTHANDLER_DIR
# (xdist worker stderr is piped through execnet and never reaches CI logs).
export ARCTICDB_FAULTHANDLER_TIMEOUT=3300
export ARCTICDB_FAULTHANDLER_DIR="$TEST_OUTPUT_DIR/faulthandler"

print_faulthandler_crashes() {
    if [ -d "$ARCTICDB_FAULTHANDLER_DIR" ] && ls "$ARCTICDB_FAULTHANDLER_DIR"/crash_*.log 1>/dev/null 2>&1; then
        echo ""
        echo "======================== faulthandler crash dumps ========================"
        for f in "$ARCTICDB_FAULTHANDLER_DIR"/crash_*.log; do
            echo "--- $f ---"
            cat "$f"
            echo ""
        done
        echo "========================================================================="
    fi
}

# Disable set -e around pytest so we can capture the exit code,
# print faulthandler crash dumps, and optionally retry on OOM.
set +e

if [ -z "$ARCTICDB_PYTEST_ARGS" ]; then
    echo "Executing tests with no additional arguments"
    $catch python -m pytest --timeout=3600 --timeout_method=thread $PYTEST_XDIST_MODE -v \
        --log-file="$TEST_OUTPUT_DIR/pytest-logger.$group.log" \
        --junitxml="$TEST_OUTPUT_DIR/pytest.$group.xml" \
        --basetemp="$PARALLEL_TEST_ROOT/temp-pytest-output" \
        $PYTEST_ADD_TO_COMMAND_LINE "$@" 2>&1 | sed -r "s#^(tests/.*/([^/]+\.py))?#\2#"

    exit_code=${PIPESTATUS[0]}
    print_faulthandler_crashes
    # Retry with reduced parallelism if OOM‑killed
    if [ "$exit_code" -eq 137 ]; then
        echo "⚠️  pytest OOM‑killed (137) — retrying with 2 workers..."
        sleep 5
        $catch python -m pytest --timeout=3600 --timeout_method=thread -n 2 -v \
            --log-file="$TEST_OUTPUT_DIR/pytest-logger.$group.log" \
            --junitxml="$TEST_OUTPUT_DIR/pytest.$group.xml" \
            --basetemp="$PARALLEL_TEST_ROOT/temp-pytest-output" \
            $PYTEST_ADD_TO_COMMAND_LINE "$@" 2>&1 | sed -r "s#^(tests/.*/([^/]+\.py))?#\2#"
        exit_code=${PIPESTATUS[0]}
        print_faulthandler_crashes
    fi

else
    echo "Executing tests with additional pytest argiments:"
    echo "from user: $ARCTICDB_PYTEST_ARGS"
    echo "from automation: $PYTEST_ADD_TO_COMMAND_LINE"
    $catch python -m pytest --timeout=3600 --timeout_method=thread $PYTEST_XDIST_MODE -v \
        --log-file="$TEST_OUTPUT_DIR/pytest-logger.$group.log" \
        --junitxml="$TEST_OUTPUT_DIR/pytest.$group.xml" \
        --basetemp="$PARALLEL_TEST_ROOT/temp-pytest-output" \
        $PYTEST_ADD_TO_COMMAND_LINE $ARCTICDB_PYTEST_ARGS 2>&1

    exit_code=$?
    print_faulthandler_crashes
    # Retry with reduced parallelism if OOM‑killed
    if [ "$exit_code" -eq 137 ]; then
        echo "⚠️  pytest OOM‑killed (137) — retrying with 2 workers..."
        sleep 5
        $catch python -m pytest --timeout=3600 --timeout_method=thread -n 2 -v \
            --log-file="$TEST_OUTPUT_DIR/pytest-logger.$group.log" \
            --junitxml="$TEST_OUTPUT_DIR/pytest.$group.xml" \
            --basetemp="$PARALLEL_TEST_ROOT/temp-pytest-output" \
            $PYTEST_ADD_TO_COMMAND_LINE $ARCTICDB_PYTEST_ARGS 2>&1
        exit_code=$?
        print_faulthandler_crashes
    fi
fi

exit $exit_code
