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

# Unset means default. 0 would be the kill switch (unbounded residency), so it must not be used as the sentinel.
if [ -n "$NUM_PROCESSING_UNITS_LIVE" ] && [ "$NUM_PROCESSING_UNITS_LIVE" != "-1" ]; then
    export ARCTICDB_VersionStore_NumProcessingUnitsLive_int=$NUM_PROCESSING_UNITS_LIVE
fi

# Enable faulthandler so SIGSEGV/SIGBUS dump tracebacks to stderr
export PYTHONFAULTHANDLER=1
# Arm a C-level per-test watchdog that dumps tracebacks and kills the worker
# if a test hangs with the GIL held (where pytest-timeout's thread method can't fire).
# Crash tracebacks are written to per-PID files in ARCTICDB_FAULTHANDLER_DIR
# (xdist worker stderr is piped through execnet and never reaches CI logs).
export ARCTICDB_FAULTHANDLER_TIMEOUT=3300
export ARCTICDB_FAULTHANDLER_DIR="$TEST_OUTPUT_DIR/faulthandler"

# A step timeout SIGKILLs the whole step: no junit XML (it is only written at session end), no --durations, and
# print_faulthandler_crashes below never runs - two hours of runner time that tell us nothing. This has happened
# on slow Windows runners, where the same suite takes 46 min or 123 min depending on which VM SKU it lands on.
# Send SIGINT a little before that instead, which makes pytest and xdist shut down, write the report and print
# the slowest tests. --kill-after covers the case where the SIGINT itself is not enough.
session_timeout=${PYTEST_SESSION_TIMEOUT:-100m}
timeout_cmd=""
if command -v timeout >/dev/null 2>&1; then
    timeout_cmd="timeout --signal=INT --kill-after=5m $session_timeout"
else
    echo "No timeout(1) available; the session will run until the CI step limit kills it"
fi

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

# Disable set -e around pytest so we can capture the exit code
# and print faulthandler crash dumps.
set +e

if [ -z "$ARCTICDB_PYTEST_ARGS" ]; then
    echo "Executing tests with no additional arguments"
    $timeout_cmd $catch python -u -m pytest --timeout=3600 --timeout_method=thread $PYTEST_XDIST_MODE -v \
        --durations=50 \
        --log-file="$TEST_OUTPUT_DIR/pytest-logger.$group.log" \
        --junitxml="$TEST_OUTPUT_DIR/pytest.$group.xml" \
        --basetemp="$PARALLEL_TEST_ROOT/temp-pytest-output" \
        $PYTEST_ADD_TO_COMMAND_LINE "$@" 2>&1 | sed -u -r "s#^(tests/.*/([^/]+\.py))?#\2#"

    exit_code=${PIPESTATUS[0]}
    print_faulthandler_crashes

else
    echo "Executing tests with additional pytest argiments:"
    echo "from user: $ARCTICDB_PYTEST_ARGS"
    echo "from automation: $PYTEST_ADD_TO_COMMAND_LINE"
    $timeout_cmd $catch python -u -m pytest --timeout=3600 --timeout_method=thread $PYTEST_XDIST_MODE -v \
        --durations=50 \
        --log-file="$TEST_OUTPUT_DIR/pytest-logger.$group.log" \
        --junitxml="$TEST_OUTPUT_DIR/pytest.$group.xml" \
        --basetemp="$PARALLEL_TEST_ROOT/temp-pytest-output" \
        $PYTEST_ADD_TO_COMMAND_LINE $ARCTICDB_PYTEST_ARGS 2>&1

    exit_code=$?
    print_faulthandler_crashes
fi

exit $exit_code
