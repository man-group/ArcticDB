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
if [ -z "$ARCTICDB_PYTEST_ARGS" ]; then
    echo "Executing tests with no additional arguments"
    $catch python -m pytest --timeout=3600 $PYTEST_XDIST_MODE -v \
        --log-file="$TEST_OUTPUT_DIR/pytest-logger.$group.log" \
        --junitxml="$TEST_OUTPUT_DIR/pytest.$group.xml" \
        --basetemp="$PARALLEL_TEST_ROOT/temp-pytest-output" \
        $PYTEST_ADD_TO_COMMAND_LINE "$@" 2>&1 | sed -r "s#^(tests/.*/([^/]+\.py))?#\2#"
else
    echo "Executing tests with additional pytest argiments:"
    echo "from user: $ARCTICDB_PYTEST_ARGS"
    echo "from automation: $PYTEST_ADD_TO_COMMAND_LINE"
    $catch python -m pytest --timeout=3600 $PYTEST_XDIST_MODE -v \
        --log-file="$TEST_OUTPUT_DIR/pytest-logger.$group.log" \
        --junitxml="$TEST_OUTPUT_DIR/pytest.$group.xml" \
        --basetemp="$PARALLEL_TEST_ROOT/temp-pytest-output" \
        $PYTEST_ADD_TO_COMMAND_LINE $ARCTICDB_PYTEST_ARGS 2>&1
fi
