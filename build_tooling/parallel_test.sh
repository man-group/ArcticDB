#!/bin/bash
set -e

tooling_dir="$(dirname $BASH_SOURCE)"
echo Saving results to ${TEST_OUTPUT_DIR:="$(realpath "$tooling_dir/../cpp/out")"}
[[ -d "$TEST_OUTPUT_DIR" ]] || mkdir -p "$TEST_OUTPUT_DIR"

[[ -e ${PARALLEL_TEST_ROOT:=/tmp/parallel_test} ]] && rm -rf $PARALLEL_TEST_ROOT

if [[ -n "${ARCTICDB_PERSISTENT_STORAGE_TESTS}" ]]; then
  # We want to run the pytests sequentially to avoid races
  splits=1
elif [[ -n "$TEST_PARALLELISM" ]]; then
  splits=${TEST_PARALLELISM}}
else
  cpus=${CMAKE_BUILD_PARALLEL_LEVEL:-`nproc || echo 2`}
  splits=$(($cpus * 15 / 10))
fi

catch=`{ which catchsegv 2>/dev/null || echo ; } | tail -n 1`

    set -o xtrace -o pipefail

    # Build a directory that's just the test assets, so can't access other Python source not in the wheel
    mkdir -p $PARALLEL_TEST_ROOT
    MSYS=winsymlinks:nativestrict ln -s "$(realpath "$tooling_dir/../python/tests")" $PARALLEL_TEST_ROOT/
    cd $PARALLEL_TEST_ROOT

    export ARCTICDB_RAND_SEED=$RANDOM

    $catch python -m pytest -n $splits $PYTEST_XDIST_MODE -v --log-file="$TEST_OUTPUT_DIR/pytest-logger.$group.log" \
        --junitxml="$TEST_OUTPUT_DIR/pytest.$group.xml" \
        --basetemp="$PARALLEL_TEST_ROOT/temp-pytest-output" \
        "$@" 2>&1 | sed -ur "s#^(tests/.*/([^/]+\.py))?#\2#"
