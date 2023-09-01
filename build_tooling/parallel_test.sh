#!/bin/bash
set -e

tooling_dir="$(dirname $BASH_SOURCE)"
echo Saving results to ${TEST_OUTPUT_DIR:="$(realpath "$tooling_dir/../cpp/out")"}
[[ -d "$TEST_OUTPUT_DIR" ]] || mkdir -p "$TEST_OUTPUT_DIR"

[[ -e ${PARALLEL_TEST_ROOT:=/tmp/parallel_test} ]] && rm -rf $PARALLEL_TEST_ROOT
splits=${TEST_PARALLELISM:-${CMAKE_BUILD_PARALLEL_LEVEL:-`nproc || echo 2`}}

catch=`{ which catchsegv 2>/dev/null || echo ; } | tail -n 1`

function worker() {
    group=${1:?Must pass the group id argument}
    shift
    new_root=$PARALLEL_TEST_ROOT/$group
    set -o xtrace -o pipefail

    # Build a directory that's just the test assets, so can't access other Python source not in the wheel
    # Each test also get a separate directory since there's a mystery lock somewhere preventing concurrent runs (even)
    # from different Python processes
    mkdir -p $new_root
    MSYS=winsymlinks:nativestrict ln -s "$(realpath "$tooling_dir/../python/tests")" $new_root/
    cd $new_root

    $catch python -m pytest -v --show-capture=no --log-file="$TEST_OUTPUT_DIR/pytest-logger.$group.log" \
        --junitxml="$TEST_OUTPUT_DIR/pytest.$group.xml" \
        --splits $splits --group $group \
        --basetemp="$new_root/temp-pytest-output" \
        "$@" 2>&1 | sed -ur "s#^(tests/.*/([^/]+\.py))?#$group: \2#"
}

for i in `seq $splits` ; do
    worker $i "$@" &
    pids[$i]=$!
done

err=0
for pid in ${pids[*]} ; do
    wait $pid || err=$?
done

exit $err
