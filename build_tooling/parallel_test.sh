#!/bin/bash
set -e

tooling_dir="$(dirname $BASH_SOURCE)"
echo Saving results to ${test_output:="$(realpath "$tooling_dir/../cpp/out")"}
[[ -d "$test_output" ]] || mkdir -p "$test_output"

function worker() {
    group=${1:?Must pass the group id argument}
    shift
    duration_file="$test_output/pytest-durations.$group.json"
    new_root=/tmp/just_tests$group
    set -o xtrace -o pipefail

    cp "$tooling_dir/pytest-durations.json" "$duration_file"

    # Build a directory that's just the test assets, so can't access other Python source not in the wheel
    # Each test also get a separate directory since there's a mystery lock somewhere preventing concurrent runs (even)
    # from different Python processes
    mkdir $new_root # Will fail the function if it exists for some reason
    ln -s "$(realpath "$tooling_dir/../python/tests")" $new_root
    cd $new_root

    python -m pytest -v --show-capture=no --log-file="$test_output/pytest-logger.$group.log" \
        --junitxml="$test_output/pytest.$group.xml" \
        --splits 2 --group $group --durations-path="$duration_file" --store-durations \
        "$@" 2>&1 | sed -r "s#^(tests/.*/([^/]+\.py))?#$group: \2#"
}

for i in 1 2 ; do
    worker $i "$@" &
    pids[$i]=$!
done

err=0
for pid in ${pids[*]} ; do
    wait $pid || err=$?
done

exit $err
