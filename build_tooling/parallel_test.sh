#!/bin/bash
set -e

tooling_dir="$(dirname $BASH_SOURCE)"
echo Saving results to ${test_output:="$(realpath "$tooling_dir/../cpp/out")"}
[[ -d "$test_output" ]] || mkdir -p "$test_output"

function worker() {
    group=${1:?Must pass the group id argument}
    duration_file="$test_output/pytest-durations.$group.json"
    cp "$tooling_dir/pytest-durations.json" "$duration_file"
    shift
    set -o xtrace -o pipefail
    python -m pytest -v --show-capture=no --log-file="$test_output/pytest-logger.$group.log" \
        --splits 2 --group $group --durations-path="$duration_file" --store-durations \
        "${@:-python/tests}" 2>&1 \
        | sed -r "s#^[./]*(project/)?(python/tests/(([^/]+).*/([^/]+\.py))?)?#$group: \4 \5#"
        #   Regex groups: 1        1 2             34     4   5         53 2 E.g. "1: unit test_x.py"
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
