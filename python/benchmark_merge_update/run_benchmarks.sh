#!/usr/bin/env bash
# Builds ArcticDB from scratch once per merge update configuration, runs benchmark.py against each build and
# re-executes PerformanceResults.ipynb on the new traces.
#
# Usage: run_benchmarks.sh [suite...]
#   suite is one of: baseline, sparse, wtst_sparse (default: all three, in that order)
#
# Environment:
#   PYTHON          Interpreter that runs the benchmarks (default: python from PATH, i.e. the active venv)
#   NOTEBOOK_PYTHON Interpreter that executes the notebook; needs perfetto, pandas, scipy, matplotlib and nbconvert
#                   (default: $PYTHON)
#   RUN_NOTEBOOK    Set to 0 to skip executing the notebook (default: 1)
#   JOBS            Build parallelism (default: 20)
#   PRESET          CMake configure preset (default: clang-release)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PYTHON_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
CPP_DIR="$(cd "$PYTHON_DIR/../cpp" && pwd)"

PYTHON="${PYTHON:-python}"
NOTEBOOK_PYTHON="${NOTEBOOK_PYTHON:-$PYTHON}"
RUN_NOTEBOOK="${RUN_NOTEBOOK:-1}"
JOBS="${JOBS:-20}"
PRESET="${PRESET:-clang-release}"

# suite -> "<output directory> <ARCTICDB_MERGE_USE_ONLY_SPARSE_CODEPATH> <ARCTICDB_MERGE_USE_WRITE_TO_SEGMENT_TASK>"
declare -A SUITES=(
    [baseline]="benchmark_merge_update_dense OFF OFF"
    [sparse]="benchmark_merge_update_sparse_codepath_for_dense_data ON OFF"
    [wtst_sparse]="benchmark_merge_update_write_to_segment_task_sparse_codepath ON ON"
)
DEFAULT_ORDER=(baseline sparse wtst_sparse)

log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*"
}

check_cache_value() {
    local build_dir="$1" name="$2" expected="$3"
    local actual
    actual="$(sed -n "s/^${name}:[A-Z]*=//p" "$build_dir/CMakeCache.txt")"
    if [[ "$actual" != "$expected" ]]; then
        echo "ERROR: $name is '$actual' in $build_dir/CMakeCache.txt, expected '$expected'" >&2
        exit 1
    fi
}

build() {
    local suite="$1" only_sparse="$2" write_to_segment="$3"
    local build_dir="$CPP_DIR/out/${PRESET}-${suite}-build"

    log "[$suite] Clean build in $build_dir (ONLY_SPARSE_CODEPATH=$only_sparse, WRITE_TO_SEGMENT_TASK=$write_to_segment)"
    rm -rf "$build_dir"
    # -B overrides the preset's binaryDir so that each configuration keeps its own build
    cmake -S "$CPP_DIR" -B "$build_dir" --preset "$PRESET" \
        -DARCTICDB_ENABLE_PERFORMANCE_SAMPLING=ON \
        -DARCTICDB_MERGE_USE_ONLY_SPARSE_CODEPATH="$only_sparse" \
        -DARCTICDB_MERGE_USE_WRITE_TO_SEGMENT_TASK="$write_to_segment"
    check_cache_value "$build_dir" ARCTICDB_ENABLE_PERFORMANCE_SAMPLING ON
    check_cache_value "$build_dir" ARCTICDB_MERGE_USE_ONLY_SPARSE_CODEPATH "$only_sparse"
    check_cache_value "$build_dir" ARCTICDB_MERGE_USE_WRITE_TO_SEGMENT_TASK "$write_to_segment"
    cmake --build "$build_dir" -j "$JOBS" --target arcticdb_ext

    local ext_suffix extension
    ext_suffix="$("$PYTHON" -c "import sysconfig; print(sysconfig.get_config_var('EXT_SUFFIX'))")"
    extension="$build_dir/arcticdb/arcticdb_ext$ext_suffix"
    if [[ ! -f "$extension" ]]; then
        echo "ERROR: $extension does not exist. Built extensions:" >&2
        ls "$build_dir"/arcticdb/arcticdb_ext*.so >&2 || true
        exit 1
    fi
    ln -sfn "$extension" "$PYTHON_DIR/arcticdb_ext$ext_suffix"
    log "[$suite] Linked $PYTHON_DIR/arcticdb_ext$ext_suffix -> $extension"
}

run_benchmark() {
    local suite="$1" output_dir="$SCRIPT_DIR/$2"

    log "[$suite] Running benchmarks, output in $output_dir"
    mkdir -p "$output_dir"
    rm -f "$output_dir"/*.pftrace "$output_dir/log.txt"
    # benchmark.py writes the traces to the working directory
    (cd "$output_dir" && PYTHONPATH="$PYTHON_DIR${PYTHONPATH:+:$PYTHONPATH}" "$PYTHON" "$SCRIPT_DIR/benchmark.py" 2>&1) \
        | tee "$output_dir/log.txt"
    log "[$suite] Done"
}

run_notebook() {
    log "Executing PerformanceResults.ipynb with $NOTEBOOK_PYTHON"
    # The notebook's python3 kernel may resolve to another interpreter, so point it at NOTEBOOK_PYTHON
    local jupyter_path
    jupyter_path="$(mktemp -d)"
    trap 'rm -rf "$jupyter_path"' RETURN
    mkdir -p "$jupyter_path/kernels/python3"
    cat > "$jupyter_path/kernels/python3/kernel.json" <<EOF
{"argv": ["$(command -v "$NOTEBOOK_PYTHON")", "-m", "ipykernel_launcher", "-f", "{connection_file}"],
 "display_name": "Python 3 (ipykernel)", "language": "python"}
EOF
    (cd "$SCRIPT_DIR" && JUPYTER_PATH="$jupyter_path" "$NOTEBOOK_PYTHON" -m jupyter nbconvert --to notebook --execute \
        --inplace --ExecutePreprocessor.timeout=-1 PerformanceResults.ipynb)
}

main() {
    local suites=("$@")
    if [[ ${#suites[@]} -eq 0 ]]; then
        suites=("${DEFAULT_ORDER[@]}")
    fi
    for suite in "${suites[@]}"; do
        if [[ -z "${SUITES[$suite]+set}" ]]; then
            echo "ERROR: unknown suite '$suite', expected one of: ${DEFAULT_ORDER[*]}" >&2
            exit 1
        fi
    done

    for suite in "${suites[@]}"; do
        local output_dir only_sparse write_to_segment
        read -r output_dir only_sparse write_to_segment <<< "${SUITES[$suite]}"
        build "$suite" "$only_sparse" "$write_to_segment"
        run_benchmark "$suite" "$output_dir"
    done

    if [[ "$RUN_NOTEBOOK" == "1" ]]; then
        run_notebook
    fi
}

main "$@"
