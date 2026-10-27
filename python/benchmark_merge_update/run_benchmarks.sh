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

build_dir_for() {
    echo "$CPP_DIR/out/${PRESET}-$1-build"
}

extension_for() {
    local ext_suffix
    ext_suffix="$("$PYTHON" -c "import sysconfig; print(sysconfig.get_config_var('EXT_SUFFIX'))")"
    echo "$(build_dir_for "$1")/arcticdb/arcticdb_ext$ext_suffix"
}

build() {
    local suite="$1" only_sparse="$2" write_to_segment="$3"
    local build_dir
    build_dir="$(build_dir_for "$suite")"

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

    local extension
    extension="$(extension_for "$suite")"
    if [[ ! -f "$extension" ]]; then
        echo "ERROR: $extension does not exist. Built extensions:" >&2
        ls "$build_dir"/arcticdb/arcticdb_ext*.so >&2 || true
        exit 1
    fi
    ln -sfn "$extension" "$PYTHON_DIR/$(basename "$extension")"
    log "[$suite] Linked $PYTHON_DIR/$(basename "$extension") -> $extension"
}

# Imports arcticdb the same way benchmark.py does and fails unless both the package and the extension come from this
# repository and the extension is the one built for the suite. Prints what was loaded.
check_loaded_extension() {
    local suite="$1" output_dir="$2" expected_extension
    expected_extension="$(realpath -m "$(extension_for "$suite")")"
    (cd "$output_dir" && PYTHONPATH="$PYTHON_DIR${PYTHONPATH:+:$PYTHONPATH}" "$PYTHON" - \
        "$SCRIPT_DIR" "$PYTHON_DIR" "$expected_extension" <<'EOF'
import hashlib
import os
import sys

script_dir, python_dir, expected_extension = sys.argv[1:]
# benchmark.py is run as a script, so its directory is first on sys.path
sys.path.insert(0, script_dir)
import arcticdb
import arcticdb_ext

package = os.path.realpath(arcticdb.__file__)
extension = os.path.realpath(arcticdb_ext.__file__)
if not package.startswith(os.path.realpath(python_dir) + os.sep):
    sys.exit(f"ERROR: arcticdb was imported from {package}, expected it under {python_dir}")
if extension != expected_extension:
    sys.exit(f"ERROR: arcticdb_ext was loaded from {extension}, expected {expected_extension}")
sha256 = hashlib.sha256()
with open(extension, "rb") as f:
    for chunk in iter(lambda: f.read(1 << 24), b""):
        sha256.update(chunk)
print(f"python: {sys.executable}")
print(f"arcticdb: {package}")
print(f"arcticdb_ext: {extension}")
print(f"arcticdb_ext sha256: {sha256.hexdigest()}")
EOF
    )
}

record_provenance() {
    local suite="$1" output_dir="$2" only_sparse="$3" write_to_segment="$4" repo_dir
    repo_dir="$(cd "$PYTHON_DIR/.." && pwd)"
    echo "suite: $suite"
    echo "ARCTICDB_MERGE_USE_ONLY_SPARSE_CODEPATH: $only_sparse"
    echo "ARCTICDB_MERGE_USE_WRITE_TO_SEGMENT_TASK: $write_to_segment"
    # The benchmark outputs, the notebook and the SeaweedFS data change between suites, so they are not part of the source
    local source_paths=(
        .
        ":(exclude)python/benchmark_merge_update/benchmark_merge_update_*"
        ":(exclude)python/benchmark_merge_update/PerformanceResults.ipynb"
        ":(exclude)python/benchmark_merge_update/seaweed"
    )
    echo "git HEAD: $(git -C "$repo_dir" rev-parse HEAD)"
    echo "git diff HEAD sha256: $(git -C "$repo_dir" diff HEAD -- "${source_paths[@]}" | sha256sum | cut -d' ' -f1)"
    echo "git status --porcelain:"
    git -C "$repo_dir" status --porcelain -- "${source_paths[@]}"
    check_loaded_extension "$suite" "$output_dir"
    echo "========================================================"
}

run_benchmark() {
    local suite="$1" output_dir="$SCRIPT_DIR/$2" only_sparse="$3" write_to_segment="$4"

    log "[$suite] Running benchmarks, output in $output_dir"
    mkdir -p "$output_dir"
    rm -f "$output_dir"/*.pftrace "$output_dir"/*.zip "$output_dir/log.txt"
    record_provenance "$suite" "$output_dir" "$only_sparse" "$write_to_segment" | tee "$output_dir/log.txt"
    # benchmark.py writes the traces to the working directory
    (cd "$output_dir" && PYTHONPATH="$PYTHON_DIR${PYTHONPATH:+:$PYTHONPATH}" "$PYTHON" "$SCRIPT_DIR/benchmark.py" 2>&1) \
        | tee -a "$output_dir/log.txt"
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
        run_benchmark "$suite" "$output_dir" "$only_sparse" "$write_to_segment"
    done

    if [[ "$RUN_NOTEBOOK" == "1" ]]; then
        run_notebook
    fi
}

main "$@"
