#!/usr/bin/env bash
# Targeted filter matrix at 100M rows to separate cause 1 (decode-ahead live set) from cause 2
# (allocator retention inflating the ru_maxrss high-water). Reuses existing rows_100000000 data.
set -u
PY=python
SCRIPT="$(dirname "$0")/measure_processing_memory.py"
LMDB="$HOME/.tmp/bench_memory_lmdb/rows_100000000"
COMMON=(--measure filter --lmdb-path "$LMDB" --num-cols 100 --zeros)

run() { # label io cpu K arena
  local label="$1" io="$2" cpu="$3" k="$4" arena="$5"
  local args=("${COMMON[@]}" --io-threads "$io" --cpu-threads "$cpu" --malloc-trim)
  [[ "$k" != "0default" ]] && args+=(--processing-units-live "$k")
  echo "############## $label (io=$io cpu=$cpu K=$k arena=$arena) ##############"
  if [[ "$arena" == "1" ]]; then
    MALLOC_ARENA_MAX=1 "$PY" "$SCRIPT" "${args[@]}" 2>&1 | grep -E "baseline|wall =|peak|live decoded|reclaimed"
  else
    "$PY" "$SCRIPT" "${args[@]}" 2>&1 | grep -E "baseline|wall =|peak|live decoded|reclaimed"
  fi
  echo
}

run "IO8  K1  default"      8  12 1  0
run "IO8  K1  arena=1"      8  12 1  1
run "IO16 K1  default"     16  24 1  0
run "IO64 K1  default"     64  96 1  0
run "IO64 K1  arena=1"     64  96 1  1
run "IO64 K0  control"     64  96 0  0
echo "ALL_DONE"
