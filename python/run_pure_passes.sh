#!/usr/bin/env bash
# Run the memory benchmark against PURE for the currently-built extension, once per allocator
# (glibc default, MALLOC_ARENA_MAX=4, tcmalloc). Emits results_pure_<build>_<alloc>.{md,csv}.
#
# Usage: BUILD=fixes ./run_pure_passes.sh          (K sweep 0 8 default)
#        BUILD=master K_LIST=default ./run_pure_passes.sh   (master has no K knob)
#
# Assumes the 100M PURE symbol already exists (SKIP_WRITE=1); write it once with write_data.py.
set -u

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BUILD="${BUILD:?set BUILD=fixes|master}"
KL="${K_LIST:-0 8 default}"
TCM="${TCM:-/lib/x86_64-linux-gnu/libtcmalloc_minimal.so.4}"

pass() { # alloc_label [env assignments...]
  local alloc="$1"; shift
  echo "########## BUILD=$BUILD ALLOC=$alloc ##########" >&2
  env "$@" \
      USE_PURE=1 SKIP_WRITE=1 ROWS_LIST=100000000 NUM_COLS=100 \
      MEASURES="column_stats filter resample groupby" THREAD_COMBOS="8:12 16:24 64:96" \
      K_LIST="$KL" \
      RESULTS="$DIR/results_pure_${BUILD}_${alloc}.md" \
      CSV="$DIR/results_pure_${BUILD}_${alloc}.csv" \
      "$DIR/bench_memory.sh"
}

pass default
pass arena4   MALLOC_ARENA_MAX=4
pass tcmalloc LD_PRELOAD="$TCM"
echo "done BUILD=$BUILD" >&2
