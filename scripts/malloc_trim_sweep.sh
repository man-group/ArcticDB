#!/usr/bin/env bash
# Sweep peak RSS vs number of reads, each run in a fresh process.
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
COUNTS=(1 5 10 20 50 100 200)
for n in "${COUNTS[@]}"; do
    python "$SCRIPT_DIR/malloc_trim_read.py" -n "$n" --quiet
    echo
done
