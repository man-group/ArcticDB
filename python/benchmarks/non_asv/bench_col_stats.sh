#!/usr/bin/env bash
set -euo pipefail
python "$(dirname "$0")/bench_col_stats.py" "$@"
